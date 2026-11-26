"""Pin and cache a monthly union of LACeS high-confidence IPv4 /24 prefixes."""

import argparse
import calendar
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import hashlib
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import tempfile
from urllib.request import Request, urlopen

import pyarrow as pa
import pyarrow.parquet as pq

REPOSITORY = "ut-dacs/anycast-census"
API = "https://api.github.com/repos/" + REPOSITORY
RAW = "https://raw.githubusercontent.com/" + REPOSITORY
RULE = "max(AB across IPv4 protocols) > 3 OR max(GCD across IPv4 protocols) > 1"
PREFIX_SCHEMA = pa.schema([("PREFIX24", pa.uint32())])
MAX_SNAPSHOT_BYTES = 64 << 20


def _read_url(url, limit):
    request = Request(
        url,
        headers={"User-Agent": "ipid-analysis-anycast/1", "Accept": "application/vnd.github+json"},
    )
    with urlopen(request, timeout=120) as response:
        if response.geturl().split("/", 3)[2] not in (
            "api.github.com",
            "raw.githubusercontent.com",
        ):
            raise ValueError("unexpected census download host")
        content = response.read(limit + 1)
    if len(content) > limit:
        raise ValueError("census response exceeds size limit")
    return content


def _json_url(url):
    return json.loads(_read_url(url, 8 << 20))


def month_dates(month):
    if not re.fullmatch(r"20\d{2}-(0[1-9]|1[0-2])", month):
        raise ValueError("expected --month YYYY-MM")
    year, number = map(int, month.split("-"))
    return [
        date(year, number, day).isoformat()
        for day in range(1, calendar.monthrange(year, number)[1] + 1)
    ]


def period_dates(month, start_date=None, end_date=None):
    days = month_dates(month)
    if start_date is None and end_date is None:
        return days
    if start_date not in days or end_date not in days or start_date > end_date:
        raise ValueError(
            "supply both --start-date and --end-date in YYYY-MM-DD order within --month"
        )
    return [day for day in days if start_date <= day <= end_date]


def inventory(month, *, revision=None, allow_partial=False, start_date=None, end_date=None):
    expected = period_dates(month, start_date, end_date)
    revision = revision or _json_url(API + "/git/ref/heads/main")["object"]["sha"]
    if not re.fullmatch(r"[a-f0-9]{40}", revision):
        raise ValueError("census revision must be a pinned Git commit")
    year, number = month.split("-")
    months = _json_url(API + f"/contents/{year}?ref={revision}")
    entry = next(
        (item for item in months if item["name"] == number and item["type"] == "dir"), None
    )
    if entry is None:
        raise ValueError(f"no census snapshots for {month} at {revision}")
    tree = _json_url(API + f"/git/trees/{entry['sha']}?recursive=1")
    if tree.get("truncated"):
        raise ValueError("truncated census month inventory")
    snapshots = []
    for item in tree["tree"]:
        match = re.fullmatch(r"(\d{2})/IPv4\.parquet", item["path"])
        if item["type"] != "blob" or not match:
            continue
        day = f"{month}-{match[1]}"
        if day not in month_dates(month):
            raise ValueError("invalid date in census inventory")
        if day not in expected:
            continue
        if not 0 < item["size"] <= MAX_SNAPSHOT_BYTES or not re.fullmatch(
            r"[a-f0-9]{40}", item["sha"]
        ):
            raise ValueError("invalid census snapshot size or blob identity")
        snapshots.append(
            dict(
                date=day,
                git_blob=item["sha"],
                size_bytes=item["size"],
                url=f"{RAW}/{revision}/{year}/{number}/{item['path']}",
            )
        )
    snapshots.sort(key=lambda item: item["date"])
    days = [item["date"] for item in snapshots]
    if not days or len(days) != len(set(days)):
        raise ValueError("empty/duplicate census snapshot dates")
    missing = sorted(set(expected) - set(days))
    if missing and not allow_partial:
        raise ValueError(
            f"{month} is incomplete: missing {', '.join(missing)}; use --allow-partial explicitly"
        )
    return revision, snapshots, missing


def _blob_digest(content):
    return hashlib.sha1(b"blob " + str(len(content)).encode() + b"\0" + content).hexdigest()


def snapshot_prefixes(path):
    """Support the published per-protocol schema and its AB/GCD summary form."""
    with pq.ParquetFile(path) as source:
        names = source.schema_arrow.names
        if "prefix" not in names or source.metadata.num_rows > 2_000_000:
            raise ValueError("unexpected IPv4 census schema/size")
        ab = [n for n in names if n == "AB" or n.startswith("AB_") and n.endswith("v4")]
        gcd = [n for n in names if n == "GCD" or n.startswith("GCD_") and n.endswith("v4")]
        if not ab or not gcd:
            raise ValueError("census must contain IPv4 AB and GCD evidence")
        prefixes, rows, selected = set(), 0, 0
        for batch in source.iter_batches(batch_size=65536, columns=["prefix", *ab, *gcd]):
            for row in batch.to_pylist():
                rows += 1

                def maximum(columns):
                    scores = [row[n] for n in columns if row[n] is not None]
                    if any(
                        type(x) not in (int, float) or not math.isfinite(x) or x < 0 or int(x) != x
                        for x in scores
                    ):
                        raise ValueError("invalid census evidence count")
                    return max(scores, default=0)

                if maximum(ab) <= 3 and maximum(gcd) <= 1:
                    continue
                network = ipaddress.ip_network(row["prefix"], strict=True)
                if network.version != 4 or network.prefixlen != 24:
                    raise ValueError(
                        "use candidate IPv4 /24 prefixes, never backing routing prefixes"
                    )
                prefixes.add(int(network.network_address) >> 8)
                selected += 1
    return prefixes, dict(rows=rows, selected_rows=selected, prefixes=len(prefixes))


def prepare(
    month,
    output_root,
    *,
    cache_root,
    revision=None,
    allow_partial=False,
    workers=4,
    start_date=None,
    end_date=None,
):
    from ipid_analysis.artifacts import file_digest, publish_new_file
    from ipid_analysis.runtime import exclusive_lock, scratch_root

    if not 1 <= workers <= 8:
        raise ValueError("census download workers must be between one and eight")
    expected = period_dates(month, start_date, end_date)
    revision, snapshots, missing = inventory(
        month,
        revision=revision,
        allow_partial=allow_partial,
        start_date=start_date,
        end_date=end_date,
    )
    destination = Path(output_root) / month / revision
    if start_date is not None:
        destination /= start_date.replace("-", "") + "-" + end_date.replace("-", "")
    manifest, aggregate = destination / "dataset.json", destination / "prefixes.pq"
    if manifest.is_file():
        load_dataset(manifest)
        return manifest
    cache = Path(cache_root) / revision
    cache.mkdir(parents=True, exist_ok=True)

    def download(item):
        path = cache / (item["date"] + ".parquet")
        with exclusive_lock("anycast-census:" + str(path.resolve())):
            if path.exists():
                content = path.read_bytes()
            else:
                content = _read_url(item["url"], MAX_SNAPSHOT_BYTES)
                if len(content) != item["size_bytes"] or _blob_digest(content) != item["git_blob"]:
                    raise ValueError("download does not match pinned Git census blob")
                temporary = path.with_suffix(".part")
                temporary.write_bytes(content)
                os.replace(temporary, path)
            if len(content) != item["size_bytes"] or _blob_digest(content) != item["git_blob"]:
                raise ValueError("cached census snapshot does not match pinned Git blob")
        prefixes, stats = snapshot_prefixes(path)
        return prefixes, item | stats | {"sha256": hashlib.sha256(content).hexdigest()}

    with exclusive_lock("anycast-dataset:" + str(destination.resolve())):
        if manifest.is_file():
            load_dataset(manifest)
            return manifest
        union, provenance = set(), []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for prefixes, item in pool.map(download, snapshots):
                union.update(prefixes)
                provenance.append(item)
        if not union:
            raise ValueError("empty high-confidence anycast union; refusing a no-op filter")
        with tempfile.TemporaryDirectory(
            prefix="anycast-census-", dir=scratch_root()
        ) as directory:
            table = Path(directory) / "prefixes.pq"
            pq.write_table(
                pa.table({"PREFIX24": sorted(union)}, schema=PREFIX_SCHEMA),
                table,
                compression="zstd",
            )
            metadata = dict(
                schema_version=2 if start_date is not None else 1,
                repository=REPOSITORY,
                revision=revision,
                requested_month=month,
                complete_month=[item["date"] for item in provenance] == month_dates(month),
                missing_dates=missing,
                dates=[item["date"] for item in provenance],
                filter=RULE,
                policy="exclude union of high-confidence candidate /24s on any included day",
                prefix_count=len(union),
                prefix_file="prefixes.pq",
                prefix_sha256=file_digest(table),
                snapshots=provenance,
                citation="https://doi.org/10.1145/3730567.3764484",
                license="CC BY-NC-SA 4.0",
            )
            if start_date is not None:
                metadata.update(
                    period_start=start_date,
                    period_end=end_date,
                    complete_period=not missing,
                    expected_snapshots=len(expected),
                    confidence="high-confidence",
                )
            document = Path(directory) / "dataset.json"
            document.write_text(json.dumps(metadata, indent=2) + "\n")
            publish_new_file(
                table,
                aggregate,
                parameters={
                    "operation": "anycast-month-union-v1",
                    "month": month,
                    "revision": revision,
                    "rule": RULE,
                },
            )
            publish_new_file(
                document,
                manifest,
                inputs=[aggregate],
                parameters={"operation": "anycast-dataset-v1"},
            )
    return manifest


def load_dataset(manifest):
    from ipid_analysis.artifacts import assert_complete, file_digest

    manifest = Path(manifest)
    assert_complete(manifest)
    if manifest.stat().st_size > 1 << 20:
        raise ValueError("unexpected census manifest size")
    data = json.loads(manifest.read_text())
    version = data.get("schema_version")
    expected = period_dates(
        data["requested_month"], data.get("period_start"), data.get("period_end")
    )
    days = data["dates"]
    missing = sorted(set(expected) - set(days))
    if (
        version not in (1, 2)
        or data.get("repository") != REPOSITORY
        or data.get("filter") != RULE
        or data.get("prefix_file") != "prefixes.pq"
        or not re.fullmatch(r"[a-f0-9]{40}", data["revision"])
        or not days
        or days != sorted(set(days))
        or set(days) - set(expected)
        or data.get("missing_dates") != missing
        or data.get("complete_month") != (days == month_dates(data["requested_month"]))
        or [item["date"] for item in data["snapshots"]] != days
        or version == 1
        and ("period_start" in data or "period_end" in data)
        or version == 2
        and (
            not data.get("period_start")
            or not data.get("period_end")
            or data.get("confidence") != "high-confidence"
            or data.get("complete_period") != (not missing)
            or data.get("expected_snapshots") != len(expected)
        )
    ):
        raise ValueError("invalid anycast dataset provenance/period")
    prefixes = manifest.with_name("prefixes.pq")
    assert_complete(prefixes)
    if file_digest(prefixes) != data["prefix_sha256"] or prefixes.stat().st_size > 8 << 20:
        raise ValueError("anycast prefix bytes disagree with manifest")
    with pq.ParquetFile(prefixes) as source:
        if (
            source.metadata.num_rows != data["prefix_count"]
            or not 0 < data["prefix_count"] <= 1 << 24
            or not source.schema_arrow.equals(PREFIX_SCHEMA)
        ):
            raise ValueError("invalid anycast prefix table")
        values = source.read()["PREFIX24"].to_pylist()
        if values != sorted(set(values)) or values[-1] >= 1 << 24:
            raise ValueError("invalid/duplicate IPv4 /24 prefixes")
    return data, prefixes


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", required=True)
    parser.add_argument(
        "--start-date", help="inclusive YYYY-MM-DD within --month; requires --end-date"
    )
    parser.add_argument(
        "--end-date", help="inclusive YYYY-MM-DD within --month; requires --start-date"
    )
    parser.add_argument(
        "--revision", help="pin an existing census Git commit (default: current main)"
    )
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="explicitly use available dates and record missing days",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "references/anycast-census",
    )
    parser.add_argument(
        "--cache-root",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "tmp/anycast-census",
    )
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args(argv)
    try:
        manifest = prepare(
            args.month,
            args.output_root,
            cache_root=args.cache_root,
            revision=args.revision,
            allow_partial=args.allow_partial,
            workers=args.workers,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        data, _ = load_dataset(manifest)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.exit(1, f"anycast-census: {exc}\n")
    print(
        f"Census: {data['dates'][0]} through {data['dates'][-1]}; {len(data['dates'])} days; complete period={data.get('complete_period', data['complete_month'])}; {data['prefix_count']} /24s"
    )
    print(f"Manifest: {manifest}")


if __name__ == "__main__":
    main()
