"""Additional probing-interval violins with detected anycast /24s excluded."""

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
from time import perf_counter

import pyarrow.parquet as pq


def filtered_continent_map(con, geo, prefixes):
    """Use a small integer hash anti-join, without an INET extension or raw data."""
    from ipid_analysis.anycast_census import PREFIX_SCHEMA

    with pq.ParquetFile(prefixes) as source:
        if not source.schema_arrow.equals(PREFIX_SCHEMA) or source.metadata.num_rows > 1 << 24:
            raise ValueError("expected a small unique IPv4 /24 table")
        table = source.read()
    con.register("anycast_prefixes", table)
    con.read_parquet(str(geo)).create_view("anycast_geo_input")
    con.execute(
        """
        CREATE TEMP VIEW anycast_geo AS
        SELECT *, PREFIX24 IN (SELECT PREFIX24 FROM anycast_prefixes) AS IS_ANYCAST
        FROM (
            SELECT IP_ADDR, CONTINENT_CODE, CONTINENT,
                CASE WHEN regexp_full_match(IP_ADDR, '(0|[1-9][0-9]{0,2})(\\.(0|[1-9][0-9]{0,2})){3}')
                    AND a BETWEEN 0 AND 255 AND b BETWEEN 0 AND 255
                    AND c BETWEEN 0 AND 255 AND d BETWEEN 0 AND 255
                THEN a * 65536 + b * 256 + c ELSE NULL END AS PREFIX24
            FROM (
                SELECT *, try_cast(split_part(IP_ADDR, '.', 1) AS BIGINT) AS a,
                    try_cast(split_part(IP_ADDR, '.', 2) AS BIGINT) AS b,
                    try_cast(split_part(IP_ADDR, '.', 3) AS BIGINT) AS c,
                    try_cast(split_part(IP_ADDR, '.', 4) AS BIGINT) AS d
                FROM anycast_geo_input
            )
        )
    """
    )
    audit = con.execute("""
        SELECT grouping(CONTINENT_CODE) AS is_total, CONTINENT_CODE, count(*)::BIGINT AS original,
            count(*) FILTER (WHERE IS_ANYCAST)::BIGINT AS removed,
            count(*) FILTER (WHERE PREFIX24 IS NULL)::BIGINT AS invalid,
            count(DISTINCT PREFIX24)::BIGINT AS original_prefixes,
            count(DISTINCT PREFIX24) FILTER (WHERE IS_ANYCAST)::BIGINT AS removed_prefixes
        FROM anycast_geo GROUP BY GROUPING SETS ((CONTINENT_CODE), ())
        ORDER BY is_total, CONTINENT_CODE
    """).fetchall()
    if any(row[4] for row in audit):
        raise ValueError("invalid/non-IPv4 addresses in the stored continent map")
    from ipid_analysis.paper_figures import CONTINENT_NAMES

    if any(not row[0] and row[1] not in (*CONTINENT_NAMES, "ZZ") for row in audit):
        raise ValueError("unknown continent code in stored continent map")
    con.execute(
        "CREATE TEMP VIEW anycast_filtered_geo AS SELECT IP_ADDR, CONTINENT_CODE, CONTINENT FROM anycast_geo WHERE NOT IS_ANYCAST"
    )
    counts = {
        code: {"original": original, "excluded": removed, "retained": original - removed}
        for total, code, original, removed, _, _, _ in audit
        if not total
    }
    statistics = dict(
        schema_version=2,
        population="unique IPs in the original comparison's stored union continent map",
        prefix_length=24,
        continents={},
    )
    for total, code, original, removed, _, prefixes_before, prefixes_removed in audit:
        row = dict(
            addresses=dict(original=original, excluded=removed, retained=original - removed),
            prefixes=dict(
                original=prefixes_before,
                excluded=prefixes_removed,
                retained=prefixes_before - prefixes_removed,
            ),
        )
        if total:
            statistics["total"] = row
        else:
            statistics["continents"][code] = row
    return "anycast_filtered_geo", {
        "anycast_exclusion_by_continent": counts,
        "anycast_filtering": statistics,
    }


@dataclass(frozen=True)
class Job:
    source: Path
    kind: str
    processed_root: Path | None
    census: Path
    output: Path | None = None

    @property
    def destination(self):
        return self.output or self.source.with_name(destination_name(self.source))


def destination_name(source):
    if source.stem.endswith("__full"):
        return source.stem.removesuffix("__full") + "__paper__full__anycast.pdf"
    return source.stem + "__paper__anycast.pdf"


def full_job(campaign, site, reports_root, census, processed_root=None):
    """Select only the registered full-RT / historical-FI paired population."""
    from ipid_analysis.artifacts import assert_complete, file_digest
    from ipid_analysis.paper_probing_plot import KIND
    from ipid_analysis.paper_ready import CONNECTION_PROBING_KIND
    from paper_values.followups import COMPARISON, registration

    spec = registration(campaign)
    if spec is None:
        raise ValueError("no registered full TCP follow-up for this campaign")
    root = Path(reports_root).resolve()
    relative = Path("followups") / spec["measurement_run"] / spec["analysis_id"] / site
    directory = root / relative
    source = (
        directory
        / "paired/figures"
        / spec["zmap_ids"][site]
        / COMPARISON
        / f"c-rt-b_fi-b_{KIND}__full.json"
    )
    marker = directory / "done__full.json"
    for path in (marker, source):
        assert_complete(path)
        if path.stat().st_size > 4 << 20:
            raise ValueError("unexpected full follow-up manifest/report size")
    done = json.loads(marker.read_text())
    if any(
        done.get(key) != value
        for key, value in (
            ("original_campaign", campaign),
            ("site", site),
            ("measurement_run", spec["measurement_run"]),
            ("analysis_id", spec["analysis_id"]),
        )
    ):
        raise ValueError("full follow-up completion differs from registration")
    entries = [item for item in done["artifacts"] if Path(item["path"]) == source]
    if (
        len(entries) != 1
        or entries[0]["sha256"] != file_digest(source)
        or entries[0]["size_bytes"] != source.stat().st_size
    ):
        raise ValueError("full interval report differs from completion inventory")
    report = json.loads(source.read_text())
    processed = (
        Path(processed_root).resolve()
        if processed_root is not None
        else root.parent / "data/processed"
    )
    expected = (
        processed
        / relative
        / "paired"
        / spec["zmap_ids"][site]
        / COMPARISON
        / f"c-rt-b_fi-b_{KIND}.pq"
    )
    if report.get("aggregate") != str(expected):
        raise ValueError(
            "full interval aggregate does not belong to the registered paired population"
        )
    return Job(source, CONNECTION_PROBING_KIND, None, census)


def _context(job):
    from ipid_analysis.anycast_census import load_dataset
    from ipid_analysis.artifacts import file_digest
    from ipid_analysis.paper_probing_plot import paths

    census, prefixes = load_dataset(job.census)
    report = json.loads(job.source.read_text())
    aggregate, metadata = paths(job.source, report, job.processed_root, anycast=job.census)
    parameters = dict(
        operation="paper-anycast-plot-v1",
        census_sha256=file_digest(job.census),
        profile_sha256=hashlib.sha256(
            Path(__file__).read_bytes()
            + Path(__file__).with_name("paper_probing_plot.py").read_bytes()
            + Path(__file__).with_name("paper_plot_style.py").read_bytes()
        ).hexdigest(),
    )
    if census["schema_version"] == 2:
        parameters["impact_metadata_version"] = 2
    return (
        census,
        aggregate,
        metadata,
        [job.source, aggregate, metadata, job.census, prefixes],
        parameters,
    )


def existing(job):
    from ipid_analysis.artifacts import cache_current, completion_path
    from ipid_analysis.paper_probing_plot import load_series

    if not os.path.lexists(job.destination):
        if os.path.lexists(completion_path(job.destination)):
            raise FileExistsError("orphan anycast PDF receipt left untouched")
        return False
    census, aggregate, metadata, inputs, parameters = _context(job)
    marker = completion_path(job.destination)
    if not marker.is_file() or json.loads(marker.read_text()).get("parameters", {}) != parameters:
        raise FileExistsError(
            "existing anycast PDF belongs to another census/profile; use --output-root to keep both comparisons"
        )
    load_series(aggregate, metadata, job.source, anycast=job.census)
    if not cache_current(job.destination, inputs, parameters):
        raise FileExistsError(
            "existing anycast PDF is bound to other inputs/profile; choose a separate output location, never overwrite"
        )
    if census["schema_version"] == 2:
        from paper_values.cache import verify_file

        sidecar = job.destination.with_suffix(".json")
        if not sidecar.is_file():
            raise FileExistsError("Anycast metadata sidecar is missing; finish --refresh")
        verify_file(sidecar)
        if sidecar.read_bytes() != metadata.read_bytes():
            raise FileExistsError(
                "Anycast metadata sidecar differs from prepared statistics; finish --refresh"
            )
    return True


def worker(job):
    from ipid_analysis.artifacts import fingerprint, publish_new_file
    from ipid_analysis.paper_probing_plot import draw, load_series
    from ipid_analysis.paper_ready import Result
    from ipid_analysis.runtime import temporary_path

    start = perf_counter()
    temporary = None
    try:
        if existing(job):
            return Result(str(job.destination), "skipped-current")
        _, aggregate, metadata, inputs, parameters = _context(job)
        before = [fingerprint(path) for path in inputs]
        rows = load_series(aggregate, metadata, job.source, anycast=job.census)
        if not rows:
            raise ValueError("no located finite interval populations remain after exclusion")
        temporary = temporary_path(job.destination)
        draw(job.kind + "-anycast", rows, temporary)
        if before != [fingerprint(path) for path in inputs]:
            raise ValueError("anycast plot source changed during rendering")
        if parameters.get("impact_metadata_version") == 2:
            publish_new_file(
                metadata,
                job.destination.with_suffix(".json"),
                inputs=inputs,
                parameters={**parameters, "operation": "anycast-probing-metadata-v2"},
            )
        publish_new_file(temporary, job.destination, inputs=inputs, parameters=parameters)
        return Result(str(job.destination), "created", perf_counter() - start)
    except Exception as exc:
        return Result(
            str(job.destination), "failed", perf_counter() - start, f"{type(exc).__name__}: {exc}"
        )
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def run(jobs, *, prepare=False, workers=4, dry_run=False, refresh=False):
    from ipid_analysis.paper_probing_plot import prepare as prepare_quantiles
    from ipid_analysis.paper_ready import run as render

    if refresh and dry_run:
        from ipid_analysis.paper_ready import Result

        return [
            Result(
                str(job.destination),
                "would-refresh" if os.path.lexists(job.destination) else "would-create",
            )
            for job in sorted(set(jobs), key=lambda item: str(item.source))
        ]
    if refresh and not dry_run:
        from ipid_analysis.anycast_refresh import refresh as replace_plots

        return replace_plots(jobs, prepare=prepare, workers=workers)

    if not dry_run:
        for job in sorted(set(jobs), key=lambda item: str(item.source)):
            if not existing(job) and prepare:
                print(f"Preparing anycast-filtered p99.9 quantiles: {job.source}", flush=True)
                print(prepare_quantiles(job, anycast=job.census), flush=True)
    return render(jobs, workers=workers, dry_run=dry_run, worker_fn=worker)


def main(argv=None):
    from ipid_analysis.anycast_census import load_dataset
    from ipid_analysis.paper_ready import PROBING_KINDS, PROTOCOLS, SITES, discover, job_for

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", type=Path)
    parser.add_argument("--campaign")
    parser.add_argument(
        "--include-tcp-full",
        action="store_true",
        help="include registered Full TCP Connection alongside the three EU stateless plots",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="back up and stage selected Anycast PDFs, then explicitly replace their PDFs/sidecars",
    )
    parser.add_argument(
        "--tcp-full",
        action="store_true",
        help="use the registered full TCP RT / historical FI paired follow-up; never the old RT sample",
    )
    parser.add_argument(
        "--census",
        type=Path,
        required=True,
        help="pinned dataset.json produced by prepare-anycast-census",
    )
    parser.add_argument(
        "--reports-root", type=Path, default=Path(__file__).resolve().parents[1] / "reports"
    )
    parser.add_argument("--processed-root", type=Path)
    parser.add_argument(
        "--connection-mode",
        choices=("no-connection", "connection"),
        help="select stateless TCP/SYN or connection-oriented interval reports explicitly",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        help="optional separate reports tree, e.g. for comparing later census snapshots",
    )
    parser.add_argument("--sites", nargs="+", choices=SITES, default=list(SITES))
    parser.add_argument("--protocols", nargs="+", choices=PROTOCOLS, default=list(PROTOCOLS))
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument(
        "--prepare",
        action="store_true",
        help="one-off sequential aggregation from existing processed per-IP intervals",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if not args.paths and not args.campaign:
        parser.error("supply --campaign or explicit original interval reports")
    if args.tcp_full and (
        args.paths
        or not args.campaign
        or args.protocols != ["tcp"]
        or args.connection_mode != "connection"
    ):
        parser.error(
            "--tcp-full requires --campaign, --protocols tcp, --connection-mode connection and no positional paths"
        )
    if args.include_tcp_full and (
        args.tcp_full
        or args.paths
        or not args.campaign
        or args.sites != ["eu"]
        or set(args.protocols) != set(PROTOCOLS)
        or args.connection_mode != "no-connection"
    ):
        parser.error("--include-tcp-full requires the three stateless EU campaign plots")
    try:
        data, _ = load_dataset(args.census)
        print(
            f"Anycast exclusion: {data['dates'][0]} through {data['dates'][-1]}; complete period={data.get('complete_period', data['complete_month'])}; high-confidence /24 union"
        )
        original = [job_for(path, processed_root=args.processed_root) for path in args.paths]
        if args.campaign and not args.tcp_full:
            original.extend(
                discover(
                    args.reports_root,
                    args.campaign,
                    sites=args.sites,
                    protocols=args.protocols,
                    processed_root=args.processed_root,
                )
            )
        jobs = []
        if args.tcp_full:
            jobs.extend(
                full_job(args.campaign, site, args.reports_root, args.census, args.processed_root)
                for site in args.sites
            )
        for job in original:
            if job.kind in PROBING_KINDS:
                mode = "connection" if job.kind.endswith("-connection") else "no-connection"
                if args.connection_mode is not None and mode != args.connection_mode:
                    continue
                output = None
                if args.output_root is not None:
                    output = args.output_root / job.source.resolve().relative_to(
                        args.reports_root.resolve()
                    )
                    output = output.with_name(destination_name(job.source))
                jobs.append(Job(job.source, job.kind, job.processed_root, args.census, output))
        if args.tcp_full and args.output_root is not None:
            jobs = [
                Job(
                    job.source,
                    job.kind,
                    job.processed_root,
                    job.census,
                    args.output_root
                    / job.source.relative_to(args.reports_root.resolve()).parent
                    / destination_name(job.source),
                )
                for job in jobs
            ]
        if args.include_tcp_full:
            jobs.append(
                full_job(args.campaign, "eu", args.reports_root, args.census, args.processed_root)
            )
            if len(jobs) != 4:
                raise ValueError("expected exactly four distinct EU comparison plots")
        if args.refresh and args.output_root is not None:
            raise ValueError(
                "--refresh uses canonical Anycast destinations; do not combine with --output-root"
            )
        if any(job.kind not in PROBING_KINDS for job in original) and args.paths:
            raise ValueError("only probing-intervals-by-continent reports are supported")
        if not jobs:
            raise ValueError("no probing-interval reports found")
        results = run(
            jobs,
            prepare=args.prepare,
            workers=args.workers,
            dry_run=args.dry_run,
            refresh=args.refresh,
        )
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.exit(1, f"paper-anycast-plots: {exc}\n")
    for result in results:
        print(
            f"{result.status}: {result.destination}"
            + (f" - {result.error}" if result.error else "")
        )
    if any(result.status == "failed" for result in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
