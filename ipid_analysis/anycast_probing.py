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
        SELECT CONTINENT_CODE, count(*)::BIGINT AS original,
            count(*) FILTER (WHERE IS_ANYCAST)::BIGINT AS removed,
            count(*) FILTER (WHERE PREFIX24 IS NULL)::BIGINT AS invalid
        FROM anycast_geo GROUP BY CONTINENT_CODE ORDER BY CONTINENT_CODE
    """).fetchall()
    if any(row[3] for row in audit):
        raise ValueError("invalid/non-IPv4 addresses in the stored continent map")
    con.execute(
        "CREATE TEMP VIEW anycast_filtered_geo AS SELECT IP_ADDR, CONTINENT_CODE, CONTINENT FROM anycast_geo WHERE NOT IS_ANYCAST"
    )
    counts = {
        code: {"original": original, "excluded": removed, "retained": original - removed}
        for code, original, removed, _ in audit
    }
    return "anycast_filtered_geo", {"anycast_exclusion_by_continent": counts}


@dataclass(frozen=True)
class Job:
    source: Path
    kind: str
    processed_root: Path | None
    census: Path
    output: Path | None = None

    @property
    def destination(self):
        return self.output or self.source.with_name(self.source.stem + "__paper__anycast.pdf")


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
    _, aggregate, metadata, inputs, parameters = _context(job)
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
        publish_new_file(temporary, job.destination, inputs=inputs, parameters=parameters)
        return Result(str(job.destination), "created", perf_counter() - start)
    except Exception as exc:
        return Result(
            str(job.destination), "failed", perf_counter() - start, f"{type(exc).__name__}: {exc}"
        )
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def run(jobs, *, prepare=False, workers=4, dry_run=False):
    from ipid_analysis.paper_probing_plot import prepare as prepare_quantiles
    from ipid_analysis.paper_ready import run as render

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
    try:
        data, _ = load_dataset(args.census)
        print(
            f"Anycast exclusion: {data['dates'][0]} through {data['dates'][-1]}; complete month={data['complete_month']}; high-confidence /24 union"
        )
        original = [job_for(path, processed_root=args.processed_root) for path in args.paths]
        if args.campaign:
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
                    output = output.with_name(job.source.stem + "__paper__anycast.pdf")
                jobs.append(Job(job.source, job.kind, job.processed_root, args.census, output))
        if any(job.kind not in PROBING_KINDS for job in original) and args.paths:
            raise ValueError("only probing-intervals-by-continent reports are supported")
        if not jobs:
            raise ValueError("no probing-interval reports found")
        results = run(jobs, prepare=args.prepare, workers=args.workers, dry_run=args.dry_run)
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
