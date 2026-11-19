"""Create additional __paper.pdf files from existing small reports/aggregates."""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
import hashlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import re
import sys
import tempfile
from time import perf_counter

from paper_values.inputs import PROTOCOLS, SITES, Inputs, protocol_from_run

KINDS = ("itdk-role-by-strategy", "measurement-type-by-strategy")
REPORTS = {f"n-rt-b_fi-m_{kind}.json": kind for kind in KINDS}
OS_KIND = "itdk-role-by-os"
REPORTS[f"n-rt-b_fi-m_{OS_KIND}.json"] = OS_KIND
CONNECTION_KIND = "measurement-type-by-strategy-with-connection"
REPORTS[f"n-rt-b_fi-m_{CONNECTION_KIND}.json"] = CONNECTION_KIND
TCP_FLAGS_KIND = "tcp-flags-by-strategy"
CONNECTION_ITDK_KIND = "itdk-role-by-strategy-connection"
CONNECTION_OS_GROUP_KIND = "operating-system-group-by-strategy-connection"
CONNECTION_BASE_KINDS = (CONNECTION_ITDK_KIND, CONNECTION_OS_GROUP_KIND)
PLOT_KINDS = {
    CONNECTION_ITDK_KIND: "itdk-role-by-strategy",
    CONNECTION_OS_GROUP_KIND: "operating-system-group-by-strategy",
}
REPORTS["c-rt-b_itdk-role-by-strategy.json"] = CONNECTION_ITDK_KIND
REPORTS["c-rt-b_operating-system-group-by-strategy.json"] = CONNECTION_OS_GROUP_KIND
REPORTS[f"n-rt-b_fi-m_{TCP_FLAGS_KIND}.json"] = TCP_FLAGS_KIND
REPORTS["n-rt-b_fi-b_increment-distributions.json"] = "increment-distributions"
PROBING_KIND = "probing-intervals-by-continent"
CONNECTION_PROBING_KIND = PROBING_KIND + "-connection"
PROBING_KINDS = (PROBING_KIND, CONNECTION_PROBING_KIND)
CONNECTION_INCREMENT_KIND = "increment-distributions-connection"
CONNECTION_INTERSECTION_KIND = "strategy-intersection-connection"
CONNECTION_COMPARISON_KINDS = (CONNECTION_INCREMENT_KIND, CONNECTION_INTERSECTION_KIND)
CONNECTION_KINDS = (
    *CONNECTION_BASE_KINDS,
    CONNECTION_PROBING_KIND,
    *CONNECTION_COMPARISON_KINDS,
)
TCP_ONLY_KINDS = (CONNECTION_KIND, TCP_FLAGS_KIND, *CONNECTION_KINDS)
PLOT_KINDS[CONNECTION_PROBING_KIND] = PROBING_KIND
PLOT_KINDS[CONNECTION_INCREMENT_KIND] = "increment-distributions"
PLOT_KINDS[CONNECTION_INTERSECTION_KIND] = "strategy-intersection"
REPORTS["c-rt-b_fi-b_increment-distributions.json"] = CONNECTION_INCREMENT_KIND
REPORTS["c-rt-b_fi-b_strategy-intersection.json"] = CONNECTION_INTERSECTION_KIND
REPORTS[f"n-rt-b_fi-b_{PROBING_KIND}.json"] = PROBING_KIND
REPORTS[f"c-rt-b_fi-b_{PROBING_KIND}.json"] = CONNECTION_PROBING_KIND
INTERSECTION_KIND = "strategy-intersection"
REPORTS[f"n-rt-b_fi-b_{INTERSECTION_KIND}.json"] = INTERSECTION_KIND
OS_GROUP_KIND = "operating-system-group-by-strategy"
REPORTS[f"n-rt-b_fi-m_{OS_GROUP_KIND}.json"] = OS_GROUP_KIND
PHASES = {
    **{kind: ("rt-fi-merged", "merged", "rt-based-base_fixed-interval-mass") for kind in KINDS},
    OS_KIND: ("rt-fi-merged", "merged", "rt-based-base_fixed-interval-mass"),
    OS_GROUP_KIND: ("rt-fi-merged", "merged", "rt-based-base_fixed-interval-mass"),
    CONNECTION_KIND: ("rt-fi-merged", "merged", "rt-based-base_fixed-interval-mass"),
    CONNECTION_ITDK_KIND: ("rt-base", "rt-based-base"),
    CONNECTION_OS_GROUP_KIND: ("rt-base", "rt-based-base"),
    TCP_FLAGS_KIND: ("rt-fi-merged", "merged", "rt-based-base_fixed-interval-mass"),
    "increment-distributions": (
        "rt-base-vs-fi-base",
        "comparison",
        "rt-based-base_fixed-interval-base",
    ),
    PROBING_KIND: ("rt-base-vs-fi-base", "comparison", "rt-based-base_fixed-interval-base"),
    CONNECTION_PROBING_KIND: (
        "rt-base-vs-fi-base",
        "comparison",
        "rt-based-base_fixed-interval-base",
    ),
    INTERSECTION_KIND: ("rt-base-vs-fi-base", "comparison", "rt-based-base_fixed-interval-base"),
    **{
        kind: ("rt-base-vs-fi-base", "comparison", "rt-based-base_fixed-interval-base")
        for kind in CONNECTION_COMPARISON_KINDS
    },
}
MAX_REPORT_BYTES = 4 << 20


@dataclass(frozen=True)
class Job:
    source: Path
    kind: str
    processed_root: Path | None = None

    @property
    def destination(self):
        return self.source.with_name(self.source.stem + "__paper.pdf")


@dataclass(frozen=True)
class Result:
    destination: str
    status: str
    seconds: float = 0
    error: str = ""


def job_for(path: Path, *, processed_root: Path | None = None) -> Job:
    if path.suffix == ".pdf":
        path = path.with_suffix(".json")
    if path.name not in REPORTS:
        raise ValueError(f"unsupported report: {path}; expected one of {', '.join(REPORTS)}")
    if not path.is_file():
        raise FileNotFoundError(path)
    return Job(path.absolute(), REPORTS[path.name], processed_root)


def discover(
    reports_root: Path, campaign: str, *, sites=SITES, protocols=PROTOCOLS, processed_root=None
):
    """Reuse the existing pruned, parallel VP inventory, not a scan of raw data."""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", campaign):
        raise ValueError("invalid campaign identifier")
    root = reports_root / "continental" / campaign
    if not root.is_dir():
        raise FileNotFoundError(root)
    contexts = [
        (
            protocol,
            "connection" if kind in CONNECTION_KINDS else "no-connection",
            phase[0],
            PLOT_KINDS.get(kind, kind),
        )
        for protocol in protocols
        for kind, phase in PHASES.items()
        if kind not in TCP_ONLY_KINDS or protocol == "tcp"
    ]
    inventory = Inputs([root], contexts=contexts, workers=min(8, len(sites)), sites=sites)
    try:
        inventory.scan()
        if inventory.errors:
            raise OSError(f"report inventory failed: {inventory.errors}")
        jobs = []
        for path in inventory.inventory[root]:
            relative = path.relative_to(root)
            if relative.parts[0] not in sites or path.name not in REPORTS:
                continue
            if REPORTS[path.name] in TCP_ONLY_KINDS:
                run_index = 2 if relative.parts[1] == "figures" else 1
                if protocol_from_run(relative.parts[run_index]) != "tcp":
                    continue
            # Select each report's own canonical connection mode and phase.
            kind = REPORTS[path.name]
            mode = "connection" if kind in CONNECTION_KINDS else "no-connection"
            suffix = (mode, *PHASES[kind][1:])
            if relative.parts[-len(suffix) - 1 : -1] != suffix:
                continue
            jobs.append(job_for(path, processed_root=processed_root))
        return jobs
    finally:
        inventory.close()


def _shares(values, *, zero_allowed=False):
    from ipid_analysis.strategies import STRATEGY_NAMES

    if not isinstance(values, dict) or set(values) - set(STRATEGY_NAMES):
        raise ValueError("missing or unknown strategy percentages")
    result = {}
    for name, value in values.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"invalid percentage for {name}")
        if not math.isfinite(value) or not 0 <= value <= 100:
            raise ValueError(f"invalid percentage for {name}: {value}")
        result[name] = float(value)
    total = sum(result.values())
    if not (zero_allowed and total == 0) and abs(total - 100) > 1e-5:
        raise ValueError(f"strategy percentages must sum to 100, got {total}")
    return result


def plot_data(kind, report):
    """Read the actual analysis report schema; do not reconstruct values from PDFs."""
    if kind == CONNECTION_ITDK_KIND:
        if tuple(
            report.get(field) for field in ("protocol", "connection_mode", "interval", "scale")
        ) != ("tcp", "connection", "rt-based", "base"):
            raise ValueError("expected a connection-oriented TCP RT-base ITDK report")
        return plot_data(PLOT_KINDS[kind], report)
    if kind == OS_KIND:
        from ipid_analysis.paper_os_plot import load_data

        return load_data(report)
    if kind == TCP_FLAGS_KIND:
        from ipid_analysis.plot_tcp_flags_strategy import FLAG_GROUPS

        if report.get("protocol", "tcp") != "tcp":
            raise ValueError("TCP-flags figures are only supported for TCP")
        return {flags: _shares(report["bars"][flags]["percentages"]) for flags in FLAG_GROUPS}
    if kind in ("measurement-type-by-strategy", CONNECTION_KIND):
        modes = ("RT-based", "Fixed-Interval")
        if kind == CONNECTION_KIND:
            from ipid_analysis.plot_strategy_refinement import MODES_WITH_CONNECTION

            if report.get("protocol", "tcp") != "tcp":
                raise ValueError("connection-oriented refinement is only supported for TCP")
            modes = MODES_WITH_CONNECTION
        bars = report["bars"]
        return {mode: _shares(bars[mode]["percentages"]) for mode in modes}
    totals = report["plot_role_totals"]
    result = {}
    for role in ("Transit-Observed", "No Transit Evidence"):
        total = totals[role]
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise ValueError(f"invalid role total: {role}")
        shares = _shares(report["plot_percentages"][role], zero_allowed=total == 0)
        if shares.get("NOT_ENOUGH_SAMPLES", 0):
            raise ValueError(
                "ITDK plot percentages must already fold insufficient samples into Unclassified"
            )
        result[role] = shares
    return result


def _comparison_aggregate(job, report):
    """Resolve existing exact aggregates without crossing connection modes/phases."""
    from ipid_analysis.paper_increment_plot import aggregate_path

    connection = job.kind in CONNECTION_COMPARISON_KINDS
    mode = "connection" if connection else "no-connection"
    if connection and tuple(
        report.get(key) for key in ("figure", "protocol", "connection_mode", "target")
    ) != (
        PLOT_KINDS[job.kind],
        "tcp",
        mode,
        "tcp.ipid.connection.rt-based.base+tcp.ipid.connection.fixed-interval.base",
    ):
        raise ValueError("expected a connection-oriented TCP RT-base/FI-base comparison report")
    path = aggregate_path(job.source, report, processed_root=job.processed_root, mode=mode)
    if connection and path.parts[-4:-1] != (mode, *PHASES[job.kind][1:]):
        raise ValueError("expected the connection RT-base/FI-base comparison aggregate")
    return path


def _initialize_worker(directory):
    # Imports in worker() remain lazy, so these limits precede NumPy/Matplotlib.
    for name in (
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
    ):
        os.environ[name] = "1"
    os.environ["MPLBACKEND"] = "Agg"
    os.environ["MPLCONFIGDIR"] = str(Path(directory) / f"mpl-{os.getpid()}")
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    sys.dont_write_bytecode = True


def worker(job: Job) -> Result:
    from ipid_analysis.artifacts import (
        assert_complete,
        cache_current,
        completion_path,
        fingerprint,
        publish_new_file,
    )
    from ipid_analysis.runtime import temporary_path

    start = perf_counter()
    destination = job.destination
    try:
        if os.path.lexists(destination):
            return Result(str(destination), "skipped-existing")
        if os.path.lexists(completion_path(destination)):
            raise FileExistsError(f"existing receipt without PDF; left untouched: {destination}")
        assert_complete(job.source)
        inputs = [job.source]
        before = [fingerprint(job.source)]
        with job.source.open("rb") as stream:
            content = stream.read(MAX_REPORT_BYTES + 1)
        if len(content) > MAX_REPORT_BYTES:
            raise ValueError(
                "report is unexpectedly large; only small aggregate JSONs are supported"
            )
        report = json.loads(content)
        profile = Path(__file__).with_name("paper_plot_style.py")
        profile_bytes = profile.read_bytes()
        if job.kind in PROBING_KINDS:
            from ipid_analysis.paper_probing_plot import draw, load_series, paths

            aggregate, metadata = paths(job.source, report, job.processed_root)
            if not aggregate.is_file() or not metadata.is_file():
                raise ValueError(
                    "p99.9/256 paper quantiles missing; run paper-plots with --prepare-probing-intervals once"
                )
            inputs.extend((aggregate, metadata))
            before.extend(fingerprint(path) for path in (aggregate, metadata))
            data = load_series(aggregate, metadata, job.source)
            for name in ("paper_probing_plot.py", "paper_figures.py"):
                profile_bytes += Path(__file__).with_name(name).read_bytes()
        elif job.kind in (OS_GROUP_KIND, CONNECTION_OS_GROUP_KIND):
            from ipid_analysis.paper_increment_plot import aggregate_path
            from ipid_analysis.paper_os_group_plot import draw, load_data

            aggregate = aggregate_path(
                job.source,
                report,
                processed_root=job.processed_root,
                phase=PHASES[job.kind][1:],
                mode="connection" if job.kind == CONNECTION_OS_GROUP_KIND else "no-connection",
            )
            inputs.append(aggregate)
            before.append(fingerprint(aggregate))
            data = load_data(
                aggregate, report, connection_base=job.kind == CONNECTION_OS_GROUP_KIND
            )
            for name in (
                "paper_os_group_plot.py",
                "paper_figures.py",
                "plot_os_group_strategy.py",
            ):
                profile_bytes += Path(__file__).with_name(name).read_bytes()
        elif job.kind in (INTERSECTION_KIND, CONNECTION_INTERSECTION_KIND):
            from ipid_analysis.paper_intersection_plot import draw, load_matrix

            aggregate = _comparison_aggregate(job, report)
            inputs.append(aggregate)
            before.append(fingerprint(aggregate))
            data = load_matrix(aggregate, report)
            for name in ("paper_intersection_plot.py", "paper_figures.py"):
                profile_bytes += Path(__file__).with_name(name).read_bytes()
        elif job.kind in ("increment-distributions", CONNECTION_INCREMENT_KIND):
            from ipid_analysis.paper_increment_plot import draw, load_histogram

            aggregate = _comparison_aggregate(job, report)
            inputs.append(aggregate)
            before.append(fingerprint(aggregate))
            data = load_histogram(aggregate, report)
            profile_bytes += Path(__file__).with_name("paper_increment_plot.py").read_bytes()
        else:
            from ipid_analysis.paper_plot_style import draw

            data = plot_data(job.kind, report)
            if job.kind == OS_KIND:
                for name in ("paper_os_plot.py", "plot_os_group_strategy.py", "plot_itdk_os.py"):
                    profile_bytes += Path(__file__).with_name(name).read_bytes()
        parameters = {
            "operation": "paper-ready-v1",
            "kind": job.kind,
            "profile_sha256": hashlib.sha256(profile_bytes).hexdigest(),
            "report_sha256": hashlib.sha256(content).hexdigest(),
        }
        # A competing renderer may have published while this report was read.
        if os.path.lexists(destination):
            status = (
                "skipped-current"
                if cache_current(destination, inputs, parameters)
                else "skipped-existing"
            )
            return Result(str(destination), status, perf_counter() - start)
        temporary = temporary_path(destination)
        try:
            draw(PLOT_KINDS.get(job.kind, job.kind), data, temporary)
            if [fingerprint(path) for path in inputs] != before:
                raise ValueError("source input changed during rendering; refusing publication")
            publish_new_file(temporary, destination, inputs=inputs, parameters=parameters)
        finally:
            temporary.unlink(missing_ok=True)
        return Result(str(destination), "created", perf_counter() - start)
    except FileExistsError as exc:
        if os.path.lexists(destination):
            return Result(str(destination), "skipped-existing", perf_counter() - start)
        return Result(str(destination), "failed", perf_counter() - start, str(exc))
    except Exception as exc:
        return Result(
            str(destination), "failed", perf_counter() - start, f"{type(exc).__name__}: {exc}"
        )


def run(jobs, *, workers=4, dry_run=False):
    """Existing outputs are immutable, including stale/legacy paper variants."""
    if workers < 1:
        raise ValueError("workers must be positive")
    jobs = sorted(set(jobs), key=lambda job: str(job.source))
    results, pending = [], []
    for job in jobs:
        if os.path.lexists(job.destination):
            results.append(Result(str(job.destination), "skipped-existing"))
        elif dry_run:
            results.append(Result(str(job.destination), "would-create"))
        else:
            pending.append(job)
    if not pending:
        return results
    from ipid_analysis.runtime import cpu_budget, scratch_root, validate_storage

    validate_storage()
    workers = min(workers, cpu_budget(), len(pending))
    # Scratch/cache writes are private and bounded by the existing runtime.
    with tempfile.TemporaryDirectory(prefix="paper-ready-", dir=scratch_root()) as directory:
        if workers == 1:
            previous = dict(os.environ)
            bytecode = sys.dont_write_bytecode
            try:
                _initialize_worker(directory)
                results.extend(worker(job) for job in pending)
            finally:
                os.environ.clear()
                os.environ.update(previous)
                sys.dont_write_bytecode = bytecode
        else:
            with ProcessPoolExecutor(
                max_workers=workers,
                mp_context=multiprocessing.get_context("spawn"),
                initializer=_initialize_worker,
                initargs=(directory,),
            ) as pool:
                results.extend(pool.map(worker, pending))
    return sorted(results, key=lambda result: result.destination)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths", nargs="*", type=Path, help="explicit original report JSON or PDF paths"
    )
    parser.add_argument("--campaign", help="campaign under reports/continental/")
    parser.add_argument(
        "--reports-root", type=Path, default=Path(__file__).resolve().parents[1] / "reports"
    )
    parser.add_argument(
        "--processed-root",
        type=Path,
        help="remap a campaign histogram's recorded continental/ path to this processed root",
    )
    parser.add_argument("--sites", nargs="+", choices=SITES, default=list(SITES))
    parser.add_argument("--protocols", nargs="+", choices=PROTOCOLS, default=list(PROTOCOLS))
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument(
        "--prepare-probing-intervals",
        action="store_true",
        help="explicit one-off p99.9/256 aggregation from stored intervals into new paper-only files; sequential and potentially slow",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="list destinations without writing anything"
    )
    args = parser.parse_args(argv)
    if not args.paths and not args.campaign:
        parser.error("supply --campaign or explicit report paths")
    if args.workers < 1:
        parser.error("--workers must be positive")
    try:
        jobs = [job_for(path, processed_root=args.processed_root) for path in args.paths]
        if args.campaign:
            jobs.extend(
                discover(
                    args.reports_root,
                    args.campaign,
                    sites=args.sites,
                    protocols=args.protocols,
                    processed_root=args.processed_root,
                )
            )
        if not jobs:
            raise ValueError("no supported paper-plot reports found")
        start = perf_counter()
        if args.prepare_probing_intervals and not args.dry_run:
            from ipid_analysis.paper_probing_plot import prepare

            for job in sorted(set(jobs), key=lambda job: str(job.source)):
                if job.kind in PROBING_KINDS and not os.path.lexists(job.destination):
                    print(f"preparing p99.9/256 probing intervals: {job.source}", flush=True)
                    print(f"paper quantiles: {prepare(job)}", flush=True)
        results = run(jobs, workers=args.workers, dry_run=args.dry_run)
    except (OSError, ValueError) as exc:
        parser.exit(1, f"paper-plots: {exc}\n")
    for result in results:
        print(
            f"{result.status}: {result.destination} ({result.seconds:.3f}s)"
            + (f" - {result.error}" if result.error else "")
        )
    failures = sum(result.status == "failed" for result in results)
    print(
        f"paper-plots: {len(results)} jobs, {sum(r.status == 'created' for r in results)} created, "
        f"{failures} failed, {perf_counter() - start:.3f}s total"
    )
    return int(failures > 0)


if __name__ == "__main__":
    raise SystemExit(main())
