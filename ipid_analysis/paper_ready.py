"""Create additional __paper.pdf files from existing small reports, without reanalysis."""

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

from paper_values.inputs import PROTOCOLS, SITES, Inputs

KINDS = ("itdk-role-by-strategy", "measurement-type-by-strategy")
REPORTS = {f"n-rt-b_fi-m_{kind}.json": kind for kind in KINDS}
MAX_REPORT_BYTES = 4 << 20


@dataclass(frozen=True)
class Job:
    source: Path
    kind: str

    @property
    def destination(self):
        return self.source.with_name(self.source.stem + "__paper.pdf")


@dataclass(frozen=True)
class Result:
    destination: str
    status: str
    seconds: float = 0
    error: str = ""


def job_for(path: Path) -> Job:
    if path.suffix == ".pdf":
        path = path.with_suffix(".json")
    if path.name not in REPORTS:
        raise ValueError(f"unsupported report: {path}; expected one of {', '.join(REPORTS)}")
    if not path.is_file():
        raise FileNotFoundError(path)
    return Job(path.absolute(), REPORTS[path.name])


def discover(reports_root: Path, campaign: str, *, sites=SITES, protocols=PROTOCOLS):
    """Reuse the existing pruned, parallel VP inventory, not a scan of raw data."""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", campaign):
        raise ValueError("invalid campaign identifier")
    root = reports_root / "continental" / campaign
    if not root.is_dir():
        raise FileNotFoundError(root)
    contexts = [
        (protocol, "no-connection", "rt-fi-merged", kind)
        for protocol in protocols
        for kind in KINDS
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
            # Only canonical stateless RT-base/FI-mass merged reports.
            if relative.parts[-4:-1] != (
                "no-connection",
                "merged",
                "rt-based-base_fixed-interval-mass",
            ):
                continue
            jobs.append(job_for(path))
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
    if kind == "measurement-type-by-strategy":
        bars = report["bars"]
        return {
            mode: _shares(bars[mode]["percentages"]) for mode in ("RT-based", "Fixed-Interval")
        }
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
        before = fingerprint(job.source)
        with job.source.open("rb") as stream:
            content = stream.read(MAX_REPORT_BYTES + 1)
        if len(content) > MAX_REPORT_BYTES:
            raise ValueError(
                "report is unexpectedly large; only small aggregate JSONs are supported"
            )
        report = json.loads(content)
        percentages = plot_data(job.kind, report)
        profile = Path(__file__).with_name("paper_plot_style.py")
        parameters = {
            "operation": "paper-ready-v1",
            "kind": job.kind,
            "profile_sha256": hashlib.sha256(profile.read_bytes()).hexdigest(),
            "report_sha256": hashlib.sha256(content).hexdigest(),
        }
        # A competing renderer may have published while this report was read.
        if os.path.lexists(destination):
            status = (
                "skipped-current"
                if cache_current(destination, [job.source], parameters)
                else "skipped-existing"
            )
            return Result(str(destination), status, perf_counter() - start)
        from ipid_analysis.paper_plot_style import draw

        temporary = temporary_path(destination)
        try:
            draw(job.kind, percentages, temporary)
            if fingerprint(job.source) != before:
                raise ValueError("source report changed during rendering; refusing publication")
            publish_new_file(temporary, destination, inputs=[job.source], parameters=parameters)
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
    parser.add_argument("--sites", nargs="+", choices=SITES, default=list(SITES))
    parser.add_argument("--protocols", nargs="+", choices=PROTOCOLS, default=list(PROTOCOLS))
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument(
        "--dry-run", action="store_true", help="list destinations without writing anything"
    )
    args = parser.parse_args(argv)
    if not args.paths and not args.campaign:
        parser.error("supply --campaign or explicit report paths")
    if args.workers < 1:
        parser.error("--workers must be positive")
    try:
        jobs = [job_for(path) for path in args.paths]
        if args.campaign:
            jobs.extend(
                discover(
                    args.reports_root, args.campaign, sites=args.sites, protocols=args.protocols
                )
            )
        if not jobs:
            raise ValueError("no supported RT-base/FI-mass reports found")
        start = perf_counter()
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
