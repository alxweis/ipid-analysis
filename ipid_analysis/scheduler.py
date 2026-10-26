"""Single-host admission and process supervision; S3 remains the durable queue.

The dispatcher never runs science or waits for a dependency. Two reserved
critical slots and one background slot are deliberately conservative defaults.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

from loguru import logger

from ipid_analysis.runtime import available_memory, cpu_budget, scratch_root

GIB = 1 << 30
SITES = ("eu", "as", "af", "na", "sa", "au")


@dataclass(frozen=True)
class Cost:
    cpus: int
    memory: int
    scratch: int


@dataclass(frozen=True)
class Task:
    kind: str
    uri: str
    prefix: str
    scope: str = ""
    priority: int = 0

    @property
    def key(self):
        return hashlib.sha256((self.kind + ":" + self.uri).encode()).hexdigest()


def tasks_from_objects(objects: set[str], prefix: str) -> list[Task]:
    """Derive dependencies from immutable control keys, without reading bulk data."""
    from ipid_analysis.continental import campaign_parts

    tasks = []
    campaigns = {}
    categories = {
        "jobs": ("handoff", 0),
        "strategy-jobs": ("strategies", 1),
        "interprotocol-target-jobs": ("targets", 2),
        "analysis-jobs": ("analysis", 10),
        "interprotocol-jobs": ("interprotocol", 10),
    }
    prefix = prefix.rstrip("/")
    for uri in sorted(objects):
        if not uri.startswith(prefix + "/"):
            continue
        relative = uri[len(prefix) + 1 :].split("/")
        if len(relative) == 3:
            site_prefix, scope, parts = prefix, "", relative
        else:
            parsed = campaign_parts(uri, prefix)
            if parsed is None:
                continue
            campaign, site, parts = parsed
            campaign_prefix = f"{prefix}/continental/{campaign}"
            campaigns[campaign] = campaign_prefix
            if site is None:
                continue
            site_prefix = f"{campaign_prefix}/sites/{site}"
            scope = f"continental/{campaign}/{site}"
        if len(parts) != 3 or parts[0] not in categories or parts[-1] != "request.json":
            continue
        job = uri.rsplit("/", 1)[0]
        if job + "/done.json" in objects or job + "/failed.json" in objects:
            continue
        kind, priority = categories[parts[0]]
        if scope and kind == "analysis":
            campaign_prefix = site_prefix.rsplit("/sites/", 1)[0]
            if (
                campaign_prefix + "/os/done.json" not in objects
                or site_prefix + "/final/submitted.json" not in objects
            ):
                continue
        if not scope and kind == "strategies":
            continue
        tasks.append(Task(kind, uri, site_prefix, scope, priority))

    for campaign, campaign_prefix in sorted(campaigns.items()):
        if (
            campaign_prefix + "/done.json" in objects
            or campaign_prefix + "/failed.json" in objects
        ):
            continue
        if all(f"{campaign_prefix}/sites/{s}/discovery.json" in objects for s in SITES):
            if not any(
                campaign_prefix + suffix in objects
                for suffix in ("/os/request.json", "/union/failed.json")
            ):
                tasks.append(
                    Task("union", campaign_prefix + "/union/request.json", prefix, priority=1)
                )
        if campaign_prefix + "/os/done.json" in objects:
            for site in SITES:
                site_prefix = f"{campaign_prefix}/sites/{site}"
                if site_prefix + "/final/request.json" not in objects:
                    continue
                scope = f"continental/{campaign}/{site}"
                regular = [
                    u
                    for u in objects
                    if u.startswith(site_prefix + "/strategy-jobs/")
                    and u.endswith("/request.json")
                ]
                # The final site event is published only after all eight
                # interprotocol measurements and all 26 regular runs.
                if len(regular) != 3:
                    continue
                if not any(
                    site_prefix + "/final/" + x in objects
                    for x in ("submitted.json", "failed.json")
                ):
                    tasks.append(
                        Task("submit", site_prefix + "/final/request.json", prefix, scope, 10)
                    )
                elif (
                    site_prefix + "/final/submitted.json" in objects
                    and site_prefix + "/final/done.json" not in objects
                    and site_prefix + "/final/failed.json" not in objects
                ):
                    finals = [
                        u.replace("/strategy-jobs/", "/analysis-jobs/").rsplit("/", 1)[0]
                        + "/done.json"
                        for u in regular
                    ]
                    if (
                        all(u in objects for u in finals)
                        and site_prefix + "/interprotocol-jobs/interprotocol/done.json" in objects
                    ):
                        tasks.append(
                            Task(
                                "site-done", site_prefix + "/final/request.json", prefix, scope, 10
                            )
                        )
        if all(f"{campaign_prefix}/sites/{s}/final/done.json" in objects for s in SITES):
            tasks.append(
                Task("campaign-done", campaign_prefix + "/campaign.json", prefix, priority=10)
            )
    return tasks


def process_group_usage(pgid: int) -> int:
    """Aggregate Linux process-group RSS, including postprocess/RIPE children."""
    total = 0
    for directory in Path("/proc").glob("[0-9]*"):
        try:
            stat = (directory / "stat").read_text().rsplit(")", 1)[1].split()
            if int(stat[2]) == pgid:
                total += int(stat[21]) * os.sysconf("SC_PAGE_SIZE")
        except (OSError, ValueError, IndexError):
            continue
    return total


def directory_bytes(directory: Path) -> int:
    total = 0
    for root, dirs, files in os.walk(directory, followlinks=False):
        dirs[:] = [d for d in dirs if not (Path(root) / d).is_symlink()]
        for name in files:
            path = Path(root) / name
            try:
                if not path.is_symlink():
                    total += path.stat().st_size
            except FileNotFoundError:
                pass
    return total


@dataclass
class Active:
    task: Task
    cost: Cost
    process: subprocess.Popen
    directory: Path
    log: object
    started: float
    cpu_ids: tuple[int, ...] = ()
    failure: str | None = None


class Scheduler:
    def __init__(self, client, prefix: str, work_root: Path, *, batch_size=1_000_000, threads=0):
        self.client, self.prefix = client, prefix.rstrip("/")
        self.root = work_root / "scheduled"
        self.root.mkdir(parents=True, exist_ok=True)
        self.batch_size = batch_size
        cpus = min(int(os.environ.get("IPID_ANALYSIS_WORKER_CPUS", cpu_budget())), cpu_budget())
        if cpus < 3:
            raise ValueError("scheduled worker needs at least three CPUs (critical + background)")
        memory = min(
            int(os.environ.get("IPID_ANALYSIS_WORKER_MEMORY_BYTES", available_memory() * 0.8)),
            int(available_memory() * 0.8),
        )
        reserve = int(os.environ.get("IPID_ANALYSIS_SCRATCH_RESERVE_BYTES", 10 * GIB))
        free = shutil.disk_usage(self.root).free
        scratch = min(
            int(os.environ.get("IPID_ANALYSIS_WORKER_SCRATCH_BYTES", max(0, free - reserve))),
            free - reserve,
        )
        if memory < 3 * GIB or scratch < 3 * GIB:
            raise ValueError(
                "insufficient RAM or scratch for reserved critical/background capacity"
            )
        self.total = Cost(cpus, memory, scratch)
        critical_cpu = max(1, cpus // 6)
        self.critical = Cost(min(critical_cpu, threads or critical_cpu), memory // 8, scratch // 8)
        self.background = Cost(
            min(cpus - 2 * critical_cpu, threads or cpus), memory * 3 // 4, scratch * 3 // 4
        )
        self.reserve = reserve
        self.active: dict[str, Active] = {}
        self.first_seen = {}
        self.retry_after = {}
        self.errors = []
        self.finished = set()
        self.failure_executor = ThreadPoolExecutor(max_workers=1)
        self.failure_futures = {}
        from ipid_analysis.s3_workflow import S3Client

        self.failure_client = (
            S3Client(client._run, timeout=30) if isinstance(client, S3Client) else client
        )

    def cost(self, task):
        return self.critical if task.priority < 10 else self.background

    def admitted(self, task):
        critical = task.priority < 10
        if sum(a.task.priority < 10 for a in self.active.values()) >= 2 and critical:
            return False
        if (
            critical
            and task.kind != "handoff"
            and any(
                a.task.priority < 10 and a.task.kind != "handoff" for a in self.active.values()
            )
        ):
            return False
        if any(a.task.priority >= 10 for a in self.active.values()) and not critical:
            return False
        cost = self.cost(task)
        used = Cost(
            *(
                sum(getattr(a.cost, f) for a in self.active.values())
                for f in ("cpus", "memory", "scratch")
            )
        )
        if any(
            getattr(used, f) + getattr(cost, f) > getattr(self.total, f)
            for f in ("cpus", "memory", "scratch")
        ):
            return False
        # Pending critical reservations remain available even when a background
        # job starts first. Retained failures and unrelated disk users count too.
        reserved = sum(
            max(0, a.cost.scratch - directory_bytes(a.directory / "scratch"))
            for a in self.active.values()
        )
        if shutil.disk_usage(self.root).free - self.reserve - reserved < cost.scratch:
            return False
        if available_memory() < cost.memory:
            return False
        return True

    def start(self, task):
        cost = self.cost(task)
        directory = self.root / task.key
        directory.mkdir(exist_ok=True)
        allocated = {cpu for a in self.active.values() for cpu in a.cpu_ids}
        allowed = (
            sorted(os.sched_getaffinity(0))
            if hasattr(os, "sched_getaffinity")
            else list(range(self.total.cpus))
        )
        cpu_ids = tuple(cpu for cpu in allowed if cpu not in allocated)[: cost.cpus]
        spec = dict(
            cpu_ids=cpu_ids,
            task=asdict(task),
            cost=asdict(cost),
            batch_size=self.batch_size,
            control_root=str(scratch_root()),
            scratch=str(directory / "scratch"),
        )
        spec_path = directory / "task.json"
        spec_path.write_text(json.dumps(spec), encoding="utf-8")
        env = dict(os.environ)
        env.update(
            IPID_ANALYSIS_SCOPE=task.scope,
            IPID_ANALYSIS_S3_PREFIX=task.prefix,
            IPID_ANALYSIS_TMP_DIR=spec["scratch"],
            IPID_ANALYSIS_THREADS=str(cost.cpus),
            IPID_ANALYSIS_JOB_MEMORY_BYTES=str(cost.memory),
            IPID_ANALYSIS_JOB_SCRATCH_BYTES=str(cost.scratch),
            IPID_ANALYSIS_DUCKDB_MEMORY=f"{cost.memory // 2}B",
            IPID_ANALYSIS_WORKFLOW_ROOT=self.prefix,
            IPID_ANALYSIS_LOCK_DIR=str(scratch_root() / "global-locks"),
            IPID_ANALYSIS_CONTROL_ROOT=str(scratch_root()),
            IPID_ANALYSIS_RIPE_WORKERS=str(cost.cpus),
        )
        for key in (
            "OMP_NUM_THREADS",
            "OPENBLAS_NUM_THREADS",
            "MKL_NUM_THREADS",
            "NUMEXPR_NUM_THREADS",
        ):
            env[key] = str(cost.cpus)
        log = (directory / "supervisor.log").open("a", encoding="utf-8")
        command = [sys.executable, "-m", "ipid_analysis.workflow_task", str(spec_path)]
        process = subprocess.Popen(
            command,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=os.name == "posix",
        )
        self.active[task.key] = Active(
            task, cost, process, directory, log, time.monotonic(), cpu_ids
        )
        logger.info(
            f"started {task.kind}: {task.uri}; CPUs={cost.cpus}, RAM={cost.memory / GIB:.1f}GiB, scratch={cost.scratch / GIB:.1f}GiB"
        )

    @staticmethod
    def terminate(active):
        if active.process.poll() is None:
            if os.name == "posix":
                os.killpg(active.process.pid, signal.SIGTERM)
            else:
                active.process.terminate()
            try:
                active.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                if os.name == "posix":
                    os.killpg(active.process.pid, signal.SIGKILL)
                else:
                    active.process.kill()
                active.process.wait(timeout=5)

    def reap(self):
        from ipid_analysis.continental import publish_task_failure

        for key, active in list(self.active.items()):
            if active.process.poll() is None:
                memory = process_group_usage(active.process.pid) if os.name == "posix" else 0
                scratch = directory_bytes(active.directory / "scratch")
                timeout = int(os.environ.get("IPID_ANALYSIS_JOB_TIMEOUT", "172800"))
                if (
                    memory > active.cost.memory
                    or scratch > active.cost.scratch
                    or time.monotonic() - active.started > timeout
                ):
                    active.failure = f"job resource limit exceeded: RSS={memory}, scratch={scratch}, timeout={timeout}"
                    self.terminate(active)
                else:
                    continue
            code = active.process.returncode
            active.log.close()
            if code == 75:  # old live process still owns the same per-job lease
                self.retry_after[key] = time.monotonic() + 30
            elif code or active.failure:
                message = (
                    active.failure
                    or f"job process exited {code}; see {active.directory / 'supervisor.log'}"
                )
                self.errors.append(message)
                logger.error(f"failed {active.task.kind}: {active.task.uri}: {message}")
                self.failure_futures[key] = self.failure_executor.submit(
                    publish_task_failure,
                    self.failure_client,
                    active.task,
                    message,
                    active.directory,
                )
            if code == 0 and not active.failure:
                logger.success(
                    f"task finished: {active.task.kind}: {active.task.uri}; log={active.directory / 'supervisor.log'}"
                )
            self.finished.add(key)
            del self.active[key]

    def dispatch(self, tasks):
        for key, future in list(self.failure_futures.items()):
            if future.done():
                try:
                    future.result()
                except Exception:
                    logger.exception("could not publish supervised task failure")
                    self.retry_after[key] = time.monotonic() + 30
                del self.failure_futures[key]
        now = time.monotonic()
        for task in tasks:
            self.first_seen.setdefault(task.key, now)
        # Age within the critical lane prevents target/strategy starvation.
        ordered = sorted(
            tasks,
            key=lambda t: (
                t.priority >= 10,
                t.priority - (now - self.first_seen[t.key]) / 60,
                self.first_seen[t.key],
                t.scope,
                t.uri,
            ),
        )
        for task in ordered:
            if (
                task.key in self.active
                or task.key in self.failure_futures
                or task.key in self.finished
                or now < self.retry_after.get(task.key, 0)
            ):
                continue
            if self.admitted(task):
                self.start(task)

    def close(self):
        for active in self.active.values():
            self.terminate(active)
            active.log.close()
        self.active.clear()
        self.failure_executor.shutdown(wait=True, cancel_futures=True)

    def run(self, *, poll_interval=30, once=False):
        # The one listing thread only does metadata I/O; slow S3 cannot suspend
        # supervision or dispatch of already discovered critical work.
        executor = ThreadPoolExecutor(max_workers=1)
        future = None
        next_poll = 0
        tasks = []
        saw_listing = False
        try:
            while True:
                self.reap()
                now = time.monotonic()
                if future is not None and future.done():
                    try:
                        tasks = tasks_from_objects(set(future.result()), self.prefix)
                        self.finished.clear()
                        saw_listing = True
                    except Exception:
                        logger.exception("workflow listing failed")
                        if once:
                            raise
                    future = None
                    next_poll = now + (0 if once else poll_interval)
                self.dispatch(tasks)
                if (
                    once
                    and saw_listing
                    and future is None
                    and not tasks
                    and not self.active
                    and not self.failure_futures
                ):
                    if self.errors:
                        raise RuntimeError("; ".join(self.errors))
                    return
                if future is None and now >= next_poll:
                    future = executor.submit(
                        lambda: [
                            u
                            for u in self.client.objects(self.prefix)
                            if u.endswith(
                                (
                                    "/request.json",
                                    "/done.json",
                                    "/failed.json",
                                    "/discovery.json",
                                    "/submitted.json",
                                    "/campaign.json",
                                )
                            )
                            and not any(
                                part in u[len(self.prefix) :].split("/")
                                for part in ("attempts", "results", "reports")
                            )
                        ]
                    )
                time.sleep(1)
        finally:
            self.close()
            executor.shutdown(wait=True, cancel_futures=True)
