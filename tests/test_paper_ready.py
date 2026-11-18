"""Read-only input and create-only output contracts for paper rendering."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import zlib

from ipid_analysis import artifacts, paper_ready, transport


def reports():
    rt = {
        "REFLECTION": 8,
        "CONSTANT": 3,
        "SINGLE": 3,
        "PER_DESTINATION": 15,
        "PER_BUCKET": 69,
        "UNCLASSIFIED": 2,
    }
    fi = {
        "SINGLE": 2,
        "PER_DESTINATION": 2,
        "PER_BUCKET": 2,
        "MULTI": 13,
        "RANDOM": 55,
        "UNCLASSIFIED": 23,
        "NOT_ENOUGH_SAMPLES": 3,
    }
    return {
        "measurement-type-by-strategy": {
            "bars": {"RT-based": {"percentages": rt}, "Fixed-Interval": {"percentages": fi}}
        },
        "itdk-role-by-strategy": {
            "plot_role_totals": {"Transit-Observed": 100, "No Transit Evidence": 100},
            "plot_percentages": {
                "Transit-Observed": {
                    "REFLECTION": 44,
                    "SINGLE": 12,
                    "PER_DESTINATION": 10,
                    "PER_BUCKET": 34,
                },
                "No Transit Evidence": {
                    "REFLECTION": 8,
                    "CONSTANT": 3,
                    "SINGLE": 4,
                    "PER_DESTINATION": 16,
                    "PER_BUCKET": 68,
                    "RANDOM": 1,
                },
            },
        },
    }


def stamp(path):
    value = path.stat()
    return value.st_mtime_ns, value.st_size, hashlib.sha256(path.read_bytes()).hexdigest()


class PaperReadyTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        environment = patch.dict(
            os.environ,
            {
                "IPID_ANALYSIS_STORAGE_MODE": "local",
                "IPID_ANALYSIS_SCRATCH_STORAGE": "disk",
                "IPID_ANALYSIS_TMP_DIR": str(self.root / "scratch"),
                "MPLCONFIGDIR": str(self.root / "mpl"),
            },
        )
        environment.start()
        self.addCleanup(environment.stop)
        from ipid_analysis.paper_plot_style import _font

        _font.cache_clear()
        self.addCleanup(_font.cache_clear)

    def fixture(self, kind, directory=None):
        directory = directory or self.root
        directory.mkdir(parents=True, exist_ok=True)
        source = directory / f"n-rt-b_fi-m_{kind}.json"
        source.write_text(json.dumps(reports()[kind]), encoding="utf-8")
        source.with_suffix(".pdf").write_bytes(b"existing original PDF, never parsed or modified")
        return paper_ready.job_for(source)

    def test_outputs_are_siblings_originals_and_repeats_are_untouched(self):
        jobs = [self.fixture(kind) for kind in paper_ready.KINDS]
        for job in jobs:
            marker = artifacts.completion_path(job.source)
            marker.parent.mkdir(exist_ok=True)
            marker.write_text(
                json.dumps(
                    {
                        "status": "complete",
                        "size_bytes": job.source.stat().st_size,
                        "sha256": hashlib.sha256(job.source.read_bytes()).hexdigest(),
                    }
                )
            )
        inputs = {path: stamp(path) for path in self.root.rglob("*") if path.is_file()}
        results = paper_ready.run(jobs, workers=1)
        self.assertEqual([r.status for r in results], ["created", "created"])
        for path, before in inputs.items():
            self.assertEqual(stamp(path), before)
        complete = {path: stamp(path) for path in self.root.rglob("*") if path.is_file()}
        with patch("ipid_analysis.paper_plot_style.draw", side_effect=AssertionError("must skip")):
            self.assertTrue(all(r.status == "skipped-existing" for r in paper_ready.run(jobs)))
        self.assertEqual(
            complete, {path: stamp(path) for path in self.root.rglob("*") if path.is_file()}
        )
        for job in jobs:
            self.assertEqual(job.destination.parent, job.source.parent)
            self.assertEqual(job.destination.name, job.source.stem + "__paper.pdf")
            pdf = job.destination.read_bytes()
            media = [float(x) for x in re.search(rb"/MediaBox\s*\[([^]]+)\]", pdf)[1].split()]
            self.assertAlmostEqual(media[2], 239.76)
            sizes = set()
            for raw in re.findall(rb"stream\r?\n(.*?)\r?\nendstream", pdf, re.S):
                try:
                    content = zlib.decompress(raw)
                except zlib.error:
                    continue
                if b"/DeviceRGB cs" in content:
                    sizes.update(float(v) for v in re.findall(rb"/\w+ ([0-9.]+) Tf", content))
            self.assertEqual(sizes, {7.0})
            self.assertNotIn(b"/Subtype /Type3", pdf)
            artifacts.assert_complete(job.destination)

    def test_rendering_does_not_open_measurement_parquet_or_duckdb(self):
        job = self.fixture("measurement-type-by-strategy")
        with (
            patch("pyarrow.parquet.read_table", side_effect=AssertionError("no parquet reads")),
            patch("duckdb.connect", side_effect=AssertionError("no queries")),
        ):
            self.assertEqual(paper_ready.run([job], workers=1)[0].status, "created")

    def test_existing_paper_output_even_invalid_is_never_repaired(self):
        job = self.fixture(paper_ready.KINDS[0])
        job.destination.write_bytes(b"pre-existing paper file")
        before = stamp(job.destination)
        self.assertEqual(paper_ready.run([job])[0].status, "skipped-existing")
        self.assertEqual(stamp(job.destination), before)
        self.assertFalse(artifacts.completion_path(job.destination).exists())

    def test_existing_orphan_receipt_is_preserved(self):
        job = self.fixture(paper_ready.KINDS[0])
        marker = artifacts.completion_path(job.destination)
        marker.parent.mkdir()
        marker.write_text("pre-existing receipt")
        before = stamp(marker)
        self.assertEqual(paper_ready.run([job], workers=1)[0].status, "failed")
        self.assertEqual(stamp(marker), before)
        self.assertFalse(job.destination.exists())

    def test_invalid_data_does_not_publish_or_modify_any_source(self):
        job = self.fixture("measurement-type-by-strategy")
        report = reports()[job.kind]
        report["bars"]["RT-based"]["percentages"]["SINGLE"] = float("nan")
        job.source.write_text(json.dumps(report))
        before = stamp(job.source)
        self.assertEqual(paper_ready.run([job], workers=1)[0].status, "failed")
        self.assertEqual(stamp(job.source), before)
        self.assertFalse(job.destination.exists())
        self.assertFalse(artifacts.completion_path(job.destination).exists())

    def test_dry_run_and_discovery_prune_unselected_sites_and_phases(self):
        root = self.root / "reports"
        campaign = root / "continental" / "fixture"
        jobs = []
        for site in ("eu", "na"):
            for protocol in ("icmp", "tcp-80", "udp-dns-53"):
                directory = (
                    campaign
                    / site
                    / "figures"
                    / (protocol + "_fixture")
                    / "no-connection"
                    / "merged"
                    / "rt-based-base_fixed-interval-mass"
                )
                jobs.append(self.fixture("measurement-type-by-strategy", directory))
        self.fixture(
            "itdk-role-by-strategy", campaign / "eu" / "figures" / "icmp_fixture" / "connection"
        )
        selected = paper_ready.discover(root, "fixture", sites=["eu"], protocols=["icmp"])
        self.assertEqual(selected, [jobs[0]])
        before = {p: stamp(p) for p in campaign.rglob("*") if p.is_file()}
        self.assertEqual(paper_ready.run(selected, dry_run=True)[0].status, "would-create")
        self.assertEqual(before, {p: stamp(p) for p in campaign.rglob("*") if p.is_file()})

    def test_spawn_batch_and_second_pass(self):
        jobs = [self.fixture(kind) for kind in paper_ready.KINDS]
        command = [
            sys.executable,
            "-B",
            "-m",
            "ipid_analysis.paper_ready",
            "--workers",
            "2",
            *(str(job.source) for job in jobs),
        ]
        first = subprocess.run(command, check=True, capture_output=True, text=True)
        self.assertIn("2 created, 0 failed", first.stdout)
        before = {job.destination: stamp(job.destination) for job in jobs}
        second = subprocess.run(command, check=True, capture_output=True, text=True)
        self.assertIn("0 created, 0 failed", second.stdout)
        self.assertEqual(before, {job.destination: stamp(job.destination) for job in jobs})

    def test_create_only_publication_race_has_one_winner(self):
        source = self.root / "source.bin"
        source.write_bytes(b"new bytes")
        destination = self.root / "new.pdf"

        def attempt(_):
            try:
                artifacts.publish_new_file(source, destination)
                return "created"
            except FileExistsError:
                return "exists"

        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(sorted(pool.map(attempt, range(2))), ["created", "exists"])
        self.assertEqual(destination.read_bytes(), source.read_bytes())
        artifacts.assert_complete(destination)

    def test_local_create_refuses_hardlinked_existing_destination(self):
        source, destination, old = (self.root / x for x in ("new", "destination", "old"))
        source.write_bytes(b"new")
        old.write_bytes(b"old")
        os.link(old, destination)
        with self.assertRaises(FileExistsError):
            artifacts._create_file(source, destination, remote=False)
        self.assertEqual(old.read_bytes(), b"old")
        self.assertEqual(destination.read_bytes(), b"old")

    def test_remote_create_requests_immutable_uploads(self):
        source = self.root / "new.bin"
        source.write_bytes(b"new")
        remote = self.root / "remote"
        remote.mkdir()
        destination = remote / "new.pdf"

        def put(src, dst, *, immutable=False):
            self.assertTrue(immutable)
            dst.parent.mkdir(parents=True, exist_ok=True)
            with dst.open("xb") as stream:
                stream.write(src.read_bytes())
            shutil.copystat(src, dst)

        with (
            patch.object(artifacts, "persistent_roots", return_value=(remote,)),
            patch.object(artifacts, "mounted_storage", return_value=True),
            patch.object(artifacts, "validate_storage"),
            patch.object(artifacts, "native_put", side_effect=put) as upload,
        ):
            artifacts.publish_new_file(source, destination)
        self.assertEqual(upload.call_count, 2)
        before = stamp(destination)
        with self.assertRaises(FileExistsError):
            artifacts.publish_new_file(source, destination)
        self.assertEqual(stamp(destination), before)

    def test_transport_immutable_flag_is_sent_without_changing_default(self):
        source, destination = self.root / "new.bin", self.root / "remote.bin"
        source.write_bytes(b"new")

        def copy(command, **kwargs):
            self.assertEqual("--immutable" in command, immutable)
            self.assertIn("--ignore-times", command)
            shutil.copy2(source, destination)

        with (
            patch.object(
                transport,
                "_mount",
                return_value=nullcontext((["rclone"], "remote:bucket/new", 123)),
            ),
            patch.object(transport.subprocess, "run", side_effect=copy),
            patch.object(transport.signal, "SIGHUP", 1, create=True),
            patch.object(transport.signal, "pidfd_send_signal", create=True),
        ):
            for immutable in (False, True):
                transport.native_put(source, destination, immutable=immutable)


if __name__ == "__main__":
    unittest.main()
