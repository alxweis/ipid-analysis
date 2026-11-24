"""Connection-only discovery and immutable reuse of the approved ITDK design."""

import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from ipid_analysis import paper_ready
from ipid_analysis.paper_plot_style import _font

NAME = "c-rt-b_itdk-role-by-strategy"


def stamp(path):
    return path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest()


def fixture(directory, *, canonical=False):
    directory.mkdir(parents=True, exist_ok=True)
    source = directory / (("n-rt-b_fi-m_itdk-role-by-strategy" if canonical else NAME) + ".json")
    source.write_text(
        json.dumps(
            {
                "protocol": "tcp",
                "connection_mode": "connection",
                "interval": "rt-based",
                "scale": "base",
                "target": "tcp.ipid.connection.rt-based.base",
                "plot_role_totals": {"Transit-Observed": 100, "No Transit Evidence": 100},
                "plot_percentages": {
                    "Transit-Observed": {
                        "CONSTANT": 2,
                        "SINGLE": 14,
                        "PER_DESTINATION": 9,
                        "PER_CONNECTION": 67,
                        "PER_BUCKET": 3,
                        "UNCLASSIFIED": 5,
                    },
                    "No Transit Evidence": {
                        "CONSTANT": 1,
                        "SINGLE": 2,
                        "PER_DESTINATION": 4,
                        "PER_CONNECTION": 92,
                        "PER_BUCKET": 1,
                    },
                },
                "sources": {"ipid": "/do-not-read/ipid.pq", "itdk": "/do-not-read/interfaces.pq"},
            }
        )
    )
    source.with_suffix(".pdf").write_bytes(b"original PDF; never read or modified")
    return paper_ready.job_for(source)


class ConnectionITDKTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        env = patch.dict(
            os.environ,
            {
                "IPID_ANALYSIS_STORAGE_MODE": "local",
                "IPID_ANALYSIS_SCRATCH_STORAGE": "disk",
                "IPID_ANALYSIS_TMP_DIR": str(self.root / "scratch"),
                "MPLCONFIGDIR": str(self.root / "mpl"),
            },
        )
        env.start()
        self.addCleanup(env.stop)
        _font.cache_clear()
        self.addCleanup(_font.cache_clear)
        self.job = fixture(self.root / "data")

    def test_same_renderer_exact_values_and_create_only_publication(self):
        from ipid_analysis.paper_plot_style import draw

        report = json.loads(self.job.source.read_text())
        expected = paper_ready.plot_data("itdk-role-by-strategy", report)
        self.assertEqual(paper_ready.plot_data(paper_ready.CONNECTION_ITDK_KIND, report), expected)
        self.assertEqual(paper_ready.job_for(self.job.source.with_suffix(".pdf")), self.job)
        self.assertEqual(self.job.destination.name, NAME + "__paper.pdf")
        before = {p: stamp(p) for p in self.job.source.parent.iterdir()}
        with (
            patch("ipid_analysis.paper_plot_style.draw", wraps=draw) as renderer,
            patch("duckdb.connect", side_effect=AssertionError("no queries")),
            patch("pyarrow.parquet.read_table", side_effect=AssertionError("no Parquet reads")),
        ):
            result = paper_ready.run([self.job], workers=1)[0]
        self.assertEqual(result.status, "created", result.error)
        self.assertEqual(renderer.call_args.args[:2], ("itdk-role-by-strategy", expected))
        self.assertEqual(before, {p: stamp(p) for p in before})
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        self.assertEqual(paper_ready.run([self.job])[0].status, "skipped-existing")
        self.assertEqual(before, {p: stamp(p) for p in before})

    def test_invalid_context_or_percentages_fail_without_output(self):
        original = json.loads(self.job.source.read_text())
        for field, value in (
            ("protocol", "icmp"),
            ("connection_mode", "no-connection"),
            ("interval", "fixed-interval"),
            ("scale", "mass"),
            ("protocol", None),
            ("plot_percentages", {}),
        ):
            with self.subTest(field=field, value=value):
                report = copy.deepcopy(original)
                if value is None:
                    report.pop(field)
                else:
                    report[field] = value
                self.job.source.write_text(json.dumps(report))
                before = stamp(self.job.source)
                result = paper_ready.run([self.job], workers=1)[0]
                self.assertEqual(result.status, "failed", result.error)
                self.assertFalse(self.job.destination.exists())
                self.assertEqual(stamp(self.job.source), before)

    def test_discovery_prunes_other_protocols_modes_phases_and_sites(self):
        root = self.root / "reports"
        expected = []
        for site in ("eu", "na"):
            for run in ("tcp-80_fixture", "icmp_fixture", "udp-dns-53_fixture"):
                for mode in ("connection", "no-connection"):
                    for phase in (
                        "rt-based-base",
                        "fixed-interval-mass",
                        "merged/rt-based-base_fixed-interval-mass",
                    ):
                        job = fixture(
                            root / "continental/campaign" / site / "figures" / run / mode / phase
                        )
                        if (
                            site == "eu"
                            and run.startswith("tcp")
                            and mode == "connection"
                            and phase == "rt-based-base"
                        ):
                            expected.append(job)
        canonical = fixture(
            root
            / "continental/campaign/eu/figures/tcp-80_fixture/no-connection/merged/rt-based-base_fixed-interval-mass",
            canonical=True,
        )
        report = json.loads(canonical.source.read_text())
        report.update(
            connection_mode="no-connection",
            target="tcp.ipid.no-connection.rt-based.base+tcp.ipid.no-connection.fixed-interval.mass",
        )
        canonical.source.write_text(json.dumps(report))
        expected.append(canonical)
        expected.append(paper_ready.final_job(canonical))
        before = {p: stamp(p) for p in root.rglob("*") if p.is_file()}
        jobs = paper_ready.discover(root, "campaign", sites=["eu"], protocols=["tcp"])
        self.assertEqual(set(jobs), set(expected))
        self.assertTrue(
            all(r.status == "would-create" for r in paper_ready.run(jobs, dry_run=True))
        )
        self.assertEqual(before, {p: stamp(p) for p in before})
        self.assertFalse(paper_ready.discover(root, "campaign", sites=["eu"], protocols=["icmp"]))

    def test_spawned_mixed_batch_and_completed_repeat(self):
        jobs = [self.job, fixture(self.root / "canonical", canonical=True)]
        command = [
            sys.executable,
            "-B",
            "-m",
            "ipid_analysis.paper_ready",
            "--workers",
            "2",
            *(str(j.source) for j in jobs),
        ]
        self.assertIn(
            "2 created, 0 failed",
            subprocess.run(command, check=True, capture_output=True, text=True).stdout,
        )
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        self.assertIn(
            "0 created, 0 failed",
            subprocess.run(command, check=True, capture_output=True, text=True).stdout,
        )
        self.assertEqual(before, {p: stamp(p) for p in before})


if __name__ == "__main__":
    unittest.main()
