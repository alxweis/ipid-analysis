"""Final ITDK complements, immutable sibling PDFs and downstream figure selection."""

from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from test_paper_values import Fixture

from ipid_analysis import artifacts, paper_ready
from ipid_analysis.paper_itdk_final import load_data
from ipid_analysis.plot_itdk_strategy import NO_TRANSIT_ROLE as OTHER
from ipid_analysis.plot_itdk_strategy import TRANSIT_ROLE as ROUTER
from paper_values.exporter import choose_plot
from paper_values.figure_zip import bundle
from paper_values.inputs import Artifact, identify_plot


def reports():
    identity = dict(
        protocol="icmp",
        connection_mode="no-connection",
        zmap_id="icmp_fixture",
        target="icmp.ipid.no-connection.rt-based.base+icmp.ipid.no-connection.fixed-interval.mass",
        base_measurement_id="icmp_base",
        mass_measurement_id="icmp_mass",
    )
    strategy = dict(
        identity,
        total_ips=100,
        counts={
            "CONSTANT": 60,
            "PER_BUCKET": 30,
            "UNCLASSIFIED": 7,
            "NOT_ENOUGH_SAMPLES": 3,
        },
    )
    role = dict(
        identity,
        coverage=dict(total=100, matched=30, unmatched=70),
        plot_role_totals={ROUTER: 20, OTHER: 10},
        raw_distribution=[
            dict(role=ROUTER, strategy="PER_BUCKET", count=18),
            dict(role=ROUTER, strategy="NOT_ENOUGH_SAMPLES", count=2),
            dict(role=OTHER, strategy="CONSTANT", count=10),
        ],
    )
    os_role = dict(
        identity,
        os=dict(measurement_id="shared-os"),
        coverage=dict(
            total_measured=100,
            os_resolved=40,
            itdk_os_resolved=10,
            transit_observed_os_resolved=6,
            no_transit_evidence_os_resolved=4,
        ),
        raw_distribution=[
            dict(role=ROUTER, os_group="router", count=6),
            dict(role=OTHER, os_group="server", count=4),
        ],
    )
    os_total = dict(
        identity,
        measurements=dict(rt_based_base="icmp_base", fixed_interval_mass="icmp_mass"),
        os_measurement_id="shared-os",
        strategy_ip_count=100,
        # This intentionally larger shared OS population is NOT our denominator.
        grouped_ip_count=500,
        matched_grouped_ip_count=40,
        groups={"router": dict(ip_count=8), "server": dict(ip_count=32)},
    )
    return {
        "strategies": strategy,
        "itdk-role-by-strategy": role,
        "itdk-role-by-os": os_role,
        "operating-system-group-by-strategy": os_total,
    }


def stamp(path):
    return path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest()


class FinalITDKTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
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
        from ipid_analysis.paper_plot_style import _font

        _font.cache_clear()
        self.addCleanup(_font.cache_clear)

    def write_reports(self, directory=None):
        directory = directory or self.root
        directory.mkdir(parents=True, exist_ok=True)
        jobs = []
        for kind, data in reports().items():
            source = directory / f"n-rt-b_fi-m_{kind}.json"
            source.write_text(json.dumps(data))
            if kind.startswith("itdk-role-"):
                source.with_suffix(".pdf").write_bytes(b"preserved original")
                source.with_name(source.stem + "__paper.pdf").write_bytes(b"preserved paper")
                jobs.append(paper_ready.final_job(paper_ready.job_for(source)))
        return jobs

    def test_all_other_includes_nontransit_and_unmatched_and_keeps_router(self):
        r = reports()
        result = load_data(
            "itdk-role-by-strategy-final", r["itdk-role-by-strategy"], r["strategies"]
        )
        self.assertEqual(result.populations["router"], 20)
        self.assertEqual(result.populations["all_other"], 80)
        self.assertEqual(result.populations["itdk_unmatched_included"], 70)
        self.assertEqual(result.plot[ROUTER]["PER_BUCKET"], 90)
        self.assertEqual(result.plot[ROUTER]["UNCLASSIFIED"], 10)
        self.assertEqual(result.plot[OTHER]["CONSTANT"], 75)
        self.assertEqual(result.plot[OTHER]["PER_BUCKET"], 15)
        self.assertEqual(result.plot[OTHER]["UNCLASSIFIED"], 10)
        self.assertEqual(sum(result.plot[OTHER].values()), 100)

    def test_os_complement_uses_only_measured_resolved_ips(self):
        r = reports()
        result = load_data(
            "itdk-role-by-os-final", r["itdk-role-by-os"], r["operating-system-group-by-strategy"]
        )
        self.assertEqual(result.populations["measured"], 40)
        self.assertEqual(result.populations["router"], 6)
        self.assertEqual(result.populations["all_other"], 34)
        self.assertEqual(result.populations["itdk_unmatched_included"], 30)
        self.assertEqual(result.plot.counts[OTHER]["server"], 32)
        self.assertEqual(result.plot.counts[OTHER]["router"], 2)
        self.assertEqual(result.plot.percentages[ROUTER]["router"], 100)

    def test_unrelated_or_inconsistent_populations_fail_instead_of_using_old_percentages(self):
        cases = [
            ("strategies", lambda a, b: b.update(zmap_id="other")),
            ("strategies", lambda a, b: b.update(base_measurement_id="other")),
            ("strategies", lambda a, b: b.update(mass_measurement_id="other")),
            ("strategies", lambda a, b: a.update(connection_mode="connection")),
            ("strategies", lambda a, b: a.update(target="wrong")),
            ("strategies", lambda a, b: b["counts"].update(CONSTANT=True)),
            ("strategies", lambda a, b: a["raw_distribution"].append(a["raw_distribution"][0])),
            ("strategies", lambda a, b: a["raw_distribution"][0].update(count=-1)),
            ("strategies", lambda a, b: a["coverage"].update(unmatched=69)),
            ("strategies", lambda a, b: a["raw_distribution"][0].update(count=99)),
            ("strategies", lambda a, b: a.pop("raw_distribution")),
            (
                "operating-system-group-by-strategy",
                lambda a, b: b.update(os_measurement_id="other"),
            ),
            (
                "operating-system-group-by-strategy",
                lambda a, b: b["measurements"].update(rt_based_base="other"),
            ),
            (
                "operating-system-group-by-strategy",
                lambda a, b: b.update(matched_grouped_ip_count=500),
            ),
            (
                "operating-system-group-by-strategy",
                lambda a, b: a["coverage"].update(total_measured=101),
            ),
            (
                "operating-system-group-by-strategy",
                lambda a, b: a["coverage"].update(itdk_os_resolved=9),
            ),
        ]
        for companion, change in cases:
            with self.subTest(companion=companion, change=change):
                r = deepcopy(reports())
                base = "itdk-role-by-strategy" if companion == "strategies" else "itdk-role-by-os"
                change(r[base], r[companion])
                with self.assertRaises((ValueError, KeyError)):
                    load_data(base + "-final", r[base], r[companion])

    def test_create_only_from_json_without_measurement_reads_and_with_population_receipts(self):
        jobs = self.write_reports()
        inputs = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        with (
            patch("pyarrow.parquet.read_table", side_effect=AssertionError("no raw reads")),
            patch("duckdb.connect", side_effect=AssertionError("no queries")),
        ):
            results = paper_ready.run(jobs, workers=1)
        self.assertEqual([r.status for r in results], ["created", "created"], results)
        for p, before in inputs.items():
            self.assertEqual(stamp(p), before)
        for job in jobs:
            self.assertEqual(paper_ready.job_for(job.destination), job)
            artifacts.assert_complete(job.destination)
            receipt = json.loads(artifacts.completion_path(job.destination).read_text())
            self.assertEqual(len(receipt["sources"]), 2)
            self.assertIn("population", receipt["parameters"])
            self.assertNotIn(b"/Subtype /Type3", job.destination.read_bytes())
        all_files = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        with patch("ipid_analysis.paper_itdk_final.draw", side_effect=AssertionError("skip")):
            self.assertTrue(all(r.status == "skipped-existing" for r in paper_ready.run(jobs)))
        self.assertEqual(all_files, {p: stamp(p) for p in all_files})

    def test_missing_companion_fails_without_creating_or_modifying_any_pdf(self):
        job = self.write_reports()[0]
        (self.root / "n-rt-b_fi-m_strategies.json").unlink()
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        self.assertEqual(paper_ready.run([job], workers=1)[0].status, "failed")
        self.assertFalse(job.destination.exists())
        self.assertEqual(before, {p: stamp(p) for p in before})

    def test_discovery_all_vps_protocols_and_final_only_dry_run(self):
        root = self.root / "reports"
        for site in ("af", "as", "au", "eu", "na", "sa"):
            for proto in ("icmp", "tcp-80", "udp-dns-53"):
                self.write_reports(
                    root
                    / "continental/campaign"
                    / site
                    / "figures"
                    / (proto + "_fixture")
                    / "no-connection/merged/rt-based-base_fixed-interval-mass"
                )
        jobs = paper_ready.discover(root, "campaign")
        final = [j for j in jobs if j.kind.endswith("-final")]
        self.assertEqual(len(final), 36)
        before = {p: stamp(p) for p in root.rglob("*") if p.is_file()}
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                "-m",
                "ipid_analysis.paper_ready",
                "--campaign",
                "campaign",
                "--reports-root",
                str(root),
                "--final-only",
                "--dry-run",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.stdout.count("would-create:"), 36)
        self.assertEqual(before, {p: stamp(p) for p in root.rglob("*") if p.is_file()})

    def test_final_cli_spawn_renders_exact_names(self):
        jobs = self.write_reports()
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                "-m",
                "ipid_analysis.paper_ready",
                "--workers",
                "2",
                *(str(j.destination) for j in jobs),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertIn("2 created, 0 failed", result.stdout)
        self.assertEqual(
            {j.destination.name for j in jobs},
            {
                "n-rt-b_fi-m_itdk-role-by-strategy__paper__final.pdf",
                "n-rt-b_fi-m_itdk-role-by-os__paper_final.pdf",
            },
        )

    def test_centered_legend_preserves_columns_squares_and_gap(self):
        import matplotlib.pyplot as plt
        from matplotlib.textpath import TextPath

        from ipid_analysis import paper_plot_style as style

        font, _ = style._font()
        fig = plt.figure(figsize=(style.WIDTH, style.HEIGHT))
        self.addCleanup(plt.close, fig)
        labels = style._legend(fig, font, itdk=True)
        w, h = fig.get_size_inches() * 72
        self.assertEqual(len(labels), 9)
        for text, box in zip(labels, fig.artists):
            self.assertAlmostEqual(box.get_width() * w, 4)
            self.assertAlmostEqual(box.get_height() * h, 4)
            self.assertAlmostEqual((text.get_position()[0] - box.get_x()) * w, 5.2)
            self.assertAlmostEqual(text.get_position()[1], box.get_y() + box.get_height() / 2)
            self.assertEqual(text.get_fontsize(), 7)
        for i in (0, 2, 4, 6):
            self.assertEqual(labels[i].get_position()[0], labels[i + 1].get_position()[0])
        left = fig.artists[0].get_x() * w
        right = (
            labels[-1].get_position()[0] * w
            + TextPath((0, 0), labels[-1].get_text(), prop=font).get_extents().x1
        )
        self.assertAlmostEqual(left, w - right)


class FinalFigureExportTest(unittest.TestCase):
    def test_final_preferred_in_values_and_zip_without_changing_scalar_keys(self):
        with tempfile.TemporaryDirectory() as temporary:
            f = Fixture(temporary)
            source = f.report(
                "itdk-role-by-strategy",
                dict(
                    coverage=dict(total=100),
                    plot_role_totals={ROUTER: 20},
                    plot_percentages={ROUTER: {"CONSTANT": 25}},
                ),
                phase="rt-fi",
            )
            original = source.with_suffix(".pdf")
            paper = source.with_name(source.stem + "__paper.pdf")
            final = source.with_name(source.stem + "__paper__final.pdf")
            for p in (original, paper):
                p.write_bytes(b"%PDF-1.4\n" + p.name.encode())
            old = f.run(strict=True)
            final.write_bytes(b"%PDF-1.4\nfinal")
            f.receipt(final)
            new = f.run(strict=True)
            key = "fig.eu.tcp.no-conn.rt-fi.itdk-role-by-strategy"
            self.assertTrue(new["values"][key]["raw"].endswith("__paper__final.pdf"))
            self.assertEqual(
                {k: v for k, v in old["values"].items() if k != key},
                {k: v for k, v in new["values"].items() if k != key},
            )
            bundle(repository_root=f.root)
            with zipfile.ZipFile(f.root / "tmp/paper-figures.zip") as archive:
                self.assertEqual(archive.namelist(), [new["values"][key]["raw"]])
                self.assertEqual(archive.read(archive.namelist()[0]), final.read_bytes())
            before = f.output_hashes()
            archive_path = f.root / "input.zip"
            with zipfile.ZipFile(archive_path, "w") as archive:
                for p in f.reports.rglob("*"):
                    if p.is_file():
                        archive.write(p, p.relative_to(f.reports).as_posix())
            self.assertEqual(new["values"], f.run(archive=archive_path, strict=True)["values"])
            final.write_bytes(b"broken")
            with self.assertRaises(ValueError):
                f.run(strict=True)
            self.assertEqual(before, f.output_hashes())
            final.unlink()
            self.assertEqual(old["values"], f.run(strict=True)["values"])

    def test_both_final_stems_share_identity_and_unrelated_finals_do_not(self):
        for kind, suffix in paper_ready.FINAL_KINDS.values():
            stem = "n-rt-b_fi-m_" + kind

            def identify(stem):
                return identify_plot(
                    stem,
                    "eu",
                    "figures/icmp_fixture/no-connection/merged/rt-based-base_fixed-interval-mass/"
                    + stem
                    + ".pdf",
                    "campaign",
                )

            original, paper, final = [identify(stem + s) for s in ("", "__paper", suffix)]
            self.assertEqual(original.original_path, final.original_path)
            self.assertEqual(original.scope, final.scope)
            self.assertEqual([p.variant_rank for p in (original, paper, final)], [0, 1, 2])
            spec = dict(
                protocol="icmp", connection="no-connection", phase="rt-fi-merged", kind=kind
            )
            self.assertEqual(
                choose_plot(spec, "eu", [final, original, paper], [])[0], final.paper_path
            )
            wrong_run = deepcopy(final)
            wrong_run.run = "icmp_unrelated"
            wrong_run.paper_path = wrong_run.paper_path.replace("icmp_fixture", wrong_run.run)
            canonical = Artifact(
                "merged",
                {"zmap_id": "icmp_fixture"},
                "",
                0,
                "",
                "eu",
                protocol="icmp",
                phase="rt-fi-merged",
            )
            self.assertEqual(
                choose_plot(spec, "eu", [wrong_run, paper], [canonical])[0], paper.paper_path
            )
            unknown = identify(stem + "__arbitrary_final")
            self.assertNotEqual(unknown.scope, final.scope)


if __name__ == "__main__":
    unittest.main()
