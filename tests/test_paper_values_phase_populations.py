"""Keep prose shares aligned with plot cohorts, preserving existing success values."""

from copy import deepcopy
import hashlib
from types import SimpleNamespace
import unittest
import zipfile

from tests.test_paper_values import Fixture

from paper_values.exporter import export
from paper_values.phase_populations import load
from paper_values.review import STRATEGIES
from paper_values.tex import formatted


def artifact(phase, population, *, kind="strategies"):
    return SimpleNamespace(
        site="eu",
        protocol="icmp",
        connection="no-connection",
        phase=phase,
        kind=kind,
        name=phase + "/" + kind,
        data=dict(counts=population, total_ips=sum(population.values())),
    )


def plot(rt, fixed):
    result = artifact("rt-fi-merged", {}, kind="measurement-type-by-strategy")
    result.data = {"bars": {}}
    for mode, population in (("RT-based", rt), ("Fixed-Interval", fixed)):
        total = sum(population.values())
        result.data["bars"][mode] = dict(
            total=total,
            counts=population,
            percentages={s: 100 * population.get(s, 0) / total for s in STRATEGIES},
        )
    return result


class PhasePopulationTest(unittest.TestCase):
    def test_eu_icmp_exact_counts_yield_success_56_18_and_plot_54_64_rounded_to_55(self):
        # Counts from the published EU ICMP measurement-type report.
        rt = artifact(
            "rt-base",
            {
                "CONSTANT": 10716088,
                "SINGLE": 10334574,
                "PER_BUCKET": 218204019,
                "PER_DESTINATION": 48026348,
                "PER_CONNECTION": 16,
                "REFLECTION": 27003852,
                "UNCLASSIFIED": 4820880,
            },
        )
        population = {
            "RANDOM": 2633904,
            "MULTI": 636694,
            "UNCLASSIFIED": 1118746,
            "SINGLE": 109885,
            "PER_DESTINATION": 101672,
            "PER_BUCKET": 81560,
            "CONSTANT": 328,
            "REFLECTION": 5137,
            "NOT_ENOUGH_SAMPLES": 132954,
        }
        fixed = artifact(
            "fi-mass", {s: n for s, n in population.items() if s != "NOT_ENOUGH_SAMPLES"}
        )
        chart = plot(rt.data["counts"], population)
        before = deepcopy((rt.data, fixed.data, chart.data))
        values, errors = load([rt, fixed, chart])
        self.assertEqual(errors, [])
        prefix = "results.eu.icmp.fi-mass.all."
        self.assertEqual(values[prefix + "count"]["raw"], 4820880)
        self.assertEqual(values[prefix + "random.count"]["raw"], 2633904)
        share = values[prefix + "random.pct"]["raw"]
        self.assertEqual(share, chart.data["bars"]["Fixed-Interval"]["percentages"]["RANDOM"])
        self.assertEqual(formatted(share, "percent"), "54.64%")
        self.assertEqual(round(share), 55)
        self.assertEqual(formatted(100 * 2633904 / 4687926, "percent"), "56.18%")
        self.assertEqual(
            formatted(values[prefix + "single-per-destination-per-bucket.pct"]["raw"], "percent"),
            "6.08%",
        )
        self.assertAlmostEqual(
            sum(values[prefix + s.lower().replace("_", "-") + ".pct"]["raw"] for s in STRATEGIES),
            100,
        )
        self.assertEqual((rt.data, fixed.data, chart.data), before)

    def test_missing_result_rows_stay_in_the_denominator_like_the_existing_plot(self):
        rt = artifact("rt-base", {"CONSTANT": 80, "UNCLASSIFIED": 20})
        fixed = artifact(
            "fi-mass", {"RANDOM": 10, "MULTI": 2, "UNCLASSIFIED": 4, "NOT_ENOUGH_SAMPLES": 2}
        )
        plotted = {**fixed.data["counts"], "NOT_ENOUGH_SAMPLES": 4}
        chart = plot(rt.data["counts"], plotted)
        for reports in ([rt, fixed], [rt, fixed, chart], [chart]):
            with self.subTest(sources=[a.name for a in reports]):
                values, errors = load(reports)
                self.assertEqual(errors, [])
                prefix = "results.eu.icmp.fi-mass.all."
                self.assertEqual(values[prefix + "count"]["raw"], 20)
                self.assertEqual(values[prefix + "random.pct"]["raw"], 50)
                self.assertEqual(values[prefix + "unclassified.pct"]["raw"], 20)
                self.assertEqual(values[prefix + "not-enough-samples.count"]["raw"], 4)
                self.assertEqual(values[prefix + "not-enough-samples.pct"]["raw"], 20)

    def test_combined_count_does_not_double_count_refined_ips_or_include_rt_failures(self):
        rt = artifact("rt-base", {"CONSTANT": 75, "UNCLASSIFIED": 20, "NOT_ENOUGH_SAMPLES": 5})
        merged = artifact("rt-fi-merged", {"CONSTANT": 75, "RANDOM": 16, "NOT_ENOUGH_SAMPLES": 9})
        values, errors = load([rt, merged])
        self.assertEqual(errors, [])
        self.assertEqual(values["results.eu.icmp.rt-fi.success-all.count"]["raw"], 95)
        self.assertIsNone(load([merged])[0]["results.eu.icmp.rt-fi.success-all.count"]["raw"])
        self.assertIsNone(values["results.eu.icmp.fi-mass.all.count"]["raw"])

    def test_missing_and_zero_are_not_fabricated_percentages(self):
        self.assertIsNone(load([])[0]["results.eu.icmp.fi-mass.all.random.pct"]["raw"])
        reports = [artifact("rt-base", {"CONSTANT": 10}), artifact("fi-mass", {})]
        values, errors = load(reports)
        self.assertEqual(errors, [])
        prefix = "results.eu.icmp.fi-mass.all."
        self.assertEqual(values[prefix + "count"]["raw"], 0)
        self.assertEqual(values[prefix + "random.count"]["raw"], 0)
        self.assertIsNone(values[prefix + "random.pct"]["raw"])

    def test_conflicting_counts_percentages_and_selection_are_rejected(self):
        rt = artifact("rt-base", {"CONSTANT": 8, "UNCLASSIFIED": 2})
        fixed = artifact("fi-mass", {"RANDOM": 1})
        chart = plot(rt.data["counts"], {"RANDOM": 1, "NOT_ENOUGH_SAMPLES": 1})
        cases = []
        wrong_pct = deepcopy(chart)
        wrong_pct.data["bars"]["Fixed-Interval"]["percentages"]["RANDOM"] = 100
        cases.append([rt, fixed, wrong_pct])
        cases.append(
            [rt, fixed, plot(rt.data["counts"], {"CONSTANT": 1, "NOT_ENOUGH_SAMPLES": 1})]
        )
        cases.append([rt, artifact("fi-mass", {"RANDOM": 3}), chart])
        coverage = artifact("fi-mass", {}, kind="coverage")
        coverage.data = dict(zmap_ip_count=1)
        cases.append([rt, fixed, chart, coverage])
        for reports in cases:
            with self.subTest(sources=[a.name for a in reports]):
                values, errors = load(reports)
                self.assertEqual(len(errors), 1)
                self.assertIsNone(values["results.eu.icmp.fi-mass.all.count"]["raw"])

    def test_unbound_legacy_phases_leave_new_values_empty_without_blocking_old_exports(self):
        rt = artifact("rt-base", {"CONSTANT": 10})
        fixed = artifact("fi-mass", {"RANDOM": 3})
        values, errors = load([rt, fixed])
        self.assertEqual(errors, [])
        self.assertIsNone(values["results.eu.icmp.fi-mass.all.random.pct"]["raw"])

    def test_export_all_vps_protocols_preserves_success_values_sources_and_order(self):
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            f = Fixture(directory)
            sites = ("af", "as", "au", "eu", "na", "sa")
            for site in sites:
                for protocol in ("icmp", "tcp", "udp"):
                    f.baseline(site=site, protocol=protocol)
            before = {
                p: (p.stat().st_mtime_ns, hashlib.sha256(p.read_bytes()).hexdigest())
                for p in f.root.rglob("*.json")
            }
            values = f.run(strict=True)["values"]
            for site in sites:
                for family in ("icmp", "tcp.no-conn", "udp"):
                    prefix = f"results.{site}.{family}"
                    expected = {
                        ".rt-base.success.count": 10,
                        ".rt-base.success.pct": 50,
                        ".fi-mass.success.count": 1,
                        ".fi-mass.success.random.pct": 100,
                        ".fi-mass.all.count": 2,
                        ".fi-mass.all.random.count": 1,
                        ".fi-mass.all.random.pct": 50,
                        ".fi-mass.all.not-enough-samples.count": 1,
                        ".fi-mass.all.not-enough-samples.pct": 50,
                        ".rt-fi.success.count": 9,
                        ".rt-fi.success.pct": 45,
                        ".rt-fi.success-all.count": 10,
                        ".rt-fi.distribution.random.pct": 10,
                    }
                    for suffix, raw in expected.items():
                        self.assertEqual(values[prefix + suffix]["raw"], raw, prefix + suffix)
            self.assertEqual(
                before,
                {
                    p: (p.stat().st_mtime_ns, hashlib.sha256(p.read_bytes()).hexdigest())
                    for p in before
                },
            )
            tex = (f.root / "tmp/values.tex").read_text()
            self.assertIn(
                "\n".join(
                    f"\\setval{{results.{site}.icmp.fi-mass.all.random.pct}}{{50.00\\%}}"
                    for site in sites
                ),
                tex,
            )

    def test_bad_plot_preserves_previous_exports_in_strict_mode(self):
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            f = Fixture(directory)
            f.baseline(protocol="icmp")
            f.run(strict=True)
            before = f.output_hashes()
            chart = plot({"CONSTANT": 6, "SINGLE": 2, "UNCLASSIFIED": 2}, {"RANDOM": 2})
            f.report("measurement-type-by-strategy", chart.data, protocol="icmp", phase="rt-fi")
            with self.assertRaisesRegex(ValueError, "refinement plot disagrees"):
                f.run(strict=True)
            self.assertEqual(f.output_hashes(), before)

    def test_archive_and_mounted_reports_use_the_same_new_populations(self):
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            f = Fixture(directory)
            f.baseline(protocol="icmp")
            mounted = f.run(strict=True)["values"]
            archive = f.root / "reports.zip"
            with zipfile.ZipFile(archive, "w") as output:
                for path in f.root.rglob("*.json"):
                    if path.is_relative_to(f.reports) or path.is_relative_to(f.processed):
                        output.write(path, path.relative_to(f.root).as_posix())
            archived = export(
                "test-campaign",
                reports_root=None,
                archive=archive,
                repository_root=f.root / "archive-output",
                strict=True,
            )["values"]
            for key, entry in mounted.items():
                if ".fi-mass.all." in key or key.endswith(".rt-fi.success-all.count"):
                    self.assertEqual(archived[key]["raw"], entry["raw"], key)


if __name__ == "__main__":
    unittest.main()
