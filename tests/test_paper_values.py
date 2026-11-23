"""Scientific populations, canonical public keys, source integrity and value-cache tests."""

import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
from threading import Barrier
import unittest
from unittest.mock import patch
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq

from paper_values.cache import ValueCache
from paper_values.diagnostics import Diagnostics
from paper_values.exporter import export, load_metrics, required_contexts
from paper_values.inputs import Inputs
from paper_values.readers import Readers, increment_histogram
from paper_values.tex import key_order, render

CAMPAIGN = "test-campaign"
RUNS = {
    "tcp": "tcp-80_2026-11-07_03-30-08",
    "icmp": "icmp_2026-11-07_03-30-07",
    "udp": "udp-dns-53_2026-11-07_03-30-08",
}
PATHS = {
    "rt-base": "rt-based-base/n-rt-b_",
    "fi-mass": "fixed-interval-mass/n-fi-m_",
    "fi-base": "fixed-interval-base/n-fi-b_",
    "rt-fi": "merged/rt-based-base_fixed-interval-mass/n-rt-b_fi-m_",
    "comparison": "comparison/rt-based-base_fixed-interval-base/n-rt-b_fi-b_",
}


class Fixture:
    def __init__(self, root):
        self.root = Path(root)
        self.reports = self.root / "reports/continental" / CAMPAIGN
        self.processed = self.root / "data/processed/continental" / CAMPAIGN
        self.raw = self.root / "data/raw/continental" / CAMPAIGN

    def report(
        self,
        kind,
        data,
        *,
        protocol="tcp",
        phase="rt-base",
        mode="no-connection",
        site="eu",
        run=None,
        processed=False,
        receipt=False,
    ):
        prefix = self.processed if processed else self.reports
        path = prefix / site
        if not processed:
            path /= "figures"
        stem = PATHS[phase]
        if mode == "connection":
            stem = stem.replace("n-", "c-")
        path = path / (run or RUNS[protocol]) / mode / (stem + kind + ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        content = json.dumps(data).encode()
        path.write_bytes(content)
        if receipt:
            self.receipt(path)
        return path

    def receipt(self, path):
        marker = path.parent / ".ipid-complete" / (path.name + ".json")
        marker.parent.mkdir(exist_ok=True)
        marker.write_text(
            json.dumps(
                dict(
                    status="complete",
                    size_bytes=path.stat().st_size,
                    sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                )
            ),
            encoding="utf-8",
        )
        return marker

    def strategy(self, c, **kwargs):
        protocol = kwargs.get("protocol", "tcp")
        phase = kwargs.get("phase", "rt-base")
        data = dict(
            protocol=protocol,
            counts=c,
            total_ips=sum(c.values()),
            zmap_id=RUNS[protocol],
            measurement_id=protocol + "_" + phase.replace("-", "_"),
        )
        return self.report("strategies", data, **kwargs)

    def baseline(self, **kwargs):
        self.strategy({"CONSTANT": 6, "SINGLE": 2, "UNCLASSIFIED": 2}, **kwargs)
        self.strategy({"RANDOM": 1}, phase="fi-mass", **kwargs)
        self.strategy(
            {"CONSTANT": 6, "SINGLE": 2, "RANDOM": 1, "NOT_ENOUGH_SAMPLES": 1},
            phase="rt-fi",
            **kwargs,
        )
        self.report(
            "coverage",
            dict(zmap_ip_count=20, ipid_ip_count=10),
            processed=True,
            **kwargs,
        )

    def snapshot(self, *, protocol="tcp", phase="rt-base", site="eu", **overrides):
        self.strategy({"CONSTANT": 1}, protocol=protocol, phase=phase, site=site)
        path = (
            self.raw
            / site
            / "ipid"
            / (protocol + "_" + phase.replace("-", "_"))
            / "ipid.snapshot.yaml"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        data = dict(
            connection_count=4,
            requests_per_connection=25 if phase == "fi-mass" else 4,
            maximum_tolerated_rtt="2s",
            fixed_interval=dict(request_interval="20ms", minimum_reply_rate=0.8),
        )
        data.update(overrides)
        path.write_text(json.dumps(data), encoding="utf-8")  # JSON is also valid YAML.
        return path

    def run(self, **kwargs):
        return export(
            CAMPAIGN,
            reports_root=self.reports,
            processed_root=self.processed,
            raw_root=self.raw,
            repository_root=self.root,
            **kwargs,
        )

    def output_hashes(self):
        return {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (self.root / "tmp").iterdir()
            if p.name in ("values.tex", "values.json")
        }


class PaperValuesTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.f = Fixture(self.temp.name)

    def inputs(self, workers=8):
        contexts, plots = required_contexts(load_metrics()["metrics"])
        return Inputs(
            (self.f.reports, self.f.processed),
            workers=workers,
            contexts=contexts,
            plot_contexts=plots,
        )

    def test_inventory_prunes_unused_branches_before_listing_them(self):
        wanted = self.f.strategy({"CONSTANT": 1})
        unused = [
            self.f.reports / "discovery-parts/large-subtree",
            wanted.parent.parent / "unused-analysis/large-subtree",
            wanted.parent.parent / ".ipynb_checkpoints/large-subtree",
            wanted.parent.parent / "comparison/unused-comparison/large-subtree",
            self.f.reports / "eu/figures" / RUNS["icmp"] / "connection/large-subtree",
        ]
        for path in unused:
            path.mkdir(parents=True)
            (path / "unrelated.json").write_text("{}", encoding="utf-8")
        walked = []
        walk = os.walk

        def record(*args, **kwargs):
            for entry in walk(*args, **kwargs):
                walked.append(Path(entry[0]))
                yield entry

        inputs = self.inputs()
        with patch("paper_values.inputs.os.walk", side_effect=record):
            inputs.scan()
        self.assertIn(wanted, inputs.inventory[self.f.reports])
        for path in unused:
            self.assertNotIn(path, walked)
            self.assertFalse(any(path in visited.parents for visited in walked))

    def test_inventory_scans_vps_concurrently(self):
        self.f.strategy({"CONSTANT": 1}, site="eu")
        self.f.strategy({"CONSTANT": 1}, site="au")
        inputs = self.inputs(workers=2)
        barrier = Barrier(2)
        scan_site = inputs.scan_site

        def synchronized(job):
            barrier.wait(timeout=10)
            return scan_site(job)

        with patch.object(inputs, "scan_site", side_effect=synchronized):
            inputs.scan()
        self.assertEqual(len(inputs.inventory[self.f.reports]), 2)

    def test_inventory_preserves_unknown_layouts_and_refreshes_each_run(self):
        inputs_root = self.f.reports / "eu/legacy/nested"
        inputs_root.mkdir(parents=True)
        legacy = inputs_root / "legacy.json"
        legacy.write_text("{}", encoding="utf-8")
        inputs = self.inputs()
        inputs.scan()
        self.assertIn(legacy, inputs.inventory[self.f.reports])
        added = self.f.strategy({"CONSTANT": 1}, site="au", processed=True)
        # A newly available input root is detected by the next export's Inputs.
        refreshed = self.inputs()
        refreshed.scan()
        self.assertIn(added, refreshed.inventory[self.f.processed])
        another = self.f.strategy({"CONSTANT": 2}, site="na")
        inputs.scan()
        self.assertIn(another, inputs.inventory[self.f.reports])

    def test_inventory_errors_preserve_existing_outputs(self):
        self.f.baseline()
        self.f.run(strict=True)
        before = self.f.output_hashes()
        walk = os.walk

        def failing(top, *, onerror):
            if Path(top) == self.f.reports / "eu":
                onerror(PermissionError("inventory permission denied"))
                return
            yield from walk(top, onerror=onerror)

        with patch("paper_values.inputs.os.walk", side_effect=failing):
            with self.assertRaisesRegex(ValueError, "inventory permission denied"):
                self.f.run(strict=True)
        self.assertEqual(before, self.f.output_hashes())

    def test_inventory_root_listing_error_is_reported(self):
        self.f.strategy({"CONSTANT": 1})
        inputs = self.inputs()
        with patch(
            "paper_values.inputs.os.scandir", side_effect=PermissionError("root listing denied")
        ):
            inputs.scan()
        self.assertEqual(inputs.inventory[self.f.reports], [])
        self.assertEqual(len(inputs.errors), 1)
        self.assertIn("root listing denied", inputs.errors[0]["error"])

    def test_pruned_parallel_export_matches_full_serial_inventory(self):
        for site in ("eu", "au"):
            for protocol in RUNS:
                self.f.baseline(site=site, protocol=protocol)
        # Evaluation reports live one level deeper than other mass reports.
        report = self.f.report("random-reproducibility-evaluation", {})
        report.unlink()
        nested = (
            report.parent.parent
            / "fixed-interval-mass/random-reproducibility"
            / "tcp-no-connection-mass-random-reproducibility-evaluation.json"
        )
        nested.parent.mkdir(parents=True)
        nested.write_text("{}", encoding="utf-8")
        plot = (
            self.f.reports
            / "eu/figures"
            / RUNS["tcp"]
            / "no-connection/comparison/rt-based-base_fixed-interval-base"
            / "n-rt-b_fi-b_increment-distributions.pdf"
        )
        plot.parent.mkdir(parents=True)
        plot.write_bytes(b"%PDF-1.4\nfixture")

        def full_scan(inputs):
            inputs.inventory = {
                root: sorted(
                    Path(directory) / name
                    for directory, _, names in os.walk(root)
                    for name in names
                    if name.endswith((".json", ".pdf"))
                )
                for root in inputs.paths
            }

        with patch.object(Inputs, "scan", full_scan):
            expected = self.f.run(strict=True, io_workers=1)
        expected_hashes = self.f.output_hashes()
        for workers in (1, 8):
            actual = self.f.run(strict=True, io_workers=workers)
            self.assertEqual(expected["values"], actual["values"])
            self.assertEqual(expected_hashes, self.f.output_hashes())
        inputs = self.inputs()
        inputs.scan()
        self.assertIn(nested, inputs.inventory[self.f.reports])
        self.assertIn(plot, inputs.inventory[self.f.reports])

    def test_automated_paper_keys_and_six_vps_remain(self):
        result = self.f.run(strict=True)
        self.assertEqual(len(result["values"]), 1770)
        self.assertEqual(result["available"], 0)
        self.assertFalse(result["input_errors"])
        self.assertTrue(
            all(
                (".vp-" not in k or k.startswith("results.vp-comparison."))
                and not k.startswith("reports.") for k in result["values"]
            )
        )
        for site in ("af", "as", "au", "eu", "na", "sa"):
            self.assertIn(
                f"results.{site}.tcp.no-conn.rt-fi.rt-pop.constant.pct",
                result["values"],
            )
        self.assertNotIn(
            "params.eu.source-addresses.count", result["values"]
        )  # Known future extension, not added silently.
        manual_keys = {"artifacts.required-disk-gib", "artifacts.required-memory-gib"}
        for site in ("af", "as", "au", "eu", "na", "sa"):
            manual_keys.update(
                {
                    f"campaign.{site}.start-month",
                    f"campaign.{site}.end-month",
                    f"params.{site}.source-addresses.prefix-length",
                }
            )
        tex = (self.f.root / "tmp/values.tex").read_text(encoding="utf-8")
        for key in manual_keys:
            self.assertNotIn(key, result["values"])
            self.assertNotIn(r"\setval{" + key + "}", tex)
        definitions = load_metrics()["metrics"]
        self.assertEqual(len(definitions), 365)
        self.assertEqual(sum(m["unit"] == "pdf-path" for m in definitions), 39)
        self.assertEqual(
            {m["key"].split(".")[0] for m in definitions},
            {"results", "validation", "classifier", "params", "fig"},
        )
        for metric in definitions:
            self.assertNotIn(".icmp.no-conn.", metric["key"])
            self.assertNotIn(".udp.no-conn.", metric["key"])
            self.assertNotIn(".transit-observed.", metric["key"])
            self.assertNotIn(".no-transit-evidence.", metric["key"])
            if metric["unit"] == "pdf-path":
                self.assertTrue(metric["key"].startswith("fig."))
        excluded = {
            "refs.itdk.release", "refs.maxmind.build-month",
            "classifier.random.threshold", "fig.validation.nist-cdf",
            "fig.validation.chi-square-cdf", "validation.data.fi.samples-per-class",
            "results.{vp}.icmp.fi-mass.recovered.rt-outliers-1.pct",
            "results.{vp}.tcp.conn.rt-base.zero.pct",
            "results.{vp}.tcp.conn.rt-base.probing-intervals.below-500ms.pct",
        }
        self.assertTrue(excluded.isdisjoint(m["key"] for m in definitions))
        for site in ("af", "as", "au", "eu", "na", "sa"):
            self.assertIn(
                f"results.{site}.tcp.conn.rt-base.itdk-strategy.all-others.single.pct",
                result["values"],
            )

    def test_tcp_connection_no_transit_single_uses_its_report_population(self):
        for site, value in (("eu", 1.9701569113685922), ("au", 0)):
            self.f.report(
                "itdk-role-by-strategy",
                {"plot_percentages": {"No Transit Evidence": {"SINGLE": value}}},
                mode="connection", site=site,
            )
        values = self.f.run(strict=True)["values"]
        for site, value in (("eu", 1.9701569113685922), ("au", 0), ("af", None)):
            key = f"results.{site}.tcp.conn.rt-base.itdk-strategy.all-others.single.pct"
            self.assertEqual(values[key]["raw"], value)
        tex = (self.f.root / "tmp/values.tex").read_text(encoding="utf-8")
        self.assertIn(
            r"\setval{results.eu.tcp.conn.rt-base.itdk-strategy.all-others.single.pct}{1.97\%}",
            tex,
        )

    def test_rt_and_successful_denominators_remain_distinct(self):
        self.f.baseline()
        v = self.f.run(strict=True)["values"]
        self.assertEqual(v["results.eu.tcp.no-conn.rt-base.success.count"]["raw"], 10)
        self.assertEqual(v["results.eu.tcp.no-conn.rt-fi.rt-pop.constant.pct"]["raw"], 60)
        self.assertEqual(v["results.eu.tcp.no-conn.rt-base.success.count"]["raw"], 10)
        self.assertIsNone(v["results.eu.tcp.no-conn.fi-to-rt-success.pct"]["raw"])

    def test_zero_is_observed_but_missing_vp_is_not_zero(self):
        self.f.strategy({"SINGLE": 10})
        v = self.f.run()["values"]
        self.assertEqual(v["results.eu.tcp.no-conn.rt-base.success.constant.pct"]["raw"], 0)
        self.assertIsNone(v["results.na.tcp.no-conn.rt-base.success.constant.pct"]["raw"])

    def test_zero_denominator_keeps_percentage_empty(self):
        self.f.strategy({})
        v = self.f.run()["values"]
        self.assertEqual(v["results.eu.tcp.no-conn.rt-base.success.count"]["raw"], 0)
        self.assertIsNone(v["results.eu.tcp.no-conn.rt-base.success.constant.pct"]["raw"])

    def test_vp_results_are_independent_even_if_counts_are_equal(self):
        for site in ("eu", "na"):
            self.f.strategy({"CONSTANT": 3, "SINGLE": 7}, site=site)
        v = self.f.run()["values"]
        self.assertEqual(v["results.eu.tcp.no-conn.rt-base.success.constant.pct"]["raw"], 30)
        self.assertEqual(v["results.na.tcp.no-conn.rt-base.success.constant.pct"]["raw"], 30)
        self.f.strategy({"CONSTANT": 8, "SINGLE": 2}, site="na")
        w = self.f.run()["values"]
        self.assertEqual(w["results.eu.tcp.no-conn.rt-base.success.constant.pct"]["raw"], 30)
        self.assertEqual(w["results.na.tcp.no-conn.rt-base.success.constant.pct"]["raw"], 80)

    def test_merged_population_inconsistency_preserves_both_outputs(self):
        self.f.baseline()
        self.f.run()
        before = self.f.output_hashes()
        self.f.strategy({"CONSTANT": 11}, phase="rt-fi")
        with self.assertRaisesRegex(ValueError, "merged population"):
            self.f.run()
        self.assertEqual(before, self.f.output_hashes())

    def test_coverage_inconsistency_preserves_existing_outputs(self):
        self.f.baseline()
        self.f.run()
        before = self.f.output_hashes()
        self.f.report("coverage", dict(zmap_ip_count=20, ipid_ip_count=11), processed=True)
        with self.assertRaisesRegex(ValueError, "coverage"):
            self.f.run()
        self.assertEqual(before, self.f.output_hashes())

    def test_invalid_counts_do_not_leak_partial_values(self):
        self.f.report("strategies", dict(protocol="tcp", total_ips=10, counts={"CONSTANT": 9}))
        result = self.f.run()
        self.assertTrue(result["input_errors"])
        self.assertIsNone(result["values"]["results.eu.tcp.no-conn.rt-base.success.count"]["raw"])
        before = self.f.output_hashes()
        with self.assertRaisesRegex(ValueError, "retained"):
            self.f.run(strict=True)
        self.assertEqual(before, self.f.output_hashes())

    def test_unused_report_family_does_not_block_the_paper_export(self):
        self.f.baseline()
        self.f.report("ripe-atlas-role-by-strategy", {"invalid": "not needed"})
        result = self.f.run(strict=True)
        self.assertFalse(result["input_errors"])
        self.assertEqual(
            result["values"]["results.eu.tcp.no-conn.rt-base.success.count"]["raw"], 10
        )

    def test_canonical_run_wins_over_a_repetition(self):
        self.f.baseline()
        self.f.strategy({"RANDOM": 999}, phase="fi-mass", run="tcp-80_repeat")
        self.assertEqual(
            self.f.run(strict=True)["values"]["results.eu.tcp.no-conn.fi-mass.success.random.pct"][
                "raw"
            ],
            100,
        )

    def streaming_mass(self, *, baseline=None, include_baseline=True, evaluation=False):
        ids = [
            "icmp_2026-11-17_01-48-11",
            "icmp_2026-11-17_05-26-23",
            "icmp_2026-11-17_05-26-57",
            "icmp_2026-11-17_05-27-29",
            "icmp_2026-11-17_05-28-02",
            "icmp_2026-11-17_05-28-36",
        ]
        baseline = baseline or ids[0]
        for mid in ids:
            if mid == baseline and not include_baseline:
                continue
            c = {"RANDOM": 8, "CONSTANT": 2} if mid == baseline else {"CONSTANT": 2}
            self.f.report(
                "strategies",
                dict(
                    protocol="icmp",
                    measurement_id=mid,
                    zmap_id=mid,
                    total_ips=sum(c.values()),
                    counts=c,
                ),
                protocol="icmp",
                phase="fi-mass",
                site="au",
                run=mid,
            )
            # The actual producer deliberately omits measurement_id in coverage JSON.
            self.f.report(
                "coverage",
                dict(zmap_ip_count=sum(c.values()), ipid_ip_count=sum(c.values())),
                protocol="icmp",
                phase="fi-mass",
                site="au",
                run=mid,
                processed=True,
            )
        if evaluation:
            self.f.report(
                "icmp-no_connection-mass-random-reproducibility-evaluation",
                dict(
                    baseline_measurement_id=baseline,
                    repeats=[
                        dict(measurement_id=mid, repeat_index=i + 1)
                        for i, mid in enumerate(mid for mid in ids if mid != baseline)
                    ],
                ),
                protocol="icmp",
                phase="fi-mass",
                site="au",
            )
        return ids

    def test_partial_au_streaming_mass_does_not_block_other_sites_or_guess_baseline(self):
        self.f.baseline(protocol="icmp")
        self.streaming_mass()
        result = self.f.run(strict=True)
        self.assertFalse(result["input_errors"])
        self.assertEqual(
            result["values"]["results.eu.icmp.fi-mass.success.random.pct"]["raw"], 100
        )
        self.assertIsNone(
            result["values"]["results.au.icmp.fi-mass.success.random.pct"]["raw"]
        )

    def test_even_one_unbound_streaming_mass_is_not_assumed_to_be_baseline(self):
        mid = "icmp_2026-11-17_05-26-23"
        self.f.report(
            "strategies",
            dict(
                protocol="icmp",
                measurement_id=mid,
                zmap_id=mid,
                counts={"CONSTANT": 10},
                total_ips=10,
            ),
            protocol="icmp",
            phase="fi-mass",
            site="au",
            run=mid,
        )
        result = self.f.run(strict=True)
        self.assertIsNone(
            result["values"]["results.au.icmp.fi-mass.success.random.pct"]["raw"]
        )

    def test_evaluation_identifies_baseline_and_filters_coverage_without_measurement_id(self):
        # The authoritative baseline is deliberately not the earliest timestamp.
        self.streaming_mass(baseline="icmp_2026-11-17_05-26-23", evaluation=True)
        result = self.f.run(strict=True)
        self.assertFalse(result["input_errors"])
        self.assertEqual(
            result["values"]["results.au.icmp.fi-mass.success.random.pct"]["raw"], 80
        )

    def test_evaluation_without_baseline_files_does_not_promote_a_repeat(self):
        self.streaming_mass(evaluation=True, include_baseline=False)
        result = self.f.run(strict=True)
        self.assertIsNone(
            result["values"]["results.au.icmp.fi-mass.success.random.pct"]["raw"]
        )

    def test_canonical_primary_report_selects_mass_before_merged_report_exists(self):
        self.streaming_mass()
        self.f.strategy({"RANDOM": 1}, protocol="icmp", phase="fi-mass", site="au")
        self.f.report(
            "coverage",
            dict(zmap_ip_count=1, ipid_ip_count=1),
            protocol="icmp",
            phase="fi-mass",
            site="au",
            processed=True,
        )
        result = self.f.run(strict=True)
        self.assertEqual(
            result["values"]["results.au.icmp.fi-mass.success.random.pct"]["raw"], 100
        )

    def test_later_merged_provenance_makes_pending_baseline_available(self):
        ids = self.streaming_mass()
        self.assertIsNone(
            self.f.run(strict=True)["values"][
                "results.au.icmp.fi-mass.success.random.pct"
            ]["raw"]
        )
        self.f.report(
            "strategies",
            dict(
                protocol="icmp",
                zmap_id=RUNS["icmp"],
                mass_measurement_id=ids[0],
                counts={"RANDOM": 8, "CONSTANT": 2},
                total_ips=10,
            ),
            protocol="icmp",
            phase="rt-fi",
            site="au",
        )
        result = self.f.run(strict=True)
        self.assertEqual(
            result["values"]["results.au.icmp.fi-mass.success.random.pct"]["raw"], 80
        )

    def test_conflicting_explicit_baseline_identities_still_fail_strict_export(self):
        self.streaming_mass(evaluation=True)
        self.f.report(
            "icmp-no_connection-mass-random-reproducibility-evaluation",
            dict(baseline_measurement_id="icmp_2026-11-17_05-26-23", repeats=[]),
            protocol="icmp",
            phase="fi-mass",
            site="au",
            run="icmp_other_evaluation",
        )
        with self.assertRaises(ValueError):
            self.f.run(strict=True)

    def test_disagreeing_canonical_copies_are_rejected(self):
        self.f.baseline()
        self.f.strategy({"SINGLE": 10}, phase="rt-fi", processed=True)
        with self.assertRaisesRegex(ValueError, "disagree"):
            self.f.run(strict=True)

    def test_bad_receipt_is_rejected(self):
        path = self.f.strategy({"CONSTANT": 10}, receipt=True)
        path.write_bytes(path.read_bytes() + b" ")
        with self.assertRaisesRegex(ValueError, "receipt"):
            self.f.run(strict=True)

    def test_archive_and_directory_conflicts_are_rejected(self):
        self.f.baseline()
        path = self.f.report(
            "strategies",
            dict(protocol="tcp", counts={"SINGLE": 10}, total_ips=10),
            phase="rt-fi",
        )
        archive = self.f.root / "input.zip"
        with zipfile.ZipFile(archive, "w") as z:
            z.writestr(path.relative_to(self.f.root).as_posix(), path.read_bytes())
        self.f.strategy({"CONSTANT": 10}, phase="rt-fi")
        with self.assertRaisesRegex(ValueError, "disagree"):
            self.f.run(archive=archive, strict=True)

    def test_recorded_configuration_is_shared_without_filling_missing_results(self):
        self.f.snapshot()
        self.f.snapshot(phase="fi-mass")
        v = self.f.run(strict=True)["values"]
        self.assertEqual(v["params.rt-base.sequence-length.count"]["raw"], 16)
        self.assertEqual(v["params.fi-mass.sequence-length.count"]["raw"], 100)
        self.assertEqual(v["params.fi-mass.min-replies.count"]["raw"], 80)
        self.assertEqual(v["params.fi-mass.request-interval"]["raw"], "20ms")
        self.assertIsNone(v["results.na.tcp.no-conn.rt-base.success.count"]["raw"])

    def test_parallel_snapshot_reads_overlap_and_read_each_request_once(self):
        for site in ("au", "eu"):
            self.f.snapshot(site=site)
        barrier = Barrier(2)
        snapshot = Readers.snapshot
        visited = []

        def synchronized(reader, site, protocol, connection, phase):
            request = (site, protocol, connection, phase)
            if site in ("au", "eu") and request[1:] == ("tcp", "no-connection", "rt-base"):
                visited.append(request)
                barrier.wait(timeout=10)
            return snapshot(reader, *request)

        with patch.object(Readers, "snapshot", synchronized):
            result = self.f.run(strict=True, io_workers=2, diagnostics=True)
        self.assertEqual(sorted(site for site, *_ in visited), ["au", "eu"])
        self.assertEqual(result["values"]["params.rt-base.sequence-length.count"]["raw"], 16)
        self.assertEqual(result["operations"]["snapshot.prefetch"]["calls"], 1)

    def test_prefetched_snapshots_match_sequential_values_sources_and_files(self):
        for site in ("au", "eu"):
            for protocol in RUNS:
                self.f.snapshot(site=site, protocol=protocol)
                self.f.snapshot(site=site, protocol=protocol, phase="fi-mass")
        with patch("paper_values.exporter.prefetch_snapshots"):
            expected = self.f.run(strict=True, io_workers=1)
        before = self.f.output_hashes()
        for workers in (1, 8):
            actual = self.f.run(strict=True, io_workers=workers, diagnostics=True)
            self.assertEqual(expected["values"], actual["values"])
            self.assertEqual(expected["input_errors"], actual["input_errors"])
            self.assertEqual(before, self.f.output_hashes())

    def test_snapshot_prefetch_preserves_error_attribution_and_strict_outputs(self):
        path = self.f.snapshot()
        self.f.run(strict=True)
        before = self.f.output_hashes()
        path.write_text("connection_count: [", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "invalid snapshot"):
            self.f.run(strict=True)
        self.assertEqual(before, self.f.output_hashes())
        with patch("paper_values.exporter.prefetch_snapshots"):
            expected = self.f.run(io_workers=1)
        actual = self.f.run(io_workers=8)
        self.assertTrue(actual["input_errors"])
        self.assertEqual(expected["input_errors"], actual["input_errors"])
        self.assertEqual(expected["values"], actual["values"])

    def test_snapshot_prefetch_rechecks_inputs_on_next_export(self):
        path = self.f.snapshot()
        first = self.f.run(strict=True)
        data = json.loads(path.read_text(encoding="utf-8"))
        data["connection_count"] = 8
        path.write_text(json.dumps(data), encoding="utf-8")
        second = self.f.run(strict=True)
        key = "params.rt-base.sequence-length.count"
        self.assertEqual(first["values"][key]["raw"], 16)
        self.assertEqual(second["values"][key]["raw"], 32)

    def test_shared_setting_conflict_fails_even_without_strict(self):
        self.f.snapshot()
        self.f.run()
        before = self.f.output_hashes()
        self.f.snapshot(protocol="icmp", requests_per_connection=5)
        with self.assertRaisesRegex(ValueError, "conflicting"):
            self.f.run()
        self.assertEqual(before, self.f.output_hashes())

    def test_invalid_yaml_is_reported_as_an_input_error(self):
        self.f.snapshot().write_text("connection_count: [", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "invalid snapshot"):
            self.f.run(strict=True)

    def test_reported_mass_configuration_works_without_raw_data(self):
        target = "tcp.ipid.no-connection.fixed-interval.mass"
        self.f.report(
            "missing-replies",
            dict(
                measurements=[
                    dict(
                        target=target,
                        sequence_length=100,
                        configured_minimum_replies=80,
                    )
                ]
            ),
            phase="fi-mass",
        )
        v = self.f.run()["values"]
        self.assertEqual(v["params.fi-mass.sequence-length.count"]["raw"], 100)
        self.assertEqual(v["params.fi-mass.min-replies.count"]["raw"], 80)

    def test_scalar_report_lookup_reads_only_the_selected_fields(self):
        self.f.report(
            "itdk-role-by-strategy",
            dict(
                plot_percentages={"Transit-Observed": {"SINGLE": 33.3}},
                plot_role_totals={"Transit-Observed": 1},
                coverage={"total": 3},
            ),
            phase="rt-fi",
        )
        v = self.f.run(strict=True)["values"]
        self.assertEqual(
            v["results.eu.tcp.no-conn.rt-fi.itdk-strategy.routers.single.pct"]["raw"],
            33.3,
        )
        self.assertAlmostEqual(
            v["results.eu.tcp.no-conn.rt-fi.itdk.routers.pct"]["raw"], 100 / 3
        )

    def test_missing_role_denominator_does_not_turn_a_count_into_a_percentage(self):
        self.f.report(
            "itdk-role-by-strategy", dict(plot_role_totals={"Transit-Observed": 12}), phase="rt-fi"
        )
        result = self.f.run(strict=True)
        self.assertIsNone(
            result["values"]["results.eu.tcp.no-conn.rt-fi.itdk.routers.pct"]["raw"]
        )

    def test_legacy_cache_is_discarded(self):
        (self.f.root / "tmp").mkdir()
        (self.f.root / "tmp/values.json").write_text(
            '{"schema_version":1,"contexts":{}}', encoding="utf-8"
        )
        self.f.run()
        data = json.loads((self.f.root / "tmp/values.json").read_text())
        self.assertEqual(data["schema_version"], 2)
        self.assertTrue(all(not ctx["entries"] for ctx in data["contexts"].values()))

    def test_additional_report_uses_canonical_keys_and_the_correct_campaign(self):
        path = self.f.root / "extra.json"
        key = "validation.fi-ideal.accuracy-pct"
        data = dict(
            schema_version=1,
            campaign_id=CAMPAIGN,
            values={
                key: dict(
                    value=99.3,
                    unit="percent",
                    definition="current synthetic validation",
                )
            },
        )
        path.write_text(json.dumps(data), encoding="utf-8")
        self.assertEqual(self.f.run(additional_reports=[path])["values"][key]["raw"], 99.3)
        before = self.f.output_hashes()
        data["campaign_id"] = "wrong"
        path.write_text(json.dumps(data), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "invalid additional"):
            self.f.run(additional_reports=[path])
        self.assertEqual(before, self.f.output_hashes())

    def test_exact_threshold_and_float_precision_are_preserved(self):
        threshold = 4.0383019016847365e-5
        kind = "tcp-no_connection-mass-random-reproducibility-evaluation"
        self.f.report(
            kind,
            dict(protocol="tcp", classifier_version="9", threshold=threshold, repeats=[]),
            phase="fi-mass",
        )
        result = self.f.run(strict=True)
        self.assertNotIn("classifier.random.threshold", result["values"])
        text = (self.f.root / "tmp/values.tex").read_text()
        self.assertIn(r"\setval{classifier.single.max-inc}{21845}", text)
        plain = render({"small": dict(raw=threshold, unit="")}, CAMPAIGN)
        self.assertIn(r"\setval{small}{4.0383019016847365e-05}", plain)

    def test_standard_todo_and_plain_values_in_the_tex_interface(self):
        text = render(
            {
                "example": dict(raw=None, unit=""),
                "count": dict(raw=1234567, unit="ips"),
                "pct": dict(raw=4.30974178195464, unit="percent"),
                "interval": dict(raw="20ms", unit=""),
                "timeout": dict(raw="2s", unit=""),
                "fractional-time": dict(raw="0.5ms", unit=""),
                "spaced-time": dict(raw="20 ms", unit=""),
                "literal": dict(raw="a~b_20ms", unit=""),
            },
            CAMPAIGN,
        )
        self.assertIn(r"\todo[inline]", text)
        self.assertIn(r"\setval{count}{1234567}", text)
        self.assertIn(r"\setval{pct}{4.31\%}", text)
        self.assertIn(r"\setval{interval}{20~ms}", text)
        self.assertIn(r"\setval{timeout}{2~s}", text)
        self.assertIn(r"\setval{fractional-time}{0.5~ms}", text)
        self.assertIn(r"\setval{spaced-time}{20~ms}", text)
        self.assertIn(r"\setval{literal}{a\textasciitilde{}b\_20ms}", text)
        self.assertNotIn("texmeta@", text)

    def test_validation_tex_uses_semantic_blocks_and_preserves_values(self):
        groups = [
            ["validation.data.fi.sequence-length", "validation.data.seed"],
            ["validation.e2e.ideal.accuracy-pct", "validation.e2e.ideal.class.multi.precision-pct",
             "validation.e2e.ideal.class.multi.recall-pct", "validation.e2e.ideal.total.count"],
            ["validation.e2e.lossy.accuracy-pct", "validation.e2e.lossy.total.count"],
            ["validation.e2e.routing.multi.forwarded.count", "validation.e2e.routing.multi.support.count",
             "validation.e2e.routing.random.forwarded.count"],
            ["validation.fi-ideal.accuracy-pct", "validation.fi-lossy.accuracy-pct",
             "validation.fi-reordered.accuracy-pct", "validation.lossy-reordered.multi-random.minimum-recall.pct"],
            ["validation.nist.threshold", "validation.nist-score.ideal.random.rejection-pct",
             "validation.nist-score.lossy.random.rejection-pct"],
            ["validation.random.threshold", "validation.random-score.ideal.random.rejection-pct"],
            ["validation.rt-ideal.accuracy-pct"],
            ["results.af.icmp.rt-base.coverage", "results.eu.icmp.rt-base.coverage"],
            ["results.eu.icmp.rt-base.other"],
            ["fig.validation.e2e-ideal.confusion-matrix"],
            ["fig.validation.fi-ideal.confusion-matrix", "fig.validation.fi-lossy-reordered.confusion-matrix",
             "fig.validation.fi-lossy-vs-lossy-reordered.confusion-matrix"],
            ["fig.validation.nist-score-cdf.ideal", "fig.validation.nist-score-cdf.lossy"],
            ["fig.validation.random-score-cdf.ideal", "fig.validation.random-score-cdf.lossy"],
        ]
        values = {key: dict(raw=i, unit="count") for i, key in enumerate(k for g in groups for k in g)}
        values["validation.random.threshold"] = dict(raw=4.0383019016847365e-5, unit="number")
        values["validation.rt-ideal.accuracy-pct"] = dict(raw=None, unit="percent")
        text = render(dict(reversed(list(values.items()))), CAMPAIGN)
        assignments = text[text.index(r"\setval{"):].rstrip()
        expected = [
            "\n".join(
                r"\setval{" + key + "}{" + ("" if values[key]["raw"] is None else str(values[key]["raw"])) + "}"
                for key in group
            )
            for group in groups
        ]
        self.assertEqual(assignments, "\n\n".join(expected))

    def test_target_continent_is_not_mistaken_for_vp_scope(self):
        self.assertEqual(
            key_order("results.eu.tcp.geo.as.count")[1],
            key_order("results.af.tcp.geo.as.count")[1],
        )
        self.assertNotEqual(
            key_order("results.eu.tcp.geo.as.count")[1],
            key_order("results.eu.tcp.geo.af.count")[1],
        )

    def test_selected_pdfs_use_canonical_paths(self):
        self.f.baseline()
        path = (
            self.f.reports
            / "eu/figures"
            / RUNS["tcp"]
            / "no-connection/merged/rt-based-base_fixed-interval-mass/n-rt-b_fi-m_operating-system-group-by-strategy.pdf"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"%PDF-1.4\nfixture")
        v = self.f.run(strict=True)["values"]
        key = "fig.eu.tcp.no-conn.rt-fi.os-by-strategy"
        self.assertTrue(v[key]["raw"].startswith("reports/continental/"))
        path.write_bytes(b"invalid PDF")
        with self.assertRaisesRegex(ValueError, "PDF signature"):
            self.f.run(strict=True)

    def test_output_cannot_replace_analysis_input(self):
        (self.f.root / "tmp").mkdir()
        with self.assertRaisesRegex(ValueError, "inside analysis inputs"):
            export(CAMPAIGN, reports_root=self.f.root, repository_root=self.f.root)

    def test_only_two_export_files_are_written(self):
        self.f.run(strict=True)
        self.assertEqual(
            {p.name for p in (self.f.root / "tmp").iterdir()},
            {"values.tex", "values.json"},
        )

    def test_detailed_timings_preserve_values_and_output_bytes(self):
        self.f.snapshot()
        plain = self.f.run(strict=True)
        before = self.f.output_hashes()
        detailed = self.f.run(strict=True, diagnostics=True)
        self.assertEqual(plain["operations"], {})
        self.assertEqual(plain["values"], detailed["values"])
        self.assertEqual(before, self.f.output_hashes())
        operations = detailed["operations"]
        self.assertEqual(operations["json.read"]["calls"], 1)
        self.assertEqual(operations["json.decode"]["calls"], 1)
        self.assertEqual(operations["json.check"]["calls"], 2)
        self.assertGreater(operations["snapshot.access"]["calls"], 0)
        for entry in operations.values():
            self.assertGreaterEqual(entry["seconds"], entry["maximum"])
            self.assertGreaterEqual(entry["maximum"], 0)

    def test_cli_reads_archive_and_does_not_import_analysis_or_plotting(self):
        archive = self.f.root / "empty.zip"
        with zipfile.ZipFile(archive, "w"):
            pass
        package = Path(__file__).resolve().parents[1] / "paper_values"
        shutil.copytree(
            package,
            self.f.root / "paper_values",
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        code = 'import sys; from paper_values.exporter import main; main(); assert not any(m.split(".")[0] in {"ipid_analysis","matplotlib","polars","pyarrow","yaml"} for m in sys.modules)'
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                "-c",
                code,
                "--campaign",
                CAMPAIGN,
                "--archive",
                str(archive),
                "--strict",
            ],
            cwd=self.f.root,
            text=True,
            capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("1770 empty", result.stdout)
        self.assertNotIn("Operations (", result.stdout)
        profiled = subprocess.run(
            [*result.args, "--timings"], cwd=self.f.root, text=True, capture_output=True
        )
        self.assertEqual(profiled.returncode, 0, profiled.stderr)
        self.assertIn("elapsed sums can exceed wall time", profiled.stdout)
        self.assertIn("cache.load: calls=1", profiled.stdout)

    @unittest.skipUnless(shutil.which("make"), "GNU Make is not installed")
    def test_make_accepts_both_argument_variable_names(self):
        archive = self.f.root / "empty.zip"
        with zipfile.ZipFile(archive, "w"):
            pass
        repository = Path(__file__).resolve().parents[1]
        shutil.copytree(
            repository / "paper_values",
            self.f.root / "paper_values",
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        shutil.copyfile(repository / "Makefile", self.f.root / "Makefile")
        for name in ("ARGS", "args"):
            result = subprocess.run(
                [
                    "make",
                    "--no-print-directory",
                    "paper-values",
                    f"CAMPAIGN_ID={CAMPAIGN}",
                    f"PYTHON_INTERPRETER={shlex.quote(sys.executable)}",
                    f"{name}=--archive {shlex.quote(str(archive))} --strict",
                ],
                cwd=self.f.root,
                text=True,
                capture_output=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("1770 empty", result.stdout)


class DerivedCacheTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.f = Fixture(self.temp.name)

    def histogram(self, counts=(98, 1), increments=(1, 900), total=100):
        report = self.f.report("increment-distributions", dict(protocol="tcp"), phase="comparison")
        path = (
            self.f.processed
            / "eu"
            / RUNS["tcp"]
            / "no-connection"
            / (PATHS["comparison"] + "increment-distributions.pq")
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        cumulative = []
        for n in counts:
            cumulative.append((cumulative[-1] if cumulative else 0) + n)
        pq.write_table(
            pa.table(
                dict(
                    MODE=["RT-based"] * len(counts),
                    IPID_SELECTION_STRATEGY=["PER_BUCKET"] * len(counts),
                    INCREMENT=list(increments),
                    COUNT=list(counts),
                    CUMULATIVE_COUNT=cumulative,
                    TOTAL_COUNT=[total] * len(counts),
                )
            ),
            path,
        )
        return path, report

    def test_clipped_histogram_does_not_invent_tail_fractions(self):
        path, _ = self.histogram()
        v = self.f.run(strict=True)["values"]
        self.assertNotIn("results.eu.tcp.no-conn.rt-base.increments.per-bucket.p99", v)
        self.assertEqual(increment_histogram(path)["RT-based:PER_BUCKET"]["p99"], 900)
        self.assertIsNone(v["results.eu.tcp.no-conn.rt-base.increments.per-bucket.below-1000.pct"]["raw"])

    def test_cached_histogram_is_not_read_again(self):
        self.histogram(counts=(98, 2), total=100)
        self.f.snapshot(protocol="tcp", phase="fi-mass")
        self.f.snapshot(protocol="tcp", phase="rt-base")
        first = self.f.run(strict=True)
        self.assertEqual(first["cache"]["histogram_misses"], 1)
        cache_path = self.f.root / "tmp/values.json"
        cached = cache_path.read_bytes()
        with patch(
            "paper_values.readers.increment_histogram",
            side_effect=AssertionError("must use cache"),
        ):
            second = self.f.run(strict=True)
        self.assertEqual(second["cache"]["histogram_hits"], 1)
        self.assertEqual(first["values"], second["values"])
        self.assertEqual(cache_path.read_bytes(), cached)
        self.assertEqual(second["values"]["params.fi-mass.request-interval"]["raw"], "20ms")
        self.assertEqual(second["values"]["params.rt-base.reply-timeout"]["raw"], "2s")
        tex = (self.f.root / "tmp/values.tex").read_text()
        self.assertIn(r"\setval{params.fi-mass.request-interval}{20~ms}", tex)
        self.assertIn(r"\setval{params.rt-base.reply-timeout}{2~s}", tex)

    def test_new_paper_pdf_updates_figure_path_without_recomputing_cached_values(self):
        _, report = self.histogram(counts=(98, 2), total=100)
        original = report.with_suffix(".pdf")
        original.write_bytes(b"%PDF-1.4\noriginal figure")
        first = self.f.run(strict=True)
        key = "fig.eu.tcp.no-conn.rt-vs-fi-base.inc-distributions"
        self.assertTrue(first["values"][key]["raw"].endswith("increment-distributions.pdf"))
        cache_path = self.f.root / "tmp/values.json"
        cached = cache_path.read_bytes()
        paper = original.with_name(original.stem + "__paper.pdf")
        paper.write_bytes(b"%PDF-1.4\npaper figure")
        self.f.receipt(paper)
        with patch(
            "paper_values.readers.increment_histogram", side_effect=AssertionError("cache")
        ):
            second = self.f.run(strict=True)
            self.assertTrue(second["values"][key]["raw"].endswith("__paper.pdf"))
            self.assertEqual(second["cache"]["histogram_hits"], 1)
            self.assertEqual(cache_path.read_bytes(), cached)
            paper.unlink()
            third = self.f.run(strict=True)
        self.assertEqual(first["values"][key]["raw"], third["values"][key]["raw"])
        self.assertEqual(third["cache"]["histogram_hits"], 1)
        self.assertEqual(cache_path.read_bytes(), cached)
        self.assertEqual(
            {k: v for k, v in first["values"].items() if k != key},
            {k: v for k, v in second["values"].items() if k != key},
        )

    def test_diagnostics_keep_warm_cache_and_identical_outputs(self):
        self.histogram(counts=(98, 2), total=100)
        cold = self.f.run(strict=True, diagnostics=True)
        self.assertEqual(cold["operations"]["cache.compute"]["calls"], 1)
        self.assertEqual(cold["operations"]["cache.verify"]["calls"], 1)
        before = self.f.output_hashes()
        with patch(
            "paper_values.readers.increment_histogram",
            side_effect=AssertionError("must use cache"),
        ):
            warm = self.f.run(strict=True, diagnostics=True)
        self.assertEqual(warm["cache"]["histogram_hits"], 1)
        self.assertEqual(warm["operations"]["cache.check"]["calls"], 1)
        self.assertNotIn("cache.compute", warm["operations"])
        self.assertNotIn("cache.verify", warm["operations"])
        self.assertEqual(cold["values"], warm["values"])
        self.assertEqual(before, self.f.output_hashes())

    def test_changed_input_and_recompute_bypass_cached_values(self):
        self.histogram(counts=(98, 2), increments=(1, 900))
        self.f.run()
        self.histogram(counts=(90, 10), increments=(1, 1200))
        result = self.f.run(strict=True)
        self.assertEqual(result["cache"]["histogram_misses"], 1)
        key = "results.eu.tcp.no-conn.rt-base.increments.per-bucket.below-1000.pct"
        self.assertEqual(result["values"][key]["raw"], 90)
        self.assertEqual(self.f.run(recompute=True)["cache"]["histogram_misses"], 1)

    def test_bad_aggregate_receipt_preserves_both_existing_outputs(self):
        path, _ = self.histogram()
        marker = self.f.receipt(path)
        self.f.run(strict=True)
        before = self.f.output_hashes()
        data = json.loads(marker.read_text())
        data["sha256"] = "0" * 64
        marker.write_text(json.dumps(data))
        with self.assertRaisesRegex(ValueError, "receipt"):
            self.f.run(strict=True)
        self.assertEqual(before, self.f.output_hashes())

    def test_os_family_ranges_reuse_the_existing_small_aggregate(self):
        groups = {
            g: dict(ip_count=100, section="General-Purpose OS")
            for g in ("rhel", "centos", "fedora")
        }
        self.f.report(
            "operating-system-group-by-strategy",
            dict(strategy_ip_count=500, matched_grouped_ip_count=300, groups=groups),
            protocol="icmp",
            phase="rt-fi",
        )
        path = (
            self.f.processed
            / "eu"
            / RUNS["icmp"]
            / "no-connection"
            / (PATHS["rt-fi"] + "operating-system-group-by-strategy.pq")
        )
        path.parent.mkdir(parents=True)
        pq.write_table(
            pa.table(
                dict(
                    OS_GROUP=["rhel", "centos", "fedora"],
                    IPID_SELECTION_STRATEGY=["PER_BUCKET"] * 3,
                    COUNT=[70, 80, 90],
                    PERCENTAGE=[70.0, 80.0, 90.0],
                )
            ),
            path,
        )
        result = self.f.run(strict=True)
        self.assertEqual(result["cache"]["os-strategy_misses"], 1)
        self.assertEqual(
            result["values"]["results.eu.icmp.rt-fi.os.rpm.per-bucket.min-pct"]["raw"],
            70,
        )
        self.assertEqual(
            result["values"]["results.eu.icmp.rt-fi.os.rpm.per-bucket.max-pct"]["raw"],
            90,
        )

    def test_cache_is_scoped_by_campaign_and_input_path(self):
        p = self.f.root / "input"
        p.write_text("input")
        cache_path = self.f.root / "values.json"
        cache = ValueCache(cache_path, "one")
        cache.derive("stat", p, "report", lambda: dict(value=1))
        cache_path.write_text(cache.serialize())
        other = ValueCache(cache_path, "two")
        self.assertEqual(other.derive("stat", p, "report", lambda: dict(value=2)), dict(value=2))
        self.assertEqual(other.stats["stat_misses"], 1)

    def test_missing_sources_are_rechecked_and_corrupt_cache_is_discarded(self):
        p = self.f.root / "input"
        cache_path = self.f.root / "values.json"
        cache_path.write_text("broken cache")
        cache = ValueCache(cache_path, CAMPAIGN)
        self.assertEqual(cache.derive("stat", p, "report", lambda: dict(value=1)), {})
        p.write_text("appeared")
        self.assertEqual(cache.derive("stat", p, "report", lambda: dict(value=2)), dict(value=2))

    def test_changed_report_and_corrupt_cached_payload_are_recomputed(self):
        p = self.f.root / "input"
        p.write_text("input")
        cache_path = self.f.root / "values.json"
        cache = ValueCache(cache_path, CAMPAIGN)
        cache.derive("stat", p, "report-1", lambda: dict(value=1))
        cache_path.write_text(cache.serialize())
        cache = ValueCache(cache_path, CAMPAIGN)
        self.assertEqual(cache.derive("stat", p, "report-2", lambda: dict(value=2)), dict(value=2))
        for entry in cache.entries.values():
            entry["values"] = {"value": 999}
        cache_path.write_text(cache.serialize())
        cache = ValueCache(cache_path, CAMPAIGN)
        self.assertEqual(cache.derive("stat", p, "report-1", lambda: dict(value=3)), dict(value=3))


class DiagnosticsTest(unittest.TestCase):
    def test_disabled_diagnostics_do_not_read_clock_or_change_result(self):
        details = Diagnostics()
        with patch(
            "paper_values.diagnostics.perf_counter", side_effect=AssertionError("disabled")
        ):
            self.assertEqual(details.call("unused", lambda: 7), 7)
            with details.measure("unused"):
                pass
        self.assertEqual(details.summary(), {})

    def test_failed_operation_is_recorded_and_exception_preserved(self):
        details = Diagnostics(enabled=True)
        with patch("paper_values.diagnostics.perf_counter", side_effect=(10.0, 13.0)):
            with self.assertRaisesRegex(OSError, "original error"):
                with details.measure("read"):
                    raise OSError("original error")
        self.assertEqual(details.summary()["read"], dict(calls=1, seconds=3.0, maximum=3.0))

    def test_parallel_calls_have_exact_counts(self):
        from concurrent.futures import ThreadPoolExecutor

        details = Diagnostics(enabled=True)
        with ThreadPoolExecutor(max_workers=8) as pool:
            values = list(pool.map(lambda value: details.call("read", lambda: value), range(1000)))
        self.assertEqual(values, list(range(1000)))
        self.assertEqual(details.summary()["read"]["calls"], 1000)


@unittest.skipUnless(os.environ.get("IPID_PAPER_VALUES_ARCHIVE"), "optional supplied EU archive")
class RealEUArchiveTest(unittest.TestCase):
    def test_existing_eu_paper_values(self):
        with tempfile.TemporaryDirectory() as root:
            result = export(
                "geo-full-20261107-r1",
                reports_root=None,
                archive=os.environ["IPID_PAPER_VALUES_ARCHIVE"],
                repository_root=Path(root),
                strict=True,
            )
            v = result["values"]
            self.assertEqual((result["available"], result["missing"]), (142, 1122))
            for protocol, total in (
                ("icmp", 319105777),
                ("tcp", 94046744),
                ("udp", 3353747),
            ):
                mode = "no-conn." if protocol == "tcp" else ""
                key = f"results.eu.{protocol}.{mode}rt-base.success.count"
                self.assertEqual(v[key]["raw"], total)
                self.assertIsNone(v[key.replace(".eu.", ".na.")]["raw"])
            self.assertEqual(
                v["results.eu.tcp.no-conn.rt-fi.rt-pop.constant.pct"]["formatted"],
                "77.80%",
            )
            self.assertEqual(
                v["results.eu.tcp.conn.rt-base.itdk-strategy.all-others.single.pct"]["raw"],
                1.9701569113685922,
            )
            self.assertTrue(
                v["fig.eu.tcp.no-conn.rt-fi.os-by-strategy"]["raw"].startswith("reports/")
            )
            self.assertFalse(result["input_errors"])


if __name__ == "__main__":
    unittest.main()
