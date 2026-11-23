"""Pinned Full populations, additive exports, and one-time read-only preparation."""

import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from tests import test_paper_values_followups as followup_tests
from tests.test_paper_values import Fixture

from paper_values import full_review
from paper_values.exporter import export, load_metrics, required_contexts
from paper_values.inputs import SITES, Inputs, select_artifacts
from paper_values.readers import classifier_constants
from paper_values.tex import render

CAMPAIGN = followup_tests.CAMPAIGN


class FullReviewTest(unittest.TestCase):
    def setUp(self):
        self.followup = followup_tests.FullFollowupTest()
        self.followup.setUp()
        self.addCleanup(self.followup.doCleanups)
        self.followup.fixture()
        self.f = Fixture(self.followup.root)
        self.f.reports = self.followup.reports
        self.f.processed = self.f.root / "data/processed/continental" / CAMPAIGN
        self.f.raw = self.f.root / "data/raw/continental" / CAMPAIGN
        self.spec = self.followup.spec
        self.analysis = self.followup.analysis
        self.version = classifier_constants(
            Path(__file__).resolve().parents[1] / "ipid_analysis/strategies.py"
        )["CLASSIFIER_VERSION"]

    def table(self, path, rows, *, labels=False):
        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(rows)
        if labels:
            table = table.replace_schema_metadata({b"classifier_version": self.version.encode()})
        pq.write_table(table, path)
        self.f.receipt(path)
        return path

    def add_inventory(self, done, path):
        # Deliberately use another machine's absolute paths, as production does.
        relative = path.relative_to(self.f.root).as_posix()
        done["artifacts"] = [
            a for a in done["artifacts"] if a["path"] != "/mnt/data01/ipid-analysis/" + relative
        ]
        done["artifacts"].append(
            dict(
                path="/mnt/data01/ipid-analysis/" + relative,
                size_bytes=path.stat().st_size,
                sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            )
        )

    def fixture(self):
        document = json.loads((self.analysis / "done__full.json").read_text())
        for done in document["sites"]:
            site, zmap = done["site"], self.spec["zmap_ids"][done["site"]]
            # Historical RT is poison: the Full preparation must never read it.
            self.f.baseline(site=site)
            before = self.f.strategy({"CONSTANT": 6, "SINGLE": 2, "UNCLASSIFIED": 2}, site=site)
            fi = self.f.strategy(
                {"CONSTANT": 3, "PER_CONNECTION": 2}, phase="fi-base", mode="connection", site=site
            )
            self.f.report(
                "coverage",
                dict(zmap_ip_count=6, ipid_ip_count=5),
                phase="fi-base",
                mode="connection",
                processed=True,
                site=site,
            )
            # Fixture's report helper defaults to EU discovery; replace each VP identity.
            for path in (self.f.reports / site).rglob("*.json"):
                data = json.loads(path.read_text())
                data["zmap_id"] = zmap
                path.write_text(json.dumps(data))

            def old_labels(report, rows):
                relative = report.relative_to(self.f.reports / site / "figures")
                return self.table(
                    self.f.processed / site / relative.with_suffix(".pq"),
                    [dict(IP_ADDR=ip, IPID_SELECTION_STRATEGY=s) for ip, s in rows],
                    labels=True,
                )

            old_labels(
                before,
                [
                    (c, "SINGLE" if c in "de" else "UNCLASSIFIED" if c in "hj" else "CONSTANT")
                    for c in "abcdefghij"
                ],
            )
            old_labels(fi, [(c, "PER_CONNECTION" if c in "cd" else "CONSTANT") for c in "abcdi"])
            target = self.f.raw / site / "zmap" / zmap / "zmap-connection-sample.pq"
            self.table(target, [dict(IP_ADDR=c) for c in "abcdij"])
            self.followup.write(
                target.with_suffix(".json"),
                dict(
                    version=1,
                    zmap_id=zmap,
                    reply_type="synack",
                    source_rows=20,
                    eligible_rows=10,
                    sample_rows=6,
                ),
            )
            root = self.analysis / site
            phase = f"{zmap}/connection/rt-based-base/c-rt-b_"
            report = root / ("full/figures/" + phase + "strategies__full.json")
            data = json.loads(report.read_text())
            data["counts"] = dict(
                CONSTANT=2, PER_CONNECTION=2, SINGLE=2, PER_BUCKET=1, UNCLASSIFIED=1
            )
            self.followup.write(report, data)
            self.add_inventory(done, report)
            processed = (
                self.f.processed.parent.parent
                / "followups"
                / self.spec["measurement_run"]
                / self.spec["analysis_id"]
                / site
            )
            path = self.table(
                processed / ("full/" + phase + "strategies.pq"),
                [
                    dict(IP_ADDR=c, IPID_SELECTION_STRATEGY=s)
                    for c, s in zip(
                        "abcdefgh",
                        (
                            "CONSTANT",
                            "CONSTANT",
                            "PER_CONNECTION",
                            "PER_CONNECTION",
                            "SINGLE",
                            "SINGLE",
                            "PER_BUCKET",
                            "UNCLASSIFIED",
                        ),
                    )
                ],
                labels=True,
            )
            self.add_inventory(done, path)
            path = self.table(
                processed / ("full/" + phase + "increments.pq"),
                [
                    dict(
                        IP_ADDR="e", IPID_SELECTION_STRATEGY="SINGLE", INCREMENTS=[0, 1999, 2000]
                    ),
                    dict(IP_ADDR="f", IPID_SELECTION_STRATEGY="SINGLE", INCREMENTS=[1]),
                    dict(
                        IP_ADDR="g",
                        IPID_SELECTION_STRATEGY="PER_BUCKET",
                        INCREMENTS=[10, 20, 10000],
                    ),
                ],
            )
            self.add_inventory(done, path)
            rows = []
            for mode, strategy, vals in (
                ("RT-based", "PER_BUCKET", [1]),
                ("Fixed-Interval", "PER_BUCKET", [5]),
                ("RT-based", "SINGLE", [1999, 2000]),
                ("Fixed-Interval", "SINGLE", [1000]),
            ):
                for i, v in enumerate(vals):
                    rows.append(
                        dict(
                            MODE=mode,
                            IPID_SELECTION_STRATEGY=strategy,
                            INCREMENT=v,
                            COUNT=1,
                            CUMULATIVE_COUNT=i + 1,
                            TOTAL_COUNT=len(vals),
                        )
                    )
            path = self.table(
                processed
                / f"paired/{zmap}/connection/comparison/rt-based-base_fixed-interval-base/c-rt-b_fi-b_increment-distributions.pq",
                rows,
            )
            self.add_inventory(done, path)
            consistency = root / ("full/figures/" + phase + "itdk-consistency-summary__full.json")
            self.followup.write(
                consistency,
                dict(
                    measurement_id=done["full_measurement"]["measurement_id"],
                    zmap_id=zmap,
                    eligible_nodes_with_two_classified_interfaces=4,
                    strict_agreement_nodes=3,
                ),
            )
            self.add_inventory(done, consistency)
            if site == "eu":
                mid = done["full_measurement"]["measurement_id"]
                raw = (
                    self.f.raw.parent.parent
                    / "followups"
                    / self.spec["measurement_run"]
                    / site
                    / "full/ipid"
                    / mid
                )
                sequences = [
                    dict(
                        IP_ADDR=c,
                        IPID_SEQUENCE=",".join(map(str, ids)),
                        DF_SEQUENCE=",".join([flag] * 16),
                    )
                    for c, ids, flag in (
                        ("a", [4] * 4 + [0] * 12, "1"),
                        ("b", [3] * 4 + [42] * 12, "0"),
                        ("c", [7] * 4 + list(range(12)), "1"),
                        ("d", list(range(16)), "1"),
                    )
                ]
                self.table(raw / "ipid.pq", sequences)
                self.followup.write(
                    raw / "ipid.snapshot.yaml",
                    dict(
                        connection_count=4,
                        requests_per_connection=4,
                        tcp=dict(establish_connection=True),
                    ),
                )
                self.followup.write(
                    raw / "measurement-complete.json",
                    dict(
                        version=1,
                        measurement_id=mid,
                        files={
                            n: dict(size_bytes=(raw / n).stat().st_size)
                            for n in ("ipid.pq", "ipid.snapshot.yaml")
                        },
                    ),
                )
            self.followup.write(root / "done__full.json", done)
        self.followup.write(self.analysis / "done__full.json", document)
        contexts, plots = required_contexts(load_metrics()["metrics"])
        inputs = Inputs((self.f.reports, self.f.processed), contexts=contexts, plot_contexts=plots)
        try:
            self.artifacts, conflicts = select_artifacts(list(inputs.artifacts(CAMPAIGN)))
            self.assertFalse(conflicts)
            self.assertFalse(inputs.errors)
        finally:
            inputs.close()
        return document

    def run_export(self):
        return export(
            CAMPAIGN,
            reports_root=self.f.reports,
            processed_root=self.f.processed,
            raw_root=self.f.raw,
            repository_root=self.f.root,
            strict=True,
        )

    def test_full_preparation_export_cache_and_source_preservation(self):
        self.fixture()
        before = {
            p: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in self.f.root.rglob("*")
            if p.is_file()
        }
        original = self.run_export()["values"]
        full_review.prepare(CAMPAIGN, self.f.reports, self.f.processed, self.f.raw)
        with (
            patch(
                "paper_values.paired.fi_rt", side_effect=AssertionError("cache should avoid joins")
            ),
            patch(
                "paper_values.tcp_review.calculate",
                side_effect=AssertionError("cache should avoid raw reads"),
            ),
            patch(
                "paper_values.full_review.full_increment_stats",
                side_effect=AssertionError("cache should avoid increments"),
            ),
        ):
            full_review.prepare(CAMPAIGN, self.f.reports, self.f.processed, self.f.raw)
            values = self.run_export()["values"]
        for k, v in original.items():
            if not k.endswith(".full"):
                self.assertEqual(values[k], v, k)
        prefix = "results.eu.tcp.conn."
        expected = {
            "rt-base.constant.df0.pct.full": 50,
            "rt-base.constant.df1.pct.full": 50,
            "rt-base.constant.ipid-zero.pct.full": 50,
            "rt-base.per-connection.handshake.constant.pct.full": 50,
            "rt-base.increments.per-bucket.p99.full": 10000,
            "rt-vs-fi-base.rt-base.increments.per-bucket.p99.full": 1,
            "rt-vs-fi-base.fi-base.increments.per-bucket.p99.full": 5,
            "rt-vs-fi-base.rt-base.increments.single.below-2000.pct.full": 50,
            "fi-to-rt-success.pct.full": 125,
            "rt-base.targets.pct.full": 100,
            "fi-base.targets.pct": 60,
            "rt-base.itdk.strict-agreement.pct.full": 75,
        }
        for suffix, n in expected.items():
            self.assertEqual(values[prefix + suffix]["raw"], n, suffix)
        self.assertAlmostEqual(
            values[prefix + "rt-base.increments.single.below-2000.pct.full"]["raw"], 200 / 3
        )
        self.assertEqual(
            values["results.eu.tcp.rt-no-conn-to-conn.constant.per-connection.column-pct.full"][
                "raw"
            ],
            50,
        )
        for site in SITES:
            self.assertEqual(
                values[f"results.{site}.tcp.conn.rt-base.distribution.constant.pct.full"]["raw"],
                25,
            )
        self.assertEqual(
            values["results.vp-comparison.tcp.conn.rt-base.constant.range.pp.full"]["raw"], 0
        )
        for path, sha in before.items():
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), sha, str(path))
        text = render(values, CAMPAIGN)
        lines = [
            line
            for line in text.splitlines()
            if line.startswith("\\setval{results.vp-comparison.tcp.conn.rt-base.constant.")
            and ".full}" in line
        ]
        self.assertIn("\n".join(lines), text)

    def test_missing_full_cache_never_reuses_historical_handshake(self):
        self.fixture()
        values = self.run_export()["values"]
        for key in full_review.defaults():
            self.assertIsNone(values[key]["raw"])
        self.assertEqual(values["results.eu.tcp.zmap.count"]["raw"], 20)
        self.assertEqual(values["results.eu.tcp.zmap.syn-ack.count"]["raw"], 10)

    def test_stale_raw_input_is_rejected_without_implicit_recomputation(self):
        self.fixture()
        full_review.prepare(CAMPAIGN, self.f.reports, self.f.processed, self.f.raw)
        path = next((self.f.raw.parent.parent / "followups").rglob("ipid.pq"))
        path.write_bytes(path.read_bytes() + b"changed")
        with self.assertRaisesRegex(ValueError, "stale"):
            full_review.read_prepared(self.f.processed, CAMPAIGN)

    def test_corrupt_pinned_increment_prevents_preparation(self):
        self.fixture()
        path = next((self.f.processed.parent.parent / "followups").rglob("c-rt-b_increments.pq"))
        path.write_bytes(b"corrupt")
        with self.assertRaisesRegex(ValueError, "pinned inventory"):
            full_review.prepare(CAMPAIGN, self.f.reports, self.f.processed, self.f.raw)

    def test_missing_eligibility_is_unknown_instead_of_all_zmap(self):
        self.fixture()
        for p in self.f.raw.rglob("zmap-connection-sample.json"):
            p.unlink()
        values = self.run_export()["values"]
        self.assertIsNone(values["results.eu.tcp.conn.fi-base.targets.pct"]["raw"])
        self.assertIsNone(values["results.eu.tcp.conn.rt-base.targets.pct.full"]["raw"])
        self.assertEqual(values["results.eu.tcp.conn.fi-base.success.pct"]["raw"], 500 / 6)

    def test_full_vp_ranges_use_complete_plot_population_not_success_only(self):
        document = self.fixture()
        done = document["sites"][-1]
        site = done["site"]
        zmap = self.spec["zmap_ids"][site]
        report = (
            self.analysis
            / site
            / f"full/figures/{zmap}/connection/rt-based-base/c-rt-b_strategies__full.json"
        )
        data = json.loads(report.read_text())
        data["counts"]["NOT_ENOUGH_SAMPLES"] = 1
        data["total_ips"] = 9
        self.followup.write(report, data)
        self.add_inventory(done, report)
        self.followup.write(self.analysis / site / "done__full.json", done)
        self.followup.write(self.analysis / "done__full.json", document)
        values = self.run_export()["values"]
        prefix = "results.vp-comparison.tcp.conn.rt-base.constant."
        self.assertEqual(values[prefix + "max.pct.full"]["raw"], 25)
        self.assertAlmostEqual(values[prefix + "min.pct.full"]["raw"], 200 / 9)
        self.assertAlmostEqual(values[prefix + "range.pp.full"]["raw"], 25 - 200 / 9)
        self.assertEqual(values[f"results.{site}.tcp.conn.rt-base.success.count.full"]["raw"], 8)

    def test_invalid_selection_cannot_publish_an_all_tcp_denominator(self):
        self.fixture()
        path = self.f.raw / "eu/zmap" / self.spec["zmap_ids"]["eu"] / "zmap-connection-sample.json"
        data = json.loads(path.read_text())
        data["source_rows"] = 19
        self.followup.write(path, data)
        with self.assertRaisesRegex(ValueError, "invalid or ambiguous inputs"):
            self.run_export()

    def test_archive_does_not_load_prepared_live_full_scalars(self):
        self.fixture()
        full_review.prepare(CAMPAIGN, self.f.reports, self.f.processed, self.f.raw)
        values, errors = full_review.load(self.f.processed, CAMPAIGN, archive=True)
        self.assertFalse(errors)
        self.assertTrue(all(v["raw"] is None for v in values.values()))

    def test_clipped_paired_tail_uses_exact_paired_inputs_only_during_prepare(self):
        document = self.fixture()
        done = next(d for d in document["sites"] if d["site"] == "eu")
        zmap = self.spec["zmap_ids"]["eu"]
        processed = (
            self.f.processed.parent.parent
            / "followups"
            / self.spec["measurement_run"]
            / self.spec["analysis_id"]
            / "eu"
        )
        path = (
            processed
            / f"paired/{zmap}/connection/comparison/rt-based-base_fixed-interval-base/c-rt-b_fi-b_increment-distributions.pq"
        )
        rows = [
            r
            for r in pq.read_table(path).to_pylist()
            if (r["MODE"], r["IPID_SELECTION_STRATEGY"]) != ("RT-based", "SINGLE")
        ]
        rows.append(
            dict(
                MODE="RT-based",
                IPID_SELECTION_STRATEGY="SINGLE",
                INCREMENT=1,
                COUNT=999,
                CUMULATIVE_COUNT=999,
                TOTAL_COUNT=1000,
            )
        )
        self.table(path, rows)
        self.add_inventory(done, path)
        tail = processed / f"paired/{zmap}/connection/rt-based-base/c-rt-b_increments.pq"
        self.table(
            tail,
            [dict(IP_ADDR="a", IPID_SELECTION_STRATEGY="SINGLE", INCREMENTS=[1] * 999 + [3000])],
        )
        self.add_inventory(done, tail)
        self.followup.write(self.analysis / "eu/done__full.json", done)
        self.followup.write(self.analysis / "done__full.json", document)
        full_review.prepare(CAMPAIGN, self.f.reports, self.f.processed, self.f.raw)
        with patch(
            "paper_values.full_review.full_increment_stats",
            side_effect=AssertionError("ordinary export must not rescan"),
        ):
            values = self.run_export()["values"]
        key = "results.eu.tcp.conn.rt-vs-fi-base.rt-base.increments.single.below-2000.pct.full"
        self.assertEqual(values[key]["raw"], 99.9)
        self.assertIn(str(tail), values[key]["sources"])


if __name__ == "__main__":
    unittest.main()
