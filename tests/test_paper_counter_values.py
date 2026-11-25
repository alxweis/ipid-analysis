"""Scientific cohorts, greedy sample rejection, cache-only exports and applicability."""

from contextlib import redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from tests.test_paper_values import CAMPAIGN, RUNS, Fixture

from ipid_analysis.deterministic_transition_analysis import greedy_outlier_scores
from ipid_analysis.strategies import CLASSIFIER_VERSION, MAX_INC, MeasurementConfig
from paper_values import outliers
from paper_values.adjusted_os import AdjustedOSReader
from paper_values.exporter import load_metrics
from paper_values.inputs import Inputs, select_artifacts
from paper_values.phase_populations import load as phase_values
from paper_values.review import vp_values
from paper_values.tex import formatted, render


def config():
    return MeasurementConfig(4, 4, np.arange(16))


def digest(path):
    return path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest()


class GreedyScoreTest(unittest.TestCase):
    def test_rejected_values_never_move_reference_and_scores_zero_through_five(self):
        rows = []
        for score in range(6):
            s = np.arange(100, 116)
            s[1 : 1 + score] = 30000
            rows.append(s)
        scores = greedy_outlier_scores(np.asarray(rows), np.asarray(["SINGLE"] * 6), config())
        np.testing.assert_array_equal(scores, np.arange(6))
        # [100, 30000, 102] has two bad adjacent edges, but only one rejected IP-ID.
        self.assertEqual(scores[1], 1)

    def test_position_streams_wraparound_and_exact_bounds(self):
        dest = np.asarray(
            [
                v
                for pair in zip([100, 30000, 101, 102, 103, 104, 105, 106], range(1000, 1008))
                for v in pair
            ]
        )
        bucket = np.asarray([100 + i // 4 + 10000 * (i % 4) for i in range(16)])
        bucket[8] = 30000
        wrap = np.arange(65530, 65546) % 65536
        matrix = np.asarray([dest, bucket, wrap])
        np.testing.assert_array_equal(
            greedy_outlier_scores(
                matrix, np.asarray(["PER_DESTINATION", "PER_BUCKET", "SINGLE"]), config()
            ),
            [1, 1, 0],
        )
        edge = np.asarray([100, 100 + MAX_INC])
        pair_cfg = MeasurementConfig(2, 1, np.arange(2))
        self.assertEqual(
            greedy_outlier_scores(edge[None, :], np.asarray(["SINGLE"]), pair_cfg)[0], 0
        )
        edge[1] += 1
        self.assertEqual(
            greedy_outlier_scores(edge[None, :], np.asarray(["SINGLE"]), pair_cfg)[0], 1
        )

    def test_vectorized_scores_match_independent_sequential_reference(self):
        rng = np.random.default_rng(279)
        matrix = rng.integers(0, 65536, size=(300, 16))
        classes = np.asarray(["SINGLE", "PER_DESTINATION", "PER_BUCKET"] * 100)
        expected = []
        for sequence, strategy in zip(matrix, classes):
            last, n = {}, 0
            for i, value in enumerate(sequence):
                stream = (
                    0
                    if strategy == "SINGLE"
                    else i % 2
                    if strategy == "PER_DESTINATION"
                    else i % 4
                )
                high = 1 if strategy == "PER_DESTINATION" else MAX_INC
                if stream not in last or 1 <= (int(value) - last[stream]) % 65536 <= high:
                    last[stream] = int(value)
                else:
                    n += 1
            expected.append(n)
        np.testing.assert_array_equal(greedy_outlier_scores(matrix, classes, config()), expected)

    def test_invalid_sequences_and_unsupported_strategy_are_rejected(self):
        for matrix, classes in (
            (np.zeros((1, 15)), ["SINGLE"]),
            (np.full((1, 16), -1), ["SINGLE"]),
            (np.full((1, 16), 65536), ["SINGLE"]),
            (np.zeros((1, 16)), ["MULTI"]),
        ):
            with self.assertRaises(ValueError):
                greedy_outlier_scores(matrix, np.asarray(classes), config())


class PaperExtensionTest(unittest.TestCase):
    def test_success_all_percentage_retains_phase_two_failures_once_and_zmap_denominator(self):
        reports = [
            SimpleNamespace(
                site="eu",
                protocol="icmp",
                connection="no-connection",
                phase=phase,
                kind=kind,
                name=phase + kind,
                data=data,
            )
            for phase, kind, data in (
                (
                    "rt-base",
                    "strategies",
                    dict(
                        counts={"CONSTANT": 75, "UNCLASSIFIED": 20, "NOT_ENOUGH_SAMPLES": 5},
                        total_ips=100,
                    ),
                ),
                (
                    "rt-fi-merged",
                    "strategies",
                    dict(
                        counts={"CONSTANT": 75, "RANDOM": 16, "NOT_ENOUGH_SAMPLES": 9},
                        total_ips=100,
                    ),
                ),
                ("rt-base", "coverage", dict(zmap_ip_count=200, ipid_ip_count=95)),
            )
        ]
        values, errors = phase_values(reports)
        self.assertEqual(errors, [])
        self.assertEqual(values["results.eu.icmp.rt-fi.success-all.count"]["raw"], 95)
        self.assertEqual(values["results.eu.icmp.rt-fi.success-all.pct"]["raw"], 47.5)
        self.assertIsNone(
            phase_values(reports[:2])[0]["results.eu.icmp.rt-fi.success-all.pct"]["raw"]
        )

    def test_vp_means_are_unweighted_unrounded_complete_and_base_only(self):
        reports = []
        for i, site in enumerate(("af", "as", "au", "eu", "na", "sa")):
            reports.append(
                SimpleNamespace(
                    site=site,
                    protocol="tcp",
                    connection="connection",
                    phase="rt-base",
                    kind="strategies",
                    name=site,
                    data=dict(counts={"CONSTANT": i + 1, "UNCLASSIFIED": 19 - i}, total_ips=20),
                )
            )
        values = vp_values(reports)
        key = "results.vp-comparison.tcp.conn.rt-base.constant.mean"
        self.assertAlmostEqual(values[key]["raw"], 17.5)
        self.assertEqual(values[key]["unit"], "percent")
        self.assertIsNone(vp_values(reports[:-1])[key]["raw"])
        for k in values:
            if ".tcp.conn.rt-base." in k:
                self.assertFalse(
                    any("." + s + "." in k for s in ("multi", "random", "not-enough-samples"))
                )

    def test_threshold_boundaries_apply_only_to_vp_ranges_and_keep_numeric_raw(self):
        key = "results.vp-comparison.tcp.conn.rt-base.constant.range.pp.full"
        for n, expected in ((0, "<0.01"), (0.009999, "<0.01"), (0.01, "0.01"), (0.014, "0.01")):
            self.assertEqual(formatted(n, "percentage-points", key), expected)
        self.assertEqual(formatted(0.001, "percentage-points", "results.other.range.pp"), "0.00")
        value = dict(raw=0.0099, unit="percentage-points")
        text = render(
            {
                key: value,
                "results.eu.icmp.rt-fi.os.tp_link.constant.pct": dict(raw=12.345, unit="percent"),
            },
            "fixture",
        )
        self.assertIn("{<0.01}", text)
        self.assertIn("{12.34\\%}", text)
        self.assertEqual(value["raw"], 0.0099)

    def test_requested_os_recipes_are_explicit_and_full_keys_never_use_sample(self):
        metrics = load_metrics()["metrics"]
        recipes = {m["key"]: m for m in metrics}
        self.assertIn("results.eu.tcp.conn.rt-base.os.hpe_comware.single.pct.full", recipes)
        self.assertNotIn("results.{vp}.icmp.rt-fi.os.mikrotik.per-bucket.pct", recipes)
        self.assertIn("results.{vp}.icmp.rt-fi.os.mikrotik_routeros.per-bucket.pct", recipes)
        for m in metrics:
            if any(
                s.get("reader") in ("os-strategy", "os-range", "os-coverage", "adjusted-os")
                for s in m["sources"]
            ):
                self.assertNotIn("sufficient-sample strategy rows", m["definition"])
        reader = AdjustedOSReader([], "test", None)
        self.assertEqual(
            reader.read(
                dict(
                    full=True,
                    protocol="tcp",
                    connection="connection",
                    phase="rt-base",
                    group="hpe-comware",
                    strategy="SINGLE",
                    stat="share-pct",
                ),
                "eu",
            ),
            (None, ()),
        )

    def test_adjusted_os_values_reuse_prepared_exact_split_without_raw_queries(self):
        from test_paper_os_adjusted import fixture

        from ipid_analysis.os_adjusted_data import prepare

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, _ = fixture(root / "original")
            with patch.dict(
                os.environ,
                dict(
                    IPID_ANALYSIS_STORAGE_MODE="local",
                    IPID_ANALYSIS_SCRATCH_STORAGE="disk",
                    IPID_ANALYSIS_TMP_DIR=str(root / "scratch"),
                ),
            ):
                prepare(source, group_root=root / "tags")
            artifact = SimpleNamespace(
                site="eu",
                protocol="icmp",
                connection="no-connection",
                phase="rt-fi-merged",
                kind="operating-system-group-by-strategy",
                name=str(source),
            )
            reader = AdjustedOSReader([artifact], "test", None)
            spec = dict(
                protocol="icmp",
                connection="no-connection",
                phase="rt-fi-merged",
                group="hpe-comware",
                strategy="SINGLE",
                stat="share-pct",
            )
            before = {p: digest(p) for p in root.rglob("*") if p.is_file()}
            with patch("duckdb.connect", side_effect=AssertionError("no per-IP queries")):
                self.assertEqual(reader.read(spec, "eu")[0], 100)
                self.assertEqual(
                    reader.read(
                        {**spec, "group": "mikrotik-routeros", "strategy": "CONSTANT"}, "eu"
                    )[0],
                    50,
                )
                self.assertIsNone(reader.read({**spec, "group": "missing"}, "eu")[0])
            self.assertEqual(before, {p: digest(p) for p in before})

    def test_full_os_scalars_are_inventory_bound_and_never_fall_back_to_sample(self):
        from test_paper_os_adjusted import fixture
        from tests.test_paper_values_followups import CAMPAIGN as full_campaign
        from tests.test_paper_values_followups import FullFollowupTest

        from ipid_analysis.os_adjusted_data import prepare

        f = FullFollowupTest()
        f.setUp()
        self.addCleanup(f.doCleanups)
        f.fixture()
        source, _ = fixture(f.root / "raw-os-fixture", connection=True)
        done_path = f.analysis / "eu/done__full.json"
        done = json.loads(done_path.read_text())
        report = json.loads(source.read_text())
        zid = f.spec["zmap_ids"]["eu"]
        report.update(
            zmap_id=zid,
            measurements=dict(rt_based_base=done["full_measurement"]["measurement_id"]),
        )
        relative = f"full/figures/{zid}/connection/rt-based-base/c-rt-b_operating-system-group-by-strategy__full.json"
        published = f.analysis / "eu" / relative
        f.write(published, report)
        done["artifacts"].append(
            dict(
                path="/mnt/data01/ipid-analysis/reports/"
                + published.relative_to(f.root / "reports").as_posix(),
                size_bytes=published.stat().st_size,
                sha256=hashlib.sha256(published.read_bytes()).hexdigest(),
            )
        )
        f.write(done_path, done)
        global_path = f.analysis / "done__full.json"
        complete = json.loads(global_path.read_text())
        complete["sites"] = [done if s["site"] == "eu" else s for s in complete["sites"]]
        f.write(global_path, complete)
        with patch.dict(
            os.environ,
            dict(
                IPID_ANALYSIS_STORAGE_MODE="local",
                IPID_ANALYSIS_SCRATCH_STORAGE="disk",
                IPID_ANALYSIS_TMP_DIR=str(f.root / "scratch"),
            ),
        ):
            prepare(published, group_root=f.root / "tags")
        spec = dict(
            protocol="tcp",
            connection="connection",
            phase="rt-base",
            full=True,
            group="hpe-comware",
            strategy="SINGLE",
            stat="share-pct",
        )
        with patch("duckdb.connect", side_effect=AssertionError("cache only")):
            reader = AdjustedOSReader([], full_campaign, f.reports)
            self.assertEqual(reader.read(spec, "eu")[0], 100)
            self.assertIn(str(published), reader.read(spec, "eu")[1])
        # A site alone must not leak values before global Full publication.
        global_path.unlink()
        self.assertIsNone(AdjustedOSReader([], full_campaign, f.reports).read(spec, "eu")[0])
        f.write(global_path, complete)
        # A forged original report cannot be used even if the prepared cache exists.
        report["measurements"]["rt_based_base"] = "another-full-run"
        f.write(published, report)
        with self.assertRaises(ValueError):
            AdjustedOSReader([], full_campaign, f.reports).read(spec, "eu")


class OutlierPreparationTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.f = Fixture(self.root)
        self.env = patch.dict(
            os.environ,
            dict(
                IPID_ANALYSIS_STORAGE_MODE="local",
                IPID_ANALYSIS_SCRATCH_STORAGE="disk",
                IPID_ANALYSIS_TMP_DIR=str(self.root / "scratch"),
            ),
        )
        self.env.start()
        self.addCleanup(self.env.stop)
        self.f.strategy({"UNCLASSIFIED": 6, "CONSTANT": 2}, protocol="icmp")
        self.f.strategy({"SINGLE": 7, "RANDOM": 1}, protocol="icmp", phase="fi-mass")
        self.raw = self.f.raw / "eu/ipid/icmp_rt_base/ipid.pq"
        self.raw.parent.mkdir(parents=True)
        self.raw.with_name("ipid.snapshot.yaml").write_text(
            json.dumps(
                dict(connection_count=4, requests_per_connection=4, request_ip_ids=list(range(16)))
            )
        )
        addresses = [f"192.0.2.{i + 1}" for i in range(8)]
        seq = []
        for score in range(6):
            s = list(range(100, 116))
            s[1 : 1 + score] = [30000] * score
            seq.append(",".join(map(str, s)))
        # Outside the cohort: invalid sequence must never be scored or parsed in Python.
        pq.write_table(
            pa.table(
                dict(
                    IP_ADDR=addresses,
                    IPID_SEQUENCE=seq + ["outside-cohort", "excluded-other-rt-label"],
                )
            ),
            self.raw,
            row_group_size=2,
        )
        for phase, codes, stem in (
            ("rt-base", ["UNCLASSIFIED"] * 6 + ["CONSTANT"] * 2, "n-rt-b"),
            ("fi-mass", ["SINGLE"] * 7 + ["RANDOM"], "n-fi-m"),
        ):
            folder = "rt-based-base" if phase == "rt-base" else "fixed-interval-mass"
            path = (
                self.f.processed
                / "eu"
                / RUNS["icmp"]
                / "no-connection"
                / folder
                / (stem + "_strategies.pq")
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            table = pa.table(
                dict(IP_ADDR=addresses, IPID_SELECTION_STRATEGY=codes)
            ).replace_schema_metadata({b"classifier_version": CLASSIFIER_VERSION.encode()})
            pq.write_table(table, path)
        inputs = Inputs(
            (self.f.reports, self.f.processed),
            contexts={
                ("icmp", "no-connection", phase, "strategies") for phase in ("rt-base", "fi-mass")
            },
        )
        try:
            self.artifacts, conflicts = select_artifacts(list(inputs.artifacts(CAMPAIGN)))
            self.assertEqual(conflicts, [])
        finally:
            inputs.close()

    def test_dry_run_is_footer_only_and_never_writes_or_selects_rows(self):
        before = {p: digest(p) for p in self.root.rglob("*") if p.is_file()}
        with patch("paper_values.paired.connection", side_effect=AssertionError("no queries")):
            with redirect_stdout(io.StringIO()) as output:
                self.assertIsNone(
                    outliers.prepare(
                        CAMPAIGN, self.artifacts, self.f.processed, self.f.raw, dry_run=True
                    )
                )
        self.assertIn("projected_compressed_bytes", output.getvalue())
        self.assertIn('"fi_candidate_count": 7', output.getvalue())
        self.assertEqual(before, {p: digest(p) for p in before})
        self.assertFalse(outliers.cache_path(self.f.processed).exists())

    def test_exact_cohort_full_histogram_cached_export_and_no_source_changes(self):
        before = {p: digest(p) for p in self.root.rglob("*") if p.is_file()}
        with redirect_stdout(io.StringIO()):
            destination = outliers.prepare(CAMPAIGN, self.artifacts, self.f.processed, self.f.raw)
        doc = json.loads(destination.read_text())
        self.assertEqual(doc["fi_candidate_count"], 7)
        self.assertEqual(doc["cohort_count"], 6)
        self.assertEqual(doc["rt_not_unclassified"], 1)
        self.assertEqual(doc["histogram"], {str(i): 1 for i in range(6)})
        self.assertEqual(doc["zero_score_ips"], 1)
        self.assertEqual(before, {p: digest(p) for p in before})
        saved = digest(destination)
        with (
            patch.object(outliers, "calculate", side_effect=AssertionError("cached")),
            patch("duckdb.connect", side_effect=AssertionError("no queries")),
        ):
            values = outliers.read_prepared(self.f.processed, CAMPAIGN, self.artifacts)
            with redirect_stdout(io.StringIO()):
                outliers.prepare(CAMPAIGN, self.artifacts, self.f.processed, self.f.raw)
        self.assertEqual(len(values), 6)
        self.assertAlmostEqual(sum(v["raw"] for v in values.values()), 100)
        self.assertAlmostEqual(values[outliers.PREFIX + ".2.pct"]["raw"], 100 / 6)
        self.assertEqual(digest(destination), saved)
        # Altering a source invalidates preparation without replacing it.
        os.utime(
            self.raw, ns=(self.raw.stat().st_atime_ns, self.raw.stat().st_mtime_ns + 1000000000)
        )
        with self.assertRaisesRegex(ValueError, "stale"):
            outliers.read_prepared(self.f.processed, CAMPAIGN, self.artifacts)
        self.assertEqual(digest(destination), saved)

    def test_missing_selected_raw_row_refuses_publication(self):
        data = pq.read_table(self.raw)
        pq.write_table(data.slice(1), self.raw)
        # Reaching the raw join directly isolates missing-IP protection from the footer count guard.
        work = outliers.plan(self.artifacts, self.f.processed, self.f.raw)
        with (
            self.assertRaisesRegex(ValueError, "missing RT raw sequences"),
            redirect_stdout(io.StringIO()),
        ):
            outliers.calculate(work)
        self.assertFalse(outliers.cache_path(self.f.processed).exists())

    def test_scores_greater_than_five_stay_in_denominator_and_export_is_cache_only(self):
        table = pq.read_table(self.raw)
        rows = table.to_pylist()
        rows[5]["IPID_SEQUENCE"] = ",".join(map(str, [100] + [30000] * 6 + list(range(107, 116))))
        pq.write_table(pa.Table.from_pylist(rows), self.raw)
        with redirect_stdout(io.StringIO()):
            outliers.prepare(CAMPAIGN, self.artifacts, self.f.processed, self.f.raw)
        with patch("duckdb.connect", side_effect=AssertionError("export is cache-only")):
            result = self.f.run(strict=True)
        values = result["values"]
        self.assertAlmostEqual(
            sum(values[f"{outliers.PREFIX}.{i}.pct"]["raw"] for i in range(6)), 500 / 6
        )
        self.assertEqual(values[f"{outliers.PREFIX}.5.pct"]["raw"], 0)
        self.assertNotIn("results.eu.tcp.conn.rt-base.distribution.multi.pct", values)
        raw = json.loads((self.root / "tmp/values.json").read_text())["paper_values"]["values"]
        self.assertEqual(raw[f"{outliers.PREFIX}.1.pct"]["raw"], 100 / 6)

    def test_numerical_identity_ignores_export_labels_and_comments_but_not_calculation(self):
        root = self.root / "identity"
        repo = Path(outliers.__file__).resolve().parents[1]
        for name in (
            "paper_values/outliers.py",
            "ipid_analysis/strategies.py",
            "ipid_analysis/deterministic_transition_analysis.py",
        ):
            target = root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((repo / name).read_bytes())
        source = root / "paper_values/outliers.py"
        with patch.object(outliers, "__file__", str(source)):
            before = outliers.code_identity()
            source.write_text(
                "# Presentation change only\n"
                + source.read_text().replace(
                    "results.eu.icmp.fi-mass.all.single-per-destination-per-bucket.rt-outlier-score",
                    "renamed-display-key",
                )
            )
            self.assertEqual(outliers.code_identity(), before)
            source.write_text(
                source.read_text().replace(
                    "r.rt_strategy='UNCLASSIFIED'", "r.rt_strategy='CONSTANT'"
                )
            )
            self.assertNotEqual(outliers.code_identity(), before)

    def test_duplicate_or_incomplete_selected_raw_sequence_refuses_publication(self):
        original = pq.read_table(self.raw)
        rows = original.to_pylist()
        rows[0]["IPID_SEQUENCE"] = "1,2,3"
        pq.write_table(pa.Table.from_pylist(rows), self.raw)
        with (
            self.assertRaisesRegex(ValueError, "missing/extra sample"),
            redirect_stdout(io.StringIO()),
        ):
            outliers.calculate(outliers.plan(self.artifacts, self.f.processed, self.f.raw))
        rows = original.to_pylist()
        rows.append(rows[0])
        pq.write_table(pa.Table.from_pylist(rows), self.raw)
        with (
            self.assertRaisesRegex(ValueError, "duplicate RT raw"),
            redirect_stdout(io.StringIO()),
        ):
            outliers.calculate(outliers.plan(self.artifacts, self.f.processed, self.f.raw))
        self.assertFalse(outliers.cache_path(self.f.processed).exists())


if __name__ == "__main__":
    unittest.main()
