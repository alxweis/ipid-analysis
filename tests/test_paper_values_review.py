"""Exact revision populations, selective details, cached raw scans and PDF wiring."""

from contextlib import redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis import paper_ready
from paper_values.exporter import export
from paper_values.figure_zip import bundle
from paper_values.inputs import SITES, Artifact
from paper_values.review import (
    COUNTERS,
    STRATEGIES,
    coverage_values,
    figure_destination,
    interprotocol_coverage,
    interprotocol_values,
    load,
    tcp_defaults,
    vp_values,
)
from paper_values.tcp_review import prepare, read_prepared

CAMPAIGN = "test-review"
ZMAP = "tcp-80_2026-11-07_03-30-08"


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))
    return path


def parquet(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)
    return path


def artifact(
    site="eu",
    protocol="tcp",
    mode="no-connection",
    phase="rt-fi-merged",
    kind="strategies",
    data=None,
):
    return Artifact(
        "report-" + site + protocol + mode + phase + kind,
        data or {},
        "",
        0,
        "",
        site,
        protocol,
        mode,
        phase,
        kind,
    )


def summary(root):
    directory = (
        root
        / "data/processed/continental"
        / CAMPAIGN
        / "eu/interprotocol/interprotocol/runs/interprotocol"
    )
    group = "icmp-tcp-udp"
    counts = {
        "SINGLE:PROTOCOL_SHARED": 2,
        "SINGLE:PARTIAL_PROTOCOL_SHARED": 3,
        "SINGLE:STRATEGY_CHANGED": 100,
        "SINGLE:NOT_ENOUGH_SAMPLES": 1,
        "MULTI:SAME_INTER_STRATEGY": 2,
        "MULTI:DIFFERENT_INTER_STRATEGY": 3,
        "MULTI:STRATEGY_CHANGED": 1,
        "MULTI:NOT_ENOUGH_SAMPLES": 4,
        "CONSTANT:DIFFERENT_PARAMETERS": 2,
    }
    data = dict(
        version=2,
        rows=sum(counts.values()),
        groups={group: dict(counts=counts, rows=sum(counts.values()))},
        partial_relationships=[
            dict(
                baseline="SINGLE",
                protocol_group=group,
                outcome="PARTIAL_PROTOCOL_SHARED",
                shared_pairs=["icmp-tcp", "icmp-udp"],
                compatible_partitions=["icmp-tcp", "icmp-udp"],
                count=3,
            )
        ],
    )
    path = write(directory / "interprotocol-campaign-summary.json", data)
    for strategy in COUNTERS:
        entries = (
            []
            if strategy != "SINGLE"
            else [
                dict(
                    protocol_group=group,
                    observed=dict(icmp="SINGLE", tcp="PER_BUCKET", udp="SINGLE"),
                    count=1,
                    state_counts=[2, 0, 1],
                ),
                dict(
                    protocol_group=group,
                    observed=dict(icmp="SINGLE", tcp="PER_BUCKET", udp="SINGLE"),
                    count=1,
                    state_counts=[0, 2, 1],
                ),
                dict(
                    protocol_group=group,
                    observed=dict(icmp="PER_BUCKET", tcp="SINGLE", udp="SINGLE"),
                    count=1,
                ),
                dict(
                    protocol_group=group,
                    observed=dict(icmp="UNCLASSIFIED", tcp="UNCLASSIFIED", udp="UNCLASSIFIED"),
                    count=97,
                ),
            ]
        )
        write(
            directory
            / (
                "interprotocol-campaign-"
                + strategy.lower().replace("_", "-")
                + "-alternatives.json"
            ),
            dict(baseline_strategy=strategy, alternatives=entries),
        )
    return path


class RevisionValuesTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        env = patch.dict(
            os.environ,
            {
                "IPID_ANALYSIS_STORAGE_MODE": "local",
                "IPID_ANALYSIS_SCRATCH_STORAGE": "disk",
                "IPID_ANALYSIS_TMP_DIR": str(self.root / "scratch"),
                "IPID_ANALYSIS_DUCKDB_MEMORY": "512MB",
                "IPID_ANALYSIS_THREADS": "1",
                "MPLCONFIGDIR": str(self.root / "mpl"),
            },
        )
        env.start()
        self.addCleanup(env.stop)

    def test_vp_ranges_use_all_six_unrounded_populations_and_keep_missing_distinct(self):
        reports = [
            artifact(
                site=site,
                data=dict(counts={"CONSTANT": i + 1, "UNCLASSIFIED": 99 - i}, total_ips=100),
            )
            for i, site in enumerate(SITES)
        ]
        values = vp_values(reports)
        prefix = "results.vp-comparison.tcp.no-conn.rt-fi.constant."
        self.assertEqual(values[prefix + "min.pct"]["raw"], 1)
        self.assertEqual(values[prefix + "max.pct"]["raw"], 6)
        self.assertEqual(values[prefix + "range.pp"]["raw"], 5)
        self.assertEqual(
            values["results.eu.tcp.no-conn.rt-fi.distribution.unclassified.pct"]["raw"], 96
        )
        self.assertIsNone(vp_values(reports[:-1])[prefix + "max.pct"]["raw"])

    def test_compact_phase_keys_use_canonical_reports_and_phase_specific_denominators(self):
        discovery = artifact(phase="rt-base", kind="coverage", data=dict(zmap_ip_count=1000))
        rt = artifact(
            phase="rt-base",
            data=dict(counts={"CONSTANT": 100, "UNCLASSIFIED": 100}, total_ips=200),
        )
        report = artifact(
            phase="fi-mass",
            data=dict(
                measurement_id="tcp-80_repeat",
                counts={"CONSTANT": 3, "UNCLASSIFIED": 2, "NOT_ENOUGH_SAMPLES": 5},
                total_ips=10,
            ),
        )
        report.relative = (
            "figures/tcp-80_repeat/no-connection/fixed-interval-mass/n-fi-m_strategies.json"
        )
        coverage = artifact(phase="fi-mass", kind="coverage", data=dict(zmap_ip_count=20))
        # Production regression: coverage is in processed, strategy in figures.
        coverage.relative = report.relative.replace("figures/", "").replace(
            "strategies", "coverage"
        )
        canonical = [discovery, rt, report, coverage]
        values = coverage_values(canonical)
        prefix = "results.eu.tcp.no-conn.fi-mass."
        self.assertEqual(values["results.eu.tcp.zmap.count"]["raw"], 1000)
        self.assertEqual(values[prefix + "targets.count"]["raw"], 20)
        self.assertEqual(values[prefix + "success.count"]["raw"], 5)
        self.assertEqual(values[prefix + "success.pct"]["raw"], 25)
        self.assertEqual(values[prefix + "targets.pct"]["raw"], 10)
        self.assertEqual(values["results.eu.tcp.no-conn.rt-base.targets.pct"]["raw"], 100)
        self.assertFalse(any(".measurements." in k or "repeat" in k for k in values))

    def test_changed_cutoff_is_strict_after_pooling_and_partial_is_not_double_counted(self):
        values = interprotocol_values(summary(self.root))
        prefix = "results.eu.interprotocol.single.icmp-tcp-udp."
        changed = {k: v["raw"] for k, v in values.items() if k.startswith(prefix + "changed.")}
        self.assertEqual(sorted(changed.values()), [2, 97])
        self.assertIn(
            prefix + "changed.icmp-unclassified.tcp-unclassified.udp-unclassified.pct", changed
        )
        self.assertFalse(any("whole" in k for k in changed))
        partial = [v["raw"] for k, v in values.items() if k.startswith(prefix + "partial.")]
        self.assertEqual(partial, [100])
        self.assertEqual(
            values["results.eu.interprotocol.multi.icmp-tcp-udp.consistent.pct"]["raw"], 50
        )
        self.assertEqual(
            values["results.eu.interprotocol.constant.icmp-tcp-udp.consistent.pct"]["raw"], 100
        )

    def tcp_fixture(self):
        processed = self.root / "data/processed/continental" / CAMPAIGN
        reports = self.root / "reports/continental" / CAMPAIGN
        raw = self.root / "data/raw/continental" / CAMPAIGN
        fixtures = []
        for mode, phase, stem, mid, labels in (
            (
                "no-connection",
                "rt-based-base",
                "n-rt-b",
                "tcp-80_base",
                [("a", "CONSTANT"), ("b", "CONSTANT"), ("c", "UNCLASSIFIED")],
            ),
            ("no-connection", "fixed-interval-mass", "n-fi-m", "tcp-80_mass", [("c", "CONSTANT")]),
            (
                "connection",
                "rt-based-base",
                "c-rt-b",
                "tcp-80_conn",
                [("d", "CONSTANT"), ("e", "PER_CONNECTION"), ("f", "PER_CONNECTION")],
            ),
        ):
            counts = {name: sum(label == name for _, label in labels) for name in STRATEGIES}
            folder = Path(ZMAP) / mode / phase
            data = dict(
                protocol="tcp",
                connection_mode=mode,
                interval="rt-based" if "rt-" in stem else "fixed-interval",
                scale="mass" if "mass" in phase else "base",
                zmap_id=ZMAP,
                measurement_id=mid,
                counts=counts,
                total_ips=len(labels),
            )
            write(reports / "eu/figures" / folder / (stem + "_strategies.json"), data)
            labels_path = parquet(
                processed / "eu" / folder / (stem + "_strategies.pq"),
                [dict(IP_ADDR=ip, IPID_SELECTION_STRATEGY=label) for ip, label in labels],
            )
            directory = raw / "eu/ipid" / mid
            directory.mkdir(parents=True)
            (directory / "ipid.snapshot.yaml").write_text(
                "connection_count: 4\nrequests_per_connection: 4\ntcp:\n  establish_connection: "
                + ("true\n" if mode == "connection" else "false\n")
            )
            fixtures.append(labels_path)
        folder = Path(ZMAP) / "no-connection/merged/rt-based-base_fixed-interval-mass"
        write(
            reports / "eu/figures" / folder / "n-rt-b_fi-m_strategies.json",
            dict(
                protocol="tcp",
                connection_mode="no-connection",
                zmap_id=ZMAP,
                base_measurement_id="tcp-80_base",
                mass_measurement_id="tcp-80_mass",
                counts={"CONSTANT": 3},
                total_ips=3,
            ),
        )
        parquet(
            processed / "eu" / folder / "n-rt-b_fi-m_strategies.pq",
            [dict(IP_ADDR=ip, IPID_SELECTION_STRATEGY="CONSTANT") for ip in "abc"],
        )

        def row(ip, ids, df):
            return dict(
                IP_ADDR=ip,
                IPID_SEQUENCE=",".join(map(str, ids)),
                DF_SEQUENCE=",".join(map(str, df)),
            )

        parquet(
            raw / "eu/ipid/tcp-80_base/ipid.pq",
            [
                row("a", [9] * 4 + [0] * 12, [0] * 4 + [1] * 12),
                row("b", [9] * 4 + [7] * 12, [1] * 4 + [0, 1] * 6),
                row("c", list(range(16)), [0] * 16),
            ],
        )
        parquet(
            raw / "eu/ipid/tcp-80_mass/ipid.pq",
            [row("c", [5, "-"] + [5] * 14, [0, "-"] + [0] * 14)],
        )
        # Connection d is Constant after skipping the handshake, despite its unequal first four IPIDs.
        # e/f are both PER_CONNECTION labels; only e has four equal handshake values.
        parquet(
            raw / "eu/ipid/tcp-80_conn/ipid.pq",
            [
                row("d", [1, 2, 3, 4] + [0] * 12, [0] * 16),
                row("e", [0] * 4 + list(range(12)), [1] * 16),
                row("f", [1, 2, 3, 4] + list(range(12)), [1] * 16),
            ],
        )
        return reports, processed, raw

    def test_prepare_cache_readonly_export_and_distinct_handshake_denominator(self):
        reports, processed, raw = self.tcp_fixture()
        before = {
            p: hashlib.sha256(p.read_bytes()).hexdigest()
            for root in (reports, processed, raw)
            for p in root.rglob("*")
            if p.is_file()
        }
        with redirect_stdout(io.StringIO()):
            path = prepare(CAMPAIGN, reports, processed, raw)
        values = read_prepared(processed, CAMPAIGN)
        self.assertEqual(set(values), set(tcp_defaults()))
        self.assertAlmostEqual(
            values["results.eu.tcp.no-conn.rt-fi.constant.df1.pct"]["raw"], 100 / 3
        )
        self.assertAlmostEqual(
            values["results.eu.tcp.no-conn.rt-fi.constant.df0.pct"]["raw"], 100 / 3
        )
        self.assertEqual(
            values["results.eu.tcp.conn.rt-base.per-connection.handshake.constant.pct"]["raw"], 50
        )
        self.assertEqual(values["results.eu.tcp.conn.rt-base.constant.ipid-zero.pct"]["raw"], 100)
        with (
            patch("paper_values.tcp_review.calculate", side_effect=AssertionError("bulk reread")),
            redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(prepare(CAMPAIGN, reports, processed, raw), path)
        with patch(
            "paper_values.tcp_review.connection", side_effect=AssertionError("export raw read")
        ):
            found, errors = load([], processed, reports, CAMPAIGN)
        self.assertFalse(errors)
        self.assertEqual(
            found["results.eu.tcp.conn.rt-base.per-connection.handshake.constant.pct"]["raw"], 50
        )
        self.assertTrue(
            all(
                hashlib.sha256(p.read_bytes()).hexdigest() == digest
                for p, digest in before.items()
            )
        )
        # Stale inputs are rejected, rather than silently keeping old percentages.
        raw_file = raw / "eu/ipid/tcp-80_conn/ipid.pq"
        raw_file.touch()
        with self.assertRaisesRegex(ValueError, "stale"):
            read_prepared(processed, CAMPAIGN)

    def test_interprotocol_group_coverage_combines_disjoint_base_mass_populations(self):
        path = summary(self.root)
        data = json.loads(path.read_text())
        data["measurements"] = {
            "icmp-tcp-udp": {"base": "interprotocol_base", "mass": "interprotocol_mass"}
        }
        write(path, data)
        canonical = [
            artifact(protocol=p, phase="rt-base", kind="coverage", data=dict(zmap_ip_count=n))
            for p, n in (("icmp", 1000), ("tcp", 2000), ("udp", 3000))
        ]
        processed = self.root / "data/processed/continental" / CAMPAIGN
        write(
            path.parents[2] / "target-summary.json",
            dict(
                pair_semantics="pair intersection minus same-strategy triple",
                targets={"icmp-tcp-udp": {"SINGLE": 190, "MULTI": 10}},
            ),
        )
        values = interprotocol_coverage(processed, canonical)
        prefix = "results.eu.interprotocol.icmp-tcp-udp."
        self.assertEqual(values[prefix + "targets.count"]["raw"], 118)
        self.assertEqual(values[prefix + "success.count"]["raw"], 113)
        self.assertAlmostEqual(values[prefix + "success.pct"]["raw"], 100 * 113 / 118)
        self.assertEqual(values[prefix + "targets.pct"]["raw"], 59)
        self.assertFalse(any(".measurements." in k or "interprotocol-base" in k for k in values))

    def test_local_reference_coverage_counts_unclassified_and_caches_without_raw_reads(self):
        from paper_values.reference_review import prepare as prepare_references
        from paper_values.reference_review import read_prepared as read_references

        processed = self.root / "data/processed/continental" / CAMPAIGN
        path = processed / "eu/interprotocol/local-references/reference-tcp-base/strategies.pq"
        parquet(
            path,
            [
                dict(IP_ADDR="a", LOCAL_STRATEGY="UNCLASSIFIED"),
                dict(IP_ADDR="b", LOCAL_STRATEGY="CONSTANT"),
                dict(IP_ADDR="c", LOCAL_STRATEGY="NOT_ENOUGH_SAMPLES"),
            ],
        )
        before = path.read_bytes()
        with redirect_stdout(io.StringIO()):
            prepare_references(CAMPAIGN, processed, [])
        prefix = "results.eu.reference.tcp.base."
        values = read_references(processed, CAMPAIGN)
        self.assertEqual(values[prefix + "targets.count"]["raw"], 3)
        self.assertEqual(values[prefix + "success.count"]["raw"], 2)
        self.assertAlmostEqual(values[prefix + "success.pct"]["raw"], 200 / 3)
        self.assertIsNone(values[prefix + "targets.pct"]["raw"])
        self.assertFalse(any(".measurements." in k or "zmap-total" in k for k in values))
        with (
            patch(
                "paper_values.reference_review.connection",
                side_effect=AssertionError("unexpected table rescan"),
            ),
            redirect_stdout(io.StringIO()),
        ):
            prepare_references(CAMPAIGN, processed, [])
            read_references(processed, CAMPAIGN)
            write(
                processed / "eu/interprotocol/interprotocol/target-summary.json",
                dict(
                    pair_semantics="pair intersection minus same-strategy triple",
                    targets={"icmp-tcp": {"SINGLE": 2}, "tcp-udp": {"CONSTANT": 3, "MULTI": 7}},
                ),
            )
            values = read_references(processed, CAMPAIGN)
            self.assertEqual(values[prefix + "targets.pct"]["raw"], 60)
        self.assertEqual(before, path.read_bytes())

    def test_legacy_df_is_unknown_and_does_not_become_df_zero(self):
        reports, processed, raw = self.tcp_fixture()
        path = raw / "eu/ipid/tcp-80_base/ipid.pq"
        data = pq.read_table(path).drop(["DF_SEQUENCE"])
        pq.write_table(data, path)
        with redirect_stdout(io.StringIO()):
            prepare(CAMPAIGN, reports, processed, raw)
        values = read_prepared(processed, CAMPAIGN)
        prefix = "results.eu.tcp.no-conn.rt-fi.constant."
        self.assertIsNone(values[prefix + "df0.pct"]["raw"])
        self.assertIsNone(values[prefix + "df1.pct"]["raw"])
        self.assertAlmostEqual(values[prefix + "ipid-zero.pct"]["raw"], 100 / 3)

    def test_upset_discovery_export_zip_and_existing_pdf_preservation(self):
        path = summary(self.root)
        reports = self.root / "reports/continental" / CAMPAIGN
        processed = self.root / "data/processed/continental" / CAMPAIGN
        reports.mkdir(parents=True)
        jobs = paper_ready.discover(self.root / "reports", CAMPAIGN)
        self.assertEqual(len(jobs), 4)
        self.assertTrue(all(job.kind.startswith("interprotocol-upset-") for job in jobs))
        self.assertTrue(all(r.status == "would-create" for r in paper_ready.run(jobs, dry_run=True)))
        self.assertFalse(any(job.destination.exists() for job in jobs))
        self.assertFalse(paper_ready.discover(self.root / "reports", CAMPAIGN, sites=("na",)))
        self.assertFalse(paper_ready.discover(self.root / "reports", CAMPAIGN, protocols=("tcp",)))
        for job in jobs:
            job.destination.parent.mkdir(parents=True, exist_ok=True)
            job.destination.write_bytes(b"%PDF-1.4\nfixture")
        self.assertTrue(all(r.status == "skipped-existing" for r in paper_ready.run(jobs)))
        result = export(
            CAMPAIGN,
            reports_root=reports,
            processed_root=processed,
            repository_root=self.root,
            strict=True,
        )
        self.assertEqual(
            result["values"]["fig.eu.interprotocol.single.upset"]["raw"],
            "reports/continental/"
            + CAMPAIGN
            + "/"
            + figure_destination(path, processed, reports, "SINGLE")
            .relative_to(reports)
            .as_posix(),
        )
        archive = bundle(repository_root=self.root)
        self.assertEqual(archive["pdfs"], 4)
        self.assertEqual(
            result["values"]["fig.eu.interprotocol.combined.upset"]["raw"],
            "reports/continental/"
            + CAMPAIGN
            + "/"
            + figure_destination(path, processed, reports, "combined")
            .relative_to(reports)
            .as_posix(),
        )
        self.assertTrue(all(job.destination.exists() for job in jobs))

    def test_real_upset_worker_embeds_paper_font_and_preserves_summary(self):
        from ipid_analysis.paper_plot_style import _font

        _font.cache_clear()
        self.addCleanup(_font.cache_clear)
        path = summary(self.root)
        before = path.read_bytes()
        (self.root / "reports/continental" / CAMPAIGN).mkdir(parents=True)
        jobs = paper_ready.discover(self.root / "reports", CAMPAIGN)
        for job in jobs:
            result = paper_ready.worker(job)
            self.assertEqual(result.status, "created", result.error)
            self.assertTrue(job.destination.read_bytes().startswith(b"%PDF-"))
            content = job.destination.read_bytes()
            self.assertIn(b"Libertine", content)
            width = b"504" if job.kind == "interprotocol-upset-combined" else b"239.76"
            self.assertIn(width, content)  # Exact 7in combined / 3.33in single-panel width.
            stamp = hashlib.sha256(content).hexdigest()
            self.assertEqual(paper_ready.worker(job).status, "skipped-existing")
            self.assertEqual(hashlib.sha256(job.destination.read_bytes()).hexdigest(), stamp)
        self.assertEqual(path.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
