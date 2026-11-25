"""Exact tag splits, immutable caches, adjusted populations and figure export."""

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

import pyarrow as pa
import pyarrow.parquet as pq
from test_paper_values import Fixture
import test_paper_values_followups as full_tests

from ipid_analysis import os_adjusted_data as prepared
from ipid_analysis import paper_os_adjusted as adjusted
from ipid_analysis import paper_ready
from ipid_analysis.paper_os_taxonomy import SPLITS, TAGS
from ipid_analysis.plot_os_group_strategy import (
    RAW_OS_COLUMNS,
    TAG_TO_GROUP,
    aggregate_os_groups,
    write_os_groups,
)
from paper_values.figure_zip import bundle


def stamp(path):
    return path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest()


def fixture(root, *, connection=False):
    root.mkdir(parents=True, exist_ok=True)
    data = root / "data/icmp_fixture/no-connection/merged/rt-based-base_fixed-interval-mass"
    if connection:
        data = root / "data/tcp-80_fixture/connection/rt-based-base"
    data.mkdir(parents=True)
    prefix = "c-rt-b_" if connection else "n-rt-b_fi-m_"
    raw = root / "os.pq"
    ips = [f"192.0.2.{i}" for i in range(1, 11)]
    tags = [
        "mikrotik-routeros",
        "mikrotik-routeros",
        "mikrotik-swos",
        "hpe-comware",
        "hpe-procurve",
        "hpe-procurve",
        "ubuntu",
        None,
        "mikrotik-routeros",
        "mikrotik",
    ]
    raw_data = {
        "IP_ADDR": ips,
        "OS_STATUS": ["resolved" if t else "unclassified" for t in tags],
        "OS_TAG": tags,
    }
    for column in RAW_OS_COLUMNS[3:]:
        raw_data[column] = [None] * 10
    pq.write_table(pa.table(raw_data), raw)
    classifications = [
        "CONSTANT",
        "SINGLE",
        "CONSTANT",
        "SINGLE",
        "CONSTANT",
        "SINGLE" if connection else "RANDOM",
        "CONSTANT",
        "UNCLASSIFIED",
        "CONSTANT",
    ]
    labels = data / (prefix + "strategies.pq")
    pq.write_table(
        pa.table({"IP_ADDR": ips[:8] + [ips[9]], "IPID_SELECTION_STRATEGY": classifications}),
        labels,
    )
    groups = root / "original-os-groups.pq"
    write_os_groups(raw, groups)
    aggregate = data / (prefix + prepared.KIND + ".pq")
    stats = aggregate_os_groups(labels, groups, aggregate)
    proto = "tcp" if connection else "icmp"
    target = (
        "tcp.ipid.connection.rt-based.base"
        if connection
        else "icmp.ipid.no-connection.rt-based.base+icmp.ipid.no-connection.fixed-interval.mass"
    )
    identity = dict(
        protocol=proto,
        target=target,
        zmap_id="tcp-80_fixture" if connection else "icmp_fixture",
        os_measurement_id="shared-os",
    )
    report = dict(
        identity,
        connection_mode="connection" if connection else "no-connection",
        figure=prepared.KIND,
        aggregate=str(aggregate),
        **stats,
        measurements=dict(
            rt_based_base="base", **({} if connection else dict(fixed_interval_mass="mass"))
        ),
        sources=dict(strategies=str(labels), os=str(raw)),
        processed_os_groups=str(groups),
    )
    source = root / (prefix + prepared.KIND + ".json")
    source.write_text(json.dumps(report))
    role = root / (prefix + "itdk-role-by-os.json")
    old_roles = {
        "Transit-Observed": {"mikrotik": 1, "hpe-network": 1},
        "No Transit Evidence": {"mikrotik": 1, "hpe-network": 1},
    }
    role_report = dict(
        identity,
        connection_mode="connection" if connection else "no-connection",
        os=dict(measurement_id="shared-os"),
        coverage=dict(
            total_measured=9,
            os_resolved=8,
            itdk_os_resolved=4,
            transit_observed_os_resolved=2,
            no_transit_evidence_os_resolved=2,
        ),
        raw_distribution=[
            dict(role=r, os_group=g, count=n) for r, cs in old_roles.items() for g, n in cs.items()
        ],
    )
    if connection:
        role_report["measurement_id"] = "base"
    else:
        role_report.update(base_measurement_id="base", mass_measurement_id="mass")
    role.write_text(json.dumps(role_report))
    joined = data / (prefix + "itdk-os-role-join.pq")
    pq.write_table(
        pa.table(
            dict(
                IP_ADDR=[ips[0], ips[1], ips[3], ips[4]],
                T=[True, False, True, False],
                OS_GROUP=["mikrotik", "mikrotik", "hpe-network", "hpe-network"],
            )
        ),
        joined,
    )
    source.with_suffix(".pdf").write_bytes(b"%PDF-1.4\npreserve original")
    return source, role


class AdjustedOSTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.env = patch.dict(
            os.environ,
            dict(
                IPID_ANALYSIS_STORAGE_MODE="local",
                IPID_ANALYSIS_SCRATCH_STORAGE="disk",
                IPID_ANALYSIS_TMP_DIR=str(self.root / "scratch"),
                MPLCONFIGDIR=str(self.root / "mpl"),
            ),
        )
        self.env.start()
        self.addCleanup(self.env.stop)
        from ipid_analysis.paper_plot_style import _font

        _font.cache_clear()
        self.addCleanup(_font.cache_clear)

    def prepare(self, source, **kwargs):
        return prepared.prepare(source, group_root=self.root / "tag-cache", **kwargs)

    def test_mapping_is_separate_exact_and_keeps_requested_remainders(self):
        self.assertEqual(TAG_TO_GROUP["mikrotik-routeros"], "mikrotik")
        self.assertEqual(TAG_TO_GROUP["hpe-comware"], "hpe-network")
        self.assertEqual(TAGS["mikrotik-routeros"], "mikrotik-routeros")
        self.assertEqual(TAGS["mikrotik-swos"], "mikrotik-swos")
        self.assertEqual(TAGS["mikrotik"], "mikrotik-swos")
        self.assertEqual(TAGS["hpe-comware"], "hpe-comware")
        self.assertEqual(TAGS["hpe-procurve"], "hpe-network")
        for key, group in TAG_TO_GROUP.items():
            if group not in SPLITS:
                self.assertEqual(TAGS[key], group)

    def test_prepare_recomputes_exact_counts_and_final_all_other_without_source_writes(self):
        source, role = fixture(self.root / "original")
        before = {p: stamp(p) for p in source.parent.rglob("*") if p.is_file()}
        self.assertEqual(self.prepare(source, need_roles=True), "prepared")
        self.assertEqual(before, {p: stamp(p) for p in before})
        doc, _, _ = prepared.load(source, need_roles=True)
        counts = {g: r["ip_count"] for g, r in doc["report"]["groups"].items()}
        self.assertEqual(
            counts,
            {
                "mikrotik-routeros": 2,
                "mikrotik-swos": 2,
                "hpe-comware": 1,
                "hpe-network": 2,
                "ubuntu": 1,
            },
        )
        self.assertEqual(doc["report"]["matched_grouped_ip_count"], 8)
        rows = {(r["OS_GROUP"], r["IPID_SELECTION_STRATEGY"]): r for r in doc["rows"]}
        self.assertEqual(rows["mikrotik-routeros", "CONSTANT"]["PERCENTAGE"], 50)
        self.assertEqual(rows["mikrotik-swos", "CONSTANT"]["PERCENTAGE"], 100)
        self.assertEqual(rows["hpe-comware", "SINGLE"]["PERCENTAGE"], 100)
        self.assertEqual(rows["hpe-network", "CONSTANT"]["PERCENTAGE"], 50)
        job = paper_ready.Job(role, adjusted.BAR, output=self.root / "role__adjusted.pdf")
        data, *_ = adjusted.load_data(job)
        self.assertEqual(data.role_totals, {"Transit-Observed": 2, "No Transit Evidence": 6})
        self.assertEqual(data.counts["Transit-Observed"]["mikrotik-routeros"], 1)
        self.assertEqual(data.counts["No Transit Evidence"]["mikrotik-swos"], 2)
        self.assertEqual(data.counts["No Transit Evidence"]["hpe-network"], 2)
        with (
            patch.object(prepared, "new_groups", side_effect=AssertionError("no raw scan")),
            patch.object(prepared, "aggregate_os_groups", side_effect=AssertionError("cached")),
        ):
            self.assertEqual(self.prepare(source, need_roles=True), "cached")

    def test_render_only_reads_compact_caches_and_keeps_existing_outputs(self):
        source, role = fixture(self.root / "original")
        self.prepare(source, need_roles=True)
        jobs = [
            paper_ready.Job(source, adjusted.HEATMAP, output=self.root / "heatmap__adjusted.pdf"),
            paper_ready.Job(role, adjusted.BAR, output=self.root / "bar__adjusted.pdf"),
        ]
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        with patch(
            "duckdb.connect", side_effect=AssertionError("render must not query per-IP data")
        ):
            results = paper_ready.run(jobs, workers=1)
        self.assertTrue(all(r.status == "created" for r in results), results)
        self.assertEqual(before, {p: stamp(p) for p in before})
        for job in jobs:
            self.assertTrue(job.destination.read_bytes().startswith(b"%PDF-"))
            self.assertNotIn(b"/Subtype /Type3", job.destination.read_bytes())
        all_outputs = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        with patch.object(adjusted, "load_data", side_effect=AssertionError("skip existing")):
            self.assertTrue(all(r.status == "skipped-existing" for r in paper_ready.run(jobs)))
        self.assertEqual(all_outputs, {p: stamp(p) for p in all_outputs})

    def test_missing_or_stale_preparation_never_falls_back_to_old_groups(self):
        source, _ = fixture(self.root / "original")
        job = paper_ready.Job(source, adjusted.HEATMAP, output=self.root / "heatmap.pdf")
        result = paper_ready.run([job], workers=1)[0]
        self.assertEqual(result.status, "failed")
        self.assertIn("--prepare-os-adjusted", result.error)
        self.prepare(source)
        _, _, _, _, _, cache = prepared.paths(source)
        old = stamp(cache)
        os_source = self.root / "original/os.pq"
        os.utime(
            os_source,
            ns=(os_source.stat().st_atime_ns, os_source.stat().st_mtime_ns + 1_000_000_000),
        )
        with self.assertRaisesRegex(ValueError, "stale"):
            self.prepare(source)
        self.assertEqual(stamp(cache), old)
        self.assertEqual(paper_ready.run([job], workers=1)[0].status, "failed")

    def test_adjusted_jobs_default_to_additional_filenames(self):
        source, role = fixture(self.root / "original")
        self.assertEqual(
            paper_ready.Job(source, adjusted.HEATMAP).destination.name,
            "n-rt-b_fi-m_operating-system-group-by-strategy__paper__adjusted.pdf",
        )
        self.assertEqual(
            paper_ready.Job(role, adjusted.BAR).destination.name,
            "n-rt-b_fi-m_itdk-role-by-os__paper_final__adjusted.pdf",
        )
        full = self.root / "c-rt-b_operating-system-group-by-strategy__full.json"
        self.assertEqual(
            paper_ready.Job(full, adjusted.FULL_HEATMAP).destination.name,
            "c-rt-b_operating-system-group-by-strategy__paper__full__adjusted.pdf",
        )

    def test_one_shared_tag_map_and_partition_conservation_failures(self):
        source, _ = fixture(self.root / "original")
        self.prepare(source)
        cache_files = list((self.root / "tag-cache").glob("*.pq"))
        self.assertEqual(len(cache_files), 1)
        second = self.root / "second.json"
        second.write_bytes(source.read_bytes())
        # Same measurement/source is intentionally bound to the original cache identity.
        with self.assertRaisesRegex(ValueError, "stale"):
            self.prepare(second)
        report = json.loads(source.read_text())
        report["matched_grouped_ip_count"] += 1
        third = self.root / "other-original.json"
        third.write_text(json.dumps(report))
        prepared.paths(source)[-1].unlink()  # Test fixture only; no published source is deleted.
        from ipid_analysis.artifacts import completion_path

        completion_path(prepared.paths(source)[-1]).unlink()
        with self.assertRaisesRegex(ValueError, "conserve"):
            self.prepare(third)

    def test_full_heatmap_keeps_base_column_selection_and_new_labels(self):
        source, _ = fixture(self.root / "full", connection=True)
        self.prepare(source)
        job = paper_ready.Job(source, adjusted.FULL_HEATMAP, output=self.root / "full.pdf")
        data, *_ = adjusted.load_data(job)
        self.assertEqual(len(data["strategies"]), 7)
        self.assertEqual(data["y_label"], "Operating-System Group / Vendor (#IP Addr.)")
        network = data["sections"][1]
        self.assertEqual(network["groups"][:2], ("mikrotik-routeros", "mikrotik-swos"))
        self.assertIn("hpe-comware", network["groups"])
        result = paper_ready.run([job], workers=1)[0]
        self.assertEqual(result.status, "created", result)

    def test_adjusted_cli_discovery_and_dry_run_do_not_prepare_data(self):
        reports = self.root / "reports"
        folder = (
            reports
            / "continental/test/eu/figures/icmp_fixture/no-connection/merged/rt-based-base_fixed-interval-mass"
        )
        source, _ = fixture(self.root / "original")
        folder.mkdir(parents=True)
        for name in (source.name, source.name.replace(prepared.KIND, "itdk-role-by-os")):
            (folder / name).write_bytes((source.parent / name).read_bytes())
        intermediate = (
            reports
            / "continental/test/eu/figures/icmp_intermediate/no-connection/merged/rt-based-base_fixed-interval-mass"
        )
        intermediate.mkdir(parents=True)
        old_extra = json.loads(source.read_text())
        old_extra.pop("aggregate")
        (intermediate / source.name).write_text(json.dumps(old_extra))
        before = {p: stamp(p) for p in reports.rglob("*") if p.is_file()}
        proc = subprocess.run(
            [
                sys.executable,
                "-B",
                "-m",
                "ipid_analysis.paper_ready",
                "--campaign",
                "test",
                "--reports-root",
                str(reports),
                "--os-adjusted",
                "--dry-run",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout.count("would-create:"), 2, proc.stdout)
        self.assertIn("__paper_final__adjusted.pdf", proc.stdout)
        self.assertEqual(before, {p: stamp(p) for p in before})


class AdjustedExportTest(unittest.TestCase):
    def test_registered_full_adjusted_selection_and_zip_are_bound_to_original_report(self):
        f = full_tests.FullFollowupTest()
        f.setUp()
        self.addCleanup(f.doCleanups)
        f.fixture()
        summaries = []
        for site in ("af", "as", "au", "eu", "na", "sa"):
            zid = f.spec["zmap_ids"][site]
            directory = f.analysis / site / f"full/figures/{zid}/connection/rt-based-base"
            source = directory / "c-rt-b_operating-system-group-by-strategy__full.json"
            f.write(
                source,
                dict(
                    figure=prepared.KIND,
                    protocol="tcp",
                    zmap_id=zid,
                    target="tcp.ipid.connection.rt-based.base",
                    measurements=dict(rt_based_base="tcp-80_2026-11-21_19-36-52"),
                ),
            )
            done = json.loads((f.analysis / site / "done__full.json").read_text())
            done["artifacts"].append(
                dict(
                    path="/mnt/data01/ipid-analysis/reports/"
                    + source.relative_to(f.root / "reports").as_posix(),
                    size_bytes=source.stat().st_size,
                    sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                )
            )
            f.write(f.analysis / site / "done__full.json", done)
            summaries.append(done)
        f.write(
            f.analysis / "done__full.json",
            dict(version=1, analysis_id=f.spec["analysis_id"], sites=summaries),
        )
        found = adjusted.jobs(
            [],
            f.root / "reports",
            full_tests.CAMPAIGN,
            sites=("af", "as", "au", "eu", "na", "sa"),
            protocols=("tcp",),
        )
        self.assertEqual(len(found), 6)
        self.assertTrue(
            all(
                j.kind == adjusted.FULL_HEATMAP
                and j.destination.name.endswith("__paper__full__adjusted.pdf")
                for j in found
            )
        )
        self.assertEqual(
            adjusted.jobs(
                [], f.root / "reports", full_tests.CAMPAIGN, sites=("eu",), protocols=("icmp",)
            ),
            [],
        )
        old = f.run_export(strict=True)
        job = next(j for j in found if "/eu/" in j.source.as_posix())
        f.write(job.destination, b"%PDF-1.4\nadjusted full")
        from ipid_analysis.artifacts import completion_path

        marker = completion_path(job.destination)
        receipt = json.loads(marker.read_text())
        receipt["parameters"] = dict(
            operation="paper-os-adjusted-v1",
            adjusted_original_report_sha256=hashlib.sha256(job.source.read_bytes()).hexdigest(),
        )
        marker.write_text(json.dumps(receipt))
        new = f.run_export(strict=True)
        changed = [k for k in old["values"] if old["values"][k] != new["values"][k]]
        self.assertEqual(changed, ["fig.eu.tcp.conn.rt-base.os-by-strategy.full"])
        self.assertTrue(new["values"][changed[0]]["raw"].endswith("__paper__full__adjusted.pdf"))
        bundle(repository_root=f.root)
        with zipfile.ZipFile(f.root / "tmp/paper-figures.zip") as archive:
            self.assertIn(new["values"][changed[0]]["raw"], archive.namelist())
        before = {p: stamp(p) for p in (f.root / "tmp").glob("values.*")}
        receipt["parameters"]["adjusted_original_report_sha256"] = "0" * 64
        marker.write_text(json.dumps(receipt))
        with self.assertRaises(ValueError):
            f.run_export(strict=True)
        self.assertEqual(before, {p: stamp(p) for p in before})

    def test_adjusted_figures_are_preferred_and_zipped_without_replacing_old_values(self):
        with tempfile.TemporaryDirectory() as directory:
            f = Fixture(directory)
            role = f.report("itdk-role-by-os", {}, protocol="icmp", phase="rt-fi")
            heat = f.report(prepared.KIND, {}, protocol="icmp", phase="rt-fi")
            final = role.with_name(role.stem + "__paper_final.pdf")
            paper = heat.with_name(heat.stem + "__paper.pdf")
            for path in (final, paper):
                path.write_bytes(b"%PDF-1.4\nold")
            before = f.run(strict=True)
            added = []
            for path in (final, paper):
                output = path.with_name(path.stem + "__adjusted.pdf")
                output.write_bytes(b"%PDF-1.4\nadjusted")
                marker = f.receipt(output)
                receipt = json.loads(marker.read_text())
                report = role if path == final else heat
                receipt["parameters"] = dict(
                    operation="paper-os-adjusted-v1",
                    report_sha256=hashlib.sha256(report.read_bytes()).hexdigest(),
                )
                marker.write_text(json.dumps(receipt))
                added.append(output)
            after = f.run(strict=True)
            changes = [k for k in before["values"] if before["values"][k] != after["values"][k]]
            self.assertEqual(len(changes), 2)
            self.assertTrue(all(k.startswith("fig.") for k in changes))
            self.assertTrue(
                all(after["values"][k]["raw"].endswith("__adjusted.pdf") for k in changes)
            )
            bundle(repository_root=f.root)
            with zipfile.ZipFile(f.root / "tmp/paper-figures.zip") as archive:
                self.assertEqual(
                    set(archive.namelist()), {p.relative_to(f.root).as_posix() for p in added}
                )


if __name__ == "__main__":
    unittest.main()
