"""Isolation, historical source selection and real small warm-cache analyses."""

from contextlib import nullcontext
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from benchmarks import analysis_ab as ab
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import yaml


class AnalysisABTests(unittest.TestCase):
    def test_null_table_timing_preserves_values_and_traces_only_generation(self):
        from ipid_analysis.empirical_random_uniformity import EmpiricalNullTables
        from ipid_analysis.multiscale_increment_uniformity import MultiscaleIncrementNullTables

        expected = EmpiricalNullTables(32, 17).spacing(4).tolist()
        original_spacing = EmpiricalNullTables.spacing
        original_single = MultiscaleIncrementNullTables.single
        original_joint = MultiscaleIncrementNullTables.joint
        fake = SimpleNamespace(record_stage=lambda *a, **kw: nullcontext())
        for name in (
            "prepare_itdk",
            "prepare_ripe_atlas",
            "render_itdk_analysis",
            "render_ripe_atlas_analysis",
            "evaluate_random_reproducibility",
            "render_strategy_refinement_plot",
        ):
            setattr(fake, name, lambda *a, **kw: None)
        with tempfile.TemporaryDirectory() as temporary:
            timings = ab.Timings(Path(temporary) / "trace.jsonl")
            with (
                patch.object(EmpiricalNullTables, "spacing", original_spacing),
                patch.object(MultiscaleIncrementNullTables, "single", original_single),
                patch.object(MultiscaleIncrementNullTables, "joint", original_joint),
            ):
                ab.instrument_science(fake, timings)
                table = EmpiricalNullTables(32, 17)
                self.assertEqual(table.spacing(4).tolist(), expected)
                self.assertEqual(table.spacing(sample_count=4).tolist(), expected)
                tables = MultiscaleIncrementNullTables(32, 17)
                first = tables.joint(8, (2, 4)).tolist()
                self.assertEqual(
                    tables.joint(transition_count=8, bin_counts=(2, 4)).tolist(), first
                )
            timings.stream.close()
            events = [
                json.loads(s) for s in (Path(temporary) / "trace.jsonl").read_text().splitlines()
            ]
            spacing = [e for e in events if "spacing" in e["name"]]
            joint = [e for e in events if "joint" in e["name"]]
            self.assertEqual(len(spacing), 1)
            self.assertEqual(len(joint), 1)

    def test_requires_opt_in_before_any_work(self):
        with patch.object(ab, "writer_lease", side_effect=AssertionError("lease touched")):
            with self.assertRaises(SystemExit):
                ab.main([])

    def test_exact_merged_pins(self):
        self.assertEqual(ab.PINS["A"], "b6e142c7de41d9c6c4a5c8f4515e82abeccc97ed")
        self.assertEqual(ab.PINS["B"], "68edad13e3b0f5a4e765f8c071dea8dc2e65a0a0")

    def test_write_and_copy_guards_handle_escape_and_parameters(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            allowed = root / "owned"
            allowed.mkdir()
            guard = ab.WriteGuard([allowed])
            guard.check(allowed / "output")
            for query, params in (
                ("COPY (SELECT 1) TO $output (FORMAT PARQUET)", {"output": str(allowed / "x.pq")}),
                ("COPY (SELECT ?) TO ?", [1, str(allowed / "x.pq")]),
                (f"COPY (SELECT 1) TO '{allowed.as_posix()}/x.pq'", None),
            ):
                guard.sql(query, params)
            for target in (root / "production", allowed / "../production"):
                with self.assertRaises(PermissionError):
                    guard.check(target)
                with self.assertRaises(PermissionError):
                    guard.sql("COPY (SELECT 1) TO $output", {"output": str(target)})
            with self.assertRaises(PermissionError):
                guard.sql("COPY tbl TO STDOUT")
            with self.assertRaises(PermissionError):
                guard.audit("urllib.Request", ("https://example.org",))

    @unittest.skipUnless(sys.platform == "linux", "Linux directory descriptors")
    def test_relative_fd_cleanup_is_allowed_only_for_owned_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            owned = root / "owned"
            owned.mkdir()
            guard = ab.WriteGuard([owned])
            good = os.open(owned, os.O_RDONLY)
            bad = os.open(root, os.O_RDONLY)
            try:
                guard.audit("os.remove", ("x", good))
                with self.assertRaises(PermissionError):
                    guard.audit("os.remove", ("production", bad))
            finally:
                os.close(good)
                os.close(bad)

    def test_database_wrapper_measures_real_copy(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            timings = ab.Timings(root / "trace.jsonl")
            con = ab.Connection(duckdb.connect(), ab.WriteGuard([root]), timings)
            con.execute(
                "COPY (SELECT 12 AS value) TO $output (FORMAT PARQUET)",
                {"output": str(root / "data.pq")},
            )
            self.assertEqual(con.execute("SELECT 9").fetchone(), (9,))
            con.close()
            timings.stream.close()
            self.assertEqual(pq.read_table(root / "data.pq")["value"].to_pylist(), [12])
            events = [json.loads(line) for line in (root / "trace.jsonl").read_text().splitlines()]
            self.assertEqual(len(events), 2)
            self.assertTrue(all(row["success"] for row in events))

    def test_publication_guard_checks_before_upload(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            owned = root / "owned"
            owned.mkdir()
            timings = ab.Timings(owned / "trace.jsonl")
            calls = []
            upload = ab.checked_publication(
                lambda *args: calls.append(args), ab.WriteGuard([owned]), timings
            )
            with self.assertRaises(PermissionError):
                upload(owned / "source", root / "production")
            self.assertFalse(calls)
            upload(owned / "source", owned / "result")
            self.assertEqual(len(calls), 1)
            timings.stream.close()

    def test_environment_isolates_inherited_worker_and_cache_configuration(self):
        spec = dict(
            tree="/code",
            scope="continental/x/as",
            control="/ctl",
            scratch="/disk",
            ram="/ram",
            label="B",
            s3_roots={"raw": "s3://test/raw"},
            scratch_limit=21,
            memory_limit=125,
            cpu_ids=[0, 1],
        )
        with patch.dict(
            os.environ,
            {
                "IPID_ANALYSIS_FORCE": "1",
                "IPID_ANALYSIS_S3_PREFIX": "s3://production/jobs",
                "IPID_MAXMIND_DB": "/old/mmdb",
            },
        ):
            env = ab.environment(spec)
        self.assertNotIn("IPID_ANALYSIS_FORCE", env)
        self.assertNotIn("IPID_ANALYSIS_S3_PREFIX", env)
        self.assertNotIn("IPID_MAXMIND_DB", env)
        self.assertEqual(env["IPID_ANALYSIS_RIPE_CACHE_S3"], "")
        self.assertEqual(env["IPID_ANALYSIS_SCRATCH_STORAGE"], "ram")
        self.assertEqual(env["IPID_ANALYSIS_TMP_DIR"], "/disk")
        self.assertEqual(env["TMPDIR"], "/ram")

    def test_rclone_allowlist_accepts_only_test_copyto_destination(self):
        with tempfile.TemporaryDirectory() as temporary:
            timings = ab.Timings(Path(temporary) / "trace.jsonl")
            calls = []
            prefix = "rclone-remote:bucket/.ipid-analysis-tests/uuid/trial"
            run = ab.checked_rclone(lambda *a, **kw: calls.append((a, kw)), timings, [prefix])
            command = [
                "/usr/bin/rclone",
                "copyto",
                "--s3-no-check-bucket",
                "/ram/file",
                prefix + "/result.pq",
            ]
            run(command, check=True)
            self.assertEqual(calls[0][0][0], command)
            for invalid in (
                ["s3cmd", "put", "/ram/file", prefix + "/result"],
                ["rclone", "copyto", "/ram/file", "rclone-remote:bucket/production/file"],
                ["rclone", "delete", prefix + "/result"],
                "rclone copyto /ram/file production",
            ):
                with self.assertRaises(PermissionError):
                    run(invalid)
            with self.assertRaises(PermissionError):
                run(command, shell=True)
            self.assertEqual(len(calls), 1)
            timings.stream.close()

    def test_actual_input_hash_rejects_corrupt_marker(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "input.json"
            path.write_text("{}")
            marker = path.parent / ".ipid-complete" / "input.json.json"
            marker.parent.mkdir()
            marker.write_text(json.dumps(dict(status="complete", sha256="0" * 64, size_bytes=2)))
            with self.assertRaisesRegex(ValueError, "integrity"):
                ab.verify_input(path)

    def test_input_pin_includes_completion_parameters(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "input.json"
            path.write_text("{}")
            marker = path.parent / ".ipid-complete" / "input.json.json"
            marker.parent.mkdir()
            info = dict(
                status="complete",
                sha256=ab.sha256(path),
                size_bytes=2,
                parameters={"version": "old"},
            )
            marker.write_text(json.dumps(info))
            before = ab.verify_input(path)
            info["parameters"]["version"] = "changed"
            marker.write_text(json.dumps(info))
            self.assertNotEqual(before, ab.verify_input(path))

    def test_science_comparison_ignores_provenance_but_rejects_changed_values(self):
        a = dict(generated_at="old", mtime_ns=1, count=12, seconds=4, source="/a/input")
        b = dict(generated_at="new", mtime_ns=2, count=12, seconds=4, source="/b/input")
        self.assertEqual(
            ab.scientific_json(a, [("/a", "<root>")]), ab.scientific_json(b, [("/b", "<root>")])
        )
        self.assertEqual(
            ab.scientific_json(
                {"sources": {"/a/data.pq": {"size": 12, "mtime_ns": 1}}}, [("/a", "<root>")]
            ),
            ab.scientific_json(
                {"sources": {"/b/data.pq": {"size": 12, "mtime_ns": 2}}}, [("/b", "<root>")]
            ),
        )
        inv = {"data": dict(kind="parquet", rows=2, logical_sha256="abc", schema=["IP_ADDR"])}
        self.assertTrue(ab.equivalence([inv, copy.deepcopy(inv)]))
        different = copy.deepcopy(inv)
        different["data"]["logical_sha256"] = "changed"
        with self.assertRaisesRegex(ValueError, "scientific"):
            ab.equivalence([inv, different])
        with self.assertRaisesRegex(ValueError, "file sets"):
            ab.equivalence([inv, {}])

    def test_uri_binding_rejects_traversal_and_unknown_root(self):
        spec = dict(repo="/repo", s3_roots={"raw": "s3://test/raw"})
        self.assertEqual(
            ab.uri_path("s3://test/raw/ipid/run/file.pq", spec),
            Path("/repo/data/raw/ipid/run/file.pq"),
        )
        for uri in ("s3://test/raw/../../outside", "s3://other/raw/input"):
            with self.assertRaises(ValueError):
                ab.uri_path(uri, spec)

    @unittest.skipUnless(sys.platform == "linux", "Linux locks and process supervision")
    def test_writer_lease_blocks_concurrent_analysis_and_releases(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            control = repo / "tmp"
            control.mkdir()
            with patch.dict(
                os.environ,
                {
                    "IPID_ANALYSIS_CONTROL_ROOT": str(control),
                    "IPID_ANALYSIS_LOCK_DIR": str(control / "custom-locks"),
                },
            ):
                with ab.writer_lease(repo):
                    with self.assertRaisesRegex(RuntimeError, "active"):
                        with ab.writer_lease(repo):
                            pass
                with ab.writer_lease(repo):
                    pass


def warm_fixture(repo):
    """Small real TCP workflow with cached external datasets, never network."""
    scope = "continental/fixture/as"
    raw = repo / "data/raw" / scope
    job = "tcp-80_2026-10-31_17-19-14"
    ips = ["1.1.1.1", "8.8.8.8", "9.9.9.9"]
    variants = {}
    for index, (mode, interval, scale, requests) in enumerate(
        (
            ("no-connection", "rt-based", "base", 4),
            ("no-connection", "fixed-interval", "base", 4),
            ("no-connection", "fixed-interval", "mass", 25),
            ("connection", "rt-based", "base", 4),
            ("connection", "fixed-interval", "base", 4),
        )
    ):
        name = f"tcp-80_2026-10-31_17-20-{index:02}"
        variants.setdefault(mode, {}).setdefault(interval, {})[scale] = name
        directory = raw / "ipid" / name
        directory.mkdir(parents=True)
        length = 4 * requests
        values = (
            [
                4107,
                15352,
                54225,
                34522,
                612,
                50201,
                19084,
                62820,
                7311,
                45002,
                28217,
                65100,
                233,
                39810,
                16601,
                57234,
            ]
            if index == 0
            else list(range(100, 100 + length))
        )
        pq.write_table(
            pa.table(
                dict(
                    IP_ADDR=ips,
                    IPID_SEQUENCE=[",".join(str(value) for value in values)] * 3,
                    SEND_TIMESTAMP_SEQUENCE=[
                        ",".join(str(1_000_000_000 + i * 20_000) for i in range(length))
                    ]
                    * 3,
                )
            ),
            directory / "ipid.pq",
        )
        (directory / "ipid.snapshot.yaml").write_text(
            yaml.safe_dump(
                dict(
                    connection_count=4,
                    requests_per_connection=requests,
                    request_ip_ids=[1000, 2000, 1001, 2001],
                )
            )
        )
        if index == 0:
            pq.write_table(pa.table({"IP_ADDR": ips}), directory / "zmap_unclassified.pq")
    zmap = raw / "zmap" / job
    zmap.mkdir(parents=True)
    pq.write_table(
        pa.table(dict(IP_ADDR=ips, REPLY_TYPE=["synack", "synack", "rst"])), zmap / "zmap.pq"
    )
    manifest = dict(tcp=dict(zmap=job, os_status="not_requested", ipid=variants))
    request_base = f"s3://test/workflow/continental/fixture/sites/as/analysis-jobs/{job}"
    request = dict(
        version=1,
        job_id=job,
        protocol="tcp",
        manifest_uri=request_base + "/manifest.json",
        zmap_prefix="s3://test/raw/" + scope + "/zmap",
        os_prefix="",
        ipid_prefix="s3://test/raw/" + scope + "/ipid",
        done_uri=request_base + "/done.json",
        failed_uri=request_base + "/failed.json",
        created_at="2026-10-31T17:19:14Z",
        os_status="not_requested",
    )
    itdk = repo / "data/raw/caida-itdk/2025-08/midar-iff-snmp-tnt"
    itdk.mkdir(parents=True)
    (itdk / "midar-iff-snmp-tnt.ifaces.bz2").write_bytes(b"retained-raw")
    (itdk / "source.json").write_text(json.dumps(dict(cache_version="1", ifaces_size=12)))
    pq.write_table(
        pa.table(
            dict(
                IP_ADDR=ips,
                NODE_ID=["N1", "N1", "N2"],
                T=[True, False, True],
                D=[False, True, False],
            )
        ),
        itdk / "interfaces.pq",
    )
    from ipid_analysis import ripe_atlas as ripe

    window = "2026-10-03_2026-10-31"
    key = "daily-dumps-s12"
    identity = hashlib.sha256(
        f"{window}|{key}|{ripe.DEFAULT_DUMP_BASE_URL}|{ripe.DEFAULT_API_BASE_URL}".encode()
    ).hexdigest()
    dataset = repo / "data/processed/ripe-atlas" / window / key
    dataset.mkdir(parents=True)
    pq.write_table(
        pa.table(
            dict(
                IP_V4=pa.array([16843009, 134744072, 151587081], type=pa.uint32()),
                IP_ADDR=ips,
                T=[True, False, True],
                D=[False, True, False],
            )
        ),
        dataset / "ipv4-roles.pq",
    )
    (dataset / "source.json").write_text(
        json.dumps(dict(cache_version="4", cache_identity=identity, addresses=3))
    )
    for directory in (repo / "reports", repo / "tmp"):
        directory.mkdir(exist_ok=True)
    return scope, request, manifest


@unittest.skipUnless(sys.platform == "linux", "real historical Linux subprocess integration")
class HistoricalAnalysisABTest(unittest.TestCase):
    def test_both_pinned_sources_run_real_warm_no_os_pipeline_and_match(self):
        # CI fetches full history. Historical integration must not silently use
        # current sources when a pinned revision is absent.
        for pin in ab.PINS.values():
            subprocess.run(
                ["git", "-C", str(ab.PROJECT), "cat-file", "-e", pin],
                check=True,
                capture_output=True,
            )
        with tempfile.TemporaryDirectory(dir=ab.PROJECT.parent) as temporary:
            root = Path(temporary)
            repo = root / "live"
            repo.mkdir()
            scope, request, manifest = warm_fixture(repo)
            request_path, manifest_path = root / "request.json", root / "manifest.json"
            request_path.write_text(json.dumps(request))
            manifest_path.write_text(json.dumps(manifest))
            inputs = root / "preflight/control/inputs.json"
            trials = []
            for name, label, mode in (
                ("preflight", "B", "preflight"),
                ("A", "A", "trial"),
                ("B", "B", "trial"),
            ):
                source = root / (name + "-source")
                ab.export_tree(ab.PROJECT, ab.PINS[label], source)
                ram = Path(tempfile.mkdtemp(prefix="ipid-ab-ci-", dir="/dev/shm"))
                self.addCleanup(ab.remove_owned_ram, ram, "ipid-ab-ci")
                spec = dict(
                    repo=str(repo),
                    tree=str(source),
                    label=label,
                    revision=ab.PINS[label],
                    mode=mode,
                    scope=scope,
                    request=str(request_path),
                    manifest=str(manifest_path),
                    control=str(root / name / "control"),
                    scratch=str(root / name / "scratch"),
                    ram=str(ram),
                    processed=str(repo / "data/processed/.ipid-analysis-tests" / name),
                    reports=str(repo / "reports/.ipid-analysis-tests" / name),
                    remote_prefixes=[],
                    inputs=str(inputs),
                    s3_roots={
                        "raw": "s3://test/raw",
                        "processed": "s3://test/processed",
                        "reports": "s3://test/reports",
                    },
                    cpu_ids=[min(os.sched_getaffinity(0))],
                    scratch_limit=ab.GIB,
                    memory_limit=4 * ab.GIB,
                    test_local=True,
                )
                try:
                    ab.run_child(spec, 180)
                except Exception:
                    print((Path(spec["control"]) / "run.log").read_text())
                    raise
                if mode == "trial":
                    trials.append(spec)
            inventories = [
                json.loads((Path(s["control"]) / "output-inventory.json").read_text())
                for s in trials
            ]
            self.assertTrue(ab.equivalence(inventories))
            for spec in trials:
                control = Path(spec["control"])
                result = json.loads((control / "result.json").read_text())
                self.assertGreater(result["analysis_seconds"], 0)
                status = json.loads((control / "postprocess-status.json").read_text())
                self.assertEqual(
                    status["optional"],
                    dict(os={"tcp": "not_requested"}, itdk="prepared", ripe="prepared"),
                )
                events = [
                    json.loads(line)
                    for line in (control / "timings.jsonl").read_text().splitlines()
                ]
                self.assertIn(
                    "render_itdk_analysis", {e["name"] for e in events if e["kind"] == "function"}
                )


if __name__ == "__main__":
    unittest.main()
