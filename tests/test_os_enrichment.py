import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis import os_enrichment as e
from ipid_analysis import continental as c
from ipid_analysis.artifacts import write_table, write_text
from ipid_analysis.scheduler import Task, tasks_from_objects
from ipid_analysis.strategy_merge import iter_strategy_merges
from test_continental import FakeClient, PREFIX, CAMPAIGN, RAW


class EnrichmentTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.work = self.root / "work"
        self.work.mkdir()
        self.client = FakeClient()
        env = patch.dict(
            os.environ,
            {
                "IPID_ANALYSIS_STORAGE_MODE": "local",
                "IPID_ANALYSIS_S3_ROOTS": "{}",
                "IPID_ANALYSIS_TMP_DIR": str(self.root / "scratch"),
            },
        )
        env.start()
        self.addCleanup(env.stop)
        import ipid_analysis.config as config

        for name, value in (
            ("PROCESSED_STORAGE_DIR", self.root / "processed"),
            ("REPORTS_STORAGE_DIR", self.root / "reports"),
        ):
            p = patch.object(config, name, value)
            p.start()
            self.addCleanup(p.stop)

    def table(self, uri, values):
        path = self.work / "table.pq"
        pq.write_table(pa.table(values), path)
        data = path.read_bytes()
        self.client.values[uri] = data
        return dict(size_bytes=len(data), sha256=hashlib.sha256(data).hexdigest())

    def protocol(self, protocol="icmp", prefix=PREFIX, scope="", stamp="2026-10-25_01-00-"):
        payload = {"icmp": "icmp", "tcp": "tcp-80", "udp-dns": "udp-dns-53"}[protocol]
        ids = [payload + "_" + stamp + f"{i:02d}" for i in range(10)]
        zmap, rt, mass, fixed = ids[:4]
        core = prefix + "/analysis-jobs/" + zmap
        raw = RAW + ("/" + scope if scope else "")
        manifest = {
            protocol: dict(
                zmap=zmap,
                os_status="not_requested",
                ipid={
                    "no-connection": {
                        "rt-based": {"base": rt},
                        "fixed-interval": {"mass": mass, "base": fixed},
                    }
                },
                random_reproducibility=dict(
                    baseline=mass,
                    repeats=ids[4:9],
                    target_file="random-reproducibility-targets.pq",
                    cohort_file="random-reproducibility-cohort.pq",
                    prepare_metadata_file="random-reproducibility-prepare.json",
                    selection_seed=42,
                    maximum_targets=10000,
                ),
            )
        }
        if protocol == "tcp":
            manifest[protocol]["ipid"]["connection"] = {
                "rt-based": {"base": ids[9]},
                "fixed-interval": {"base": payload + "_2026-10-25_01-01-00"},
            }
        self.client.json(core + "/manifest.json", manifest)
        self.client.json(
            core + "/request.json",
            dict(
                version=1,
                job_id=zmap,
                protocol=protocol,
                manifest_uri=core + "/manifest.json",
                zmap_prefix=raw + "/zmap",
                os_prefix="",
                ipid_prefix=raw + "/ipid",
                done_uri=core + "/done.json",
                failed_uri=core + "/failed.json",
                created_at="2026-10-25T01:00:00Z",
                os_status="not_requested",
            ),
        )
        self.client.json(
            core + "/done.json",
            dict(
                version=1,
                job_id=zmap,
                manifest_uri=core + "/manifest.json",
                os_status="not_requested",
            ),
        )
        self.client.json(
            core + "/postprocess-status.json",
            dict(version=1, required_complete=True, optional=dict(os={protocol: "not_requested"})),
        )
        from ipid_analysis.manifest import resolve

        sources = list(iter_strategy_merges(manifest))
        connection = resolve(manifest, "tcp.ipid.connection.rt-based.base")
        if connection:
            sources.append(connection)
        for source in sources:
            write_table(
                pa.table(dict(IP_ADDR=["192.0.2.1"], IPID_SELECTION_STRATEGY=["SINGLE"])),
                source.artifact_path(self.root / "processed" / scope, "strategies"),
            )
        evidence = self.table(
            raw + "/zmap/" + zmap + "/zmap.pq", dict(IP_ADDR=["192.0.2.1"], REPLY_TYPE=[""])
        )
        self.client.json(
            raw + "/zmap/" + zmap + "/measurement-complete.json",
            dict(version=1, measurement_id=zmap, files={"zmap.pq": evidence}),
        )
        return core

    def request(self, core):
        if "/analysis-jobs/" in core:
            self.client.json(
                core + "/os-policy.json",
                dict(version=1, job_id=core.rsplit("/", 1)[-1], os_status="not_requested"),
            )
        root = core + "/enrichments/os"
        self.client.json(
            root + "/request.json",
            dict(
                version=1,
                core_prefix=core,
                core_done_sha256=hashlib.sha256(
                    self.client.values[core + "/done.json"]
                ).hexdigest(),
                requested_at="2026-10-26T10:00:00Z",
                status="requested",
                vantage="late-os-vm",
            ),
        )
        return root

    def scan(self, root):
        scan = self.client.value(root + "/scan/request.json")
        target = self.work / "targets.pq"
        self.client.download(scan["target"]["uri"], target)
        addresses = sorted(set(pq.read_table(target)["IP_ADDR"].to_pylist()))
        os_columns = {
            column: [""] * len(addresses)
            for column in __import__(
                "ipid_analysis.plot_os_group_strategy", fromlist=["RAW_OS_COLUMNS"]
            ).RAW_OS_COLUMNS
        }
        os_columns["IP_ADDR"] = addresses
        # SSH OS evidence resolves one Linux observation per target.
        os_columns["SSH_OS_TAG"] = ["linux"] * len(addresses)
        os_columns["OS_TAG"] = ["linux"] * len(addresses)
        os_columns["OS_STATUS"] = ["resolved"] * len(addresses)
        files = {
            "os.pq": self.table(root + "/raw/os.pq", os_columns),
            "os-observations.pq": self.table(
                root + "/raw/os-observations.pq",
                dict(
                    IP_ADDR=addresses,
                    RESPONSE_MASK=[1] * len(addresses),
                    EVIDENCE_MASK=[1] * len(addresses),
                    TAG_MASK=[1] * len(addresses),
                    OS_STATUS=["resolved"] * len(addresses),
                    CONFLICT=[""] * len(addresses),
                ),
            ),
        }
        for name, data in (("os-coverage.json", b"{}"), ("os.snapshot.yaml", b"late: true\n")):
            self.client.values[root + "/raw/" + name] = data
            files[name] = dict(size_bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
        self.client.json(
            root + "/scan/done.json",
            dict(
                version=1,
                campaign_id=scan["campaign_id"],
                late_enrichment=True,
                measurement_id="tcp-80_2026-10-26_10-01-00",
                target=scan["target"],
                members=scan["members"],
                files=files,
                vantage="late-os-vm",
                raw_prefix=root + "/raw",
                started_at="2026-10-26T10:01:00Z",
                completed_at="2026-10-26T11:00:00Z",
            ),
        )

    def test_protocol_actual_os_only_render_and_duplicate_preserve_core_bytes(self):
        core = self.protocol()
        root = self.request(core)
        remote = dict(self.client.values)
        local = {p: p.read_bytes() for p in (self.root / "processed").rglob("*") if p.is_file()}
        e.prepare(self.client, root, PREFIX, self.work)
        target = self.client.value(root + "/scan/request.json")["target"]
        self.assertEqual(
            self.client.values[target["uri"]],
            self.client.values[RAW + "/zmap/" + core.rsplit("/", 1)[-1] + "/zmap.pq"],
        )
        e.prepare(self.client, root, PREFIX, self.work)
        with self.assertRaises(KeyError):
            e.analyze(self.client, root, PREFIX, self.work, 1)
        self.assertFalse((self.root / "processed/enrichments").exists())
        self.scan(root)
        e.analyze(self.client, root, PREFIX, self.work, 1)
        done = self.client.value(root + "/done.json")
        self.assertEqual(done["status"], "completed")
        self.assertTrue(any(a["path"].endswith(".pdf") for a in done["analyses"][0]["artifacts"]))
        count = len(self.client.writes)
        e.analyze(self.client, root, PREFIX, self.work, 1)
        self.assertEqual(len(self.client.writes), count)
        for uri, data in remote.items():
            self.assertEqual(self.client.values[uri], data)
        for path, data in local.items():
            self.assertEqual(path.read_bytes(), data)

    def test_tcp_connection_and_udp_reuse_core_roles_without_non_os_producers(self):
        from ipid_analysis.plot_itdk_os import TRANSIT_ROLE
        from ipid_analysis.manifest import resolve

        for protocol in ("tcp", "udp-dns"):
            with self.subTest(protocol=protocol):
                core = self.protocol(protocol)
                manifest = self.client.value(core + "/manifest.json")
                sources = list(iter_strategy_merges(manifest))
                connection = resolve(manifest, "tcp.ipid.connection.rt-based.base")
                if connection:
                    sources.append(connection)
                original = {}
                for source in sources:
                    for role, kind in (("itdk", "itdk"), ("ripe_atlas", "ripe-atlas")):
                        joined = source.artifact_path(
                            self.root / "processed", kind + "-strategy-join"
                        )
                        write_table(
                            pa.table(
                                dict(
                                    IP_ADDR=["192.0.2.1"],
                                    NODE_ID=["N1"],
                                    T=[True],
                                    D=[False],
                                    OBSERVED_ROLE=[TRANSIT_ROLE],
                                )
                            ),
                            joined,
                        )
                        metadata = source.artifact_path(
                            self.root / "reports/figures", kind + "-role-by-strategy", "json"
                        )
                        value = (
                            dict(release="original", topology="ipv4", metadata="original-metadata")
                            if role == "itdk"
                            else dict(
                                source="original",
                                window_start="2026-10-20T00:00:00+00:00",
                                window_end="2026-10-27T00:00:00+00:00",
                                metadata="original-metadata",
                            )
                        )
                        write_text(metadata, json.dumps({role: value}))
                        status = self.client.value(core + "/postprocess-status.json")
                        status.setdefault("stages", []).append(
                            dict(
                                stage="render_itdk_analysis"
                                if role == "itdk"
                                else "render_ripe_atlas_analysis",
                                target=source.target,
                                status="completed",
                            )
                        )
                        self.client.json(core + "/postprocess-status.json", status)
                        original[joined] = joined.read_bytes()
                        original[metadata] = metadata.read_bytes()
                root = self.request(core)
                e.prepare(self.client, root, PREFIX, self.work)
                self.scan(root)
                with (
                    patch(
                        "ipid_analysis.plot_ripe_atlas_os.render_ripe_atlas_analysis",
                        side_effect=AssertionError("core role producer called"),
                    ),
                    patch(
                        "ipid_analysis.strategies.classify_measurement",
                        side_effect=AssertionError("classifier called"),
                    ),
                    patch(
                        "ipid_analysis.strategy_merge.merge_strategies",
                        side_effect=AssertionError("merge called"),
                    ),
                ):
                    e.analyze(self.client, root, PREFIX, self.work, 1)
                stages = self.client.value(root + "/done.json")["analyses"][0]["stages"]
                self.assertEqual(len(stages), len(sources) * 2)
                self.assertTrue(all(s["status"] == "completed" for s in stages))
                for path, data in original.items():
                    self.assertEqual(path.read_bytes(), data)

    def geographic(self):
        self.client.json(
            CAMPAIGN + "/campaign.json",
            dict(
                version=1,
                campaign_id="round",
                sites=list(c.SITES),
                protocols=list(c.PROTOCOLS),
                raw_root=RAW,
                os_mode="disabled",
            ),
        )
        self.client.json(
            CAMPAIGN + "/os/not-requested.json",
            dict(version=1, campaign_id="round", status="not_requested"),
        )
        self.client.json(CAMPAIGN + "/done.json", dict(version=1, os_status="not_requested"))
        for site in c.SITES:
            scope = "continental/round/" + site
            prefix = CAMPAIGN + "/sites/" + site
            ids = {}
            for protocol in c.PROTOCOLS:
                ids[protocol] = self.protocol(protocol, prefix, scope).rsplit("/", 1)[-1]
                raw = RAW + "/" + scope + "/zmap/" + ids[protocol]
                number = c.SITES.index(site) * 3 + c.PROTOCOLS.index(protocol) + 1
                evidence = self.table(
                    raw + "/zmap.pq",
                    dict(IP_ADDR=["192.0.2.1", f"198.51.100.{number}"], REPLY_TYPE=["", ""]),
                )
                self.client.json(
                    raw + "/measurement-complete.json",
                    dict(version=1, measurement_id=ids[protocol], files={"zmap.pq": evidence}),
                )
            self.client.json(prefix + "/final/done.json", dict(os_status="not_requested"))
            self.client.json(
                prefix + "/discovery.json",
                dict(
                    version=1,
                    campaign_id="round",
                    site=site,
                    raw_prefix=RAW + "/" + scope + "/zmap",
                    zmap=ids,
                ),
            )
        return self.request(CAMPAIGN)

    def test_geographic_exact_eighteen_union_restart_and_all_protocols(self):
        root = self.geographic()
        before = dict(self.client.values)
        e.prepare(self.client, root, PREFIX, self.work)
        scan = self.client.value(root + "/scan/request.json")
        self.assertEqual(len(scan["members"]), 18)
        self.assertEqual(scan["target"]["rows"], 19)
        self.assertNotIn(CAMPAIGN + "/os/request.json", self.client.values)
        # Compare with the unchanged enabled producer's exact member/set semantics.
        normal = FakeClient()
        normal.values = dict(self.client.values)
        with patch.object(c, "os_disabled", return_value=False):
            c.union_discovery(normal, CAMPAIGN, self.work / "enabled")
        enabled = normal.value(CAMPAIGN + "/os/request.json")
        self.assertEqual(enabled["members"], scan["members"])
        normal_path, late_path = self.work / "normal.pq", self.work / "late.pq"
        normal.download(enabled["target"]["uri"], normal_path)
        self.client.download(scan["target"]["uri"], late_path)
        self.assertEqual(
            set(pq.read_table(normal_path)["IP_ADDR"].to_pylist()),
            set(pq.read_table(late_path)["IP_ADDR"].to_pylist()),
        )

        self.scan(root)
        calls = []

        def render(entry, raw, done, processed, figures, threads):
            calls.append((entry["scope"], entry["job_id"]))
            if len(calls) == 3:
                raise RuntimeError("interrupted analysis")
            write_text(processed / "os-test.json", "{}")
            return []

        with patch.object(e, "render_core", side_effect=render):
            with self.assertRaisesRegex(RuntimeError, "interrupted"):
                e.analyze(self.client, root, PREFIX, self.work, 1)
        self.assertNotIn(root + "/done.json", self.client.values)
        with patch.object(
            e,
            "render_core",
            side_effect=lambda entry, raw, done, processed, figures, threads: (
                write_text(processed / "os-test.json", "{}"),
                [],
            )[1],
        ) as resumed:
            e.analyze(self.client, root, PREFIX, self.work, 1)
            self.assertEqual(resumed.call_count, 16)
        self.assertEqual(len(self.client.value(root + "/done.json")["analyses"]), 18)
        for uri, data in before.items():
            self.assertEqual(self.client.values[uri], data)

    def test_scheduler_status_failure_and_explicit_retry_are_isolated(self):
        core = self.protocol()
        root = core + "/enrichments/os"
        self.assertEqual(e.status(self.client, root), "not_requested")
        root = self.request(core)
        self.assertEqual(e.status(self.client, root), "requested")
        self.assertEqual(
            [t.kind for t in e.tasks(set(self.client.values), PREFIX)], ["os-enrichment-prepare"]
        )
        self.assertIn(
            "os-enrichment-prepare",
            [t.kind for t in tasks_from_objects(set(self.client.values), PREFIX)],
        )
        e.prepare(self.client, root, PREFIX, self.work)
        self.assertEqual(e.tasks(set(self.client.values), PREFIX), [])
        c.publish_task_failure(
            self.client,
            Task("os-enrichment-analysis", root + "/request.json", PREFIX),
            "failed",
            self.work,
        )
        self.assertEqual(e.status(self.client, root), "failed")
        self.assertNotIn(core + "/failed.json", self.client.values)
        self.assertTrue(e.retry(self.client, root + "/request.json", PREFIX, self.work))
        self.assertTrue(any("/attempts/" in u for u in self.client.writes))
        self.scan(root)
        self.assertEqual(
            [t.kind for t in e.tasks(set(self.client.values), PREFIX)], ["os-enrichment-analysis"]
        )
        self.client.json(root + "/running.json", dict(status="running"))
        self.assertEqual(e.status(self.client, root), "running")
        self.client.json(root + "/done.json", dict(status="completed"))
        self.assertEqual(e.status(self.client, root), "completed")
        self.assertEqual(e.tasks(set(self.client.values), PREFIX), [])
        with self.assertRaisesRegex(ValueError, "without success"):
            e.retry(self.client, root + "/request.json", PREFIX, self.work)

    def test_legacy_polling_never_consumes_nested_enrichment_requests(self):
        from ipid_analysis.s3_workflow import S3Client

        client = S3Client()
        core = PREFIX + "/analysis-jobs/icmp_2026-10-25_01-00-00"
        objects = [
            core + "/request.json",
            core + "/enrichments/os/request.json",
            core + "/enrichments/os/scan/request.json",
        ]
        with patch.object(client, "objects", return_value=objects):
            self.assertEqual(list(client.list_analysis_requests(PREFIX)), [core + "/request.json"])

    def test_changed_core_scan_integrity_and_real_io_errors_block_completion(self):
        core = self.protocol()
        root = self.request(core)
        e.prepare(self.client, root, PREFIX, self.work)
        self.scan(root)
        for uri, failure in (
            (core + "/manifest.json", ValueError),
            (root + "/raw/os.pq", ValueError),
        ):
            original = self.client.values[uri]
            self.client.values[uri] = b"{}"
            with self.assertRaises(failure):
                e.analyze(self.client, root, PREFIX, self.work, 1)
            self.client.values[uri] = original
        self.client.fail = core + "/done.json"
        with self.assertRaises(ConnectionError):
            e.analyze(self.client, root, PREFIX, self.work, 1)
        self.assertNotIn(root + "/done.json", self.client.values)


if __name__ == "__main__":
    unittest.main()
