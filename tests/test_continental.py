import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis import continental as c
from ipid_analysis.artifacts import assert_complete
from ipid_analysis.scheduler import Task, tasks_from_objects

PREFIX = "s3://bucket/workflow"
CAMPAIGN = PREFIX + "/continental/round"
RAW = "s3://bucket/raw"


class FakeClient:
    def __init__(self):
        self.values = {}
        self.writes = []
        self.fail = None
        self.fail_upload = None

    def exists(self, uri):
        if uri == self.fail:
            raise PermissionError("AccessDenied")
        return uri in self.values

    def download(self, uri, path):
        if uri == self.fail:
            raise ConnectionError("network failure")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(self.values[uri])

    def upload(self, path, uri):
        assert_complete(path)
        if uri == self.fail or uri == self.fail_upload:
            raise PermissionError("AccessDenied")
        self.values[uri] = path.read_bytes()
        self.writes.append(uri)

    def input_path(self, uri, fallback):
        self.download(uri, fallback)
        return fallback

    def mapped_path(self, uri):
        return None

    def objects(self, prefix):
        return [u for u in self.values if u.startswith(prefix)]

    def delete_failure(self, uri):
        assert uri.endswith("/failed.json")
        del self.values[uri]

    def json(self, uri, value):
        self.values[uri] = json.dumps(value).encode()

    def value(self, uri):
        return json.loads(self.values[uri])


class ContinentalTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.work = self.root / "work"
        self.work.mkdir()
        self.client = FakeClient()
        self.env = patch.dict(
            os.environ,
            {
                "IPID_ANALYSIS_STORAGE_MODE": "local",
                "IPID_ANALYSIS_S3_ROOTS": "{}",
                "IPID_ANALYSIS_TMP_DIR": str(self.root / "scratch"),
                "IPID_ANALYSIS_CONTROL_ROOT": str(self.root / "control"),
            },
        )
        self.env.start()
        self.addCleanup(self.env.stop)
        self.client.json(
            CAMPAIGN + "/campaign.json",
            dict(
                version=1,
                campaign_id="round",
                sites=list(c.SITES),
                protocols=list(c.PROTOCOLS),
                raw_root=RAW,
            ),
        )

    def discoveries(self):
        for si, site in enumerate(c.SITES):
            measurements = {}
            raw = f"{RAW}/continental/round/{site}/zmap"
            for pi, proto in enumerate(c.PROTOCOLS):
                payload = ("icmp", "tcp-80", "udp-dns-53")[pi]
                measurement = payload + "_2026-10-25_01-00-00"
                measurements[proto] = measurement
                path = self.root / "input.pq"
                pq.write_table(
                    pa.table(
                        dict(
                            IP_ADDR=["192.0.2.1", f"198.51.100.{si * 3 + pi + 1}"],
                            REPLY_TYPE=["", ""],
                        )
                    ),
                    path,
                )
                data = path.read_bytes()
                self.client.values[f"{raw}/{measurement}/zmap.pq"] = data
                self.client.json(
                    f"{raw}/{measurement}/measurement-complete.json",
                    dict(
                        version=1,
                        measurement_id=measurement,
                        files={
                            "zmap.pq": dict(
                                size_bytes=len(data), sha256=hashlib.sha256(data).hexdigest()
                            )
                        },
                    ),
                )
            self.client.json(
                f"{CAMPAIGN}/sites/{site}/discovery.json",
                dict(version=1, campaign_id="round", site=site, zmap=measurements, raw_prefix=raw),
            )

    def test_all_eighteen_discoveries_union_and_request_last(self):
        self.discoveries()
        c.union_discovery(self.client, CAMPAIGN, self.work)
        request = self.client.value(CAMPAIGN + "/os/request.json")
        self.assertEqual(request["target"]["rows"], 19)
        self.assertEqual(len(request["members"]), 18)
        self.assertEqual(self.client.writes[-1], CAMPAIGN + "/os/request.json")
        self.assertEqual(len({m["uri"] for m in request["members"]}), 18)
        self.assertEqual(request["reference_zmap"], "tcp-80_2026-10-25_01-00-00")
        # The immutable request is reusable after losing the successful exit.
        c.union_discovery(self.client, CAMPAIGN, self.work)
        self.assertEqual(request, self.client.value(CAMPAIGN + "/os/request.json"))

    def test_missing_discovery_or_bad_binding_or_integrity_cannot_start_os(self):
        self.discoveries()
        uri = CAMPAIGN + "/sites/au/discovery.json"
        original = self.client.values.pop(uri)
        with self.assertRaises(KeyError):
            c.union_discovery(self.client, CAMPAIGN, self.work)
        self.assertNotIn(CAMPAIGN + "/os/request.json", self.client.values)
        self.client.values[uri] = original
        event = self.client.value(uri)
        event["site"] = "eu"
        self.client.json(uri, event)
        with self.assertRaisesRegex(ValueError, "binding"):
            c.union_discovery(self.client, CAMPAIGN, self.work)
        self.client.values[uri] = original
        raw = RAW + "/continental/round/eu/zmap/icmp_2026-10-25_01-00-00/zmap.pq"
        self.client.values[raw] = b"corrupted"
        with self.assertRaisesRegex(ValueError, "integrity"):
            c.union_discovery(self.client, CAMPAIGN, self.work)
        self.assertNotIn(CAMPAIGN + "/os/request.json", self.client.values)

    def test_failed_os_request_upload_does_not_publish_readiness(self):
        self.discoveries()
        self.client.fail_upload = CAMPAIGN + "/os/request.json"
        with self.assertRaises(PermissionError):
            c.union_discovery(self.client, CAMPAIGN, self.work)
        self.assertNotIn(CAMPAIGN + "/os/request.json", self.client.values)
        self.assertIn(CAMPAIGN + "/os/targets.pq", self.client.values)

    def test_scheduler_discovery_and_final_dependencies_are_site_specific(self):
        self.discoveries()
        objects = set(self.client.values)
        missing = objects - {CAMPAIGN + "/sites/au/discovery.json"}
        self.assertNotIn("union", [t.kind for t in tasks_from_objects(missing, PREFIX)])
        self.assertIn("union", [t.kind for t in tasks_from_objects(objects, PREFIX)])
        objects.add(CAMPAIGN + "/os/request.json")
        site = CAMPAIGN + "/sites/eu"
        objects.add(site + "/final/request.json")
        for proto in ("icmp", "tcp-80", "udp-dns-53"):
            objects.add(site + f"/strategy-jobs/{proto}_2026-10-25_01-00-00/request.json")
        self.assertNotIn("submit", [t.kind for t in tasks_from_objects(objects, PREFIX)])
        objects.add(CAMPAIGN + "/os/done.json")
        submit = [t for t in tasks_from_objects(objects, PREFIX) if t.kind == "submit"]
        self.assertEqual(len(submit), 1)
        self.assertEqual(submit[0].scope, "continental/round/eu")
        objects.add(site + "/final/submitted.json")
        self.assertNotIn("site-done", [t.kind for t in tasks_from_objects(objects, PREFIX)])
        objects |= {
            u.replace("/strategy-jobs/", "/analysis-jobs/").replace("/request.json", "/done.json")
            for u in list(objects)
            if "/strategy-jobs/" in u
        }
        objects.add(site + "/interprotocol-jobs/interprotocol/done.json")
        self.assertEqual(
            len([t for t in tasks_from_objects(objects, PREFIX) if t.kind == "site-done"]), 1
        )
        objects.add(site + "/final/failed.json")
        self.assertNotIn("site-done", [t.kind for t in tasks_from_objects(objects, PREFIX)])

    def test_legacy_and_same_second_scoped_requests_are_independent(self):
        uris = {PREFIX + "/jobs/icmp_2026-10-25_01-00-00/request.json"}
        for site in c.SITES:
            uris.add(f"{CAMPAIGN}/sites/{site}/jobs/icmp_2026-10-25_01-00-00/request.json")
        tasks = [t for t in tasks_from_objects(uris, PREFIX) if t.kind == "handoff"]
        self.assertEqual(len(tasks), 7)
        self.assertEqual(len({t.key for t in tasks}), 7)
        self.assertEqual(len({t.scope for t in tasks}), 7)
        for uri in uris:
            uris.add(uri.replace("/request.json", "/done.json"))
            break
        self.assertEqual(len(tasks_from_objects(uris, PREFIX)), 6)

    def test_regional_os_coverage_includes_response_only_targets(self):
        zmap = self.root / "zmap.pq"
        observations = self.root / "os-observations.pq"
        pq.write_table(
            pa.table(
                dict(IP_ADDR=["192.0.2.1", "192.0.2.2", "192.0.2.1"], REPLY_TYPE=["", "", ""])
            ),
            zmap,
        )
        pq.write_table(
            pa.table(
                dict(
                    IP_ADDR=["192.0.2.1", "192.0.2.2", "192.0.2.3"],
                    RESPONSE_MASK=[8, 1, 0],
                    EVIDENCE_MASK=[0, 1, 0],
                    TAG_MASK=[0, 1, 0],
                    OS_STATUS=["no-evidence", "resolved", "no-evidence"],
                    CONFLICT=["", "", ""],
                )
            ),
            observations,
        )
        result = c.regional_coverage(
            zmap, observations, self.root / "coverage.json", provenance={"site": "eu"}
        )
        self.assertEqual(
            result["detail"]["targets"],
            dict(
                total=2,
                with_response=2,
                without_response=0,
                with_evidence=1,
                without_evidence=1,
                with_tag=1,
                without_tag=1,
            ),
        )
        self.assertEqual(result["overview"]["services"]["https"]["responded"], 1)
        self.assertEqual(result["detail"]["classification"]["resolved"], 1)
        pq.write_table(pa.table(dict(IP_ADDR=["203.0.113.1"])), zmap)
        with self.assertRaisesRegex(ValueError, "missing 1"):
            c.regional_coverage(zmap, observations, self.root / "invalid.json", provenance={})

    def test_child_scope_keeps_physical_mount_mapping_and_shared_caches(self):
        scope = "continental/round/eu"
        env = dict(
            os.environ,
            IPID_ANALYSIS_SCOPE=scope,
            IPID_ANALYSIS_WORKFLOW_ROOT=PREFIX,
            IPID_ANALYSIS_S3_PREFIX=CAMPAIGN + "/sites/eu",
            IPID_ANALYSIS_S3_ROOTS=json.dumps({"raw": RAW}),
        )
        env.pop("IPID_ANALYSIS_RIPE_CACHE_S3", None)
        code = "from ipid_analysis.config import *; from ipid_analysis.runtime import persistent_roots; from ipid_analysis.s3_workflow import S3Client; from ipid_analysis.ripe_cache import RoleCache; import json; print(json.dumps(dict(scope=RAW_DATA_DIR.relative_to(RAW_STORAGE_DIR).as_posix(),mapped=str(S3Client().mapped_path('s3://bucket/raw/continental/round/eu/zmap/run/zmap.pq')),expected=str(RAW_DATA_DIR/'zmap/run/zmap.pq'),mounts=[str(p) for p in persistent_roots()],expected_mounts=[str(RAW_STORAGE_DIR),str(PROCESSED_STORAGE_DIR),str(REPORTS_STORAGE_DIR)],cache=RoleCache().prefix)))"
        result = subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            capture_output=True,
            text=True,
            timeout=20,
            check=True,
        )
        value = json.loads(result.stdout.splitlines()[-1])
        self.assertEqual(value["scope"], scope)
        self.assertEqual(value["mapped"], value["expected"])
        self.assertEqual(value["mounts"], value["expected_mounts"])
        self.assertEqual(value["cache"], PREFIX + "/ripe-atlas-cache/v4")

    def test_terminal_failure_preserves_existing_done_and_original_failure(self):
        task = Task(
            "strategies",
            CAMPAIGN + "/sites/eu/strategy-jobs/icmp/request.json",
            CAMPAIGN + "/sites/eu",
        )
        done = task.uri.replace("request.json", "done.json")
        self.client.json(done, {"original": True})
        c.publish_task_failure(self.client, task, "new error", self.work)
        self.assertEqual(self.client.writes, [])
        del self.client.values[done]
        c.publish_task_failure(self.client, task, "original error", self.work)
        c.publish_task_failure(self.client, task, "replacement", self.work)
        self.assertEqual(self.client.value(c.failure_uri(task))["error"], "original error")

    def protocol_events(self, no_os=False, site_code="eu"):
        from ipid_analysis.s3_workflow import AnalysisRequest

        events = []
        manifests = {}
        site = CAMPAIGN + "/sites/" + site_code
        for pi, protocol in enumerate(c.PROTOCOLS):
            payload = ("icmp", "tcp-80", "udp-dns-53")[pi]
            zmap = payload + "_2026-10-25_01-00-00"
            rt = payload + "_2026-10-25_01-00-01"
            mass = payload + "_2026-10-25_01-00-02"
            manifest = {
                protocol: dict(
                    zmap=zmap,
                    os="shared-os",
                    ipid={
                        "no-connection": {
                            "rt-based": {"base": rt},
                            "fixed-interval": {"mass": mass},
                        }
                    },
                )
            }
            job = site + "/analysis-jobs/" + zmap
            raw = RAW + "/continental/round/" + site_code
            request = AnalysisRequest(
                1,
                zmap,
                protocol,
                job + "/manifest.json",
                raw + "/zmap",
                RAW + "/continental/round/os",
                raw + "/ipid",
                job + "/done.json",
                job + "/failed.json",
                "2026-10-25T01:00:00Z",
            )
            if no_os:
                manifest[protocol].pop("os")
                manifest[protocol]["os_status"] = "not_requested"
                from dataclasses import replace

                request = replace(request, os_prefix="", os_status="not_requested")
            self.client.json(job + "/manifest.json", manifest)

            event = site + "/strategy-jobs/" + zmap + "/request.json"
            self.client.json(event, request.payload())
            events.append(event)
            manifests["udp" if protocol == "udp-dns" else protocol] = job + "/manifest.json"
            for mid, ips, labels in [
                (rt, ["192.0.2.1", "192.0.2.2"], ["CONSTANT", "UNCLASSIFIED"]),
                (mass, ["192.0.2.2"], ["RANDOM"]),
            ]:
                local = self.root / "fixture.pq"
                pq.write_table(
                    pa.table(dict(IP_ADDR=ips, IPID_SEQUENCE=["[1,1]"] * len(ips))), local
                )
                prefix = raw + "/ipid/" + mid
                self.client.values[prefix + "/ipid.pq"] = local.read_bytes()
                self.client.values[prefix + "/ipid.snapshot.yaml"] = (
                    b"connection_count: 4\nrequests_per_connection: 4\nrequest_ip_ids: [1,2]\n"
                )
                pq.write_table(pa.table(dict(IP_ADDR=ips, IPID_SELECTION_STRATEGY=labels)), local)
                self.client.values[prefix + "/strategies.pq"] = local.read_bytes()
        return events, manifests

    def test_real_preliminary_merges_and_interprotocol_targets_without_os(self):
        from ipid_analysis import s3_workflow as workflow
        from ipid_analysis import strategies, strategy_merge

        events, manifests = self.protocol_events()
        raw = self.root / "raw"
        processed = self.root / "processed"
        original_classify = strategies.classify_measurement
        original_merge = strategy_merge.merge_strategies

        def classify(m, **options):
            return original_classify(m, raw_root=raw, processed_root=processed, **options)

        def merge(m, **options):
            return original_merge(m, processed_root=processed, **options)

        site = CAMPAIGN + "/sites/eu"
        with (
            patch.object(workflow, "SCOPE", "continental/round/eu"),
            patch.object(c, "RAW_DATA_DIR", raw),
            patch.object(strategies, "classify_measurement", side_effect=classify),
            patch.object(strategy_merge, "merge_strategies", side_effect=merge),
        ):
            for event in events:
                c.prepare_strategies(self.client, event, site, self.work, 10, 1)
                self.assertIn(
                    event.replace("/strategy-jobs/", "/analysis-jobs/").replace(
                        "request.json", "strategies-ready.json"
                    ),
                    self.client.values,
                )
                self.assertNotIn(
                    event.replace("/strategy-jobs/", "/analysis-jobs/").replace(
                        "request.json", "done.json"
                    ),
                    self.client.values,
                )
            job = site + "/interprotocol-target-jobs/interprotocol"
            self.client.json(
                job + "/request.json",
                dict(
                    version=1,
                    job_id="interprotocol",
                    campaign_id="interprotocol",
                    manifests=manifests,
                    target_prefix=job + "/targets",
                    done_uri=job + "/done.json",
                    failed_uri=job + "/failed.json",
                    created_at="2026-10-25T01:00:00Z",
                ),
            )
            self.assertTrue(
                workflow.process_interprotocol_target_request(
                    self.client,
                    job + "/request.json",
                    site,
                    self.work / "targets",
                    output_root=processed / "interprotocol",
                    processed_root=processed,
                    figures_root=self.root / "figures",
                )
            )
        self.assertNotIn(CAMPAIGN + "/os/done.json", self.client.values)
        self.assertEqual(
            self.client.value(job + "/done.json")["rows"],
            {"icmp-tcp": 0, "icmp-udp": 0, "tcp-udp": 0, "icmp-tcp-udp": 2},
        )

    def test_protocol_events_cannot_mix_sites(self):
        from ipid_analysis import s3_workflow as workflow

        events, _ = self.protocol_events()
        event = self.client.value(events[0])
        event["ipid_prefix"] = RAW + "/continental/round/as/ipid"
        self.client.json(events[0], event)
        with patch.object(workflow, "SCOPE", "continental/round/eu"):
            with self.assertRaisesRegex(ValueError, "boundaries"):
                c.protocol_request(self.client, events[0], CAMPAIGN + "/sites/eu", self.work)

    def shared_os(self):
        self.discoveries()
        c.union_discovery(self.client, CAMPAIGN, self.work)
        request = self.client.value(CAMPAIGN + "/os/request.json")
        target = self.root / "target.pq"
        target.write_bytes(self.client.values[request["target"]["uri"]])
        ips = pq.read_table(target)["IP_ADDR"].to_pylist()
        pq.write_table(
            pa.table(
                dict(
                    IP_ADDR=ips,
                    RESPONSE_MASK=[0] * len(ips),
                    EVIDENCE_MASK=[0] * len(ips),
                    TAG_MASK=[0] * len(ips),
                    OS_STATUS=["no-evidence"] * len(ips),
                    CONFLICT=[""] * len(ips),
                )
            ),
            self.root / "os-observations.pq",
        )
        pq.write_table(
            pa.table(
                dict(
                    IP_ADDR=pa.array([], pa.string()),
                    OS_STATUS=pa.array([], pa.string()),
                    OS_TAG=pa.array([], pa.string()),
                )
            ),
            self.root / "os.pq",
        )
        (self.root / "os-coverage.json").write_text(
            json.dumps(dict(schema_version="3", classifier_version="3"))
        )
        (self.root / "os.snapshot.yaml").write_text("record_target_status: true\n")
        files = {}
        for name in ("os.pq", "os-observations.pq", "os-coverage.json", "os.snapshot.yaml"):
            data = (self.root / name).read_bytes()
            uri = RAW + "/continental/round/os/shared-os/" + name
            self.client.values[uri] = data
            files[name] = dict(size_bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
        done = dict(
            version=1,
            campaign_id="round",
            measurement_id="tcp-80_2026-10-25_03-00-00",
            target=request["target"],
            members=request["members"],
            vantage="shared-os-vm",
            files=files,
        )
        self.client.json(CAMPAIGN + "/os/done.json", done)
        return done

    def test_shared_os_submit_and_final_completion_use_own_site_only(self):
        from ipid_analysis import s3_workflow as workflow

        self.shared_os()
        self.protocol_events()
        site = CAMPAIGN + "/sites/eu"
        self.client.json(
            site + "/final/request.json",
            dict(version=1, campaign_id="round", site="eu", interprotocol_job="interprotocol"),
        )
        with (
            patch.object(c, "RAW_STORAGE_DIR", self.root / "raw"),
            patch.object(c, "PROCESSED_DATA_DIR", self.root / "processed"),
            patch.object(workflow, "SCOPE", "continental/round/eu"),
        ):
            c.submit_site(self.client, site, self.work)
            submitted = self.client.value(site + "/final/submitted.json")
            self.assertEqual(self.client.writes[-1], site + "/final/submitted.json")
            for request in submitted["requests"]:
                coverage = json.loads(
                    (
                        self.root / "processed" / request["job_id"] / "shared-os-coverage.json"
                    ).read_text()
                )
                self.assertEqual(coverage["detail"]["targets"]["total"], 2)
                self.assertEqual(coverage["classifier_version"], "3")
                self.assertEqual(coverage["provenance"]["os_vantage"], "shared-os-vm")
                self.assertIn(
                    request["done_uri"].replace("/done.json", "/request.json"), self.client.values
                )
                self.client.json(
                    request["done_uri"],
                    dict(job_id=request["job_id"], manifest_uri=request["manifest_uri"]),
                )
            with self.assertRaisesRegex(ValueError, "interprotocol"):
                c.finish_site(self.client, site, self.work)
            self.client.json(site + "/interprotocol-jobs/interprotocol/done.json", dict(version=1))
            c.finish_site(self.client, site, self.work)
        self.assertIn(site + "/final/done.json", self.client.values)
        self.assertNotIn(CAMPAIGN + "/sites/as/final/done.json", self.client.values)

    def test_shared_os_corruption_and_target_population_mismatch_are_rejected(self):
        done = self.shared_os()
        uri = RAW + "/continental/round/os/shared-os/os-observations.pq"
        original = self.client.values[uri]
        self.client.values[uri] = b"corrupted"
        with patch.object(c, "RAW_STORAGE_DIR", self.root / "raw"):
            with self.assertRaisesRegex(ValueError, "integrity"):
                c.verify_shared_os(self.client, CAMPAIGN, self.work)
        self.client.values[uri] = original
        path = self.root / "changed.pq"
        table = pq.read_table(self.root / "os-observations.pq")
        ips = table["IP_ADDR"].to_pylist()
        ips[0] = "203.0.113.254"
        table = table.set_column(0, "IP_ADDR", pa.array(ips))
        pq.write_table(table, path)
        self.client.values[uri] = path.read_bytes()
        done["files"]["os-observations.pq"] = dict(
            size_bytes=path.stat().st_size, sha256=hashlib.sha256(path.read_bytes()).hexdigest()
        )
        self.client.json(CAMPAIGN + "/os/done.json", done)
        with patch.object(c, "RAW_STORAGE_DIR", self.root / "raw2"):
            with self.assertRaisesRegex(ValueError, "match discovery"):
                c.verify_shared_os(self.client, CAMPAIGN, self.work)

    def test_explicit_continental_retry_archives_failure_without_touching_data(self):
        from ipid_analysis.s3_workflow import retry_failed_request

        uri = CAMPAIGN + "/union/request.json"
        self.client.json(CAMPAIGN + "/union/failed.json", dict(error="original"))
        self.client.values[RAW + "/keep.pq"] = b"keep"
        retry_failed_request(self.client, uri, PREFIX)
        self.assertNotIn(CAMPAIGN + "/union/failed.json", self.client.values)
        archived = [u for u in self.client.values if "/attempts/" in u]
        self.assertEqual(len(archived), 1)
        self.assertEqual(self.client.value(archived[0]), dict(error="original"))
        self.assertEqual(self.client.values[RAW + "/keep.pq"], b"keep")

    def disable_os(self):
        binding = self.client.value(CAMPAIGN + "/campaign.json")
        binding["os_mode"] = "disabled"
        self.client.json(CAMPAIGN + "/campaign.json", binding)
        self.client.json(
            CAMPAIGN + "/os/not-requested.json",
            dict(version=1, campaign_id="round", status="not_requested"),
        )

    def test_no_os_six_sites_and_campaign_complete_without_os_or_global_discovery_wait(self):
        from dataclasses import asdict

        from ipid_analysis import s3_workflow as workflow
        from ipid_analysis import workflow_task

        self.discoveries()
        self.disable_os()
        # Other discoveries have not arrived yet; EU must still be runnable.
        delayed = {}
        for site in c.SITES[1:]:
            uri = f"{CAMPAIGN}/sites/{site}/discovery.json"
            delayed[uri] = self.client.values.pop(uri)
        forbidden = {
            CAMPAIGN + "/os/request.json",
            CAMPAIGN + "/os/done.json",
            CAMPAIGN + "/os/targets.pq",
        }
        original_download = self.client.download
        original_exists = self.client.exists

        def download(uri, path):
            self.assertNotIn(uri, forbidden)
            self.assertNotIn("/os/shared-os/", uri)
            original_download(uri, path)

        def exists(uri):
            self.assertNotIn(uri, forbidden)
            return original_exists(uri)

        self.client.download = download
        self.client.exists = exists
        for index, site_code in enumerate(c.SITES):
            site = f"{CAMPAIGN}/sites/{site_code}"
            if index:
                uri = site + "/discovery.json"
                self.client.values[uri] = delayed[uri]
            self.protocol_events(no_os=True, site_code=site_code)
            self.client.json(
                site + "/final/request.json",
                dict(
                    version=1,
                    campaign_id="round",
                    site=site_code,
                    interprotocol_job="interprotocol",
                ),
            )
            pending = tasks_from_objects(set(self.client.values), PREFIX)
            self.assertFalse(any(t.kind == "union" for t in pending))
            self.assertTrue(
                any(t.kind == "submit" and t.scope.endswith("/" + site_code) for t in pending)
            )
            with (
                patch.object(workflow, "SCOPE", f"continental/round/{site_code}"),
                patch.object(
                    c,
                    "verify_shared_os",
                    side_effect=AssertionError("OS verification in No-OS mode"),
                ),
                patch.object(
                    c, "regional_coverage", side_effect=AssertionError("fake OS coverage")
                ),
            ):
                if index == 0:
                    # Publish one final request, then fail uploading the next.
                    failed_request = (
                        site + "/analysis-jobs/tcp-80_2026-10-25_01-00-00/request.json"
                    )
                    self.client.fail_upload = failed_request
                    with self.assertRaises(PermissionError):
                        c.submit_site(self.client, site, self.work)
                    self.assertNotIn(site + "/final/submitted.json", self.client.values)
                    self.client.fail_upload = None
                    task = Task(
                        "submit",
                        site + "/final/request.json",
                        PREFIX,
                        f"continental/round/{site_code}",
                        10,
                    )
                    c.publish_task_failure(
                        self.client, task, "temporary upload failure", self.work
                    )
                    workflow.retry_failed_request(self.client, task.uri, PREFIX)
                c.submit_site(self.client, site, self.work)
                submitted = self.client.value(site + "/final/submitted.json")
                self.assertEqual(submitted["os_status"], "not_requested")
                self.assertNotIn("shared_os", submitted)
                pending = tasks_from_objects(set(self.client.values), PREFIX)
                self.assertEqual(
                    len(
                        [
                            t
                            for t in pending
                            if t.kind == "analysis" and t.scope.endswith("/" + site_code)
                        ]
                    ),
                    3,
                )

                def finish_analysis(client, uri, *args):
                    request = client.value(uri)
                    client.json(
                        request["done_uri"],
                        dict(
                            job_id=request["job_id"],
                            manifest_uri=request["manifest_uri"],
                            os_status="not_requested",
                        ),
                    )
                    return True

                with (
                    patch.object(workflow, "S3Client", return_value=self.client),
                    patch.object(
                        workflow, "process_analysis_request", side_effect=finish_analysis
                    ),
                ):
                    for request in submitted["requests"]:
                        task = Task(
                            "analysis",
                            request["done_uri"].replace("/done.json", "/request.json"),
                            site,
                            f"continental/round/{site_code}",
                            10,
                        )
                        self.assertEqual(
                            workflow_task.execute(
                                dict(task=asdict(task), batch_size=10, cost={"cpus": 1})
                            ),
                            0,
                        )
                with self.assertRaisesRegex(ValueError, "interprotocol"):
                    c.finish_site(self.client, site, self.work)
                self.client.json(
                    site + "/interprotocol-jobs/interprotocol/done.json", dict(version=1)
                )
                c.finish_site(self.client, site, self.work)
        pending = tasks_from_objects(set(self.client.values), PREFIX)
        completed = [t for t in pending if t.kind == "campaign-done"]
        self.assertEqual(len(completed), 1)
        with patch.object(workflow, "S3Client", return_value=self.client):
            self.assertEqual(
                workflow_task.execute(
                    dict(task=asdict(completed[0]), batch_size=10, cost={"cpus": 1})
                ),
                0,
            )
        done = self.client.value(CAMPAIGN + "/done.json")
        self.assertEqual(done["os_status"], "not_requested")
        self.assertEqual(set(done["sites"]), set(c.SITES))
        self.assertTrue(
            all(site["os_status"] == "not_requested" for site in done["sites"].values())
        )
        self.assertFalse(forbidden.intersection(self.client.values))

    def test_disabled_policy_cannot_dispatch_os_or_accept_enabled_protocol(self):
        from ipid_analysis import s3_workflow as workflow

        self.disable_os()
        events, _ = self.protocol_events()
        with self.assertRaisesRegex(ValueError, "not requested"):
            c.union_discovery(self.client, CAMPAIGN, self.work)
        with self.assertRaisesRegex(ValueError, "not requested"):
            c.verify_shared_os(self.client, CAMPAIGN, self.work)
        with patch.object(workflow, "SCOPE", "continental/round/eu"):
            with self.assertRaises(ValueError):
                c.protocol_request(self.client, events[0], CAMPAIGN + "/sites/eu", self.work)
        binding = self.client.value(CAMPAIGN + "/campaign.json")
        binding["os_mode"] = "failed"
        self.client.json(CAMPAIGN + "/campaign.json", binding)
        with self.assertRaisesRegex(ValueError, "OS policy"):
            c.descriptor(self.client, CAMPAIGN, self.work)

    def test_no_os_preliminary_strategies_and_targets_keep_scientific_results(self):
        from ipid_analysis import s3_workflow as workflow
        from ipid_analysis import strategies, strategy_merge

        self.disable_os()
        events, manifests = self.protocol_events(no_os=True)
        raw, processed = self.root / "raw", self.root / "processed"
        original_classify, original_merge = (
            strategies.classify_measurement,
            strategy_merge.merge_strategies,
        )
        site = CAMPAIGN + "/sites/eu"
        with (
            patch.object(workflow, "SCOPE", "continental/round/eu"),
            patch.object(c, "RAW_DATA_DIR", raw),
            patch.object(c, "PROCESSED_DATA_DIR", processed),
            patch.object(
                strategies,
                "classify_measurement",
                side_effect=lambda m, **opts: original_classify(
                    m, raw_root=raw, processed_root=processed, **opts
                ),
            ),
            patch.object(
                strategy_merge,
                "merge_strategies",
                side_effect=lambda m, **opts: original_merge(m, processed_root=processed, **opts),
            ),
        ):
            for event in events:
                c.prepare_strategies(self.client, event, site, self.work, 10, 1)
                # Simulate a lost process response; committed preliminary work is reusable.
                c.prepare_strategies(self.client, event, site, self.work, 10, 1)
            job = site + "/interprotocol-target-jobs/interprotocol"
            self.client.json(
                job + "/request.json",
                dict(
                    version=1,
                    job_id="interprotocol",
                    campaign_id="interprotocol",
                    manifests=manifests,
                    target_prefix=job + "/targets",
                    done_uri=job + "/done.json",
                    failed_uri=job + "/failed.json",
                    created_at="2026-10-25T01:00:00Z",
                ),
            )
            self.assertTrue(
                workflow.process_interprotocol_target_request(
                    self.client,
                    job + "/request.json",
                    site,
                    self.work / "targets",
                    output_root=processed / "interprotocol",
                    processed_root=processed,
                    figures_root=self.root / "figures",
                )
            )
        self.assertEqual(
            self.client.value(job + "/done.json")["rows"],
            {"icmp-tcp": 0, "icmp-udp": 0, "tcp-udp": 0, "icmp-tcp-udp": 2},
        )


if __name__ == "__main__":
    unittest.main()
