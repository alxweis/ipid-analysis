import json
import os
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis import local_references as r
from ipid_analysis.artifacts import file_digest
from ipid_analysis.parallel import tasks
from tests.test_parallel import ROOT, CAMPAIGN, SITE
from tests.test_continental import FakeClient
from ipid_analysis.scheduler import Task


def snapshot(protocol="tcp", sample="base"):
    return dict(
        connection_count=4,
        requests_per_connection=4 if sample == "base" else 25,
        measurement_mode="rt-based" if sample == "base" else "fixed-interval",
        fixed_interval=dict(minimum_reply_rate=0.8, request_interval="20ms"),
        interface=dict(ip_a="192.0.2.1", ip_b="192.0.2.2"),
        tcp=dict(establish_connection=False),
        request_ip_ids=[1, 2],
        interprotocol=dict(
            protocols=[protocol],
            sample=sample,
            request_ipid_index="logical",
            retries=0,
            local_reference=True,
            tcp_port=80,
            udp_port=53,
        ),
    )


class LocalReferenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def targets(self, path, ips, labels):
        pq.write_table(pa.table(dict(IP_ADDR=ips, IPID_SELECTION_STRATEGY=labels)), path)

    def test_target_union_preserves_original_cohorts_and_separates_samples(self):
        before = {}
        for group in r.GROUPS:
            path = self.root / f"{group}-targets.pq"
            self.targets(path, ["198.51.100.1", "198.51.100.2"], ["CONSTANT", "RANDOM"])
            before[path] = file_digest(path)
        outputs = r.build_targets(self.root)
        self.assertEqual(len(outputs), 6)
        for path in outputs:
            self.assertEqual(pq.read_table(path).num_rows, 1)
            self.assertEqual(
                pq.read_table(path)["IPID_SELECTION_STRATEGY"][0].as_py(),
                "RANDOM" if "-mass-" in path.name else "CONSTANT",
            )
        self.assertEqual(before, {path: file_digest(path) for path in before})

    def test_reference_classification_keeps_missing_targets(self):
        target, raw, output = [self.root / name for name in ("target.pq", "raw.pq", "result.pq")]
        self.targets(target, ["198.51.100.1", "198.51.100.2"], ["CONSTANT", "CONSTANT"])
        pq.write_table(
            pa.table(
                dict(
                    IP_ADDR=["198.51.100.1"],
                    IPID_SELECTION_STRATEGY=["CONSTANT"],
                    IPID_SEQUENCE=[",".join(["99"] * 16)],
                )
            ),
            raw,
        )
        r.classify_reference(raw, target, snapshot(), "tcp", "base", output)
        rows = pq.read_table(output).to_pylist()
        self.assertEqual([v["LOCAL_STRATEGY"] for v in rows], ["CONSTANT", "NOT_ENOUGH_SAMPLES"])
        self.assertEqual([v["RECEIVED_SAMPLES"] for v in rows], [16, 0])
        self.assertEqual([v["BASELINE_STRATEGY"] for v in rows], ["CONSTANT", "CONSTANT"])
        self.targets(raw, ["203.0.113.9"], ["CONSTANT"])
        with self.assertRaisesRegex(ValueError, "population"):
            r.classify_reference(raw, target, snapshot(), "tcp", "base", output)

    def test_empty_cohort_produces_valid_empty_reference(self):
        target, raw, output = [self.root / name for name in ("target.pq", "raw.pq", "result.pq")]
        schema = pa.schema(
            [
                ("IP_ADDR", pa.string()),
                ("IPID_SELECTION_STRATEGY", pa.string()),
                ("IPID_SEQUENCE", pa.string()),
            ]
        )
        pq.write_table(pa.Table.from_pylist([], schema=schema), raw)
        pq.write_table(pa.Table.from_pylist([], schema=schema.remove(2)), target)
        r.classify_reference(raw, target, snapshot("udp", "mass"), "udp", "mass", output)
        self.assertEqual(pq.read_metadata(output).num_rows, 0)

    def test_reference_requires_identical_layout_and_distinct_sources(self):
        for key, value in [
            ("local_reference", False),
            ("protocols", ["tcp", "udp"]),
            ("retries", 1),
            ("sample", "mass"),
        ]:
            config = snapshot()
            config["interprotocol"][key] = value
            with self.assertRaises(ValueError):
                r.validate_snapshot(config, "tcp", "base")
        config = snapshot()
        config["interface"]["ip_b"] = config["interface"]["ip_a"]
        with self.assertRaises(ValueError):
            r.validate_snapshot(config, "tcp", "base")

    def test_reference_analysis_is_independent_and_resume_skips_success(self):
        uri = SITE + "/reference-jobs/reference-tcp-base/request.json"
        objects = {CAMPAIGN + "/parallel.json", uri}
        self.assertIn("parallel-reference", [t.kind for t in tasks(objects, ROOT)])
        objects.add(uri.replace("request.json", "done.json"))
        self.assertNotIn("parallel-reference", [t.kind for t in tasks(objects, ROOT)])
        comparison = SITE + "/reference-comparison/request.json"
        objects.add(comparison)
        self.assertNotIn("parallel-reference-comparison", [t.kind for t in tasks(objects, ROOT)])
        objects.update(SITE + f"/reference-jobs/{n}/done.json" for n in r.names())
        objects.add(SITE + "/interprotocol-jobs/interprotocol/done.json")
        self.assertIn("parallel-reference-comparison", [t.kind for t in tasks(objects, ROOT)])

    def test_comparison_retains_baseline_local_interleaved_and_missing(self):
        outcomes = self.root / "outcomes.pq"
        pq.write_table(
            pa.table(
                dict(
                    IP_ADDR=["198.51.100.1", "198.51.100.2"],
                    PROTOCOL_GROUP=["icmp-tcp"] * 2,
                    BASELINE_STRATEGY=["SINGLE"] * 2,
                    SAMPLE=["base"] * 2,
                    OUTCOME=["STRATEGY_CHANGED"] * 2,
                    PROTOCOLS=[["icmp", "tcp"]] * 2,
                    OBSERVED_STRATEGIES=[["CONSTANT", "SINGLE"], ["SINGLE", "SINGLE"]],
                    RECEIVED_SAMPLES=[[16, 16]] * 2,
                )
            ),
            outcomes,
        )
        refs = {}
        for protocol in r.PROTOCOLS:
            for sample in r.SAMPLES:
                path = self.root / f"{protocol}-{sample}.pq"
                pq.write_table(
                    pa.Table.from_pylist(
                        [
                            dict(
                                IP_ADDR="198.51.100.1",
                                BASELINE_STRATEGY="SINGLE",
                                LOCAL_STRATEGY="CONSTANT",
                                RECEIVED_SAMPLES=16,
                                EXPECTED_SAMPLES=16,
                            )
                        ]
                        if sample == "base"
                        else [],
                        schema=r.SCHEMA,
                    ),
                    path,
                )
                refs[protocol, sample] = path
        original = file_digest(outcomes)
        output = r.compare_files(outcomes, refs, self.root / "comparison.pq")
        rows = pq.read_table(output).to_pylist()
        self.assertEqual(len(rows), 4)
        changed = [v for v in rows if v["IP_ADDR"] == "198.51.100.1" and v["PROTOCOL"] == "tcp"][0]
        self.assertEqual(
            (
                changed["BASELINE_STRATEGY"],
                changed["LOCAL_STRATEGY"],
                changed["INTERLEAVED_STRATEGY"],
                changed["LOCAL_TO_INTERLEAVED"],
            ),
            ("SINGLE", "CONSTANT", "SINGLE", "CHANGED"),
        )
        self.assertEqual(sum(v["LOCAL_TO_INTERLEAVED"] == "NOT_ENOUGH_SAMPLES" for v in rows), 2)
        self.assertEqual(file_digest(outcomes), original)
        self.assertEqual(json.loads(output.with_suffix(".json").read_text())["version"], 1)

    def test_changed_source_or_probe_parameters_prevent_comparison(self):
        import copy

        control = snapshot()
        interleaved = copy.deepcopy(control)
        interleaved["interprotocol"].update(local_reference=False, protocols=["icmp", "tcp"])
        r.assert_comparable(interleaved, control, "tcp", "base")
        for key, value in [
            ("interface", dict(ip_a="192.0.2.3", ip_b="192.0.2.4")),
            ("request_ip_ids", [5, 6]),
            ("tcp", dict(establish_connection=True)),
        ]:
            changed = copy.deepcopy(interleaved)
            changed[key] = value
            with self.assertRaises(ValueError):
                r.assert_comparable(changed, control, "tcp", "base")

    def test_mass_control_retains_missing_positions_and_uses_mass_classifier(self):
        target, raw, output = [self.root / name for name in ("target.pq", "raw.pq", "result.pq")]
        self.targets(target, ["198.51.100.1"], ["MULTI"])
        sequence = [str(100 + (i % 2) * 100) for i in range(80)] + ["-"] * 20
        pq.write_table(
            pa.table(
                dict(
                    IP_ADDR=["198.51.100.1"],
                    IPID_SELECTION_STRATEGY=["MULTI"],
                    IPID_SEQUENCE=[",".join(sequence)],
                )
            ),
            raw,
        )
        r.classify_reference(raw, target, snapshot("udp", "mass"), "udp", "mass", output)
        row = pq.read_table(output).to_pylist()[0]
        self.assertEqual((row["RECEIVED_SAMPLES"], row["EXPECTED_SAMPLES"]), (80, 100))
        self.assertNotEqual(row["LOCAL_STRATEGY"], "NOT_ENOUGH_SAMPLES")

    def test_continent_final_waits_for_local_comparison_only(self):
        objects = {
            CAMPAIGN + "/parallel.json",
            CAMPAIGN + "/local-references.json",
            SITE + "/final/request.json",
            SITE + "/interprotocol-jobs/interprotocol/done.json",
        }
        objects.update(
            SITE + f"/protocols/{p}/analysis-jobs/{p}_1/done.json"
            for p in ("icmp", "tcp", "udp-dns")
        )
        self.assertNotIn("parallel-site", [t.kind for t in tasks(objects, ROOT)])
        objects.add(SITE + "/reference-comparison/done.json")
        self.assertIn("parallel-site", [t.kind for t in tasks(objects, ROOT)])

    def test_protocol_target_manifests_bind_to_the_same_continent_and_protocol(self):
        from ipid_analysis import s3_workflow as workflow

        prefix = SITE + "/interprotocol-target-jobs/interprotocol"
        data = dict(
            version=1,
            job_id="interprotocol",
            campaign_id="interprotocol",
            created_at="2026-11-01T00:00:00Z",
            target_prefix=prefix + "/targets",
            done_uri=prefix + "/done.json",
            failed_uri=prefix + "/failed.json",
            local_references=True,
            manifests={
                p: SITE
                + f"/protocols/{'udp-dns' if p == 'udp' else p}/analysis-jobs/{p}_1/manifest.json"
                for p in r.PROTOCOLS
            },
        )
        with patch.object(workflow, "SCOPE", "continental/round/eu"):
            workflow.InterprotocolTargetRequest.parse(data, SITE)
            data["manifests"]["tcp"] = data["manifests"]["tcp"].replace("/sites/eu/", "/sites/as/")
            with self.assertRaisesRegex(ValueError, "boundaries"):
                workflow.InterprotocolTargetRequest.parse(data, SITE)

    def test_reference_event_consumes_verified_inputs_and_publishes_success_last(self):
        import yaml

        client = FakeClient()
        rawprefix = "s3://bucket/raw/continental/round/eu/ipid"
        mid = "interprotocol-tcp_2026-11-01_01-00-00_base"
        targeturi = (
            SITE + "/interprotocol-target-jobs/interprotocol/targets/reference-tcp-base-targets.pq"
        )
        target = self.root / "targets.pq"
        source = self.root / "raw.pq"
        config = self.root / "snapshot.yaml"
        self.targets(target, ["198.51.100.1"], ["CONSTANT"])
        pq.write_table(
            pa.table(
                dict(
                    IP_ADDR=["198.51.100.1"],
                    IPID_SELECTION_STRATEGY=["CONSTANT"],
                    IPID_SEQUENCE=[",".join(["99"] * 16)],
                )
            ),
            source,
        )
        config.write_text(yaml.safe_dump(snapshot()))
        evidence = lambda path: dict(size_bytes=path.stat().st_size, sha256=file_digest(path))
        client.values[targeturi] = target.read_bytes()
        for filename, path in [
            ("interprotocol.pq", source),
            ("interprotocol.snapshot.yaml", config),
        ]:
            client.values[f"{rawprefix}/{mid}/{filename}"] = path.read_bytes()
        client.json(
            f"{rawprefix}/{mid}/measurement-complete.json",
            dict(
                version=1,
                measurement_id=mid,
                files={
                    "interprotocol.pq": evidence(source),
                    "interprotocol.snapshot.yaml": evidence(config),
                },
            ),
        )
        client.json(
            CAMPAIGN + "/campaign.json",
            dict(
                version=1,
                campaign_id="round",
                sites=["eu", "as", "af", "na", "sa", "au"],
                protocols=["icmp", "tcp", "udp-dns"],
                raw_root="s3://bucket/raw",
            ),
        )
        client.json(
            CAMPAIGN + "/parallel.json",
            dict(version=1, mode="parallel-protocols", campaign_id="round", local_references=True),
        )
        uri = SITE + "/reference-jobs/reference-tcp-base/request.json"
        event = dict(
            version=1,
            campaign_id="round",
            site="eu",
            protocol="tcp",
            sample="base",
            measurement_id=mid,
            raw_prefix=rawprefix,
            target=dict(uri=targeturi, **evidence(target)),
        )
        client.json(uri, event)
        task = Task("parallel-reference", uri, SITE, "continental/round/eu", 10)
        with patch.object(r, "PROCESSED_DATA_DIR", self.root / "processed"):
            r.analyze(client, task, self.root / "work", 32)
            self.assertEqual(client.writes[-1], uri.replace("request.json", "done.json"))
            event["site"] = "as"
            client.json(uri, event)
            with self.assertRaisesRegex(ValueError, "ownership"):
                r.analyze(client, task, self.root / "work2", 32)

    def test_reference_retry_archives_its_failure_and_preserves_success(self):
        workflow = "s3://bucket/reference-workflow"
        site = workflow + "/continental/round/sites/eu"
        env = dict(
            IPID_ANALYSIS_STORAGE_MODE="local",
            IPID_ANALYSIS_CONTROL_ROOT=str(self.root / "control"),
            IPID_ANALYSIS_TMP_DIR=str(self.root / "scratch"),
        )
        for relative in ("reference-jobs/reference-tcp-base", "reference-comparison"):
            client = FakeClient()
            uri = site + "/" + relative + "/request.json"
            failed = uri.replace("request.json", "failed.json")
            done = uri.replace("request.json", "done.json")
            client.json(uri, dict(version=1))
            client.json(failed, dict(error="upload failed"))
            old = client.values[failed]
            with patch.dict(os.environ, env), patch.object(r, "binding") as check:
                self.assertTrue(r.retry(client, uri, workflow))
                check.assert_called_once()
                self.assertEqual(check.call_args.args[1], site)
                self.assertNotIn(failed, client.values)
                archives = [u for u in client.values if "/attempts/" in u]
                self.assertEqual(len(archives), 1)
                self.assertEqual(client.values[archives[0]], old)
                client.json(done, dict(version=1))
                client.json(failed, dict(error="keep"))
                with self.assertRaises(ValueError):
                    r.retry(client, uri, workflow)
                self.assertIn(failed, client.values)


if __name__ == "__main__":
    unittest.main()
