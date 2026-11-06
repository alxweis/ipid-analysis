from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from ipid_analysis import network_roles, postprocess, ripe_atlas


class PreparationReached(Exception):
    pass


class RipeLookbackCLITest(unittest.TestCase):
    def test_all_entry_points_share_lookback_and_cli_precedence(self):
        for module, option in (
            (ripe_atlas, "--lookback-days"),
            (postprocess, "--ripe-lookback-days"),
            (network_roles, "--ripe-lookback-days"),
        ):
            for environment, override, expected in ((None, None, 14), ("28", None, 28), ("28", "7", 7)):
                with self.subTest(module=module.__name__, environment=environment, override=override):
                    with ExitStack() as stack:
                        stack.enter_context(patch.object(module, "start_run"))
                        stack.enter_context(patch.object(module, "logger", MagicMock()))
                        if hasattr(module, "validate_storage"):
                            stack.enter_context(patch.object(module, "validate_storage"))
                        if module is ripe_atlas:
                            args = ["--campaign-start", "2026-11-07T18:30:00Z"]
                        else:
                            stack.enter_context(patch.object(module, "load_manifest", return_value={}))
                            stack.enter_context(patch.object(module, "infer_campaign_start", return_value=datetime(2026, 11, 7, tzinfo=timezone.utc)))
                            args = ["manifest.json", "--skip-itdk"]
                        prepare = stack.enter_context(patch.object(module, "prepare_ripe_atlas", side_effect=PreparationReached()))
                        if override is not None:
                            args.extend([option, override])
                        result = CliRunner().invoke(module.app, args, env={"IPID_ANALYSIS_RIPE_LOOKBACK_DAYS": environment})
                        self.assertIsInstance(result.exception, PreparationReached, result.output)
                        self.assertEqual(prepare.call_args.kwargs["lookback_days"], expected)

    def test_invalid_days_rejected_before_preparation(self):
        for module, args in (
            (ripe_atlas, ["--campaign-start", "2026-11-07T18:30:00Z"]),
            (postprocess, ["manifest.json"]),
            (network_roles, ["manifest.json"]),
        ):
            for invalid in ("0", "-1", "not-a-number"):
                with self.subTest(module=module.__name__, value=invalid):
                    with patch.object(module, "prepare_ripe_atlas") as prepare:
                        result = CliRunner().invoke(module.app, args, env={"IPID_ANALYSIS_RIPE_LOOKBACK_DAYS": invalid})
                    self.assertEqual(result.exit_code, 2, result.output)
                    prepare.assert_not_called()

    def test_28_day_preparation_reuses_completed_lookup_without_reparsing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = root / "sources"
            sources.mkdir()
            # One valid trace is sufficient to exercise the real parser/cache.
            timestamp = int(datetime(2026, 10, 15, tzinfo=timezone.utc).timestamp())
            import json

            (sources / "trace.jsonl").write_text(json.dumps({
                "type": "traceroute", "af": 4, "timestamp": timestamp,
                "dst_addr": "1.1.1.1", "result": [
                    {"hop": 1, "result": [{"from": "8.8.8.8", "rtt": 1.0}]},
                    {"hop": 2, "result": [{"from": "1.1.1.1", "rtt": 2.0}]},
                ],
            }) + "\n")
            options = dict(
                campaign_start=datetime(2026, 11, 7, 18, 30, tzinfo=timezone.utc),
                lookback_days=28, input_dir=sources,
                raw_root=root / "raw", processed_root=root / "processed", preprocess_workers=1,
            )
            with patch.dict("os.environ", {"IPID_ANALYSIS_RIPE_CACHE_S3": ""}):
                first = ripe_atlas.prepare_ripe_atlas(**options)
                self.assertEqual(json.loads(first.metadata_path.read_text())["lookback_days"], 28)
                self.assertEqual(first.window.start.isoformat(), "2026-10-10T00:00:00+00:00")
                with patch.object(ripe_atlas, "build_role_lookup", side_effect=AssertionError("cache was rebuilt")), patch.object(ripe_atlas, "_local_files", side_effect=AssertionError("raw inputs were requested")):
                    options["campaign_start"] = datetime(2026, 11, 7, 23, tzinfo=timezone.utc)
                    second = ripe_atlas.prepare_ripe_atlas(**options)
                self.assertEqual(first, second)
