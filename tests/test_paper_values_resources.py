"""Resource budgets, RAM-only computation and reuse of already read strategy data."""

from contextlib import redirect_stderr
import io
import os
import tempfile
import unittest
from unittest.mock import patch

import test_paper_values_completion as completion

from paper_values.paired import connection, connection_config, labels


class ResourcesTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.case = completion.CompletionTest()
        self.case.f = completion.Fixture(self.temp.name)

    def test_existing_resource_environment_controls_only_memory_and_threads(self):
        with patch.dict(
            os.environ, {"IPID_ANALYSIS_DUCKDB_MEMORY": "64GB", "IPID_ANALYSIS_THREADS": "12"}
        ):
            config = connection_config()
        self.assertEqual(config["memory_limit"], "64GB")
        self.assertEqual(config["threads"], 12)
        self.assertEqual(config["temp_directory"], "")
        self.assertEqual(config["max_temp_directory_size"], "0B")

    def test_invalid_thread_budgets_fail(self):
        for value in ("0", "-1", "invalid"):
            with (
                self.subTest(value=value),
                patch.dict(os.environ, {"IPID_ANALYSIS_THREADS": value}),
            ):
                with self.assertRaises(ValueError):
                    connection_config()

    def test_duckdb_really_has_no_spill_directory_or_allowance(self):
        with patch.dict(
            os.environ, {"IPID_ANALYSIS_DUCKDB_MEMORY": "64MB", "IPID_ANALYSIS_THREADS": "3"}
        ):
            with patch(
                "tempfile.TemporaryDirectory", side_effect=AssertionError("no spill directory")
            ):
                with connection() as con:
                    self.assertEqual(
                        con.execute("SELECT current_setting('threads')").fetchone()[0], 3
                    )
                    self.assertEqual(
                        con.execute("SELECT current_setting('temp_directory')").fetchone()[0], ""
                    )
                    self.assertEqual(
                        con.execute(
                            "SELECT current_setting('max_temp_directory_size')"
                        ).fetchone()[0],
                        "0 bytes",
                    )

    def test_exhausted_budget_fails_instead_of_falling_back_to_disk(self):
        with patch.dict(
            os.environ, {"IPID_ANALYSIS_DUCKDB_MEMORY": "1MB", "IPID_ANALYSIS_THREADS": "1"}
        ):
            with patch(
                "tempfile.TemporaryDirectory", side_effect=AssertionError("no spill directory")
            ):
                with self.assertRaisesRegex(ValueError, "file spilling is disabled"):
                    with connection() as con:
                        con.execute(
                            "CREATE TABLE big AS SELECT i, repeat(i::VARCHAR, 1024) AS s "
                            "FROM range(10000) t(i)"
                        )

    def test_repeated_fi_queries_read_the_ram_table_instead_of_parquet(self):
        path, report = self.case.labels([("1", "CONSTANT"), ("2", "UNCLASSIFIED")])
        import json

        with connection() as con:
            labels(con, "cached_fi", path, json.loads(report.read_text()), "9", materialize=True)
            plan = con.execute(
                "EXPLAIN SELECT count(DISTINCT IP_ADDR) FROM cached_fi "
                "WHERE IPID_SELECTION_STRATEGY = 'CONSTANT'"
            ).fetchone()[1]
            self.assertNotIn("PARQUET", plan.upper())
            self.assertEqual(con.execute("SELECT count(*) FROM cached_fi").fetchone()[0], 2)

    def test_fi_and_tcp_after_sources_are_materialized_but_full_rt_is_not(self):
        self.case.fi_pair(protocol="tcp")
        self.case.labels([("1", "PER_CONNECTION")], protocol="tcp", mode="connection")
        with patch("paper_values.paired.labels", wraps=labels) as read:
            self.case.f.run(strict=True)
        by_name = {
            call.args[1]: call.kwargs.get("materialize", False) for call in read.call_args_list
        }
        self.assertTrue(by_name["fi_source"])
        self.assertTrue(by_name["after_source"])
        self.assertFalse(by_name["rt_source"])
        self.assertFalse(by_name["before_source"])

    def test_different_budgets_preserve_values_and_reuse_the_derived_cache(self):
        self.case.fi_pair()
        with patch.dict(
            os.environ, {"IPID_ANALYSIS_DUCKDB_MEMORY": "64MB", "IPID_ANALYSIS_THREADS": "1"}
        ):
            first = self.case.f.run(strict=True)
        previous = self.case.f.output_hashes()
        with patch.dict(
            os.environ, {"IPID_ANALYSIS_DUCKDB_MEMORY": "128MB", "IPID_ANALYSIS_THREADS": "2"}
        ):
            with patch("paper_values.paired.fi_rt", side_effect=AssertionError("must use cache")):
                warm = self.case.f.run(strict=True)
            recomputed = self.case.f.run(strict=True, recompute=True)
        self.assertEqual(warm["cache"]["fi-rt_hits"], 1)
        self.assertEqual(first["values"], warm["values"])
        self.assertEqual(first["values"], recomputed["values"])
        self.assertEqual(previous, self.case.f.output_hashes())

    def test_timings_emit_live_progress_without_changing_output_files(self):
        self.case.fi_pair()
        output = io.StringIO()
        with redirect_stderr(output):
            first = self.case.f.run(strict=True, diagnostics=True)
        self.assertIn("fi-rt: starting:", output.getvalue())
        self.assertIn("fi-rt: completed:", output.getvalue())
        previous = self.case.f.output_hashes()
        output = io.StringIO()
        with redirect_stderr(output):
            second = self.case.f.run(strict=True, diagnostics=True)
        self.assertIn("fi-rt: cache hit:", output.getvalue())
        self.assertEqual(first["values"], second["values"])
        self.assertEqual(previous, self.case.f.output_hashes())
