"""Parallel preparation must preserve serial seeds, observations and decisions."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np

from ipid_analysis import classifier_validation_parallel as parallel
from ipid_analysis import classifier_validation_preparation as data
from ipid_analysis.empirical_random_uniformity import EmpiricalNullTables
from ipid_analysis.nist_short_sequence import ShortNistNullTables
from ipid_analysis.random_classifier_candidate import create_candidate_null_tables


class ValidationParallelTest(unittest.TestCase):
    def test_gap_reference_matches_serial(self):
        with ThreadPoolExecutor(4) as pool:
            for n in (80, 100):
                expected = EmpiricalNullTables(57, 123, batch_size=7).spacing(n)
                actual = parallel.build_gap_null(
                    EmpiricalNullTables(57, 123, batch_size=7), n, pool, 4
                )
                np.testing.assert_array_equal(actual, expected)

    def test_nist_reference_matches_serial(self):
        with ThreadPoolExecutor(4) as pool:
            for n in (1280, 1600):
                expected = ShortNistNullTables(43, 123, batch_size=7).minimum(n)
                actual = parallel.build_nist_null(
                    ShortNistNullTables(43, 123, batch_size=7), n, pool, 4
                )
                np.testing.assert_array_equal(actual, expected)

    def test_e2e_serial_parallel_counts_identical(self):
        tables = create_candidate_null_tables(64)
        for n in (80, 100):
            tables.gap.spacing(n)
        expected = data.e2e_metrics(16, 42, tables)
        with ThreadPoolExecutor(4) as pool:
            actual = data.e2e_metrics(16, 42, tables, pool=pool, workers=4)
        self.assertEqual(actual, expected)

    def test_resource_limits(self):
        with (
            patch.object(parallel.runtime, "cpu_budget", return_value=24),
            patch.object(parallel.runtime, "available_memory", return_value=256 << 30),
        ):
            self.assertEqual(parallel.worker_count(16, 100000), 16)
            with self.assertRaises(ValueError):
                parallel.worker_count(0, 100000)
        with (
            patch.object(parallel.runtime, "cpu_budget", return_value=4),
            patch.object(parallel.runtime, "available_memory", return_value=256 << 30),
        ):
            self.assertEqual(parallel.worker_count(16, 100000), 4)
        with (
            patch.object(parallel.runtime, "cpu_budget", return_value=24),
            patch.object(parallel.runtime, "available_memory", return_value=3 << 30),
        ):
            self.assertLess(parallel.worker_count(16, 100000), 16)

    def test_null_cache_reuse(self):
        with tempfile.TemporaryDirectory() as directory:
            import pyarrow.parquet as pq

            root = Path(directory)
            tables = create_candidate_null_tables(32)
            nist = ShortNistNullTables(32, 123, 8)

            def publish(path, table, inputs, parameters):
                if not path.exists():
                    pq.write_table(table, path)

            with ThreadPoolExecutor(2) as pool:
                paths = parallel.prepare_nulls(root, tables, nist, [], {}, publish, pool, 2)
            self.assertEqual(len(paths), 4)
            tables2 = create_candidate_null_tables(32)
            with (
                patch.object(parallel, "build_gap_null", side_effect=AssertionError("must reuse")),
                patch.object(
                    parallel, "build_nist_null", side_effect=AssertionError("must reuse")
                ),
            ):
                parallel.prepare_nulls(
                    root, tables2, ShortNistNullTables(32, 123, 8), [], {}, publish
                )
            np.testing.assert_array_equal(tables.gap.spacing(80), tables2.gap.spacing(80))
            self.assertFalse(tables2.gap.spacing(80).flags.writeable)


if __name__ == "__main__":
    unittest.main()
