import ipaddress
from pathlib import Path
import random
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis import ipv4_union as u
from ipid_analysis.artifacts import atomic_output


class IPv4UnionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def partition(self, name, values, replies=None):
        path = self.root / name
        pq.write_table(pa.table({
            "IP_ADDR": pa.array(values, type=pa.string()),
            "REPLY_TYPE": replies if replies is not None else [""] * len(values),
        }), path)
        return path

    def test_exact_union_across_batches_files_and_full_ipv4_boundaries(self):
        rng = random.Random(42)
        numbers = [rng.randrange(1 << 32) for _ in range(10000)]
        # All eight bits in one byte, row/block boundaries and uint32 extrema.
        numbers += list(range(16)) + [255, 256, 262143, 262144, (1 << 31), (1 << 32) - 1]
        values = [str(ipaddress.IPv4Address(n)) for n in numbers]
        inputs = [
            self.partition("a.pq", values + values[:100]),
            self.partition("b.pq", values[::-1]),
            self.partition("empty.pq", []),
        ]
        output = self.root / "union.pq"
        with patch.object(u, "BATCH_ROWS", 1024):
            rows = u.write_union(inputs, output)
        actual = pq.read_table(output)
        self.assertEqual(actual.schema, u.SCHEMA)
        expected = [str(ipaddress.IPv4Address(n)) for n in sorted(set(numbers))]
        self.assertEqual(actual.column("IP_ADDR").to_pylist(), expected)
        self.assertEqual(actual.column("REPLY_TYPE").to_pylist(), [""] * len(expected))
        self.assertEqual(rows, len(expected))
        self.assertLessEqual(pq.read_metadata(output).row_group(0).num_rows, 1024)

    def test_rejects_invalid_ipv4_instead_of_changing_population(self):
        for value in (None, "", "::1", "1.2.3", "01.2.3.4", "256.2.3.4",
                      "1.2.3.4 ", "1.2.3.4\n", "-1.2.3.4", "1.2.3.4.5"):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "IPv4"):
                u.addresses(pa.array(["1.2.3.4", value], type=pa.string()))

    def test_invalid_late_partition_does_not_replace_published_target(self):
        output = self.root / "published.pq"
        output.write_bytes(b"previous committed target")
        inputs = [self.partition("good.pq", ["1.2.3.4"]),
                  self.partition("bad.pq", ["not an IP"])]
        with self.assertRaisesRegex(ValueError, "IPv4"):
            with atomic_output(output, inputs=inputs) as temporary:
                u.write_union(inputs, temporary)
        self.assertEqual(output.read_bytes(), b"previous committed target")

    def test_empty_union_and_non_normalized_partitions_fail(self):
        empty = self.partition("empty.pq", [])
        with self.assertRaisesRegex(ValueError, "empty discovery union"):
            u.write_union([empty], self.root / "union.pq")
        for replies in (["synack"], [None]):
            source = self.partition("bad.pq", ["1.2.3.4"], replies)
            with self.assertRaisesRegex(ValueError, "REPLY_TYPE"):
                u.write_union([source], self.root / "union.pq")
        pq.write_table(pa.table({"IP_ADDR": ["1.2.3.4"]}), empty)
        with self.assertRaisesRegex(ValueError, "columns"):
            u.write_union([empty], self.root / "union.pq")
