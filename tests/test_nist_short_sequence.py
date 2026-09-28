import unittest

import numpy as np

from ipid_analysis.nist_short_sequence import (
    NIST_TEST_NAMES,
    ShortNistNullTables,
    _cumulative_sums,
    _frequency,
    _runs,
    ipids_to_bitstreams,
    nist_component_pvalues,
)


class NistShortSequenceTest(unittest.TestCase):
    def test_published_hundred_bit_examples(self):
        sequence = (
            "11001001000011111101101010100010001000010110100011"
            "00001000110100110001001100011001100010100010111000"
        )
        bits = np.asarray([[int(bit) for bit in sequence]], dtype=np.uint8)

        self.assertAlmostEqual(float(_frequency(bits)[0]), 0.109599, places=6)
        self.assertAlmostEqual(float(_runs(bits)[0]), 0.500798, places=6)
        forward, reverse = _cumulative_sums(bits)
        self.assertAlmostEqual(float(forward[0]), 0.219194, places=6)
        self.assertAlmostEqual(float(reverse[0]), 0.114866, places=6)

    def test_ipid_encoding_is_big_endian_and_omits_missing_values(self):
        values = np.asarray([[0x8001, 0x00FF, 0xAAAA]], dtype=np.uint16)
        present = np.asarray([[True, False, True]])
        bits = ipids_to_bitstreams(values, present)
        expected = "10000000000000011010101010101010"
        np.testing.assert_array_equal(bits[0], np.asarray([int(bit) for bit in expected]))

    def test_components_and_empirical_combination_are_probabilities(self):
        rng = np.random.default_rng(7)
        bits = rng.integers(0, 2, size=(4, 1_280), dtype=np.uint8)
        components = nist_component_pvalues(bits)
        self.assertEqual(tuple(components), NIST_TEST_NAMES)
        for values in components.values():
            self.assertTrue(np.all(np.isfinite(values)))
            self.assertTrue(np.all((values >= 0.0) & (values <= 1.0)))

        combined, _ = ShortNistNullTables(32, seed=9, batch_size=8).combined_pvalues(bits)
        self.assertTrue(np.all((combined > 0.0) & (combined <= 1.0)))

    def test_null_table_is_independent_of_batch_size(self):
        first = ShortNistNullTables(17, seed=11, batch_size=3).minimum(1_280)
        second = ShortNistNullTables(17, seed=11, batch_size=8).minimum(1_280)
        np.testing.assert_array_equal(first, second)


if __name__ == "__main__":
    unittest.main()
