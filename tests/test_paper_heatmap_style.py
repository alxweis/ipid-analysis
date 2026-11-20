"""Display rounding uses decimal ties and exact populations, not old labels."""

from decimal import Decimal
import unittest

import numpy as np

from ipid_analysis.paper_heatmap_style import (
    colorbar_gap_points,
    format_percentage,
    format_population,
    round_half_away,
)


class PaperHeatmapStyleTest(unittest.TestCase):
    def test_decimal_ties_away_from_zero_including_negative_values(self):
        for value, places, expected in (
            (2.5, 0, "3"),
            (-2.5, 0, "-3"),
            (6.25, 1, "6.3"),
            (-6.25, 1, "-6.3"),
            (2.35, 1, "2.4"),
            (1.005, 2, "1.01"),
        ):
            with self.subTest(value=value):
                self.assertEqual(round_half_away(value, places), Decimal(expected))

    def test_population_thresholds_no_double_rounding_or_trailing_zeros(self):
        cases = {
            0: "0",
            999: "999",
            1000: "1k",
            2000: "2k",
            35000: "35k",
            57000: "57k",
            57050: "57.1k",
            64700: "64.7k",
            99949: "99.9k",
            99950: "100k",
            100000: "100k",
            100499: "100k",
            100500: "101k",
            156470: "156k",
            156499: "156k",
            156500: "157k",
            156501: "157k",
            999499: "999k",
            999500: "1M",
            1000000: "1M",
            1050000: "1.1M",
            2000000: "2M",
            2050000: "2.1M",
        }
        for value, expected in cases.items():
            with self.subTest(value=value):
                self.assertEqual(format_population(value), expected)
                self.assertEqual(format_population(np.int64(value)), expected)
        for invalid in (-1, 1.5, True):
            with self.assertRaises(ValueError):
                format_population(invalid)

    def test_percentages_preserve_zero_and_small_positive_labels(self):
        self.assertEqual(format_percentage(0, 0), "-")
        for value, expected in (
            (0.00001, "0.0"),
            (0.049, "0.0"),
            (0.05, "0.1"),
            (6.25, "6.3"),
            (85, "85.0"),
            (99.94, "99.9"),
            (99.95, "100"),
            (100, "100"),
        ):
            with self.subTest(value=value):
                self.assertEqual(format_percentage(value, 1), expected)

    def test_gap_is_capped_without_expanding_smaller_preferences(self):
        self.assertEqual(colorbar_gap_points(0), 0)
        self.assertEqual(colorbar_gap_points(1.44), 1.44)
        self.assertEqual(colorbar_gap_points(3), 3)
        self.assertEqual(colorbar_gap_points(4), 4)
        self.assertEqual(colorbar_gap_points(5.04), 4)
        self.assertEqual(colorbar_gap_points(7.2), 4)


if __name__ == "__main__":
    unittest.main()
