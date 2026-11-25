"""Counter cohort semantics and the approved combined paper layout."""

import unittest
from unittest.mock import patch

from ipid_analysis.paper_interprotocol_combined import KIND, PANELS, draw, segment_label
from paper_values.review import GROUPS


def summary():
    groups = {}
    for group in GROUPS:
        counts = {}
        for strategy, _ in PANELS:
            partial = 100 if group == "icmp-tcp-udp" else 0
            counts.update(
                {
                    strategy + ":PROTOCOL_SHARED": 600 - partial,
                    strategy + ":PROTOCOL_ISOLATED": 250,
                    strategy + ":STRATEGY_CHANGED": 90,
                    strategy + ":NOT_ENOUGH_SAMPLES": 60,
                }
            )
            if partial:
                counts[strategy + ":PARTIAL_PROTOCOL_SHARED"] = partial
        groups[group] = dict(counts=counts, rows=3000)
    return dict(version=2, groups=groups, rows=12000)


class CombinedUpSetTest(unittest.TestCase):
    def test_exact_threshold_precedes_half_away_rounding(self):
        self.assertIsNone(segment_label(999, 10000))  # 9.99 rounds to 10, still hidden.
        self.assertEqual(segment_label(1000, 10000), "10")
        self.assertEqual(segment_label(1050, 10000), "11")
        self.assertEqual(segment_label(9950, 10000), "100")
        self.assertIsNone(segment_label(0, 0))

    def render(self, data):
        with patch("ipid_analysis.paper_interprotocol_combined._save") as save:
            draw(KIND, data, None)
        return save.call_args.args[0]

    def test_cohort_denominators_include_insufficient_and_matrix_is_same_population(self):
        fig = self.render(summary())
        self.assertEqual(tuple(fig.get_size_inches()), (7.0, 2.2))
        titles = [t.get_text() for t in fig.texts if t.get_text() in dict(PANELS).values()]
        self.assertEqual(titles, [title for _, title in PANELS])
        for panel in range(3):
            bars, matrix = fig.axes[2 * panel : 2 * panel + 2]
            for column in range(4):
                heights = [p.get_height() for p in bars.patches[column::4]]
                self.assertAlmostEqual(sum(heights), 100)
                self.assertAlmostEqual(heights[-1], 6)  # NES remains in denominator.
                self.assertAlmostEqual(heights[0], 50 if column == 3 else 60)
            self.assertEqual(
                [t.get_text() for t in matrix.get_yticklabels()],
                ["ICMP (3k)", "TCP (3k)", "UDP (3k)"],
            )
            labels = [t.get_text() for t in bars.texts]
            self.assertEqual(labels.count("25"), 4)
            self.assertNotIn("9", labels)
            self.assertNotIn("6", labels)
            for spine in bars.spines.values():
                self.assertAlmostEqual(spine.get_linewidth(), 0.45)
            self.assertEqual(len(bars.get_xticks()), 4)
            for tick in (*bars.xaxis.get_major_ticks(), *bars.yaxis.get_major_ticks()):
                self.assertAlmostEqual(tick.tick1line.get_markeredgewidth(), 0.45)
            for grid in bars.get_ygridlines():
                self.assertEqual(grid.get_color(), "#BDBDBD")
                self.assertEqual(grid.get_linestyle(), "--")
                self.assertAlmostEqual(grid.get_linewidth(), 0.35)
                self.assertAlmostEqual(grid.get_alpha(), 0.7)
        self.assertEqual(len(fig.artists), 5)  # Single legend, not three copies.
        for square in fig.artists:
            self.assertAlmostEqual(square.get_width() * 7 * 72, 4)
            self.assertAlmostEqual(square.get_height() * 2.2 * 72, 4)

    def test_missing_groups_and_empty_counter_panels_do_not_divide_by_zero(self):
        data = dict(
            version=2,
            groups={"icmp-tcp": dict(counts={"PER_DESTINATION:NOT_ENOUGH_SAMPLES": 2}, rows=2)},
            rows=2,
        )
        fig = self.render(data)
        self.assertEqual([t.get_text() for t in fig.axes[0].texts], ["100"])
        for panel in (1, 2):
            self.assertFalse(fig.axes[2 * panel].texts)
            self.assertTrue(all(p.get_height() == 0 for p in fig.axes[2 * panel].patches))


if __name__ == "__main__":
    unittest.main()
