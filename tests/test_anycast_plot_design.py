"""Anycast-only display changes preserve the underlying numerical series."""

from copy import deepcopy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ipid_analysis import paper_probing_plot as probing
from ipid_analysis.paper_figures import CONTINENT_ORDER, MODES


class AnycastPlotDesignTest(unittest.TestCase):
    def test_count_units_rounding_and_significant_zeros(self):
        for value, expected in (
            (0, "0"),
            (787, "787"),
            (999, "999"),
            (1000, "1k"),
            (1050, "1.1k"),
            (7857, "7.9k"),
            (23700, "23.7k"),
            (23750, "23.8k"),
            (53000, "53k"),
            (99999, "100k"),
            (100000, "100k"),
            (120000, "120k"),
            (234000, "234k"),
            (234500, "235k"),
            (999999, "1000k"),
            (1000000, "1M"),
            (1200000, "1.2M"),
            (1250000, "1.3M"),
            (76000000, "76M"),
        ):
            with self.subTest(value=value):
                self.assertEqual(probing._anycast_count(value), expected)

    def test_all_four_variants_only_show_rt_quartiles_and_format_actual_counts(self):
        from matplotlib.figure import Figure

        counts = (787, 7857, 23700, 53000, 100000, 234000, 1200000)
        labels = ("787", "7.9k", "23.7k", "53k", "100k", "234k", "1.2M")
        rows = [
            dict(
                CONTINENT_CODE=code,
                MODE=mode,
                IP_COUNT_UNION=count,
                QUANTILES_MS=[float(n + 1) for n in range(256)],
                Q1_MS=64.0,
                MEDIAN_MS=128.0,
                Q3_MS=192.0,
            )
            for code, count in zip(CONTINENT_ORDER, counts)
            for mode in MODES
        ]
        original = deepcopy(rows)
        savefig = Figure.savefig

        def inspect(fig, *args, **kwargs):
            ax = fig.axes[0]
            self.assertEqual(len(ax.lines), 3 * len(CONTINENT_ORDER))
            for line, row in zip(
                ax.lines, [r for r in rows if r["MODE"] == MODES[0] for _ in range(3)]
            ):
                center = CONTINENT_ORDER.index(row["CONTINENT_CODE"])
                self.assertLessEqual(max(line.get_xdata()), center)
            self.assertEqual(
                [label.get_text().split("\n")[-1] for label in ax.get_xticklabels()],
                [f"({label})" for label in labels],
            )
            return savefig(fig, *args, **kwargs)

        with tempfile.TemporaryDirectory() as directory:
            for variant in ("icmp", "tcp-no-conn", "udp", "tcp-conn-full"):
                with self.subTest(variant=variant), patch.object(Figure, "savefig", inspect):
                    output = Path(directory) / (variant + ".pdf")
                    kind = probing.KIND + ("-connection" if variant == "tcp-conn-full" else "")
                    probing.draw(kind + "-anycast", rows, output)
                    self.assertTrue(output.read_bytes().startswith(b"%PDF-"))
        self.assertEqual(rows, original)
