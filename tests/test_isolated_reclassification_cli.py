from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from typer.testing import CliRunner

from ipid_analysis.manifest import IpidMeasurement
from ipid_analysis.plot_strategies import app as plot_app
from ipid_analysis.strategies import app as strategies_app


class IsolatedReclassificationCliTest(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()
        self.measurement = IpidMeasurement(
            protocol="tcp",
            connection_mode="no-connection",
            interval="fixed-interval",
            scale="mass",
            measurement_id="tcp-mass",
            zmap_id="tcp-zmap",
        )

    def test_strategies_cli_forwards_isolated_processed_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.json"
            processed_root = root / "reclassification-v7"
            with (
                patch("ipid_analysis.strategies.load_manifest", return_value={}),
                patch("ipid_analysis.strategies.resolve", return_value=self.measurement),
                patch(
                    "ipid_analysis.strategies.classify_measurement",
                    return_value=processed_root / "strategies.pq",
                ) as classify,
            ):
                result = self.runner.invoke(
                    strategies_app,
                    [
                        self.measurement.target,
                        "--manifest",
                        str(manifest),
                        "--processed-root",
                        str(processed_root),
                        "--reclassify",
                    ],
                )

            self.assertEqual(result.exit_code, 0, result.output)
            classify.assert_called_once_with(
                self.measurement,
                batch_size=1_000_000,
                compression="zstd",
                threads=0,
                reclassify=True,
                processed_root=processed_root,
            )

    def test_plot_cli_forwards_isolated_input_and_figure_roots(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.json"
            processed_root = root / "reclassification-v7"
            figures_root = root / "reclassification-v7-figures"
            pdf_path = figures_root / "strategies.pdf"
            json_path = figures_root / "strategies.json"
            with (
                patch("ipid_analysis.plot_strategies.load_manifest", return_value={}),
                patch("ipid_analysis.plot_strategies.resolve", return_value=self.measurement),
                patch(
                    "ipid_analysis.plot_strategies.render",
                    return_value=(pdf_path, json_path),
                ) as render,
            ):
                result = self.runner.invoke(
                    plot_app,
                    [
                        self.measurement.target,
                        "--manifest",
                        str(manifest),
                        "--processed-root",
                        str(processed_root),
                        "--figures-root",
                        str(figures_root),
                    ],
                )

            self.assertEqual(result.exit_code, 0, result.output)
            render.assert_called_once_with(
                self.measurement,
                processed_root=processed_root,
                figures_root=figures_root,
            )


if __name__ == "__main__":
    unittest.main()
