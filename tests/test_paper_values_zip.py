"""The figure ZIP exactly mirrors values.tex and never modifies analysis inputs."""

import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from paper_values.figure_zip import bundle, figure_paths
from paper_values.tex import render


def stamp(path):
    return path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest()


class FigureZipTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        (self.root / "tmp").mkdir()
        self.values = self.root / "tmp/values.tex"
        self.output = self.root / "tmp/paper-figures.zip"
        self.original = (
            "reports/continental/geo-full/eu/figures/icmp_run/no-connection/merged/plot.pdf"
        )
        self.paper = self.original.removesuffix(".pdf") + "__paper.pdf"
        self.pdf(self.original)
        self.pdf(self.paper, b"%PDF-1.4\npaper variant")
        self.write_values({"fig.eu.example": self.paper})

    def pdf(self, member, content=b"%PDF-1.4\noriginal fixture"):
        path = self.root / member
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return path

    def write_values(self, entries):
        values = {key: dict(raw=path, unit="pdf-path") for key, path in entries.items()}
        # Non-figure values, including literal strings that look like PDFs, are not bundled.
        values["params.note"] = dict(raw="reports/unused.pdf", unit="")
        self.values.write_text(render(values, "geo-full"), encoding="utf-8")

    def receipt(self, path):
        marker = path.parent / ".ipid-complete" / (path.name + ".json")
        marker.parent.mkdir(exist_ok=True)
        marker.write_text(
            json.dumps(
                dict(
                    status="complete",
                    size_bytes=path.stat().st_size,
                    sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                )
            )
        )
        return marker

    def run_bundle(self, **kwargs):
        return bundle(repository_root=self.root, **kwargs)

    def test_exact_members_deduplication_extraction_layout_and_read_only_sources(self):
        validation = "reports/figures/classifier-validation/confusion__paper.pdf"
        self.pdf(validation)
        self.receipt(self.root / self.paper)
        self.pdf("reports/unused.pdf")
        self.write_values(
            {
                "fig.eu.one": self.paper,
                "fig.eu.same": self.paper,
                "fig.validation.example": validation,
                "fig.af.pending": None,
            }
        )
        cache = self.root / "tmp/values.json"
        cache.write_text("numeric cache is not read or changed")
        before = {p: stamp(p) for p in self.root.rglob("*") if p.is_file()}
        result = self.run_bundle()
        self.assertEqual(result["pdfs"], 2)
        self.assertEqual(result["empty_figures"], 1)
        self.assertEqual(result["path"], self.output)
        with zipfile.ZipFile(self.output) as archive:
            self.assertEqual(archive.namelist(), sorted([self.paper, validation]))
            self.assertIsNone(archive.testzip())
            paper_repo = self.root / "paper-repo"
            archive.extractall(paper_repo)
        for member in (self.paper, validation):
            self.assertEqual((paper_repo / member).read_bytes(), (self.root / member).read_bytes())
        self.assertEqual(before, {p: stamp(p) for p in before})
        self.assertFalse(list(self.output.parent.glob(".paper-figures-*")))

    def test_exact_original_selection_is_not_upgraded_by_the_zip_command(self):
        self.write_values({"fig.eu.example": self.original})
        self.run_bundle()
        with zipfile.ZipFile(self.output) as archive:
            self.assertEqual(archive.namelist(), [self.original])
            self.assertEqual(archive.read(self.original), (self.root / self.original).read_bytes())

    def test_large_pdf_spaces_checksum_and_old_timestamp(self):
        member = "reports/figures/with spaces/large plot__paper.pdf"
        path = self.pdf(member, b"%PDF-1.4\n" + b"bounded streaming\0" * 150000)
        os.utime(path, (0, 0))
        self.receipt(path)
        self.write_values({"fig.example": member})
        before = stamp(path)
        self.run_bundle()
        with zipfile.ZipFile(self.output) as archive:
            self.assertEqual(archive.read(member), path.read_bytes())
            self.assertEqual(archive.getinfo(member).date_time[0], 1980)
        self.assertEqual(stamp(path), before)

    def test_missing_figures_preserve_previous_zip(self):
        self.output.write_bytes(b"existing archive must survive")
        before = stamp(self.output)
        self.write_values({"fig.eu.exists": self.paper, "fig.af.missing": "reports/missing.pdf"})
        with self.assertRaisesRegex(FileNotFoundError, "reports/missing.pdf"):
            self.run_bundle()
        self.assertEqual(stamp(self.output), before)
        self.assertFalse(list(self.output.parent.glob(".paper-figures-*")))

    def test_invalid_pdf_or_receipt_preserves_previous_zip(self):
        self.output.write_bytes(b"previous zip")
        before = stamp(self.output)
        path = self.root / self.paper
        path.write_bytes(b"not a PDF")
        with self.assertRaisesRegex(ValueError, "PDF signature"):
            self.run_bundle()
        path.write_bytes(b"%PDF-1.4\nfixture")
        marker = self.receipt(path)
        for content in (
            "null",
            marker.read_text().replace("complete", "writing"),
            marker.read_text().replace(hashlib.sha256(path.read_bytes()).hexdigest(), "0" * 64),
        ):
            marker.write_text(content)
            with self.assertRaisesRegex(ValueError, "receipt"):
                self.run_bundle()
        self.assertEqual(stamp(self.output), before)
        self.assertFalse(list(self.output.parent.glob(".paper-figures-*")))

    def test_source_change_aborts_and_preserves_previous_zip(self):
        from paper_values.figure_zip import _copy_pdf

        self.output.write_bytes(b"previous zip")
        before = stamp(self.output)

        def change(archive, member, path, fingerprint):
            _copy_pdf(archive, member, path, fingerprint)
            path.write_bytes(b"%PDF-1.4\nnew version")

        with patch("paper_values.figure_zip._copy_pdf", side_effect=change):
            with self.assertRaisesRegex(ValueError, "figure changed"):
                self.run_bundle()
        self.assertEqual(stamp(self.output), before)

    def test_values_change_aborts_and_preserves_previous_zip(self):
        from paper_values.figure_zip import _copy_pdf

        self.output.write_bytes(b"previous zip")
        before = stamp(self.output)

        def change(*args):
            _copy_pdf(*args)
            self.values.write_text(self.values.read_text() + "\n% changed\n")

        with patch("paper_values.figure_zip._copy_pdf", side_effect=change):
            with self.assertRaisesRegex(ValueError, "values.tex changed"):
                self.run_bundle()
        self.assertEqual(stamp(self.output), before)

    def test_path_traversal_and_output_names_are_rejected(self):
        for member in (
            "reports/../private.pdf",
            "/private.pdf",
            "reports/plot.pdf/../../private.pdf",
        ):
            self.values.write_text(r"\setval{fig.example}{\detokenize{" + member + "}}")
            with self.assertRaises(ValueError):
                self.run_bundle()
        for name in ("../outside.zip", "reports/file.zip", "values.tex"):
            with self.assertRaises(ValueError):
                self.run_bundle(output_name=name)
        self.assertFalse(self.output.exists())

    def test_symlink_escape_is_rejected(self):
        private = self.pdf("private.pdf")
        link = self.root / "reports/link.pdf"
        try:
            link.symlink_to(private)
        except OSError:
            self.skipTest("symlink creation requires privileges")
        self.write_values({"fig.example": "reports/link.pdf"})
        with self.assertRaisesRegex(ValueError, "outside the reports"):
            self.run_bundle()

    def test_empty_or_ambiguous_figure_assignments_fail(self):
        self.write_values({"fig.pending": None})
        with self.assertRaisesRegex(ValueError, "no nonempty figure paths"):
            self.run_bundle()
        with self.assertRaisesRegex(ValueError, "conflicting figure assignments"):
            figure_paths(
                r"\setval{fig.x}{reports/a.pdf}" + "\n" + r"\setval{fig.x}{reports/b.pdf}"
            )
        with self.assertRaisesRegex(ValueError, "unsupported figure assignment"):
            figure_paths(r"\setval{fig.x}{\detokenize{reports/a.pdf}")

    def test_alternative_reports_root_keeps_paper_relative_archive_paths(self):
        elsewhere = self.root / "mounted-reports"
        source = self.root / self.paper
        mapped = elsewhere.joinpath(*Path(self.paper).parts[1:])
        mapped.parent.mkdir(parents=True)
        shutil.copy2(source, mapped)
        source.unlink()
        self.run_bundle(reports_root=elsewhere, output_name="campaign-figures.zip")
        with zipfile.ZipFile(self.output.with_name("campaign-figures.zip")) as archive:
            self.assertEqual(archive.namelist(), [self.paper])

    @unittest.skipUnless(shutil.which("make"), "requires make")
    def test_make_target_cli_needs_no_analysis_or_plotting_imports(self):
        project = Path(__file__).resolve().parents[1]
        shutil.copytree(
            project / "paper_values",
            self.root / "paper_values",
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        shutil.copy2(project / "Makefile", self.root / "Makefile")
        (self.root / "sitecustomize.py").write_text(
            "import sys\nclass Guard:\n def find_spec(self,name,path=None,target=None):\n"
            "  if name.split('.')[0] in ('ipid_analysis','numpy','pyarrow','matplotlib'):\n"
            "   raise RuntimeError('unexpected analysis import: '+name)\n"
            "sys.meta_path.insert(0,Guard())\n"
        )
        for option in ("ARGS", "args"):
            result = subprocess.run(
                [
                    "make",
                    "paper-figures-zip",
                    "PYTHON_INTERPRETER=" + shlex.quote(sys.executable),
                    option + "=--output-name cli-figures.zip",
                ],
                cwd=self.root,
                capture_output=True,
                text=True,
                env=dict(os.environ, PYTHONPATH=str(self.root)),
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("1 PDFs", result.stdout)
        with zipfile.ZipFile(self.root / "tmp/cli-figures.zip") as archive:
            self.assertEqual(archive.namelist(), [self.paper])


if __name__ == "__main__":
    unittest.main()
