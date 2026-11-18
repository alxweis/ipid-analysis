"""Prefer sibling paper PDFs without changing figure keys, runs, or source files."""

import hashlib
import tempfile
import unittest
import zipfile

from test_paper_values import CAMPAIGN, PATHS, RUNS, Fixture

from paper_values.exporter import export

KINDS = ("measurement-type-by-strategy", "itdk-role-by-strategy", "increment-distributions")


class PaperFigurePreferenceTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.f = Fixture(temporary.name)

    def plot(
        self, kind=KINDS[0], *, site="eu", run=None, paper=False, content=b"%PDF-1.4\nfixture"
    ):
        phase = "comparison" if kind == "increment-distributions" else "rt-fi"
        path = (
            self.f.reports
            / site
            / "figures"
            / (run or RUNS["icmp"])
            / "no-connection"
            / (PATHS[phase] + kind + ("__paper" if paper else "") + ".pdf")
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return path

    def key(self, kind=KINDS[0], site="eu"):
        phase = "rt-vs-fi-base" if kind == "increment-distributions" else "rt-fi"
        label = {
            "measurement-type-by-strategy": "phase-by-strategy",
            "increment-distributions": "inc-distributions",
        }.get(kind, kind)
        return f"fig.{site}.icmp.{phase}.{label}"

    def relative(self, path):
        return path.relative_to(self.f.root).as_posix()

    def test_original_fallback_then_paper_preference_for_all_supported_plots_and_sites(self):
        sites = ("af", "as", "au", "eu", "na", "sa")
        originals = {(kind, site): self.plot(kind, site=site) for kind in KINDS for site in sites}
        before = {
            path: (path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
            for path in originals.values()
        }
        first = self.f.run(strict=True)["values"]
        for (kind, site), path in originals.items():
            self.assertEqual(first[self.key(kind, site)]["raw"], self.relative(path))
        papers = {
            (kind, site): self.plot(kind, site=site, paper=True)
            for kind in KINDS
            for site in sites
        }
        for path in papers.values():
            self.f.receipt(path)
        all_before = {
            path: (path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
            for path in self.f.reports.rglob("*")
            if path.is_file()
        }
        values = self.f.run(strict=True)["values"]
        self.assertEqual(set(first), set(values))
        for (kind, site), path in papers.items():
            record = values[self.key(kind, site)]
            self.assertEqual(record["raw"], self.relative(path))
            self.assertEqual(record["sources"], [path.as_posix()])
        self.assertEqual(
            before,
            {
                p: (p.stat().st_mtime_ns, hashlib.sha256(p.read_bytes()).hexdigest())
                for p in before
            },
        )
        self.assertEqual(
            all_before,
            {
                path: (path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
                for path in all_before
            },
        )

    def test_standalone_paper_pdf_is_recognized(self):
        paper = self.plot(paper=True)
        self.assertEqual(
            self.f.run(strict=True)["values"][self.key()]["raw"], self.relative(paper)
        )

    def test_preference_never_overrides_canonical_run(self):
        self.f.baseline(protocol="icmp")
        current = self.plot()
        self.plot(run="icmp_2025-01-01_00-00-00", paper=True)
        self.assertEqual(
            self.f.run(strict=True)["values"][self.key()]["raw"], self.relative(current)
        )

    def test_different_runs_remain_ambiguous(self):
        self.plot()
        self.plot(run="icmp_2025-01-01_00-00-00", paper=True)
        with self.assertRaisesRegex(ValueError, "ambiguous PDF"):
            self.f.run(strict=True)

    def test_invalid_preferred_variant_keeps_strict_integrity_checks_and_previous_export(self):
        self.plot()
        self.f.run(strict=True)
        before = self.f.output_hashes()
        paper = self.plot(paper=True, content=b"unfinished PDF")
        with self.assertRaisesRegex(ValueError, "PDF signature"):
            self.f.run(strict=True)
        self.assertEqual(before, self.f.output_hashes())
        paper.write_bytes(b"%PDF-1.4\ncomplete-looking bytes")
        marker = self.f.receipt(paper)
        marker.write_text(marker.read_text().replace('"complete"', '"writing"'))
        with self.assertRaisesRegex(ValueError, "completion receipt"):
            self.f.run(strict=True)
        self.assertEqual(before, self.f.output_hashes())

    def test_unselected_original_signature_does_not_hide_valid_paper(self):
        self.plot(content=b"invalid old original")
        paper = self.plot(paper=True)
        self.assertEqual(
            self.f.run(strict=True)["values"][self.key()]["raw"], self.relative(paper)
        )

    def test_archive_and_mixed_inputs_prefer_the_paper_path(self):
        original = self.plot()
        archive = self.f.root / "original.zip"
        with zipfile.ZipFile(archive, "w") as bundle:
            bundle.write(original, self.relative(original))
        paper = self.plot(paper=True)
        self.assertEqual(
            self.f.run(strict=True, archive=archive)["values"][self.key()]["raw"],
            self.relative(paper),
        )
        both = self.f.root / "both.zip"
        with zipfile.ZipFile(both, "w") as bundle:
            for path in (original, paper):
                bundle.write(path, self.relative(path))
        values = export(
            CAMPAIGN, reports_root=None, archive=both, repository_root=self.f.root, strict=True
        )["values"]
        self.assertEqual(values[self.key()]["raw"], self.relative(paper))

    def test_conflicting_copies_of_preferred_pdf_remain_an_error(self):
        paper = self.plot(paper=True)
        archive = self.f.root / "conflict.zip"
        with zipfile.ZipFile(archive, "w") as bundle:
            bundle.writestr(self.relative(paper), b"%PDF-1.4\na different paper PDF")
        with self.assertRaisesRegex(ValueError, "ambiguous PDF"):
            self.f.run(strict=True, archive=archive)


if __name__ == "__main__":
    unittest.main()
