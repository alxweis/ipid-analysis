"""Bundle the exact PDFs referenced by values.tex, preserving paper-relative paths."""

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import tempfile
from time import perf_counter
import zipfile

from paper_values.cache import fingerprint
from paper_values.exporter import output_path
from paper_values.inputs import validate_plot_path


def figure_paths(text):
    """Read generated setval assignments, never execute or evaluate TeX."""
    selected = {}
    for number, line in enumerate(text.splitlines(), 1):
        line = line.strip()
        match = re.fullmatch(r"\\setval\{([a-z0-9][a-z0-9_.-]*)\}\{(.*)\}", line)
        if match is None:
            if line.startswith(r"\setval{fig."):
                raise ValueError(f"unsupported figure assignment at values.tex line {number}")
            continue
        key, value = match.groups()
        if not key.startswith("fig."):
            continue
        if value.startswith(r"\detokenize{"):
            if not value.endswith("}"):
                raise ValueError(f"unsupported figure assignment at values.tex line {number}")
            value = value[len(r"\detokenize{") : -1]
        if value:
            value = PurePosixPath(validate_plot_path(value)).as_posix()
        if key in selected and selected[key] != value:
            raise ValueError(f"conflicting figure assignments: {key}")
        selected[key] = value
    return sorted({value for value in selected.values() if value}), sum(
        not v for v in selected.values()
    )


def _copy_pdf(archive, member, path, before):
    """Stream once from the source, checking PDF signature and optional receipt."""
    marker = path.parent / ".ipid-complete" / (path.name + ".json")
    has_receipt = marker.is_file()
    receipt = json.loads(marker.read_text(encoding="utf-8-sig")) if has_receipt else None
    if has_receipt and (
        not isinstance(receipt, dict)
        or receipt.get("status") != "complete"
        or receipt.get("size_bytes") != before["stat"][0]
        or not re.fullmatch(r"[a-fA-F0-9]{64}", str(receipt.get("sha256", "")))
    ):
        raise ValueError(f"incomplete or invalid PDF receipt: {member}")
    info = zipfile.ZipInfo.from_file(path, arcname=member, strict_timestamps=False)
    info.compress_type = zipfile.ZIP_DEFLATED
    digest, copied = hashlib.sha256(), 0
    with path.open("rb") as source:
        header = source.read(5)
        if header != b"%PDF-":
            raise ValueError(f"referenced figure has no PDF signature: {member}")
        with archive.open(info, "w", force_zip64=True) as destination:
            block = header
            while block:
                destination.write(block)
                digest.update(block)
                copied += len(block)
                block = source.read(1 << 20)
    if copied != before["stat"][0] or fingerprint(path) != before:
        raise ValueError(f"figure changed while archiving: {member}")
    if receipt is not None and digest.hexdigest().lower() != receipt["sha256"].lower():
        raise ValueError(f"PDF checksum differs from its receipt: {member}")


def bundle(
    *, repository_root=None, values_path=None, reports_root=None, output_name="paper-figures.zip"
):
    root = Path(repository_root or Path(__file__).resolve().parents[1]).resolve()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*\.zip", output_name):
        raise ValueError("--output-name must be a .zip filename without directories")
    values = Path(values_path) if values_path is not None else root / "tmp/values.tex"
    if not values.is_absolute():
        values = root / values
    reports = Path(reports_root) if reports_root is not None else root / "reports"
    if not reports.is_absolute():
        reports = root / reports
    reports = reports.resolve()
    output = output_path(root, output_name, (reports,), (values,))
    if values.stat().st_size > 16 << 20:
        raise ValueError("expected a generated values.tex of at most 16 MiB")
    values_before = fingerprint(values)
    members, empty = figure_paths(values.read_text(encoding="utf-8-sig"))
    if not members:
        raise ValueError("no nonempty figure paths in values.tex; run make paper-values first")
    files, missing = {}, []
    for member in members:
        path = reports.joinpath(*PurePosixPath(member).parts[1:])
        if not path.resolve().is_relative_to(reports):
            raise ValueError(f"figure resolves outside the reports directory: {member}")
        if not path.is_file():
            missing.append(member)
        else:
            files[member] = (path, fingerprint(path))
    if missing:
        raise FileNotFoundError("missing referenced figure PDFs:\n" + "\n".join(missing))
    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=".paper-figures-", suffix=".zip", dir=output.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w+b") as stream:
            with zipfile.ZipFile(
                stream, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True
            ) as archive:
                for member, (path, before) in files.items():
                    _copy_pdf(archive, member, path, before)
            stream.flush()
            os.fsync(stream.fileno())
            with zipfile.ZipFile(stream, "r") as archive:
                bad = archive.testzip()
                if bad is not None:
                    raise ValueError(f"archive CRC check failed: {bad}")
        if fingerprint(values) != values_before:
            raise ValueError("values.tex changed while archiving; rerun the command")
        if any(fingerprint(path) != before for path, before in files.values()):
            raise ValueError("a referenced figure changed while archiving; rerun the command")
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    return dict(
        path=output, pdfs=len(files), empty_figures=empty, size_bytes=output.stat().st_size
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--values", type=Path, help="existing values.tex (default: tmp/values.tex)"
    )
    parser.add_argument(
        "--reports-root", type=Path, help="mounted/local reports root (default: reports)"
    )
    parser.add_argument(
        "--output-name", default="paper-figures.zip", help="ZIP filename under tmp/"
    )
    args = parser.parse_args(argv)
    started = perf_counter()
    try:
        result = bundle(
            values_path=args.values, reports_root=args.reports_root, output_name=args.output_name
        )
    except (OSError, ValueError, zipfile.BadZipFile, zipfile.LargeZipFile) as exc:
        parser.exit(1, f"paper-figures-zip: {exc}\n")
    print(
        f"Created {result['path']}: {result['pdfs']} PDFs, "
        f"{result['empty_figures']} empty figure values skipped, "
        f"{result['size_bytes'] / (1 << 20):.2f} MiB, {perf_counter() - started:.2f}s"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
