"""Explicit, age-bounded cleanup of application-owned local scratch only."""

import os
from pathlib import Path
import re
import time

import typer

from ipid_analysis.runtime import cleanup_work, exclusive_lock, scratch_root

app = typer.Typer()


def cleanup_candidates(root: Path, cutoff: float, *, include_failed=False):
    for path in (root / "artifacts").glob("artifact-*"):
        if path.is_symlink() or not path.is_file() or path.stat().st_mtime >= cutoff:
            continue
        owner = re.match(r"artifact-(\d+)-", path.name)
        if owner:
            try:
                os.kill(int(owner[1]), 0)
                continue  # never remove work belonging to a live process
            except ProcessLookupError:
                pass
            except PermissionError:
                continue
        yield path
    for category in ("downloads", "ripe-downloads", "retry"):
        parent = root / category
        if not parent.is_dir():
            continue
        for path in parent.iterdir():
            if path.is_symlink() or not path.is_dir():
                continue
            files = list(path.rglob("*"))
            if files and max(item.stat().st_mtime for item in files) < cutoff:
                yield path
    if include_failed:
        for marker in (root / "workflow").glob("*/*/.failure-published"):
            if marker.stat().st_mtime < cutoff and not marker.parent.is_symlink():
                yield marker.parent


@app.command()
def main(
    older_than_hours: float = typer.Option(168, min=1),
    include_failed: bool = False,
    apply: bool = False,
):
    """List cleanup candidates; --apply removes only owned local files."""
    root = scratch_root()
    with (
        exclusive_lock("analysis-writer", timeout_seconds=0),
        exclusive_lock("workflow-worker", timeout_seconds=0),
    ):
        for path in cleanup_candidates(
            root, time.time() - older_than_hours * 3600, include_failed=include_failed
        ):
            resolved = path.resolve()
            if root not in resolved.parents or path.is_symlink():
                raise ValueError(f"unsafe cleanup candidate: {path}")
            typer.echo(str(path))
            if apply:
                if path.is_dir():
                    cleanup_work(path)
                else:
                    path.unlink()


if __name__ == "__main__":
    app()
