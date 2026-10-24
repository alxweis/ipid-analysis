"""Native publication through the exact rclone remote backing a mounted path."""

from contextlib import contextmanager
import os
from pathlib import Path
import signal
import subprocess
import time

from ipid_analysis.runtime import persistent_roots


def _discover_mount(destination: Path):
    roots = [
        root for root in persistent_roots() if root.resolve() in destination.resolve().parents
    ]
    if not roots:
        raise ValueError(f"destination is not inside configured persistent roots: {destination}")
    root = max(roots, key=lambda item: len(item.parts)).resolve()
    if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
        raise RuntimeError("mounted-s3 publication requires Linux pidfd signal support")
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        descriptor = None
        try:
            # Bind the signal to this process, even if a PID is recycled.
            descriptor = os.pidfd_open(int(entry.name))
            if entry.stat().st_uid != os.geteuid():
                continue
            arguments = [
                value.decode() for value in (entry / "cmdline").read_bytes().split(b"\0") if value
            ]
            if not arguments or Path(arguments[0]).name != "rclone" or "mount" not in arguments:
                continue
            index = arguments.index("mount")
            if len(arguments) <= index + 2 or Path(arguments[index + 2]).resolve() != root:
                continue
            command = [arguments[0]]
            for position, value in enumerate(arguments):
                if value.startswith("--config="):
                    command.append(value)
                elif value == "--config" and position + 1 < len(arguments):
                    command.extend((value, arguments[position + 1]))
            remote = (
                arguments[index + 1].rstrip("/")
                + "/"
                + destination.resolve().relative_to(root).as_posix()
            )
            selected = descriptor
            descriptor = None
            return command, remote, selected
        except (ProcessLookupError, FileNotFoundError, PermissionError):
            continue
        finally:
            if descriptor is not None:
                os.close(descriptor)
    raise RuntimeError(f"cannot identify the same-user rclone mount for {root}")


@contextmanager
def _mount(destination: Path):
    command, remote, descriptor = _discover_mount(destination)
    try:
        yield command, remote, descriptor
    finally:
        os.close(descriptor)


def native_put(source: Path, destination: Path) -> None:
    """Known-size object upload; never terminate an unknown-length FUSE stream.

    SIGHUP only clears the mount's directory cache. It does not restart the mount
    or change its configuration. The documented behavior exists in rclone 1.60.1.
    """
    expected = source.stat()
    timeout = int(os.environ.get("IPID_ANALYSIS_S3_TIMEOUT", "7200"))
    with _mount(destination) as (command, remote, descriptor):
        error = None
        try:
            subprocess.run(
                [*command, "copyto", "--ignore-times", "--no-traverse", str(source), remote],
                check=True,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except (subprocess.SubprocessError, OSError) as exc:
            error = exc
        # Invalidate even after ambiguous failures; subsequent recovery must not
        # mistake an old cached inode for the current complete object.
        signal.pidfd_send_signal(descriptor, signal.SIGHUP)
        if error is not None:
            raise error
        deadline = time.monotonic() + int(
            os.environ.get("IPID_ANALYSIS_VISIBILITY_TIMEOUT", "330")
        )
        while True:
            try:
                observed = destination.stat()
                if (
                    observed.st_size == expected.st_size
                    and abs(observed.st_mtime - expected.st_mtime) < 1e-5
                ):
                    return
            except FileNotFoundError:
                pass
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"native publication is not visible with the expected size/mtime: {destination}"
                )
            time.sleep(0.1)
