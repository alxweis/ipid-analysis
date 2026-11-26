"""Explicit, backed-up replacement of already published Anycast sensitivity PDFs."""

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil


def _state(path):
    if not os.path.lexists(path):
        return None
    from ipid_analysis.artifacts import assert_complete, fingerprint

    assert_complete(path)
    return dict(
        fingerprint=fingerprint(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest()
    )


def refresh(jobs, *, prepare=False, workers=4, backup_root=None):
    from ipid_analysis.anycast_probing import Job, _context, existing, worker
    from ipid_analysis.artifacts import (
        assert_complete,
        completion_path,
        fingerprints,
        publish_file,
    )
    from ipid_analysis.paper_probing_plot import prepare as prepare_quantiles
    from ipid_analysis.paper_ready import Result
    from ipid_analysis.paper_ready import run as render
    from ipid_analysis.runtime import exclusive_lock, validate_storage

    jobs = sorted(set(jobs), key=lambda job: str(job.source))
    if not jobs:
        return []
    if workers < 1:
        raise ValueError("workers must be positive")
    from ipid_analysis.anycast_census import load_dataset
    from ipid_analysis.artifacts import file_digest

    datasets = [load_dataset(job.census)[0] for job in jobs]
    if any(data["schema_version"] != 2 or not data.get("complete_period") for data in datasets):
        raise ValueError("refresh requires an explicit, complete Census period")
    if len({file_digest(job.census) for job in jobs}) != 1:
        raise ValueError("refresh requires the same Census mask for all selected plots")
    if any(
        job.output is not None or not job.destination.name.endswith("__anycast.pdf")
        for job in jobs
    ):
        raise ValueError("refresh replaces only canonical Anycast PDFs")
    validate_storage()
    with exclusive_lock("anycast-paper-refresh", timeout_seconds=0):
        pending, results = [], []
        for job in jobs:
            try:
                current = existing(job)
            except FileExistsError:
                current = False
            if current and job.destination.with_suffix(".json").is_file():
                from paper_values.cache import verify_file

                assert_complete(job.destination.with_suffix(".json"))
                verify_file(job.destination.with_suffix(".json"))
                _, _, metadata, _, _ = _context(job)
                if job.destination.with_suffix(".json").read_bytes() == metadata.read_bytes():
                    results.append(Result(str(job.destination), "skipped-current"))
                    continue
            pending.append(job)
        if not pending:
            return results
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        parent = (
            Path(backup_root)
            if backup_root is not None
            else Path(__file__).resolve().parents[1] / "tmp"
        )
        batch = parent / ("anycast-before-refresh-" + stamp)
        batch.mkdir(parents=True, mode=0o700)
        print(f"Anycast backup and staging directory: {batch}", flush=True)
        states = {}
        for index, job in enumerate(pending):
            for label, path in (
                ("pdf", job.destination),
                ("pdf-receipt", completion_path(job.destination)),
                ("metadata", job.destination.with_suffix(".json")),
                ("metadata-receipt", completion_path(job.destination.with_suffix(".json"))),
            ):
                states[str(path)] = _state(path)
                if states[str(path)] is not None:
                    backup = batch / "before" / str(index) / (label + path.suffix)
                    backup.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(path, backup)
                    if (
                        hashlib.sha256(backup.read_bytes()).hexdigest()
                        != states[str(path)]["sha256"]
                    ):
                        raise ValueError("Anycast backup verification failed")
        (batch / "before.json").write_text(json.dumps(states, indent=2))
        for job in pending:
            if prepare:
                print(f"Preparing selected-period Anycast quantiles: {job.source}", flush=True)
                print(prepare_quantiles(job, anycast=job.census), flush=True)
        staged = [
            Job(
                j.source,
                j.kind,
                j.processed_root,
                j.census,
                batch / "rendered" / str(index) / j.destination.name,
            )
            for index, j in enumerate(pending)
        ]
        rendered = render(staged, workers=workers, worker_fn=worker)
        if any(result.status != "created" for result in rendered):
            raise ValueError(
                "staged Anycast rendering failed; canonical PDFs were not replaced: "
                + "; ".join(result.error for result in rendered if result.error)
            )
        # Validate every staged plot before replacing the first published artifact.
        for job, temporary in zip(pending, staged):
            assert_complete(temporary.destination)
            _, _, metadata, inputs, parameters = _context(job)
            receipt = json.loads(completion_path(temporary.destination).read_text())
            if receipt["sources"] != fingerprints(inputs) or receipt["parameters"] != parameters:
                raise ValueError("Anycast inputs changed after staged rendering")
            if not temporary.destination.read_bytes().startswith(b"%PDF-"):
                raise ValueError("invalid staged Anycast PDF")
            # Metadata is copied verbatim from the versioned numerical preparation.
            if (
                json.loads(metadata.read_text()).get("anycast_filtering", {}).get("schema_version")
                != 2
            ):
                raise ValueError("refresh requires the new IP/prefix impact metadata")
            from paper_values.anycast_metadata import validate

            validate(json.loads(metadata.read_text()), receipt)
        for path, before in states.items():
            if _state(Path(path)) != before:
                raise ValueError(
                    "published Anycast artifact changed concurrently; no replacement performed"
                )
        for job, temporary in zip(pending, staged):
            _, _, metadata, inputs, parameters = _context(job)
            publish_file(
                metadata,
                job.destination.with_suffix(".json"),
                inputs=inputs,
                parameters={**parameters, "operation": "anycast-probing-metadata-v2"},
            )
            publish_file(
                temporary.destination, job.destination, inputs=inputs, parameters=parameters
            )
            if job.destination.read_bytes() != temporary.destination.read_bytes():
                raise ValueError("published Anycast PDF differs from staged bytes")
            results.append(Result(str(job.destination), "updated"))
            print(f"Updated Anycast PDF and metadata: {job.destination}", flush=True)
        (batch / "finished.json").write_text(json.dumps(dict(updated=len(pending))))
        return results
