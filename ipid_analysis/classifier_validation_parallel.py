"""Bounded NumPy parallelism and shared, deterministic validation null tables."""

from collections import deque

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ipid_analysis import runtime
from ipid_analysis.empirical_random_uniformity import _spacing_cvm_statistics, _stable_rng
from ipid_analysis.nist_short_sequence import _cusum_pvalue_table, nist_minimum_pvalues


def worker_count(requested, samples):
    if not 1 <= requested <= 32:
        raise ValueError("validation workers must be between 1 and 32")
    # Reserve the shared populations separately, plus conservative per-job space.
    memory = max(0, runtime.available_memory() - max(1 << 30, samples * 116 * 16))
    per_job = max(512 << 20, samples * 100 * 24)
    return max(1, min(requested, runtime.cpu_budget(), memory // per_job))


def bounded_map(pool, function, jobs, workers):
    """Consume results in input order with at most two queued jobs per worker."""
    if pool is None:
        yield from map(function, jobs)
        return
    jobs = iter(jobs)
    pending = deque()
    try:
        for _ in range(2 * workers):
            try:
                pending.append(pool.submit(function, next(jobs)))
            except StopIteration:
                break
        while pending:
            yield pending.popleft().result()
            try:
                pending.append(pool.submit(function, next(jobs)))
            except StopIteration:
                pass
    finally:
        for future in pending:
            future.cancel()


def _gap_block(job):
    seed, length, index, size = job
    values = _stable_rng(seed, 23, length, index).integers(
        0, 65536, size=(size, length), dtype=np.uint16
    )
    return _spacing_cvm_statistics(values, np.ones_like(values, dtype=bool))


def build_gap_null(tables, length, pool=None, workers=1):
    jobs = (
        (tables.seed, length, index, min(tables.batch_size, tables.sample_count - offset))
        for index, offset in enumerate(range(0, tables.sample_count, tables.batch_size))
    )
    result = np.concatenate(list(bounded_map(pool, _gap_block, jobs, workers)))
    result.sort()
    return result


def _nist_block(bits):
    return nist_minimum_pvalues(bits)[0]


def build_nist_null(tables, length, pool=None, workers=1):
    # The parent alone consumes the reference RNG, exactly as the existing
    # serial producer does. Threads only evaluate immutable bit blocks.
    _cusum_pvalue_table(length)
    rng = np.random.default_rng(np.random.SeedSequence([tables.seed, length]))
    jobs = (
        rng.integers(
            0,
            2,
            size=(min(tables.batch_size, tables.sample_count - offset), length),
            dtype=np.uint8,
        )
        for offset in range(0, tables.sample_count, tables.batch_size)
    )
    result = np.concatenate(list(bounded_map(pool, _nist_block, jobs, workers)))
    result.sort()
    return result


def prepare_nulls(root, tables, nist, inputs, parameters, publish, pool=None, workers=1):
    """Publish each reference once, and share read-only arrays across all jobs."""
    paths = []
    for method, lengths, reference, build, store in (
        ("gap", (80, 100), tables.gap, build_gap_null, tables.gap._spacing),
        ("nist", (1280, 1600), nist, build_nist_null, nist._minimum),
    ):
        for length in lengths:
            path = root / f"null-{method}-{length}.pq"
            print(f"validation prepare: shared {method} null, length={length}", flush=True)
            if path.exists():
                publish(path, None, inputs, parameters)  # provenance check, no replacement
                with pq.ParquetFile(path) as source:
                    if (
                        source.metadata.num_rows != reference.sample_count
                        or source.schema_arrow.remove_metadata()
                        != pa.schema([("MINIMUM", pa.float64())])
                    ):
                        raise ValueError("invalid validation null table")
                    values = source.read()["MINIMUM"].to_numpy()
                if not np.isfinite(values).all() or np.any(np.diff(values) < 0):
                    raise ValueError("invalid validation null values")
            else:
                values = build(reference, length, pool, workers)
                publish(path, pa.table({"MINIMUM": values}), inputs, parameters)
            values.setflags(write=False)
            store[length] = values
            paths.append(path)
    return paths
