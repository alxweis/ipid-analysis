"""Optional operation timings; worker elapsed sums are not wall-clock phases."""

from contextlib import contextmanager
from threading import Lock
from time import perf_counter


class Diagnostics:
    def __init__(self, enabled=False):
        self.enabled = enabled
        self.operations = {}
        self.lock = Lock()

    @contextmanager
    def measure(self, name):
        if not self.enabled:
            yield
            return
        start = perf_counter()
        try:
            yield
        finally:
            elapsed = perf_counter() - start
            with self.lock:
                entry = self.operations.setdefault(name, dict(calls=0, seconds=0.0, maximum=0.0))
                entry["calls"] += 1
                entry["seconds"] += elapsed
                entry["maximum"] = max(entry["maximum"], elapsed)

    def call(self, name, function, *args, **kwargs):
        if not self.enabled:
            return function(*args, **kwargs)
        with self.measure(name):
            return function(*args, **kwargs)

    def summary(self):
        with self.lock:
            return {name: dict(entry) for name, entry in sorted(self.operations.items())}
