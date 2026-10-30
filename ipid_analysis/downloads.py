"""Validated, resumable HTTP transfers to local scratch only."""

from __future__ import annotations

from http.client import IncompleteRead
import json
from pathlib import Path
import re
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request

from loguru import logger


def download(
    url: str,
    destination: Path,
    *,
    opener,
    resume=True,
    expected_size=None,
    max_attempts=5,
    finalize=None,
):
    if max_attempts < 1:
        raise ValueError("max_attempts must be positive")
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(destination.suffix + ".part")
    state_path = partial.with_suffix(partial.suffix + ".http.json")
    try:
        state = json.loads(state_path.read_text())
    except (OSError, ValueError):
        state = {}
    if not isinstance(state, dict):
        state = {}
    same_url = state.get("url") in {None, url}
    if not same_url:
        state = {}
    if (
        expected_size is None
        and isinstance(state.get("expected_size"), int)
        and state["expected_size"] > 0
    ):
        expected_size = state["expected_size"]
    parts = urlsplit(url)
    diagnostic_url = urlunsplit(
        (parts.scheme, parts.netloc.rsplit("@", 1)[-1], parts.path, "", "")
    )
    status = None
    if (
        same_url
        and destination.is_file()
        and (expected_size is None or destination.stat().st_size == expected_size)
    ):
        return destination
    # Old incomplete finals can only be resumed with a recorded remote validator.
    if destination.is_file() and not partial.is_file() and state.get("validator"):
        destination.replace(partial)  # local owned transfer; do not double its disk usage
    for attempt in range(max_attempts):
        offset = (
            partial.stat().st_size
            if resume and partial.is_file() and state.get("validator")
            else 0
        )
        if expected_size is not None and offset > expected_size:
            offset = 0
        if offset and offset == expected_size:
            return (
                finalize(partial, destination, expected_size)
                if finalize
                else _finalize(partial, destination)
            )
        headers = {"User-Agent": "ipid-analysis"}
        if offset:
            headers.update(Range=f"bytes={offset}-", **{"If-Range": state["validator"]})
        try:
            with opener(Request(url, headers=headers), timeout=120) as response:
                status = getattr(response, "status", 200)
                response_headers = getattr(response, "headers", {})
                validator = response_headers.get("ETag") or response_headers.get("Last-Modified")
                if validator and validator.startswith("W/"):
                    validator = response_headers.get("Last-Modified")
                resumed = offset and getattr(response, "status", None) == 206
                if resumed:
                    content_range = response_headers.get("Content-Range", "")
                    match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", content_range)
                    if (
                        not match
                        or int(match[1]) != offset
                        or not offset <= int(match[2]) < int(match[3])
                        or validator != state["validator"]
                    ):
                        raise ValueError(
                            "HTTP resume does not match the partial object's validator/range"
                        )
                    if expected_size is not None and int(match[3]) != expected_size:
                        raise ValueError("HTTP object size changed during resume")
                    expected_size = int(match[3])
                elif getattr(response, "status", 200) == 206:
                    raise ValueError("unexpected partial HTTP response")
                else:
                    offset = 0
                    length = response_headers.get("Content-Length")
                    if length is not None:
                        if expected_size is not None and int(length) != expected_size:
                            raise ValueError(
                                "HTTP response length differs from expected object size"
                            )
                        expected_size = int(length)
                state = {"url": url, "validator": validator, "expected_size": expected_size}
                state_path.write_text(json.dumps(state), encoding="utf-8")
                with partial.open("ab" if resumed else "wb") as output:
                    received = offset
                    while True:
                        size = (
                            min(8 << 20, expected_size - received + 1)
                            if expected_size is not None
                            else 8 << 20
                        )
                        chunk = response.read(size)
                        if not chunk:
                            break
                        if expected_size is not None and received + len(chunk) > expected_size:
                            # Never grow a known-size transfer beyond its disk
                            # reservation or promote its full-length prefix on
                            # the next call after observing an oversized body.
                            state_path.unlink(missing_ok=True)
                            raise ValueError("HTTP body exceeds expected object size")
                        output.write(chunk)
                        received += len(chunk)
            if expected_size is None or partial.stat().st_size == expected_size:
                return (
                    finalize(partial, destination, expected_size)
                    if finalize
                    else _finalize(partial, destination)
                )
            if partial.stat().st_size > expected_size:
                raise ValueError("HTTP body exceeds expected object size")
            logger.warning(
                f"short HTTP body: url={diagnostic_url} attempt={attempt + 1}/{max_attempts} "
                f"status={status} offset={offset} received={partial.stat().st_size} "
                f"expected={expected_size} validator={state.get('validator')!r} partial={partial}"
            )
        except HTTPError as exc:
            logger.warning(
                f"HTTP retry: url={diagnostic_url} attempt={attempt + 1}/{max_attempts} status={exc.code} offset={offset} expected={expected_size}"
            )
            if exc.code == 416:
                # A 416 is not evidence of completeness; restart on the next attempt.
                state = {}
                state_path.unlink(missing_ok=True)
            elif exc.code not in {408, 429, 500, 502, 503, 504}:
                raise
            if attempt + 1 == max_attempts:
                raise
        except (OSError, URLError, IncompleteRead) as exc:
            received = partial.stat().st_size if partial.is_file() else 0
            logger.warning(
                f"HTTP transfer interrupted: url={diagnostic_url} attempt={attempt + 1}/{max_attempts} status={status} offset={offset} received={received} expected={expected_size} error={type(exc).__name__}"
            )
            if attempt + 1 == max_attempts:
                raise
        except ValueError:
            received = partial.stat().st_size if partial.is_file() else 0
            logger.warning(
                f"HTTP validation failed: url={diagnostic_url} attempt={attempt + 1}/{max_attempts} status={status} offset={offset} received={received} expected={expected_size} validator={state.get('validator')!r}"
            )
            raise
        if attempt + 1 < max_attempts:
            time.sleep(min(2**attempt, 8))
    received = partial.stat().st_size if partial.is_file() else 0
    raise OSError(
        f"incomplete HTTP download retained at {partial}; url={diagnostic_url} attempts={max_attempts} status={status} received={received} expected={expected_size} validator={state.get('validator')!r}"
    )


def _finalize(partial, destination):
    partial.replace(destination)  # both are local, owned scratch paths
    return destination
