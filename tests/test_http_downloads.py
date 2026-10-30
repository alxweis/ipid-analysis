"""HTTP resume contracts with deterministic responses, without network access."""

import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from ipid_analysis.downloads import download


class Response(io.BytesIO):
    def __init__(self, body, *, status=200, **headers):
        super().__init__(body)
        self.status, self.headers = status, headers


class HttpDownloadsTest(unittest.TestCase):
    def test_oversized_body_never_exceeds_its_reservation_or_commits_a_prefix(self):
        with self.assertRaisesRegex(ValueError, "exceeds expected"):
            download(
                self.url,
                self.destination,
                expected_size=5,
                opener=lambda *a, **k: Response(b"too large", **{"ETag": '"v1"'}),
            )
        partial = self.destination.with_suffix(".gz.part")
        self.assertLessEqual(partial.stat().st_size, 5)
        self.assertFalse(partial.with_suffix(".part.http.json").exists())
        self.assertFalse(self.destination.exists())
        download(
            self.url,
            self.destination,
            expected_size=5,
            opener=lambda *a, **k: Response(b"valid", **{"ETag": '"v1"'}),
        )
        self.assertEqual(self.destination.read_bytes(), b"valid")

    def test_restart_uses_recorded_total_size_when_caller_has_no_new_head(self):
        self.partial()
        download(
            self.url,
            self.destination,
            opener=lambda *a, **k: Response(
                b"end",
                status=206,
                **{
                    "ETag": '"old"',
                    "Content-Range": "bytes 3-5/6",
                },
            ),
        )
        self.assertEqual(self.destination.read_bytes(), b"oldend")

    def test_exhausted_short_response_diagnoses_sizes_offsets_and_attempts(self):
        self.url = "https://user:secret@example.invalid/input.gz?token=secret"
        messages = []
        with (
            patch("ipid_analysis.downloads.time.sleep"),
            patch("ipid_analysis.downloads.logger.warning", side_effect=messages.append),
            self.assertRaisesRegex(OSError, "received=3 expected=6") as error,
        ):
            download(
                self.url,
                self.destination,
                expected_size=6,
                max_attempts=2,
                opener=lambda *a, **k: Response(b"old", **{"Content-Length": "6"}),
            )
        self.assertIn("attempts=2", str(error.exception))
        self.assertIn("offset=0", messages[-1])
        self.assertIn("attempt=2/2", messages[-1])
        self.assertNotIn("secret", str(error.exception) + "".join(messages))
        self.assertFalse(self.destination.exists())

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.destination = Path(directory.name) / "input.gz"
        self.url = "https://example.invalid/input.gz"

    def partial(self):
        partial = self.destination.with_suffix(".gz.part")
        partial.write_bytes(b"old")
        partial.with_suffix(".part.http.json").write_text(
            json.dumps({"url": self.url, "validator": '"old"', "expected_size": 6})
        )
        return partial

    def test_validated_range_appends_exact_bytes(self):
        self.partial()

        def opener(request, **kwargs):
            self.assertEqual(request.get_header("Range"), "bytes=3-")
            self.assertEqual(request.get_header("If-range"), '"old"')
            return Response(
                b"end", status=206, **{"ETag": '"old"', "Content-Range": "bytes 3-5/6"}
            )

        download(self.url, self.destination, opener=opener, expected_size=6)
        self.assertEqual(self.destination.read_bytes(), b"oldend")

    def test_changed_validator_or_wrong_range_never_commits(self):
        for headers in (
            {"ETag": '"new"', "Content-Range": "bytes 3-5/6"},
            {"ETag": '"old"', "Content-Range": "bytes 2-5/6"},
        ):
            with self.subTest(headers=headers):
                self.partial()
                with self.assertRaises(ValueError):
                    download(
                        self.url,
                        self.destination,
                        opener=lambda *args, **kwargs: Response(b"end", status=206, **headers),
                        expected_size=6,
                    )
                self.assertFalse(self.destination.exists())

    def test_full_response_after_if_range_replaces_partial(self):
        self.partial()
        download(
            self.url,
            self.destination,
            opener=lambda *args, **kwargs: Response(
                b"newnew", **{"ETag": '"new"', "Content-Length": "6"}
            ),
            expected_size=6,
        )
        self.assertEqual(self.destination.read_bytes(), b"newnew")

    def test_416_restarts_without_promoting_partial(self):
        self.partial()
        requests = []

        def opener(request, **kwargs):
            requests.append(request)
            if len(requests) == 1:
                raise HTTPError(self.url, 416, "range", {}, None)
            self.assertIsNone(request.get_header("Range"))
            self.assertFalse(self.destination.exists())
            return Response(b"newnew", **{"ETag": '"new"', "Content-Length": "6"})

        with patch("ipid_analysis.downloads.time.sleep"):
            download(self.url, self.destination, opener=opener, expected_size=6)
        self.assertEqual(self.destination.read_bytes(), b"newnew")

    def test_short_body_retries_from_validated_offset(self):
        calls = []

        def opener(request, **kwargs):
            calls.append(request)
            if len(calls) == 1:
                return Response(b"old", **{"ETag": '"old"', "Content-Length": "6"})
            self.assertEqual(request.get_header("Range"), "bytes=3-")
            return Response(
                b"end", status=206, **{"ETag": '"old"', "Content-Range": "bytes 3-5/6"}
            )

        with patch("ipid_analysis.downloads.time.sleep"):
            download(self.url, self.destination, opener=opener, expected_size=6)
        self.assertEqual(self.destination.read_bytes(), b"oldend")


if __name__ == "__main__":
    unittest.main()
