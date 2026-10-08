"""Regression tests for license-request-only headers without importing the recorder entrypoint."""

from __future__ import annotations

import ast
import base64
import io
import json
from pathlib import Path
import re
import unittest
from typing import List
from urllib.request import Request
import xml.etree.ElementTree as ET

from recorder_source import playlist_headers


KID = "00112233445566778899aabbccddeeff"
KEY = "ffeeddccbbaa99887766554433221100"
STREAM_URL = "https://cdn.test/starsports1hd.mpd"
LICENSE_URL = "https://license.test/starsports1hd.mpd?license=1"
MANIFEST = (
    '<MPD xmlns:cenc="urn:mpeg:cenc:2013">'
    '<ContentProtection cenc:default_KID="00112233-4455-6677-8899-aabbccddeeff" />'
    '</MPD>'
)


def _encode(hex_string: str) -> str:
    return base64.urlsafe_b64encode(bytes.fromhex(hex_string)).decode("ascii").rstrip("=")


def _decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


class LicenseRequestHeaderTests(unittest.TestCase):
    def _run_resolver(self, license_key: str, *, through_selection: bool = True):
        source = (Path(__file__).resolve().parents[1] / "record_dynamic.py").read_text(
            encoding="utf-8"
        )
        parsed = ast.parse(source)
        selected = [
            node for node in parsed.body
            if isinstance(node, ast.FunctionDef)
            and node.name in {"resolve_nm3u8dl_license_url", "resolve_nm3u8dl_source_keys"}
        ]
        self.assertEqual(len(selected), 2)

        captured_requests = []
        manifest_calls = []

        def fake_manifest(stream_url, headers):
            manifest_calls.append((stream_url, dict(headers)))
            return MANIFEST

        def fake_urlopen(request, timeout):
            captured_requests.append((request, timeout))
            response = {"keys": [{"kid": _encode(KID), "k": _encode(KEY)}]}
            return io.BytesIO(json.dumps(response).encode("utf-8"))

        namespace = {
            "List": List,
            "ET": ET,
            "json": json,
            "re": re,
            "Request": Request,
            "urlopen": fake_urlopen,
            "source_playlist_headers": playlist_headers,
            "_fetch_nm3u8dl_stream_manifest_text": fake_manifest,
            "_is_nm3u8dl_drmlive_host": lambda url: False,
            "_nm3u8dl_b64url_encode": lambda raw: base64.urlsafe_b64encode(raw).decode("ascii").rstrip("="),
            "_nm3u8dl_b64url_decode": _decode,
            "NM3U8DL_QUALITY_HTTP_TIMEOUT_SEC": 15,
            "log_timeout_exception": lambda *a, **kw: None,
            "log": lambda message: None,
        }
        exec(compile(ast.Module(body=selected, type_ignores=[]), "<isolated recorder funcs>", "exec"), namespace)
        stream_headers = {
            "User-Agent": "Stream-only UA",
            "Referer": "https://stream-only.test/",
            "Cookie": "stream-only-cookie",
        }
        if through_selection:
            resolved = namespace["resolve_nm3u8dl_source_keys"](
                {"keys": [license_key], "stream_url": STREAM_URL},
                stream_headers,
            )
        else:
            resolved = namespace["resolve_nm3u8dl_license_url"](
                license_key, STREAM_URL, stream_headers,
            )
        self.assertEqual(resolved, [f"{KID}:{KEY}"])
        self.assertEqual(manifest_calls, [(STREAM_URL, stream_headers)])
        self.assertEqual(len(captured_requests), 1)
        request, timeout = captured_requests[0]
        self.assertEqual(timeout, 20)
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(json.loads(request.data), {
            "kids": [_encode(KID)],
            "type": "temporary",
        })
        return request, {name.casefold(): value for name, value in request.header_items()}

    def test_license_pipe_headers_apply_only_to_license_request(self):
        request, headers = self._run_resolver(
            LICENSE_URL
            + "|User-Agent=plaYtv/7.1.5 (Linux; Android 13) ExoPlayerLib/2.11.6"
            + "&Referer=https://www.jiotv.co/&Origin=https://www.jiotv.co/"
        )
        self.assertEqual(request.full_url, LICENSE_URL)
        self.assertEqual(headers["user-agent"], "plaYtv/7.1.5 (Linux; Android 13) ExoPlayerLib/2.11.6")
        self.assertEqual(headers["referer"], "https://www.jiotv.co/")
        self.assertEqual(headers["origin"], "https://www.jiotv.co/")
        self.assertNotIn("cookie", headers)
        self.assertEqual(headers["content-type"], "application/json")
        self.assertEqual(headers["accept"], "application/json")

    def test_plain_license_url_keeps_existing_request_defaults(self):
        request, headers = self._run_resolver(LICENSE_URL, through_selection=False)
        self.assertEqual(request.full_url, LICENSE_URL)
        self.assertEqual(headers["user-agent"], "curl/8.21.0")
        self.assertNotIn("referer", headers)
        self.assertNotIn("cookie", headers)


if __name__ == "__main__":
    unittest.main()
