"""Playback/session fingerprint rules shared by recorder paths."""

from __future__ import annotations

import hashlib
import json
from typing import Mapping

PLAYBACK_FINGERPRINT_HEADERS = (
    "Cookie",
    "Authorization",
    "Referer",
    "Origin",
)

def _header_value(headers: Mapping[str, object], wanted_name: str) -> str:
    wanted = str(wanted_name or "").strip().casefold()
    for name, value in (headers or {}).items():
        if str(name).strip().casefold() == wanted:
            return str(value or "").strip()
    return ""

def playback_fingerprint(
    final_manifest_url: object,
    effective_headers: Mapping[str, object],
) -> str:
    """Identify one effective playback/session incarnation.

    Contract: complete final redirected manifest URL plus Cookie, Authorization,
    Referer and Origin. Playlist provenance, quality, User-Agent, DRM keys and
    separately calculated expiry are deliberately excluded.
    """
    final_url = str(final_manifest_url or "").strip()
    if not final_url:
        return ""
    payload = {
        "final_manifest_url": final_url,
        "headers": {
            name.casefold(): _header_value(effective_headers, name)
            for name in PLAYBACK_FINGERPRINT_HEADERS
        },
    }
    serialized = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
