"""Provider-neutral manifest-content classification."""

from __future__ import annotations

import re

_DASH_MPD_RE = re.compile(
    r'<(?:[A-Za-z_][\w.-]*:)?MPD\b',
    re.IGNORECASE,
)


def manifest_type_from_text(value: object) -> str:
    """Return HLS, DASH, or an empty string from manifest content."""
    stripped = str(value or "").lstrip()
    if stripped.startswith("#EXTM3U"):
        return "HLS"
    if _DASH_MPD_RE.search(stripped) is not None:
        return "DASH"
    return ""


def is_manifest_text(value: object) -> bool:
    """Return whether text begins with a recognized HLS or DASH manifest."""
    return bool(manifest_type_from_text(value))
