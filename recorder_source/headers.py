"""Provider-neutral HTTP header normalization shared by recorder paths."""

from __future__ import annotations

_HEADER_ALIASES = {
    "cookie": "Cookie",
    "referer": "Referer",
    "referrer": "Referer",
    "origin": "Origin",
    "user-agent": "User-Agent",
    "useragent": "User-Agent",
    "authorization": "Authorization",
    "accept": "Accept",
}

def canonicalize_header_name(name: object) -> str:
    """Return the canonical spelling used by recorder HTTP metadata."""
    raw_name = str(name or "").strip()
    normalized = raw_name.casefold().replace("_", "-")
    return _HEADER_ALIASES.get(normalized, raw_name)
