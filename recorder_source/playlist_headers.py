"""Shared playlist-entry URL/header parsing with explicit compatibility policy."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Callable, Dict, Mapping, Optional, Sequence, Tuple
from urllib.parse import unquote

from .headers import canonicalize_header_name


ConflictCallback = Callable[[str, str, str, str, str], None]


@dataclass(frozen=True)
class PlaylistHeaderParsePolicy:
    decode_pipe_metadata: bool
    trim_empty_query_before_pipe: bool
    case_sensitive_directives: bool
    strict_exthttp_json: bool
    include_extvlc_origin: bool
    keep_blank_values: bool


MATURE_PLAYLIST_HEADER_POLICY = PlaylistHeaderParsePolicy(
    decode_pipe_metadata=False,
    trim_empty_query_before_pipe=True,
    case_sensitive_directives=True,
    strict_exthttp_json=True,
    include_extvlc_origin=False,
    keep_blank_values=True,
)

NORMALIZED_PLAYLIST_HEADER_POLICY = PlaylistHeaderParsePolicy(
    decode_pipe_metadata=True,
    trim_empty_query_before_pipe=False,
    case_sensitive_directives=False,
    strict_exthttp_json=False,
    include_extvlc_origin=True,
    keep_blank_values=False,
)


def _directive_matches(text: str, prefix: str, *, case_sensitive: bool) -> bool:
    if case_sensitive:
        return text.startswith(prefix)
    return text.casefold().startswith(prefix.casefold())


def split_stream_url_metadata(
    stream_url: object,
    *,
    policy: PlaylistHeaderParsePolicy,
) -> Tuple[str, Dict[str, str]]:
    """Split player-style URL pipe metadata without owning precedence rules."""
    raw = str(stream_url or "").strip()
    clean_url, separator, metadata_text = raw.partition("|")
    clean_url = clean_url.strip()
    if not separator:
        return clean_url, {}
    if policy.trim_empty_query_before_pipe and clean_url.endswith("?"):
        clean_url = clean_url[:-1]

    headers: Dict[str, str] = {}
    for item in metadata_text.split("&"):
        item = item.strip()
        if not item:
            continue
        name, equals, value = item.partition("=")
        if not equals:
            continue
        if policy.decode_pipe_metadata:
            name = unquote(name)
            value = unquote(value)
        header_name = canonicalize_header_name(name)
        value = str(value).strip()
        if not header_name:
            continue
        if not value and not policy.keep_blank_values:
            continue
        headers[header_name] = value
    return clean_url, headers


def playlist_header_sources(
    option_lines: Sequence[str],
    *,
    policy: PlaylistHeaderParsePolicy,
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Parse EXTVLCOPT and EXTHTTP request-header metadata."""
    extvlc: Dict[str, str] = {}
    exthttp: Dict[str, str] = {}

    for raw_line in option_lines:
        text = str(raw_line or "").strip()
        if _directive_matches(text, "#EXTHTTP:", case_sensitive=policy.case_sensitive_directives):
            payload = text.split(":", 1)[1].strip()
            try:
                parsed = json.loads(payload)
            except (json.JSONDecodeError, TypeError):
                if policy.strict_exthttp_json:
                    raise
                parsed = None
            if isinstance(parsed, Mapping):
                for name, value in parsed.items():
                    if value is None:
                        continue
                    header_name = canonicalize_header_name(name)
                    header_value = str(value).strip()
                    if not header_name:
                        continue
                    if not header_value and not policy.keep_blank_values:
                        continue
                    exthttp[header_name] = header_value
            continue

        direct = (
            ("#EXTVLCOPT:http-cookie=", "Cookie"),
            ("#EXTVLCOPT:http-referrer=", "Referer"),
            ("#EXTVLCOPT:http-user-agent=", "User-Agent"),
        )
        matched = False
        for prefix, header_name in direct:
            if _directive_matches(text, prefix, case_sensitive=policy.case_sensitive_directives):
                value = text.split("=", 1)[1].strip()
                if value or policy.keep_blank_values:
                    extvlc[header_name] = value
                matched = True
                break
        if matched:
            continue

        origin_prefix = "#EXTVLCOPT:http-origin="
        if (
            policy.include_extvlc_origin
            and _directive_matches(text, origin_prefix, case_sensitive=policy.case_sensitive_directives)
        ):
            value = text.split("=", 1)[1].strip()
            if value or policy.keep_blank_values:
                extvlc["Origin"] = value
            continue

        extra_prefix = "#EXTVLCOPT:http-extra-headers="
        if _directive_matches(text, extra_prefix, case_sensitive=policy.case_sensitive_directives):
            raw_header = text.split("=", 1)[1]
            name, separator, value = raw_header.partition(":")
            if separator:
                header_name = canonicalize_header_name(name)
                header_value = value.strip()
                if header_name and (header_value or policy.keep_blank_values):
                    extvlc[header_name] = header_value

    return extvlc, exthttp


def merge_header_layers(
    *layers: Tuple[str, Mapping[str, object]],
    policy: PlaylistHeaderParsePolicy,
    conflict_callback: Optional[ConflictCallback] = None,
) -> Dict[str, str]:
    """Merge low-to-high-precedence header layers case-insensitively."""
    headers: Dict[str, str] = {}
    sources: Dict[str, str] = {}
    for source_name, values in layers:
        for raw_name, raw_value in values.items():
            if raw_value is None:
                continue
            name = canonicalize_header_name(raw_name)
            value = str(raw_value).strip()
            if not name:
                continue
            if not value and not policy.keep_blank_values:
                continue
            key = name.casefold()
            existing_name = next((item for item in headers if item.casefold() == key), None)
            if existing_name is not None:
                existing_value = headers[existing_name]
                existing_source = sources[key]
                if not value and str(existing_value).strip():
                    if conflict_callback is not None:
                        conflict_callback(name, existing_source, source_name, str(existing_value), "<blank>")
                    continue
                if str(existing_value) != value and conflict_callback is not None:
                    conflict_callback(name, existing_source, source_name, str(existing_value), value)
                del headers[existing_name]
            headers[name] = value
            sources[key] = source_name
    return headers


def parse_stream_url_and_headers(
    raw_url: object,
    option_lines: Sequence[str],
    *,
    policy: PlaylistHeaderParsePolicy,
    conflict_callback: Optional[ConflictCallback] = None,
) -> Tuple[str, Dict[str, str]]:
    """Normalize one playlist entry using an explicit compatibility policy."""
    clean_url, pipe_headers = split_stream_url_metadata(raw_url, policy=policy)
    extvlc_headers, exthttp_headers = playlist_header_sources(option_lines, policy=policy)
    headers = merge_header_layers(
        ("#EXTVLCOPT", extvlc_headers),
        ("#EXTHTTP", exthttp_headers),
        ("URL pipe metadata", pipe_headers),
        policy=policy,
        conflict_callback=conflict_callback,
    )
    return clean_url, headers
