"""Shared JSON-playlist adaptation used by mature and Coordinator source paths."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Mapping, Optional, Sequence, Tuple


JSON_RECORD_LIST_ALIASES = ("channels", "streams", "items", "entries", "data")
JSON_FIELD_ALIASES = {
    "name": ("name", "channel_name", "channel", "title"),
    "stream_url": ("stream_url", "stream", "url", "link"),
    "id": ("id", "channel_id", "tvg_id", "tvg-id"),
    "group_title": ("group_title", "group", "category"),
    "key_id": ("key_id", "kid"),
    "key": ("key",),
    "license_key": ("license_key", "drm_key", "clearkey"),
}
JSON_HEADER_FIELD_ALIASES = {
    "Cookie": ("cookie", "cookies"),
    "User-Agent": ("user_agent", "user-agent", "useragent"),
    "Origin": ("origin",),
    "Referer": ("referer", "referrer"),
    "Authorization": ("authorization", "auth_header"),
}
JSON_HEADER_OBJECT_ALIASES = ("headers", "http_headers", "request_headers")


@dataclass(frozen=True)
class JsonPlaylistAdapterPolicy:
    """Compatibility choices around one shared JSON adaptation algorithm."""

    require_stream_url: bool
    first_record_alias_wins: bool
    header_object_case_insensitive_override: bool
    strip_header_object_names: bool
    skip_blank_header_object_fields: bool
    diagnostics_profile: str


MATURE_JSON_PLAYLIST_POLICY = JsonPlaylistAdapterPolicy(
    require_stream_url=True,
    first_record_alias_wins=True,
    header_object_case_insensitive_override=True,
    strip_header_object_names=True,
    skip_blank_header_object_fields=True,
    diagnostics_profile="mature",
)

NORMALIZED_JSON_PLAYLIST_POLICY = JsonPlaylistAdapterPolicy(
    require_stream_url=False,
    first_record_alias_wins=False,
    header_object_case_insensitive_override=False,
    strip_header_object_names=False,
    skip_blank_header_object_fields=False,
    diagnostics_profile="normalized",
)


def _normalize_field_name(value: str) -> str:
    return (
        str(value or "")
        .strip()
        .casefold()
        .replace("-", "_")
        .replace(" ", "_")
    )


def _alias_value(
    record: Mapping[str, object],
    aliases: Sequence[str],
    *,
    first_wins: bool,
):
    normalized = {}
    for key, value in record.items():
        normalized_key = _normalize_field_name(key)
        if not normalized_key:
            continue
        if first_wins and normalized_key in normalized:
            continue
        normalized[normalized_key] = value

    for alias in aliases:
        normalized_alias = _normalize_field_name(alias)
        if normalized_alias in normalized:
            return normalized[normalized_alias]
    return None


def _text_value(
    record: Mapping[str, object],
    aliases: Sequence[str],
    *,
    first_wins: bool,
) -> str:
    value = _alias_value(record, aliases, first_wins=first_wins)
    if value is None or isinstance(value, (dict, list, tuple, set)):
        return ""
    return str(value).replace("\r", " ").replace("\n", " ").strip()


def _find_records(data, *, diagnostics_profile: str):
    if isinstance(data, list):
        return data, "$"

    if not isinstance(data, dict):
        raise RuntimeError("JSON playlist root must be an object or array")

    normalized_root = {
        _normalize_field_name(key): (key, value)
        for key, value in data.items()
        if _normalize_field_name(key)
    }
    for alias in JSON_RECORD_LIST_ALIASES:
        matched = normalized_root.get(_normalize_field_name(alias))
        if matched is None:
            continue
        original_key, value = matched
        if isinstance(value, list):
            return value, str(original_key)

    if diagnostics_profile == "mature":
        raise RuntimeError(
            "JSON playlist contains no recognized record list "
            f"({', '.join(JSON_RECORD_LIST_ALIASES)})"
        )
    raise RuntimeError("JSON playlist contains no recognized record list")


def _safe_extinf_attribute(value: str) -> str:
    return str(value or "").replace('"', "'").strip()


def _record_headers(
    record: Mapping[str, object],
    *,
    policy: JsonPlaylistAdapterPolicy,
) -> dict:
    headers = {}

    for header_name, aliases in JSON_HEADER_FIELD_ALIASES.items():
        value = _text_value(
            record,
            aliases,
            first_wins=policy.first_record_alias_wins,
        )
        if value:
            headers[header_name] = value

    header_object = _alias_value(
        record,
        JSON_HEADER_OBJECT_ALIASES,
        first_wins=policy.first_record_alias_wins,
    )
    if not isinstance(header_object, dict):
        return headers

    for raw_name, value in header_object.items():
        if value is None or isinstance(value, (dict, list, tuple, set)):
            continue

        header_name = str(raw_name or "")
        if policy.strip_header_object_names:
            header_name = header_name.strip()

        header_value = (
            str(value)
            .replace("\r", " ")
            .replace("\n", " ")
            .strip()
        )

        if policy.skip_blank_header_object_fields and (
            not header_name or not header_value
        ):
            continue

        if policy.header_object_case_insensitive_override:
            existing_name = next(
                (
                    current_name
                    for current_name in headers
                    if current_name.casefold() == header_name.casefold()
                ),
                None,
            )
            if existing_name is not None:
                del headers[existing_name]

        headers[header_name] = header_value

    return headers


def _entry_lines(
    record: Mapping[str, object],
    *,
    policy: JsonPlaylistAdapterPolicy,
) -> Tuple[list[str], bool]:
    text = lambda aliases: _text_value(
        record,
        aliases,
        first_wins=policy.first_record_alias_wins,
    )
    name = text(JSON_FIELD_ALIASES["name"])
    stream_url = text(JSON_FIELD_ALIASES["stream_url"])

    if not name or (policy.require_stream_url and not stream_url):
        return [], False

    tvg_id = text(JSON_FIELD_ALIASES["id"])
    group_title = text(JSON_FIELD_ALIASES["group_title"])
    attributes = []
    if tvg_id:
        attributes.append(f'tvg-id="{_safe_extinf_attribute(tvg_id)}"')
    attributes.append(f'tvg-name="{_safe_extinf_attribute(name)}"')
    if group_title:
        attributes.append(
            f'group-title="{_safe_extinf_attribute(group_title)}"'
        )

    lines = ["#EXTINF:-1 " + " ".join(attributes) + f",{name}"]

    license_key = text(JSON_FIELD_ALIASES["license_key"])
    if not license_key:
        key_id = text(JSON_FIELD_ALIASES["key_id"])
        key_value = text(JSON_FIELD_ALIASES["key"])
        if key_id and key_value:
            license_key = f"{key_id}:{key_value}"

    if license_key:
        lines.extend([
            "#KODIPROP:inputstream.adaptive.license_type=clearkey",
            "#KODIPROP:inputstream.adaptive.license_key=" + license_key,
        ])

    headers = _record_headers(record, policy=policy)
    if headers:
        lines.append(
            "#EXTHTTP:"
            + json.dumps(
                headers,
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )

    if stream_url:
        lines.append(stream_url)
    return lines, bool(stream_url)


def adapt_json_playlist_text(
    playlist_text: str,
    *,
    policy: JsonPlaylistAdapterPolicy = NORMALIZED_JSON_PLAYLIST_POLICY,
):
    """Convert recognized JSON records into synthetic M3U under one policy."""
    text = str(playlist_text or "")
    stripped = text.lstrip()
    if not stripped.startswith(("{", "[")):
        return text, {
            "adapted": False,
            "source_format": "m3u",
        }

    try:
        data = json.loads(text)
    except json.JSONDecodeError as error:
        if policy.diagnostics_profile == "mature":
            raise RuntimeError(
                "Playlist response looks like JSON but could not be parsed "
                f"(line {error.lineno}, column {error.colno})"
            ) from error
        raise RuntimeError(
            "playlist response looks like JSON but is invalid at "
            f"line {error.lineno}, column {error.colno}"
        ) from error

    records, record_container = _find_records(
        data,
        diagnostics_profile=policy.diagnostics_profile,
    )
    output_lines = ["#EXTM3U"]
    observed_record_count = 0
    playable_record_count = 0
    metadata_only_record_count = 0
    skipped_record_count = 0

    for record in records:
        if not isinstance(record, dict):
            skipped_record_count += 1
            continue

        entry_lines, playable = _entry_lines(record, policy=policy)
        if not entry_lines:
            skipped_record_count += 1
            continue

        output_lines.extend(entry_lines)
        observed_record_count += 1
        if playable:
            playable_record_count += 1
        else:
            metadata_only_record_count += 1

    if observed_record_count == 0:
        if policy.diagnostics_profile == "mature":
            raise RuntimeError(
                "JSON playlist contained no usable records with recognized "
                "name and stream URL fields"
            )
        raise RuntimeError(
            "JSON playlist contained no recognized named records"
        )

    synthetic_text = "\n".join(output_lines) + "\n"

    if policy.diagnostics_profile == "mature":
        return synthetic_text, {
            "adapted": True,
            "source_format": "json",
            "record_container": record_container,
            "record_count": len(records),
            "usable_record_count": playable_record_count,
            "skipped_record_count": skipped_record_count,
            "synthetic_m3u_sha256": hashlib.sha256(
                synthetic_text.encode("utf-8")
            ).hexdigest(),
        }

    return synthetic_text, {
        "adapted": True,
        "source_format": "json",
        "record_container": record_container,
        "record_count": len(records),
        "observed_record_count": observed_record_count,
        "usable_record_count": playable_record_count,
        "playable_record_count": playable_record_count,
        "metadata_only_record_count": metadata_only_record_count,
        "skipped_record_count": skipped_record_count,
    }
