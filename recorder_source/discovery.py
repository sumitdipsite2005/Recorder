"""Shared playlist source-acquisition boundary.

The functions here fetch configured playlist documents, normalize matching M3U
entries into ``SourceCandidate`` objects, and perform a deliberately small HLS
availability/quality probe for Inspect/Watch.  Recording execution remains out
of this module.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urljoin, urlsplit
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

from .matching import evaluate_match
from .quality import parse_hls_manifest_quality, probe_stream_quality_ffprobe
from .models import (
    PlaylistSourceSpec,
    SourceAcquisitionRequest,
    SourceAcquisitionResult,
    SourceCandidate,
)



PROVIDER_PROBE_HEADERS = {
    "SONYLIV": {
        "Accept": "*/*",
        "Origin": "https://www.sonyliv.com",
        "Referer": "https://www.sonyliv.com/",
        "Sec-GPC": "1",
    },
    "FANCODE": {
        "Accept": "*/*",
        "Origin": "https://www.fancode.com",
        "Referer": "https://www.fancode.com/",
        "Sec-GPC": "1",
    },
    "HOTSTAR": {
        "Accept": "*/*",
        "Sec-GPC": "1",
    },
    "KHEL": {
        "Accept": "*/*",
        "Sec-GPC": "1",
    },
}

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

DEFAULT_HTTP_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
)


def _normalize_json_field_name(value: str) -> str:
    return str(value or "").strip().casefold().replace("-", "_").replace(" ", "_")


def _json_alias_value(record: Mapping[str, object], aliases: Sequence[str]):
    normalized = {_normalize_json_field_name(key): value for key, value in record.items()}
    for alias in aliases:
        key = _normalize_json_field_name(alias)
        if key in normalized:
            return normalized[key]
    return None


def _json_text(record: Mapping[str, object], aliases: Sequence[str]) -> str:
    value = _json_alias_value(record, aliases)
    if value is None or isinstance(value, (dict, list, tuple, set)):
        return ""
    return str(value).replace("\r", " ").replace("\n", " ").strip()


def adapt_json_playlist_text(playlist_text: str) -> Tuple[str, Mapping[str, object]]:
    """Convert recognized JSON playlist records into the common M3U parser input."""
    text = str(playlist_text or "")
    stripped = text.lstrip()
    if not stripped.startswith(("{", "[")):
        return text, {"adapted": False, "source_format": "m3u"}
    try:
        data = json.loads(text)
    except json.JSONDecodeError as error:
        raise RuntimeError(
            "playlist response looks like JSON but is invalid at "
            f"line {error.lineno}, column {error.colno}"
        ) from error

    if isinstance(data, list):
        records = data
        container = "$"
    elif isinstance(data, dict):
        records = None
        container = ""
        normalized = {_normalize_json_field_name(key): (key, value) for key, value in data.items()}
        for alias in JSON_RECORD_LIST_ALIASES:
            item = normalized.get(_normalize_json_field_name(alias))
            if item is not None and isinstance(item[1], list):
                container, records = str(item[0]), item[1]
                break
        if records is None:
            raise RuntimeError("JSON playlist contains no recognized record list")
    else:
        raise RuntimeError("JSON playlist root must be an object or array")

    output = ["#EXTM3U"]
    observed = 0
    playable = 0
    metadata_only = 0
    skipped = 0
    for record in records:
        if not isinstance(record, dict):
            skipped += 1
            continue
        name = _json_text(record, JSON_FIELD_ALIASES["name"])
        stream_url = _json_text(record, JSON_FIELD_ALIASES["stream_url"])
        if not name:
            skipped += 1
            continue
        tvg_id = _json_text(record, JSON_FIELD_ALIASES["id"])
        group_title = _json_text(record, JSON_FIELD_ALIASES["group_title"])
        safe = lambda value: str(value or "").replace('"', "'").strip()
        attrs = []
        if tvg_id:
            attrs.append(f'tvg-id="{safe(tvg_id)}"')
        attrs.append(f'tvg-name="{safe(name)}"')
        if group_title:
            attrs.append(f'group-title="{safe(group_title)}"')
        output.append("#EXTINF:-1 " + " ".join(attrs) + f",{name}")

        license_key = _json_text(record, JSON_FIELD_ALIASES["license_key"])
        if not license_key:
            key_id = _json_text(record, JSON_FIELD_ALIASES["key_id"])
            key_value = _json_text(record, JSON_FIELD_ALIASES["key"])
            if key_id and key_value:
                license_key = f"{key_id}:{key_value}"
        if license_key:
            output.append("#KODIPROP:inputstream.adaptive.license_type=clearkey")
            output.append("#KODIPROP:inputstream.adaptive.license_key=" + license_key)

        headers: Dict[str, str] = {}
        for header_name, aliases in JSON_HEADER_FIELD_ALIASES.items():
            value = _json_text(record, aliases)
            if value:
                headers[header_name] = value
        header_object = _json_alias_value(record, JSON_HEADER_OBJECT_ALIASES)
        if isinstance(header_object, dict):
            for name_key, value in header_object.items():
                if value is not None and not isinstance(value, (dict, list, tuple, set)):
                    headers[str(name_key)] = (
                        str(value).replace("\r", " ").replace("\n", " ").strip()
                    )
        if headers:
            output.append(
                "#EXTHTTP:"
                + json.dumps(headers, ensure_ascii=False, separators=(",", ":"))
            )
        if stream_url:
            output.append(stream_url)
            playable += 1
        else:
            metadata_only += 1
        observed += 1

    if observed == 0:
        raise RuntimeError("JSON playlist contained no recognized named records")
    return "\n".join(output) + "\n", {
        "adapted": True,
        "source_format": "json",
        "record_container": container,
        "record_count": len(records),
        "observed_record_count": observed,
        "usable_record_count": playable,
        "playable_record_count": playable,
        "metadata_only_record_count": metadata_only,
        "skipped_record_count": skipped,
    }


def parse_extinf_metadata(extinf: str) -> Dict[str, str]:
    text = str(extinf or "").strip()

    def get_attribute(name: str) -> str:
        match = re.search(
            rf'(?i)(?:^|[\s,]){re.escape(name)}\s*=\s*"([^"]*)"',
            text,
        )
        if match is None:
            match = re.search(
                rf"(?i)(?:^|[\s,]){re.escape(name)}\s*=\s*'([^']*)'",
                text,
            )
        return match.group(1).strip() if match else ""

    tvg_name = get_attribute("tvg-name")
    group_title = get_attribute("group-title")
    attribute_matches = list(
        re.finditer(
            r'(?i)(?:^|[\s,])[\w-]+\s*=\s*(?:"[^"]*"|\'[^\']*\')',
            text,
        )
    )
    if attribute_matches:
        title_search_start = attribute_matches[-1].end()
    else:
        colon_index = text.find(":")
        title_search_start = colon_index + 1 if colon_index >= 0 else 0

    title_separator = text.find(",", title_search_start)
    entry_title = text[title_separator + 1 :].strip() if title_separator >= 0 else ""
    return {
        "tvg_name": tvg_name,
        "group_title": group_title,
        "entry_title": entry_title,
    }


def _canonical_header_name(name: str) -> str:
    text = str(name or "").strip()
    known = {
        "user-agent": "User-Agent",
        "referer": "Referer",
        "referrer": "Referer",
        "origin": "Origin",
        "cookie": "Cookie",
        "authorization": "Authorization",
        "accept": "Accept",
    }
    return known.get(text.casefold(), text)


def _apply_headers(target: Dict[str, str], values: Mapping[str, object]) -> None:
    """Apply one metadata layer case-insensitively; later layers win."""
    for raw_name, raw_value in values.items():
        if raw_value is None:
            continue
        name = _canonical_header_name(str(raw_name))
        value = str(raw_value).strip()
        if not name or not value:
            continue
        existing = next((key for key in target if key.casefold() == name.casefold()), None)
        if existing is not None:
            del target[existing]
        target[name] = value


def _parse_stream_url_and_headers(
    raw_url: str,
    option_lines: Sequence[str],
) -> Tuple[str, Dict[str, str]]:
    """Normalize playlist playback headers using mature precedence direction.

    Lowest -> highest precedence is EXTVLCOPT, EXTHTTP, then URL pipe metadata.
    """
    raw = str(raw_url or "").strip()
    clean_url = raw
    pipe_headers: Dict[str, str] = {}
    if "|" in raw:
        clean_url, suffix = raw.split("|", 1)
        for item in suffix.split("&"):
            if "=" not in item:
                continue
            name, value = item.split("=", 1)
            name = unquote(name).strip()
            value = unquote(value).strip()
            if name and value:
                pipe_headers[name] = value

    extvlc_headers: Dict[str, str] = {}
    exthttp_headers: Dict[str, str] = {}
    for line in option_lines:
        text = str(line or "").strip()
        lower = text.casefold()
        if lower.startswith("#extvlcopt:http-cookie="):
            extvlc_headers["Cookie"] = text.split("=", 1)[1].strip()
        elif lower.startswith("#extvlcopt:http-referrer="):
            extvlc_headers["Referer"] = text.split("=", 1)[1].strip()
        elif lower.startswith("#extvlcopt:http-origin="):
            extvlc_headers["Origin"] = text.split("=", 1)[1].strip()
        elif lower.startswith("#extvlcopt:http-user-agent="):
            extvlc_headers["User-Agent"] = text.split("=", 1)[1].strip()
        elif lower.startswith("#extvlcopt:http-extra-headers="):
            raw_header = text.split("=", 1)[1]
            name, separator, value = raw_header.partition(":")
            if separator:
                extvlc_headers[name.strip()] = value.strip()
        elif lower.startswith("#exthttp:"):
            payload = text.split(":", 1)[1].strip()
            try:
                parsed = json.loads(payload)
            except (json.JSONDecodeError, TypeError):
                parsed = None
            if isinstance(parsed, dict):
                _apply_headers(exthttp_headers, parsed)

    headers: Dict[str, str] = {}
    _apply_headers(headers, extvlc_headers)
    _apply_headers(headers, exthttp_headers)
    _apply_headers(headers, pipe_headers)
    return clean_url.strip(), headers


def _playlist_license_metadata(option_lines: Sequence[str]) -> Tuple[str, Tuple[str, ...], str]:
    license_type = ""
    keys: List[str] = []
    unsupported_drm = ""
    type_prefix = "#KODIPROP:inputstream.adaptive.license_type="
    key_prefix = "#KODIPROP:inputstream.adaptive.license_key="
    for raw_line in option_lines:
        line = str(raw_line or "").strip()
        if line.casefold().startswith(type_prefix.casefold()):
            license_type = line[len(type_prefix):].strip()
        elif line.casefold().startswith(key_prefix.casefold()):
            value = line[len(key_prefix):].strip()
            if value and value not in keys:
                keys.append(value)
    normalized_type = license_type.casefold().replace("-", "").replace("_", "")
    if "widevine" in normalized_type:
        unsupported_drm = "Widevine"
    return license_type, tuple(keys), unsupported_drm


def _stream_type_from_url(value: str) -> str:
    path = urljoin(str(value or ""), urlsplit(str(value or "")).path).casefold() if value else ""
    if ".mpd" in path:
        return "DASH"
    if ".m3u8" in path:
        return "HLS"
    return ""


def _fingerprint_header_value(headers: Mapping[str, str], wanted: str) -> str:
    for name, value in headers.items():
        if str(name).strip().casefold() == wanted.casefold():
            return str(value or "").strip()
    return ""


def _playback_fingerprint(final_url: str, headers: Mapping[str, str]) -> str:
    if not str(final_url or "").strip():
        return ""
    payload = {
        "final_manifest_url": str(final_url).strip(),
        "headers": {
            name.casefold(): _fingerprint_header_value(headers, name)
            for name in ("Cookie", "Authorization", "Referer", "Origin")
        },
    }
    serialized = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def parse_playlist_text(
    playlist_text: str,
    *,
    playlist_url: str = "",
    source_name: str = "",
    source_group: str = "",
    provider: str = "UNKNOWN",
    stream_headers: Optional[Mapping[str, str]] = None,
) -> SourceAcquisitionResult:
    """Normalize one playlist document without applying a target/search policy."""
    playlist_text, adapter_metadata = adapt_json_playlist_text(playlist_text)
    lines = [line.strip() for line in str(playlist_text or "").splitlines()]
    candidates: List[SourceCandidate] = []
    index = 0
    observation_index = 0

    while index < len(lines):
        if not lines[index].startswith("#EXTINF:"):
            index += 1
            continue

        observation_index += 1
        extinf = lines[index]
        metadata = parse_extinf_metadata(extinf)
        option_lines: List[str] = []
        raw_stream_url = ""
        cursor = index + 1

        while cursor < len(lines) and not lines[cursor].startswith("#EXTINF:"):
            line = lines[cursor]
            if line.startswith(("http://", "https://")):
                raw_stream_url = line
                break
            if line:
                option_lines.append(line)
            cursor += 1

        stream_url, headers = _parse_stream_url_and_headers(raw_stream_url, option_lines)
        merged_headers = dict(stream_headers or {})
        merged_headers.update(headers)
        license_type, keys, unsupported_drm = _playlist_license_metadata(option_lines)
        candidates.append(
            SourceCandidate(
                playlist_url=playlist_url,
                matching_entry_index=observation_index,
                extinf=extinf,
                tvg_name=metadata["tvg_name"],
                group_title=metadata["group_title"],
                entry_title=metadata["entry_title"],
                option_lines=tuple(option_lines),
                raw_stream_url=raw_stream_url,
                stream_url=stream_url,
                headers=merged_headers,
                keys=keys,
                license_type=license_type,
                unsupported_drm=unsupported_drm,
                stream_type=_stream_type_from_url(stream_url),
                extra={
                    "source_name": source_name or playlist_url,
                    "source_group": source_group,
                    "provider": provider,
                },
            )
        )

        index = cursor + 1 if raw_stream_url else cursor

    return SourceAcquisitionResult(
        candidates=tuple(candidates),
        diagnostics={
            "playlist_url": playlist_url,
            "observed_count": len(candidates),
            "adapter": dict(adapter_metadata),
        },
    )


def discover_playlist_text(
    playlist_text: str,
    request: SourceAcquisitionRequest,
    *,
    playlist_url: str = "",
    source_name: str = "",
    source_group: str = "",
    provider: str = "UNKNOWN",
    stream_headers: Optional[Mapping[str, str]] = None,
) -> SourceAcquisitionResult:
    """Compatibility helper: acquire observations, then apply shared matching.

    New Identity Coordinator code uses ``parse_playlist_text`` directly so
    Source Acquisition remains independent of target policy.  ONE BEST keeps
    this wrapper while its larger mature resolver migrates incrementally.
    """
    acquired = parse_playlist_text(
        playlist_text,
        playlist_url=playlist_url,
        source_name=source_name,
        source_group=source_group,
        provider=provider,
        stream_headers=stream_headers,
    )
    matched: List[SourceCandidate] = []
    for candidate in acquired.candidates:
        evaluation = evaluate_match(
            request.match,
            tvg_name=candidate.tvg_name,
            group_title=candidate.group_title,
            entry_title=candidate.entry_title,
            stream_url=candidate.stream_url,
        )
        # Preserve the mature ONE BEST compatibility behavior: a playlist entry
        # without a playable URL is not a selectable ONE BEST source.
        if evaluation.matches and candidate.stream_url:
            matched.append(
                replace(
                    candidate,
                    preferred_qualifier_score=evaluation.preferred_qualifier_score,
                )
            )

    diagnostics = dict(acquired.diagnostics)
    diagnostics["matched_count"] = len(matched)
    return SourceAcquisitionResult(
        candidates=tuple(matched),
        source_errors=acquired.source_errors,
        diagnostics=diagnostics,
    )


def fetch_playlist_documents(
    sources: Sequence[PlaylistSourceSpec],
    *,
    timeout_sec: float = 20.0,
    max_workers: int = 8,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> Tuple[Mapping[str, str], Tuple[str, ...], Mapping[str, Mapping[str, object]]]:
    """Fetch all sources concurrently while preserving per-source diagnostics."""
    unique_sources: List[PlaylistSourceSpec] = []
    seen = set()
    for source in sources:
        if source.url and source.url not in seen:
            seen.add(source.url)
            unique_sources.append(source)

    documents: Dict[str, str] = {}
    errors: List[str] = []
    diagnostics: Dict[str, Mapping[str, object]] = {}

    def fetch_one(source: PlaylistSourceSpec):
        headers = {"User-Agent": DEFAULT_HTTP_USER_AGENT}
        headers.update(dict(source.request_headers or {}))
        started = time.monotonic()
        request = Request(source.url, headers=headers)
        with urlopen(request, timeout=float(timeout_sec)) as response:
            payload = response.read()
            final_url = response.geturl()
            charset = response.headers.get_content_charset() or "utf-8"
        return payload.decode(charset, errors="replace"), final_url, time.monotonic() - started

    worker_count = min(max(1, int(max_workers)), max(1, len(unique_sources)))
    if not unique_sources:
        return documents, tuple(errors), diagnostics

    with ThreadPoolExecutor(
        max_workers=worker_count,
        thread_name_prefix="playlist_fetch",
    ) as executor:
        future_map = {executor.submit(fetch_one, source): source for source in unique_sources}
        completed: Dict[str, object] = {}
        completed_count = 0
        for future in as_completed(future_map):
            source = future_map[future]
            try:
                completed[source.url] = future.result()
            except Exception as error:
                completed[source.url] = error
            finally:
                completed_count += 1
                if progress_callback is not None:
                    progress_callback(completed_count, len(unique_sources))

    # Interpret in configured order so concurrency never changes visible ordering.
    for source in unique_sources:
        result = completed[source.url]
        if isinstance(result, Exception):
            detail = f"{source.name or source.url}: {type(result).__name__}: {result}"
            errors.append(detail)
            diagnostics[source.url] = {
                "ok": False,
                "source_name": source.name or source.url,
                "error": str(result),
            }
            continue
        text, final_url, duration = result
        documents[source.url] = text
        diagnostics[source.url] = {
            "ok": True,
            "source_name": source.name or source.url,
            "final_url": final_url,
            "fetch_duration_sec": round(float(duration), 4),
            "size_bytes": len(text.encode("utf-8", errors="replace")),
        }

    return documents, tuple(errors), diagnostics


def _extract_expiry(*values: str) -> Optional[float]:
    expiries: List[int] = []
    for value in values:
        text = str(value or "")
        for match in re.finditer(r"(?i)(?:^|[?&/~=/])(?:exp|expires|expiry)=(\d{9,12})", text):
            try:
                expiries.append(int(match.group(1)))
            except ValueError:
                continue
    return float(min(expiries)) if expiries else None


def _parse_hls_quality(text: str, manifest_url: str = "") -> Optional[dict]:
    return parse_hls_manifest_quality(
        text,
        manifest_url,
        motion_cap_fps=50.0,
        expiry_parser=lambda value: _extract_expiry(value),
    )


def _parse_dash_frame_rate(value: str) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    if "/" in text:
        left, right = text.split("/", 1)
        try:
            denominator = float(right)
            return float(left) / denominator if denominator else 0.0
        except ValueError:
            return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _parse_dash_quality(text: str) -> Tuple[bool, int, int, float, int, str, bool]:
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return False, 0, 0, 0.0, 0, "", False

    widevine = "edef8ba9" in text.casefold() or "widevine" in text.casefold()
    best = None
    for element in root.iter():
        if element.tag.rsplit("}", 1)[-1] != "Representation":
            continue
        try:
            width = int(element.attrib.get("width") or 0)
            height = int(element.attrib.get("height") or 0)
            bitrate = int(element.attrib.get("bandwidth") or 0)
        except ValueError:
            width = height = bitrate = 0
        fps = _parse_dash_frame_rate(element.attrib.get("frameRate") or "")
        rank = (1 if fps >= 49 else 0, height, fps, width * height, bitrate)
        if best is None or rank > best[0]:
            best = (rank, width, height, fps, bitrate)
    if best is None:
        return False, 0, 0, 0.0, 0, "", widevine
    _, width, height, fps, bitrate = best
    return True, width, height, fps, bitrate, "progressive", widevine

def probe_candidate_hls(
    candidate: SourceCandidate,
    *,
    timeout_sec: float = 15.0,
) -> SourceCandidate:
    """Inspect availability and master-playlist quality without starting a recorder."""
    provider = str(candidate.extra.get("provider") or "UNKNOWN").strip().upper()
    if not candidate.stream_url:
        return replace(
            candidate,
            launchable=False,
            probe_status="no_playable_source",
            reason="no playable source yet",
        )

    headers = {"User-Agent": DEFAULT_HTTP_USER_AGENT}
    headers.update(PROVIDER_PROBE_HEADERS.get(provider, {}))
    # Playlist/source metadata wins over profile defaults, matching the mature
    # recorder's precedence direction.
    headers.update(dict(candidate.headers or {}))
    expiry = _extract_expiry(
        candidate.stream_url,
        candidate.raw_stream_url,
        *[str(value) for value in dict(candidate.headers or {}).values()],
    )

    if candidate.unsupported_drm:
        return replace(
            candidate,
            expiry=expiry,
            expiry_source="URL/header" if expiry is not None else "",
            launchable=False,
            probe_status="unsupported",
            reason=f"unsupported DRM ({candidate.unsupported_drm})",
        )

    if expiry is not None and expiry <= time.time():
        return replace(
            candidate,
            expiry=expiry,
            expiry_source="URL",
            launchable=False,
            probe_status="expired",
            reason="authorization expired",
        )

    try:
        request = Request(candidate.stream_url, headers=headers)
        with urlopen(request, timeout=float(timeout_sec)) as response:
            payload = response.read(1024 * 1024)
            final_url = response.geturl()
            content_type = str(response.headers.get("Content-Type") or "")
        text = payload.decode("utf-8", errors="replace")
        expiry = _extract_expiry(
            candidate.stream_url,
            final_url,
            text,
            *[str(value) for value in headers.values()],
        ) or expiry
        is_hls = "#EXTM3U" in text or "mpegurl" in content_type.casefold()
        is_dash = "<MPD" in text[:500] or "dash+xml" in content_type.casefold()
        unsupported_drm = candidate.unsupported_drm
        if is_dash:
            (
                quality_known,
                width,
                height,
                fps,
                bitrate,
                scan_type,
                widevine,
            ) = _parse_dash_quality(text)
            if widevine:
                unsupported_drm = "Widevine"
        else:
            hls_quality = _parse_hls_quality(text, final_url)
            if hls_quality:
                quality_known = bool(hls_quality.get("quality_known"))
                width = int(hls_quality.get("video_width") or 0)
                height = int(hls_quality.get("video_height") or 0)
                fps = float(hls_quality.get("video_fps") or 0.0)
                bitrate = int(hls_quality.get("video_bitrate_bps") or 0)
                scan_type = str(hls_quality.get("video_scan_type") or "")
            else:
                quality_known = False
                width = height = bitrate = 0
                fps = 0.0
                scan_type = ""
            if "com.widevine" in text.casefold() or "widevine" in text.casefold():
                unsupported_drm = "Widevine"
        is_playlist = bool(is_hls or is_dash)
        expired_now = expiry is not None and expiry <= time.time()
        launchable = bool(is_playlist and not unsupported_drm and not expired_now)

        quality_source = "manifest" if quality_known else ""
        video_fps_source = "manifest" if fps > 0 else ""
        ffprobe_failure = ""
        manifest_complete = bool(
            fps > 0
            and width > 0
            and height > 0
            and bitrate > 0
        )
        if launchable and not manifest_complete:
            try:
                probe_stream_url = (
                    str(hls_quality.get("manifest_variant_url") or "").strip()
                    if is_hls and hls_quality
                    else ""
                ) or final_url or candidate.stream_url
                ffprobe_quality = probe_stream_quality_ffprobe(
                    probe_stream_url,
                    headers,
                    timeout_sec=min(20.0, max(1.0, float(timeout_sec))),
                    target_quality={
                        "video_width": width,
                        "video_height": height,
                        "video_fps": fps,
                    },
                    motion_cap_fps=50.0,
                )
                if ffprobe_quality:
                    ffprobe_fps = float(ffprobe_quality.get("video_fps") or 0.0)
                    if fps <= 0 and ffprobe_fps > 0:
                        fps = ffprobe_fps
                        video_fps_source = "ffprobe"

                    ffprobe_width = int(ffprobe_quality.get("video_width") or 0)
                    ffprobe_height = int(ffprobe_quality.get("video_height") or 0)
                    if width <= 0 and ffprobe_width > 0:
                        width = ffprobe_width
                    if height <= 0 and ffprobe_height > 0:
                        height = ffprobe_height

                    ffprobe_bitrate = int(
                        ffprobe_quality.get("video_bitrate_bps") or 0
                    )
                    if bitrate <= 0 and ffprobe_bitrate > 0:
                        bitrate = ffprobe_bitrate

                    ffprobe_scan_type = str(
                        ffprobe_quality.get("video_scan_type") or ""
                    ).strip()
                    if not scan_type and ffprobe_scan_type:
                        scan_type = ffprobe_scan_type

                    quality_known = bool(
                        fps > 0
                        or (width > 0 and height > 0)
                        or bitrate > 0
                    )
                    quality_source = (
                        "manifest+ffprobe" if quality_source else "ffprobe"
                    )
            except Exception as error:
                # Availability is already proven by the manifest request. A
                # quality fallback failure must not turn a working source OFF.
                ffprobe_failure = f"{type(error).__name__}: {error}"

        probe_status = (
            "expired" if expired_now else "working" if launchable else "unsupported"
        )
        return replace(
            candidate,
            final_stream_url=final_url,
            expiry=expiry,
            expiry_source="URL/manifest" if expiry is not None else "",
            unsupported_drm=unsupported_drm,
            stream_type="DASH" if is_dash else "HLS" if is_hls else candidate.stream_type,
            playback_fingerprint=_playback_fingerprint(final_url, headers),
            quality_known=quality_known,
            video_width=width,
            video_height=height,
            video_fps=fps,
            video_bitrate_bps=bitrate,
            video_scan_type=scan_type,
            launchable=launchable,
            probe_status=probe_status,
            reason=(
                "authorization expired"
                if expired_now
                else "unsupported DRM"
                if unsupported_drm
                else "not an HLS/DASH playlist"
                if not is_playlist
                else ""
            ),
            extra={
                **dict(candidate.extra),
                "manifest_final_url": final_url,
                "manifest_variant_url": (
                    str(hls_quality.get("manifest_variant_url") or "")
                    if is_hls and hls_quality
                    else ""
                ),
                "quality_source": quality_source,
                "video_fps_source": video_fps_source,
                "ffprobe_probe_failure": ffprobe_failure,
            },
        )
    except HTTPError as error:
        blocked = int(getattr(error, "code", 0) or 0) in (401, 403, 451)
        return replace(
            candidate,
            expiry=expiry,
            expiry_source="URL" if expiry is not None else "",
            launchable=False,
            access_blocked=blocked,
            probe_status="access_blocked" if blocked else "probe_failed",
            probe_error=f"HTTP {getattr(error, 'code', '')}",
            reason="access blocked" if blocked else f"HTTP {getattr(error, 'code', '')}",
        )
    except (URLError, TimeoutError, OSError, ValueError) as error:
        return replace(
            candidate,
            expiry=expiry,
            expiry_source="URL" if expiry is not None else "",
            launchable=False,
            probe_status="probe_failed",
            probe_error=f"{type(error).__name__}: {error}",
            reason="probe failed",
        )


def probe_candidates(
    candidates: Sequence[SourceCandidate],
    *,
    timeout_sec: float = 15.0,
    max_workers: int = 8,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> Tuple[SourceCandidate, ...]:
    if not candidates:
        return ()

    result: List[Optional[SourceCandidate]] = [None] * len(candidates)
    worker_count = min(max(1, int(max_workers)), len(candidates))
    with ThreadPoolExecutor(
        max_workers=worker_count,
        thread_name_prefix="candidate_probe",
    ) as executor:
        future_map = {
            executor.submit(probe_candidate_hls, candidate, timeout_sec=timeout_sec): index
            for index, candidate in enumerate(candidates)
        }
        completed_count = 0
        for future in as_completed(future_map):
            index = future_map[future]
            try:
                result[index] = future.result()
            except Exception as error:
                result[index] = replace(
                    candidates[index],
                    launchable=False,
                    probe_status="probe_failed",
                    probe_error=f"{type(error).__name__}: {error}",
                    reason="probe failed",
                )
            finally:
                completed_count += 1
                if progress_callback is not None:
                    progress_callback(completed_count, len(candidates))
    return tuple(
        item if item is not None else candidates[index]
        for index, item in enumerate(result)
    )
