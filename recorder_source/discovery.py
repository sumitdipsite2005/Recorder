"""Shared playlist source-acquisition boundary.

The functions here fetch configured playlist documents, normalize matching M3U
entries into ``SourceCandidate`` objects, and perform a deliberately small HLS
availability/quality probe for Inspect/Watch.  Recording execution remains out
of this module.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote, unquote, urljoin, urlsplit
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

from .headers import canonicalize_header_name
from .manifest import manifest_type_from_text
from .matching import evaluate_match
from .playback import playback_fingerprint
from .playlist_headers import (
    NORMALIZED_PLAYLIST_HEADER_POLICY,
    parse_stream_url_and_headers,
)
from .quality import (
    extract_auth_expiry,
    merge_auth_expiries,
    merge_ffprobe_quality_evidence,
    inspect_dash_manifest_drm,
    inspect_hls_manifest_drm,
    parse_dash_manifest_quality,
    parse_hls_manifest_quality,
    QUALITY_FFPROBE_TIMEOUT_SEC,
    QUALITY_PROBE_WORKERS,
    probe_stream_quality_ffprobe,
    quality_probe_identity,
    run_grouped_quality_probes,
)
from .policy import (
    PLAYLIST_GROUP_LIFECYCLES,
    PLAYLIST_USER_AGENTS,
    PROVIDER_ADDED_HEADERS,
    apply_lifecycle_scan_type_policy,
)
from . import transport as source_transport
from .selection import normalize_video_scan_type
from .models import (
    PlaylistSourceSpec,
    SourceAcquisitionRequest,
    SourceAcquisitionResult,
    SourceCandidate,
)



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

# Playlist-document and GitHub metadata fetches use the mature default UA.
DEFAULT_HTTP_USER_AGENT = PLAYLIST_USER_AGENTS["DEFAULT"]


def build_effective_probe_headers(
    provider: str,
    candidate_headers: Optional[Mapping[str, object]] = None,
    *,
    base_headers: Optional[Mapping[str, object]] = None,
    default_user_agent: Optional[str] = None,
) -> Dict[str, str]:
    """Build effective stream-request headers with mature precedence."""
    provider_name = str(provider or "UNKNOWN").strip().upper()
    headers: Dict[str, str] = {}
    defaults = (
        dict(base_headers)
        if base_headers is not None
        else dict(PROVIDER_ADDED_HEADERS.get(provider_name, {}))
    )
    _apply_headers(headers, defaults)
    _apply_headers(headers, candidate_headers or {})
    has_user_agent = any(
        str(name).casefold() == "user-agent" and str(value).strip()
        for name, value in headers.items()
    )
    if not has_user_agent:
        fallback = str(
            default_user_agent
            if default_user_agent is not None
            else PLAYLIST_USER_AGENTS.get("DEFAULT", "")
        ).strip()
        if not fallback:
            raise RuntimeError("Missing DEFAULT user-agent profile")
        headers["User-Agent"] = fallback
    return headers

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


def _apply_headers(target: Dict[str, str], values: Mapping[str, object]) -> None:
    """Apply one metadata layer case-insensitively; later layers win."""
    for raw_name, raw_value in values.items():
        if raw_value is None:
            continue
        name = canonicalize_header_name(str(raw_name))
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
    """Normalize playlist playback metadata through the shared parser."""
    return parse_stream_url_and_headers(
        raw_url,
        option_lines,
        policy=NORMALIZED_PLAYLIST_HEADER_POLICY,
    )

def _b64url_decode(value: str) -> bytes:
    text = str(value or "").strip()
    padding = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + padding)


def normalize_playlist_license_key(value: str) -> Tuple[str, ...]:
    """Normalize the ClearKey metadata forms already supported by mature ONE BEST."""
    text = str(value or "").strip()
    if not text:
        return ()
    if not text.startswith("{"):
        return (text,)
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return (text,)
    jwk_keys = data.get("keys") if isinstance(data, dict) else None
    if not isinstance(jwk_keys, list):
        return (text,)

    normalized: List[str] = []
    for item in jwk_keys:
        if not isinstance(item, dict):
            continue
        kty = str(item.get("kty") or "").strip().casefold()
        kid_b64 = str(item.get("kid") or "").strip()
        key_b64 = str(item.get("k") or "").strip()
        if kty != "oct" or not kid_b64 or not key_b64:
            continue
        try:
            kid_hex = _b64url_decode(kid_b64).hex()
            key_hex = _b64url_decode(key_b64).hex()
        except Exception as error:
            raise RuntimeError(
                "Invalid base64url value in ClearKey JWK license_key"
            ) from error
        if len(kid_hex) != 32 or len(key_hex) != 32:
            raise RuntimeError(
                "Invalid ClearKey JWK license_key: "
                "KID and key must each decode to 16 bytes"
            )
        pair = f"{kid_hex}:{key_hex}"
        if pair not in normalized:
            normalized.append(pair)

    if not normalized:
        raise RuntimeError(
            "ClearKey JWK license_key contained no usable oct keys"
        )
    return tuple(normalized)


def _playlist_license_metadata(
    option_lines: Sequence[str],
    stream_url: str = "",
) -> Tuple[str, Tuple[str, ...], str]:
    license_type = ""
    keys: List[str] = []
    type_prefix = "#KODIPROP:inputstream.adaptive.license_type="
    key_prefix = "#KODIPROP:inputstream.adaptive.license_key="
    for raw_line in option_lines:
        line = str(raw_line or "").strip()
        if line.casefold().startswith(type_prefix.casefold()):
            license_type = line[len(type_prefix):].strip()
        elif line.casefold().startswith(key_prefix.casefold()):
            for value in normalize_playlist_license_key(
                line[len(key_prefix):].strip()
            ):
                if value not in keys:
                    keys.append(value)

    normalized_type = license_type.casefold().replace("-", "").replace("_", "")
    try:
        parsed = urlsplit(str(stream_url or ""))
        host = str(parsed.hostname or "").casefold()
        path = str(parsed.path or "").casefold()
    except Exception:
        host = ""
        path = str(stream_url or "").casefold()
    direct_drmlive_dash = bool(
        (host == "drmlive.net" or host.endswith(".drmlive.net"))
        and path.endswith(".mpd")
    )
    unsupported_drm = (
        "Widevine"
        if "widevine" in normalized_type and direct_drmlive_dash
        else ""
    )
    return license_type, tuple(keys), unsupported_drm


def _candidate_decryption_key(candidate: SourceCandidate) -> str:
    """Return a directly usable ClearKey value for FFprobe/FFmpeg when present."""
    for raw_value in candidate.keys:
        value = str(raw_value or "").strip()
        match = re.fullmatch(r"[0-9a-fA-F]{32}:([0-9a-fA-F]{32})", value)
        if match:
            return match.group(1)
    return ""


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
        license_type, keys, unsupported_drm = _playlist_license_metadata(
            option_lines,
            stream_url,
        )
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
                stream_type=source_transport.stream_type_from_url(stream_url),
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


def _parse_source_timestamp(value: object) -> Optional[float]:
    text = str(value or "").strip()
    if not text:
        return None

    upper = text.upper()
    if upper.endswith(" IST"):
        text = text[:-4].rstrip() + "+05:30"
    elif upper.endswith(" UTC") or upper.endswith(" GMT"):
        text = text[:-4].rstrip() + "+00:00"
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        pass

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
    ):
        try:
            return datetime.strptime(text, fmt).timestamp()
        except ValueError:
            continue
    return None


def _embedded_playlist_generated_timestamp(text: str) -> Optional[float]:
    """Extract an explicit generated/updated timestamp near the document header."""
    timestamp_pattern = re.compile(
        r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2})?"
        r"(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2}|\s+(?:UTC|GMT|IST))?"
        r"|\d{2}[-/]\d{2}[-/]\d{4}\s+\d{2}:\d{2}(?::\d{2})?)",
        re.IGNORECASE,
    )
    freshness_words = re.compile(
        r"\b(?:generated|generated\s+at|generated\s+on|updated|updated\s+at|"
        r"updated\s+on|last\s+updated)\b",
        re.IGNORECASE,
    )
    for line in str(text or "").splitlines()[:80]:
        if not freshness_words.search(line):
            continue
        match = timestamp_pattern.search(line)
        if not match:
            continue
        parsed = _parse_source_timestamp(match.group(1))
        if parsed is not None:
            return parsed
    return None


def _github_raw_file_parts(url: str) -> Optional[Tuple[str, str, str, str]]:
    try:
        parsed = urlsplit(str(url or ""))
    except Exception:
        return None
    if parsed.netloc.casefold() != "raw.githubusercontent.com":
        return None
    parts = [unquote(part) for part in parsed.path.split("/") if part]
    if len(parts) < 4:
        return None
    owner, repo = parts[0], parts[1]
    if len(parts) >= 6 and parts[2:4] == ["refs", "heads"]:
        branch = parts[4]
        file_parts = parts[5:]
    else:
        branch = parts[2]
        file_parts = parts[3:]
    if not branch or not file_parts:
        return None
    return owner, repo, branch, "/".join(file_parts)


def _github_file_commit_timestamp(
    playlist_url: str,
    *,
    timeout_sec: float = 5.0,
    max_attempts: int = 3,
    retry_base_sec: float = 0.35,
) -> Optional[float]:
    parts = _github_raw_file_parts(playlist_url)
    if parts is None:
        return None
    owner, repo, branch, path = parts
    api_url = (
        f"https://api.github.com/repos/{quote(owner, safe='')}/{quote(repo, safe='')}/commits"
        f"?path={quote(path, safe='/')}&sha={quote(branch, safe='')}&per_page=1"
    )
    request = Request(
        api_url,
        headers={
            "User-Agent": DEFAULT_HTTP_USER_AGENT,
            "Accept": "application/vnd.github+json",
        },
    )
    payload = None
    attempts = max(1, int(max_attempts))
    for attempt in range(attempts):
        try:
            with urlopen(request, timeout=float(timeout_sec)) as response:
                payload = json.loads(
                    response.read().decode("utf-8", errors="replace")
                )
            break
        except HTTPError as error:
            # Retry only transient HTTP failures. Permanent/rate-limit responses
            # should fall back immediately rather than making the scan hang.
            if error.code not in (408, 425, 429, 500, 502, 503, 504):
                return None
        except (URLError, TimeoutError, OSError):
            pass
        except Exception:
            return None

        if attempt + 1 < attempts:
            time.sleep(float(retry_base_sec) * (2 ** attempt))

    if payload is None:
        return None
    if not isinstance(payload, list) or not payload:
        return None
    commit = payload[0].get("commit") if isinstance(payload[0], dict) else None
    if not isinstance(commit, dict):
        return None
    for section_name in ("committer", "author"):
        section = commit.get(section_name)
        if isinstance(section, dict):
            parsed = _parse_source_timestamp(section.get("date"))
            if parsed is not None:
                return parsed
    return None


def resolve_playlist_source_freshness(
    playlist_url: str,
    text: str,
    fetch_diagnostic: Optional[Mapping[str, object]] = None,
    *,
    previous: Optional[Mapping[str, object]] = None,
    now_ts: Optional[float] = None,
) -> Mapping[str, object]:
    """Resolve best-known document freshness and preserve witnessed ordering.

    On the first observation, prefer GitHub file commit time, then an explicit
    generated/updated timestamp in the document, then HTTP Last-Modified.
    During one Coordinator run, a changed document is stronger evidence: we
    witnessed the newer version ourselves, so its observation time becomes the
    freshness timestamp without another external lookup.
    """
    payload = str(text or "")
    content_hash = hashlib.sha256(payload.encode("utf-8", errors="replace")).hexdigest()
    previous_hash = str((previous or {}).get("content_hash") or "")
    if previous_hash:
        if previous_hash == content_hash:
            previous_result = dict(previous or {})
            previous_source = str(previous_result.get("source") or "unknown")
            # A transient GitHub failure on the first scan must not permanently
            # freeze a weaker fallback. Retry unchanged GitHub documents until a
            # commit timestamp is obtained. A witnessed in-run change remains the
            # stronger evidence and does not need a GitHub replacement.
            if (
                _github_raw_file_parts(playlist_url) is not None
                and previous_source not in ("commit", "observed")
            ):
                commit_ts = _github_file_commit_timestamp(playlist_url)
                if commit_ts is not None:
                    return {
                        "timestamp": commit_ts,
                        "source": "commit",
                        "content_hash": content_hash,
                    }
            return previous_result
        return {
            "timestamp": float(time.time() if now_ts is None else now_ts),
            "source": "observed",
            "content_hash": content_hash,
        }

    commit_ts = _github_file_commit_timestamp(playlist_url)
    if commit_ts is not None:
        return {
            "timestamp": commit_ts,
            "source": "commit",
            "content_hash": content_hash,
        }

    generated_ts = _embedded_playlist_generated_timestamp(payload)
    if generated_ts is not None:
        return {
            "timestamp": generated_ts,
            "source": "generated",
            "content_hash": content_hash,
        }

    last_modified = str((fetch_diagnostic or {}).get("last_modified") or "").strip()
    if last_modified:
        try:
            parsed = parsedate_to_datetime(last_modified)
            if parsed is not None:
                return {
                    "timestamp": parsed.timestamp(),
                    "source": "last-modified",
                    "content_hash": content_hash,
                }
        except (TypeError, ValueError, OverflowError):
            pass

    return {
        "timestamp": None,
        "source": "unknown",
        "content_hash": content_hash,
    }


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
            header_get = getattr(response.headers, "get", None)
            last_modified = (
                header_get("Last-Modified")
                if callable(header_get)
                else None
            )
            etag = header_get("ETag") if callable(header_get) else None
        return (
            payload.decode(charset, errors="replace"),
            final_url,
            time.monotonic() - started,
            last_modified,
            etag,
        )

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
        text, final_url, duration, last_modified, etag = result
        documents[source.url] = text
        diagnostics[source.url] = {
            "ok": True,
            "source_name": source.name or source.url,
            "final_url": final_url,
            "fetch_duration_sec": round(float(duration), 4),
            "size_bytes": len(text.encode("utf-8", errors="replace")),
            "last_modified": str(last_modified or ""),
            "etag": str(etag or ""),
        }

    return documents, tuple(errors), diagnostics


def _extract_expiry(*values: str) -> Optional[float]:
    value = extract_auth_expiry(*values)
    return float(value) if value is not None else None


def _parse_hls_quality(text: str, manifest_url: str = "") -> Optional[dict]:
    return parse_hls_manifest_quality(
        text,
        manifest_url,
        motion_cap_fps=50.0,
        expiry_parser=lambda value: _extract_expiry(value),
    )


def _effective_probe_headers(candidate: SourceCandidate) -> Dict[str, str]:
    provider = str(candidate.extra.get("provider") or "UNKNOWN").strip().upper()
    return build_effective_probe_headers(provider, candidate.headers)


def _merge_expiries(*values: Optional[float]) -> Optional[float]:
    merged = merge_auth_expiries(*values)
    return float(merged) if merged is not None else None


def _candidate_url_header_expiry(candidate: SourceCandidate) -> Optional[float]:
    return _extract_expiry(
        candidate.stream_url,
        candidate.raw_stream_url,
        *[str(value) for value in dict(candidate.headers or {}).values()],
    )


def probe_candidate_hls(
    candidate: SourceCandidate,
    *,
    timeout_sec: Optional[float] = None,
) -> SourceCandidate:
    """Inspect one normalized candidate and return shared quality/probe facts."""
    if not candidate.stream_url:
        return replace(
            candidate,
            launchable=False,
            probe_status="no_playable_source",
            reason="no playable source yet",
        )

    headers = _effective_probe_headers(candidate)
    timeout_value = (
        source_transport.QUALITY_HTTP_TIMEOUT_SEC
        if timeout_sec is None
        else float(timeout_sec)
    )
    url_header_expiry = _candidate_url_header_expiry(candidate)
    if url_header_expiry is not None and url_header_expiry <= time.time():
        return replace(
            candidate,
            expiry=url_header_expiry,
            expiry_source="URL/header",
            launchable=False,
            probe_status="expired",
            reason="authorization expired",
        )

    try:
        text, final_url = source_transport.fetch_stream_manifest_text(
            candidate.stream_url,
            headers,
            default_user_agent=PLAYLIST_USER_AGENTS["DEFAULT"],
            urlopen_fn=urlopen,
        )
        manifest_expiry = _extract_expiry(final_url, text)
        expiry = _merge_expiries(url_header_expiry, manifest_expiry)

        manifest_type = manifest_type_from_text(text)
        is_hls = manifest_type == "HLS"
        is_dash = manifest_type == "DASH"
        hls_quality = None
        dash_quality = None
        if is_dash:
            dash_quality = parse_dash_manifest_quality(
                text,
                final_url,
                motion_cap_fps=50.0,
            )
            if dash_quality:
                quality_known = bool(dash_quality.get("quality_known"))
                width = int(dash_quality.get("video_width") or 0)
                height = int(dash_quality.get("video_height") or 0)
                fps = float(dash_quality.get("video_fps") or 0.0)
                bitrate = int(dash_quality.get("video_bitrate_bps") or 0)
                scan_type = str(dash_quality.get("video_scan_type") or "")
                manifest_expiry = _merge_expiries(
                    manifest_expiry,
                    dash_quality.get("manifest_expiry"),
                )
                expiry = _merge_expiries(url_header_expiry, manifest_expiry)
            else:
                quality_known = False
                width = height = bitrate = 0
                fps = 0.0
                scan_type = ""
            manifest_drm = inspect_dash_manifest_drm(text)

            resource_route = source_transport.resolve_selected_dash_resource_route(
                dash_quality or {},
                headers,
                default_user_agent=PLAYLIST_USER_AGENTS["DEFAULT"],
                expiry_parser=_extract_expiry,
                urlopen_fn=urlopen,
            )
            resource_expiry = resource_route.get("resource_expiry")
            expiry = _merge_expiries(
                url_header_expiry,
                manifest_expiry,
                resource_expiry,
            )
        else:
            resource_route = {}
            resource_expiry = None
            hls_quality = _parse_hls_quality(text, final_url)
            if hls_quality:
                quality_known = bool(hls_quality.get("quality_known"))
                width = int(hls_quality.get("video_width") or 0)
                height = int(hls_quality.get("video_height") or 0)
                fps = float(hls_quality.get("video_fps") or 0.0)
                bitrate = int(hls_quality.get("video_bitrate_bps") or 0)
                scan_type = str(hls_quality.get("video_scan_type") or "")
                manifest_expiry = _merge_expiries(
                    manifest_expiry,
                    hls_quality.get("manifest_expiry"),
                )
                expiry = _merge_expiries(url_header_expiry, manifest_expiry)
            else:
                quality_known = False
                width = height = bitrate = 0
                fps = 0.0
                scan_type = ""
            manifest_drm = inspect_hls_manifest_drm(text)
            resource_route = {}
            resource_expiry = None

        drm_inspection_failure = ""
        hls_variant_probe_status = ""
        hls_variant_probe_failure = ""
        if (
            is_hls
            and hls_quality
            and not candidate.keys
            and not manifest_drm.get("drm_key_required")
        ):
            variant_url = str(
                hls_quality.get("manifest_variant_url") or ""
            ).strip()
            if variant_url:
                child_text = ""
                child_error = None

                try:
                    child_text, _ = source_transport.fetch_stream_manifest_text(
                        variant_url,
                        headers,
                        default_user_agent=PLAYLIST_USER_AGENTS["DEFAULT"],
                        urlopen_fn=urlopen,
                    )
                except HTTPError as error:
                    child_error = error
                    if int(getattr(error, "code", 0) or 0) == 403:
                        try:
                            child_text = (
                                source_transport.fetch_hls_child_with_master_cookie_session(
                                    final_url or candidate.stream_url,
                                    variant_url,
                                    headers,
                                    timeout_sec=timeout_value,
                                )
                            )
                            child_error = None
                        except Exception as retry_error:
                            child_error = retry_error
                except Exception as error:
                    child_error = error

                if child_text:
                    if "#EXTM3U" not in child_text:
                        child_failure = source_transport.classify_hls_variant_probe_failure(
                            non_hls_response=True,
                        )
                        hls_variant_probe_status = str(
                            child_failure.get("status") or ""
                        )
                        hls_variant_probe_failure = str(
                            child_failure.get("reason") or ""
                        )
                    else:
                        try:
                            child_drm = inspect_hls_manifest_drm(child_text)
                        except Exception as error:
                            drm_inspection_failure = (
                                "HLS DRM inspection failed — "
                                f"{type(error).__name__}: {error}"
                            )
                        else:
                            manifest_drm = {
                                **dict(manifest_drm),
                                "drm_protected": bool(
                                    manifest_drm.get("drm_protected")
                                    or child_drm.get("drm_protected")
                                ),
                                "drm_key_required": bool(
                                    manifest_drm.get("drm_key_required")
                                    or child_drm.get("drm_key_required")
                                ),
                                "drm_detail": str(
                                    child_drm.get("drm_detail")
                                    or manifest_drm.get("drm_detail")
                                    or ""
                                ),
                            }
                elif child_error is not None:
                    child_failure = source_transport.classify_hls_variant_probe_failure(
                        child_error,
                    )
                    hls_variant_probe_status = str(
                        child_failure.get("status") or ""
                    )
                    hls_variant_probe_failure = str(
                        child_failure.get("reason") or ""
                    )

        is_playlist = bool(is_hls or is_dash)
        expired_now = expiry is not None and expiry <= time.time()
        drm_key_required = bool(manifest_drm.get("drm_key_required"))
        drm_key_missing = bool(drm_key_required and not candidate.keys)
        probe_transport_launchable = bool(
            is_playlist
            and not drm_key_missing
            and not expired_now
            and not drm_inspection_failure
            and not hls_variant_probe_failure
        )
        launchable = bool(
            probe_transport_launchable and not candidate.unsupported_drm
        )
        decryption_key = _candidate_decryption_key(candidate)

        quality_source = "manifest" if quality_known else ""
        video_resolution_source = "manifest" if width > 0 or height > 0 else ""
        video_fps_source = "manifest" if fps > 0 else ""
        video_scan_type_source = "manifest" if scan_type else ""
        video_bitrate_source = "manifest" if bitrate > 0 else ""

        source_group = str(candidate.extra.get("source_group") or "").strip().upper()
        scan_type, video_scan_type_source = apply_lifecycle_scan_type_policy(
            PLAYLIST_GROUP_LIFECYCLES.get(source_group, "") if is_playlist else "",
            scan_type,
            video_scan_type_source,
        )

        ffprobe_failure = ""
        bitrate_sample_failure = ""
        manifest_complete = bool(
            fps > 0
            and width > 0
            and height > 0
            and bitrate > 0
        )
        hls_needs_scan_type = bool(is_hls and not scan_type)
        if probe_transport_launchable and (
            not manifest_complete or hls_needs_scan_type
        ):
            try:
                probe_stream_url = (
                    str(hls_quality.get("manifest_variant_url") or "").strip()
                    if is_hls and hls_quality
                    else ""
                ) or final_url or candidate.stream_url
                ffprobe_quality = probe_stream_quality_ffprobe(
                    probe_stream_url,
                    headers,
                    timeout_sec=QUALITY_FFPROBE_TIMEOUT_SEC,
                    target_quality={
                        "video_width": width,
                        "video_height": height,
                        "video_fps": fps,
                    },
                    motion_cap_fps=50.0,
                    decryption_key=decryption_key,
                    sample_missing_bitrate=(bitrate <= 0),
                )
                if ffprobe_quality:
                    bitrate_sample_failure = str(
                        ffprobe_quality.get("_bitrate_sample_failure") or ""
                    )
                    merged_quality = merge_ffprobe_quality_evidence(
                        {
                            "quality_known": quality_known,
                            "quality_source": quality_source,
                            "video_fps": fps,
                            "video_fps_source": video_fps_source,
                            "video_width": width,
                            "video_height": height,
                            "video_resolution_source": video_resolution_source,
                            "video_bitrate_bps": bitrate,
                            "video_bitrate_source": video_bitrate_source,
                            "video_scan_type": scan_type,
                            "video_scan_type_source": video_scan_type_source,
                        },
                        ffprobe_quality,
                        include_scan_type=is_hls,
                        include_sample_in_quality_source=True,
                        default_bitrate_source="ffprobe",
                    )
                    quality_known = bool(merged_quality["quality_known"])
                    quality_source = str(merged_quality["quality_source"] or "")
                    fps = float(merged_quality["video_fps"] or 0.0)
                    video_fps_source = str(
                        merged_quality["video_fps_source"] or ""
                    )
                    width = int(merged_quality["video_width"] or 0)
                    height = int(merged_quality["video_height"] or 0)
                    video_resolution_source = str(
                        merged_quality["video_resolution_source"] or ""
                    )
                    bitrate = int(merged_quality["video_bitrate_bps"] or 0)
                    video_bitrate_source = str(
                        merged_quality["video_bitrate_source"] or ""
                    )
                    scan_type = str(merged_quality["video_scan_type"] or "")
                    video_scan_type_source = str(
                        merged_quality["video_scan_type_source"] or ""
                    )
            except Exception as error:
                ffprobe_failure = f"{type(error).__name__}: {error}"

        probe_status = (
            "expired"
            if expired_now
            else "unsupported"
            if candidate.unsupported_drm
            else "drm_key_missing"
            if drm_key_missing
            else "drm_check_failed"
            if drm_inspection_failure
            else hls_variant_probe_status
            if hls_variant_probe_failure
            else "working"
            if probe_transport_launchable
            else "unsupported"
        )
        return replace(
            candidate,
            final_stream_url=final_url,
            expiry=expiry,
            expiry_source="URL/manifest" if expiry is not None else "",
            stream_type="DASH" if is_dash else "HLS" if is_hls else candidate.stream_type,
            playback_fingerprint=playback_fingerprint(final_url, headers),
            quality_known=quality_known,
            quality_source=quality_source,
            video_width=width,
            video_height=height,
            video_resolution_source=video_resolution_source,
            video_fps=fps,
            video_fps_source=video_fps_source,
            video_bitrate_bps=bitrate,
            video_bitrate_source=video_bitrate_source,
            video_scan_type=scan_type,
            video_scan_type_source=video_scan_type_source,
            launchable=launchable,
            probe_status=probe_status,
            reason=(
                "authorization expired"
                if expired_now
                else f"unsupported DRM ({candidate.unsupported_drm})"
                if candidate.unsupported_drm
                else "DRM key missing"
                if drm_key_missing
                else drm_inspection_failure
                if drm_inspection_failure
                else hls_variant_probe_failure
                if hls_variant_probe_failure
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
                "manifest_expiry": manifest_expiry,
                "resource_expiry": resource_expiry,
                "selected_media_final_url": str(
                    resource_route.get("final_url") or ""
                ),
                "resource_probe_failure": str(
                    resource_route.get("failure") or ""
                ),
                "_dash_selected_route_index": resource_route.get("route_index"),
                **({
                    key: value
                    for key, value in dict(dash_quality or {}).items()
                    if str(key).startswith("_dash_")
                }),
                "drm_protected": bool(manifest_drm.get("drm_protected")),
                "drm_key_required": drm_key_required,
                "drm_key_missing": drm_key_missing,
                "drm_detail": str(manifest_drm.get("drm_detail") or ""),
                "drm_inspection_failure": drm_inspection_failure,
                "hls_variant_probe_status": hls_variant_probe_status,
                "hls_variant_probe_failure": hls_variant_probe_failure,
                "ffprobe_probe_failure": ffprobe_failure,
                "bitrate_sample_failure": bitrate_sample_failure,
                "probe_transport_launchable": probe_transport_launchable,
            },
        )
    except HTTPError as error:
        access = source_transport.classify_http_access_error(
            error,
            source_group=str(candidate.extra.get("source_group") or ""),
            provider=str(candidate.extra.get("provider") or ""),
        )
        blocked = bool(access.get("blocked"))
        return replace(
            candidate,
            expiry=url_header_expiry,
            expiry_source="URL/header" if url_header_expiry is not None else "",
            launchable=False,
            access_blocked=blocked,
            probe_status="access_blocked" if blocked else "probe_failed",
            probe_error=f"HTTP {getattr(error, 'code', '')}",
            reason="access blocked" if blocked else f"HTTP {getattr(error, 'code', '')}",
            extra={
                **dict(candidate.extra),
                "access_block_kind": str(access.get("kind") or ""),
                "access_block_http_status": access.get("http_status"),
                "geo_country": access.get("geo_country"),
            },
        )
    except (URLError, TimeoutError, OSError, ValueError) as error:
        return replace(
            candidate,
            expiry=url_header_expiry,
            expiry_source="URL/header" if url_header_expiry is not None else "",
            launchable=False,
            probe_status="probe_failed",
            probe_error=f"{type(error).__name__}: {error}",
            reason="probe failed",
        )


_PROBE_SOURCE_EXTRA_KEYS = frozenset({
    "source_name",
    "source_group",
    "provider",
    "provider_identity_hint",
})


def _reuse_probe_result(
    candidate: SourceCandidate,
    probed: SourceCandidate,
) -> SourceCandidate:
    """Apply shared probe facts without replacing source/metadata provenance."""
    merged_extra = dict(candidate.extra)
    merged_extra.update({
        key: value
        for key, value in dict(probed.extra).items()
        if key not in _PROBE_SOURCE_EXTRA_KEYS
    })

    expiry = _merge_expiries(
        _candidate_url_header_expiry(candidate),
        merged_extra.get("manifest_expiry"),
        merged_extra.get("resource_expiry"),
    )
    expired_now = expiry is not None and expiry <= time.time()
    transport_launchable = bool(
        merged_extra.get("probe_transport_launchable", probed.launchable)
    )
    drm_key_missing = bool(merged_extra.get("drm_key_missing"))
    launchable = bool(
        transport_launchable
        and not expired_now
        and not drm_key_missing
        and not candidate.unsupported_drm
    )
    if expired_now:
        status = "expired"
        reason = "authorization expired"
    elif candidate.unsupported_drm:
        status = "unsupported"
        reason = f"unsupported DRM ({candidate.unsupported_drm})"
    elif drm_key_missing:
        status = "drm_key_missing"
        reason = "DRM key missing"
    elif transport_launchable:
        status = "working"
        reason = ""
    else:
        status = probed.probe_status
        reason = probed.reason

    return replace(
        candidate,
        final_stream_url=probed.final_stream_url,
        expiry=expiry,
        expiry_source="URL/manifest" if expiry is not None else "",
        stream_type=probed.stream_type,
        playback_fingerprint=probed.playback_fingerprint,
        quality_known=probed.quality_known,
        quality_source=probed.quality_source,
        video_width=probed.video_width,
        video_height=probed.video_height,
        video_resolution_source=probed.video_resolution_source,
        video_fps=probed.video_fps,
        video_fps_source=probed.video_fps_source,
        video_bitrate_bps=probed.video_bitrate_bps,
        video_bitrate_source=probed.video_bitrate_source,
        video_scan_type=probed.video_scan_type,
        video_scan_type_source=probed.video_scan_type_source,
        launchable=launchable,
        probe_status=status,
        probe_error=probed.probe_error,
        access_blocked=probed.access_blocked,
        reason=candidate.reason if candidate.ignored else reason,
        extra=merged_extra,
    )


def probe_candidates(
    candidates: Sequence[SourceCandidate],
    *,
    timeout_sec: Optional[float] = None,
    max_workers: int = QUALITY_PROBE_WORKERS,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> Tuple[SourceCandidate, ...]:
    """Probe each effective stream once and reuse that result across observations."""
    if not candidates:
        return ()

    now = time.time()

    def group_key(candidate: SourceCandidate):
        return quality_probe_identity(
            candidate,
            effective_headers=_effective_probe_headers(candidate),
        )

    def representative(grouped_candidates: Sequence[SourceCandidate]) -> SourceCandidate:
        for candidate in grouped_candidates:
            expiry = _candidate_url_header_expiry(candidate)
            if expiry is None or expiry > now:
                return candidate
        return grouped_candidates[0]

    def probe_group(
        representative_candidate: SourceCandidate,
        grouped_candidates: Sequence[SourceCandidate],
    ) -> SourceCandidate:
        del grouped_candidates
        return probe_candidate_hls(
            representative_candidate,
            timeout_sec=timeout_sec,
        )

    def failure_result(
        representative_candidate: SourceCandidate,
        error: BaseException,
    ) -> SourceCandidate:
        return replace(
            representative_candidate,
            launchable=False,
            probe_status="probe_failed",
            probe_error=f"{type(error).__name__}: {error}",
            reason="probe failed",
        )

    return tuple(run_grouped_quality_probes(
        candidates,
        group_key=group_key,
        representative=representative,
        probe=probe_group,
        apply_result=_reuse_probe_result,
        failure_result=failure_result,
        max_workers=max_workers,
        progress_callback=progress_callback,
        thread_name_prefix="candidate_probe",
    ))
