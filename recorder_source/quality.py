"""Shared media-quality probing helpers.

This module owns the common FFprobe command shape and result interpretation used
by both the mature recorder and the Identity Coordinator. Callers remain free to
supply their own process runner so recorder-specific logging/redaction behavior
does not leak into the shared source-intelligence layer.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Callable, Mapping, Optional, Sequence, Tuple, Union
from urllib.parse import urljoin

from .models import SourceCandidate
from .selection import (
    comparable_motion_fps,
    has_quality_evidence,
    normalize_video_scan_type,
    video_quality_rank,
)


QualityLike = Union[Mapping[str, object], SourceCandidate]

QUALITY_PROBE_WORKERS = 6
QUALITY_FFPROBE_TIMEOUT_SEC = 20.0
QUALITY_BITRATE_SAMPLE_SEC = 4.0
QUALITY_BITRATE_SAMPLE_TIMEOUT_SEC = 12.0


def _quality_value(candidate: QualityLike, name: str, default: object = "") -> object:
    if isinstance(candidate, SourceCandidate):
        value = getattr(candidate, name, default)
        if value not in ("", None):
            return value
        if isinstance(candidate.extra, Mapping):
            return candidate.extra.get(name, value)
        return value
    return candidate.get(name, default)


def quality_probe_identity(
    candidate: QualityLike,
    *,
    effective_headers: Optional[Mapping[str, str]] = None,
) -> Tuple[object, ...]:
    """Return the request identity used to share one quality probe.

    Source provenance and playlist-entry position are deliberately excluded.
    They describe where an observation came from, not a different media probe.
    """
    stream_url = str(_quality_value(candidate, "stream_url", "") or "").strip()
    if effective_headers is None:
        raw_headers = _quality_value(candidate, "headers", {})
        headers = dict(raw_headers) if isinstance(raw_headers, Mapping) else {}
    else:
        headers = dict(effective_headers)

    normalized_headers = tuple(sorted(
        (
            str(name or "").strip().casefold(),
            str(value),
        )
        for name, value in headers.items()
        if str(name or "").strip() and value is not None
    ))
    raw_keys = _quality_value(candidate, "keys", ())
    has_decryption_keys = bool(raw_keys or ())
    return stream_url, normalized_headers, has_decryption_keys


_QUALITY_SOURCE_LABELS = {
    "manifest": "manifest",
    "ffprobe": "FFprobe",
    "stream": "FFprobe",
    "format": "FFprobe",
    "sps": "SPS",
    "h264-picture": "picture",
    "idet": "idet",
    "sample": "FFmpeg sample",
}


def _quality_source_labels(*source_values: object) -> list[str]:
    labels: list[str] = []
    for source_value in source_values:
        for raw_part in re.split(r"[+,]", str(source_value or "")):
            raw_part = raw_part.strip()
            if not raw_part:
                continue
            label = _QUALITY_SOURCE_LABELS.get(raw_part.casefold(), raw_part)
            if label not in labels:
                labels.append(label)
    return labels


def format_candidate_quality(
    candidate: QualityLike,
    *,
    motion_cap_fps: float = 50.0,
    include_provenance: bool = True,
) -> str:
    """Render Resolution | FPS/P-I | Bitrate using shared quality evidence."""
    if not has_quality_evidence(candidate):
        return "unknown"

    fps = float(_quality_value(candidate, "video_fps", 0.0) or 0.0)
    width = int(_quality_value(candidate, "video_width", 0) or 0)
    height = int(_quality_value(candidate, "video_height", 0) or 0)
    bitrate = int(_quality_value(candidate, "video_bitrate_bps", 0) or 0)
    scan_type = normalize_video_scan_type(
        _quality_value(candidate, "video_scan_type", "")
    )

    def with_sources(text: str, *source_values: object) -> str:
        if not include_provenance:
            return text
        labels = _quality_source_labels(*source_values)
        if not labels or labels == ["manifest"]:
            return text
        return f"{text} [{', '.join(labels)}]"

    resolution_text = (
        f"{width}x{height}"
        if width > 0 and height > 0
        else "resolution UNKNOWN"
    )
    resolution_text = with_sources(
        resolution_text,
        _quality_value(candidate, "video_resolution_source", ""),
    )

    if fps > 0:
        display_fps = (
            comparable_motion_fps(candidate, motion_cap_fps=float(motion_cap_fps))
            if scan_type == "interlaced"
            else fps
        )
        fps_text = f"{display_fps:.3f}".rstrip("0").rstrip(".")
        if scan_type == "interlaced":
            fps_text = f"{fps_text}i"
        elif scan_type == "progressive":
            fps_text = f"{fps_text}p"
        else:
            fps_text = f"{fps_text} fps (P/I UNKNOWN)"
        fps_text = with_sources(
            fps_text,
            _quality_value(candidate, "video_fps_source", ""),
            (
                _quality_value(candidate, "video_scan_type_source", "")
                if scan_type
                else ""
            ),
        )
    else:
        fps_text = "fps UNKNOWN"

    if bitrate > 0:
        bitrate_text = f"{int(round(bitrate / 1000.0))} Kbps"
        bitrate_source = str(
            _quality_value(candidate, "video_bitrate_source", "") or ""
        )
        if bitrate_source == "sample":
            bitrate_text = "~" + bitrate_text
        bitrate_text = with_sources(bitrate_text, bitrate_source)
    else:
        bitrate_text = "bitrate UNKNOWN"

    return " | ".join((resolution_text, fps_text, bitrate_text))


def parse_frame_rate(value: object) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    if "/" in text:
        left, right = text.split("/", 1)
        try:
            denominator = float(right)
            return float(left) / denominator if denominator else 0.0
        except (TypeError, ValueError):
            return 0.0
    try:
        return float(text)
    except (TypeError, ValueError):
        return 0.0


def extract_auth_expiries(value: str) -> Tuple[int, ...]:
    expiries = []
    for match in re.finditer(
        r'(?:^|[?&~=;])(?:exp|expires)=(\d+)',
        str(value or ""),
        flags=re.IGNORECASE,
    ):
        expiries.append(int(match.group(1)))
    return tuple(expiries)


def extract_auth_expiry(*values: object) -> Optional[int]:
    expiries = []
    for value in values:
        expiries.extend(extract_auth_expiries(str(value or "")))
    return min(expiries) if expiries else None


def merge_auth_expiries(*values: Optional[float]) -> Optional[int]:
    known = [int(value) for value in values if value is not None]
    return min(known) if known else None


def inspect_hls_manifest_drm(manifest_text: str) -> dict:
    """Identify HLS encryption that needs an external DRM/decryption key."""
    result = {
        "drm_protected": False,
        "drm_key_required": False,
        "drm_detail": "",
    }

    for raw_line in str(manifest_text or "").splitlines():
        line = raw_line.strip()
        if not line.startswith(("#EXT-X-KEY:", "#EXT-X-SESSION-KEY:")):
            continue

        attributes = line.split(":", 1)[1]
        method_match = re.search(
            r'(?:^|,)\s*METHOD=([^,]+)',
            attributes,
            re.IGNORECASE,
        )
        method = method_match.group(1).strip().strip('"') if method_match else ""
        if not method or method.upper() == "NONE":
            continue

        result["drm_protected"] = True
        keyformat_match = re.search(
            r'(?:^|,)\s*KEYFORMAT=(?:"([^"]*)"|([^,]*))',
            attributes,
            re.IGNORECASE,
        )
        keyformat = (
            ((keyformat_match.group(1) or keyformat_match.group(2) or "").strip())
            if keyformat_match
            else "identity"
        ) or "identity"

        uri_match = re.search(
            r'(?:^|,)\s*URI=(?:"([^"]*)"|([^,]*))',
            attributes,
            re.IGNORECASE,
        )
        key_uri = (
            ((uri_match.group(1) or uri_match.group(2) or "").strip())
            if uri_match
            else ""
        )

        if method.upper() == "AES-128" and keyformat.casefold() == "identity" and key_uri:
            continue

        result["drm_key_required"] = True
        result["drm_detail"] = f"HLS {method} ({keyformat})" if keyformat else f"HLS {method}"
        return result

    return result


def inspect_dash_manifest_drm(manifest_text: str) -> dict:
    """Identify DASH ContentProtection that requires a decryption key."""
    result = {
        "drm_protected": False,
        "drm_key_required": False,
        "drm_detail": "",
    }

    try:
        root = ET.fromstring(manifest_text)
    except ET.ParseError:
        return result

    for element in root.iter():
        if element.tag.rsplit("}", 1)[-1] != "ContentProtection":
            continue

        scheme = str(element.attrib.get("schemeIdUri") or "").strip()
        value = str(element.attrib.get("value") or "").strip()
        detail = value or scheme or "ContentProtection"
        result["drm_protected"] = True
        result["drm_key_required"] = True
        result["drm_detail"] = f"DASH {detail}"
        return result

    return result


def parse_hls_manifest_quality(
    manifest_text: str,
    manifest_url: str = "",
    *,
    motion_cap_fps: float = 50.0,
    expiry_parser: Optional[Callable[[str], Optional[float]]] = None,
) -> Optional[dict]:
    """Return the best advertised HLS variant and its exact child URL.

    The recorder and Coordinator use this same parser so quality ranking and
    FFprobe fallback start from the same selected variant.
    """
    qualities: list[dict] = []
    lines = [raw_line.strip() for raw_line in str(manifest_text or "").splitlines()]

    for index, line in enumerate(lines):
        if not line.startswith("#EXT-X-STREAM-INF:"):
            continue

        attrs = line.split(":", 1)[1]
        resolution = re.search(
            r"(?:^|,)\s*RESOLUTION=(\d+)x(\d+)",
            attrs,
            re.IGNORECASE,
        )
        frame_rate = re.search(
            r"(?:^|,)\s*FRAME-RATE=([0-9.]+)",
            attrs,
            re.IGNORECASE,
        )
        average_bandwidth = re.search(
            r"(?:^|,)\s*AVERAGE-BANDWIDTH=(\d+)",
            attrs,
            re.IGNORECASE,
        )
        bandwidth = re.search(
            r"(?:^|,)\s*BANDWIDTH=(\d+)",
            attrs,
            re.IGNORECASE,
        )
        codecs = re.search(
            r'(?:^|,)\s*CODECS="([^"]+)"',
            attrs,
            re.IGNORECASE,
        )

        width = int(resolution.group(1)) if resolution else 0
        height = int(resolution.group(2)) if resolution else 0
        fps = parse_frame_rate(frame_rate.group(1)) if frame_rate else 0.0
        advertised_bitrate = int(bandwidth.group(1)) if bandwidth else 0
        average_bitrate = int(average_bandwidth.group(1)) if average_bandwidth else 0
        bitrate = average_bitrate or advertised_bitrate

        variant_uri = ""
        for following in lines[index + 1:]:
            if not following:
                continue
            if following.startswith("#"):
                break
            variant_uri = following
            break

        variant_url = urljoin(manifest_url, variant_uri) if variant_uri else ""
        variant_expiry = (
            (
                expiry_parser(variant_url)
                if expiry_parser is not None
                else extract_auth_expiry(variant_url)
            )
            if variant_url
            else None
        )

        qualities.append({
            "quality_known": bool(
                fps > 0 or (width > 0 and height > 0) or bitrate > 0
            ),
            "video_fps": fps,
            "video_width": width,
            "video_height": height,
            "video_scan_type": "",
            "video_scan_type_source": "",
            "video_bitrate_bps": bitrate,
            "manifest_expiry": variant_expiry,
            "manifest_variant_url": variant_url,
            "_hls_bandwidth_bps": advertised_bitrate,
            "_hls_average_bandwidth_bps": average_bitrate,
            "_hls_codecs": str(codecs.group(1) if codecs else "").strip(),
        })

    if not qualities:
        return None

    return max(
        qualities,
        key=lambda item: video_quality_rank(
            item,
            motion_cap_fps=float(motion_cap_fps),
        ),
    )


def _dash_template_substitute(
    template: str,
    *,
    representation_id: str,
    bandwidth: int,
    number: Optional[int] = None,
    time_value: Optional[int] = None,
) -> str:
    """Resolve the standard DASH SegmentTemplate identifiers we need here."""
    value = str(template or "")
    if not value:
        return ""

    escaped_dollar = "\x00DASH_DOLLAR\x00"
    value = value.replace(chr(36) * 2, escaped_dollar)

    values = {
        "RepresentationID": str(representation_id or ""),
        "Bandwidth": int(bandwidth or 0),
        "Number": number,
        "Time": time_value,
    }

    unresolved = False

    def replace_token(match):
        nonlocal unresolved
        name = match.group(1)
        width_text = match.group(3)
        token_value = values.get(name)

        if token_value is None or (name == "RepresentationID" and not token_value):
            unresolved = True
            return match.group(0)

        if name == "RepresentationID":
            return str(token_value)

        numeric_value = int(token_value)
        if width_text:
            return f"{numeric_value:0{int(width_text)}d}"
        return str(numeric_value)

    value = re.sub(
        r"\$(RepresentationID|Bandwidth|Number|Time)(%0(\d+)d)?\$",
        replace_token,
        value,
    )
    value = value.replace(escaped_dollar, "$")

    if unresolved or re.search(r"\$[^$]+\$", value):
        return ""

    return value


def _parse_dash_iso8601_datetime_timestamp(value: str) -> Optional[float]:
    """Parse an MPD UTC timestamp without adding a third-party dependency."""
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return float(parsed.timestamp())
    except (TypeError, ValueError, OverflowError):
        return None


def _parse_dash_iso8601_duration_seconds(value: str) -> float:
    """Parse the ISO-8601 duration subset used by DASH MPD timing fields."""
    text = str(value or "").strip().upper()
    if not text:
        return 0.0
    match = re.fullmatch(
        r"P(?:(?P<days>[0-9]+(?:\.[0-9]+)?)D)?"
        r"(?:T(?:(?P<hours>[0-9]+(?:\.[0-9]+)?)H)?"
        r"(?:(?P<minutes>[0-9]+(?:\.[0-9]+)?)M)?"
        r"(?:(?P<seconds>[0-9]+(?:\.[0-9]+)?)S)?)?",
        text,
    )
    if not match:
        return 0.0
    values = {
        name: float(match.group(name) or 0.0)
        for name in ("days", "hours", "minutes", "seconds")
    }
    return (
        values["days"] * 86400.0
        + values["hours"] * 3600.0
        + values["minutes"] * 60.0
        + values["seconds"]
    )


def parse_dash_manifest_quality(
    manifest_text: str,
    manifest_url: str = "",
    *,
    motion_cap_fps: float = 50.0,
    now_ts: Optional[float] = None,
) -> Optional[dict]:
    """Parse DASH quality/addressing using the mature recorder rules."""
    root = ET.fromstring(manifest_text)
    qualities = []
    current_ts = time.time() if now_ts is None else float(now_ts)

    mpd_is_dynamic = str(root.attrib.get("type") or "").strip().casefold() == "dynamic"
    mpd_availability_start_ts = _parse_dash_iso8601_datetime_timestamp(
        root.attrib.get("availabilityStartTime") or ""
    )
    mpd_publish_ts = _parse_dash_iso8601_datetime_timestamp(
        root.attrib.get("publishTime") or ""
    )
    mpd_suggested_delay_sec = _parse_dash_iso8601_duration_seconds(
        root.attrib.get("suggestedPresentationDelay") or ""
    )

    def local_name(tag: str) -> str:
        return tag.rsplit("}", 1)[-1]

    def element_context_expiry(
        element,
        *,
        excluded_child_names=(),
    ) -> Optional[int]:
        values = [str(value) for value in element.attrib.values()]

        if element.text:
            values.append(str(element.text))

        excluded = {str(name) for name in excluded_child_names}

        for child in element:
            if local_name(child.tag) in excluded:
                continue
            values.append(ET.tostring(child, encoding="unicode"))

        expiries = []
        for value in values:
            expiries.extend(extract_auth_expiries(value))
        return min(expiries) if expiries else None

    parent_map = {
        child: parent
        for parent in root.iter()
        for child in parent
    }

    def element_chain(element):
        chain = []
        current = element
        while current is not None:
            chain.append(current)
            current = parent_map.get(current)
        return list(reversed(chain))

    def first_child(element, name: str):
        for child in element:
            if local_name(child.tag) == name:
                return child
        return None

    def resolve_base_urls(element) -> List[str]:
        # DASH BaseURL is hierarchical and each level may expose alternatives.
        # Preserve every valid path in document order instead of discarding all
        # but the first BaseURL at each level.
        base_urls = [str(manifest_url or "").strip()]

        for node in element_chain(element):
            node_base_texts = []

            for child in node:
                if local_name(child.tag) != "BaseURL":
                    continue
                base_text = str(child.text or "").strip()
                if base_text and base_text not in node_base_texts:
                    node_base_texts.append(base_text)

            if not node_base_texts:
                continue

            resolved_urls = []
            for current_base in base_urls:
                for base_text in node_base_texts:
                    resolved = urljoin(current_base, base_text)
                    if resolved and resolved not in resolved_urls:
                        resolved_urls.append(resolved)

            if resolved_urls:
                base_urls = resolved_urls

        return base_urls

    def inherited_segment_template(element):
        attributes = {}
        timeline = None
        for node in element_chain(element):
            template = first_child(node, "SegmentTemplate")
            if template is None:
                continue
            attributes.update(template.attrib)
            template_timeline = first_child(template, "SegmentTimeline")
            if template_timeline is not None:
                timeline = template_timeline
        return attributes, timeline

    def inherited_segment_list(element):
        selected = None
        for node in element_chain(element):
            segment_list = first_child(node, "SegmentList")
            if segment_list is not None:
                selected = segment_list
        return selected

    def inherited_segment_base(element):
        selected = None
        for node in element_chain(element):
            segment_base = first_child(node, "SegmentBase")
            if segment_base is not None:
                selected = segment_base
        return selected

    def period_start_seconds(element) -> float:
        for node in reversed(element_chain(element)):
            if local_name(node.tag) == "Period":
                return _parse_dash_iso8601_duration_seconds(
                    node.attrib.get("start") or ""
                )
        return 0.0

    def live_presentation_time_units(
        element,
        *,
        timescale: int,
        presentation_time_offset: int,
    ) -> Optional[int]:
        if not mpd_is_dynamic or mpd_availability_start_ts is None:
            return None
        reference_ts = float(mpd_publish_ts or current_ts)
        if mpd_suggested_delay_sec > 0:
            reference_ts -= float(mpd_suggested_delay_sec)
        elapsed_sec = (
            reference_ts
            - float(mpd_availability_start_ts)
            - float(period_start_seconds(element))
        )
        if elapsed_sec <= 0:
            return int(presentation_time_offset)
        return int(
            int(presentation_time_offset)
            + (elapsed_sec * max(1, int(timescale)))
        )

    def recent_timeline_times(
        timeline,
        representation,
        *,
        timescale: int,
        presentation_time_offset: int,
        limit: int = 3,
    ):
        if timeline is None:
            return []

        entries = [
            entry
            for entry in timeline
            if local_name(entry.tag) == "S"
        ]
        if not entries:
            return []

        recent = []
        current_time = None
        live_units = live_presentation_time_units(
            representation,
            timescale=timescale,
            presentation_time_offset=presentation_time_offset,
        )

        for entry_index, entry in enumerate(entries):
            duration = int(entry.attrib.get("d") or 0)
            if duration <= 0:
                continue
            if entry.attrib.get("t") is not None:
                current_time = int(entry.attrib.get("t") or 0)
            elif current_time is None:
                current_time = int(presentation_time_offset or 0)

            repeat = int(entry.attrib.get("r") or 0)
            if repeat >= 0:
                count = repeat + 1
            else:
                next_time = None
                for next_entry in entries[entry_index + 1:]:
                    if next_entry.attrib.get("t") is not None:
                        next_time = int(next_entry.attrib.get("t") or 0)
                        break
                if next_time is not None and next_time > current_time:
                    count = max(1, (next_time - current_time + duration - 1) // duration)
                elif live_units is not None and live_units > current_time:
                    count = max(1, (live_units - current_time) // duration)
                else:
                    continue

            keep = max(limit + 2, 5)
            first_recent_index = max(0, int(count) - keep)
            for repeat_index in range(first_recent_index, int(count)):
                recent.append(int(current_time + (repeat_index * duration)))
                if len(recent) > keep:
                    recent = recent[-keep:]

            current_time += int(count) * duration

        if not recent:
            return []

        ordered = []
        for index in (-2, -3, -1, -4, -5):
            if abs(index) <= len(recent):
                value = recent[index]
                if value not in ordered:
                    ordered.append(value)
            if len(ordered) >= limit:
                break
        return ordered

    def resolve_addressing(representation, representation_id: str, bandwidth: int):
        def build_route(base_url: str) -> dict:
            init_url = ""
            init_range = ""
            media_urls = []
            media_ranges = []
            media_self_contained = False

            template_attrs, timeline = inherited_segment_template(representation)
            if template_attrs:
                initialization = _dash_template_substitute(
                    template_attrs.get("initialization") or "",
                    representation_id=representation_id,
                    bandwidth=bandwidth,
                )
                if initialization:
                    init_url = urljoin(base_url, initialization)

                media_template = str(template_attrs.get("media") or "")
                start_number = int(template_attrs.get("startNumber") or 1)
                timescale = max(1, int(template_attrs.get("timescale") or 1))
                presentation_time_offset = int(
                    template_attrs.get("presentationTimeOffset") or 0
                )
                duration = int(template_attrs.get("duration") or 0)
                if media_template:
                    if "$Time" in media_template:
                        time_values = recent_timeline_times(
                            timeline,
                            representation,
                            timescale=timescale,
                            presentation_time_offset=presentation_time_offset,
                            limit=3,
                        )
                        if not time_values and duration > 0:
                            live_units = live_presentation_time_units(
                                representation,
                                timescale=timescale,
                                presentation_time_offset=presentation_time_offset,
                            )
                            if live_units is not None:
                                live_index = max(
                                    0,
                                    int((live_units - presentation_time_offset) // duration),
                                )
                                for offset in (1, 2, 0):
                                    index = max(0, live_index - offset)
                                    value = presentation_time_offset + (index * duration)
                                    if value not in time_values:
                                        time_values.append(value)
                            elif not mpd_is_dynamic:
                                time_values = [
                                    presentation_time_offset,
                                    presentation_time_offset + duration,
                                    presentation_time_offset + (duration * 2),
                                ]
                        for time_value in time_values:
                            media = _dash_template_substitute(
                                media_template,
                                representation_id=representation_id,
                                bandwidth=bandwidth,
                                number=start_number,
                                time_value=time_value,
                            )
                            if media:
                                media_urls.append(urljoin(base_url, media))
                    else:
                        numbers = []
                        if duration > 0:
                            live_units = live_presentation_time_units(
                                representation,
                                timescale=timescale,
                                presentation_time_offset=presentation_time_offset,
                            )
                            if live_units is not None:
                                live_index = max(
                                    0,
                                    int((live_units - presentation_time_offset) // duration),
                                )
                                current_number = start_number + live_index
                                for offset in (1, 2, 0):
                                    value = max(start_number, current_number - offset)
                                    if value not in numbers:
                                        numbers.append(value)
                        if not numbers and not mpd_is_dynamic:
                            numbers = list(range(start_number, start_number + 3))

                        for number in numbers:
                            media = _dash_template_substitute(
                                media_template,
                                representation_id=representation_id,
                                bandwidth=bandwidth,
                                number=number,
                                time_value=None,
                            )
                            if media:
                                media_urls.append(urljoin(base_url, media))

            if not init_url and not media_urls:
                segment_list = inherited_segment_list(representation)
                if segment_list is not None:
                    initialization = first_child(segment_list, "Initialization")
                    if initialization is not None:
                        source_url = str(initialization.attrib.get("sourceURL") or "").strip()
                        init_url = urljoin(base_url, source_url) if source_url else base_url
                        init_range = str(initialization.attrib.get("range") or "").strip()

                    segment_pairs = []
                    for segment_url in segment_list:
                        if local_name(segment_url.tag) != "SegmentURL":
                            continue
                        media = str(segment_url.attrib.get("media") or "").strip()
                        if not media:
                            continue
                        segment_pairs.append((
                            urljoin(base_url, media),
                            str(segment_url.attrib.get("mediaRange") or "").strip(),
                        ))

                    if segment_pairs:
                        if mpd_is_dynamic:
                            recent_pairs = []
                            for index in (-2, -3, -1):
                                if abs(index) <= len(segment_pairs):
                                    pair = segment_pairs[index]
                                    if pair not in recent_pairs:
                                        recent_pairs.append(pair)
                            segment_pairs = recent_pairs
                        else:
                            segment_pairs = segment_pairs[:3]

                    for media_url, media_range in segment_pairs[:3]:
                        media_urls.append(media_url)
                        media_ranges.append(media_range)

            if not init_url and not media_urls:
                segment_base = inherited_segment_base(representation)
                if segment_base is not None and base_url:
                    initialization = first_child(segment_base, "Initialization")
                    if initialization is not None:
                        source_url = str(initialization.attrib.get("sourceURL") or "").strip()
                        init_url = urljoin(base_url, source_url) if source_url else base_url
                        init_range = str(initialization.attrib.get("range") or "").strip()
                    media_urls = [base_url]
                    media_self_contained = True

            if (
                not init_url
                and not media_urls
                and base_url
                and base_url != str(manifest_url or "").strip()
            ):
                media_urls = [base_url]
                media_self_contained = True

            return {
                "base_url": base_url,
                "initialization_url": init_url,
                "initialization_range": init_range,
                "media_urls": media_urls,
                "media_ranges": media_ranges,
                "media_self_contained": media_self_contained,
            }

        routes = [
            build_route(base_url)
            for base_url in resolve_base_urls(representation)
            if str(base_url or "").strip()
        ]

        if not routes:
            routes = [build_route(str(manifest_url or "").strip())]

        primary = routes[0]

        return {
            "_dash_representation_id": representation_id,
            "_dash_representation_bandwidth": int(bandwidth or 0),
            "_dash_representation_base_url": primary["base_url"],
            "_dash_representation_base_urls": [
                route["base_url"]
                for route in routes
            ],
            "_dash_initialization_url": primary["initialization_url"],
            "_dash_initialization_range": primary["initialization_range"],
            "_dash_media_urls": list(primary["media_urls"]),
            "_dash_media_ranges": list(primary["media_ranges"]),
            "_dash_media_self_contained": bool(
                primary["media_self_contained"]
            ),
            "_dash_resource_routes": routes,
        }

    for adaptation in root.iter():
        if local_name(adaptation.tag) != "AdaptationSet":
            continue

        adaptation_content_type = str(adaptation.attrib.get("contentType") or "").lower()
        adaptation_mime_type = str(adaptation.attrib.get("mimeType") or "").lower()
        adaptation_frame_rate = adaptation.attrib.get("frameRate")
        adaptation_width = int(adaptation.attrib.get("width") or 0)
        adaptation_height = int(adaptation.attrib.get("height") or 0)
        adaptation_codecs = str(adaptation.attrib.get("codecs") or "").strip()
        adaptation_scan_type = normalize_video_scan_type(
            adaptation.attrib.get("scanType")
        )

        inherited_expiry = None
        ancestor = parent_map.get(adaptation)
        while ancestor is not None:
            ancestor_expiry = element_context_expiry(
                ancestor,
                excluded_child_names=("Period", "AdaptationSet", "Representation"),
            )
            inherited_expiry = merge_auth_expiries(
                inherited_expiry,
                ancestor_expiry,
            )
            ancestor = parent_map.get(ancestor)

        adaptation_expiry = element_context_expiry(
            adaptation,
            excluded_child_names=("Representation",),
        )

        for representation in adaptation:
            if local_name(representation.tag) != "Representation":
                continue

            representation_mime_type = str(
                representation.attrib.get("mimeType")
                or adaptation_mime_type
                or ""
            ).lower()
            width = int(representation.attrib.get("width") or adaptation_width or 0)
            height = int(representation.attrib.get("height") or adaptation_height or 0)
            fps = parse_frame_rate(
                representation.attrib.get("frameRate") or adaptation_frame_rate
            )
            bitrate = int(representation.attrib.get("bandwidth") or 0)
            representation_id = str(representation.attrib.get("id") or "").strip()
            codecs = str(
                representation.attrib.get("codecs")
                or adaptation_codecs
                or ""
            ).strip()
            video_scan_type = (
                normalize_video_scan_type(representation.attrib.get("scanType"))
                or adaptation_scan_type
            )

            is_video = (
                adaptation_content_type == "video"
                or "video" in adaptation_mime_type
                or "video" in representation_mime_type
                or (width > 0 and height > 0)
                or fps > 0
            )
            if not is_video:
                continue

            representation_expiry = element_context_expiry(representation)
            quality = {
                "quality_known": bool(
                    fps > 0 or (width > 0 and height > 0) or bitrate > 0
                ),
                "video_fps": fps,
                "video_width": width,
                "video_height": height,
                "video_scan_type": video_scan_type,
                "video_scan_type_source": "manifest" if video_scan_type else "",
                "video_bitrate_bps": bitrate,
                "manifest_expiry": merge_auth_expiries(
                    inherited_expiry,
                    adaptation_expiry,
                    representation_expiry,
                ),
                "_dash_codecs": codecs,
            }
            quality.update(
                resolve_addressing(representation, representation_id, bitrate)
            )
            qualities.append(quality)

    if not qualities:
        return None

    return max(
        qualities,
        key=lambda item: video_quality_rank(
            item,
            motion_cap_fps=float(motion_cap_fps),
        ),
    )


def build_ffprobe_quality_command(
    stream_url: str,
    headers: Mapping[str, str],
    *,
    decryption_key: str = "",
) -> list[str]:
    command = ["ffprobe", "-v", "error"]
    if headers:
        header_blob = "".join(
            f"{name}: {value}\r\n"
            for name, value in headers.items()
        )
        command.extend(["-headers", header_blob])
    if decryption_key:
        command.extend(["-decryption_key", decryption_key])
    command.extend([
        "-select_streams", "v",
        "-show_entries",
        (
            "stream=index,codec_name,width,height,avg_frame_rate,r_frame_rate,"
            "bit_rate,field_order:format=bit_rate"
        ),
        "-of", "json",
        stream_url,
    ])
    return command


def parse_ffprobe_quality_output(
    stdout: str,
    *,
    target_quality: Optional[Mapping[str, object]] = None,
    motion_cap_fps: float = 50.0,
) -> Optional[dict]:
    data = json.loads(str(stdout or ""))
    format_bitrate = int((data.get("format") or {}).get("bit_rate") or 0)
    qualities: list[dict] = []

    for stream in data.get("streams") or []:
        width = int(stream.get("width") or 0)
        height = int(stream.get("height") or 0)
        fps = parse_frame_rate(stream.get("avg_frame_rate"))
        if fps <= 0:
            fps = parse_frame_rate(stream.get("r_frame_rate"))

        stream_bitrate = int(stream.get("bit_rate") or 0)
        bitrate = stream_bitrate or format_bitrate
        bitrate_source = (
            "stream" if stream_bitrate > 0
            else "format" if format_bitrate > 0
            else ""
        )

        field_order = str(stream.get("field_order") or "").strip().casefold()
        if field_order == "progressive":
            scan_type = "progressive"
        elif field_order in {"tt", "bb", "tb", "bt"}:
            scan_type = "interlaced"
        else:
            scan_type = ""

        qualities.append({
            "quality_known": bool(
                fps > 0 or (width > 0 and height > 0) or bitrate > 0
            ),
            "video_fps": fps,
            "video_width": width,
            "video_height": height,
            "video_scan_type": scan_type,
            "video_scan_type_source": "ffprobe" if scan_type else "",
            "video_bitrate_bps": bitrate,
            "video_bitrate_source": bitrate_source,
            "_ffprobe_stream_index": int(stream.get("index") or 0),
            "_ffprobe_codec_name": str(stream.get("codec_name") or "").strip(),
        })

    if not qualities:
        return None

    target = target_quality or {}
    target_width = int(target.get("video_width") or 0)
    target_height = int(target.get("video_height") or 0)
    target_fps = float(target.get("video_fps") or 0.0)

    matched = []
    for item in qualities:
        if target_width > 0 and target_height > 0:
            if (
                int(item.get("video_width") or 0) != target_width
                or int(item.get("video_height") or 0) != target_height
            ):
                continue
        if target_fps > 0:
            item_fps = float(item.get("video_fps") or 0.0)
            if item_fps <= 0 or abs(item_fps - target_fps) > 0.05:
                continue
        matched.append(item)

    best = max(
        matched or qualities,
        key=lambda item: video_quality_rank(
            item,
            motion_cap_fps=float(motion_cap_fps),
        ),
    )
    best["_ffprobe_target_match_count"] = (
        len(matched) if target_quality else len(qualities)
    )
    return best


def probe_stream_quality_ffprobe(
    stream_url: str,
    headers: Mapping[str, str],
    *,
    timeout_sec: float = QUALITY_FFPROBE_TIMEOUT_SEC,
    target_quality: Optional[Mapping[str, object]] = None,
    motion_cap_fps: float = 50.0,
    decryption_key: str = "",
    sample_missing_bitrate: bool = False,
    bitrate_sample_sec: float = QUALITY_BITRATE_SAMPLE_SEC,
    bitrate_sample_timeout_sec: float = QUALITY_BITRATE_SAMPLE_TIMEOUT_SEC,
    bitrate_sample_callback: Optional[Callable[[int], int]] = None,
    failure_describer: Optional[Callable[[object], str]] = None,
    runner: Optional[Callable[[Sequence[str], float], object]] = None,
) -> Optional[dict]:
    """Probe video quality and complete a missing bitrate through one shared rule."""
    command = build_ffprobe_quality_command(
        stream_url,
        headers,
        decryption_key=decryption_key,
    )

    if runner is None:
        def default_runner(args: Sequence[str], timeout: float):
            kwargs = {
                "capture_output": True,
                "text": True,
                "timeout": timeout,
                "check": False,
            }
            if os.name == "nt":
                kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
            return subprocess.run(list(args), **kwargs)
        runner = default_runner

    result = runner(command, float(timeout_sec))
    stdout = str(getattr(result, "stdout", "") or "").strip()
    returncode = int(getattr(result, "returncode", 0) or 0)
    # Preserve mature behavior: usable video facts in stdout remain usable even
    # if ffprobe exits non-zero after emitting them.
    if not stdout:
        detail = (
            str(failure_describer(result) or "").strip()
            if failure_describer is not None
            else str(getattr(result, "stderr", "") or "").strip()
        )
        if not detail:
            detail = (
                f"ffprobe exited with code {returncode}"
                if returncode
                else "ffprobe returned no video stream information"
            )
        raise RuntimeError(detail)

    try:
        best_quality = parse_ffprobe_quality_output(
            stdout,
            target_quality=target_quality,
            motion_cap_fps=motion_cap_fps,
        )
    except Exception as error:
        raise RuntimeError(
            f"ffprobe quality output could not be parsed ({type(error).__name__}: {error})"
        ) from error

    if not best_quality:
        detail = (
            str(failure_describer(result) or "").strip()
            if failure_describer is not None
            else ""
        )
        raise RuntimeError(detail or "ffprobe returned no video stream information")

    if (
        sample_missing_bitrate
        and int(best_quality.get("video_bitrate_bps") or 0) <= 0
    ):
        stream_index = int(best_quality.get("_ffprobe_stream_index") or 0)
        try:
            if bitrate_sample_callback is not None:
                sampled_bitrate = int(bitrate_sample_callback(stream_index) or 0)
            else:
                sampled_bitrate = sample_stream_video_bitrate(
                    stream_url,
                    headers,
                    sample_sec=bitrate_sample_sec,
                    timeout_sec=bitrate_sample_timeout_sec,
                    decryption_key=decryption_key,
                    stream_index=stream_index,
                )
        except Exception as error:
            best_quality["_bitrate_sample_failure"] = (
                f"{type(error).__name__}: {error}"
            )
        else:
            if sampled_bitrate > 0:
                best_quality["video_bitrate_bps"] = sampled_bitrate
                best_quality["video_bitrate_source"] = "sample"
                best_quality["quality_known"] = True

    return best_quality


def build_ffmpeg_bitrate_sample_command(
    stream_url: str,
    headers: Mapping[str, str],
    *,
    sample_sec: float = 4.0,
    byte_range: str = "",
    decryption_key: str = "",
    stream_index: Optional[int] = None,
) -> list[str]:
    command = ["ffmpeg", "-v", "error", "-nostdin"]
    effective_headers = dict(headers or {})
    if byte_range:
        effective_headers["Range"] = f"bytes={byte_range}"
    if effective_headers:
        header_blob = "".join(
            f"{name}: {value}\r\n"
            for name, value in effective_headers.items()
        )
        command.extend(["-headers", header_blob])
    if decryption_key:
        command.extend(["-decryption_key", decryption_key])

    map_value = (
        f"0:{int(stream_index)}"
        if stream_index is not None and int(stream_index) >= 0
        else "0:v:0"
    )
    command.extend([
        "-i", stream_url,
        "-map", map_value,
        "-c:v", "copy",
        "-an",
        "-sn",
        "-dn",
        "-t", str(float(sample_sec)),
        "-progress", "pipe:2",
        "-nostats",
        "-f", "mpegts",
        "pipe:1",
    ])
    return command


def parse_ffmpeg_bitrate_progress(stderr_text: str) -> int:
    total_size = 0
    out_time_us = 0
    for line in str(stderr_text or "").splitlines():
        key, separator, value = line.partition("=")
        if not separator:
            continue
        try:
            if key == "total_size":
                total_size = max(total_size, int(value))
            elif key == "out_time_us":
                out_time_us = max(out_time_us, int(value))
        except (TypeError, ValueError):
            continue

    if total_size <= 0 or out_time_us <= 0:
        return 0
    sampled_bitrate = int(
        (float(total_size) * 8.0 * 1_000_000.0)
        / float(out_time_us)
    )
    return sampled_bitrate if sampled_bitrate > 0 else 0


def sample_stream_video_bitrate(
    stream_url: str,
    headers: Mapping[str, str],
    *,
    sample_sec: float = 4.0,
    timeout_sec: float = 12.0,
    byte_range: str = "",
    decryption_key: str = "",
    stream_index: Optional[int] = None,
    runner: Optional[Callable[[Sequence[str], float], object]] = None,
) -> int:
    command = build_ffmpeg_bitrate_sample_command(
        stream_url,
        headers,
        sample_sec=sample_sec,
        byte_range=byte_range,
        decryption_key=decryption_key,
        stream_index=stream_index,
    )
    if runner is None:
        def default_runner(args: Sequence[str], timeout: float):
            kwargs = {
                "stdout": subprocess.DEVNULL,
                "stderr": subprocess.PIPE,
                "text": True,
                "timeout": timeout,
                "check": False,
            }
            if os.name == "nt":
                kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
            return subprocess.run(list(args), **kwargs)
        runner = default_runner

    result = runner(command, float(timeout_sec))
    if int(getattr(result, "returncode", 0) or 0) != 0:
        return 0
    return parse_ffmpeg_bitrate_progress(
        str(getattr(result, "stderr", "") or "")
    )
