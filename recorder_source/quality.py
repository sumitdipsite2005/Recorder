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
from typing import Callable, Mapping, Optional, Sequence
from urllib.parse import urljoin

from .selection import video_quality_rank


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
            expiry_parser(variant_url)
            if expiry_parser is not None and variant_url
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
    timeout_sec: float = 20.0,
    target_quality: Optional[Mapping[str, object]] = None,
    motion_cap_fps: float = 50.0,
    decryption_key: str = "",
    runner: Optional[Callable[[Sequence[str], float], object]] = None,
) -> Optional[dict]:
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
    if returncode != 0 or not stdout:
        stderr = str(getattr(result, "stderr", "") or "").strip()
        detail = stderr or (
            f"ffprobe exited with code {returncode}"
            if returncode
            else "ffprobe returned no video stream information"
        )
        raise RuntimeError(detail)

    return parse_ffprobe_quality_output(
        stdout,
        target_quality=target_quality,
        motion_cap_fps=motion_cap_fps,
    )


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
