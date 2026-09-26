"""Shared passive source-probe transport policy.

HTTP retry/error mechanics used by both mature ONE BEST inspection and
Identity Coordinator Inspect/Watch live here. Recording orchestration,
alarms, scan cadence, and downloader retries remain with their runtime owner.
"""

from __future__ import annotations

import os
import re
import subprocess
import time
from datetime import timezone
from email.utils import parsedate_to_datetime
from http.cookiejar import CookieJar
from typing import Callable, Mapping, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, build_opener, HTTPCookieProcessor, urlopen

from .policy import is_vpn_route_suspected_403


QUALITY_HTTP_TIMEOUT_SEC = 8.0
QUALITY_HTTP_MAX_ATTEMPTS = 2
QUALITY_HTTP_RETRY_BASE_SEC = 0.5
QUALITY_HTTP_RETRY_MAX_SEC = 2.0
QUALITY_HTTP_RETRYABLE_STATUS_CODES = (408, 425, 429, 500, 502, 503, 504)


def is_timeout_exception(error: BaseException) -> bool:
    if isinstance(error, (subprocess.TimeoutExpired, TimeoutError)):
        return True
    if isinstance(error, URLError):
        reason = getattr(error, "reason", None)
        return isinstance(reason, (subprocess.TimeoutExpired, TimeoutError))
    return False


def is_retryable_http_get_error(error: BaseException) -> bool:
    if isinstance(error, HTTPError):
        try:
            return int(getattr(error, "code", 0) or 0) in QUALITY_HTTP_RETRYABLE_STATUS_CODES
        except Exception:
            return False
    if is_timeout_exception(error):
        return True
    if isinstance(error, URLError):
        reason = getattr(error, "reason", None)
        return isinstance(reason, (OSError, ConnectionError))
    return isinstance(error, subprocess.TimeoutExpired)


def http_retry_delay_sec(error: BaseException, retry_number: int) -> float:
    delay = min(
        QUALITY_HTTP_RETRY_MAX_SEC,
        QUALITY_HTTP_RETRY_BASE_SEC * (2 ** max(0, int(retry_number) - 1)),
    )
    if isinstance(error, HTTPError):
        headers = getattr(error, "headers", None)
        retry_after = (
            str(headers.get("Retry-After") or "").strip()
            if headers is not None
            else ""
        )
        if retry_after:
            parsed_delay = None
            try:
                parsed_delay = max(0.0, float(retry_after))
            except Exception:
                try:
                    retry_at = parsedate_to_datetime(retry_after)
                    if retry_at.tzinfo is None:
                        retry_at = retry_at.replace(tzinfo=timezone.utc)
                    parsed_delay = max(0.0, retry_at.timestamp() - time.time())
                except Exception:
                    parsed_delay = None
            if parsed_delay is not None:
                delay = min(QUALITY_HTTP_RETRY_MAX_SEC, float(parsed_delay))
    return max(0.0, float(delay))


def run_retryable_http_get(
    operation: Callable[[], object],
    *,
    stop_requested: Optional[Callable[[], bool]] = None,
    cancel_message: str = "Quality probe cancelled by stop request",
    sleep_fn: Callable[[float], None] = time.sleep,
):
    """Run one idempotent GET with the mature recorder's bounded transient retry."""
    max_attempts = max(1, int(QUALITY_HTTP_MAX_ATTEMPTS))
    for attempt in range(1, max_attempts + 1):
        if stop_requested is not None and stop_requested():
            raise RuntimeError(cancel_message)
        try:
            return operation()
        except Exception as error:
            if attempt >= max_attempts or not is_retryable_http_get_error(error):
                raise
            try:
                close_fn = getattr(error, "close", None)
                if callable(close_fn):
                    close_fn()
            except Exception:
                pass
            delay = http_retry_delay_sec(error, retry_number=attempt)
            deadline = time.monotonic() + delay
            while time.monotonic() < deadline:
                if stop_requested is not None and stop_requested():
                    raise RuntimeError(cancel_message)
                sleep_fn(min(0.1, max(0.0, deadline - time.monotonic())))
    raise RuntimeError("HTTP retry loop ended unexpectedly")


def ascii_safe_request_headers(headers: Optional[Mapping[str, object]] = None) -> dict:
    safe = {
        str(name): str(value)
        for name, value in (headers or {}).items()
        if str(name).strip()
    }
    for header_name in list(safe):
        if header_name.casefold() != "user-agent":
            continue
        original = str(safe[header_name]).strip()
        ascii_value = original.encode("ascii", errors="ignore").decode("ascii").strip()
        if ascii_value:
            safe[header_name] = ascii_value
        else:
            del safe[header_name]
    return safe



def is_drmlive_host(url: str) -> bool:
    try:
        host = str(urlsplit(str(url or "")).hostname or "").casefold()
    except Exception:
        return False
    return host == "drmlive.net" or host.endswith(".drmlive.net")


def stream_type_from_url(url: str) -> str:
    try:
        path = str(urlsplit(str(url or "")).path or "").casefold()
    except Exception:
        path = str(url or "").casefold()
    if path.endswith(".mpd"):
        return "DASH"
    if path.endswith(".m3u8"):
        return "HLS"
    return ""


def _curl_get_text(
    url: str,
    user_agent: str,
    *,
    headers: Optional[Mapping[str, str]] = None,
    timeout_sec: float,
    runner: Callable[..., object] = subprocess.run,
    timeout_callback: Optional[Callable[[BaseException, float, str], None]] = None,
):
    status_marker = b"\n__RECORDER_CURL_HTTP_STATUS__:"
    final_url_marker = b"\n__RECORDER_CURL_FINAL_URL__:"
    args = [
        "curl.exe" if os.name == "nt" else "curl",
        "-sS", "-L", "--compressed", "-A", str(user_agent),
    ]
    for name, value in (headers or {}).items():
        header_name = str(name or "").strip()
        if (
            not header_name
            or value is None
            or header_name.casefold() == "user-agent"
        ):
            continue
        args.extend(["-H", f"{header_name}: {str(value)}"])
    args.extend([
        "-w",
        (
            "\n__RECORDER_CURL_HTTP_STATUS__:%{http_code}"
            "\n__RECORDER_CURL_FINAL_URL__:%{url_effective}"
        ),
        str(url),
    ])
    try:
        result = runner(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=float(timeout_sec),
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        if timeout_callback is not None:
            timeout_callback(error, float(timeout_sec), str(url))
        raise
    if int(getattr(result, "returncode", 0) or 0) != 0:
        stderr = bytes(getattr(result, "stderr", b"") or b"").decode(
            "utf-8", errors="replace"
        ).strip()
        raise RuntimeError(
            "curl GET failed" + (f": {stderr}" if stderr else "")
        )
    stdout = bytes(getattr(result, "stdout", b"") or b"")
    body, separator, trailer = stdout.rpartition(status_marker)
    if not separator:
        raise RuntimeError("curl GET returned no HTTP status marker")
    status_bytes, separator, final_url_bytes = trailer.partition(final_url_marker)
    if not separator:
        raise RuntimeError("curl GET returned no final-URL marker")
    try:
        status = int(status_bytes.strip())
    except Exception as error:
        raise RuntimeError("curl GET returned an invalid HTTP status") from error
    final_url = final_url_bytes.decode("utf-8", errors="replace").strip()
    if not (200 <= status < 300):
        raise HTTPError(
            final_url or str(url),
            status,
            f"HTTP Error {status}",
            None,
            None,
        )
    return body.decode("utf-8-sig", errors="replace"), final_url


def fetch_stream_manifest_text(
    stream_url: str,
    headers: Mapping[str, str],
    *,
    default_user_agent: str,
    stop_requested: Optional[Callable[[], bool]] = None,
    urlopen_fn: Callable[..., object] = urlopen,
    subprocess_runner: Callable[..., object] = subprocess.run,
    timeout_callback: Optional[Callable[[BaseException, float, str], None]] = None,
):
    """Fetch a candidate manifest with the mature recorder's transport rules."""
    if stop_requested is not None and stop_requested():
        raise RuntimeError("Quality probe cancelled by stop request")

    if is_drmlive_host(stream_url):
        request_headers = ascii_safe_request_headers(headers)
        user_agent = "OTT Navigator/1.7.1.4"
        curl_headers = None
        if stream_type_from_url(stream_url) == "DASH":
            curl_headers = request_headers
            for name, value in request_headers.items():
                if str(name).casefold() == "user-agent" and str(value).strip():
                    user_agent = str(value)
                    break

        return run_retryable_http_get(
            lambda: _curl_get_text(
                stream_url,
                user_agent,
                headers=curl_headers,
                timeout_sec=QUALITY_HTTP_TIMEOUT_SEC + 5.0,
                runner=subprocess_runner,
                timeout_callback=timeout_callback,
            ),
            stop_requested=stop_requested,
        )

    request_headers = ascii_safe_request_headers(headers)
    if not any(
        str(name).casefold() == "user-agent" and str(value).strip()
        for name, value in request_headers.items()
    ):
        request_headers["User-Agent"] = str(default_user_agent)

    def fetch_once():
        request = Request(stream_url, headers=request_headers)
        with urlopen_fn(request, timeout=QUALITY_HTTP_TIMEOUT_SEC) as response:
            read_chunk = getattr(response, "read1", response.read)
            if stop_requested is not None and stop_requested():
                raise RuntimeError("Quality probe cancelled by stop request")
            first_bytes = read_chunk(64 * 1024)
            first_text = first_bytes.decode("utf-8-sig", errors="replace")
            stripped = first_text.lstrip()
            looks_like_manifest = (
                stripped.startswith("#EXTM3U")
                or re.search(
                    r'<(?:[A-Za-z_][\w.-]*:)?MPD\b',
                    stripped,
                    re.IGNORECASE,
                ) is not None
            )
            chunks = [first_bytes]
            if looks_like_manifest:
                while True:
                    if stop_requested is not None and stop_requested():
                        raise RuntimeError("Quality probe cancelled by stop request")
                    chunk = read_chunk(64 * 1024)
                    if not chunk:
                        break
                    chunks.append(chunk)
            text = b"".join(chunks).decode("utf-8-sig", errors="replace")
            final_url = str(response.geturl() or stream_url).strip()
            return text, final_url

    return run_retryable_http_get(
        fetch_once,
        stop_requested=stop_requested,
    )


def dash_resource_routes(quality: Mapping[str, object]) -> list[dict]:
    """Return DASH representation routes, selected route first when known."""
    routes = []
    for original_index, route in enumerate(quality.get("_dash_resource_routes") or []):
        if not isinstance(route, Mapping):
            continue
        normalized = {
            "_route_index": int(original_index),
            "base_url": str(route.get("base_url") or "").strip(),
            "initialization_url": str(route.get("initialization_url") or "").strip(),
            "initialization_range": str(route.get("initialization_range") or "").strip(),
            "media_urls": [
                str(value or "").strip()
                for value in (route.get("media_urls") or [])
                if str(value or "").strip()
            ],
            "media_ranges": list(route.get("media_ranges") or []),
            "media_self_contained": bool(route.get("media_self_contained", False)),
        }
        if normalized not in routes:
            routes.append(normalized)

    if not routes:
        routes.append({
            "_route_index": 0,
            "base_url": str(quality.get("_dash_representation_base_url") or "").strip(),
            "initialization_url": str(quality.get("_dash_initialization_url") or "").strip(),
            "initialization_range": str(quality.get("_dash_initialization_range") or "").strip(),
            "media_urls": [
                str(value or "").strip()
                for value in (quality.get("_dash_media_urls") or [])
                if str(value or "").strip()
            ],
            "media_ranges": list(quality.get("_dash_media_ranges") or []),
            "media_self_contained": bool(
                quality.get("_dash_media_self_contained", False)
            ),
        })

    try:
        selected_index = int(quality.get("_dash_selected_route_index"))
    except Exception:
        selected_index = -1
    selected = next(
        (
            route
            for route in routes
            if int(route.get("_route_index", -1)) == selected_index
        ),
        None,
    )
    if selected is not None:
        routes = [selected] + [route for route in routes if route is not selected]
    return routes


def _single_byte_range(byte_range: str = "") -> str:
    match = re.fullmatch(r"(\d+)\s*-\s*(\d+)?", str(byte_range or "").strip())
    return f"{match.group(1)}-{match.group(1)}" if match else "0-0"


def build_resource_request_headers(
    headers: Mapping[str, str],
    *,
    default_user_agent: str,
    byte_range: str = "",
) -> dict:
    request_headers = ascii_safe_request_headers(headers)
    if not any(
        str(name).casefold() == "user-agent" and str(value).strip()
        for name, value in request_headers.items()
    ):
        request_headers["User-Agent"] = str(default_user_agent)
    if byte_range:
        request_headers["Range"] = f"bytes={byte_range}"
    return request_headers


def resolve_http_resource_final_url(
    resource_url: str,
    headers: Mapping[str, str],
    *,
    default_user_agent: str,
    byte_range: str = "",
    stop_requested: Optional[Callable[[], bool]] = None,
    urlopen_fn: Callable[..., object] = urlopen,
) -> str:
    """Follow one media-resource GET and return its effective URL."""
    def run(headers_for_request):
        def fetch_once():
            request = Request(resource_url, headers=headers_for_request)
            with urlopen_fn(request, timeout=QUALITY_HTTP_TIMEOUT_SEC) as response:
                response.read(1)
                return str(response.geturl() or resource_url).strip()
        return run_retryable_http_get(
            fetch_once,
            stop_requested=stop_requested,
        )

    ranged = build_resource_request_headers(
        headers,
        default_user_agent=default_user_agent,
        byte_range=_single_byte_range(byte_range),
    )
    try:
        return str(run(ranged) or resource_url).strip()
    except HTTPError as error:
        if int(getattr(error, "code", 0) or 0) != 416:
            raise

    normal = build_resource_request_headers(
        headers,
        default_user_agent=default_user_agent,
    )
    return str(run(normal) or resource_url).strip()


def resolve_selected_dash_resource_route(
    quality: Mapping[str, object],
    headers: Mapping[str, str],
    *,
    default_user_agent: str,
    expiry_parser: Callable[[str], Optional[float]],
    stop_requested: Optional[Callable[[], bool]] = None,
    urlopen_fn: Callable[..., object] = urlopen,
    error_describer: Optional[Callable[[BaseException], str]] = None,
) -> dict:
    """Resolve one usable selected-representation resource across alternatives."""
    failures = []
    for route in dash_resource_routes(quality):
        route_index = int(route.get("_route_index", 0) or 0)
        media_urls = list(route.get("media_urls") or [])
        media_ranges = list(route.get("media_ranges") or [])
        candidates = [
            (
                str(media_url or "").strip(),
                str(media_ranges[index] or "").strip()
                if index < len(media_ranges)
                else "",
            )
            for index, media_url in enumerate(media_urls[:3])
        ]
        if not candidates and route.get("initialization_url"):
            candidates.append((
                str(route.get("initialization_url") or "").strip(),
                str(route.get("initialization_range") or "").strip(),
            ))

        for request_url, request_range in candidates:
            if not request_url:
                continue
            try:
                final_url = resolve_http_resource_final_url(
                    request_url,
                    headers,
                    default_user_agent=default_user_agent,
                    byte_range=request_range,
                    stop_requested=stop_requested,
                    urlopen_fn=urlopen_fn,
                )
                known = [
                    value
                    for value in (
                        expiry_parser(request_url),
                        expiry_parser(final_url),
                    )
                    if value is not None
                ]
                return {
                    "route_index": route_index,
                    "request_url": request_url,
                    "final_url": final_url,
                    "resource_expiry": min(known) if known else None,
                    "failure": "",
                }
            except Exception as error:
                if stop_requested is not None and stop_requested():
                    raise
                detail = (
                    str(error_describer(error) or "").strip()
                    if error_describer is not None
                    else f"{type(error).__name__}: {error}"
                )
                if detail:
                    failures.append(detail)

    return {
        "route_index": None,
        "request_url": "",
        "final_url": "",
        "resource_expiry": None,
        "failure": next((value for value in failures if value), ""),
    }

def fetch_hls_child_with_master_cookie_session(
    master_url: str,
    child_url: str,
    headers: Mapping[str, str],
    *,
    timeout_sec: float = QUALITY_HTTP_TIMEOUT_SEC,
    stop_requested: Optional[Callable[[], bool]] = None,
) -> str:
    """Fetch an HLS child after allowing the master response to establish cookies."""
    if stop_requested is not None and stop_requested():
        raise RuntimeError("Quality probe cancelled by stop request")
    request_headers = ascii_safe_request_headers(headers)
    cookie_jar = CookieJar()
    opener = build_opener(HTTPCookieProcessor(cookie_jar))
    master_request = Request(master_url, headers=request_headers)
    with opener.open(master_request, timeout=float(timeout_sec)) as response:
        response.read(1)
    if stop_requested is not None and stop_requested():
        raise RuntimeError("Quality probe cancelled by stop request")
    child_request = Request(child_url, headers=request_headers)
    with opener.open(child_request, timeout=float(timeout_sec)) as response:
        return response.read().decode("utf-8-sig", errors="replace")


def classify_hls_variant_probe_failure(
    error: Optional[BaseException] = None,
    *,
    non_hls_response: bool = False,
) -> dict:
    """Classify a selected HLS child/variant failure without inventing DRM.

    The child is fetched partly so we can inspect encryption declarations, but a
    transport/path failure is not itself DRM evidence. Keep that distinction
    explicit for both the mature recorder and Coordinator.
    """
    if non_hls_response:
        return {
            "status": "hls_variant_invalid",
            "classification": "HLS VARIANT INVALID",
            "reason": "selected HLS variant returned a non-HLS response",
            "http_status": None,
        }

    if isinstance(error, HTTPError):
        status = int(getattr(error, "code", 0) or 0)
        reason = str(getattr(error, "reason", "") or "").strip()
        http_text = (
            f"HTTP {status}" + (f" {reason}" if reason else "")
            if status
            else "HTTP request failed"
        )
        if status in (404, 410):
            return {
                "status": "hls_variant_unavailable",
                "classification": "HLS VARIANT UNAVAILABLE",
                "reason": f"{http_text} — selected HLS variant/path unavailable",
                "http_status": status,
            }
        if status in (401, 403):
            return {
                "status": "hls_variant_access_failed",
                "classification": "HLS VARIANT ACCESS FAILED",
                "reason": f"{http_text} — selected HLS variant access failed",
                "http_status": status,
            }
        return {
            "status": "hls_variant_check_failed",
            "classification": "HLS VARIANT CHECK FAILED",
            "reason": f"{http_text} — selected HLS variant check failed",
            "http_status": status or None,
        }

    if error is not None and is_timeout_exception(error):
        return {
            "status": "hls_variant_check_failed",
            "classification": "HLS VARIANT CHECK FAILED",
            "reason": "connection timed out while checking selected HLS variant",
            "http_status": None,
        }

    detail = ""
    if error is not None:
        detail = str(error).strip()
        if detail:
            detail = f"{type(error).__name__}: {detail}"
        else:
            detail = type(error).__name__
    return {
        "status": "hls_variant_check_failed",
        "classification": "HLS VARIANT CHECK FAILED",
        "reason": (
            f"{detail} — selected HLS variant check failed"
            if detail
            else "selected HLS variant check failed"
        ),
        "http_status": None,
    }


def classify_http_access_error(
    error: BaseException,
    *,
    source_group: str = "",
    provider: str = "",
) -> dict:
    """Return the mature recorder's actionable access/VPN classification."""
    if not isinstance(error, HTTPError):
        return {"blocked": False, "kind": "", "http_status": None, "geo_country": None}

    status = int(getattr(error, "code", 0) or 0)
    headers = getattr(error, "headers", None)
    error_type = (
        str(headers.get("X-ErrorType") or "").strip()
        if headers is not None
        else ""
    )
    if status == 403 and error_type.casefold() == "geo-blocked":
        country = (
            str(headers.get("Country") or "").strip()
            if headers is not None
            else ""
        )
        return {
            "blocked": True,
            "kind": "confirmed_geo",
            "http_status": 403,
            "geo_country": country or None,
        }

    if status == 403 and is_vpn_route_suspected_403(
        source_group=source_group,
        provider=provider,
    ):
        return {
            "blocked": True,
            "kind": "vpn_route_suspected",
            "http_status": 403,
            "geo_country": None,
        }

    if status in (450, 451):
        return {
            "blocked": True,
            "kind": "vpn_route_suspected",
            "http_status": status,
            "geo_country": None,
        }

    return {
        "blocked": False,
        "kind": "",
        "http_status": status or None,
        "geo_country": None,
    }
