"""Shared passive source-probe transport policy.

HTTP retry/error mechanics used by both mature ONE BEST inspection and
Identity Coordinator Inspect/Watch live here. Recording orchestration,
alarms, scan cadence, and downloader retries remain with their runtime owner.
"""

from __future__ import annotations

import subprocess
import time
from datetime import timezone
from email.utils import parsedate_to_datetime
from http.cookiejar import CookieJar
from typing import Callable, Mapping, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, build_opener, HTTPCookieProcessor


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
    group = str(source_group or "").strip().upper()
    provider_name = str(provider or "").strip().upper()

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

    if status == 403 and (
        group in ("FANCODE", "JIO_STAR_SPORTS")
        or provider_name in ("FANCODE", "JIO")
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
