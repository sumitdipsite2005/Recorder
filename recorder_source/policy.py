"""Shared source-policy definitions used by both recorder entry points.

Only rules that must mean the same thing in normal recording and Inspect/Watch
live here. Recorder-only execution behavior remains in record_dynamic.py.
"""

from __future__ import annotations

from typing import Optional

from .models import MATCH_MODE_EVENT_PHRASE, MATCH_MODE_EXACT_CHANNEL, SelectionPolicy


DEFAULT_MANDATORY_MIN_REMAINING_SEC = 15 * 60

PLAYLIST_GROUP_PROFILES = {
    "HOTSTAR_EVENTS": "HOTSTAR",
    "KHEL": "KHEL",
    "JIO_STAR_SPORTS": "JIO",
    "SONYLIV_EVENTS": "SONYLIV",
    "SONY_TV": "SONYLIV",
    "FANCODE": "FANCODE",
}

PLAYLIST_GROUP_MATCH_MODES = {
    "HOTSTAR_EVENTS": MATCH_MODE_EVENT_PHRASE,
    "JIO_STAR_SPORTS": MATCH_MODE_EXACT_CHANNEL,
    "KHEL": MATCH_MODE_EXACT_CHANNEL,
    "SONY_TV": MATCH_MODE_EXACT_CHANNEL,
    "SONYLIV_EVENTS": MATCH_MODE_EVENT_PHRASE,
    "FANCODE": MATCH_MODE_EVENT_PHRASE,
}

PLAYLIST_GROUP_LIFECYCLES = {
    "HOTSTAR_EVENTS": "EVENT",
    "SONYLIV_EVENTS": "EVENT",
    "FANCODE": "EVENT",
    "JIO_STAR_SPORTS": "LINEAR_TV",
    "KHEL": "LINEAR_TV",
    "SONY_TV": "LINEAR_TV",
}

PLAYLIST_GROUP_SOURCE_BUCKETS = {
    "JIO_STAR_SPORTS": "TV",
    "KHEL": "TV",
    "SONY_TV": "TV",
}

PLAYLIST_USER_AGENTS = {
    "DEFAULT": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
    ),
    "OTT_NAVIGATOR": "OTT Navigator/1.7.1.4",
    "TIVIMATE": "TiviMate",
}

# Provider request defaults shared by ONE BEST and Inspect/Watch.
# Playlist-entry metadata is applied afterward and therefore wins.
PROVIDER_ADDED_HEADERS = {
    "HOTSTAR": {
        "Accept": "*/*",
        "Sec-GPC": "1",
    },
    "JIO": {},
    "KHEL": {
        "Accept": "*/*",
        "Sec-GPC": "1",
    },
    "SONYLIV": {
        "Accept": "*/*",
        "Origin": "https://www.sonyliv.com",
        "Referer": "https://www.sonyliv.com/",
        "Sec-GPC": "1",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/142.0.0.0 Safari/537.36"
        ),
    },
    "FANCODE": {
        "Accept": "*/*",
        "Origin": "https://www.fancode.com",
        "Referer": "https://www.fancode.com/",
        "Sec-GPC": "1",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/142.0.0.0 Safari/537.36"
        ),
    },
}

DEFAULT_SELECTION_POLICY = SelectionPolicy(
    mandatory_min_remaining_sec=DEFAULT_MANDATORY_MIN_REMAINING_SEC,
    upgrade_min_remaining_sec=DEFAULT_MANDATORY_MIN_REMAINING_SEC,
    allow_unknown_expiry=False,
)

PROVIDER_SELECTION_POLICIES = {
    "HOTSTAR": DEFAULT_SELECTION_POLICY,
    "SONYLIV": SelectionPolicy(
        mandatory_min_remaining_sec=DEFAULT_MANDATORY_MIN_REMAINING_SEC,
        upgrade_min_remaining_sec=DEFAULT_MANDATORY_MIN_REMAINING_SEC,
        allow_unknown_expiry=True,
    ),
    "FANCODE": SelectionPolicy(
        mandatory_min_remaining_sec=DEFAULT_MANDATORY_MIN_REMAINING_SEC,
        upgrade_min_remaining_sec=DEFAULT_MANDATORY_MIN_REMAINING_SEC,
        allow_unknown_expiry=True,
        prefer_unknown_expiry_on_equal_quality=True,
    ),
    "JIO": SelectionPolicy(
        mandatory_min_remaining_sec=DEFAULT_MANDATORY_MIN_REMAINING_SEC,
        upgrade_min_remaining_sec=DEFAULT_MANDATORY_MIN_REMAINING_SEC,
        allow_unknown_expiry=True,
    ),
    "KHEL": SelectionPolicy(
        mandatory_min_remaining_sec=DEFAULT_MANDATORY_MIN_REMAINING_SEC,
        upgrade_min_remaining_sec=DEFAULT_MANDATORY_MIN_REMAINING_SEC,
        allow_unknown_expiry=True,
    ),
}


def selection_policy_for_provider(
    provider: object,
    *,
    mandatory_min_remaining_sec: Optional[int] = None,
    upgrade_min_remaining_sec: Optional[int] = None,
    allow_unknown_expiry: Optional[bool] = None,
    prefer_unknown_expiry_on_equal_quality: Optional[bool] = None,
    motion_cap_fps: Optional[float] = None,
) -> SelectionPolicy:
    """Resolve one provider selection policy with explicit runtime overrides."""
    provider_name = str(provider or "").strip().upper()
    base = PROVIDER_SELECTION_POLICIES.get(
        provider_name,
        DEFAULT_SELECTION_POLICY,
    )
    return SelectionPolicy(
        mandatory_min_remaining_sec=(
            int(mandatory_min_remaining_sec)
            if mandatory_min_remaining_sec is not None
            else int(base.mandatory_min_remaining_sec)
        ),
        upgrade_min_remaining_sec=(
            int(upgrade_min_remaining_sec)
            if upgrade_min_remaining_sec is not None
            else int(base.upgrade_min_remaining_sec)
        ),
        allow_unknown_expiry=(
            bool(allow_unknown_expiry)
            if allow_unknown_expiry is not None
            else bool(base.allow_unknown_expiry)
        ),
        prefer_unknown_expiry_on_equal_quality=(
            bool(prefer_unknown_expiry_on_equal_quality)
            if prefer_unknown_expiry_on_equal_quality is not None
            else bool(base.prefer_unknown_expiry_on_equal_quality)
        ),
        motion_cap_fps=(
            float(motion_cap_fps)
            if motion_cap_fps is not None
            else float(base.motion_cap_fps)
        ),
    )
