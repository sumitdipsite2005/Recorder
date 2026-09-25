"""Shared source-policy definitions used by both recorder entry points.

Only rules that must mean the same thing in normal recording and Inspect/Watch
live here. Recorder-only execution behavior remains in record_dynamic.py.
"""

from __future__ import annotations

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
