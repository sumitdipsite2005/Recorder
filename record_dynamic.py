
"""
Dynamic playlist-search live-stream recorder orchestrator.

This is the main dynamic recorder. It is designed around N_m3u8DL-RE and
automatically searches configured playlists for matching live sources,
selects and monitors the chosen source, and manages source renewal,
quality upgrades, recovery attempts, alarms, chunk validation, logging,
runtime controls, and finalization.

User-maintained search phrases, qualifiers, playlist-group selection,
playlist URL lists, schedule, recording duration, and output base name are
kept separately in ``recorder_dynamic_user_config.py``.

Provider-specific command profiles, headers, key/decryption behavior,
renewal rules, stall overrides, and other recorder policy remain in this file.

Run this file directly to start the dynamic recorder.
"""

from __future__ import annotations

import subprocess
import time
import os
import signal
import sys
import json
import runpy
import base64
import hashlib
import socket
import queue
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
try:
    import winsound
except ImportError:
    winsound = None

import ctypes
import shlex

try:
    import msvcrt  # Windows-only, used for single-key ACK/Restart handling
except Exception:
    msvcrt = None
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Iterable, List, Optional
import xml.etree.ElementTree as ET
from send2trash import send2trash
import re
from urllib.request import Request, urlopen, build_opener, HTTPCookieProcessor
from http.cookiejar import CookieJar
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse, urljoin, parse_qsl, urlencode

from recorder_runtime import sound as runtime_sound
from recorder_runtime.paths import build_recorder_output_paths
from recorder_source import discovery as source_discovery
from recorder_source import matching as source_matching
from recorder_source import selection as source_selection
from recorder_source import quality as source_quality
from recorder_source import transport as source_transport
from recorder_source.models import (
    SelectionDecision,
    SelectionPolicy,
    SourceAcquisitionRequest,
    SourceCandidate,
)
from recorder_source.policy import (
    PLAYLIST_GROUP_LIFECYCLES as SHARED_PLAYLIST_GROUP_LIFECYCLES,
    PLAYLIST_GROUP_MATCH_MODES as SHARED_PLAYLIST_GROUP_MATCH_MODES,
    PLAYLIST_GROUP_PROFILES as SHARED_PLAYLIST_GROUP_PROFILES,
    PLAYLIST_GROUP_SOURCE_BUCKETS as SHARED_PLAYLIST_GROUP_SOURCE_BUCKETS,
    PLAYLIST_USER_AGENTS as SHARED_PLAYLIST_USER_AGENTS,
    PROVIDER_ADDED_HEADERS as SHARED_PROVIDER_ADDED_HEADERS,
    PROVIDER_SELECTION_POLICIES as SHARED_PROVIDER_SELECTION_POLICIES,
)

# User configuration is shared through OneDrive across all recorder machines.
if sys.platform == "darwin":
    RECORDER_CONFIG_DIR = os.path.expanduser(
        "~/Library/CloudStorage/OneDrive-Personal/RECORDER"
    )
elif os.name == "nt":
    ONEDRIVE_ROOT = os.environ.get("OneDrive")
    if not ONEDRIVE_ROOT:
        raise RuntimeError("Windows OneDrive folder could not be located.")
    RECORDER_CONFIG_DIR = os.path.join(ONEDRIVE_ROOT, "RECORDER")
else:
    raise RuntimeError("Unsupported operating system for recorder config location.")

DYNAMIC_CONFIG_PATH = os.path.join(
    RECORDER_CONFIG_DIR,
    "recorder_dynamic_user_config.py",
)

if not os.path.isfile(DYNAMIC_CONFIG_PATH):
    raise RuntimeError(
        f"Dynamic recorder config not found: {DYNAMIC_CONFIG_PATH}"
    )

# Load the verified OneDrive config directly by absolute path. This avoids
# platform-specific Python module-search behavior for cloud-synced folders.
_dynamic_user_config = runpy.run_path(DYNAMIC_CONFIG_PATH)

NM3U8DL_PLAYLIST_PRIMARY_PHRASES = _dynamic_user_config["NM3U8DL_PLAYLIST_PRIMARY_PHRASES"]
NM3U8DL_PLAYLIST_REQUIRED_QUALIFIERS = _dynamic_user_config["NM3U8DL_PLAYLIST_REQUIRED_QUALIFIERS"]
NM3U8DL_PLAYLIST_REJECTED_QUALIFIERS = _dynamic_user_config["NM3U8DL_PLAYLIST_REJECTED_QUALIFIERS"]
NM3U8DL_PLAYLIST_PREFERRED_QUALIFIERS = _dynamic_user_config["NM3U8DL_PLAYLIST_PREFERRED_QUALIFIERS"]
NM3U8DL_PLAYLIST_GROUP = _dynamic_user_config["NM3U8DL_PLAYLIST_GROUP"]
NM3U8DL_PLAYLIST_GROUPS = _dynamic_user_config["NM3U8DL_PLAYLIST_GROUPS"]
SCHEDULE_START = _dynamic_user_config["SCHEDULE_START"]
RUN_DURATION_MIN = _dynamic_user_config["RUN_DURATION_MIN"]
BASE_NAME = _dynamic_user_config["BASE_NAME"]
OUTPUT_PATHS = build_recorder_output_paths(
    _dynamic_user_config.get("RECORDING_OUTPUT_DIR")
)
RECORDING_OUTPUT_DIR = str(OUTPUT_PATHS.root)
RECORDING_LOGS_DIR = str(OUTPUT_PATHS.recording_logs)
PLAYLIST_HISTORY_DIR = str(OUTPUT_PATHS.playlist_history)



# DOWNLOAD MODE SELECTOR
# DOWNLOAD_MODE = "ffmpeg"  # Change "ffmpeg" to "nm3u8dl" to use N_m3u8DL-RE
DOWNLOAD_MODE = "N_m3u8DL-RE"  # Change "ffmpeg" to "nm3u8dl" to use N_m3u8DL-RE

# ==================== CONFIG FOR FFMPEG DOWNLOAD ===================================================================

## CHANNELS REMOVED FROM THIS DYNAMIC N_m3u8DL-RE VARIANT OF THE RECORDER. USE STATIC VARIANT FOR FFMPEG CHANNEL RECORDING.

# ======================================================================================================================

# ==================== CONFIG FOR N_m3u8DL-RE DOWNLOAD  ===================================================================
# Dynamic playlist source
NM3U8DL_SOURCE_MODE = "playlist"  # "static" or "playlist"

# Local evidence-only playlist history. Set False while deliberately testing/breaking
# playlist behavior so test scans do not contaminate production history. This flag
# affects history collection only; source selection and recording behavior are unchanged.
PLAYLIST_HISTORY_ENABLED = True
PLAYLIST_HISTORY_LOCK_WAIT_SEC = 30
PLAYLIST_HISTORY_STALE_LOCK_SEC = 5 * 60


# Playlist group → command/runtime profile.
# The user selects only NM3U8DL_PLAYLIST_GROUP above.
NM3U8DL_PLAYLIST_GROUP_PROFILES = dict(SHARED_PLAYLIST_GROUP_PROFILES)

NM3U8DL_PLAYLIST_GROUP_PROFILE_OVERRIDES = {
    "SONY_TV": {
        "quality_upgrade_enabled": False,
    },
}

# Matching semantics belong to the playlist group, not to the downloader/runtime
# profile. Fixed TV-channel groups use a strong channel phrase/alias match plus
# qualifier gates; event groups keep flexible phrase matching for variable titles.
# The historical EXACT_CHANNEL mode name is retained for compatibility/history.
NM3U8DL_PLAYLIST_GROUP_MATCH_MODES = dict(SHARED_PLAYLIST_GROUP_MATCH_MODES)

# Stream lifecycle belongs to the playlist group, independently of matching or
# downloader profile. Event streams may genuinely end; linear TV channels should
# recover by resolving a fresh source instead of ending the overall recording.
NM3U8DL_PLAYLIST_GROUP_LIFECYCLES = dict(SHARED_PLAYLIST_GROUP_LIFECYCLES)

# Cookie handling is universal by default:
# if a canonical Cookie exists, send it once.
#
# Only known source-specific exceptions belong here.
NM3U8DL_PLAYLIST_GROUP_COOKIE_POLICY = {
    "HOTSTAR_EVENTS": "AUTO" #"SUPPRESS",
}


# Dynamic playlist command profiles
NM3U8DL_PLAYLIST_PROFILES = {
    "HOTSTAR": {
        "safe_overtime_min": 60,
        "renewal_mode": "EXPIRY_ROLLOVER",
        "allow_unknown_expiry": SHARED_PROVIDER_SELECTION_POLICIES["HOTSTAR"].allow_unknown_expiry,
        "added_headers": dict(SHARED_PROVIDER_ADDED_HEADERS["HOTSTAR"]),
        "key_mode": "SHAKA",
        "extra_args": "",
        "quality_upgrade_enabled": True,
        "quality_upgrade_target_fps": 50,
        "quality_upgrade_check_min": 5,
        "quality_upgrade_min_remaining_min": 15,
    },

    "JIO": {
        "safe_overtime_min": 0,
        "renewal_mode": "EXPIRY_ROLLOVER",
        "allow_unknown_expiry": SHARED_PROVIDER_SELECTION_POLICIES["JIO"].allow_unknown_expiry,
        "added_headers": dict(SHARED_PROVIDER_ADDED_HEADERS["JIO"]),
        "key_mode": "MP4DECRYPT",
        "extra_args": "--thread-count 1 --live-keep-segments",
        "hard_stall_required": 20,
        "soft_stall_required": 30,
        "min_file_appear_sec": 60,
        "quality_upgrade_enabled": True,
        "quality_upgrade_target_fps": 50,
        "quality_upgrade_check_min": 5,
        "quality_upgrade_min_remaining_min": 15,
    },
    
    "KHEL": {
        "safe_overtime_min": 0,
        "renewal_mode": "EXPIRY_ROLLOVER",
        "allow_unknown_expiry": SHARED_PROVIDER_SELECTION_POLICIES["KHEL"].allow_unknown_expiry,
        "added_headers": dict(SHARED_PROVIDER_ADDED_HEADERS["KHEL"]),
        "key_mode": "SHAKA",
        "extra_args": "",
    },

    "SONYLIV": {
        "safe_overtime_min": 0,
        "renewal_mode": "EXPIRY_ROLLOVER",
        "allow_unknown_expiry": SHARED_PROVIDER_SELECTION_POLICIES["SONYLIV"].allow_unknown_expiry,
        "added_headers": dict(SHARED_PROVIDER_ADDED_HEADERS["SONYLIV"]),
        "key_mode": "NONE",
        "extra_args": "",
        "quality_upgrade_enabled": True,
        "quality_upgrade_target_fps": 50,
        "quality_upgrade_check_min": 5,
        "quality_upgrade_min_remaining_min": 15,
    },
    
    "FANCODE": {
        "safe_overtime_min": 0,
        "renewal_mode": "EXPIRY_ROLLOVER",
        "allow_unknown_expiry": SHARED_PROVIDER_SELECTION_POLICIES["FANCODE"].allow_unknown_expiry,
        "prefer_unknown_expiry_on_equal_quality": SHARED_PROVIDER_SELECTION_POLICIES["FANCODE"].prefer_unknown_expiry_on_equal_quality,
        "added_headers": dict(SHARED_PROVIDER_ADDED_HEADERS["FANCODE"]),
        "key_mode": "SHAKA",
        "extra_args": "",
        "quality_upgrade_enabled": True,
        "quality_upgrade_target_fps": 50,
        "quality_upgrade_check_min": 5,
        "quality_upgrade_min_remaining_min": 15,
    },
}

NM3U8DL_PLAYLIST_USER_AGENTS = dict(SHARED_PLAYLIST_USER_AGENTS)


# N_M3U8DL CONFIG (if DOWNLOAD_MODE == "N_m3u8DL-RE")
# Per-stream stdout filters (N_m3u8DL only).
# - Keys are substring matches against the stream URL (extracted from NM3U8DL_PART_A).
# - Values are lists of substrings; matching lines are suppressed from stdout/logs.
NM3U8DL_STDOUT_EXCLUDE_DEFAULT = [
    #"ERROR:",
    "Failed to get KEY, ignore.",
]
NM3U8DL_STDOUT_EXCLUDE_BY_URL = {
    # Example: suppress noisy key-fetch errors for this stream.
    # "csm-e-cesevextprdausw2live": [
    #     "Failed to get KEY, ignore.",
    # ],
}
NM3U8DL_AUDIO_OFFSET_SEC = 0.0


# Part A: Dynamic per-source (URL + headers + keys)

# ICCCTV
# NM3U8DL_PART_A = 'N_m3u8DL-RE "https://live-d-01-icc-we.akamaized.net/variant/v1blackout/vcg-01-d/DASH_DASH/Live/channel(vcg-01-ch-hd-03)/manifest.mpd?vcfilter=486d73e7-26d4-45cb-a5ae-d4c7f346ce60&hdnts=st=1782046873~exp=1782046913~acl=/variant/v1blackout/vcg-01-d/*~id=96870cdd-9d68-4ce7-9dfd-6f94a7306c54~hmac=f3abfa6f7d9b92610b977d27444b7a71579ac3493725086c2ffcc4b14e27490e&hdcore=2.11.3" -H "Accept: */*" -H "Origin: https://www.icc-cricket.com" -H "Referer: https://www.icc-cricket.com/" -H "Sec-GPC: 1" -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36" --key 39bd302cdeed3710a89fb004d232ab67:9869f804ba18d7e508cbff781829221c --use-shaka-packager'

# 7AU
# NM3U8DL_PART_A = 'N_m3u8DL-RE "https://csm-e-sevenextprdlive-eb.bln1.yospace.com/csm/extlive/sevenprd01,MISC501.m3u8?appId=7plus&deviceType=web&platformType=web&ppId=cd7879b06cb4f677c1646ab19f231ae4d8763ff04bafe729c7f810cfe5169dbb&videoType=live&accountId=5650355166001&advertId=null&uaId=cd7879b06cb4f677c1646ab19f231ae4d8763ff04bafe729c7f810cfe5169dbb&optinDeviceType=&optinAdTracking=0&tvid=509e1aa82e31411d80be01940533e5ff&pc=3000&deviceId=68353795-bee5-4347-b991-5e58ae6defa1&mstatus=true&hl=en&ozid=2bb67ed5-c8f1-4a9e-8da2-396211d02e09&deviceSubType=desktop&referenceId=MISC501&vid=6309122219112&yo.hb=5000&pp=csai-web&custParams=y%253D7%2526c%253Dn%2526dpc%253D3205&y=7&c=n&dpc=3205&yo.pp=aGRudHM9ZXhwPTE3ODczODY3MjJ-YWNsPS8qfmhtYWM9NWRlZGM1Y2RkNjY0MmRiMmNhYzk2ZWJhZDk2ZjNmZTU4MjhhNzMzOTcxMWEwNTU5MzgwODk2Zjc0OTAzY2I1OSZQb2xpY3k9ZXlKVGRHRjBaVzFsYm5RaU9sdDdJa052Ym1ScGRHbHZiaUk2ZXlKRVlYUmxUR1Z6YzFSb1lXNGlPbnNpUVZkVE9rVndiMk5vVkdsdFpTSTZNVGM0TnpNNE5qY3lNbjBzSWtSaGRHVkhjbVZoZEdWeVZHaGhiaUk2ZXlKQlYxTTZSWEJ2WTJoVWFXMWxJam94TnpnM016QXdNekl4ZlgxOVhYMF8mU2lnbmF0dXJlPVdwMEFIamVMLXYzfkpQNllNRktKYlZpWUV-UUJTYUh0cmN-S1J6ZWVJNlZ6Y0dLckFDUlIxUkV2aFFTeVYtREN-Vnl1RXNkbHpxbUdjaWxnY3dzQ203WU5QVjNDTks4aWpiTHpDNWZpdllSUnpxSWZuNS1tUUhKYk5Xc3dUYVFVZzJMaTg5Y2VLZXdMNkl1UUJ3dnZmOXNRajU0RXU5VExuc1pHOWFrcEQ5VHZCZWMxQWdVVHdzdEI0VnQ1aWw3TXdicn5jS3QyWWlXT1V2bHpBcUY0UUVnbjRjdUFGM0dHYlhkLU9pVWw2cHRwVlE1SDRoSTNVZDdHUXB-TnlPSUcxc2YzNkRkZ0JkN0NHYlF4MnNQUlY0bmhsQUJwMXJBdDJOUU5qMVR5SWRzdVl4c1RJTGZqUjZRMkJKYzd-TjhOdVd-cHRvcGE1eFkzY3VhMEF0VS1PUV9fJktleS1QYWlyLUlkPUFQS0FJT1Q0TTZLTjZDUU9BV0VR" -H "Accept: */*" -H "Origin: null" -H "Referer: https://7plus.com.au/" -H "Sec-GPC: 1" -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36" --key 9cd408c15ec3359cb2512ef8c12627b9:df75b6bc664fe0b9d3d244531b7d28b6 --key c447e1401f34397d957a0b52ebe714eb:9a0031a73287e13676d342af13127b11 --use-shaka-packager --live-wait-time 10'

# HOTSTAR
# NM3U8DL_PART_A = 'N_m3u8DL-RE "https://live15p.hotstar.com/hls/live/2004207/inallow-duleep-2026/hin/1540076896/15mindvrm025927a804abe9424582fbd0398011a09723august2026/master_ap.m3u8?a=ns&hdnea=exp=1787466045~acl=/hls/live/2004207/inallow-duleep-2026/hin/1540076896/15mindvrm025927a804abe9424582fbd0398011a09723august2026/master_ap*~data=ip=Zt26r0bIrlZrAEgJsnoSNT-userid=SpWJsURgPPjk1o6gO2zKOfKEwVe7wgTqILOkNnIKa2cK-did=SeF3lusrNCTsZBeenGRygWmXHKOmgloOXrinQRyCdNpd-cc=in-bl=jhs-de=6-pl=androidtv-ap=25.02.16.1-ut=paid-ttl=1800-type=paid-raf=1787464845-~hmac=d15e2d817c470626e3149e2f6ea95fb8f7461c5dd04b465c19ad7733da40f456" -H "Accept: */*" -H "Origin: https://www.hotstar.com" -H "Referer: https://www.hotstar.com/" -H "Sec-GPC: 1" -H "User-Agent: Hotstar;in.startv.hotstar/25.02.24.8.11169@rtxcric(Android/15)"'

# FANCODE FROM DRMLIVE
# NM3U8DL_PART_A = 'N_m3u8DL-RE  "https://in-mc-fblive.fancode.com/mumbai/143399_english_hls_b76266f66a74436_1ta-di_h264/index.m3u8" -H "Referer: https://www.fancode.com/" -H "Sec-GPC: 1" -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"'

# SONY LIV FROM DRMLIVE
# NM3U8DL_PART_A = 'N_m3u8DL-RE  "https://sonydaimenew.akamaized.net/hls/live/2105079/cricodi2308/ENG/std_lrh-800300010.m3u8?hdnea=exp=1787497331~acl=/*~id=71421041997590671143781694294564~hmac=115f8cbf63088c063cbdc30cd40b575908f5365de03104d3ef7f6c1d999833a9" -H "Accept: */*" -H "Origin: https://www.sonyliv.com" -H "Referer: https://www.sonyliv.com/" -H "Sec-GPC: 1" -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"'


# Part B: Static/default options (appended to command with dynamic chunk name).
# Dynamic playlist mode replaces --select-video best at launch with the representation already selected by the recorder.
NM3U8DL_PART_B = '-M format=ts --live-real-time-merge --live-pipe-mux --download-retry-count 30 --http-request-timeout 90 --select-video best --select-audio best'

# ======================================================================================================================











# Engine identifiers (prep for future split)
# NOTE: All baseline helpers remain intact; this file only adds lightweight
# orchestration wrappers to ease the eventual module split without dropping
# any existing behavior.
# The goal of this layout is to make copy/paste refactors trivial later on:
# - Keep every FFmpeg-only helper isolated behind ENGINE_FFMPEG metadata.
# - Keep every N_m3u8DL-only helper isolated behind ENGINE_NM3U8DL metadata.
# - Run-time selection (resolve_engine → orchestrate_recording) works off
#   those descriptors instead of raw mode strings, so each engine can be
#   lifted into its own module without rewriting orchestration logic.

ENGINE_FFMPEG = "ffmpeg"
ENGINE_NM3U8DL = "N_m3u8DL-RE"

# N_m3u8DL Monitoring thresholds
NM3U8DL_SLOW_CHECK_THRESHOLD = 6   # consecutive slow checks before restart
NM3U8DL_SPEED_DEGRADATION_FACTOR = 0.05  # consider stalling if speed drops to ~5% of expected (300-400 Kbps range)
NM3U8DL_FALLBACK_BITRATE_KBPS = 4000 # Fallback only when N_m3u8DL detects a valid live refresh interval but does not report a numeric bitrate.
HARD_STALL_ZERO_BYTES = 1024
NM3U8DL_HARD_STALL_REQUIRED = 6 # 6 FOR NORMAL #60 for AU PRIME #30 FOR 7AU (not needed anymore with --live-wait-time 10) #20 for JIO
NM3U8DL_SOFT_STALL_REQUIRED = 10 # 10 FOR NORMAL #100 for AU PRIME #50 FOR 7AU (not needed anymore with --live-wait-time 10) #30 for JIO
NM3U8DL_STALL_MAX_EVENTS = 5
NM3U8DL_CRASH_MAX_ATTEMPTS = 2
CONTINUOUS_ALARM_MAX_SEC = 5 * 60
NM3U8DL_CHECK_INTERVAL_MULTIPLIER = 2
NM3U8DL_MIN_GROWTH_CHECK_INTERVAL_SEC = 30  # Never judge N_m3u8DL file growth on a window shorter than 30s.
NM3U8DL_TERMINAL_PROGRESS_INTERVAL_SEC = 60  # Permanent Vid/Aud terminal snapshots; native N refresh remains unchanged.
NM3U8DL_HEALTH_LOG_INTERVAL_SEC = 60         # Match the other engines' normal Good speed log cadence.
NM3U8DL_ALARM_BEEP_HZ = 1500
NM3U8DL_ALARM_BEEP_MS = 500
NM3U8DL_ALARM_BEEP_GAP_SEC = 0.05
ALARM_SOUND_FILENAME = "alarm.wav"
ALARM_LINGER_SEC = 30
FFMPEG_OFF_ALARM_THRESHOLD_SEC = 300
FF_GROWTH_CHECK_INTERVAL = 30
FF_HEALTH_LOG_INTERVAL = 60
FF_HARD_STALL_ZERO_BYTES = 1024
FF_HARD_STALL_REQUIRED = 3
FF_SOFT_STALL_REQUIRED = 6
FF_STALL_MAX_ATTEMPTS = 5
GOOD_BEEP_HEALTHY_REQUIRED = 1
RUNTIME_CONTROLS_REMINDER_SEC = 10 * 60

# Orchestrator backoff controls (shared)
DEFAULT_MIN_BACKOFF = 5
DEFAULT_MAX_BACKOFF = 30
STREAM_OFF_SLEEP = 15

# N_m3u8DL MANIFEST_DEAD retry policy (NM-B)
MANIFEST_DEAD_MAX_ATTEMPTS_MANUAL_STARTUP = 1
MANIFEST_DEAD_MAX_ATTEMPTS_SCHEDULED_STARTUP = 2
MANIFEST_DEAD_MAX_ATTEMPTS_RECOVERY = 2

# N_m3u8DL NO_FILE_APPEAR detection + retry policy (NM-C)
FILE_APPEAR_DEADLINE_MULTIPLIER = 2
FILE_APPEAR_DEADLINE_EXTRA_SEC = 10
FILE_APPEAR_POLL_SEC = 0.25

NO_FILE_APPEAR_MAX_ATTEMPTS_MANUAL_STARTUP = 2
NO_FILE_APPEAR_MAX_ATTEMPTS_SCHEDULED_STARTUP = 4
NO_FILE_APPEAR_MAX_ATTEMPTS_RECOVERY = 4

# Dynamic URL renewal policy
NM3U8DL_REPLACEMENT_SAFETY_MARGIN_MIN = 5
NM3U8DL_PLAYLIST_RENEWAL_LEAD_MIN = 30
NM3U8DL_NEW_SOURCE_MIN_REMAINING_MIN = 15
NM3U8DL_PLAYLIST_CHECK_INTERVAL_MIN = 5
NM3U8DL_PLAYLIST_RISK_CHECK_INTERVAL_SEC = 60
# Access/VPN confirmation and recovery scans are intentionally separate from
# authorization-risk checks so VPN attention can react faster without changing
# renewal behavior.
NM3U8DL_PLAYLIST_ACCESS_CHECK_INTERVAL_SEC = 30

# Dynamic playlist discovery: only the network fetch is parallelized. Playlist
# adaptation, authorization/activation, parsing, candidate construction, and
# history/result ordering remain sequential and deterministic.
NM3U8DL_PLAYLIST_FETCH_WORKERS = 8

# Dynamic source quality inspection
NM3U8DL_QUALITY_PROBE_WORKERS = 6
NM3U8DL_QUALITY_HTTP_TIMEOUT_SEC = source_transport.QUALITY_HTTP_TIMEOUT_SEC
NM3U8DL_QUALITY_HTTP_MAX_ATTEMPTS = source_transport.QUALITY_HTTP_MAX_ATTEMPTS
NM3U8DL_QUALITY_HTTP_RETRY_BASE_SEC = source_transport.QUALITY_HTTP_RETRY_BASE_SEC
NM3U8DL_QUALITY_HTTP_RETRY_MAX_SEC = source_transport.QUALITY_HTTP_RETRY_MAX_SEC
NM3U8DL_QUALITY_HTTP_RETRYABLE_STATUS_CODES = (
    source_transport.QUALITY_HTTP_RETRYABLE_STATUS_CODES
)
NM3U8DL_QUALITY_FFPROBE_TIMEOUT_SEC = 20
NM3U8DL_QUALITY_BITRATE_SAMPLE_SEC = 4
NM3U8DL_QUALITY_BITRATE_SAMPLE_TIMEOUT_SEC = 12

# DASH P/I detection follows: MPD scanType -> H.264 SPS -> H.264 picture/field
# structure -> idet. FFprobe is deliberately not part of P/I detection.
NM3U8DL_QUALITY_H264_TRACE_TIMEOUT_SEC = 12
NM3U8DL_QUALITY_H264_PICTURE_SAMPLE_FRAMES = 60
NM3U8DL_QUALITY_H264_MIN_PICTURE_HEADERS = 12
NM3U8DL_QUALITY_IDET_SAMPLE_FRAMES = 360
NM3U8DL_QUALITY_IDET_TIMEOUT_SEC = 90
NM3U8DL_QUALITY_IDET_INTERLACE_THRESHOLD = 1.20
NM3U8DL_QUALITY_IDET_MIN_CLASSIFIED_FRAMES = 12
NM3U8DL_QUALITY_IDET_MIN_CLASSIFIED_SHARE = 0.75
NM3U8DL_QUALITY_IDET_DOMINANT_SHARE = 0.90

# Known Phase-2 limit: current source inventory tops out at 50-motion video.
# Ranking intentionally does not give 60+ motion a higher tier yet. The 50
# quality-upgrade target remains, while periodic checks may still improve
# resolution/scan/bitrate inside the 50-motion class (for example 720p50 ->
# 1080p50). If real 60/59.94+ sources are introduced, extend this cap and
# revisit the 50-target lifecycle gates together.
NM3U8DL_QUALITY_RANKING_MOTION_CAP_FPS = 50.0

@dataclass(frozen=True)
class EngineResult:
    """Minimal contract returned by each worker engine."""

    status: str  # ok | stalled | ended | no_stream | error | duration_reached
    reason: str
    chunk_path: Optional[str]
    chunk_name: Optional[str]
    metrics: dict = field(default_factory=dict)


@dataclass(frozen=True)
class RecorderEngine:
    """Lightweight engine descriptor to enable clean future splits."""

    name: str
    run_cycle: Callable[[RecorderState, int, Optional[Callable[[str], None]], Optional[float]], EngineResult]
    description: str
    summary_lines_fn: Optional[Callable[[], Iterable[str]]] = None

    def summary_lines(self) -> List[str]:
        if not self.summary_lines_fn:
            return []
        return list(self.summary_lines_fn())

# Runtime state container (no scattered globals)
@dataclass
class RecorderState:
    """All runtime state that changes while recording."""

    # Stop / timing
    stop_flag: bool = False
    start_time: float = field(default_factory=time.time)
    deadline_ts: Optional[float] = None
    alarm_linger_until: Optional[float] = None

    # Mutable runtime-duration controls
    original_duration_min: Optional[float] = None
    original_deadline_ts: Optional[float] = None
    duration_change_count: int = 0
    timing_lock: object = field(default_factory=threading.Lock, repr=False)
    console_input_mode: Optional[str] = None
    console_input_buffer: str = ""
    console_pending_runtime_change: Optional[dict] = None
    runtime_controls_reminder_started: bool = False
    runtime_controls_reminder_stop_event: Optional[threading.Event] = None
    runtime_controls_reminder_thread: Optional[threading.Thread] = None

    # Recorder-wide sound snooze. This suppresses sound only; monitoring, alarm
    # state, retries, recovery, logging, and source handling continue unchanged.
    sound_snooze_mode: Optional[str] = None  # timed | run | recording
    sound_snooze_until_ts: Optional[float] = None
    sound_snooze_run_attempt: Optional[int] = None

    # Chunk / worker-run numbering
    chunk_index: int = 0
    run_attempt_index: int = 0

    # Best video params + consistency flags
    best_w: int = 0
    best_h: int = 0
    best_fps: float = 0.0
    same_params: bool = True
    mixed_reason: str = ""

    # Audio / A/V stream-layout consistency tracking (locked after first GOOD chunk)
    audio_expected: Optional[bool] = None
    audio_layout_ref: Optional[list] = None
    audio_codec_ref: Optional[str] = None
    audio_sr_ref: Optional[int] = None
    audio_ch_ref: Optional[int] = None
    stream_layout_ref: Optional[list] = None

    # N_m3u8DL monitoring/counters
    nm3u8dl_selected_bitrate_kbps: int = 0
    nm3u8dl_bitrate_is_fallback: bool = False
    nm3u8dl_refresh_interval_s: int = 0
    nm3u8dl_stall_flag: bool = False
    nm3u8dl_stall_reason: Optional[str] = None
    nm3u8dl_hard_count: int = 0
    nm3u8dl_soft_count: int = 0
    nm3u8dl_run_had_healthy_check: bool = False
    nm3u8dl_last_health_log_t: float = 0.0
    # A stall EVENT exists only after the configured hard/soft confirmation
    # threshold is reached. Process restarts, manifest failures, and no-file
    # failures do not increment or clear this counter.
    nm3u8dl_stall_event_count: int = 0
    manifest_fail_count: int = 0
    no_file_appear_count: int = 0
    nm3u8dl_crash_count: int = 0
    nm3u8dl_stop_event: Optional[threading.Event] = None
    nm3u8dl_event_queue: Optional["queue.SimpleQueue"] = None
    nm3u8dl_alarm_active: bool = False
    # Once the stall alarm is ACKed or reaches its five-minute sound cap, keep
    # the same stall incident quiet until confirmed healthy growth resets it.
    nm3u8dl_post_ack_silent: bool = False
    nm3u8dl_ack_requested: bool = False
    nm3u8dl_manual_restart_requested: bool = False
    nm3u8dl_manual_exclude_requested: bool = False
    nm3u8dl_pending_manual_exclusion: Optional[dict] = None
    nm3u8dl_run_active: bool = False

    # Shared alarm / keyboard runtime state
    alarm_ack_requested: bool = False
    shared_alarm_stop_event: Optional[threading.Event] = None
    shared_alarm_thread: Optional[threading.Thread] = None
    shared_alarm_type: Optional[str] = None
    shared_alarm_timeout_timer: Optional[threading.Timer] = None
    shared_alarm_deadline: Optional[float] = None
    shared_alarm_activity_id: Optional[str] = None
    nm3u8dl_alarm_stop_event: Optional[threading.Event] = None
    nm3u8dl_alarm_thread: Optional[threading.Thread] = None
    nm3u8dl_key_listener_started: bool = False
    nm3u8dl_key_listener_stop_event: Optional[threading.Event] = None
    nm3u8dl_key_listener_thread: Optional[threading.Thread] = None
    
    # Dynamic playlist renewal / access-block state
    nm3u8dl_running_source: Optional[dict] = None
    nm3u8dl_pending_source: Optional[dict] = None
    nm3u8dl_renewal_rollover_requested: bool = False
    nm3u8dl_rollover_reason: Optional[str] = None
    nm3u8dl_access_block_consecutive: int = 0
    nm3u8dl_access_block_alarm_acknowledged: bool = False
    nm3u8dl_access_block_detected_ts: Optional[float] = None
    nm3u8dl_access_block_playlist_urls: List[str] = field(default_factory=list)
    # Access incidents discovered while a lower-quality source is recording are
    # quality-recovery incidents. Authorization incidents are kept separate so
    # reaching target quality does not suppress a later expiry-risk VPN problem.
    nm3u8dl_access_block_purpose: Optional[str] = None
    # Authoritative per-source state from the previous completed access/VPN
    # verification. Used only to decide when the full recheck table changed.
    nm3u8dl_access_block_status_snapshot: dict = field(default_factory=dict)
    nm3u8dl_playlist_connectivity_alarm_silenced: bool = False

    # Dynamic-playlist downloader-failure failover. This is deliberately separate
    # from the mature MANIFEST/NO_FILE/STALL/CRASH counters above: those counters
    # keep their existing incident/recovery meanings, while this state answers one
    # narrower question — has this exact effective stream already used its direct
    # second chance during the current recording?
    nm3u8dl_running_stream_fingerprint: Optional[str] = None
    # Per-fingerprint first-failure probation survives source changes until a
    # different stream proves healthy. This keeps planned rollover behavior from
    # accidentally erasing a stream's already-used first chance.
    nm3u8dl_failover_probations: dict = field(default_factory=dict)
    nm3u8dl_failover_retry_source: Optional[dict] = None
    nm3u8dl_bad_stream_fingerprints: dict = field(default_factory=dict)
    # Operator rejections are intentionally separate from automatic failover.
    # They last only for this RecorderState/recording and must survive VPN/access
    # resets that are allowed to forgive route-dependent automatic failures.
    # Manual rejection is broader than automatic failover: it identifies the feed
    # by delivery family + stream type + video quality, not by one exact URL/header
    # fingerprint, so alternate playlist routes to the same feed stay excluded.
    nm3u8dl_manual_excluded_feed_signatures: dict = field(default_factory=dict)
    nm3u8dl_failover_waiting_for_alternative: bool = False
    nm3u8dl_failover_alarm_silenced: bool = False

    # Evidence-only playlist scan history. Failures here must never interrupt
    # recording; final cleanup is gated only so uncommitted evidence is retained.
    playlist_history_active: bool = False
    playlist_history_id: str = ""
    playlist_history_machine: str = ""
    playlist_history_started_ts: Optional[float] = None
    playlist_history_txt_path: Optional[str] = None
    playlist_history_jsonl_path: Optional[str] = None
    playlist_history_scan_index: int = 0
    playlist_history_txt_capture_failed: bool = False
    playlist_history_json_capture_failed: bool = False
    playlist_history_commit_ok: bool = True

    # FFmpeg alarm tracking (shared alarm system)
    ffmpeg_alarm_active: bool = False
    ffmpeg_post_ack_silent: bool = False
    ffmpeg_post_ack_silent_type: Optional[str] = None
    ffmpeg_stall_run_count: int = 0
    ffmpeg_alarm_only_state: bool = False
    ffmpeg_max_attempts_exhausted: bool = False
    ffmpeg_off_timer_start: Optional[float] = None
    ffmpeg_off_alarm_active: bool = False
    ffmpeg_off_post_ack_silent: bool = False
    ffmpeg_selected_bitrate_kbps_max: int = 0
    ffmpeg_healthy_consec_count: int = 0
    ffmpeg_beeped_this_healthy_streak: bool = False
    ffmpeg_had_alarm_incident: bool = False

    # Rolling stats used for summary
    stats: dict = field(default_factory=lambda: {
        "process_start": None,
        "process_end": None,
        "good_chunks": 0,
        "bad_chunks": 0,
        "good_time": 0.0,      # sum of GOOD chunk durations (seconds)
        "min_chunk": None,
        "max_chunk": None,
        "sessions": [],        # list of dicts: {"start": ts, "dur": dur}
        "last_good_br_kbps": None,
        "last_run_start": None,
    })
    
    nm3u8dl_healthy_consec_count: int = 0
    nm3u8dl_beeped_this_healthy_streak: bool = False
    nm3u8dl_had_alarm_incident: bool = False
    
# EngineResult.metrics keys (stable contract):
# - ran_30s: bool  (engine stayed healthy for >= 30s)
# - stall_flag: bool  (N_m3u8DL only: file-growth stall detected)
# - selected_bitrate_kbps: int  (N_m3u8DL only: parsed bitrate, or synthetic fallback used for stall monitoring)
# - refresh_interval_s: int  (N_m3u8DL only: parsed manifest refresh interval in seconds; 0 means unknown)
# - manifest_dead: bool  (N_m3u8DL only: startup could not establish a usable live manifest; missing bitrate alone is allowed when refresh is valid)
# - stall_type: str  (FFmpeg only: "hard" or "soft" when status="stalled")
# These will be initialized later, after wait_until_start()
run_ts = None
FINAL_FILE = None
final_base = None
CHUNKS_DIR = None
LIST_FILE = None
TERMCAP_PATH = None
RAW_EXTERNAL_PATH = None

FFMPEG_PROGRESS_PERIOD = 1   # seconds
STABILITY_SEC = 15          # minimum good duration in seconds
BLACK_LEN = 1.0             # use only integers
FF_SELECTED_BITRATE_LOG_MIN_DELTA_KBPS = 250
FF_SELECTED_BITRATE_LOG_MIN_INTERVAL_SEC = 10

class WindowsInhibitor:
    ES_CONTINUOUS       = 0x80000000
    ES_SYSTEM_REQUIRED  = 0x00000001

    def inhibit(self):
        ctypes.windll.kernel32.SetThreadExecutionState(
            self.ES_CONTINUOUS | self.ES_SYSTEM_REQUIRED
        )

    def uninhibit(self):
        ctypes.windll.kernel32.SetThreadExecutionState(
            self.ES_CONTINUOUS
        )
# ==============================================================================
# Shared utilities (logging, terminal capture, sounds, alarms, signals, media probing)
# ==============================================================================
PROGRESS_LINE_ACTIVE = False
PROGRESS_LINE_TEXT = None
PROGRESS_LINE_LOCK = threading.Lock()
CONSOLE_OUTPUT_LOCK = threading.RLock()
CONSOLE_INPUT_ACTIVE = False
CONSOLE_DEFERRED_TERMINAL_LINES = []

# Terminal presentation is grouped by logical activity rather than by message
# type. Output from the same activity stays together; switching to another
# activity inserts exactly one visual separator. Thread identity is the safe
# default, while recurring/structured work can assign a unique activity id.
TERMINAL_ACTIVITY_COUNTER = 0
TERMINAL_LAST_ACTIVITY_ID = None
TERMINAL_LAST_OUTPUT_WAS_BLANK = False
TERMINAL_ACTIVITY_LOCAL = threading.local()


def set_progress_line_active(active: bool, text: Optional[str] = None):
    global PROGRESS_LINE_ACTIVE, PROGRESS_LINE_TEXT
    with PROGRESS_LINE_LOCK:
        PROGRESS_LINE_ACTIVE = active
        PROGRESS_LINE_TEXT = text if active else None

def clear_progress_line():
    global PROGRESS_LINE_ACTIVE, PROGRESS_LINE_TEXT
    with CONSOLE_OUTPUT_LOCK:
        with PROGRESS_LINE_LOCK:
            if PROGRESS_LINE_ACTIVE:
                # Interactive E/D entry owns the terminal. The progress line was
                # cleared before entry began, so do not redraw/advance it here.
                if not CONSOLE_INPUT_ACTIVE:
                    sys.stdout.write("\r\n")
                    sys.stdout.flush()

                # Preserve the last visible progress snapshot in terminal capture.
                if PROGRESS_LINE_TEXT:
                    termcap_write(PROGRESS_LINE_TEXT)

                PROGRESS_LINE_ACTIVE = False
                PROGRESS_LINE_TEXT = None


def new_terminal_activity(label: str = "activity") -> str:
    """Create one unique id for a logical terminal-output activity instance."""
    global TERMINAL_ACTIVITY_COUNTER

    with CONSOLE_OUTPUT_LOCK:
        TERMINAL_ACTIVITY_COUNTER += 1
        safe_label = str(label or "activity").strip() or "activity"
        return f"{safe_label}:{TERMINAL_ACTIVITY_COUNTER}"


def get_terminal_activity_id() -> str:
    """Return this thread's explicit activity id, or its stable default id."""
    explicit_activity = getattr(
        TERMINAL_ACTIVITY_LOCAL,
        "activity_id",
        None,
    )

    if explicit_activity:
        return str(explicit_activity)

    return f"thread:{threading.get_ident()}"


def set_terminal_activity_context(activity_id: Optional[str]):
    """Set/clear the logical activity owned by the current thread."""
    if activity_id is None:
        if hasattr(TERMINAL_ACTIVITY_LOCAL, "activity_id"):
            delattr(TERMINAL_ACTIVITY_LOCAL, "activity_id")
        return

    TERMINAL_ACTIVITY_LOCAL.activity_id = str(activity_id)


class terminal_activity_scope:
    """Temporarily assign one logical terminal activity to the current thread."""

    def __init__(self, activity_id: Optional[str] = None, label: str = "activity"):
        self.activity_id = activity_id or new_terminal_activity(label)
        self.previous_activity_id = None

    def __enter__(self):
        self.previous_activity_id = getattr(
            TERMINAL_ACTIVITY_LOCAL,
            "activity_id",
            None,
        )
        set_terminal_activity_context(self.activity_id)
        return self.activity_id

    def __exit__(self, exc_type, exc, tb):
        set_terminal_activity_context(self.previous_activity_id)
        return False


def _prepare_terminal_activity(
    activity_id: Optional[str] = None,
    *,
    upcoming_output_is_blank: bool = False,
) -> str:
    """
    Switch terminal ownership to one logical activity.

    Recurring runtime activities stay on the timestamped timeline. When terminal
    ownership changes, emit one blank [INFO] row only when the terminal does not
    already end in a blank row and the upcoming output is not itself blank. This
    keeps explicit section spacing and automatic activity spacing from doubling.
    """
    global TERMINAL_LAST_ACTIVITY_ID, TERMINAL_LAST_OUTPUT_WAS_BLANK

    effective_activity_id = str(
        activity_id or get_terminal_activity_id()
    )

    with CONSOLE_OUTPUT_LOCK:
        activity_changed = (
            TERMINAL_LAST_ACTIVITY_ID is not None
            and effective_activity_id != TERMINAL_LAST_ACTIVITY_ID
        )

        if (
            activity_changed
            and not TERMINAL_LAST_OUTPUT_WAS_BLANK
            and not upcoming_output_is_blank
        ):
            clear_progress_line()
            separator = (
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [INFO]"
            )
            print(separator)
            termcap_write(separator)
            TERMINAL_LAST_OUTPUT_WAS_BLANK = True

        TERMINAL_LAST_ACTIVITY_ID = effective_activity_id

    return effective_activity_id


def render_progress_line(
    text: str,
    pad: int = 0,
    colorize: bool = False,
    activity_id: Optional[str] = None,
) -> bool:
    """Render one updating progress line unless interactive runtime input owns stdout."""
    with CONSOLE_OUTPUT_LOCK:
        if CONSOLE_INPUT_ACTIVE:
            return False

        global TERMINAL_LAST_OUTPUT_WAS_BLANK

        _prepare_terminal_activity(activity_id)

        display_text = colorize_terminal_log_line(text) if colorize else text

        # Never let erase-padding wrap onto another terminal row. The progress
        # text itself stays unchanged; only surplus spaces are capped to the
        # currently available width.
        safe_pad = max(0, int(pad))
        try:
            if getattr(sys.stdout, "isatty", lambda: False)():
                terminal_columns = int(os.get_terminal_size(sys.stdout.fileno()).columns)
                if terminal_columns > 0:
                    safe_pad = min(
                        safe_pad,
                        max(0, terminal_columns - len(text) - 1),
                    )
        except (OSError, ValueError, AttributeError):
            pass

        sys.stdout.write("\r" + display_text + (" " * safe_pad))
        sys.stdout.flush()
        TERMINAL_LAST_OUTPUT_WAS_BLANK = False
        set_progress_line_active(True, text)
        return True


# Dynamic playlist progress belongs to the active scan activity. It remains one
# in-place row while that activity owns the terminal; unrelated activities are
# separated centrally by the shared terminal-activity renderer.


def render_dynamic_playlist_progress(stage: str, current: int, total: int):
    """Show one in-place dynamic playlist-resolution progress line."""
    total = max(1, int(total))
    current = min(max(1, int(current)), total)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = (
        f"{ts} [INFO] Resolving dynamic playlist source → "
        f"{stage} {current}/{total}..."
    )
    render_progress_line(line, pad=60, colorize=True)


def finish_dynamic_playlist_progress(message: str):
    """Replace the live progress row with one permanent phase-completion line."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = (
        f"{ts} [INFO] Resolving dynamic playlist source → "
        f"{message}"
    )
    render_progress_line(line, pad=60, colorize=True)
    clear_progress_line()



def _nm3u8dl_terminal_endpoint_color(endpoint: str) -> str:
    """Assign stable per-process categorical colors to effective endpoints."""
    endpoint_key = str(endpoint or "").strip().casefold()
    endpoint_palette = (
        27,   # royal blue
        201,  # magenta
        208,  # orange
        37,   # teal
        99,   # violet-blue
        213,  # orchid pink
        39,   # sky blue
        135,  # purple
        180,  # warm tan
        45,   # bright cyan-blue
    )

    color_map = getattr(
        _nm3u8dl_terminal_endpoint_color,
        "_color_map",
        None,
    )
    if color_map is None:
        color_map = {}
        _nm3u8dl_terminal_endpoint_color._color_map = color_map

    if endpoint_key not in color_map:
        color_map[endpoint_key] = endpoint_palette[
            len(color_map) % len(endpoint_palette)
        ]

    return f"\033[1;38;5;{color_map[endpoint_key]}m"


def colorize_terminal_log_line(line: str) -> str:
    """
    Add restrained N_m3u8DL-style terminal colors.

    IMPORTANT:
    - Used only for live terminal display.
    - Saved terminal capture/log files remain plain text.
    - If stdout is redirected, ANSI colors are not emitted.
    """
    if not getattr(sys.stdout, "isatty", lambda: False)():
        return line

    RESET = "\033[0m"
    GREEN = "\033[1;92m"
    YELLOW = "\033[1;93m"
    RED = "\033[1;91m"
    EXCLUDED_COLOR = "\033[38;5;45m"
    CYAN = "\033[1;96m"
    # Secondary/muted semantic colors. The bright/bold variants above are
    # reserved for the scan cues and the fixed summary numbers, so labels and
    # source identities remain readable without competing for attention.
    MUTED_GREEN = "\033[2;32m"
    MUTED_YELLOW = "\033[2;33m"
    MUTED_RED = "\033[2;31m"
    MUTED_CYAN = "\033[2;36m"
    SOFT_GREEN = "\033[32m"
    SOFT_YELLOW = "\033[33m"
    SOFT_RED = "\033[31m"
    SOFT_CYAN = "\033[36m"
    VPN_ACTION = "\033[1;30;103m"  # Bold black text on bright yellow background
    GRAY = "\033[90m"
    SELECTED = "\033[1;97;42m"

    # User-action VPN warnings should be impossible to miss. Color the entire
    # terminal line, while saved terminal/log capture remains plain text.
    if (
        "Potentially better matching source is access-blocked" in line
        or "Source access remains blocked after confirmation" in line
    ):
        return f"{VPN_ACTION}{line}{RESET}"

    if re.search(r"=+\s+RUN \d+ START\b", line):
        return f"{CYAN}{line}{RESET}"

    colored = line

    # ---------------------------------------------------------
    # Log severity
    # ---------------------------------------------------------
    colored = colored.replace(
        "[WARN]",
        f"{YELLOW}[WARN]{RESET}",
    )
    colored = colored.replace(
        "[ERROR]",
        f"{RED}[ERROR]{RESET}",
    )

    # N_m3u8DL's own severity after its ANSI has been stripped.
    colored = colored.replace(
        "N_m3u8DL: WARN :",
        f"N_m3u8DL: {YELLOW}WARN{RESET} :",
    )
    colored = colored.replace(
        "N_m3u8DL: ERROR :",
        f"N_m3u8DL: {RED}ERROR{RESET} :",
    )

    # ---------------------------------------------------------
    # Major sections / workflow markers
    # ---------------------------------------------------------
    for token in (
        "=== DYNAMIC SOURCE STATUS ===",
        "=== WAITING FOR PLAYLIST SOURCE ===",
        "=== ACCESS/VPN RECOVERY RESULTS ===",
        "=== ACCESS/VPN RECHECK RESULTS ===",
        "=== PLAYLIST RENEWAL CHECK ===",
        "=== QUALITY UPGRADE FOUND ===",
    ):
        colored = colored.replace(
            token,
            f"{CYAN}{token}{RESET}",
        )

    group_separator_match = re.search(
        r"-{20}\s+[A-Za-z0-9_]+\s+-{20}",
        line,
    )
    if group_separator_match:
        group_separator = group_separator_match.group(0)
        colored = colored.replace(
            group_separator,
            f"{CYAN}{group_separator}{RESET}",
            1,
        )

    colored = colored.replace(
        "[RUN]",
        f"{CYAN}[RUN]{RESET}",
    )
    colored = colored.replace(
        "[WAIT]",
        f"{CYAN}[WAIT]{RESET}",
    )
    colored = colored.replace(
        "[GAP]",
        f"{YELLOW}[GAP]{RESET}",
    )
    colored = colored.replace(
        "[SLEEP]",
        f"{GRAY}[SLEEP]{RESET}",
    )

    # ---------------------------------------------------------
    # Dynamic playlist results
    # ---------------------------------------------------------
    # Keep playlist URLs and summary counts/labels neutral. Only the explicit
    # uppercase status cues carry color so the table stays easy to scan.
    if re.search(r"\bPLAYLIST\s+:", line):
        colored = colored.replace(
            "PLAYLIST",
            f"{CYAN}PLAYLIST{RESET}",
            1,
        )

    if re.search(r"\bPLAYLIST\s+:.*\b\d+ MATCHED\b", line):
        colored = re.sub(
            r"\b(\d+ MATCHED)\b",
            lambda match: f"{GREEN}{match.group(1)}{RESET}",
            colored,
            count=1,
        )

    colored = colored.replace(
        "[SELECTED]",
        f"{SELECTED}[SELECTED]{RESET}",
    )
    colored = colored.replace(
        "[RETAINED]",
        f"{SELECTED}[RETAINED]{RESET}",
    )

    # Candidate rows use one simple availability cue on the left. Detailed
    # unusable-state classifications live after the colon with the source facts.
    colored = colored.replace(
        "[ON]",
        f"{GREEN}[ON]{RESET}",
    )
    colored = colored.replace(
        "[OFF]",
        f"{YELLOW}[OFF]{RESET}",
    )

    # Color only the final/effective endpoint in each candidate identity.
    # The mapping is derived from the endpoint text, so the same endpoint keeps
    # the same color across scans and separate recorder runs.
    if "[ON]" in line or "[OFF]" in line:
        endpoint_identity_match = re.search(
            r"\b(?:DASH|HLS|STREAM)\s+\(([^()]*)\)(?:\s+\[([^\]]+)\])?",
            line,
        )
        if endpoint_identity_match:
            source_family = str(
                endpoint_identity_match.group(2) or ""
            ).strip()
            identity_hosts = [
                host.strip()
                for host in endpoint_identity_match.group(1).split("→")
                if host.strip()
            ]
            if identity_hosts:
                effective_endpoint = identity_hosts[-1]
                endpoint_color = _nm3u8dl_terminal_endpoint_color(
                    effective_endpoint
                )
                if source_family:
                    colored = colored.replace(
                        f"[{source_family}]",
                        f"{endpoint_color}[{source_family}]{RESET}",
                        1,
                    )
                colored = colored.replace(
                    effective_endpoint,
                    f"{endpoint_color}{effective_endpoint}{RESET}",
                    1,
                )

    event_candidate_line = bool(
        re.search(r"\bEvent \d+ \[(?:ON|OFF)\]\s*:", line)
    )

    if "[ON]" in line and ": SELECTED —" in line:
        colored = colored.replace(
            "SELECTED",
            f"{SELECTED}SELECTED{RESET}",
            1,
        )

    if "[ON]" in line and "BEST —" in line:
        colored = colored.replace(
            "BEST",
            f"{CYAN}BEST{RESET}",
            1,
        )

    # Quality provenance stays compact. Manifest is the normal/common source
    # and remains neutral; fallback/inspection sources are highlighted.
    quality_source_token = (
        r"(?:manifest|FFprobe|SPS|picture|idet|FFmpeg sample)"
    )
    quality_source_group = (
        rf"\[({quality_source_token}"
        rf"(?:,\s*{quality_source_token})*)\]"
    )

    def _color_quality_source_group(match):
        parts = [
            part.strip()
            for part in match.group(1).split(",")
        ]
        rendered = [
            (
                part
                if part == "manifest"
                else f"{SOFT_CYAN}{part}{RESET}"
            )
            for part in parts
        ]
        return "[" + ", ".join(rendered) + "]"

    colored = re.sub(
        quality_source_group,
        _color_quality_source_group,
        colored,
    )

    # Interlaced scan is decision-relevant and uncommon enough to call out.
    # Progressive scan stays in the normal quality formatting.
    colored = re.sub(
        r"(?<![A-Za-z0-9_.])(\d+(?:\.\d+)?i)(?![A-Za-z0-9_])",
        lambda match: f"{SOFT_CYAN}{match.group(1)}{RESET}",
        colored,
    )

    # Missing individual quality values are explicit rather than silently
    # omitted. Highlight only the UNKNOWN token, using the existing warning
    # yellow, while keeping the Resolution | FPS | Bitrate structure intact.
    for quality_unknown_phrase in (
        "resolution UNKNOWN",
        "fps UNKNOWN",
        "bitrate UNKNOWN",
        "P/I UNKNOWN",
    ):
        colored = colored.replace(
            quality_unknown_phrase,
            quality_unknown_phrase.replace(
                "UNKNOWN",
                f"{YELLOW}UNKNOWN{RESET}",
            ),
        )

    # Rare but decision-relevant facts should stand out even on otherwise
    # healthy/usable candidates.
    if "[ON]" in line:
        colored = colored.replace(
            "expires unknown",
            f"{YELLOW}expires unknown{RESET}",
            1,
        )
        colored = colored.replace(
            "not selected: expiry unknown",
            f"not selected: {YELLOW}expiry unknown{RESET}",
            1,
        )
        colored = colored.replace(
            "unknown-expiry source preferred",
            f"{YELLOW}unknown-expiry source preferred{RESET}",
            1,
        )

        for phrase in (
            f"less than {int(NM3U8DL_NEW_SOURCE_MIN_REMAINING_MIN)} min remaining",
            "less preferred match",
        ):
            colored = colored.replace(
                phrase,
                f"{SOFT_CYAN}{phrase}{RESET}",
                1,
            )

    manual_reject_phrase = (
        "manually rejected feed signature during this recording"
    )
    colored = colored.replace(
        manual_reject_phrase,
        f"{SOFT_CYAN}{manual_reject_phrase}{RESET}",
        1,
    )

    # Problem counts are uncommon in a healthy playlist summary, so surface
    # them without coloring the routine OK/working counts.
    if "PLAYLIST" in line:
        colored = re.sub(
            r"\b(\d+ (?:not working|excluded))\b",
            lambda match: f"{RED}{match.group(1)}{RESET}",
            colored,
        )
        colored = re.sub(
            r"\b(\d+ (?:blocked|expired|expiry unknown|ignored))\b",
            lambda match: f"{YELLOW}{match.group(1)}{RESET}",
            colored,
        )

    # OFF classifications are one warning family. Standard availability
    # classifications stay yellow. DRM capability/key/inspection failures are
    # hard failures and stay red. Every explicit classification is expected
    # immediately after the row colon, regardless of playlist match mode.
    if "[OFF]" in line:
        off_classification = None

        for token in (
            "DRM UNSUPPORTED",
            "DRM KEY MISSING",
            "DRM CHECK FAILED",
        ):
            if f": {token} —" in line:
                off_classification = token
                colored = colored.replace(
                    token,
                    f"{RED}{token}{RESET}",
                    1,
                )
                break

        if (
            off_classification is None
            and ": EXCLUDED —" in line
        ):
            off_classification = "EXCLUDED"
            colored = colored.replace(
                "EXCLUDED",
                f"{EXCLUDED_COLOR}EXCLUDED{RESET}",
                1,
            )
            colored = colored.replace(
                "2 consecutive downloader failures",
                f"{RED}2 consecutive downloader failures{RESET}",
                1,
            )

        for token in (
            "EXPIRY UNKNOWN",
            "EXPIRED",
            "BLOCKED",
            "IGNORED",
        ):
            if off_classification is not None:
                break
            if (
                f": {token} —" in line
            ):
                off_classification = token
                colored = colored.replace(
                    token,
                    f"{YELLOW}{token}{RESET}",
                    1,
                )
                break

        if off_classification is None:
            off_detail_match = re.search(
                r"\[OFF\]\s*:\s*(.+)$",
                line,
            )
            if off_detail_match:
                off_detail = off_detail_match.group(1)
                if event_candidate_line:
                    event_name_separator = off_detail.find(" — ")
                    if event_name_separator >= 0:
                        off_detail = off_detail[event_name_separator + 3:].strip()
                reason_separator = off_detail.find(" — ")
                if reason_separator >= 0:
                    failure_reason = off_detail[reason_separator + 3:].strip()
                    if failure_reason:
                        colored = colored.replace(
                            failure_reason,
                            f"{RED}{failure_reason}{RESET}",
                            1,
                        )
        else:
            # BLOCKED rows can still contain a concrete HTTP failure; keep that
            # concrete transport/auth fact red while the BLOCKED state is yellow.
            colored = re.sub(
                r"\bHTTP \d{3} [^—]+?(?=\s+—|$)",
                lambda match: f"{RED}{match.group(0).strip()}{RESET}",
                colored,
                count=1,
            )

    # Expired candidates keep the concrete expired timestamp highlighted.
    if "[OFF]" in line and ": EXPIRED —" in line:
        colored = re.sub(
            r"\bexpired \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\b",
            lambda match: f"{YELLOW}{match.group(0)}{RESET}",
            colored,
            count=1,
        )

    # Expiry-unknown candidates have no timestamp, so emphasize the reason
    # that makes the candidate unusable instead.
    if "[OFF]" in line and ": EXPIRY UNKNOWN —" in line:
        colored = colored.replace(
            "no recognizable authorization expiry",
            f"{YELLOW}no recognizable authorization expiry{RESET}",
            1,
        )
    colored = colored.replace(
        "NO MATCH",
        f"{GRAY}NO MATCH{RESET}",
    )
    colored = colored.replace(
        "UNUSABLE",
        f"{YELLOW}UNUSABLE{RESET}",
    )
    colored = colored.replace(
        "ACCESS BLOCKED",
        f"{RED}ACCESS BLOCKED{RESET}",
    )
    colored = colored.replace(
        "FETCH ERROR",
        f"{RED}FETCH ERROR{RESET}",
    )

    # Playlist fetch/generic errors are actionable failures. Keep the URL
    # neutral, but make both the failure class and concrete reason red.
    playlist_error = (
        "PLAYLIST" in line
        and (
            "FETCH ERROR" in line
            or ("ERROR on" in line and "FETCH ERROR" not in line)
        )
    )
    if playlist_error:
        if "ERROR on" in line and "FETCH ERROR" not in line:
            colored = colored.replace(
                "ERROR",
                f"{RED}ERROR{RESET}",
                1,
            )

        playlist_error_match = re.search(
            r"\s—\s(.+)$",
            line,
        )
        if playlist_error_match:
            reason = playlist_error_match.group(1)
            colored = colored.replace(
                reason,
                f"{RED}{reason}{RESET}",
                1,
            )
    colored = re.sub(
        r"PLAYLIST WARNING\s*:",
        lambda match: f"{RED}{match.group(0)}{RESET}",
        colored,
    )
    colored = colored.replace(
        "latest expired",
        f"{YELLOW}latest expired{RESET}",
    )
    colored = colored.replace(
        "(expired)",
        f"{YELLOW}(expired){RESET}",
    )
    colored = colored.replace(
        "(valid)",
        f"{GREEN}(valid){RESET}",
    )

    # Highlight the important details of the source we actually chose.
    for label in (
        "Matched entry           :",
        "Selected quality        :",
        "Authorization expires   :",
        "Time remaining          :",
        "Running renewal target  :",
        "Checked at              :",
        "Running authorization   :",
        "Best candidate          :",
        "Best candidate quality  :",
        "Best candidate expires  :",
        "Retained replacement    :",
        "Replacement quality     :",
        "Replacement expires     :",
        "Replacement status      :",
        "Planned rollover        :",
        "Decision                :",
        "Next playlist check     :",
    ):
        colored = colored.replace(
            label,
            f"{CYAN}{label}{RESET}",
        )

    # ---------------------------------------------------------
    # Recovery / health
    # ---------------------------------------------------------
    colored = colored.replace(
        "GOOD_BEEP",
        f"{GREEN}GOOD_BEEP{RESET}",
    )
    colored = colored.replace(
        "ALARM_START",
        f"{RED}ALARM_START{RESET}",
    )
    colored = colored.replace(
        "ALARM_ACK",
        f"{YELLOW}ALARM_ACK{RESET}",
    )

    return colored

def log(*args, level="INFO", **print_kwargs):
    global TERMINAL_LAST_OUTPUT_WAS_BLANK

    activity_id = print_kwargs.pop("activity_id", None)
    effective_activity_id = str(
        activity_id or get_terminal_activity_id()
    )

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = " ".join(str(a) for a in args)
    output_is_blank = (msg == "")

    # Plain version is authoritative and is what gets saved.
    line = f"{ts} [{level}] {msg}"

    # Only the interactive terminal gets ANSI coloring.
    terminal_line = colorize_terminal_log_line(line)

    with CONSOLE_OUTPUT_LOCK:
        if CONSOLE_INPUT_ACTIVE:
            # Keep recording/monitoring/log capture fully active, but defer the
            # terminal presentation until E/D/Y/N entry is finished. Preserve
            # the originating activity and blank-row state so deferred output
            # keeps the same activity-based spacing when it is finally displayed.
            termcap_write(line)
            CONSOLE_DEFERRED_TERMINAL_LINES.append(
                (effective_activity_id, terminal_line, output_is_blank)
            )
            return

        _prepare_terminal_activity(
            effective_activity_id,
            upcoming_output_is_blank=output_is_blank,
        )

        if output_is_blank and TERMINAL_LAST_OUTPUT_WAS_BLANK:
            return

        clear_progress_line()
        print(terminal_line, **print_kwargs)
        termcap_write(line)
        TERMINAL_LAST_OUTPUT_WAS_BLANK = output_is_blank


def _is_timeout_exception(error) -> bool:
    return source_transport.is_timeout_exception(error)


def _is_nm3u8dl_retryable_http_get_error(error: Exception) -> bool:
    return source_transport.is_retryable_http_get_error(error)


def _nm3u8dl_http_retry_delay_sec(error: Exception, retry_number: int) -> float:
    return source_transport.http_retry_delay_sec(error, retry_number)


def _run_nm3u8dl_retryable_http_get(
    operation: Callable[[], object],
    *,
    stop_requested: Optional[Callable[[], bool]] = None,
):
    return source_transport.run_retryable_http_get(
        operation,
        stop_requested=stop_requested,
    )


def _format_timeout_source(source) -> str:
    """Show a useful host/path without exposing signed query strings."""
    text = str(source or "").strip()
    if not text:
        return ""

    try:
        parsed = urlparse(text)
        if parsed.scheme.lower() in ("http", "https") and parsed.netloc:
            return f"{parsed.netloc}{parsed.path or '/'}"
    except Exception:
        pass

    return text


def _format_timeout_playlist_source(source) -> str:
    """Compact playlist identity for timeout attribution."""
    text = str(source or "").strip()
    if not text:
        return ""

    try:
        parsed = urlparse(text)
        host = str(parsed.netloc or "").strip()
        path = str(parsed.path or "").strip()

        # GitHub playlist URLs are extremely long. The filename is the useful
        # row identity in the terminal table and keeps timeout lines readable.
        if host.casefold() == "raw.githubusercontent.com":
            filename = path.rstrip("/").rsplit("/", 1)[-1]
            if filename:
                return filename

        if host:
            return f"{host}{path or '/'}"
    except Exception:
        pass

    return _format_timeout_source(text)


def _format_candidate_timeout_route(playlist_urls, stream_url: str) -> str:
    """Show which playlist row(s) feed one shared candidate probe."""
    playlist_labels = []

    for playlist_url in playlist_urls or []:
        label = _format_timeout_playlist_source(playlist_url)
        if label and label not in playlist_labels:
            playlist_labels.append(label)

    if len(playlist_labels) == 1:
        playlist_text = playlist_labels[0]
    elif playlist_labels:
        playlist_text = "{" + ", ".join(playlist_labels) + "}"
    else:
        playlist_text = ""

    stream_text = _format_timeout_source(stream_url)

    if playlist_text and stream_text:
        return f"{playlist_text} > {stream_text}"

    return playlist_text or stream_text


def log_timeout_event(
    tool: str,
    timeout_sec,
    *,
    context: str = "",
    source: str = "",
):
    """Emit one compact diagnostic timeout line without changing behavior."""
    parts = [
        "TIMEOUT",
        str(tool or "unknown").strip() or "unknown",
    ]

    if timeout_sec is not None:
        try:
            parts.append(f"{float(timeout_sec):g}s")
        except Exception:
            parts.append(f"{str(timeout_sec).strip()}s")

    context_text = " ".join(str(context or "").split())
    if context_text:
        parts.append(context_text)

    source_text = _format_timeout_source(source)
    if source_text:
        parts.append(source_text)

    # Timeout diagnostics belong to whatever terminal activity is already
    # visible. Do not create an activity transition just for the timeout; that
    # would add blank separator rows before/after the diagnostic line.
    with CONSOLE_OUTPUT_LOCK:
        current_activity_id = str(
            TERMINAL_LAST_ACTIVITY_ID
            or get_terminal_activity_id()
        )
        log(
            " | ".join(parts),
            level="WARN",
            activity_id=current_activity_id,
        )


def log_timeout_exception(
    error,
    tool: str,
    timeout_sec,
    *,
    context: str = "",
    source: str = "",
) -> bool:
    """Log one timeout once, even if the same exception propagates upward."""
    if not _is_timeout_exception(error):
        return False

    if getattr(error, "_recorder_timeout_logged", False):
        return True

    log_timeout_event(
        tool,
        timeout_sec,
        context=context,
        source=source,
    )

    try:
        setattr(error, "_recorder_timeout_logged", True)
    except Exception:
        pass

    return True


def log_process_shutdown_timeout(tool: str, timeout_sec):
    """Compact shared message for a process that must be killed after shutdown timeout."""
    log_timeout_event(
        tool,
        timeout_sec,
        context="process shutdown; killing",
    )


def join_thread_with_timeout_logging(
    thread: threading.Thread,
    timeout_sec,
    *,
    context: str,
):
    """Preserve Thread.join behavior while exposing an actual join timeout."""
    thread.join(timeout=timeout_sec)

    if thread.is_alive():
        log_timeout_event(
            "thread",
            timeout_sec,
            context=context,
        )


def log_run_start_banner(state: RecorderState, engine_label: str):
    """Show one numbered visual boundary for each actual worker run attempt."""
    state.run_attempt_index += 1
    log("")
    log(
        f"==================== RUN {state.run_attempt_index} START — "
        f"{engine_label} ===================="
    )


def ensure_chunks_dir():
    os.makedirs(CHUNKS_DIR, exist_ok=True)

def init_termcap():
    """Create/overwrite the temp terminal capture log inside CHUNKS_DIR."""
    global TERMCAP_PATH
    ensure_chunks_dir()
    TERMCAP_PATH = os.path.join(CHUNKS_DIR, "_terminal_capture.tmp.log")
    try:
        with open(TERMCAP_PATH, "w", encoding="utf-8", errors="replace") as _:
            pass
    except Exception as e:
        try:
            print(f"[WARN] Terminal capture failed: {e}", file=sys.stderr)
        except Exception:
            pass

def termcap_write(full_terminal_line: str):
    """Write EXACTLY what was printed to terminal (already timestamped)."""
    if not TERMCAP_PATH or not full_terminal_line:
        return
    try:
        with open(TERMCAP_PATH, "a", encoding="utf-8", errors="replace") as f:
            f.write(full_terminal_line if full_terminal_line.endswith("\n") else full_terminal_line + "\n")
    except Exception as e:
        try:
            print(f"[WARN] Terminal Write failed: {e}", file=sys.stderr)
        except Exception:
            pass

def append_termcap_to_summarylog(summary_path: str) -> int:
    if not TERMCAP_PATH or not os.path.exists(TERMCAP_PATH):
        return 0

    appended = 0
    with open(summary_path, "a", encoding="utf-8", errors="replace") as out:
        out.write("\n\n")
        out.write("====================================================================\n")
        out.write("TERMINAL OUTPUT (captured during run)\n")
        out.write("====================================================================\n")

        with open(TERMCAP_PATH, "r", encoding="utf-8", errors="replace") as inp:
            while True:
                buf = inp.read(1024 * 1024)
                if not buf:
                    break
                out.write(buf)
                appended += len(buf)

    return appended
        


# Raw external-tool diagnostics. This capture is deliberately separate from
# terminal capture: live terminal behavior stays unchanged, while the final log
# can include the complete black-box output from engines and external probes.
RAW_EXTERNAL_LOCK = threading.RLock()
RAW_EXTERNAL_COUNTER = 0
_RAW_ANSI_CSI_RE = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
_RAW_ANSI_OSC_RE = re.compile(r"\x1B\][^\x07]*(?:\x07|\x1B\\)")
_RAW_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def _raw_external_timestamp(ts: Optional[float] = None) -> str:
    dt = datetime.fromtimestamp(time.time() if ts is None else float(ts))
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _clean_raw_external_text(value) -> str:
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    text = str(value)
    text = _RAW_ANSI_OSC_RE.sub("", text)
    text = _RAW_ANSI_CSI_RE.sub("", text)
    return _RAW_CONTROL_RE.sub("", text)


def init_raw_external_capture():
    """Create/overwrite the temp black-box output capture inside CHUNKS_DIR."""
    global RAW_EXTERNAL_PATH, RAW_EXTERNAL_COUNTER
    ensure_chunks_dir()
    RAW_EXTERNAL_PATH = os.path.join(CHUNKS_DIR, "_raw_external_output.tmp.log")
    RAW_EXTERNAL_COUNTER = 0
    try:
        with open(RAW_EXTERNAL_PATH, "w", encoding="utf-8", errors="replace"):
            pass
    except Exception:
        # Diagnostics must never alter recorder behavior or terminal output.
        RAW_EXTERNAL_PATH = None


def _raw_external_emit(line: str):
    if not RAW_EXTERNAL_PATH:
        return
    try:
        with RAW_EXTERNAL_LOCK:
            with open(RAW_EXTERNAL_PATH, "a", encoding="utf-8", errors="replace") as f:
                f.write(line if line.endswith("\n") else line + "\n")
    except Exception:
        # Raw diagnostics are best-effort and must remain behavior-neutral.
        pass


def raw_external_start(tool: str, context: str = ""):
    """Start one external black-box invocation and return its capture token."""
    global RAW_EXTERNAL_COUNTER
    if not RAW_EXTERNAL_PATH:
        return None

    with RAW_EXTERNAL_LOCK:
        RAW_EXTERNAL_COUNTER += 1
        invocation_id = f"{tool}#{RAW_EXTERNAL_COUNTER:03d}"

    token = {
        "id": invocation_id,
        "tool": tool,
        "context": str(context or "").strip(),
        "ended": False,
    }
    suffix = f" | {token['context']}" if token["context"] else ""
    _raw_external_emit(
        f"{_raw_external_timestamp()} --- START {invocation_id}{suffix} ---"
    )
    return token


def raw_external_write(invocation, text, stream: str = "output", timestamp: Optional[float] = None):
    """Append unfiltered external output, adding only time/tool identity and cleanup."""
    if invocation is None or text is None:
        return

    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="replace")
    else:
        text = str(text)

    if not text:
        return

    # A carriage return is a console redraw. Preserve every redraw as its own
    # readable diagnostic line instead of letting later output overwrite it.
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    parts = normalized.split("\n")
    if normalized.endswith("\n"):
        parts = parts[:-1]

    stream_name = str(stream or "output").strip()
    stream_suffix = "" if stream_name == "output" else f" {stream_name}"
    ts_text = _raw_external_timestamp(timestamp)

    for part in parts:
        cleaned = _clean_raw_external_text(part)
        _raw_external_emit(
            f"{ts_text} [{invocation['id']}{stream_suffix}] {cleaned}"
        )


def raw_external_end(invocation, returncode=None, status: str = ""):
    """Finish one external invocation; safe to call more than once."""
    if invocation is None:
        return

    with RAW_EXTERNAL_LOCK:
        if invocation.get("ended"):
            return
        invocation["ended"] = True

    if status:
        ending = str(status)
    elif returncode is None:
        ending = "returned control"
    else:
        ending = f"exit {returncode}"

    _raw_external_emit(
        f"{_raw_external_timestamp()} --- END {invocation['id']} | {ending} ---"
    )


def run_external_capture(cmd, *, raw_tool: str, raw_context: str = "", **kwargs):
    """subprocess.run wrapper that adds raw diagnostics without changing live output."""
    invocation = raw_external_start(raw_tool, raw_context)
    raw_external_write(invocation, repr(cmd), "command")
    raw_external_write(
        invocation,
        f"timeout={kwargs.get('timeout')!r} cwd={kwargs.get('cwd')!r}",
        "execution",
    )
    try:
        result = subprocess.run(cmd, **kwargs)
    except subprocess.TimeoutExpired as exc:
        raw_external_write(invocation, getattr(exc, "stdout", None), "stdout")
        raw_external_write(invocation, getattr(exc, "stderr", None), "stderr")
        raw_external_end(invocation, status="timeout")
        log_timeout_exception(
            exc,
            raw_tool,
            kwargs.get("timeout"),
            context=raw_context,
        )
        raise
    except Exception as exc:
        raw_external_end(invocation, status=f"exception {type(exc).__name__}")
        raise

    raw_external_write(invocation, getattr(result, "stdout", None), "stdout")
    raw_external_write(invocation, getattr(result, "stderr", None), "stderr")
    raw_external_end(invocation, returncode=result.returncode)
    return result


def check_output_external(cmd, *, raw_tool: str, raw_context: str = "", **kwargs):
    """subprocess.check_output wrapper; preserves its existing stdout semantics."""
    invocation = raw_external_start(raw_tool, raw_context)
    raw_external_write(invocation, repr(cmd), "command")
    raw_external_write(
        invocation,
        f"timeout={kwargs.get('timeout')!r} cwd={kwargs.get('cwd')!r}",
        "execution",
    )
    try:
        output = subprocess.check_output(cmd, **kwargs)
    except subprocess.CalledProcessError as exc:
        raw_external_write(invocation, getattr(exc, "output", None), "stdout")
        raw_external_end(invocation, returncode=exc.returncode)
        raise
    except subprocess.TimeoutExpired as exc:
        raw_external_write(invocation, getattr(exc, "output", None), "stdout")
        raw_external_end(invocation, status="timeout")
        log_timeout_exception(
            exc,
            raw_tool,
            kwargs.get("timeout"),
            context=raw_context,
        )
        raise
    except Exception as exc:
        raw_external_end(invocation, status=f"exception {type(exc).__name__}")
        raise

    raw_external_write(invocation, output, "stdout")
    raw_external_end(invocation, returncode=0)
    return output


def append_raw_external_to_summarylog(summary_path: str) -> int:
    """Append the black-box diagnostic dump as the absolute final log section."""
    if not RAW_EXTERNAL_PATH or not os.path.exists(RAW_EXTERNAL_PATH):
        return 0

    appended = 0
    try:
        with open(summary_path, "a", encoding="utf-8", errors="replace") as out:
            out.write("\n\n")
            out.write("====================================================================\n")
            out.write("RAW DOWNLOADER OUTPUT (engines + external probes)\n")
            out.write("====================================================================\n")

            with open(RAW_EXTERNAL_PATH, "r", encoding="utf-8", errors="replace") as inp:
                while True:
                    buf = inp.read(1024 * 1024)
                    if not buf:
                        break
                    out.write(buf)
                    appended += len(buf)
    except Exception:
        # Diagnostics must never change recorder behavior or terminal output.
        return 0

    return appended


def _clear_sound_snooze(state: RecorderState):
    runtime_sound.clear_sound_snooze(state)
    state.sound_snooze_run_attempt = None


def is_sound_snoozed(state: Optional[RecorderState], now_ts: Optional[float] = None) -> bool:
    """Return whether recorder-owned sounds are currently snoozed."""
    if state is None:
        return False

    mode = getattr(state, "sound_snooze_mode", None)
    timed_state = runtime_sound.timed_sound_snoozed(
        state,
        now_ts=now_ts,
    )
    if timed_state is not None:
        if timed_state:
            return True
        state.sound_snooze_run_attempt = None
        log("SOUND SNOOZE ENDED — 15-minute snooze expired; sound restored")
        return False

    if mode == "run":
        snoozed_run = getattr(state, "sound_snooze_run_attempt", None)
        current_run = int(getattr(state, "run_attempt_index", 0) or 0)
        if snoozed_run is not None and current_run == int(snoozed_run):
            return True
        _clear_sound_snooze(state)
        return False

    if mode == "recording":
        return True

    if mode is not None:
        _clear_sound_snooze(state)
    return False


def get_sound_state_text(state: RecorderState) -> str:
    if not is_sound_snoozed(state):
        return "ON"

    mode = getattr(state, "sound_snooze_mode", None)
    if mode == "timed":
        until_ts = float(getattr(state, "sound_snooze_until_ts", 0.0) or 0.0)
        remaining = max(0.0, until_ts - time.time())
        return f"SNOOZED — {fmt_hms(remaining)} remaining"
    if mode == "run":
        return "SNOOZED — current RUN"
    if mode == "recording":
        return "SNOOZED — full recording"
    return "ON"


def finish_sound_snooze_for_completed_run(state: RecorderState):
    """End RUN-scoped snooze as soon as that worker attempt returns."""
    if getattr(state, "sound_snooze_mode", None) != "run":
        return

    snoozed_run = getattr(state, "sound_snooze_run_attempt", None)
    current_run = int(getattr(state, "run_attempt_index", 0) or 0)
    if snoozed_run is not None and current_run == int(snoozed_run):
        _clear_sound_snooze(state)
        log(f"SOUND SNOOZE ENDED — RUN {current_run} completed; sound restored")


def beep_bad(state: Optional[RecorderState] = None):
    # Three distinct "bad" status sounds
    if is_sound_snoozed(state):
        return
    if sys.platform == "darwin":
        for _ in range(3):
            if is_sound_snoozed(state):
                return
            subprocess.run(
                ["afplay", "/System/Library/Sounds/Basso.aiff"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            time.sleep(0.4)

    elif winsound is not None:
        freq = 500
        dur = 200
        gap = 600
        for _ in range(3):
            if is_sound_snoozed(state):
                return
            winsound.Beep(freq, dur)
            time.sleep(gap / 1000.0)


def beep_good(state: Optional[RecorderState] = None):
    # One distinct "good" status sound
    if is_sound_snoozed(state):
        return
    if sys.platform == "darwin":
        subprocess.run(
            ["afplay", "/System/Library/Sounds/Glass.aiff"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    elif winsound is not None:
        winsound.Beep(700, 600)
    
def log_good_beep(reason: str):
    # One GOOD_BEEP is its own activity. The shared renderer adds separation
    # before it and again when whatever activity was interrupted resumes.
    activity_id = new_terminal_activity("good_beep")
    log(
        f"GOOD_BEEP reason={reason}",
        activity_id=activity_id,
    )
    
def maybe_trigger_good_beep(state: RecorderState, engine_label: str, is_healthy: bool, had_alarm_incident: bool, notify):
    count_attr = f"{engine_label}_healthy_consec_count"
    beeped_attr = f"{engine_label}_beeped_this_healthy_streak"
    if is_healthy:
        count = getattr(state, count_attr, 0) + 1
        setattr(state, count_attr, count)
        if count == GOOD_BEEP_HEALTHY_REQUIRED and not getattr(state, beeped_attr, False):
            reason = "recovery_healthy" if had_alarm_incident else "healthy_check"
            if notify:
                notify(f"good_beep:{reason}")
            setattr(state, beeped_attr, True)
    else:
        setattr(state, count_attr, 0)
        setattr(state, beeped_attr, False)

def reset_good_beep_state(state: RecorderState, engine_label: str):
    count_attr = f"{engine_label}_healthy_consec_count"
    beeped_attr = f"{engine_label}_beeped_this_healthy_streak"
    setattr(state, count_attr, 0)
    setattr(state, beeped_attr, False)
    

def sleep_with_interrupts(state: RecorderState, total_seconds: float, step_seconds: float = 0.2):
    end_ts = time.time() + max(0.0, total_seconds)
    while time.time() < end_ts:
        if getattr(state, "stop_flag", False):
            break
        if recording_deadline_reached(state):
            break
        if getattr(state, "alarm_ack_requested", False):
            break
        if getattr(state, "nm3u8dl_ack_requested", False):
            break
        remaining = end_ts - time.time()
        if remaining <= 0:
            break
        time.sleep(min(step_seconds, remaining))

def is_alarm_active(state: RecorderState) -> bool:
    if shared_alarm_is_active(state):
        return True
    return any(
        (
            getattr(state, "nm3u8dl_alarm_active", False),
            getattr(state, "ffmpeg_alarm_active", False),
            getattr(state, "ffmpeg_off_alarm_active", False),
        )
    )
    
def alarm_critical(state):
    """
    CRITICAL ALARM (non-blocking, no popup).
    Starts the shared alarm sound and returns immediately.
    """
    # Start (or keep) the shared alarm sound.
    shared_alarm_start(state, alarm_type="critical", incident=None)

def _nm3u8dl_key_listener_loop(state, stop_event: threading.Event):

    def handle_key(ch):
        if handle_runtime_control_key(state, ch):
            return

        if ch in ("a", "A"):
            t_alarm = getattr(state, "shared_alarm_thread", None)
            alarm_active = (
                (t_alarm is not None and getattr(t_alarm, "is_alive", lambda: False)())
                or getattr(state, "ffmpeg_off_alarm_active", False)
                or getattr(state, "ffmpeg_alarm_active", False)
                or getattr(state, "nm3u8dl_alarm_active", False)
            )

            if alarm_active:
                # Access/VPN alarms can occur while N_m3u8DL is actively recording,
                # so ACK them directly from the key listener instead of waiting for
                # the boss loop or a playlist-monitor wake-up.
                if (
                    getattr(state, "shared_alarm_type", None)
                    == "playlist_access_block"
                ):
                    alarm_activity_id = getattr(
                        state,
                        "shared_alarm_activity_id",
                        None,
                    )
                    log(
                        "ALARM_ACK",
                        activity_id=alarm_activity_id,
                    )
                    shared_alarm_stop(state)
                    state.alarm_ack_requested = False
                    state.nm3u8dl_access_block_alarm_acknowledged = True
                    log(
                        "Access/VPN status → ACKNOWLEDGED / SILENT — "
                        "alarm silenced; recording continues; blocked sources "
                        f"will be rechecked every "
                        f"{int(NM3U8DL_PLAYLIST_ACCESS_CHECK_INTERVAL_SEC)} seconds.",
                        level="WARN",
                        activity_id=alarm_activity_id,
                    )
                elif getattr(state, "nm3u8dl_alarm_active", False):
                    acknowledge_nm3u8dl_stall_alarm(state)
                else:
                    state.alarm_ack_requested = True

        elif ch in ("r", "R"):
            # R is N_m3u8DL-only.
            if getattr(state, "nm3u8dl_run_active", False):
                if not getattr(state, "nm3u8dl_alarm_active", False):
                    state.nm3u8dl_manual_restart_requested = True

    # Windows
    if msvcrt is not None:
        while not stop_event.is_set():
            try:
                if msvcrt.kbhit():
                    handle_key(msvcrt.getwch())
            except Exception:
                pass

            time.sleep(0.05)

        return

    # macOS
    if sys.platform == "darwin":
        try:
            import select
            import termios
            import tty
            import atexit

            fd = sys.stdin.fileno()
            old_settings = termios.tcgetattr(fd)

        except Exception as exc:
            log(f"Mac keyboard listener unavailable: {exc}", level="WARN")
            return

        def restore_terminal():
            try:
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
            except Exception:
                pass

        # Safety: restore normal terminal behavior when Python exits.
        atexit.register(restore_terminal)

        try:
            # Discard any keystrokes entered while a scheduled recording was waiting.
            termios.tcflush(fd, termios.TCIFLUSH)
            tty.setcbreak(fd)

            while not stop_event.is_set():
                try:
                    ready, _, _ = select.select([sys.stdin], [], [], 0.05)

                    if ready:
                        ch = sys.stdin.read(1)
                        handle_key(ch)

                except Exception:
                    time.sleep(0.05)

        finally:
            restore_terminal()

def ensure_global_key_listener(state):
    # Start once per script run.
    if getattr(state, "nm3u8dl_key_listener_started", False):
        return

    # Windows can retain keystrokes entered while a scheduled recording is
    # waiting to start. Discard anything already buffered before enabling the
    # runtime controls so stale input cannot be replayed as recorder commands.
    if msvcrt is not None:
        try:
            while msvcrt.kbhit():
                msvcrt.getwch()
        except Exception:
            pass

    stop_evt = threading.Event()
    t = threading.Thread(
        target=_nm3u8dl_key_listener_loop,
        args=(state, stop_evt),
        daemon=True,
        name="global_key_listener",
    )
    state.nm3u8dl_key_listener_started = True
    state.nm3u8dl_key_listener_stop_event = stop_evt
    t.start()
    state.nm3u8dl_key_listener_thread = t

def ensure_nm3u8dl_key_listener(state):
    # Back-compat alias (nm3u8dl historically owned this listener).
    ensure_global_key_listener(state)

def _alarm_beep_fallback_loop(
    stop_event: threading.Event,
    state: RecorderState,
    activity_id: Optional[str] = None,
):
    # Existing v15 behavior: continuous winsound.Beep loop. Sound snooze gates
    # playback only; this alarm thread remains alive so sound can resume later.
    log(
        "ALARM: wav/mp3 unavailable; using beep fallback loop",
        activity_id=activity_id,
    )
    while not stop_event.is_set():
        if is_sound_snoozed(state):
            stop_event.wait(0.1)
            continue
        try:
            winsound.Beep(int(NM3U8DL_ALARM_BEEP_HZ), int(NM3U8DL_ALARM_BEEP_MS))
        except Exception:
            try:
                winsound.MessageBeep()
            except Exception:
                pass
        stop_event.wait(float(NM3U8DL_ALARM_BEEP_GAP_SEC))

def _resolve_alarm_sound_path() -> Optional[str]:
    # ALARM_SOUND_FILENAME may be absolute or relative to this script folder.
    try:
        if os.path.isabs(ALARM_SOUND_FILENAME):
            p = ALARM_SOUND_FILENAME
        else:
            p = os.path.join(os.path.dirname(os.path.abspath(__file__)), ALARM_SOUND_FILENAME)
    except Exception:
        return None
    return p

def _shared_alarm_sound_loop(
    stop_event: threading.Event,
    state: RecorderState,
    activity_id: Optional[str] = None,
):
    # Primary: loop WAV alarm file. The alarm remains logically active while
    # snoozed; only physical playback is paused.
    sound_path = _resolve_alarm_sound_path()

    if sound_path and os.path.isfile(sound_path):

        # macOS
        if sys.platform == "darwin":
            while not stop_event.is_set():
                if is_sound_snoozed(state):
                    stop_event.wait(0.1)
                    continue

                try:
                    proc = subprocess.Popen(
                        ["afplay", sound_path],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )

                    while proc.poll() is None:
                        if stop_event.wait(0.1):
                            proc.terminate()
                            try:
                                proc.wait(timeout=2)
                            except subprocess.TimeoutExpired:
                                log_process_shutdown_timeout("afplay", 2)
                                proc.kill()
                            return

                        if is_sound_snoozed(state):
                            proc.terminate()
                            try:
                                proc.wait(timeout=2)
                            except subprocess.TimeoutExpired:
                                log_process_shutdown_timeout("afplay", 2)
                                proc.kill()
                            break

                except Exception as exc:
                    log(
                        f"ALARM: failed to play {sound_path} with afplay ({exc})",
                        activity_id=activity_id,
                    )
                    break

            return

        # Windows
        if winsound is not None:
            sound_playing = False
            try:
                while not stop_event.is_set():
                    snoozed = is_sound_snoozed(state)

                    if snoozed and sound_playing:
                        winsound.PlaySound(None, winsound.SND_PURGE)
                        sound_playing = False
                    elif not snoozed and not sound_playing:
                        winsound.PlaySound(
                            sound_path,
                            winsound.SND_FILENAME | winsound.SND_ASYNC | winsound.SND_LOOP
                        )
                        sound_playing = True

                    stop_event.wait(0.1)

                if sound_playing:
                    winsound.PlaySound(None, winsound.SND_PURGE)
                return
            except Exception as exc:
                log(
                    f"ALARM: failed to play {sound_path}; falling back to beep loop ({exc})",
                    activity_id=activity_id,
                )
                try:
                    winsound.PlaySound(None, winsound.SND_PURGE)
                except Exception:
                    pass

    else:
        log(
            f"ALARM: sound file missing at {sound_path}; falling back to beep loop",
            activity_id=activity_id,
        )

    _alarm_beep_fallback_loop(
        stop_event,
        state,
        activity_id=activity_id,
    )


def _auto_silence_shared_alarm(state, alarm_type: str):
    """Stop a continuous attention alarm after the global five-minute cap."""
    if (
        getattr(state, "shared_alarm_type", None) != alarm_type
        or not shared_alarm_is_active(state)
    ):
        return

    set_terminal_activity_context(
        getattr(state, "shared_alarm_activity_id", None)
    )

    log(
        f"ALARM_TIMEOUT type={alarm_type} "
        f"after={int(CONTINUOUS_ALARM_MAX_SEC)}s",
        level="WARN",
    )

    if alarm_type == "nm3u8dl_stall":
        state.nm3u8dl_alarm_active = False
        state.nm3u8dl_post_ack_silent = True
        state.nm3u8dl_ack_requested = False
        log("ALARM_STALL_STOP reason=timeout", level="WARN")
    elif alarm_type == "ffmpeg":
        state.ffmpeg_alarm_active = False
        state.ffmpeg_post_ack_silent = True
        state.ffmpeg_post_ack_silent_type = "ffmpeg"
        log("ALARM_STALL_STOP reason=timeout", level="WARN")
    elif alarm_type == "ffmpeg_off":
        state.ffmpeg_off_alarm_active = False
        state.ffmpeg_off_post_ack_silent = True
        log("ALARM_OFF_STOP reason=timeout", level="WARN")
    elif alarm_type == "playlist_connectivity":
        state.nm3u8dl_playlist_connectivity_alarm_silenced = True
        state.alarm_ack_requested = False
        log("ALARM_PLAYLIST_CONNECTIVITY_STOP reason=timeout", level="WARN")
    elif alarm_type == "playlist_access_block":
        state.nm3u8dl_access_block_alarm_acknowledged = True
        state.alarm_ack_requested = False
        log("ALARM_PLAYLIST_ACCESS_BLOCK_STOP reason=timeout", level="WARN")
    elif alarm_type == "playlist_failover":
        state.nm3u8dl_failover_alarm_silenced = True
        state.alarm_ack_requested = False
        log("ALARM_PLAYLIST_FAILOVER_STOP reason=timeout", level="WARN")

    shared_alarm_stop(state)


def shared_alarm_start(state, alarm_type: str, incident: Optional[int] = None):
    # ACK/timeout silences a continuing incident until its recovery rule clears it.
    if alarm_type == "ffmpeg" and getattr(state, "ffmpeg_post_ack_silent", False):
        return
    if alarm_type == "nm3u8dl_stall" and getattr(state, "nm3u8dl_post_ack_silent", False):
        return

    # Only one alarm sound at a time (shared across engines).
    t_existing = getattr(state, "shared_alarm_thread", None)
    if t_existing is not None and getattr(t_existing, "is_alive", lambda: False)():
        return

    state.shared_alarm_type = alarm_type
    # Keep the alarm attached to the activity that triggered it. ACK/timeout
    # messages can therefore resume the same logical block even from another
    # thread.
    state.shared_alarm_activity_id = get_terminal_activity_id()

    stop_evt = threading.Event()
    t = threading.Thread(
        target=_shared_alarm_sound_loop,
        args=(stop_evt, state, state.shared_alarm_activity_id),
        daemon=True,
        name="shared_alarm",
    )

    state.shared_alarm_stop_event = stop_evt
    state.shared_alarm_thread = t

    # Back-compat mirrors used elsewhere by the recorder.
    state.nm3u8dl_alarm_stop_event = stop_evt
    state.nm3u8dl_alarm_thread = t

    t.start()

    # Critical alarms already stop the recorder after ALARM_LINGER_SEC. All other
    # continuous attention alarms auto-silence after five minutes.
    if alarm_type != "critical":
        timer = threading.Timer(
            float(CONTINUOUS_ALARM_MAX_SEC),
            _auto_silence_shared_alarm,
            args=(state, alarm_type),
        )
        timer.daemon = True
        state.shared_alarm_timeout_timer = timer
        state.shared_alarm_deadline = time.monotonic() + float(CONTINUOUS_ALARM_MAX_SEC)
        timer.start()
    else:
        state.shared_alarm_timeout_timer = None
        state.shared_alarm_deadline = None

    if incident is None:
        log(
            f"ALARM_START type={alarm_type}",
            activity_id=state.shared_alarm_activity_id,
        )
    else:
        log(
            f"ALARM_START type={alarm_type} incident={incident}",
            activity_id=state.shared_alarm_activity_id,
        )
    log(
        "ALARM: press A to acknowledge",
        activity_id=state.shared_alarm_activity_id,
    )


def shared_alarm_stop(state):
    timer = getattr(state, "shared_alarm_timeout_timer", None)
    if timer is not None and timer is not threading.current_thread():
        try:
            timer.cancel()
        except Exception:
            pass

    stop_evt = getattr(state, "shared_alarm_stop_event", None)
    if stop_evt:
        try:
            stop_evt.set()
        except Exception:
            pass

    t = getattr(state, "shared_alarm_thread", None)
    if t and t.is_alive():
        try:
            join_thread_with_timeout_logging(
                t,
                1.0,
                context="shared alarm shutdown",
            )
        except Exception:
            pass

    state.shared_alarm_thread = None
    state.shared_alarm_stop_event = None
    state.shared_alarm_type = None
    state.shared_alarm_timeout_timer = None
    state.shared_alarm_deadline = None

    # Back-compat mirrors
    state.nm3u8dl_alarm_thread = None
    state.nm3u8dl_alarm_stop_event = None


def shared_alarm_is_active(state) -> bool:
    t_existing = getattr(state, "shared_alarm_thread", None)
    return t_existing is not None and getattr(t_existing, "is_alive", lambda: False)()


def nm3u8dl_alarm_start(state, stall_type: str, incident: int):
    # A confirmed stall event owns this alarm; it survives process replacement.
    shared_alarm_start(state, alarm_type="nm3u8dl_stall", incident=incident)
    state.nm3u8dl_alarm_active = (
        getattr(state, "shared_alarm_type", None) == "nm3u8dl_stall"
        and shared_alarm_is_active(state)
    )

    if state.nm3u8dl_alarm_active:
        state.nm3u8dl_had_alarm_incident = True
        log(f"ALARM_STALL_TYPE type={stall_type} incident={incident}")
    elif shared_alarm_is_active(state):
        # Another continuous alarm is already attracting attention. Do not queue
        # a second alarm that could restart later for the same stall incident.
        state.nm3u8dl_post_ack_silent = True
        log(
            "ALARM_STALL_SUPPRESSED reason=shared_alarm_already_active",
            level="WARN",
        )


def nm3u8dl_alarm_stop(state):
    state.nm3u8dl_alarm_active = False
    shared_alarm_stop(state)


def acknowledge_nm3u8dl_stall_alarm(state, reason: str = "ack") -> bool:
    alarm_type = getattr(state, "shared_alarm_type", None)
    if not (
        getattr(state, "nm3u8dl_alarm_active", False)
        or alarm_type == "nm3u8dl_stall"
    ):
        return False

    alarm_activity_id = getattr(
        state,
        "shared_alarm_activity_id",
        None,
    )

    if reason == "ack":
        log(
            "ALARM_ACK",
            activity_id=alarm_activity_id,
        )
    log(
        f"ALARM_STALL_STOP reason={reason}",
        activity_id=alarm_activity_id,
    )
    state.nm3u8dl_alarm_active = False
    state.nm3u8dl_post_ack_silent = True
    state.nm3u8dl_ack_requested = False
    state.alarm_ack_requested = False
    shared_alarm_stop(state)
    return True


def ffmpeg_alarm_start(state, alarm_type: str = "ffmpeg"):
    shared_alarm_start(state, alarm_type=alarm_type, incident=None)
    state.ffmpeg_alarm_active = (
        getattr(state, "shared_alarm_type", None) == alarm_type
        and shared_alarm_is_active(state)
    )

def ffmpeg_alarm_stop(state):
    state.ffmpeg_alarm_active = False
    shared_alarm_stop(state)

def ffmpeg_off_alarm_start(state):
    if shared_alarm_is_active(state):
        return
    shared_alarm_start(state, alarm_type="ffmpeg_off", incident=None)
    state.ffmpeg_off_alarm_active = (
        getattr(state, "shared_alarm_type", None) == "ffmpeg_off"
        and shared_alarm_is_active(state)
    )
    if state.ffmpeg_off_alarm_active:
        log("ALARM_OFF_START")

def ffmpeg_off_alarm_stop(state):
    state.ffmpeg_off_alarm_active = False
    shared_alarm_stop(state)
    
def make_signal_handler(state: RecorderState):
    def _handler(sig, frame):
        cancel_console_interaction_if_active(
            state,
            "Runtime entry cancelled by Ctrl-C.",
        )
        log("")
        log("Stopping by Ctrl-C...", level="WARN")
        state.stop_flag = True
        if state.nm3u8dl_stop_event is not None:
            state.nm3u8dl_stop_event.set()
    return _handler

def fmt_hms(seconds):
    seconds = int(seconds)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    else:
        return f"{m:02d}:{s:02d}"


def _format_minutes_value(value: Optional[float]) -> str:
    if value is None:
        return "until stopped"
    value = float(value)
    if abs(value - round(value)) < 1e-9:
        return f"{int(round(value))} minutes"
    return f"{value:.2f} minutes"


def get_recording_deadline(state: RecorderState) -> Optional[float]:
    lock = getattr(state, "timing_lock", None)
    if lock is None:
        return state.deadline_ts
    with lock:
        return state.deadline_ts


def recording_deadline_reached(state: RecorderState, now_ts: Optional[float] = None) -> bool:
    deadline = get_recording_deadline(state)
    if deadline is None:
        return False
    now = time.time() if now_ts is None else float(now_ts)
    return now >= deadline


def get_current_planned_duration_min(state: RecorderState) -> Optional[float]:
    deadline = get_recording_deadline(state)
    if deadline is None:
        return None
    return max(0.0, (float(deadline) - float(state.start_time)) / 60.0)


def get_runtime_adjustment_text(state: RecorderState) -> str:
    current = get_current_planned_duration_min(state)
    original = getattr(state, "original_duration_min", None)

    if original is None:
        return "none" if current is None else "set during recording"
    if current is None:
        return "changed to until stopped"

    delta = current - float(original)
    if abs(delta) < 1e-9:
        return "none"
    sign = "+" if delta > 0 else "-"
    return f"{sign}{_format_minutes_value(abs(delta))}"


def format_current_duration_for_log(state: RecorderState) -> str:
    return _format_minutes_value(get_current_planned_duration_min(state))


def append_runtime_summary_lines(lines: list, state: RecorderState):
    current = get_current_planned_duration_min(state)
    changed = int(getattr(state, "duration_change_count", 0) or 0) > 0

    if changed:
        lines.append(
            f"Original duration   : {_format_minutes_value(getattr(state, 'original_duration_min', None))}"
        )
        lines.append(f"Planned duration    : {_format_minutes_value(current)}")
        lines.append(f"Runtime adjustment  : {get_runtime_adjustment_text(state)}")
        if current is not None and current <= 1e-9:
            lines.append("Final action        : immediate stop confirmed by runtime control")
    else:
        lines.append(f"Planned duration    : {_format_minutes_value(current)}")

    deadline = get_recording_deadline(state)
    if deadline is not None:
        lines.append(
            "Planned end         : "
            + datetime.fromtimestamp(deadline).strftime("%Y-%m-%d %H:%M:%S")
        )


def _console_color(text: str, color: str) -> str:
    if not getattr(sys.stdout, "isatty", lambda: False)():
        return text
    codes = {
        "green": "\033[1;92m",
        "yellow": "\033[1;93m",
        "cyan": "\033[1;96m",
    }
    code = codes.get(color)
    if not code:
        return text
    return f"{code}{text}\033[0m"


def _console_write(text: str):
    with CONSOLE_OUTPUT_LOCK:
        sys.stdout.write(text)
        sys.stdout.flush()


def _console_write_colored(text: str, color: str):
    _console_write(_console_color(text, color))


def _console_print_lines(lines, color: Optional[str] = None, colored_indexes=None):
    global TERMINAL_LAST_OUTPUT_WAS_BLANK

    clear_progress_line()
    colored_indexes = None if colored_indexes is None else set(colored_indexes)
    with CONSOLE_OUTPUT_LOCK:
        for idx, line in enumerate(lines):
            text = str(line)
            use_color = color is not None and (colored_indexes is None or idx in colored_indexes)
            print(_console_color(text, color) if use_color else text)
            termcap_write(text)
            TERMINAL_LAST_OUTPUT_WAS_BLANK = (text == "")


def _console_print_timestamped_line(message: str):
    global TERMINAL_LAST_OUTPUT_WAS_BLANK

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    plain = f"{ts} [INFO] {message}"
    with CONSOLE_OUTPUT_LOCK:
        print(colorize_terminal_log_line(plain))
        termcap_write(plain)
        TERMINAL_LAST_OUTPUT_WAS_BLANK = (str(message) == "")


def _print_runtime_controls_reminder():
    activity_id = new_terminal_activity("runtime_controls_reminder")

    with CONSOLE_OUTPUT_LOCK:
        _prepare_terminal_activity(activity_id)
        clear_progress_line()
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        plain = f"{ts} [INFO] Press I for recording information & controls"
        print(colorize_terminal_log_line(plain))
        termcap_write(plain)


def _runtime_controls_reminder_loop(state: RecorderState, stop_event: threading.Event):
    # First reminder is emitted immediately after the first healthy GOOD_BEEP.
    first = True
    while not stop_event.is_set():
        if not first and stop_event.wait(float(RUNTIME_CONTROLS_REMINDER_SEC)):
            return
        first = False

        # Never write over E/D/Y-N entry. Recording and monitoring continue.
        while CONSOLE_INPUT_ACTIVE and not stop_event.wait(0.25):
            pass
        if stop_event.is_set() or state.stop_flag:
            return

        _print_runtime_controls_reminder()


def start_runtime_controls_reminder(state: RecorderState):
    if getattr(state, "runtime_controls_reminder_started", False):
        return
    stop_event = threading.Event()
    thread = threading.Thread(
        target=_runtime_controls_reminder_loop,
        args=(state, stop_event),
        daemon=True,
        name="runtime_controls_reminder",
    )
    state.runtime_controls_reminder_started = True
    state.runtime_controls_reminder_stop_event = stop_event
    state.runtime_controls_reminder_thread = thread
    thread.start()


def stop_runtime_controls_reminder(state: RecorderState):
    stop_event = getattr(state, "runtime_controls_reminder_stop_event", None)
    if stop_event is not None:
        stop_event.set()


def _flush_deferred_terminal_lines_locked():
    global CONSOLE_DEFERRED_TERMINAL_LINES, TERMINAL_LAST_OUTPUT_WAS_BLANK
    pending = CONSOLE_DEFERRED_TERMINAL_LINES
    CONSOLE_DEFERRED_TERMINAL_LINES = []

    for item in pending:
        if (
            isinstance(item, tuple)
            and len(item) == 3
        ):
            activity_id, line, output_is_blank = item
        elif (
            isinstance(item, tuple)
            and len(item) == 2
        ):
            activity_id, line = item
            output_is_blank = bool(
                re.search(r"\[INFO\]\s*$", str(line))
            )
        else:
            # Backward-compatible fallback for any pre-existing queued string.
            activity_id = get_terminal_activity_id()
            line = item
            output_is_blank = False

        _prepare_terminal_activity(
            activity_id,
            upcoming_output_is_blank=output_is_blank,
        )

        if output_is_blank and TERMINAL_LAST_OUTPUT_WAS_BLANK:
            continue

        print(line)
        TERMINAL_LAST_OUTPUT_WAS_BLANK = output_is_blank


def _begin_console_interaction(state: RecorderState, mode: str, prompt: str):
    global CONSOLE_INPUT_ACTIVE
    clear_progress_line()
    with CONSOLE_OUTPUT_LOCK:
        CONSOLE_INPUT_ACTIVE = True
        state.console_input_mode = mode
        state.console_input_buffer = ""
        state.console_pending_runtime_change = None
        if prompt:
            sys.stdout.write("\r\n")
            sys.stdout.write(_console_color(prompt, "cyan"))
        sys.stdout.flush()


def _finish_console_interaction(state: RecorderState):
    global CONSOLE_INPUT_ACTIVE
    with CONSOLE_OUTPUT_LOCK:
        state.console_input_mode = None
        state.console_input_buffer = ""
        state.console_pending_runtime_change = None
        CONSOLE_INPUT_ACTIVE = False
        _flush_deferred_terminal_lines_locked()


def _console_cancel_interaction(state: RecorderState, message: str = "Runtime change cancelled."):
    if getattr(state, "console_input_mode", None) == "confirm_manual_stream_exclusion":
        state.nm3u8dl_pending_manual_exclusion = None
    _console_write("\r\n" + message + "\r\n\r\n")
    _finish_console_interaction(state)


def cancel_console_interaction_if_active(state: RecorderState, message: str):
    if getattr(state, "console_input_mode", None) is not None:
        _console_cancel_interaction(state, message)


def _set_sound_snooze(state: RecorderState, mode: Optional[str]):
    if mode == "timed":
        runtime_sound.set_timed_sound_snooze(
            state,
            duration_sec=15 * 60.0,
        )
        state.sound_snooze_run_attempt = None
    elif mode == "run":
        runtime_sound.set_indefinite_sound_snooze(state, "run")
        state.sound_snooze_run_attempt = int(getattr(state, "run_attempt_index", 0) or 0)
    elif mode == "recording":
        runtime_sound.set_indefinite_sound_snooze(state, "recording")
        state.sound_snooze_run_attempt = None
    else:
        _clear_sound_snooze(state)


def _begin_sound_snooze_menu(state: RecorderState):
    _begin_console_interaction(state, "sound_snooze_menu", "")
    _console_print_lines([
        "",
        "================ SOUND / ALARM SNOOZE ================",
        f"Current sound state : {get_sound_state_text(state)}",
        "",
        "  M  Snooze for 15 minutes",
        "  R  Snooze for current RUN",
        "  F  Snooze for full recording",
        "  U  Unsnooze / restore sounds",
        "  Esc  Cancel",
        "========================================================",
        "",
    ], color="cyan", colored_indexes={1, 9})
    _console_write_colored("Select M/R/F/U or Esc: ", "cyan")


def _apply_sound_snooze_menu_choice(state: RecorderState, ch: str) -> bool:
    choice = str(ch or "").upper()
    if choice not in ("M", "R", "F", "U"):
        return False

    _console_write(choice + "\r\n")

    if choice == "M":
        _set_sound_snooze(state, "timed")
        message = "Sound snoozed for 15 minutes. Recorder monitoring and recovery continue normally."
    elif choice == "R":
        if int(getattr(state, "run_attempt_index", 0) or 0) <= 0:
            message = "No worker RUN has started yet; sound state is unchanged."
        else:
            _set_sound_snooze(state, "run")
            message = "Sound snoozed for the current RUN. Sound returns when the next RUN starts."
    elif choice == "F":
        _set_sound_snooze(state, "recording")
        message = "Sound snoozed for the full recording. Recorder monitoring and recovery continue normally."
    else:
        _set_sound_snooze(state, None)
        message = "Sound restored."

    _console_print_lines([message, f"Sound state         : {get_sound_state_text(state)}", ""])
    _finish_console_interaction(state)
    return True


def _runtime_source_info_lines(state: RecorderState):
    lines = []
    try:
        engine = build_engine_registry().get(DOWNLOAD_MODE)
        if engine is not None:
            lines.extend(engine.summary_lines())
    except Exception:
        pass

    # Dynamic recorder: show the active matched source when available.
    source = getattr(state, "nm3u8dl_running_source", None)
    if source:
        extinf_metadata = parse_nm3u8dl_extinf_metadata(
            str(source.get("extinf") or "")
        )

        tvg_name = source.get(
            "tvg_name",
            extinf_metadata["tvg_name"],
        )
        group_title = source.get(
            "group_title",
            extinf_metadata["group_title"],
        )
        entry_title = source.get(
            "entry_title",
            extinf_metadata["entry_title"],
        )

        lines.append(f"TVG name            : {tvg_name}")
        lines.append(f"Group title         : {group_title}")
        lines.append(f"Entry title         : {entry_title}")

        try:
            selected_quality = format_nm3u8dl_candidate_quality(source)
            if _nm3u8dl_has_quality_evidence(source):
                lines.append(
                    f"Selected quality    : {selected_quality}"
                )
        except Exception:
            pass
    return lines


def show_recording_info(state: RecorderState):
    now = time.time()
    deadline = get_recording_deadline(state)
    current_duration = get_current_planned_duration_min(state)
    original_duration = getattr(state, "original_duration_min", None)

    lines = [
        "",
        "================ RECORDING INFO ================",
        f"Engine             : {DOWNLOAD_MODE}",
        f"Mode               : {'scheduled' if SCHEDULE_START else 'manual'}",
        f"Base name          : {BASE_NAME}",
    ]
    if SCHEDULE_START:
        lines.append(f"Schedule start     : {SCHEDULE_START}")

    lines.extend(_runtime_source_info_lines(state))
    lines.extend([
        f"Original duration  : {_format_minutes_value(original_duration)}",
        f"Current duration   : {_format_minutes_value(current_duration)}",
        f"Runtime adjustment : {get_runtime_adjustment_text(state)}",
        f"Sound              : {get_sound_state_text(state)}",
        "Recording started  : "
        + datetime.fromtimestamp(state.start_time).strftime("%Y-%m-%d %H:%M:%S"),
    ])

    if deadline is None:
        lines.append("Current end time   : until stopped")
        lines.append("Time remaining     : unlimited")
    else:
        lines.append(
            "Current end time   : "
            + datetime.fromtimestamp(deadline).strftime("%Y-%m-%d %H:%M:%S")
        )
        lines.append(f"Time remaining     : {fmt_hms(max(0.0, deadline - now))}")

    lines.extend([
        "",
        "Controls:",
        "  +  Add 15 minutes",
        "  -  Reduce 15 minutes",
        "  E  Custom extend / set total duration",
        "  D  Custom reduce",
        "  S  Sound / alarm snooze",
        "  A  Acknowledge alarm",
        "  R  Restart N_m3u8DL",
        "  X  Reject current feed (Y/N confirm) and find another",
        "================================================",
        "",
    ])
    # Only the title/separator lines are highlighted; the body stays easy to scan.
    _console_print_lines(lines, color="cyan", colored_indexes={1, len(lines) - 2})


def _print_runtime_update(state: RecorderState, old_deadline: Optional[float], new_deadline: float):
    now_ts = time.time()
    current_time_text = datetime.fromtimestamp(now_ts).strftime("%Y-%m-%d %H:%M:%S")
    old_text = (
        "until stopped"
        if old_deadline is None
        else datetime.fromtimestamp(old_deadline).strftime("%Y-%m-%d %H:%M:%S")
    )
    new_text = datetime.fromtimestamp(new_deadline).strftime("%Y-%m-%d %H:%M:%S")
    remaining = max(0.0, new_deadline - now_ts)
    remaining_minutes = int(remaining // 60)
    if remaining <= 0:
        remaining_text = "0 minutes"
    elif remaining_minutes < 1:
        remaining_text = "<1 minute"
    elif remaining_minutes == 1:
        remaining_text = "1 minute"
    else:
        remaining_text = f"{remaining_minutes} minutes"
    lines = [
        "",
        "RUNTIME UPDATED",
        f"Current time      : {current_time_text}",
        f"Previous end      : {old_text}",
        f"New end           : {new_text}",
        f"Current duration  : {_format_minutes_value(get_current_planned_duration_min(state))}",
        f"Time remaining    : {remaining_text}",
    ]
    _console_print_lines(lines, color="green")


def _commit_runtime_deadline(
    state: RecorderState,
    new_deadline: float,
    action: str,
    requested_minutes: int,
):
    lock = getattr(state, "timing_lock", None)
    if lock is None:
        old_deadline = state.deadline_ts
        state.deadline_ts = float(new_deadline)
        state.duration_change_count += 1
    else:
        with lock:
            old_deadline = state.deadline_ts
            state.deadline_ts = float(new_deadline)
            state.duration_change_count += 1

    _print_runtime_update(state, old_deadline, float(new_deadline))
    _console_print_timestamped_line(
        f"RUNTIME_CHANGE action={action} requested_minutes={requested_minutes} "
        f"old_end={old_deadline if old_deadline is not None else 'none'} "
        f"new_end={float(new_deadline):.3f}"
    )
    _console_write("\r\n")


def _warn_and_confirm_runtime_change(
    state: RecorderState,
    old_deadline: Optional[float],
    new_deadline: float,
    action: str,
    requested_minutes: int,
):
    now = time.time()
    elapsed_min = max(0.0, (now - state.start_time) / 60.0)
    requested_total_min = max(0.0, (new_deadline - state.start_time) / 60.0)

    state.console_pending_runtime_change = {
        "old_deadline": old_deadline,
        "new_deadline": float(new_deadline),
        "action": action,
        "requested_minutes": int(requested_minutes),
    }
    state.console_input_mode = "confirm_runtime_stop"
    state.console_input_buffer = ""

    warning_lines = [
        "",
        "WARNING: This timing change would end the recording immediately.",
        f"Elapsed time       : {_format_minutes_value(elapsed_min)}",
        f"Requested duration : {_format_minutes_value(requested_total_min)}",
        f"Requested end      : {datetime.fromtimestamp(new_deadline).strftime('%Y-%m-%d %H:%M:%S')}",
        f"Current time       : {datetime.fromtimestamp(now).strftime('%Y-%m-%d %H:%M:%S')}",
        "",
    ]
    _console_print_lines(warning_lines, color="yellow")
    _console_write_colored("Apply and stop recording now? [Y/N]: ", "yellow")

def _prepare_runtime_change(
    state: RecorderState,
    new_deadline: float,
    action: str,
    requested_minutes: int,
    already_interactive: bool,
):
    old_deadline = get_recording_deadline(state)
    now = time.time()

    if new_deadline <= now:
        if not already_interactive:
            _begin_console_interaction(state, "confirm_runtime_stop", "")
        _warn_and_confirm_runtime_change(
            state,
            old_deadline,
            new_deadline,
            action,
            requested_minutes,
        )
        return

    _commit_runtime_deadline(state, new_deadline, action, requested_minutes)
    if already_interactive:
        _finish_console_interaction(state)


def _begin_custom_runtime_entry(state: RecorderState, action: str):
    deadline = get_recording_deadline(state)

    if action == "extend":
        if deadline is None:
            _begin_console_interaction(
                state,
                "set_total_duration",
                "Recording currently has no planned end time.\r\n"
                "Set total recording duration from recording start (minutes): ",
            )
        else:
            _begin_console_interaction(
                state,
                "extend_duration",
                "Extend recording by minutes: ",
            )
        return

    if deadline is None:
        _console_print_lines([
            "Recording currently has no finite end time.",
            "There is nothing to reduce. Press E to set a total recording duration.",
        ])
        return

    _begin_console_interaction(
        state,
        "reduce_duration",
        "Reduce recording by minutes: ",
    )


def _apply_custom_runtime_entry(state: RecorderState):
    raw = state.console_input_buffer.strip()
    if not raw:
        _console_cancel_interaction(state, "No value entered. Runtime change cancelled.")
        return

    try:
        minutes = int(raw)
    except ValueError:
        _console_cancel_interaction(state, "Invalid value. Runtime change cancelled.")
        return

    if minutes <= 0:
        _console_cancel_interaction(state, "Enter a positive number of minutes. Runtime change cancelled.")
        return

    mode = state.console_input_mode
    current_deadline = get_recording_deadline(state)

    try:
        if mode == "set_total_duration":
            new_deadline = float(state.start_time) + (float(minutes) * 60.0)
            action = "set_total"
        elif mode == "extend_duration":
            if current_deadline is None:
                new_deadline = float(state.start_time) + (float(minutes) * 60.0)
                action = "set_total"
            else:
                new_deadline = float(current_deadline) + (float(minutes) * 60.0)
                action = "extend"
        elif mode == "reduce_duration":
            if current_deadline is None:
                _console_cancel_interaction(
                    state,
                    "Recording has no finite end time. Press E to set a total duration.",
                )
                return
            new_deadline = max(
                float(state.start_time),
                float(current_deadline) - (float(minutes) * 60.0),
            )
            action = "reduce"
        else:
            _console_cancel_interaction(state)
            return
    except (OverflowError, ValueError):
        _console_cancel_interaction(state, "Duration value is too large. Runtime change cancelled.")
        return

    _console_write("\r\n")
    _prepare_runtime_change(
        state,
        new_deadline,
        action,
        minutes,
        already_interactive=True,
    )


def handle_runtime_control_key(state: RecorderState, ch: str) -> bool:
    """Handle I/+/-/E/D/S plus interactive runtime controls. Return True if consumed."""
    mode = getattr(state, "console_input_mode", None)

    if mode is not None:
        if ch == "\x1b":  # Escape
            if mode == "confirm_manual_stream_exclusion":
                _console_cancel_interaction(
                    state,
                    "Manual stream rejection cancelled; recording continues unchanged.",
                )
            elif mode == "sound_snooze_menu":
                _console_cancel_interaction(state, "Sound control cancelled; sound state is unchanged.")
            else:
                _console_cancel_interaction(state)
            return True

        if mode == "sound_snooze_menu":
            _apply_sound_snooze_menu_choice(state, ch)
            return True

        if mode == "confirm_manual_stream_exclusion":
            if ch in ("y", "Y"):
                pending = getattr(
                    state,
                    "nm3u8dl_pending_manual_exclusion",
                    None,
                ) or {}
                source = getattr(state, "nm3u8dl_running_source", None)
                current_signature = get_nm3u8dl_manual_feed_signature(source)

                if (
                    not pending
                    or not current_signature
                    or current_signature.get("key") != pending.get("key")
                    or not getattr(state, "nm3u8dl_run_active", False)
                ):
                    state.nm3u8dl_pending_manual_exclusion = None
                    _console_write(
                        "Y\r\nCurrent feed changed before confirmation; "
                        "manual rejection cancelled.\r\n\r\n"
                    )
                    _finish_console_interaction(state)
                    return True

                state.nm3u8dl_manual_exclude_requested = True
                _console_write("Y\r\nManual feed rejection confirmed.\r\n\r\n")
                _finish_console_interaction(state)
                log(
                    "MANUAL STREAM REJECT confirmed — current feed signature "
                    "will be excluded for this recording and the full playlist "
                    "set will be rescanned."
                )
            elif ch in ("n", "N"):
                state.nm3u8dl_pending_manual_exclusion = None
                _console_write(
                    "N\r\nManual stream rejection cancelled; "
                    "recording continues unchanged.\r\n\r\n"
                )
                _finish_console_interaction(state)
            return True

        if mode == "confirm_runtime_stop":
            if ch in ("y", "Y"):
                _console_write("Y\r\n")
                pending = state.console_pending_runtime_change or {}
                if pending:
                    _commit_runtime_deadline(
                        state,
                        float(pending["new_deadline"]),
                        str(pending["action"]),
                        int(pending["requested_minutes"]),
                    )
                _finish_console_interaction(state)
            elif ch in ("n", "N"):
                _console_write("N\r\nRuntime change cancelled; previous end time is unchanged.\r\n\r\n")
                _finish_console_interaction(state)
            return True

        if ch.isdigit():
            state.console_input_buffer += ch
            _console_write(ch)
            return True

        if ch in ("\x08", "\x7f"):  # Windows/macOS Backspace
            if state.console_input_buffer:
                state.console_input_buffer = state.console_input_buffer[:-1]
                _console_write("\b \b")
            return True

        if ch in ("\r", "\n"):
            _apply_custom_runtime_entry(state)
            return True

        # Interactive entry owns the keyboard; ignore unrelated keys so A/R or
        # other commands cannot accidentally fire while a number is being typed.
        return True

    if ch in ("i", "I"):
        show_recording_info(state)
        return True

    if ch in ("s", "S"):
        _begin_sound_snooze_menu(state)
        return True

    if ch == "+":
        deadline = get_recording_deadline(state)
        if deadline is None:
            _console_print_lines([
                "Recording currently has no finite end time.",
                "Press E to set a total recording duration first.",
            ])
        else:
            _prepare_runtime_change(
                state,
                float(deadline) + (15 * 60.0),
                "quick_extend",
                15,
                already_interactive=False,
            )
        return True

    if ch == "-":
        deadline = get_recording_deadline(state)
        if deadline is None:
            _console_print_lines([
                "Recording currently has no finite end time.",
                "There is nothing to reduce. Press E to set a total recording duration.",
            ])
        else:
            new_deadline = max(float(state.start_time), float(deadline) - (15 * 60.0))
            _prepare_runtime_change(
                state,
                new_deadline,
                "quick_reduce",
                15,
                already_interactive=False,
            )
        return True

    if ch in ("e", "E"):
        _begin_custom_runtime_entry(state, "extend")
        return True

    if ch in ("d", "D"):
        _begin_custom_runtime_entry(state, "reduce")
        return True

    if ch in ("x", "X"):
        if (
            DOWNLOAD_MODE != ENGINE_NM3U8DL
            or NM3U8DL_SOURCE_MODE != "playlist"
            or not getattr(state, "nm3u8dl_run_active", False)
        ):
            _console_print_lines([
                "No active dynamic N_m3u8DL playlist stream is available to reject.",
            ])
            return True

        if getattr(state, "nm3u8dl_manual_exclude_requested", False):
            _console_print_lines([
                "Manual stream rejection is already pending.",
            ])
            return True

        source = getattr(state, "nm3u8dl_running_source", None)
        signature = get_nm3u8dl_manual_feed_signature(source)

        if not source or not signature:
            _console_print_lines([
                "Current feed cannot be safely rejected: a complete feed signature "
                "(family/type/resolution/fps/bitrate) is unavailable.",
                "Recording continues unchanged.",
            ])
            return True

        _begin_console_interaction(
            state,
            "confirm_manual_stream_exclusion",
            "",
        )
        state.nm3u8dl_pending_manual_exclusion = dict(signature)

        entry_title = str(
            source.get("entry_title")
            or source.get("tvg_name")
            or "current stream"
        ).strip()
        _console_print_lines([
            "",
            "MANUAL STREAM REJECTION",
            f"Entry              : {entry_title}",
            f"Feed signature     : {format_nm3u8dl_manual_feed_signature(signature)}",
            "Scope              : this recording only",
            "Effect             : all matching feeds with this signature will be excluded",
            "",
        ], color="yellow", colored_indexes={1})
        _console_write_colored(
            "Reject this feed and find another? [Y/N]: ",
            "yellow",
        )
        return True

    return False


def get_file_info(path):
    try:
        out = check_output_external(
            [
                "ffprobe",
                "-v", "quiet",
                "-print_format", "json",
                "-show_entries", "format=duration,bit_rate",
                path,
            ],
            raw_tool="ffprobe",
            raw_context=f"{os.path.basename(path)} | file info",
            timeout=5,
        ).decode()
        data = json.loads(out)["format"]
        dur = float(data.get("duration", 0))
        br = int(data.get("bit_rate", 0)) / 1000
        return dur, br
    except:
        return 0, 0

def get_video_params(path):
    """
    Return (width, height, fps) from a video file.
    Falls back to 1920x1080@50 if probe fails.
    """
    try:
        out = check_output_external(
            [
                "ffprobe",
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=width,height,avg_frame_rate,r_frame_rate",
                "-of", "json",
                path,
            ],
            raw_tool="ffprobe",
            raw_context=f"{os.path.basename(path)} | video params",
            timeout=5,
        ).decode()
        data = json.loads(out)["streams"][0]
        w = int(data.get("width", 1920))
        h = int(data.get("height", 1080))
       
        fps_str = data.get("avg_frame_rate", data.get("r_frame_rate", "50/1"))  # default 50fps
        
        num, den = fps_str.split("/")
        fps = int(round(float(num) / float(den)))
        return w, h, fps
    except:
        return 1920, 1080, 50
        
def get_audio_layout(path):
    """
    Return (has_audio, layout_list) for ALL audio streams in a TS file.
    layout_list items: {codec_name, channels, sample_rate, channel_layout, bit_rate}
    """
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "a",
        "-show_entries", "stream=index,codec_name,channels,sample_rate,channel_layout,bit_rate",
        "-of", "json",
        path
    ]
    try:
        p = run_external_capture(
            cmd,
            raw_tool="ffprobe",
            raw_context=f"{os.path.basename(path)} | audio layout",
            capture_output=True,
            text=True,
        )
        if p.returncode != 0:
            return False, []
        data = json.loads(p.stdout or "{}")
        streams = data.get("streams") or []
        if not streams:
            return False, []
        layout = []
        for s in streams:
            layout.append({
                "codec_name": s.get("codec_name"),
                "channels": int(s.get("channels") or 0),
                "sample_rate": int(s.get("sample_rate") or 0),
                "channel_layout": s.get("channel_layout") or None,
                "bit_rate": int(s.get("bit_rate") or 0),
            })
        return True, layout
    except Exception:
        return False, []

def get_av_stream_layout(path):
    """
    Return ordered audio/video stream metadata exactly as ffprobe reports it.
    This is used to make concat separators match the first GOOD chunk's
    stream order and codecs.
    """
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries",
        (
            "stream=index,codec_type,codec_name,width,height,"
            "avg_frame_rate,r_frame_rate,pix_fmt,"
            "channels,sample_rate,channel_layout,bit_rate"
        ),
        "-of", "json",
        path,
    ]

    try:
        p = run_external_capture(
            cmd,
            raw_tool="ffprobe",
            raw_context=f"{os.path.basename(path)} | A/V stream layout",
            capture_output=True,
            text=True,
        )
        if p.returncode != 0:
            return []

        data = json.loads(p.stdout or "{}")
        layout = []

        for s in (data.get("streams") or []):
            codec_type = s.get("codec_type")
            if codec_type not in ("video", "audio"):
                continue

            layout.append({
                "index": int(s.get("index") or 0),
                "codec_type": codec_type,
                "codec_name": s.get("codec_name"),
                "width": int(s.get("width") or 0),
                "height": int(s.get("height") or 0),
                "avg_frame_rate": s.get("avg_frame_rate") or None,
                "r_frame_rate": s.get("r_frame_rate") or None,
                "pix_fmt": s.get("pix_fmt") or None,
                "channels": int(s.get("channels") or 0),
                "sample_rate": int(s.get("sample_rate") or 0),
                "channel_layout": s.get("channel_layout") or None,
                "bit_rate": int(s.get("bit_rate") or 0),
            })

        return sorted(layout, key=lambda s: s["index"])

    except Exception:
        return []


def make_black_clip(state: RecorderState):
    """
    Create a short separator whose A/V stream order and codecs match the
    first GOOD chunk. Video becomes black; audio becomes silence.
    Returns the separator path, or None if a safe matching separator cannot
    be generated.
    """
    layout = list(state.stream_layout_ref or [])

    if not layout:
        log("Cannot create separator: first GOOD chunk A/V layout is unknown.", level="WARN")
        return None

    video_encoder_map = {
        "h264": "libx264",
        "hevc": "libx265",
        "mpeg2video": "mpeg2video",
        "mpeg4": "mpeg4",
    }

    audio_encoder_map = {
        "aac": "aac",
        "ac3": "ac3",
        "eac3": "eac3",
        "mp2": "mp2",
        "mp3": "libmp3lame",
    }

    def _fps_value(stream):
        fps_text = stream.get("avg_frame_rate") or stream.get("r_frame_rate")
        try:
            num, den = str(fps_text).split("/", 1)
            den_v = float(den)
            if den_v != 0:
                return float(num) / den_v
        except Exception:
            pass
        return float(state.best_fps or 50)

    def _channel_layout(stream):
        if stream.get("channel_layout"):
            return str(stream["channel_layout"])

        channels = int(stream.get("channels") or 0)
        return {
            1: "mono",
            2: "stereo",
            6: "5.1",
            8: "7.1",
        }.get(channels)

    signature_parts = []
    for stream in layout:
        codec_type = stream.get("codec_type")
        codec_name = stream.get("codec_name") or "unknown"
        signature_parts.append(f"{codec_type[0]}-{codec_name}")

    safe_signature = "_".join(signature_parts)
    name = (
        f"black_{state.best_w}x{state.best_h}_{int(round(state.best_fps or 50))}fps_"
        f"{safe_signature}_{int(BLACK_LEN)}s.ts"
    )
    black_path = os.path.join(CHUNKS_DIR, name)

    if os.path.exists(black_path):
        return black_path

    cmd = ["ffmpeg", "-y"]
    output_streams = []

    for stream in layout:
        codec_type = stream.get("codec_type")
        codec_name = stream.get("codec_name")

        if codec_type == "video":
            encoder = video_encoder_map.get(codec_name)
            if not encoder:
                log(
                    f"Cannot create separator: unsupported video codec {codec_name}.",
                    level="WARN",
                )
                return None

            width = int(stream.get("width") or state.best_w or 1920)
            height = int(stream.get("height") or state.best_h or 1080)
            fps = _fps_value(stream)

            input_index = len(output_streams)
            cmd += [
                "-f", "lavfi",
                "-i", f"color=black:s={width}x{height}:r={fps}:d={BLACK_LEN}",
            ]
            output_streams.append({
                "input_index": input_index,
                "codec_type": "video",
                "encoder": encoder,
                "pix_fmt": stream.get("pix_fmt") or "yuv420p",
            })

        elif codec_type == "audio":
            encoder = audio_encoder_map.get(codec_name)
            if not encoder:
                log(
                    f"Cannot create separator: unsupported audio codec {codec_name}.",
                    level="WARN",
                )
                return None

            sample_rate = int(stream.get("sample_rate") or 48000)
            channel_layout = _channel_layout(stream)

            if not channel_layout:
                log(
                    f"Cannot create separator: unsupported audio channel layout "
                    f"({stream.get('channels')} channels).",
                    level="WARN",
                )
                return None

            input_index = len(output_streams)
            cmd += [
                "-f", "lavfi",
                "-i",
                f"anullsrc=r={sample_rate}:cl={channel_layout}:d={BLACK_LEN}",
            ]
            output_streams.append({
                "input_index": input_index,
                "codec_type": "audio",
                "encoder": encoder,
                "bit_rate": int(stream.get("bit_rate") or 0),
            })

    if not any(s["codec_type"] == "video" for s in output_streams):
        log("Cannot create separator: no video stream found.", level="WARN")
        return None

    # Map generated inputs in the exact same order as the source streams.
    for stream in output_streams:
        cmd += ["-map", f"{stream['input_index']}:0"]

    video_no = 0
    audio_no = 0

    for stream in output_streams:
        if stream["codec_type"] == "video":
            cmd += [
                f"-c:v:{video_no}", stream["encoder"],
                f"-pix_fmt:v:{video_no}", stream["pix_fmt"],
            ]
            if stream["encoder"] in ("libx264", "libx265"):
                cmd += [f"-preset:v:{video_no}", "veryfast"]
            video_no += 1

        else:
            cmd += [f"-c:a:{audio_no}", stream["encoder"]]
            if stream["bit_rate"] > 0:
                cmd += [f"-b:a:{audio_no}", str(stream["bit_rate"])]
            audio_no += 1

    cmd += [
        "-t", str(BLACK_LEN),
        "-f", "mpegts",
        black_path,
    ]

    log(f"Creating matching black separator clip {name}...")
    proc = subprocess.run(cmd, capture_output=True, text=True)

    if proc.returncode != 0 or not os.path.exists(black_path):
        log("Matching black separator creation failed.", level="WARN")
        if proc.stderr:
            log(proc.stderr.strip(), level="WARN")
        try:
            if os.path.exists(black_path):
                os.remove(black_path)
        except OSError:
            pass
        return None

    return black_path

# ==============================================================================
# Engine: FFmpeg
# ==============================================================================

def is_stream_alive():
    try:
        # Try to read a very short snippet with ffprobe (or ffmpeg -t 1 -f null -)
        result = run_external_capture(
            [
                "ffprobe",
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=width",
                "-of", "csv=p=0",
                LIVE_URL,
            ],
            raw_tool="ffprobe",
            raw_context=(
                "source availability probe | "
                f"{_format_timeout_source(LIVE_URL)}"
            ),
            capture_output=True,
            text=True,
            timeout=10, # change to 10 for normal use, 90 for slow iptv
        )
        return result.returncode == 0
    except Exception as e:
        if not _is_timeout_exception(e):
            log(f"ffprobe availability check failed: {e}")
        return False

def get_stream_expected_kbps(url: str) -> Optional[float]:
    """Best-effort expected bitrate from stream metadata, in Kbps."""
    try:
        result = run_external_capture(
            [
                "ffprobe",
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=bit_rate",
                "-of", "csv=p=0",
                url,
            ],
            raw_tool="ffprobe",
            raw_context=(
                "expected bitrate probe | video stream | "
                f"{_format_timeout_source(url)}"
            ),
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            value = (result.stdout or "").strip()
            if value.isdigit():
                kbps = int(value) / 1000
                if kbps > 0:
                    return kbps
    except Exception:
        pass

    try:
        result = run_external_capture(
            [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=bit_rate",
                "-of", "csv=p=0",
                url,
            ],
            raw_tool="ffprobe",
            raw_context=(
                "expected bitrate probe | container | "
                f"{_format_timeout_source(url)}"
            ),
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            value = (result.stdout or "").strip()
            if value.isdigit():
                kbps = int(value) / 1000
                if kbps > 0:
                    return kbps
    except Exception:
        pass
    return None
    
def run_ffmpeg_with_capture(cmd, cwd, tag="FFmpeg"):
    import threading
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    last_progress_line = None
    progress_line_active = False
    kv = {}

    def monitor_thread():
        nonlocal last_progress_line, progress_line_active, kv
        try:
            while True:
                line = proc.stderr.readline()
                if not line:
                    break
                s = line.strip()
                m = re.match(r'^([A-Za-z0-9_]+)=(.*)$', s)
                if m:
                    k, v = m.group(1), m.group(2)
                    kv[k] = v
                    if k == "progress":
                        parts = []
                        for key in ("frame", "fps", "total_size", "out_time", "bitrate", "speed"):
                            if key in kv:
                                label = "time" if key == "out_time" else ("size" if key == "total_size" else key)
                                parts.append(f"{label}={kv[key]}")
                        prog = " ".join(parts)
                        if prog:
                            progress_line_active = render_progress_line(prog, pad=80)
                            last_progress_line = prog
                        kv = {}
                    continue
                if progress_line_active:
                    clear_progress_line()
                    progress_line_active = False
                if s:
                    log(f"{tag}: {s}")
        except Exception as e:
            log(f"{tag} monitor error: {e}")

    t = threading.Thread(target=monitor_thread, daemon=True)
    t.start()
    
    rc = proc.wait()
    
    try:
        join_thread_with_timeout_logging(
            t,
            3,
            context=f"{tag} output monitor",
        )
    except:
        pass
    
    if progress_line_active:
        clear_progress_line()
    if last_progress_line:
        log(f"{tag}: {last_progress_line}")
    return rc

def start_ffmpeg_to_chunk(state: RecorderState, notify=None, deadline_ts: Optional[float] = None) -> EngineResult:
    """
    Start one FFmpeg run that writes directly to the 'next' chunk_NNN.ts.
    Does NOT increment state.chunk_index yet. Returns EngineResult with run facts only.
    """
    chunk_path, chunk_name = next_chunk_names(state)
    
    # record when this recording attempt started
    state.stats["last_run_start"] = time.time()

    cmd = [
        "ffmpeg",
        "-y",
        "-loglevel", "info",
        "-hide_banner",
        
        # Input/network behavior (must be BEFORE -i)
        "-rw_timeout", "15000000", # "15000000" for normal use, "90000000" for slow IPTV
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_at_eof", "1",
        "-reconnect_delay_max", "10",

        "-nostats",
        "-progress", "pipe:2",
        "-stats_period", str(FFMPEG_PROGRESS_PERIOD),
        
        "-i", LIVE_URL,
        
        # Explicit stream selection:
        # - Take all video streams
        # - Take all audio streams
        # - But exclude MP2 audio (and mp2float if it appears)
        "-map", "0:v",
        "-map", "0:a?",
        "-map", "-0:a:m:codec:mp2",
        "-map", "-0:a:m:codec:mp2float",
        "-map", "-0:a:m:codec:mp3",
        "-map", "-0:a:m:codec:mp3float",

        "-c", "copy",
        "-f", "mpegts",
        chunk_path,
    ]
    ff_expected_kbps = get_stream_expected_kbps(LIVE_URL)
    ff_expected_source = "stream" if ff_expected_kbps else None
    if ff_expected_kbps is None:
        log("EXPECTED_KBPS_PROBE_FAILED source=ffprobe reason=missing_bitrate")
    else:
        if ff_expected_kbps > state.ffmpeg_selected_bitrate_kbps_max:
            state.ffmpeg_selected_bitrate_kbps_max = int(ff_expected_kbps)
            log(f"FFmpeg: Selected bitrate detected {int(ff_expected_kbps)} Kbps (source=stream)")
        else:
            ff_expected_kbps = float(state.ffmpeg_selected_bitrate_kbps_max)
            ff_expected_source = "max"
            log(
                f"FFmpeg: Selected bitrate detected {int(ff_expected_kbps)} Kbps "
                "(source=previous_max)"
            )

    log(f"Starting FFmpeg → writing to {chunk_name} until stream dies...")
    log("")
    start_run = time.time()
    start_run_mono = time.monotonic()
    raw_invocation = raw_external_start("FFmpeg", chunk_name)
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
    except Exception as exc:
        raw_external_end(
            raw_invocation,
            status=f"launch exception {type(exc).__name__}",
        )
        raise

    last_progress_line = None
    last_progress_len = 0
    progress_line_active = False
    hot_audio_restart = threading.Event()
    ran_30s = False
    status = "ended"
    reason = "ffmpeg exited"
    stall_type = None
    ff_progress_kbps: Optional[float] = None
    last_bitrate_log_kbps: Optional[float] = None
    last_bitrate_log_ts = 0.0
    state.ffmpeg_healthy_consec_count = 0
    state.ffmpeg_beeped_this_healthy_streak = False
    state.ffmpeg_had_alarm_incident = bool(
        state.ffmpeg_stall_run_count > 0
        or state.ffmpeg_alarm_active
        or state.ffmpeg_post_ack_silent
    )

    ff_prev_size = os.path.getsize(chunk_path) if os.path.exists(chunk_path) else 0
    ff_prev_t = time.monotonic()
    ff_last_health_log_t = ff_prev_t
    ff_next_check = ff_prev_t + float(FF_GROWTH_CHECK_INTERVAL)

    # FFmpeg's live progress row and its recurring Good-speed health line are
    # one terminal activity. They can interrupt other work together without
    # adding separators between each other.
    ffmpeg_progress_activity_id = new_terminal_activity("ffmpeg_progress")
    ff_hard_count = 0
    ff_soft_count = 0
    ff_recovered = False

    def format_elapsed(seconds: float) -> str:
        if seconds < 0:
            seconds = 0
        whole = int(seconds)
        hundredths = int((seconds - whole) * 100)
        hours = whole // 3600
        minutes = (whole % 3600) // 60
        secs = whole % 60
        return f"{hours}:{minutes:02d}:{secs:02d}.{hundredths:02d}"

    def flush_progress_line():
        nonlocal progress_line_active, last_progress_len
        if progress_line_active:
            clear_progress_line()
            progress_line_active = False
            last_progress_len = 0
            
    def parse_progress_bitrate(value: str) -> Optional[float]:
        if not value or value == "N/A":
            return None
        match = re.search(r"([0-9]*\.?[0-9]+)\s*([kKmMgG]?bits/s)", value)
        if not match:
            return None
        amount = float(match.group(1))
        unit = match.group(2).lower()
        if unit.startswith("g"):
            amount *= 1_000_000
        elif unit.startswith("m"):
            amount *= 1_000
        elif unit.startswith("k"):
            amount *= 1
        else:
            amount = amount / 1000.0
        return amount

    def monitor_ffmpeg(p):
        nonlocal last_progress_line, progress_line_active
        nonlocal last_progress_len
        nonlocal ff_expected_kbps, ff_expected_source, ff_progress_kbps
        nonlocal last_bitrate_log_kbps, last_bitrate_log_ts
        kv = {}
        try:
            while True:
                line = p.stderr.readline()
                if not line:
                    break

                # Capture before any recorder filtering/suppression/parsing.
                raw_external_write(raw_invocation, line, "stderr")
                s = line.strip()
                
                # HOT-AUDIO: if audio appears mid-run, restart FFmpeg so NEXT chunk includes audio
                if "new audio stream" in s.lower():
                    if not hot_audio_restart.is_set():
                        if progress_line_active:
                            clear_progress_line()
                            progress_line_active = False
                        hot_audio_restart.set()
                        log(f"FFmpeg: {s}")
                        log("FFmpeg: HOT-AUDIO detected → restarting FFmpeg so next chunk includes audio.")
                    continue

                m = re.match(r'^([A-Za-z0-9_]+)=(.*)$', s)
                if m:
                    k, v = m.group(1), m.group(2)
                    kv[k] = v
                    if k == "progress":
                        if "bitrate" in kv:
                            parsed_kbps = parse_progress_bitrate(kv["bitrate"])
                            if parsed_kbps:
                                ff_progress_kbps = parsed_kbps
                                if parsed_kbps > state.ffmpeg_selected_bitrate_kbps_max:
                                    state.ffmpeg_selected_bitrate_kbps_max = int(parsed_kbps)
                                    now = time.monotonic()
                                    should_log = (
                                        last_bitrate_log_kbps is None
                                        or (
                                            parsed_kbps - last_bitrate_log_kbps >= FF_SELECTED_BITRATE_LOG_MIN_DELTA_KBPS
                                            and (now - last_bitrate_log_ts) >= FF_SELECTED_BITRATE_LOG_MIN_INTERVAL_SEC
                                        )
                                    )
                                    if should_log:
                                        flush_progress_line()
                                        log("")
                                        log(
                                            f"FFmpeg: Selected bitrate detected {int(parsed_kbps)} Kbps "
                                            "(source=progress)"
                                        )
                                        last_bitrate_log_kbps = parsed_kbps
                                        last_bitrate_log_ts = now
                                if ff_expected_kbps is None or parsed_kbps > ff_expected_kbps:
                                    ff_expected_kbps = parsed_kbps
                                    ff_expected_source = "progress"
                        parts = []
                        for key in ("frame", "fps", "total_size", "out_time", "bitrate", "speed"):
                            if key in kv:
                                label = "time" if key == "out_time" else ("size" if key == "total_size" else key)
                                parts.append(f"{label}={kv[key]}")
                        prog = " ".join(parts)

                        if prog:
                            elapsed = format_elapsed(time.monotonic() - start_run_mono)
                            prog = f"{prog} elapsed={elapsed}"
                            pad = max(0, last_progress_len - len(prog))
                            # Show on screen as ONE updating line (no newlines)
                            progress_line_active = render_progress_line(
                                prog,
                                pad=pad,
                                activity_id=ffmpeg_progress_activity_id,
                            )
                            last_progress_line = prog
                            last_progress_len = len(prog)

                        kv = {}
                    continue
                
                # Non-progress line: end the progress line once, then log message
                if progress_line_active:
                    clear_progress_line()
                    progress_line_active = False
                    last_progress_len = 0

                if s:
                    log(f"FFmpeg: {s}")
        except Exception as e:
            log(f"FFmpeg stderr monitor error: {e}")

    t = threading.Thread(target=monitor_ffmpeg, args=(proc,), daemon=True)
    t.start()

    while True:
        ret = proc.poll()
        if ret is not None:
            # FFmpeg finished naturally
            break

        if hot_audio_restart.is_set():
            reason = "hot audio restart"
            log("FFmpeg: HOT-AUDIO restart requested → terminating FFmpeg now.")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                log_process_shutdown_timeout("FFmpeg", 5)
                proc.kill()
                proc.wait()
            break

        if recording_deadline_reached(state):
            reason = "duration reached"
            status = "duration_reached"
            log(f"Max duration reached {format_current_duration_for_log(state)} → terminating FFmpeg...")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                log_process_shutdown_timeout("FFmpeg", 5)
                proc.kill()
                proc.wait()
            break

        if state.stop_flag:
            reason = "external stop"
            log(f"stop_flag detected → terminating FFmpeg...")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                log_process_shutdown_timeout("FFmpeg", 5)
                proc.kill()
                proc.wait()
            break

        if not ran_30s and (time.time() - start_run) >= 30:
            ran_30s = True

        # FFmpeg still running
        now_t = time.monotonic()
        if now_t >= ff_next_check:
            ff_next_check = now_t + float(FF_GROWTH_CHECK_INTERVAL)
            cur_size = os.path.getsize(chunk_path) if os.path.exists(chunk_path) else 0
            elapsed_s = max(0.001, now_t - ff_prev_t)
            growth_bytes = max(0, cur_size - ff_prev_size)
            growth_kbps = (growth_bytes * 8.0) / elapsed_s / 1000.0
            ff_has_file = os.path.exists(chunk_path)

            ff_prev_size = cur_size
            ff_prev_t = now_t

            is_hard = (growth_bytes <= FF_HARD_STALL_ZERO_BYTES)
            if is_hard:
                ff_hard_count += 1
                ff_soft_count = 0
                flush_progress_line()
                log(
                    f"HARD_STALL_CHECK {min(ff_hard_count, FF_HARD_STALL_REQUIRED)}/{FF_HARD_STALL_REQUIRED} growth_bytes={growth_bytes}"
                )
                if not is_alarm_active(state):
                    beep_bad(state)
            else:
                ff_hard_count = 0
                if ff_expected_kbps is None:
                    if ff_progress_kbps:
                        ff_expected_kbps = ff_progress_kbps
                        ff_expected_source = "progress"
                        flush_progress_line()
                        log(
                            f"FFmpeg: Selected bitrate detected {int(ff_expected_kbps)} Kbps "
                            "(source=progress fallback_from=ffprobe)"
                        )
                    elif state.ffmpeg_selected_bitrate_kbps_max > 0:
                        ff_expected_kbps = float(state.ffmpeg_selected_bitrate_kbps_max)
                        ff_expected_source = "max"
                        flush_progress_line()
                        log(
                            f"FFmpeg: Selected bitrate detected {int(ff_expected_kbps)} Kbps "
                            "(source=previous_max)"
                        )
                    else:
                        ff_expected_kbps = growth_kbps
                        ff_expected_source = "growth"
                        state.ffmpeg_selected_bitrate_kbps_max = int(ff_expected_kbps)
                        flush_progress_line()
                        log(
                            f"FFmpeg: Selected bitrate detected {int(ff_expected_kbps)} Kbps "
                            "(source=growth fallback_from=ffprobe)"
                        )

                if ff_expected_kbps is not None:
                    threshold_kbps = ff_expected_kbps * float(NM3U8DL_SPEED_DEGRADATION_FACTOR)
                    if growth_kbps < threshold_kbps:
                        ff_soft_count += 1
                        flush_progress_line()
                        log(
                            f"SOFT_STALL_CHECK {min(ff_soft_count, FF_SOFT_STALL_REQUIRED)}/{FF_SOFT_STALL_REQUIRED} "
                            f"growth_kbps={int(growth_kbps)} threshold_kbps={int(threshold_kbps)} expected_kbps={int(ff_expected_kbps)}"
                        )
                        if not is_alarm_active(state):
                            beep_bad(state)
                    else:
                        ff_soft_count = 0
                        if not ff_recovered:
                            ff_recovered = True
                            if notify: notify("ffmpeg_recovery")
                        maybe_trigger_good_beep(
                            state,
                            "ffmpeg",
                            ff_has_file,
                            state.ffmpeg_had_alarm_incident,
                            notify,
                        )
                        now_health_log_t = time.monotonic()
                        if (now_health_log_t - ff_last_health_log_t) >= FF_HEALTH_LOG_INTERVAL:
                            flush_progress_line()
                            log(
                                f"FFmpeg: Good speed {int(growth_kbps)} Kbps (threshold {int(threshold_kbps)})",
                                activity_id=ffmpeg_progress_activity_id,
                            )
                            ff_last_health_log_t = now_health_log_t

            hard_trigger = (ff_hard_count >= FF_HARD_STALL_REQUIRED)
            soft_trigger = (ff_soft_count >= FF_SOFT_STALL_REQUIRED)

            if hard_trigger or soft_trigger:
                stall_type = "hard" if hard_trigger else "soft"
                flush_progress_line()
                log(f"CLASSIFY STALL_{stall_type.upper()}", level="WARN")
                reset_good_beep_state(state, "ffmpeg")
                status = "stalled"
                reason = f"STALL_{stall_type.upper()}"
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    log_process_shutdown_timeout("FFmpeg", 5)
                    proc.kill()
                    proc.wait()
                break

        time.sleep(0.5)

    try:
        join_thread_with_timeout_logging(
            t,
            3,
            context="FFmpeg output monitor",
        )
    except threading.ThreadError:
        pass
    proc.wait()
    raw_external_end(raw_invocation, returncode=proc.returncode)
        
    # Finish the on-screen progress line cleanly (only if it is active)
    if progress_line_active:
        clear_progress_line()
        progress_line_active = False

    # Log one final progress snapshot into your capture log
    if last_progress_line:
        log(
            f"FFmpeg: {last_progress_line}",
            activity_id=ffmpeg_progress_activity_id,
        )

    unexpected_process_exit = (
        proc.returncode not in (0, None)
        and reason == "ffmpeg exited"
        and not state.stop_flag
    )

    if unexpected_process_exit:
        log(f"FFmpeg exited unexpectedly with code {proc.returncode}", level="WARN")

    log("FFmpeg stopped for this run")
    return EngineResult(
        status="duration_reached" if status == "duration_reached" else ("stalled" if status == "stalled" else "ended"),
        reason=reason if status != "ended" or proc.returncode != 0 else f"exit code {proc.returncode}",
        chunk_path=chunk_path,
        chunk_name=chunk_name,
        metrics={
            "ran_30s": ran_30s,
            "stall_type": stall_type,
            "unexpected_process_exit": unexpected_process_exit,
        },
    )

def run_ffmpeg_cycle(state: RecorderState, backoff_sec, notify=None, deadline_ts: Optional[float] = None):
    """One FFmpeg worker cycle: start/monitor/stop and return run facts only."""

    log_run_start_banner(state, "FFmpeg")
    return start_ffmpeg_to_chunk(state, notify=notify, deadline_ts=deadline_ts)
    
# ==============================================================================
# Engine: N_m3u8DL-RE
# ==============================================================================

def get_nm3u8dl_playlist_profile() -> dict:
    group_name = NM3U8DL_PLAYLIST_GROUP.strip().upper()

    profile_name = NM3U8DL_PLAYLIST_GROUP_PROFILES.get(
        group_name
    )

    if profile_name is None:
        raise RuntimeError(
            f'No command profile mapped for playlist group '
            f'"{group_name}"'
        )

    profile = NM3U8DL_PLAYLIST_PROFILES.get(profile_name)

    if profile is None:
        raise RuntimeError(
            f'Playlist group "{group_name}" maps to unknown '
            f'profile "{profile_name}"'
        )

    resolved_profile = dict(profile)
    resolved_profile.update(
        NM3U8DL_PLAYLIST_GROUP_PROFILE_OVERRIDES.get(
            group_name,
            {},
        )
    )

    return resolved_profile


def get_nm3u8dl_playlist_match_mode() -> str:
    group_name = NM3U8DL_PLAYLIST_GROUP.strip().upper()
    match_mode = NM3U8DL_PLAYLIST_GROUP_MATCH_MODES.get(group_name)

    if match_mode not in ("EXACT_CHANNEL", "EVENT_PHRASE"):
        raise RuntimeError(
            f'No matching mode configured for playlist group "{group_name}"'
        )

    return match_mode


def get_nm3u8dl_playlist_lifecycle() -> str:
    group_name = NM3U8DL_PLAYLIST_GROUP.strip().upper()
    lifecycle = NM3U8DL_PLAYLIST_GROUP_LIFECYCLES.get(group_name)

    if lifecycle not in ("EVENT", "LINEAR_TV"):
        raise RuntimeError(
            f'No stream lifecycle configured for playlist group "{group_name}"'
        )

    return lifecycle



def _normalize_nm3u8dl_playlist_source_config(source) -> tuple:
    if isinstance(source, dict):
        playlist_url = str(source.get("url") or "").strip()
        playlist_user_agent_profile = str(
            source.get("playlist_user_agent") or ""
        ).strip().upper()
        stream_user_agent_profile = str(
            source.get("stream_user_agent") or ""
        ).strip().upper()
        return (
            playlist_url,
            playlist_user_agent_profile,
            stream_user_agent_profile,
        )

    return str(source).strip(), "", ""


def get_nm3u8dl_playlist_source_bucket() -> str:
    """Return the configured playlist-source bucket for the active runtime group."""
    group_name = NM3U8DL_PLAYLIST_GROUP.strip().upper()

    return SHARED_PLAYLIST_GROUP_SOURCE_BUCKETS.get(group_name, group_name)


def get_nm3u8dl_playlist_urls() -> List[str]:
    group_name = NM3U8DL_PLAYLIST_GROUP.strip().upper()
    source_bucket = get_nm3u8dl_playlist_source_bucket()

    if (
        not group_name
        or group_name == "COMMON"
        or source_bucket not in NM3U8DL_PLAYLIST_GROUPS
    ):
        valid_buckets = [
            name
            for name in NM3U8DL_PLAYLIST_GROUPS
            if name != "COMMON"
        ]

        raise RuntimeError(
            f'No playlist source bucket configured for group "{group_name}". '
            f'Available source buckets: {", ".join(valid_buckets)}'
        )

    playlist_urls = []

    for source_group in ("COMMON", source_bucket):
        for playlist_source in NM3U8DL_PLAYLIST_GROUPS.get(
            source_group,
            [],
        ):
            playlist_url, _, _ = _normalize_nm3u8dl_playlist_source_config(
                playlist_source
            )

            if (
                playlist_url
                and playlist_url not in playlist_urls
            ):
                playlist_urls.append(playlist_url)

    if not playlist_urls:
        raise RuntimeError(
            f'No playlist URLs configured for group "{group_name}"'
        )

    return playlist_urls


def get_nm3u8dl_playlist_source_group(playlist_url: str) -> str:
    """Return the configured source bucket that owns one playlist URL."""
    target_url = str(playlist_url or "").strip()
    source_bucket = get_nm3u8dl_playlist_source_bucket()

    if not target_url:
        return ""

    for source_group in ("COMMON", source_bucket):
        for playlist_source in NM3U8DL_PLAYLIST_GROUPS.get(
            source_group,
            [],
        ):
            configured_url, _, _ = _normalize_nm3u8dl_playlist_source_config(
                playlist_source
            )

            if configured_url == target_url:
                return source_group

    return ""


def normalize_nm3u8dl_match_text(value: str) -> str:
    return source_matching.normalize_match_text(value)


def _get_nm3u8dl_match_definition():
    return source_matching.make_match_definition(
        mode=get_nm3u8dl_playlist_match_mode(),
        primary=NM3U8DL_PLAYLIST_PRIMARY_PHRASES,
        required=NM3U8DL_PLAYLIST_REQUIRED_QUALIFIERS,
        rejected=NM3U8DL_PLAYLIST_REJECTED_QUALIFIERS,
        preferred=NM3U8DL_PLAYLIST_PREFERRED_QUALIFIERS,
    )


def get_nm3u8dl_playlist_match_rules():
    definition = _get_nm3u8dl_match_definition()
    return (
        [list(group) for group in definition.primary_groups],
        [list(group) for group in definition.required_qualifier_groups],
        [list(group) for group in definition.rejected_qualifier_groups],
        [list(group) for group in definition.preferred_qualifier_groups],
    )


def get_nm3u8dl_playlist_match_description() -> str:
    return source_matching.describe_match_definition(
        _get_nm3u8dl_match_definition()
    )

class NM3U8DLPlaylistFetchError(RuntimeError):
    """One configured playlist could not be fetched/read."""

    def __init__(
        self,
        playlist_url: str,
        error: Exception,
        *,
        fetch_duration_sec: Optional[float] = None,
    ):
        response_detail = ""

        if isinstance(error, HTTPError):
            http_status = getattr(error, "code", None)
            http_reason = str(
                getattr(error, "reason", "") or ""
            ).strip()

            if http_reason.casefold() in ("none", "<none>"):
                http_reason = ""

            try:
                response_body = error.read(1024).decode(
                    "utf-8",
                    errors="replace",
                )
                response_detail = " ".join(response_body.split()).strip()
                if len(response_detail) > 200:
                    response_detail = response_detail[:197] + "..."
            except Exception:
                response_detail = ""

            if (
                http_status == 530
                and re.search(
                    r"\b(?:error code:\s*)?1033\b",
                    response_detail,
                    re.IGNORECASE,
                )
            ):
                detail = (
                    "HTTP 530 / Cloudflare 1033 — provider's Cloudflare "
                    "Tunnel unavailable; provider/server-side issue; retry later"
                )

            elif http_status == 404:
                detail = (
                    "HTTP 404 Not Found — playlist URL/path not found; "
                    "source-side issue"
                )

            elif http_status == 401:
                detail = (
                    "HTTP 401 Unauthorized — playlist authorization "
                    "rejected/required; source-auth issue"
                )

            elif http_status == 403:
                detail = (
                    "HTTP 403 Forbidden — playlist access rejected; "
                    "auth/route/source-policy cause unclear"
                )

            elif http_status == 429:
                detail = (
                    "HTTP 429 Too Many Requests — source rate-limited; "
                    "retry later"
                )

            elif (
                http_status is not None
                and 500 <= int(http_status) <= 599
            ):
                status_text = f"HTTP {http_status} {http_reason}".strip()
                detail = (
                    f"{status_text} — upstream/provider server error; "
                    "retry later"
                )

            else:
                detail = (
                    f"HTTP {http_status} {http_reason}".strip()
                    if http_status is not None
                    else str(error).strip() or repr(error)
                )

                normalized_reason = re.sub(
                    r"[^a-z0-9]+",
                    " ",
                    http_reason.casefold(),
                ).strip()
                normalized_response = re.sub(
                    r"[^a-z0-9]+",
                    " ",
                    response_detail.casefold(),
                ).strip()

                if (
                    response_detail
                    and normalized_response
                    and normalized_response != normalized_reason
                    and normalized_response
                    != f"{http_status} {normalized_reason}".strip()
                ):
                    detail += f" — response: {response_detail}"

        else:
            detail = str(error).strip() or repr(error)
            lowered_detail = detail.casefold()

            if (
                isinstance(error, TimeoutError)
                or "timed out" in lowered_detail
            ):
                detail = (
                    "connection timed out — route/source did not respond; "
                    "side unclear"
                )

            elif any(
                marker in lowered_detail
                for marker in (
                    "getaddrinfo failed",
                    "name or service not known",
                    "nodename nor servname provided",
                )
            ):
                detail = (
                    "DNS/host lookup failed — local DNS, route, or "
                    "source hostname issue; side unclear"
                )

            elif "connection refused" in lowered_detail:
                detail = (
                    "connection refused — remote host reachable but not "
                    "accepting connection; source/server-side issue likely"
                )

            elif "network is unreachable" in lowered_detail:
                detail = (
                    "network unreachable — local/VPN/route issue likely"
                )

            elif (
                "certificate verify failed" in lowered_detail
                or "ssl:" in lowered_detail
                or "tls" in lowered_detail
            ):
                detail = (
                    f"{detail} — TLS/certificate failure; local trust, "
                    "interception, or source configuration issue"
                )

        super().__init__(
            f"{type(error).__name__}: {detail}"
        )
        self.playlist_url = playlist_url
        self.connectivity_error = (
            not isinstance(error, HTTPError)
            and isinstance(error, (URLError, TimeoutError, OSError))
        )
        self.fetch_duration_sec = fetch_duration_sec
        self.http_status = (
            int(getattr(error, "code"))
            if getattr(error, "code", None) is not None
            else None
        )
        self.response_detail = response_detail
        try:
            self.final_url = str(error.geturl() or "").strip()
        except Exception:
            self.final_url = ""



def get_nm3u8dl_playlist_user_agent(playlist_url: str) -> str:
    target_url = str(playlist_url or "").strip()
    source_bucket = get_nm3u8dl_playlist_source_bucket()
    profile_name = "DEFAULT"

    for source_group in ("COMMON", source_bucket):
        for playlist_source in NM3U8DL_PLAYLIST_GROUPS.get(
            source_group,
            [],
        ):
            configured_url, configured_profile, _ = (
                _normalize_nm3u8dl_playlist_source_config(
                    playlist_source
                )
            )

            if (
                configured_url == target_url
                and configured_profile
            ):
                profile_name = configured_profile
                break

        if profile_name != "DEFAULT":
            break

    user_agent = NM3U8DL_PLAYLIST_USER_AGENTS.get(profile_name)

    if user_agent is None:
        raise RuntimeError(
            f'Unknown playlist user-agent profile "{profile_name}" '
            f'for {target_url}'
        )

    return user_agent


def get_nm3u8dl_stream_user_agent(playlist_url: str) -> str:
    target_url = str(playlist_url or "").strip()
    source_bucket = get_nm3u8dl_playlist_source_bucket()
    profile_name = ""

    for source_group in ("COMMON", source_bucket):
        for playlist_source in NM3U8DL_PLAYLIST_GROUPS.get(
            source_group,
            [],
        ):
            configured_url, _, configured_profile = (
                _normalize_nm3u8dl_playlist_source_config(
                    playlist_source
                )
            )

            if (
                configured_url == target_url
                and configured_profile
            ):
                profile_name = configured_profile
                break

        if profile_name:
            break

    if not profile_name:
        return ""

    user_agent = NM3U8DL_PLAYLIST_USER_AGENTS.get(profile_name)

    if user_agent is None:
        raise RuntimeError(
            f'Unknown stream user-agent profile "{profile_name}" '
            f'for {target_url}'
        )

    return user_agent


def _get_nm3u8dl_curl_binary() -> str:
    return "curl.exe" if os.name == "nt" else "curl"


def _run_nm3u8dl_curl_get_text(
    url: str,
    user_agent: str,
    *,
    headers: Optional[dict] = None,
    timeout_sec: float = 25,
) -> tuple:
    """
    Fetch one small text resource with real curl and return
    (text, http_status, final_url, raw_bytes).

    Used only for the DRMLive path whose server behavior has been proven
    manually with curl but not with Python urllib.
    """
    status_marker = b"\n__RECORDER_CURL_HTTP_STATUS__:"
    final_url_marker = b"\n__RECORDER_CURL_FINAL_URL__:"

    curl_args = [
        _get_nm3u8dl_curl_binary(),
        "-sS",
        "-L",
        "--compressed",
        "-A",
        str(user_agent),
    ]

    for name, value in (headers or {}).items():
        header_name = str(name or "").strip()

        if (
            not header_name
            or value is None
            or header_name.casefold() == "user-agent"
        ):
            continue

        curl_args.extend([
            "-H",
            f"{header_name}: {str(value)}",
        ])

    curl_args.extend([
        "-w",
        (
            "\n__RECORDER_CURL_HTTP_STATUS__:%{http_code}"
            "\n__RECORDER_CURL_FINAL_URL__:%{url_effective}"
        ),
        str(url),
    ])

    try:
        result = subprocess.run(
            curl_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=float(timeout_sec),
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        log_timeout_exception(
            error,
            "curl",
            timeout_sec,
            context="GET",
            source=url,
        )
        raise

    if result.returncode != 0:
        stderr_text = result.stderr.decode(
            "utf-8",
            errors="replace",
        ).strip()
        raise RuntimeError(
            "curl GET failed"
            + (f": {stderr_text}" if stderr_text else "")
        )

    body, separator, trailer = result.stdout.rpartition(
        status_marker
    )

    if not separator:
        raise RuntimeError(
            "curl GET returned no HTTP status marker"
        )

    status_bytes, separator, final_url_bytes = trailer.partition(
        final_url_marker
    )

    if not separator:
        raise RuntimeError(
            "curl GET returned no final-URL marker"
        )

    try:
        http_status = int(status_bytes.strip())
    except Exception as error:
        raise RuntimeError(
            "curl GET returned an invalid HTTP status"
        ) from error

    final_url = final_url_bytes.decode(
        "utf-8",
        errors="replace",
    ).strip()

    if not (200 <= http_status < 300):
        raise HTTPError(
            final_url or str(url),
            http_status,
            f"HTTP Error {http_status}",
            None,
            None,
        )

    text = body.decode(
        "utf-8-sig",
        errors="replace",
    )

    return text, http_status, final_url, body


def _run_nm3u8dl_curl_status_request(
    args: List[str],
    *,
    timeout_sec: float = 25,
) -> int:
    """Run curl with the response body discarded and return only HTTP status."""
    request_url = next(
        (
            str(value)
            for value in reversed(args)
            if str(value).strip().lower().startswith(("http://", "https://"))
        ),
        "",
    )

    try:
        result = subprocess.run(
            [
                _get_nm3u8dl_curl_binary(),
                "-sS",
                "-L",
                *[str(value) for value in args],
                "-o",
                os.devnull,
                "-w",
                "%{http_code}",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=float(timeout_sec),
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        log_timeout_exception(
            error,
            "curl",
            timeout_sec,
            context="status request",
            source=request_url,
        )
        raise

    if result.returncode != 0:
        stderr_text = result.stderr.decode(
            "utf-8",
            errors="replace",
        ).strip()
        raise RuntimeError(
            "curl request failed"
            + (f": {stderr_text}" if stderr_text else "")
        )

    status_text = result.stdout.decode(
        "ascii",
        errors="replace",
    ).strip()

    try:
        return int(status_text)
    except Exception as error:
        raise RuntimeError(
            f"curl returned invalid HTTP status {status_text!r}"
        ) from error


def _is_nm3u8dl_drmlive_host(url: str) -> bool:
    try:
        host = str(urlparse(str(url or "")).hostname or "").lower()
    except Exception:
        return False

    return host == "drmlive.net" or host.endswith(".drmlive.net")


# Provider-specific playlist activation. Extend only for separately proven providers.
NM3U8DL_PLAYLIST_ACTIVATION_PROVIDERS = {
    "la.drmlive.net": "DRMLIVE_CLEARKEY_IP",
}


def get_nm3u8dl_playlist_activation_provider(playlist_url: str) -> str:
    try:
        host = str(urlparse(str(playlist_url or "")).hostname or "").lower()
    except Exception:
        return ""

    return NM3U8DL_PLAYLIST_ACTIVATION_PROVIDERS.get(host, "")


# Generic JSON-playlist adapter. JSON source URLs remain entirely in the user
# playlist-group configuration; the recorder recognizes supported JSON vocabulary
# by field/container aliases rather than by provider name, host, or URL.
NM3U8DL_JSON_RECORD_LIST_ALIASES = (
    "channels",
    "streams",
    "items",
    "entries",
    "data",
)

NM3U8DL_JSON_FIELD_ALIASES = {
    "name": (
        "name",
        "channel_name",
        "channel",
        "title",
    ),
    "stream_url": (
        "stream_url",
        "stream",
        "url",
        "link",
    ),
    "id": (
        "id",
        "channel_id",
        "tvg_id",
        "tvg-id",
    ),
    "group_title": (
        "group_title",
        "group",
        "category",
    ),
    "key_id": (
        "key_id",
        "kid",
    ),
    "key": (
        "key",
    ),
    "license_key": (
        "license_key",
        "drm_key",
        "clearkey",
    ),
}

# Convenience aliases for common per-record HTTP headers. A JSON record may
# alternatively provide a generic headers/http_headers/request_headers object.
# The explicit header object wins if both forms supply the same header.
NM3U8DL_JSON_HEADER_FIELD_ALIASES = {
    "Cookie": (
        "cookie",
        "cookies",
    ),
    "User-Agent": (
        "user_agent",
        "user-agent",
        "useragent",
    ),
    "Origin": (
        "origin",
    ),
    "Referer": (
        "referer",
        "referrer",
    ),
    "Authorization": (
        "authorization",
        "auth_header",
    ),
}

NM3U8DL_JSON_HEADER_OBJECT_ALIASES = (
    "headers",
    "http_headers",
    "request_headers",
)


def _normalize_nm3u8dl_json_field_name(value: str) -> str:
    return (
        str(value or "")
        .strip()
        .casefold()
        .replace("-", "_")
        .replace(" ", "_")
    )


def _get_nm3u8dl_json_alias_value(record: dict, aliases) -> object:
    if not isinstance(record, dict):
        return None

    normalized_record = {}

    for key, value in record.items():
        normalized_key = _normalize_nm3u8dl_json_field_name(key)

        if normalized_key and normalized_key not in normalized_record:
            normalized_record[normalized_key] = value

    for alias in aliases:
        normalized_alias = _normalize_nm3u8dl_json_field_name(alias)

        if normalized_alias in normalized_record:
            return normalized_record[normalized_alias]

    return None


def _get_nm3u8dl_json_text_value(record: dict, aliases) -> str:
    value = _get_nm3u8dl_json_alias_value(record, aliases)

    if value is None or isinstance(value, (dict, list, tuple, set)):
        return ""

    return (
        str(value)
        .replace("\r", " ")
        .replace("\n", " ")
        .strip()
    )


def _find_nm3u8dl_json_records(data) -> tuple:
    if isinstance(data, list):
        return data, "$"

    if not isinstance(data, dict):
        raise RuntimeError(
            "JSON playlist root must be an object or array"
        )

    normalized_root = {
        _normalize_nm3u8dl_json_field_name(key): (key, value)
        for key, value in data.items()
        if _normalize_nm3u8dl_json_field_name(key)
    }

    for alias in NM3U8DL_JSON_RECORD_LIST_ALIASES:
        normalized_alias = _normalize_nm3u8dl_json_field_name(alias)
        matched = normalized_root.get(normalized_alias)

        if matched is None:
            continue

        original_key, value = matched

        if isinstance(value, list):
            return value, str(original_key)

    raise RuntimeError(
        "JSON playlist contains no recognized record list "
        f"({', '.join(NM3U8DL_JSON_RECORD_LIST_ALIASES)})"
    )


def _escape_nm3u8dl_json_extinf_attribute(value: str) -> str:
    # M3U attributes are quoted. Preserve the human-readable value while making
    # an embedded quote unable to terminate the synthetic attribute early.
    return str(value or "").replace('"', "'").strip()


def _get_nm3u8dl_json_record_headers(record: dict) -> dict:
    headers = {}

    # Convenience scalar fields first.
    for header_name, aliases in (
        NM3U8DL_JSON_HEADER_FIELD_ALIASES.items()
    ):
        value = _get_nm3u8dl_json_text_value(record, aliases)

        if value:
            headers[header_name] = value

    # A dedicated header object is more explicit and therefore higher
    # precedence than the convenience scalar aliases above.
    header_object = _get_nm3u8dl_json_alias_value(
        record,
        NM3U8DL_JSON_HEADER_OBJECT_ALIASES,
    )

    if isinstance(header_object, dict):
        for name, value in header_object.items():
            header_name = str(name or "").strip()

            if (
                not header_name
                or value is None
                or isinstance(value, (dict, list, tuple, set))
            ):
                continue

            header_value = (
                str(value)
                .replace("\r", " ")
                .replace("\n", " ")
                .strip()
            )

            if not header_value:
                continue

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


def _build_nm3u8dl_m3u_entry_from_json_record(record: dict) -> list:
    name = _get_nm3u8dl_json_text_value(
        record,
        NM3U8DL_JSON_FIELD_ALIASES["name"],
    )
    stream_url = _get_nm3u8dl_json_text_value(
        record,
        NM3U8DL_JSON_FIELD_ALIASES["stream_url"],
    )

    if not name or not stream_url:
        return []

    tvg_id = _get_nm3u8dl_json_text_value(
        record,
        NM3U8DL_JSON_FIELD_ALIASES["id"],
    )
    group_title = _get_nm3u8dl_json_text_value(
        record,
        NM3U8DL_JSON_FIELD_ALIASES["group_title"],
    )

    attributes = []

    if tvg_id:
        attributes.append(
            f'tvg-id="{_escape_nm3u8dl_json_extinf_attribute(tvg_id)}"'
        )

    attributes.append(
        f'tvg-name="{_escape_nm3u8dl_json_extinf_attribute(name)}"'
    )

    if group_title:
        attributes.append(
            f'group-title="{_escape_nm3u8dl_json_extinf_attribute(group_title)}"'
        )

    extinf = (
        "#EXTINF:-1 "
        + " ".join(attributes)
        + f",{name}"
    )

    lines = [extinf]

    license_key = _get_nm3u8dl_json_text_value(
        record,
        NM3U8DL_JSON_FIELD_ALIASES["license_key"],
    )

    if not license_key:
        key_id = _get_nm3u8dl_json_text_value(
            record,
            NM3U8DL_JSON_FIELD_ALIASES["key_id"],
        )
        key_value = _get_nm3u8dl_json_text_value(
            record,
            NM3U8DL_JSON_FIELD_ALIASES["key"],
        )

        if key_id and key_value:
            license_key = f"{key_id}:{key_value}"

    if license_key:
        lines.extend([
            "#KODIPROP:inputstream.adaptive.license_type=clearkey",
            "#KODIPROP:inputstream.adaptive.license_key=" + license_key,
        ])

    headers = _get_nm3u8dl_json_record_headers(record)

    if headers:
        lines.append(
            "#EXTHTTP:"
            + json.dumps(
                headers,
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )

    lines.append(stream_url)
    return lines


def adapt_nm3u8dl_json_playlist_text(playlist_text: str) -> tuple:
    """
    Convert a recognized JSON playlist into synthetic M3U text.

    Returns (playlist_text, metadata). Non-JSON input is returned unchanged with
    metadata indicating no adaptation. JSON is recognized by content, never URL.
    """
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
        raise RuntimeError(
            "Playlist response looks like JSON but could not be parsed "
            f"(line {error.lineno}, column {error.colno})"
        ) from error

    records, record_container = _find_nm3u8dl_json_records(data)
    output_lines = ["#EXTM3U"]
    usable_record_count = 0
    skipped_record_count = 0

    for record in records:
        if not isinstance(record, dict):
            skipped_record_count += 1
            continue

        entry_lines = _build_nm3u8dl_m3u_entry_from_json_record(record)

        if not entry_lines:
            skipped_record_count += 1
            continue

        output_lines.extend(entry_lines)
        usable_record_count += 1

    if usable_record_count == 0:
        raise RuntimeError(
            "JSON playlist contained no usable records with recognized "
            "name and stream URL fields"
        )

    synthetic_text = "\n".join(output_lines) + "\n"

    return synthetic_text, {
        "adapted": True,
        "source_format": "json",
        "record_container": record_container,
        "record_count": len(records),
        "usable_record_count": usable_record_count,
        "skipped_record_count": skipped_record_count,
        "synthetic_m3u_sha256": hashlib.sha256(
            synthetic_text.encode("utf-8")
        ).hexdigest(),
    }


def fetch_nm3u8dl_playlist_text(
    playlist_url: str,
    *,
    stop_requested: Optional[Callable[[], bool]] = None,
    include_metadata: bool = False,
):
    fetch_started = time.monotonic()
    user_agent = get_nm3u8dl_playlist_user_agent(playlist_url)

    # DRMLive is deliberately fetched with real curl. That is the transport
    # used by the only activation sequence that has actually unlocked TEN5.
    if (
        get_nm3u8dl_playlist_activation_provider(playlist_url)
        == "DRMLIVE_CLEARKEY_IP"
    ):
        try:
            if stop_requested is not None and stop_requested():
                raise RuntimeError(
                    "Playlist scan cancelled by stop request"
                )

            text, http_status, final_url, raw_content = (
                _run_nm3u8dl_curl_get_text(
                    playlist_url,
                    user_agent,
                )
            )

            if not include_metadata:
                return text

            metadata = {
                "configured_url": playlist_url,
                "final_url": final_url,
                "http_status": http_status,
                "content_size_bytes": len(raw_content),
                "content_sha256": hashlib.sha256(
                    raw_content
                ).hexdigest(),
                "fetch_duration_sec": round(
                    time.monotonic() - fetch_started,
                    6,
                ),
                "transport": "curl",
            }
            metadata["redirected"] = bool(
                final_url and final_url != playlist_url
            )
            return text, metadata

        except Exception as error:
            if stop_requested is not None and stop_requested():
                raise RuntimeError(
                    "Playlist scan cancelled by stop request"
                ) from error

            raise NM3U8DLPlaylistFetchError(
                playlist_url,
                error,
                fetch_duration_sec=round(
                    time.monotonic() - fetch_started,
                    6,
                ),
            ) from error

    request = Request(
        playlist_url,
        headers={"User-Agent": user_agent},
    )

    try:
        with urlopen(request, timeout=20) as response:
            chunks = []
            read_chunk = getattr(response, "read1", response.read)

            while True:
                if stop_requested is not None and stop_requested():
                    raise RuntimeError("Playlist scan cancelled by stop request")

                chunk = read_chunk(64 * 1024)
                if not chunk:
                    break

                chunks.append(chunk)

            raw_content = b"".join(chunks)
            text = raw_content.decode(
                "utf-8-sig",
                errors="replace",
            )

            if not include_metadata:
                return text

            metadata = {
                "configured_url": playlist_url,
                "final_url": str(response.geturl() or "").strip(),
                "http_status": int(getattr(response, "status", 0) or response.getcode() or 0) or None,
                "content_size_bytes": len(raw_content),
                "content_sha256": hashlib.sha256(raw_content).hexdigest(),
                "fetch_duration_sec": round(time.monotonic() - fetch_started, 6),
            }
            metadata["redirected"] = bool(
                metadata["final_url"]
                and metadata["final_url"] != playlist_url
            )
            return text, metadata

    except Exception as error:
        log_timeout_exception(
            error,
            "HTTP",
            20,
            context="playlist fetch",
            source=playlist_url,
        )

        if stop_requested is not None and stop_requested():
            raise RuntimeError(
                "Playlist scan cancelled by stop request"
            ) from error

        # This boundary is intentionally broad: any ordinary exception raised
        # while contacting/reading this ONE external playlist is a source
        # failure, not a reason to kill the entire recorder.
        raise NM3U8DLPlaylistFetchError(
            playlist_url,
            error,
            fetch_duration_sec=round(time.monotonic() - fetch_started, 6),
        ) from error


def parse_nm3u8dl_extinf_metadata(extinf: str) -> dict:
    return source_discovery.parse_extinf_metadata(extinf)


def find_nm3u8dl_playlist_activation_entry(playlist_text: str) -> dict:
    """Return the explicit playlist-activation stream + ClearKey URL, if declared."""
    lines = [
        line.strip()
        for line in str(playlist_text or "").splitlines()
    ]

    expected_group = normalize_nm3u8dl_match_text(
        "1-Playlist-Activation"
    )
    expected_title = normalize_nm3u8dl_match_text(
        "Activate Playlist"
    )
    license_prefix = (
        "#KODIPROP:inputstream.adaptive.license_key="
    )
    license_type_prefix = (
        "#KODIPROP:inputstream.adaptive.license_type="
    )

    for index, line in enumerate(lines):
        if not line.startswith("#EXTINF:"):
            continue

        metadata = parse_nm3u8dl_extinf_metadata(line)

        if (
            normalize_nm3u8dl_match_text(
                metadata.get("group_title") or ""
            )
            != expected_group
            or normalize_nm3u8dl_match_text(
                metadata.get("entry_title") or ""
            )
            != expected_title
        ):
            continue

        option_lines = []

        # DRMLive declares the activation KODIPROP lines immediately before
        # EXTINF, unlike ordinary entries where options follow EXTINF.
        preceding = []
        previous_index = index - 1

        while previous_index >= 0:
            previous_line = lines[previous_index]

            if previous_line.startswith(
                ("http://", "https://", "#EXTINF:")
            ):
                break

            if previous_line:
                preceding.append(previous_line)

            previous_index -= 1

        option_lines.extend(reversed(preceding))

        activation_url = ""

        for following_line in lines[index + 1:]:
            if following_line.startswith("#EXTINF:"):
                break

            if following_line.startswith(
                ("http://", "https://")
            ):
                activation_url = following_line
                break

            if following_line:
                option_lines.append(following_line)

        license_url = ""
        license_type = ""

        for option_line in option_lines:
            if option_line.startswith(license_prefix):
                candidate = option_line[
                    len(license_prefix):
                ].strip()

                if candidate.startswith(
                    ("http://", "https://")
                ):
                    license_url = candidate

            elif option_line.startswith(
                license_type_prefix
            ):
                license_type = option_line[
                    len(license_type_prefix):
                ].strip()

        return {
            "activation_url": activation_url,
            "license_url": license_url,
            "license_type": license_type,
        }

    return {
        "activation_url": "",
        "license_url": "",
        "license_type": "",
    }


def _extract_nm3u8dl_activation_kids(
    activation_mpd_text: str
) -> List[str]:
    """Extract base64url ClearKey KIDs from the activation MPD."""
    try:
        root = ET.fromstring(activation_mpd_text)
    except ET.ParseError as error:
        raise RuntimeError(
            "Activation MPD XML could not be parsed"
        ) from error

    kid_hex_values = []

    for element in root.iter():
        for attribute_name, attribute_value in (
            element.attrib.items()
        ):
            if not (
                attribute_name == "default_KID"
                or attribute_name.endswith(
                    "}default_KID"
                )
            ):
                continue

            kid_hex = re.sub(
                r"[^0-9A-Fa-f]",
                "",
                str(attribute_value),
            ).lower()

            if (
                len(kid_hex) == 32
                and kid_hex not in kid_hex_values
            ):
                kid_hex_values.append(kid_hex)

    if not kid_hex_values:
        raise RuntimeError(
            "Activation MPD contains no usable default_KID"
        )

    return [
        base64.urlsafe_b64encode(
            bytes.fromhex(kid_hex)
        )
        .decode("ascii")
        .rstrip("=")
        for kid_hex in kid_hex_values
    ]


def _find_nm3u8dl_activation_verification_target(
    playlist_text: str
) -> dict:
    """
    Prefer the proven matching DRMLive HLS as activation proof. If no matching
    DRMLive HLS exists, fall back to a matching DRMLive DASH wrapper and carry
    its playlist-provided headers so the wrapper can resolve correctly.
    """
    try:
        entries = find_nm3u8dl_playlist_entries(
            playlist_text
        )
    except Exception:
        return {"url": "", "kind": "", "headers": {}}

    dash_target = None

    for entry in entries:
        stream_url = str(
            entry.get("stream_url") or ""
        ).strip()

        if not _is_nm3u8dl_drmlive_host(stream_url):
            continue

        try:
            path = str(
                urlparse(stream_url).path or ""
            ).lower()
        except Exception:
            path = stream_url.lower()

        if path.endswith(".m3u8"):
            return {
                "url": stream_url,
                "kind": "HLS",
                "headers": {},
            }

        if path.endswith(".mpd") and dash_target is None:
            try:
                normalized_entry = normalize_nm3u8dl_playlist_entry(
                    entry
                )
                dash_url = normalized_entry["stream_url"]
                dash_headers = dict(
                    normalized_entry.get("headers") or {}
                )
            except Exception:
                dash_url = stream_url
                dash_headers = {}

            dash_target = {
                "url": dash_url,
                "kind": "DASH",
                "headers": dash_headers,
            }

    return dash_target or {
        "url": "",
        "kind": "",
        "headers": {},
    }


def activate_nm3u8dl_playlist_if_present(
    playlist_url: str,
    playlist_text: str,
    *,
    stop_requested: Optional[Callable[[], bool]] = None,
) -> dict:
    """
    Apply DRMLive IP/ClearKey activation only when the matching HLS proves
    authorization is missing. No GET is ever sent to the ClearKey endpoint.
    """
    provider = get_nm3u8dl_playlist_activation_provider(playlist_url)
    activation_entry = (
        find_nm3u8dl_playlist_activation_entry(playlist_text)
        if provider == "DRMLIVE_CLEARKEY_IP"
        else {
            "activation_url": "",
            "license_url": "",
            "license_type": "",
        }
    )
    activation_url = activation_entry["activation_url"]
    license_url = activation_entry["license_url"]
    license_type = activation_entry["license_type"]

    metadata = {
        "detected": bool(activation_url),
        "attempted": False,
        "succeeded": False,
        "blocking_failure": False,
        "reactivated": False,
        "transport": "",
        "activation_host": (
            urlparse(activation_url).netloc
            if activation_url
            else ""
        ),
        "license_host": (
            urlparse(license_url).netloc
            if license_url
            else ""
        ),
        "precheck_http_status": None,
        "activation_http_status": None,
        "activation_final_url": "",
        "license_http_status": None,
        "request_body_bytes": None,
        "verification_url": "",
        "verification_http_status": None,
        "http_status": None,
        "final_url": "",
        "error": "",
    }

    if not activation_url:
        return metadata

    if stop_requested is not None and stop_requested():
        raise RuntimeError("Playlist scan cancelled by stop request")

    user_agent = get_nm3u8dl_playlist_user_agent(playlist_url)
    verification_target = _find_nm3u8dl_activation_verification_target(
        playlist_text
    )
    verification_url = str(
        verification_target.get("url") or ""
    ).strip()
    verification_kind = str(
        verification_target.get("kind") or ""
    ).strip().upper()
    verification_headers = dict(
        verification_target.get("headers") or {}
    )
    verification_label = verification_kind or "stream"

    metadata["verification_url"] = verification_url
    metadata["verification_kind"] = verification_kind

    if not verification_url:
        log(
            "DRMLive authorization: no matching DRMLive HLS/DASH for this target "
            "— activation skipped."
        )
        return metadata

    verification_args = ["--compressed"]

    if verification_kind == "HLS":
        # Preserve the proven HLS verification behavior exactly.
        verification_args.extend([
            "-A",
            user_agent,
        ])
    else:
        safe_headers = get_nm3u8dl_ascii_safe_request_headers(
            verification_headers,
            emit_logs=False,
        )
        verification_user_agent = user_agent

        for name, value in safe_headers.items():
            if str(name).casefold() == "user-agent":
                verification_user_agent = str(value)
                break

        verification_args.extend([
            "-A",
            verification_user_agent,
        ])

        for name, value in safe_headers.items():
            if str(name).casefold() == "user-agent":
                continue

            verification_args.extend([
                "-H",
                f"{name}: {value}",
            ])

    verification_args.append(verification_url)

    # Reuse an already-authorized IP and never POST again. Only 401/403 is
    # allowed to trigger activation; every other failure stays ordinary source
    # handling. HLS remains preferred whenever a matching HLS entry exists.
    try:
        precheck_status = _run_nm3u8dl_curl_status_request(
            verification_args
        )
        metadata["precheck_http_status"] = precheck_status
    except Exception as error:
        log(
            f"DRMLive authorization: {verification_label} precheck failed "
            f"({type(error).__name__}) — activation not attempted; "
            "normal source handling continues.",
            level="WARN",
        )
        return metadata

    if 200 <= precheck_status < 300:
        metadata["succeeded"] = True
        metadata["verification_http_status"] = precheck_status
        log(
            "DRMLive authorization: already active — "
            f"{verification_label} HTTP {precheck_status}; activation skipped."
        )
        return metadata

    if precheck_status not in (401, 403):
        log(
            "DRMLive authorization: "
            f"{verification_label} precheck HTTP {precheck_status} — "
            "not an activation condition; activation skipped and normal "
            "source handling continues.",
            level="WARN",
        )
        return metadata

    log(
        "DRMLive authorization: "
        f"{verification_label} HTTP {precheck_status} — "
        "activation required; attempting once."
    )

    if not license_url:
        metadata["blocking_failure"] = True
        metadata["error"] = (
            f"DRMLive {verification_label} requires authorization but activation "
            "entry has no HTTP ClearKey license URL"
        )
        return metadata

    if stop_requested is not None and stop_requested():
        raise RuntimeError("Playlist scan cancelled by stop request")

    metadata["attempted"] = True
    metadata["transport"] = "curl"

    try:
        # Step 1: fetch the fresh activation MPD with real curl + OTT UA.
        (
            activation_mpd_text,
            activation_status,
            activation_final_url,
            _,
        ) = _run_nm3u8dl_curl_get_text(
            activation_url,
            user_agent,
        )

        metadata["activation_http_status"] = activation_status
        metadata["activation_final_url"] = activation_final_url
        metadata["final_url"] = activation_final_url

        if stop_requested is not None and stop_requested():
            raise RuntimeError("Playlist scan cancelled by stop request")

        # Step 2: derive the KID(s) from that fresh MPD.
        request_kids = _extract_nm3u8dl_activation_kids(
            activation_mpd_text
        )
        request_body = json.dumps(
            {
                "kids": request_kids,
                "type": "temporary",
            },
            separators=(",", ":"),
        )
        metadata["request_body_bytes"] = len(
            request_body.encode("utf-8")
        )

    except Exception as error:
        if stop_requested is not None and stop_requested():
            raise RuntimeError(
                "Playlist scan cancelled by stop request"
            ) from error

        metadata["blocking_failure"] = True
        metadata["error"] = (
            f"{type(error).__name__}: "
            f"{str(error).strip() or repr(error)}"
        )
        return metadata

    # Step 3: exactly one ClearKey POST. Never GET or store the response body.
    # HLS verification remains authoritative even if curl cannot cleanly report
    # the POST result, because the POST may still have reached the server.
    license_error = ""
    try:
        license_status = _run_nm3u8dl_curl_status_request(
            [
                "-A",
                "curl/8.21.0",
                "-H",
                "Content-Type: application/json",
                "-H",
                "Accept: application/json",
                "--data-raw",
                request_body,
                license_url,
            ]
        )
        metadata["license_http_status"] = license_status
        metadata["http_status"] = license_status
    except Exception as error:
        license_error = (
            f"{type(error).__name__}: "
            f"{str(error).strip() or repr(error)}"
        )

    if stop_requested is not None and stop_requested():
        raise RuntimeError("Playlist scan cancelled by stop request")

    # Step 4: the same matching DRMLive stream decides whether activation
    # succeeded. HLS remains authoritative when present; DASH is used only when
    # no matching HLS exists.
    try:
        verification_status = _run_nm3u8dl_curl_status_request(
            verification_args
        )
        metadata["verification_http_status"] = verification_status
    except Exception as error:
        metadata["blocking_failure"] = True
        metadata["error"] = (
            "DRMLive activation verification failed — "
            f"{type(error).__name__}: "
            f"{str(error).strip() or repr(error)}"
        )
        return metadata

    if 200 <= verification_status < 300:
        metadata["succeeded"] = True
        metadata["reactivated"] = True
        post_result = (
            f"HTTP {metadata['license_http_status']}"
            if metadata["license_http_status"] is not None
            else (license_error or "status unavailable")
        )
        log(
            "DRMLive authorization: activation verified — "
            f"POST {post_result}; {verification_label} HTTP {verification_status}."
        )
        return metadata

    metadata["blocking_failure"] = True
    post_result = (
        f"HTTP {metadata['license_http_status']}"
        if metadata["license_http_status"] is not None
        else (license_error or "status unavailable")
    )
    metadata["error"] = (
        f"DRMLive activation did not restore {verification_label} access — "
        f"POST {post_result}; verification HTTP {verification_status}"
    )
    log(
        "DRMLive authorization: activation failed — "
        f"POST {post_result}; {verification_label} verification "
        f"HTTP {verification_status}.",
        level="WARN",
    )
    return metadata

def find_nm3u8dl_playlist_entries(
    playlist_text: str
) -> List[dict]:
    request = SourceAcquisitionRequest(
        match=_get_nm3u8dl_match_definition(),
        target_name=BASE_NAME,
    )
    result = source_discovery.discover_playlist_text(
        playlist_text,
        request,
    )

    matches = [
        {
            "extinf": candidate.extinf,
            "tvg_name": candidate.tvg_name,
            "group_title": candidate.group_title,
            "entry_title": candidate.entry_title,
            "option_lines": list(candidate.option_lines),
            "stream_url": candidate.stream_url,
            "preferred_qualifier_score": candidate.preferred_qualifier_score,
        }
        for candidate in result.candidates
    ]

    if not matches:
        raise RuntimeError(
            "No playlist entry matched "
            f"{get_nm3u8dl_playlist_match_description()}"
        )

    return matches


def _extract_nm3u8dl_auth_expiries(value: str) -> List[int]:
    return list(source_quality.extract_auth_expiries(value))


def _merge_nm3u8dl_auth_expiries(*expiries) -> Optional[int]:
    return source_quality.merge_auth_expiries(*expiries)


def get_nm3u8dl_expiry_source(
    url_header_expiry: Optional[int],
    manifest_expiry: Optional[int],
    resource_expiry: Optional[int] = None,
) -> str:
    if resource_expiry is None:
        if url_header_expiry is None and manifest_expiry is None:
            return ""

        if url_header_expiry is None:
            return "manifest"

        if manifest_expiry is None:
            return "URL/header"

        if manifest_expiry < url_header_expiry:
            return "manifest; URL/header also present"

        if url_header_expiry < manifest_expiry:
            return "URL/header; manifest also present"

        return "manifest + URL/header"

    sources = [
        ("URL/header", url_header_expiry),
        ("manifest", manifest_expiry),
        ("media URL", resource_expiry),
    ]
    present = [
        (label, int(value))
        for label, value in sources
        if value is not None
    ]

    if not present:
        return ""

    earliest = min(value for _, value in present)
    earliest_labels = [
        label
        for label, value in present
        if value == earliest
    ]
    other_labels = [
        label
        for label, value in present
        if value != earliest
    ]

    primary = " + ".join(earliest_labels)
    if not other_labels:
        return primary

    return f"{primary}; {' + '.join(other_labels)} also present"


def format_nm3u8dl_expiry_with_source(
    expiry: Optional[float],
    expiry_source: str = "",
) -> str:
    if expiry is None:
        return "unknown"

    text = datetime.fromtimestamp(
        float(expiry)
    ).strftime("%Y-%m-%d %H:%M:%S")

    source = str(expiry_source or "").strip()

    if source:
        text += f" [{source}]"

    return text
    
def _get_nm3u8dl_unambiguous_text_expiry(
    value: str
) -> Optional[int]:
    expiries = set(_extract_nm3u8dl_auth_expiries(value))

    if len(expiries) != 1:
        return None

    return next(iter(expiries))


def get_nm3u8dl_auth_expiry(
    stream_url: str,
    headers: Optional[dict] = None
) -> Optional[int]:
    values = [stream_url]
    if headers:
        values.extend(str(value) for value in headers.values())
    return source_quality.extract_auth_expiry(*values)


def canonicalize_nm3u8dl_header_name(name: str) -> str:
    raw_name = str(name).strip()

    normalized = (
        raw_name
        .lower()
        .replace("_", "-")
    )

    aliases = {
        "cookie": "Cookie",
        "referer": "Referer",
        "referrer": "Referer",
        "origin": "Origin",
        "user-agent": "User-Agent",
        "useragent": "User-Agent",
        "authorization": "Authorization",
    }

    return aliases.get(normalized, raw_name)


def split_nm3u8dl_stream_url_metadata(
    stream_url: str
):
    stream_url = str(stream_url).strip()

    clean_url, separator, metadata_text = (
        stream_url.partition("|")
    )

    clean_url = clean_url.strip()

    # Some playlists use:
    #
    #   index.mpd?|cookie=...
    #
    # Here the "?" is empty and belongs to the player-style
    # pipe syntax rather than to a real URL query.
    #
    # Genuine query strings remain untouched:
    #
    #   index.mpd?hdnea=...|cookie=...
    #
    pipe_headers = {}

    if not separator:
        return clean_url, pipe_headers

    if clean_url.endswith("?"):
        clean_url = clean_url[:-1]

    for item in metadata_text.split("&"):
        item = item.strip()

        if not item:
            continue

        name, equals, value = item.partition("=")

        if not equals:
            continue

        header_name = canonicalize_nm3u8dl_header_name(
            name
        )

        if not header_name:
            continue

        pipe_headers[header_name] = value.strip()

    return clean_url, pipe_headers


def get_nm3u8dl_playlist_header_sources(
    option_lines: List[str]
):
    extvlc_headers = {}
    exthttp_headers = {}

    for line in option_lines:
        if line.startswith("#EXTHTTP:"):
            raw_json = line[len("#EXTHTTP:"):].strip()
            data = json.loads(raw_json)

            for name, value in data.items():
                if value is None:
                    continue

                header_name = (
                    canonicalize_nm3u8dl_header_name(name)
                )

                exthttp_headers[header_name] = (
                    str(value).strip()
                )

        elif line.startswith(
            "#EXTVLCOPT:http-cookie="
        ):
            extvlc_headers["Cookie"] = (
                line.split("=", 1)[1].strip()
            )

        elif line.startswith(
            "#EXTVLCOPT:http-referrer="
        ):
            extvlc_headers["Referer"] = (
                line.split("=", 1)[1].strip()
            )

        elif line.startswith(
            "#EXTVLCOPT:http-user-agent="
        ):
            extvlc_headers["User-Agent"] = (
                line.split("=", 1)[1].strip()
            )

        elif line.startswith(
            "#EXTVLCOPT:http-extra-headers="
        ):
            raw_header = line.split("=", 1)[1]

            name, separator, value = (
                raw_header.partition(":")
            )

            if separator:
                header_name = (
                    canonicalize_nm3u8dl_header_name(name)
                )

                extvlc_headers[header_name] = (
                    value.strip()
                )

    return extvlc_headers, exthttp_headers

NM3U8DL_METADATA_CONFLICTS_SEEN = set()

def merge_nm3u8dl_playlist_headers(
    extvlc_headers: dict,
    exthttp_headers: dict,
    pipe_headers: dict,
) -> dict:

    headers = {}
    header_sources = {}

    def apply_headers(
        source_name: str,
        source_headers: dict,
    ):
        for name, value in source_headers.items():
            header_name = (
                canonicalize_nm3u8dl_header_name(name)
            )

            header_key = header_name.lower()
            value = str(value).strip()

            existing_name = None

            for current_name in headers:
                if current_name.lower() == header_key:
                    existing_name = current_name
                    break

            if existing_name is not None:
                existing_value = headers[existing_name]
                existing_source = header_sources[
                    header_key
                ]

                if not value and str(existing_value).strip():
                    # A blank higher-precedence value is missing information,
                    # not an instruction to erase a useful header already found
                    # on this same playlist entry.
                    conflict_signature = (
                        header_key,
                        existing_source,
                        source_name,
                        existing_value,
                        "<blank>",
                    )

                    if (
                        conflict_signature
                        not in NM3U8DL_METADATA_CONFLICTS_SEEN
                    ):
                        log(
                            "Playlist metadata conflict : "
                            f"{header_name} is blank in {source_name}; "
                            f"keeping {existing_source}",
                            level="WARN",
                        )

                        NM3U8DL_METADATA_CONFLICTS_SEEN.add(
                            conflict_signature
                        )

                    continue

                if existing_value != value:
                    conflict_signature = (
                        header_key,
                        existing_source,
                        source_name,
                        existing_value,
                        value,
                    )

                    if (
                        conflict_signature
                        not in NM3U8DL_METADATA_CONFLICTS_SEEN
                    ):
                        log(
                            "Playlist metadata conflict : "
                            f"{header_name} differs between "
                            f"{existing_source} and {source_name}; "
                            f"using {source_name}",
                            level="WARN",
                        )

                        NM3U8DL_METADATA_CONFLICTS_SEEN.add(
                            conflict_signature
                        )

                del headers[existing_name]

            headers[header_name] = value
            header_sources[header_key] = source_name

    # Lowest → highest precedence.
    apply_headers(
        "#EXTVLCOPT",
        extvlc_headers,
    )

    apply_headers(
        "#EXTHTTP",
        exthttp_headers,
    )

    apply_headers(
        "URL pipe metadata",
        pipe_headers,
    )

    return headers


def normalize_nm3u8dl_playlist_entry(
    entry: dict
) -> dict:

    clean_stream_url, pipe_headers = (
        split_nm3u8dl_stream_url_metadata(
            entry["stream_url"]
        )
    )

    extvlc_headers, exthttp_headers = (
        get_nm3u8dl_playlist_header_sources(
            entry["option_lines"]
        )
    )

    headers = merge_nm3u8dl_playlist_headers(
        extvlc_headers=extvlc_headers,
        exthttp_headers=exthttp_headers,
        pipe_headers=pipe_headers,
    )

    return {
        "extinf": entry["extinf"],
        "option_lines": entry["option_lines"],
        "stream_url": clean_stream_url,
        "headers": headers,
    }

def _nm3u8dl_b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _nm3u8dl_b64url_decode(value: str) -> bytes:
    value = value.strip()
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def normalize_nm3u8dl_playlist_license_key(value: str) -> List[str]:
    """Use the shared mature/Coordinator ClearKey metadata normalizer."""
    return list(source_discovery.normalize_playlist_license_key(value))


def get_nm3u8dl_playlist_license_type(
    option_lines: List[str]
) -> str:
    prefix = "#KODIPROP:inputstream.adaptive.license_type="

    for line in option_lines:
        if line.startswith(prefix):
            return line[len(prefix):].strip()

    return ""


def get_nm3u8dl_playlist_keys(option_lines: List[str]) -> List[str]:
    keys = []

    prefix = "#KODIPROP:inputstream.adaptive.license_key="

    for line in option_lines:
        if not line.startswith(prefix):
            continue

        value = line[len(prefix):].strip()

        for normalized_key in normalize_nm3u8dl_playlist_license_key(value):
            if normalized_key not in keys:
                keys.append(normalized_key)

    return keys


def resolve_nm3u8dl_license_url(
    license_url: str,
    stream_url: str,
    stream_headers: dict,
) -> List[str]:
    try:
        manifest_text = _fetch_nm3u8dl_stream_manifest_text(
            stream_url,
            stream_headers,
        )
    except Exception as error:
        log_timeout_exception(
            error,
            "HTTP",
            (
                NM3U8DL_QUALITY_HTTP_TIMEOUT_SEC + 5
                if _is_nm3u8dl_drmlive_host(stream_url)
                else NM3U8DL_QUALITY_HTTP_TIMEOUT_SEC
            ),
            context="ClearKey manifest fetch",
            source=stream_url,
        )
        raise RuntimeError(
            f"ClearKey resolver could not fetch selected MPD "
            f"({type(error).__name__}: {error})"
        ) from error

    try:
        root = ET.fromstring(manifest_text)
    except ET.ParseError as error:
        raise RuntimeError(
            "ClearKey resolver could not parse selected MPD XML"
        ) from error

    kid_hex_values = []

    for element in root.iter():
        for attribute_name, attribute_value in element.attrib.items():
            if not (
                attribute_name == "default_KID"
                or attribute_name.endswith("}default_KID")
            ):
                continue

            kid_hex = re.sub(
                r"[^0-9A-Fa-f]",
                "",
                str(attribute_value),
            ).lower()

            if len(kid_hex) != 32:
                continue

            if kid_hex not in kid_hex_values:
                kid_hex_values.append(kid_hex)

    if not kid_hex_values:
        raise RuntimeError(
            "ClearKey resolver found no default_KID in selected MPD"
        )

    request_kids = [
        _nm3u8dl_b64url_encode(bytes.fromhex(kid_hex))
        for kid_hex in kid_hex_values
    ]

    payload = json.dumps(
        {
            "kids": request_kids,
            "type": "temporary",
        },
        separators=(",", ":"),
    ).encode("utf-8")

    request = Request(
        license_url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "curl/8.21.0",
        },
        method="POST",
    )

    try:
        with urlopen(request, timeout=20) as response:
            response_data = json.loads(
                response.read().decode(
                    "utf-8-sig",
                    errors="replace",
                )
            )
    except Exception as error:
        log_timeout_exception(
            error,
            "HTTP",
            20,
            context="ClearKey license request",
            source=license_url,
        )
        raise RuntimeError(
            f"ClearKey license request failed "
            f"({type(error).__name__}: {error})"
        ) from error

    response_keys = response_data.get("keys")

    if not isinstance(response_keys, list) or not response_keys:
        raise RuntimeError(
            "ClearKey license URL returned no keys"
        )

    requested_kids = set(kid_hex_values)
    resolved_keys = []

    for item in response_keys:
        if not isinstance(item, dict):
            continue

        response_kid = str(item.get("kid") or "").strip()
        response_key = str(item.get("k") or "").strip()

        if not response_kid or not response_key:
            continue

        try:
            kid_hex = _nm3u8dl_b64url_decode(
                response_kid
            ).hex()

            key_hex = _nm3u8dl_b64url_decode(
                response_key
            ).hex()
        except Exception:
            continue

        if (
            len(kid_hex) != 32
            or len(key_hex) != 32
            or kid_hex not in requested_kids
        ):
            continue

        pair = f"{kid_hex}:{key_hex}"

        if pair not in resolved_keys:
            resolved_keys.append(pair)

    resolved_kids = {
        pair.split(":", 1)[0]
        for pair in resolved_keys
    }

    missing_kids = requested_kids - resolved_kids

    if missing_kids:
        raise RuntimeError(
            "ClearKey license URL did not return all requested keys"
        )

    log(
        f"N_m3u8DL: ClearKey license URL resolved → "
        f"{len(resolved_keys)} key(s)"
    )

    return resolved_keys


def resolve_nm3u8dl_source_keys(
    source: dict,
    stream_headers: dict,
) -> List[str]:
    resolved_keys = []

    for key_value in source.get("keys") or []:
        value = str(key_value).strip()

        if value.lower().startswith(
            ("http://", "https://")
        ):
            log(
                "N_m3u8DL: Resolving ClearKey license URL "
                "for selected source..."
            )

            url_keys = resolve_nm3u8dl_license_url(
                value,
                source["stream_url"],
                stream_headers,
            )

            for resolved_key in url_keys:
                if resolved_key not in resolved_keys:
                    resolved_keys.append(resolved_key)

        elif value and value not in resolved_keys:
            resolved_keys.append(value)

    return resolved_keys


def get_nm3u8dl_effective_headers(
    playlist_headers: Optional[dict] = None,
    *,
    emit_logs: bool = False,
) -> dict:
    """Build the exact shared HTTP-header set used for this playlist group."""
    profile = get_nm3u8dl_playlist_profile()
    group_name = NM3U8DL_PLAYLIST_GROUP.strip().upper()
    provider = SHARED_PLAYLIST_GROUP_PROFILES.get(group_name, group_name)
    cookie_policy = (
        NM3U8DL_PLAYLIST_GROUP_COOKIE_POLICY
        .get(group_name, "AUTO")
        .strip()
        .upper()
    )
    if cookie_policy not in ("AUTO", "SUPPRESS"):
        raise RuntimeError(
            f'Unknown Cookie policy "{cookie_policy}" '
            f'for playlist group "{group_name}"'
        )

    filtered_headers = {}
    for name, value in (playlist_headers or {}).items():
        header_name = canonicalize_nm3u8dl_header_name(name)
        if not header_name:
            continue
        if header_name.casefold() == "cookie" and cookie_policy == "SUPPRESS":
            if emit_logs:
                log(
                    f"Playlist Cookie found but suppressed by "
                    f"{group_name} Cookie policy",
                    level="WARN",
                )
            continue
        playlist_value = str(value).strip()
        existing_name = next(
            (
                current_name
                for current_name in profile["added_headers"]
                if current_name.casefold() == header_name.casefold()
            ),
            None,
        )
        if not playlist_value:
            if emit_logs and existing_name is not None:
                log(
                    f"Playlist {header_name} is blank → "
                    f"keeping profile default",
                    level="WARN",
                )
            continue
        if (
            emit_logs
            and existing_name is not None
            and str(profile["added_headers"][existing_name]).strip() != playlist_value
        ):
            log(
                "Playlist/profile header conflict : "
                f"{header_name} differs between profile default "
                f"and playlist metadata; using playlist metadata",
                level="WARN",
            )
        filtered_headers[header_name] = value

    return source_discovery.build_effective_probe_headers(
        provider,
        filtered_headers,
        base_headers=profile["added_headers"],
        default_user_agent=NM3U8DL_PLAYLIST_USER_AGENTS.get("DEFAULT"),
    )


def get_nm3u8dl_ascii_safe_request_headers(
    headers: Optional[dict] = None,
    *,
    emit_logs: bool = False,
) -> dict:
    original = {
        str(name): str(value)
        for name, value in (headers or {}).items()
        if str(name).strip()
    }
    safe_headers = source_transport.ascii_safe_request_headers(original)
    if emit_logs:
        original_ua_name = next(
            (name for name in original if name.casefold() == "user-agent"),
            None,
        )
        safe_ua_name = next(
            (name for name in safe_headers if name.casefold() == "user-agent"),
            None,
        )
        if original_ua_name is not None:
            original_ua = str(original[original_ua_name]).strip()
            safe_ua = (
                str(safe_headers[safe_ua_name]).strip()
                if safe_ua_name is not None
                else ""
            )
            if safe_ua != original_ua:
                if safe_ua:
                    log(
                        "N_m3u8DL User-Agent contains non-ASCII characters → "
                        "using ASCII-safe value",
                        level="WARN",
                    )
                else:
                    log(
                        "N_m3u8DL User-Agent contains no ASCII-safe characters → "
                        "omitting User-Agent",
                        level="WARN",
                    )
    return safe_headers


_NM3U8DL_FAILOVER_FINGERPRINT_HEADERS = (
    "Cookie",
    "Authorization",
    "Referer",
    "Origin",
)


def _nm3u8dl_fingerprint_header_value(headers: dict, wanted_name: str) -> str:
    wanted = str(wanted_name or "").strip().casefold()
    for name, value in (headers or {}).items():
        if str(name).strip().casefold() == wanted:
            return str(value or "").strip()
    return ""


def get_nm3u8dl_stream_fingerprint(candidate: Optional[dict]) -> str:
    """Return the draft-one effective-stream identity used only for failover."""
    if not candidate:
        return ""

    # Fingerprint contract: the URL reached by the normal manifest probe, not
    # the exposed playlist URL. If the probe did not establish a final manifest
    # URL, do not silently substitute another identity component.
    final_manifest_url = str(
        candidate.get("manifest_final_url") or ""
    ).strip()

    if not final_manifest_url:
        return ""

    effective_headers = dict(candidate.get("effective_headers") or {})
    if not effective_headers:
        try:
            effective_headers = get_nm3u8dl_effective_headers(
                candidate.get("headers") or {},
                emit_logs=False,
            )
        except Exception:
            effective_headers = dict(candidate.get("headers") or {})

    payload = {
        "final_manifest_url": final_manifest_url,
        "headers": {
            header_name.casefold(): _nm3u8dl_fingerprint_header_value(
                effective_headers,
                header_name,
            )
            for header_name in _NM3U8DL_FAILOVER_FINGERPRINT_HEADERS
        },
    }

    serialized = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def get_nm3u8dl_manual_feed_signature(candidate: Optional[dict]) -> Optional[dict]:
    """Return the recording-local manual feed identity, or None if incomplete."""
    if not candidate:
        return None

    family = str(get_nm3u8dl_candidate_source_family(candidate) or "").strip()
    stream_type = str(
        candidate.get("stream_type")
        or _get_nm3u8dl_stream_type_from_url(candidate.get("stream_url") or "")
        or ""
    ).strip().upper()
    width = int(candidate.get("video_width") or 0)
    height = int(candidate.get("video_height") or 0)
    fps = float(candidate.get("video_fps") or 0.0)
    bitrate = int(candidate.get("video_bitrate_bps") or 0)
    scan_type = _normalize_nm3u8dl_video_scan_type(
        candidate.get("video_scan_type")
    )

    # Manual rejection is deliberately broader than one exact URL, so refuse to
    # create that broader identity when the quality/family evidence is incomplete.
    if (
        not family
        or not stream_type
        or width <= 0
        or height <= 0
        or fps <= 0
        or bitrate <= 0
    ):
        return None

    fps_milli = int(round(fps * 1000.0))
    payload = {
        "family": family,
        "stream_type": stream_type,
        "width": width,
        "height": height,
        "fps_milli": fps_milli,
        "scan_type": scan_type or "",
        "bitrate_bps": bitrate,
    }
    key = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )

    return {
        "key": key,
        **payload,
    }


def format_nm3u8dl_manual_feed_signature(signature: Optional[dict]) -> str:
    if not signature:
        return "unknown"

    fps = float(int(signature.get("fps_milli") or 0)) / 1000.0
    fps_text = f"{fps:.3f}".rstrip("0").rstrip(".") if fps > 0 else "?"
    scan_type = str(signature.get("scan_type") or "").strip()
    if scan_type == "progressive":
        fps_text += "p"
    elif scan_type == "interlaced":
        fps_text += "i"
    else:
        fps_text += " fps"

    bitrate_kbps = int(round(int(signature.get("bitrate_bps") or 0) / 1000.0))
    return (
        f"{signature.get('family') or 'unknown'} — "
        f"{signature.get('stream_type') or 'STREAM'} — "
        f"{int(signature.get('width') or 0)}x{int(signature.get('height') or 0)} | "
        f"{fps_text} | {bitrate_kbps} Kbps"
    )


def _nm3u8dl_mark_bad_fingerprint_candidates(
    state: Optional[RecorderState],
    candidates: List[dict],
):
    """Annotate candidates excluded by automatic failover or manual rejection."""
    bad_fingerprints = (
        getattr(state, "nm3u8dl_bad_stream_fingerprints", {})
        if state is not None
        else {}
    ) or {}
    manual_excluded_signatures = (
        getattr(state, "nm3u8dl_manual_excluded_feed_signatures", {})
        if state is not None
        else {}
    ) or {}

    for candidate in candidates:
        candidate.pop("failover_excluded", None)
        candidate.pop("failover_exclusion_reason", None)
        candidate.pop("stream_fingerprint", None)

        fingerprint = get_nm3u8dl_stream_fingerprint(candidate)
        if fingerprint:
            candidate["stream_fingerprint"] = fingerprint

        manual_signature = get_nm3u8dl_manual_feed_signature(candidate)
        manual_signature_key = (
            str(manual_signature.get("key") or "")
            if manual_signature
            else ""
        )

        if manual_signature_key and manual_signature_key in manual_excluded_signatures:
            candidate["failover_excluded"] = True
            candidate["failover_exclusion_reason"] = (
                "manually rejected feed signature during this recording"
            )
        elif fingerprint and fingerprint in bad_fingerprints:
            candidate["failover_excluded"] = True
            candidate["failover_exclusion_reason"] = (
                "2 consecutive downloader failures"
            )


def _nm3u8dl_clear_reactivated_drmlive_failover_state(
    state: Optional[RecorderState],
    candidates: List[dict],
    playlist_history_meta: dict,
) -> int:
    """Clear stale failover state only for DRMLive fingerprints just restored."""
    if state is None:
        return 0

    reactivated_playlists = {
        str(playlist_url).strip()
        for playlist_url, playlist_meta in (playlist_history_meta or {}).items()
        if bool(
            ((playlist_meta or {}).get("activation") or {}).get("reactivated")
        )
    }
    if not reactivated_playlists:
        return 0

    bad_fingerprints = (
        getattr(state, "nm3u8dl_bad_stream_fingerprints", {}) or {}
    )
    probations = getattr(state, "nm3u8dl_failover_probations", {}) or {}
    cleared = set()

    for candidate in candidates or []:
        if (
            str(candidate.get("playlist_url") or "").strip()
            not in reactivated_playlists
            or not _is_nm3u8dl_drmlive_host(
                candidate.get("stream_url") or ""
            )
            or not candidate.get("manifest_reachable", False)
        ):
            continue

        fingerprint = get_nm3u8dl_stream_fingerprint(candidate)
        if not fingerprint:
            continue

        if (
            fingerprint in bad_fingerprints
            or fingerprint in probations
        ):
            bad_fingerprints.pop(fingerprint, None)
            probations.pop(fingerprint, None)
            cleared.add(fingerprint)

    if cleared:
        state.nm3u8dl_failover_alarm_silenced = False
        log(
            "DRMLive authorization recovery verified → "
            f"cleared stale failover state for "
            f"{len(cleared)} restored fingerprint(s)."
        )

    return len(cleared)


def _nm3u8dl_set_running_stream_identity(
    state: RecorderState,
    source: dict,
):
    fingerprint = str(source.get("stream_fingerprint") or "").strip()
    if not fingerprint:
        fingerprint = get_nm3u8dl_stream_fingerprint(source)
        if fingerprint:
            source["stream_fingerprint"] = fingerprint

    state.nm3u8dl_running_source = source
    state.nm3u8dl_running_stream_fingerprint = fingerprint or None


def _nm3u8dl_apply_manual_stream_exclusion(state: RecorderState) -> bool:
    """Reject the confirmed feed signature for this recording only."""
    source = getattr(state, "nm3u8dl_running_source", None)
    pending = getattr(state, "nm3u8dl_pending_manual_exclusion", None) or {}
    current_signature = get_nm3u8dl_manual_feed_signature(source)

    state.nm3u8dl_manual_exclude_requested = False
    state.nm3u8dl_pending_manual_exclusion = None

    if (
        not source
        or not pending
        or not current_signature
        or current_signature.get("key") != pending.get("key")
    ):
        log(
            "MANUAL STREAM REJECT could not be applied: current feed changed "
            "or its feed signature became unavailable; nothing was excluded.",
            level="WARN",
        )
        return False

    signature_key = str(pending.get("key") or "")
    state.nm3u8dl_manual_excluded_feed_signatures[signature_key] = {
        "marked_ts": time.time(),
        "playlist_url": str(source.get("playlist_url") or "").strip(),
        "entry_title": str(
            source.get("entry_title")
            or source.get("tvg_name")
            or ""
        ).strip(),
        "signature": dict(pending),
        "reason": "manual",
    }

    # If this exact route happened to be on first-failure probation, remove that
    # automatic probation only. The broader manual feed exclusion remains separate
    # from automatic bad-fingerprint state and therefore survives VPN/access resets.
    fingerprint = str(
        getattr(state, "nm3u8dl_running_stream_fingerprint", None) or ""
    ).strip()
    if fingerprint:
        state.nm3u8dl_failover_probations.pop(fingerprint, None)

    state.nm3u8dl_failover_retry_source = None
    state.nm3u8dl_pending_source = None
    state.nm3u8dl_renewal_rollover_requested = False
    state.nm3u8dl_rollover_reason = None
    state.nm3u8dl_failover_waiting_for_alternative = True
    state.nm3u8dl_failover_alarm_silenced = False

    log(
        "MANUAL FEED EXCLUDED — "
        f"{format_nm3u8dl_manual_feed_signature(pending)} — "
        "all matching feeds are excluded for this recording; "
        "full playlist rescan required.",
        level="WARN",
    )
    return True


def _nm3u8dl_note_failover_healthy_growth(state: RecorderState):
    """A different healthy fingerprint clears older first-failure probations."""
    if NM3U8DL_SOURCE_MODE != "playlist":
        return

    current = str(
        getattr(state, "nm3u8dl_running_stream_fingerprint", None) or ""
    ).strip()
    probations = getattr(state, "nm3u8dl_failover_probations", {}) or {}

    if not current or not probations:
        return

    # Healthy growth on the same fingerprint does NOT erase its first failure.
    # But once a different stream proves healthy, any older one-failure incident
    # is genuinely recovered and no longer needs to follow that old stream.
    cleared = [
        fingerprint
        for fingerprint in list(probations)
        if fingerprint != current
    ]
    if not cleared:
        return

    for fingerprint in cleared:
        probations.pop(fingerprint, None)

    log(
        "Stream failover recovery confirmed on a different healthy stream → "
        "clearing previous one-failure probation."
    )
    state.nm3u8dl_failover_alarm_silenced = False


def _nm3u8dl_handle_playlist_stream_failure(
    state: RecorderState,
    failure_type: str,
) -> str:
    """Apply the playlist-only two-attempt policy; return retry/rescan action."""
    source = getattr(state, "nm3u8dl_running_source", None)
    fingerprint = str(
        getattr(state, "nm3u8dl_running_stream_fingerprint", None) or ""
    ).strip()

    if source and not fingerprint:
        fingerprint = get_nm3u8dl_stream_fingerprint(source)
        if fingerprint:
            source["stream_fingerprint"] = fingerprint
            state.nm3u8dl_running_stream_fingerprint = fingerprint

    # A normal selected playlist source should have a final manifest URL from its
    # probe. If it does not, fail safe toward a fresh scan rather than inventing
    # an exposed-URL fingerprint that violates the agreed identity contract.
    if not source or not fingerprint:
        log(
            "STREAM_FAILOVER: effective stream fingerprint unavailable → "
            "forcing a full playlist rescan without blacklisting.",
            level="WARN",
        )
        state.nm3u8dl_failover_retry_source = None
        state.nm3u8dl_failover_waiting_for_alternative = True
        state.nm3u8dl_failover_alarm_silenced = False
        state.nm3u8dl_pending_source = None
        state.nm3u8dl_renewal_rollover_requested = False
        state.nm3u8dl_rollover_reason = None
        return "rescan"

    probations = state.nm3u8dl_failover_probations

    if fingerprint in probations:
        first_failure = str(
            (probations.get(fingerprint) or {}).get("first_failure")
            or "unknown"
        )
        state.nm3u8dl_bad_stream_fingerprints[fingerprint] = {
            "marked_ts": time.time(),
            "first_failure": first_failure,
            "second_failure": str(failure_type),
        }
        probations.pop(fingerprint, None)
        state.nm3u8dl_failover_retry_source = None
        state.nm3u8dl_failover_waiting_for_alternative = True
        state.nm3u8dl_failover_alarm_silenced = False

        # Any planned retained handoff was chosen before this stream proved bad.
        # After rejection, the next decision must come from a complete fresh scan.
        state.nm3u8dl_pending_source = None
        state.nm3u8dl_renewal_rollover_requested = False
        state.nm3u8dl_rollover_reason = None

        log(
            f"STREAM_FAILOVER failure 2/2 type={failure_type} → "
            "stream marked EXCLUDED; full playlist rescan required.",
            level="WARN",
        )
        return "rescan"

    probations[fingerprint] = {
        "first_failure": str(failure_type),
        "started_ts": time.time(),
    }
    state.nm3u8dl_failover_retry_source = dict(source)
    state.nm3u8dl_failover_waiting_for_alternative = False
    state.nm3u8dl_failover_alarm_silenced = False

    # The next run must be the exact selected source, not a retained upgrade or a
    # fresh resolution. Existing planned state can be rediscovered later if needed.
    state.nm3u8dl_pending_source = None
    state.nm3u8dl_renewal_rollover_requested = False
    state.nm3u8dl_rollover_reason = None

    log(
        f"STREAM_FAILOVER failure 1/2 type={failure_type} → "
        "direct retry of the same source; no playlist scan.",
        level="WARN",
    )
    return "retry"


def _parse_nm3u8dl_frame_rate(value) -> float:
    if value is None:
        return 0.0

    text = str(value).strip()

    if not text:
        return 0.0

    try:
        if "/" in text:
            numerator_text, denominator_text = text.split("/", 1)
            numerator = float(numerator_text)
            denominator = float(denominator_text)

            if denominator == 0:
                return 0.0

            return numerator / denominator

        return float(text)

    except (TypeError, ValueError):
        return 0.0


def _normalize_nm3u8dl_video_scan_type(value) -> str:
    return source_selection.normalize_video_scan_type(value)


def _nm3u8dl_has_quality_evidence(quality: dict) -> bool:
    return source_selection.has_quality_evidence(quality)


def _nm3u8dl_video_scan_rank(quality: dict) -> int:
    return source_selection.video_scan_rank(quality)


def _nm3u8dl_comparable_motion_fps(quality: dict) -> float:
    return source_selection.comparable_motion_fps(
        quality,
        motion_cap_fps=NM3U8DL_QUALITY_RANKING_MOTION_CAP_FPS,
    )


def _nm3u8dl_ranking_motion_fps(quality: dict) -> float:
    return source_selection.ranking_motion_fps(
        quality,
        motion_cap_fps=NM3U8DL_QUALITY_RANKING_MOTION_CAP_FPS,
    )


def _nm3u8dl_video_resolution_class(quality: dict) -> int:
    return source_selection.video_resolution_class(quality)


def _nm3u8dl_video_quality_rank(quality: dict):
    return source_selection.video_quality_rank(
        quality,
        motion_cap_fps=NM3U8DL_QUALITY_RANKING_MOTION_CAP_FPS,
    )


def _get_nm3u8dl_selection_policy(
    *,
    upgrade_min_remaining_min: Optional[int] = None,
    allow_unknown_expiry: Optional[bool] = None,
) -> SelectionPolicy:
    profile = get_nm3u8dl_playlist_profile()
    if upgrade_min_remaining_min is None:
        upgrade_min_remaining_min = NM3U8DL_NEW_SOURCE_MIN_REMAINING_MIN
    if allow_unknown_expiry is None:
        allow_unknown_expiry = bool(
            profile.get("allow_unknown_expiry", False)
        )

    return SelectionPolicy(
        mandatory_min_remaining_sec=int(
            NM3U8DL_NEW_SOURCE_MIN_REMAINING_MIN * 60
        ),
        upgrade_min_remaining_sec=int(upgrade_min_remaining_min * 60),
        allow_unknown_expiry=bool(allow_unknown_expiry),
        prefer_unknown_expiry_on_equal_quality=bool(
            profile.get(
                "prefer_unknown_expiry_on_equal_quality",
                False,
            )
        ),
        motion_cap_fps=float(
            NM3U8DL_QUALITY_RANKING_MOTION_CAP_FPS
        ),
    )


def get_nm3u8dl_candidate_quality_rank(candidate: dict):
    return source_selection.candidate_quality_rank(
        SourceCandidate.from_mapping(candidate),
        _get_nm3u8dl_selection_policy(),
    )


def get_nm3u8dl_join_selection_decision(
    candidates: List[dict],
    *,
    now_ts: Optional[float] = None,
) -> SelectionDecision:
    return source_selection.select_join_candidate(
        [SourceCandidate.from_mapping(candidate) for candidate in candidates],
        _get_nm3u8dl_selection_policy(),
        now_ts=now_ts,
    )


def get_nm3u8dl_join_candidate(
    candidates: List[dict],
    *,
    now_ts: Optional[float] = None,
) -> Optional[dict]:
    decision = get_nm3u8dl_join_selection_decision(
        candidates,
        now_ts=now_ts,
    )
    if decision.selected_index is None:
        return None
    return candidates[decision.selected_index]


def format_nm3u8dl_candidate_quality(candidate: dict) -> str:
    """Use the shared quality/evidence formatter."""
    return source_quality.format_candidate_quality(
        candidate,
        motion_cap_fps=NM3U8DL_QUALITY_RANKING_MOTION_CAP_FPS,
    )


def _parse_nm3u8dl_hls_manifest_quality(
    manifest_text: str,
    manifest_url: str = "",
) -> Optional[dict]:
    return source_quality.parse_hls_manifest_quality(
        manifest_text,
        manifest_url,
        motion_cap_fps=NM3U8DL_QUALITY_RANKING_MOTION_CAP_FPS,
        expiry_parser=get_nm3u8dl_auth_expiry,
    )


def _inspect_nm3u8dl_hls_manifest_drm(manifest_text: str) -> dict:
    """Use the shared recorder/Coordinator HLS DRM interpretation."""
    return source_quality.inspect_hls_manifest_drm(manifest_text)


def _inspect_nm3u8dl_dash_manifest_drm(manifest_text: str) -> dict:
    """Use the shared recorder/Coordinator DASH DRM interpretation."""
    return source_quality.inspect_dash_manifest_drm(manifest_text)


def _parse_nm3u8dl_dash_manifest_quality(
    manifest_text: str,
    manifest_url: str = "",
) -> Optional[dict]:
    """Use the shared mature/Coordinator DASH manifest parser."""
    return source_quality.parse_dash_manifest_quality(
        manifest_text,
        manifest_url,
        motion_cap_fps=NM3U8DL_QUALITY_RANKING_MOTION_CAP_FPS,
    )


def _fetch_nm3u8dl_stream_manifest_text(
    stream_url: str,
    headers: dict,
    *,
    include_final_url: bool = False,
    stop_requested: Optional[Callable[[], bool]] = None,
):
    def on_curl_timeout(error, timeout_sec, source_url):
        log_timeout_exception(
            error,
            "curl",
            timeout_sec,
            context="GET",
            source=source_url,
        )

    manifest_text, final_url = source_transport.fetch_stream_manifest_text(
        stream_url,
        headers,
        default_user_agent=NM3U8DL_PLAYLIST_USER_AGENTS["DEFAULT"],
        stop_requested=stop_requested,
        urlopen_fn=urlopen,
        subprocess_runner=subprocess.run,
        timeout_callback=on_curl_timeout,
    )
    if include_final_url:
        return manifest_text, final_url
    return manifest_text


def _fetch_nm3u8dl_hls_child_with_master_cookie_session(
    master_url: str,
    child_url: str,
    headers: dict,
    *,
    stop_requested: Optional[Callable[[], bool]] = None,
) -> str:
    """Use the shared master-cookie child-fetch mechanic."""
    request_headers = get_nm3u8dl_ascii_safe_request_headers(
        headers,
        emit_logs=False,
    )
    if not any(
        str(name).casefold() == "user-agent" and str(value).strip()
        for name, value in request_headers.items()
    ):
        request_headers["User-Agent"] = str(
            NM3U8DL_PLAYLIST_USER_AGENTS["DEFAULT"]
        )
    return source_transport.fetch_hls_child_with_master_cookie_session(
        master_url,
        child_url,
        request_headers,
        timeout_sec=NM3U8DL_QUALITY_HTTP_TIMEOUT_SEC,
        stop_requested=stop_requested,
    )


def _get_nm3u8dl_stream_type_from_url(stream_url: str) -> str:
    try:
        path = str(urlparse(str(stream_url or "")).path or "").lower()
    except Exception:
        path = str(stream_url or "").lower()

    if path.endswith(".mpd"):
        return "DASH"

    if path.endswith(".m3u8"):
        return "HLS"

    return ""


def _normalize_nm3u8dl_probe_failure_text(value: str) -> str:
    """Reduce raw probe output to one safe, useful failure description."""
    text = " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split())
    lowered = text.lower()

    http_patterns = (
        r"server returned\s+(\d{3})\s+([^\[]+?)(?:\s*\[|$)",
        r"http(?: error)?\s+(\d{3})\s+([^\[]+?)(?:\s*\[|$)",
        r"http/\S+\s+(\d{3})\s+([^\[]+?)(?:\s*\[|$)",
    )

    for pattern in http_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            code = match.group(1)
            reason = " ".join(match.group(2).strip(" .:-").split())
            if len(reason) > 80:
                reason = reason[:80].rstrip()
            return f"HTTP {code}" + (f" {reason}" if reason else "")

    if "timed out" in lowered or "timeout" in lowered:
        return "connection timed out"

    if any(token in lowered for token in (
        "temporary failure in name resolution",
        "name or service not known",
        "nodename nor servname provided",
        "no such host",
        "host not found",
    )):
        return "DNS/host lookup failed"

    if "connection refused" in lowered:
        return "connection refused"

    if "network is unreachable" in lowered:
        return "network unreachable"

    if "certificate" in lowered or "tls" in lowered or "ssl" in lowered:
        return "TLS/certificate failure"

    return ""


def _describe_nm3u8dl_probe_exception(error: Exception) -> str:
    if isinstance(error, HTTPError):
        status_code = getattr(error, "code", None)
        reason = str(getattr(error, "reason", "") or "").strip()
        http_text = (
            f"HTTP {status_code}" + (f" {reason}" if reason else "")
            if status_code is not None
            else "HTTP request failed"
        )

        # Some source endpoints return a small JSON error body with a useful
        # provider-side reason. Preserve that reason when it is safe and concise.
        try:
            body_text = error.read(4096).decode(
                "utf-8-sig",
                errors="replace",
            ).strip()
            response_data = json.loads(body_text) if body_text else None

            if isinstance(response_data, dict):
                for field in ("error", "message", "detail"):
                    server_reason = response_data.get(field)
                    if isinstance(server_reason, str):
                        server_reason = " ".join(server_reason.split())
                        if server_reason and len(server_reason) <= 120:
                            return f"{http_text} — {server_reason}"
        except Exception:
            pass

        return http_text

    if isinstance(error, TimeoutError):
        return "connection timed out"

    if isinstance(error, URLError):
        reason = getattr(error, "reason", None)
        if isinstance(reason, TimeoutError):
            return "connection timed out"

        normalized = _normalize_nm3u8dl_probe_failure_text(reason)
        if normalized:
            return normalized

        return "network request failed"

    if isinstance(error, RuntimeError):
        normalized = _normalize_nm3u8dl_probe_failure_text(str(error))
        if normalized:
            return normalized

        safe_text = str(error).strip()
        if safe_text and len(safe_text) <= 120 and "http" not in safe_text.lower():
            return safe_text

    normalized = _normalize_nm3u8dl_probe_failure_text(str(error))
    if normalized:
        return normalized

    return f"probe failed ({type(error).__name__})"


def _summarize_nm3u8dl_ffprobe_failure(result) -> str:
    stderr = str(getattr(result, "stderr", "") or "").strip()
    normalized = _normalize_nm3u8dl_probe_failure_text(stderr)

    if normalized:
        return normalized

    returncode = getattr(result, "returncode", None)
    if returncode not in (None, 0):
        return f"ffprobe exited with code {returncode}"

    return "ffprobe returned no video stream information"


def _sample_nm3u8dl_stream_video_bitrate(
    stream_url: str,
    headers: dict,
    *,
    timeout_route: str = "",
    byte_range: str = "",
    decryption_key: str = "",
    stream_index: Optional[int] = None,
) -> int:
    """Estimate video bitrate from a short copied-media sample."""
    command = source_quality.build_ffmpeg_bitrate_sample_command(
        stream_url,
        headers,
        sample_sec=NM3U8DL_QUALITY_BITRATE_SAMPLE_SEC,
        byte_range=byte_range,
        decryption_key=decryption_key,
        stream_index=stream_index,
    )

    parsed_url = urlparse(stream_url)
    probe_identity = (
        parsed_url.netloc + parsed_url.path
        if parsed_url.netloc
        else "candidate stream"
    )
    timeout_identity = str(timeout_route or probe_identity).strip()
    invocation = raw_external_start(
        "ffmpeg",
        f"candidate bitrate sample | {timeout_identity}",
    )
    try:
        result = subprocess.run(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            timeout=float(NM3U8DL_QUALITY_BITRATE_SAMPLE_TIMEOUT_SEC),
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raw_external_write(
            invocation,
            getattr(error, "stderr", None),
            "stderr",
        )
        raw_external_end(invocation, status="timeout")
        log_timeout_exception(
            error,
            "ffmpeg",
            NM3U8DL_QUALITY_BITRATE_SAMPLE_TIMEOUT_SEC,
            context=f"candidate bitrate sample | {timeout_identity}",
        )
        return 0
    except Exception as error:
        raw_external_end(
            invocation,
            status=f"exception {type(error).__name__}",
        )
        return 0

    stderr_text = str(result.stderr or "")
    raw_external_write(invocation, stderr_text, "stderr")
    raw_external_end(invocation, returncode=result.returncode)

    if result.returncode != 0:
        return 0
    return source_quality.parse_ffmpeg_bitrate_progress(stderr_text)

def _parse_nm3u8dl_idet_scan_type(stderr_text: str) -> str:
    """Return progressive/interlaced only when idet evidence is conclusive."""
    matches = re.findall(
        (
            r"Multi frame detection:\s*"
            r"TFF:\s*(\d+)\s+"
            r"BFF:\s*(\d+)\s+"
            r"Progressive:\s*(\d+)\s+"
            r"Undetermined:\s*(\d+)"
        ),
        str(stderr_text or ""),
        re.IGNORECASE,
    )

    if not matches:
        return ""

    tff, bff, progressive, undetermined = (
        int(value)
        for value in matches[-1]
    )
    interlaced = tff + bff
    classified = interlaced + progressive
    total = classified + undetermined

    if classified < int(NM3U8DL_QUALITY_IDET_MIN_CLASSIFIED_FRAMES):
        return ""

    if total <= 0:
        return ""

    if (
        float(classified) / float(total)
        < float(NM3U8DL_QUALITY_IDET_MIN_CLASSIFIED_SHARE)
    ):
        return ""

    progressive_share = float(progressive) / float(classified)
    interlaced_share = float(interlaced) / float(classified)

    if progressive_share >= float(NM3U8DL_QUALITY_IDET_DOMINANT_SHARE):
        return "progressive"

    if interlaced_share >= float(NM3U8DL_QUALITY_IDET_DOMINANT_SHARE):
        return "interlaced"

    return ""


def _is_nm3u8dl_transient_stream_url_param(name: str, value: str = "") -> bool:
    """Return whether one URL field is delivery/auth metadata, not stream identity."""
    normalized_name = re.sub(
        r"[^a-z0-9]+",
        "",
        str(name or "").strip().casefold(),
    )

    if not normalized_name:
        return False

    # General CDN/auth/session fields may legitimately vary between playlist
    # routes while still addressing the same underlying video stream. Unknown
    # fields are preserved so provider-specific content selectors are never
    # discarded merely to increase cache hits.
    if normalized_name.startswith(("xamz", "xgoog", "aka")):
        return True

    if normalized_name in {
        "exp",
        "expires",
        "expiry",
        "hdnea",
        "hdntl",
        "hmac",
        "acl",
        "keypairid",
    }:
        return True

    if any(
        marker in normalized_name
        for marker in (
            "token",
            "auth",
            "signature",
            "session",
            "credential",
            "policy",
            "nonce",
            "jwt",
        )
    ):
        return True

    # Some signed values embed their expiry inside the value rather than in the
    # field name (for example exp=... inside a CDN policy blob).
    return bool(
        _extract_nm3u8dl_auth_expiries(
            f"{name}={value}"
        )
    )


def _get_nm3u8dl_scan_type_cache_identity(stream_url: str) -> str:
    """Return a provider-neutral underlying-stream identity for P/I reuse."""
    try:
        parsed_url = urlparse(str(stream_url or "").strip())
    except Exception:
        return ""

    host = str(parsed_url.hostname or "").strip().casefold()
    if not host:
        return ""

    try:
        port = parsed_url.port
    except ValueError:
        port = None

    authority = f"{host}:{port}" if port is not None else host
    stable_path_parts = []

    for path_part in str(parsed_url.path or "").split("/"):
        field_name, separator, field_value = path_part.partition("=")
        if (
            separator
            and _is_nm3u8dl_transient_stream_url_param(
                field_name,
                field_value,
            )
        ):
            continue
        stable_path_parts.append(path_part)

    stable_path = "/".join(stable_path_parts)

    stable_query_items = []
    try:
        query_items = parse_qsl(
            str(parsed_url.query or ""),
            keep_blank_values=True,
        )
    except Exception:
        query_items = []

    for name, value in query_items:
        if _is_nm3u8dl_transient_stream_url_param(name, value):
            continue
        stable_query_items.append((str(name), str(value)))

    stable_query_items.sort()
    stable_query = urlencode(stable_query_items, doseq=True)

    identity = f"{authority}{stable_path}"
    if stable_query:
        identity += f"?{stable_query}"

    return identity



def _run_nm3u8dl_external_capture_redacted(
    command,
    *,
    raw_tool: str,
    raw_context: str,
    timeout: float,
    secret_values=(),
):
    """Run one external probe while keeping decryption keys out of raw logs."""
    invocation = raw_external_start(raw_tool, raw_context)
    redacted_command = list(command)
    secrets = {str(value) for value in secret_values if str(value)}
    if secrets:
        redacted_command = [
            "<redacted>" if str(value) in secrets else value
            for value in redacted_command
        ]
    raw_external_write(invocation, repr(redacted_command), "command")
    raw_external_write(invocation, f"timeout={timeout!r} cwd=None", "execution")

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raw_external_write(invocation, getattr(error, "stdout", None), "stdout")
        raw_external_write(invocation, getattr(error, "stderr", None), "stderr")
        raw_external_end(invocation, status="timeout")
        log_timeout_exception(
            error,
            raw_tool,
            timeout,
            context=raw_context,
        )
        raise
    except Exception as error:
        raw_external_end(invocation, status=f"exception {type(error).__name__}")
        raise

    raw_external_write(invocation, result.stdout, "stdout")
    raw_external_write(invocation, result.stderr, "stderr")
    raw_external_end(invocation, returncode=result.returncode)
    return result


def _get_nm3u8dl_dash_resource_routes(quality: dict) -> List[dict]:
    return source_transport.dash_resource_routes(quality)


def _build_nm3u8dl_http_request_headers(
    headers: dict,
    *,
    byte_range: str = "",
) -> dict:
    return source_transport.build_resource_request_headers(
        headers,
        default_user_agent=NM3U8DL_PLAYLIST_USER_AGENTS["DEFAULT"],
        byte_range=byte_range,
    )


def _resolve_nm3u8dl_selected_dash_resource_route(
    quality: dict,
    headers: dict,
    *,
    stop_requested: Optional[Callable[[], bool]] = None,
) -> dict:
    return source_transport.resolve_selected_dash_resource_route(
        quality,
        headers,
        default_user_agent=NM3U8DL_PLAYLIST_USER_AGENTS["DEFAULT"],
        expiry_parser=get_nm3u8dl_auth_expiry,
        stop_requested=stop_requested,
        urlopen_fn=urlopen,
        error_describer=_describe_nm3u8dl_probe_exception,
    )


def _fetch_nm3u8dl_binary_resource(
    resource_url: str,
    headers: dict,
    *,
    byte_range: str = "",
    max_bytes: int = 24 * 1024 * 1024,
    include_final_url: bool = False,
):
    """Fetch one selected-representation DASH resource with existing headers."""
    request_headers = _build_nm3u8dl_http_request_headers(
        headers,
        byte_range=byte_range,
    )

    def fetch_once():
        request = Request(resource_url, headers=request_headers)
        with urlopen(
            request,
            timeout=NM3U8DL_QUALITY_HTTP_TIMEOUT_SEC,
        ) as response:
            payload = response.read(int(max_bytes) + 1)
            final_url = str(response.geturl() or resource_url).strip()
        return payload, final_url

    payload, final_url = _run_nm3u8dl_retryable_http_get(fetch_once)

    if len(payload) > int(max_bytes):
        raise RuntimeError("selected DASH media sample exceeded safe probe size")

    if include_final_url:
        return payload, final_url

    return payload


def _nm3u8dl_ffmpeg_header_args(headers: dict, *, byte_range: str = "") -> List[str]:
    effective = dict(headers or {})
    if byte_range:
        effective["Range"] = f"bytes={byte_range}"
    if not effective:
        return []
    header_blob = "".join(
        f"{name}: {value}\r\n"
        for name, value in effective.items()
    )
    return ["-headers", header_blob]


def _trace_nm3u8dl_h264_headers(
    input_value: str,
    headers: dict,
    *,
    byte_range: str = "",
    decryption_key: str = "",
    frame_limit: Optional[int] = None,
    timeout_route: str = "",
) -> str:
    command = [
        "ffmpeg",
        "-hide_banner",
        "-v", "info",
        "-nostdin",
    ]
    if input_value.lower().startswith(("http://", "https://")):
        command.extend(
            _nm3u8dl_ffmpeg_header_args(headers, byte_range=byte_range)
        )
    if decryption_key:
        command.extend(["-decryption_key", decryption_key])
    command.extend([
        "-i", input_value,
        "-map", "0:v:0",
        "-an",
        "-sn",
        "-dn",
        "-c:v", "copy",
        "-bsf:v", "trace_headers",
    ])
    if frame_limit is not None and int(frame_limit) > 0:
        command.extend(["-frames:v", str(int(frame_limit))])
    command.extend(["-f", "null", "-"])

    context = f"candidate H.264 syntax probe | {timeout_route or input_value}"
    try:
        result = _run_nm3u8dl_external_capture_redacted(
            command,
            raw_tool="ffmpeg",
            raw_context=context,
            timeout=NM3U8DL_QUALITY_H264_TRACE_TIMEOUT_SEC,
            secret_values=(decryption_key,),
        )
    except Exception:
        return ""
    return str(result.stderr or "")


def _parse_nm3u8dl_h264_sps_frame_mbs_only(stderr_text: str) -> Optional[int]:
    values = [
        int(value)
        for value in re.findall(
            r"\bframe_mbs_only_flag\b[^\r\n]*?=\s*([01])(?:\s|$)",
            str(stderr_text or ""),
            re.IGNORECASE,
        )
    ]
    if not values:
        return None
    if all(value == 1 for value in values):
        return 1
    if any(value == 0 for value in values):
        return 0
    return None


def _parse_nm3u8dl_h264_picture_structure(stderr_text: str) -> str:
    text = str(stderr_text or "")
    field_pic_flags = [
        int(value)
        for value in re.findall(
            r"\bfield_pic_flag\b[^\r\n]*?=\s*([01])(?:\s|$)",
            text,
            re.IGNORECASE,
        )
    ]
    mbaff_flags = [
        int(value)
        for value in re.findall(
            r"\bmb_adaptive_frame_field_flag\b[^\r\n]*?=\s*([01])(?:\s|$)",
            text,
            re.IGNORECASE,
        )
    ]

    if any(value == 1 for value in field_pic_flags):
        return "interlaced"

    if (
        len(field_pic_flags) >= int(NM3U8DL_QUALITY_H264_MIN_PICTURE_HEADERS)
        and all(value == 0 for value in field_pic_flags)
        and mbaff_flags
        and all(value == 0 for value in mbaff_flags)
    ):
        return "progressive"

    return ""


def _resolve_nm3u8dl_candidate_decryption_keys(
    candidate: dict,
    headers: dict,
) -> List[str]:
    if not (candidate.get("keys") or []):
        return []
    try:
        resolved_pairs = resolve_nm3u8dl_source_keys(candidate, headers)
    except Exception:
        return []

    values = []
    for pair in resolved_pairs:
        _, separator, key_value = str(pair or "").partition(":")
        key_value = key_value.strip() if separator else ""
        if re.fullmatch(r"[0-9a-fA-F]{32}", key_value) and key_value not in values:
            values.append(key_value)
    return values


def _is_nm3u8dl_h264_dash_representation(quality: dict) -> bool:
    codecs = str(quality.get("_dash_codecs") or "").strip().casefold()
    if not codecs:
        return False
    codec_tokens = [token.strip() for token in codecs.split(",") if token.strip()]
    return any(
        token == "h264"
        or token.startswith("avc1")
        or token.startswith("avc3")
        for token in codec_tokens
    )


def _get_nm3u8dl_dash_representation_cache_identity(quality: dict) -> str:
    representation_id = str(quality.get("_dash_representation_id") or "").strip()
    if representation_id:
        return f"id={representation_id}"

    width = int(quality.get("video_width") or 0)
    height = int(quality.get("video_height") or 0)
    fps = float(quality.get("video_fps") or 0.0)
    bitrate = int(
        quality.get("_dash_representation_bandwidth")
        or quality.get("video_bitrate_bps")
        or 0
    )
    codecs = str(quality.get("_dash_codecs") or "").strip().casefold()

    if width <= 0 or height <= 0 or fps <= 0 or bitrate <= 0 or not codecs:
        return ""

    return (
        f"res={width}x{height}|fps={fps:.6f}|"
        f"bitrate={bitrate}|codec={codecs}"
    )


def _nm3u8dl_known_media_probe_keys(
    candidate: dict,
    quality: dict,
    headers: dict,
) -> List[str]:
    external_key_required = bool(
        quality.get("drm_key_required")
        or (candidate.get("keys") or [])
    )

    resolved_keys = _resolve_nm3u8dl_candidate_decryption_keys(
        candidate,
        headers,
    )

    if external_key_required:
        return resolved_keys

    return [""]


def _record_nm3u8dl_dash_effective_resource_url(
    quality: dict,
    *,
    request_url: str,
    final_url: str,
    route_index: Optional[int] = None,
):
    final_url = str(final_url or "").strip()
    request_url = str(request_url or "").strip()

    if final_url:
        quality["selected_media_final_url"] = final_url

    if route_index is not None:
        quality["_dash_selected_route_index"] = int(route_index)

    quality["resource_expiry"] = _merge_nm3u8dl_auth_expiries(
        quality.get("resource_expiry"),
        get_nm3u8dl_auth_expiry(request_url),
        get_nm3u8dl_auth_expiry(final_url),
    )


def _prepare_nm3u8dl_selected_dash_media_sample(
    quality: dict,
    headers: dict,
) -> tuple:
    """Return (input, headers, range, temp_path) for the selected DASH rep."""
    routes = _get_nm3u8dl_dash_resource_routes(quality)

    for route in routes:
        route_index = int(route.get("_route_index", 0) or 0)
        media_urls = list(route.get("media_urls") or [])
        media_ranges = list(route.get("media_ranges") or [])

        if not media_urls:
            continue

        if route.get("media_self_contained"):
            quality["_dash_selected_route_index"] = route_index
            return media_urls[0], headers, (
                str(media_ranges[0] or "").strip() if media_ranges else ""
            ), ""

        init_url = str(route.get("initialization_url") or "").strip()
        if not init_url:
            continue
        init_range = str(route.get("initialization_range") or "").strip()

        try:
            init_bytes, init_final_url = _fetch_nm3u8dl_binary_resource(
                init_url,
                headers,
                byte_range=init_range,
                max_bytes=4 * 1024 * 1024,
                include_final_url=True,
            )
            _record_nm3u8dl_dash_effective_resource_url(
                quality,
                request_url=init_url,
                final_url=init_final_url,
                route_index=route_index,
            )
        except Exception:
            continue

        for index, media_url in enumerate(media_urls):
            media_range = (
                str(media_ranges[index] or "").strip()
                if index < len(media_ranges)
                else ""
            )
            try:
                media_bytes, media_final_url = _fetch_nm3u8dl_binary_resource(
                    media_url,
                    headers,
                    byte_range=media_range,
                    include_final_url=True,
                )
                _record_nm3u8dl_dash_effective_resource_url(
                    quality,
                    request_url=media_url,
                    final_url=media_final_url,
                    route_index=route_index,
                )
            except Exception:
                continue

            temp_path = ""
            try:
                with tempfile.NamedTemporaryFile(
                    mode="wb",
                    suffix=".mp4",
                    delete=False,
                ) as temp_file:
                    temp_file.write(init_bytes)
                    temp_file.write(media_bytes)
                    temp_path = temp_file.name
                return temp_path, {}, "", temp_path
            except Exception:
                if temp_path:
                    try:
                        os.remove(temp_path)
                    except Exception:
                        pass
                return "", {}, "", ""

    return "", {}, "", ""


def _ffprobe_nm3u8dl_selected_dash_representation_quality(
    candidate: dict,
    quality: dict,
    headers: dict,
    *,
    timeout_route: str = "",
) -> Optional[dict]:
    """Probe only the DASH representation already selected by the MPD parser."""
    sample_input, sample_headers, sample_range, temp_path = (
        _prepare_nm3u8dl_selected_dash_media_sample(quality, headers)
    )
    if not sample_input:
        return None

    try:
        key_values = _nm3u8dl_known_media_probe_keys(
            candidate,
            quality,
            headers,
        )
        if not key_values:
            return None

        last_error = None
        for key_value in key_values:
            try:
                return _ffprobe_nm3u8dl_stream_quality(
                    sample_input,
                    sample_headers,
                    sample_missing_bitrate=True,
                    timeout_route=timeout_route,
                    decryption_key=key_value,
                    target_quality=None,
                )
            except Exception as error:
                last_error = error

        if last_error is not None:
            raise last_error
        return None
    finally:
        if temp_path:
            try:
                os.remove(temp_path)
            except Exception:
                pass


def _sample_nm3u8dl_selected_dash_representation_bitrate(
    candidate: dict,
    quality: dict,
    headers: dict,
    *,
    timeout_route: str = "",
) -> int:
    """Sample bitrate only from the DASH representation already selected."""
    sample_input, sample_headers, sample_range, temp_path = (
        _prepare_nm3u8dl_selected_dash_media_sample(quality, headers)
    )
    if not sample_input:
        return 0

    try:
        key_values = _nm3u8dl_known_media_probe_keys(
            candidate,
            quality,
            headers,
        )
        if not key_values:
            return 0

        for key_value in key_values:
            sampled = _sample_nm3u8dl_stream_video_bitrate(
                sample_input,
                sample_headers,
                timeout_route=timeout_route,
                byte_range=sample_range,
                decryption_key=key_value,
            )
            if sampled > 0:
                return sampled
        return 0
    finally:
        if temp_path:
            try:
                os.remove(temp_path)
            except Exception:
                pass


def _detect_nm3u8dl_dash_selected_representation_scan_type(
    candidate: dict,
    quality: dict,
    headers: dict,
    *,
    scan_type_cache: Optional[dict] = None,
    timeout_route: str = "",
) -> tuple:
    """MPD scanType -> H.264 SPS/picture -> idet for one exact DASH rep."""
    if _normalize_nm3u8dl_video_scan_type(quality.get("video_scan_type")):
        return (
            _normalize_nm3u8dl_video_scan_type(quality.get("video_scan_type")),
            str(quality.get("video_scan_type_source") or "manifest"),
        )

    base_identity = _get_nm3u8dl_scan_type_cache_identity(
        quality.get("manifest_final_url") or candidate.get("stream_url") or ""
    )
    selected_identity = _get_nm3u8dl_dash_representation_cache_identity(quality)
    cache_key = (
        f"{base_identity}|dash-representation={selected_identity}"
        if base_identity and selected_identity and scan_type_cache is not None
        else ""
    )

    def detect_once() -> tuple:
        is_h264 = _is_nm3u8dl_h264_dash_representation(quality)

        if is_h264:
            init_url = str(quality.get("_dash_initialization_url") or "").strip()
            init_range = str(quality.get("_dash_initialization_range") or "").strip()

            if init_url:
                sps_text = _trace_nm3u8dl_h264_headers(
                    init_url,
                    headers,
                    byte_range=init_range,
                    timeout_route=timeout_route,
                )
                frame_mbs_only = _parse_nm3u8dl_h264_sps_frame_mbs_only(sps_text)
                if frame_mbs_only == 1:
                    return "progressive", "sps"

        sample_input, sample_headers, sample_range, temp_path = (
            _prepare_nm3u8dl_selected_dash_media_sample(quality, headers)
        )
        if not sample_input:
            return "", ""

        try:
            key_values = _nm3u8dl_known_media_probe_keys(
                candidate,
                quality,
                headers,
            )
            if not key_values:
                return "", ""

            if is_h264:
                for key_value in key_values:
                    picture_text = _trace_nm3u8dl_h264_headers(
                        sample_input,
                        sample_headers,
                        byte_range=sample_range,
                        decryption_key=key_value,
                        frame_limit=NM3U8DL_QUALITY_H264_PICTURE_SAMPLE_FRAMES,
                        timeout_route=timeout_route,
                    )
                    sample_frame_mbs_only = _parse_nm3u8dl_h264_sps_frame_mbs_only(
                        picture_text
                    )
                    if sample_frame_mbs_only == 1:
                        return "progressive", "sps"

                    picture_scan_type = _parse_nm3u8dl_h264_picture_structure(
                        picture_text
                    )
                    if picture_scan_type:
                        return picture_scan_type, "h264-picture"

            for key_value in key_values:
                idet_scan_type = _detect_nm3u8dl_stream_scan_type_with_idet(
                    sample_input,
                    sample_headers,
                    scan_type_cache=None,
                    timeout_route=timeout_route,
                    decryption_key=key_value,
                )
                if idet_scan_type:
                    return idet_scan_type, "idet"

            return "", ""
        finally:
            if temp_path:
                try:
                    os.remove(temp_path)
                except Exception:
                    pass

    if not cache_key:
        return detect_once()

    cache_guard = scan_type_cache["lock"]
    with cache_guard:
        if cache_key in scan_type_cache["results"]:
            cached = scan_type_cache["results"][cache_key]
            if isinstance(cached, tuple):
                return cached
            return str(cached or ""), ""
        stream_lock = scan_type_cache["stream_locks"].setdefault(
            cache_key,
            threading.Lock(),
        )

    with stream_lock:
        with cache_guard:
            if cache_key in scan_type_cache["results"]:
                cached = scan_type_cache["results"][cache_key]
                if isinstance(cached, tuple):
                    return cached
                return str(cached or ""), ""
        result = detect_once()
        with cache_guard:
            scan_type_cache["results"][cache_key] = result
        return result

def _detect_nm3u8dl_stream_scan_type_with_idet(
    stream_url: str,
    headers: dict,
    *,
    stream_index: Optional[int] = None,
    scan_type_cache: Optional[dict] = None,
    timeout_route: str = "",
    decryption_key: str = "",
) -> str:
    """Final P/I fallback using decoded frames from the targeted input."""
    command = [
        "ffmpeg",
        "-hide_banner",
        "-v", "info",
        "-nostdin",
    ]

    if stream_url.lower().startswith(("http://", "https://")) and headers:
        command.extend(_nm3u8dl_ffmpeg_header_args(headers))

    if decryption_key:
        command.extend(["-decryption_key", decryption_key])

    command.extend(["-i", stream_url])

    if stream_index is not None and int(stream_index) >= 0:
        command.extend(["-map", f"0:{int(stream_index)}"])
        stream_identity = str(int(stream_index))
    else:
        command.extend(["-map", "0:v:0"])
        stream_identity = "v0"

    command.extend([
        "-an",
        "-sn",
        "-dn",
        "-vf", (
            "idet=intl_thres="
            f"{float(NM3U8DL_QUALITY_IDET_INTERLACE_THRESHOLD):g}"
        ),
        "-frames:v", str(int(NM3U8DL_QUALITY_IDET_SAMPLE_FRAMES)),
        "-f", "null", "-",
    ])

    parsed_url = urlparse(stream_url)
    probe_identity = (
        parsed_url.netloc + parsed_url.path
        if parsed_url.netloc
        else "selected representation"
    )
    timeout_identity = str(timeout_route or probe_identity).strip()

    def run_idet_probe() -> tuple:
        try:
            result = _run_nm3u8dl_external_capture_redacted(
                command,
                raw_tool="ffmpeg",
                raw_context=f"candidate scan-type idet | {timeout_identity}",
                timeout=NM3U8DL_QUALITY_IDET_TIMEOUT_SEC,
                secret_values=(decryption_key,),
            )
        except Exception:
            return "", False

        if result.returncode != 0:
            return "", False

        return _parse_nm3u8dl_idet_scan_type(result.stderr or ""), True

    base_cache_key = _get_nm3u8dl_scan_type_cache_identity(stream_url)
    cache_key = (
        f"{base_cache_key}|stream={stream_identity}"
        if base_cache_key and scan_type_cache is not None
        else ""
    )

    if not cache_key:
        scan_type, _ = run_idet_probe()
        return scan_type

    cache_guard = scan_type_cache["lock"]
    with cache_guard:
        if cache_key in scan_type_cache["results"]:
            cached = scan_type_cache["results"][cache_key]
            if isinstance(cached, tuple):
                return str(cached[0] or "")
            return str(cached or "")
        stream_lock = scan_type_cache["stream_locks"].setdefault(
            cache_key,
            threading.Lock(),
        )

    with stream_lock:
        with cache_guard:
            if cache_key in scan_type_cache["results"]:
                cached = scan_type_cache["results"][cache_key]
                if isinstance(cached, tuple):
                    return str(cached[0] or "")
                return str(cached or "")
        scan_type, cacheable = run_idet_probe()
        if cacheable:
            with cache_guard:
                scan_type_cache["results"][cache_key] = scan_type
        return scan_type

def _ffprobe_nm3u8dl_stream_quality(
    stream_url: str,
    headers: dict,
    *,
    sample_missing_bitrate: bool = True,
    timeout_route: str = "",
    decryption_key: str = "",
    target_quality: Optional[dict] = None,
) -> Optional[dict]:
    command = source_quality.build_ffprobe_quality_command(
        stream_url,
        headers,
        decryption_key=decryption_key,
    )

    parsed_url = urlparse(stream_url)
    probe_identity = (
        parsed_url.netloc + parsed_url.path
        if parsed_url.netloc
        else "candidate stream"
    )
    timeout_identity = str(timeout_route or probe_identity).strip()
    result = _run_nm3u8dl_external_capture_redacted(
        command,
        raw_tool="ffprobe",
        raw_context=f"candidate quality probe | {timeout_identity}",
        timeout=NM3U8DL_QUALITY_FFPROBE_TIMEOUT_SEC,
        secret_values=(decryption_key,),
    )

    stdout = (result.stdout or "").strip()
    if not stdout:
        raise RuntimeError(_summarize_nm3u8dl_ffprobe_failure(result))

    try:
        best_quality = source_quality.parse_ffprobe_quality_output(
            stdout,
            target_quality=target_quality,
            motion_cap_fps=NM3U8DL_QUALITY_RANKING_MOTION_CAP_FPS,
        )
    except Exception as error:
        raise RuntimeError(
            f"ffprobe quality output could not be parsed ({type(error).__name__}: {error})"
        ) from error

    if not best_quality:
        raise RuntimeError(_summarize_nm3u8dl_ffprobe_failure(result))

    if (
        sample_missing_bitrate
        and int(best_quality.get("video_bitrate_bps") or 0) <= 0
    ):
        sampled_bitrate = _sample_nm3u8dl_stream_video_bitrate(
            stream_url,
            headers,
            timeout_route=timeout_identity,
            decryption_key=decryption_key,
            stream_index=int(best_quality.get("_ffprobe_stream_index") or 0),
        )
        if sampled_bitrate > 0:
            best_quality["video_bitrate_bps"] = sampled_bitrate
            best_quality["video_bitrate_source"] = "sample"
            best_quality["quality_known"] = True

    return best_quality

def _probe_nm3u8dl_candidate_quality(
    candidate: dict,
    stop_requested: Optional[Callable[[], bool]] = None,
    scan_type_cache: Optional[dict] = None,
    timeout_playlist_urls: Optional[List[str]] = None,
) -> dict:
    probe_started = time.monotonic()

    playlist_urls_for_timeout = [
        str(value or "").strip()
        for value in (timeout_playlist_urls or [])
        if str(value or "").strip()
    ]
    if not playlist_urls_for_timeout:
        candidate_playlist_url = str(
            candidate.get("playlist_url") or ""
        ).strip()
        if candidate_playlist_url:
            playlist_urls_for_timeout.append(candidate_playlist_url)

    candidate_timeout_route = _format_candidate_timeout_route(
        playlist_urls_for_timeout,
        candidate.get("stream_url") or "",
    )

    if stop_requested is not None and stop_requested():
        raise RuntimeError("Quality probe cancelled by stop request")

    quality = {
        "quality_known": False,
        "quality_source": "",
        "video_fps": 0.0,
        "video_fps_source": "",
        "video_width": 0,
        "video_height": 0,
        "video_resolution_source": "",
        "video_scan_type": "",
        "video_scan_type_source": "",
        "video_bitrate_bps": 0,
        "video_bitrate_source": "",
        "manifest_expiry": None,
        "resource_expiry": None,
        "manifest_final_url": "",
        "selected_media_final_url": "",
        "resource_probe_failure": "",
        "manifest_reachable": False,
        "ffprobe_reachable": False,
        "launchable": False,
        "drm_protected": False,
        "drm_key_required": False,
        "drm_key_missing": False,
        "drm_detail": "",
        "drm_inspection_failure": "",
        "hls_variant_probe_status": "",
        "hls_variant_probe_failure": "",
        "access_blocked": False,
        "access_block_kind": "",
        "access_block_http_status": None,
        "geo_country": None,
        "stream_type": _get_nm3u8dl_stream_type_from_url(
            candidate.get("stream_url") or ""
        ),
        "header_preparation_failure": "",
        "manifest_probe_failure": "",
        "ffprobe_probe_failure": "",
        "quality_probe_error": "",
        "effective_headers": {},
        "probe_duration_sec": None,
    }

    errors = []

    try:
        effective_headers = get_nm3u8dl_effective_headers(
            candidate.get("headers") or {},
            emit_logs=False,
        )
        quality["effective_headers"] = dict(effective_headers)
    except Exception as error:
        effective_headers = dict(candidate.get("headers") or {})
        quality["effective_headers"] = dict(effective_headers)
        quality["header_preparation_failure"] = (
            f"header preparation failed ({type(error).__name__})"
        )
        errors.append(
            quality["header_preparation_failure"]
        )

    manifest_quality = None

    try:
        manifest_text, final_manifest_url = (
            _fetch_nm3u8dl_stream_manifest_text(
                candidate["stream_url"],
                effective_headers,
                include_final_url=True,
                stop_requested=stop_requested,
            )
        )

        quality["manifest_final_url"] = str(final_manifest_url or "").strip()

        final_manifest_type = _get_nm3u8dl_stream_type_from_url(
            final_manifest_url
        )
        if final_manifest_type:
            quality["stream_type"] = final_manifest_type

        redirected_expiry = get_nm3u8dl_auth_expiry(
            final_manifest_url
        )

        if redirected_expiry is not None:
            quality["manifest_expiry"] = redirected_expiry

        stripped_manifest = manifest_text.lstrip()

        if stripped_manifest.startswith("#EXTM3U"):
            quality["manifest_reachable"] = True
            quality["stream_type"] = "HLS"
            manifest_quality = _parse_nm3u8dl_hls_manifest_quality(
                manifest_text,
                final_manifest_url,
            )

            quality.update(
                _inspect_nm3u8dl_hls_manifest_drm(
                    manifest_text
                )
            )

            # A master playlist can look healthy while encryption is declared
            # only in its child media playlist. For an unkeyed candidate, inspect
            # the selected child before allowing the source to become launchable.
            if (
                not (candidate.get("keys") or [])
                and not quality.get("drm_key_required")
                and manifest_quality
            ):
                variant_url = str(
                    manifest_quality.get("manifest_variant_url")
                    or ""
                ).strip()

                if variant_url:
                    variant_text = ""
                    child_error = None

                    try:
                        variant_text = (
                            _fetch_nm3u8dl_stream_manifest_text(
                                variant_url,
                                effective_headers,
                                stop_requested=stop_requested,
                            )
                        )
                    except HTTPError as error:
                        child_error = error

                        # Proven production case: the HLS master sets an
                        # authorization cookie that the child requires. urllib
                        # urlopen() calls do not retain that response cookie, so
                        # retry the master -> child sequence in one CookieJar.
                        if getattr(error, "code", None) == 403:
                            try:
                                variant_text = (
                                    _fetch_nm3u8dl_hls_child_with_master_cookie_session(
                                        final_manifest_url,
                                        variant_url,
                                        effective_headers,
                                        stop_requested=stop_requested,
                                    )
                                )
                                child_error = None
                            except Exception as retry_error:
                                child_error = retry_error
                    except Exception as error:
                        child_timeout_route = _format_candidate_timeout_route(
                            playlist_urls_for_timeout,
                            variant_url,
                        )
                        log_timeout_exception(
                            error,
                            "HTTP",
                            (
                                NM3U8DL_QUALITY_HTTP_TIMEOUT_SEC + 5
                                if _is_nm3u8dl_drmlive_host(variant_url)
                                else NM3U8DL_QUALITY_HTTP_TIMEOUT_SEC
                            ),
                            context=(
                                "HLS variant check | "
                                f"{child_timeout_route}"
                            ),
                        )
                        child_error = error

                    if variant_text:
                        if not variant_text.lstrip().startswith("#EXTM3U"):
                            child_failure = (
                                source_transport.classify_hls_variant_probe_failure(
                                    non_hls_response=True,
                                )
                            )
                            quality["hls_variant_probe_status"] = str(
                                child_failure.get("status") or ""
                            )
                            quality["hls_variant_probe_failure"] = str(
                                child_failure.get("reason") or ""
                            )
                        else:
                            try:
                                variant_drm = (
                                    _inspect_nm3u8dl_hls_manifest_drm(
                                        variant_text
                                    )
                                )
                            except Exception as error:
                                quality["drm_inspection_failure"] = (
                                    "HLS DRM inspection failed — "
                                    + _describe_nm3u8dl_probe_exception(error)
                                )
                            else:
                                quality["drm_protected"] = bool(
                                    quality.get("drm_protected")
                                    or variant_drm.get("drm_protected")
                                )

                                if variant_drm.get("drm_key_required"):
                                    quality["drm_key_required"] = True
                                    quality["drm_detail"] = str(
                                        variant_drm.get("drm_detail")
                                        or ""
                                    )
                    elif child_error is not None:
                        child_failure = (
                            source_transport.classify_hls_variant_probe_failure(
                                child_error,
                            )
                        )
                        quality["hls_variant_probe_status"] = str(
                            child_failure.get("status") or ""
                        )
                        quality["hls_variant_probe_failure"] = str(
                            child_failure.get("reason") or ""
                        )

        elif re.search(
            r'<(?:[A-Za-z_][\w.-]*:)?MPD\b',
            stripped_manifest,
            re.IGNORECASE,
        ):
            quality["manifest_reachable"] = True
            quality["stream_type"] = "DASH"
            manifest_quality = _parse_nm3u8dl_dash_manifest_quality(
                manifest_text,
                final_manifest_url,
            )
            quality.update(
                _inspect_nm3u8dl_dash_manifest_drm(
                    manifest_text
                )
            )
        else:
            quality["manifest_probe_failure"] = (
                "response was not a recognizable HLS/DASH manifest"
            )

        if manifest_quality:
            manifest_quality["manifest_expiry"] = (
                _merge_nm3u8dl_auth_expiries(
                    manifest_quality.get("manifest_expiry"),
                    redirected_expiry,
                )
            )

    except Exception as error:
        log_timeout_exception(
            error,
            "HTTP",
            (
                NM3U8DL_QUALITY_HTTP_TIMEOUT_SEC + 5
                if _is_nm3u8dl_drmlive_host(candidate["stream_url"])
                else NM3U8DL_QUALITY_HTTP_TIMEOUT_SEC
            ),
            context=(
                "candidate manifest fetch | "
                f"{candidate_timeout_route}"
            ),
        )

        quality["manifest_probe_failure"] = (
            _describe_nm3u8dl_probe_exception(error)
        )

        if isinstance(error, HTTPError):
            status_code = getattr(error, "code", None)
            response_headers = getattr(error, "headers", None)

            error_type = (
                str(response_headers.get("X-ErrorType") or "").strip()
                if response_headers is not None
                else ""
            )

            # urllib follows redirects before raising HTTPError. If the final
            # redirected authorization URL contains an expiry, preserve it so an
            # expired source is reported transparently instead of as unknown-expiry.
            final_error_url = str(error.geturl() or "").strip()
            quality["manifest_final_url"] = final_error_url

            final_error_type = _get_nm3u8dl_stream_type_from_url(
                final_error_url
            )
            if final_error_type:
                quality["stream_type"] = final_error_type

            redirected_expiry = get_nm3u8dl_auth_expiry(final_error_url)

            if redirected_expiry is not None:
                quality["manifest_expiry"] = redirected_expiry

            access = source_transport.classify_http_access_error(
                error,
                source_group=NM3U8DL_PLAYLIST_GROUP,
                provider=SHARED_PLAYLIST_GROUP_PROFILES.get(
                    NM3U8DL_PLAYLIST_GROUP.strip().upper(),
                    "",
                ),
            )
            quality["access_blocked"] = bool(access.get("blocked"))
            quality["access_block_kind"] = str(access.get("kind") or "")
            quality["access_block_http_status"] = access.get("http_status")
            quality["geo_country"] = access.get("geo_country")

        errors.append(
            f"manifest probe failed ({type(error).__name__})"
        )

    if manifest_quality:
        quality.update(manifest_quality)
        quality["quality_source"] = "manifest"

        if float(quality.get("video_fps") or 0.0) > 0:
            quality["video_fps_source"] = "manifest"

        if (
            int(quality.get("video_width") or 0) > 0
            or int(quality.get("video_height") or 0) > 0
        ):
            quality["video_resolution_source"] = "manifest"

        if int(quality.get("video_bitrate_bps") or 0) > 0:
            quality["video_bitrate_source"] = "manifest"

        if quality.get("stream_type") == "DASH":
            resource_route = _resolve_nm3u8dl_selected_dash_resource_route(
                quality,
                effective_headers,
                stop_requested=stop_requested,
            )

            if resource_route.get("final_url"):
                quality["selected_media_final_url"] = str(
                    resource_route.get("final_url") or ""
                ).strip()
                quality["resource_expiry"] = _merge_nm3u8dl_auth_expiries(
                    quality.get("resource_expiry"),
                    resource_route.get("resource_expiry"),
                )

                route_index = resource_route.get("route_index")
                if route_index is not None:
                    quality["_dash_selected_route_index"] = int(route_index)
            elif resource_route.get("failure"):
                quality["resource_probe_failure"] = str(
                    resource_route.get("failure") or ""
                ).strip()

    probe_stream_url = str(
        quality.get("manifest_variant_url")
        or candidate["stream_url"]
    ).strip()
    probe_timeout_route = _format_candidate_timeout_route(
        playlist_urls_for_timeout,
        quality.get("manifest_final_url") or probe_stream_url,
    )

    # EVENT policy: event streams are treated as progressive.
    # This deliberately prevents DASH/HLS P/I probing, including idet.
    # LINEAR_TV is completely unchanged and continues through the existing
    # manifest/SPS/picture/FFprobe/idet P/I detection path below.
    event_lifecycle = (
        NM3U8DL_SOURCE_MODE == "playlist"
        and get_nm3u8dl_playlist_lifecycle() == "EVENT"
    )
    if event_lifecycle:
        quality["video_scan_type"] = "progressive"
        quality["video_scan_type_source"] = "event-policy"

    # DASH P/I is independent from ffprobe quality fallback. MPD scanType has
    # already been applied by the parser. Only a missing DASH scan type enters
    # the selected-representation SPS -> picture/field -> idet path.
    if (
        manifest_quality
        and quality.get("stream_type") == "DASH"
        and not _normalize_nm3u8dl_video_scan_type(
            quality.get("video_scan_type")
        )
    ):
        dash_scan_type, dash_scan_source = (
            _detect_nm3u8dl_dash_selected_representation_scan_type(
                candidate,
                quality,
                effective_headers,
                scan_type_cache=scan_type_cache,
                timeout_route=probe_timeout_route,
            )
        )
        if dash_scan_type:
            quality["video_scan_type"] = dash_scan_type
            quality["video_scan_type_source"] = dash_scan_source

    # Missing DASH quality is probed only from the exact representation already
    # selected by the MPD parser. Never reopen the whole multi-representation MPD
    # with FFprobe just to recover one missing field. HLS/direct-stream behavior
    # keeps its established general FFprobe fallback.
    manifest_complete = (
        float(quality.get("video_fps") or 0.0) > 0
        and int(quality.get("video_width") or 0) > 0
        and int(quality.get("video_height") or 0) > 0
        and int(quality.get("video_bitrate_bps") or 0) > 0
    )
    successful_ffprobe_key = None

    if (
        quality.get("stream_type") == "DASH"
        and manifest_quality
        and not manifest_complete
    ):
        try:
            ffprobe_quality = (
                _ffprobe_nm3u8dl_selected_dash_representation_quality(
                    candidate,
                    quality,
                    effective_headers,
                    timeout_route=probe_timeout_route,
                )
            )

            if ffprobe_quality:
                quality["ffprobe_reachable"] = True

                ffprobe_fps = float(
                    ffprobe_quality.get("video_fps") or 0.0
                )
                if (
                    not float(quality.get("video_fps") or 0.0)
                    and ffprobe_fps > 0
                ):
                    quality["video_fps"] = ffprobe_fps
                    quality["video_fps_source"] = "ffprobe"

                resolution_filled_from_ffprobe = False
                for field in ("video_width", "video_height"):
                    ffprobe_value = int(ffprobe_quality.get(field) or 0)
                    if not int(quality.get(field) or 0) and ffprobe_value > 0:
                        quality[field] = ffprobe_value
                        resolution_filled_from_ffprobe = True

                if resolution_filled_from_ffprobe:
                    existing_resolution_source = str(
                        quality.get("video_resolution_source") or ""
                    ).strip()
                    quality["video_resolution_source"] = (
                        "manifest+ffprobe"
                        if existing_resolution_source == "manifest"
                        else "ffprobe"
                    )

                if not int(quality.get("video_bitrate_bps") or 0):
                    ffprobe_bitrate = int(
                        ffprobe_quality.get("video_bitrate_bps") or 0
                    )
                    if ffprobe_bitrate > 0:
                        quality["video_bitrate_bps"] = ffprobe_bitrate
                        quality["video_bitrate_source"] = str(
                            ffprobe_quality.get("video_bitrate_source") or ""
                        )

                quality["quality_source"] = "manifest+ffprobe"

        except Exception as error:
            quality["ffprobe_probe_failure"] = (
                _describe_nm3u8dl_probe_exception(error)
            )
            failure_text = f"ffprobe failed ({type(error).__name__})"
            if failure_text not in errors:
                errors.append(failure_text)

    manifest_complete = (
        float(quality.get("video_fps") or 0.0) > 0
        and int(quality.get("video_width") or 0) > 0
        and int(quality.get("video_height") or 0) > 0
        and int(quality.get("video_bitrate_bps") or 0) > 0
    )
    hls_needs_scan_type = (
        quality.get("stream_type") == "HLS"
        and quality.get("manifest_reachable")
        and not _normalize_nm3u8dl_video_scan_type(
            quality.get("video_scan_type")
        )
    )
    needs_general_ffprobe = (
        quality.get("stream_type") != "DASH"
        and ((not manifest_complete) or hls_needs_scan_type)
    )

    if needs_general_ffprobe:
        if stop_requested is not None and stop_requested():
            raise RuntimeError("Quality probe cancelled by stop request")

        if quality.get("stream_type") == "HLS":
            ffprobe_key_values = _nm3u8dl_known_media_probe_keys(
                candidate,
                quality,
                effective_headers,
            )
        else:
            ffprobe_key_values = [""]

        for ffprobe_key in ffprobe_key_values:
            try:
                ffprobe_quality = _ffprobe_nm3u8dl_stream_quality(
                    probe_stream_url,
                    effective_headers,
                    sample_missing_bitrate=(
                        int(quality.get("video_bitrate_bps") or 0) <= 0
                    ),
                    timeout_route=probe_timeout_route,
                    decryption_key=ffprobe_key,
                    target_quality=quality if manifest_quality else None,
                )

                if ffprobe_quality:
                    successful_ffprobe_key = ffprobe_key
                    quality["ffprobe_reachable"] = True

                    ffprobe_fps = float(
                        ffprobe_quality.get("video_fps") or 0.0
                    )
                    if (
                        not float(quality.get("video_fps") or 0.0)
                        and ffprobe_fps > 0
                    ):
                        quality["video_fps"] = ffprobe_fps
                        quality["video_fps_source"] = "ffprobe"

                    resolution_filled_from_ffprobe = False

                    for field in (
                        "video_width",
                        "video_height",
                    ):
                        ffprobe_value = int(
                            ffprobe_quality.get(field) or 0
                        )
                        if not int(quality.get(field) or 0) and ffprobe_value > 0:
                            quality[field] = ffprobe_value
                            resolution_filled_from_ffprobe = True

                    if resolution_filled_from_ffprobe:
                        existing_resolution_source = str(
                            quality.get("video_resolution_source") or ""
                        ).strip()

                        if existing_resolution_source == "manifest":
                            quality["video_resolution_source"] = (
                                "manifest+ffprobe"
                            )
                        else:
                            quality["video_resolution_source"] = "ffprobe"

                    if not quality.get("video_bitrate_bps"):
                        ffprobe_bitrate = int(
                            ffprobe_quality.get("video_bitrate_bps") or 0
                        )
                        quality["video_bitrate_bps"] = ffprobe_bitrate
                        quality["video_bitrate_source"] = str(
                            ffprobe_quality.get("video_bitrate_source") or ""
                        )

                    if (
                        quality.get("stream_type") == "HLS"
                        and not _normalize_nm3u8dl_video_scan_type(
                            quality.get("video_scan_type")
                        )
                    ):
                        ffprobe_scan_type = _normalize_nm3u8dl_video_scan_type(
                            ffprobe_quality.get("video_scan_type")
                        )
                        if ffprobe_scan_type:
                            quality["video_scan_type"] = ffprobe_scan_type
                            quality["video_scan_type_source"] = "ffprobe"

                    quality["quality_known"] = bool(
                        quality.get("video_fps")
                        or (
                            quality.get("video_width")
                            and quality.get("video_height")
                        )
                        or quality.get("video_bitrate_bps")
                    )

                    quality["quality_source"] = (
                        "manifest+ffprobe"
                        if manifest_quality
                        else "ffprobe"
                    )
                    break

            except Exception as error:
                quality["ffprobe_probe_failure"] = (
                    _describe_nm3u8dl_probe_exception(error)
                )
                failure_text = f"ffprobe failed ({type(error).__name__})"
                if failure_text not in errors:
                    errors.append(failure_text)

    # If DASH bitrate is still absent after exact-representation metadata probing,
    # sample only the exact representation already selected by the MPD parser.
    if (
        quality.get("stream_type") == "DASH"
        and manifest_quality
        and int(quality.get("video_bitrate_bps") or 0) <= 0
    ):
        sampled_bitrate = _sample_nm3u8dl_selected_dash_representation_bitrate(
            candidate,
            quality,
            effective_headers,
            timeout_route=probe_timeout_route,
        )
        if sampled_bitrate > 0:
            quality["video_bitrate_bps"] = sampled_bitrate
            quality["video_bitrate_source"] = "sample"
            quality["quality_known"] = True

    # HLS P/I: FFprobe first, then idet. Encrypted HLS starts with an already-known
    # key rather than deliberately probing without it and retrying later.
    if (
        quality.get("stream_type") == "HLS"
        and quality.get("manifest_reachable")
        and not _normalize_nm3u8dl_video_scan_type(
            quality.get("video_scan_type")
        )
    ):
        known_idet_keys = _nm3u8dl_known_media_probe_keys(
            candidate,
            quality,
            effective_headers,
        )
        if successful_ffprobe_key is not None:
            idet_key_values = [successful_ffprobe_key] + [
                key_value
                for key_value in known_idet_keys
                if key_value != successful_ffprobe_key
            ]
        else:
            idet_key_values = known_idet_keys

        for idet_key in idet_key_values:
            idet_scan_type = _detect_nm3u8dl_stream_scan_type_with_idet(
                probe_stream_url,
                effective_headers,
                scan_type_cache=scan_type_cache,
                timeout_route=probe_timeout_route,
                decryption_key=idet_key,
            )
            if idet_scan_type:
                quality["video_scan_type"] = idet_scan_type
                quality["video_scan_type_source"] = "idet"
                break

    quality["quality_known"] = bool(
        quality.get("video_fps")
        or (
            quality.get("video_width")
            and quality.get("video_height")
        )
        or quality.get("video_bitrate_bps")
    )

    quality["launchable"] = bool(
        quality.get("manifest_reachable")
        or quality.get("ffprobe_reachable")
    )

    quality["drm_key_missing"] = bool(
        quality.get("drm_key_required")
        and not (candidate.get("keys") or [])
    )

    if (
        quality["drm_key_missing"]
        or quality.get("drm_inspection_failure")
        or quality.get("hls_variant_probe_failure")
    ):
        quality["launchable"] = False

    if quality["launchable"]:
        quality["access_blocked"] = False
        quality["access_block_kind"] = ""
        quality["access_block_http_status"] = None
        quality["geo_country"] = None

    if errors:
        quality["quality_probe_error"] = "; ".join(errors)

    quality["probe_duration_sec"] = round(
        time.monotonic() - probe_started,
        6,
    )
    return quality


def _get_nm3u8dl_candidate_probe_identity(candidate: dict) -> tuple:
    """Use the shared effective-stream quality-probe identity."""
    try:
        identity_headers = get_nm3u8dl_effective_headers(
            candidate.get("headers") or {},
            emit_logs=False,
        )
    except Exception:
        identity_headers = dict(candidate.get("headers") or {})

    return source_quality.quality_probe_identity(
        SourceCandidate.from_mapping(candidate),
        effective_headers=identity_headers,
    )


def enrich_nm3u8dl_candidate_qualities(
    candidates: List[dict],
    *,
    stop_requested: Optional[Callable[[], bool]] = None,
    show_progress: bool = True,
) -> bool:
    if not candidates:
        return True

    if stop_requested is not None and stop_requested():
        return False

    total_candidates = len(candidates)

    if show_progress:
        render_dynamic_playlist_progress(
            "checking matched candidates",
            1,
            total_candidates,
        )

    probe_groups = {}

    for candidate in candidates:
        probe_identity = _get_nm3u8dl_candidate_probe_identity(candidate)
        probe_groups.setdefault(probe_identity, []).append(candidate)

    worker_count = min(
        max(1, int(NM3U8DL_QUALITY_PROBE_WORKERS)),
        len(probe_groups),
    )

    # One scan-local cache lets duplicate playlist routes share one P/I result
    # when their provider-neutral effective stream identity is the same.
    scan_type_cache = {
        "lock": threading.Lock(),
        "stream_locks": {},
        "results": {},
    }

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {}

        for grouped_candidates in probe_groups.values():
            representative = grouped_candidates[0]
            future = executor.submit(
                _probe_nm3u8dl_candidate_quality,
                representative,
                stop_requested=stop_requested,
                scan_type_cache=scan_type_cache,
                timeout_playlist_urls=[
                    str(candidate.get("playlist_url") or "").strip()
                    for candidate in grouped_candidates
                    if str(candidate.get("playlist_url") or "").strip()
                ],
            )
            future_map[future] = grouped_candidates

        completed_candidates = 0

        for future in as_completed(future_map):
            if stop_requested is not None and stop_requested():
                break

            grouped_candidates = future_map[future]
            representative = grouped_candidates[0]

            try:
                quality = future.result()
            except Exception as error:
                quality = {
                    "quality_known": False,
                    "quality_source": "",
                    "video_fps": 0.0,
                    "video_width": 0,
                    "video_height": 0,
                    "video_scan_type": "",
                    "video_scan_type_source": "",
                    "video_bitrate_bps": 0,
                    "video_bitrate_source": "",
                    "manifest_expiry": None,
                    "resource_expiry": None,
                    "manifest_final_url": "",
                    "selected_media_final_url": "",
                    "resource_probe_failure": "",
                    "manifest_reachable": False,
                    "ffprobe_reachable": False,
                    "launchable": False,
                    "drm_protected": False,
                    "drm_key_required": False,
                    "drm_key_missing": False,
                    "drm_detail": "",
                    "drm_inspection_failure": "",
                    "access_blocked": False,
                    "access_block_kind": "",
                    "access_block_http_status": None,
                    "geo_country": None,
                    "stream_type": _get_nm3u8dl_stream_type_from_url(
                        representative.get("stream_url") or ""
                    ),
                    "header_preparation_failure": "",
                    "manifest_probe_failure": "",
                    "ffprobe_probe_failure": (
                        f"quality probe failed ({type(error).__name__})"
                    ),
                    "quality_probe_error": (
                        f"quality probe failed ({type(error).__name__})"
                    ),
                    "effective_headers": dict(
                        representative.get("headers") or {}
                    ),
                    "probe_duration_sec": None,
                }

            for candidate in grouped_candidates:
                candidate_quality = dict(quality)

                if isinstance(quality.get("effective_headers"), dict):
                    candidate_quality["effective_headers"] = dict(
                        quality["effective_headers"]
                    )

                url_header_expiry = candidate.get("url_header_expiry")

                candidate.update(candidate_quality)

                if candidate.get("unsupported_drm"):
                    # Keep all probe evidence (quality, redirect, expiry), but never
                    # promote a DRM mode the recorder cannot decrypt into selection.
                    candidate["launchable"] = False

                manifest_expiry = candidate.get("manifest_expiry")
                resource_expiry = candidate.get("resource_expiry")

                candidate["expiry"] = _merge_nm3u8dl_auth_expiries(
                    url_header_expiry,
                    manifest_expiry,
                    resource_expiry,
                )
                candidate["expiry_source"] = get_nm3u8dl_expiry_source(
                    url_header_expiry,
                    manifest_expiry,
                    resource_expiry,
                )

            completed_candidates += len(grouped_candidates)

            if show_progress:
                render_dynamic_playlist_progress(
                    "checking matched candidates",
                    completed_candidates,
                    total_candidates,
                )

    if stop_requested is not None and stop_requested():
        if show_progress:
            clear_progress_line()
        return False

    # The resolver finishes this phase after it has calculated current stream
    # fingerprints, because a probed/launchable candidate can still be EXCLUDED.
    return True


def format_nm3u8dl_access_block_warning(
    candidates: List[dict]
) -> str:
    countries = sorted({
        str(candidate.get("geo_country") or "").strip()
        for candidate in candidates
        if (
            candidate.get("access_blocked", False)
            and str(candidate.get("geo_country") or "").strip()
        )
    })

    kinds = {
        str(candidate.get("access_block_kind") or "").strip()
        for candidate in candidates
        if candidate.get("access_blocked", False)
    }

    if kinds == {"confirmed_geo"}:
        if len(countries) == 1:
            return (
                "MANIFEST ACCESS BLOCKED: confirmed geo-block "
                f"(detected country: {countries[0]})"
            )

        if len(countries) > 1:
            return (
                "MANIFEST ACCESS BLOCKED: confirmed geo-block "
                f"(detected countries: {', '.join(countries)})"
            )

        return "MANIFEST ACCESS BLOCKED: confirmed geo-block"

    if kinds == {"vpn_route_suspected"}:
        status_codes = sorted({
            int(candidate.get("access_block_http_status"))
            for candidate in candidates
            if (
                candidate.get("access_blocked", False)
                and candidate.get("access_block_kind") == "vpn_route_suspected"
                and candidate.get("access_block_http_status") is not None
            )
        })
        http_text = (
            f"HTTP {'/'.join(str(code) for code in status_codes)} Forbidden"
            if status_codes
            else "access restriction"
        )
        return (
            f"MANIFEST ACCESS BLOCKED: {http_text} — "
            "VPN/route suspected"
        )

    return (
        "MANIFEST ACCESS BLOCKED: source access restriction detected — "
        "VPN/route attention may be needed"
    )


def _make_nm3u8dl_candidate_display_row(candidate: dict) -> dict:
    """Keep only safe, presentation-relevant candidate facts in source results."""
    fields = (
        "playlist_url",
        "matching_entry_index",
        "tvg_name",
        "entry_title",
        "stream_url",
        "manifest_final_url",
        "selected_media_final_url",
        "manifest_variant_url",
        "expiry",
        "resource_expiry",
        "expiry_source",
        "quality_known",
        "quality_source",
        "video_fps",
        "video_fps_source",
        "video_width",
        "video_height",
        "video_resolution_source",
        "video_scan_type",
        "video_scan_type_source",
        "video_bitrate_bps",
        "video_bitrate_source",
        "manifest_reachable",
        "ffprobe_reachable",
        "launchable",
        "drm_protected",
        "drm_key_required",
        "drm_key_missing",
        "drm_detail",
        "drm_inspection_failure",
        "hls_variant_probe_status",
        "hls_variant_probe_failure",
        "access_blocked",
        "access_block_kind",
        "access_block_http_status",
        "geo_country",
        "stream_type",
        "license_type",
        "unsupported_drm",
        "header_preparation_failure",
        "manifest_probe_failure",
        "resource_probe_failure",
        "ffprobe_probe_failure",
        "preferred_qualifier_score",
        "stream_fingerprint",
        "failover_excluded",
        "failover_exclusion_reason",
    )
    return {
        field: candidate.get(field)
        for field in fields
    }


def get_nm3u8dl_candidate_source_family(candidate: dict) -> str:
    """Best-effort delivery-family label from the effective media host."""
    hosts = []

    for url_value in (
        candidate.get("selected_media_final_url"),
        candidate.get("manifest_variant_url"),
        candidate.get("manifest_final_url"),
        candidate.get("stream_url"),
    ):
        try:
            host = str(
                urlparse(str(url_value or "")).hostname or ""
            ).strip().casefold()
        except Exception:
            host = ""

        if host and host not in hosts:
            hosts.append(host)

    for host in hosts:
        if host == "jiotvpllive.cdn.jio.com":
            return "JioTV+/STB"

        if host == "jiotvmblive.cdn.jio.com":
            return "Jio Mobile"

        if host == "hotstar.com" or host.endswith(".hotstar.com"):
            return "Hotstar Digital"

        if host == "dishmt.slivcdn.com":
            return "SonyLIV"

    return ""


def _nm3u8dl_candidate_identity(candidate: dict) -> str:
    stream_type = str(
        candidate.get("stream_type")
        or _get_nm3u8dl_stream_type_from_url(
            candidate.get("stream_url") or ""
        )
        or "STREAM"
    ).strip().upper()

    hosts = []

    for url_value in (
        candidate.get("stream_url"),
        candidate.get("manifest_final_url"),
        candidate.get("selected_media_final_url"),
        candidate.get("manifest_variant_url"),
    ):
        try:
            current_host = str(
                urlparse(str(url_value or "")).hostname or ""
            ).strip()
        except Exception:
            current_host = ""

        if (
            current_host
            and not any(
                current_host.casefold() == existing.casefold()
                for existing in hosts
            )
        ):
            hosts.append(current_host)

    source_family = get_nm3u8dl_candidate_source_family(candidate)
    family_text = f" [{source_family}]" if source_family else ""

    if hosts:
        return f"{stream_type} ({' → '.join(hosts)}){family_text}"

    return f"{stream_type}{family_text}"


def _nm3u8dl_same_candidate(left: Optional[dict], right: Optional[dict]) -> bool:
    if not left or not right:
        return False

    return (
        str(left.get("playlist_url") or "")
        == str(right.get("playlist_url") or "")
        and int(left.get("matching_entry_index") or 0)
        == int(right.get("matching_entry_index") or 0)
        and int(left.get("matching_entry_index") or 0) > 0
    )


def _nm3u8dl_candidate_status(
    candidate: dict,
    *,
    selected_candidate: Optional[dict],
    now_ts: float,
) -> str:
    if candidate.get("ignored"):
        return "IGNORED"

    if candidate.get("failover_excluded", False):
        return "EXCLUDED"

    expiry = candidate.get("expiry")

    if expiry is None and candidate.get("expiry_required", False):
        return "EXPIRY UNKNOWN"

    if expiry is not None and float(expiry) <= float(now_ts):
        return "EXPIRED"

    if candidate.get("access_blocked", False):
        return "BLOCKED"

    if candidate.get("launchable", False):
        if _nm3u8dl_same_candidate(candidate, selected_candidate):
            return "SELECTED"
        return "WORKING"

    return "NOT WORKING"


def _nm3u8dl_candidate_display_classification(
    candidate: dict,
    *,
    status: str,
) -> str:
    """Return the user-facing classification token for a candidate row.

    Operational status stays separate from presentation.  In particular,
    DRM failures remain operationally NOT WORKING while still presenting a
    precise classification before the candidate identity.
    """
    standard = {
        "SELECTED": "SELECTED",
        "EXPIRED": "EXPIRED",
        "EXPIRY UNKNOWN": "EXPIRY UNKNOWN",
        "BLOCKED": "BLOCKED",
        "IGNORED": "IGNORED",
        "EXCLUDED": "EXCLUDED",
    }.get(status)

    if standard:
        return standard

    if status != "NOT WORKING":
        return ""

    if candidate.get("unsupported_drm"):
        return "DRM UNSUPPORTED"

    if candidate.get("drm_key_missing", False):
        return "DRM KEY MISSING"

    if str(candidate.get("drm_inspection_failure") or "").strip():
        return "DRM CHECK FAILED"

    hls_variant_status = str(
        candidate.get("hls_variant_probe_status") or ""
    ).strip()
    if hls_variant_status:
        return {
            "hls_variant_unavailable": "HLS VARIANT UNAVAILABLE",
            "hls_variant_access_failed": "HLS VARIANT ACCESS FAILED",
            "hls_variant_invalid": "HLS VARIANT INVALID",
            "hls_variant_check_failed": "HLS VARIANT CHECK FAILED",
        }.get(hls_variant_status, "HLS VARIANT CHECK FAILED")

    return ""


def _nm3u8dl_nonselection_reason(
    candidate: dict,
    selected_candidate: Optional[dict],
    *,
    now_ts: float,
) -> str:
    if not selected_candidate or not candidate.get("launchable", False):
        return ""

    if _nm3u8dl_same_candidate(candidate, selected_candidate):
        return ""

    min_remaining_sec = int(
        NM3U8DL_NEW_SOURCE_MIN_REMAINING_MIN * 60
    )

    def stable_for_join(item: dict) -> bool:
        expiry = item.get("expiry")
        return (
            expiry is None
            or float(expiry) - float(now_ts) >= min_remaining_sec
        )

    if stable_for_join(selected_candidate) and not stable_for_join(candidate):
        return (
            "not selected: less than "
            f"{int(NM3U8DL_NEW_SOURCE_MIN_REMAINING_MIN)} min remaining"
        )

    candidate_preferred = int(
        candidate.get("preferred_qualifier_score") or 0
    )
    selected_preferred = int(
        selected_candidate.get("preferred_qualifier_score") or 0
    )

    if candidate_preferred < selected_preferred:
        return "not selected: less preferred match"

    candidate_video_rank = _nm3u8dl_video_quality_rank(candidate)
    selected_video_rank = _nm3u8dl_video_quality_rank(selected_candidate)

    if candidate_video_rank < selected_video_rank:
        if (
            candidate_video_rank[:-1] == selected_video_rank[:-1]
            and candidate_video_rank[-1] <= 0
            and selected_video_rank[-1] > 0
        ):
            return "not selected: bitrate unknown"
        return "not selected: lower quality"

    candidate_expiry = candidate.get("expiry")
    selected_expiry = selected_candidate.get("expiry")

    if candidate_video_rank == selected_video_rank:
        prefer_unknown_expiry = bool(
            get_nm3u8dl_playlist_profile().get(
                "prefer_unknown_expiry_on_equal_quality",
                False,
            )
        )

        if (
            candidate_expiry is not None
            and selected_expiry is not None
            and float(candidate_expiry) < float(selected_expiry)
        ):
            return "not selected: expires sooner"

        if candidate_expiry is None and selected_expiry is not None:
            if not prefer_unknown_expiry:
                return "not selected: expiry unknown"

        if candidate_expiry is not None and selected_expiry is None:
            if prefer_unknown_expiry:
                return (
                    "not selected: equal quality; "
                    "unknown-expiry source preferred"
                )

        return "not selected: equivalent alternative"

    return "not selected: another candidate ranked higher"


def _nm3u8dl_not_working_reason(candidate: dict) -> str:
    unsupported_drm = str(
        candidate.get("unsupported_drm") or ""
    ).strip()

    if unsupported_drm:
        return f"DRM UNSUPPORTED — {unsupported_drm}"

    if candidate.get("drm_key_missing", False):
        # Operator-facing output should state the actionable condition, not leak
        # protocol-level DRM tags/UUIDs from the inspection layer.  The detailed
        # DRM evidence remains on the candidate for diagnostics.
        return "DRM KEY MISSING"

    drm_inspection_failure = str(
        candidate.get("drm_inspection_failure") or ""
    ).strip()

    if drm_inspection_failure:
        return f"DRM CHECK FAILED — {drm_inspection_failure}"

    hls_variant_failure = str(
        candidate.get("hls_variant_probe_failure") or ""
    ).strip()
    if hls_variant_failure:
        classification = _nm3u8dl_candidate_display_classification(
            candidate,
            status="NOT WORKING",
        )
        return (
            f"{classification} — {hls_variant_failure}"
            if classification
            else hls_variant_failure
        )

    header_reason = str(
        candidate.get("header_preparation_failure") or ""
    ).strip()

    if header_reason:
        return (
            f"{header_reason} — recorder-side handling needs investigation"
        )

    manifest_reason = str(
        candidate.get("manifest_probe_failure") or ""
    ).strip()
    ffprobe_reason = str(
        candidate.get("ffprobe_probe_failure") or ""
    ).strip()

    reasons = []
    for reason in (manifest_reason, ffprobe_reason):
        if reason and reason not in reasons:
            reasons.append(reason)

    combined = " | ".join(reasons)
    lowered = combined.lower()

    http_match = re.search(r"\bHTTP\s+(\d{3})\b", combined, re.IGNORECASE)
    if http_match:
        code = int(http_match.group(1))
        primary = next(
            (reason for reason in reasons if re.search(rf"\bHTTP\s+{code}\b", reason, re.IGNORECASE)),
            f"HTTP {code}",
        )

        if code == 404:
            if " — " in primary:
                return primary
            return (
                f"{primary} — source/path unavailable; "
                "source-side issue likely"
            )

        if code == 410:
            return f"{primary} — source no longer available; source-side issue"

        if code in (401, 403):
            return f"{primary} — access/auth cause unclear; investigate"

        if code == 429:
            return f"{primary} — source rate-limited; retry/investigate if persistent"

        if 500 <= code <= 599:
            return f"{primary} — upstream/source issue"

        return f"{primary} — needs investigation"

    if "dns/host lookup failed" in lowered:
        return "DNS/host lookup failed — network/source issue; investigate"

    if "connection timed out" in lowered:
        return "connection timed out — source/network issue; investigate"

    if "connection refused" in lowered:
        return "connection refused — source/network issue; investigate"

    if "network unreachable" in lowered:
        return "network unreachable — network/route issue; investigate"

    if "tls/certificate failure" in lowered:
        return "TLS/certificate failure — network/source configuration; investigate"

    if reasons:
        if len(reasons) == 1:
            return f"{reasons[0]} — needs investigation"
        return (
            f"manifest: {manifest_reason or 'failed'}; "
            f"ffprobe: {ffprobe_reason or 'failed'} — needs investigation"
        )

    return "manifest and ffprobe probes failed — needs investigation"


def _format_nm3u8dl_candidate_operational_facts(candidate: dict) -> str:
    """Render the standard source facts shown to the recorder operator."""
    identity = _nm3u8dl_candidate_identity(candidate)
    quality_text = format_nm3u8dl_candidate_quality(candidate)
    expiry_text = format_nm3u8dl_expiry_with_source(
        candidate.get("expiry"),
        candidate.get("expiry_source") or "",
    )

    if _nm3u8dl_has_quality_evidence(candidate):
        return f"{identity} — {quality_text}; expires {expiry_text}"

    # No quality inspection evidence exists for this candidate. Do not invent
    # an all-UNKNOWN quality block; show the other operational facts normally.
    return f"{identity} — expires {expiry_text}"


def _format_nm3u8dl_candidate_display_details(
    candidate: dict,
    *,
    status: str,
    selected_candidate: Optional[dict],
    now_ts: float,
    include_classification: bool = True,
) -> str:
    identity = _nm3u8dl_candidate_identity(candidate)

    if status == "IGNORED":
        reason = str(candidate.get("reason") or "").strip()
        return f"{identity} — {reason or 'entry could not be evaluated — needs investigation'}"

    if status == "EXPIRY UNKNOWN":
        return (
            f"{identity} — no recognizable authorization expiry — "
            "cannot use with current profile"
        )

    if status == "EXPIRED":
        return (
            f"{identity} — expired "
            f"{format_nm3u8dl_expiry_with_source(candidate.get('expiry'), candidate.get('expiry_source') or '')}"
        )

    if status == "BLOCKED":
        kind = str(candidate.get("access_block_kind") or "").strip()
        country = str(candidate.get("geo_country") or "").strip()

        if kind == "confirmed_geo":
            country_text = f" (detected country: {country})" if country else ""
            return f"{identity} — confirmed geo-block{country_text} — VPN/route may fix"

        if kind == "vpn_route_suspected":
            status_code = candidate.get("access_block_http_status")
            http_text = f"HTTP {status_code} Forbidden" if status_code else "access blocked"
            return f"{identity} — {http_text} — VPN/route suspected"

        return f"{identity} — source access restriction detected — investigate route/access"

    if status == "EXCLUDED":
        reason = str(
            candidate.get("failover_exclusion_reason")
            or "2 consecutive downloader failures"
        ).strip()
        return (
            f"{_format_nm3u8dl_candidate_operational_facts(candidate)} — "
            f"{reason}"
        )

    if status == "NOT WORKING":
        classification = _nm3u8dl_candidate_display_classification(
            candidate,
            status=status,
        )

        # DRM key absence is already the complete operator-facing explanation.
        # Keep the same source/quality/expiry facts as normal candidate rows; do
        # not append encryption method names, UUIDs, or other inspection detail.
        if candidate.get("drm_key_missing", False):
            facts = _format_nm3u8dl_candidate_operational_facts(candidate)
            if include_classification:
                return f"{classification} — {facts}"
            return facts

        reason = _nm3u8dl_not_working_reason(candidate)

        if (
            not include_classification
            and classification
            and reason.startswith(f"{classification} — ")
        ):
            reason = reason[len(classification) + 3:]

        if candidate.get("unsupported_drm"):
            facts = _format_nm3u8dl_candidate_operational_facts(candidate)
            if include_classification:
                return f"{reason} — {facts}"
            return f"{facts} — {reason}"

        return f"{identity} — {reason}"

    text = _format_nm3u8dl_candidate_operational_facts(candidate)

    if status == "WORKING":
        reason = _nm3u8dl_nonselection_reason(
            candidate,
            selected_candidate,
            now_ts=now_ts,
        )
        if reason:
            text += f" — {reason}"

    return text


def get_nm3u8dl_access_block_playlist_urls(
    source_results: List[dict]
) -> List[str]:
    """Return playlist sources that produced at least one access-blocked candidate."""
    playlist_urls = []

    for result in source_results or []:
        blocked_count = int(result.get("access_blocked_count") or 0)

        if blocked_count <= 0:
            continue

        playlist_url = str(result.get("playlist_url") or "").strip()

        if playlist_url and playlist_url not in playlist_urls:
            playlist_urls.append(playlist_url)

    return playlist_urls


def get_nm3u8dl_inconclusive_access_recheck_urls(
    previous_urls: List[str],
    source_results: List[dict],
) -> List[str]:
    """Keep previously blocked sources whose current access state is inconclusive."""
    results_by_url = {
        str(result.get("playlist_url") or "").strip(): result
        for result in source_results or []
        if str(result.get("playlist_url") or "").strip()
    }

    inconclusive_urls = []

    for playlist_url in previous_urls or []:
        playlist_url = str(playlist_url).strip()

        if not playlist_url:
            continue

        result = results_by_url.get(playlist_url)

        # Missing result, fetch/probe failure, or another unusable state does not
        # prove that a previously blocked source recovered. Only a conclusive
        # matched/no-match/expired/access-blocked result resolves this question.
        if result is None:
            inconclusive_urls.append(playlist_url)
            continue

        status = str(result.get("status") or "").strip().lower()

        if status not in (
            "matched",
            "no_match",
            "expired",
            "access_blocked",
        ):
            inconclusive_urls.append(playlist_url)

    return inconclusive_urls



def get_nm3u8dl_access_recheck_source_results(
    tracked_urls: List[str],
    source_results: List[dict],
) -> List[dict]:
    """Return exactly the tracked incident sources, in stable incident order."""
    results_by_url = {
        str(result.get("playlist_url") or "").strip(): result
        for result in source_results or []
        if str(result.get("playlist_url") or "").strip()
    }

    ordered_results = []

    for playlist_url in tracked_urls or []:
        playlist_url = str(playlist_url).strip()

        if not playlist_url:
            continue

        result = results_by_url.get(playlist_url)

        if result is None:
            # Missing is explicitly inconclusive. Keep a visible row so an
            # incident with nine sources always reports all nine when the table
            # is emitted, rather than silently dropping an unverified source.
            result = {
                "playlist_url": playlist_url,
                "status": "error",
                "detail": "no result returned by access/VPN verification pass",
                "expiry": None,
                "access_blocked_count": 0,
            }

        ordered_results.append(result)

    return ordered_results


def get_nm3u8dl_access_recheck_snapshot(
    tracked_urls: List[str],
    source_results: List[dict],
) -> dict:
    """Build the access-relevant signature used to detect a changed recheck."""
    ordered_results = get_nm3u8dl_access_recheck_source_results(
        tracked_urls,
        source_results,
    )

    return {
        str(result.get("playlist_url") or "").strip(): (
            str(result.get("status") or "").strip().lower(),
            int(result.get("access_blocked_count") or 0),
            str(result.get("detail") or "").strip(),
            str(result.get("quality_summary") or "").strip(),
        )
        for result in ordered_results
    }


def _nm3u8dl_access_route_observation(snapshot_value):
    """Return only the conclusive access/route part of one stored source state."""
    if not isinstance(snapshot_value, (list, tuple)) or len(snapshot_value) < 2:
        return None

    status = str(snapshot_value[0] or "").strip().lower()

    try:
        blocked_count = int(snapshot_value[1] or 0)
    except (TypeError, ValueError):
        blocked_count = 0

    # Only matched/access-blocked results answer the same access question
    # conclusively. NO MATCH / expired / fetch-error / unusable can change for
    # source-content reasons and must not masquerade as a VPN/route transition.
    if status == "access_blocked" or blocked_count > 0:
        return "blocked"

    if status == "matched":
        return "reachable"

    return None


def _nm3u8dl_access_route_state_changed(
    previous_snapshot: dict,
    current_snapshot: dict,
) -> bool:
    """Detect a material, conclusive accessibility change on the fixed incident set."""
    previous_snapshot = dict(previous_snapshot or {})
    current_snapshot = dict(current_snapshot or {})

    for playlist_url, previous_value in previous_snapshot.items():
        if playlist_url not in current_snapshot:
            continue

        previous_observation = _nm3u8dl_access_route_observation(
            previous_value
        )
        current_observation = _nm3u8dl_access_route_observation(
            current_snapshot.get(playlist_url)
        )

        if (
            previous_observation is not None
            and current_observation is not None
            and current_observation != previous_observation
        ):
            return True

    return False


def _nm3u8dl_reset_failover_for_access_environment_change(
    state: RecorderState,
) -> bool:
    """Invalidate route-dependent failover judgments after confirmed access change."""
    bad_fingerprints = getattr(
        state,
        "nm3u8dl_bad_stream_fingerprints",
        {},
    ) or {}
    probations = getattr(
        state,
        "nm3u8dl_failover_probations",
        {},
    ) or {}

    excluded_count = len(bad_fingerprints)
    probation_count = len(probations)

    if excluded_count <= 0 and probation_count <= 0:
        return False

    bad_fingerprints.clear()
    probations.clear()
    state.nm3u8dl_failover_retry_source = None
    state.nm3u8dl_failover_waiting_for_alternative = False
    state.nm3u8dl_failover_alarm_silenced = False

    if getattr(state, "shared_alarm_type", None) == "playlist_failover":
        shared_alarm_stop(state)
        state.alarm_ack_requested = False
        log(
            "ALARM_PLAYLIST_FAILOVER_STOP "
            "reason=access_environment_changed"
        )

    log(
        "Access/VPN environment change confirmed → stream-failover state reset; "
        f"{excluded_count} excluded fingerprint(s) and "
        f"{probation_count} one-failure probation(s) cleared. "
        "Previously excluded streams are eligible for fresh evaluation.",
        level="WARN",
    )
    return True


def _nm3u8dl_confirm_access_change_and_reset_failover(
    state: RecorderState,
    *,
    tracked_urls: List[str],
    previous_snapshot: dict,
    first_pass_source_results: List[dict],
    stop_event: Optional[threading.Event] = None,
) -> bool:
    """Confirm a changed access environment with a fixed-set second pass."""
    tracked_urls = [
        str(playlist_url).strip()
        for playlist_url in tracked_urls or []
        if str(playlist_url).strip()
    ]
    previous_snapshot = dict(previous_snapshot or {})

    if not (
        tracked_urls
        and previous_snapshot
        and (
            getattr(state, "nm3u8dl_bad_stream_fingerprints", {})
            or getattr(state, "nm3u8dl_failover_probations", {})
        )
    ):
        return False

    first_pass_snapshot = get_nm3u8dl_access_recheck_snapshot(
        tracked_urls,
        first_pass_source_results,
    )

    if not _nm3u8dl_access_route_state_changed(
        previous_snapshot,
        first_pass_snapshot,
    ):
        return False

    # A full playlist scan can straddle a VPN/route switch. Recheck the exact
    # original incident set once more before forgiving downloader failures.
    try:
        verification = resolve_nm3u8dl_playlist_source(
            include_candidate_pool=True,
            playlist_urls_override=tracked_urls,
            progress_stage="verifying changed blocked playlist",
            progress_completion_label=(
                "blocked-source verification pass complete"
            ),
            state=state,
            stop_event=stop_event,
            show_progress=False,
        )
        verified_source_results = verification.get(
            "source_results",
            [],
        )
    except RuntimeError as verification_error:
        if (
            state.stop_flag
            or (stop_event is not None and stop_event.is_set())
        ):
            return False
        verified_source_results = getattr(
            verification_error,
            "source_results",
            [],
        )

    verified_snapshot = get_nm3u8dl_access_recheck_snapshot(
        tracked_urls,
        verified_source_results,
    )

    if not _nm3u8dl_access_route_state_changed(
        previous_snapshot,
        verified_snapshot,
    ):
        return False

    # Preserve the authoritative pass-2 state for the mature VPN incident
    # machinery. The original fixed URL set is intentionally not shrunk here.
    state.nm3u8dl_access_block_status_snapshot = verified_snapshot

    return _nm3u8dl_reset_failover_for_access_environment_change(state)


def process_nm3u8dl_targeted_access_verification(
    state: RecorderState,
    *,
    tracked_urls: List[str],
    source_results: List[dict],
    source_errors: List[str],
    candidate_count: int,
    playlist_group: str,
    match_description: str,
    access_block_alarm_actionable: bool,
    access_block_alarm_quality: Optional[str],
    running_source: dict,
    access_check_interval_sec: int,
    selected_candidate: Optional[dict] = None,
):
    """Apply authoritative pass-2 results without shrinking the incident set."""
    tracked_urls = [
        str(playlist_url).strip()
        for playlist_url in tracked_urls or []
        if str(playlist_url).strip()
    ]

    ordered_results = get_nm3u8dl_access_recheck_source_results(
        tracked_urls,
        source_results,
    )
    current_snapshot = get_nm3u8dl_access_recheck_snapshot(
        tracked_urls,
        ordered_results,
    )
    previous_snapshot = dict(
        getattr(state, "nm3u8dl_access_block_status_snapshot", {}) or {}
    )
    snapshot_changed = (current_snapshot != previous_snapshot)
    access_route_changed = _nm3u8dl_access_route_state_changed(
        previous_snapshot,
        current_snapshot,
    )

    # The full incident table is event-driven: one changed source is enough to
    # show the status of every originally tracked source. Unchanged 30-second
    # checks stay concise.
    if snapshot_changed:
        tracked_url_set = set(tracked_urls)
        filtered_errors = [
            source_error
            for source_error in source_errors or []
            if any(
                source_error.startswith(f"{playlist_url}:")
                for playlist_url in tracked_url_set
            )
        ]

        log_nm3u8dl_playlist_scan_results(
            source_results=ordered_results,
            source_errors=filtered_errors,
            playlist_group=playlist_group,
            match_description=match_description,
            playlist_source_count=len(tracked_urls),
            candidate_count=int(candidate_count or 0),
            selected_candidate=selected_candidate,
            heading="=== ACCESS/VPN RECHECK RESULTS ===",
            source_count_label="Previously blocked sources rechecked",
            show_access_blocked_source_count=True,
            show_no_match=True,
        )

    state.nm3u8dl_access_block_status_snapshot = current_snapshot

    # targeted_access_scan already performs the protective second pass whenever
    # the first pass changes. Reaching this point with a material access-state
    # change therefore means the route/access environment really changed.
    if access_route_changed:
        _nm3u8dl_reset_failover_for_access_environment_change(state)

    confirmed_blocked_urls = set(
        get_nm3u8dl_access_block_playlist_urls(ordered_results)
    )
    inconclusive_access_urls = set(
        get_nm3u8dl_inconclusive_access_recheck_urls(
            tracked_urls,
            ordered_results,
        )
    )

    if not confirmed_blocked_urls and not inconclusive_access_urls:
        update_nm3u8dl_access_block_alarm_state(
            state,
            actionable=False,
            blocked_quality=None,
            running_quality=format_nm3u8dl_candidate_quality(
                running_source
            ),
        )
        return snapshot_changed

    # Do not shrink 9 -> 2 (or similar) during one access incident. Every
    # verification cycle keeps answering the same question about the same set.
    state.nm3u8dl_access_block_playlist_urls = list(tracked_urls)

    if access_block_alarm_actionable:
        update_nm3u8dl_access_block_alarm_state(
            state,
            actionable=True,
            blocked_quality=access_block_alarm_quality,
            running_quality=format_nm3u8dl_candidate_quality(
                running_source
            ),
        )
        return snapshot_changed

    verified_no_longer_blocked = max(
        0,
        len(tracked_urls)
        - len(confirmed_blocked_urls)
        - len(inconclusive_access_urls),
    )
    ack_text = (
        " (alarm acknowledged)"
        if state.nm3u8dl_access_block_alarm_acknowledged
        else ""
    )

    log(
        f"Access/VPN status → PARTIAL RECOVERY{ack_text} — "
        f"{verified_no_longer_blocked}/{len(tracked_urls)} tracked sources "
        f"verified no longer blocked; "
        f"{len(confirmed_blocked_urls)} still access-blocked; "
        f"{len(inconclusive_access_urls)} inconclusive; "
        f"recording continues; next recheck in "
        f"{access_check_interval_sec} seconds.",
        level="WARN",
    )
    return snapshot_changed


def retire_nm3u8dl_quality_access_incident_if_target_healthy(
    state: RecorderState,
) -> bool:
    """Retire an old quality-recovery VPN incident after target run health is proven."""
    if (
        getattr(state, "nm3u8dl_access_block_purpose", None)
        != "quality"
        or int(getattr(state, "nm3u8dl_access_block_consecutive", 0) or 0)
        <= 0
    ):
        return False

    running_source = getattr(state, "nm3u8dl_running_source", None)

    if not running_source:
        return False

    profile = get_nm3u8dl_playlist_profile()

    if not bool(profile.get("quality_upgrade_enabled", False)):
        return False

    target_fps = float(profile.get("quality_upgrade_target_fps", 50))
    running_motion_fps = _nm3u8dl_ranking_motion_fps(running_source)

    if running_motion_fps < target_fps:
        return False

    if getattr(state, "shared_alarm_type", None) == "playlist_access_block":
        shared_alarm_stop(state)
        state.alarm_ack_requested = False
        log(
            "ALARM_PLAYLIST_ACCESS_BLOCK_STOP "
            "reason=healthy_target_quality_reached"
        )

    log(
        "Access/VPN quality recovery complete → healthy target "
        f"{target_fps:g} motion source established."
    )
    log(
        "30-second blocked-source rechecks stopped; remaining playlist-source "
        "accessibility will be reevaluated during normal authorization renewal."
    )

    state.nm3u8dl_access_block_consecutive = 0
    state.nm3u8dl_access_block_alarm_acknowledged = False
    state.nm3u8dl_access_block_detected_ts = None
    state.nm3u8dl_access_block_playlist_urls = []
    state.nm3u8dl_access_block_purpose = None
    state.nm3u8dl_access_block_status_snapshot = {}
    return True


def _nm3u8dl_access_block_quality_is_complete(candidate: dict) -> bool:
    return (
        float(candidate.get("video_fps") or 0.0) > 0
        and int(candidate.get("video_width") or 0) > 0
        and int(candidate.get("video_height") or 0) > 0
    )


def _nm3u8dl_access_block_alarm_quality_rank(candidate: dict):
    if _nm3u8dl_access_block_quality_is_complete(candidate):
        return _nm3u8dl_video_quality_rank(candidate)

    # A blocked manifest may reveal no complete quality at all. For alarm
    # comparison only, conservatively assume 1920x1080 @ 50 fps so we do not
    # silently keep a lower-quality recording when fixing the VPN/route might
    # expose a better source. This does not alter normal source selection.
    return _nm3u8dl_video_quality_rank({
        "quality_known": True,
        "video_fps": 50.0,
        "video_width": 1920,
        "video_height": 1080,
        "video_scan_type": "",
        "video_bitrate_bps": 0,
    })


def get_nm3u8dl_access_block_alarm_metadata(
    usable_candidates: List[dict],
    launchable_candidates: List[dict],
) -> dict:
    access_blocked_candidates = [
        candidate
        for candidate in usable_candidates
        if candidate.get("access_blocked", False)
    ]

    if not access_blocked_candidates:
        return {
            "actionable": False,
            "blocked_quality": None,
        }

    if launchable_candidates:
        best_launchable_rank = max(
            (
                int(candidate.get("preferred_qualifier_score") or 0),
                *_nm3u8dl_video_quality_rank(candidate),
            )
            for candidate in launchable_candidates
        )

        actionable_candidates = [
            candidate
            for candidate in access_blocked_candidates
            if (
                (
                    int(candidate.get("preferred_qualifier_score") or 0),
                    *_nm3u8dl_access_block_alarm_quality_rank(candidate),
                )
                > best_launchable_rank
            )
        ]
    else:
        # If nothing can launch, any recognized access block is actionable
        # because fixing the route may restore recording entirely.
        actionable_candidates = access_blocked_candidates

    if not actionable_candidates:
        return {
            "actionable": False,
            "blocked_quality": None,
        }

    best_blocked = max(
        actionable_candidates,
        key=lambda candidate: (
            int(candidate.get("preferred_qualifier_score") or 0),
            *_nm3u8dl_access_block_alarm_quality_rank(candidate),
        ),
    )

    blocked_quality = (
        format_nm3u8dl_candidate_quality(best_blocked)
        if _nm3u8dl_access_block_quality_is_complete(best_blocked)
        else "assumed 1920x1080 | 50 fps"
    )

    return {
        "actionable": True,
        "blocked_quality": blocked_quality,
    }


def update_nm3u8dl_access_block_alarm_state(
    state: RecorderState,
    *,
    actionable: bool,
    blocked_quality: Optional[str],
    running_quality: Optional[str],
):
    previous_count = int(
        getattr(state, "nm3u8dl_access_block_consecutive", 0) or 0
    )

    if actionable:
        state.nm3u8dl_access_block_consecutive = previous_count + 1

        current_text = (
            f"running {running_quality}"
            if running_quality
            else "no working source"
        )

        if (
            state.nm3u8dl_access_block_alarm_acknowledged
            and previous_count >= 2
        ):
            log(
                "Access/VPN status → STILL BLOCKED (alarm acknowledged) — "
                f"blocked candidate {blocked_quality or 'quality unknown'}; "
                f"{current_text}; recording continues; next recheck in "
                f"{int(NM3U8DL_PLAYLIST_ACCESS_CHECK_INTERVAL_SEC)} seconds.",
                level="WARN",
            )
        else:
            log(
                f"Access/VPN actionable scan: "
                f"{min(state.nm3u8dl_access_block_consecutive, 2)}/2 — "
                f"blocked candidate {blocked_quality or 'quality unknown'}; "
                f"{current_text}",
                level="WARN",
            )

        if previous_count == 0:
            state.nm3u8dl_access_block_detected_ts = time.time()
            log(
                "Potentially better matching source is access-blocked → "
                "VPN/route attention may be needed; recording will use the "
                "best reachable source and recheck in 30 seconds.",
                level="WARN",
            )
            beep_bad(state)

        if (
            state.nm3u8dl_access_block_consecutive >= 2
            and not state.nm3u8dl_access_block_alarm_acknowledged
            and not shared_alarm_is_active(state)
        ):
            log(
                "Source access remains blocked after confirmation and is "
                "preventing recording or a potentially better-quality source "
                "→ VPN/route attention needed.",
                level="WARN",
            )
            shared_alarm_start(
                state,
                alarm_type="playlist_access_block",
                incident=None,
            )

        return

    if previous_count > 0:
        if (
            getattr(state, "shared_alarm_type", None)
            == "playlist_access_block"
        ):
            shared_alarm_stop(state)
            state.alarm_ack_requested = False
            log(
                "ALARM_PLAYLIST_ACCESS_BLOCK_STOP "
                "reason=source_access_recovered"
            )

        log(
            "Access/VPN block cleared → source access recovered.",
            level="INFO",
        )

    state.nm3u8dl_access_block_consecutive = 0
    state.nm3u8dl_access_block_alarm_acknowledged = False
    state.nm3u8dl_access_block_detected_ts = None
    state.nm3u8dl_access_block_playlist_urls = []
    state.nm3u8dl_access_block_purpose = None
    state.nm3u8dl_access_block_status_snapshot = {}
    
class NM3U8DLPlaylistResolutionError(RuntimeError):
    def __init__(
        self,
        message: str,
        source_results: List[dict],
        source_errors: List[str],
        overall_warning: Optional[str] = None,
        access_block_alarm_actionable: bool = False,
        access_block_alarm_quality: Optional[str] = None,
        history_scan: Optional[dict] = None,
    ):
        super().__init__(message)
        self.source_results = source_results
        self.source_errors = source_errors
        self.overall_warning = overall_warning
        self.access_block_alarm_actionable = access_block_alarm_actionable
        self.access_block_alarm_quality = access_block_alarm_quality
        self.history_scan = history_scan


def _format_nm3u8dl_playlist_summary_counts(
    result: dict,
    *,
    include_matching_entries: bool = True,
) -> str:
    """Render the compact per-source candidate funnel used by MATCHED tables."""
    counts = result.get("summary_counts")

    if not isinstance(counts, dict):
        return str(result.get("detail") or "unknown result")

    parts = []

    if include_matching_entries:
        matching_entries = int(counts.get("matching_entries") or 0)
        entry_word = "entry" if matching_entries == 1 else "entries"
        parts.append(f"{matching_entries} matching {entry_word}")

    # Show only categories that actually have a count.
    for count_key, label in (
        ("auth_expiry_ok", "auth/expiry OK"),
        ("working", "working"),
        ("excluded", "excluded"),
        ("blocked", "blocked"),
        ("not_working", "not working"),
        ("expired", "expired"),
        ("expiry_unknown", "expiry unknown"),
        ("ignored", "ignored"),
    ):
        count = int(counts.get(count_key) or 0)
        if count > 0:
            parts.append(f"{count} {label}")

    return "; ".join(parts)


def _termcap_write_nm3u8dl_candidate_url_details(candidate_details: List[dict]):
    """Write candidate URL identities to saved terminal capture only."""
    if not candidate_details:
        return

    try:
        def write_detail(message: str):
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            termcap_write(f"{ts} [INFO] {message}")

        separator = "---------------- CANDIDATE URL DETAILS — LOG ONLY ----------------"
        write_detail(separator)

        displayed_playlist = None

        for detail in candidate_details:
            playlist_url = str(detail.get("playlist_url") or "").strip()
            if playlist_url != displayed_playlist:
                write_detail(f"Playlist            : {playlist_url or 'unknown'}")
                displayed_playlist = playlist_url

            role = "SELECTED" if detail.get("selected", False) else "NOT SELECTED"
            write_detail(f"{detail.get('label') or 'Candidate'} [{role}]")

            event_name = str(detail.get("event_name") or "").strip()
            if event_name:
                write_detail(f"  Event name        : {event_name}")

            stream_url = str(detail.get("stream_url") or "").strip()
            write_detail(f"  Stream URL        : {stream_url or 'unknown'}")

            final_url = str(detail.get("manifest_final_url") or "").strip()
            if final_url and final_url != stream_url:
                write_detail(f"  Final manifest URL: {final_url}")

            variant_url = str(detail.get("manifest_variant_url") or "").strip()
            if (
                variant_url
                and variant_url != stream_url
                and variant_url != final_url
            ):
                write_detail(f"  Media manifest URL: {variant_url}")

        write_detail("---------------- END CANDIDATE URL DETAILS ----------------")
    except Exception:
        # This is evidence-only logging. It must never affect recorder behavior.
        pass


def log_nm3u8dl_playlist_scan_results(
    *,
    source_results: List[dict],
    source_errors: List[str],
    playlist_group: str,
    match_description: str,
    playlist_source_count: int,
    candidate_count: int,
    selected_playlist_url: Optional[str] = None,
    selected_candidate: Optional[dict] = None,
    selected_tag_label: str = "SELECTED",
    heading: str = "=== DYNAMIC SOURCE STATUS ===",
    section_level: str = "INFO",
    source_count_label: str = "Playlist sources checked",
    show_access_blocked_source_count: bool = True,
    show_no_match: bool = False,
    emit_log: Optional[Callable] = None,
):
    emit = emit_log or log
    capture_candidate_urls_to_termcap = emit_log is None
    candidate_url_details = []
    emit("")
    emit(heading, level=section_level)
    emit("Source mode             : playlist", level=section_level)
    emit(
        f"Playlist group          : {playlist_group}",
        level=section_level,
    )
    emit(
        f"Match rule              : {match_description}",
        level=section_level,
    )
    emit(
        f"{source_count_label:<24}: {playlist_source_count}",
        level=section_level,
    )

    if show_access_blocked_source_count:
        access_blocked_source_count = len(
            get_nm3u8dl_access_block_playlist_urls(source_results)
        )
        emit(
            f"{'Access-blocked sources':<24}: "
            f"{access_blocked_source_count}",
            level=section_level,
        )

    emit(
        f"{'Working candidates':<24}: {candidate_count}",
        level=section_level,
    )

    excluded_rows = [
        candidate_row
        for result in source_results or []
        for candidate_row in (result.get("candidate_rows") or [])
        if candidate_row.get("failover_excluded", False)
    ]
    if excluded_rows:
        excluded_fingerprints = {
            str(candidate_row.get("stream_fingerprint") or "").strip()
            for candidate_row in excluded_rows
            if str(candidate_row.get("stream_fingerprint") or "").strip()
        }
        excluded_unique = len(excluded_fingerprints)
        excluded_entries = len(excluded_rows)
        if excluded_unique == 1 and excluded_entries == 1:
            excluded_text = "1"
        else:
            excluded_text = (
                f"{excluded_unique} unique — "
                f"{excluded_entries} matching entries"
            )
        emit(
            f"{'Excluded candidates':<24}: {excluded_text}",
            level=section_level,
        )

    summarized_source_errors = set()
    emitted_source_warnings = set()

    # One adaptive colon column for the whole scan. Event-mode groups use a
    # compact stable label (Event N [ON/OFF]); fixed-channel groups continue to
    # size from their actual channel names.
    event_candidate_display = (
        get_nm3u8dl_playlist_match_mode() == "EVENT_PHRASE"
    )
    candidate_label_lengths = []

    for result in source_results:
        width_candidate_rows = list(result.get("candidate_rows") or [])
        index_width = len(str(max(1, len(width_candidate_rows))))

        if event_candidate_display and width_candidate_rows:
            candidate_label_lengths.append(
                len("Event ")
                + index_width
                + 1
                + len("[OFF]")
            )
            continue

        for candidate_row in width_candidate_rows:
            candidate_name = str(
                candidate_row.get("entry_title") or ""
            ).strip()
            if not candidate_name:
                candidate_name = str(
                    candidate_row.get("tvg_name") or ""
                ).strip()
            if not candidate_name:
                candidate_name = "Unknown channel"

            candidate_label_lengths.append(
                index_width
                + 2
                + len(candidate_name)
                + 1
                + len("[OFF]")
            )

    table_label_width = min(
        30,
        max(
            18,
            max(candidate_label_lengths, default=len("PLAYLIST")) + 2,
        ),
    )

    displayed_source_group = None

    for result in source_results:
        status = result["status"]

        if status == "no_match" and not show_no_match:
            playlist_url = result["playlist_url"]
            detail = result.get("detail", "unknown result")
            summarized_source_errors.add(
                f"{playlist_url}: {detail}"
            )
            continue

        playlist_url = result["playlist_url"]
        source_group = get_nm3u8dl_playlist_source_group(playlist_url)

        if source_group and source_group != displayed_source_group:
            emit(
                f"-------------------- {source_group} --------------------",
                level=section_level,
            )
            displayed_source_group = source_group

        detail = result.get("detail", "unknown result")
        selected_tag_text = str(
            selected_tag_label or "SELECTED"
        ).strip().upper()

        selected_tag = (
            f" [{selected_tag_text}]"
            if (
                selected_playlist_url == playlist_url
                and selected_tag_text != "SELECTED"
            )
            else ""
        )

        matching_entries = int(
            (result.get("summary_counts") or {}).get("matching_entries") or 0
        )

        summary_text = _format_nm3u8dl_playlist_summary_counts(
            result,
            include_matching_entries=status not in (
                "matched",
                "expired",
                "unusable",
            ),
        )

        if status == "matched":
            playlist_text = (
                f"{matching_entries} MATCHED{selected_tag} on {playlist_url}"
            )
            if summary_text:
                playlist_text += f" — {summary_text}"
            result_level = "INFO"

        elif status == "expired":
            playlist_text = f"{matching_entries} MATCHED on {playlist_url}"
            if summary_text:
                playlist_text += f" — {summary_text}"
            result_level = "INFO"

        elif status == "no_match":
            playlist_text = f"NO MATCH on {playlist_url}"
            result_level = "INFO"

        elif status == "access_blocked":
            playlist_text = f"ACCESS BLOCKED on {playlist_url}"
            if summary_text:
                playlist_text += f" — {summary_text}"
            result_level = "WARN"

        elif status == "unusable":
            playlist_text = (
                f"{matching_entries} MATCHED on {playlist_url}"
            )
            if summary_text:
                playlist_text += f" — {summary_text}"
            result_level = "INFO"

        elif status == "fetch_error":
            playlist_text = f"FETCH ERROR on {playlist_url} — {detail}"
            result_level = "WARN"

        else:
            playlist_text = f"ERROR on {playlist_url} — {detail}"
            result_level = "WARN"

        emit(
            f"{'PLAYLIST':<{table_label_width}}: {playlist_text}",
            level=result_level,
        )

        candidate_rows = list(result.get("candidate_rows") or [])
        if candidate_rows:
            candidate_now = time.time()
            candidate_statuses = [
                _nm3u8dl_candidate_status(
                    candidate_row,
                    selected_candidate=selected_candidate,
                    now_ts=candidate_now,
                )
                for candidate_row in candidate_rows
            ]

            working_candidate_rows = [
                candidate_row
                for candidate_row, candidate_status in zip(
                    candidate_rows,
                    candidate_statuses,
                )
                if candidate_status in ("SELECTED", "WORKING")
            ]
            best_candidate_row = (
                get_nm3u8dl_join_candidate(
                    working_candidate_rows,
                    now_ts=candidate_now,
                )
                if len(candidate_rows) > 1 and working_candidate_rows
                else None
            )

            display_candidate_rows = list(
                zip(candidate_rows, candidate_statuses)
            )
            display_candidate_rows.sort(
                key=lambda item: (
                    0
                    if _nm3u8dl_same_candidate(
                        item[0],
                        best_candidate_row,
                    )
                    else 1
                    if item[1] in ("SELECTED", "WORKING")
                    else 2
                )
            )

            for candidate_index, (candidate_row, candidate_status) in enumerate(
                display_candidate_rows,
                start=1,
            ):
                candidate_name = str(
                    candidate_row.get("entry_title") or ""
                ).strip()
                if not candidate_name:
                    candidate_name = str(
                        candidate_row.get("tvg_name") or ""
                    ).strip()
                if not candidate_name:
                    candidate_name = "Unknown channel"

                availability = (
                    "ON"
                    if candidate_status in ("SELECTED", "WORKING")
                    else "OFF"
                )

                if event_candidate_display:
                    label = f"Event {candidate_index} [{availability}]"
                else:
                    label = (
                        f"{candidate_index}. {candidate_name} "
                        f"[{availability}]"
                    )

                # Event-mode rows keep a compact stable left label. Fixed-channel
                # rows retain the channel name beside ON/OFF as before.
                label = f"{label:<{table_label_width}}"

                display_classification = (
                    _nm3u8dl_candidate_display_classification(
                        candidate_row,
                        status=candidate_status,
                    )
                )
                candidate_detail = _format_nm3u8dl_candidate_display_details(
                    candidate_row,
                    status=candidate_status,
                    selected_candidate=selected_candidate,
                    now_ts=candidate_now,
                    include_classification=not bool(display_classification),
                )

                row_markers = []
                if display_classification:
                    row_markers.append(display_classification)
                if _nm3u8dl_same_candidate(candidate_row, best_candidate_row):
                    row_markers.append("BEST")

                if event_candidate_display:
                    candidate_detail = " — ".join(
                        [*row_markers, candidate_name, candidate_detail]
                    )
                elif row_markers:
                    candidate_detail = " — ".join(
                        [*row_markers, candidate_detail]
                    )

                candidate_level = (
                    "INFO"
                    if candidate_status in ("SELECTED", "WORKING")
                    else "WARN"
                )
                emit(
                    f"{label}: {candidate_detail}",
                    level=candidate_level,
                )

                if capture_candidate_urls_to_termcap:
                    candidate_url_details.append({
                        "playlist_url": playlist_url,
                        "label": (
                            f"Event {candidate_index}"
                            if event_candidate_display
                            else f"{candidate_index}. {candidate_name}"
                        ),
                        "selected": bool(
                            selected_candidate
                            and _nm3u8dl_same_candidate(
                                candidate_row,
                                selected_candidate,
                            )
                        ),
                        "event_name": (
                            candidate_name
                            if event_candidate_display
                            else ""
                        ),
                        "stream_url": candidate_row.get("stream_url") or "",
                        "manifest_final_url": (
                            candidate_row.get("manifest_final_url") or ""
                        ),
                        "manifest_variant_url": (
                            candidate_row.get("manifest_variant_url") or ""
                        ),
                    })

        if status in (
            "no_match",
            "access_blocked",
            "unusable",
            "error",
            "fetch_error",
        ):
            summarized_source_errors.add(
                f"{playlist_url}: {detail}"
            )

        for source_error in source_errors:
            if source_error in summarized_source_errors:
                continue

            warning_prefix = f"{playlist_url}:"

            if not source_error.startswith(warning_prefix):
                continue

            warning_text = source_error[
                len(warning_prefix):
            ].lstrip()

            emit(
                f"{'PLAYLIST WARNING':<{table_label_width}}: {warning_text}",
                level="WARN",
            )

            emitted_source_warnings.add(source_error)

    for source_error in source_errors:
        if (
            source_error in summarized_source_errors
            or source_error in emitted_source_warnings
        ):
            continue

        emit(
            f"{'PLAYLIST WARNING':<{table_label_width}}: {source_error}",
            level="WARN",
        )

    if capture_candidate_urls_to_termcap and candidate_url_details:
        _termcap_write_nm3u8dl_candidate_url_details(candidate_url_details)



# ------------------------------------------------------------------------------
# Evidence-only playlist scan history
# ------------------------------------------------------------------------------

def _playlist_history_applicable() -> bool:
    return bool(
        PLAYLIST_HISTORY_ENABLED
        and DOWNLOAD_MODE == ENGINE_NM3U8DL
        and NM3U8DL_SOURCE_MODE == "playlist"
    )


def _playlist_history_safe_machine_name() -> str:
    try:
        raw = str(socket.gethostname() or "").strip()
    except Exception:
        raw = ""

    if not raw:
        raw = "UNKNOWN_MACHINE"

    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", raw).strip("._-")
    return safe or "UNKNOWN_MACHINE"


def _playlist_history_timestamp_text(ts: Optional[float]) -> str:
    if ts is None:
        return "unknown"
    return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")


def _playlist_history_match_context() -> dict:
    primary, required, rejected, preferred = get_nm3u8dl_playlist_match_rules()
    return {
        "playlist_group": NM3U8DL_PLAYLIST_GROUP.strip().upper(),
        "match_mode": get_nm3u8dl_playlist_match_mode(),
        "match_description": get_nm3u8dl_playlist_match_description(),
        "primary_phrases": [list(group) for group in primary],
        "required_qualifiers": [list(group) for group in required],
        "rejected_qualifiers": [list(group) for group in rejected],
        "preferred_qualifiers": [list(group) for group in preferred],
    }


def _playlist_history_policy_snapshot(profile: Optional[dict] = None) -> dict:
    profile = dict(profile or get_nm3u8dl_playlist_profile())
    return {
        "renewal_mode": profile.get("renewal_mode"),
        "safe_overtime_min": profile.get("safe_overtime_min"),
        "allow_unknown_expiry": bool(profile.get("allow_unknown_expiry", False)),
        "prefer_unknown_expiry_on_equal_quality": bool(
            profile.get("prefer_unknown_expiry_on_equal_quality", False)
        ),
        "new_source_min_remaining_min": NM3U8DL_NEW_SOURCE_MIN_REMAINING_MIN,
        "replacement_safety_margin_min": NM3U8DL_REPLACEMENT_SAFETY_MARGIN_MIN,
        "quality_upgrade_enabled": bool(profile.get("quality_upgrade_enabled", False)),
        "quality_upgrade_target_fps": profile.get("quality_upgrade_target_fps"),
        "quality_upgrade_check_min": profile.get("quality_upgrade_check_min"),
        "quality_upgrade_min_remaining_min": profile.get(
            "quality_upgrade_min_remaining_min"
        ),
    }


def init_playlist_history(state: RecorderState):
    """Create per-recording temp history files inside CHUNKS_DIR."""
    state.playlist_history_active = _playlist_history_applicable()
    state.playlist_history_commit_ok = True

    if not state.playlist_history_active:
        return

    state.playlist_history_commit_ok = False
    state.playlist_history_started_ts = float(state.start_time)
    state.playlist_history_machine = _playlist_history_safe_machine_name()
    state.playlist_history_id = (
        f"{state.playlist_history_machine}_"
        f"{datetime.fromtimestamp(state.start_time).strftime('%Y%m%d_%H%M%S')}_"
        f"pid{os.getpid()}"
    )
    state.playlist_history_scan_index = 0
    state.playlist_history_txt_capture_failed = False
    state.playlist_history_json_capture_failed = False

    state.playlist_history_txt_path = os.path.join(
        CHUNKS_DIR,
        "_playlist_scan_history.tmp.txt",
    )
    state.playlist_history_jsonl_path = os.path.join(
        CHUNKS_DIR,
        "_playlist_scan_history.tmp.jsonl",
    )

    try:
        ensure_chunks_dir()
        match_context = _playlist_history_match_context()
        header = [
            "=" * 76,
            "PLAYLIST SCAN HISTORY — RECORDING",
            "=" * 76,
            f"Machine              : {state.playlist_history_machine}",
            f"Recording/history ID : {state.playlist_history_id}",
            f"Recording output     : {FINAL_FILE}",
            f"Recording start      : {_playlist_history_timestamp_text(state.start_time)}",
            f"Playlist group       : {match_context['playlist_group']}",
            f"Match mode           : {match_context['match_mode']}",
            f"Primary phrases      : {json.dumps(match_context['primary_phrases'], ensure_ascii=False)}",
            f"Required qualifiers  : {json.dumps(match_context['required_qualifiers'], ensure_ascii=False)}",
            f"Rejected qualifiers  : {json.dumps(match_context['rejected_qualifiers'], ensure_ascii=False)}",
            f"Preferred qualifiers : {json.dumps(match_context['preferred_qualifiers'], ensure_ascii=False)}",
            "=" * 76,
            "",
        ]

        with open(
            state.playlist_history_txt_path,
            "w",
            encoding="utf-8",
            errors="replace",
        ) as out:
            out.write("\n".join(header))

        with open(
            state.playlist_history_jsonl_path,
            "w",
            encoding="utf-8",
            errors="strict",
        ):
            pass

        # Capture is healthy until a later temp write says otherwise. The final
        # monthly append still has to succeed before cleanup is allowed.
        state.playlist_history_commit_ok = False

    except Exception as error:
        state.playlist_history_txt_capture_failed = True
        state.playlist_history_json_capture_failed = True
        log(
            "Playlist history initialization failed → recording will continue; "
            f"chunks will be retained for manual handling ({type(error).__name__}: {error})",
            level="WARN",
        )


def _playlist_history_mark_temp_failure(
    state: RecorderState,
    *,
    format_name: str,
    error: Exception,
):
    attr = (
        "playlist_history_txt_capture_failed"
        if format_name == "TXT"
        else "playlist_history_json_capture_failed"
    )

    if getattr(state, attr, False):
        return

    setattr(state, attr, True)
    state.playlist_history_commit_ok = False
    log(
        f"Playlist history {format_name} temp write failed → recording will continue; "
        f"chunks will be retained for manual handling ({type(error).__name__}: {error})",
        level="WARN",
    )


def _playlist_history_append_temp_text(state: RecorderState, text: str) -> bool:
    if (
        not state.playlist_history_active
        or state.playlist_history_txt_capture_failed
        or not state.playlist_history_txt_path
    ):
        return False

    try:
        with open(
            state.playlist_history_txt_path,
            "a",
            encoding="utf-8",
            errors="replace",
        ) as out:
            out.write(text)
            if text and not text.endswith("\n"):
                out.write("\n")
        return True
    except Exception as error:
        _playlist_history_mark_temp_failure(
            state,
            format_name="TXT",
            error=error,
        )
        return False


def _playlist_history_append_temp_json(state: RecorderState, item: dict) -> bool:
    if (
        not state.playlist_history_active
        or state.playlist_history_json_capture_failed
        or not state.playlist_history_jsonl_path
    ):
        return False

    try:
        line = json.dumps(
            item,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        with open(
            state.playlist_history_jsonl_path,
            "a",
            encoding="utf-8",
            errors="strict",
        ) as out:
            out.write(line + "\n")
        return True
    except Exception as error:
        _playlist_history_mark_temp_failure(
            state,
            format_name="JSONL",
            error=error,
        )
        return False


def _playlist_history_candidate_json(
    candidate: dict,
    *,
    selected_candidate: Optional[dict],
    scan_ts: float,
) -> dict:
    expiry = candidate.get("expiry")
    keys = [str(value) for value in (candidate.get("keys") or [])]
    stream_url = str(candidate.get("stream_url") or "").strip()
    raw_stream_url = str(candidate.get("raw_stream_url") or stream_url).strip()
    final_url = str(candidate.get("manifest_final_url") or "").strip()
    media_final_url = str(
        candidate.get("selected_media_final_url") or ""
    ).strip()

    try:
        source_host = str(urlparse(stream_url).hostname or "").strip() or None
    except Exception:
        source_host = None

    try:
        final_host = str(urlparse(final_url).hostname or "").strip() or None
    except Exception:
        final_host = None

    try:
        media_final_host = (
            str(urlparse(media_final_url).hostname or "").strip() or None
        )
    except Exception:
        media_final_host = None

    selected = bool(
        selected_candidate
        and _nm3u8dl_same_candidate(candidate, selected_candidate)
    )

    effective_headers = dict(candidate.get("effective_headers") or {})
    if not effective_headers and not candidate.get("ignored"):
        try:
            effective_headers = get_nm3u8dl_effective_headers(
                candidate.get("headers") or {},
                emit_logs=False,
            )
        except Exception:
            effective_headers = dict(candidate.get("headers") or {})

    try:
        status = _nm3u8dl_candidate_status(
            candidate,
            selected_candidate=selected_candidate,
            now_ts=scan_ts,
        )
        status_detail = _format_nm3u8dl_candidate_display_details(
            candidate,
            status=status,
            selected_candidate=selected_candidate,
            now_ts=scan_ts,
        )
    except Exception:
        status = "IGNORED" if candidate.get("ignored") else "UNKNOWN"
        status_detail = str(candidate.get("reason") or "").strip()

    if selected:
        selection_reason = "selected by resolver"
    elif status == "WORKING":
        selection_reason = _nm3u8dl_nonselection_reason(
            candidate,
            selected_candidate,
            now_ts=scan_ts,
        ) or "working alternative not selected"
    else:
        selection_reason = status_detail or status.lower()

    remaining_lifetime_sec = None
    if expiry is not None:
        try:
            remaining_lifetime_sec = int(float(expiry) - float(scan_ts))
        except Exception:
            remaining_lifetime_sec = None

    return {
        "matching_entry_index": candidate.get("matching_entry_index"),
        "tvg_name": candidate.get("tvg_name") or "",
        "group_title": candidate.get("group_title") or "",
        "entry_title": candidate.get("entry_title") or "",
        "extinf": candidate.get("extinf") or "",
        "option_lines": list(candidate.get("option_lines") or []),
        "raw_stream_url": raw_stream_url,
        "stream_url": stream_url,
        "manifest_final_url": final_url or None,
        "selected_media_final_url": media_final_url or None,
        "stream_type": candidate.get("stream_type")
        or _get_nm3u8dl_stream_type_from_url(stream_url),
        "source_host": source_host,
        "final_host": final_host,
        "media_final_host": media_final_host,
        "playlist_headers": dict(candidate.get("headers") or {}),
        "effective_headers": effective_headers,
        "quality": {
            "known": bool(candidate.get("quality_known", False)),
            "source": candidate.get("quality_source") or "",
            "width": int(candidate.get("video_width") or 0),
            "height": int(candidate.get("video_height") or 0),
            "fps": float(candidate.get("video_fps") or 0.0),
            "scan_type": _normalize_nm3u8dl_video_scan_type(
                candidate.get("video_scan_type")
            ) or None,
            "scan_type_source": candidate.get("video_scan_type_source") or None,
            "bitrate_bps": int(candidate.get("video_bitrate_bps") or 0),
            "bitrate_source": candidate.get("video_bitrate_source") or "",
        },
        "probe_duration_sec": candidate.get("probe_duration_sec"),
        "expiry": expiry,
        "expiry_source": candidate.get("expiry_source") or "",
        "resource_expiry": candidate.get("resource_expiry"),
        "remaining_lifetime_sec": remaining_lifetime_sec,
        "manifest_reachable": bool(candidate.get("manifest_reachable", False)),
        "ffprobe_reachable": bool(candidate.get("ffprobe_reachable", False)),
        "launchable": bool(candidate.get("launchable", False)),
        "access_blocked": bool(candidate.get("access_blocked", False)),
        "access_block_kind": candidate.get("access_block_kind") or "",
        "access_block_http_status": candidate.get("access_block_http_status"),
        "geo_country": candidate.get("geo_country"),
        "header_preparation_failure": candidate.get("header_preparation_failure") or "",
        "manifest_probe_failure": candidate.get("manifest_probe_failure") or "",
        "resource_probe_failure": candidate.get("resource_probe_failure") or "",
        "ffprobe_probe_failure": candidate.get("ffprobe_probe_failure") or "",
        "hls_variant_probe_status": candidate.get("hls_variant_probe_status") or "",
        "hls_variant_probe_failure": candidate.get("hls_variant_probe_failure") or "",
        "quality_probe_error": candidate.get("quality_probe_error") or "",
        "preferred_qualifier_score": int(
            candidate.get("preferred_qualifier_score") or 0
        ),
        "status": status,
        "status_detail": status_detail,
        "selected": selected,
        "selection_reason": selection_reason,
        "drm_protected": bool(
            candidate.get("drm_protected", False)
            or keys
        ),
        "key_required": bool(
            candidate.get("drm_key_required", False)
            or keys
        ),
        "key_type": "ClearKey" if keys else None,
        "key_count": len(keys),
        "key_values": keys,
        "key_fingerprints_sha256": [
            hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()
            for value in keys
        ],
    }


def _build_playlist_history_event(
    state: RecorderState,
    snapshot: dict,
    *,
    scan_id: str,
    scan_type: str,
    scan_reason: str,
    context: Optional[dict],
) -> dict:
    scan_ts = float(snapshot.get("scan_completed_ts") or time.time())
    selected_candidate = snapshot.get("selected_candidate")
    candidates = list(snapshot.get("candidates") or [])
    playlist_meta = dict(snapshot.get("playlist_meta") or {})
    source_results = list(snapshot.get("source_results") or [])
    results_by_url = {
        str(result.get("playlist_url") or ""): result
        for result in source_results
    }

    playlists = []
    for playlist_url in snapshot.get("playlist_urls") or []:
        result = results_by_url.get(str(playlist_url), {})
        meta = dict(playlist_meta.get(str(playlist_url)) or {})
        playlist_candidates = [
            _playlist_history_candidate_json(
                candidate,
                selected_candidate=selected_candidate,
                scan_ts=scan_ts,
            )
            for candidate in candidates
            if str(candidate.get("playlist_url") or "") == str(playlist_url)
        ]
        playlist_errors = [
            error
            for error in (snapshot.get("source_errors") or [])
            if str(error).startswith(f"{playlist_url}:")
        ]

        playlists.append({
            "playlist_url": playlist_url,
            "status": result.get("status") or "unknown",
            "detail": result.get("detail") or "",
            "summary_counts": dict(result.get("summary_counts") or {}),
            "fetch": {
                "duration_sec": meta.get("fetch_duration_sec"),
                "http_status": meta.get("http_status"),
                "final_url": meta.get("final_url") or None,
                "redirected": bool(meta.get("redirected", False)),
                "content_size_bytes": meta.get("content_size_bytes"),
                "content_sha256": meta.get("content_sha256") or None,
            },
            "activation": dict(meta.get("activation") or {}),
            "playlist_scan_duration_sec": meta.get("playlist_scan_duration_sec"),
            "errors": playlist_errors,
            "candidates": playlist_candidates,
        })

    selected_summary = None
    if selected_candidate:
        selected_summary = {
            "playlist_url": selected_candidate.get("playlist_url"),
            "matching_entry_index": selected_candidate.get("matching_entry_index"),
            "stream_url": selected_candidate.get("stream_url"),
            "manifest_final_url": selected_candidate.get("manifest_final_url") or None,
            "selected_media_final_url": selected_candidate.get("selected_media_final_url") or None,
            "quality": (
                format_nm3u8dl_candidate_quality(selected_candidate)
                if _nm3u8dl_has_quality_evidence(selected_candidate)
                else None
            ),
            "expiry": selected_candidate.get("expiry"),
            "expiry_source": selected_candidate.get("expiry_source") or "",
        }

    return {
        "schema_version": 1,
        "machine": state.playlist_history_machine,
        "os": {
            "sys_platform": sys.platform,
            "os_name": os.name,
        },
        "recording_id": state.playlist_history_id,
        "recording_output": FINAL_FILE,
        "recording_start_ts": state.playlist_history_started_ts,
        "recording_start": _playlist_history_timestamp_text(
            state.playlist_history_started_ts
        ),
        "scan_id": scan_id,
        "scan_type": str(scan_type),
        "scan_reason": str(scan_reason),
        "scan_started_ts": snapshot.get("scan_started_ts"),
        "scan_completed_ts": snapshot.get("scan_completed_ts"),
        "scan_started": _playlist_history_timestamp_text(
            snapshot.get("scan_started_ts")
        ),
        "scan_completed": _playlist_history_timestamp_text(
            snapshot.get("scan_completed_ts")
        ),
        "scan_duration_sec": snapshot.get("scan_duration_sec"),
        "targeted_scan": bool(snapshot.get("targeted_scan", False)),
        "playlist_group": snapshot.get("playlist_group"),
        "match_mode": snapshot.get("match_mode"),
        "match_description": snapshot.get("match_description"),
        "primary_phrases": snapshot.get("primary_phrases") or [],
        "required_qualifiers": snapshot.get("required_qualifiers") or [],
        "rejected_qualifiers": snapshot.get("rejected_qualifiers") or [],
        "preferred_qualifiers": snapshot.get("preferred_qualifiers") or [],
        "policy": dict(snapshot.get("policy") or {}),
        "source_errors": list(snapshot.get("source_errors") or []),
        "selected_candidate": selected_summary,
        "context": dict(context or {}),
        "playlists": playlists,
    }


def record_playlist_history_scan(
    state: RecorderState,
    snapshot: Optional[dict],
    *,
    scan_type: str,
    scan_reason: str,
    heading: str = "=== DYNAMIC SOURCE STATUS ===",
    section_level: str = "INFO",
    context: Optional[dict] = None,
):
    """Persist one meaningful scan to this recording's private temp history."""
    if not state.playlist_history_active:
        return

    if not isinstance(snapshot, dict):
        error = RuntimeError("resolver returned no history snapshot")
        _playlist_history_mark_temp_failure(
            state,
            format_name="TXT",
            error=error,
        )
        _playlist_history_mark_temp_failure(
            state,
            format_name="JSONL",
            error=error,
        )
        return

    state.playlist_history_scan_index += 1
    scan_id = (
        f"{state.playlist_history_id}_"
        f"scan{state.playlist_history_scan_index:04d}"
    )

    try:
        event = _build_playlist_history_event(
            state,
            snapshot,
            scan_id=scan_id,
            scan_type=scan_type,
            scan_reason=scan_reason,
            context=context,
        )
    except Exception as error:
        # History is evidence-only. A build bug must never escape into recorder
        # control flow or interrupt the recording.
        _playlist_history_mark_temp_failure(
            state,
            format_name="TXT",
            error=error,
        )
        _playlist_history_mark_temp_failure(
            state,
            format_name="JSONL",
            error=error,
        )
        return

    _playlist_history_append_temp_json(state, event)

    try:
        lines = [
            "-" * 76,
            f"SCAN ID              : {scan_id}",
            f"Scan type            : {scan_type}",
            f"Scan reason          : {scan_reason}",
            f"Scan started         : {_playlist_history_timestamp_text(snapshot.get('scan_started_ts'))}",
            f"Scan duration        : {float(snapshot.get('scan_duration_sec') or 0.0):.3f}s",
            f"Scan scope           : {'targeted' if snapshot.get('targeted_scan') else 'full'}",
        ]

        for key, value in (context or {}).items():
            if value is None or value == "":
                continue
            lines.append(f"{str(key).replace('_', ' ').title():<21}: {value}")

        captured_lines = []

        def capture_log(*args, level="INFO", **_kwargs):
            msg = " ".join(str(arg) for arg in args)
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            captured_lines.append(f"{ts} [{level}] {msg}")

        try:
            log_nm3u8dl_playlist_scan_results(
                source_results=list(snapshot.get("source_results") or []),
                source_errors=list(snapshot.get("source_errors") or []),
                playlist_group=str(snapshot.get("playlist_group") or ""),
                match_description=str(snapshot.get("match_description") or ""),
                playlist_source_count=len(snapshot.get("playlist_urls") or []),
                candidate_count=int(snapshot.get("candidate_count") or 0),
                selected_playlist_url=(
                    snapshot.get("selected_candidate") or {}
                ).get("playlist_url"),
                selected_candidate=snapshot.get("selected_candidate"),
                heading=heading,
                section_level=section_level,
                show_no_match=True,
                emit_log=capture_log,
            )
        except Exception as error:
            captured_lines.append(
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [WARN] "
                f"Could not render scan table for history ({type(error).__name__}: {error})"
            )

        lines.extend(captured_lines)
        lines.extend(["-" * 76, "", ""])
        _playlist_history_append_temp_text(state, "\n".join(lines))
    except Exception as error:
        _playlist_history_mark_temp_failure(
            state,
            format_name="TXT",
            error=error,
        )


def _playlist_history_master_paths(state: RecorderState):
    start_ts = float(state.playlist_history_started_ts or state.start_time)
    month_key = datetime.fromtimestamp(start_ts).strftime("%Y-%m")
    history_dir = PLAYLIST_HISTORY_DIR
    base = f"playlist_history_{state.playlist_history_machine}_{month_key}"
    return (
        history_dir,
        os.path.join(history_dir, base + ".txt"),
        os.path.join(history_dir, base + ".jsonl"),
        os.path.join(history_dir, "." + base + ".lock"),
    )


def _playlist_history_acquire_lock(lock_path: str) -> Optional[int]:
    deadline = time.monotonic() + float(PLAYLIST_HISTORY_LOCK_WAIT_SEC)

    while True:
        try:
            fd = os.open(
                lock_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
            os.write(
                fd,
                f"pid={os.getpid()} time={time.time():.3f}\n".encode("ascii", errors="replace"),
            )
            return fd
        except FileExistsError:
            try:
                age = time.time() - os.path.getmtime(lock_path)
                if age > float(PLAYLIST_HISTORY_STALE_LOCK_SEC):
                    os.remove(lock_path)
                    continue
            except FileNotFoundError:
                continue
            except Exception:
                pass

            if time.monotonic() >= deadline:
                return None
            time.sleep(0.25)


def _playlist_history_release_lock(lock_path: str, lock_fd: Optional[int]):
    try:
        if lock_fd is not None:
            os.close(lock_fd)
    except Exception:
        pass
    try:
        os.remove(lock_path)
    except Exception:
        pass


def _playlist_history_append_file(
    temp_path: str,
    master_path: str,
    *,
    text_separator: bool,
):
    master_exists = os.path.exists(master_path) and os.path.getsize(master_path) > 0
    with open(master_path, "ab") as out, open(temp_path, "rb") as inp:
        if text_separator and master_exists:
            out.write(b"\n\n")
        while True:
            chunk = inp.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
        out.flush()


def finalize_playlist_history(state: RecorderState) -> bool:
    """Commit this recording's private history before media finalization."""
    if not state.playlist_history_active:
        state.playlist_history_commit_ok = True
        return True

    if (
        int(getattr(state, "playlist_history_scan_index", 0) or 0) <= 0
        and not state.playlist_history_txt_capture_failed
        and not state.playlist_history_json_capture_failed
    ):
        state.playlist_history_commit_ok = True
        log("")
        log("Playlist history not updated → no completed playlist scans.")
        log("")
        return True

    txt_ready = bool(
        not state.playlist_history_txt_capture_failed
        and state.playlist_history_txt_path
        and os.path.exists(state.playlist_history_txt_path)
    )
    json_ready = bool(
        not state.playlist_history_json_capture_failed
        and state.playlist_history_jsonl_path
        and os.path.exists(state.playlist_history_jsonl_path)
    )

    history_dir = txt_master = json_master = lock_path = None
    try:
        history_dir, txt_master, json_master, lock_path = (
            _playlist_history_master_paths(state)
        )
        os.makedirs(history_dir, exist_ok=True)
    except Exception as error:
        state.playlist_history_commit_ok = False
        log(
            "Playlist history commit failed → could not prepare monthly history folder; "
            f"keeping chunks for manual handling ({type(error).__name__}: {error})",
            level="WARN",
        )
        return False

    lock_fd = _playlist_history_acquire_lock(lock_path)
    if lock_fd is None:
        state.playlist_history_commit_ok = False
        log(
            f"Playlist history commit failed → monthly history lock was not available "
            f"within {int(PLAYLIST_HISTORY_LOCK_WAIT_SEC)} seconds; keeping chunks for manual handling.",
            level="WARN",
        )
        return False

    txt_committed = False
    json_committed = False
    txt_error = None
    json_error = None

    try:
        if txt_ready:
            try:
                _playlist_history_append_file(
                    state.playlist_history_txt_path,
                    txt_master,
                    text_separator=True,
                )
                txt_committed = True
            except Exception as error:
                txt_error = error

        if json_ready:
            try:
                _playlist_history_append_file(
                    state.playlist_history_jsonl_path,
                    json_master,
                    text_separator=False,
                )
                json_committed = True
            except Exception as error:
                json_error = error
    finally:
        _playlist_history_release_lock(lock_path, lock_fd)

    if txt_committed:
        try:
            os.remove(state.playlist_history_txt_path)
        except Exception:
            pass

    if json_committed:
        try:
            os.remove(state.playlist_history_jsonl_path)
        except Exception:
            pass

    txt_ok = txt_committed and not state.playlist_history_txt_capture_failed
    json_ok = json_committed and not state.playlist_history_json_capture_failed
    state.playlist_history_commit_ok = bool(txt_ok and json_ok)

    if not state.playlist_history_commit_ok:
        reasons = []
        if state.playlist_history_txt_capture_failed:
            reasons.append("TXT temp capture incomplete")
        elif not txt_committed:
            reasons.append(
                "TXT monthly append failed"
                + (f" ({type(txt_error).__name__}: {txt_error})" if txt_error else "")
            )
        if state.playlist_history_json_capture_failed:
            reasons.append("JSONL temp capture incomplete")
        elif not json_committed:
            reasons.append(
                "JSONL monthly append failed"
                + (f" ({type(json_error).__name__}: {json_error})" if json_error else "")
            )

        retained = [
            path
            for path in (
                state.playlist_history_txt_path,
                state.playlist_history_jsonl_path,
            )
            if path and os.path.exists(path)
        ]
        retained_text = ", ".join(retained) if retained else CHUNKS_DIR
        log(
            "Playlist history commit incomplete → keeping chunks folder for manual handling; "
            f"reason: {'; '.join(reasons) or 'unknown history failure'}; "
            f"retained: {retained_text}",
            level="WARN",
        )
        return False

    log("")
    log(
        "Playlist history committed OK → "
        f"{os.path.basename(txt_master)} / {os.path.basename(json_master)}"
    )
    log("")
    return True


def resolve_nm3u8dl_playlist_source(
    *,
    include_candidate_pool: bool = False,
    playlist_urls_override: Optional[List[str]] = None,
    progress_stage: str = "checking playlist",
    progress_completion_label: str = "playlist scan complete",
    state: Optional[RecorderState] = None,
    stop_event: Optional[threading.Event] = None,
    show_progress: bool = True,
) -> dict:
    candidates = []
    source_errors = []
    source_results = []
    history_candidates = []
    playlist_history_meta = {}
    scan_started_ts = time.time()
    scan_started_mono = time.monotonic()

    def scan_stop_requested() -> bool:
        return bool(
            (state is not None and state.stop_flag)
            or (stop_event is not None and stop_event.is_set())
        )

    profile = get_nm3u8dl_playlist_profile()
    allow_unknown_expiry = bool(
        profile.get("allow_unknown_expiry", False)
    )

    if playlist_urls_override is None:
        playlist_urls = get_nm3u8dl_playlist_urls()
    else:
        playlist_urls = []

        for playlist_url in playlist_urls_override:
            playlist_url = str(playlist_url).strip()

            if playlist_url and playlist_url not in playlist_urls:
                playlist_urls.append(playlist_url)

        if not playlist_urls:
            raise RuntimeError(
                "No playlist URLs supplied for targeted access/VPN recheck"
            )

    total_playlists = len(playlist_urls)

    def build_history_snapshot(
        *,
        selected_candidate: Optional[dict] = None,
        candidate_count: Optional[int] = None,
    ) -> dict:
        completed_ts = time.time()
        match_context = _playlist_history_match_context()
        if candidate_count is None:
            candidate_count = sum(
                1
                for candidate in candidates
                if candidate.get("launchable", False)
            )
        selected_history_candidate = (
            dict(selected_candidate)
            if selected_candidate is not None
            else None
        )
        if selected_history_candidate is not None:
            selected_history_candidate.pop("_history_scan", None)
            selected_history_candidate.pop("_candidate_pool", None)
            selected_history_candidate.pop("source_results", None)
            selected_history_candidate.pop("source_errors", None)

        return {
            "scan_started_ts": scan_started_ts,
            "scan_completed_ts": completed_ts,
            "scan_duration_sec": round(time.monotonic() - scan_started_mono, 6),
            "targeted_scan": playlist_urls_override is not None,
            "playlist_urls": list(playlist_urls),
            "playlist_group": match_context["playlist_group"],
            "match_mode": match_context["match_mode"],
            "match_description": match_context["match_description"],
            "primary_phrases": match_context["primary_phrases"],
            "required_qualifiers": match_context["required_qualifiers"],
            "rejected_qualifiers": match_context["rejected_qualifiers"],
            "preferred_qualifiers": match_context["preferred_qualifiers"],
            "policy": _playlist_history_policy_snapshot(profile),
            "source_results": list(source_results),
            "source_errors": list(source_errors),
            "candidates": list(history_candidates),
            "selected_candidate": selected_history_candidate,
            "candidate_count": int(candidate_count or 0),
            "playlist_meta": dict(playlist_history_meta),
        }

    # Network fetch is the expensive part of playlist discovery. Fetch playlist
    # documents concurrently, but do not interpret or act on them in worker
    # threads. The main thread later processes every result in the original
    # configured order, preserving activation behavior, candidate ordering,
    # source_results/source_errors ordering, and playlist-history determinism.
    playlist_fetch_results = {}

    if total_playlists > 0:
        fetch_worker_count = min(
            max(1, int(NM3U8DL_PLAYLIST_FETCH_WORKERS)),
            total_playlists,
        )
        scan_activity_id = get_terminal_activity_id()

        def fetch_playlist_for_scan(playlist_url: str) -> dict:
            # Timeout/error diagnostics emitted inside the fetch helper should
            # stay attached to the same terminal activity as the parent scan.
            with terminal_activity_scope(activity_id=scan_activity_id):
                try:
                    playlist_text, fetch_metadata = (
                        fetch_nm3u8dl_playlist_text(
                            playlist_url,
                            stop_requested=scan_stop_requested,
                            include_metadata=True,
                        )
                    )
                    return {
                        "playlist_text": playlist_text,
                        "fetch_metadata": fetch_metadata,
                        "error": None,
                    }
                except Exception as error:
                    return {
                        "playlist_text": None,
                        "fetch_metadata": {},
                        "error": error,
                    }

        with ThreadPoolExecutor(
            max_workers=fetch_worker_count,
            thread_name_prefix="playlist_fetch",
        ) as executor:
            future_map = {
                executor.submit(
                    fetch_playlist_for_scan,
                    playlist_url,
                ): playlist_url
                for playlist_url in playlist_urls
            }

            completed_fetches = 0

            for future in as_completed(future_map):
                playlist_url = future_map[future]

                try:
                    playlist_fetch_results[playlist_url] = future.result()
                except Exception as error:
                    # The worker normally converts ordinary failures into a
                    # stored error result. Keep this boundary as a defensive
                    # fallback so one worker failure cannot abort the pool.
                    playlist_fetch_results[playlist_url] = {
                        "playlist_text": None,
                        "fetch_metadata": {},
                        "error": error,
                    }

                completed_fetches += 1

                if show_progress:
                    render_dynamic_playlist_progress(
                        "fetching playlists",
                        completed_fetches,
                        total_playlists,
                    )

                if scan_stop_requested():
                    for pending_future in future_map:
                        if not pending_future.done():
                            pending_future.cancel()
                    break

        if scan_stop_requested():
            if show_progress:
                clear_progress_line()
            raise RuntimeError("Playlist scan cancelled by stop request")

    for playlist_index, playlist_url in enumerate(playlist_urls, start=1):
        if scan_stop_requested():
            if show_progress:
                clear_progress_line()
            raise RuntimeError("Playlist scan cancelled by stop request")

        playlist_processing_started = time.monotonic()
        playlist_history_meta[playlist_url] = {
            "configured_url": playlist_url,
        }

        try:
            fetch_result = playlist_fetch_results.get(playlist_url)

            if fetch_result is None:
                raise RuntimeError(
                    "Playlist fetch result missing from parallel fetch stage"
                )

            fetch_error = fetch_result.get("error")

            if fetch_error is not None:
                raise fetch_error

            playlist_text = fetch_result.get("playlist_text")
            fetch_metadata = dict(
                fetch_result.get("fetch_metadata") or {}
            )

            if playlist_text is None:
                raise RuntimeError(
                    "Playlist fetch returned no text"
                )

            playlist_history_meta[playlist_url].update(fetch_metadata)

            playlist_text, adapter_metadata = (
                adapt_nm3u8dl_json_playlist_text(playlist_text)
            )
            playlist_history_meta[playlist_url][
                "source_adapter"
            ] = adapter_metadata

            activation_metadata = activate_nm3u8dl_playlist_if_present(
                playlist_url,
                playlist_text,
                stop_requested=scan_stop_requested,
            )
            playlist_history_meta[playlist_url][
                "activation"
            ] = activation_metadata

            if activation_metadata.get("blocking_failure"):
                activation_error = str(
                    activation_metadata.get("error")
                    or "activation did not verify"
                ).strip()

                detail = (
                    "Playlist activation failed — "
                    f"{activation_error}"
                )

                source_errors.append(
                    f"{playlist_url}: {detail}"
                )
                source_results.append({
                    "playlist_url": playlist_url,
                    "status": "activation_failed",
                    "detail": detail,
                    "expiry": None,
                    "candidate_rows": [],
                    "summary_counts": {},
                })

                continue

            entries = find_nm3u8dl_playlist_entries(
                playlist_text
            )

            playlist_candidates = []
            ignored_entries = 0
            ignored_candidate_rows = []

            for entry_index, entry in enumerate(entries, start=1):
                entry_name = (
                    entry.get("entry_title")
                    or entry.get("tvg_name")
                    or entry.get("group_title")
                    or entry["extinf"].strip()
                )

                try:
                    normalized_entry = (
                        normalize_nm3u8dl_playlist_entry(
                            entry
                        )
                    )

                    headers = dict(normalized_entry["headers"])
                    stream_user_agent = get_nm3u8dl_stream_user_agent(
                        playlist_url
                    )

                    if (
                        stream_user_agent
                        and not str(headers.get("User-Agent") or "").strip()
                    ):
                        headers["User-Agent"] = stream_user_agent

                    license_type = get_nm3u8dl_playlist_license_type(
                        normalized_entry["option_lines"]
                    )
                    unsupported_drm = (
                        "Widevine"
                        if (
                            license_type.casefold() == "com.widevine.alpha"
                            and _is_nm3u8dl_drmlive_host(
                                normalized_entry["stream_url"]
                            )
                            and _get_nm3u8dl_stream_type_from_url(
                                normalized_entry["stream_url"]
                            ) == "DASH"
                        )
                        else ""
                    )

                    # Only the direct DRMLive DASH/Widevine wrapper case is
                    # unsupported here. All other playlist entries preserve
                    # their existing key/decryption behavior unchanged.
                    keys = (
                        []
                        if unsupported_drm
                        else get_nm3u8dl_playlist_keys(
                            normalized_entry["option_lines"]
                        )
                    )

                    expiry = get_nm3u8dl_auth_expiry(
                        normalized_entry["stream_url"],
                        headers,
                    )

                    # URL-type license keys remain attached to the candidate.
                    # They are resolved only if this candidate wins selection.

                except (TypeError, ValueError, RuntimeError) as error:
                    ignored_entries += 1

                    ignored_candidate = {
                        "playlist_url": playlist_url,
                        "matching_entry_index": entry_index,
                        "extinf": entry.get("extinf", ""),
                        "tvg_name": entry.get("tvg_name", ""),
                        "group_title": entry.get("group_title", ""),
                        "entry_title": entry.get("entry_title", ""),
                        "option_lines": list(entry.get("option_lines") or []),
                        "raw_stream_url": entry.get("stream_url", ""),
                        "stream_url": entry.get("stream_url", ""),
                        "stream_type": _get_nm3u8dl_stream_type_from_url(
                            entry.get("stream_url", "")
                        ),
                        "preferred_qualifier_score": int(
                            entry.get("preferred_qualifier_score") or 0
                        ),
                        "ignored": True,
                        "reason": (
                            "playlist metadata/auth parsing failed "
                            f"({type(error).__name__}) — needs investigation"
                        ),
                        "expiry_required": not allow_unknown_expiry,
                    }
                    ignored_candidate_rows.append(ignored_candidate)
                    history_candidates.append(ignored_candidate)

                    source_errors.append(
                        f'{playlist_url}: matched entry "{entry_name}" ignored '
                        f'because normalization/auth parsing failed '
                        f'({type(error).__name__})'
                    )

                    continue

                candidate = {
                    "playlist_url": playlist_url,
                    "matching_entry_index": entry_index,
                    "extinf": normalized_entry["extinf"],
                    "tvg_name": entry.get("tvg_name", ""),
                    "group_title": entry.get("group_title", ""),
                    "entry_title": entry.get("entry_title", ""),
                    "option_lines": list(normalized_entry.get("option_lines") or []),
                    "raw_stream_url": entry.get("stream_url", ""),
                    "stream_url": normalized_entry["stream_url"],
                    "headers": headers,
                    "keys": keys,
                    "license_type": license_type,
                    "unsupported_drm": unsupported_drm,
                    "url_header_expiry": expiry,
                    "expiry": expiry,
                    "expiry_source": get_nm3u8dl_expiry_source(
                        expiry,
                        None,
                    ),
                    "preferred_qualifier_score": int(
                        entry.get("preferred_qualifier_score") or 0
                    ),
                }

                candidates.append(candidate)
                playlist_candidates.append(candidate)
                history_candidates.append(candidate)

            matching_entries = len(entries)

            if not playlist_candidates:
                entry_word = (
                    "entry"
                    if matching_entries == 1
                    else "entries"
                )

                detail = (
                    f"{matching_entries} matching {entry_word} found, "
                    f"but none survived normalization/auth parsing"
                )

                source_errors.append(
                    f"{playlist_url}: {detail}"
                )

                source_results.append({
                    "playlist_url": playlist_url,
                    "status": "unusable",
                    "detail": detail,
                    "expiry": None,
                    "candidate_rows": ignored_candidate_rows,
                    "summary_counts": {
                        "matching_entries": matching_entries,
                        "auth_expiry_ok": 0,
                        "working": 0,
                        "blocked": 0,
                        "not_working": 0,
                        "expired": 0,
                        "expiry_unknown": 0,
                        "ignored": ignored_entries,
                    },
                })

                continue

            entry_word = (
                "entry"
                if matching_entries == 1
                else "entries"
            )

            detail = f"{matching_entries} matching {entry_word}"

            if ignored_entries:
                detail += f"; {ignored_entries} ignored"

            source_results.append({
                "playlist_url": playlist_url,
                "status": "pending",
                "detail": detail,
                "expiry": None,
                "matching_entries": matching_entries,
                "ignored_entries": ignored_entries,
                "ignored_candidate_rows": ignored_candidate_rows,
            })

        except (
            HTTPError,
            URLError,
            TimeoutError,
            OSError,
            ValueError,
            RuntimeError,
        ) as error:
            if scan_stop_requested():
                if show_progress:
                    clear_progress_line()
                raise RuntimeError(
                    "Playlist scan cancelled by stop request"
                ) from error

            detail = str(error)
            connectivity_error = False

            if isinstance(error, NM3U8DLPlaylistFetchError):
                status = "fetch_error"
                connectivity_error = bool(
                    getattr(error, "connectivity_error", False)
                )
                playlist_history_meta[playlist_url].update({
                    "fetch_duration_sec": getattr(error, "fetch_duration_sec", None),
                    "http_status": getattr(error, "http_status", None),
                    "final_url": getattr(error, "final_url", "") or "",
                    "redirected": bool(
                        getattr(error, "final_url", "")
                        and getattr(error, "final_url", "") != playlist_url
                    ),
                    "content_size_bytes": None,
                    "content_sha256": "",
                })
            elif detail.startswith(
                "No playlist entry matched"
            ):
                status = "no_match"
            else:
                status = "error"

            source_errors.append(
                f"{playlist_url}: {detail}"
            )

            source_results.append({
                "playlist_url": playlist_url,
                "status": status,
                "detail": detail,
                "expiry": None,
                "connectivity_error": connectivity_error,
            })

        finally:
            playlist_meta = playlist_history_meta.setdefault(
                playlist_url,
                {},
            )
            processing_duration_sec = round(
                time.monotonic() - playlist_processing_started,
                6,
            )
            playlist_meta[
                "playlist_processing_duration_sec"
            ] = processing_duration_sec

            fetch_duration_sec = playlist_meta.get(
                "fetch_duration_sec"
            )

            try:
                fetch_duration_value = float(fetch_duration_sec)
            except (TypeError, ValueError):
                fetch_duration_value = 0.0

            playlist_meta[
                "playlist_scan_duration_sec"
            ] = round(
                fetch_duration_value + processing_duration_sec,
                6,
            )

    if scan_stop_requested():
        if show_progress:
            clear_progress_line()
        raise RuntimeError("Playlist scan cancelled by stop request")

    if not candidates:
        if show_progress:
            finish_dynamic_playlist_progress(
                f"{progress_completion_label} — "
                f"{total_playlists}/{total_playlists} checked; "
                "0 matched candidates to check"
            )

        details = "; ".join(source_errors)

        if not details:
            details = "no playlist URLs configured"

        raise NM3U8DLPlaylistResolutionError(
            f"No usable playlist source found "
            f"({details})",
            source_results,
            source_errors,
            history_scan=build_history_snapshot(candidate_count=0),
        )

    selection_now = time.time()

    # Ordinarily, preserve the mature optimization that does not probe an entry
    # whose known top-level/header authorization has already expired. Once this
    # recording has rejected a fingerprint, however, every matching normalized
    # entry must be probed on full scans: only the probe can reveal its current
    # final redirected manifest URL and therefore whether it is still the same
    # bad stream or has become a new eligible fingerprint.
    failover_fingerprint_check_active = bool(
        state is not None
        and getattr(state, "nm3u8dl_bad_stream_fingerprints", {})
    )
    probe_candidates = [
        candidate
        for candidate in candidates
        if (
            failover_fingerprint_check_active
            or candidate.get("expiry") is None
            or candidate["expiry"] > selection_now
        )
    ]

    if show_progress:
        finish_dynamic_playlist_progress(
            f"{progress_completion_label} — "
            f"{total_playlists}/{total_playlists} checked; "
            f"{len(probe_candidates)} matched candidates to check"
        )

    candidate_scan_completed = enrich_nm3u8dl_candidate_qualities(
        probe_candidates,
        stop_requested=scan_stop_requested,
        show_progress=show_progress,
    )

    if not candidate_scan_completed:
        raise RuntimeError("Playlist scan cancelled by stop request")

    selection_now = time.time()

    # Successful reactivation can restore the exact same effective fingerprint.
    _nm3u8dl_clear_reactivated_drmlive_failover_state(
        state,
        probe_candidates,
        playlist_history_meta,
    )

    _nm3u8dl_mark_bad_fingerprint_candidates(
        state,
        probe_candidates,
    )

    authorization_candidates = [
        candidate
        for candidate in candidates
        if (
            candidate.get("expiry") is not None
            or allow_unknown_expiry
        )
    ]

    usable_candidates = [
        candidate
        for candidate in authorization_candidates
        if (
            candidate.get("expiry") is None
            or candidate["expiry"] > selection_now
        )
    ]

    expired_candidates = [
        candidate
        for candidate in authorization_candidates
        if (
            candidate.get("expiry") is not None
            and candidate["expiry"] <= selection_now
        )
    ]

    excluded_candidates = [
        candidate
        for candidate in candidates
        if candidate.get("failover_excluded", False)
    ]

    eligible_expired_candidates = [
        candidate
        for candidate in expired_candidates
        if not candidate.get("failover_excluded", False)
    ]

    eligible_usable_candidates = [
        candidate
        for candidate in usable_candidates
        if not candidate.get("failover_excluded", False)
    ]

    launchable_candidates = [
        candidate
        for candidate in eligible_usable_candidates
        if candidate.get("launchable", False)
    ]

    if show_progress and probe_candidates:
        excluded_probe_count = sum(
            1
            for candidate in probe_candidates
            if candidate.get("failover_excluded", False)
        )
        completion_text = (
            f"candidate check complete — "
            f"{len(probe_candidates)}/{len(probe_candidates)} checked; "
            f"{len(launchable_candidates)} working"
        )
        if excluded_probe_count:
            completion_text += (
                f" eligible; {excluded_probe_count} excluded"
            )
        finish_dynamic_playlist_progress(completion_text)

    access_block_alarm = get_nm3u8dl_access_block_alarm_metadata(
        eligible_usable_candidates,
        launchable_candidates,
    )

    for result in source_results:
        if result.get("status") != "pending":
            continue

        playlist_url = result["playlist_url"]
        matching_entries = int(
            result.get("matching_entries") or 0
        )
        normalization_ignored = int(
            result.get("ignored_entries") or 0
        )

        source_candidates = [
            candidate
            for candidate in candidates
            if candidate["playlist_url"] == playlist_url
        ]

        source_unknown_disallowed = [
            candidate
            for candidate in source_candidates
            if (
                not candidate.get("failover_excluded", False)
                and candidate.get("expiry") is None
                and not allow_unknown_expiry
            )
        ]

        source_authorization_candidates = [
            candidate
            for candidate in source_candidates
            if (
                candidate.get("expiry") is not None
                or allow_unknown_expiry
            )
        ]

        source_usable_candidates = [
            candidate
            for candidate in source_authorization_candidates
            if (
                candidate.get("expiry") is None
                or candidate["expiry"] > selection_now
            )
        ]

        source_expired_candidates = [
            candidate
            for candidate in source_authorization_candidates
            if (
                not candidate.get("failover_excluded", False)
                and candidate.get("expiry") is not None
                and candidate["expiry"] <= selection_now
            )
        ]

        source_excluded_candidates = [
            candidate
            for candidate in source_candidates
            if candidate.get("failover_excluded", False)
        ]

        source_eligible_usable_candidates = [
            candidate
            for candidate in source_usable_candidates
            if not candidate.get("failover_excluded", False)
        ]

        source_launchable_candidates = [
            candidate
            for candidate in source_eligible_usable_candidates
            if candidate.get("launchable", False)
        ]

        source_access_blocked_candidates = [
            candidate
            for candidate in source_eligible_usable_candidates
            if candidate.get("access_blocked", False)
        ]

        source_not_working_candidates = [
            candidate
            for candidate in source_eligible_usable_candidates
            if (
                not candidate.get("launchable", False)
                and not candidate.get("access_blocked", False)
            )
        ]

        result["summary_counts"] = {
            "matching_entries": matching_entries,
            "auth_expiry_ok": len(source_usable_candidates),
            "working": len(source_launchable_candidates),
            "excluded": len(source_excluded_candidates),
            "blocked": len(source_access_blocked_candidates),
            "not_working": len(source_not_working_candidates),
            "expired": len(source_expired_candidates),
            "expiry_unknown": len(source_unknown_disallowed),
            "ignored": normalization_ignored,
        }

        candidate_rows = [
            {
                **_make_nm3u8dl_candidate_display_row(candidate),
                "expiry_required": not allow_unknown_expiry,
            }
            for candidate in source_candidates
        ]
        candidate_rows.extend(
            result.get("ignored_candidate_rows") or []
        )
        candidate_rows.sort(
            key=lambda row: int(row.get("matching_entry_index") or 0)
        )
        result["candidate_rows"] = candidate_rows

        # Keep this as structured state so the 30-second access/VPN recovery
        # loop can target only playlist sources that actually produced blocks.
        result["access_blocked_count"] = len(
            source_access_blocked_candidates
        )

        ignored_count = (
            normalization_ignored
            + len(source_unknown_disallowed)
        )

        entry_word = (
            "entry"
            if matching_entries == 1
            else "entries"
        )

        detail = f"{matching_entries} matching {entry_word}"

        if source_usable_candidates:
            detail += (
                f"; {len(source_usable_candidates)} auth/expiry OK"
            )

        unknown_allowed_count = sum(
            1
            for candidate in source_usable_candidates
            if candidate.get("expiry") is None
        )

        if unknown_allowed_count:
            detail += (
                f"; {unknown_allowed_count} unknown expiry"
            )

        if source_expired_candidates:
            detail += (
                f"; {len(source_expired_candidates)} expired"
            )

        if source_excluded_candidates:
            detail += (
                f"; {len(source_excluded_candidates)} excluded"
            )

        if ignored_count:
            detail += f"; {ignored_count} ignored"

        if source_access_blocked_candidates:
            access_block_text = format_nm3u8dl_access_block_warning(
                source_access_blocked_candidates
            ).replace("MANIFEST ACCESS BLOCKED: ", "", 1)

            detail += (
                f"; {len(source_access_blocked_candidates)} access-blocked "
                f"({access_block_text})"
            )

            source_errors.append(
                f'{playlist_url}: '
                f'{format_nm3u8dl_access_block_warning(source_access_blocked_candidates)}'
            )

        if source_usable_candidates:
            result["status"] = "matched"

            known_usable = [
                candidate
                for candidate in source_usable_candidates
                if candidate.get("expiry") is not None
            ]

            if known_usable:
                result["expiry"] = max(
                    candidate["expiry"]
                    for candidate in known_usable
                )
            else:
                result["expiry"] = None

            if source_launchable_candidates:
                detail += (
                    f"; {len(source_launchable_candidates)} working"
                )

                best_source_candidate = get_nm3u8dl_join_candidate(
                    source_launchable_candidates,
                    now_ts=selection_now,
                )

                result["best_candidate_expiry"] = (
                    best_source_candidate.get("expiry")
                )
                result["best_candidate_expiry_source"] = (
                    best_source_candidate.get("expiry_source") or ""
                )
                best_source_quality = format_nm3u8dl_candidate_quality(
                    best_source_candidate
                )
                if _nm3u8dl_has_quality_evidence(best_source_candidate):
                    result["quality_summary"] = best_source_quality
            else:
                detail += "; 0 working"

                if source_eligible_usable_candidates:
                    if source_access_blocked_candidates:
                        result["status"] = "access_blocked"
                    else:
                        detail += " (manifest and ffprobe probes failed)"
                        result["status"] = "unusable"

                    source_errors.append(
                        f"{playlist_url}: {detail}"
                    )
                else:
                    # All auth/expiry-eligible rows for this playlist were probed
                    # and resolve to streams/feed signatures already rejected here.
                    result["status"] = "matched"

                result["best_candidate_expiry"] = None
                result["quality_summary"] = "unknown"

        elif source_expired_candidates:
            result["status"] = "expired"

            latest_expired_candidate = max(
                source_expired_candidates,
                key=lambda candidate: candidate["expiry"],
            )

            result["expiry"] = (
                latest_expired_candidate["expiry"]
            )
            result["best_candidate_expiry"] = None
            result["quality_summary"] = "unknown"

        elif source_excluded_candidates:
            # These rows were probed in this scan and resolved to streams/feed
            # signatures already rejected for this recording. Keep the playlist as a
            # normal matched source container; the candidate rows carry EXCLUDED.
            result["status"] = "matched"
            result["expiry"] = None
            result["best_candidate_expiry"] = None
            result["quality_summary"] = "unknown"

        else:
            result["status"] = "unusable"
            result["expiry"] = None
            result["best_candidate_expiry"] = None
            result["quality_summary"] = "unknown"

            if source_unknown_disallowed:
                detail += (
                    "; no recognizable authorization expiry "
                    "after manifest inspection"
                )

            source_errors.append(
                f"{playlist_url}: {detail}"
            )

        result["detail"] = detail
        result.pop("matching_entries", None)
        result.pop("ignored_entries", None)
        result.pop("ignored_candidate_rows", None)

    if launchable_candidates:
        selected = dict(
            get_nm3u8dl_join_candidate(
                launchable_candidates,
                now_ts=selection_now,
            )
        )

    elif eligible_usable_candidates:
        all_usable_candidates_access_blocked = (
            bool(eligible_usable_candidates)
            and all(
                candidate.get("access_blocked", False)
                for candidate in eligible_usable_candidates
            )
        )

        overall_warning = (
            format_nm3u8dl_access_block_warning(eligible_usable_candidates)
            if all_usable_candidates_access_blocked
            else None
        )

        raise NM3U8DLPlaylistResolutionError(
            "No working eligible playlist source found "
            "(remaining candidates with auth/expiry OK failed "
            "manifest and ffprobe probes)",
            source_results,
            source_errors,
            overall_warning=overall_warning,
            access_block_alarm_actionable=bool(
                access_block_alarm.get("actionable", False)
            ),
            access_block_alarm_quality=access_block_alarm.get(
                "blocked_quality"
            ),
            history_scan=build_history_snapshot(candidate_count=0),
        )

    elif eligible_expired_candidates and not (
        state is not None
        and getattr(state, "nm3u8dl_failover_waiting_for_alternative", False)
    ):
        # Preserve the existing all-expired startup/ordinary-wait behavior. During
        # failover exhaustion we stay in the explicit no-alternative recovery loop
        # so its non-terminal alarm/search semantics remain authoritative.
        selected = dict(
            max(
                eligible_expired_candidates,
                key=lambda candidate: candidate["expiry"],
            )
        )

    elif excluded_candidates:
        raise NM3U8DLPlaylistResolutionError(
            "No eligible playlist source found "
            "(matching stream fingerprints remain excluded for this recording)",
            source_results,
            source_errors,
            history_scan=build_history_snapshot(candidate_count=0),
        )

    else:
        details = "; ".join(source_errors)

        if not details:
            if (
                expired_candidates
                and state is not None
                and getattr(state, "nm3u8dl_failover_waiting_for_alternative", False)
            ):
                details = "no currently viable alternative; remaining candidates are expired"
            else:
                details = (
                    "no candidate has recognizable authorization expiry"
                )

        raise NM3U8DLPlaylistResolutionError(
            f"No usable playlist source found ({details})",
            source_results,
            source_errors,
            history_scan=build_history_snapshot(candidate_count=0),
        )

    selected["candidate_count"] = len(launchable_candidates)
    selected["playlist_source_count"] = len(playlist_urls)
    selected["playlist_group"] = (
        NM3U8DL_PLAYLIST_GROUP.strip().upper()
    )
    selected["match_description"] = (
        get_nm3u8dl_playlist_match_description()
    )
    selected["source_errors"] = source_errors
    selected["source_results"] = source_results
    selected["access_block_alarm_actionable"] = bool(
        access_block_alarm.get("actionable", False)
    )
    selected["access_block_alarm_quality"] = access_block_alarm.get(
        "blocked_quality"
    )
    selected["_history_scan"] = build_history_snapshot(
        selected_candidate=selected,
        candidate_count=len(launchable_candidates),
    )

    if include_candidate_pool:
        selected["_candidate_pool"] = [
            dict(candidate)
            for candidate in launchable_candidates
        ]

    return selected


def resolve_nm3u8dl_launch_source(
    state: RecorderState
) -> Optional[dict]:

    profile = get_nm3u8dl_playlist_profile()
    safe_overtime_min = profile["safe_overtime_min"]
    allow_unknown_expiry = bool(
        profile.get("allow_unknown_expiry", False)
    )

    renewal_mode = profile.get(
        "renewal_mode",
        "EXPIRY_ROLLOVER",
    )

    if renewal_mode not in (
        "EXPIRY_ROLLOVER",
        "RESOLVE_ON_RESTART",
    ):
        raise RuntimeError(
            f'Unknown renewal mode: "{renewal_mode}"'
        )

    rollover_reason = state.nm3u8dl_rollover_reason

    # Expiry-based profiles may carry a retained authorization replacement.
    # A quality-upgrade rollover may also carry its chosen source into the
    # next run, even if a future profile uses a different renewal mode.
    if (
        renewal_mode == "EXPIRY_ROLLOVER"
        or rollover_reason == "quality_upgrade"
    ):
        retained_source = state.nm3u8dl_pending_source
    else:
        retained_source = None

    direct_retry_source = state.nm3u8dl_failover_retry_source
    state.nm3u8dl_failover_retry_source = None

    previous_running_source = state.nm3u8dl_running_source
    previous_playlist_url = (
        previous_running_source.get("playlist_url")
        if previous_running_source
        else None
    )

    state.nm3u8dl_pending_source = None
    state.nm3u8dl_running_source = None
    state.nm3u8dl_running_stream_fingerprint = None

    last_wait_signature = None
    last_expired_wait_signature = None
    consecutive_connectivity_failures = 0
    source_wait_is_recovery = (
        state.stats.get("good_chunks", 0) > 0
        and rollover_reason is None
    )
    retained_source_label = None

    def scan_has_recovery_connectivity_failure(
        source_results: List[dict]
    ) -> bool:
        if not source_wait_is_recovery or not source_results:
            return False

        all_connectivity_fetches_failed = all(
            result.get("status") == "fetch_error"
            and bool(result.get("connectivity_error", False))
            for result in source_results
        )

        previous_source_connectivity_failed = (
            previous_playlist_url is not None
            and any(
                result.get("playlist_url") == previous_playlist_url
                and bool(result.get("connectivity_error", False))
                for result in source_results
            )
        )

        return (
            all_connectivity_fetches_failed
            or previous_source_connectivity_failed
        )

    def stop_source_wait_alarm(reason: str):
        alarm_type = getattr(state, "shared_alarm_type", None)

        if alarm_type == "playlist_connectivity":
            shared_alarm_stop(state)
            state.alarm_ack_requested = False
            log(
                f"ALARM_PLAYLIST_CONNECTIVITY_STOP "
                f"reason={reason}"
            )

        elif alarm_type == "playlist_access_block":
            shared_alarm_stop(state)
            state.alarm_ack_requested = False
            log(
                f"ALARM_PLAYLIST_ACCESS_BLOCK_STOP "
                f"reason={reason}"
            )

        elif alarm_type == "playlist_failover":
            shared_alarm_stop(state)
            state.alarm_ack_requested = False
            log(
                f"ALARM_PLAYLIST_FAILOVER_STOP "
                f"reason={reason}"
            )
            
    def source_wait_deadline_reached() -> bool:
        if (
            get_recording_deadline(state) is None
            or not recording_deadline_reached(state)
        ):
            return False

        stop_source_wait_alarm("planned_end")
        log(
            "Planned recording end reached while waiting for "
            "playlist source → ending source wait."
        )
        return True

    while True:
        if getattr(state, "alarm_ack_requested", False):
            waiting_alarm_type = getattr(state, "shared_alarm_type", None)
            if waiting_alarm_type in ("playlist_connectivity", "playlist_failover"):
                log(
                    "ALARM_ACK",
                    activity_id=getattr(
                        state,
                        "shared_alarm_activity_id",
                        None,
                    ),
                )
                stop_source_wait_alarm("ack")
                if waiting_alarm_type == "playlist_connectivity":
                    state.nm3u8dl_playlist_connectivity_alarm_silenced = True
                else:
                    state.nm3u8dl_failover_alarm_silenced = True

        if state.stop_flag:
            stop_source_wait_alarm("stop")
            set_terminal_activity_context(None)
            return None

        if source_wait_deadline_reached():
            set_terminal_activity_context(None)
            return None

        if direct_retry_source is not None:
            set_terminal_activity_context(
                new_terminal_activity("stream_failover_direct_retry")
            )
            source = direct_retry_source
            direct_retry_source = None
            fingerprint = str(
                source.get("stream_fingerprint")
                or get_nm3u8dl_stream_fingerprint(source)
                or ""
            ).strip()
            if fingerprint:
                source["stream_fingerprint"] = fingerprint

            log(
                "Using direct stream-failure retry selected during the "
                "previous run; no new playlist scan."
            )
            log(f"Direct retry source       : {source.get('playlist_url') or 'unknown'}")

            _nm3u8dl_set_running_stream_identity(state, source)
            state.nm3u8dl_renewal_rollover_requested = False
            state.nm3u8dl_rollover_reason = None
            set_terminal_activity_context(None)
            return source

        if retained_source is not None:
            retained_expiry = retained_source.get("expiry")

            if (
                (
                    retained_expiry is None
                    and allow_unknown_expiry
                )
                or (
                    retained_expiry is not None
                    and retained_expiry > time.time()
                )
            ):
                set_terminal_activity_context(
                    new_terminal_activity("retained_playlist_source")
                )
                source = retained_source
                retained_source = None

                retained_label = (
                    "quality-upgrade source"
                    if rollover_reason == "quality_upgrade"
                    else "authorization replacement"
                )
                retained_source_label = retained_label

                log(
                    f"Using retained {retained_label} selected during the "
                    "previous run; no new playlist scan."
                )
                log(
                    f"Retained source          : {source['playlist_url']}"
                )

            else:
                retained_source = None

                log(
                    "Retained authorization replacement is no longer "
                    "valid → resolving from playlist sources again...",
                    level="WARN",
                )

                continue

        else:
            # Each source-resolution attempt is one logical terminal activity.
            # A retry therefore starts a fresh block, while all scan/probe/result
            # lines from this attempt stay together unless another thread interrupts.
            set_terminal_activity_context(
                new_terminal_activity("dynamic_playlist_scan")
            )

            access_route_baseline_urls = list(
                state.nm3u8dl_access_block_playlist_urls
            )
            access_route_baseline_snapshot = dict(
                state.nm3u8dl_access_block_status_snapshot or {}
            )

            try:
                source = resolve_nm3u8dl_playlist_source(
                    state=state,
                )

                initial_source_results = source.get("source_results", [])

                if _nm3u8dl_confirm_access_change_and_reset_failover(
                    state,
                    tracked_urls=access_route_baseline_urls,
                    previous_snapshot=access_route_baseline_snapshot,
                    first_pass_source_results=initial_source_results,
                ):
                    # The just-completed scan still applied the old exclusions.
                    # Rerun immediately so every candidate is ranked under the
                    # newly confirmed access environment.
                    last_wait_signature = None
                    continue
                initial_blocked_urls = (
                    get_nm3u8dl_access_block_playlist_urls(
                        initial_source_results
                    )
                )
                initial_access_actionable = bool(
                    source.get("access_block_alarm_actionable", False)
                )
                quality_profile_enabled = bool(
                    profile.get("quality_upgrade_enabled", False)
                )
                quality_target_fps = float(
                    profile.get("quality_upgrade_target_fps", 50)
                )
                source_motion_fps = _nm3u8dl_ranking_motion_fps(source)

                # Preserve the existing access/VPN policy: once the running source
                # reaches the configured target motion, do not keep a 30-second
                # quality-only VPN incident alive solely for further quality gains.
                # Normal periodic quality scans remain enabled and can still find
                # a better same-motion source such as 720p50 -> 1080p50.
                if (
                    initial_access_actionable
                    and quality_profile_enabled
                    and source_motion_fps >= quality_target_fps
                ):
                    initial_access_actionable = False

                if initial_access_actionable:
                    state.nm3u8dl_access_block_playlist_urls = list(
                        initial_blocked_urls
                    )
                    state.nm3u8dl_access_block_status_snapshot = (
                        get_nm3u8dl_access_recheck_snapshot(
                            initial_blocked_urls,
                            initial_source_results,
                        )
                    )
                    state.nm3u8dl_access_block_purpose = (
                        "quality" if quality_profile_enabled else "access"
                    )
                else:
                    state.nm3u8dl_access_block_playlist_urls = []
                    state.nm3u8dl_access_block_status_snapshot = {}
                    state.nm3u8dl_access_block_purpose = None

                update_nm3u8dl_access_block_alarm_state(
                    state,
                    actionable=initial_access_actionable,
                    blocked_quality=source.get("access_block_alarm_quality"),
                    running_quality=format_nm3u8dl_candidate_quality(source),
                )

                # A returned source can still be expired. Do not declare
                # connectivity recovered until launchability is checked below.
                last_wait_signature = None

            except RuntimeError as error:
                if state.stop_flag:
                    set_terminal_activity_context(None)
                    return None

                source_results = getattr(
                    error,
                    "source_results",
                    [],
                )
                source_errors = getattr(
                    error,
                    "source_errors",
                    [],
                )
                overall_warning = getattr(
                    error,
                    "overall_warning",
                    None,
                )
                access_block_alarm_actionable = bool(
                    getattr(
                        error,
                        "access_block_alarm_actionable",
                        False,
                    )
                )
                access_block_alarm_quality = getattr(
                    error,
                    "access_block_alarm_quality",
                    None,
                )

                if _nm3u8dl_confirm_access_change_and_reset_failover(
                    state,
                    tracked_urls=access_route_baseline_urls,
                    previous_snapshot=access_route_baseline_snapshot,
                    first_pass_source_results=source_results,
                ):
                    # Do not print/alarm on the stale result that still contained
                    # exclusions from the old route. Re-evaluate immediately.
                    last_wait_signature = None
                    continue

                # A no-source wait can be the first place an access incident is
                # discovered. Establish the fixed blocked-source baseline so a
                # later VPN/route recovery can receive the same two-pass proof.
                if (
                    access_block_alarm_actionable
                    and not state.nm3u8dl_access_block_playlist_urls
                ):
                    waiting_blocked_urls = (
                        get_nm3u8dl_access_block_playlist_urls(
                            source_results
                        )
                    )
                    if waiting_blocked_urls:
                        state.nm3u8dl_access_block_playlist_urls = list(
                            waiting_blocked_urls
                        )
                        state.nm3u8dl_access_block_status_snapshot = (
                            get_nm3u8dl_access_recheck_snapshot(
                                waiting_blocked_urls,
                                source_results,
                            )
                        )
                        state.nm3u8dl_access_block_purpose = "access"

                source_retry_interval_sec = (
                    NM3U8DL_PLAYLIST_ACCESS_CHECK_INTERVAL_SEC
                    if access_block_alarm_actionable
                    else NM3U8DL_PLAYLIST_RISK_CHECK_INTERVAL_SEC
                )

                connectivity_failure_scan = (
                    scan_has_recovery_connectivity_failure(
                        source_results
                    )
                )

                if connectivity_failure_scan:
                    consecutive_connectivity_failures += 1
                else:
                    if (
                        getattr(state, "shared_alarm_type", None)
                        == "playlist_connectivity"
                    ):
                        stop_source_wait_alarm(
                            "source_access_recovered"
                        )

                    consecutive_connectivity_failures = 0
                    state.nm3u8dl_playlist_connectivity_alarm_silenced = False

                # Compare what the operator would actually see in the scan table,
                # not probe-internal fields that can vary while the displayed result
                # is unchanged.
                wait_display_lines = []

                def collect_wait_display(*args, level="INFO", **kwargs):
                    wait_display_lines.append(
                        (
                            str(level),
                            " ".join(str(arg) for arg in args),
                        )
                    )

                log_nm3u8dl_playlist_scan_results(
                    source_results=source_results,
                    source_errors=source_errors,
                    playlist_group=NM3U8DL_PLAYLIST_GROUP.strip().upper(),
                    match_description=get_nm3u8dl_playlist_match_description(),
                    playlist_source_count=len(get_nm3u8dl_playlist_urls()),
                    candidate_count=0,
                    heading="=== WAITING FOR PLAYLIST SOURCE ===",
                    section_level="WARN",
                    emit_log=collect_wait_display,
                )

                wait_signature = (
                    connectivity_failure_scan,
                    source_retry_interval_sec,
                    tuple(wait_display_lines),
                    overall_warning or "",
                    str(error) if not source_results else "",
                )

                if wait_signature == last_wait_signature:
                    log(
                        f"Still waiting for playlist source — no change; "
                        f"next check in "
                        f"{source_retry_interval_sec}s",
                        level="WARN",
                    )

                else:
                    record_playlist_history_scan(
                        state,
                        getattr(error, "history_scan", None),
                        scan_type=(
                            "recovery_wait"
                            if source_wait_is_recovery
                            else "startup_wait"
                        ),
                        scan_reason="no usable playlist source while waiting",
                        heading="=== WAITING FOR PLAYLIST SOURCE ===",
                        section_level="WARN",
                        context={
                            "error": str(error),
                            "overall_warning": overall_warning or "",
                        },
                    )

                    log_nm3u8dl_playlist_scan_results(
                        source_results=source_results,
                        source_errors=source_errors,
                        playlist_group=(
                            NM3U8DL_PLAYLIST_GROUP.strip().upper()
                        ),
                        match_description=(
                            get_nm3u8dl_playlist_match_description()
                        ),
                        playlist_source_count=len(
                            get_nm3u8dl_playlist_urls()
                        ),
                        candidate_count=0,
                        heading="=== WAITING FOR PLAYLIST SOURCE ===",
                        section_level="WARN",
                    )

                    if not source_results:
                        log(
                            f"Reason             : {error}",
                            level="WARN",
                        )

                    if overall_warning:
                        log(
                            overall_warning,
                            level="WARN",
                        )

                    log(
                        "No usable playlist source found.",
                        level="WARN",
                    )
                    log(
                        f"Next playlist check: "
                        f"{source_retry_interval_sec} seconds",
                        level="WARN",
                    )
                    log(
                        "===================================",
                        level="WARN",
                    )

                    last_wait_signature = wait_signature

                update_nm3u8dl_access_block_alarm_state(
                    state,
                    actionable=access_block_alarm_actionable,
                    blocked_quality=access_block_alarm_quality,
                    running_quality=None,
                )

                if (
                    state.nm3u8dl_failover_waiting_for_alternative
                    and not connectivity_failure_scan
                    and not access_block_alarm_actionable
                    and not state.nm3u8dl_failover_alarm_silenced
                    and not shared_alarm_is_active(state)
                ):
                    log(
                        "No viable playlist alternative remains after stream "
                        "rejection → alarm active while full playlist rescans continue.",
                        level="WARN",
                    )
                    shared_alarm_start(
                        state,
                        alarm_type="playlist_failover",
                        incident=None,
                    )

                if connectivity_failure_scan:
                    log(
                        f"Connectivity recovery scan: "
                        f"{min(consecutive_connectivity_failures, 2)}/2",
                        level="WARN",
                    )

                if source_wait_deadline_reached():
                    set_terminal_activity_context(None)
                    return None

                if (
                    connectivity_failure_scan
                    and consecutive_connectivity_failures >= 2
                    and not state.nm3u8dl_playlist_connectivity_alarm_silenced
                    and not shared_alarm_is_active(state)
                ):
                    log(
                        "Playlist/source access failed on two consecutive "
                        "recovery scans → possible "
                        "connectivity/DNS/VPN/source-access problem.",
                        level="WARN",
                    )
                    shared_alarm_start(
                        state,
                        alarm_type="playlist_connectivity",
                        incident=None,
                    )

                sleep_with_interrupts(
                    state,
                    source_retry_interval_sec,
                )

                # ACK and Ctrl-C cleanup are handled at the top of the loop
                # before another source scan begins.
                continue

        expiry = source.get("expiry")
        expiry_source = source.get("expiry_source") or ""

        now = time.time()
        connectivity_failure_scan = (
            scan_has_recovery_connectivity_failure(
                source.get("source_results", [])
            )
        )

        if expiry is None or expiry > now:
            if (
                getattr(state, "shared_alarm_type", None)
                == "playlist_connectivity"
            ):
                stop_source_wait_alarm("source_recovered")

            if (
                getattr(state, "shared_alarm_type", None)
                == "playlist_failover"
            ):
                stop_source_wait_alarm("viable_alternative_found")

            if state.nm3u8dl_failover_waiting_for_alternative:
                log(
                    "Stream failover found a viable alternative → "
                    "recording will resume with the selected source."
                )
            state.nm3u8dl_failover_waiting_for_alternative = False
            state.nm3u8dl_failover_alarm_silenced = False

            consecutive_connectivity_failures = 0
            state.nm3u8dl_playlist_connectivity_alarm_silenced = False

            if expiry is None:
                expiry_local = "unknown"
                remaining = None
            else:
                expiry_local = datetime.fromtimestamp(expiry).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                remaining = int(expiry - now)

            running_target = None
            running_target_local = None

            if (
                renewal_mode == "EXPIRY_ROLLOVER"
                and expiry is not None
            ):
                running_target = (
                    expiry
                    + int(safe_overtime_min * 60)
                )

                running_target_local = datetime.fromtimestamp(
                    running_target
                ).strftime("%Y-%m-%d %H:%M:%S")

            extinf_metadata = parse_nm3u8dl_extinf_metadata(
                source["extinf"]
            )

            tvg_name = source.get(
                "tvg_name",
                extinf_metadata["tvg_name"],
            )
            group_title = source.get(
                "group_title",
                extinf_metadata["group_title"],
            )
            entry_title = source.get(
                "entry_title",
                extinf_metadata["entry_title"],
            )

            if retained_source_label is None:
                record_playlist_history_scan(
                    state,
                    source.get("_history_scan"),
                    scan_type=(
                        "recovery"
                        if source_wait_is_recovery
                        else "startup"
                    ),
                    scan_reason=(
                        "source recovery resolution"
                        if source_wait_is_recovery
                        else "initial source resolution"
                    ),
                    heading="=== DYNAMIC SOURCE STATUS ===",
                    context={
                        "selected_quality": format_nm3u8dl_candidate_quality(source),
                        "selected_expiry": format_nm3u8dl_expiry_with_source(
                            expiry,
                            expiry_source,
                        ),
                    },
                )

                log_nm3u8dl_playlist_scan_results(
                    source_results=source["source_results"],
                    source_errors=source["source_errors"],
                    playlist_group=source["playlist_group"],
                    match_description=source["match_description"],
                    playlist_source_count=source["playlist_source_count"],
                    candidate_count=source["candidate_count"],
                    selected_playlist_url=source["playlist_url"],
                    selected_candidate=source,
                )

            log("")
            log(f"TVG name                : {tvg_name}")
            log(f"Group title             : {group_title}")
            log(f"Entry title             : {entry_title}")
            selected_quality = format_nm3u8dl_candidate_quality(source)
            if _nm3u8dl_has_quality_evidence(source):
                log(
                    f"Selected quality        : {selected_quality}"
                )
            log(
                f"Authorization expires   : "
                f"{format_nm3u8dl_expiry_with_source(expiry, expiry_source)}"
            )
            log(
                f"Time remaining          : "
                f"{fmt_hms(remaining) if remaining is not None else 'unknown'}"
            )

            if (
                remaining is not None
                and remaining
                < int(NM3U8DL_NEW_SOURCE_MIN_REMAINING_MIN * 60)
            ):
                log(
                    f"Source join policy      : no "
                    f"{NM3U8DL_NEW_SOURCE_MIN_REMAINING_MIN}-minute "
                    "candidate available → using best shorter-lived "
                    "source to preserve continuity",
                    level="WARN",
                )

            if renewal_mode == "EXPIRY_ROLLOVER":
                if expiry is not None:
                    log(
                        f"Overtime allowance      : "
                        f"{safe_overtime_min} minutes"
                    )
                    log(
                        f"Running renewal target  : "
                        f"{running_target_local}"
                    )
                    log(
                        f"Normal playlist check   : every "
                        f"{NM3U8DL_PLAYLIST_CHECK_INTERVAL_MIN} minutes"
                    )
                    log(
                        f"Risk playlist check     : every "
                        f"{NM3U8DL_PLAYLIST_RISK_CHECK_INTERVAL_SEC} seconds"
                    )
                else:
                    log(
                        "Authorization renewal   : "
                        "disabled for this run (expiry unknown)"
                    )
                    log(
                        "Failure recovery        : "
                        "resolve fresh source on restart"
                    )

            else:
                log(
                    "Renewal behavior         : "
                    "resolve fresh source on restart"
                )
                log(
                    "Background renewal       : disabled"
                )

            quality_upgrade_profile_enabled = bool(
                profile.get("quality_upgrade_enabled", False)
            )
            quality_upgrade_target_fps = float(
                profile.get("quality_upgrade_target_fps", 50)
            )
            running_motion_fps = _nm3u8dl_ranking_motion_fps(source)

            # quality upgrade logic reverted by sumit 21st sep 2026
            #if quality_upgrade_profile_enabled:
            #    log(
            #        f"Quality upgrade check  : every "
            #        f"{float(profile.get('quality_upgrade_check_min', 5)):g} "
            #        f"minutes"
            #    )
            #    log(
            #        f"Quality upgrade target : "
            #        f"{quality_upgrade_target_fps:g} fps"
            #    )
            #    log(
            #        f"Quality upgrade minimum: "
            #        f"{int(profile.get('quality_upgrade_min_remaining_min', 15))} "
            #        f"minutes when expiry is known"
            #    )
            event_quality_cutoff_reached = (
                get_nm3u8dl_playlist_lifecycle() == "EVENT"
                and _nm3u8dl_video_resolution_class(source) >= 1080
                and running_motion_fps >= quality_upgrade_target_fps
            )

            if (
                quality_upgrade_profile_enabled
                and not event_quality_cutoff_reached
            ):
                log(
                    f"Quality upgrade check  : every "
                    f"{float(profile.get('quality_upgrade_check_min', 5)):g} "
                    f"minutes"
                )
                log(
                    f"Quality upgrade target : "
                    f"{quality_upgrade_target_fps:g} fps"
                )
                log(
                    f"Quality upgrade minimum: "
                    f"{int(profile.get('quality_upgrade_min_remaining_min', 15))} "
                    f"minutes when expiry is known"
                )

            elif (
                quality_upgrade_profile_enabled
                and event_quality_cutoff_reached
            ):
                log(
                    f"Quality upgrade check   : disabled "
                    f"(event already at "
                    f"{int(source.get('video_width') or 0)}x"
                    f"{int(source.get('video_height') or 0)} "
                    f"{running_motion_fps:g} fps)"
                )
                
            target_quality_recovery_attempt = (
                state.nm3u8dl_access_block_consecutive > 0
                and state.nm3u8dl_access_block_purpose == "quality"
                and quality_upgrade_profile_enabled
                and running_motion_fps >= quality_upgrade_target_fps
            )

            if (
                state.nm3u8dl_access_block_consecutive > 0
                and not target_quality_recovery_attempt
            ):
                log(
                    f"Access/VPN recheck      : every "
                    f"{NM3U8DL_PLAYLIST_ACCESS_CHECK_INTERVAL_SEC} seconds "
                    f"until cleared"
                )

            log("=============================")

            _nm3u8dl_set_running_stream_identity(state, source)
            state.nm3u8dl_renewal_rollover_requested = False
            state.nm3u8dl_rollover_reason = None
            set_terminal_activity_context(None)
            return source

        stale_by = int(now - expiry)
        expiry_local = datetime.fromtimestamp(expiry).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        if connectivity_failure_scan:
            consecutive_connectivity_failures += 1
        else:
            if (
                getattr(state, "shared_alarm_type", None)
                == "playlist_connectivity"
            ):
                stop_source_wait_alarm("source_access_recovered")

            consecutive_connectivity_failures = 0
            state.nm3u8dl_playlist_connectivity_alarm_silenced = False

        expired_wait_signature = (
            connectivity_failure_scan,
            source["playlist_group"],
            source["match_description"],
            tuple(
                (
                    result.get("playlist_url"),
                    result.get("status"),
                    result.get("detail"),
                    result.get("expiry"),
                    result.get("best_candidate_expiry"),
                )
                for result in source["source_results"]
            ),
            expiry,
            tuple(source["source_errors"]),
        )

        if expired_wait_signature == last_expired_wait_signature:
            log(
                f"Still waiting for playlist source — no change; "
                f"next check in "
                f"{NM3U8DL_PLAYLIST_RISK_CHECK_INTERVAL_SEC}s",
                level="WARN",
            )

        else:
            record_playlist_history_scan(
                state,
                source.get("_history_scan"),
                scan_type=(
                    "recovery_wait"
                    if source_wait_is_recovery
                    else "startup_wait"
                ),
                scan_reason="resolved source authorization already expired",
                heading="=== DYNAMIC SOURCE STATUS ===",
                section_level="WARN",
                context={
                    "expired_at": expiry_local,
                    "stale_by_seconds": stale_by,
                },
            )

            log_nm3u8dl_playlist_scan_results(
                source_results=source["source_results"],
                source_errors=source["source_errors"],
                playlist_group=source["playlist_group"],
                match_description=source["match_description"],
                playlist_source_count=source["playlist_source_count"],
                candidate_count=source["candidate_count"],
            )

            log("=============================")
            log("")

            if connectivity_failure_scan:
                log(
                    "Recovery source access is failing → possible "
                    "connectivity/DNS/VPN/source-access problem.",
                    level="WARN",
                )
            else:
                log(
                    f"Playlist authorization expired at {expiry_local} "
                    f"({stale_by}s ago) → waiting for playlist refresh...",
                    level="WARN",
                )

            last_expired_wait_signature = expired_wait_signature

        if connectivity_failure_scan:
            log(
                f"Connectivity recovery scan: "
                f"{min(consecutive_connectivity_failures, 2)}/2",
                level="WARN",
            )

        if source_wait_deadline_reached():
            return None

        if (
            connectivity_failure_scan
            and consecutive_connectivity_failures >= 2
            and not state.nm3u8dl_playlist_connectivity_alarm_silenced
            and not shared_alarm_is_active(state)
        ):
            log(
                "Playlist/source access failed on two consecutive "
                "recovery scans → possible "
                "connectivity/DNS/VPN/source-access problem.",
                level="WARN",
            )
            shared_alarm_start(
                state,
                alarm_type="playlist_connectivity",
                incident=None,
            )

        sleep_with_interrupts(
            state,
            NM3U8DL_PLAYLIST_RISK_CHECK_INTERVAL_SEC,
        )

        if state.stop_flag:
            stop_source_wait_alarm("stop")
            return None
        
def _nm3u8dl_regex_exact(value: str) -> str:
    return "^" + re.escape(str(value or "")) + "$"


def _nm3u8dl_frame_rate_regex(fps: float) -> str:
    fps_text = f"{float(fps):.6f}".rstrip("0").rstrip(".")
    escaped = re.escape(fps_text)
    if "." in fps_text:
        return f"^{escaped}(0+)?$"
    return rf"^{escaped}(\.0+)?$"


def _build_nm3u8dl_selected_video_filter(source: dict) -> str:
    """Build N_m3u8DL selection for the video representation already ranked."""
    stream_type = str(source.get("stream_type") or "").strip().upper()

    if stream_type == "DASH":
        representation_id = str(
            source.get("_dash_representation_id") or ""
        ).strip()
        # N_m3u8DL uses ':' as the option delimiter. Common DASH IDs are safe to
        # target directly; unusual colon-bearing IDs use the composite identity.
        if representation_id and ":" not in representation_id:
            return f"id={_nm3u8dl_regex_exact(representation_id)}:for=best"

    parts = []
    width = int(source.get("video_width") or 0)
    height = int(source.get("video_height") or 0)
    fps = float(source.get("video_fps") or 0.0)

    if width > 0 and height > 0:
        parts.append(f"res={_nm3u8dl_regex_exact(f'{width}x{height}')}")

    if fps > 0:
        parts.append(f"frame={_nm3u8dl_frame_rate_regex(fps)}")

    if stream_type == "DASH":
        codecs = str(source.get("_dash_codecs") or "").strip()
        bandwidth_bps = int(
            source.get("_dash_representation_bandwidth")
            or source.get("video_bitrate_bps")
            or 0
        )
    else:
        hls_codecs = str(source.get("_hls_codecs") or "").strip()
        hls_codec_tokens = [
            token.strip()
            for token in hls_codecs.split(",")
            if token.strip()
        ]
        codecs = next(
            (
                token
                for token in hls_codec_tokens
                if token.casefold().startswith(
                    ("avc1", "avc3", "hev1", "hvc1", "vp09", "av01")
                )
            ),
            "",
        )
        bandwidth_bps = int(
            source.get("_hls_average_bandwidth_bps")
            or source.get("video_bitrate_bps")
            or source.get("_hls_bandwidth_bps")
            or 0
        )

    if codecs and ":" not in codecs:
        parts.append(f"codecs={_nm3u8dl_regex_exact(codecs)}")

    if bandwidth_bps > 0:
        bandwidth_kbps = max(1, int(round(bandwidth_bps / 1000.0)))
        parts.append(f"bwMin={max(1, bandwidth_kbps - 1)}")
        parts.append(f"bwMax={bandwidth_kbps + 1}")

    if not parts:
        return "best"

    parts.append("for=best")
    return ":".join(parts)

# Reverted back: wanted to send the same quality selected to nm3u8dl, edge case best/best can pick a different variant
#def _get_nm3u8dl_part_b_for_launch(state: RecorderState) -> str:
#    """Keep static mode unchanged; dynamic mode pins the already-ranked video."""
#    if NM3U8DL_SOURCE_MODE != "playlist":
#        return NM3U8DL_PART_B
#
#    source = getattr(state, "nm3u8dl_running_source", None) or {}
#    video_filter = _build_nm3u8dl_selected_video_filter(source)
#    return re.sub(
#        r"--select-video\s+best\b",
#        f'--select-video "{video_filter}"',
#        NM3U8DL_PART_B,
#        count=1,
#    )


def _get_nm3u8dl_part_b_for_launch(state: RecorderState) -> str:
    """Use the configured N_m3u8DL selection unchanged."""
    return NM3U8DL_PART_B
    

def get_nm3u8dl_part_a(
    state: RecorderState
) -> Optional[str]:

    if NM3U8DL_SOURCE_MODE == "static":
        return NM3U8DL_PART_A

    if NM3U8DL_SOURCE_MODE != "playlist":
        raise RuntimeError(
            f'Unknown NM3U8DL_SOURCE_MODE: "{NM3U8DL_SOURCE_MODE}"'
        )

    profile = get_nm3u8dl_playlist_profile()

    source = resolve_nm3u8dl_launch_source(state)

    if source is None:
        return None

    headers = get_nm3u8dl_effective_headers(
        source["headers"],
        emit_logs=True,
    )

    headers = get_nm3u8dl_ascii_safe_request_headers(
        headers,
        emit_logs=True,
    )

    resolved_keys = resolve_nm3u8dl_source_keys(
        source,
        headers,
    )

    parts = [
        "N_m3u8DL-RE",
        f'"{source["stream_url"]}"',
    ]

    for name, value in headers.items():
        parts.append(f'-H "{name}: {value}"')

    for key in resolved_keys:
        parts.append(f"--key {key}")

    if resolved_keys:
        key_mode = profile["key_mode"]

        if key_mode == "SHAKA":
            parts.append("--use-shaka-packager")

        elif key_mode == "MP4DECRYPT":
            parts.append("--decryption-engine MP4DECRYPT")

        elif key_mode == "NONE":
            pass

        else:
            raise RuntimeError(
                f'Unknown key mode: "{key_mode}"'
            )

    extra_args = profile["extra_args"].strip()

    if extra_args:
        parts.append(extra_args)

    return " ".join(parts)
    
def get_nm3u8dl_renewal_targets(
    running_expiry: Optional[int],
    replacement_expiry: Optional[int],
) -> dict:
    
    safe_overtime_min = (
        get_nm3u8dl_playlist_profile()["safe_overtime_min"]
    )
    
    running_target = None
    replacement_safe_deadline = None
    planned_rollover = None

    if running_expiry is not None:
        running_target = (
            running_expiry
            + int(safe_overtime_min * 60)
        )

    if replacement_expiry is not None:
        replacement_safe_deadline = (
            replacement_expiry
            - int(NM3U8DL_REPLACEMENT_SAFETY_MARGIN_MIN * 60)
        )

    targets = [
        ts
        for ts in (running_target, replacement_safe_deadline)
        if ts is not None
    ]

    if targets:
        planned_rollover = min(targets)

    return {
        "running_target": running_target,
        "replacement_safe_deadline": replacement_safe_deadline,
        "planned_rollover": planned_rollover,
    }

def is_nm3u8dl_usable_replacement(
    running_expiry: Optional[int],
    candidate_url: str,
    candidate_expiry: Optional[int],
    now_ts: Optional[float] = None,
) -> bool:

    now = time.time() if now_ts is None else float(now_ts)

    if not candidate_url or candidate_expiry is None:
        return False

    # A replacement must still be alive now and must extend authorization
    # beyond the currently running source. Short-lived replacements remain
    # usable fallbacks when nothing safer exists.
    if float(candidate_expiry) <= now:
        return False

    if (
        running_expiry is not None
        and candidate_expiry <= running_expiry
    ):
        return False

    return True


def is_nm3u8dl_stable_replacement(
    running_expiry: Optional[int],
    candidate_expiry: Optional[int],
) -> bool:
    if candidate_expiry is None:
        return False

    planned_rollover = get_nm3u8dl_renewal_targets(
        running_expiry,
        candidate_expiry,
    )["planned_rollover"]

    if planned_rollover is None:
        return False

    remaining_at_rollover = (
        float(candidate_expiry) - float(planned_rollover)
    )

    return remaining_at_rollover >= int(
        NM3U8DL_NEW_SOURCE_MIN_REMAINING_MIN * 60
    )


def get_nm3u8dl_quality_upgrade_selection_decision(
    running_source: dict,
    candidates: List[dict],
    target_fps: float,
    min_remaining_min: int,
    allow_unknown_expiry: bool = False,
    now_ts: Optional[float] = None,
) -> SelectionDecision:
    if not running_source:
        return SelectionDecision(
            selected=None,
            selected_index=None,
            decision_type="no_upgrade",
            reason="no running source supplied",
            fallback_used=False,
            considered_count=len(candidates),
            eligible_count=0,
        )

    return source_selection.select_quality_upgrade(
        SourceCandidate.from_mapping(running_source),
        [SourceCandidate.from_mapping(candidate) for candidate in candidates],
        target_fps,
        _get_nm3u8dl_selection_policy(
            upgrade_min_remaining_min=min_remaining_min,
            allow_unknown_expiry=allow_unknown_expiry,
        ),
        now_ts=now_ts,
    )


def get_nm3u8dl_quality_upgrade_candidate(
    running_source: dict,
    candidates: List[dict],
    target_fps: float,
    min_remaining_min: int,
    allow_unknown_expiry: bool = False,
    now_ts: Optional[float] = None,
) -> Optional[dict]:
    decision = get_nm3u8dl_quality_upgrade_selection_decision(
        running_source,
        candidates,
        target_fps,
        min_remaining_min,
        allow_unknown_expiry=allow_unknown_expiry,
        now_ts=now_ts,
    )
    if decision.selected_index is None:
        return None
    return dict(candidates[decision.selected_index])


def get_nm3u8dl_renewal_state(
    running_expiry: Optional[int],
    replacement_expiry: Optional[int],
    has_safe_replacement: bool,
    now_ts: Optional[float] = None,
) -> dict:

    now = time.time() if now_ts is None else float(now_ts)

    targets = get_nm3u8dl_renewal_targets(
        running_expiry,
        replacement_expiry if has_safe_replacement else None,
    )

    running_target = targets["running_target"]
    planned_rollover = targets["planned_rollover"]

    is_overtime = (
        running_expiry is not None
        and now >= running_expiry
    )

    rollover_due = (
        planned_rollover is not None
        and now >= planned_rollover
    )

    if rollover_due and has_safe_replacement:
        state = "rollover_due"

    elif rollover_due and not has_safe_replacement:
        state = "renewal_at_risk"

    elif has_safe_replacement:
        state = "standby_ready"

    elif is_overtime:
        state = "overtime"

    else:
        state = "normal"

    return {
        "state": state,
        "is_overtime": is_overtime,
        "running_target": running_target,
        "replacement_safe_deadline": targets["replacement_safe_deadline"],
        "planned_rollover": planned_rollover,
    }

def log_nm3u8dl_playlist_renewal_check(
    checked_at_ts: float,
    source_results: List[dict],
    running_expiry: Optional[int],
    candidate: Optional[dict],
    retained_source: Optional[dict],
    decision: str,
    next_check_ts: Optional[float],
    decision_level: str = "INFO",
    replacement_locked: bool = False,
):
    def format_timestamp(timestamp: Optional[float]) -> str:
        if timestamp is None:
            return "unknown"

        return datetime.fromtimestamp(
            float(timestamp)
        ).strftime("%Y-%m-%d %H:%M:%S")

    metadata_source = candidate or retained_source or {}

    log_nm3u8dl_playlist_scan_results(
        source_results=source_results,
        source_errors=list(
            metadata_source.get("source_errors") or []
        ),
        playlist_group=(
            metadata_source.get("playlist_group")
            or NM3U8DL_PLAYLIST_GROUP.strip().upper()
        ),
        match_description=(
            metadata_source.get("match_description")
            or get_nm3u8dl_playlist_match_description()
        ),
        playlist_source_count=int(
            metadata_source.get("playlist_source_count")
            or len(source_results)
        ),
        candidate_count=int(
            metadata_source.get("candidate_count") or 0
        ),
        selected_playlist_url=(
            retained_source.get("playlist_url")
            if retained_source
            else None
        ),
        selected_tag_label="RETAINED",
        heading="=== PLAYLIST RENEWAL CHECK ===",
    )

    running_target = get_nm3u8dl_renewal_targets(
        running_expiry,
        None,
    )["running_target"]

    log("")
    log(
        f"{'Checked at':<24}: "
        f"{format_timestamp(checked_at_ts)}"
    )
    log(
        f"{'Running authorization':<24}: expires "
        f"{format_timestamp(running_expiry)}"
    )
    log(
        f"{'Running renewal target':<24}: "
        f"{format_timestamp(running_target)}"
    )

    if candidate:
        log(
            f"{'Best candidate':<24}: "
            f"{candidate['playlist_url']}"
        )
        log(
            f"{'Best candidate quality':<24}: "
            f"{format_nm3u8dl_candidate_quality(candidate)}"
        )
        log(
            f"{'Best candidate expires':<24}: "
            f"{format_timestamp(candidate.get('expiry'))}"
        )
    else:
        log(f"{'Best candidate':<24}: none")

    if retained_source:
        retained_expiry = retained_source.get("expiry")
        planned_rollover = get_nm3u8dl_renewal_targets(
            running_expiry,
            retained_expiry,
        )["planned_rollover"]

        if (
            retained_expiry is not None
            and planned_rollover is not None
        ):
            remaining_at_rollover = fmt_hms(
                max(
                    0,
                    int(
                        float(retained_expiry)
                        - float(planned_rollover)
                    ),
                )
            )
        else:
            remaining_at_rollover = "unknown"

        log(
            f"{'Retained replacement':<24}: "
            f"{retained_source['playlist_url']}"
        )
        log(
            f"{'Replacement quality':<24}: "
            f"{format_nm3u8dl_candidate_quality(retained_source)}"
        )
        log(
            f"{'Replacement expires':<24}: "
            f"{format_timestamp(retained_expiry)}"
        )
        log(
            f"{'Replacement status':<24}: "
            f"{'LOCKED' if replacement_locked else 'FALLBACK'} — "
            f"{remaining_at_rollover} remains at rollover"
        )
        log(
            f"{'Planned rollover':<24}: "
            f"{format_timestamp(planned_rollover)}"
        )
    else:
        log(f"{'Retained replacement':<24}: none")

    log(
        f"{'Decision':<24}: {decision}",
        level=decision_level,
    )

    if replacement_locked and next_check_ts is None:
        next_check_text = (
            "not needed — retained replacement locked"
        )
    elif next_check_ts is None:
        next_check_text = "not scheduled"
    else:
        next_check_text = format_timestamp(next_check_ts)

    log(
        f"{'Next playlist check':<24}: "
        f"{next_check_text}"
    )
    log("=============================")
    log("")


def monitor_nm3u8dl_playlist_renewal(
    state: RecorderState,
    stop_event: threading.Event,
):
    if (
        DOWNLOAD_MODE != ENGINE_NM3U8DL
        or NM3U8DL_SOURCE_MODE != "playlist"
    ):
        return

    profile = get_nm3u8dl_playlist_profile()

    renewal_mode = profile.get(
        "renewal_mode",
        "EXPIRY_ROLLOVER",
    )

    quality_upgrade_profile_enabled = bool(
        profile.get("quality_upgrade_enabled", False)
    )
    allow_unknown_expiry = bool(
        profile.get("allow_unknown_expiry", False)
    )

    running_source = state.nm3u8dl_running_source

    if not running_source:
        return

    running_expiry = running_source.get("expiry")

    authorization_renewal_enabled = (
        renewal_mode == "EXPIRY_ROLLOVER"
        and running_expiry is not None
    )

    running_motion_fps = _nm3u8dl_ranking_motion_fps(running_source)

    quality_upgrade_target_fps = float(
        profile.get("quality_upgrade_target_fps", 50)
    )

    # KNOWN PHASE-2 LIMIT: the target remains 50 motion and 60+ is deferred.
    # Reaching 50 motion no longer disables periodic quality scans, because the
    # recorder may still improve within that class (for example 720p50 ->
    # 1080p50). Candidate acceptance still requires target motion plus a better
    # full quality rank.
    # quality_upgrade_enabled = quality_upgrade_profile_enabled
    
    # EVENT quality cutoff:
    # keep looking until 1080p50 is reached, then stop quality-upgrade scans.
    # LINEAR_TV keeps its existing behavior unchanged.
    event_quality_cutoff_reached = (
        get_nm3u8dl_playlist_lifecycle() == "EVENT"
        and _nm3u8dl_video_resolution_class(running_source) >= 1080
        and running_motion_fps >= quality_upgrade_target_fps
    )

    quality_upgrade_enabled = (
        quality_upgrade_profile_enabled
        and not event_quality_cutoff_reached
    )

    target_quality_recovery_attempt = (
        state.nm3u8dl_access_block_consecutive > 0
        and state.nm3u8dl_access_block_purpose == "quality"
        and quality_upgrade_profile_enabled
        and running_motion_fps >= quality_upgrade_target_fps
    )

    access_block_monitor_enabled = (
        state.nm3u8dl_access_block_consecutive > 0
        and not target_quality_recovery_attempt
    )

    if (
        not authorization_renewal_enabled
        and not quality_upgrade_enabled
        and not access_block_monitor_enabled
    ):
        return

    normal_interval_sec = int(
        NM3U8DL_PLAYLIST_CHECK_INTERVAL_MIN * 60
    )

    risk_interval_sec = int(
        NM3U8DL_PLAYLIST_RISK_CHECK_INTERVAL_SEC
    )

    access_check_interval_sec = int(
        NM3U8DL_PLAYLIST_ACCESS_CHECK_INTERVAL_SEC
    )

    quality_check_interval_min = float(
        profile.get("quality_upgrade_check_min", 5)
    )

    quality_check_interval_sec = max(
        1,
        int(quality_check_interval_min * 60),
    )

    quality_min_remaining_min = int(
        profile.get(
            "quality_upgrade_min_remaining_min",
            15,
        )
    )

    now = time.time()

    running_target = None
    normal_check_start_ts = None
    risk_check_start_ts = None
    next_renewal_check_ts = None

    if authorization_renewal_enabled:
        renewal_targets = get_nm3u8dl_renewal_targets(
            running_expiry,
            None,
        )

        running_target = renewal_targets["running_target"]

        normal_check_start_ts = (
            float(running_target)
            - int(NM3U8DL_PLAYLIST_RENEWAL_LEAD_MIN * 60)
        )

        risk_check_start_ts = (
            float(running_target)
            - int(NM3U8DL_REPLACEMENT_SAFETY_MARGIN_MIN * 60)
        )

        if now >= risk_check_start_ts:
            next_renewal_check_ts = now

        elif now >= normal_check_start_ts:
            next_renewal_check_ts = min(
                now + normal_interval_sec,
                risk_check_start_ts,
            )

        else:
            next_renewal_check_ts = normal_check_start_ts

    next_quality_check_ts = (
        now + quality_check_interval_sec
        if quality_upgrade_enabled
        else None
    )

    if access_block_monitor_enabled:
        first_access_block_ts = (
            state.nm3u8dl_access_block_detected_ts
            if state.nm3u8dl_access_block_detected_ts is not None
            else now
        )
        next_access_block_check_ts = max(
            now,
            float(first_access_block_ts) + access_check_interval_sec,
        )
    else:
        next_access_block_check_ts = None

    overtime_logged = False
    risk_logged = False

    def format_local_timestamp(timestamp: Optional[float]) -> str:
        if timestamp is None:
            return "disabled"

        return datetime.fromtimestamp(
            float(timestamp)
        ).strftime("%Y-%m-%d %H:%M:%S")

    def calculate_next_renewal_check(
        check_completed_ts: float
    ) -> Optional[float]:
        if not authorization_renewal_enabled:
            return None

        if check_completed_ts >= risk_check_start_ts:
            next_ts = (
                check_completed_ts + risk_interval_sec
            )

            if check_completed_ts < running_target:
                next_ts = min(
                    next_ts,
                    float(running_target),
                )

            return next_ts

        return min(
            check_completed_ts + normal_interval_sec,
            risk_check_start_ts,
        )

    def log_quality_upgrade_found(
        upgrade_candidate: dict,
        check_time: float,
    ):
        upgrade_expiry = upgrade_candidate.get("expiry")

        if upgrade_expiry is None:
            upgrade_expiry_text = "unknown"
            upgrade_remaining_text = "unknown"
            minimum_required_text = (
                "waived (unknown expiry allowed by profile)"
            )
        else:
            upgrade_expiry_text = format_local_timestamp(
                upgrade_expiry
            )
            upgrade_remaining_text = fmt_hms(
                max(
                    0,
                    int(float(upgrade_expiry) - check_time),
                )
            )
            minimum_required_text = (
                f"{quality_min_remaining_min} minutes"
            )

        log("")
        log("=== QUALITY UPGRADE FOUND ===")
        log(
            f"Running quality           : "
            f"{format_nm3u8dl_candidate_quality(running_source)}"
        )
        log(
            f"Upgrade quality           : "
            f"{format_nm3u8dl_candidate_quality(upgrade_candidate)}"
        )
        log(
            f"Upgrade source            : "
            f"{upgrade_candidate['playlist_url']}"
        )
        log(
            f"Upgrade expires           : "
            f"{upgrade_expiry_text}"
        )
        log(
            f"Time remaining            : "
            f"{upgrade_remaining_text}"
        )
        log(
            f"Minimum required          : "
            f"{minimum_required_text}"
        )
        log(
            "Decision                  : "
            "controlled quality-upgrade rollover"
        )
        log("=============================")
        log("")

    def record_runtime_scan_history(
        snapshot: Optional[dict],
        *,
        authorization_due: bool,
        quality_due: bool,
        access_changed: bool,
    ):
        if access_changed:
            scan_type = "access_vpn"
            scan_reason = "access/VPN verification state changed"
            heading = "=== ACCESS/VPN RECHECK RESULTS ==="
        elif authorization_due and quality_due:
            scan_type = "authorization+quality"
            scan_reason = "scheduled authorization and quality check"
            heading = "=== PLAYLIST RENEWAL CHECK ==="
        elif authorization_due:
            scan_type = "authorization"
            scan_reason = "scheduled authorization check"
            heading = "=== PLAYLIST RENEWAL CHECK ==="
        elif quality_due:
            scan_type = "quality"
            scan_reason = "scheduled quality check"
            heading = "=== DYNAMIC SOURCE STATUS ==="
        else:
            return

        record_playlist_history_scan(
            state,
            snapshot,
            scan_type=scan_type,
            scan_reason=scan_reason,
            heading=heading,
            context={
                "running_quality": format_nm3u8dl_candidate_quality(running_source),
                "running_expiry": format_nm3u8dl_expiry_with_source(
                    running_expiry,
                    running_source.get("expiry_source") or "",
                ),
            },
        )

    if authorization_renewal_enabled:
        log(
            f"Authorization renewal monitor started → "
            f"first check at "
            f"{format_local_timestamp(next_renewal_check_ts)}"
        )
        log(
            f"Authorization risk checks begin → "
            f"{format_local_timestamp(risk_check_start_ts)} "
            f"(every {risk_interval_sec} seconds)"
        )
    
    if access_block_monitor_enabled:
        log(
            f"Access/VPN monitor enabled → "
            f"next confirmation scan in {access_check_interval_sec} seconds"
        )
        
    if quality_upgrade_enabled:
        log(
            f"Quality upgrade monitor enabled → "
            f"every {quality_check_interval_min:g} minutes"
        )

        if allow_unknown_expiry:
            log(
                f"Quality upgrade expiry rule → "
                f"{quality_min_remaining_min} minutes if expiry is known; "
                f"unknown expiry allowed"
            )
        else:
            log(
                f"Quality upgrade minimum   → "
                f"{quality_min_remaining_min} minutes authorization remaining"
            )

        log(
            f"First quality check       → "
            f"{format_local_timestamp(next_quality_check_ts)}"
        )

    log("")

    while not stop_event.is_set():
        if state.stop_flag:
            return

        now = time.time()

        pending_source = state.nm3u8dl_pending_source
        pending_expiry = (
            pending_source.get("expiry")
            if pending_source
            else None
        )

        if (
            pending_source is not None
            and (
                pending_expiry is None
                or pending_expiry <= now
            )
        ):
            log(
                "Retained authorization replacement expired before "
                "rollover → discarding it...",
                level="WARN",
            )

            state.nm3u8dl_pending_source = None
            pending_source = None
            pending_expiry = None

            if (
                authorization_renewal_enabled
                and next_renewal_check_ts is None
            ):
                next_renewal_check_ts = now

        renewal_state = None

        if authorization_renewal_enabled:
            renewal_state = get_nm3u8dl_renewal_state(
                running_expiry,
                pending_expiry,
                pending_source is not None,
                now_ts=now,
            )

            if (
                renewal_state["state"] == "rollover_due"
                and pending_source is not None
            ):
                log(
                    "Authorization rollover time reached → "
                    "requesting a new chunk with the retained replacement..."
                )

                state.nm3u8dl_rollover_reason = "authorization"
                state.nm3u8dl_renewal_rollover_requested = True
                return

            if (
                renewal_state["state"] == "overtime"
                and not overtime_logged
            ):
                log(
                    "Current authorization expiry reached → recording "
                    f"continues while replacement checks increase to every "
                    f"{risk_interval_sec} seconds.",
                    level="WARN",
                )

                overtime_logged = True

            if (
                renewal_state["state"] == "renewal_at_risk"
                and not risk_logged
            ):
                log(
                    "Renewal target reached with no safe replacement → "
                    "current recording remains uninterrupted while checking "
                    f"every {risk_interval_sec} seconds.",
                    level="WARN",
                )

                risk_logged = True

        wake_candidates = []

        if next_renewal_check_ts is not None:
            wake_candidates.append(
                float(next_renewal_check_ts)
            )

        if next_quality_check_ts is not None:
            wake_candidates.append(
                float(next_quality_check_ts)
            )

        if next_access_block_check_ts is not None:
            wake_candidates.append(
                float(next_access_block_check_ts)
            )

        if (
            authorization_renewal_enabled
            and pending_source is not None
            and renewal_state is not None
            and renewal_state["planned_rollover"] is not None
        ):
            wake_candidates.append(
                float(renewal_state["planned_rollover"])
            )

        if not wake_candidates:
            return

        wake_ts = min(wake_candidates)
        wait_seconds = max(0.0, wake_ts - now)

        if wait_seconds > 0:
            if stop_event.wait(wait_seconds):
                return

            continue

        due_time = time.time()

        authorization_check_due = (
            authorization_renewal_enabled
            and next_renewal_check_ts is not None
            and due_time >= next_renewal_check_ts
        )

        quality_check_due = (
            quality_upgrade_enabled
            and next_quality_check_ts is not None
            and due_time >= next_quality_check_ts
        )

        access_block_check_due = (
            next_access_block_check_ts is not None
            and due_time >= next_access_block_check_ts
        )

        if (
            not authorization_check_due
            and not quality_check_due
            and not access_block_check_due
        ):
            continue

        candidate = None
        source_results = []
        decision = ""
        decision_level = "INFO"
        quality_decision = None
        quality_decision_level = "INFO"

        full_scan_due = (
            authorization_check_due
            or quality_check_due
        )

        targeted_access_scan = (
            access_block_check_due
            and not full_scan_due
            and bool(state.nm3u8dl_access_block_playlist_urls)
        )
        history_authorization_due = bool(authorization_check_due)
        history_quality_due = bool(quality_check_due)

        previous_access_block_urls = list(
            state.nm3u8dl_access_block_playlist_urls
        )
        previous_access_block_snapshot = dict(
            state.nm3u8dl_access_block_status_snapshot or {}
        )

        # Every monitor pass is a new logical activity instance. The resolver's
        # live progress, phase completions, and final VPN/quality decision all
        # stay in this same block unless another activity interrupts them.
        set_terminal_activity_context(
            new_terminal_activity("dynamic_playlist_scan")
        )

        try:
            if targeted_access_scan:
                # The first pass is authoritative when access state is unchanged.
                # Only a changed/failed pass gets one complete verification pass,
                # protecting against a VPN/route change occurring mid-scan without
                # doubling the work of every routine 30-second recheck.
                first_pass_resolution = None
                first_pass_changed = True

                try:
                    first_pass_resolution = resolve_nm3u8dl_playlist_source(
                        include_candidate_pool=True,
                        playlist_urls_override=previous_access_block_urls,
                        progress_stage="checking blocked playlist",
                        progress_completion_label=(
                            "blocked-source scan complete"
                        ),
                        state=state,
                        stop_event=stop_event,
                        show_progress=False,
                    )

                    first_pass_snapshot = (
                        get_nm3u8dl_access_recheck_snapshot(
                            previous_access_block_urls,
                            first_pass_resolution.get(
                                "source_results",
                                [],
                            ),
                        )
                    )

                    previous_snapshot = dict(
                        getattr(
                            state,
                            "nm3u8dl_access_block_status_snapshot",
                            {},
                        )
                        or {}
                    )

                    first_pass_changed = (
                        first_pass_snapshot != previous_snapshot
                    )

                except RuntimeError:
                    # A failed/partial first pass may itself be caused by a route
                    # transition, so verify the whole fixed incident set once more.
                    first_pass_changed = True

                if stop_event.is_set() or state.stop_flag:
                    return

                if first_pass_changed:
                    resolution = resolve_nm3u8dl_playlist_source(
                        include_candidate_pool=True,
                        playlist_urls_override=previous_access_block_urls,
                        progress_stage=(
                            "verifying changed blocked playlist"
                        ),
                        progress_completion_label=(
                            "blocked-source verification pass complete"
                        ),
                        state=state,
                        stop_event=stop_event,
                        show_progress=False,
                    )
                else:
                    resolution = first_pass_resolution
            else:
                resolution = resolve_nm3u8dl_playlist_source(
                    include_candidate_pool=True,
                    playlist_urls_override=None,
                    progress_stage="checking playlist",
                    progress_completion_label="playlist scan complete",
                    state=state,
                    stop_event=stop_event,
                    show_progress=False,
                )

        except RuntimeError as error:
            if stop_event.is_set() or state.stop_flag:
                return

            source_results = getattr(
                error,
                "source_results",
                [],
            )
            history_snapshot = getattr(error, "history_scan", None)
            access_history_changed = False

            if not targeted_access_scan:
                access_history_changed = (
                    _nm3u8dl_confirm_access_change_and_reset_failover(
                        state,
                        tracked_urls=previous_access_block_urls,
                        previous_snapshot=previous_access_block_snapshot,
                        first_pass_source_results=source_results,
                        stop_event=stop_event,
                    )
                )

            access_block_alarm_actionable = bool(
                getattr(
                    error,
                    "access_block_alarm_actionable",
                    False,
                )
            )
            access_block_alarm_quality = getattr(
                error,
                "access_block_alarm_quality",
                None,
            )

            if targeted_access_scan:
                access_history_changed = process_nm3u8dl_targeted_access_verification(
                    state,
                    tracked_urls=previous_access_block_urls,
                    source_results=source_results,
                    source_errors=getattr(error, "source_errors", []),
                    candidate_count=0,
                    playlist_group=NM3U8DL_PLAYLIST_GROUP.strip().upper(),
                    match_description=get_nm3u8dl_playlist_match_description(),
                    access_block_alarm_actionable=access_block_alarm_actionable,
                    access_block_alarm_quality=access_block_alarm_quality,
                    running_source=running_source,
                    access_check_interval_sec=access_check_interval_sec,
                )
            else:
                confirmed_blocked_urls = (
                    get_nm3u8dl_access_block_playlist_urls(
                        source_results
                    )
                )
                inconclusive_access_urls = (
                    get_nm3u8dl_inconclusive_access_recheck_urls(
                        previous_access_block_urls,
                        source_results,
                    )
                )

                tracked_blocked_urls = [
                    playlist_url
                    for playlist_url in previous_access_block_urls
                    if playlist_url in set(confirmed_blocked_urls)
                ]

                if access_block_alarm_actionable:
                    replace_incident_set = (
                        state.nm3u8dl_access_block_consecutive <= 0
                        or (
                            previous_access_block_urls
                            and not tracked_blocked_urls
                            and not inconclusive_access_urls
                        )
                    )

                    if replace_incident_set:
                        state.nm3u8dl_access_block_playlist_urls = list(
                            confirmed_blocked_urls
                        )
                        state.nm3u8dl_access_block_status_snapshot = (
                            get_nm3u8dl_access_recheck_snapshot(
                                confirmed_blocked_urls,
                                source_results,
                            )
                        )
                        state.nm3u8dl_access_block_purpose = (
                            "authorization"
                            if authorization_check_due
                            else "quality"
                        )
                    else:
                        state.nm3u8dl_access_block_playlist_urls = list(
                            previous_access_block_urls
                        )

                    update_nm3u8dl_access_block_alarm_state(
                        state,
                        actionable=True,
                        blocked_quality=access_block_alarm_quality,
                        running_quality=format_nm3u8dl_candidate_quality(
                            running_source
                        ),
                    )
                elif (
                    state.nm3u8dl_access_block_consecutive > 0
                    and (tracked_blocked_urls or inconclusive_access_urls)
                ):
                    state.nm3u8dl_access_block_playlist_urls = list(
                        previous_access_block_urls
                    )
                    log(
                        "Access/VPN status → PARTIAL / INCONCLUSIVE — one or more "
                        "tracked sources remain blocked or could not be verified; "
                        "incident remains active; recording continues.",
                        level="WARN",
                    )
                elif state.nm3u8dl_access_block_consecutive > 0:
                    update_nm3u8dl_access_block_alarm_state(
                        state,
                        actionable=False,
                        blocked_quality=None,
                        running_quality=format_nm3u8dl_candidate_quality(
                            running_source
                        ),
                    )

            check_completed_ts = time.time()
            next_access_block_check_ts = (
                check_completed_ts + access_check_interval_sec
                if state.nm3u8dl_access_block_consecutive > 0
                else None
            )

            # A targeted access/VPN recheck must never stand in for a full
            # authorization or five-minute quality scan. If either becomes due
            # while this targeted scan is running, leave its timer overdue so
            # the loop immediately follows with a full playlist scan.
            if targeted_access_scan:
                authorization_check_due = False
                quality_check_due = False
            else:
                authorization_check_due = (
                    authorization_renewal_enabled
                    and next_renewal_check_ts is not None
                    and check_completed_ts >= next_renewal_check_ts
                )

                quality_check_due = (
                    quality_upgrade_enabled
                    and next_quality_check_ts is not None
                    and check_completed_ts >= next_quality_check_ts
                )

            if quality_check_due:
                next_quality_check_ts = (
                    check_completed_ts
                    + quality_check_interval_sec
                )

            if authorization_check_due:
                next_renewal_check_ts = (
                    calculate_next_renewal_check(
                        check_completed_ts
                    )
                )

                decision = (
                    "no usable authorization found; "
                    "current recording continues"
                )
                decision_level = "WARN"

                log_nm3u8dl_playlist_renewal_check(
                    checked_at_ts=check_completed_ts,
                    source_results=source_results,
                    running_expiry=running_expiry,
                    candidate=None,
                    retained_source=state.nm3u8dl_pending_source,
                    decision=decision,
                    next_check_ts=next_renewal_check_ts,
                    decision_level=decision_level,
                )

            if quality_check_due:
                log("")
                log(
                    f"Quality upgrade check → no usable playlist "
                    f"candidate; current "
                    f"{format_nm3u8dl_candidate_quality(running_source)} "
                    f"continues; next check in "
                    f"{quality_check_interval_min:g} minutes.",
                    level="WARN",
                )
                log("")

            record_runtime_scan_history(
                history_snapshot,
                authorization_due=history_authorization_due,
                quality_due=history_quality_due,
                access_changed=access_history_changed,
            )

            continue

        if stop_event.is_set() or state.stop_flag:
            return

        source_results = resolution.get(
            "source_results",
            [],
        )

        access_block_alarm_actionable = bool(
            resolution.get("access_block_alarm_actionable", False)
        )

        candidate_pool = resolution.get(
            "_candidate_pool",
            [],
        )

        check_time = time.time()
        history_snapshot = resolution.get("_history_scan")
        access_history_changed = False

        if not targeted_access_scan:
            access_history_changed = (
                _nm3u8dl_confirm_access_change_and_reset_failover(
                    state,
                    tracked_urls=previous_access_block_urls,
                    previous_snapshot=previous_access_block_snapshot,
                    first_pass_source_results=source_results,
                    stop_event=stop_event,
                )
            )

        targeted_upgrade_candidate = None

        if targeted_access_scan and quality_upgrade_enabled:
            targeted_upgrade_candidate = (
                get_nm3u8dl_quality_upgrade_candidate(
                    running_source,
                    candidate_pool,
                    quality_upgrade_target_fps,
                    quality_min_remaining_min,
                    allow_unknown_expiry=allow_unknown_expiry,
                    now_ts=check_time,
                )
            )

            if targeted_upgrade_candidate is not None:
                log_quality_upgrade_found(
                    targeted_upgrade_candidate,
                    check_time,
                )

        if targeted_access_scan:
            access_history_changed = process_nm3u8dl_targeted_access_verification(
                state,
                tracked_urls=previous_access_block_urls,
                source_results=source_results,
                source_errors=resolution.get("source_errors", []),
                candidate_count=len(candidate_pool),
                playlist_group=resolution["playlist_group"],
                match_description=resolution["match_description"],
                access_block_alarm_actionable=access_block_alarm_actionable,
                access_block_alarm_quality=resolution.get(
                    "access_block_alarm_quality"
                ),
                running_source=running_source,
                access_check_interval_sec=access_check_interval_sec,
                selected_candidate=targeted_upgrade_candidate,
            )
        else:
            confirmed_blocked_urls = (
                get_nm3u8dl_access_block_playlist_urls(
                    source_results
                )
            )
            inconclusive_access_urls = (
                get_nm3u8dl_inconclusive_access_recheck_urls(
                    previous_access_block_urls,
                    source_results,
                )
            )

            tracked_blocked_urls = [
                playlist_url
                for playlist_url in previous_access_block_urls
                if playlist_url in set(confirmed_blocked_urls)
            ]

            if access_block_alarm_actionable:
                replace_incident_set = (
                    state.nm3u8dl_access_block_consecutive <= 0
                    or (
                        previous_access_block_urls
                        and not tracked_blocked_urls
                        and not inconclusive_access_urls
                    )
                )

                if replace_incident_set:
                    state.nm3u8dl_access_block_playlist_urls = list(
                        confirmed_blocked_urls
                    )
                    state.nm3u8dl_access_block_status_snapshot = (
                        get_nm3u8dl_access_recheck_snapshot(
                            confirmed_blocked_urls,
                            source_results,
                        )
                    )
                    state.nm3u8dl_access_block_purpose = (
                        "authorization"
                        if authorization_check_due
                        else "quality"
                    )
                else:
                    state.nm3u8dl_access_block_playlist_urls = list(
                        previous_access_block_urls
                    )

                update_nm3u8dl_access_block_alarm_state(
                    state,
                    actionable=True,
                    blocked_quality=resolution.get(
                        "access_block_alarm_quality"
                    ),
                    running_quality=format_nm3u8dl_candidate_quality(
                        running_source
                    ),
                )
            elif (
                state.nm3u8dl_access_block_consecutive > 0
                and (tracked_blocked_urls or inconclusive_access_urls)
            ):
                state.nm3u8dl_access_block_playlist_urls = list(
                    previous_access_block_urls
                )
                log(
                    "Access/VPN status → PARTIAL / INCONCLUSIVE — one or more "
                    "tracked sources remain blocked or could not be verified; "
                    "incident remains active; recording continues.",
                    level="WARN",
                )
            elif state.nm3u8dl_access_block_consecutive > 0:
                update_nm3u8dl_access_block_alarm_state(
                    state,
                    actionable=False,
                    blocked_quality=None,
                    running_quality=format_nm3u8dl_candidate_quality(
                        running_source
                    ),
                )

        record_runtime_scan_history(
            history_snapshot,
            authorization_due=history_authorization_due,
            quality_due=history_quality_due,
            access_changed=access_history_changed,
        )

        next_access_block_check_ts = (
            check_time + access_check_interval_sec
            if state.nm3u8dl_access_block_consecutive > 0
            else None
        )

        # A targeted access/VPN scan is allowed to trigger an immediate
        # recovered-quality rollover, but it must never satisfy a full
        # authorization or normal five-minute quality check. Those remain due
        # and will cause an immediate full scan on the next loop iteration.
        if targeted_access_scan:
            authorization_check_due = False
            quality_check_due = False
        else:
            authorization_check_due = (
                authorization_renewal_enabled
                and next_renewal_check_ts is not None
                and check_time >= next_renewal_check_ts
            )

            quality_check_due = (
                quality_upgrade_enabled
                and next_quality_check_ts is not None
                and check_time >= next_quality_check_ts
            )

        quality_evaluation_due = (
            quality_check_due
            or (access_block_check_due and quality_upgrade_enabled)
        )

        if quality_evaluation_due:
            upgrade_candidate = targeted_upgrade_candidate

            if upgrade_candidate is None:
                upgrade_candidate = (
                    get_nm3u8dl_quality_upgrade_candidate(
                        running_source,
                        candidate_pool,
                        quality_upgrade_target_fps,
                        quality_min_remaining_min,
                        allow_unknown_expiry=allow_unknown_expiry,
                        now_ts=check_time,
                    )
                )

            if upgrade_candidate is not None:
                for metadata_key in (
                    "candidate_count",
                    "playlist_source_count",
                    "playlist_group",
                    "match_description",
                    "source_errors",
                    "source_results",
                ):
                    upgrade_candidate[metadata_key] = (
                        resolution.get(metadata_key)
                    )

                if targeted_upgrade_candidate is None:
                    log_quality_upgrade_found(
                        upgrade_candidate,
                        check_time,
                    )

                state.nm3u8dl_pending_source = upgrade_candidate
                state.nm3u8dl_rollover_reason = "quality_upgrade"
                state.nm3u8dl_renewal_rollover_requested = True
                return

            running_rank = _nm3u8dl_video_quality_rank(
                running_source
            )
            running_preferred_score = int(
                running_source.get("preferred_qualifier_score") or 0
            )

            better_candidates = [
                pool_candidate
                for pool_candidate in candidate_pool
                if (
                    int(
                        pool_candidate.get("preferred_qualifier_score") or 0
                    ) >= running_preferred_score
                    and
                    _nm3u8dl_ranking_motion_fps(
                        pool_candidate
                    ) >= quality_upgrade_target_fps
                    and
                    _nm3u8dl_video_quality_rank(pool_candidate)
                    > running_rank
                )
            ]

            if better_candidates:
                best_better = max(
                    better_candidates,
                    key=get_nm3u8dl_candidate_quality_rank,
                )

                best_better_expiry = best_better.get("expiry")
                best_better_remaining = max(
                    0,
                    int(
                        float(best_better_expiry or 0)
                        - check_time
                    ),
                )

                quality_decision = (
                    f"better "
                    f"{format_nm3u8dl_candidate_quality(best_better)} "
                    f"source found, but only "
                    f"{fmt_hms(best_better_remaining)} remains "
                    f"(< {quality_min_remaining_min} minutes); "
                    f"keeping "
                    f"{format_nm3u8dl_candidate_quality(running_source)}"
                )
                quality_decision_level = "WARN"

            elif candidate_pool:
                best_available = max(
                    candidate_pool,
                    key=get_nm3u8dl_candidate_quality_rank,
                )

                quality_decision = (
                    f"no better source than running "
                    f"{format_nm3u8dl_candidate_quality(running_source)} "
                    f"(best available "
                    f"{format_nm3u8dl_candidate_quality(best_available)})"
                )

            else:
                quality_decision = (
                    f"no usable candidate; current "
                    f"{format_nm3u8dl_candidate_quality(running_source)} "
                    f"continues"
                )
                quality_decision_level = "WARN"

        if authorization_check_due:
            replacement_candidates = [
                pool_candidate
                for pool_candidate in candidate_pool
                if is_nm3u8dl_usable_replacement(
                    running_expiry,
                    pool_candidate.get("stream_url", ""),
                    pool_candidate.get("expiry"),
                    now_ts=check_time,
                )
            ]

            stable_candidates = [
                pool_candidate
                for pool_candidate in replacement_candidates
                if is_nm3u8dl_stable_replacement(
                    running_expiry,
                    pool_candidate.get("expiry"),
                )
            ]

            scan_replacement_pool = (
                stable_candidates
                or replacement_candidates
            )

            if scan_replacement_pool:
                candidate = dict(
                    max(
                        scan_replacement_pool,
                        key=get_nm3u8dl_candidate_quality_rank,
                    )
                )

                for metadata_key in (
                    "candidate_count",
                    "playlist_source_count",
                    "playlist_group",
                    "match_description",
                    "source_errors",
                    "source_results",
                ):
                    candidate[metadata_key] = resolution.get(
                        metadata_key
                    )

            else:
                candidate = resolution

            candidate_expiry = candidate.get("expiry")
            retained_source = state.nm3u8dl_pending_source

            retained_usable = bool(
                retained_source
                and is_nm3u8dl_usable_replacement(
                    running_expiry,
                    retained_source.get("stream_url", ""),
                    retained_source.get("expiry"),
                    now_ts=check_time,
                )
            )

            retained_stable = bool(
                retained_usable
                and is_nm3u8dl_stable_replacement(
                    running_expiry,
                    retained_source.get("expiry"),
                )
            )

            replacement_locked = False

            if stable_candidates:
                if (
                    retained_stable
                    and get_nm3u8dl_candidate_quality_rank(
                        retained_source
                    )
                    >= get_nm3u8dl_candidate_quality_rank(
                        candidate
                    )
                ):
                    state.nm3u8dl_pending_source = retained_source
                else:
                    state.nm3u8dl_pending_source = candidate

                replacement_locked = True
                decision = (
                    "safe replacement retained and locked; "
                    "current recording continues"
                )

            elif replacement_candidates:
                if retained_stable:
                    state.nm3u8dl_pending_source = retained_source
                    replacement_locked = True
                    decision = (
                        "retained replacement remains safe and locked; "
                        "current recording continues"
                    )

                else:
                    if (
                        retained_usable
                        and get_nm3u8dl_candidate_quality_rank(
                            retained_source
                        )
                        >= get_nm3u8dl_candidate_quality_rank(
                            candidate
                        )
                    ):
                        state.nm3u8dl_pending_source = retained_source
                    else:
                        state.nm3u8dl_pending_source = candidate

                    decision = (
                        "fallback replacement retained; "
                        f"continuing renewal scans for a "
                        f"{NM3U8DL_NEW_SOURCE_MIN_REMAINING_MIN}-minute "
                        "replacement"
                    )

            elif retained_source:
                replacement_locked = retained_stable

                if replacement_locked:
                    decision = (
                        "retained replacement remains safe and locked; "
                        "current recording continues"
                    )
                else:
                    decision = (
                        "no better replacement found; fallback retained; "
                        "current recording continues"
                    )

            elif candidate_expiry is None:
                decision = (
                    "best candidate has no recognized expiry; "
                    "current recording continues"
                )

            elif (
                running_expiry is not None
                and candidate_expiry <= running_expiry
            ):
                decision = (
                    "best candidate does not extend the running "
                    "authorization; current recording continues"
                )

            elif candidate_expiry <= check_time:
                decision = (
                    "best candidate is already expired; "
                    "current recording continues"
                )

            else:
                decision = (
                    "best candidate is not a usable replacement; "
                    "current recording continues"
                )

        check_completed_ts = time.time()

        if quality_check_due:
            next_quality_check_ts = (
                check_completed_ts
                + quality_check_interval_sec
            )

        if authorization_check_due:
            if replacement_locked:
                next_renewal_check_ts = None
            else:
                next_renewal_check_ts = (
                    calculate_next_renewal_check(
                        check_completed_ts
                    )
                )

            log_nm3u8dl_playlist_renewal_check(
                checked_at_ts=check_completed_ts,
                source_results=source_results,
                running_expiry=running_expiry,
                candidate=candidate,
                retained_source=state.nm3u8dl_pending_source,
                decision=decision,
                next_check_ts=next_renewal_check_ts,
                decision_level=decision_level,
                replacement_locked=replacement_locked,
            )

        if quality_check_due and quality_decision:
            log("")
            log(
                f"Quality upgrade check → {quality_decision}; "
                f"next check in "
                f"{quality_check_interval_min:g} minutes.",
                level=quality_decision_level,
            )
            log("")

def print_nm3u8dl_playlist_source_diagnostic():
    source = resolve_nm3u8dl_playlist_source()

    print("")
    print("=== N_m3u8DL PLAYLIST RESOLUTION TEST ===")
    print(f'EXTINF: {source["extinf"]}')
    print(f'Stream URL: {source["stream_url"]}')
    print(f'Headers: {source["headers"]}')
    print(f'Keys: {source["keys"]}')
    source_quality = format_nm3u8dl_candidate_quality(source)
    if _nm3u8dl_has_quality_evidence(source):
        print(f'Quality: {source_quality}')
    print(f'Expiry: {source["expiry"]}')
    print("========================================")
    print("")
    
def _nm3u8dl_stream_url_from_part_a() -> str:
    match = re.search(r'"(https?://[^"]+)"', NM3U8DL_PART_A)
    return match.group(1) if match else ""

def get_nm3u8dl_exclude_substrings(part_a: Optional[str] = None) -> List[str]:
    source_part_a = NM3U8DL_PART_A if part_a is None else part_a

    match = re.search(r'"(https?://[^"]+)"', source_part_a)
    stream_url = match.group(1) if match else ""

    excludes = list(NM3U8DL_STDOUT_EXCLUDE_DEFAULT)

    for key, extra in NM3U8DL_STDOUT_EXCLUDE_BY_URL.items():
        if key in stream_url:
            excludes.extend(extra)

    return excludes

def should_exclude_nm3u8dl_line(stripped_line: str, exclude_substrings: List[str]) -> bool:
    if stripped_line in ("ERROR", "ERROR:"):
        return True
    return any(ex in stripped_line for ex in exclude_substrings)
    
def extract_nm3u8dl_startup_info(proc, exclude_substrings=None, raw_invocation=None):
    """
    Read N_m3u8DL startup stdout until the manifest refresh interval is found.

    Supports both older newline-delimited output and current N_m3u8DL
    redirected-console output where timestamped records may be concatenated.

    Stops reading immediately after the refresh-interval message so live
    Vid/Aud progress remains in stdout for monitor_nm3u8dl_stdout().
    """

    bitrate = 0
    refresh = 0
    in_selected_section = False

    if exclude_substrings is None:
        exclude_substrings = []

    # Detect the beginning of another logical N_m3u8DL record even
    # when there is no newline between records. This includes raw Vid/Aud
    # console-refresh records, which can appear while startup parsing still
    # owns stdout if no manifest refresh interval is emitted.
    record_start_suffix_re = re.compile(
        r'(?i)(?:'
        r'\d{2}:\d{2}:\d{2}\.\d{3}\s+'
        r'(?:TRACE|DEBUG|INFO|WARN|ERROR|FATAL|EXTRA)\s*:\s*'
        r'|Vid(?:\s*\*\S+)?\s+'
        r'|Aud(?:\s*\*\S+)?\s+'
        r')$'
    )

    timestamp_prefix_only_re = re.compile(
        r'^\d{2}:\d{2}:\d{2}\.\d{3}\s+'
        r'(?:TRACE|DEBUG|INFO|WARN|ERROR|FATAL|EXTRA)\s*:\s*$',
        re.IGNORECASE,
    )

    pending = ""
    startup_progress_last_log_time = time.time()
    startup_progress_log_interval = 30.0
    startup_latest_progress = {"vid": None, "aud": None}
    startup_last_logged_progress = {"vid": None, "aud": None}

    def maybe_log_startup_progress(force=False):
        nonlocal startup_progress_last_log_time

        now = time.time()
        if not force and (now - startup_progress_last_log_time) < startup_progress_log_interval:
            return

        logged_any = False
        for key in ("vid", "aud"):
            value = startup_latest_progress[key]
            if value and value != startup_last_logged_progress[key]:
                log(f"N_m3u8DL: {value}")
                startup_last_logged_progress[key] = value
                logged_any = True

        if logged_any:
            startup_progress_last_log_time = now

    def handle_startup_record(record):
        nonlocal bitrate, in_selected_section

        raw_external_write(raw_invocation, record)
        stripped_line = record.strip()

        if not stripped_line:
            return

        # Remove inner N_m3u8DL timestamp.
        stripped_line = re.sub(
            r'^\d{2}:\d{2}:\d{2}\.\d{3}\s+',
            '',
            stripped_line
        )

        if not stripped_line:
            return

        # Raw console-refresh progress has no timestamp/newline. Keep only the
        # latest Vid/Aud record and log it periodically instead of dumping every
        # refresh fragment into one unreadable line.
        progress_match = re.match(
            r'^(Vid|Aud)(?:\s*\*\S+)?\s+',
            stripped_line,
            re.IGNORECASE,
        )
        if progress_match:
            key = "vid" if progress_match.group(1).lower() == "vid" else "aud"
            startup_latest_progress[key] = stripped_line
            maybe_log_startup_progress()
            return

        if not should_exclude_nm3u8dl_line(
            stripped_line,
            exclude_substrings
        ):
            log(f"N_m3u8DL: {stripped_line}")

        if "Selected streams:" in stripped_line:
            in_selected_section = True
            return

        # Selected VIDEO bitrate only.
        if (
            in_selected_section
            and "Kbps" in stripped_line
            and "Segments" in stripped_line
        ):
            match = re.search(
                r'\bVid(?:\s*\*\S+)?\s+'
                r'\d+x\d+\s*\|\s*(\d+)\s+Kbps\b',
                stripped_line,
                re.IGNORECASE,
            )

            if match and bitrate == 0:
                try:
                    bitrate = int(match.group(1))
                    log(
                        f"N_m3u8DL: Selected bitrate detected: "
                        f"{bitrate} Kbps"
                    )
                except Exception:
                    pass

    try:
        while True:
            if proc.stdout is None:
                break

            # Read continuously instead of readline().
            ch = proc.stdout.read(1)

            if ch == "":
                if proc.poll() is not None:
                    break

                time.sleep(0.01)
                continue

            # Normal old-style line ending.
            if ch in ("\r", "\n"):
                if pending.strip():
                    handle_startup_record(pending)

                pending = ""
                continue

            pending += ch

            # Most important part:
            # as soon as the refresh message is complete, stop consuming stdout.
            refresh_match = re.search(
                r'set refresh interval to\s+(\d+)\s+seconds',
                pending,
                re.IGNORECASE,
            )

            if refresh_match:
                try:
                    refresh = int(refresh_match.group(1))

                    # Log only the real refresh record, not anything that
                    # N_m3u8DL may start writing immediately afterwards.
                    refresh_record = pending[:refresh_match.end()]
                    handle_startup_record(refresh_record)

                    log(
                        f"N_m3u8DL: Refresh interval detected: "
                        f"{refresh} seconds"
                    )

                    return {
                        'bitrate': bitrate,
                        'refresh_interval': refresh
                    }

                except Exception:
                    pass

            # Current N_m3u8DL can begin another logical record without
            # inserting a newline first.
            match = record_start_suffix_re.search(pending)

            if match and match.start() > 0:
                previous = pending[:match.start()]

                # Do not split immediately between a timestamp/level prefix
                # and the Vid/Aud text belonging to that same normal record.
                if timestamp_prefix_only_re.fullmatch(previous):
                    continue

                handle_startup_record(previous)
                pending = pending[match.start():]

        # Process any remaining startup text if the process ended, then emit
        # the latest clean progress record once.
        if pending.strip():
            handle_startup_record(pending)
        maybe_log_startup_progress(force=True)

    except Exception as e:
        log(f"Error reading N_m3u8DL startup: {e}")

    return {
        'bitrate': bitrate,
        'refresh_interval': refresh
    }
    
def monitor_nm3u8dl_stdout(
    proc,
    refresh_interval=60,
    exclude_substrings=None,
    live_end_event=None,
    raw_invocation=None,
    progress_activity_id: Optional[str] = None,
):
    """
    Monitor N_m3u8DL stdout.

    Supports both:
    - older newline-based N_m3u8DL progress output
    - current N_m3u8DL console-refresh output, where Vid/Aud progress
      fragments may arrive without normal newline terminators

    Latest Vid/Aud progress is printed as permanent terminal snapshots at the
    recorder's configured interval; N's native refresh cadence stays unchanged.
    """

    if progress_activity_id is None:
        progress_activity_id = new_terminal_activity("nm3u8dl_progress")

    last_log_time = time.time()
    latest_progress = {"vid": None, "aud": None}
    log_interval = float(NM3U8DL_TERMINAL_PROGRESS_INTERVAL_SEC)

    if exclude_substrings is None:
        exclude_substrings = []

    monitoring_interval = (
        60.0
        if not refresh_interval
        else max(
            float(NM3U8DL_MIN_GROWTH_CHECK_INTERVAL_SEC),
            float(NM3U8DL_CHECK_INTERVAL_MULTIPLIER) * float(refresh_interval),
        )
    )
    log(f"N_m3u8DL: Monitoring interval set to {monitoring_interval:g}s")
    log("")

    # Current N_m3u8DL may concatenate records without \n.
    # These patterns let us recognize the START of the next logical record.
    record_start_suffix_re = re.compile(
        r'(?i)(?:'
        r'\d{2}:\d{2}:\d{2}\.\d{3}\s+'
        r'(?:TRACE|DEBUG|INFO|WARN|ERROR|FATAL|EXTRA)\s*:\s*'
        r'|Vid(?:\s*\*\S+)?\s+'
        r'|Aud(?:\s*\*\S+)?\s+'
        r')$'
    )

    timestamp_prefix_only_re = re.compile(
        r'^\d{2}:\d{2}:\d{2}\.\d{3}\s+'
        r'(?:TRACE|DEBUG|INFO|WARN|ERROR|FATAL|EXTRA)\s*:\s*$',
        re.IGNORECASE,
    )

    pending = ""

    def maybe_log_progress():
        nonlocal last_log_time

        now = time.time()

        if now - last_log_time >= log_interval:
            if latest_progress["vid"]:
                log(
                    f"N_m3u8DL: {latest_progress['vid']}",
                    activity_id=progress_activity_id,
                )

            if latest_progress["aud"]:
                log(
                    f"N_m3u8DL: {latest_progress['aud']}",
                    activity_id=progress_activity_id,
                )

            last_log_time = now

    def handle_record(record):
        raw_external_write(raw_invocation, record)
        raw = record.strip()

        if not raw:
            return

        # Remove normal N_m3u8DL timestamp when present.
        stripped = re.sub(
            r'^\d{2}:\d{2}:\d{2}\.\d{3}\s+',
            '',
            raw
        )

        # Current N_m3u8DL redirected progress output can collapse the
        # separator between duration and segment count:
        #   00m33s/00m33s11/11
        #   01h00m14s/01h00m14s913/913
        # become:
        #   00m33s/00m33s 11/11
        #   01h00m14s/01h00m14s 913/913
        stripped = re.sub(
            r'((?:\d{2}h)?\d{2}m\d{2}s/(?:\d{2}h)?\d{2}m\d{2}s)(\d+/\d+)',
            r'\1 \2',
            stripped
        )

        if not stripped.strip():
            return

        # N_m3u8DL emits this when the live playlist has actually ended.
        # Record it as a run fact; let N_m3u8DL finish/finalize normally.
        if (
            live_end_event is not None
            and "live stream ended, will stop recording soon" in stripped.lower()
        ):
            live_end_event.set()

        if should_exclude_nm3u8dl_line(stripped, exclude_substrings):
            return

        # Supports:
        #   Vid ...
        #   Vid *CENC ...
        #   Vid*UNKNOWN ...
        #   Aud ...
        #   Aud*UNKNOWN ...
        is_video_line = bool(
            re.search(
                r'(^|\s)Vid(?:\s*\*\S+)?\s+',
                stripped,
                re.IGNORECASE
            )
        )

        is_audio_line = bool(
            re.search(
                r'(^|\s)Aud(?:\s*\*\S+)?\s+',
                stripped,
                re.IGNORECASE
            )
        )

        if is_video_line or is_audio_line:
            if is_video_line:
                latest_progress["vid"] = stripped

            if is_audio_line:
                latest_progress["aud"] = stripped

        else:
            # Non-progress messages continue to log immediately.
            log(f"N_m3u8DL: {stripped}")

        maybe_log_progress()

    try:
        while True:
            if proc.stdout is None:
                break

            # IMPORTANT:
            # Do not use readline() here.
            # Current N_m3u8DL progress output is not reliably newline-delimited.
            ch = proc.stdout.read(1)

            if ch == "":
                if proc.poll() is not None:
                    break

                time.sleep(0.05)
                continue

            # Old/newline-based output still works normally.
            if ch in ("\r", "\n"):
                if pending.strip():
                    handle_record(pending)

                pending = ""
                continue

            pending += ch

            # Detect when N_m3u8DL has started another logical record even
            # though it did not emit a newline between the two records.
            match = record_start_suffix_re.search(pending)

            if match and match.start() > 0:
                previous = pending[:match.start()]

                # Do not split:
                #   20:48:10.123 INFO : Vid ...
                # immediately between "INFO :" and "Vid".
                if timestamp_prefix_only_re.fullmatch(previous):
                    continue

                handle_record(previous)
                pending = pending[match.start():]

        # Flush one final partial record at process exit.
        if pending.strip():
            handle_record(pending)

    except Exception as e:
        log(f"N_m3u8DL stdout monitor error: {e}")
    finally:
        raw_external_end(
            raw_invocation,
            returncode=proc.poll(),
        )

def monitor_nm3u8dl_file_growth(state, output_path: str, stop_event: threading.Event):
    """NM-D (Simplified v14): emit growth events only (no decisions)."""
    try:
        refresh_s = float(getattr(state, "nm3u8dl_refresh_interval_s", 0) or 0)
        selected_bitrate_kbps = float(getattr(state, "nm3u8dl_selected_bitrate_kbps", 0) or 0)
        threshold_kbps = selected_bitrate_kbps * float(NM3U8DL_SPEED_DEGRADATION_FACTOR)

        check_interval = (
            60.0
            if (refresh_s is None or refresh_s == 0)
            else max(
                float(NM3U8DL_MIN_GROWTH_CHECK_INTERVAL_SEC),
                float(NM3U8DL_CHECK_INTERVAL_MULTIPLIER) * refresh_s,
            )
        )
        
        prev_size = os.path.getsize(output_path) if os.path.exists(output_path) else 0
        prev_t = time.monotonic()

        # Per-run counters/state
        state.nm3u8dl_hard_count = 0
        state.nm3u8dl_soft_count = 0
        state.nm3u8dl_run_had_healthy_check = False

        while not stop_event.is_set():
            time.sleep(check_interval)
            if stop_event.is_set():
                break

            if not os.path.exists(output_path):
                continue

            cur_size = os.path.getsize(output_path)
            now_t = time.monotonic()
            elapsed_s = max(0.001, now_t - prev_t)

            growth_bytes = max(0, cur_size - prev_size)
            growth_kbps = (growth_bytes * 8.0) / elapsed_s / 1000.0

            prev_size = cur_size
            prev_t = now_t

            event_queue = getattr(state, "nm3u8dl_event_queue", None)
            if event_queue is not None:
                event_queue.put({
                    "type": "growth",
                    "growth_bytes": growth_bytes,
                    "growth_kbps": growth_kbps,
                    "threshold_kbps": threshold_kbps,
                    "expected_kbps": selected_bitrate_kbps,
                })

    except Exception as e:
        log(f"[NM3U8DL] Growth monitor error: {e}", level="WARN")

def monitor_nm3u8dl_health_display(
    state: RecorderState,
    output_path: str,
    stop_event: threading.Event,
    progress_activity_id: str,
):
    """
    Presentation-only N_m3u8DL health cadence.

    This sampler exists only to honor NM3U8DL_HEALTH_LOG_INTERVAL_SEC exactly.
    It does not change stall counters, recovery decisions, alarms, or restart
    behavior; the existing growth monitor remains the sole stall detector.
    """
    try:
        selected_bitrate_kbps = float(
            getattr(state, "nm3u8dl_selected_bitrate_kbps", 0) or 0
        )
        threshold_kbps = (
            selected_bitrate_kbps
            * float(NM3U8DL_SPEED_DEGRADATION_FACTOR)
        )
        health_interval = max(
            1.0,
            float(NM3U8DL_HEALTH_LOG_INTERVAL_SEC),
        )

        prev_size = (
            os.path.getsize(output_path)
            if os.path.exists(output_path)
            else 0
        )
        prev_t = time.monotonic()
        next_health_t = prev_t + health_interval

        while not stop_event.is_set():
            wait_s = max(
                0.0,
                next_health_t - time.monotonic(),
            )
            if stop_event.wait(wait_s):
                break

            now_t = time.monotonic()

            if not os.path.exists(output_path):
                prev_size = 0
                prev_t = now_t
                next_health_t = now_t + health_interval
                continue

            cur_size = os.path.getsize(output_path)
            elapsed_s = max(0.001, now_t - prev_t)
            growth_bytes = max(0, cur_size - prev_size)
            growth_kbps = (
                (growth_bytes * 8.0)
                / elapsed_s
                / 1000.0
            )

            prev_size = cur_size
            prev_t = now_t

            # Stay anchored to the configured cadence without printing bursts
            # if this presentation thread was briefly delayed.
            while next_health_t <= now_t:
                next_health_t += health_interval

            if (
                growth_bytes <= HARD_STALL_ZERO_BYTES
                or growth_kbps < threshold_kbps
            ):
                continue

            log(
                f"N_m3u8DL: Good speed {int(growth_kbps)} Kbps "
                f"(threshold {int(threshold_kbps)})",
                activity_id=progress_activity_id,
            )

    except Exception as e:
        log(
            f"[NM3U8DL] Health display error: {e}",
            level="WARN",
        )

def nm3u8dl_handle_growth_events(state: RecorderState, stop_event: threading.Event, notify=None):
    """Boss-owned decision handler for confirmed N_m3u8DL growth events."""

    hard_stall_required = NM3U8DL_HARD_STALL_REQUIRED
    soft_stall_required = NM3U8DL_SOFT_STALL_REQUIRED

    if NM3U8DL_SOURCE_MODE == "playlist":
        profile = get_nm3u8dl_playlist_profile()
        hard_stall_required = int(
            profile.get("hard_stall_required", hard_stall_required)
        )
        soft_stall_required = int(
            profile.get("soft_stall_required", soft_stall_required)
        )
    # ACK belongs to the stall incident, not to one particular N process.
    if getattr(state, "nm3u8dl_ack_requested", False):
        acknowledge_nm3u8dl_stall_alarm(state)

    # Manual X/Y rejects the confirmed feed signature for this recording.
    # Recheck the signature immediately before stopping so a feed change that
    # happened while the confirmation prompt was open cannot reject the wrong feed.
    if getattr(state, "nm3u8dl_manual_exclude_requested", False):
        source = getattr(state, "nm3u8dl_running_source", None)
        pending = getattr(state, "nm3u8dl_pending_manual_exclusion", None) or {}
        current_signature = get_nm3u8dl_manual_feed_signature(source)

        if (
            not source
            or not pending
            or not current_signature
            or current_signature.get("key") != pending.get("key")
        ):
            state.nm3u8dl_manual_exclude_requested = False
            state.nm3u8dl_pending_manual_exclusion = None
            log(
                "MANUAL STREAM REJECT refused: current feed changed or its "
                "feed signature is unavailable; recording continues unchanged.",
                level="WARN",
            )
        else:
            log("RUN_REJECT reason=manual_feed_exclusion", level="WARN")
            state.nm3u8dl_stall_flag = True
            state.nm3u8dl_stall_reason = "manual_exclude"
            stop_event.set()
            return

    # Manual R remains a process-level restart request and is not a stall event.
    if getattr(state, "nm3u8dl_manual_restart_requested", False):
        log("RUN_RESTART reason=manual", level="WARN")
        state.nm3u8dl_stall_flag = True
        state.nm3u8dl_stall_reason = "manual"
        state.nm3u8dl_manual_restart_requested = False
        stop_event.set()
        return

    event_queue = getattr(state, "nm3u8dl_event_queue", None)
    if event_queue is None:
        return

    while True:
        try:
            event = event_queue.get_nowait()
        except Exception:
            break

        if event.get("type") != "growth":
            continue

        growth_bytes = event.get("growth_bytes", 0)
        growth_kbps = float(event.get("growth_kbps", 0))
        threshold_kbps = float(event.get("threshold_kbps", 0))
        expected_kbps = float(event.get("expected_kbps", 0))

        # Preserve the existing internal confirmation logic. A hard/soft CHECK is
        # not a stall event; only reaching the configured threshold below creates
        # one confirmed stall event.
        is_hard = (growth_bytes <= HARD_STALL_ZERO_BYTES)
        if is_hard:
            state.nm3u8dl_hard_count += 1
            log(
                f"HARD_STALL_CHECK "
                f"{min(state.nm3u8dl_hard_count, hard_stall_required)}"
                f"/{hard_stall_required} "
                f"growth_bytes={growth_bytes}",
                level="WARN",
            )
            if not is_alarm_active(state):
                beep_bad(state)
        else:
            is_soft = (growth_kbps < threshold_kbps)
            if is_soft:
                state.nm3u8dl_soft_count += 1
                log(
                    f"SOFT_STALL_CHECK "
                    f"{min(state.nm3u8dl_soft_count, soft_stall_required)}"
                    f"/{soft_stall_required} "
                    f"growth_kbps={int(growth_kbps)} "
                    f"threshold_kbps={int(threshold_kbps)} "
                    f"expected_kbps={int(expected_kbps)}",
                    level="WARN",
                )
                if not is_alarm_active(state):
                    beep_bad(state)
            else:
                # One confirmed healthy growth check is the ONLY recovery reset.
                state.nm3u8dl_run_had_healthy_check = True
                _nm3u8dl_note_failover_healthy_growth(state)

                # A retained target-quality run makes the old quality-recovery
                # VPN incident irrelevant only after this healthy-growth proof.
                # Authorization-purpose incidents are intentionally untouched.
                retire_nm3u8dl_quality_access_incident_if_target_healthy(
                    state
                )

                had_failure_incident = bool(
                    state.nm3u8dl_stall_event_count
                    or state.manifest_fail_count
                    or state.no_file_appear_count
                    or state.nm3u8dl_crash_count
                    or state.nm3u8dl_had_alarm_incident
                )

                if getattr(state, "nm3u8dl_alarm_active", False):
                    log("ALARM_STALL_STOP reason=recovery")
                    nm3u8dl_alarm_stop(state)

                state.nm3u8dl_post_ack_silent = False
                state.nm3u8dl_ack_requested = False
                state.nm3u8dl_hard_count = 0
                state.nm3u8dl_soft_count = 0
                state.nm3u8dl_stall_event_count = 0
                state.manifest_fail_count = 0
                state.no_file_appear_count = 0
                state.nm3u8dl_crash_count = 0

                maybe_trigger_good_beep(
                    state,
                    "nm3u8dl",
                    True,
                    had_failure_incident,
                    notify,
                )
                state.nm3u8dl_had_alarm_incident = False
                continue

        hard_trigger = (
            state.nm3u8dl_hard_count == hard_stall_required
        )
        soft_trigger = (
            state.nm3u8dl_soft_count == soft_stall_required
        )

        if not (hard_trigger or soft_trigger):
            continue

        stall_type = "hard" if hard_trigger else "soft"
        log(f"CLASSIFY STALL_{stall_type.upper()}", level="WARN")
        reset_good_beep_state(state, "nm3u8dl")

        state.nm3u8dl_stall_event_count += 1
        event_count = state.nm3u8dl_stall_event_count
        state.nm3u8dl_had_alarm_incident = True

        log(
            f"STALL_EVENT type={stall_type} "
            f"event_count {event_count}/{NM3U8DL_STALL_MAX_EVENTS}",
            level="WARN",
        )

        if (
            NM3U8DL_SOURCE_MODE != "playlist"
            and event_count == 2
            and not getattr(state, "nm3u8dl_post_ack_silent", False)
        ):
            log("ALARM_STALL_START press A to acknowledge", level="WARN")
            nm3u8dl_alarm_start(state, stall_type, incident=2)

        # Every confirmed stall event ends this process immediately. Recovery is
        # then owned by the orchestrator (static retry or dynamic replacement/search).
        state.nm3u8dl_stall_flag = True
        state.nm3u8dl_stall_reason = f"STALL_{stall_type.upper()}"
        stop_event.set()
        return

def find_nm3u8dl_output_path(expected_path, not_before=0):
    """
    Find N_m3u8DL's actual TS output.

    Accepts:
      chunk_001.ts
      chunk_001.eng.ts
      chunk_001.<other-suffix>.ts

    Only accepts a file created/modified during the current run.
    """
    directory = os.path.dirname(expected_path)
    expected_name = os.path.basename(expected_path)
    stem, ext = os.path.splitext(expected_name)

    def is_current_file(path):
        try:
            return (
                os.path.isfile(path)
                and os.path.getmtime(path) >= (not_before - 1.0)
            )
        except OSError:
            return False

    # Prefer exact canonical name.
    if is_current_file(expected_path):
        return expected_path

    prefix = stem + "."

    try:
        candidates = []
        for name in os.listdir(directory):
            if (
                name.startswith(prefix)
                and name.lower().endswith(ext.lower())
            ):
                path = os.path.join(directory, name)
                if is_current_file(path):
                    candidates.append(path)
    except FileNotFoundError:
        return None

    if not candidates:
        return None

    # If more than one suffix variant exists, use the most recently modified.
    return max(candidates, key=os.path.getmtime)

def start_nm3u8dl_to_chunk(state: RecorderState, notify=None, deadline_ts: Optional[float] = None) -> EngineResult:
    """
    Start N_m3u8DL-RE process with monitoring threads.
    Detects crashes via file growth stalls and returns EngineResult facts only.
    """
    state.nm3u8dl_run_active = True
    try:
        chunk_path, chunk_name = next_chunk_names(state)
        state.nm3u8dl_stall_flag = False

        # Build full command: resolved Part_A + save-name + Part_B
        chunk_name_without_ext = chunk_name.replace('.ts', '')
        nm3u8dl_part_a = get_nm3u8dl_part_a(state)

        if nm3u8dl_part_a is None:
            return EngineResult(
                status="ended",
                reason="external stop before N_m3u8DL launch",
                chunk_path=None,
                chunk_name=None,
                metrics={"stop_requested": True},
            )

        nm3u8dl_part_b = _get_nm3u8dl_part_b_for_launch(state)
        cmd = f'{nm3u8dl_part_a} --save-name "{chunk_name_without_ext}" {nm3u8dl_part_b}'
        
        exclude_substrings = get_nm3u8dl_exclude_substrings(nm3u8dl_part_a)

        cmd = cmd.replace(
            "--use-shaka-packager",
            "--decryption-engine SHAKA_PACKAGER"
        )

        # Per-process state only. Stall/crash counters and stall-alarm state are
        # incident-level state and survive process replacement/restart until one
        # confirmed healthy growth check proves recovery.
        ensure_nm3u8dl_key_listener(state)
        state.nm3u8dl_stall_flag = False
        state.nm3u8dl_stall_reason = None
        state.nm3u8dl_ack_requested = False
        state.nm3u8dl_manual_restart_requested = False
        state.nm3u8dl_manual_exclude_requested = False
        state.nm3u8dl_hard_count = 0
        state.nm3u8dl_soft_count = 0
        state.nm3u8dl_run_had_healthy_check = False
        state.nm3u8dl_healthy_consec_count = 0
        state.nm3u8dl_beeped_this_healthy_streak = False
        state.nm3u8dl_refresh_interval_s = 0.0

        log(f"Starting N_m3u8DL → writing to {chunk_name}...")
        log(f"Command: {cmd}")

        popen_cmd = cmd if os.name == "nt" else shlex.split(cmd)

        run_launch_ts = time.time()
        state.stats["last_run_start"] = run_launch_ts
        
        popen_env = os.environ.copy()

        if NM3U8DL_AUDIO_OFFSET_SEC != 0:
            offset = NM3U8DL_AUDIO_OFFSET_SEC
            popen_env["RE_LIVE_PIPE_OPTIONS"] = (
                f'-bsf:a "setts=pts=PTS{offset:+g}/TB:dts=DTS{offset:+g}/TB" '
                f'-f mpegts -shortest "{chunk_name}"'
            )
        else:
            popen_env.pop("RE_LIVE_PIPE_OPTIONS", None)
    
        raw_invocation = raw_external_start("N_m3u8DL-RE", chunk_name)

        raw_external_write(
            raw_invocation,
            repr(popen_cmd),
            "command",
        )
        raw_external_write(
            raw_invocation,
            f"cwd={CHUNKS_DIR!r}",
            "execution",
        )

        try:
            proc = subprocess.Popen(
                popen_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=CHUNKS_DIR,
                env=popen_env
            )
        except Exception as exc:
            raw_external_end(
                raw_invocation,
                status=f"launch exception {type(exc).__name__}",
            )
            raise

        # Extract bitrate AND refresh interval AND startup info in ONE PASS
        startup_data = extract_nm3u8dl_startup_info(
            proc,
            exclude_substrings=exclude_substrings,
            raw_invocation=raw_invocation,
        )
        state.nm3u8dl_selected_bitrate_kbps = startup_data['bitrate']
        state.nm3u8dl_bitrate_is_fallback = False
        state.nm3u8dl_refresh_interval_s = startup_data['refresh_interval']
        # refresh_interval = startup_data['refresh_interval']

        if (
            state.nm3u8dl_refresh_interval_s > 0
            and state.nm3u8dl_selected_bitrate_kbps == 0
        ):
            # Keep a synthetic bitrate only for live stall monitoring. Some valid
            # streams do not advertise bitrate, so 0 cannot mean MANIFEST_DEAD;
            # however, this fallback is not real evidence for final chunk quality.
            state.nm3u8dl_selected_bitrate_kbps = NM3U8DL_FALLBACK_BITRATE_KBPS
            state.nm3u8dl_bitrate_is_fallback = True
            log(
                f"N_m3u8DL: Bitrate unavailable → using fallback "
                f"{state.nm3u8dl_selected_bitrate_kbps} Kbps for stall monitoring only"
            )

        if state.nm3u8dl_refresh_interval_s == 0 or state.nm3u8dl_selected_bitrate_kbps == 0:
            startup_exit_code = proc.poll()
            if (
                NM3U8DL_SOURCE_MODE == "playlist"
                and startup_exit_code not in (None, 0)
            ):
                log(
                    f"N_m3u8DL exited unexpectedly during startup "
                    f"with code {startup_exit_code} → ENGINE_CRASH",
                    level="WARN",
                )
                raw_external_end(raw_invocation, returncode=startup_exit_code)
                return EngineResult(
                    status="ended",
                    reason=f"startup process exit code {startup_exit_code}",
                    chunk_path=chunk_path,
                    chunk_name=chunk_name,
                    metrics={
                        "selected_bitrate_kbps": state.nm3u8dl_selected_bitrate_kbps,
                        "refresh_interval_s": state.nm3u8dl_refresh_interval_s,
                        "unexpected_process_exit": True,
                    },
                )

            log("No stream/manifest → fail immediately")
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    log_process_shutdown_timeout("N_m3u8DL", 5)
                    proc.kill()
                    proc.wait()
            raw_external_end(raw_invocation, returncode=proc.poll())
            return EngineResult(
                status="no_stream",
                reason="MANIFEST_DEAD",
                chunk_path=chunk_path,
                chunk_name=chunk_name,
                metrics={
                    "selected_bitrate_kbps": state.nm3u8dl_selected_bitrate_kbps,
                    "refresh_interval_s": state.nm3u8dl_refresh_interval_s,
                    "manifest_dead": True,
                },
            )
            
        # Start monitoring threads
        stop_event = threading.Event()
        live_end_event = threading.Event()

        state.nm3u8dl_stop_event = stop_event

        # Vid/Aud snapshots and Good-speed health lines are one recurring
        # N_m3u8DL terminal activity for the lifetime of this run.
        nm3u8dl_progress_activity_id = new_terminal_activity(
            "nm3u8dl_progress"
        )

        # Start nm3u8dl output monitoring thread
        stdout_thread = threading.Thread(
            target=monitor_nm3u8dl_stdout,
            args=(
                proc,
                state.nm3u8dl_refresh_interval_s,
                exclude_substrings,
                live_end_event,
                raw_invocation,
                nm3u8dl_progress_activity_id,
            ),
            daemon=True,
        )
        stdout_thread.start()
        
        # NM-C: after startup parsing succeeds, require N_m3u8DL output to exist.
        file_appear_deadline_s = int(
            FILE_APPEAR_DEADLINE_MULTIPLIER
            * state.nm3u8dl_refresh_interval_s
            + FILE_APPEAR_DEADLINE_EXTRA_SEC
        )

        if NM3U8DL_SOURCE_MODE == "playlist":
            profile = get_nm3u8dl_playlist_profile()

            file_appear_deadline_s = max(
                file_appear_deadline_s,
                int(profile.get("min_file_appear_sec", 0)),
            )

        file_appear_deadline_ts = time.time() + file_appear_deadline_s
        file_wait_start_ts = time.time()

        actual_chunk_path = None

        while (
            time.time() < file_appear_deadline_ts
            and not state.stop_flag
            and not recording_deadline_reached(state)
        ):
            actual_chunk_path = find_nm3u8dl_output_path(
                chunk_path,
                not_before=run_launch_ts
            )

            if actual_chunk_path:
                if actual_chunk_path != chunk_path:
                    log(
                        f"N_m3u8DL: output filename variant detected → "
                        f"{os.path.basename(actual_chunk_path)}"
                    )
                break

            if proc.poll() is not None:
                break

            time.sleep(FILE_APPEAR_POLL_SEC)

        if actual_chunk_path is None and recording_deadline_reached(state):
            log(
                f"Max duration reached {format_current_duration_for_log(state)} "
                "while waiting for N_m3u8DL output → terminating N_m3u8DL..."
            )
            stop_event.set()
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                log_process_shutdown_timeout("N_m3u8DL", 5)
                proc.kill()
                proc.wait()

            final_output_path = find_nm3u8dl_output_path(
                chunk_path,
                not_before=run_launch_ts,
            )
            return EngineResult(
                status="duration_reached",
                reason="duration reached",
                chunk_path=final_output_path or chunk_path,
                chunk_name=(
                    os.path.basename(final_output_path)
                    if final_output_path
                    else chunk_name
                ),
                metrics={
                    "selected_bitrate_kbps": state.nm3u8dl_selected_bitrate_kbps,
                    "refresh_interval_s": state.nm3u8dl_refresh_interval_s,
                },
            )

        if actual_chunk_path is None:
            waited_s = int(time.time() - file_wait_start_ts)
            exit_code = proc.poll()

            if (
                NM3U8DL_SOURCE_MODE == "playlist"
                and exit_code not in (None, 0)
            ):
                log("")
                log(
                    f"N_m3u8DL exited unexpectedly before an output file appeared "
                    f"(exit_code={exit_code}) → ENGINE_CRASH",
                    level="WARN",
                )
                raw_external_end(raw_invocation, returncode=exit_code)
                return EngineResult(
                    status="ended",
                    reason=f"process exit before output file; code {exit_code}",
                    chunk_path=chunk_path,
                    chunk_name=chunk_name,
                    metrics={
                        "selected_bitrate_kbps": state.nm3u8dl_selected_bitrate_kbps,
                        "refresh_interval_s": state.nm3u8dl_refresh_interval_s,
                        "unexpected_process_exit": True,
                    },
                )

            log("")
            log(
                f"No output file appeared after {waited_s}s "
                f"(deadline {file_appear_deadline_s}s) "
                f"→ ending run (exit_code={exit_code})"
            )

            if proc.poll() is None:
                proc.terminate()

                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    log_process_shutdown_timeout("N_m3u8DL", 5)
                    proc.kill()
                    proc.wait()

            raw_external_end(raw_invocation, returncode=proc.poll())
            return EngineResult(
                status="no_stream",
                reason="NO_FILE_APPEAR",
                chunk_path=chunk_path,
                chunk_name=chunk_name,
                metrics={
                    "selected_bitrate_kbps":
                        state.nm3u8dl_selected_bitrate_kbps,
                    "refresh_interval_s":
                        state.nm3u8dl_refresh_interval_s,
                    "file_appear_deadline_s":
                        file_appear_deadline_s,
                    "no_file_appear": True,
                },
            )


        # Start nm3u8dl file growth monitoring thread (events only)
        state.nm3u8dl_event_queue = queue.SimpleQueue()
        state.nm3u8dl_last_health_log_t = time.monotonic()
        threshold_kbps = state.nm3u8dl_selected_bitrate_kbps * float(NM3U8DL_SPEED_DEGRADATION_FACTOR)
        log(f"N_m3u8DL: Stall threshold set to {threshold_kbps:.0f} Kbps")
        file_growth_thread = threading.Thread(
            target=monitor_nm3u8dl_file_growth,
            args=(state, actual_chunk_path, stop_event),
            daemon=True,
        )
        file_growth_thread.start()

        # Separate presentation cadence: this does not feed stall/recovery
        # decisions. It only prints Good speed at the configured 60-second
        # health interval using the same terminal activity as Vid/Aud progress.
        health_display_thread = threading.Thread(
            target=monitor_nm3u8dl_health_display,
            args=(
                state,
                actual_chunk_path,
                stop_event,
                nm3u8dl_progress_activity_id,
            ),
            daemon=True,
        )
        health_display_thread.start()
        
        if NM3U8DL_SOURCE_MODE == "playlist":
            playlist_profile = get_nm3u8dl_playlist_profile()

            renewal_monitor_needed = (
                playlist_profile.get(
                    "renewal_mode",
                    "EXPIRY_ROLLOVER",
                )
                == "EXPIRY_ROLLOVER"
                or bool(
                    playlist_profile.get(
                        "quality_upgrade_enabled",
                        False,
                    )
                )
                or state.nm3u8dl_access_block_consecutive > 0
            )

            if renewal_monitor_needed:
                renewal_thread = threading.Thread(
                    target=monitor_nm3u8dl_playlist_renewal,
                    args=(state, stop_event),
                    daemon=True,
                )
                renewal_thread.start()
            
        start_run = time.time()
        ran_30s = False
        renewal_rollover = False
        rollover_reason = None
        status = "ended"
        reason = "nm3u8dl exited"

        while True:
            ret = proc.poll()
            
            nm3u8dl_handle_growth_events(state, stop_event, notify)

            # Check for crash/stall signals
            if state.nm3u8dl_stall_flag:
                reason = state.nm3u8dl_stall_reason or "file growth stall"
                status = "stalled"
                if reason == "manual_exclude":
                    log(
                        "N_m3u8DL: manual feed rejection confirmed → "
                        "terminating current process...",
                        level="WARN",
                    )
                else:
                    log(
                        f"N_m3u8DL: stall handling requested → "
                        f"terminating process... reason={reason}",
                        level="WARN",
                    )
                stop_event.set()
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    log_process_shutdown_timeout("N_m3u8DL", 5)
                    proc.kill()
                    proc.wait()

                break

            # Check if process exited naturally
            if ret is not None:
                log(f"N_m3u8DL exited with code {ret}")
                stop_event.set()
                break

            if recording_deadline_reached(state):
                reason = "duration reached"
                status = "duration_reached"
                log(f"Max duration reached {format_current_duration_for_log(state)} → terminating N_m3u8DL...")
                stop_event.set()
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    log_process_shutdown_timeout("N_m3u8DL", 5)
                    proc.kill()
                    proc.wait()
                break

            if state.stop_flag:
                reason = "external stop"
                log(f"stop_flag detected → terminating N_m3u8DL...")
                stop_event.set()
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    log_process_shutdown_timeout("N_m3u8DL", 5)
                    proc.kill()
                    proc.wait()
                break

            if state.nm3u8dl_renewal_rollover_requested:
                rollover_reason = (
                    state.nm3u8dl_rollover_reason
                    or "authorization"
                )

                reason = (
                    "quality upgrade rollover"
                    if rollover_reason == "quality_upgrade"
                    else "authorization rollover"
                )

                status = "ok"
                renewal_rollover = True

                if rollover_reason == "quality_upgrade":
                    log(
                        "Quality upgrade ready → "
                        "ending current chunk for controlled rollover..."
                    )
                else:
                    log(
                        "Authorization replacement ready → "
                        "ending current chunk for rollover..."
                    )

                stop_event.set()
                proc.terminate()

                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    log_process_shutdown_timeout("N_m3u8DL", 5)
                    proc.kill()
                    proc.wait()

                break
                
            if not ran_30s and (time.time() - start_run) >= 30:
                ran_30s = True

            time.sleep(0.5)

        # Let the stdout monitor flush N_m3u8DL's final records before we
        # decide whether this run contained an explicit live-end signal.
        join_thread_with_timeout_logging(
            stdout_thread,
            2.0,
            context="N_m3u8DL stdout monitor flush",
        )

        # Normalize N_m3u8DL suffix variants after the process has stopped.
        final_actual_chunk_path = (
            find_nm3u8dl_output_path(
                chunk_path,
                not_before=run_launch_ts
            )
            or actual_chunk_path
        )

        result_chunk_path = final_actual_chunk_path
        result_chunk_name = os.path.basename(final_actual_chunk_path)

        if final_actual_chunk_path != chunk_path:
            try:
                os.replace(final_actual_chunk_path, chunk_path)

                log(
                    f"N_m3u8DL: normalized output filename → "
                    f"{os.path.basename(final_actual_chunk_path)} → {chunk_name}"
                )

                result_chunk_path = chunk_path
                result_chunk_name = chunk_name

            except OSError as e:
                log(
                    f"N_m3u8DL: could not normalize output filename: {e}",
                    level="WARN"
                )

        log("N_m3u8DL stopped for this run")
        state.nm3u8dl_stop_event = None

        return EngineResult(
            status=status,
            reason=reason if status != "ended" or proc.returncode != 0 else f"exit code {proc.returncode}",
            chunk_path=result_chunk_path,
            chunk_name=result_chunk_name,
            metrics={
                "selected_bitrate_kbps": state.nm3u8dl_selected_bitrate_kbps,
                "stall_flag": state.nm3u8dl_stall_flag,
                "ran_30s": ran_30s,
                "renewal_rollover": renewal_rollover,
                "rollover_reason": rollover_reason,
                "live_stream_ended": live_end_event.is_set(),
            },
        )
    finally:
        state.nm3u8dl_run_active = False
        state.nm3u8dl_event_queue = None

def run_nm3u8dl_cycle(state: RecorderState, backoff_sec, notify=None, deadline_ts: Optional[float] = None):
    """One N_m3u8DL worker cycle: start/monitor/stop and return run facts only."""

    log_run_start_banner(state, "N_m3u8DL")
    return start_nm3u8dl_to_chunk(state, notify=notify, deadline_ts=deadline_ts)
    
# ==============================================================================
# Orchestrator (chunk naming, acceptance, finalization, scheduling, main loop)
# ==============================================================================

def next_chunk_names(state: RecorderState):
    """Return the name/path for the next chunk number WITHOUT incrementing."""
    ensure_chunks_dir()
    name = f"chunk_{state.chunk_index + 1:03d}.ts"
    path = os.path.join(CHUNKS_DIR, name)
    return path, name
    
def cleanup_nm3u8dl_work_dir(chunk_name, context):
    """Remove N_m3u8DL's per-save-name working folder after a run."""
    if DOWNLOAD_MODE != ENGINE_NM3U8DL or not chunk_name:
        return True

    work_dir = os.path.join(
        CHUNKS_DIR,
        os.path.splitext(os.path.basename(chunk_name))[0],
    )

    if not os.path.exists(work_dir):
        return True

    for attempt in range(5):
        try:
            send2trash(work_dir)
            return True
        except FileNotFoundError:
            return True
        except PermissionError:
            if attempt < 4:
                time.sleep(0.5)
            else:
                log(
                    f"{context}: N_m3u8DL work-folder cleanup blocked → {os.path.basename(work_dir)}",
                    level="WARN",
                )
        except OSError as exc:
            log(
                f"{context}: N_m3u8DL work-folder cleanup failed for "
                f"{os.path.basename(work_dir)}: {exc}",
                level="WARN",
            )
            break

    return not os.path.exists(work_dir)


def maybe_accept_chunk(state: RecorderState, chunk_path, chunk_name, context):
    """
    Decide if chunk is good.
    If good: increment state.chunk_index, keep file, add to list.txt.
    If bad: delete file, DO NOT increment; number is reused next time.
    Returns True if GOOD, False otherwise.
    """
    if not os.path.exists(chunk_path):
        log(f"{context}: no chunk file found ({chunk_name})")

        work_dir_deleted = cleanup_nm3u8dl_work_dir(chunk_name, context)
        if not work_dir_deleted:
            # Never retry the same N save-name while stale working data still exists.
            state.chunk_index += 1
            log(
                f"{context}: reserving {chunk_name} number because "
                "N_m3u8DL work-folder cleanup was incomplete",
                level="WARN",
            )

        #state.stats["bad_chunks"] += 1
        return False

    dur, br = get_file_info(chunk_path)
    log(f"{context}: {chunk_name} → {dur:.1f}s, {br:.0f}kbps")

    # Dynamic bitrate threshold (last GOOD chunk)
    if DOWNLOAD_MODE == ENGINE_NM3U8DL:
        if (
            state.nm3u8dl_selected_bitrate_kbps > 0
            and not state.nm3u8dl_bitrate_is_fallback
        ):
            # Real N_m3u8DL bitrate can be used as chunk-quality evidence.
            min_expected_br = state.nm3u8dl_selected_bitrate_kbps * 0.40
        else:
            # Unknown real bitrate: the 4000 Kbps fallback exists only so live
            # stall monitoring can work. Do not let that synthetic value reject
            # a recorded chunk; keep the human in the loop for manual judgment.
            min_expected_br = None
    else:
        # ffmpeg mode: learn from the last accepted GOOD chunk (no hardcoded 500)
        last_good = state.stats.get("last_good_br_kbps")
        min_expected_br = (last_good * 0.40) if last_good else None

    bitrate_ok = (br > min_expected_br) if (min_expected_br is not None) else True
    if dur > STABILITY_SEC and bitrate_ok:
        # Update best resolution/fps from this chunk and track consistency
        w, h, fps = get_video_params(chunk_path)
        log(f"{context}: {chunk_name} params {w}x{h}@{fps}fps")

        if state.stats["good_chunks"] == 0:
            # first GOOD chunk defines reference params
            state.best_w, state.best_h, state.best_fps = w, h, fps
            log(f"{context}: FIRST video params {state.best_w}x{state.best_h}@{state.best_fps}fps")
        else:
            # if any GOOD chunk differs, mark params as mixed
            if (w, h, fps) != (state.best_w, state.best_h, state.best_fps):
                state.same_params = False
                if state.mixed_reason == "":
                    state.mixed_reason = "video params differ across chunks"
                log(f"{context}: VIDEO MIXED → {w}x{h}@{fps}fps (ref {state.best_w}x{state.best_h}@{state.best_fps}fps)")

        def _stream_sig(layout):
            return [
                (
                    s.get("codec_type"),
                    s.get("codec_name"),
                    int(s.get("width") or 0),
                    int(s.get("height") or 0),
                    s.get("pix_fmt") or None,
                    int(s.get("channels") or 0),
                    int(s.get("sample_rate") or 0),
                    s.get("channel_layout") or None,
                )
                for s in (layout or [])
            ]

        av_layout = get_av_stream_layout(chunk_path)

        if state.stream_layout_ref is None:
            state.stream_layout_ref = av_layout
        elif _stream_sig(av_layout) != _stream_sig(state.stream_layout_ref):
            state.same_params = False
            if state.mixed_reason == "":
                state.mixed_reason = (
                    f"A/V stream layout differs: got {_stream_sig(av_layout)} "
                    f"expected {_stream_sig(state.stream_layout_ref)}"
                )
            log(f"{context}: STREAM LAYOUT MIXED → {state.mixed_reason}")

        def _audio_sig(layout):
            return [
                (s.get("codec_name"), int(s.get("channels") or 0), int(s.get("sample_rate") or 0))
                for s in (layout or [])
            ]

        has_a, a_layout = get_audio_layout(chunk_path)

        if state.audio_expected is None:
            state.audio_expected = has_a
            if has_a:
                state.audio_layout_ref = a_layout
                # keep these for logs / filenames if you still use them elsewhere
                state.audio_codec_ref = a_layout[0].get("codec_name")
                state.audio_sr_ref = a_layout[0].get("sample_rate")
                state.audio_ch_ref = a_layout[0].get("channels")
        else:
            if state.audio_expected != has_a:
                state.same_params = False
                state.mixed_reason = "audio presence differs"
            elif state.audio_expected and _audio_sig(a_layout) != _audio_sig(state.audio_layout_ref):
                state.same_params = False
                state.mixed_reason = f"audio layout differs: got {_audio_sig(a_layout)} expected {_audio_sig(state.audio_layout_ref)}"
                log(f"{context}: AUDIO MIXED → {state.mixed_reason}")

        # For state.stats we still keep track of best height/fps
        if (h, fps) > (state.best_h, state.best_fps):  # prioritize higher height, then fps
            state.best_w, state.best_h, state.best_fps = w, h, fps
            log(f"{context}: NEW BEST video params {state.best_w}x{state.best_h}@{state.best_fps}fps")

        # Stats: good chunk
        state.stats["good_chunks"] += 1

        # Update ffmpeg baseline ONLY after accepting this chunk as GOOD
        state.stats["last_good_br_kbps"] = br

        # Media duration from file
        dur_media = dur

        # Wall-clock duration of this FFmpeg run
        run_start = state.stats.get("last_run_start", time.time())
        dur_run = time.time() - run_start

        # For "GOOD time" we want wall-clock FFmpeg running time
        state.stats["good_time"] += dur_run

        # Min/max based on media duration (what you see in the file)
        if state.stats["min_chunk"] is None or dur_media < state.stats["min_chunk"]:
            state.stats["min_chunk"] = dur_media
        if state.stats["max_chunk"] is None or dur_media > state.stats["max_chunk"]:
            state.stats["max_chunk"] = dur_media

        # Session timeline: store both media and run durations
        state.stats["sessions"].append({
            "start": run_start,
            "dur_media": dur_media,
            "dur_run": dur_run,
            "w": w,
            "h": h,
            "fps": fps,
            "name": chunk_name,
        })

        with open(LIST_FILE, "a", encoding="utf-8") as f:
            if state.chunk_index > 0:
                # from second GOOD chunk onward, insert a separator that matches
                # the first GOOD chunk's A/V stream order and codecs
                black_path = make_black_clip(state)

                if black_path is None:
                    state.same_params = False
                    if state.mixed_reason == "":
                        state.mixed_reason = "matching concat separator could not be created"
                    log(
                        f"{context}: separator unavailable → "
                        "final auto-concat will be skipped",
                        level="WARN",
                    )
                else:
                    f.write(f"file '{os.path.basename(black_path)}'\n")

            f.write(f"file '{chunk_name}'\n")

        state.chunk_index += 1
        size_mb = os.path.getsize(chunk_path) / (1024 * 1024)
        log(f"{context}: GOOD → keeping {chunk_name} ({size_mb:.1f}MB)")

        # The finished .ts is recorder-owned; N's temporary working folder is not needed.
        cleanup_nm3u8dl_work_dir(chunk_name, context)

        return True
    else:
        if not os.path.basename(chunk_name).startswith("black_"):
            state.stats["bad_chunks"] += 1
        log(f"{context}: BAD → deleting {chunk_name}")
        deleted = False
        for attempt in range(5):
            try:
                send2trash(chunk_path)   # instead of os.remove(chunk_path)
                deleted = True
                break
            except FileNotFoundError:
                deleted = True
                break
            except PermissionError:
                if attempt < 4:
                    time.sleep(0.5)
                else:
                    log(
                        f"{context}: BAD cleanup blocked by file lock → keeping {chunk_name}",
                        level="WARN",
                    )
            except OSError as exc:
                log(
                    f"{context}: BAD cleanup failed for {chunk_name}: {exc}",
                    level="WARN",
                )
                break

        work_dir_deleted = cleanup_nm3u8dl_work_dir(chunk_name, context)

        if (
            (not deleted and os.path.exists(chunk_path))
            or not work_dir_deleted
        ):
            # Do not reuse a save-name while either the output file or N's working
            # folder still exists, because the next run could inherit stale data.
            state.chunk_index += 1
            log(
                f"{context}: reserving {chunk_name} number because cleanup was incomplete",
                level="WARN",
            )

        return False


def nm3u8dl_critical_stop(state: RecorderState, reason: str):
    """Stop N recovery after one of its independent failure policies is exhausted."""
    log(reason, level="ERROR")

    # A lower-severity attention alarm may already be sounding. Replace it with
    # the critical stop alarm rather than letting shared-alarm exclusivity hide it.
    if shared_alarm_is_active(state):
        shared_alarm_stop(state)

    state.nm3u8dl_alarm_active = False
    state.nm3u8dl_post_ack_silent = False
    state.nm3u8dl_ack_requested = False
    alarm_critical(state)
    state.alarm_linger_until = time.time() + ALARM_LINGER_SEC
    state.stop_flag = True


def apply_orchestrator_policy(state: RecorderState, engine: RecorderEngine, result: EngineResult, backoff_sec: int) -> int:
    """Centralize boss-level decisions for retries, alarms, beeps, and sleeps."""

    if result.metrics.get("stop_requested"):
        return backoff_sec
    
    if result.status == "duration_reached":
        log_good_beep("recording_end_ok")
        beep_good(state)
        log(f"Max duration reached {format_current_duration_for_log(state)} → stopping loop.")
        state.stop_flag = True
        #return backoff_sec # # DO NOT return yet — we still want to accept the last chunk and write concat_list.txt

    ok = False
    if result.chunk_path and result.chunk_name:
        if engine.name == ENGINE_NM3U8DL:
            time.sleep(1)  # allow mux flush
        ok = maybe_accept_chunk(state, result.chunk_path, result.chunk_name, context="Run end")

    # Duration reached is a normal terminal exit.
    # The final chunk has now been accepted, so do not classify it as a recovery failure.
    if result.status == "duration_reached":
        return backoff_sec

    if (
        engine.name == ENGINE_NM3U8DL
        and result.metrics.get("live_stream_ended")
    ):
        if (
            NM3U8DL_SOURCE_MODE == "playlist"
            and get_nm3u8dl_playlist_lifecycle() == "LINEAR_TV"
        ):
            log(
                "N_m3u8DL explicit live-end confirmed on linear TV → "
                "keeping final chunk and resolving a fresh playlist source immediately."
            )
            # A live-end is a source/session lifecycle event, not a downloader
            # failure. Do not exclude or penalize the source. Also discard any
            # retained rollover candidate so the next run performs a full fresh scan.
            state.nm3u8dl_pending_source = None
            state.nm3u8dl_renewal_rollover_requested = False
            state.nm3u8dl_rollover_reason = None
            return 0

        log(
            "N_m3u8DL explicit live-end confirmed → "
            "stopping overall recorder after final chunk."
        )
        state.stop_flag = True

    if (
        engine.name == ENGINE_NM3U8DL
        and result.metrics.get("renewal_rollover")
    ):
        # A planned rollover is not proof of recovery. Keep every outstanding
        # N failure counter and any stall-alarm incident intact until the
        # replacement itself produces confirmed healthy growth.
        rollover_reason = (
            result.metrics.get("rollover_reason")
            or "authorization"
        )

        if rollover_reason == "quality_upgrade":
            log(
                "[RUN] Quality upgrade rollover complete → "
                "starting retained upgrade immediately..."
            )
        else:
            log(
                "[RUN] Authorization rollover complete → "
                "starting retained replacement immediately..."
            )

        return 0
    
    if engine.name == ENGINE_FFMPEG:
        unexpected_process_exit = bool(result.metrics.get("unexpected_process_exit"))

        if result.status == "stalled" or unexpected_process_exit:
            if unexpected_process_exit:
                stall_type = "process_exit"
            else:
                stall_type = result.metrics.get("stall_type") or ("hard" if "HARD" in result.reason else "soft")

            state.ffmpeg_stall_run_count += 1
            log(f"STALL_RUN_STOP type={stall_type} stall_run_count {state.ffmpeg_stall_run_count}/{FF_STALL_MAX_ATTEMPTS}", level="WARN")

            if state.ffmpeg_stall_run_count == 2:
                if not state.ffmpeg_alarm_active and not state.ffmpeg_post_ack_silent:
                    log("ALARM_STALL_START press A to acknowledge", level="WARN")
                    ffmpeg_alarm_start(state)

            if state.ffmpeg_stall_run_count >= FF_STALL_MAX_ATTEMPTS:
                if not state.ffmpeg_max_attempts_exhausted:
                    log("EXIT reason=ff_stall_max_attempts_exhausted", level="ERROR")
                    state.ffmpeg_post_ack_silent = False
                    state.ffmpeg_max_attempts_exhausted = True
                    alarm_critical(state)
                    state.alarm_linger_until = time.time() + ALARM_LINGER_SEC
                    state.stop_flag = True
                return backoff_sec

        if result.status == "no_stream":
            now = time.time()
            if state.ffmpeg_off_timer_start is None:
                state.ffmpeg_off_timer_start = now
            off_duration = int(now - state.ffmpeg_off_timer_start)
            log(f"OFF_TIMER={off_duration}s")
            if off_duration >= FFMPEG_OFF_ALARM_THRESHOLD_SEC:
                if not state.ffmpeg_off_alarm_active and not state.ffmpeg_off_post_ack_silent:
                    ffmpeg_off_alarm_start(state)
        else:
            if state.ffmpeg_off_alarm_active:
                ffmpeg_off_alarm_stop(state)
                log("ALARM_OFF_STOP reason=probe_success")
            state.ffmpeg_off_timer_start = None
            state.ffmpeg_off_post_ack_silent = False

    if engine.name == ENGINE_NM3U8DL and not state.stop_flag:
        selected_bitrate = result.metrics.get("selected_bitrate_kbps", 0)
        stall_flag = bool(result.metrics.get("stall_flag", False))
        manifest_dead = bool(
            result.metrics.get("manifest_dead", False)
            or (result.status == "no_stream" and selected_bitrate == 0)
        )
        no_file_appear = bool(
            result.metrics.get("no_file_appear", False)
            or (
                result.status == "no_stream"
                and selected_bitrate > 0
                and result.reason == "NO_FILE_APPEAR"
            )
        )
        confirmed_stall = (
            result.status == "stalled"
            and stall_flag
            and str(result.reason).startswith("STALL_")
        )
        manual_restart = (
            result.status == "stalled"
            and stall_flag
            and result.reason == "manual"
        )
        manual_exclude = (
            result.status == "stalled"
            and stall_flag
            and result.reason == "manual_exclude"
        )
        playlist_failure_type = None

        if manifest_dead:
            # Independent failure class: change only its own counter.
            state.manifest_fail_count += 1

            mode = "scheduled" if SCHEDULE_START else "manual"
            success_happened = state.stats.get("good_chunks", 0) > 0
            context = "recovery" if success_happened else "startup"

            if context == "recovery":
                max_attempts = MANIFEST_DEAD_MAX_ATTEMPTS_RECOVERY
            else:
                max_attempts = (
                    MANIFEST_DEAD_MAX_ATTEMPTS_SCHEDULED_STARTUP
                    if mode == "scheduled"
                    else MANIFEST_DEAD_MAX_ATTEMPTS_MANUAL_STARTUP
                )

            log("CLASSIFY MANIFEST_DEAD")
            if NM3U8DL_SOURCE_MODE == "playlist":
                log(
                    f"MANIFEST_DEAD mode={mode} context={context} "
                    f"diagnostic_count={state.manifest_fail_count}; "
                    "stream-failover policy owns retry"
                )
                playlist_failure_type = "MANIFEST_DEAD"
            else:
                log(
                    f"MANIFEST_DEAD mode={mode} context={context} "
                    f"attempt {state.manifest_fail_count}/{max_attempts}"
                )

            if (
                NM3U8DL_SOURCE_MODE != "playlist"
                and state.manifest_fail_count >= max_attempts
            ):
                if mode == "manual" and context == "startup":
                    log("MANIFEST_DEAD attempts exhausted → EXIT NO ALARM")
                    state.stop_flag = True
                else:
                    nm3u8dl_critical_stop(
                        state,
                        "MANIFEST_DEAD attempts exhausted → EXIT + ALARM_CRITICAL",
                    )

        elif no_file_appear:
            # Independent failure class: change only its own counter.
            state.no_file_appear_count += 1

            mode = "scheduled" if SCHEDULE_START else "manual"
            success_happened = state.stats.get("good_chunks", 0) > 0
            context = "recovery" if success_happened else "startup"

            if context == "recovery":
                max_attempts = NO_FILE_APPEAR_MAX_ATTEMPTS_RECOVERY
            else:
                max_attempts = (
                    NO_FILE_APPEAR_MAX_ATTEMPTS_SCHEDULED_STARTUP
                    if mode == "scheduled"
                    else NO_FILE_APPEAR_MAX_ATTEMPTS_MANUAL_STARTUP
                )

            deadline_s = int(result.metrics.get("file_appear_deadline_s", 0))
            log("CLASSIFY NO_FILE_APPEAR")
            if NM3U8DL_SOURCE_MODE == "playlist":
                log(
                    f"NO_FILE_APPEAR deadline={deadline_s}s mode={mode} "
                    f"context={context} diagnostic_count={state.no_file_appear_count}; "
                    "stream-failover policy owns retry"
                )
                playlist_failure_type = "NO_FILE_APPEAR"
            else:
                log(
                    f"NO_FILE_APPEAR deadline={deadline_s}s mode={mode} "
                    f"context={context} "
                    f"attempt {state.no_file_appear_count}/{max_attempts}"
                )

            if (
                NM3U8DL_SOURCE_MODE != "playlist"
                and state.no_file_appear_count >= max_attempts
            ):
                if mode == "manual" and context == "startup":
                    log("NO_FILE_APPEAR attempts exhausted → EXIT NO ALARM")
                    state.stop_flag = True
                else:
                    nm3u8dl_critical_stop(
                        state,
                        "NO_FILE_APPEAR attempts exhausted → EXIT + ALARM_CRITICAL",
                    )

        elif confirmed_stall:
            # The growth handler already incremented the confirmed stall EVENT.
            event_count = int(state.nm3u8dl_stall_event_count or 0)
            if NM3U8DL_SOURCE_MODE == "playlist":
                log(
                    f"STALL_RUN_STOP diagnostic_event_count={event_count}; "
                    "stream-failover policy owns retry",
                    level="WARN",
                )
                playlist_failure_type = "STALL"
            else:
                log(
                    f"STALL_RUN_STOP event_count "
                    f"{event_count}/{NM3U8DL_STALL_MAX_EVENTS}",
                    level="WARN",
                )

            if (
                NM3U8DL_SOURCE_MODE != "playlist"
                and event_count >= NM3U8DL_STALL_MAX_EVENTS
            ):
                nm3u8dl_critical_stop(
                    state,
                    "EXIT reason=nm3u8dl_stall_max_events_exhausted",
                )

        elif manual_exclude:
            # Manual X/Y is an operator decision about feed content/quality. It is
            # not a downloader failure and must not consume the automatic 2-try policy.
            _nm3u8dl_apply_manual_stream_exclusion(state)
            log(
                "CLASSIFY MANUAL_FEED_EXCLUSION → full playlist rescan "
                "without failure-counter change"
            )
            return 0

        elif manual_restart:
            # Manual R is an operator-requested process replacement. It does not
            # increment stall, manifest, no-file, or crash failure history.
            log("CLASSIFY MANUAL_RESTART → retry without failure-counter change")

        elif result.status == "ended":
            # N launched far enough to create/run its output, but exited without
            # EOS, planned rollover, duration end, external stop, or confirmed
            # stall. Keep this distinct from stall/manifest/no-file.
            state.nm3u8dl_crash_count += 1
            log("CLASSIFY ENGINE_CRASH", level="WARN")
            if NM3U8DL_SOURCE_MODE == "playlist":
                log(
                    f"ENGINE_CRASH diagnostic_count={state.nm3u8dl_crash_count} "
                    f"reason={result.reason}; stream-failover policy owns retry",
                    level="WARN",
                )
                playlist_failure_type = "ENGINE_CRASH"
            else:
                log(
                    f"ENGINE_CRASH attempt "
                    f"{state.nm3u8dl_crash_count}/{NM3U8DL_CRASH_MAX_ATTEMPTS} "
                    f"reason={result.reason}",
                    level="WARN",
                )

            if (
                NM3U8DL_SOURCE_MODE != "playlist"
                and state.nm3u8dl_crash_count >= NM3U8DL_CRASH_MAX_ATTEMPTS
            ):
                nm3u8dl_critical_stop(
                    state,
                    "ENGINE_CRASH attempts exhausted → EXIT + ALARM_CRITICAL",
                )

        elif ok:
            # GOOD chunk acceptance is useful media validation, but it is not the
            # health signal that clears an outstanding failure incident.
            backoff_sec = DEFAULT_MIN_BACKOFF

        elif result.status not in ("ok", "duration_reached"):
            # Defensive finite fallback: an unknown N failure must not become an
            # endless retry loop. Treat it as an engine crash, never as a stall.
            state.nm3u8dl_crash_count += 1
            if NM3U8DL_SOURCE_MODE == "playlist":
                log(
                    f"CLASSIFY ENGINE_CRASH reason=unclassified_{result.status} "
                    f"diagnostic_count={state.nm3u8dl_crash_count}; "
                    "stream-failover policy owns retry",
                    level="WARN",
                )
                playlist_failure_type = "ENGINE_CRASH"
            else:
                log(
                    f"CLASSIFY ENGINE_CRASH reason=unclassified_{result.status} "
                    f"attempt {state.nm3u8dl_crash_count}/{NM3U8DL_CRASH_MAX_ATTEMPTS}",
                    level="WARN",
                )
            if (
                NM3U8DL_SOURCE_MODE != "playlist"
                and state.nm3u8dl_crash_count >= NM3U8DL_CRASH_MAX_ATTEMPTS
            ):
                nm3u8dl_critical_stop(
                    state,
                    "ENGINE_CRASH attempts exhausted → EXIT + ALARM_CRITICAL",
                )

        if playlist_failure_type is not None and not state.stop_flag:
            _nm3u8dl_handle_playlist_stream_failure(
                state,
                playlist_failure_type,
            )
            # Both direct retry and post-rejection full scan should begin promptly.
            # Source resolution itself owns any longer no-source waiting interval.
            return 0

        backoff_sec = DEFAULT_MIN_BACKOFF
    else:
        if result.status != "no_stream":
            backoff_sec = DEFAULT_MIN_BACKOFF if ok else min(backoff_sec + 5, DEFAULT_MAX_BACKOFF)

    if result.status == "stalled":
        backoff_sec = 0

    if state.stop_flag:
        return backoff_sec

    wait_for = STREAM_OFF_SLEEP if result.status == "no_stream" else backoff_sec
    if wait_for is None:
        wait_for = backoff_sec

    log("")
    if result.status == "no_stream":
        log(f"[GAP] Stream OFF → wait {wait_for}s")
    else:
        log(f"[SLEEP] Waiting {wait_for}s before next check...")
    if not is_alarm_active(state):
        beep_bad(state)
    sleep_with_interrupts(state, wait_for)
    return backoff_sec

def write_summary_log(state, dur, finalized=True):
    """
    Write the persistent recording log under recorder_logs/recording_logs.
    """
    try:
        os.makedirs(RECORDING_LOGS_DIR, exist_ok=True)
        summary_path = os.path.join(
            RECORDING_LOGS_DIR,
            os.path.basename(os.path.splitext(FINAL_FILE)[0] + "_log.log"),
        )
        total_runtime = 0.0
        if state.stats["process_start"] and state.stats["process_end"]:
            total_runtime = state.stats["process_end"] - state.stats["process_start"]
        good_time = state.stats["good_time"]

        # Time buckets based on wall-clock
        sessions = sorted(state.stats["sessions"], key=lambda x: x["start"])
        if sessions:
            first_start = sessions[0]["start"]
            # Last session end in wall-clock
            last_end_run = sessions[-1]["start"] + sessions[-1].get("dur_run", 0.0)
        else:
            first_start = state.stats["process_start"] or 0.0
            last_end_run = state.stats["process_start"] or 0.0

        # 1) Time downloading GOOD chunks (wall-clock while FFmpeg was running and produced GOOD data)
        time_good = good_time  # already sum of dur_run

        # 2) Overhead: before first FFmpeg run + after last run/concat
        overhead = 0.0
        if state.stats["process_start"]:
            overhead += max(0.0, first_start - state.stats["process_start"])
        if state.stats["process_end"]:
            overhead += max(0.0, state.stats["process_end"] - last_end_run)

        # 3) Retry / offline / BAD = everything else
        time_retry_offline = max(0.0, total_runtime - time_good - overhead)

        lines = []
        if finalized:
            lines.append(f"Recording summary for {FINAL_FILE}")
        else:
            lines.append(f"Recording summary (NOT finalized - mixed params) for {FINAL_FILE}")
        lines.append("=" * 68)
        lines.append("")
        
        # Extra metadata
        lines.append(f"Recording Engine    : {DOWNLOAD_MODE}")
        # Determine mode: scheduled vs manual
        mode = "scheduled" if SCHEDULE_START else "manual"
        lines.append(f"Mode                : {mode}")
        if DOWNLOAD_MODE == "ENGINE_FFMPEG":
            lines.append(f"Live URL            : {LIVE_URL}")
        append_runtime_summary_lines(lines, state)
        lines.append("")

        lines.append("Overall")
        lines.append("-------")
        if state.stats["process_start"]:
            lines.append("Process start : " + datetime.fromtimestamp(state.stats["process_start"]).strftime("%Y-%m-%d %H:%M:%S"))
        if state.stats["process_end"]:
            lines.append("Process end   : " + datetime.fromtimestamp(state.stats["process_end"]).strftime("%Y-%m-%d %H:%M:%S"))
        lines.append("Total runtime : " + fmt_hms(total_runtime))
        lines.append("")
        total_media = sum(s.get("dur_media", 0.0) for s in sessions)

        lines.append("Download time")
        lines.append("-------------")
        lines.append("Active download time    : " + fmt_hms(time_good))
        label = "Final file duration     : " if finalized else "Estimated final duration : "
        lines.append(label + fmt_hms(dur))
        lines.append("Media downloaded        : " + fmt_hms(total_media))
        lines.append("Download efficiency     : " + fmt_hms(total_media) + " / " + fmt_hms(time_good))
        lines.append("")
        lines.append("Gaps and overhead")
        lines.append("-----------------")
        lines.append("Startup / tail overhead : " + fmt_hms(overhead))
        lines.append("Retry / offline / BAD   : " + fmt_hms(time_retry_offline))
        lines.append("")


        lines.append("Chunks")
        lines.append("------")
        lines.append(f"GOOD chunks count : {state.stats['good_chunks']}")
        lines.append(f"BAD chunks count  : {state.stats['bad_chunks']}")
        if state.stats["min_chunk"] is not None:
            lines.append("Min GOOD chunk    : " + fmt_hms(state.stats["min_chunk"]))
        if state.stats["max_chunk"] is not None:
            lines.append("Max GOOD chunk    : " + fmt_hms(state.stats["max_chunk"]))
        lines.append("")
        lines.append("Timeline")
        lines.append("--------")

        sessions = state.stats["sessions"]
        sessions = sorted(sessions, key=lambda x: x["start"])
        last_end = None
        playhead = 0.0          # position in final file (seconds)
        black_len = BLACK_LEN           # length of black separator clip you generate
        for idx, sess in enumerate(sessions, start=1):
            start_ts = sess["start"]
            dur_media = sess.get("dur_media", 0.0)
            dur_run   = sess.get("dur_run", 0.0)
            end_ts = start_ts + (dur_run if dur_run > 0 else dur_media)
            start_str = datetime.fromtimestamp(start_ts).strftime("%Y-%m-%d %H:%M:%S")
            end_str = datetime.fromtimestamp(end_ts).strftime("%Y-%m-%d %H:%M:%S")
            w = sess.get("w")
            h = sess.get("h")
            fps = sess.get("fps")
            chunk_name = sess.get("name", f"chunk_{idx:03d}.ts")

            # print gap BEFORE this session if there was a previous one
            if last_end is not None and start_ts > last_end:
                gap = start_ts - last_end
                lines.append(
                    f"Gap             {fmt_hms(gap)} with no data (retries / stream down)"
                )

            seg_start = playhead
            seg_end = playhead + dur_media

            lines.append(f"Segment {idx:02d}: {chunk_name}, {w}x{h}@{fps}fps")
            lines.append(f"  Connected at     : {start_str}")
            lines.append(f" Recorded for {fmt_hms(dur_media)} media; run {fmt_hms(dur_run)}")
            lines.append(f"  Disconnected at  : {end_str}")
            lines.append(f"  Final file range : {fmt_hms(seg_start)}–{fmt_hms(seg_end)}")

            # advance playhead: session duration + black separator after it
            playhead = seg_end + black_len
            last_end = end_ts
        
        lines.append("")
        lines.append("Chunk details")
        lines.append("-------------")
        for idx, sess in enumerate(sessions, start=1):
            name = sess.get("name", f"chunk_{idx:03d}.ts")
            dur_media = sess.get("dur_media", 0.0)
            dur_run = sess.get("dur_run", 0.0)
            size_str = "unknown"
            try:
                path = os.path.join(CHUNKS_DIR, name)
                if os.path.exists(path):
                    size_mb = os.path.getsize(path) / (1024 * 1024)
                    size_str = f"{size_mb:.1f}MB"
            except:
                pass
            w = sess.get("w")
            h = sess.get("h")
            fps = sess.get("fps")
            lines.append(f"Chunk {idx:02d}: {name}")
            lines.append(f"  Duration / run   : {fmt_hms(dur_media)} / {fmt_hms(dur_run)}")
            lines.append(f"  Size / video     : {size_str} / {w}x{h}@{fps}fps")

        with open(summary_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        # Line 1: written INTO the summary file (before terminal section)
        with open(summary_path, "a", encoding="utf-8", errors="replace") as out:
            out.write("\n")
            out.write(f"Wrote summary log to {summary_path}\n")

        log(f"Wrote summary log to {summary_path}")

        # Append terminal capture + get confirmation
        appended_bytes = append_termcap_to_summarylog(summary_path)

        # Line 2: written INTO the summary file (after terminal section)
        with open(summary_path, "a", encoding="utf-8", errors="replace") as out:
            out.write("\n")
            out.write(f"Appended terminal output OK ({appended_bytes} bytes from {TERMCAP_PATH})\n")

        log(f"Appended terminal output OK ({appended_bytes} bytes)")

        # Keep the unfiltered black-box diagnostic dump at the absolute bottom.
        append_raw_external_to_summarylog(summary_path)
        
    except Exception as e:
        log(f"Could not write summary log: {e}")
    
def finalize_recording(state: RecorderState):
    """
    Run ffmpeg concat to build FINAL_FILE, then robust checks and cleanup.
    """
    if not os.path.exists(LIST_FILE):
        log(f"No concat list file → {LIST_FILE} missing → no chunks to concat. Skipping finalize.")
        return

    if os.path.exists(FINAL_FILE):
        log(f"WARNING: {FINAL_FILE} already exists. Overwriting.")
        send2trash(FINAL_FILE)

    # Fallback if somehow nothing was set (should not happen if any GOOD chunk)
    if state.best_h == 0 or state.best_fps == 0:
        state.best_w, state.best_h, state.best_fps = 1920, 1080, 50
        log(f"No best params recorded, falling back to {state.best_w}x{state.best_h}@{state.best_fps}fps")

    # Decide whether we can avoid re-encoding
    if state.same_params:
        log(f"All GOOD chunks {state.best_w}x{state.best_h}@{state.best_fps}fps → fast concat (no re-encode) → {FINAL_FILE}")
        cmd = [
            "ffmpeg",
            "-y",
            "-f", "concat",
            "-safe", "0",
            
            "-nostats",           
            "-progress", "pipe:2",
            "-stats_period", str(FFMPEG_PROGRESS_PERIOD), 
    
            "-i", os.path.basename(LIST_FILE),
            
            # Keep all video + all audio streams from the concatenated input
            "-map", "0:v",
            "-map", "0:a?",
         
    
            "-c", "copy",
            os.path.join("..", FINAL_FILE),
        ]
    else:
        log("Mixed params → NOT auto-finalizing. Leaving chunks folder for manual analysis.")
        
        if state.mixed_reason:
            log(f"Reason: {state.mixed_reason}")
            
        # close out timing so totals are correct in summary
        state.stats["process_end"] = time.time()

        # Estimate final duration (no FINAL_FILE to probe)
        black_len = BLACK_LEN    # must match make_black_clip() duration
        sessions = state.stats.get("sessions", [])
        total_media = sum(s.get("dur_media", 0.0) for s in sessions)
        est_final_dur = total_media + black_len * max(0, len(sessions) - 1)

        write_summary_log(state, est_final_dur, finalized=False)
        return
        # commenting out the below for now. I want to manually handle mixed param cases.
        #log(f"Mixed params → re-encoding to {state.best_w}x{state.best_h}@{state.best_fps}fps → {FINAL_FILE}")
        #cmd = [
        #    "ffmpeg",
        #    "-y",
        #    "-f", "concat",
        #    "-safe", "0",
        #    "-i", "list.txt",
        #    "-vf", f"scale={state.best_w}:{state.best_h},fps={state.best_fps}",
        #    "-c:v", "libx264",
        #    "-preset", "veryfast",
        #    "-c:a", "aac",
        #    "-b:a", "128k",
        #   os.path.join("..", FINAL_FILE),
        #]
    # proc = subprocess.run(cmd, cwd=CHUNKS_DIR)
    rc = run_ffmpeg_with_capture(cmd, CHUNKS_DIR, tag="FFmpeg-FINAL")
    if rc != 0:
        log("FINALIZATION FAILED → FFmpeg could not create final MKV; keeping chunks folder for inspection.")
        return
        
    if not os.path.exists(FINAL_FILE):
        log("FINAL FILE MISSING after concat → keeping chunks.")
        return

    final_size = os.path.getsize(FINAL_FILE)
    if final_size <= 0:
        log("FINAL FILE SIZE 0 → keeping chunks.")
        return

    state.stats["process_end"] = time.time()
    dur, _ = get_file_info(FINAL_FILE)
    if dur <= 0:
        log("FINAL FILE has no valid duration → keeping chunks.")
        return

    log(
        f"FINAL OK → {FINAL_FILE}, duration {dur:.1f}s, size {final_size/1024/1024:.1f}MB"
    )

    # Write summary log next to FINAL_FILE
    write_summary_log(state, dur, finalized=True)
    
    # Playlist history is evidence-only, but uncommitted temp evidence must not
    # be destroyed by an otherwise successful media finalization. Existing media
    # finalization behavior remains unchanged; this is only one extra cleanup gate.
    if (
        getattr(state, "playlist_history_active", False)
        and not getattr(state, "playlist_history_commit_ok", False)
    ):
        log(
            "Playlist history is not fully committed → keeping chunks folder "
            "for manual handling.",
            level="WARN",
        )
        return

    # All checks passed → safe to delete chunks folder
    try:
        log(f"Sent chunks folder {CHUNKS_DIR} to Recycle Bin")
        send2trash(CHUNKS_DIR)       # instead of shutil.rmtree(CHUNKS_DIR)
    except Exception as e:
        log(f"Could not delete chunks folder {CHUNKS_DIR}: {e}")

def wait_until_start(state: RecorderState, selected_engine: RecorderEngine):
    """
    If SCHEDULE_START is set to a future time, wait until then.
    Format: "YYYY-MM-DD HH:MM" in local time.
    """
    if not SCHEDULE_START:
        # Manual start (no scheduling)
        mode = "manual"
        now = datetime.now()
        if RUN_DURATION_MIN is not None:
            end_time = now + timedelta(minutes=RUN_DURATION_MIN)
            log(f"RECORDING ENGINE  : {selected_engine.name}")
            log(f"Recording mode    : {mode}")
            log(f"Base name         : {BASE_NAME}")
            for line in selected_engine.summary_lines():
                log(line)
            log(f"Planned duration  : {RUN_DURATION_MIN} minutes")
            log(f"Expected end time : {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            log(f"RECORDING ENGINE  : {selected_engine.name}")
            log(f"Recording mode    : {mode}")
            log(f"Base name         : {BASE_NAME}")
            for line in selected_engine.summary_lines():
                log(line)
            log("Planned duration  : until stopped")
        return  # start immediately
        
    # Scheduled mode
    mode = "scheduled"
    try:
        target = datetime.strptime(SCHEDULE_START, "%Y-%m-%d %H:%M")
    except ValueError:
        log(f"Invalid SCHEDULE_START '{SCHEDULE_START}', starting immediately.")
        return
    # Header printed once
    if RUN_DURATION_MIN is not None:
        end_time = target + timedelta(minutes=RUN_DURATION_MIN)
        log(f"RECORDING ENGINE  : {selected_engine.name}")
        log(f"Recording mode    : {mode}")
        log(f"Base name         : {BASE_NAME}")
        for line in selected_engine.summary_lines():
            log(line)
        if selected_engine.name == ENGINE_NM3U8DL:
            planned_chunk_name = f"chunk_{state.chunk_index + 1:03d}.ts"
            planned_save_name = planned_chunk_name.replace(".ts", "")
            if NM3U8DL_SOURCE_MODE == "static":
                planned_cmd = f'{NM3U8DL_PART_A} --save-name "{planned_save_name}" {NM3U8DL_PART_B}'
                log(f"Planned command   : {planned_cmd}")
            else:
                log("Planned command   : playlist source will be resolved at recording start")
        log(f"Schedule start    : {SCHEDULE_START}")
        log(f"Planned duration  : {RUN_DURATION_MIN} minutes")
        log(f"Expected end time : {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        log(f"RECORDING ENGINE  : {selected_engine.name}")
        log(f"Recording mode    : {mode}")
        log(f"Base name         : {BASE_NAME}")
        for line in selected_engine.summary_lines():
            log(line)
        if selected_engine.name == ENGINE_NM3U8DL:
            planned_chunk_name = f"chunk_{state.chunk_index + 1:03d}.ts"
            planned_save_name = planned_chunk_name.replace(".ts", "")
            if NM3U8DL_SOURCE_MODE == "static":
                planned_cmd = f'{NM3U8DL_PART_A} --save-name "{planned_save_name}" {NM3U8DL_PART_B}'
                log(f"Planned command   : {planned_cmd}")
            else:
                log("Planned command   : playlist source will be resolved at recording start")
        log(f"Schedule start    : {SCHEDULE_START}")
        log("Planned duration  : until stopped")
        
    log(f"Scheduled start time parsed as {target.strftime('%Y-%m-%d %H:%M:%S')}")
    
    now = datetime.now()
    if now >= target:
        log(f"SCHEDULE_START {SCHEDULE_START} is in the past → starting immediately.")
        return

    last_wait_len = 0

    while True:
        if state.stop_flag:
            log("Cancelled by Ctrl-C")
            sys.exit(0)

        now = datetime.now()
        if now >= target:
            break

        remaining = max(0.0, (target - now).total_seconds())
        wait_text = (
            f"[WAIT] Waiting for start time {SCHEDULE_START} "
            f"(remaining {fmt_hms(remaining)})..."
        )
        pad = max(0, last_wait_len - len(wait_text))
        render_progress_line(wait_text, pad=pad, colorize=True)
        last_wait_len = len(wait_text)

        time.sleep(min(1.0, remaining))

    clear_progress_line()
    log(f"Reached scheduled start time {SCHEDULE_START} → starting recording.")
    

def ffmpeg_summary_lines():
    return [f"Live URL          : {LIVE_URL}"]


def nm3u8dl_summary_lines():
    if NM3U8DL_SOURCE_MODE != "playlist":
        return []

    return [
        f"Playlist group    : {NM3U8DL_PLAYLIST_GROUP.strip().upper()}",
        f"Match rule        : {get_nm3u8dl_playlist_match_description()}",
    ]


def build_engine_registry():
    """Assemble the per-engine descriptors used by the orchestrator.

    Keeping this registry small and declarative makes it easy to lift each
    engine into its own module later—only the callables in the descriptor would
    need to move, not the orchestration code that consumes them.
    """

    return {
        ENGINE_FFMPEG: RecorderEngine(
            name=ENGINE_FFMPEG,
            run_cycle=run_ffmpeg_cycle,
            description="Direct TS capture via FFmpeg",
            summary_lines_fn=ffmpeg_summary_lines,
        ),
        ENGINE_NM3U8DL: RecorderEngine(
            name=ENGINE_NM3U8DL,
            run_cycle=run_nm3u8dl_cycle,
            description="Segmented downloader via N_m3u8DL-RE",
            summary_lines_fn=nm3u8dl_summary_lines,
        ),
    }


def resolve_engine(mode: str) -> Optional[RecorderEngine]:
    """Resolve a mode string to its engine descriptor, logging if unknown."""

    registry = build_engine_registry()
    engine = registry.get(mode)
    if not engine:
        log(f"Unknown download mode '{mode}' → exiting.")
        sys.exit(1)
    return engine


def orchestrate_recording(state: RecorderState, engine: RecorderEngine):
    """Boss loop: decides what happens next; workers (engines) only report facts."""

    backoff_sec = DEFAULT_MIN_BACKOFF

    log("")
    log("Live stream recorder started...")
    log(f"Engine selected   : {engine.description}")
    init_termcap()
    init_raw_external_capture()
    
    state.start_time = time.time()
    state.stats["process_start"] = state.start_time
    state.original_duration_min = (float(RUN_DURATION_MIN) if RUN_DURATION_MIN is not None else None)
    state.deadline_ts = (state.start_time + RUN_DURATION_MIN * 60) if RUN_DURATION_MIN is not None else None
    state.original_deadline_ts = state.deadline_ts
    init_playlist_history(state)
    ensure_global_key_listener(state)

    def notify(event: str):
        # Engines report events; boss decides what to do.
        if event.startswith("good_beep:"):
            reason = event.split(":", 1)[1]
            log_good_beep(reason)
            beep_good(state)
            start_runtime_controls_reminder(state)
            return
        if event == "ffmpeg_recovery":
            # FFmpeg recovery => eligible to ring again after a prior ACK
            state.ffmpeg_stall_run_count = 0
            state.ffmpeg_post_ack_silent = False
            state.ffmpeg_post_ack_silent_type = None
            if state.ffmpeg_alarm_active:
                log("ALARM_STALL_STOP reason=recovery")
                ffmpeg_alarm_stop(state)
        elif event == "stall":
            if not is_alarm_active(state):
                beep_bad(state)

    while True:
        # Global ACK (for shared alarm cases like CRITICAL / FFmpeg alarms)
        if getattr(state, "alarm_ack_requested", False) and not getattr(state, "nm3u8dl_alarm_active", False):
            alarm_type = getattr(state, "shared_alarm_type", None)
            if state.ffmpeg_off_alarm_active or alarm_type == "ffmpeg_off":
                alarm_activity_id = getattr(
                    state,
                    "shared_alarm_activity_id",
                    None,
                )
                log(
                    "ALARM_ACK",
                    activity_id=alarm_activity_id,
                )
                log(
                    "ALARM_OFF_STOP reason=ack",
                    activity_id=alarm_activity_id,
                )
                ffmpeg_off_alarm_stop(state)
                state.ffmpeg_off_post_ack_silent = True
                state.alarm_ack_requested = False
                state.alarm_linger_until = None
            else:
                alarm_activity_id = getattr(
                    state,
                    "shared_alarm_activity_id",
                    None,
                )
                log(
                    "ALARM_ACK",
                    activity_id=alarm_activity_id,
                )
                if state.ffmpeg_alarm_active:
                    log(
                        "ALARM_STALL_STOP reason=ack",
                        activity_id=alarm_activity_id,
                    )
                    state.ffmpeg_alarm_active = False
                shared_alarm_stop(state)
                state.alarm_ack_requested = False
                state.ffmpeg_post_ack_silent = True
                state.ffmpeg_post_ack_silent_type = alarm_type
                state.ffmpeg_had_alarm_incident = True
                state.alarm_linger_until = None

        if state.stop_flag:
            cancel_console_interaction_if_active(
                state,
                "Runtime entry cancelled because the recorder is stopping.",
            )
            linger_until = getattr(state, "alarm_linger_until", None)
            if linger_until and time.time() < linger_until:
                time.sleep(0.2)
                continue
            break
            
        if recording_deadline_reached(state):
            result = EngineResult(
                status="duration_reached",
                reason="duration budget met",
                chunk_path=None,
                chunk_name=None,
            )
        else:
            # Stream availability check is boss work (shared across engines).
            # For N_m3u8DL, skip ffprobe-based checks because ffprobe does not use N_m3u8DL headers/keys.
            if engine.name == ENGINE_NM3U8DL:
                result = engine.run_cycle(state, backoff_sec, notify, state.deadline_ts)
            else:
                if not is_stream_alive():
                    result = EngineResult(
                        status="no_stream",
                        reason="stream not reachable",
                        chunk_path=None,
                        chunk_name=None,
                        metrics={},
                    )
                else:
                    if state.ffmpeg_off_alarm_active:
                        ffmpeg_off_alarm_stop(state)
                        log("ALARM_OFF_STOP reason=probe_success")
                    state.ffmpeg_off_timer_start = None
                    state.ffmpeg_off_post_ack_silent = False
                    result = engine.run_cycle(state, backoff_sec, notify, state.deadline_ts)

        finish_sound_snooze_for_completed_run(state)
        backoff_sec = apply_orchestrator_policy(state, engine, result, backoff_sec)

    stop_runtime_controls_reminder(state)
    try:
        finalize_playlist_history(state)
    except Exception as error:
        # History is evidence-only. Even an unexpected history bug must not prevent
        # the recorder from running its existing media finalization path.
        state.playlist_history_commit_ok = False
        log(
            "Playlist history finalization failed unexpectedly → media finalization "
            f"will continue; chunks will be retained for manual handling "
            f"({type(error).__name__}: {error})",
            level="WARN",
        )
    finalize_recording(state)


def main(state: RecorderState, engine: RecorderEngine):
    orchestrate_recording(state, engine)

# ==============================================================================
# Entrypoint
# ==============================================================================

if __name__ == "__main__":
    osSleep = None
    if os.name == "nt":
        osSleep = WindowsInhibitor()
        osSleep.inhibit()
    try:
        selected_engine = resolve_engine(DOWNLOAD_MODE)
        state = RecorderState()
        signal.signal(signal.SIGINT, make_signal_handler(state))

        run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(RECORDING_OUTPUT_DIR, exist_ok=True)
        FINAL_FILE = os.path.join(
            RECORDING_OUTPUT_DIR,
            f"{BASE_NAME}_{run_ts}.mkv",
        )

        final_base = os.path.splitext(FINAL_FILE)[0]
        CHUNKS_DIR = f"{final_base}_chunks"
        LIST_FILE = os.path.join(CHUNKS_DIR, "concat_list.txt")
        os.makedirs(CHUNKS_DIR, exist_ok=True)

        wait_until_start(state, selected_engine)
        main(state, selected_engine)
    finally:
        if osSleep:
            osSleep.uninhibit()

