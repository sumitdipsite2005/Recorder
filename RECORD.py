
"""
Static live-stream recorder orchestrator.

This is the main static recorder. It supports FFmpeg, N_m3u8DL-RE, yt-dlp,
and Streamlink recording workflows while keeping recorder-owned monitoring,
stall/recovery handling, alarms, chunk validation, logging, scheduling,
runtime controls, and finalization in one orchestrator.

Routine per-recording settings are kept separately in
``recorder_static_user_config.py`` and imported at startup. Normal recording
setup should therefore be done in that config file rather than by editing this
recorder.

Run this file directly to start the static recorder.
"""

from __future__ import annotations

import subprocess
import socket
import time
import os
import signal
import sys
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(errors="replace")
    except Exception:
        pass
if hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(errors="replace")
    except Exception:
        pass
import json
import shutil
import runpy
import queue
from dataclasses import dataclass, field
from datetime import datetime, timedelta
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
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
try:
    from send2trash import send2trash  # pyright: ignore[reportMissingModuleSource]
except ImportError:
    def send2trash(path):
        import shutil
        if os.path.isdir(path):
            shutil.rmtree(path, ignore_errors=True)
        elif os.path.isfile(path):
            try:
                os.remove(path)
            except Exception:
                pass
import re
import urllib.request
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse
from urllib.request import ProxyHandler, Request, build_opener, urlopen
import xml.etree.ElementTree as ET

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

STATIC_CONFIG_PATH = os.path.join(
    RECORDER_CONFIG_DIR,
    "recorder_static_user_config.py",
)

# Load config: try OneDrive path first; if not found or inaccessible, fallback to local workspace copy.
_static_user_config = None
try:
    if os.path.isfile(STATIC_CONFIG_PATH):
        _static_user_config = runpy.run_path(STATIC_CONFIG_PATH)
except Exception:
    pass

if _static_user_config is None:
    local_cfg = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "recorder_static_user_config.py",
    )
    if os.path.isfile(local_cfg):
        STATIC_CONFIG_PATH = local_cfg
        _static_user_config = runpy.run_path(STATIC_CONFIG_PATH)
    else:
        raise RuntimeError(
            f"Static recorder config not found: {STATIC_CONFIG_PATH}"
        )

DOWNLOAD_MODE = _static_user_config["DOWNLOAD_MODE"]
LIVE_URL = _static_user_config["LIVE_URL"]
FFMPEG_PART_A = _static_user_config["FFMPEG_PART_A"]
YTDLP_PART_A = _static_user_config["YTDLP_PART_A"]
STREAMLINK_PART_A = _static_user_config["STREAMLINK_PART_A"]
STREAMLINK_STREAM = _static_user_config["STREAMLINK_STREAM"]
NM3U8DL_PART_A = _static_user_config["NM3U8DL_PART_A"]
SCHEDULE_START = _static_user_config["SCHEDULE_START"]
RUN_DURATION_MIN = _static_user_config["RUN_DURATION_MIN"]
BASE_NAME = _static_user_config["BASE_NAME"]
REQUIRED_VPN_COUNTRY = _static_user_config.get("REQUIRED_VPN_COUNTRY", None)
VPN_SERVER_PRIORITY = _static_user_config.get("VPN_SERVER_PRIORITY", {})




# ==================== CONFIG FOR STREAMLINK DOWNLOAD ======================================================================

# Recorder-owned yt-dlp base used only for YouTube metadata probing while the
# Streamlink engine is selected. Probe-relevant HTTP context is copied from
# STREAMLINK_PART_A by a small adapter; Streamlink recording options remain untouched.
STREAMLINK_YTDLP_PROBE_BASE = 'yt-dlp --no-config --remote-components ejs:github'

# ==================== CONFIG FOR N_m3u8DL-RE DOWNLOAD  ===================================================================

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

# Part B: Static options (appended to command with dynamic chunk name)
# Browser-paced defaults (--thread-count 2 --live-take-count 2 --live-wait-time 3 --download-retry-count 2) to prevent 429 CDN ban storms
NM3U8DL_PART_B = '-M format=ts --live-real-time-merge --live-pipe-mux --thread-count 2 --live-take-count 2 --live-wait-time 3 --download-retry-count 2 --http-request-timeout 90 --select-video best --select-audio best'

# Network Guardrail & Rate-Limit Circuit Breaker constants
VPN_HOLD_TIMEOUT_SEC = 1800.0       # 30-minute standby hold on VPN collapse
INTERNET_HOLD_TIMEOUT_SEC = 1800.0  # 30-minute standby hold on general internet outage
TOKEN_HOLD_TIMEOUT_SEC = 1800.0     # 30-minute standby hold on stream token expiry
STARTUP_SILENT_WINDOW_SEC = 120.0
TOKEN_REPLACEMENT_PROBE_BYPASS_SEC = 45.0
TOKEN_HOLD_COUNTDOWN_LOG_SEC = 30.0

# Terminal output serialization lock (B4)
_CONSOLE_LOCK = threading.RLock()

# Module-level URL extractor for token discovery across commands and URLs (N5)
_URL_RE = re.compile(r'https?://[^\s"\']+')
RATE_LIMIT_COOLDOWN_SEC = 90.0      # 90-second backoff cooldown on HTTP 429 (Strike 1)
RATE_LIMIT_EXTENDED_COOLDOWN_SEC = 240.0  # 240-second in-place backoff cooldown on HTTP 429 (Strike 2)
RATE_LIMIT_ERROR_THRESHOLD = 3      # Trigger sustained rate-limit if >= 3 429s in window
RATE_LIMIT_WINDOW_SEC = 30.0        # Rolling window in seconds for 429 detection

# Wireproxy userspace tunneling constants
WIREPROXY_SOCKS5_PORT = 10808
WIREPROXY_HTTP_PORT = 10809

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
ENGINE_YTDLP = "yt-dlp"
ENGINE_STREAMLINK = "streamlink"

# N_m3u8DL Monitoring thresholds
NM3U8DL_SLOW_CHECK_THRESHOLD = 6   # consecutive slow checks before restart
NM3U8DL_SPEED_DEGRADATION_FACTOR = 0.05  # consider stalling if speed drops to ~5% of expected (300-400 Kbps range)
NM3U8DL_FALLBACK_BITRATE_KBPS = 4000 # Fallback only when N_m3u8DL detects a valid live refresh interval but does not report a numeric bitrate.
HARD_STALL_ZERO_BYTES = 1024

# Default stall tolerance
NM3U8DL_HARD_STALL_REQUIRED = 6 #6 FOR NORMAL #60 for AU PRIME #30 FOR 7AU (as below condition) #20 for JIO
NM3U8DL_SOFT_STALL_REQUIRED = 10 #10 FOR NORMAL #100 for AU PRIME #50 FOR 7AU (as below condition) #30 for JIO

# YoSpace streams need more tolerance during ad transitions.
if DOWNLOAD_MODE == "N_m3u8DL-RE" and "yospace" in NM3U8DL_PART_A.lower():
    NM3U8DL_HARD_STALL_REQUIRED = 30
    NM3U8DL_SOFT_STALL_REQUIRED = 50
    
NM3U8DL_STALL_MAX_EVENTS = 5
NM3U8DL_CRASH_MAX_ATTEMPTS = 2
CONTINUOUS_ALARM_MAX_SEC = 5 * 60
STREAM_CORRUPT_WINDOW_SEC = 1800.0  # Rolling 30-minute window of fatal (chunk-cut) corruption events (time.monotonic)
STREAM_CORRUPT_ALARM_THRESHOLD = 2  # Fatal cuts inside the window required to sound the persistent siren
NM3U8DL_CHECK_INTERVAL_MULTIPLIER = 2
NM3U8DL_MIN_GROWTH_CHECK_INTERVAL_SEC = 30  # Never judge N_m3u8DL file growth on a window shorter than 30s.
NM3U8DL_TERMINAL_PROGRESS_INTERVAL_SEC = 60  # Permanent Vid/Aud terminal snapshots; native N refresh remains unchanged.
NM3U8DL_HEALTH_LOG_INTERVAL_SEC = 60         # Match the other engines' normal Good speed log cadence.
NM3U8DL_ALARM_BEEP_HZ = 1500
NM3U8DL_ALARM_BEEP_MS = 500
NM3U8DL_ALARM_BEEP_GAP_SEC = 0.05
ALARM_SOUND_FILENAME = "alarm.wav"
ALARM_LINGER_SEC = 30
LIVE_OFF_ALARM_THRESHOLD_SEC = 300
LIVE_GROWTH_CHECK_INTERVAL = 30
LIVE_HEALTH_LOG_INTERVAL = 60
LIVE_HARD_STALL_ZERO_BYTES = 1024
LIVE_HARD_STALL_REQUIRED = 3
LIVE_SOFT_STALL_FACTOR = 0.05
LIVE_SOFT_STALL_REQUIRED = 6
LIVE_STALL_MAX_ATTEMPTS = 5
GOOD_BEEP_HEALTHY_REQUIRED = 1
RUNTIME_CONTROLS_REMINDER_SEC = 10 * 60

# Orchestrator backoff controls (shared)
DEFAULT_MIN_BACKOFF = 5
DEFAULT_MAX_BACKOFF = 30
STREAM_OFF_SLEEP = 15
BACKOFF_STEP_SEC = 5
BACKOFF_MAX_SEC = 15
HEALTHY_CHUNK_RESET_SEC = 60.0

# Stage 3 source-availability / yt-dlp waiting controls
SOURCE_PROBE_TIMEOUT_SEC = 20
YTDLP_METADATA_PROBE_TIMEOUT_SEC = 30
YTDLP_WAIT_FOR_VIDEO_SEC = 15
STREAMLINK_WAIT_FOR_STREAM_SEC = 15

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

    # Chunk / worker-run numbering & clean boundary tracking
    chunk_index: int = 0
    run_attempt_index: int = 0
    manual_stop_requested: bool = False
    normal_terminal_reason: Optional[str] = None
    nm3u8dl_renewal_rollover_requested: bool = False

    # Best video params + consistency flags
    best_w: int = 0
    best_h: int = 0
    best_fps: float = 0.0
    same_params: bool = True
    mixed_reason: str = ""
    last_finalization_failure: str = ""

    # Quality downgrade alarm & baseline tracking
    quality_downgrade_alarm_active: bool = False
    baseline_video_params: Optional[tuple] = None
    last_downgraded_quality_params: Optional[tuple] = None
    acknowledged_quality_params: Optional[tuple] = None

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

    # Shared FFmpeg/yt-dlp/Streamlink stall policy state
    live_stall_alarm_active: bool = False
    live_stall_post_ack_silent: bool = False
    live_stall_post_ack_silent_type: Optional[str] = None
    live_stall_run_count: int = 0
    live_stall_max_attempts_exhausted: bool = False

    # Shared FFmpeg/yt-dlp/Streamlink availability state
    live_off_timer_start: Optional[float] = None
    live_off_alarm_active: bool = False
    live_off_post_ack_silent: bool = False

    # FFmpeg health state
    ffmpeg_selected_bitrate_kbps_max: int = 0
    ffmpeg_healthy_consec_count: int = 0
    ffmpeg_beeped_this_healthy_streak: bool = False
    ffmpeg_stream_corrupt_alarm_active: bool = False
    ffmpeg_stream_corrupt_post_ack_silent: bool = False
    ffmpeg_stream_corrupt_incidents: int = 0  # fatal: chunk cut because of corruption
    ffmpeg_stream_corrupt_nonfatal_incidents: int = 0  # defect >= 8s that self-healed
    stream_corrupt_recycle_timestamps: list[float] = field(default_factory=list)
    stream_corrupt_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    # Network Guard / VPN Drop alarm state
    lenient_alarm_active: bool = False
    vpn_drop_post_ack_silent: bool = False
    internet_drop_active: bool = False
    internet_drop_post_ack_silent: bool = False
    network_guard_hold_active: bool = False
    internet_hold_active: bool = False
    token_hold_active: bool = False
    token_expired_post_ack_silent: bool = False
    token_hold_input_buffer: str = ""
    token_hold_submitted_url: Optional[str] = None

    # yt-dlp waiting / health state
    ytdlp_wait_for_video: bool = False
    ytdlp_download_started: bool = False
    ytdlp_live_status: Optional[str] = None
    ytdlp_selected_bitrate_kbps_max: int = 0
    ytdlp_healthy_consec_count: int = 0
    ytdlp_beeped_this_healthy_streak: bool = False

    # Streamlink waiting / health state (YouTube live_status is populated only for YouTube)
    streamlink_wait_for_video: bool = False
    streamlink_download_started: bool = False
    streamlink_live_status: Optional[str] = None
    streamlink_selected_bitrate_kbps_max: int = 0
    streamlink_healthy_consec_count: int = 0
    streamlink_beeped_this_healthy_streak: bool = False

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

    # Network Guardrail & Rate-Limit state
    network_guard: Optional[Any] = None
    wireproxy_manager: Optional[Any] = None
    network_guard_triggered: bool = False
    rate_limit_429_detected: bool = False
    network_guard_hold_active: bool = False
    lenient_alarm_active: bool = False
    
# EngineResult.metrics keys (stable contract):
# - ran_30s: bool  (engine stayed healthy for >= 30s)
# - stall_flag: bool  (N_m3u8DL only: file-growth stall detected)
# - selected_bitrate_kbps: int  (N_m3u8DL only: parsed bitrate; 0 means unknown)
# - refresh_interval_s: int  (N_m3u8DL only: parsed manifest refresh interval in seconds; 0 means unknown)
# - manifest_dead: bool  (N_m3u8DL only: True when bitrate==0 OR refresh_interval==0 after startup parsing)
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
MIN_ACCEPTED_CHUNK_SECONDS = 60  # minimum duration for mid-run flapping chunks
STABILITY_WALL_SEC = 25     # minimum wall-clock seconds for speed ratio evaluation
BURST_RATIO = 3.0           # media-to-wall speed threshold indicating a server replay burst
DUP_MAX_SEC = 180           # maximum media duration to consider as a replay burst
BLACK_LEN = 1.0             # use only integers
FINAL_AUDIO_MIN_KBPS = 192  # minimum audio bitrate for Tier 2 fallback re-encode
FINAL_AUDIO_KBPS_PER_CHANNEL = 64  # per-channel audio bitrate floor in kbps for Tier 2
FINAL_MIN_DURATION_RATIO = 0.90  # minimum ratio of expected media duration required to accept final output
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


def safe_print(*args, **kwargs):
    """Safely print to stdout/stderr, replacing unencodable characters on legacy charmap consoles."""
    try:
        print(*args, **kwargs)
    except (UnicodeEncodeError, LookupError):
        try:
            stream = kwargs.get("file") or sys.stdout
            encoding = getattr(stream, "encoding", None) or "ascii"
            sep = kwargs.get("sep", " ")
            text = sep.join(str(a) for a in args)
            safe_text = text.encode(encoding, errors="replace").decode(encoding, errors="replace")
            new_kwargs = dict(kwargs)
            new_kwargs.pop("sep", None)
            print(safe_text, **new_kwargs)
        except Exception:
            pass


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
            safe_print(separator)
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
        sys.stdout.write("\r" + display_text + (" " * max(0, int(pad))))
        sys.stdout.flush()
        TERMINAL_LAST_OUTPUT_WAS_BLANK = False
        set_progress_line_active(True, text)
        return True


def colorize_terminal_log_line(line: str) -> str:
    """
    Add restrained terminal colors.

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
    CYAN = "\033[1;96m"
    RUN_BLUE = "\033[1;94m"  # Bright blue: more distinct than cyan on black/white terminals.
    GRAY = "\033[90m"
    BOLD = "\033[1m"  # Bold styling with terminal default foreground color (adapts to light/dark themes).

    if re.search(r"=+\s+RUN \d+ START\b", line):
        return f"{RUN_BLUE}{line}{RESET}"

    # High-visibility full-line styling for stream integrity lifecycle (matching RECORD_DYNAMIC.py)
    if "STREAM_INTEGRITY: Defect detected" in line:
        return f"{YELLOW}{line}{RESET}"
    if "STREAM_INTEGRITY: Stream stabilized" in line or "STREAM_INTEGRITY: Stream recovered" in line:
        return f"{GREEN}{line}{RESET}"
    if "ALARM_STREAM_CORRUPT_START" in line:
        return f"{RED}{line}{RESET}"

    # High-visibility full-line styling for Network Guard & Rate-Limit lifecycle
    if "NETWORK_GUARD: Tracking" in line or "NETWORK_GUARD: Mismatch" in line or "RATE_LIMIT: Tracking" in line or "RATE_LIMIT: Cooldown remaining" in line:
        return f"{YELLOW}{line}{RESET}"
    if "NETWORK_GUARD: VPN connection restored" in line or "RATE_LIMIT: 90s cooldown completed" in line or "NETWORK_GUARD: Stream verified" in line:
        return f"{GREEN}{line}{RESET}"
    if "NETWORK_GUARD_HOLD" in line or "RATE_LIMIT_COOLDOWN" in line or "RATE-LIMIT COOLDOWN" in line or "ALARM_VPN_DROP" in line or "RATE_LIMIT_CRITICAL" in line:
        return f"{RED}{line}{RESET}"

    colored = line

    # Log severity
    colored = colored.replace(
        "[WARN]",
        f"{YELLOW}[WARN]{RESET}",
    )
    colored = colored.replace(
        "WARNING",
        f"{YELLOW}WARNING{RESET}",
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

    # Major workflow markers
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

    # Recovery / health
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

    # Startup and configuration header labels (bold default foreground, adapting to light/dark themes)
    for header_label in (
        "RECORDING ENGINE  :",
        "Host Platform     :",
        "Recording mode    :",
        "Base name         :",
        "Recording start   :",
        "Planned duration  :",
        "Expected end time :",
        "Schedule start    :",
        "Planned command   :",
        "Live URL          :",
        "Network Guard     :",
        "Proxy Endpoints   :",
        "Network Route     :",
        "Engine selected   :",
    ):
        colored = colored.replace(
            header_label,
            f"{BOLD}{header_label}{RESET}",
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
        safe_print(terminal_line, **print_kwargs)
        termcap_write(line)
        TERMINAL_LAST_OUTPUT_WAS_BLANK = output_is_blank


def log_run_start_banner(state: RecorderState, engine_label: str):
    """Show one numbered visual boundary for each actual worker run attempt."""
    state.run_attempt_index += 1
    log("")
    log(
        f"==================== RUN {state.run_attempt_index} START — "
        f"{engine_label} ===================="
    )
    guard = getattr(state, "network_guard", None)
    if guard:
        for g_line in guard.format_header_lines():
            log(g_line)
        if guard.is_armed:
            if guard.vpn_mode == "WIREPROXY":
                srv = getattr(guard.wireproxy_manager, "active_server", None) or "wireproxy"
                log(f"Network Route     : Connected [WIREPROXY] (Country: {guard.launch_country or guard.required_country} | Server: {srv} | IP: {guard.launch_ip or 'verified'})")
            else:
                prov = guard.active_system_vpn or "SYSTEM_VPN"
                log(f"Network Route     : Connected [{prov}] (Country: {guard.launch_country or guard.required_country} | Adapter: {guard.launch_interface or 'default'} | IP: {guard.launch_ip or 'verified'})")



def should_suppress_live_engine_output_line(line: str) -> bool:
    """Suppress repetitive live transport chatter that adds no recorder-state value."""
    s = line.strip()

    if "Opening '" in s and s.endswith("' for reading"):
        return True

    if "Cannot reuse HTTP connection for different host:" in s:
        return True

    if (
        "keepalive request failed for '" in s
        and "with error: 'Invalid argument' when opening url, retrying with new connection" in s
    ):
        return True

    return False
    
def ensure_chunks_dir():
    os.makedirs(CHUNKS_DIR, exist_ok=True)

def init_termcap(header_lines=None):
    """Create/initialize the temp terminal capture log inside CHUNKS_DIR."""
    global TERMCAP_PATH
    ensure_chunks_dir()
    TERMCAP_PATH = os.path.join(CHUNKS_DIR, "_terminal_capture.tmp.log")
    try:
        if header_lines:
            with open(TERMCAP_PATH, "w", encoding="utf-8", errors="replace") as f:
                for line in header_lines:
                    f.write(line + "\n")
        elif not os.path.isfile(TERMCAP_PATH):
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
    """Create/initialize the temp black-box output capture inside CHUNKS_DIR."""
    global RAW_EXTERNAL_PATH, RAW_EXTERNAL_COUNTER
    ensure_chunks_dir()
    RAW_EXTERNAL_PATH = os.path.join(CHUNKS_DIR, "_raw_external_output.tmp.log")
    if not os.path.isfile(RAW_EXTERNAL_PATH):
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
    try:
        result = subprocess.run(cmd, **kwargs)
    except subprocess.TimeoutExpired as exc:
        raw_external_write(invocation, getattr(exc, "stdout", None), "stdout")
        raw_external_write(invocation, getattr(exc, "stderr", None), "stderr")
        raw_external_end(invocation, status="timeout")
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
    try:
        output = subprocess.check_output(cmd, **kwargs)
    except subprocess.CalledProcessError as exc:
        raw_external_write(invocation, getattr(exc, "output", None), "stdout")
        raw_external_end(invocation, returncode=exc.returncode)
        raise
    except subprocess.TimeoutExpired as exc:
        raw_external_write(invocation, getattr(exc, "output", None), "stdout")
        raw_external_end(invocation, status="timeout")
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
    state.sound_snooze_mode = None
    state.sound_snooze_until_ts = None
    state.sound_snooze_run_attempt = None


def is_sound_snoozed(state: Optional[RecorderState], now_ts: Optional[float] = None) -> bool:
    """Return whether recorder-owned sounds are currently snoozed."""
    if state is None:
        return False

    mode = getattr(state, "sound_snooze_mode", None)
    now = time.time() if now_ts is None else float(now_ts)

    if mode == "timed":
        until_ts = getattr(state, "sound_snooze_until_ts", None)
        if until_ts is not None and now < float(until_ts):
            return True
        _clear_sound_snooze(state)
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
            getattr(state, "live_stall_alarm_active", False),
            getattr(state, "live_off_alarm_active", False),
            getattr(state, "quality_downgrade_alarm_active", False),
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
        if getattr(state, "token_hold_active", False):
            esc = getattr(state, "_hold_esc_state", None)
            if esc:
                if time.monotonic() - getattr(state, "_hold_esc_ts", 0.0) > 0.25:
                    esc = None                      # stale partial sequence: abandon it
                    state._hold_esc_state = None
            if esc == "esc":
                if ch in ("[", "O"):
                    state._hold_esc_state = "csi"
                    state._hold_esc_ts = time.monotonic()
                    return
                state._hold_esc_state = None        # not a sequence: fall through and process ch normally
            elif esc == "csi":
                if "@" <= ch <= "~":
                    state._hold_esc_state = None
                    return
                if ch < " ":                        # control byte: sequence aborted, reprocess ch
                    state._hold_esc_state = None
                else:
                    state._hold_esc_ts = time.monotonic()
                    return                          # parameter/intermediate byte (0x20-0x3F)
            if ch == "\x1b":
                state._hold_esc_state = "esc"
                state._hold_esc_ts = time.monotonic()
                return
            # N7: \b \b cannot rewind across terminal-wrapped lines (>500 chars); accepted console UI limitation.
            if ch == "\x15":                    # Ctrl-U clears the line
                try:
                    with _CONSOLE_LOCK:
                        buf = getattr(state, "token_hold_input_buffer", "")
                        if buf:
                            state.token_hold_input_buffer = ""
                            sys.stdout.write("\b \b" * len(buf))
                            sys.stdout.flush()
                except (OSError, ValueError):
                    pass
                return

            # Hotkey 'a' / 'A' on empty buffer acknowledges token hold alarm (silences siren).
            # Accepted risk: a pasted command starting with 'a'/'A' on an empty buffer triggers ack;
            # standard stream URLs (http...) and commands (N_m3u8DL-RE) do not start with 'a'/'A'.
            if not getattr(state, "token_hold_input_buffer", "") and ch in ("a", "A"):
                state.alarm_ack_requested = True
                acknowledge_token_expired_alarm(state)
                return

            if ch in ("\r", "\n"):
                state._hold_esc_state = None
                state._hold_esc_ts = 0.0
                if getattr(state, "_last_submit_ch", "") == "\r" and ch == "\n":
                    state._last_submit_ch = ""
                    return
                state._last_submit_ch = ch
                try:
                    with _CONSOLE_LOCK:
                        buf = getattr(state, "token_hold_input_buffer", "").strip()
                        if buf:
                            state.token_hold_submitted_url = buf
                            state.token_hold_input_buffer = ""
                            sys.stdout.write("\r\n")
                            sys.stdout.flush()
                except (OSError, ValueError):
                    pass
                return
            state._last_submit_ch = ""

            if ch in ("\x08", "\x7f"):
                try:
                    with _CONSOLE_LOCK:
                        buf = getattr(state, "token_hold_input_buffer", "")
                        if buf:
                            state.token_hold_input_buffer = buf[:-1]
                            sys.stdout.write("\b \b")
                            sys.stdout.flush()
                except (OSError, ValueError):
                    pass
                return

            if ch == "\t":
                ch = " "
            if ch.isprintable():
                try:
                    with _CONSOLE_LOCK:
                        # Buffer mutation and echo are atomic vs. _hold_log redraws (D2).
                        state.token_hold_input_buffer = getattr(state, "token_hold_input_buffer", "") + ch
                        sys.stdout.write(ch)
                        sys.stdout.flush()
                except (OSError, ValueError):
                    pass
                return

            return

        if handle_runtime_control_key(state, ch):
            return

        if ch in ("a", "A"):
            t_alarm = getattr(state, "shared_alarm_thread", None)
            alarm_active = (
                (t_alarm is not None and getattr(t_alarm, "is_alive", lambda: False)())
                or getattr(state, "live_off_alarm_active", False)
                or getattr(state, "live_stall_alarm_active", False)
                or getattr(state, "nm3u8dl_alarm_active", False)
                or getattr(state, "ffmpeg_stream_corrupt_alarm_active", False)
                or getattr(state, "lenient_alarm_active", False)
                or getattr(state, "network_guard_hold_active", False)
                or getattr(state, "internet_drop_active", False)
                or getattr(state, "internet_hold_active", False)
                or getattr(state, "token_hold_active", False)
                or getattr(state, "quality_downgrade_alarm_active", False)
                or getattr(state, "shared_alarm_type", None) in ("vpn_drop", "internet_drop", "token_expired", "critical", "quality_downgrade")
            )

            if alarm_active:
                # ACK the N_m3u8DL stall incident immediately. The alarm
                # may still be sounding while the next process is starting.
                if getattr(state, "nm3u8dl_alarm_active", False):
                    acknowledge_nm3u8dl_stall_alarm(state)
                elif (
                    getattr(state, "live_stall_alarm_active", False)
                    or getattr(state, "shared_alarm_type", None) == "live_stall"
                ):
                    acknowledge_live_stall_alarm(state)
                elif (
                    getattr(state, "ffmpeg_stream_corrupt_alarm_active", False)
                    or getattr(state, "shared_alarm_type", None) == "stream_corrupt"
                ):
                    acknowledge_stream_corrupt_alarm(state)
                elif (
                    getattr(state, "quality_downgrade_alarm_active", False)
                    or getattr(state, "shared_alarm_type", None) == "quality_downgrade"
                ):
                    acknowledge_quality_downgrade_alarm(state)
                elif (
                    getattr(state, "lenient_alarm_active", False)
                    or getattr(state, "network_guard_hold_active", False)
                    or getattr(state, "shared_alarm_type", None) == "vpn_drop"
                ):
                    acknowledge_vpn_drop_alarm(state)
                elif (
                    getattr(state, "internet_drop_active", False)
                    or getattr(state, "internet_hold_active", False)
                    or getattr(state, "shared_alarm_type", None) == "internet_drop"
                ):
                    acknowledge_internet_drop_alarm(state)
                elif (
                    getattr(state, "token_hold_active", False)
                    or getattr(state, "shared_alarm_type", None) == "token_expired"
                ):
                    acknowledge_token_expired_alarm(state)
                else:
                    # OFF / critical alarms keep their existing boss-loop ACK path.
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
                drained = 0
                while msvcrt.kbhit() and drained < 4096:
                    ch = msvcrt.getwch()
                    drained += 1
                    # N5: \xe0 is both Windows scan-code prefix and latin 'à'. Stream URLs are ASCII, so swallow scan-code is accepted platform risk.
                    if ch in ("\x00", "\xe0"):      # special key prefix: swallow scan code
                        if msvcrt.kbhit():
                            msvcrt.getwch()
                        continue
                    handle_key(ch)
            except Exception:
                pass

            time.sleep(0.02)

        return

    # macOS & Linux terminal listener
    if sys.platform in ("darwin", "linux"):
        try:
            import codecs
            import os
            import select
            import termios
            import tty
            import atexit
            import io

            fd = sys.stdin.fileno()
            old_settings = termios.tcgetattr(fd)

        except (termios.error, io.UnsupportedOperation, AttributeError, Exception) as exc:
            log(f"Terminal keyboard listener unavailable: {exc}", level="WARN")
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
            decoder = codecs.getincrementaldecoder("utf-8")("ignore")

            while not stop_event.is_set():
                try:
                    ready, _, _ = select.select([fd], [], [], 0.05)

                    if ready:
                        raw = os.read(fd, 4096)
                        if not raw:
                            break
                        text = decoder.decode(raw)
                        text = text.replace("\x1b[200~", "").replace("\x1b[201~", "")
                        for ch in text:
                            handle_key(ch)

                except Exception:
                    time.sleep(0.05)

        except (termios.error, io.UnsupportedOperation, AttributeError, OSError, Exception) as exc:
            log(f"Terminal keyboard listener unavailable: {exc}", level="WARN")
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
                                proc.kill()
                            return

                        if is_sound_snoozed(state):
                            proc.terminate()
                            try:
                                proc.wait(timeout=2)
                            except subprocess.TimeoutExpired:
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
    elif alarm_type == "live_stall":
        state.live_stall_alarm_active = False
        state.live_stall_post_ack_silent = True
        state.live_stall_post_ack_silent_type = "live_stall"
        log("ALARM_STALL_STOP reason=timeout", level="WARN")
    elif alarm_type == "live_off":
        state.live_off_alarm_active = False
        state.live_off_post_ack_silent = True
        log("ALARM_OFF_STOP reason=timeout", level="WARN")
    elif alarm_type == "stream_corrupt":
        state.ffmpeg_stream_corrupt_alarm_active = False
        state.ffmpeg_stream_corrupt_post_ack_silent = True
        log("ALARM_CORRUPT_STOP reason=timeout", level="WARN")
    elif alarm_type == "vpn_drop":
        state.vpn_drop_post_ack_silent = True
        log("ALARM_VPN_STOP reason=timeout", level="WARN")
    elif alarm_type == "internet_drop":
        state.internet_drop_post_ack_silent = True
        log("ALARM_INTERNET_STOP reason=timeout", level="WARN")
    elif alarm_type == "token_expired":
        state.token_expired_post_ack_silent = True
        log("ALARM_TOKEN_EXPIRED_STOP reason=timeout", level="WARN")
    elif alarm_type == "quality_downgrade":
        state.quality_downgrade_alarm_active = False
        if getattr(state, "last_downgraded_quality_params", None) is not None:
            state.acknowledged_quality_params = state.last_downgraded_quality_params
        log("ALARM_QUALITY_DOWNGRADE_STOP reason=timeout", level="WARN")

    shared_alarm_stop(state)


def shared_alarm_start(state, alarm_type: str, incident: Optional[int] = None):
    # ACK/timeout silences a continuing incident until its recovery rule clears it.
    if alarm_type == "live_stall" and getattr(state, "live_stall_post_ack_silent", False):
        return
    if alarm_type == "nm3u8dl_stall" and getattr(state, "nm3u8dl_post_ack_silent", False):
        return
    if alarm_type == "stream_corrupt" and getattr(state, "ffmpeg_stream_corrupt_post_ack_silent", False):
        return
    if alarm_type == "vpn_drop" and getattr(state, "vpn_drop_post_ack_silent", False):
        return
    if alarm_type == "internet_drop" and getattr(state, "internet_drop_post_ack_silent", False):
        return
    if alarm_type == "token_expired" and getattr(state, "token_expired_post_ack_silent", False):
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
            t.join(timeout=1.0)
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


def live_stall_alarm_start(state):
    shared_alarm_start(state, alarm_type="live_stall", incident=None)
    state.live_stall_alarm_active = (
        getattr(state, "shared_alarm_type", None) == "live_stall"
        and shared_alarm_is_active(state)
    )

def live_stall_alarm_stop(state):
    state.live_stall_alarm_active = False
    shared_alarm_stop(state)

def acknowledge_live_stall_alarm(state) -> bool:
    """Acknowledge the shared FFmpeg/yt-dlp live-stall alarm immediately."""
    alarm_type = getattr(state, "shared_alarm_type", None)
    if not (
        getattr(state, "live_stall_alarm_active", False)
        or alarm_type == "live_stall"
    ):
        return False

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
        "ALARM_STALL_STOP reason=ack",
        activity_id=alarm_activity_id,
    )
    live_stall_alarm_stop(state)
    state.live_stall_post_ack_silent = True
    state.live_stall_post_ack_silent_type = "live_stall"
    state.alarm_ack_requested = False
    state.alarm_linger_until = None
    return True


def acknowledge_stream_corrupt_alarm(state) -> bool:
    """Acknowledge the live stream corruption alarm immediately.

    Flags are mutated under stream_corrupt_lock; the (up to 1s) alarm thread join in
    shared_alarm_stop() runs only after the lock is released.
    """
    with state.stream_corrupt_lock:
        alarm_type = getattr(state, "shared_alarm_type", None)
        if not (
            getattr(state, "ffmpeg_stream_corrupt_alarm_active", False)
            or alarm_type == "stream_corrupt"
        ):
            return False

        alarm_activity_id = getattr(state, "shared_alarm_activity_id", None)
        state.ffmpeg_stream_corrupt_alarm_active = False
        state.ffmpeg_stream_corrupt_post_ack_silent = True
        state.alarm_ack_requested = False
        state.alarm_linger_until = None

    log("ALARM_ACK", activity_id=alarm_activity_id)
    log("ALARM_CORRUPT_STOP reason=ack", activity_id=alarm_activity_id)
    shared_alarm_stop(state)
    return True


def _prune_locked(state, now: float) -> None:
    """Expire stale fatal-cut timestamps. Caller MUST hold state.stream_corrupt_lock.

    When the rolling window transitions from non-empty to empty (30 clean monotonic
    minutes), the operator silence is cleared so the next episode re-arms the siren.
    """
    timestamps = state.stream_corrupt_recycle_timestamps
    was_non_empty = bool(timestamps)
    timestamps[:] = [t for t in timestamps if not (now - t > STREAM_CORRUPT_WINDOW_SEC)]
    if was_non_empty and not timestamps:
        state.ffmpeg_stream_corrupt_post_ack_silent = False
        log(
            f"STREAM_INTEGRITY: No fatal cuts for {int(STREAM_CORRUPT_WINDOW_SEC)}s. "
            "Corruption window cleared; alarm re-armed.",
            level="INFO",
        )


def prune_stream_corrupt_window(state, now: Optional[float] = None) -> int:
    """Prune the rolling fatal-cut window and return the remaining count."""
    if now is None:
        now = time.monotonic()
    with state.stream_corrupt_lock:
        _prune_locked(state, now)
        return len(state.stream_corrupt_recycle_timestamps)


def register_stream_corrupt_recycle(state, now: Optional[float] = None) -> bool:
    """Record one fatal (chunk-cut) corruption event.

    Single owner of ffmpeg_stream_corrupt_incidents. Starts the escalation siren when the
    rolling window holds >= STREAM_CORRUPT_ALARM_THRESHOLD cuts and the operator has not
    silenced the episode. Returns True when the caller should play the short Bad Beep
    (no alarm of any kind is sounding).
    """
    if now is None:
        now = time.monotonic()
    with state.stream_corrupt_lock:
        # Prune first so a window-empty re-arm is visible to the silence check below.
        _prune_locked(state, now)
        state.stream_corrupt_recycle_timestamps.append(now)
        state.ffmpeg_stream_corrupt_incidents += 1
        count = len(state.stream_corrupt_recycle_timestamps)

        if (
            count >= STREAM_CORRUPT_ALARM_THRESHOLD
            and not getattr(state, "ffmpeg_stream_corrupt_post_ack_silent", False)
        ):
            log(
                f"ALARM_STREAM_CORRUPT_START: {count} fatal stream cuts within "
                f"{int(STREAM_CORRUPT_WINDOW_SEC)}s. Press 'A' to silence.",
                level="ERROR",
            )
            shared_alarm_start(state, alarm_type="stream_corrupt", incident=None)
            if getattr(state, "shared_alarm_type", None) == "stream_corrupt":
                state.ffmpeg_stream_corrupt_alarm_active = True

        return not is_alarm_active(state)


def play_stream_corrupt_bad_beep_async(state) -> None:
    """Play the short Bad Beep on a daemon thread so the main loop never blocks."""
    threading.Thread(
        target=beep_bad,
        args=(state,),
        daemon=True,
        name="stream_corrupt_beep",
    ).start()


def acknowledge_vpn_drop_alarm(state) -> bool:
    """Acknowledge the VPN drop alarm immediately."""
    alarm_type = getattr(state, "shared_alarm_type", None)
    if not (
        getattr(state, "lenient_alarm_active", False)
        or getattr(state, "network_guard_hold_active", False)
        or alarm_type == "vpn_drop"
    ):
        return False

    alarm_activity_id = getattr(state, "shared_alarm_activity_id", None)
    log("ALARM_ACK", activity_id=alarm_activity_id)
    log("ALARM_VPN_DROP_STOP reason=ack", activity_id=alarm_activity_id)
    state.vpn_drop_post_ack_silent = True
    state.alarm_ack_requested = False
    state.alarm_linger_until = None
    shared_alarm_stop(state)
    return True


def acknowledge_internet_drop_alarm(state) -> bool:
    """Acknowledge the internet drop alarm immediately."""
    alarm_type = getattr(state, "shared_alarm_type", None)
    if not (
        getattr(state, "internet_drop_active", False)
        or getattr(state, "internet_hold_active", False)
        or alarm_type == "internet_drop"
    ):
        return False

    alarm_activity_id = getattr(state, "shared_alarm_activity_id", None)
    log("ALARM_ACK", activity_id=alarm_activity_id)
    log("ALARM_INTERNET_DROP_STOP reason=ack", activity_id=alarm_activity_id)
    state.internet_drop_post_ack_silent = True
    state.alarm_ack_requested = False
    state.alarm_linger_until = None
    shared_alarm_stop(state)
    return True


def acknowledge_token_expired_alarm(state) -> bool:
    """Acknowledge the token expired alarm immediately."""
    alarm_type = getattr(state, "shared_alarm_type", None)
    if not (
        getattr(state, "token_hold_active", False)
        or alarm_type == "token_expired"
    ):
        return False

    alarm_activity_id = getattr(state, "shared_alarm_activity_id", None)
    log("ALARM_ACK", activity_id=alarm_activity_id)
    log("ALARM_TOKEN_EXPIRED_STOP reason=ack", activity_id=alarm_activity_id)
    state.token_expired_post_ack_silent = True
    state.alarm_ack_requested = False
    state.alarm_linger_until = None
    shared_alarm_stop(state)
    return True


def quality_downgrade_alarm_start(state: RecorderState) -> None:
    shared_alarm_start(state, alarm_type="quality_downgrade", incident=None)
    state.quality_downgrade_alarm_active = (
        getattr(state, "shared_alarm_type", None) == "quality_downgrade"
    )
    if state.quality_downgrade_alarm_active:
        log("ALARM_QUALITY_DOWNGRADE_START", level="WARN")


def quality_downgrade_alarm_stop(state: RecorderState, reason: str = "recovered") -> None:
    was_active = getattr(state, "quality_downgrade_alarm_active", False) or (
        getattr(state, "shared_alarm_type", None) == "quality_downgrade"
    )
    state.quality_downgrade_alarm_active = False
    if getattr(state, "shared_alarm_type", None) == "quality_downgrade":
        shared_alarm_stop(state)
    if was_active:
        log(f"ALARM_QUALITY_DOWNGRADE_STOP reason={reason}", level="INFO")


def acknowledge_quality_downgrade_alarm(state: RecorderState) -> bool:
    alarm_type = getattr(state, "shared_alarm_type", None)
    if not (
        getattr(state, "quality_downgrade_alarm_active", False)
        or alarm_type == "quality_downgrade"
    ):
        return False

    alarm_activity_id = getattr(state, "shared_alarm_activity_id", None)
    state.quality_downgrade_alarm_active = False
    if getattr(state, "last_downgraded_quality_params", None) is not None:
        state.acknowledged_quality_params = state.last_downgraded_quality_params
    state.alarm_ack_requested = False
    state.alarm_linger_until = None
    log("ALARM_ACK", activity_id=alarm_activity_id)
    log("ALARM_QUALITY_DOWNGRADE_STOP reason=ack", activity_id=alarm_activity_id)
    if alarm_type == "quality_downgrade":
        shared_alarm_stop(state)
    return True


def live_stall_incident_active(state) -> bool:
    return bool(
        state.live_stall_run_count > 0
        or state.live_stall_alarm_active
        or state.live_stall_post_ack_silent
    )

def live_off_alarm_start(state):
    if shared_alarm_is_active(state):
        return
    shared_alarm_start(state, alarm_type="live_off", incident=None)
    state.live_off_alarm_active = (
        getattr(state, "shared_alarm_type", None) == "live_off"
        and shared_alarm_is_active(state)
    )
    if state.live_off_alarm_active:
        log("ALARM_OFF_START")

def live_off_alarm_stop(state):
    state.live_off_alarm_active = False
    shared_alarm_stop(state)

def reset_live_off_state(state, reason: str):
    if state.live_off_alarm_active:
        live_off_alarm_stop(state)
        log(f"ALARM_OFF_STOP reason={reason}")
    state.live_off_timer_start = None
    state.live_off_post_ack_silent = False
    
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
        if getattr(state, "wireproxy_manager", None):
            state.wireproxy_manager.stop()
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
            safe_print(_console_color(text, color) if use_color else text)
            termcap_write(text)
            TERMINAL_LAST_OUTPUT_WAS_BLANK = (text == "")


def _console_print_timestamped_line(message: str):
    global TERMINAL_LAST_OUTPUT_WAS_BLANK

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    plain = f"{ts} [INFO] {message}"
    with CONSOLE_OUTPUT_LOCK:
        safe_print(colorize_terminal_log_line(plain))
        termcap_write(plain)
        TERMINAL_LAST_OUTPUT_WAS_BLANK = (str(message) == "")


def _print_runtime_controls_reminder():
    activity_id = new_terminal_activity("runtime_controls_reminder")

    with CONSOLE_OUTPUT_LOCK:
        _prepare_terminal_activity(activity_id)
        clear_progress_line()
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        plain = f"{ts} [INFO] Press I for recording information & controls"
        safe_print(colorize_terminal_log_line(plain))
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

        safe_print(line)
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
    _console_write("\r\n" + message + "\r\n\r\n")
    _finish_console_interaction(state)


def cancel_console_interaction_if_active(state: RecorderState, message: str):
    if getattr(state, "console_input_mode", None) is not None:
        _console_cancel_interaction(state, message)


def _set_sound_snooze(state: RecorderState, mode: Optional[str]):
    if mode == "timed":
        state.sound_snooze_mode = "timed"
        state.sound_snooze_until_ts = time.time() + (15 * 60.0)
        state.sound_snooze_run_attempt = None
    elif mode == "run":
        state.sound_snooze_mode = "run"
        state.sound_snooze_until_ts = None
        state.sound_snooze_run_attempt = int(getattr(state, "run_attempt_index", 0) or 0)
    elif mode == "recording":
        state.sound_snooze_mode = "recording"
        state.sound_snooze_until_ts = None
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

    if DOWNLOAD_MODE == ENGINE_NM3U8DL:
        active_url = get_active_source_url()
        if active_url:
            lines.append(f"Source URL         : {active_url}")
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
    if getattr(state, "network_guard", None):
        lines.extend(state.network_guard.format_info_lines())
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
    if getattr(state, "token_hold_active", False):
        return False

    mode = getattr(state, "console_input_mode", None)

    if mode is not None:
        if ch == "\x1b":  # Escape
            if mode == "sound_snooze_menu":
                _console_cancel_interaction(state, "Sound control cancelled; sound state is unchanged.")
            else:
                _console_cancel_interaction(state)
            return True

        if mode == "sound_snooze_menu":
            _apply_sound_snooze_menu_choice(state, ch)
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
        w = int(data.get("width") or 1920)
        h = int(data.get("height") or 1080)

        fps = 50
        for rate_key in ("avg_frame_rate", "r_frame_rate"):
            fps_str = str(data.get(rate_key) or "").strip()
            if not fps_str or fps_str in ("0/0", "0", "N/A"):
                continue
            if "/" in fps_str:
                num, den = fps_str.split("/", 1)
                den_f = float(den)
                if den_f > 0 and float(num) > 0:
                    fps = int(round(float(num) / den_f))
                    break
            else:
                val_f = float(fps_str)
                if val_f > 0:
                    fps = int(round(val_f))
                    break
        return w, h, fps
    except Exception:
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
        
def make_black_clip(
    state: RecorderState,
    output_ext: str = ".ts",
    output_format: str = "mpegts",
):
    """
    Create a short separator whose A/V stream order and codecs match the
    first GOOD chunk. Video becomes black; audio becomes silence.
    The existing FFmpeg/N_m3u8DL/Streamlink path keeps MPEG-TS defaults; yt-dlp
    finalization can request a Matroska/MKV separator.
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
        "av1": "libsvtav1",
        "vp9": "libvpx-vp9",
        "vp8": "libvpx",
    }

    audio_encoder_map = {
        "aac": "aac",
        "ac3": "ac3",
        "eac3": "eac3",
        "mp2": "mp2",
        "mp3": "libmp3lame",
        "opus": "libopus",
        "vorbis": "libvorbis",
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

    if not output_ext.startswith("."):
        output_ext = f".{output_ext}"

    signature_parts = []
    for stream in layout:
        codec_type = stream.get("codec_type")
        codec_name = stream.get("codec_name") or "unknown"
        signature_parts.append(f"{codec_type[0]}-{codec_name}")

    safe_signature = "_".join(signature_parts)
    name = (
        f"black_{state.best_w}x{state.best_h}_{int(round(state.best_fps or 50))}fps_"
        f"{safe_signature}_{int(BLACK_LEN)}s{output_ext}"
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
            elif stream["encoder"] == "libsvtav1":
                cmd += [f"-preset:v:{video_no}", "12"]
            video_no += 1

        else:
            cmd += [f"-c:a:{audio_no}", stream["encoder"]]
            if stream["bit_rate"] > 0:
                cmd += [f"-b:a:{audio_no}", str(stream["bit_rate"])]
            audio_no += 1

    cmd += [
        "-t", str(BLACK_LEN),
        "-f", output_format,
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

def is_stream_alive(proxy_url: Optional[str] = None):
    try:
        # Try to read a very short snippet with ffprobe (or ffmpeg -t 1 -f null -)
        proxy_args = ["-http_proxy", proxy_url] if proxy_url else []
        cmd = [
            "ffprobe",
            "-v", "error",
            *proxy_args,
            "-select_streams", "v:0",
            "-show_entries", "stream=width",
            "-of", "csv=p=0",
            LIVE_URL,
        ]
        result = run_external_capture(
            cmd,
            raw_tool="ffprobe",
            raw_context="source availability probe",
            capture_output=True,
            text=True,
            timeout=10, # change to 10 for normal use, 90 for slow iptv
        )
        return result.returncode == 0
    except Exception as e:
        log(f"ffprobe availability check failed: {e}")
        return False

def _ffmpeg_input_url_from_part_a() -> str:
    """Extract the configured FFmpeg input URL that follows -i."""
    match = re.search(
        r'(?:^|\s)-i\s+(?:"([^"]+)"|\'([^\']+)\'|(\S+))',
        FFMPEG_PART_A or "",
        re.IGNORECASE,
    )
    if not match:
        return ""
    return next((value for value in match.groups() if value), "")


def _ffmpeg_http_headers_from_part_a() -> dict:
    """Extract HTTP headers supplied through FFmpeg's -headers option."""
    headers = {}
    match = re.search(
        r'(?:^|\s)-headers\s+(?:"([^"]*)"|\'([^\']*)\')',
        FFMPEG_PART_A or "",
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return headers

    raw_headers = next((value for value in match.groups() if value is not None), "")
    raw_headers = raw_headers.replace(r"\r\n", "\r\n").replace(r"\n", "\n")

    for line in raw_headers.splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        name, value = line.split(":", 1)
        name = name.strip()
        value = value.strip()
        if name:
            headers[name] = value

    return headers


def _manifest_kind(url: str) -> Optional[str]:
    """Return 'hls', 'dash', or None from the input URL path."""
    path = urlparse(url or "").path.lower()
    if path.endswith(".m3u8"):
        return "hls"
    if path.endswith(".mpd"):
        return "dash"
    return None


def _http_get_manifest_text(url: str, headers: dict, deadline_mono: float, proxy_url: Optional[str] = None):
    """Fetch only manifest text, sharing one overall probe deadline."""
    remaining = deadline_mono - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("manifest probe deadline reached")

    request = Request(url, headers=headers, method="GET")
    if proxy_url:
        opener = build_opener(ProxyHandler({"http": proxy_url, "https": proxy_url}))
        response_cm = opener.open(request, timeout=remaining)
    else:
        response_cm = urlopen(request, timeout=remaining)

    with response_cm as response:
        status = getattr(response, "status", 200)
        if status < 200 or status >= 300:
            raise RuntimeError(f"HTTP {status}")

        # Manifest files are small. Cap the read so this probe can never turn
        # into a media download if a server returns unexpected content.
        data = response.read(512 * 1024)
        charset = response.headers.get_content_charset() or "utf-8"
        text = data.decode(charset, errors="replace")
        return text, response.geturl()


def _hls_media_present(text: str) -> bool:
    """True when an HLS media playlist currently advertises media."""
    upper = text.upper()
    return "#EXTINF:" in upper or "#EXT-X-PART:" in upper


def _first_hls_variant_url(text: str, base_url: str) -> str:
    """Resolve one rendition from an HLS master playlist."""
    lines = [line.strip() for line in text.splitlines()]
    for index, line in enumerate(lines):
        if not line.upper().startswith("#EXT-X-STREAM-INF:"):
            continue
        for candidate in lines[index + 1:]:
            if not candidate or candidate.startswith("#"):
                continue
            return urljoin(base_url, candidate)
    return ""


def _probe_hls_manifest(url: str, headers: dict, proxy_url: Optional[str] = None) -> bool:
    """Lightweight HLS liveness check: manifest text only, no media segments."""
    log("[PROBE] Checking HLS manifest availability (manifest-only HTTP GET)...")
    deadline_mono = time.monotonic() + SOURCE_PROBE_TIMEOUT_SEC

    try:
        text, effective_url = _http_get_manifest_text(url, headers, deadline_mono, proxy_url=proxy_url)
        if "#EXTM3U" not in text.upper():
            log("[PROBE] HLS source returned data, but not a valid HLS playlist.", level="WARN")
            return False

        if _hls_media_present(text):
            log("[PROBE] HLS media playlist is available → media entries found.")
            return True

        variant_url = _first_hls_variant_url(text, effective_url)
        if not variant_url:
            log("[PROBE] HLS playlist is reachable but has no live media/rendition entries.", level="WARN")
            return False

        log("[PROBE] HLS master playlist is available → checking one rendition playlist...")
        child_text, _ = _http_get_manifest_text(variant_url, headers, deadline_mono, proxy_url=proxy_url)
        if "#EXTM3U" in child_text.upper() and _hls_media_present(child_text):
            log("[PROBE] HLS rendition is available → media entries found.")
            return True

        log("[PROBE] HLS rendition is reachable but currently has no media entries.", level="WARN")
        return False

    except HTTPError as e:
        log(f"[PROBE] HLS manifest unavailable: HTTP {e.code}.", level="WARN")
    except (URLError, TimeoutError, OSError) as e:
        log(f"[PROBE] HLS manifest unavailable: {e}.", level="WARN")
    except Exception as e:
        log(f"[PROBE] HLS manifest check failed: {e}", level="WARN")

    return False


def _probe_dash_manifest(url: str, headers: dict, proxy_url: Optional[str] = None) -> bool:
    """Lightweight DASH liveness check: validate MPD text only, no media segments."""
    log("[PROBE] Checking DASH manifest availability (manifest-only HTTP GET)...")
    deadline_mono = time.monotonic() + SOURCE_PROBE_TIMEOUT_SEC

    try:
        text, _ = _http_get_manifest_text(url, headers, deadline_mono, proxy_url=proxy_url)
        root = ET.fromstring(text.lstrip("\ufeff"))
        root_name = root.tag.rsplit("}", 1)[-1].lower()
        if root_name != "mpd":
            log("[PROBE] DASH source returned data, but not a valid MPD.", level="WARN")
            return False

        has_period = any(node.tag.rsplit("}", 1)[-1].lower() == "period" for node in root.iter())
        has_representation = any(
            node.tag.rsplit("}", 1)[-1].lower() == "representation"
            for node in root.iter()
        )

        if has_period and has_representation:
            log("[PROBE] DASH MPD is available → stream representations found.")
            return True

        log("[PROBE] DASH MPD is reachable but has no usable stream representations.", level="WARN")
        return False

    except ET.ParseError as e:
        log(f"[PROBE] DASH manifest is not readable XML: {e}", level="WARN")
    except HTTPError as e:
        log(f"[PROBE] DASH manifest unavailable: HTTP {e.code}.", level="WARN")
    except (URLError, TimeoutError, OSError) as e:
        log(f"[PROBE] DASH manifest unavailable: {e}.", level="WARN")
    except Exception as e:
        log(f"[PROBE] DASH manifest check failed: {e}", level="WARN")

    return False


def _probe_advanced_ffmpeg_fallback(proxy_url: Optional[str] = None) -> bool:
    """Fallback for an advanced FFmpeg input that is not HLS/DASH."""
    advanced_cmd = FFMPEG_PART_A.strip()
    if proxy_url and not re.search(r'(?<!\S)-http_proxy(?=\s|$)', advanced_cmd):
        input_match = re.search(r'(?<!\S)-i(?=\s)', advanced_cmd)
        proxy_opt = f"-http_proxy {proxy_url} "
        if input_match:
            advanced_cmd = (
                advanced_cmd[:input_match.start()]
                + proxy_opt
                + advanced_cmd[input_match.start():]
            )
        else:
            advanced_cmd = proxy_opt + advanced_cmd

    probe_cmd_text = (
        f'{advanced_cmd} '
        f'-nostdin -loglevel error -t 1 -f null -'
    )
    cmd = probe_cmd_text if os.name == "nt" else shlex.split(probe_cmd_text)

    log("[PROBE] Checking non-manifest advanced FFmpeg source availability...")

    try:
        result = run_external_capture(
            cmd,
            raw_tool="FFmpeg",
            raw_context="advanced source availability probe",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            errors="replace",
            timeout=SOURCE_PROBE_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired:
        log(
            f"[PROBE] Advanced FFmpeg source check timed out after "
            f"{SOURCE_PROBE_TIMEOUT_SEC}s.",
            level="WARN",
        )
        return False
    except Exception as e:
        log(f"[PROBE] Advanced FFmpeg source check failed: {e}", level="WARN")
        return False

    if result.returncode == 0:
        log("[PROBE] Advanced FFmpeg source is available.")
        return True

    detail = (result.stderr or result.stdout or "").strip().splitlines()
    if detail:
        log(f"[PROBE] Advanced FFmpeg source unavailable: {detail[-1]}", level="WARN")
    else:
        log("[PROBE] Advanced FFmpeg source unavailable.", level="WARN")
    return False


def is_advanced_ffmpeg_stream_alive(proxy_url: Optional[str] = None):
    """
    Source-aware availability gate for advanced FFmpeg inputs.

    HLS/DASH inputs are checked by fetching manifest text only; no FFmpeg media
    process is launched and no media segment is downloaded. Other advanced
    inputs retain the previous short FFmpeg fallback probe.
    """
    if not FFMPEG_PART_A.strip():
        return is_stream_alive(proxy_url=proxy_url)

    source_url = _ffmpeg_input_url_from_part_a()
    if not source_url:
        log("[PROBE] Could not find FFmpeg input URL after -i.", level="WARN")
        return False

    headers = _ffmpeg_http_headers_from_part_a()
    kind = _manifest_kind(source_url)

    if kind == "hls":
        return _probe_hls_manifest(source_url, headers, proxy_url=proxy_url)
    if kind == "dash":
        return _probe_dash_manifest(source_url, headers, proxy_url=proxy_url)

    return _probe_advanced_ffmpeg_fallback(proxy_url=proxy_url)


def get_stream_expected_kbps(url: str, proxy_url: Optional[str] = None) -> Optional[float]:
    """Best-effort expected bitrate from stream metadata, in Kbps."""
    proxy_args = ["-http_proxy", proxy_url] if proxy_url else []
    try:
        result = run_external_capture(
            [
                "ffprobe",
                "-v", "error",
                *proxy_args,
                "-select_streams", "v:0",
                "-show_entries", "stream=bit_rate",
                "-of", "csv=p=0",
                url,
            ],
            raw_tool="ffprobe",
            raw_context="expected bitrate probe | video stream",
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
                *proxy_args,
                "-show_entries", "format=bit_rate",
                "-of", "csv=p=0",
                url,
            ],
            raw_tool="ffprobe",
            raw_context="expected bitrate probe | container",
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
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    last_progress_line = None
    progress_line_active = False
    kv = {}

    def monitor_thread():
        nonlocal last_progress_line, progress_line_active, kv
        timing_burst_count = 0
        timing_suppressed_count = 0
        last_timing_warning_mono = 0.0
        last_timing_summary_mono = 0.0

        def check_flush_timing_summary(force=False):
            nonlocal timing_burst_count, timing_suppressed_count
            nonlocal last_timing_warning_mono, last_timing_summary_mono
            if timing_suppressed_count <= 0:
                return None
            now_m = time.monotonic()
            burst_idle = (now_m - last_timing_warning_mono) >= 3.0
            should_flush = (
                force
                or burst_idle
                or (last_timing_summary_mono > 0 and (now_m - last_timing_summary_mono) >= 3.0)
            )
            if not should_flush:
                return None
            count = timing_suppressed_count
            timing_suppressed_count = 0
            if force or burst_idle:
                timing_burst_count = 0
                last_timing_summary_mono = 0.0
            else:
                last_timing_summary_mono = now_m
            return f"{tag}: Timing warning burst (suppressed {count} repetitive lines from screen)"

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
                        burst_sum = check_flush_timing_summary()
                        if burst_sum:
                            if progress_line_active:
                                clear_progress_line()
                                progress_line_active = False
                            log(burst_sum)
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

                burst_sum = check_flush_timing_summary()
                if burst_sum:
                    if progress_line_active:
                        clear_progress_line()
                        progress_line_active = False
                    log(burst_sum)

                s_lower = s.lower()
                is_timing_warning = (
                    "non-monotonic dts" in s_lower
                    or "non-monotonic pts" in s_lower
                    or "timestamp discontinuity" in s_lower
                )
                suppress_from_terminal = False
                now_m = time.monotonic()

                if is_timing_warning:
                    if last_timing_warning_mono == 0.0 or (now_m - last_timing_warning_mono) > 3.0:
                        timing_burst_count = 1
                        last_timing_warning_mono = now_m
                        last_timing_summary_mono = 0.0
                    else:
                        timing_burst_count += 1
                        last_timing_warning_mono = now_m
                        if timing_burst_count > 2:
                            timing_suppressed_count += 1
                            if last_timing_summary_mono == 0.0:
                                last_timing_summary_mono = now_m
                            suppress_from_terminal = True

                if suppress_from_terminal:
                    ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    termcap_write(f"{ts_str} [INFO] {tag}: {s}")
                else:
                    if progress_line_active:
                        clear_progress_line()
                        progress_line_active = False
                    if s:
                        log(f"{tag}: {s}")
        except Exception as e:
            log(f"{tag} monitor error: {e}")
        finally:
            try:
                burst_sum = check_flush_timing_summary(force=True)
                if burst_sum:
                    if progress_line_active:
                        clear_progress_line()
                        progress_line_active = False
                    log(burst_sum)
            except Exception:
                pass

    t = threading.Thread(target=monitor_thread, daemon=True)
    t.start()
    
    rc = proc.wait()
    
    try:
        t.join(timeout=3)
    except:
        pass
    
    if progress_line_active:
        clear_progress_line()
        progress_line_active = False
    if last_progress_line:
        log(f"{tag}: {last_progress_line}")
    return rc

@dataclass
class LiveFileGrowthTracker:
    """Per-attempt file-growth state shared by FFmpeg, yt-dlp, and Streamlink."""
    prev_size: int
    prev_t: float
    next_check: float
    hard_count: int = 0
    soft_count: int = 0


def start_live_file_growth_tracker(path: Optional[str]) -> LiveFileGrowthTracker:
    """Start the shared growth clock for one active recording attempt."""
    now_t = time.monotonic()
    size = os.path.getsize(path) if path and os.path.exists(path) else 0

    return LiveFileGrowthTracker(
        prev_size=size,
        prev_t=now_t,
        next_check=now_t + float(LIVE_GROWTH_CHECK_INTERVAL),
    )


def poll_live_file_growth(path: Optional[str], tracker: LiveFileGrowthTracker):
    """Return one shared file-growth sample when the next check is due."""
    now_t = time.monotonic()

    if now_t < tracker.next_check:
        return None

    tracker.next_check = now_t + float(LIVE_GROWTH_CHECK_INTERVAL)

    has_file = bool(path and os.path.exists(path))
    cur_size = os.path.getsize(path) if has_file else 0
    elapsed_s = max(0.001, now_t - tracker.prev_t)
    growth_bytes = max(0, cur_size - tracker.prev_size)
    growth_kbps = (growth_bytes * 8.0) / elapsed_s / 1000.0

    tracker.prev_size = cur_size
    tracker.prev_t = now_t

    is_hard = growth_bytes <= LIVE_HARD_STALL_ZERO_BYTES

    if is_hard:
        tracker.hard_count += 1
        tracker.soft_count = 0
    else:
        tracker.hard_count = 0

    return {
        "has_file": has_file,
        "growth_bytes": growth_bytes,
        "growth_kbps": growth_kbps,
        "is_hard": is_hard,
        "hard_count": tracker.hard_count,
        "hard_trigger": tracker.hard_count >= LIVE_HARD_STALL_REQUIRED,
    }


def classify_live_soft_stall(
    tracker: LiveFileGrowthTracker,
    growth_kbps: float,
    expected_kbps: Optional[float],
    soft_stall_required: int = LIVE_SOFT_STALL_REQUIRED,
):
    """Apply the shared FFmpeg/yt-dlp soft-stall rule to one non-hard growth check."""
    if expected_kbps is None or expected_kbps <= 0:
        tracker.soft_count = 0
        return {
            "is_soft": False,
            "soft_count": 0,
            "soft_trigger": False,
            "threshold_kbps": None,
        }

    threshold_kbps = expected_kbps * LIVE_SOFT_STALL_FACTOR
    is_soft = growth_kbps < threshold_kbps

    if is_soft:
        tracker.soft_count += 1
    else:
        tracker.soft_count = 0

    return {
        "is_soft": is_soft,
        "soft_count": tracker.soft_count,
        "soft_trigger": tracker.soft_count >= soft_stall_required,
        "threshold_kbps": threshold_kbps,
    }


class StreamIntegrityMonitor:
    """
    Self-contained live stream integrity monitor for FFmpeg capture.

    Evaluates stream health across 3 independent pillars:
      P1: Transport & Demuxer (discontinuity storms / corrupt TS packets)
      P2: Progress & Timeline (frozen/looping timeline while bytes grow)
      P3: Video Payload Decodability (codec parameter loss: PPS/SPS, NALU parse failures)

    Decision Gate:
      is_defect_active = P1 or P2 or P3

    State Machine:
      - Continuous defect >= 8.0s (2 broadcast GOPs) -> starts an incident and a grace
        countdown; if the defect persists past the grace window, requests a chunk recycle.
      - Clean stream >= 15.0s continuous (with no recycle requested) -> incident recorded as
        non-fatal and defect state reset.
      - Suppresses terminal log spam for repetitive errors (max 1 summary per 3s).

    Ownership boundary: this monitor only detects defects, requests recycles, and counts
    non-fatal recoveries. It never starts or stops sirens. Fatal-cut counting, the escalation
    siren, operator silence state and re-arming are owned by the 30-minute rolling window on
    RecorderState (register_stream_corrupt_recycle / prune_stream_corrupt_window).
    """

    RE_CODEC_PARAM_LOSS = re.compile(
        r'(?:non-existing\s+[PS]PS|Failed to parse header of NALU|missing picture)',
        re.IGNORECASE,
    )
    RE_INVALID_TIMESTAMPS = re.compile(
        r'Invalid timestamps\s+stream=\d+,\s*pts=(-?\d+|NOPTS),\s*dts=(-?\d+|NOPTS)',
        re.IGNORECASE,
    )
    RE_UNSET_TIMESTAMPS = re.compile(
        r'Timestamps are unset in a packet',
        re.IGNORECASE,
    )
    RE_AAC_EXTRADATA_MISSING = re.compile(
        r'AAC bitstream not in ADTS format and extradata missing',
        re.IGNORECASE,
    )
    RE_DISCONT_DELTA = re.compile(
        r'timestamp discontinuity.*?:\s*([+-]?\d+)',
        re.IGNORECASE,
    )
    RE_CORRUPT_PKT = re.compile(
        r'(?:corrupt input packet|Packet corrupt)',
        re.IGNORECASE,
    )

    # Tier 2: Fatal Container Poisoning Thresholds
    IMPOSSIBLE_DISCONT_DELTA = 324_000_000     # >= 324s (5.4m) in µs (AV_TIME_BASE)
    OPPOSING_AV_TEAR_DELTA = 54_000_000        # >= 54s in µs (AV_TIME_BASE) opposing A/V desync tear
    PAIRED_CLOCK_RESET_TOLERANCE_US = 2_000_000  # 2.0s tolerance for equal-and-opposite A/V clock rollover
    STARTUP_GRACE_OUT_TIME_US = 10_000_000     # 10.0s startup immunity for initial packet alignment
    FAST_SEAL_MATURE_CHUNK_US = 60_000_000     # 60.0s mature chunk threshold (MIN_ACCEPTED_CHUNK_SECONDS)
    FAST_SEAL_COALESCE_SEC = 2.0               # 2.0s coalesce window to flush splice burst before sealing
    FATAL_ON_SOLO_TEAR = False                 # solo same-sign tear is Tier 1 unless corroborated
    STARTUP_GRACE_WALL_SEC = 10.0              # wall-clock startup grace for benign DTS wrap
    WRAPPED_DTS_FLOOR = (1 << 33) - 450_000    # ~5s before 2^33 at 90kHz => negative DTS wrapped
    WRAPPED_PTS_TOLERANCE = 450_000            # ~5s at 90kHz: benign rollover window
    MATURE_MIN_WALL_SEC = 30.0                 # burst must not count as mature
    BURST_RATIO = 3.0                          # speed ratio threshold to discriminate bursts from live media

    # Phase 2: Autonomous Stream Recycling Grace Thresholds
    GRACE_CODEC_SEC = 15.0
    GRACE_TIMELINE_SEC = 30.0
    GRACE_TRANSPORT_SEC = 60.0

    def __init__(self, state: RecorderState):
        self.state = state
        self._lock = threading.RLock()
        self.start_mono: float = time.monotonic()
        self.last_tear_mono: float = 0.0

        # Transport & Discontinuity Health (Tier 1)
        self.transport_discont_times: list[float] = []
        self.last_transport_delta: Optional[int] = None
        self.last_transport_discont_mono: float = 0.0
        self.transport_defect: bool = False
        self.last_transport_flag_mono: float = 0.0

        # Video Elementary Stream Progress & Freeze Health (Tier 1)
        self.last_progress_mono: float = time.monotonic()
        self.last_out_time_us: Optional[int] = None
        self.last_size: Optional[int] = None
        self.last_video_frame: Optional[int] = None
        self.frames_advancing: bool = True
        self.freeze_start_mono: Optional[float] = None
        self.video_freeze_defect: bool = False

        # Codec & Payload Decodability Health (Tier 1)
        self.last_codec_error_mono: float = 0.0
        self.last_codec_error_msg: str = ""
        self.codec_defect: bool = False

        # Fatal Container / Timeline Poisoning Health (Tier 2 — Latching)
        self.container_defect: bool = False
        self.container_defect_start_mono: Optional[float] = None
        self.last_container_error_mono: float = 0.0
        self.last_container_error_msg: str = ""
        self._container_defect_logged: bool = False

        # Phase 2: Autonomous Worker Recycling & Grace Management
        self.recycle_requested: bool = False
        self.recycle_reason: str = ""

        # State Machine & Timers
        self.defect_start_mono: Optional[float] = None
        self._defect_detected_logged: bool = False
        self.clean_since_mono: float = time.monotonic()
        self.incident_active: bool = False
        self.current_incident_start: Optional[float] = None

        # Anti-spam terminal rate limiting & burst throttling (Item 3A/3B)
        self.last_terminal_spam_mono: float = 0.0
        self.timing_burst_count: int = 0
        self.timing_suppressed_count: int = 0
        self.last_timing_warning_mono: float = 0.0
        self.last_timing_summary_mono: float = 0.0
        self._container_error_counts: dict[str, int] = {}
        self._container_error_reasons: dict[str, str] = {}

    def _latch_container_defect(self, now: float, reason: str) -> None:
        """Latch an irreversible Tier 2 container/timeline poisoning defect with deduplication."""
        category = reason.split("(")[0].strip() if "(" in reason else reason.strip()
        if not hasattr(self, "_container_error_counts"):
            self._container_error_counts = {}
        if not hasattr(self, "_container_error_reasons"):
            self._container_error_reasons = {}

        if not self.container_defect:
            self.container_defect = True
            self.container_defect_start_mono = now
            self._container_error_counts = {category: 1}
            self._container_error_reasons = {category: reason}
            self.last_container_error_msg = reason[:200]
        else:
            counts = self._container_error_counts
            reasons = self._container_error_reasons
            counts[category] = counts.get(category, 0) + 1
            if category not in reasons:
                reasons[category] = reason
            parts = [
                f"{cat} [x{cnt}]" if cnt > 1 else reasons.get(cat, cat)
                for cat, cnt in counts.items()
            ]
            self.last_container_error_msg = "; ".join(parts)[:200]
        self.last_container_error_mono = now

    def check_flush_timing_summary(self, force: bool = False) -> Optional[str]:
        """
        If repetitive timing warnings were suppressed from screen, return a summary notice to log.
        Flushes if force=True, or if >= 3.0s elapsed since last warning, or if >= 3.0s elapsed since last summary during an ongoing burst.
        """
        with self._lock:
            if self.timing_suppressed_count <= 0:
                return None
            now = time.monotonic()
            burst_idle = (now - self.last_timing_warning_mono) >= 3.0
            should_flush = (
                force
                or burst_idle
                or (self.last_timing_summary_mono > 0 and (now - self.last_timing_summary_mono) >= 3.0)
            )
            if not should_flush:
                return None

            count = self.timing_suppressed_count
            self.timing_suppressed_count = 0
            if force or burst_idle:
                self.timing_burst_count = 0
                self.last_timing_summary_mono = 0.0
            else:
                self.last_timing_summary_mono = now
            return f"FFmpeg: Timing warning burst (suppressed {count} repetitive lines from screen)"

    # Backward-compatible property accessors
    @property
    def p1_defect(self) -> bool:
        return self.transport_defect

    @p1_defect.setter
    def p1_defect(self, val: bool) -> None:
        self.transport_defect = val

    @property
    def p2_defect(self) -> bool:
        return self.video_freeze_defect

    @p2_defect.setter
    def p2_defect(self, val: bool) -> None:
        self.video_freeze_defect = val

    @property
    def p3_defect(self) -> bool:
        return self.codec_defect

    @p3_defect.setter
    def p3_defect(self, val: bool) -> None:
        self.codec_defect = val

    def process_stderr_line(self, line: str) -> bool:
        """
        Inspect one FFmpeg stderr line for corruption signals.
        Returns True if this line should be suppressed from the interactive terminal (anti-spam).
        """
        with self._lock:
            now = time.monotonic()
            is_integrity_line = False

            # Tier 1: Codec parameter loss / invalid NALUs (recoverable if timeline stays intact)
            m_codec = self.RE_CODEC_PARAM_LOSS.search(line)
            if m_codec:
                self.codec_defect = True
                self.last_codec_error_mono = now
                self.last_codec_error_msg = m_codec.group(0)
                is_integrity_line = True

            # Rule 2C: Invalid timestamps (DTS > PTS protocol inversion violation)
            m_inv = self.RE_INVALID_TIMESTAMPS.search(line)
            if m_inv:
                m_dts = re.search(r'\bdts=(-?\d+)', line)
                m_pts = re.search(r'\bpts=(-?\d+)', line)
                is_wrapped_dts = bool(
                    m_dts and m_pts
                    and int(m_dts.group(1)) >= self.WRAPPED_DTS_FLOOR
                    and 0 <= int(m_pts.group(1)) < self.WRAPPED_PTS_TOLERANCE
                )
                if is_wrapped_dts:
                    pass  # benign 33-bit wrapped negative-DTS / rollover; do not poison container
                else:
                    self.transport_defect = True
                    self.last_transport_flag_mono = now
                    self._latch_container_defect(now, "Protocol DTS > PTS Timestamp Inversion")
                    is_integrity_line = True

            # Rule 2C: Mid-stream AAC extradata destruction
            if self.RE_AAC_EXTRADATA_MISSING.search(line):
                self.codec_defect = True
                self.last_codec_error_mono = now
                self.last_codec_error_msg = "AAC Extradata Missing"
                if (self.last_out_time_us or 0) >= self.STARTUP_GRACE_OUT_TIME_US or self.transport_defect:
                    self._latch_container_defect(now, "AAC Extradata Missing")
                is_integrity_line = True

            # Rule 2C: Mid-stream unset packet timestamps (causes FFmpeg-FINAL muxer crash)
            if self.RE_UNSET_TIMESTAMPS.search(line):
                if (self.last_out_time_us or 0) >= self.STARTUP_GRACE_OUT_TIME_US and (
                    self.codec_defect or self.transport_defect
                ):
                    self.transport_defect = True
                    self.last_transport_flag_mono = now
                    self._latch_container_defect(now, "Unset Packet Timestamps")
                    is_integrity_line = True

            # Transport discontinuities and corrupt packets
            if "timestamp discontinuity" in line.lower():
                self.transport_discont_times.append(now)
                self.transport_discont_times = [t for t in self.transport_discont_times if (now - t) <= 2.0]

                m_delta = self.RE_DISCONT_DELTA.search(line)
                delta = int(m_delta.group(1)) if m_delta else None
                is_alternating = (
                    delta is not None
                    and self.last_transport_delta is not None
                    and ((self.last_transport_delta > 0 and delta < 0) or (self.last_transport_delta < 0 and delta > 0))
                )

                is_paired_clock_reset = False
                if is_alternating and (now - self.last_transport_discont_mono) <= 5.0:
                    magnitude_diff = abs(abs(delta) - abs(self.last_transport_delta))
                    if (
                        magnitude_diff <= self.PAIRED_CLOCK_RESET_TOLERANCE_US
                        and not self.codec_defect
                        and not self.video_freeze_defect
                    ):
                        is_paired_clock_reset = True

                if is_paired_clock_reset:
                    # Benign equal-and-opposite provider clock rollover compensated by FFmpeg.
                    # Clear any solo transport_defect set by the first leg of the pair.
                    if len(self.transport_discont_times) < 3 and not self.codec_defect:
                        self.transport_defect = False
                        if (
                            not self.container_defect
                            and not self.video_freeze_defect
                            and not self._defect_detected_logged
                        ):
                            self.defect_start_mono = None
                    self.last_transport_delta = None
                    self.last_transport_discont_mono = now
                else:
                    if delta is not None and abs(delta) >= self.IMPOSSIBLE_DISCONT_DELTA:
                        self.transport_defect = True
                        self.last_transport_flag_mono = now
                        self.last_tear_mono = now
                        corroborated = self.codec_defect or self.container_defect
                        if self.FATAL_ON_SOLO_TEAR or corroborated:
                            self._latch_container_defect(now, f"Extreme Timeline Tear ({delta:+d})")

                    # Rule 2B: True Asymmetric Opposing Audio/Video Desync Tear within 5.0s
                    if (
                        is_alternating
                        and (now - self.last_transport_discont_mono) <= 5.0
                        and abs(delta - self.last_transport_delta) >= self.OPPOSING_AV_TEAR_DELTA
                    ):
                        self.transport_defect = True
                        self.last_transport_flag_mono = now
                        self._latch_container_defect(
                            now, f"Opposing A/V Timeline Tear ({self.last_transport_delta:+d} vs {delta:+d})"
                        )

                    if delta is not None:
                        self.last_transport_delta = delta
                        self.last_transport_discont_mono = now

                    if len(self.transport_discont_times) >= 3 or is_alternating:
                        self.transport_defect = True
                        self.last_transport_flag_mono = now
                is_integrity_line = True

            elif self.RE_CORRUPT_PKT.search(line):
                self.last_transport_flag_mono = now
                is_integrity_line = True

            elif "non-monotonic dts" in line.lower() or "non-monotonic pts" in line.lower():
                is_integrity_line = True

            # Anti-spam throttling on the interactive console
            if is_integrity_line:
                # If this is a timing warning (timestamp discontinuity or non-monotonic DTS/PTS),
                # apply multi-line burst throttling with summary flushing (Item 3A).
                line_lower = line.lower()
                if (
                    "timestamp discontinuity" in line_lower
                    or "non-monotonic dts" in line_lower
                    or "non-monotonic pts" in line_lower
                ):
                    if self.last_timing_warning_mono == 0.0 or (now - self.last_timing_warning_mono) > 3.0:
                        self.timing_burst_count = 1
                        self.last_timing_warning_mono = now
                        self.last_timing_summary_mono = 0.0
                        return False
                    else:
                        self.timing_burst_count += 1
                        self.last_timing_warning_mono = now
                        if self.timing_burst_count <= 2:
                            return False
                        else:
                            self.timing_suppressed_count += 1
                            if self.last_timing_summary_mono == 0.0:
                                self.last_timing_summary_mono = now
                            return True

                if (now - self.last_terminal_spam_mono) < 3.0:
                    return True
                else:
                    self.last_terminal_spam_mono = now
                    return False

            return False

    def process_progress(self, kv: dict[str, str]) -> None:
        """Inspect FFmpeg progress key-values for timeline freeze & video frame stalls."""
        with self._lock:
            now = time.monotonic()
            out_time_us_str = kv.get("out_time_us")
            total_size_str = kv.get("total_size")
            frame_str = kv.get("frame")

            cur_frame = None
            if frame_str and frame_str.isdigit():
                try:
                    cur_frame = int(frame_str)
                except (ValueError, TypeError):
                    pass

            if cur_frame is not None and self.last_video_frame is not None:
                self.frames_advancing = (cur_frame > self.last_video_frame)
            elif cur_frame is not None:
                self.frames_advancing = True

            if out_time_us_str and total_size_str:
                try:
                    cur_out_time_us = int(out_time_us_str)
                    cur_size = int(total_size_str)

                    if self.last_out_time_us is not None and self.last_size is not None:
                        elapsed_wall = now - self.last_progress_mono
                        adv_out_time_s = (cur_out_time_us - self.last_out_time_us) / 1_000_000.0
                        size_growth = cur_size - self.last_size

                        # Case A: Timeline freeze (muxer out_time stalled while bytes grow)
                        timeline_stalled = (elapsed_wall >= 4.0 and size_growth > 0 and adv_out_time_s < 0.5)

                        # Case B: Video elementary stream freeze (audio masks video failure)
                        # Bytes actively downloading (> 50KB growth), but video frame counter is frozen
                        video_frames_stalled = (
                            cur_frame is not None
                            and self.last_video_frame is not None
                            and cur_frame == self.last_video_frame
                            and size_growth > 50_000
                        )

                        if timeline_stalled or video_frames_stalled:
                            if self.freeze_start_mono is None:
                                self.freeze_start_mono = now
                            if (now - self.freeze_start_mono) >= 6.0:
                                self.video_freeze_defect = True
                        else:
                            self.freeze_start_mono = None
                            self.video_freeze_defect = False

                    self.last_out_time_us = cur_out_time_us
                    self.last_size = cur_size
                    if cur_frame is not None:
                        self.last_video_frame = cur_frame
                    self.last_progress_mono = now
                except (ValueError, TypeError):
                    pass

    def evaluate_health(self, flush_cb=None) -> None:
        """Evaluate stream defect signals, drive the alarm, and enforce Phase 2 grace recycling."""
        with self._lock:
            now = time.monotonic()

            # Decay Tier 1 defect flags ONLY if stream achieves active convergence AND container is not poisoned.
            # Tier 2 container poisoning (multi-hour timeline tear, opposing A/V tear, DTS > PTS inversion,
            # or mid-stream header loss) permanently corrupts the active .ts muxer state in -c copy mode
            # even when frames continue advancing.
            if not self.container_defect:
                if self.transport_defect and (now - self.last_transport_flag_mono) > 4.0:
                    if self.frames_advancing and not self.video_freeze_defect:
                        self.transport_defect = False

                if self.codec_defect and (now - self.last_codec_error_mono) > 4.0:
                    if self.frames_advancing and not self.transport_defect and not self.video_freeze_defect:
                        self.codec_defect = False

            # Boolean OR Decision Gate
            is_defect_active = (
                self.transport_defect
                or self.video_freeze_defect
                or self.codec_defect
                or self.container_defect
            )

            if is_defect_active:
                self.clean_since_mono = now
                if self.defect_start_mono is None:
                    self.defect_start_mono = now
                    self._defect_detected_logged = False

                awaiting_pair_coalesce = (
                    self.transport_defect
                    and not self.container_defect
                    and not self.codec_defect
                    and not self.video_freeze_defect
                    and len(self.transport_discont_times) == 1
                    and self.last_transport_delta is not None
                    and (now - self.defect_start_mono) < 0.5
                )

                if not self._defect_detected_logged and not awaiting_pair_coalesce:
                    self._defect_detected_logged = True
                    causes = []
                    if self.container_defect:
                        causes.append(f"Fatal Container Poisoning ({self.last_container_error_msg})")
                        self._container_defect_logged = True
                    if self.codec_defect:
                        causes.append(f"Codec Error ({self.last_codec_error_msg})")
                    if self.transport_defect:
                        causes.append("Transport Discontinuity Storm")
                    if self.video_freeze_defect:
                        causes.append("Video Elementary Freeze")
                    cause_str = ", ".join(causes)
                    if flush_cb:
                        flush_cb()
                    log(f"STREAM_INTEGRITY: Defect detected -> [{cause_str}] | Tracking 8s window...", level="WARN")
                elif self.container_defect and not self._container_defect_logged:
                    self._container_defect_logged = True
                    if flush_cb:
                        flush_cb()
                    log(
                        f"STREAM_INTEGRITY: Escalated to Fatal Container Poisoning -> "
                        f"[{self.last_container_error_msg}]",
                        level="WARN",
                    )

                # Tier 2 Fast-Seal Gate for Mature Chunks (out_time >= FAST_SEAL_MATURE_CHUNK_US):
                # Once a mature chunk suffers irreversible container/timeline poisoning, waiting 23-68s
                # only appends corrupt packets that crash FFmpeg-FINAL during MKV remuxing.
                # Seal after a 2.0s coalesce window so the splice burst is logged and the chunk is preserved clean.
                elapsed_wall = max(0.001, now - self.start_mono)
                media_sec = (self.last_out_time_us or 0) / 1_000_000.0
                mature_floor_sec = self.FAST_SEAL_MATURE_CHUNK_US / 1_000_000.0
                is_mature_chunk = (
                    media_sec >= mature_floor_sec
                    and (
                        media_sec >= 180.0
                        or (media_sec / elapsed_wall) <= self.BURST_RATIO
                    )
                )
                if (
                    self.container_defect
                    and is_mature_chunk
                    and self.container_defect_start_mono is not None
                    and (now - self.container_defect_start_mono) >= self.FAST_SEAL_COALESCE_SEC
                    and not self.recycle_requested
                ):
                    self.recycle_requested = True
                    self.recycle_reason = (
                        f"Fatal Container Poisoning ({self.last_container_error_msg}) "
                        f"after {self.FAST_SEAL_COALESCE_SEC:.0f}s coalesce"
                    )
                    return

                # Check if defect has persisted continuously for >= 8.0s (2 GOPs)
                elapsed_defect = now - self.defect_start_mono
                if elapsed_defect >= 8.0:
                    if not self.incident_active:
                        self.incident_active = True
                        self.current_incident_start = now
                        if flush_cb:
                            flush_cb()
                        log(
                            "STREAM_INTEGRITY: Sustained corruption >= 8.0s -> tracking grace countdown before chunk recycle.",
                            level="WARN",
                        )

                    # Phase 2: Autonomous Worker Recycling & Grace Countdown Gate
                    if self.current_incident_start is not None:
                        incident_elapsed = now - self.current_incident_start

                        # Grace Countdown by Defect Severity
                        active_grace_sec = self.GRACE_TRANSPORT_SEC
                        active_defect_name = "Transport Discontinuity"
                        if self.container_defect:
                            active_grace_sec = self.GRACE_CODEC_SEC
                            active_defect_name = f"Fatal Container Poisoning ({self.last_container_error_msg})"
                        elif self.codec_defect:
                            active_grace_sec = self.GRACE_CODEC_SEC
                            active_defect_name = f"Codec Parameter Loss ({self.last_codec_error_msg})"
                        elif self.video_freeze_defect:
                            active_grace_sec = self.GRACE_TIMELINE_SEC
                            active_defect_name = "Video Elementary Freeze"

                        if incident_elapsed >= active_grace_sec and not self.recycle_requested:
                            self.recycle_requested = True
                            self.recycle_reason = f"{active_defect_name} sustained >= {active_grace_sec:.0f}s"
            else:
                # All signals clean
                if self.defect_start_mono is not None and not self.incident_active and self._defect_detected_logged:
                    if flush_cb:
                        flush_cb()
                    log(
                        "STREAM_INTEGRITY: Stream stabilized / self-healed within 8s window. Defect cleared; alarm suppressed.",
                        level="INFO",
                    )
                self.defect_start_mono = None
                self._defect_detected_logged = False

                # Check for recovery: clean for >= 15.0 continuous seconds.
                # A pending recycle request is never cancelled or double-counted here:
                # once requested, the chunk is cut and counted as fatal by the main loop.
                if (
                    self.incident_active
                    and not self.recycle_requested
                    and (now - self.clean_since_mono) >= 15.0
                ):
                    self.state.ffmpeg_stream_corrupt_nonfatal_incidents += 1
                    self.incident_active = False
                    self.current_incident_start = None
                    if flush_cb:
                        flush_cb()
                    log(
                        "STREAM_INTEGRITY: Stream recovered without chunk cut (non-fatal; clean decodable frames >= 15s).",
                        level="INFO",
                    )

    @property
    def is_corrupt_or_defective(self) -> bool:
        """Real-time health of THIS chunk only (never the rolling fatal-cut window)."""
        with self._lock:
            return (
                self.incident_active
                or self.recycle_requested
                or self.transport_defect
                or self.video_freeze_defect
                or self.codec_defect
                or self.container_defect
            )


def get_active_source_url() -> str:
    """Helper to extract active target stream URL across all engines."""
    if DOWNLOAD_MODE == "N_m3u8DL-RE":
        m = re.search(r'["\'](https?://[^"\']+)["\']', NM3U8DL_PART_A)
        if m:
            return m.group(1)
        m2 = re.search(r'(https?://\S+)', NM3U8DL_PART_A)
        if m2:
            return m2.group(1)
    elif DOWNLOAD_MODE == "ffmpeg":
        if LIVE_URL.strip():
            return LIVE_URL.strip()
        m = re.search(r'-i\s+["\']?(https?://\S+?)["\']?(\s|$)', FFMPEG_PART_A)
        if m:
            return m.group(1)
    elif DOWNLOAD_MODE == "yt-dlp":
        m = re.search(r'["\']?(https?://\S+?)["\']?(\s|$)', YTDLP_PART_A)
        if m:
            return m.group(1)
    elif DOWNLOAD_MODE == "streamlink":
        m = re.search(r'["\']?(https?://\S+?)["\']?(\s|$)', STREAMLINK_PART_A)
        if m:
            return m.group(1)
    return ""


class WireproxyManager:
    """
    Manages local userspace wireproxy child process lifecycle,
    runtime config generation, health checks, and 3-2-1 tiered failover.
    """
    def __init__(
        self,
        socks5_port: int = WIREPROXY_SOCKS5_PORT,
        http_port: int = WIREPROXY_HTTP_PORT,
    ):
        self.socks5_port = socks5_port
        self.http_port = http_port
        self.proc: Optional[subprocess.Popen] = None
        self.active_config_path: Optional[str] = None
        self.active_server: Optional[str] = None
        self.config_dir: Optional[str] = self.resolve_config_dir()
        self.wireproxy_bin: Optional[str] = self.find_wireproxy_binary()
        self.raw_token: Optional[dict] = None

    @staticmethod
    def find_wireproxy_binary() -> Optional[str]:
        """
        Locate wireproxy binary using universal two-tier discovery:
        1. System PATH via shutil.which("wireproxy") -> return "wireproxy" to invoke directly by name
        2. Local script directory fallback -> return absolute path if wireproxy(.exe) is co-located with script
        """
        if shutil.which("wireproxy"):
            return "wireproxy"
        script_dir = os.path.dirname(os.path.abspath(__file__))
        local_name = "wireproxy.exe" if os.name == "nt" else "wireproxy"
        local_path = os.path.join(script_dir, local_name)
        if os.path.isfile(local_path):
            return local_path
        return None

    @staticmethod
    def resolve_config_dir() -> Optional[str]:
        """
        Locate wireguard_configs directory:
        1. ../wireguard_configs (central folder at parent level)
        2. ./wireguard_configs (local subfolder)
        3. ./ (script directory)
        """
        script_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(script_dir)

        candidates = [
            os.path.join(parent_dir, "wireguard_configs"),
            os.path.join(script_dir, "wireguard_configs"),
            script_dir,
        ]
        for c in candidates:
            if os.path.isdir(c):
                return c
        return None

    def get_server_conf_path(self, server_name: str) -> Optional[str]:
        """Find the raw .conf file for a given server name."""
        if not self.config_dir:
            return None
        base = server_name[:-5] if server_name.endswith(".conf") else server_name
        target = os.path.join(self.config_dir, f"{base}.conf")
        if os.path.isfile(target):
            return target
        return None

    def create_runtime_config(self, source_conf_path: str, temp_dir: str) -> str:
        """
        Create ephemeral runtime config inside temp_dir (CHUNKS_DIR),
        appending [Socks5] and [http] blocks without modifying source file.
        """
        os.makedirs(temp_dir, exist_ok=True)
        runtime_conf_path = os.path.join(temp_dir, "_wireproxy_active.tmp.conf")

        with open(source_conf_path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()

        blocks = []
        if "[Socks5]" not in content and "[socks5]" not in content:
            blocks.append(f"\n[Socks5]\nBindAddress = 127.0.0.1:{self.socks5_port}\n")
        if "[http]" not in content and "[Http]" not in content:
            blocks.append(f"\n[http]\nBindAddress = 127.0.0.1:{self.http_port}\n")

        with open(runtime_conf_path, "w", encoding="utf-8", errors="replace") as f:
            f.write(content + "".join(blocks))

        return runtime_conf_path

    def start(self, server_name: str, temp_dir: str) -> bool:
        """
        Launch wireproxy with server_name.
        Returns True if process starts and port responds within 2.5s.
        """
        if not self.wireproxy_bin:
            return False

        source_conf = self.get_server_conf_path(server_name)
        if not source_conf:
            log(f"WIREPROXY: Configuration file for '{server_name}' not found in {self.config_dir}!", level="WARN")
            return False

        self.stop(silent=True)

        try:
            self.active_config_path = self.create_runtime_config(source_conf, temp_dir)
            cmd = [self.wireproxy_bin, "-c", self.active_config_path]

            self.raw_token = raw_external_start("Wireproxy", server_name)
            popen_kwargs = {
                "stdout": subprocess.PIPE,
                "stderr": subprocess.STDOUT,
                "text": True,
                "bufsize": 1,
            }
            if os.name == "nt":
                popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
            else:
                popen_kwargs["start_new_session"] = True

            self.proc = subprocess.Popen(cmd, **popen_kwargs)

            # Spawn background daemon thread to stream wireproxy output into raw diagnostic log
            raw_token = self.raw_token
            def _drain_wireproxy_stdout(p, token):
                if not p or not p.stdout:
                    return
                try:
                    for line in iter(p.stdout.readline, ""):
                        if not line:
                            break
                        raw_external_write(token, line.rstrip("\r\n"))
                except Exception:
                    pass
                finally:
                    try:
                        p.stdout.close()
                    except Exception:
                        pass

            drain_thread = threading.Thread(
                target=_drain_wireproxy_stdout,
                args=(self.proc, raw_token),
                daemon=True,
            )
            drain_thread.start()

            deadline = time.monotonic() + 2.5
            while time.monotonic() < deadline:
                if self.proc.poll() is not None:
                    self.stop(silent=True)
                    return False
                if self.check_port_responsive(timeout=0.15):
                    self.active_server = server_name
                    self.export_environment()
                    return True
                time.sleep(0.1)

            self.stop(silent=True)
            return False
        except Exception as e:
            log(f"WIREPROXY: Failed to launch wireproxy: {e}", level="ERROR")
            if self.raw_token:
                raw_external_end(self.raw_token, status=f"launch exception {type(e).__name__}")
                self.raw_token = None
            self.stop(silent=True)
            return False

    def export_environment(self) -> None:
        """Export proxy environment variables for Python urllib and subprocesses."""
        proxy_url = f"http://127.0.0.1:{self.http_port}"
        os.environ["HTTP_PROXY"] = proxy_url
        os.environ["HTTPS_PROXY"] = proxy_url
        os.environ["http_proxy"] = proxy_url
        os.environ["https_proxy"] = proxy_url
        os.environ["ALL_PROXY"] = f"socks5://127.0.0.1:{self.socks5_port}"
        try:
            import urllib.request
            urllib.request._opener = None
            opener = urllib.request.build_opener(
                urllib.request.ProxyHandler({
                    "http": proxy_url,
                    "https": proxy_url,
                })
            )
            urllib.request.install_opener(opener)
        except Exception:
            pass
        try:
            NetworkRouteResolver.invalidate_cache()
        except Exception:
            pass

    def unexport_environment(self) -> None:
        """Remove proxy environment variables upon tunnel termination."""
        for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY"):
            os.environ.pop(var, None)
        try:
            import urllib.request
            urllib.request._opener = None
            direct_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            urllib.request.install_opener(direct_opener)
        except Exception:
            pass
        try:
            NetworkRouteResolver.invalidate_cache()
        except Exception:
            pass

    def stop(self, silent: bool = False):
        """Clean termination of wireproxy child process and temporary config cleanup."""
        self.unexport_environment()
        was_running = False
        rc = None
        if self.proc:
            try:
                if self.proc.poll() is None:
                    self.proc.terminate()
                    try:
                        rc = self.proc.wait(timeout=1.5)
                    except subprocess.TimeoutExpired:
                        self.proc.kill()
                        rc = self.proc.wait()
                    was_running = True
                else:
                    rc = self.proc.poll()
            except Exception:
                pass
            self.proc = None

        if self.raw_token:
            raw_external_end(self.raw_token, returncode=rc)
            self.raw_token = None

        if self.active_config_path:
            try:
                if os.path.isfile(self.active_config_path):
                    os.remove(self.active_config_path)
            except Exception:
                pass
            self.active_config_path = None

        if was_running and not silent:
            log("Network Route     : Wireproxy tunnel stopped cleanly.", level="INFO")

    def is_running(self) -> bool:
        """Check if wireproxy child process is actively running."""
        return bool(self.proc and self.proc.poll() is None)

    def check_port_responsive(self, timeout: float = 0.2) -> bool:
        """Fast zero-traffic local TCP socket probe to verify listener is accepting connections."""
        try:
            s = socket.create_connection(("127.0.0.1", self.socks5_port), timeout=timeout)
            s.close()
            return True
        except (OSError, socket.timeout):
            return False

    def recover_3_2_1(
        self,
        country: str,
        priority_list: List[str],
        temp_dir: str,
        geo_verifier: Optional[Callable[[str], bool]] = None,
    ) -> bool:
        """
        Universal 3-2-1 Tiered Auto-Recovery Engine:
        - Tier 1 (Primary): 3 attempts
        - Tier 2 (Secondary): 2 attempts
        - Tier 3 (Tertiary): 1 attempt
        """
        if not priority_list:
            return False

        attempts_per_tier = [3, 2, 1]

        servers_to_try = list(priority_list)
        if self.active_server and self.active_server in servers_to_try:
            servers_to_try.remove(self.active_server)
            servers_to_try.insert(0, self.active_server)

        for tier_idx, server in enumerate(servers_to_try):
            max_attempts = attempts_per_tier[tier_idx] if tier_idx < len(attempts_per_tier) else 1
            tier_name = f"Tier {tier_idx + 1}"

            for attempt in range(1, max_attempts + 1):
                log(f"NETWORK_GUARD: [STAGE 1 AUTO-RECOVERY] {tier_name} ({server}) attempt {attempt}/{max_attempts}...", level="INFO")
                if self.start(server, temp_dir):
                    if geo_verifier:
                        if geo_verifier(server):
                            log(f"NETWORK_GUARD: [STAGE 1 AUTO-RECOVERY] Recovered successfully via {server} ({tier_name})!", level="INFO")
                            return True
                        else:
                            log(f"NETWORK_GUARD: {server} port opened but geo verification failed.", level="WARN")
                            self.stop()
                    else:
                        log(f"NETWORK_GUARD: [STAGE 1 AUTO-RECOVERY] Recovered successfully via {server} ({tier_name})!", level="INFO")
                        return True
                time.sleep(0.3)

        log("NETWORK_GUARD: [STAGE 1 AUTO-RECOVERY] All 3-2-1 recovery tiers exhausted without restoration.", level="WARN")
        return False


def is_wireproxy_active(state: Optional[RecorderState]) -> bool:
    """Return True if userspace wireproxy child process is actively running."""
    if not state:
        return False
    mgr = getattr(state, "wireproxy_manager", None)
    return bool(mgr and mgr.is_running())


class NetworkRouteResolver:
    """
    Cross-platform network route and interface resolver.
    """

    _geoip_cache_time: float = 0.0
    _geoip_cache_ip: Optional[str] = None
    _geoip_cache_country: Optional[str] = None

    @classmethod
    def invalidate_cache(cls) -> None:
        """Purge GeoIP cache on network interface transitions or proxy state changes."""
        cls._geoip_cache_time = 0.0
        cls._geoip_cache_ip = None
        cls._geoip_cache_country = None

    @staticmethod
    def get_default_route_info() -> Tuple[str, bool]:
        """
        Inspect the OS routing table to find the egress interface for default traffic.
        Returns: (interface_name, is_vpn_tunnel)
        """
        if sys.platform == "darwin":
            try:
                # Query route to public internet destination (handles both default route & /1 split tunnels)
                res = subprocess.run(
                    ["route", "-n", "get", "8.8.8.8"],
                    capture_output=True,
                    text=True,
                    timeout=1.5,
                )
                m = re.search(r'interface:\s*(\S+)', res.stdout)
                if not m:
                    res = subprocess.run(
                        ["route", "-n", "get", "default"],
                        capture_output=True,
                        text=True,
                        timeout=1.5,
                    )
                    m = re.search(r'interface:\s*(\S+)', res.stdout)
                if m:
                    iface = m.group(1).strip()
                    # On macOS, VPN tunnels typically use utun*, ppp*, or tun*
                    is_vpn = iface.startswith(("utun", "ppp", "tun", "ipsec"))
                    return iface, is_vpn
            except Exception:
                pass
            return "", False

        elif os.name == "nt":
            vpn_keywords = (
                "wireguard", "tap-windows", "wintun", "openvpn",
                "nordlynx", "tailscale", "vpn", "surfshark",
                "ikev2", "ipsec", "proton",
            )
            # Primary: Native Win32 iphlpapi.dll GetBestInterface & GetIfEntry (Zero subprocess, <1ms)
            try:
                iphlpapi = ctypes.windll.iphlpapi
                best_if_index = ctypes.c_ulong()
                # 8.8.8.8 in network byte order as DWORD is 0x08080808
                dest_ip = ctypes.c_ulong(0x08080808)
                if iphlpapi.GetBestInterface(dest_ip, ctypes.byref(best_if_index)) == 0:
                    if_idx = best_if_index.value

                    class MIB_IFROW(ctypes.Structure):
                        _fields_ = [
                            ("wszName", ctypes.c_uint16 * 256),
                            ("dwIndex", ctypes.c_uint32),
                            ("dwType", ctypes.c_uint32),
                            ("dwMtu", ctypes.c_uint32),
                            ("dwSpeed", ctypes.c_uint32),
                            ("dwPhysAddrLen", ctypes.c_uint32),
                            ("bPhysAddr", ctypes.c_uint8 * 8),
                            ("dwAdminStatus", ctypes.c_uint32),
                            ("dwOperStatus", ctypes.c_uint32),
                            ("dwLastChange", ctypes.c_uint32),
                            ("dwInOctets", ctypes.c_uint32),
                            ("dwInUcastPkts", ctypes.c_uint32),
                            ("dwInNUcastPkts", ctypes.c_uint32),
                            ("dwInDiscards", ctypes.c_uint32),
                            ("dwInErrors", ctypes.c_uint32),
                            ("dwInUnknownProtos", ctypes.c_uint32),
                            ("dwOutOctets", ctypes.c_uint32),
                            ("dwOutUcastPkts", ctypes.c_uint32),
                            ("dwOutNUcastPkts", ctypes.c_uint32),
                            ("dwOutDiscards", ctypes.c_uint32),
                            ("dwOutErrors", ctypes.c_uint32),
                            ("dwOutQLen", ctypes.c_uint32),
                            ("dwDescrLen", ctypes.c_uint32),
                            ("bDescr", ctypes.c_uint8 * 256),
                        ]

                    row = MIB_IFROW()
                    row.dwIndex = if_idx
                    if iphlpapi.GetIfEntry(ctypes.byref(row)) == 0:
                        descr = bytes(row.bDescr[:row.dwDescrLen]).decode("utf-8", errors="ignore").strip()
                        is_vpn = any(k in descr.lower() for k in vpn_keywords)
                        iface_name = descr or f"adapter_{if_idx}"
                        return iface_name, is_vpn
            except Exception:
                pass

            # Fallback 1: netsh interface ipv4 show route 0.0.0.0/0
            try:
                res = subprocess.run(
                    ["netsh", "interface", "ipv4", "show", "route", "0.0.0.0/0"],
                    capture_output=True,
                    text=True,
                    timeout=1.5,
                )
                stdout = res.stdout or ""
                for line in stdout.splitlines():
                    if "0.0.0.0/0" in line:
                        parts = line.split()
                        if parts:
                            iface_candidate = " ".join(parts[5:]) if len(parts) >= 6 else parts[-1]
                            is_vpn = any(k in iface_candidate.lower() for k in vpn_keywords)
                            return iface_candidate, is_vpn
            except Exception:
                pass

            # Fallback 2: Parse Active Routes table in route print strictly (ignore Interface List)
            try:
                res = subprocess.run(
                    ["route", "print", "0.0.0.0"],
                    capture_output=True,
                    text=True,
                    timeout=1.5,
                )
                stdout = res.stdout or ""
                lines = stdout.splitlines()
                in_active_routes = False
                for line in lines:
                    if "active routes:" in line.lower():
                        in_active_routes = True
                        continue
                    if in_active_routes:
                        if "persistent routes:" in line.lower():
                            break
                        parts = line.split()
                        if len(parts) >= 5 and parts[0] == "0.0.0.0" and parts[1] == "0.0.0.0":
                            iface_ip = parts[3]
                            # If interface IP is on typical home LAN (192.168.x.x, 10.x.x.x, 172.16.x.x), it's physical LAN
                            if iface_ip.startswith(("192.168.", "172.16.", "172.17.", "172.18.", "172.19.", "172.20.", "172.21.", "172.22.", "172.23.", "172.24.", "172.25.", "172.26.", "172.27.", "172.28.", "172.29.", "172.30.", "172.31.")):
                                return iface_ip, False
                            return iface_ip, True
            except Exception:
                pass

            return "", False

        return "", False

    @staticmethod
    def get_active_vpn_app_name() -> Optional[str]:
        """
        Detect running commercial VPN client applications by inspecting native OS
        service status and running process signatures.
        Returns a friendly brand name (e.g. 'Surfshark', 'NordVPN', 'ExpressVPN') or None.
        """
        vpn_signatures = [
            ("surfshark", "Surfshark"),
            ("nordvpn", "NordVPN"),
            ("nordsec", "NordVPN"),
            ("expressvpn", "ExpressVPN"),
            ("express", "ExpressVPN"),
            ("mullvad", "Mullvad"),
            ("protonvpn", "ProtonVPN"),
            ("proton", "ProtonVPN"),
            ("tailscale", "Tailscale"),
            ("windscribe", "Windscribe"),
            ("cyberghost", "CyberGhost"),
            ("pia-client", "Private Internet Access"),
            ("privateinternetaccess", "Private Internet Access"),
            ("cloudflare", "Cloudflare WARP"),
            ("warp", "Cloudflare WARP"),
            ("anyconnect", "Cisco AnyConnect"),
            ("wireguard", "WireGuard"),
            ("openvpn", "OpenVPN"),
        ]

        if sys.platform == "darwin":
            # 1. macOS Network Extension / scutil service inspection
            try:
                res = subprocess.run(
                    ["scutil", "--nc", "list"],
                    capture_output=True,
                    text=True,
                    timeout=1.5,
                )
                if res.returncode == 0:
                    for line in (res.stdout or "").splitlines():
                        line_low = line.lower()
                        if "(connected)" in line_low:
                            for key, brand in vpn_signatures:
                                if key in line_low:
                                    return brand
            except Exception:
                pass

            # 2. macOS active process inspection
            try:
                pattern = "surfshark|nordvpn|expressvpn|mullvad|proton|tailscale|windscribe|cyberghost|wireguard|openvpn"
                res = subprocess.run(
                    ["pgrep", "-l", "-i", pattern],
                    capture_output=True,
                    text=True,
                    timeout=1.5,
                )
                if res.returncode == 0 and res.stdout:
                    proc_stdout = res.stdout.lower()
                    for key, brand in vpn_signatures:
                        if key in proc_stdout:
                            return brand
            except Exception:
                pass

        elif os.name == "nt":
            # Windows active process inspection via tasklist
            try:
                res = subprocess.run(
                    ["tasklist", "/NH", "/FO", "CSV"],
                    capture_output=True,
                    text=True,
                    timeout=2.0,
                )
                if res.returncode == 0 and res.stdout:
                    proc_stdout = res.stdout.lower()
                    for key, brand in vpn_signatures:
                        if key in proc_stdout:
                            return brand
            except Exception:
                pass

        return None

    @staticmethod
    def get_friendly_vpn_name(iface: str) -> str:
        """Extract a friendly VPN provider name from active app or adapter description."""
        # 1. Prioritize active commercial VPN app/process on host
        app_name = NetworkRouteResolver.get_active_vpn_app_name()
        if app_name:
            return app_name

        # 2. Fallback to adapter name / driver description
        low = (iface or "").lower()
        if "surfshark" in low:
            return "Surfshark"
        if "nord" in low or "nordlynx" in low:
            return "NordVPN"
        if "mullvad" in low:
            return "Mullvad"
        if "proton" in low:
            return "ProtonVPN"
        if "tailscale" in low:
            return "Tailscale"
        if "wireguard" in low:
            return "WireGuard"
        if "openvpn" in low:
            return "OpenVPN"
        if "express" in low or "expressvpn" in low:
            return "ExpressVPN"
        if low.startswith("utun"):
            return "System VPN"
        if "wintun" in low:
            return "Wintun VPN"
        if "tap" in low:
            return "TAP VPN"
        return iface or "System VPN"

    @classmethod
    def get_public_ip_and_country(
        cls,
        timeout: float = 2.5,
        proxy_url: Optional[str] = None,
        force_direct: bool = False,
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Fast, lightweight query to retrieve public IP and 2-letter ISO country code.
        Supports optional proxy_url (e.g. http://127.0.0.1:10809) for Wireproxy egress verification,
        or force_direct to bypass ambient proxy environment variables when probing host routes.
        Uses 10-second cache when query destination is identical.
        Primary: http://ip-api.com/json (accurate BGP/WHOIS geolocation for virtual VPN endpoints)
        Fallback 1: https://api.country.is/
        Fallback 2: https://ipwho.is/
        Fallback 3: https://1.1.1.1/cdn-cgi/trace
        """
        now = time.monotonic()
        if not proxy_url and not force_direct and cls._geoip_cache_time and (now - cls._geoip_cache_time < 10.0):
            if cls._geoip_cache_ip and cls._geoip_cache_country:
                return cls._geoip_cache_ip, cls._geoip_cache_country

        opener = None
        if proxy_url:
            try:
                opener = build_opener(ProxyHandler({
                    "http": proxy_url,
                    "https": proxy_url,
                }))
            except Exception as e:
                log(f"NETWORK_GUARD: Failed to configure proxy opener for {proxy_url}: {e}", level="WARN")
                opener = None
        elif force_direct:
            try:
                opener = build_opener(ProxyHandler({}))
            except Exception:
                opener = None

        def _do_open(req, to):
            if opener is not None:
                return opener.open(req, timeout=to)
            return urlopen(req, timeout=to)

        # Primary: ip-api.com (reflects true IP broadcast country matching Hotstar/Akamai)
        try:
            req = Request(
                "http://ip-api.com/json",
                headers={"User-Agent": "Mozilla/5.0 (RecorderNetworkGuard/1.0)"},
            )
            with _do_open(req, timeout) as resp:
                data = json.loads(resp.read().decode("utf-8", errors="ignore"))
                if data.get("status") == "success":
                    ip = data.get("query")
                    country = data.get("countryCode")
                    if ip and country:
                        ip_clean = str(ip).strip()
                        country_clean = str(country).strip().upper()
                        if not proxy_url:
                            cls._geoip_cache_time = now
                            cls._geoip_cache_ip = ip_clean
                            cls._geoip_cache_country = country_clean
                        return ip_clean, country_clean
        except Exception:
            pass

        # Secondary: api.country.is
        try:
            req = Request(
                "https://api.country.is/",
                headers={"User-Agent": "Mozilla/5.0 (RecorderNetworkGuard/1.0)"},
            )
            with _do_open(req, timeout) as resp:
                data = json.loads(resp.read().decode("utf-8", errors="ignore"))
                ip = data.get("ip")
                country = data.get("country")
                if ip and country:
                    ip_clean = str(ip).strip()
                    country_clean = str(country).strip().upper()
                    if not proxy_url:
                        cls._geoip_cache_time = now
                        cls._geoip_cache_ip = ip_clean
                        cls._geoip_cache_country = country_clean
                    return ip_clean, country_clean
        except Exception:
            pass

        # Tertiary: ipwho.is
        try:
            req = Request(
                "https://ipwho.is/",
                headers={"User-Agent": "Mozilla/5.0 (RecorderNetworkGuard/1.0)"},
            )
            with _do_open(req, timeout) as resp:
                data = json.loads(resp.read().decode("utf-8", errors="ignore"))
                ip = data.get("ip")
                country = data.get("country_code")
                if ip and country:
                    ip_clean = str(ip).strip()
                    country_clean = str(country).strip().upper()
                    if not proxy_url:
                        cls._geoip_cache_time = now
                        cls._geoip_cache_ip = ip_clean
                        cls._geoip_cache_country = country_clean
                    return ip_clean, country_clean
        except Exception:
            pass

        # Fallback: Cloudflare trace
        for endpoint in ("https://1.1.1.1/cdn-cgi/trace", "https://cloudflare.com/cdn-cgi/trace"):
            try:
                req = Request(
                    endpoint,
                    headers={"User-Agent": "Mozilla/5.0 (RecorderNetworkGuard/1.0)"},
                )
                with _do_open(req, timeout) as resp:
                    data = resp.read().decode("utf-8", errors="ignore")
                    parsed = dict(
                        line.split("=", 1)
                        for line in data.strip().splitlines()
                        if "=" in line
                    )
                    ip = parsed.get("ip")
                    loc = parsed.get("loc")
                    if ip and loc:
                        ip_clean = ip.strip()
                        country_clean = loc.strip().upper()
                        if not proxy_url:
                            cls._geoip_cache_time = now
                            cls._geoip_cache_ip = ip_clean
                            cls._geoip_cache_country = country_clean
                        return ip_clean, country_clean
            except Exception:
                continue

        return None, None

    @classmethod
    def is_internet_online(cls, timeout: float = 1.5) -> bool:
        """
        Fast low-overhead TCP socket connectivity test to public DNS servers on port 53 / 443.
        Returns True if outbound internet is reachable, False if physical internet is down.
        """
        endpoints = (
            ("1.1.1.1", 53),
            ("8.8.8.8", 53),
            ("1.1.1.1", 443),
        )
        for host, port in endpoints:
            try:
                s = socket.create_connection((host, port), timeout=timeout)
                s.close()
                return True
            except OSError:
                continue
        return False

    @classmethod
    def is_internet_online_debounced(
        cls,
        required_consecutive_failures: int = 2,
        retry_delay: float = 1.5,
        timeout_per_probe: float = 1.5,
    ) -> bool:
        """
        Verify outbound internet with confirmation retry.
        Returns True if internet is online on any attempt.
        Returns False only if ALL consecutive attempts fail, ensuring transient
        routing-table switches (1-2s during VPN disconnect on macOS/Windows)
        do not trigger false-positive disconnects.
        """
        for attempt in range(max(1, int(required_consecutive_failures))):
            if cls.is_internet_online(timeout=timeout_per_probe):
                return True
            if attempt < required_consecutive_failures - 1:
                time.sleep(retry_delay)
        return False


class NetworkGuard:
    """
    Self-contained Network Guardrail & Rate-Limit Circuit Breaker.
    """

    DEFAULT_PROVIDER_POLICIES: Dict[str, dict] = {
        # STRICT PROVIDERS (Instant Kill - Token / Strict Geo Protection)
        # Only Hotstar is strict by default due to strict session token invalidation
        "hotstar.com":       {"country": "IN", "policy": "STRICT"},

        # LENIENT PROVIDERS (Allow N_m3u8DL-RE Natural Retry & Catch-Up)
        # All other providers default to LENIENT
        "fancode.com":       {"country": "IN", "policy": "LENIENT"},
        "jiocinema.com":     {"country": "IN", "policy": "LENIENT"},
        "jiotv.com":         {"country": "IN", "policy": "LENIENT"},
        "hundxvision.co.uk": {"country": "IN", "policy": "LENIENT"},
        "kayosports.com.au": {"country": "AU", "policy": "LENIENT"},
        "foxtel.com.au":     {"country": "AU", "policy": "LENIENT"},
        "sky.com":           {"country": "GB", "policy": "LENIENT"},
        "bbc.co.uk":         {"country": "GB", "policy": "LENIENT"},
        "sonyliv.com":       {"country": "IN", "policy": "LENIENT"},
        "7plus.com.au":      {"country": "AU", "policy": "LENIENT"},
        "yospace.com":       {"country": "AU", "policy": "LENIENT"},
        "tapmad.com":        {"country": "PK", "policy": "LENIENT"},
        "tamashaweb.com":    {"country": "PK", "policy": "LENIENT"},
    }

    def __init__(self, state=None):
        self.state = state

        target_mat = ""
        if DOWNLOAD_MODE == "N_m3u8DL-RE":
            target_mat = NM3U8DL_PART_A
        elif DOWNLOAD_MODE == "ffmpeg":
            target_mat = f"{LIVE_URL} {FFMPEG_PART_A}"
        elif DOWNLOAD_MODE == "yt-dlp":
            target_mat = YTDLP_PART_A
        elif DOWNLOAD_MODE == "streamlink":
            target_mat = STREAMLINK_PART_A

        req_country, policy, source = self.resolve_provider_settings(target_mat, REQUIRED_VPN_COUNTRY)
        self.required_country = req_country
        self.drop_policy = policy
        self.detection_source = source
        self.is_armed = (req_country is not None)
        self.lenient_drop_active: bool = False

        # Resolve exclusive mode: UNRESTRICTED, WIREPROXY, or LEGACY_VPN
        self.wireproxy_manager: Optional[WireproxyManager] = None
        self.vpn_mode = "UNRESTRICTED"
        self.active_system_vpn: Optional[str] = None
        self.bypassed_wireproxy: bool = False
        self.vpn_conflict: bool = False
        self.conflict_country: Optional[str] = None
        self.conflict_iface: str = ""
        self.conflict_vpn_name: str = ""

        # Launch network snapshot
        self.launch_ip: Optional[str] = None
        self.launch_country: Optional[str] = None
        self.launch_interface: str = ""
        self.launch_is_vpn: bool = False

        if self.is_armed:
            # 1. Inspect host system default route
            host_iface, host_is_vpn = NetworkRouteResolver.get_default_route_info()
            host_provider = NetworkRouteResolver.get_friendly_vpn_name(host_iface) if host_is_vpn else ""

            # Check if wireproxy is capable on this machine
            wm = WireproxyManager(
                socks5_port=WIREPROXY_SOCKS5_PORT,
                http_port=WIREPROXY_HTTP_PORT,
            )
            server_list = VPN_SERVER_PRIORITY.get(self.required_country, [])
            has_configs = any(wm.get_server_conf_path(s) for s in server_list) if server_list else False
            wireproxy_capable = bool(wm.wireproxy_bin and has_configs)

            if host_is_vpn:
                # Host system ALREADY has an active VPN app (e.g. Surfshark, NordVPN, etc.)
                host_ip, host_loc = NetworkRouteResolver.get_public_ip_and_country(timeout=2.0)
                if host_loc == self.required_country:
                    # Target country matches! Auto-bypass Wireproxy to prevent double-VPN conflict
                    self.vpn_mode = "LEGACY_VPN"
                    self.active_system_vpn = host_provider
                    self.bypassed_wireproxy = wireproxy_capable
                    self.launch_ip = host_ip
                    self.launch_country = host_loc
                    self.launch_interface = host_iface
                    self.launch_is_vpn = True
                    self.last_route_interface = host_iface
                    self.last_route_is_vpn = True
                else:
                    # Host VPN is connected to a conflicting country (e.g. US != IN)
                    self.vpn_mode = "LEGACY_VPN"
                    self.active_system_vpn = host_provider
                    self.vpn_conflict = True
                    self.conflict_country = host_loc
                    self.conflict_iface = host_iface
                    self.conflict_vpn_name = host_provider
                    self.launch_ip = host_ip
                    self.launch_country = host_loc
                    self.launch_interface = host_iface
                    self.launch_is_vpn = True
                    self.last_route_interface = host_iface
                    self.last_route_is_vpn = True
            elif wireproxy_capable:
                # No host VPN running, and Wireproxy configs are present -> pure userspace tunnel
                self.vpn_mode = "WIREPROXY"
                self.wireproxy_manager = wm
                if state:
                    state.wireproxy_manager = wm
            else:
                # No host VPN and no Wireproxy configs -> Legacy VPN (will verify host route in preflight)
                self.vpn_mode = "LEGACY_VPN"

        # Tracking metrics
        self.vpn_drop_count: int = 0
        self.vpn_hold_total_duration: float = 0.0
        self.internet_drop_count: int = 0
        self.internet_hold_total_duration: float = 0.0
        self.rate_limit_429_count: int = 0
        self.last_429_incident_mono: float = 0.0
        self.token_expiry_count: int = 0
        # Initialize startup window timestamp explicitly at launch (B5)
        self._last_token_gate_pass_mono: Optional[float] = time.monotonic()

        # Real-time 429 & 403 tracking
        self.recent_429_timestamps: List[float] = []
        self.rate_limit_strike: int = 0
        self.has_403_error: bool = False
        self.blocked_403_ip: Optional[str] = None
        self.last_403_incident_mono: float = 0.0

        # Runtime route tracking
        self.last_route_interface: str = ""
        self.last_route_is_vpn: bool = False
        self.last_route_check_mono: float = 0.0
        self.last_internet_check_mono: float = 0.0
        self.last_internet_status: bool = True
        self.hold_active: bool = False

        # Double-VPN runtime tracking
        self._runtime_double_vpn_warned: bool = False
        self._last_double_vpn_provider: str = ""

    @classmethod
    def resolve_provider_settings(
        cls,
        target_material: str,
        country_override: Optional[str] = None,
    ) -> Tuple[Optional[str], str, str]:
        """
        Resolve required VPN country and drop policy (STRICT vs LENIENT).
        Default for unlisted/unknown providers is LENIENT (unless engine is FFmpeg).
        """
        mat_lower = target_material.lower()
        matched_country = None
        matched_policy = "LENIENT"  # Default for unlisted/other providers is LENIENT
        source_label = "unrestricted (open stream)"

        for domain, info in cls.DEFAULT_PROVIDER_POLICIES.items():
            if domain in mat_lower:
                matched_country = info["country"]
                matched_policy = info["policy"]
                source_label = f"auto_domain: {domain}"
                break

        final_country = matched_country
        if country_override and str(country_override).strip():
            c_val = str(country_override).strip().upper()
            if c_val in ("NONE", "UNRESTRICTED", "ANY", "OFF"):
                final_country = None
                source_label = "user_override: unrestricted"
            else:
                final_country = c_val
                source_label = f"user_override: {c_val}"

        return final_country, matched_policy, source_label

    def is_lenient_drop_allowed(self) -> bool:
        """
        LENIENT mode (do NOT kill process mid-stream) is ONLY permitted when:
        1. Download mode is N_m3u8DL-RE (segmented downloader with retry buffer)
        2. Provider policy is LENIENT (e.g. SonyLIV, unlisted)
        """
        return (
            DOWNLOAD_MODE == ENGINE_NM3U8DL
            and self.drop_policy == "LENIENT"
        )

    def attempt_wireguard_auto_recovery(self, blocked_ip: Optional[str] = None) -> bool:
        """
        Stage 1: Silent WireGuard Auto-Recovery Hook.
        Under Wireproxy mode, executes 3-2-1 tiered recovery across configured servers.
        Returns False immediately when unconfigured or non-wireproxy.
        """
        if getattr(self.state, "stop_flag", False):
            return False

        if self.vpn_mode != "WIREPROXY" or not self.wireproxy_manager:
            return False

        temp_dir = CHUNKS_DIR or (f"{BASE_NAME}_chunks" if BASE_NAME else "")
        priority_list = VPN_SERVER_PRIORITY.get(self.required_country, [])
        if not priority_list:
            return False

        def _geo_check(server_name: str) -> bool:
            proxy_url = f"http://127.0.0.1:{self.wireproxy_manager.http_port}"
            ip, loc = NetworkRouteResolver.get_public_ip_and_country(timeout=2.5, proxy_url=proxy_url)
            if loc != self.required_country:
                return False
            if blocked_ip and ip == blocked_ip:
                return False
            self.launch_ip = ip
            self.launch_country = loc
            return True

        recovered = self.wireproxy_manager.recover_3_2_1(
            country=self.required_country,
            priority_list=priority_list,
            temp_dir=temp_dir,
            geo_verifier=_geo_check,
        )
        if recovered:
            self.last_route_is_vpn = True
            return True

        self.last_route_is_vpn = False
        return False

    def format_header_lines(self) -> List[str]:
        """Format standard lines for inclusion in the main startup header."""
        lines = []
        if self.vpn_conflict:
            effective_policy = "LENIENT" if self.is_lenient_drop_allowed() else "STRICT"
            vpn_label = f"SYSTEM_VPN: {self.conflict_vpn_name}" if self.conflict_vpn_name else "SYSTEM_VPN"
            lines.append(f"Network Guard     : CONFLICT [{vpn_label}] (Connected: {self.conflict_country or 'Unknown'} != Required: {self.required_country} | Policy: {effective_policy} | Target: {self.detection_source})")
        elif self.vpn_mode == "WIREPROXY":
            effective_policy = "LENIENT" if self.is_lenient_drop_allowed() else "STRICT"
            servers = VPN_SERVER_PRIORITY.get(self.required_country, [])
            primary = servers[0] if servers else "unspecified"
            lines.append(f"Network Guard     : ARMED [WIREPROXY] (Required: {self.required_country} | Primary: {primary} | Policy: {effective_policy} | Target: {self.detection_source})")
            lines.append(f"Proxy Endpoints   : SOCKS5 127.0.0.1:{WIREPROXY_SOCKS5_PORT} | HTTP 127.0.0.1:{WIREPROXY_HTTP_PORT}")
        elif self.vpn_mode == "LEGACY_VPN":
            effective_policy = "LENIENT" if self.is_lenient_drop_allowed() else "STRICT"
            vpn_label = f"SYSTEM_VPN: {self.active_system_vpn}" if self.active_system_vpn else "SYSTEM_VPN"
            adapter_info = f" | Adapter: {self.launch_interface}" if self.launch_interface else ""
            lines.append(f"Network Guard     : ARMED [{vpn_label}] (Required: {self.required_country}{adapter_info} | Policy: {effective_policy} | Target: {self.detection_source})")
        else:
            lines.append("Network Guard     : UNRESTRICTED (Open / Generic Stream)")
        return lines

    def preflight_check(self, is_scheduled_trigger: bool = False) -> bool:
        """
        Verify network route and geo egress before connecting to broadcaster endpoints.
        """
        if self.vpn_mode == "UNRESTRICTED":
            if not NetworkRouteResolver.is_internet_online():
                log("Network Route     : Outbound internet connectivity unreachable!", level="WARN")
                return self.handle_internet_hold_state(self.state, is_startup=True)
            return True

        if self.vpn_conflict:
            log(
                f"Network Route     : Conflict detected! Host system VPN ({self.conflict_vpn_name}) is active on '{self.conflict_iface}' "
                f"(Country: {self.conflict_country or 'Unknown'}), but stream requires {self.required_country}.",
                level="WARN",
            )
            log(
                f"Network Route     : Nesting Wireproxy inside another VPN causes packet drop. "
                f"Please disconnect {self.conflict_vpn_name} or switch it to {self.required_country}.",
                level="WARN",
            )
            return self.handle_vpn_hold_state(self.state, is_startup=True)

        is_scheduled = (bool(SCHEDULE_START and SCHEDULE_START.strip()) and not is_scheduled_trigger)

        if self.bypassed_wireproxy:
            if not self.launch_ip or not self.launch_country:
                ip, loc = NetworkRouteResolver.get_public_ip_and_country(timeout=2.5)
                self.launch_ip = ip or self.launch_ip
                self.launch_country = loc or self.launch_country
            if not self.launch_interface:
                iface, _ = NetworkRouteResolver.get_default_route_info()
                self.launch_interface = iface

            if is_scheduled:
                log(f"Network Route     : Ready [SYSTEM_VPN: {self.active_system_vpn}] (Staged for scheduled start | Adapter: {self.launch_interface})", level="INFO")
                return True
            log(f"Network Route     : Active System VPN detected on host ({self.active_system_vpn} | IP: {self.launch_ip} | Country: {self.launch_country})", level="INFO")
            log(f"Network Route     : Wireproxy bypassed to avoid double-VPN conflict. Using {self.active_system_vpn} directly.", level="INFO")
            log(
                f"Network Route     : Connected [SYSTEM_VPN: {self.active_system_vpn}] "
                f"(Country: {self.launch_country} | Adapter: {self.launch_interface} | IP: {self.launch_ip})",
                level="INFO",
            )
            return True

        if self.vpn_mode == "WIREPROXY":
            server_list = VPN_SERVER_PRIORITY.get(self.required_country, [])
            primary_server = server_list[0] if server_list else "default"
            temp_dir = CHUNKS_DIR or (f"{BASE_NAME}_chunks" if BASE_NAME else "")

            if not NetworkRouteResolver.is_internet_online():
                log("Network Route     : Outbound internet connectivity unreachable!", level="WARN")
                if not self.handle_internet_hold_state(self.state, is_startup=True):
                    return False

            if is_scheduled:
                log(f"Network Route     : Ready [WIREPROXY] (Staged for scheduled start | Server: {primary_server})", level="INFO")
                return True

            log(f"Network Route     : Launching Wireproxy ({primary_server})...", level="INFO")
            if not self.wireproxy_manager.start(primary_server, temp_dir):
                log(f"Network Route     : Initial launch of {primary_server} failed. Executing auto-recovery...", level="WARN")
                if not self.attempt_wireguard_auto_recovery():
                    return self.handle_vpn_hold_state(self.state, is_startup=True)

            proxy_url = f"http://127.0.0.1:{self.wireproxy_manager.http_port}"
            ip, loc = NetworkRouteResolver.get_public_ip_and_country(timeout=3.0, proxy_url=proxy_url)
            self.launch_ip = ip
            self.launch_country = loc
            self.last_route_is_vpn = True

            if loc == self.required_country:
                log(
                    f"Network Route     : Connected [WIREPROXY] (Country: {loc} | Server: {self.wireproxy_manager.active_server} | IP: {ip})",
                    level="INFO",
                )
                return True
            else:
                log(
                    f"Network Route     : Country mismatch via Wireproxy! Connected to {loc or 'Unknown'} != Required: {self.required_country}",
                    level="WARN",
                )
                return self.handle_vpn_hold_state(self.state, is_startup=True)

        elif self.vpn_mode == "LEGACY_VPN":
            iface, is_vpn = NetworkRouteResolver.get_default_route_info()
            ip, loc = NetworkRouteResolver.get_public_ip_and_country(timeout=2.5)

            self.last_route_interface = iface
            self.last_route_is_vpn = is_vpn
            provider = NetworkRouteResolver.get_friendly_vpn_name(iface)
            self.active_system_vpn = provider if is_vpn else None

            if not is_vpn:
                log(
                    f"Network Route     : Default route is not a VPN interface ({iface or 'unknown'})!",
                    level="WARN",
                )
                return self.handle_vpn_hold_state(self.state, is_startup=True)

            if is_scheduled:
                log(f"Network Route     : Ready [SYSTEM_VPN: {provider}] (Staged for scheduled start | Adapter: {iface})", level="INFO")
                return True

            if loc == self.required_country:
                self.launch_ip = ip
                self.launch_country = loc
                self.launch_interface = iface
                self.launch_is_vpn = is_vpn
                log(
                    f"Network Route     : Connected [SYSTEM_VPN: {provider}] (Country: {loc} | Adapter: {iface} | IP: {ip})",
                    level="INFO",
                )
                return True
            else:
                log(
                    f"Network Route     : Country mismatch! Connected to {loc or 'Unknown'} "
                    f"({iface}) != Required: {self.required_country}",
                    level="WARN",
                )
                return self.handle_vpn_hold_state(self.state, is_startup=True)

        return True

    def check_runtime_route(self) -> bool:
        """
        Zero-traffic route check (called every 1s during recording).
        In Wireproxy mode: checks wireproxy child process & local port 10808 (0 internet packets).
        In Legacy mode: checks OS kernel default route (utun* / Windows adapter).
        Returns False if the VPN connection has dropped.
        """
        if getattr(self.state, "stop_flag", False):
            return True

        if self.vpn_mode == "UNRESTRICTED":
            return True

        now = time.monotonic()
        if now - self.last_route_check_mono < 1.0:
            return self.last_route_is_vpn
        self.last_route_check_mono = now

        if self.vpn_mode == "WIREPROXY":
            is_ok = bool(
                self.wireproxy_manager
                and self.wireproxy_manager.is_running()
                and self.wireproxy_manager.check_port_responsive(timeout=0.15)
            )
            prev_ok = self.last_route_is_vpn
            self.last_route_is_vpn = is_ok
            if not is_ok and (prev_ok or prev_ok is None):
                log("NETWORK_GUARD: Wireproxy connection severed or port 10808 unresponsive!", level="WARN")

            # Runtime Double-VPN detection: alert if host system VPN app auto-connected in background
            if is_ok:
                host_iface, host_is_vpn = NetworkRouteResolver.get_default_route_info()
                if host_is_vpn:
                    provider = NetworkRouteResolver.get_friendly_vpn_name(host_iface)
                    self._last_double_vpn_provider = provider
                    if not getattr(self, "_runtime_double_vpn_warned", False):
                        self._runtime_double_vpn_warned = True
                        log(
                            f"NETWORK_GUARD: Active System VPN ({provider}) detected on host ({host_iface}) "
                            f"while Wireproxy is running! Nesting Wireproxy inside {provider} creates a double-VPN conflict. "
                            f"Please disconnect {provider} to maintain clean Wireproxy routing.",
                            level="WARN",
                        )
                        beep_bad(self.state)
                else:
                    if getattr(self, "_runtime_double_vpn_warned", False):
                        if NetworkRouteResolver.is_internet_online(timeout=1.0):
                            prev_provider = getattr(self, "_last_double_vpn_provider", "") or "System VPN"
                            self._runtime_double_vpn_warned = False
                            self._last_double_vpn_provider = ""
                            log(
                                f"NETWORK_GUARD: Host System VPN ({prev_provider}) disconnected. "
                                f"Double-VPN conflict resolved; clean Wireproxy routing restored.",
                                level="INFO",
                            )
                            log_good_beep("double_vpn_resolved")
                            beep_good(self.state)
            return is_ok

        elif self.vpn_mode == "LEGACY_VPN":
            prev_is_vpn = self.last_route_is_vpn
            iface, is_vpn = NetworkRouteResolver.get_default_route_info()
            self.last_route_interface = iface
            self.last_route_is_vpn = is_vpn

            if not is_vpn:
                if prev_is_vpn or prev_is_vpn is None:
                    log(f"NETWORK_GUARD: Default route severed from VPN interface ({iface or 'unknown'})!", level="WARN")
                return False

            return True

        return True

    def is_wireproxy_nested_in_vpn(self) -> Tuple[bool, str]:
        """
        Check if Wireproxy is currently running nested inside an active host system VPN.
        Returns: (is_nested, host_vpn_name)
        """
        if self.vpn_mode == "WIREPROXY":
            host_iface, host_is_vpn = NetworkRouteResolver.get_default_route_info()
            if host_is_vpn:
                provider = NetworkRouteResolver.get_friendly_vpn_name(host_iface)
                return True, provider
        return False, ""

    def mark_startup_begin(self) -> None:
        """Restart the silent startup window at the moment the first chunk launch is attempted (D3)."""
        self._last_token_gate_pass_mono = time.monotonic()

    def is_startup_phase(self) -> bool:
        """
        Check if recorder is in initial startup phase (silent hold window).
        True strictly when zero good chunks have been recorded AND within STARTUP_SILENT_WINDOW_SEC
        of launch / last token gate pass. Once first chunk is safely sealed or 120s elapses, returns False.
        """
        st = self.state
        good = 0
        if st and hasattr(st, "stats"):
            try:
                good = int(st.stats.get("good_chunks", 0) or 0)
            except Exception:
                good = 0
        if good > 0:
            return False
        last = self._last_token_gate_pass_mono
        if last is None:
            # Fallback lazy initialization if engine did not explicitly seed timestamp (B5)
            self._last_token_gate_pass_mono = time.monotonic()
            return True
        return (time.monotonic() - last) < STARTUP_SILENT_WINDOW_SEC

    def _proactive_token_gate_check(self, state=None) -> bool:
        """Evaluate stream token validity before chunk launch, bypassing dynamic playlist mode."""
        st = state or self.state
        source_mode = getattr(st, "source_mode", None)
        if source_mode != "playlist":
            if self.is_stream_token_expired(lead_time_sec=0.0, state=st) and \
                    getattr(st, "token_clock_override_url", None) != self.get_active_source_url():
                is_startup = self.is_startup_phase()
                log(
                    "NETWORK_GUARD: Stream authentication token already expired! "
                    "Diverting to Token Hold before chunk launch...",
                    level="WARN",
                )
                return self.handle_token_hold_state(st, is_startup=is_startup)
        self._last_token_gate_pass_mono = time.monotonic()
        return True

    def inter_chunk_gate_check(self, state, temp_dir: Optional[str] = None) -> bool:
        """
        Inter-chunk route re-assessment (evaluates network route between chunks / attempts).
        - If Wireproxy was running and a host VPN (e.g. Surfshark) auto-connected:
          * If country matches required_country: seamlessly transitions to direct LEGACY_VPN,
            stops Wireproxy, avoiding nested double-tunneling.
          * If country conflicts: flags conflict and enters VPN Hold.
        - If LEGACY_VPN was active and host VPN dropped:
          * If Wireproxy configs are available: automatically starts Wireproxy and transitions to WIREPROXY.
          * Otherwise enters VPN Hold.
        - If physical internet dropped: enters Internet Hold.
        Returns True if route is valid to launch next attempt, False if recording should stop.
        """
        if state and getattr(state, "stop_flag", False):
            return False

        if self.vpn_mode == "UNRESTRICTED":
            if not self.check_runtime_internet():
                time.sleep(1.5)
                if not self.check_runtime_internet():
                    log("NETWORK_GUARD: Internet offline before chunk launch! Entering Hold state...", level="WARN")
                    return self.handle_internet_hold_state(state, is_startup=False)
            return self._proactive_token_gate_check(state)

        if not self.is_armed:
            return self._proactive_token_gate_check(state)

        # Check physical internet first
        if not NetworkRouteResolver.is_internet_online(timeout=1.5):
            time.sleep(1.5)
            if not NetworkRouteResolver.is_internet_online(timeout=1.5):
                log("NETWORK_GUARD: Outbound internet not reachable before chunk launch! Entering Internet Hold...", level="WARN")
                return self.handle_internet_hold_state(state, is_startup=False)

        host_iface, host_is_vpn = NetworkRouteResolver.get_default_route_info()
        host_provider = NetworkRouteResolver.get_friendly_vpn_name(host_iface) if host_is_vpn else ""

        # Case 1: Currently in WIREPROXY mode, but host system VPN connected in background
        if self.vpn_mode == "WIREPROXY":
            if host_is_vpn:
                # Host VPN appeared! Invalidate GeoIP cache to prevent reading Wireproxy's cached entry
                NetworkRouteResolver.invalidate_cache()
                # Probe country to check for target match or conflict via direct host route,
                # bypassing Wireproxy's active proxy environment variables
                host_ip, host_loc = NetworkRouteResolver.get_public_ip_and_country(timeout=2.0, force_direct=True)
                if host_loc == self.required_country:
                    log(
                        f"NETWORK_GUARD: Active System VPN ({host_provider} | {host_iface}) verified on target country ({host_loc}). "
                        f"Seamlessly transitioning to direct VPN mode for next attempt.",
                        level="INFO",
                    )
                    self.vpn_mode = "LEGACY_VPN"
                    self.active_system_vpn = host_provider
                    self.launch_ip = host_ip
                    self.launch_country = host_loc
                    self.launch_interface = host_iface
                    self.launch_is_vpn = True
                    self.last_route_interface = host_iface
                    self.last_route_is_vpn = True
                    self.bypassed_wireproxy = True
                    if self.wireproxy_manager:
                        self.wireproxy_manager.stop()
                    for h_line in self.format_header_lines():
                        log(h_line)
                    log(
                        f"Network Route     : Connected [SYSTEM_VPN: {self.active_system_vpn}] "
                        f"(Country: {self.launch_country} | Adapter: {self.launch_interface} | IP: {self.launch_ip})",
                        level="INFO",
                    )
                    return self._proactive_token_gate_check(state)
                else:
                    log(
                        f"NETWORK_GUARD: Host System VPN ({host_provider} | {host_iface}) connected to conflicting country "
                        f"({host_loc or 'Unknown'} != {self.required_country})! Diverting to VPN Hold...",
                        level="WARN",
                    )
                    self.vpn_conflict = True
                    self.conflict_country = host_loc
                    self.conflict_iface = host_iface
                    self.conflict_vpn_name = host_provider
                    return self.handle_vpn_hold_state(state, is_startup=False)
            else:
                # No host VPN, continue with Wireproxy check
                if not self.check_runtime_route():
                    time.sleep(1.5)
                    if not self.check_runtime_route():
                        log("NETWORK_GUARD: Wireproxy route not verified before chunk launch! Entering Hold state...", level="WARN")
                        return self.handle_vpn_hold_state(state, is_startup=False)
                return self._proactive_token_gate_check(state)

        # Case 2: Currently in LEGACY_VPN mode
        elif self.vpn_mode == "LEGACY_VPN":
            if not host_is_vpn:
                # Host VPN dropped! Check if Wireproxy can auto-recover
                server_list = VPN_SERVER_PRIORITY.get(self.required_country, [])
                wm = self.wireproxy_manager or WireproxyManager(
                    socks5_port=WIREPROXY_SOCKS5_PORT,
                    http_port=WIREPROXY_HTTP_PORT,
                )
                has_configs = any(wm.get_server_conf_path(s) for s in server_list) if server_list else False
                wireproxy_capable = bool(wm.wireproxy_bin and has_configs)
                if wireproxy_capable:
                    log("NETWORK_GUARD: Host System VPN disconnected between chunks. Auto-engaging Wireproxy tunnel...", level="INFO")
                    primary_server = server_list[0] if server_list else "default"
                    self.wireproxy_manager = wm
                    if state:
                        state.wireproxy_manager = wm
                    target_dir = temp_dir or CHUNKS_DIR or (f"{BASE_NAME}_chunks" if BASE_NAME else "")
                    if self.wireproxy_manager.start(primary_server, target_dir):
                        proxy_url = f"http://127.0.0.1:{self.wireproxy_manager.http_port}"
                        ip, loc = NetworkRouteResolver.get_public_ip_and_country(timeout=3.0, proxy_url=proxy_url)
                        if loc == self.required_country:
                            self.vpn_mode = "WIREPROXY"
                            self.active_system_vpn = None
                            self.launch_ip = ip
                            self.launch_country = loc
                            self.last_route_is_vpn = True
                            self.bypassed_wireproxy = False
                            for h_line in self.format_header_lines():
                                log(h_line)
                            log(
                                f"Network Route     : Connected [WIREPROXY] (Country: {loc} | Server: {self.wireproxy_manager.active_server} | IP: {ip})",
                                level="INFO",
                            )
                            return self._proactive_token_gate_check(state)
                        else:
                            self.wireproxy_manager.stop()
                    return self.handle_vpn_hold_state(state, is_startup=False)
                else:
                    log(f"NETWORK_GUARD: Default route severed from VPN interface ({host_iface or 'unknown'})! Diverting to VPN Hold...", level="WARN")
                    return self.handle_vpn_hold_state(state, is_startup=False)
            else:
                # Host is still on VPN, verify route
                if not self.check_runtime_route():
                    time.sleep(1.5)
                    if not self.check_runtime_route():
                        log("NETWORK_GUARD: VPN route not verified before chunk launch! Entering Hold state...", level="WARN")
                        return self.handle_vpn_hold_state(state, is_startup=False)
                return self._proactive_token_gate_check(state)

        return self._proactive_token_gate_check(state)

    def check_runtime_internet(self) -> bool:
        """
        In-flight internet connectivity test throttled to 5.0 seconds.
        Returns False if outbound internet is unreachable.
        """
        now = time.monotonic()
        if now - self.last_internet_check_mono < 5.0:
            return self.last_internet_status

        self.last_internet_check_mono = now
        status = NetworkRouteResolver.is_internet_online(timeout=1.5)
        if not status:
            # Debounce: brief 1.0s backoff and second confirmation probe to filter transient packet loss / jitter
            time.sleep(1.0)
            status = NetworkRouteResolver.is_internet_online(timeout=1.5)
        self.last_internet_status = status
        return status

    def inspect_stdout_line(self, line: str) -> bool:
        """
        Inspect child engine stdout line for HTTP 429 (Too Many Requests) or 403 Forbidden.
        Returns True if sustained rate-limit threshold is crossed.
        """
        now = time.monotonic()
        raw = line.lower()

        if "403" in raw and ("forbidden" in raw or "status code does not indicate success: 403" in raw or "http error 403" in raw):
            self.last_403_incident_mono = now
            self.has_403_error = True
            if not self.blocked_403_ip:
                self.blocked_403_ip = self.launch_ip

        if "429" in raw and ("too many requests" in raw or "status code does not indicate success: 429" in raw or "http error 429" in raw):
            self.recent_429_timestamps.append(now)
            self.recent_429_timestamps = [t for t in self.recent_429_timestamps if now - t <= RATE_LIMIT_WINDOW_SEC]

            if len(self.recent_429_timestamps) >= RATE_LIMIT_ERROR_THRESHOLD:
                self.rate_limit_429_count += 1
                self.last_429_incident_mono = now
                self.recent_429_timestamps.clear()
                return True
            return False

        return False

    def handle_vpn_hold_state(self, state, is_startup: bool = False, blocked_ip: Optional[str] = None) -> bool:
        """
        30-Minute VPN Hold State: freezes traffic, preserves captured media,
        sounds alarm, and recovers upon VPN restoration.
        """
        if state:
            state.lenient_alarm_active = False
        # If VPN route is already healthy upon entering hold, bypass hold immediately
        if self.vpn_mode == "WIREPROXY":
            if self.wireproxy_manager and self.wireproxy_manager.is_running() and self.wireproxy_manager.check_port_responsive():
                proxy_url = f"http://127.0.0.1:{self.wireproxy_manager.http_port}"
                ip, loc = NetworkRouteResolver.get_public_ip_and_country(timeout=2.0, proxy_url=proxy_url)
                if loc == self.required_country and (not blocked_ip or ip != blocked_ip):
                    self.launch_ip = ip
                    self.launch_country = loc
                    self.last_route_is_vpn = True
                    self.blocked_403_ip = None
                    if state:
                        state.network_guard_hold_active = False
                        state.vpn_drop_post_ack_silent = False
                    log(f"NETWORK_GUARD: Wireproxy route verified healthy (Country: {loc} | Server: {self.wireproxy_manager.active_server} | IP: {ip}). Bypassing Hold state.", level="INFO")
                    if not is_startup and state:
                        return self._verify_and_resume_stream(state)
                    return True
        else:
            cur_iface, cur_is_vpn = NetworkRouteResolver.get_default_route_info()
            if cur_is_vpn:
                ip, loc = NetworkRouteResolver.get_public_ip_and_country(timeout=2.0)
                if loc == self.required_country and (not blocked_ip or ip != blocked_ip):
                    self.launch_ip = ip
                    self.launch_country = loc
                    self.launch_interface = cur_iface
                    self.launch_is_vpn = cur_is_vpn
                    self.last_route_interface = cur_iface
                    self.last_route_is_vpn = cur_is_vpn
                    provider = NetworkRouteResolver.get_friendly_vpn_name(cur_iface)
                    self.active_system_vpn = provider
                    self.blocked_403_ip = None
                    if state:
                        state.network_guard_hold_active = False
                        state.vpn_drop_post_ack_silent = False
                    log(f"NETWORK_GUARD: VPN route verified healthy (Country: {loc} | Provider: {provider} | IP: {ip}). Bypassing Hold state.", level="INFO")
                    if not is_startup and state:
                        return self._verify_and_resume_stream(state)
                    return True

        self.hold_active = True
        if state:
            state.network_guard_hold_active = True
            state.lenient_alarm_active = False
        self.vpn_drop_count += 1
        hold_start_mono = time.monotonic()

        should_alarm = (bool(SCHEDULE_START and str(SCHEDULE_START).strip()) if is_startup else True)
        provider_name = self.active_system_vpn or "System VPN"

        if blocked_ip:
            banner = [
                "====================================================================",
                f"[NETWORK_GUARD_HOLD] Egress IP {blocked_ip} rejected with HTTP 403 Forbidden!",
                f"Broadcaster CDN has flagged this {provider_name} server IP.",
                f"Please reconnect or switch location in {provider_name} to acquire a fresh IP.",
                f"Target Country : {self.required_country}",
                "Holding for up to 30 minutes for IP rotation / VPN restoration...",
            ]
        else:
            banner = [
                "====================================================================",
                "[NETWORK_GUARD_HOLD] VPN route severed or country mismatch!",
                f"Target Country : {self.required_country}",
                "Active Chunk   : Safely sealed to disk (0 packets leaked outside VPN)",
                "Holding for up to 30 minutes for VPN restoration...",
            ]
        already_silenced = bool(state and getattr(state, "vpn_drop_post_ack_silent", False))
        if should_alarm and not already_silenced:
            banner.append("Press 'A' to silence audio alarm.")
        elif already_silenced:
            banner.append("Audio alarm already silenced by operator.")
        else:
            banner.append("Connect VPN to resume, or press Ctrl+C to cancel.")
        banner.append("====================================================================")

        for b_line in banner:
            log(b_line, level="WARN")

        internet_active = bool(state and (getattr(state, "internet_hold_active", False) or getattr(state, "internet_drop_active", False)))
        if state and should_alarm and not already_silenced and not internet_active:
            shared_alarm_start(state, alarm_type="vpn_drop")
        else:
            if state and not already_silenced and not internet_active:
                beep_bad(state)

        last_wait_len = 0
        alarm_silenced = already_silenced

        while True:
            now = time.monotonic()

            if state and state.stop_flag:
                self.vpn_hold_total_duration += (now - hold_start_mono)
                self.hold_active = False
                if state:
                    state.network_guard_hold_active = False
                    shared_alarm_stop(state)
                clear_progress_line()
                return False

            if state and getattr(state, "alarm_ack_requested", False):
                shared_alarm_stop(state)
                state.alarm_ack_requested = False
                alarm_silenced = True
                clear_progress_line()
                log("ALARM_ACK (VPN Hold audio alarm silenced by operator)", level="INFO")

            elapsed = now - hold_start_mono
            if elapsed >= VPN_HOLD_TIMEOUT_SEC:
                self.vpn_hold_total_duration += elapsed
                self.hold_active = False
                if state:
                    state.network_guard_hold_active = False
                    shared_alarm_stop(state)
                clear_progress_line()
                log("NETWORK_GUARD: 30-minute VPN Hold timed out without restoration. Finalizing recording.", level="ERROR")
                return False

            rem = max(0.0, VPN_HOLD_TIMEOUT_SEC - elapsed)

            # Live single-line countdown on the terminal (using the codebase's standard render_progress_line)
            alarm_hint = " (Alarm silenced)" if (alarm_silenced or getattr(state, "vpn_drop_post_ack_silent", False)) else (" (Press 'A' to silence alarm)" if should_alarm else "")
            if blocked_ip:
                hold_text = f"[NETWORK_GUARD_HOLD] Waiting for fresh IP != {blocked_ip} ({self.required_country})... Timeout remaining: {fmt_hms(rem)}{alarm_hint}"
            else:
                hold_text = f"[NETWORK_GUARD_HOLD] Waiting for VPN ({self.required_country})... Timeout remaining: {fmt_hms(rem)}{alarm_hint}"
            pad = max(0, last_wait_len - len(hold_text))
            render_progress_line(hold_text, pad=pad)
            last_wait_len = len(hold_text)

            if self.vpn_mode == "WIREPROXY":
                if int(elapsed) % 10 == 0:
                    if self.attempt_wireguard_auto_recovery(blocked_ip=blocked_ip):
                        clear_progress_line()
                        self.vpn_hold_total_duration += (now - hold_start_mono)
                        self.hold_active = False
                        self.blocked_403_ip = None
                        if state:
                            state.network_guard_hold_active = False
                            state.vpn_drop_post_ack_silent = False
                            shared_alarm_stop(state)
                            log_good_beep("vpn_restored")
                            beep_good(state)
                        log(
                            f"NETWORK_GUARD: Wireproxy connection restored (Server: {self.wireproxy_manager.active_server} | IP: {self.launch_ip}). Guardrail cleared.",
                            level="INFO",
                        )
                        if not is_startup and state:
                            return self._verify_and_resume_stream(state)
                        return True
            else:
                cur_iface, cur_is_vpn = NetworkRouteResolver.get_default_route_info()
                if cur_is_vpn:
                    ip, loc = NetworkRouteResolver.get_public_ip_and_country(timeout=2.0)
                    if loc == self.required_country and (not blocked_ip or ip != blocked_ip):
                        clear_progress_line()
                        self.vpn_hold_total_duration += (now - hold_start_mono)
                        self.hold_active = False
                        self.launch_ip = ip
                        self.launch_country = loc
                        self.launch_interface = cur_iface
                        self.launch_is_vpn = cur_is_vpn
                        self.last_route_interface = cur_iface
                        self.last_route_is_vpn = cur_is_vpn
                        provider = NetworkRouteResolver.get_friendly_vpn_name(cur_iface)
                        self.active_system_vpn = provider
                        self.blocked_403_ip = None
                        if state:
                            state.network_guard_hold_active = False
                            state.vpn_drop_post_ack_silent = False
                            shared_alarm_stop(state)
                            log_good_beep("vpn_restored")
                            beep_good(state)
                        log(
                            f"NETWORK_GUARD: VPN connection restored (Country: {loc} | Provider: {provider} | IP: {ip}). Guardrail cleared.",
                            level="INFO",
                        )
                        if not is_startup and state:
                            return self._verify_and_resume_stream(state)
                        return True

            time.sleep(1.0)

    def get_active_source_url(self) -> Optional[str]:
        """Extract the active streaming URL from state or engine configuration."""
        st = self.state
        if st:
            live_url = getattr(st, "live_url", None) or getattr(st, "active_stream_url", None)
            if isinstance(live_url, str) and live_url.strip():
                return live_url.strip()
        mat = self.get_active_source_material()
        if mat and isinstance(mat, str):
            m = re.search(r'["\'](https?://[^"\']+)["\']', mat)
            if m:
                return m.group(1)
            m2 = re.search(r'(https?://\S+)', mat)
            if m2:
                return m2.group(1)
        global_url = get_active_source_url()
        if global_url and isinstance(global_url, str) and global_url.strip():
            return global_url.strip()
        return None

    def get_active_source_material(self) -> str:
        """Retrieve active command line material or source URL string."""
        if self.state:
            for attr in ("nm3u8dl_part_a", "active_command", "live_url", "active_stream_url"):
                val = getattr(self.state, attr, None)
                if isinstance(val, str) and val.strip():
                    return val.strip()
        if DOWNLOAD_MODE == "N_m3u8DL-RE" and NM3U8DL_PART_A:
            return NM3U8DL_PART_A
        elif DOWNLOAD_MODE == "ffmpeg":
            return f"{LIVE_URL} {FFMPEG_PART_A}"
        elif DOWNLOAD_MODE == "yt-dlp":
            return YTDLP_PART_A
        elif DOWNLOAD_MODE == "streamlink":
            return STREAMLINK_PART_A
        return ""

    @staticmethod
    def extract_stream_expiry(url_or_cmd: Optional[str]) -> Optional[int]:
        """
        Extract token expiry epoch timestamp from URL or command string.
        Supports query params: exp, expires, expiry. Normalizes 13-digit ms to seconds.
        """
        if not url_or_cmd:
            return None
        expiries = []
        for match in re.finditer(
            r'(?:^|[?&~=;])(?:exp(?:ires?|iry)?)=(\d+)',
            str(url_or_cmd),
            flags=re.IGNORECASE,
        ):
            raw_val = int(match.group(1))
            if 1_000_000_000_000 <= raw_val <= 2_500_000_000_000:
                raw_val = raw_val // 1000
            if 1_000_000_000 <= raw_val <= 2_500_000_000:
                expiries.append(raw_val)
        return min(expiries) if expiries else None

    def is_stream_token_expired(self, lead_time_sec: float = 0.0, state=None) -> bool:
        """
        Check if the active stream token is already expired (default: no future runway).
        Returns False if no expiry parameter is present.
        """
        st = state or self.state
        tokenised, expiry = self._stream_is_token_governed(self.get_active_source_url() or "", state=st)
        if not tokenised or expiry is None:
            return False
        return float(expiry) <= (time.time() + lead_time_sec)

    def _probe_stream_status(self, url: str, timeout: float = 3.0, use_range: bool = True) -> Tuple[bool, Optional[int]]:
        """
        Probe stream URL via HTTP GET with optional Range request.
        Returns:
            (True, status_code) on HTTP 200/206.
            (False, status_code) on HTTP error (e.g. 401, 403, 404, 416).
            (False, None) on network timeout or connection failure.
        """
        if not url:
            return False, None
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
            }
            if use_range:
                headers["Range"] = "bytes=0-100"
            req = Request(url, headers=headers)
            opener = None
            if self.vpn_mode == "WIREPROXY" and self.wireproxy_manager and self.wireproxy_manager.is_running():
                proxy_handler = ProxyHandler({
                    "http": f"http://127.0.0.1:{self.wireproxy_manager.http_port}",
                    "https": f"http://127.0.0.1:{self.wireproxy_manager.http_port}",
                })
                opener = build_opener(proxy_handler)

            open_fn = opener.open if opener else urlopen
            with open_fn(req, timeout=timeout) as resp:
                status = getattr(resp, "status", None) or getattr(resp, "code", None)
                if status in (200, 206):
                    return True, status
                return False, status
        except HTTPError as e:
            return False, e.code
        except Exception:
            return False, None

    def _probe_stream_url(self, url: str, timeout: float = 3.0) -> bool:
        """Bool wrapper preserved for the operator re-paste path and legacy callers."""
        return self._probe_stream_status(url, timeout)[0]

    def _stream_is_token_governed(self, url: str, state=None) -> Tuple[bool, Optional[int]]:
        """
        Evaluate if a stream is governed by an expiring authentication token.
        Scans URLs found in the stream URL and all active source material fields for expiration timestamps (N5).
        """
        st = state or self.state
        raw = [url] if url else []
        if st:
            for attr in ("nm3u8dl_part_a", "active_command", "live_url", "active_stream_url"):
                val = getattr(st, attr, None)
                if isinstance(val, str) and val.strip():
                    raw.append(val.strip())
        urls = []
        for s in raw:
            found = _URL_RE.findall(s)
            if found:
                urls.append(found[0])
        exps = [e for e in (self.extract_stream_expiry(u) for u in urls) if e is not None]
        expiry = min(exps) if exps else None
        return (expiry is not None), expiry

    def update_stream_url(self, new_input: str) -> None:
        """
        Update stream URL and headers across active command material.
        Accepts both bare URLs and full command lines.
        """
        candidate = str(new_input or "").strip()
        if not candidate:
            return

        is_bare_url = (
            (candidate.startswith("http://") or candidate.startswith("https://"))
            and len(candidate.split()) == 1
        )

        new_url = ""
        if is_bare_url:
            new_url = candidate
        else:
            m = re.search(r'["\'](https?://[^"\']+)["\']', candidate)
            if m:
                new_url = m.group(1)
            else:
                m2 = re.search(r'(https?://\S+)', candidate)
                if m2:
                    new_url = m2.group(1)

        if not new_url:
            return

        cleaned_cmd = ""
        if not is_bare_url:
            cleaned_cmd = re.sub(
                r'\s+--save-name\s+(?:"[^"]*"|\'[^\']*\'|\S+)',
                "",
                candidate,
            ).strip()

        def _replace_url_in_cmd(cmd_str: str) -> str:
            if not cmd_str:
                return cmd_str
            if not is_bare_url and cleaned_cmd:
                return cleaned_cmd
            # Replace exactly the first URL (the stream URL), the same one _stream_is_token_governed scans,
            # so quoted secondary URLs (Referer/Origin headers) can never absorb the replacement.
            first = re.search(r'https?://[^\s"\']+', cmd_str)
            if first:
                return cmd_str[:first.start()] + new_url + cmd_str[first.end():]
            return cmd_str

        global NM3U8DL_PART_A, LIVE_URL, FFMPEG_PART_A, YTDLP_PART_A, STREAMLINK_PART_A
        if DOWNLOAD_MODE == "N_m3u8DL-RE":
            NM3U8DL_PART_A = _replace_url_in_cmd(NM3U8DL_PART_A)
        elif DOWNLOAD_MODE == "ffmpeg":
            LIVE_URL = new_url
            if FFMPEG_PART_A and FFMPEG_PART_A.strip():
                FFMPEG_PART_A = _replace_url_in_cmd(FFMPEG_PART_A)
        elif DOWNLOAD_MODE == "yt-dlp":
            YTDLP_PART_A = _replace_url_in_cmd(YTDLP_PART_A)
        elif DOWNLOAD_MODE == "streamlink":
            STREAMLINK_PART_A = _replace_url_in_cmd(STREAMLINK_PART_A)

        st = self.state
        if st:
            setattr(st, "token_clock_override_url", None)
            setattr(st, "active_stream_url", new_url)
            setattr(st, "live_url", new_url)
            for attr in ("nm3u8dl_part_a", "active_command"):
                part_a = getattr(st, attr, "")
                if isinstance(part_a, str) and part_a.strip():
                    setattr(st, attr, _replace_url_in_cmd(part_a))
                elif attr == "nm3u8dl_part_a" and not is_bare_url and cleaned_cmd:
                    setattr(st, attr, cleaned_cmd)

    def _verify_and_resume_stream(self, state=None) -> bool:
        """
        Unattended Resumption Protocol (token-aware, format-agnostic):
        - Feeds without expiring tokens: Resumes unattended recording immediately without probing.
        - Dynamic playlist mode: Exempt from clock-based holds; resumes unattended for dynamic manifest refresh.
        - Feeds with expiring tokens:
          * Probes endpoint (Question 1 Option B confirmation probe with Range fallback & transport retry).
          * Diverts to Token Hold on HTTP 401, 403, or 410 (non-playlist feeds).
          * Inconclusive probe results (timeout, 5xx, 416): Resumes recording via engine retry.
        """
        st = state or self.state
        url = self.get_active_source_url()

        def _resume(msg: str) -> bool:
            log(msg, level="INFO")
            return True

        def _hold(msg: str) -> bool:
            if st is not None:
                setattr(st, "token_clock_override_url", None)
            log(msg, level="WARN")
            return self.handle_token_hold_state(st, is_startup=self.is_startup_phase())

        if not url:
            return True

        network_type = "VPN" if self.is_armed else "internet connection"
        geo = f" [Launch egress: {getattr(self, 'launch_country', None) or 'Unknown'} | Required: {getattr(self, 'required_country', None) or 'ANY'}]"
        tokenised, expiry = self._stream_is_token_governed(url, state=st)
        is_playlist = getattr(st, "source_mode", None) == "playlist"

        # 1. Non-tokenized feeds or dynamic playlist mode: zero probing, immediate unattended resumption
        if is_playlist or not tokenised:
            return _resume(f"NETWORK_GUARD: No hold-eligible token on stream (playlist or untokenised). Route restored via {network_type}. Resuming unattended recording.")

        # 2. Tokenized feeds: probe endpoint first (Option B confirmation probe)
        ok, status = self._probe_stream_status(url)
        if not ok and status is None:
            ok, status = self._probe_stream_status(url)                   # one transport retry, no sleep
        if not ok and status in (400, 405, 406, 416, 501):
            ok, status = self._probe_stream_status(url, use_range=False)  # live feed rejects Range

        if ok:
            if expiry is not None and float(expiry) <= time.time() and st is not None:
                setattr(st, "token_clock_override_url", url)              # probe beat the clock (Option B)
            return _resume(f"NETWORK_GUARD: Stream verified via restored {network_type}. Resuming unattended recording.")

        # 3. Divert to Token Hold on explicit auth rejection (401/403) or expired signed link (410 on tokenized feed)
        if status in (401, 403) or (status == 410 and tokenised):
            return _hold(f"NETWORK_GUARD: Stream authorization rejected (HTTP {status}). Diverting to Token Hold...{geo}")

        # 4. If timestamp elapsed during outage and probe confirmed 404 (dead link) on non-playlist feed
        if expiry is not None and float(expiry) <= time.time() and status == 404:
            return _hold(f"NETWORK_GUARD: Stream token expired during outage. Diverting to Token Hold...{geo}")

        # 5. Inconclusive / non-auth probe failure: resume via engine retry
        return _resume(f"NETWORK_GUARD: Post-restoration probe inconclusive ({status or 'timeout'}). Resuming via recorder retry.")

    def _redraw_hold_prompt(self, state=None) -> None:
        try:
            with _CONSOLE_LOCK:
                st = state or self.state
                buf = getattr(st, "token_hold_input_buffer", "") if st else ""
                if not isinstance(buf, str):
                    buf = ""
                sys.stdout.write("TOKEN_HOLD> " + buf)
                sys.stdout.flush()
        except (OSError, ValueError):
            pass

    def _hold_log(self, msg: str, level: str = "INFO", state=None) -> None:
        try:
            with _CONSOLE_LOCK:
                clear_progress_line()  # "\r\x1b[2K": wipe partial prompt line (B3)
                log(msg, level=level)
                self._redraw_hold_prompt(state)
        except (OSError, ValueError):
            pass

    def handle_token_hold_state(self, state=None, is_startup: bool = False) -> bool:
        """
        30-Minute Stream Token Expiry Hold State:
        Safely halts downstream attempts, manages alarm (silent on manual startup,
        audible mid-match/scheduled), displays clean prompt with 30s countdown logs,
        and accepts updated URL or command.
        Guarantees cleanup of token_hold_active, escape state, and alarm via try...finally (B1).
        """
        st = state or self.state
        if st:
            setattr(st, "token_clock_override_url", None)
        try:
            return self._token_hold_loop(st, is_startup=is_startup)
        finally:
            if st:
                st.token_hold_active = False
                st._hold_esc_state = None
                st._hold_esc_ts = 0.0
                st.token_hold_input_buffer = ""
                st.token_hold_submitted_url = None
                st._last_submit_ch = ""
            try:
                if st:
                    shared_alarm_stop(st)
            except Exception as exc:
                try:
                    log(f"NETWORK_GUARD: [TOKEN_HOLD] alarm_stop failed during cleanup: {exc}", level="WARN")
                except Exception:
                    pass  # never let a logging failure mask the original exception

    def _token_hold_loop(self, state=None, is_startup: bool = False) -> bool:
        st = state or self.state
        self.token_expiry_count += 1
        if not sys.stdin.isatty():
            log(
                "NETWORK_GUARD: [TOKEN_HOLD] Headless/non-interactive stdin detected. "
                "Cannot prompt operator for updated URL. Finalizing recording.",
                level="ERROR",
            )
            if st:
                st.stop_flag = True
            return False

        hold_start_mono = time.monotonic()
        if st:
            st.token_hold_active = True
            st.token_hold_input_buffer = ""
            st.token_hold_submitted_url = None
            st._hold_esc_state = None
            st._hold_esc_ts = 0.0

        is_scheduled_run = bool(st and getattr(st, "is_scheduled", False))
        should_alarm = (is_scheduled_run if is_startup else True)
        already_silenced = bool(st and getattr(st, "token_expired_post_ack_silent", False))
        alarm_wanted = should_alarm and not already_silenced
        good = 0
        if st and hasattr(st, "stats"):
            try:
                good = int(st.stats.get("good_chunks", 0) or 0)
            except Exception:
                good = 0

        banner = [
            "====================================================================",
            "[TOKEN_HOLD] Stream authentication token expired!",
            "Outbound VPN and internet verified healthy.",
        ]
        if good > 0:
            banner.append("Active chunk safely sealed to disk.")
        banner += [
            "Holding for up to 30 minutes for updated stream URL.",
            "Paste updated URL or full N_m3u8DL-RE command and press ENTER to resume.",
            "Press 'A' to silence audio alarm." if alarm_wanted
            else "Audio alarm inactive (manual startup; sounds after 120s without response / already silenced).",
            "====================================================================",
        ]

        for b_line in banner:
            log(b_line, level="WARN")

        if st and alarm_wanted:
            shared_alarm_start(st, alarm_type="token_expired")
            if not shared_alarm_is_active(st):
                beep_bad(st)

        self._redraw_hold_prompt(st)

        alarm_silenced = already_silenced
        escalated = False
        last_countdown_log_mono = hold_start_mono
        attended_mono = hold_start_mono
        last_seen_buf = ""

        while True:
            now = time.monotonic()
            cur_buf = getattr(st, "token_hold_input_buffer", "") if st else ""
            if isinstance(cur_buf, str) and cur_buf != last_seen_buf:
                last_seen_buf = cur_buf
                attended_mono = now          # operator is typing/pasting

            if st and st.stop_flag:
                if st:
                    st.token_hold_active = False
                    shared_alarm_stop(st)
                clear_progress_line()
                return False

            deadline_ts = getattr(st, "deadline_ts", None) if st else None
            if isinstance(deadline_ts, (int, float)) and time.time() >= deadline_ts:
                if st:
                    st.token_hold_active = False
                    st.stop_flag = True
                    shared_alarm_stop(st)
                clear_progress_line()
                log("NETWORK_GUARD: Planned recording duration reached during Token Hold. Finalizing recording.", level="INFO")
                return False

            if st and getattr(st, "alarm_ack_requested", False):
                shared_alarm_stop(st)
                st.alarm_ack_requested = False
                st.token_expired_post_ack_silent = True
                alarm_silenced = True
                self._hold_log("ALARM_ACK (Token Expiry Hold audio alarm silenced by operator)", level="INFO", state=st)

            elapsed = now - hold_start_mono
            if (not should_alarm and not alarm_silenced and not escalated
                    and (now - attended_mono) >= STARTUP_SILENT_WINDOW_SEC):
                escalated = True
                alarm_wanted = True
                self._hold_log(f"NETWORK_GUARD: [TOKEN_HOLD] No operator response for {int(STARTUP_SILENT_WINDOW_SEC)}s. Sounding alarm.", level="WARN", state=st)
                if st:
                    shared_alarm_start(st, alarm_type="token_expired")
                    if not shared_alarm_is_active(st):
                        beep_bad(st)
            if elapsed >= TOKEN_HOLD_TIMEOUT_SEC:
                if st:
                    st.token_hold_active = False
                    st.stop_flag = True
                    shared_alarm_stop(st)
                clear_progress_line()
                log("NETWORK_GUARD: 30-minute Token Hold timed out without updated URL. Finalizing recording.", level="ERROR")
                return False

            submitted = getattr(st, "token_hold_submitted_url", None) if st else None
            if submitted:
                attended_mono = now
                st.token_hold_submitted_url = None
                candidate = submitted.strip()
                self._hold_log("[TOKEN_HOLD] Operator entered candidate stream URL / command. Verifying connectivity...", level="INFO", state=st)

                try:
                    probe_url = ""
                    if candidate.startswith("http://") or candidate.startswith("https://"):
                        probe_url = candidate.split()[0]
                    else:
                        m = re.search(r'["\'](https?://[^"\']+)["\']', candidate)
                        if m:
                            probe_url = m.group(1)
                        else:
                            m2 = re.search(r'(https?://\S+)', candidate)
                            if m2:
                                probe_url = m2.group(1)

                    cand_exp = self.extract_stream_expiry(candidate)
                    now_ts = time.time()
                    if cand_exp is not None and float(cand_exp) <= now_ts:
                        self._hold_log(
                            "NETWORK_GUARD: [TOKEN_HOLD] Submitted URL token is already expired. "
                            "Please paste a freshly generated stream URL or command:",
                            level="WARN",
                            state=st,
                        )
                    else:
                        bypass_probe = bool(
                            probe_url and cand_exp is not None
                            and (float(cand_exp) - now_ts) < TOKEN_REPLACEMENT_PROBE_BYPASS_SEC
                        )
                        if bypass_probe or (probe_url and self._probe_stream_url(probe_url)):
                            self.update_stream_url(candidate)  # only pre-commit step allowed to trigger re-paste path
                            self._last_token_gate_pass_mono = time.monotonic()
                            post_actions = []
                            if st:
                                st.token_hold_active = False
                                st.token_expired_post_ack_silent = False
                                st._hold_esc_state = None
                                st._hold_esc_ts = 0.0
                                post_actions = [
                                    lambda: shared_alarm_stop(st),
                                    lambda: log_good_beep("stream_token_renewed"),
                                    lambda: beep_good(st),
                                ]
                            # Post-commit side effects: URL already applied, so failures here must never resume the hold.
                            for post_action in post_actions:
                                try:
                                    post_action()
                                except Exception as post_exc:
                                    try:
                                        log(
                                            f"NETWORK_GUARD: [TOKEN_HOLD] post-renewal action failed (ignored): {post_exc}",
                                            level="WARN",
                                        )
                                    except Exception:
                                        pass
                            log("NETWORK_GUARD: Stream token verified and updated successfully! Resuming recording.", level="INFO")
                            return True
                        else:
                            self._hold_log(
                                "NETWORK_GUARD: Candidate URL failed verification probe (HTTP 401/403/404 or connection error). "
                                "Please paste a valid, active stream URL or command:",
                                level="WARN",
                                state=st,
                            )
                except Exception as exc:
                    self._hold_log(
                        f"NETWORK_GUARD: [TOKEN_HOLD] Candidate processing failed ({type(exc).__name__}: {exc}). "
                        "Please paste a valid, active stream URL or command:",
                        level="WARN",
                        state=st,
                    )

            rem = max(0.0, TOKEN_HOLD_TIMEOUT_SEC - elapsed)
            post_ack = bool(st and getattr(st, "token_expired_post_ack_silent", False))
            if alarm_silenced or post_ack:
                alarm_hint = " (Alarm silenced)"
            elif not alarm_wanted:
                alarm_hint = " (Alarm inactive - manual startup)"
            else:
                alarm_hint = " (Press 'A' to silence alarm)"
            if now - last_countdown_log_mono >= TOKEN_HOLD_COUNTDOWN_LOG_SEC:
                last_countdown_log_mono = now
                self._hold_log(f"[TOKEN_HOLD] Waiting for updated stream URL... Timeout remaining: {fmt_hms(rem)}{alarm_hint}", level="INFO", state=st)

            time.sleep(1.0)

    def handle_403_recovery(self, state, is_startup: bool = False) -> bool:
        """Unified 403 Forbidden (CDN IP-Block / Geo-Block / Token Expiry) Recovery Protocol."""
        st = state or self.state
        blocked_ip = self.blocked_403_ip or self.launch_ip
        if not blocked_ip:
            ip, _ = NetworkRouteResolver.get_public_ip_and_country(timeout=1.5)
            blocked_ip = ip or "unknown"
            self.blocked_403_ip = blocked_ip

        provider_name = self.active_system_vpn or "System VPN"
        log(f"NETWORK_GUARD: Broadcaster CDN rejected egress IP ({blocked_ip}) with HTTP 403 Forbidden!", level="WARN")

        # 1. Verify route health first: if VPN dropped or country mismatch, divert to VPN Hold
        vpn_healthy = True
        if self.is_armed:
            if not self.check_runtime_route():
                vpn_healthy = False
            elif self.launch_country and self.launch_country != self.required_country:
                vpn_healthy = False

        if not vpn_healthy:
            log(f"NETWORK_GUARD: Egress VPN route severed or country mismatch during 403. Diverting to VPN Hold...", level="WARN")
            recovered = self.handle_vpn_hold_state(st, is_startup=is_startup, blocked_ip=blocked_ip)
            if recovered:
                self.has_403_error = False
                self.blocked_403_ip = None
                return True
            return False

        good_chunks = 0
        if st and hasattr(st, "stats"):
            good_chunks = st.stats.get("good_chunks", 0)

        is_token_expired = self.is_stream_token_expired(lead_time_sec=0.0)

        # STAGE 1: Under Wireproxy on startup before any chunks and token not expired, attempt 3-2-1 IP rotation
        if self.vpn_mode == "WIREPROXY" and self.wireproxy_manager and good_chunks == 0 and not is_token_expired:
            log(f"NETWORK_GUARD: Initiating Wireproxy 3-2-1 IP pool rotation to evade 403 block on {blocked_ip}...", level="INFO")
            if self.attempt_wireguard_auto_recovery(blocked_ip=blocked_ip):
                log(f"NETWORK_GUARD: Successfully acquired fresh IP ({self.launch_ip}) != {blocked_ip} via Wireproxy. Resuming recording.", level="INFO")
                self.has_403_error = False
                self.blocked_403_ip = None
                return True
            log("NETWORK_GUARD: Wireproxy 3-2-1 pool rotation failed to find unblocked IP.", level="WARN")

        # STAGE 2: Decoupled Token Expiry Hold (VPN route is verified healthy)
        log(
            f"NETWORK_GUARD: Route verified healthy ({self.launch_country or self.required_country}); "
            f"403 classified as STREAM_TOKEN_EXPIRED. Diverting to Token Hold...",
            level="WARN",
        )
        recovered = self.handle_token_hold_state(st, is_startup=is_startup)
        if recovered:
            self.has_403_error = False
            self.blocked_403_ip = None
            return True
        return False

    def handle_rate_limit_cooldown(self, state) -> bool:
        """
        Three-Strike HTTP 429 Lifecycle:
        - Strike 1: 90-second backoff cooldown to let edge bucket clear.
        - Strike 2: 240-second extended in-place backoff cooldown on the same IP (suppressing mid-stream IP rotation to prevent token-hopping account lockouts).
        - Strike 3: Persistent 429 after extended in-place cooldowns confirms account concurrency lock; terminates with critical siren.
        """
        self.rate_limit_strike += 1
        provider_name = self.active_system_vpn or "System VPN"
        current_ip = self.launch_ip or "unknown"

        if self.rate_limit_strike == 1:
            cooldown_start = time.monotonic()
            banner = [
                "====================================================================",
                "[RATE-LIMIT STRIKE 1/3] HTTP 429 detected from Broadcaster CDN edge.",
                "Active chunk safely sealed to disk.",
                f"Halting requests for {int(RATE_LIMIT_COOLDOWN_SEC)}s cooldown to allow server rate limits to reset...",
                "====================================================================",
            ]
            for b_line in banner:
                log(b_line, level="WARN")

            total_cd = float(RATE_LIMIT_COOLDOWN_SEC)
            last_cd_len = 0

            while True:
                now = time.monotonic()
                if state and state.stop_flag:
                    clear_progress_line()
                    return False

                elapsed = now - cooldown_start
                rem = max(0.0, total_cd - elapsed)
                if rem <= 0.0:
                    break

                cd_text = f"[RATE_LIMIT_COOLDOWN] Strike 1/3: CDN 429 backoff... Cooldown remaining: {int(rem)}s"
                pad = max(0, last_cd_len - len(cd_text))
                render_progress_line(cd_text, pad=pad)
                last_cd_len = len(cd_text)

                time.sleep(1.0)

            clear_progress_line()
            log("RATE_LIMIT: 90s cooldown completed. Resuming next chunk attempt.", level="INFO")
            return True

        elif self.rate_limit_strike == 2:
            cooldown_start = time.monotonic()
            banner = [
                "====================================================================",
                "[RATE-LIMIT STRIKE 2/3] HTTP 429 persisted after 90s cooldown!",
                f"Suppressing mid-stream IP rotation to prevent token-hopping account lockout on egress IP ({current_ip}).",
                f"Halting requests for {int(RATE_LIMIT_EXTENDED_COOLDOWN_SEC)}s extended cooldown to allow CDN edge bucket to drain...",
                "====================================================================",
            ]
            for b_line in banner:
                log(b_line, level="WARN")

            total_cd = float(RATE_LIMIT_EXTENDED_COOLDOWN_SEC)
            last_cd_len = 0

            while True:
                now = time.monotonic()
                if state and state.stop_flag:
                    clear_progress_line()
                    return False

                elapsed = now - cooldown_start
                rem = max(0.0, total_cd - elapsed)
                if rem <= 0.0:
                    break

                cd_text = f"[RATE_LIMIT_COOLDOWN] Strike 2/3: Extended CDN 429 backoff... Cooldown remaining: {int(rem)}s"
                pad = max(0, last_cd_len - len(cd_text))
                render_progress_line(cd_text, pad=pad)
                last_cd_len = len(cd_text)

                time.sleep(1.0)

            clear_progress_line()
            log(
                f"RATE_LIMIT: {int(RATE_LIMIT_EXTENDED_COOLDOWN_SEC)}s extended cooldown completed on {current_ip}. Resuming next chunk attempt.",
                level="INFO",
            )
            return True

        else:
            banner = [
                "====================================================================",
                "[RATE-LIMIT STRIKE 3/3] HTTP 429 persisted after extended in-place cooldowns!",
                "Broadcaster CDN is enforcing an account-level rate limit or concurrency lock.",
                "Terminating recording with critical alarm to protect subscriber token.",
                "====================================================================",
            ]
            for b_line in banner:
                log(b_line, level="ERROR")

            nm3u8dl_critical_stop(
                state,
                "HTTP 429 persistent after extended cooldowns → Account concurrency exhausted or token locked → EXIT + ALARM_CRITICAL",
            )
            return False

    def handle_internet_hold_state(self, state, is_startup: bool = False) -> bool:
        """
        30-Minute General Internet Hold: freezes engine attempts during general
        network outages, prevents false-positive retry exhaustion on non-VPN streams,
        sounds alarm, and resumes automatically upon internet restoration.
        """
        if state:
            state.lenient_alarm_active = False
        # If internet is already online upon entering hold, bypass hold immediately
        if NetworkRouteResolver.is_internet_online(timeout=2.0):
            if state:
                state.internet_hold_active = False
                state.internet_drop_active = False
                state.internet_drop_post_ack_silent = False
            log("NETWORK_GUARD: Outbound internet verified online. Bypassing Internet Hold state.", level="INFO")
            if not is_startup and state:
                return self._verify_and_resume_stream(state)
            return True

        self.hold_active = True
        if state:
            state.internet_hold_active = True
            state.lenient_alarm_active = False
            state.vpn_drop_post_ack_silent = False
        self.internet_drop_count += 1
        hold_start_mono = time.monotonic()

        should_alarm = not is_startup

        banner = [
            "====================================================================",
            "[INTERNET_HOLD] Outbound internet connectivity lost!",
            "Active Chunk   : Safely sealed to disk",
            "Holding for up to 30 minutes for internet restoration...",
        ]
        already_silenced = bool(state and getattr(state, "internet_drop_post_ack_silent", False))
        if should_alarm and not already_silenced:
            banner.append("Press 'A' to silence audio alarm.")
        elif already_silenced:
            banner.append("Audio alarm already silenced by operator.")
        else:
            banner.append("Restore internet to resume, or press Ctrl+C to cancel.")
        banner.append("====================================================================")

        for b_line in banner:
            log(b_line, level="WARN")

        if state and should_alarm and not already_silenced:
            shared_alarm_start(state, alarm_type="internet_drop")
        else:
            if state and not already_silenced:
                beep_bad(state)

        last_wait_len = 0
        alarm_silenced = already_silenced

        while True:
            now = time.monotonic()

            if state and state.stop_flag:
                self.internet_hold_total_duration += (now - hold_start_mono)
                self.hold_active = False
                if state:
                    state.internet_hold_active = False
                    state.internet_drop_active = False
                    shared_alarm_stop(state)
                clear_progress_line()
                return False

            if state and getattr(state, "alarm_ack_requested", False):
                shared_alarm_stop(state)
                state.alarm_ack_requested = False
                state.internet_drop_post_ack_silent = True
                alarm_silenced = True
                clear_progress_line()
                log("ALARM_ACK (Internet Hold audio alarm silenced by operator)", level="INFO")

            elapsed = now - hold_start_mono
            if elapsed >= INTERNET_HOLD_TIMEOUT_SEC:
                self.internet_hold_total_duration += elapsed
                self.hold_active = False
                if state:
                    state.internet_hold_active = False
                    state.internet_drop_active = False
                    shared_alarm_stop(state)
                clear_progress_line()
                log("NETWORK_GUARD: 30-minute Internet Hold timed out without restoration. Finalizing recording.", level="ERROR")
                return False

            rem = max(0.0, INTERNET_HOLD_TIMEOUT_SEC - elapsed)

            alarm_hint = " (Alarm silenced)" if (alarm_silenced or getattr(state, "internet_drop_post_ack_silent", False)) else (" (Press 'A' to silence alarm)" if should_alarm else "")
            hold_text = f"[INTERNET_HOLD] Waiting for internet restoration... Timeout remaining: {fmt_hms(rem)}{alarm_hint}"
            pad = max(0, last_wait_len - len(hold_text))
            render_progress_line(hold_text, pad=pad)
            last_wait_len = len(hold_text)

            if NetworkRouteResolver.is_internet_online(timeout=1.5):
                clear_progress_line()

                # If stream requires a VPN, settle routing table / allow VPN client up to 15s to establish tunnel
                if self.is_armed:
                    vpn_settle_start = time.monotonic()
                    last_settle_len = 0
                    vpn_settled = False
                    internet_re_dropped = False
                    log("NETWORK_GUARD: Outbound internet connectivity restored! Waiting up to 15s for VPN tunnel to settle...", level="INFO")
                    while True:
                        now_s = time.monotonic()
                        if state and state.stop_flag:
                            self.internet_hold_total_duration += (now - hold_start_mono)
                            self.hold_active = False
                            return False

                        # If internet cuts out again while waiting for VPN, drop back to internet hold loop
                        if not NetworkRouteResolver.is_internet_online(timeout=1.0):
                            clear_progress_line()
                            log("NETWORK_GUARD: Internet dropped again during VPN settling window! Resuming Internet Hold...", level="WARN")
                            internet_re_dropped = True
                            break

                        # Check if VPN tunnel is established and valid country
                        if self.vpn_mode == "WIREPROXY":
                            if self.wireproxy_manager and self.wireproxy_manager.is_running() and self.wireproxy_manager.check_port_responsive(timeout=0.2):
                                proxy_url = f"http://127.0.0.1:{self.wireproxy_manager.http_port}"
                                ip, loc = NetworkRouteResolver.get_public_ip_and_country(timeout=1.5, proxy_url=proxy_url)
                                if loc == self.required_country:
                                    self.launch_ip = ip
                                    self.launch_country = loc
                                    self.last_route_is_vpn = True
                                    vpn_settled = True
                                    break
                        else:
                            cur_iface, cur_is_vpn = NetworkRouteResolver.get_default_route_info()
                            if cur_is_vpn:
                                ip, loc = NetworkRouteResolver.get_public_ip_and_country(timeout=1.5)
                                if loc == self.required_country:
                                    self.launch_ip = ip
                                    self.launch_country = loc
                                    self.launch_interface = cur_iface
                                    self.launch_is_vpn = cur_is_vpn
                                    self.last_route_interface = cur_iface
                                    self.last_route_is_vpn = cur_is_vpn
                                    provider = NetworkRouteResolver.get_friendly_vpn_name(cur_iface)
                                    self.active_system_vpn = provider
                                    vpn_settled = True
                                    break

                        s_elapsed = now_s - vpn_settle_start
                        if s_elapsed >= 15.0:
                            break

                        s_rem = max(0.0, 15.0 - s_elapsed)
                        settle_text = f"[NETWORK_GUARD] Internet online. Waiting for VPN ({self.required_country})... Window remaining: {int(s_rem)}s"
                        pad_s = max(0, last_settle_len - len(settle_text))
                        render_progress_line(settle_text, pad=pad_s)
                        last_settle_len = len(settle_text)
                        time.sleep(0.5)

                    clear_progress_line()
                    if internet_re_dropped:
                        continue

                    if not vpn_settled:
                        # VPN failed to reconnect within 15 seconds. Transition smoothly to VPN Hold!
                        self.internet_hold_total_duration += (time.monotonic() - hold_start_mono)
                        self.hold_active = False
                        if state:
                            state.internet_hold_active = False
                            state.internet_drop_active = False
                            shared_alarm_stop(state)
                        log("NETWORK_GUARD: Internet restored but VPN tunnel did not establish within 15s. Diverting to VPN Hold...", level="WARN")
                        return self.handle_vpn_hold_state(state, is_startup=is_startup)

                # Both Internet (and VPN if armed) are verified healthy!
                self.internet_hold_total_duration += (now - hold_start_mono)
                self.hold_active = False
                if state:
                    state.internet_hold_active = False
                    state.internet_drop_active = False
                    state.internet_drop_post_ack_silent = False
                    state.manifest_fail_count = 0
                    state.no_file_appear_count = 0
                    state.nm3u8dl_crash_count = 0
                    state.live_stall_run_count = 0
                    shared_alarm_stop(state)
                    log_good_beep("recovery_healthy")
                    beep_good(state)
                if self.is_armed:
                    if self.vpn_mode == "WIREPROXY":
                        server_name = self.wireproxy_manager.active_server if self.wireproxy_manager else "wireproxy"
                        vpn_desc = f"[WIREPROXY] (Country: {self.launch_country} | Server: {server_name} | IP: {self.launch_ip})"
                    else:
                        vpn_desc = f"[SYSTEM_VPN: {self.active_system_vpn or 'VPN'}] (Country: {self.launch_country} | Adapter: {self.launch_interface or 'default'} | IP: {self.launch_ip})"
                    log(f"NETWORK_GUARD: Outbound internet and network route verified healthy {vpn_desc}! Guardrail cleared.", level="INFO")
                else:
                    log("NETWORK_GUARD: Outbound internet verified online! Guardrail cleared.", level="INFO")
                if not is_startup and state:
                    return self._verify_and_resume_stream(state)
                return True

            time.sleep(1.0)

    def format_info_lines(self) -> List[str]:
        lines = []
        if self.is_armed:
            lines.append(f"Guardrail status   : ARMED (Required: {self.required_country})")
            if self.active_system_vpn and self.launch_interface and self.launch_interface != self.active_system_vpn:
                egress_desc = f"{self.launch_ip} via {self.active_system_vpn} ({self.launch_interface})"
            elif self.launch_interface:
                egress_desc = f"{self.launch_ip} via {self.launch_interface}"
            else:
                egress_desc = f"{self.launch_ip}"
            lines.append(f"Launch egress      : {egress_desc}")
            cur_iface, cur_vpn = NetworkRouteResolver.get_default_route_info()
            lines.append(f"Current route      : {cur_iface} ({'VPN' if cur_vpn else 'PHYSICAL / UNPROTECTED'})")
            lines.append(f"VPN drop events    : {self.vpn_drop_count}")
            lines.append(f"Total VPN hold     : {fmt_hms(self.vpn_hold_total_duration)}")
            lines.append(f"HTTP 429 incidents : {self.rate_limit_429_count}")
            lines.append(f"Token expiry holds : {self.token_expiry_count}")
        else:
            lines.append("Guardrail status   : UNRESTRICTED")
            if self.internet_drop_count > 0:
                lines.append(f"Internet drop events: {self.internet_drop_count}")
                lines.append(f"Total Internet hold : {fmt_hms(self.internet_hold_total_duration)}")
            if self.token_expiry_count > 0:
                lines.append(f"Token expiry holds  : {self.token_expiry_count}")
        return lines

    def format_summary_lines(self) -> List[str]:
        lines = [
            "Network & VPN Guardrail",
            "-----------------------",
        ]
        if self.is_armed:
            lines.append(f"Target Country          : {self.required_country} ({self.detection_source})")
            if self.vpn_mode == "WIREPROXY":
                active_srv = getattr(self.wireproxy_manager, "active_server", None) or "wireproxy"
                via_desc = f"Wireproxy ({active_srv})"
            elif self.active_system_vpn:
                if self.launch_interface and self.launch_interface != self.active_system_vpn:
                    via_desc = f"System VPN: {self.active_system_vpn} ({self.launch_interface})"
                else:
                    via_desc = f"System VPN: {self.active_system_vpn}"
            elif self.launch_interface:
                via_desc = self.launch_interface
            else:
                via_desc = "direct"
            lines.append(f"Launch Network          : {self.launch_ip} ({self.launch_country}) via {via_desc}")
            lines.append(f"VPN Disconnect Events   : {self.vpn_drop_count}")
            lines.append(f"Total VPN Hold Duration : {fmt_hms(self.vpn_hold_total_duration)}")
            lines.append(f"Token Expiry Holds      : {self.token_expiry_count}")
            lines.append(f"HTTP 429 Incidents      : {self.rate_limit_429_count}")
            lines.append("Guardrail Violations    : 0 (0 packets leaked outside VPN)")
        else:
            lines.append("Guardrail Status        : UNRESTRICTED (Open / Generic Stream)")
            if self.token_expiry_count > 0:
                lines.append(f"Token Expiry Holds      : {self.token_expiry_count}")
            if self.internet_drop_count > 0:
                lines.append(f"Internet Drop Events    : {self.internet_drop_count}")
                lines.append(f"Total Internet Hold     : {fmt_hms(self.internet_hold_total_duration)}")
            else:
                lines.append("Internet Outages        : 0")
        return lines



def start_ffmpeg_to_chunk(state: RecorderState, notify=None, deadline_ts: Optional[float] = None) -> EngineResult:
    """
    Start one FFmpeg run that writes directly to the 'next' chunk_NNN.ts.
    Does NOT increment state.chunk_index yet. Returns EngineResult with run facts only.
    """
    chunk_path, chunk_name = next_chunk_names(state)
    
    # record when this recording attempt started
    state.stats["last_run_start"] = time.time()

    proxy_url = f"http://127.0.0.1:{WIREPROXY_HTTP_PORT}" if is_wireproxy_active(state) else None

    # Two FFmpeg source styles are supported:
    #   1) simple mode   -> existing LIVE_URL command remains unchanged
    #   2) advanced mode -> user supplies variable FFmpeg command material in FFMPEG_PART_A;
    #                       recorder appends progress monitoring + dynamic chunk path
    if FFMPEG_PART_A.strip():
        # Advanced HTTP(S) sources get standard transient-network reconnect defaults.
        # These are input options, so they must be inserted before the first -i.
        advanced_cmd_text = FFMPEG_PART_A.strip()

        if proxy_url and not re.search(r'(?<!\S)-http_proxy(?=\s|$)', advanced_cmd_text):
            input_match = re.search(r'(?<!\S)-i(?=\s)', advanced_cmd_text)
            proxy_opt = f"-http_proxy {proxy_url} "
            if input_match:
                advanced_cmd_text = (
                    advanced_cmd_text[:input_match.start()]
                    + proxy_opt
                    + advanced_cmd_text[input_match.start():]
                )
            else:
                advanced_cmd_text = proxy_opt + advanced_cmd_text

        if re.search(r'https?://', advanced_cmd_text, re.IGNORECASE):
            input_match = re.search(r'(?<!\S)-i(?=\s)', advanced_cmd_text)

            if input_match:
                reconnect_defaults = (
                    ("-rw_timeout", "15000000"),
                    ("-reconnect", "1"),
                    ("-reconnect_streamed", "1"),
                    ("-reconnect_on_network_error", "1"),
                    ("-reconnect_delay_max", "10"),
                    ("-reconnect_delay_total_max", "30"),
                )

                missing_parts = []

                for option, value in reconnect_defaults:
                    if not re.search(rf'(?<!\S){re.escape(option)}(?=\s|$)', advanced_cmd_text):
                        missing_parts.extend((option, value))

                if missing_parts:
                    reconnect_text = " ".join(missing_parts) + " "
                    advanced_cmd_text = (
                        advanced_cmd_text[:input_match.start()]
                        + reconnect_text
                        + advanced_cmd_text[input_match.start():]
                    )

        ffmpeg_cmd_text = (
            f'{advanced_cmd_text} '
            f'-y -nostats -progress pipe:2 -stats_period {FFMPEG_PROGRESS_PERIOD} '
            f'"{chunk_path}"'
        )
        cmd = ffmpeg_cmd_text if os.name == "nt" else shlex.split(ffmpeg_cmd_text)
        ff_expected_kbps = None
        ff_expected_source = None
        log("FFmpeg source mode: advanced command configuration")
        log(f"Command: {ffmpeg_cmd_text}")
    else:
        proxy_opts = ["-http_proxy", proxy_url] if proxy_url else []
        cmd = [
            "ffmpeg",
            "-y",
            "-loglevel", "info",
            "-hide_banner",
            *proxy_opts,
            
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
        if state.ffmpeg_selected_bitrate_kbps_max > 0:
            ff_expected_kbps = float(state.ffmpeg_selected_bitrate_kbps_max)
            ff_expected_source = "previous_max"
            log(f"FFmpeg: Selected bitrate detected {int(ff_expected_kbps)} Kbps (source=previous_max)")
        else:
            ff_expected_kbps = None
            ff_expected_source = None

    log(f"Starting FFmpeg → writing to {chunk_name} until stream dies...")
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
    ff_had_stall_incident = live_stall_incident_active(state)

    growth_tracker = start_live_file_growth_tracker(chunk_path)
    ff_last_health_log_t = time.monotonic()

    # FFmpeg's live progress row and its recurring Good-speed health line are
    # one terminal activity. They can interrupt other work together without
    # adding separators between each other.
    ffmpeg_progress_activity_id = new_terminal_activity("ffmpeg_progress")

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

    integrity_monitor = StreamIntegrityMonitor(state)
    prune_stream_corrupt_window(state)

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
                if not s:
                    continue

                # Network Guard: Inspect line for HTTP 403 / 429
                if state is not None and getattr(state, "network_guard", None):
                    if state.network_guard.inspect_stdout_line(s):
                        state.rate_limit_429_detected = True

                # Shared live-engine noise filter.
                if should_suppress_live_engine_output_line(s):
                    continue

                # Pass to stream integrity monitor (detects P1/P3 and rate-limits repetitive errors)
                suppress_from_terminal = integrity_monitor.process_stderr_line(s)
                
                # HOT-AUDIO: if audio appears mid-run, restart FFmpeg so NEXT chunk includes audio
                if "new audio stream" in s.lower():
                    if not hot_audio_restart.is_set():
                        flush_progress_line()
                        hot_audio_restart.set()
                        log(f"FFmpeg: {s}")
                        log("FFmpeg: HOT-AUDIO detected → restarting FFmpeg so next chunk includes audio.")
                    continue

                m = re.match(r'^([A-Za-z0-9_]+)=(.*)$', s)
                if m:
                    k, v = m.group(1), m.group(2)
                    kv[k] = v
                    if k == "progress":
                        integrity_monitor.process_progress(kv)
                        integrity_monitor.evaluate_health(flush_cb=flush_progress_line)
                        burst_summary = integrity_monitor.check_flush_timing_summary()
                        if burst_summary:
                            flush_progress_line()
                            log(burst_summary)

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

                integrity_monitor.evaluate_health(flush_cb=flush_progress_line)
                burst_summary = integrity_monitor.check_flush_timing_summary()
                if burst_summary:
                    flush_progress_line()
                    log(burst_summary)

                # Non-progress line: end and preserve the current progress line,
                # then log the FFmpeg message (if not rate-limited anti-spam).
                if suppress_from_terminal:
                    # Item 3B: Retain 100% on-disk fidelity in terminal capture
                    ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    termcap_write(f"{ts_str} [INFO] FFmpeg: {s}")
                else:
                    flush_progress_line()
                    if s:
                        log(f"FFmpeg: {s}")
        except Exception as e:
            log(f"FFmpeg stderr monitor error: {e}")
        finally:
            try:
                burst_summary = integrity_monitor.check_flush_timing_summary(force=True)
                if burst_summary:
                    flush_progress_line()
                    log(burst_summary)
            except Exception:
                pass

    t = threading.Thread(target=monitor_ffmpeg, args=(proc,), daemon=True)
    t.start()

    while True:
        ret = proc.poll()
        if ret is not None:
            # FFmpeg finished naturally
            break

        # Rolling 30-minute corruption window sweeper (re-arms alarm when it empties).
        prune_stream_corrupt_window(state)
        integrity_monitor.evaluate_health(flush_cb=flush_progress_line)
        if integrity_monitor.recycle_requested:
            reason = "stream corruption grace countdown expired"
            status = "stream_corrupt_recycle"
            flush_progress_line()
            log(
                f"FFmpeg: [STREAM RECYCLE] {integrity_monitor.recycle_reason} → "
                f"terminating FFmpeg to seal chunk and re-anchor keyframe...",
                level="WARN",
            )
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                log("FFmpeg did not terminate gracefully, killing.", level="WARN")
                proc.kill()
                proc.wait()
            # Chunk is sealed. Count the fatal cut, escalate to the siren on the
            # 2nd cut inside the rolling window, otherwise give a short Bad Beep.
            if register_stream_corrupt_recycle(state):
                play_stream_corrupt_bad_beep_async(state)
            break

        if hot_audio_restart.is_set():
            reason = "hot audio restart"
            log("FFmpeg: HOT-AUDIO restart requested → terminating FFmpeg now.")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                log("FFmpeg did not terminate gracefully, killing.")
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
                log("FFmpeg did not terminate gracefully, killing...")
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
                log("FFmpeg did not terminate gracefully, killing...")
                proc.kill()
                proc.wait()
            break

        if not ran_30s and (time.time() - start_run) >= 30:
            ran_30s = True

        # FFmpeg still running: shared file-growth sampling/hard-stall classification.
        growth_check = poll_live_file_growth(chunk_path, growth_tracker)

        if growth_check is not None:
            live_hard_required = LIVE_HARD_STALL_REQUIRED
            live_soft_required = LIVE_SOFT_STALL_REQUIRED
            is_double_vpn_capped = False
            guard = getattr(state, "network_guard", None)
            if guard and hasattr(guard, "is_wireproxy_nested_in_vpn"):
                is_nested, _ = guard.is_wireproxy_nested_in_vpn()
                if is_nested and (live_hard_required > 4 or live_soft_required > 4):
                    is_double_vpn_capped = True
                    live_hard_required = min(4, live_hard_required)
                    live_soft_required = min(4, live_soft_required)

            growth_bytes = growth_check["growth_bytes"]
            growth_kbps = growth_check["growth_kbps"]
            ff_has_file = growth_check["has_file"]
            soft_trigger = False

            if growth_check["is_hard"]:
                flush_progress_line()
                log(
                    f"HARD_STALL_CHECK "
                    f"{min(growth_check['hard_count'], live_hard_required)}/{live_hard_required} "
                    f"growth_bytes={growth_bytes}"
                )
                if not is_alarm_active(state):
                    beep_bad(state)
            else:
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

                soft_check = classify_live_soft_stall(
                    growth_tracker,
                    growth_kbps,
                    ff_expected_kbps,
                    live_soft_required,
                )
                soft_trigger = soft_check["soft_trigger"]

                if soft_check["is_soft"]:
                    threshold_kbps = soft_check["threshold_kbps"]
                    flush_progress_line()
                    log(
                        f"SOFT_STALL_CHECK "
                        f"{min(soft_check['soft_count'], live_soft_required)}/{live_soft_required} "
                        f"growth_kbps={int(growth_kbps)} threshold_kbps={int(threshold_kbps)} "
                        f"expected_kbps={int(ff_expected_kbps)}"
                    )
                    if not is_alarm_active(state):
                        beep_bad(state)
                else:
                    threshold_kbps = soft_check["threshold_kbps"]
                    if notify:
                        notify("live_stall_recovery")
                    chunk_defective = integrity_monitor.is_corrupt_or_defective
                    alarm_active = getattr(state, "ffmpeg_stream_corrupt_alarm_active", False)
                    if not chunk_defective and not alarm_active:
                        maybe_trigger_good_beep(
                            state,
                            "ffmpeg",
                            ff_has_file,
                            ff_had_stall_incident,
                            notify,
                        )
                    if threshold_kbps is not None:
                        now_health_log_t = time.monotonic()
                        if (now_health_log_t - ff_last_health_log_t) >= LIVE_HEALTH_LOG_INTERVAL:
                            flush_progress_line()
                            if chunk_defective:
                                log(
                                    f"FFmpeg: [CORRUPTION ACTIVE] Traffic speed {int(growth_kbps)} Kbps (stream unplayable)",
                                    activity_id=ffmpeg_progress_activity_id,
                                )
                            elif alarm_active:
                                log(
                                    f"FFmpeg: [ALARM ACTIVE] Good speed {int(growth_kbps)} Kbps "
                                    f"(threshold {int(threshold_kbps)}) - press 'A' to acknowledge",
                                    activity_id=ffmpeg_progress_activity_id,
                                )
                            else:
                                log(
                                    f"FFmpeg: Good speed {int(growth_kbps)} Kbps "
                                    f"(threshold {int(threshold_kbps)})",
                                    activity_id=ffmpeg_progress_activity_id,
                                )
                            ff_last_health_log_t = now_health_log_t

            # Network Guard: Zero-traffic kernel route monitoring (every 1s)
            if getattr(state, "network_guard", None) and state.network_guard.is_armed and not state.stop_flag:
                if not state.network_guard.check_runtime_route():
                    if state.network_guard.attempt_wireguard_auto_recovery():
                        continue
                    if state.network_guard.is_lenient_drop_allowed():
                        if not getattr(state, "lenient_alarm_active", False):
                            state.lenient_alarm_active = True
                            if not getattr(state, "vpn_drop_post_ack_silent", False):
                                log("NETWORK_GUARD: [STAGE 2 - LENIENT] VPN dropped! Sounding alarm. Downloader allowed to continue retrying...", level="WARN")
                                shared_alarm_start(state, alarm_type="vpn_drop")
                        if not NetworkRouteResolver.is_internet_online_debounced(
                            required_consecutive_failures=2,
                            retry_delay=1.5,
                            timeout_per_probe=1.5,
                        ):
                            log("NETWORK_GUARD: [LENIENT] Outbound internet severed while in VPN drop! Aborting retry to seal chunk...", level="WARN")
                            proc.terminate()
                            try:
                                proc.wait(timeout=3)
                            except subprocess.TimeoutExpired:
                                proc.kill()
                                proc.wait()
                            status = "internet_drop"
                            reason = "internet connection lost during lenient drop"
                            break
                    else:
                        state.network_guard_triggered = True
                        status = "vpn_drop"
                        reason = "vpn route dropped"
                        flush_progress_line()
                        log("NETWORK_GUARD: [STAGE 2 - STRICT] Default route severed from VPN interface! Terminating FFmpeg to seal chunk...", level="WARN")
                        proc.terminate()
                        try:
                            proc.wait(timeout=3)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait()
                        break
            elif getattr(state, "network_guard", None) and not state.network_guard.is_armed and not state.stop_flag:
                if not state.network_guard.check_runtime_internet():
                    state.internet_drop_active = True
                    status = "internet_drop"
                    reason = "outbound internet dropped"
                    flush_progress_line()
                    if not getattr(state, "internet_drop_post_ack_silent", False):
                        log("NETWORK_GUARD: [STRICT] Outbound internet dropped! Terminating FFmpeg to seal chunk...", level="WARN")
                        shared_alarm_start(state, alarm_type="internet_drop")
                    proc.terminate()
                    try:
                        proc.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
                    break

            hard_trigger = growth_check["hard_count"] >= live_hard_required

            if hard_trigger or soft_trigger:
                stall_type = "hard" if hard_trigger else "soft"
                flush_progress_line()
                log(f"CLASSIFY STALL_{stall_type.upper()}", level="WARN")
                reset_good_beep_state(state, "ffmpeg")

                # Network Guard: In-flight attribution for nested double-VPN stall
                guard = getattr(state, "network_guard", None)
                if guard and hasattr(guard, "is_wireproxy_nested_in_vpn"):
                    is_nested, vpn_name = guard.is_wireproxy_nested_in_vpn()
                    if is_nested:
                        if is_double_vpn_capped:
                            effective_trigger = live_soft_required if soft_trigger else live_hard_required
                            normal_trigger = LIVE_SOFT_STALL_REQUIRED if soft_trigger else LIVE_HARD_STALL_REQUIRED
                            log(
                                f"NETWORK_GUARD: Active double-VPN ({vpn_name}) detected during stall! "
                                f"Smart stall cap enforced: ending chunk early at check {effective_trigger} (normal {normal_trigger}) "
                                f"to seal video bytes safely and hand over routing for next chunk.",
                                level="WARN",
                            )
                        else:
                            log(
                                f"NETWORK_GUARD: Stream stalled while Wireproxy was nested inside {vpn_name}! "
                                f"Attributing stall to nested tunnel packet loss / MTU conflict.",
                                level="WARN",
                            )

                status = "stalled"
                reason = f"STALL_{stall_type.upper()}"
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    log("FFmpeg did not terminate gracefully, killing.", level="WARN")
                    proc.kill()
                    proc.wait()
                break

        time.sleep(0.5)

    try:
        t.join(timeout=3)
    except threading.ThreadError:
        pass
    proc.wait()
    raw_external_end(raw_invocation, returncode=proc.returncode)
        
    # Finish and preserve the final on-screen progress line.
    flush_progress_line()

    unexpected_process_exit = (
        proc.returncode not in (0, None)
        and reason == "ffmpeg exited"
        and not state.stop_flag
    )

    if unexpected_process_exit:
        log(f"FFmpeg exited unexpectedly with code {proc.returncode}", level="WARN")

    log("FFmpeg stopped for this run")
    return EngineResult(
        status=status if status in ("duration_reached", "stalled", "vpn_drop", "internet_drop", "stream_corrupt_recycle") else "ended",
        reason=reason if status != "ended" or proc.returncode != 0 else f"exit code {proc.returncode}",
        chunk_path=chunk_path,
        chunk_name=chunk_name,
        metrics={
            "ran_30s": ran_30s,
            "stall_type": stall_type,
            "unexpected_process_exit": unexpected_process_exit,
            "internet_drop": getattr(state, "internet_drop_active", False),
        },
    )

def run_ffmpeg_cycle(state: RecorderState, backoff_sec, notify=None, deadline_ts: Optional[float] = None):
    """One FFmpeg worker cycle: start/monitor/stop and return run facts only."""

    log_run_start_banner(state, "FFmpeg")
    return start_ffmpeg_to_chunk(state, notify=notify, deadline_ts=deadline_ts)
    
# ==============================================================================
# Engine: N_m3u8DL-RE
# ==============================================================================

def _nm3u8dl_stream_url_from_part_a() -> str:
    match = re.search(r'"(https?://[^"]+)"', NM3U8DL_PART_A)
    return match.group(1) if match else ""

def get_nm3u8dl_exclude_substrings() -> List[str]:
    stream_url = _nm3u8dl_stream_url_from_part_a()
    excludes = list(NM3U8DL_STDOUT_EXCLUDE_DEFAULT)
    for key, extra in NM3U8DL_STDOUT_EXCLUDE_BY_URL.items():
        if key in stream_url:
            excludes.extend(extra)
    return excludes

def should_exclude_nm3u8dl_line(stripped_line: str, exclude_substrings: List[str]) -> bool:
    if stripped_line in ("ERROR", "ERROR:"):
        return True
    return any(ex in stripped_line for ex in exclude_substrings)

def is_nm3u8dl_fatal_producer_error(line: str) -> bool:
    """
    Detect fatal N_m3u8DL-RE internal live playlist producer crash/timeout.
    When N_m3u8DL's DASH/HLS RefreshPlayListAsync times out or cancels (e.g. during a network drop),
    its producer task terminates permanently while consumer workers sit in 'Waiting 100%' indefinitely.
    Fast-path terminating the deadlocked process avoids wasting 10 minutes in stall cycles.
    """
    raw = line.lower()
    if (
        ("refreshplaylist" in raw or "playlistproduce" in raw)
        and (
            "taskcanceledexception" in raw
            or "timeoutexception" in raw
            or "socketexception" in raw
            or "error" in raw
        )
    ):
        return True
    if "taskcanceledexception" in raw and "httpclient.timeout" in raw:
        return True
    return False
    
def extract_nm3u8dl_startup_info(proc, exclude_substrings=None, raw_invocation=None, state=None):
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
    has_403 = False
    has_429 = False

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
        nonlocal bitrate, in_selected_section, has_403, has_429

        raw_external_write(raw_invocation, record)
        stripped_line = record.strip()

        if not stripped_line:
            return

        # Check for HTTP 403 / 429 during startup
        raw_lower = stripped_line.lower()
        if "403" in raw_lower and ("forbidden" in raw_lower or "status code does not indicate success: 403" in raw_lower or "http error 403" in raw_lower):
            has_403 = True
        if "429" in raw_lower and ("too many requests" in raw_lower or "status code does not indicate success: 429" in raw_lower or "http error 429" in raw_lower):
            has_429 = True

        # Remove inner N_m3u8DL timestamp.
        stripped_line = re.sub(
            r'^\d{2}:\d{2}:\d{2}\.\d{3}\s+',
            '',
            stripped_line
        )

        if not stripped_line:
            return

        if state is not None and getattr(state, "network_guard", None):
            if state.network_guard.inspect_stdout_line(stripped_line):
                state.rate_limit_429_detected = True
                has_429 = True

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

        if is_nm3u8dl_fatal_producer_error(stripped_line):
            if state is not None and not getattr(state, "nm3u8dl_stall_flag", False):
                state.nm3u8dl_stall_flag = True
                state.nm3u8dl_stall_reason = "playlist_producer_timeout"
                log(
                    "N_m3u8DL: Fatal playlist producer timeout/cancellation detected (TaskCanceledException)! "
                    "Fast-path terminating stalled process to seal chunk...",
                    level="WARN",
                )

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
                        'refresh_interval': refresh,
                        'has_403': has_403,
                        'has_429': has_429,
                    }

                except Exception:
                    pass

            # Current N_m3u8DL can begin another timestamped record without
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
        'refresh_interval': refresh,
        'has_403': has_403,
        'has_429': has_429,
    }
    
def monitor_nm3u8dl_stdout(
    proc,
    refresh_interval=60,
    exclude_substrings=None,
    live_end_event=None,
    raw_invocation=None,
    progress_activity_id: Optional[str] = None,
    state: Optional[RecorderState] = None,
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
        #   00m33s/00m33s11/11  ->  00m33s/00m33s 11/11
        stripped = re.sub(
            r'((?:\d{2}h)?\d{2}m\d{2}s/(?:\d{2}h)?\d{2}m\d{2}s)(\d+/\d+)',
            r'\1 \2',
            stripped
        )

        if not stripped.strip():
            return

        # Fatal N_m3u8DL playlist producer crash/timeout intercept:
        # When N_m3u8DL's DASH/HLS RefreshPlayListAsync times out or cancels (e.g. during a network drop),
        # its producer thread dies permanently while consumer workers sit in 'Waiting 100%' indefinitely.
        # Fast-path terminate to seal chunk and allow instant orchestrator recovery without wasting 10 minutes in stalls.
        if is_nm3u8dl_fatal_producer_error(stripped):
            if state is not None and not getattr(state, "nm3u8dl_stall_flag", False):
                state.nm3u8dl_stall_flag = True
                state.nm3u8dl_stall_reason = "playlist_producer_timeout"
                log(
                    "N_m3u8DL: Fatal playlist producer timeout/cancellation detected (TaskCanceledException)! "
                    "Fast-path terminating stalled process to seal chunk...",
                    level="WARN",
                )

        # Check HTTP 429 rate-limiting
        if state is not None and getattr(state, "network_guard", None):
            if state.network_guard.inspect_stdout_line(stripped):
                state.rate_limit_429_detected = True

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
    normal_hard_stall = hard_stall_required
    is_double_vpn_capped = False

    # In-flight double-VPN smart stall cap: if host VPN is nested over Wireproxy, cap stall wait to max 4 checks
    guard = getattr(state, "network_guard", None)
    if guard and hasattr(guard, "is_wireproxy_nested_in_vpn"):
        is_nested, _ = guard.is_wireproxy_nested_in_vpn()
        if is_nested and (hard_stall_required > 4 or soft_stall_required > 4):
            is_double_vpn_capped = True
            hard_stall_required = min(4, hard_stall_required)
            soft_stall_required = min(4, soft_stall_required)
    # ACK belongs to the stall incident, not to one particular N process.
    if getattr(state, "nm3u8dl_ack_requested", False):
        acknowledge_nm3u8dl_stall_alarm(state)

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
                if getattr(state, "network_guard", None):
                    if (time.monotonic() - getattr(state.network_guard, "last_429_incident_mono", 0.0)) > RATE_LIMIT_WINDOW_SEC:
                        state.network_guard.rate_limit_strike = 0

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
            state.nm3u8dl_hard_count >= hard_stall_required
        )
        soft_trigger = (
            state.nm3u8dl_soft_count >= soft_stall_required
        )

        if not (hard_trigger or soft_trigger):
            continue

        stall_type = "hard" if hard_trigger else "soft"
        log(f"CLASSIFY STALL_{stall_type.upper()}", level="WARN")
        reset_good_beep_state(state, "nm3u8dl")

        # Network Guard: In-flight attribution for nested double-VPN stall
        guard = getattr(state, "network_guard", None)
        if guard and hasattr(guard, "is_wireproxy_nested_in_vpn"):
            is_nested, vpn_name = guard.is_wireproxy_nested_in_vpn()
            if is_nested:
                if is_double_vpn_capped:
                    log(
                        f"NETWORK_GUARD: Active double-VPN ({vpn_name}) detected during stall! "
                        f"Smart stall cap enforced: ending chunk early at check {hard_stall_required} (normal {normal_hard_stall}) "
                        f"to seal video bytes safely and hand over routing for next chunk.",
                        level="WARN",
                    )
                else:
                    log(
                        f"NETWORK_GUARD: Stream stalled while Wireproxy was nested inside {vpn_name}! "
                        f"Attributing stall to nested tunnel packet loss / MTU conflict.",
                        level="WARN",
                    )

        state.nm3u8dl_stall_event_count += 1
        event_count = state.nm3u8dl_stall_event_count
        state.nm3u8dl_had_alarm_incident = True

        log(
            f"STALL_EVENT type={stall_type} "
            f"event_count {event_count}/{NM3U8DL_STALL_MAX_EVENTS}",
            level="WARN",
        )

        if (
            event_count == 2
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


_NM3U8DL_DEDUP_FLAGS = (
    "--thread-count",
    "--live-take-count",
    "--live-wait-time",
    "--download-retry-count",
    "--http-request-timeout",
)


def _deduplicate_nm3u8dl_part_b(part_a: str, part_b: str) -> str:
    """Strip default pacing/timeout flags from PART_B when explicitly overridden in PART_A."""
    if not part_a or not part_b:
        return part_b
    deduped = part_b
    for flag in _NM3U8DL_DEDUP_FLAGS:
        if re.search(rf"(?<!\S){re.escape(flag)}(?=\s|=|$)", part_a):
            deduped = re.sub(rf"\s*{re.escape(flag)}(?:\s+\S+|=[^\s]+)", "", deduped)
    return re.sub(r"\s{2,}", " ", deduped).strip()


def start_nm3u8dl_to_chunk(state: RecorderState, notify=None, deadline_ts: Optional[float] = None) -> EngineResult:
    """
    Start N_m3u8DL-RE process with monitoring threads.
    Detects crashes via file growth stalls and returns EngineResult facts only.
    """
    state.nm3u8dl_run_active = True
    try:
        if not (NM3U8DL_PART_A or "").strip():
            log("ERROR: NM3U8DL_PART_A is empty in user config! Cannot launch N_m3u8DL-RE.", level="ERROR")
            log("Please set NM3U8DL_PART_A = 'N_m3u8DL-RE \"<STREAM_URL>\"' in recorder_static_user_config.py", level="ERROR")
            return EngineResult(
                status="fatal_misconfig",
                reason="NM3U8DL_PART_A is empty in user config",
                wall_duration=0.0,
                media_duration=0.0,
                bytes_recorded=0,
            )

        chunk_path, chunk_name = next_chunk_names(state)
        state.nm3u8dl_stall_flag = False

        # Build full command: Part_A + save-name + deduplicated Part_B
        chunk_name_without_ext = chunk_name.replace('.ts', '')
        nm3u8dl_part_b = _deduplicate_nm3u8dl_part_b(NM3U8DL_PART_A, NM3U8DL_PART_B)
        cmd = f'{NM3U8DL_PART_A} --save-name "{chunk_name_without_ext}" {nm3u8dl_part_b}'

        cmd = cmd.replace(
            "--use-shaka-packager",
            "--decryption-engine SHAKA_PACKAGER"
        )

        if is_wireproxy_active(state) and "--custom-proxy" not in cmd:
            cmd = f"{cmd} --custom-proxy http://127.0.0.1:{WIREPROXY_HTTP_PORT}"

        # Per-process state only. Stall/crash counters and stall-alarm state are
        # incident-level state and survive process replacement/restart until one
        # confirmed healthy growth check proves recovery.
        ensure_nm3u8dl_key_listener(state)
        state.nm3u8dl_stall_flag = False
        state.nm3u8dl_stall_reason = None
        state.nm3u8dl_ack_requested = False
        state.nm3u8dl_manual_restart_requested = False
        state.nm3u8dl_hard_count = 0
        state.nm3u8dl_soft_count = 0
        state.nm3u8dl_run_had_healthy_check = False
        state.nm3u8dl_healthy_consec_count = 0
        state.nm3u8dl_beeped_this_healthy_streak = False
        state.nm3u8dl_refresh_interval_s = 0.0
        state.rate_limit_429_detected = False

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
            exclude_substrings=get_nm3u8dl_exclude_substrings(),
            raw_invocation=raw_invocation,
            state=state,
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

        if startup_data.get('has_403') or (getattr(state, "network_guard", None) and state.network_guard.has_403_error):
            log("N_m3u8DL: Startup failed due to HTTP 403 Forbidden (Geo-Block / CDN IP-Block)!", level="WARN")
            if getattr(state, "network_guard", None):
                state.network_guard.has_403_error = True
                if not state.network_guard.blocked_403_ip:
                    state.network_guard.blocked_403_ip = state.network_guard.launch_ip
            proc.terminate()
            raw_external_end(raw_invocation, returncode=proc.poll())
            return EngineResult(
                status="no_stream",
                reason="GEO_BLOCK_403",
                chunk_path=chunk_path,
                chunk_name=chunk_name,
                metrics={
                    "selected_bitrate_kbps": 0,
                    "refresh_interval_s": 0,
                    "geo_block_403": True,
                    "manifest_dead": False,
                },
            )

        startup_failed = state.nm3u8dl_refresh_interval_s == 0 or state.nm3u8dl_selected_bitrate_kbps == 0
        if getattr(state, "rate_limit_429_detected", False) or (startup_failed and startup_data.get('has_429')):
            log("N_m3u8DL: Startup failed due to HTTP 429 Too Many Requests (Rate-Limit)!", level="WARN")
            if getattr(state, "network_guard", None):
                state.rate_limit_429_detected = True
                if state.network_guard.rate_limit_429_count == 0 or (time.monotonic() - state.network_guard.last_429_incident_mono > 1.0):
                    state.network_guard.rate_limit_429_count += 1
                    state.network_guard.last_429_incident_mono = time.monotonic()
            proc.terminate()
            raw_external_end(raw_invocation, returncode=proc.poll())
            return EngineResult(
                status="no_stream",
                reason="RATE_LIMIT_429",
                chunk_path=chunk_path,
                chunk_name=chunk_name,
                metrics={
                    "selected_bitrate_kbps": 0,
                    "refresh_interval_s": 0,
                    "rate_limit_429": True,
                    "manifest_dead": False,
                },
            )

        if startup_failed:
            log("No stream/manifest → fail immediately")
            proc.terminate()
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
                get_nm3u8dl_exclude_substrings(),
                live_end_event,
                raw_invocation,
                nm3u8dl_progress_activity_id,
                state,
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

            # Process alarm silence request if operator pressed 'A'
            if getattr(state, "alarm_ack_requested", False):
                state.alarm_ack_requested = False
                shared_alarm_stop(state)

            # Network Guard: Zero-traffic kernel route monitoring (every 1s)
            if getattr(state, "network_guard", None) and state.network_guard.is_armed and not state.stop_flag:
                if not state.network_guard.check_runtime_route():
                    # STAGE 1: Attempt Silent WireGuard Auto-Recovery
                    if state.network_guard.attempt_wireguard_auto_recovery():
                        continue

                    # STAGE 2: Fallback Guardrail (STRICT vs LENIENT)
                    if state.network_guard.is_lenient_drop_allowed():
                        # Check physical internet FIRST before sounding any VPN drop alarm!
                        if not NetworkRouteResolver.is_internet_online_debounced(
                            required_consecutive_failures=2,
                            retry_delay=1.5,
                            timeout_per_probe=1.5,
                        ):
                            log("NETWORK_GUARD: [LENIENT] Outbound internet severed while in VPN drop! Aborting retry to seal chunk...", level="WARN")
                            stop_event.set()
                            proc.terminate()
                            try:
                                proc.wait(timeout=3)
                            except subprocess.TimeoutExpired:
                                proc.kill()
                                proc.wait()
                            return EngineResult(
                                status="internet_drop",
                                reason="internet connection lost during lenient drop",
                                chunk_path=chunk_path,
                                chunk_name=chunk_name,
                                metrics={
                                    "selected_bitrate_kbps": state.nm3u8dl_selected_bitrate_kbps,
                                    "refresh_interval_s": state.nm3u8dl_refresh_interval_s,
                                },
                            )

                        # Outbound internet is verified UP: this is a genuine VPN-only drop
                        if not getattr(state, "lenient_alarm_active", False):
                            state.lenient_alarm_active = True
                            if not getattr(state, "vpn_drop_post_ack_silent", False):
                                log("NETWORK_GUARD: [STAGE 2 - LENIENT] VPN dropped! Sounding alarm. Downloader allowed to continue retrying...", level="WARN")
                                shared_alarm_start(state, alarm_type="vpn_drop")
                    else:
                        # In STRICT mode, terminate child process immediately to prevent non-compliant egress
                        if stop_event:
                            stop_event.set()
                        if proc:
                            proc.terminate()
                            try:
                                proc.wait(timeout=3)
                            except subprocess.TimeoutExpired:
                                proc.kill()
                                proc.wait()

                        # Classify root cause post-termination: if physical internet died, divert to internet_drop
                        if not NetworkRouteResolver.is_internet_online_debounced(
                            required_consecutive_failures=2,
                            retry_delay=1.5,
                            timeout_per_probe=1.5,
                        ):
                            log("NETWORK_GUARD: [STRICT] Outbound internet severed! Sealed chunk to disk.", level="WARN")
                            return EngineResult(
                                status="internet_drop",
                                reason="internet connection lost during strict drop",
                                chunk_path=chunk_path,
                                chunk_name=chunk_name,
                                metrics={
                                    "selected_bitrate_kbps": state.nm3u8dl_selected_bitrate_kbps,
                                    "refresh_interval_s": state.nm3u8dl_refresh_interval_s,
                                },
                            )

                        state.network_guard_triggered = True
                        log("NETWORK_GUARD: [STAGE 2 - STRICT] Default route severed from VPN interface! Terminating child process to seal chunk...", level="WARN")
                        return EngineResult(
                            status="vpn_drop",
                            reason="vpn route dropped",
                            chunk_path=chunk_path,
                            chunk_name=chunk_name,
                            metrics={
                                "selected_bitrate_kbps": state.nm3u8dl_selected_bitrate_kbps,
                                "refresh_interval_s": state.nm3u8dl_refresh_interval_s,
                            },
                        )
                else:
                    # Route is verified healthy: clear lenient alarm if it was active
                    if getattr(state, "lenient_alarm_active", False) or getattr(state, "vpn_drop_post_ack_silent", False):
                        state.lenient_alarm_active = False
                        state.vpn_drop_post_ack_silent = False
                        shared_alarm_stop(state)
                        log_good_beep("vpn_restored")
                        beep_good(state)

            # Universal physical internet supervision across all stream modes
            if getattr(state, "network_guard", None) and not state.stop_flag:
                if not state.network_guard.check_runtime_internet():
                    if not getattr(state, "internet_drop_active", False):
                        state.internet_drop_active = True
                        if not getattr(state, "internet_drop_post_ack_silent", False):
                            log("NETWORK_GUARD: [LENIENT] Outbound internet dropped! Sounding alarm. Downloader allowed to continue retrying...", level="WARN")
                            shared_alarm_start(state, alarm_type="internet_drop")
                else:
                    if getattr(state, "internet_drop_active", False) or getattr(state, "internet_drop_post_ack_silent", False):
                        state.internet_drop_active = False
                        state.internet_drop_post_ack_silent = False
                        shared_alarm_stop(state)
                        log_good_beep("internet_restored")
                        beep_good(state)
                        log("NETWORK_GUARD: [LENIENT] Internet connection restored while downloader active! Resuming seamless recording.", level="INFO")

            # Rate-Limit Circuit Breaker: Sustained HTTP 429 detected while waiting for output file
            if getattr(state, "rate_limit_429_detected", False):
                log("RATE_LIMIT: Sustained HTTP 429 threshold crossed! Terminating child process to seal chunk...", level="WARN")
                stop_event.set()
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                raw_external_end(raw_invocation, returncode=proc.poll())
                return EngineResult(
                    status="rate_limit_429",
                    reason="http 429 sustained rate limit",
                    chunk_path=chunk_path,
                    chunk_name=chunk_name,
                    metrics={
                        "selected_bitrate_kbps": state.nm3u8dl_selected_bitrate_kbps,
                        "refresh_interval_s": state.nm3u8dl_refresh_interval_s,
                        "rate_limit_429": True,
                    },
                )

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

            log("")
            log(
                f"No output file appeared after {waited_s}s "
                f"(deadline {file_appear_deadline_s}s) "
                f"→ ending run (exit_code={exit_code})"
            )

            proc.terminate()

            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

            if getattr(state, "internet_drop_active", False):
                return EngineResult(
                    status="internet_drop",
                    reason="outbound internet dropped",
                    chunk_path=chunk_path,
                    chunk_name=chunk_name,
                    metrics={
                        "selected_bitrate_kbps":
                            state.nm3u8dl_selected_bitrate_kbps,
                        "refresh_interval_s":
                            state.nm3u8dl_refresh_interval_s,
                        "file_appear_deadline_s":
                            file_appear_deadline_s,
                        "internet_drop": True,
                    },
                )

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

        start_run = time.time()
        ran_30s = False
        status = "ended"
        reason = "nm3u8dl exited"

        while True:
            ret = proc.poll()
            
            nm3u8dl_handle_growth_events(state, stop_event, notify)

            # Process alarm silence request if operator pressed 'A'
            if getattr(state, "alarm_ack_requested", False):
                state.alarm_ack_requested = False
                shared_alarm_stop(state)

            # Network Guard: Zero-traffic kernel route monitoring (every 1s)
            if getattr(state, "network_guard", None) and state.network_guard.is_armed and not state.stop_flag:
                if not state.network_guard.check_runtime_route():
                    # STAGE 1: Attempt Silent WireGuard Auto-Recovery
                    if state.network_guard.attempt_wireguard_auto_recovery():
                        continue

                    # STAGE 2: Fallback Guardrail (STRICT vs LENIENT)
                    if state.network_guard.is_lenient_drop_allowed():
                        # Check physical internet FIRST before sounding any VPN drop alarm!
                        if not NetworkRouteResolver.is_internet_online_debounced(
                            required_consecutive_failures=2,
                            retry_delay=1.5,
                            timeout_per_probe=1.5,
                        ):
                            log("NETWORK_GUARD: [LENIENT] Outbound internet severed while in VPN drop! Aborting retry to seal chunk...", level="WARN")
                            reason = "internet connection lost during lenient drop"
                            status = "internet_drop"
                            stop_event.set()
                            proc.terminate()
                            try:
                                proc.wait(timeout=3)
                            except subprocess.TimeoutExpired:
                                proc.kill()
                                proc.wait()
                            break

                        # Outbound internet is verified UP: this is a genuine VPN-only drop
                        if not getattr(state, "lenient_alarm_active", False):
                            state.lenient_alarm_active = True
                            if not getattr(state, "vpn_drop_post_ack_silent", False):
                                log("NETWORK_GUARD: [STAGE 2 - LENIENT] VPN dropped! Sounding alarm. Downloader allowed to continue retrying...", level="WARN")
                                shared_alarm_start(state, alarm_type="vpn_drop")
                    else:
                        # In STRICT mode, terminate child process immediately to prevent non-compliant egress
                        if stop_event:
                            stop_event.set()
                        if proc:
                            proc.terminate()
                            try:
                                proc.wait(timeout=3)
                            except subprocess.TimeoutExpired:
                                proc.kill()
                                proc.wait()

                        # Classify root cause post-termination: if physical internet died, divert to internet_drop
                        if not NetworkRouteResolver.is_internet_online_debounced(
                            required_consecutive_failures=2,
                            retry_delay=1.5,
                            timeout_per_probe=1.5,
                        ):
                            log("NETWORK_GUARD: [STRICT] Outbound internet severed! Sealed chunk to disk.", level="WARN")
                            reason = "internet connection lost during strict drop"
                            status = "internet_drop"
                            break

                        state.network_guard_triggered = True
                        reason = "vpn route dropped"
                        status = "vpn_drop"
                        log("NETWORK_GUARD: [STAGE 2 - STRICT] Default route severed from VPN interface! Terminating child process to seal chunk...", level="WARN")
                        break
                else:
                    # Route is verified healthy: clear lenient alarm if it was active
                    if getattr(state, "lenient_alarm_active", False) or getattr(state, "vpn_drop_post_ack_silent", False):
                        state.lenient_alarm_active = False
                        state.vpn_drop_post_ack_silent = False
                        shared_alarm_stop(state)
                        log_good_beep("vpn_restored")
                        beep_good(state)
                        log("NETWORK_GUARD: [LENIENT] VPN connection restored while downloader active! Resuming seamless recording.", level="INFO")

            # Universal physical internet supervision across all stream modes
            if getattr(state, "network_guard", None) and not state.stop_flag:
                if not state.network_guard.check_runtime_internet():
                    if not getattr(state, "internet_drop_active", False):
                        state.internet_drop_active = True
                        if not getattr(state, "internet_drop_post_ack_silent", False):
                            log("NETWORK_GUARD: [LENIENT] Outbound internet dropped! Sounding alarm. Downloader allowed to continue retrying...", level="WARN")
                            shared_alarm_start(state, alarm_type="internet_drop")
                else:
                    if getattr(state, "internet_drop_active", False) or getattr(state, "internet_drop_post_ack_silent", False):
                        state.internet_drop_active = False
                        state.internet_drop_post_ack_silent = False
                        shared_alarm_stop(state)
                        log_good_beep("internet_restored")
                        beep_good(state)
                        log("NETWORK_GUARD: [LENIENT] Internet connection restored while downloader active! Resuming seamless recording.", level="INFO")

            # Rate-Limit Circuit Breaker: Sustained HTTP 429 detected
            if getattr(state, "rate_limit_429_detected", False):
                reason = "http 429 sustained rate limit"
                status = "rate_limit_429"
                log("RATE_LIMIT: Sustained HTTP 429 threshold crossed! Terminating child process to seal chunk...", level="WARN")
                stop_event.set()
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                break

            # Check for crash/stall signals
            if state.nm3u8dl_stall_flag:
                reason = state.nm3u8dl_stall_reason or "file growth stall"
                status = "stalled"
                log(f"N_m3u8DL: stall handling requested → terminating process... reason={reason}", level="WARN")
                stop_event.set()
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    log("N_m3u8DL did not terminate gracefully, killing...")
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
                    proc.kill()
                    proc.wait()
                break

            if not ran_30s and (time.time() - start_run) >= 30:
                ran_30s = True

            time.sleep(0.5)

        # Let the stdout monitor flush N_m3u8DL's final records before we
        # decide whether this run contained an explicit live-end signal.
        stdout_thread.join(timeout=2.0)
        
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

        if getattr(state, "internet_drop_active", False):
            status = "internet_drop"
            reason = "outbound internet dropped"

        if getattr(state, "lenient_alarm_active", False) or (
            getattr(state, "network_guard", None)
            and state.network_guard.is_armed
            and not state.network_guard.check_runtime_route()
        ):
            status = "vpn_drop"
            reason = "vpn route dropped mid-stream"

        return EngineResult(
            status=status,
            reason=reason if status != "ended" or proc.returncode != 0 else f"exit code {proc.returncode}",
            chunk_path=result_chunk_path,
            chunk_name=result_chunk_name,
            metrics={
                "selected_bitrate_kbps": state.nm3u8dl_selected_bitrate_kbps,
                "stall_flag": state.nm3u8dl_stall_flag,
                "ran_30s": ran_30s,
                "live_stream_ended": live_end_event.is_set(),
                "internet_drop": getattr(state, "internet_drop_active", False),
                "geo_block_403": bool(getattr(state, "network_guard", None) and state.network_guard.has_403_error),
            },
        )
    finally:
        if getattr(state, "lenient_alarm_active", False):
            state.lenient_alarm_active = False
            shared_alarm_stop(state)
        if getattr(state, "internet_drop_active", False):
            shared_alarm_stop(state)
        state.nm3u8dl_run_active = False
        state.nm3u8dl_event_queue = None

def run_nm3u8dl_cycle(state: RecorderState, backoff_sec, notify=None, deadline_ts: Optional[float] = None):
    """One N_m3u8DL worker cycle: start/monitor/stop and return run facts only."""

    log_run_start_banner(state, "N_m3u8DL")
    return start_nm3u8dl_to_chunk(state, notify=notify, deadline_ts=deadline_ts)


# ==============================================================================
# Engine: yt-dlp
# ==============================================================================

def _first_http_url_from_command(command_text: str) -> str:
    """Extract the first HTTP(S) URL from user-supplied engine command material."""
    match = re.search(r'https?://[^\\s"\\\']+', command_text or "")
    return match.group(0) if match else ""


def _is_youtube_url(url: str) -> bool:
    value = (url or "").lower()
    return (
        "youtube.com/" in value
        or "youtu.be/" in value
        or "youtube-nocookie.com/" in value
    )


def probe_youtube_live_status(
    probe_args: Optional[List[str]] = None,
    waiter_label: str = "yt-dlp",
    wait_seconds: int = YTDLP_WAIT_FOR_VIDEO_SEC,
    proxy_url: Optional[str] = None,
):
    """
    Use yt-dlp metadata extraction only. No media is downloaded.

    probe_args=None preserves the existing yt-dlp-engine behavior by reusing
    YTDLP_PART_A verbatim. Streamlink mode supplies a recorder-built yt-dlp argv
    containing only the YouTube URL plus probe-relevant access context.

    Returns:
        (available_for_recorder, live_status, reason)
    """
    if probe_args is None:
        if not YTDLP_PART_A.strip():
            return False, None, "empty yt-dlp configuration"

        proxy_opt = f"--proxy {proxy_url} " if proxy_url and "--proxy" not in YTDLP_PART_A else ""
        probe_cmd_text = (
            f'{YTDLP_PART_A.strip()} '
            f'{proxy_opt}'
            f'--skip-download '
            f'--ignore-no-formats-error '
            f'--dump-single-json'
        )
        cmd = probe_cmd_text if os.name == "nt" else shlex.split(probe_cmd_text)
    else:
        if not probe_args:
            return False, None, "empty yt-dlp probe command"
        cmd = list(probe_args)
        if proxy_url and "--proxy" not in cmd:
            cmd.extend(["--proxy", proxy_url])
        cmd.extend([
            "--skip-download",
            "--ignore-no-formats-error",
            "--dump-single-json",
        ])

    log("[PROBE] Checking YouTube live status with yt-dlp metadata...")

    try:
        result = run_external_capture(
            cmd,
            raw_tool="yt-dlp",
            raw_context="YouTube live-status probe",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            errors="replace",
            timeout=YTDLP_METADATA_PROBE_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired:
        return (
            False,
            None,
            f"yt-dlp metadata probe timed out after {YTDLP_METADATA_PROBE_TIMEOUT_SEC}s",
        )
    except Exception as e:
        return False, None, f"yt-dlp metadata probe failed: {e}"

    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip().splitlines()
        reason = detail[-1] if detail else f"yt-dlp metadata probe exit code {result.returncode}"
        return False, None, reason

    raw = (result.stdout or "").strip()
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        # Be tolerant of an unexpected informational line before the JSON.
        info = None
        for candidate in reversed(raw.splitlines()):
            candidate = candidate.strip()
            if not candidate.startswith("{"):
                continue
            try:
                info = json.loads(candidate)
                break
            except json.JSONDecodeError:
                continue

        if info is None:
            return False, None, "yt-dlp metadata probe returned no readable JSON"

    live_status = info.get("live_status")

    # Fallback only if an extractor does not explicitly provide live_status.
    if not live_status:
        if info.get("is_live") is True:
            live_status = "is_live"
        elif info.get("was_live") is True:
            live_status = "was_live"

    if live_status == "is_live":
        log("[PROBE] YouTube status: is_live → ready to record.")
        return True, live_status, "YouTube live"

    if live_status == "is_upcoming":
        log(
            f"[PROBE] YouTube status: is_upcoming → {waiter_label} will wait "
            f"{wait_seconds}s between checks."
        )
        return True, live_status, "YouTube upcoming"

    if live_status in ("was_live", "post_live", "not_live"):
        log(
            f"[PROBE] YouTube status: {live_status} → not currently recordable as live.",
            level="WARN",
        )
        return False, live_status, f"YouTube live_status={live_status}"

    log(
        f"[PROBE] YouTube live status unavailable/unknown ({live_status!r}).",
        level="WARN",
    )
    return False, live_status, "YouTube live status unavailable"




def _strip_outer_quotes(value: str) -> str:
    value = (value or "").strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def _split_user_command_material(command_text: str) -> List[str]:
    """Tokenize user-owned command material without changing the command itself."""
    try:
        tokens = shlex.split(command_text or "", posix=(os.name != "nt"))
    except ValueError as exc:
        log(f"[PROBE] Could not parse Streamlink probe context: {exc}", level="WARN")
        return []
    return [_strip_outer_quotes(token) for token in tokens]


def get_streamlink_source_url() -> str:
    """Extract the actual source URL from STREAMLINK_PART_A without assuming YouTube."""
    if not STREAMLINK_PART_A.strip():
        return ""

    tokens = _split_user_command_material(STREAMLINK_PART_A)
    if not tokens:
        return ""

    # Explicit --url takes precedence.
    for i, token in enumerate(tokens):
        if token == "--url" and i + 1 < len(tokens):
            return tokens[i + 1]
        if token.startswith("--url="):
            return token.split("=", 1)[1]

    # Skip values belonging to known Streamlink options so an HTTP proxy or similar
    # cannot be mistaken for the media/page source URL.
    value_options = {
        "--http-header", "--http-cookie", "--http-cookies-file",
        "--http-query-param", "--http-proxy", "--http-timeout",
        "--interface", "--plugin-dirs", "--loglevel", "--player",
    }

    candidates = []
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if token in value_options:
            i += 2
            continue
        if any(token.startswith(option + "=") for option in value_options):
            i += 1
            continue
        if token.lower().startswith(("http://", "https://")):
            candidates.append(token)
        i += 1

    # Streamlink's positional URL is normally the last HTTP(S) positional token.
    return candidates[-1] if candidates else ""


def build_streamlink_youtube_probe_args(state: Optional[RecorderState] = None):
    """
    Build a yt-dlp metadata-probe argv from STREAMLINK_PART_A without translating
    the Streamlink recording command. Only request/access context needed by the
    probe is mapped: YouTube URL, headers, cookies, proxy, query params, timeout,
    TLS verification, IP family, and source address. Unknown Streamlink options
    remain completely untouched for the actual Streamlink recording command.

    Returns (probe_args, source_url, reason).
    """
    if not STREAMLINK_PART_A.strip():
        return None, "", "empty STREAMLINK_PART_A"

    tokens = _split_user_command_material(STREAMLINK_PART_A)
    if not tokens:
        return None, "", "STREAMLINK_PART_A could not be parsed for probing"

    source_url = ""
    headers = []
    cookies = []
    cookie_files = []
    query_params = []
    proxy = None
    timeout_value = None
    source_address = None
    no_ssl_verify = False
    force_ipv4 = False
    force_ipv6 = False

    def take_value(index: int, option: str):
        token = tokens[index]
        if token == option:
            if index + 1 >= len(tokens):
                return None, index
            return tokens[index + 1], index + 1
        prefix = option + "="
        if token.startswith(prefix):
            return token[len(prefix):], index
        return None, index

    i = 0
    while i < len(tokens):
        token = tokens[i]

        value, consumed = take_value(i, "--url")
        if value is not None:
            source_url = value
            i = consumed + 1
            continue

        matched = False
        for option, bucket in (
            ("--http-header", headers),
            ("--http-cookie", cookies),
            ("--http-cookies-file", cookie_files),
            ("--http-query-param", query_params),
        ):
            value, consumed = take_value(i, option)
            if value is not None:
                bucket.append(value)
                i = consumed + 1
                matched = True
                break
        if matched:
            continue

        value, consumed = take_value(i, "--http-proxy")
        if value is not None:
            proxy = value
            i = consumed + 1
            continue

        value, consumed = take_value(i, "--http-timeout")
        if value is not None:
            timeout_value = value
            i = consumed + 1
            continue

        value, consumed = take_value(i, "--interface")
        if value is not None:
            source_address = value
            i = consumed + 1
            continue

        if token == "--http-no-ssl-verify":
            no_ssl_verify = True
            i += 1
            continue

        if token in ("-4", "--ipv4"):
            force_ipv4 = True
            force_ipv6 = False
            i += 1
            continue

        if token in ("-6", "--ipv6"):
            force_ipv6 = True
            force_ipv4 = False
            i += 1
            continue

        # Positional source URL. Only consider a token that itself starts with
        # HTTP(S), so values like Referer=https://... cannot be mistaken for it.
        if token.lower().startswith(("http://", "https://")):
            source_url = token

        i += 1

    if not source_url:
        return None, "", "no source URL found in STREAMLINK_PART_A"

    if not _is_youtube_url(source_url):
        return None, source_url, "Streamlink source is not YouTube; YouTube metadata probe not required"

    # Streamlink query parameters are session-wide. Preserve them on the source
    # URL used by the yt-dlp probe.
    if query_params:
        parsed = urlparse(source_url)
        combined_query = parse_qsl(parsed.query, keep_blank_values=True)
        for item in query_params:
            if "=" in item:
                key, value = item.split("=", 1)
            else:
                key, value = item, ""
            combined_query.append((key, value))
        source_url = parsed._replace(query=urlencode(combined_query, doseq=True)).geturl()

    try:
        probe_args = shlex.split(
            STREAMLINK_YTDLP_PROBE_BASE,
            posix=(os.name != "nt"),
        )
        probe_args = [_strip_outer_quotes(token) for token in probe_args]
    except ValueError as exc:
        return None, source_url, f"invalid STREAMLINK_YTDLP_PROBE_BASE: {exc}"

    if not probe_args:
        return None, source_url, "empty STREAMLINK_YTDLP_PROBE_BASE"

    # Map generic request context only. These options affect metadata access but
    # do not attempt to translate Streamlink's downloader/plugin behavior.
    for header in headers:
        if "=" in header:
            key, value = header.split("=", 1)
            probe_args.extend(["--add-headers", f"{key}:{value}"])

    if cookies:
        cookie_value = "; ".join(item for item in cookies if item)
        if cookie_value:
            probe_args.extend(["--add-headers", f"Cookie:{cookie_value}"])

    if cookie_files:
        # yt-dlp accepts one active cookie file; use the last Streamlink cookie
        # file because later CLI values are the closest equivalent to override.
        probe_args.extend(["--cookies", cookie_files[-1]])

    if proxy:
        probe_args.extend(["--proxy", proxy])
    elif is_wireproxy_active(state):
        probe_args.extend(["--proxy", f"socks5://127.0.0.1:{WIREPROXY_SOCKS5_PORT}"])

    if timeout_value:
        probe_args.extend(["--socket-timeout", timeout_value])

    if source_address:
        probe_args.extend(["--source-address", source_address])

    if no_ssl_verify:
        probe_args.append("--no-check-certificates")

    if force_ipv4:
        probe_args.append("-4")
    elif force_ipv6:
        probe_args.append("-6")

    probe_args.append(source_url)
    return probe_args, source_url, "available"


def probe_source_for_engine(state: RecorderState, engine: RecorderEngine):
    """
    Stage 3 source-aware pre-attempt availability check.

    Returns (available, reason). N_m3u8DL keeps its existing self-managed
    startup/manifest checks.
    """
    proxy_url = f"http://127.0.0.1:{WIREPROXY_HTTP_PORT}" if is_wireproxy_active(state) else None
    socks5_proxy = f"socks5://127.0.0.1:{WIREPROXY_SOCKS5_PORT}" if is_wireproxy_active(state) else None

    if engine.name == ENGINE_NM3U8DL:
        return True, "N_m3u8DL self-managed availability"

    if engine.name == ENGINE_FFMPEG:
        if FFMPEG_PART_A.strip():
            ok = is_advanced_ffmpeg_stream_alive(proxy_url=proxy_url)
            return ok, "advanced FFmpeg source unavailable" if not ok else "available"

        ok = is_stream_alive(proxy_url=proxy_url)
        return ok, "direct FFmpeg source unavailable" if not ok else "available"

    if engine.name == ENGINE_YTDLP:
        state.ytdlp_wait_for_video = False
        state.ytdlp_live_status = None

        source_url = _first_http_url_from_command(YTDLP_PART_A)
        if not source_url:
            return False, "no source URL found in YTDLP_PART_A"

        if _is_youtube_url(source_url):
            ok, live_status, reason = probe_youtube_live_status(proxy_url=socks5_proxy)
            state.ytdlp_live_status = live_status
            state.ytdlp_wait_for_video = (live_status == "is_upcoming")
            return ok, reason

        # Non-YouTube yt-dlp source-specific probing (notably FanCode) is
        # completed in Stage 5. Do not incorrectly apply YouTube semantics.
        return True, "non-YouTube yt-dlp source"

    if engine.name == ENGINE_STREAMLINK:
        state.streamlink_wait_for_video = False
        state.streamlink_live_status = None

        source_url = get_streamlink_source_url()
        if not source_url:
            reason = "no source URL found in STREAMLINK_PART_A"
            log(f"[PROBE] Streamlink source setup failed: {reason}", level="ERROR")
            state.stop_flag = True
            return False, reason

        if _is_youtube_url(source_url):
            probe_args, _, reason = build_streamlink_youtube_probe_args(state=state)
            if not probe_args:
                log(f"[PROBE] Streamlink YouTube probe setup failed: {reason}", level="ERROR")
                state.stop_flag = True
                return False, reason

            ok, live_status, reason = probe_youtube_live_status(
                probe_args=probe_args,
                waiter_label="Streamlink",
                wait_seconds=STREAMLINK_WAIT_FOR_STREAM_SEC,
                proxy_url=socks5_proxy,
            )
            state.streamlink_live_status = live_status
            state.streamlink_wait_for_video = (live_status == "is_upcoming")
            return ok, reason

        # Non-YouTube Streamlink sources deliberately bypass YouTube metadata logic.
        # Streamlink itself owns protocol/plugin resolution and source availability;
        # the existing recorder worker then owns file-growth monitoring/retry policy.
        log("[PROBE] Non-YouTube Streamlink source → using Streamlink native source handling.")
        return True, "non-YouTube Streamlink source"

    return False, f"unsupported engine {engine.name}"


def find_ytdlp_native_chunk(chunk_stem: str):
    """Return the actual yt-dlp media file for this recorder chunk, if present."""
    if not os.path.isdir(CHUNKS_DIR):
        return None, None

    media_exts = {
        ".webm", ".mp4", ".mkv", ".ts", ".m4v", ".mov", ".flv", ".avi",
        ".m4a", ".mp3", ".ogg", ".opus", ".aac", ".flac", ".wav",
    }
    prefix = f"{chunk_stem}."
    candidates = []

    for name in os.listdir(CHUNKS_DIR):
        if not name.startswith(prefix):
            continue

        path = os.path.join(CHUNKS_DIR, name)
        if not os.path.isfile(path):
            continue

        ext = os.path.splitext(name)[1].lower()
        if ext not in media_exts:
            continue

        candidates.append(path)

    if not candidates:
        return None, None

    # A normal yt-dlp attempt produces one combined A/V file. If an unusual
    # source leaves more than one media candidate, prefer the largest one.
    path = max(candidates, key=os.path.getsize)
    return path, os.path.basename(path)


def prepare_ytdlp_finalization_inputs(state: RecorderState) -> bool:
    """
    After recording has fully ended, losslessly remux accepted native yt-dlp
    chunks to MKV and build the MKV concat list/separators.

    Native downloaded files are never deleted here. Any failure stops only
    automatic finalization and leaves all captured media for manual handling.
    """
    sessions = sorted(state.stats.get("sessions", []), key=lambda x: x["start"])

    if not sessions:
        log("yt-dlp finalization: no accepted GOOD chunks found.", level="WARN")
        return False

    normalized_names = []

    log("")
    log("yt-dlp finalization → preparing accepted chunks as MKV (stream copy)...")

    for sess in sessions:
        native_name = sess.get("name")
        if not native_name:
            log(
                "yt-dlp finalization: accepted chunk name is missing → manual handling required.",
                level="ERROR",
            )
            return False

        native_path = os.path.join(CHUNKS_DIR, native_name)
        if not os.path.exists(native_path):
            log(
                f"yt-dlp finalization: accepted chunk missing ({native_name}) "
                "→ manual handling required.",
                level="ERROR",
            )
            return False

        stem = os.path.splitext(native_name)[0]
        mkv_name = f"{stem}.mkv"
        mkv_path = os.path.join(CHUNKS_DIR, mkv_name)

        if os.path.normcase(os.path.abspath(native_path)) == os.path.normcase(os.path.abspath(mkv_path)):
            log(f"yt-dlp finalization: {native_name} already MKV → no remux needed.")
            normalized_names.append(mkv_name)
            continue

        log(f"yt-dlp finalization: {native_name} → {mkv_name} (stream copy)")

        cmd = [
            "ffmpeg",
            "-y",
            "-i", native_name,
            "-map", "0:v",
            "-map", "0:a?",
            "-c", "copy",
            mkv_name,
        ]

        rc = run_ffmpeg_with_capture(cmd, CHUNKS_DIR, tag="FFmpeg-YTDLP-MKV")

        if (
            rc != 0
            or not os.path.exists(mkv_path)
            or os.path.getsize(mkv_path) <= 0
        ):
            log(
                f"yt-dlp finalization: MKV remux failed for {native_name} "
                "→ automatic finalization stopped; native chunks preserved for manual handling.",
                level="ERROR",
            )
            return False

        mkv_dur, _ = get_file_info(mkv_path)
        if mkv_dur <= 0:
            log(
                f"yt-dlp finalization: remuxed MKV is not readable ({mkv_name}) "
                "→ automatic finalization stopped; native chunks preserved for manual handling.",
                level="ERROR",
            )
            return False

        normalized_names.append(mkv_name)
        log(f"yt-dlp finalization: MKV ready → {mkv_name}")

    concat_entries = []

    for idx, mkv_name in enumerate(normalized_names):
        if idx > 0:
            black_path = make_black_clip(
                state,
                output_ext=".mkv",
                output_format="matroska",
            )

            if black_path is None:
                log(
                    "yt-dlp finalization: matching MKV separator could not be created "
                    "→ automatic finalization stopped; chunks preserved for manual handling.",
                    level="ERROR",
                )
                return False

            concat_entries.append(os.path.basename(black_path))

        concat_entries.append(mkv_name)

    temp_list = LIST_FILE + ".tmp"
    try:
        with open(temp_list, "w", encoding="utf-8") as f:
            for name in concat_entries:
                f.write(f"file '{name}'\n")
        os.replace(temp_list, LIST_FILE)
    except Exception as e:
        log(
            f"yt-dlp finalization: could not build concat list: {e} "
            "→ chunks preserved for manual handling.",
            level="ERROR",
        )
        try:
            if os.path.exists(temp_list):
                os.remove(temp_list)
        except OSError:
            pass
        return False

    log("yt-dlp finalization: MKV concat inputs ready.")
    return True


def run_ytdlp_cycle(state: RecorderState, backoff_sec, notify=None, deadline_ts: Optional[float] = None):
    """One yt-dlp worker cycle: start/monitor/stop and return run facts only."""

    log_run_start_banner(state, "yt-dlp")

    if not YTDLP_PART_A.strip():
        log("yt-dlp configuration is empty → cannot start.", level="ERROR")
        state.stop_flag = True
        return EngineResult(
            status="error",
            reason="empty YTDLP_PART_A",
            chunk_path=None,
            chunk_name=None,
            metrics={},
        )

    chunk_stem = next_chunk_stem(state)
    output_template = os.path.join(CHUNKS_DIR, f"{chunk_stem}.%(ext)s")
    before_dl_marker = os.path.join(CHUNKS_DIR, f".{chunk_stem}_before_dl.marker")
    state.stats["last_run_start"] = time.time()
    state.ytdlp_download_started = False

    try:
        if os.path.exists(before_dl_marker):
            os.remove(before_dl_marker)
    except OSError:
        pass

    wait_for_video_arg = (
        f'--wait-for-video {YTDLP_WAIT_FOR_VIDEO_SEC} '
        if state.ytdlp_wait_for_video
        else ''
    )

    proxy_arg = ""
    ffmpeg_proxy_arg = ""
    if is_wireproxy_active(state):
        if "--proxy" not in YTDLP_PART_A:
            proxy_arg = f"--proxy socks5://127.0.0.1:{WIREPROXY_SOCKS5_PORT} "
        ffmpeg_proxy_arg = f" -http_proxy http://127.0.0.1:{WIREPROXY_HTTP_PORT}"

    # User supplies source-specific yt-dlp material. Recorder preserves best
    # format selection and lets yt-dlp choose the natural capture container.
    # before_dl writes a tiny marker only when yt-dlp transitions from legitimate
    # waiting into the actual download stage. Stage 4 will attach file-growth
    # supervision to this already-established transition.
    ytdlp_cmd_text = (
        f'{YTDLP_PART_A.strip()} '
        f'{proxy_arg}'
        f'--no-live-from-start '
        f'{wait_for_video_arg}'
        f'--print-to-file "before_dl:RECORDER_BEFORE_DL TOP=%(tbr)s F0=%(requested_formats.0.tbr)s F1=%(requested_formats.1.tbr)s" "{before_dl_marker}" '
        f'-f "bv*+ba/b" '
        f'--downloader ffmpeg '
        f'--downloader-args "ffmpeg_i:-rw_timeout 15000000 -reconnect 1 -reconnect_streamed 1 -reconnect_on_network_error 1 -reconnect_delay_max 10 -reconnect_delay_total_max 30{ffmpeg_proxy_arg}" '
        f'--downloader-args "ffmpeg_i2:-isync 0" '
        f'--hls-use-mpegts '
        f'--no-part '
        f'-o "{output_template}"'
    )
    cmd = ytdlp_cmd_text if os.name == "nt" else shlex.split(ytdlp_cmd_text)

    log(
        f"Starting yt-dlp → recorder chunk {chunk_stem} "
        "(container selected by yt-dlp)..."
    )
    log(f"Command: {ytdlp_cmd_text}")
    log("")

    popen_kwargs = {
        "stdout": subprocess.PIPE,
        "stderr": subprocess.STDOUT,
        "text": True,
        "errors": "replace",
        "bufsize": 1,
    }

    if os.name == "nt":
        popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        popen_kwargs["start_new_session"] = True

    raw_invocation = raw_external_start("yt-dlp", chunk_stem)
    try:
        proc = subprocess.Popen(cmd, **popen_kwargs)
    except Exception as e:
        raw_external_end(
            raw_invocation,
            status=f"launch exception {type(e).__name__}",
        )
        log(f"yt-dlp failed to start: {e}", level="ERROR")
        state.stop_flag = True
        return EngineResult(
            status="error",
            reason=f"yt-dlp launch failed: {e}",
            chunk_path=None,
            chunk_name=None,
            metrics={},
        )

    def monitor_ytdlp_output():
        last_progress_len = 0

        # During --wait-for-video, yt-dlp can repeat the same extraction/wait
        # boilerplate every polling cycle. Log each distinct message once while
        # waiting, but immediately show anything that actually changes.
        seen_wait_messages = set()
        wait_cycle_started = False

        try:
            while True:
                line = proc.stdout.readline()
                if not line:
                    break

                # Capture before recorder suppression, rewriting, or progress handling.
                raw_external_write(raw_invocation, line)
                s = line.strip()
                if not s:
                    continue

                # Network Guard: Inspect line for HTTP 403 / 429
                if state is not None and getattr(state, "network_guard", None):
                    if state.network_guard.inspect_stdout_line(s):
                        state.rate_limit_429_detected = True

                if should_suppress_live_engine_output_line(s):
                    continue

                # Repetitive live-status output should update one terminal
                # line instead of creating permanent log entries.
                if (
                    s.startswith("frame=")
                    or "[wait] Remaining time until next attempt:" in s
                ):
                    prog = f"yt-dlp: {s}"
                    pad = max(0, last_progress_len - len(prog))
                    render_progress_line(prog, pad=pad)
                    last_progress_len = len(prog)
                    continue

                # yt-dlp's standalone Ctrl+C meaning is different inside
                # the recorder, where Ctrl+C stops the overall recording.
                if "[wait] Waiting for " in s and "Press Ctrl+C to try now" in s:
                    s = s.replace(
                        "Press Ctrl+C to try now",
                        "Press Ctrl+C to stop the recorder",
                    )

                # Messages that yt-dlp commonly repeats unchanged during
                # --wait-for-video polling.
                is_wait_boilerplate = (
                    s.startswith("[youtube] Extracting URL:")
                    or (
                        s.startswith("[youtube]")
                        and ": Downloading webpage" in s
                    )
                    or (
                        s.startswith("[youtube]")
                        and "Downloading" in s
                        and "player API JSON" in s
                    )
                    or s.startswith(
                        "WARNING: [youtube] This live event will begin"
                    )
                    or s == "WARNING: Release time of video is not known"
                    or s.startswith(
                        "[wait] Wait period ended; Re-extracting data"
                    )
                    or s.startswith("[wait] Waiting for ")
                )

                if is_wait_boilerplate:
                    if wait_cycle_started and s in seen_wait_messages:
                        continue

                    seen_wait_messages.add(s)

                    if s.startswith("[wait] Waiting for "):
                        wait_cycle_started = True

                # Once yt-dlp actually moves into download, stop applying the
                # waiting-cycle deduplication and return to normal output.
                if (
                    (s.startswith("[info]") and "Downloading" in s and "format" in s)
                    or s.startswith("[download] Destination:")
                ):
                    wait_cycle_started = False
                    seen_wait_messages.clear()

                # Any meaningful non-progress message becomes a normal
                # permanent log entry. log() also clears/preserves the
                # current live progress line through shared machinery.
                log(f"yt-dlp: {s}")
                last_progress_len = 0

        except Exception as e:
            log(f"yt-dlp output monitor error: {e}", level="WARN")
        finally:
            clear_progress_line()

    output_thread = threading.Thread(
        target=monitor_ytdlp_output,
        daemon=True,
    )
    output_thread.start()

    def stop_ytdlp_process_tree(force_tree: bool = False):
        """Stop yt-dlp and its external FFmpeg child; force the full tree for a detected stall."""
        if proc.poll() is not None:
            return

        if os.name == "nt":
            if force_tree:
                subprocess.run(
                    ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                return

            try:
                proc.send_signal(signal.CTRL_BREAK_EVENT)
                proc.wait(timeout=8)
                return
            except (subprocess.TimeoutExpired, OSError, ValueError):
                pass

            log(
                "yt-dlp/FFmpeg did not stop gracefully → killing process tree.",
                level="WARN",
            )
            subprocess.run(
                ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

            return

        if force_tree:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (OSError, ProcessLookupError):
                if proc.poll() is None:
                    proc.kill()
            proc.wait()
            return

        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGINT)
            proc.wait(timeout=8)
            return
        except (subprocess.TimeoutExpired, OSError, ProcessLookupError):
            pass

        log(
            "yt-dlp/FFmpeg did not stop gracefully → killing process group.",
            level="WARN",
        )

        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (OSError, ProcessLookupError):
            if proc.poll() is None:
                proc.kill()

        proc.wait()

    status = "ended"
    reason = "yt-dlp exited"
    chunk_path = None
    chunk_name = None
    growth_tracker = None
    stall_type = None
    ytdlp_expected_kbps: Optional[float] = None
    ytdlp_expected_source: Optional[str] = None
    ytdlp_source_recovery_reported = False
    ytdlp_last_health_log_t = 0.0

    state.ytdlp_healthy_consec_count = 0
    state.ytdlp_beeped_this_healthy_streak = False
    ytdlp_had_stall_incident = live_stall_incident_active(state)

    while True:
        ret = proc.poll()

        if (
            not state.ytdlp_download_started
            and os.path.exists(before_dl_marker)
        ):
            state.ytdlp_download_started = True
            state.stats["last_run_start"] = time.time()

            # Prefer yt-dlp's selected-format bitrate metadata when it is available.
            # For separate video+audio selections, fall back to the sum of their tbr values.
            try:
                with open(before_dl_marker, "r", encoding="utf-8", errors="replace") as f:
                    marker_text = f.read()

                top_match = re.search(r"\bTOP=([0-9]+(?:\.[0-9]+)?)", marker_text)
                f0_match = re.search(r"\bF0=([0-9]+(?:\.[0-9]+)?)", marker_text)
                f1_match = re.search(r"\bF1=([0-9]+(?:\.[0-9]+)?)", marker_text)

                top_kbps = float(top_match.group(1)) if top_match else 0.0
                parts_kbps = sum(
                    float(m.group(1))
                    for m in (f0_match, f1_match)
                    if m is not None and float(m.group(1)) > 0
                )
                selected_kbps = top_kbps if top_kbps > 0 else parts_kbps

                if selected_kbps > 0:
                    ytdlp_expected_kbps = selected_kbps
                    ytdlp_expected_source = "selected_metadata"
                    state.ytdlp_selected_bitrate_kbps_max = max(
                        state.ytdlp_selected_bitrate_kbps_max,
                        int(selected_kbps),
                    )
                    log(
                        f"yt-dlp: Selected bitrate detected {int(selected_kbps)} Kbps "
                        "(source=selected_metadata)"
                    )
            except OSError:
                pass

            # R8-R10: file-growth supervision begins only after before_dl.
            # The native file may not exist yet; starting from size 0 is intentional.
            growth_tracker = start_live_file_growth_tracker(chunk_path)
            ytdlp_last_health_log_t = time.monotonic()

            log("yt-dlp before_dl → actual download stage started.")

        # Resolve the actual native growing file as soon as yt-dlp creates it.
        # Stage 4 stall monitoring will use this same resolved path, but only
        # after ytdlp_download_started has become True.
        if chunk_path is None:
            found_path, found_name = find_ytdlp_native_chunk(chunk_stem)
            if found_path:
                chunk_path = found_path
                chunk_name = found_name
                log(f"yt-dlp active chunk resolved → {chunk_name}")

        # R12 recovery: for non-YouTube yt-dlp sources the pre-probe is intentionally
        # not authoritative. Actual growing media is the proof that the source returned.
        if (
            state.ytdlp_download_started
            and chunk_path is not None
            and not ytdlp_source_recovery_reported
        ):
            try:
                if os.path.getsize(chunk_path) > LIVE_HARD_STALL_ZERO_BYTES:
                    if notify:
                        notify("live_source_recovery")
                    ytdlp_source_recovery_reported = True
            except OSError:
                pass

        if ret is not None:
            break

        if recording_deadline_reached(state):
            status = "duration_reached"
            reason = "duration reached"
            log(
                f"Max duration reached {format_current_duration_for_log(state)} → "
                "terminating yt-dlp/FFmpeg..."
            )
            stop_ytdlp_process_tree()
            break

        if state.stop_flag:
            reason = "external stop"
            log("stop_flag detected → terminating yt-dlp/FFmpeg...")
            stop_ytdlp_process_tree()
            break

        # R8-R10: once actual downloading has begun, use the same shared
        # file-growth, hard-stall, and soft-stall machinery as FFmpeg.
        if state.ytdlp_download_started and growth_tracker is not None:
            growth_check = poll_live_file_growth(chunk_path, growth_tracker)

            if growth_check is not None:
                live_hard_required = LIVE_HARD_STALL_REQUIRED
                live_soft_required = LIVE_SOFT_STALL_REQUIRED
                is_double_vpn_capped = False
                guard = getattr(state, "network_guard", None)
                if guard and hasattr(guard, "is_wireproxy_nested_in_vpn"):
                    is_nested, _ = guard.is_wireproxy_nested_in_vpn()
                    if is_nested and (live_hard_required > 4 or live_soft_required > 4):
                        is_double_vpn_capped = True
                        live_hard_required = min(4, live_hard_required)
                        live_soft_required = min(4, live_soft_required)

                growth_kbps = growth_check["growth_kbps"]
                soft_trigger = False

                if growth_check["is_hard"]:
                    log(
                        f"HARD_STALL_CHECK "
                        f"{min(growth_check['hard_count'], live_hard_required)}/"
                        f"{live_hard_required} "
                        f"growth_bytes={growth_check['growth_bytes']}"
                    )
                    if not is_alarm_active(state):
                        beep_bad(state)
                else:
                    # R10 bitrate priority: selected-format metadata first; otherwise
                    # reuse the prior healthy maximum, then fall back to actual growth.
                    if ytdlp_expected_kbps is None:
                        if state.ytdlp_selected_bitrate_kbps_max > 0:
                            ytdlp_expected_kbps = float(state.ytdlp_selected_bitrate_kbps_max)
                            ytdlp_expected_source = "previous_max"
                            log(
                                f"yt-dlp: Selected bitrate detected {int(ytdlp_expected_kbps)} Kbps "
                                "(source=previous_max)"
                            )
                        else:
                            ytdlp_expected_kbps = growth_kbps
                            ytdlp_expected_source = "growth"
                            state.ytdlp_selected_bitrate_kbps_max = int(growth_kbps)
                            log(
                                f"yt-dlp: Selected bitrate detected {int(ytdlp_expected_kbps)} Kbps "
                                "(source=growth fallback_from=selected_metadata)"
                            )
                    elif (
                        ytdlp_expected_source != "selected_metadata"
                        and growth_kbps > state.ytdlp_selected_bitrate_kbps_max
                    ):
                        state.ytdlp_selected_bitrate_kbps_max = int(growth_kbps)
                        ytdlp_expected_kbps = growth_kbps
                        ytdlp_expected_source = "growth"

                    soft_check = classify_live_soft_stall(
                        growth_tracker,
                        growth_kbps,
                        ytdlp_expected_kbps,
                        live_soft_required,
                    )
                    soft_trigger = soft_check["soft_trigger"]

                    if soft_check["is_soft"]:
                        threshold_kbps = soft_check["threshold_kbps"]
                        log(
                            f"SOFT_STALL_CHECK "
                            f"{min(soft_check['soft_count'], live_soft_required)}/"
                            f"{live_soft_required} "
                            f"growth_kbps={int(growth_kbps)} "
                            f"threshold_kbps={int(threshold_kbps)} "
                            f"expected_kbps={int(ytdlp_expected_kbps)}"
                        )
                        if not is_alarm_active(state):
                            beep_bad(state)
                    else:
                        threshold_kbps = soft_check["threshold_kbps"]

                        if notify:
                            notify("live_stall_recovery")
                        maybe_trigger_good_beep(
                            state,
                            "ytdlp",
                            growth_check["has_file"],
                            ytdlp_had_stall_incident,
                            notify,
                        )

                        if threshold_kbps is not None:
                            now_health_log_t = time.monotonic()
                            if (now_health_log_t - ytdlp_last_health_log_t) >= LIVE_HEALTH_LOG_INTERVAL:
                                log(
                                    f"yt-dlp: Good speed {int(growth_kbps)} Kbps "
                                    f"(threshold {int(threshold_kbps)})",
                                    activity_id=new_terminal_activity("ytdlp_health"),
                                )
                                ytdlp_last_health_log_t = now_health_log_t

                hard_trigger = growth_check["hard_count"] >= live_hard_required

                if hard_trigger or soft_trigger:
                    stall_type = "hard" if hard_trigger else "soft"
                    log(f"CLASSIFY STALL_{stall_type.upper()}", level="WARN")
                    reset_good_beep_state(state, "ytdlp")

                    # Network Guard: In-flight attribution for nested double-VPN stall
                    guard = getattr(state, "network_guard", None)
                    if guard and hasattr(guard, "is_wireproxy_nested_in_vpn"):
                        is_nested, vpn_name = guard.is_wireproxy_nested_in_vpn()
                        if is_nested:
                            if is_double_vpn_capped:
                                effective_trigger = live_soft_required if soft_trigger else live_hard_required
                                normal_trigger = LIVE_SOFT_STALL_REQUIRED if soft_trigger else LIVE_HARD_STALL_REQUIRED
                                log(
                                    f"NETWORK_GUARD: Active double-VPN ({vpn_name}) detected during stall! "
                                    f"Smart stall cap enforced: ending chunk early at check {effective_trigger} (normal {normal_trigger}) "
                                    f"to seal video bytes safely and hand over routing for next chunk.",
                                    level="WARN",
                                )
                            else:
                                log(
                                    f"NETWORK_GUARD: Stream stalled while Wireproxy was nested inside {vpn_name}! "
                                    f"Attributing stall to nested tunnel packet loss / MTU conflict.",
                                    level="WARN",
                                )

                    status = "stalled"
                    reason = f"STALL_{stall_type.upper()}"
                    stop_ytdlp_process_tree(force_tree=True)
                    break

        time.sleep(0.5)

    if proc.poll() is None:
        stop_ytdlp_process_tree()

    try:
        output_thread.join(timeout=3)
    except threading.ThreadError:
        pass

    # Final fallback if the process ended before the polling loop observed it.
    if (
        not state.ytdlp_download_started
        and os.path.exists(before_dl_marker)
    ):
        state.ytdlp_download_started = True
        state.stats["last_run_start"] = time.time()
        log("yt-dlp before_dl → actual download stage started.")

    if chunk_path is None:
        chunk_path, chunk_name = find_ytdlp_native_chunk(chunk_stem)
        if chunk_path:
            log(f"yt-dlp completed chunk resolved → {chunk_name}")

    returncode = proc.poll()
    raw_external_end(raw_invocation, returncode=returncode)
    download_started = state.ytdlp_download_started

    # R12: any unrequested yt-dlp ending that was not classified as a stall
    # is a source-unavailable/offline ending, regardless of yt-dlp's exit code.
    if status == "ended" and reason == "yt-dlp exited" and not state.stop_flag:
        status = "no_stream"
        reason = "yt-dlp source unavailable"
        log("CLASSIFY SOURCE_UNAVAILABLE", level="WARN")

    try:
        if os.path.exists(before_dl_marker):
            os.remove(before_dl_marker)
    except OSError:
        pass

    log(f"yt-dlp stopped for this run (exit code {returncode})")

    return EngineResult(
        status=status,
        reason=reason if reason != "yt-dlp exited" else f"exit code {returncode}",
        chunk_path=chunk_path,
        chunk_name=chunk_name,
        metrics={
            "exit_code": returncode,
            "download_started": download_started,
            "waited_for_video": state.ytdlp_wait_for_video,
            "live_status": state.ytdlp_live_status,
            "stall_type": stall_type,
        },
    )



# ==============================================================================
# Engine: Streamlink
# ==============================================================================

def run_streamlink_cycle(state: RecorderState, backoff_sec, notify=None, deadline_ts: Optional[float] = None):
    """One Streamlink worker cycle: start/monitor/stop and return run facts only."""

    log_run_start_banner(state, "Streamlink")

    if not STREAMLINK_PART_A.strip():
        log("Streamlink configuration is empty → cannot start.", level="ERROR")
        state.stop_flag = True
        return EngineResult(
            status="error",
            reason="empty STREAMLINK_PART_A",
            chunk_path=None,
            chunk_name=None,
            metrics={},
        )

    chunk_path, chunk_name = next_chunk_names(state)
    state.stats["last_run_start"] = time.time()
    state.streamlink_download_started = False

    wait_args = (
        f'--retry-streams {STREAMLINK_WAIT_FOR_STREAM_SEC} --retry-max 0 '
        if state.streamlink_wait_for_video
        else ''
    )

    proxy_arg = ""
    if is_wireproxy_active(state) and "--http-proxy" not in STREAMLINK_PART_A:
        proxy_arg = f"--http-proxy socks5://127.0.0.1:{WIREPROXY_SOCKS5_PORT} "

    # Preserve STREAMLINK_PART_A verbatim. Recorder-owned options are appended:
    # optional upcoming-stream retry, dynamic chunk output, and recorder-selected quality.
    streamlink_cmd_text = (
        f'{STREAMLINK_PART_A.strip()} '
        f'{proxy_arg}'
        f'{wait_args}'
        f'--progress=force '
        f'--output "{chunk_path}" '
        f'{STREAMLINK_STREAM}'
    )
    cmd = streamlink_cmd_text if os.name == "nt" else shlex.split(streamlink_cmd_text)

    log(f"Starting Streamlink → writing to {chunk_name}...")
    log(f"Command: {streamlink_cmd_text}")
    log("")

    popen_kwargs = {
        "stdout": subprocess.PIPE,
        "stderr": subprocess.STDOUT,
        "text": True,
        "errors": "replace",
        "bufsize": 1,
    }

    if os.name == "nt":
        popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        popen_kwargs["start_new_session"] = True

    raw_invocation = raw_external_start("Streamlink", chunk_name)
    try:
        proc = subprocess.Popen(cmd, **popen_kwargs)
    except Exception as e:
        raw_external_end(
            raw_invocation,
            status=f"launch exception {type(e).__name__}",
        )
        log(f"Streamlink failed to start: {e}", level="ERROR")
        state.stop_flag = True
        return EngineResult(
            status="error",
            reason=f"Streamlink launch failed: {e}",
            chunk_path=None,
            chunk_name=None,
            metrics={},
        )

    def monitor_streamlink_output():
        last_progress_len = 0

        try:
            while True:
                line = proc.stdout.readline()
                if not line:
                    break

                # Capture before recorder suppression or progress handling.
                raw_external_write(raw_invocation, line)
                s = line.strip()
                if not s:
                    continue

                if should_suppress_live_engine_output_line(s):
                    continue

                if "this live event has ended" in s.lower():
                    state.streamlink_live_status = "post_live"

                # Streamlink download/wait progress: keep updating ONE terminal line.
                if (
                    s.startswith("[download] Written ")
                    or "LIVE_STREAM_OFFLINE:" in s
                ):
                    prog = f"Streamlink: {s}"
                    pad = max(0, last_progress_len - len(prog))

                    render_progress_line(prog, pad=pad)
                    last_progress_len = len(prog)
                    continue

                # This repeats every retry and adds no information beyond the
                # LIVE_STREAM_OFFLINE countdown shown on the live progress line.
                if s.startswith("[cli][info] Waiting for streams, retrying every "):
                    continue

                # Meaningful non-progress output becomes a normal permanent log line.
                # log() automatically finishes/preserves any active progress line first.
                log(f"Streamlink: {s}")
                last_progress_len = 0

        except Exception as e:
            log(f"Streamlink output monitor error: {e}", level="WARN")
        finally:
            clear_progress_line()

    output_thread = threading.Thread(
        target=monitor_streamlink_output,
        daemon=True,
    )
    output_thread.start()

    def stop_streamlink_process_tree(force_tree: bool = False):
        if proc.poll() is not None:
            return

        if os.name == "nt":
            if force_tree:
                subprocess.run(
                    ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                return

            try:
                proc.send_signal(signal.CTRL_BREAK_EVENT)
                proc.wait(timeout=8)
                return
            except (subprocess.TimeoutExpired, OSError, ValueError):
                pass

            log(
                "Streamlink did not stop gracefully → killing process tree.",
                level="WARN",
            )
            subprocess.run(
                ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            return

        if force_tree:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (OSError, ProcessLookupError):
                if proc.poll() is None:
                    proc.kill()
            proc.wait()
            return

        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGINT)
            proc.wait(timeout=8)
            return
        except (subprocess.TimeoutExpired, OSError, ProcessLookupError):
            pass

        log(
            "Streamlink did not stop gracefully → killing process group.",
            level="WARN",
        )
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (OSError, ProcessLookupError):
            if proc.poll() is None:
                proc.kill()
        proc.wait()

    status = "ended"
    reason = "Streamlink exited"
    growth_tracker = None
    stall_type = None
    streamlink_expected_kbps: Optional[float] = None
    download_start_ts = None
    streamlink_last_health_log_t = 0.0
    ran_30s = False

    previous_good_kbps = float(state.stats.get("last_good_br_kbps") or 0)
    if previous_good_kbps > 0:
        streamlink_expected_kbps = previous_good_kbps
        state.streamlink_selected_bitrate_kbps_max = max(
            state.streamlink_selected_bitrate_kbps_max,
            int(previous_good_kbps),
        )

    state.streamlink_healthy_consec_count = 0
    state.streamlink_beeped_this_healthy_streak = False
    streamlink_had_stall_incident = live_stall_incident_active(state)

    while True:
        ret = proc.poll()

        if not state.streamlink_download_started and os.path.exists(chunk_path):
            try:
                current_size = os.path.getsize(chunk_path)
            except OSError:
                current_size = 0

            if current_size > LIVE_HARD_STALL_ZERO_BYTES:
                state.streamlink_download_started = True
                download_start_ts = time.time()
                state.stats["last_run_start"] = download_start_ts
                growth_tracker = start_live_file_growth_tracker(chunk_path)
                streamlink_last_health_log_t = time.monotonic()
                log("Streamlink output started → actual download stage started.")
                if notify:
                    notify("live_source_recovery")

        if ret is not None:
            break

        if recording_deadline_reached(state):
            status = "duration_reached"
            reason = "duration reached"
            log(f"Max duration reached {format_current_duration_for_log(state)} → terminating Streamlink...")
            stop_streamlink_process_tree()
            break

        if state.stop_flag:
            reason = "external stop"
            log("stop_flag detected → terminating Streamlink...")
            stop_streamlink_process_tree()
            break

        if download_start_ts is not None and not ran_30s and (time.time() - download_start_ts) >= 30:
            ran_30s = True

        if state.streamlink_download_started and growth_tracker is not None:
            growth_check = poll_live_file_growth(chunk_path, growth_tracker)

            if growth_check is not None:
                live_hard_required = LIVE_HARD_STALL_REQUIRED
                live_soft_required = LIVE_SOFT_STALL_REQUIRED
                is_double_vpn_capped = False
                guard = getattr(state, "network_guard", None)
                if guard and hasattr(guard, "is_wireproxy_nested_in_vpn"):
                    is_nested, _ = guard.is_wireproxy_nested_in_vpn()
                    if is_nested and (live_hard_required > 4 or live_soft_required > 4):
                        is_double_vpn_capped = True
                        live_hard_required = min(4, live_hard_required)
                        live_soft_required = min(4, live_soft_required)

                growth_kbps = growth_check["growth_kbps"]
                soft_trigger = False

                if growth_check["is_hard"]:
                    log(
                        f"HARD_STALL_CHECK "
                        f"{min(growth_check['hard_count'], live_hard_required)}/"
                        f"{live_hard_required} "
                        f"growth_bytes={growth_check['growth_bytes']}"
                    )
                    if not is_alarm_active(state):
                        beep_bad(state)
                else:
                    if streamlink_expected_kbps is None:
                        streamlink_expected_kbps = growth_kbps
                        state.streamlink_selected_bitrate_kbps_max = int(growth_kbps)
                        log(
                            f"Streamlink: Selected bitrate baseline {int(growth_kbps)} Kbps "
                            "(source=growth)"
                        )
                    elif growth_kbps > state.streamlink_selected_bitrate_kbps_max:
                        state.streamlink_selected_bitrate_kbps_max = int(growth_kbps)
                        streamlink_expected_kbps = growth_kbps

                    soft_check = classify_live_soft_stall(
                        growth_tracker,
                        growth_kbps,
                        streamlink_expected_kbps,
                        live_soft_required,
                    )
                    soft_trigger = soft_check["soft_trigger"]

                    if soft_check["is_soft"]:
                        threshold_kbps = soft_check["threshold_kbps"]
                        log(
                            f"SOFT_STALL_CHECK "
                            f"{min(soft_check['soft_count'], live_soft_required)}/"
                            f"{live_soft_required} "
                            f"growth_kbps={int(growth_kbps)} "
                            f"threshold_kbps={int(threshold_kbps)} "
                            f"expected_kbps={int(streamlink_expected_kbps)}"
                        )
                        if not is_alarm_active(state):
                            beep_bad(state)
                    else:
                        threshold_kbps = soft_check["threshold_kbps"]
                        if notify:
                            notify("live_stall_recovery")
                        maybe_trigger_good_beep(
                            state,
                            "streamlink",
                            growth_check["has_file"],
                            streamlink_had_stall_incident,
                            notify,
                        )
                        if threshold_kbps is not None:
                            now_health_log_t = time.monotonic()
                            if (now_health_log_t - streamlink_last_health_log_t) >= LIVE_HEALTH_LOG_INTERVAL:
                                growth_kib_s = growth_kbps * 1000.0 / 8.0 / 1024.0

                                if growth_kib_s >= 1024.0:
                                    growth_streamlink = f"{growth_kib_s / 1024.0:.2f} MiB/s"
                                else:
                                    growth_streamlink = f"{growth_kib_s:.1f} KiB/s"

                                log(
                                    f"Streamlink: Good speed {int(growth_kbps)} Kbps "
                                    f"[{growth_streamlink}] "
                                    f"(threshold {int(threshold_kbps)})",
                                    activity_id=new_terminal_activity("streamlink_health"),
                                )
                                streamlink_last_health_log_t = now_health_log_t

                hard_trigger = growth_check["hard_count"] >= live_hard_required
                if hard_trigger or soft_trigger:
                    stall_type = "hard" if hard_trigger else "soft"
                    log(f"CLASSIFY STALL_{stall_type.upper()}", level="WARN")
                    reset_good_beep_state(state, "streamlink")

                    # Network Guard: In-flight attribution for nested double-VPN stall
                    guard = getattr(state, "network_guard", None)
                    if guard and hasattr(guard, "is_wireproxy_nested_in_vpn"):
                        is_nested, vpn_name = guard.is_wireproxy_nested_in_vpn()
                        if is_nested:
                            if is_double_vpn_capped:
                                effective_trigger = live_soft_required if soft_trigger else live_hard_required
                                normal_trigger = LIVE_SOFT_STALL_REQUIRED if soft_trigger else LIVE_HARD_STALL_REQUIRED
                                log(
                                    f"NETWORK_GUARD: Active double-VPN ({vpn_name}) detected during stall! "
                                    f"Smart stall cap enforced: ending chunk early at check {effective_trigger} (normal {normal_trigger}) "
                                    f"to seal video bytes safely and hand over routing for next chunk.",
                                    level="WARN",
                                )
                            else:
                                log(
                                    f"NETWORK_GUARD: Stream stalled while Wireproxy was nested inside {vpn_name}! "
                                    f"Attributing stall to nested tunnel packet loss / MTU conflict.",
                                    level="WARN",
                                )

                    status = "stalled"
                    reason = f"STALL_{stall_type.upper()}"
                    stop_streamlink_process_tree(force_tree=True)
                    break

        time.sleep(0.5)

    if proc.poll() is None:
        stop_streamlink_process_tree()

    try:
        output_thread.join(timeout=3)
    except threading.ThreadError:
        pass

    returncode = proc.poll()
    raw_external_end(raw_invocation, returncode=returncode)

    # Any unrequested natural Streamlink exit is treated as source unavailable.
    # The boss accepts a valid final chunk first. YouTube sources are then re-probed
    # for post_live/was_live; non-YouTube sources follow the normal OFF/retry policy.
    if status == "ended" and reason == "Streamlink exited" and not state.stop_flag:
        status = "no_stream"
        reason = "Streamlink source unavailable"
        log("CLASSIFY SOURCE_UNAVAILABLE", level="WARN")

    result_chunk_path = chunk_path if os.path.exists(chunk_path) else None
    result_chunk_name = chunk_name if result_chunk_path else None

    log(f"Streamlink stopped for this run (exit code {returncode})")

    return EngineResult(
        status=status,
        reason=reason if reason != "Streamlink exited" else f"exit code {returncode}",
        chunk_path=result_chunk_path,
        chunk_name=result_chunk_name,
        metrics={
            "exit_code": returncode,
            "download_started": state.streamlink_download_started,
            "waited_for_video": state.streamlink_wait_for_video,
            "live_status": state.streamlink_live_status,
            "stall_type": stall_type,
            "ran_30s": ran_30s,
        },
    )


# ==============================================================================
# Orchestrator (chunk naming, acceptance, finalization, scheduling, main loop)
# ==============================================================================

def next_chunk_stem(state: RecorderState):
    """Return the recorder-owned stem for the next chunk WITHOUT incrementing."""
    ensure_chunks_dir()
    return f"chunk_{state.chunk_index + 1:03d}"


def next_chunk_names(state: RecorderState):
    """Return the existing TS name/path used by FFmpeg and N_m3u8DL-RE."""
    stem = next_chunk_stem(state)
    name = f"{stem}.ts"
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

    run_start = state.stats.get("last_run_start")
    run_wall = max(0.001, time.time() - float(run_start)) if run_start else None
    is_replay_burst = (
        DOWNLOAD_MODE != ENGINE_NM3U8DL
        and state.stats.get("good_chunks", 0) > 0
        and run_wall is not None
        and run_wall < STABILITY_WALL_SEC
        and dur < DUP_MAX_SEC
        and (dur / run_wall) > BURST_RATIO
    )
    if is_replay_burst:
        log(f"{context}: {chunk_name} REPLAY_BURST ({dur:.0f}s media in {run_wall:.1f}s wall, {dur/run_wall:.1f}x speed) → treating as BAD", level="WARN")

    is_intentional_boundary = bool(
        getattr(state, "stop_flag", False)
        or getattr(state, "manual_stop_requested", False)
        or getattr(state, "normal_terminal_reason", None) in ("duration_reached", "live_stream_ended")
        or getattr(state, "nm3u8dl_renewal_rollover_requested", False)
        or recording_deadline_reached(state)
    )
    effective_min_dur = (
        STABILITY_SEC
        if (state.stats.get("good_chunks", 0) == 0 or is_intentional_boundary)
        else MIN_ACCEPTED_CHUNK_SECONDS
    )

    probed_video_params = None
    is_short_downgrade = False
    if (
        dur < MIN_ACCEPTED_CHUNK_SECONDS
        and getattr(state, "baseline_video_params", None) is not None
        and dur > effective_min_dur
    ):
        probed_video_params = get_video_params(chunk_path)
        probe_w, probe_h, probe_fps = probed_video_params
        base_w, base_h, base_fps = state.baseline_video_params
        if (probe_w < base_w) or (probe_h < base_h) or (probe_fps < (base_fps - 5.0)):
            is_short_downgrade = True
            log(
                f"{context}: {chunk_name} REJECTED ({dur:.1f}s < {MIN_ACCEPTED_CHUNK_SECONDS}s "
                f"with downgraded quality {probe_w}x{probe_h}@{probe_fps}fps below baseline {base_w}x{base_h}@{base_fps}fps) → treating as BAD",
                level="WARN",
            )

    if dur > effective_min_dur and bitrate_ok and not is_replay_burst and not is_short_downgrade:
        # Update best resolution/fps from this chunk and track consistency
        w, h, fps = probed_video_params if probed_video_params is not None else get_video_params(chunk_path)
        log(f"{context}: {chunk_name} params {w}x{h}@{fps}fps")

        # Item 6: Quality Downgrade Alarm & Baseline Tracking
        if state.baseline_video_params is None:
            state.baseline_video_params = (w, h, fps)
        else:
            base_w, base_h, base_fps = state.baseline_video_params
            is_downgrade = (w < base_w) or (h < base_h) or (fps < (base_fps - 5.0))
            is_upgrade = (h > base_h) or (h == base_h and w > base_w) or (h == base_h and w == base_w and fps > (base_fps + 5.0))

            if is_upgrade:
                # Stream upgraded above initial baseline (e.g. 720p -> 1080p50); lock new high-water baseline
                state.baseline_video_params = (w, h, fps)
                if state.quality_downgrade_alarm_active or state.acknowledged_quality_params is not None:
                    quality_downgrade_alarm_stop(state, reason="upgraded")
                    state.acknowledged_quality_params = None
                    state.last_downgraded_quality_params = None
                    log_good_beep("quality_upgraded")
                    beep_good(state)
            elif is_downgrade:
                if state.stats.get("good_chunks", 0) > 0:
                    state.same_params = False
                downgrade_msg = f"Quality downgraded: {w}x{h}@{fps}fps (baseline {base_w}x{base_h}@{base_fps}fps)"
                state.mixed_reason = downgrade_msg
                state.last_downgraded_quality_params = (w, h, fps)
                if state.acknowledged_quality_params != (w, h, fps):
                    log(f"[ALARM] QUALITY DOWNGRADE DETECTED: {downgrade_msg} (Press 'A' to ACK)", level="WARN")
                    quality_downgrade_alarm_start(state)
            else:
                if state.quality_downgrade_alarm_active or state.acknowledged_quality_params is not None:
                    quality_downgrade_alarm_stop(state, reason="restored")
                    state.acknowledged_quality_params = None
                    state.last_downgraded_quality_params = None
                    log_good_beep("quality_restored")
                    beep_good(state)
                    log(f"Broadcast quality restored to baseline {base_w}x{base_h}@{base_fps}fps")

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

        # Reset network guard rate-limit strike on good chunk
        if getattr(state, "network_guard", None):
            state.network_guard.rate_limit_strike = 0

        # Update ffmpeg baseline ONLY after accepting this chunk as GOOD
        state.stats["last_good_br_kbps"] = br

        # Media duration from file
        dur_media = dur

        # Wall-clock duration of this FFmpeg run
        run_start = state.stats.get("last_run_start") or time.time()
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


        # FFmpeg, N_m3u8DL-RE, and Streamlink keep their existing live-built concat list.
        # yt-dlp keeps only accepted native chunks during recording; its MKV
        # normalization, separators, and concat list are created at finalization.
        if DOWNLOAD_MODE != ENGINE_YTDLP:
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


def apply_live_stall_policy(state: RecorderState, result: EngineResult) -> bool:
    """Shared repeated-live-failure policy for FFmpeg, yt-dlp, and Streamlink."""
    unexpected_process_exit = bool(result.metrics.get("unexpected_process_exit"))

    if result.status != "stalled" and not unexpected_process_exit:
        return False

    if unexpected_process_exit:
        stall_type = "process_exit"
    else:
        stall_type = result.metrics.get("stall_type") or (
            "hard" if "HARD" in result.reason else "soft"
        )

    state.live_stall_run_count += 1
    log(
        f"STALL_RUN_STOP type={stall_type} "
        f"stall_run_count {state.live_stall_run_count}/{LIVE_STALL_MAX_ATTEMPTS}",
        level="WARN",
    )

    if state.live_stall_run_count == 2:
        if not state.live_stall_alarm_active and not state.live_stall_post_ack_silent:
            log("ALARM_STALL_START press A to acknowledge", level="WARN")
            live_stall_alarm_start(state)

    if state.live_stall_run_count >= LIVE_STALL_MAX_ATTEMPTS:
        if not state.live_stall_max_attempts_exhausted:
            log("EXIT reason=live_stall_max_attempts_exhausted", level="ERROR")
            state.live_stall_post_ack_silent = False
            state.live_stall_max_attempts_exhausted = True
            alarm_critical(state)
            state.alarm_linger_until = time.time() + ALARM_LINGER_SEC
            state.stop_flag = True
        return True

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

    if result.status == "duration_reached":
        log_good_beep("recording_end_ok")
        beep_good(state)
        log(f"Max duration reached {format_current_duration_for_log(state)} → stopping loop.")
        state.normal_terminal_reason = "duration_reached"
        state.stop_flag = True
        #return backoff_sec # # DO NOT return yet — we still want to accept the last chunk and write concat_list.txt

    if result.metrics and (
        (engine.name == ENGINE_NM3U8DL and result.metrics.get("live_stream_ended"))
        or (
            engine.name in (ENGINE_YTDLP, ENGINE_STREAMLINK)
            and result.metrics.get("live_status") in ("post_live", "was_live")
        )
    ):
        state.normal_terminal_reason = "live_stream_ended"

    ok = False
    if result.chunk_path and result.chunk_name:
        if engine.name == ENGINE_NM3U8DL:
            time.sleep(1)  # allow mux flush
        ok = maybe_accept_chunk(
            state,
            result.chunk_path,
            result.chunk_name,
            context="Run end"
        )
        if ok and engine.name in (ENGINE_FFMPEG, ENGINE_YTDLP, ENGINE_STREAMLINK):
            reset_live_off_state(state, "good_chunk")

    # Duration reached is a normal terminal exit.
    # The final chunk has now been accepted, so do not classify it as a recovery failure.
    if result.status == "duration_reached":
        return backoff_sec

    # Universal Internet Outage Protection ([INTERNET_HOLD] across all streams)
    if getattr(state, "network_guard", None) and not state.stop_flag:
        if result.status == "internet_drop" or getattr(state, "internet_drop_active", False):
            log("NETWORK_GUARD: Outbound internet connectivity lost! Diverting to Internet Hold...", level="WARN")
            res = state.network_guard.handle_internet_hold_state(state, is_startup=False)
            if not res:
                state.stop_flag = True
            return 0.0

    # Network Guardrail & Rate-Limit Circuit Breaker intercept
    if getattr(state, "network_guard_triggered", False) or result.status == "vpn_drop":
        state.network_guard_triggered = False
        if getattr(state, "network_guard", None):
            res = state.network_guard.handle_vpn_hold_state(state, is_startup=False)
            if not res:
                state.stop_flag = True
        return 0.0

    if getattr(state, "rate_limit_429_detected", False) or "429" in str(result.reason or "") or (result.metrics and result.metrics.get("rate_limit_429", False)):
        state.rate_limit_429_detected = False
        if getattr(state, "network_guard", None):
            res = state.network_guard.handle_rate_limit_cooldown(state)
            if not res:
                state.stop_flag = True
        return 0.0

    # Defensive Guardrail: Intercept unexpected engine exit or 403 geo-block
    if getattr(state, "network_guard", None) and state.network_guard.is_armed and not state.stop_flag:
        # 1. Route Severance / VPN Drop takes precedence (Section 11)
        if not state.network_guard.check_runtime_route() or result.status == "vpn_drop":
            log("NETWORK_GUARD: Egress dropped off VPN! Diverting to Hold state...", level="WARN")
            res = state.network_guard.handle_vpn_hold_state(state, is_startup=False)
            if not res:
                state.stop_flag = True
            return 0.0

        # 2. Burned IP Block: Egress route verified healthy, but CDN returns 403 (Section 13)
        has_403 = (
            getattr(state.network_guard, "has_403_error", False)
            or "403" in str(result.reason or "")
            or (result.metrics and result.metrics.get("geo_block_403", False))
        )
        if has_403:
            state.network_guard.has_403_error = False
            is_startup = (
                state.network_guard.is_startup_phase()
                if hasattr(state.network_guard, "is_startup_phase")
                else (state.stats.get("good_chunks", 0) == 0)
            )
            res = state.network_guard.handle_403_recovery(state, is_startup=is_startup)
            if not res:
                state.stop_flag = True
            return 0.0

    if (
        engine.name == ENGINE_NM3U8DL
        and result.metrics.get("live_stream_ended")
    ):
        log(
            "N_m3u8DL explicit live-end confirmed → "
            "stopping overall recorder after final chunk."
        )
        state.stop_flag = True

    if (
        engine.name in (ENGINE_YTDLP, ENGINE_STREAMLINK)
        and result.metrics.get("live_status") in ("post_live", "was_live")
    ):
        reset_live_off_state(state, "youtube_live_ended")
        log(
            f"YouTube {result.metrics.get('live_status')} confirmed → "
            "stopping overall recorder."
        )
        state.stop_flag = True
        return backoff_sec
        
    if engine.name in (ENGINE_FFMPEG, ENGINE_YTDLP, ENGINE_STREAMLINK):
        if apply_live_stall_policy(state, result):
            return backoff_sec

    if engine.name in (ENGINE_FFMPEG, ENGINE_YTDLP, ENGINE_STREAMLINK) and result.status == "no_stream":
        now = time.time()
        if state.live_off_timer_start is None:
            state.live_off_timer_start = now
        off_duration = int(now - state.live_off_timer_start)
        log(f"OFF_TIMER={off_duration}s")
        if off_duration >= LIVE_OFF_ALARM_THRESHOLD_SEC:
            if not state.live_off_alarm_active and not state.live_off_post_ack_silent:
                live_off_alarm_start(state)

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
            log(
                f"MANIFEST_DEAD mode={mode} context={context} "
                f"attempt {state.manifest_fail_count}/{max_attempts}"
            )

            if state.manifest_fail_count >= max_attempts:
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
            log(
                f"NO_FILE_APPEAR deadline={deadline_s}s mode={mode} "
                f"context={context} "
                f"attempt {state.no_file_appear_count}/{max_attempts}"
            )

            if state.no_file_appear_count >= max_attempts:
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
            log(
                f"STALL_RUN_STOP event_count "
                f"{event_count}/{NM3U8DL_STALL_MAX_EVENTS}",
                level="WARN",
            )

            if event_count >= NM3U8DL_STALL_MAX_EVENTS:
                nm3u8dl_critical_stop(
                    state,
                    "EXIT reason=nm3u8dl_stall_max_events_exhausted",
                )

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
            log(
                f"ENGINE_CRASH attempt "
                f"{state.nm3u8dl_crash_count}/{NM3U8DL_CRASH_MAX_ATTEMPTS} "
                f"reason={result.reason}",
                level="WARN",
            )

            if state.nm3u8dl_crash_count >= NM3U8DL_CRASH_MAX_ATTEMPTS:
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
            log(
                f"CLASSIFY ENGINE_CRASH reason=unclassified_{result.status} "
                f"attempt {state.nm3u8dl_crash_count}/{NM3U8DL_CRASH_MAX_ATTEMPTS}",
                level="WARN",
            )
            if state.nm3u8dl_crash_count >= NM3U8DL_CRASH_MAX_ATTEMPTS:
                nm3u8dl_critical_stop(
                    state,
                    "ENGINE_CRASH attempts exhausted → EXIT + ALARM_CRITICAL",
                )

        backoff_sec = DEFAULT_MIN_BACKOFF
    else:
        if result.status != "no_stream":
            backoff_sec = DEFAULT_MIN_BACKOFF if ok else min(backoff_sec + BACKOFF_STEP_SEC, BACKOFF_MAX_SEC)

    is_corrupt_recycle = (
        result.status == "stream_corrupt_recycle"
        or "stream corruption" in str(result.reason or "").lower()
    )
    chunk_dur = 0.0
    if ok and state.stats.get("sessions"):
        chunk_dur = float(state.stats["sessions"][-1].get("dur_media", 0.0))

    if result.status == "stalled" or (is_corrupt_recycle and ok) or (ok and chunk_dur >= HEALTHY_CHUNK_RESET_SEC):
        backoff_sec = 0

    if state.stop_flag:
        return backoff_sec

    wait_for = STREAM_OFF_SLEEP if result.status == "no_stream" else backoff_sec
    if wait_for is None:
        wait_for = backoff_sec

    if wait_for > 0:
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
    Write summary log next to FINAL_FILE.
    """
    try:
        summary_path = os.path.splitext(FINAL_FILE)[0] + "_log.log"
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
            if not getattr(state, "same_params", True):
                reason = getattr(state, "mixed_reason", "") or "stream parameters differ across chunks"
                lines.append(f"Recording summary (NOT finalized - mixed params: {reason}) for {FINAL_FILE}")
            else:
                failure_msg = getattr(state, "last_finalization_failure", "") or "assembly failed"
                lines.append(f"Recording summary (NOT finalized - {failure_msg}) for {FINAL_FILE}")
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

        corrupt_fatal = getattr(state, "ffmpeg_stream_corrupt_incidents", 0)
        corrupt_nonfatal = getattr(state, "ffmpeg_stream_corrupt_nonfatal_incidents", 0)
        if (corrupt_fatal + corrupt_nonfatal) > 0:
            lines.append("Stream Integrity")
            lines.append("----------------")
            lines.append(f"Corruption incidents (fatal)    : {corrupt_fatal}")
            lines.append(f"Corruption incidents (non-fatal): {corrupt_nonfatal}")
            lines.append("")

        if getattr(state, "network_guard", None):
            lines.extend(state.network_guard.format_summary_lines())
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
    # Stop wireproxy child process before anything else to free ports and unlock temporary files
    if getattr(state, "wireproxy_manager", None):
        state.wireproxy_manager.stop()

    # A normal waiting-only run may end by duration/Ctrl+C without ever
    # capturing a GOOD chunk. There is simply nothing to finalize.
    if DOWNLOAD_MODE in (ENGINE_YTDLP, ENGINE_STREAMLINK) and not state.stats.get("sessions"):
        log("No GOOD chunks captured → nothing to finalize.")
        state.stats["process_end"] = time.time()
        return

    # yt-dlp records native containers during capture. Only after recording has
    # fully ended do we losslessly prepare MKV working chunks for shared concat.
    if DOWNLOAD_MODE == ENGINE_YTDLP and state.same_params:
        if not prepare_ytdlp_finalization_inputs(state):
            log(
                "yt-dlp automatic finalization could not be completed → "
                "leaving native/working chunks for manual handling.",
                level="ERROR",
            )
            state.last_finalization_failure = "yt-dlp input preparation failed"
            state.stats["process_end"] = time.time()
            sessions = state.stats.get("sessions", [])
            total_media = sum(s.get("dur_media", 0.0) for s in sessions)
            est_final_dur = total_media + BLACK_LEN * max(0, len(sessions) - 1)
            write_summary_log(state, est_final_dur, finalized=False)
            return

    # If no chunks were captured (e.g. startup abort, dead manifest, 403), skip finalization immediately.
    if not LIST_FILE or not os.path.exists(LIST_FILE):
        missing_name = LIST_FILE or "concat_list.txt"
        log(f"No concat list file → {missing_name} missing → no chunks to concat. Skipping finalize.")
        return

    if FINAL_FILE and os.path.exists(FINAL_FILE):
        log(f"WARNING: {FINAL_FILE} already exists. Overwriting.")
        send2trash(FINAL_FILE)

    # Fallback if somehow nothing was set (should not happen if any GOOD chunk)
    if state.best_h == 0 or state.best_fps == 0:
        state.best_w, state.best_h, state.best_fps = 1920, 1080, 50
        log(f"No best params recorded, falling back to {state.best_w}x{state.best_h}@{state.best_fps}fps")

    # Decide whether we can avoid re-encoding
    if state.same_params:
        def _build_final_cmd(tier: int) -> list:
            base = [
                "ffmpeg", "-y",
                "-err_detect", "ignore_err",
                "-fflags", "+genpts+igndts+discardcorrupt",
                "-f", "concat",
                "-safe", "0",
                "-nostats",
                "-progress", "pipe:2",
                "-stats_period", str(FFMPEG_PROGRESS_PERIOD),
                "-i", os.path.basename(LIST_FILE),
                "-map", "0:v",
                "-map", "0:a?",
            ]
            if tier == 1:
                return base + [
                    "-c", "copy",
                    "-avoid_negative_ts", "make_zero",
                    "-max_interleave_delta", "0",
                    os.path.join("..", FINAL_FILE),
                ]
            base += [
                "-c:v", "copy",
                "-bsf:v", "setts=ts='if(eq(PTS,NOPTS),PREV_OUTPTS+1,PTS)':dts='if(eq(DTS,NOPTS),PREV_OUTDTS+1,DTS)'",
                "-c:a", "aac",
                "-af", "aresample=async=1:first_pts=0",
                "-avoid_negative_ts", "make_zero",
                "-max_interleave_delta", "0",
            ]
            audio_streams = [s for s in (state.stream_layout_ref or [])
                             if s.get("codec_type") == "audio"] or list(state.audio_layout_ref or [])
            for idx, meta in enumerate(audio_streams):
                channels = int(meta.get("channels") or 0)
                base += [f"-b:a:{idx}", f"{max(FINAL_AUDIO_MIN_KBPS, FINAL_AUDIO_KBPS_PER_CHANNEL * channels)}k"]
                if meta.get("sample_rate"):
                    base += [f"-ar:a:{idx}", str(meta["sample_rate"])]
                if channels:
                    base += [f"-ac:a:{idx}", str(channels)]
            if not audio_streams:
                base += ["-b:a", f"{FINAL_AUDIO_MIN_KBPS}k"]
            return base + [os.path.join("..", FINAL_FILE)]

        def _validate_final_output() -> tuple[bool, str, bool]:
            if not os.path.exists(FINAL_FILE) or os.path.getsize(FINAL_FILE) <= 0:
                return False, "missing or 0-byte output file", False
            dur, _ = get_file_info(FINAL_FILE)
            if dur <= 0:
                return False, "invalid media duration (0s)", False
            sessions = state.stats.get("sessions", [])
            expected = sum(s.get("dur_media", 0.0) for s in sessions) + BLACK_LEN * max(0, len(sessions) - 1)
            if expected > 0 and dur < (expected * FINAL_MIN_DURATION_RATIO):
                return False, f"duration truncated ({dur:.1f}s vs expected {expected:.1f}s)", True
            return True, "", False

        rc = 1
        last_failure_reason = ""
        for tier in (1, 2):
            tier_desc = "Tier 1 (lossless fast copy)" if tier == 1 else "Tier 2 fallback (lossless video + audio resample/re-encode)"
            log(f"FINALIZE {tier_desc} → {FINAL_FILE}")
            rc = run_ffmpeg_with_capture(_build_final_cmd(tier), CHUNKS_DIR, tag=f"FFmpeg-FINAL-T{tier}")
            ok, why, is_truncated = _validate_final_output() if rc == 0 else (False, f"ffmpeg exit code {rc}", False)
            last_failure_reason = why
            if rc == 0 and ok:
                break

            log(f"FINALIZE Tier {tier} failed: {why}", level="WARN")
            if os.path.exists(FINAL_FILE):
                try:
                    os.remove(FINAL_FILE)
                except OSError as e:
                    log(f"Could not remove failed {FINAL_FILE}: {e}", level="WARN")
            rc = rc or 1

        if rc != 0:
            if os.path.exists(FINAL_FILE):
                try:
                    os.remove(FINAL_FILE)
                except OSError as e:
                    log(f"Could not remove failed {FINAL_FILE}: {e}", level="WARN")
            err_suffix = f" (last failure: {last_failure_reason})" if last_failure_reason else ""
            log(f"FINALIZATION FAILED (both tiers{err_suffix}) → FFmpeg could not create validated final MKV; keeping chunks folder for inspection.", level="ERROR")
            state.last_finalization_failure = last_failure_reason or "ffmpeg concat failure"
            state.stats["process_end"] = time.time()
            black_len = BLACK_LEN
            sessions = state.stats.get("sessions", [])
            total_media = sum(s.get("dur_media", 0.0) for s in sessions)
            est_final_dur = total_media + black_len * max(0, len(sessions) - 1)
            write_summary_log(state, est_final_dur, finalized=False)
            return
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

    final_size = os.path.getsize(FINAL_FILE)
    state.stats["process_end"] = time.time()
    dur, _ = get_file_info(FINAL_FILE)

    log(
        f"FINAL OK → {FINAL_FILE}, duration {dur:.1f}s, size {final_size/1024/1024:.1f}MB"
    )

    # Write summary log next to FINAL_FILE
    write_summary_log(state, dur, finalized=True)
    
    # All checks passed → safe to delete chunks folder
    try:
        log(f"Sent chunks folder {CHUNKS_DIR} to Recycle Bin")
        send2trash(CHUNKS_DIR)       # instead of shutil.rmtree(CHUNKS_DIR)
    except Exception as e:
        log(f"Could not delete chunks folder {CHUNKS_DIR}: {e}")

def get_planned_command(selected_engine: RecorderEngine, state: RecorderState) -> Optional[str]:
    """Return the formatted command that will be launched for chunk 1, for display in scheduled mode header."""
    try:
        name = selected_engine.name
        guard = getattr(state, "network_guard", None)
        use_wp = (guard and guard.vpn_mode == "WIREPROXY") or is_wireproxy_active(state)

        if name == ENGINE_NM3U8DL:
            planned_chunk_name = f"chunk_{state.chunk_index + 1:03d}.ts"
            planned_save_name = planned_chunk_name.replace(".ts", "")
            planned_cmd = f'{NM3U8DL_PART_A} --save-name "{planned_save_name}" {NM3U8DL_PART_B}'
            planned_cmd = planned_cmd.replace("--use-shaka-packager", "--decryption-engine SHAKA_PACKAGER")
            if use_wp and "--custom-proxy" not in planned_cmd:
                planned_cmd = f"{planned_cmd} --custom-proxy http://127.0.0.1:{WIREPROXY_HTTP_PORT}"
            return planned_cmd

        if name == ENGINE_FFMPEG:
            chunk_path, _ = next_chunk_names(state)
            proxy_url = f"http://127.0.0.1:{WIREPROXY_HTTP_PORT}" if use_wp else None
            if FFMPEG_PART_A.strip():
                advanced_cmd_text = FFMPEG_PART_A.strip()
                if proxy_url and not re.search(r'(?<!\S)-http_proxy(?=\s|$)', advanced_cmd_text):
                    input_match = re.search(r'(?<!\S)-i(?=\s)', advanced_cmd_text)
                    proxy_opt = f"-http_proxy {proxy_url} "
                    if input_match:
                        advanced_cmd_text = (
                            advanced_cmd_text[:input_match.start()]
                            + proxy_opt
                            + advanced_cmd_text[input_match.start():]
                        )
                    else:
                        advanced_cmd_text = proxy_opt + advanced_cmd_text

                if re.search(r'https?://', advanced_cmd_text, re.IGNORECASE):
                    input_match = re.search(r'(?<!\S)-i(?=\s)', advanced_cmd_text)
                    if input_match:
                        reconnect_defaults = (
                            ("-rw_timeout", "15000000"),
                            ("-reconnect", "1"),
                            ("-reconnect_streamed", "1"),
                            ("-reconnect_on_network_error", "1"),
                            ("-reconnect_delay_max", "10"),
                            ("-reconnect_delay_total_max", "30"),
                        )
                        missing_parts = []
                        for option, value in reconnect_defaults:
                            if not re.search(rf'(?<!\S){re.escape(option)}(?=\s|$)', advanced_cmd_text):
                                missing_parts.extend((option, value))
                        if missing_parts:
                            reconnect_text = " ".join(missing_parts) + " "
                            advanced_cmd_text = (
                                advanced_cmd_text[:input_match.start()]
                                + reconnect_text
                                + advanced_cmd_text[input_match.start():]
                            )
                return (
                    f'{advanced_cmd_text} '
                    f'-y -nostats -progress pipe:2 -stats_period {FFMPEG_PROGRESS_PERIOD} '
                    f'"{chunk_path}"'
                )
            else:
                proxy_opts = ["-http_proxy", proxy_url] if proxy_url else []
                cmd = [
                    "ffmpeg",
                    "-y",
                    "-loglevel", "info",
                    "-hide_banner",
                    *proxy_opts,
                    "-rw_timeout", "15000000",
                    "-reconnect", "1",
                    "-reconnect_streamed", "1",
                    "-reconnect_at_eof", "1",
                    "-reconnect_delay_max", "10",
                    "-nostats",
                    "-progress", "pipe:2",
                    "-stats_period", str(FFMPEG_PROGRESS_PERIOD),
                    "-i", LIVE_URL,
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
                return shlex.join(cmd)

        if name == ENGINE_YTDLP:
            if not YTDLP_PART_A.strip():
                return None
            chunk_stem = next_chunk_stem(state)
            output_template = os.path.join(CHUNKS_DIR, f"{chunk_stem}.%(ext)s")
            before_dl_marker = os.path.join(CHUNKS_DIR, f".{chunk_stem}_before_dl.marker")
            wait_for_video_arg = (
                f'--wait-for-video {YTDLP_WAIT_FOR_VIDEO_SEC} '
                if getattr(state, "ytdlp_wait_for_video", False)
                else ''
            )
            proxy_arg = ""
            ffmpeg_proxy_arg = ""
            if use_wp:
                if "--proxy" not in YTDLP_PART_A:
                    proxy_arg = f"--proxy socks5://127.0.0.1:{WIREPROXY_SOCKS5_PORT} "
                ffmpeg_proxy_arg = f" -http_proxy http://127.0.0.1:{WIREPROXY_HTTP_PORT}"

            return (
                f'{YTDLP_PART_A.strip()} '
                f'{proxy_arg}'
                f'--no-live-from-start '
                f'{wait_for_video_arg}'
                f'--print-to-file "before_dl:RECORDER_BEFORE_DL TOP=%(tbr)s F0=%(requested_formats.0.tbr)s F1=%(requested_formats.1.tbr)s" "{before_dl_marker}" '
                f'-f "bv*+ba/b" '
                f'--downloader ffmpeg '
                f'--downloader-args "ffmpeg_i:-rw_timeout 15000000 -reconnect 1 -reconnect_streamed 1 -reconnect_on_network_error 1 -reconnect_delay_max 10 -reconnect_delay_total_max 30{ffmpeg_proxy_arg}" '
                f'--downloader-args "ffmpeg_i2:-isync 0" '
                f'--hls-use-mpegts '
                f'--no-part '
                f'-o "{output_template}"'
            )

        if name == ENGINE_STREAMLINK:
            if not STREAMLINK_PART_A.strip():
                return None
            chunk_path, _ = next_chunk_names(state)
            wait_args = (
                f'--retry-streams {STREAMLINK_WAIT_FOR_STREAM_SEC} --retry-max 0 '
                if getattr(state, "streamlink_wait_for_video", False)
                else ''
            )
            proxy_arg = ""
            if use_wp and "--http-proxy" not in STREAMLINK_PART_A:
                proxy_arg = f"--http-proxy socks5://127.0.0.1:{WIREPROXY_SOCKS5_PORT} "

            return (
                f'{STREAMLINK_PART_A.strip()} '
                f'{proxy_arg}'
                f'{wait_args}'
                f'--progress=force '
                f'--output "{chunk_path}" '
                f'{STREAMLINK_STREAM}'
            )
    except Exception:
        return None
    return None


def get_host_platform_summary() -> str:
    """Return concise standard Hostname, OS, architecture, and Python runtime info."""
    import platform
    import socket
    try:
        host_name = socket.gethostname() or "unknown"
    except Exception:
        host_name = "unknown"
    arch = platform.machine() or "unknown"
    py_ver = platform.python_version()
    if os.name == "nt":
        win_ver = platform.version()
        return f"{host_name} | Windows {win_ver} ({arch}) | Python {py_ver}"
    elif sys.platform == "darwin":
        mac_ver = platform.mac_ver()[0] or "macOS"
        return f"{host_name} | macOS {mac_ver} (Darwin {arch}) | Python {py_ver}"
    else:
        system = platform.system()
        release = platform.release()
        return f"{host_name} | {system} {release} ({arch}) | Python {py_ver}"


def wait_until_start(state: RecorderState, selected_engine: RecorderEngine):
    """
    If SCHEDULE_START is set to a future time, wait until then.
    Format: "YYYY-MM-DD HH:MM" in local time.
    """
    mode = "scheduled" if SCHEDULE_START else "manual"
    target = None
    if SCHEDULE_START:
        try:
            target = datetime.strptime(SCHEDULE_START, "%Y-%m-%d %H:%M")
        except ValueError:
            log(f"Invalid SCHEDULE_START '{SCHEDULE_START}', starting immediately.")
            mode = "manual"

    # Unified startup session header
    log(f"RECORDING ENGINE  : {selected_engine.name}")
    log(f"Host Platform     : {get_host_platform_summary()}")
    log(f"Recording mode    : {mode}")
    log(f"Base name         : {BASE_NAME}")
    for line in selected_engine.summary_lines():
        log(line)

    if mode == "scheduled":
        planned_cmd = get_planned_command(selected_engine, state)
        if planned_cmd:
            log(f"Planned command   : {planned_cmd}")
        log(f"Schedule start    : {SCHEDULE_START}")

    ref_time = target if (mode == "scheduled" and target) else datetime.now()
    if RUN_DURATION_MIN is not None:
        end_time = ref_time + timedelta(minutes=RUN_DURATION_MIN)
        log(f"Planned duration  : {RUN_DURATION_MIN} minutes")
        log(f"Expected end time : {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        log("Planned duration  : until stopped")

    # Group networking guardrail and route verification contiguously
    if getattr(state, "network_guard", None):
        for g_line in state.network_guard.format_header_lines():
            log(g_line)
        guard_ok = state.network_guard.preflight_check()
        if not guard_ok:
            sys.exit(0 if getattr(state, "stop_flag", False) else 1)

    if mode != "scheduled" or target is None:
        return  # start immediately

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

    if getattr(state, "network_guard", None):
        guard_ok = state.network_guard.preflight_check(is_scheduled_trigger=True)
        if not guard_ok:
            sys.exit(0 if getattr(state, "stop_flag", False) else 1)
    

def ffmpeg_summary_lines():
    if FFMPEG_PART_A.strip():
        return ["FFmpeg source mode: advanced command configuration"]
    return [f"Live URL          : {LIVE_URL}"]


def nm3u8dl_summary_lines():
    return []


def ytdlp_summary_lines():
    if YTDLP_PART_A.strip():
        return ["yt-dlp config     : variable command material configured"]
    return ["yt-dlp config     : empty"]


def streamlink_summary_lines():
    if STREAMLINK_PART_A.strip():
        source_url = get_streamlink_source_url()
        source_line = (
            "Source handling    : YouTube metadata probe + Streamlink"
            if _is_youtube_url(source_url)
            else "Source handling    : Streamlink native"
        )
        return [
            "Streamlink config  : variable command material configured",
            source_line,
            f"Stream quality     : {STREAMLINK_STREAM}",
        ]
    return ["Streamlink config  : empty"]


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
            description="Live capture via FFmpeg",
            summary_lines_fn=ffmpeg_summary_lines,
        ),
        ENGINE_NM3U8DL: RecorderEngine(
            name=ENGINE_NM3U8DL,
            run_cycle=run_nm3u8dl_cycle,
            description="Segmented downloader via N_m3u8DL-RE",
            summary_lines_fn=nm3u8dl_summary_lines,
        ),
        ENGINE_YTDLP: RecorderEngine(
            name=ENGINE_YTDLP,
            run_cycle=run_ytdlp_cycle,
            description="Live capture via yt-dlp",
            summary_lines_fn=ytdlp_summary_lines,
        ),
        ENGINE_STREAMLINK: RecorderEngine(
            name=ENGINE_STREAMLINK,
            run_cycle=run_streamlink_cycle,
            description="Live capture via Streamlink",
            summary_lines_fn=streamlink_summary_lines,
        ),
    }


def resolve_engine(mode: str) -> Optional[RecorderEngine]:
    """Resolve a mode string to its engine descriptor, logging if unknown."""

    registry = build_engine_registry()
    engine = registry.get(mode)
    if not engine:
        log(f"Unknown download mode '{mode}' → exiting.")
        sys.exit(1)

    if mode == ENGINE_NM3U8DL and not (NM3U8DL_PART_A or "").strip():
        log("ERROR: DOWNLOAD_MODE is set to 'N_m3u8DL-RE', but NM3U8DL_PART_A is empty in user config!", level="ERROR")
        log("Please configure NM3U8DL_PART_A in recorder_static_user_config.py (e.g. NM3U8DL_PART_A = 'N_m3u8DL-RE \"<STREAM_URL>\"')", level="ERROR")
        sys.exit(1)
    if mode == ENGINE_FFMPEG and not (LIVE_URL or "").strip():
        log("ERROR: DOWNLOAD_MODE is set to 'ffmpeg', but LIVE_URL is empty in user config!", level="ERROR")
        sys.exit(1)
    if mode == ENGINE_YTDLP and not (YTDLP_PART_A or "").strip():
        log("ERROR: DOWNLOAD_MODE is set to 'yt-dlp', but YTDLP_PART_A is empty in user config!", level="ERROR")
        sys.exit(1)
    if mode == ENGINE_STREAMLINK and not (STREAMLINK_PART_A or "").strip():
        log("ERROR: DOWNLOAD_MODE is set to 'streamlink', but STREAMLINK_PART_A is empty in user config!", level="ERROR")
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
    ensure_global_key_listener(state)

    def notify(event: str):
        # Engines report events; boss decides what to do.
        if event.startswith("good_beep:"):
            reason = event.split(":", 1)[1]
            log_good_beep(reason)
            beep_good(state)
            start_runtime_controls_reminder(state)
            return
        if event == "live_source_recovery":
            reset_live_off_state(state, "data_started")
            return
        if event == "live_stall_recovery":
            # One healthy check confirms source recovery and resets/re-arms
            # the shared FFmpeg/yt-dlp/Streamlink OFF + stall policies.
            had_live_stall_incident = live_stall_incident_active(state)
            reset_live_off_state(state, "healthy_data")
            state.live_stall_run_count = 0
            state.live_stall_post_ack_silent = False
            state.live_stall_post_ack_silent_type = None
            state.live_stall_max_attempts_exhausted = False
            if state.live_stall_alarm_active:
                log("ALARM_STALL_STOP reason=recovery")
                live_stall_alarm_stop(state)
            if had_live_stall_incident:
                log("STALL_POLICY_RESET stall_run_count=0 reason=healthy_recovery")
        elif event == "stall":
            if not is_alarm_active(state):
                beep_bad(state)

    while True:
        # Global ACK (for shared alarm cases like CRITICAL / FFmpeg alarms)
        if getattr(state, "alarm_ack_requested", False) and not getattr(state, "nm3u8dl_alarm_active", False):
            alarm_type = getattr(state, "shared_alarm_type", None)
            if state.live_off_alarm_active or alarm_type == "live_off":
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
                live_off_alarm_stop(state)
                state.live_off_post_ack_silent = True
                state.alarm_ack_requested = False
                state.alarm_linger_until = None
            else:
                if state.live_stall_alarm_active or alarm_type == "live_stall":
                    acknowledge_live_stall_alarm(state)
                elif getattr(state, "quality_downgrade_alarm_active", False) or alarm_type == "quality_downgrade":
                    acknowledge_quality_downgrade_alarm(state)
                else:
                    log(
                        "ALARM_ACK",
                        activity_id=getattr(
                            state,
                            "shared_alarm_activity_id",
                            None,
                        ),
                    )
                    shared_alarm_stop(state)
                    state.alarm_ack_requested = False
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
            # Network Guardrail Pre-Cycle Gate: Never launch a chunk worker if VPN is down
            if getattr(state, "network_guard", None):
                if hasattr(state.network_guard, "inter_chunk_gate_check"):
                    # D3: start the silent startup window at the first launch attempt, not at guard construction.
                    if not getattr(state, "_startup_window_marked", False):
                        state._startup_window_marked = True
                        if hasattr(state.network_guard, "mark_startup_begin"):
                            state.network_guard.mark_startup_begin()
                    if not state.network_guard.inter_chunk_gate_check(state, temp_dir=CHUNKS_DIR):
                        break
                    if state.stop_flag:
                        break
                elif state.network_guard.is_armed:
                    if not state.network_guard.check_runtime_route():
                        # Brief 1.5s settling backoff to allow OS routing table convergence before entering Hold
                        time.sleep(1.5)
                        if not state.network_guard.check_runtime_route():
                            log("NETWORK_GUARD: VPN route not verified before chunk launch! Entering Hold state...", level="WARN")
                            if not state.network_guard.handle_vpn_hold_state(state, is_startup=False):
                                break
                            if state.stop_flag:
                                break
                else:
                    if not NetworkRouteResolver.is_internet_online(timeout=1.5):
                        # Brief 1.5s settling backoff to allow OS routing table convergence before entering Hold
                        time.sleep(1.5)
                        if not NetworkRouteResolver.is_internet_online(timeout=1.5):
                            log("NETWORK_GUARD: Outbound internet not reachable before chunk launch! Entering Internet Hold...", level="WARN")
                            if not state.network_guard.handle_internet_hold_state(state, is_startup=False):
                                break
                            if state.stop_flag:
                                break

            # Stage 3 source-aware pre-attempt availability:
            # - N_m3u8DL keeps its existing self-managed startup checks.
            # - simple FFmpeg uses the existing direct-media FFprobe check.
            # - advanced FFmpeg probes with its configured source/access command material.
            # - yt-dlp YouTube mode uses yt-dlp metadata/live_status; is_upcoming
            #   launches yt-dlp with --wait-for-video rather than being treated as OFF.
            # - Streamlink YouTube mode uses yt-dlp metadata/live_status before recording.
            # - Streamlink non-YouTube mode bypasses YouTube metadata and lets Streamlink
            #   resolve the source/protocol natively.
            if engine.name == ENGINE_NM3U8DL:
                result = engine.run_cycle(state, backoff_sec, notify, state.deadline_ts)
            else:
                source_ok, source_reason = probe_source_for_engine(state, engine)

                # Ctrl+C can arrive while a source probe is blocking. Do not
                # launch a recording engine after the user has already stopped.
                if state.stop_flag:
                    break

                if not source_ok:
                    result = EngineResult(
                        status="no_stream",
                        reason=source_reason,
                        chunk_path=None,
                        chunk_name=None,
                        metrics={
                            "live_status": (
                                state.ytdlp_live_status
                                if engine.name == ENGINE_YTDLP
                                else state.streamlink_live_status
                                if engine.name == ENGINE_STREAMLINK
                                else None
                            )
                        },
                    )
                else:
                    if engine.name == ENGINE_FFMPEG:
                        reset_live_off_state(state, "probe_success")
                    elif (
                        engine.name == ENGINE_YTDLP
                        and state.ytdlp_live_status in ("is_live", "is_upcoming")
                    ):
                        # YouTube metadata probing is authoritative enough to confirm
                        # live/upcoming availability. Non-YouTube yt-dlp waits for data.
                        reset_live_off_state(state, "probe_success")
                    elif (
                        engine.name == ENGINE_STREAMLINK
                        and state.streamlink_live_status in ("is_live", "is_upcoming")
                    ):
                        reset_live_off_state(state, "probe_success")

                    result = engine.run_cycle(
                        state,
                        backoff_sec,
                        notify,
                        state.deadline_ts,
                    )

        finish_sound_snooze_for_completed_run(state)
        backoff_sec = apply_orchestrator_policy(state, engine, result, backoff_sec)

    stop_runtime_controls_reminder(state)
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
        FINAL_FILE = f"{BASE_NAME}_{run_ts}.mkv"

        final_base = os.path.splitext(FINAL_FILE)[0]
        CHUNKS_DIR = f"{final_base}_chunks"
        LIST_FILE = os.path.join(CHUNKS_DIR, "concat_list.txt")
        os.makedirs(CHUNKS_DIR, exist_ok=True)

        init_termcap()
        init_raw_external_capture()
        state.network_guard = NetworkGuard(state)
        ensure_global_key_listener(state)
        wait_until_start(state, selected_engine)
        main(state, selected_engine)
    finally:
        if 'state' in locals() and getattr(state, "wireproxy_manager", None):
            state.wireproxy_manager.stop()
        if osSleep:
            osSleep.uninhibit()

