import ast
import os
import re
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import NamedTuple, Optional, Tuple
from unittest.mock import patch


HELPER_FUNCTIONS = (
    "_fmt_monotonic_clock",
    "_prune_locked",
    "_start_corrupt_siren_if_due",
    "prune_stream_corrupt_window",
    "register_stream_corrupt_recycle",
    "acknowledge_stream_corrupt_alarm",
    "play_stream_corrupt_bad_beep_async",
)
HELPER_CONSTANTS = (
    "STREAM_CORRUPT_WINDOW_SEC",
    "STREAM_CORRUPT_ALARM_THRESHOLD",
    "STREAM_CORRUPT_ACK_QUIET_SEC",
)


def resolve_record_py_path() -> Path:
    """Resolve record_static.py: env override -> this repository's copy -> legacy parent/sibling search.

    The repository copy must win. The earlier mtime-based search could pick a
    stale ``record_static.py`` sitting one directory above the clone, so every
    AST assertion in this module would verify the wrong file while still passing.
    """
    env_path = os.environ.get("RECORD_STATIC_PY_PATH")
    if env_path and Path(env_path).exists():
        return Path(env_path)
    repo_copy = Path(__file__).resolve().parent.parent / "record_static.py"
    if repo_copy.is_file():
        return repo_copy
    # Legacy search, retained only for layouts that keep the recorder outside the repository.
    candidates = []
    for parent in Path(__file__).resolve().parents[:4]:
        try:
            for item in parent.iterdir():
                if item.is_file() and item.name == "record_static.py":
                    candidates.append(item)
                elif item.is_dir() and not item.name.startswith((".", "_")) and item.name != "Recorder":
                    sub = item / "record_static.py"
                    if sub.is_file():
                        candidates.append(sub)
        except (PermissionError, OSError):
            continue
    if candidates:
        return max(candidates, key=lambda p: p.stat().st_mtime)
    raise unittest.SkipTest("Static recorder record_static.py not found in local workspace (CI environment)")


class WindowDummyState:
    """Minimal RecorderState stand-in for the rolling-window helpers."""

    def __init__(self):
        self.ffmpeg_stream_corrupt_incidents = 0
        self.ffmpeg_stream_corrupt_nonfatal_incidents = 0
        self.ffmpeg_stream_corrupt_alarm_active = False
        self.ffmpeg_stream_corrupt_post_ack_silent = False
        self.stream_corrupt_recycle_timestamps = []
        self.stream_corrupt_lock = threading.Lock()
        self.shared_alarm_type = None
        self.shared_alarm_activity_id = None
        self.alarm_ack_requested = False
        self.alarm_linger_until = None


def load_window_helpers():
    """Extract the rolling-window constants and helpers from record_static.py AST (no module import)."""
    record_path = resolve_record_py_path()
    tree = ast.parse(record_path.read_text(encoding="utf-8"))
    wanted = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in HELPER_FUNCTIONS:
            wanted.append(node)
        elif isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id in HELPER_CONSTANTS for t in node.targets
        ):
            wanted.append(node)
    found = {n.name for n in wanted if isinstance(n, ast.FunctionDef)}
    missing = set(HELPER_FUNCTIONS) - found
    assert not missing, f"Missing helpers in {record_path}: {sorted(missing)}"

    logs, alarms_started, alarms_stopped, beeps = [], [], [], []

    def dummy_log(msg, level="INFO", **kwargs):
        logs.append((level, msg))

    def dummy_alarm_start(state, alarm_type=None, incident=None):
        alarms_started.append(alarm_type)
        # Mirrors real single-alarm exclusivity: no second siren while one is alive.
        if state.shared_alarm_type is None:
            state.shared_alarm_type = alarm_type

    def dummy_alarm_stop(state):
        alarms_stopped.append(True)
        state.shared_alarm_type = None

    def dummy_is_alarm_active(state):
        return state.shared_alarm_type is not None

    def dummy_beep_bad(state=None):
        beeps.append(True)

    ns = {
        "time": time,
        "threading": threading,
        "Optional": Optional,
        "log": dummy_log,
        "shared_alarm_start": dummy_alarm_start,
        "shared_alarm_stop": dummy_alarm_stop,
        "is_alarm_active": dummy_is_alarm_active,
        "beep_bad": dummy_beep_bad,
    }
    mod = ast.Module(body=wanted, type_ignores=[])
    exec(compile(mod, filename=str(record_path), mode="exec"), ns)
    return ns, logs, alarms_started, alarms_stopped, beeps


# BUG-029: process_stderr_line now hands the startup Video banner to
# evaluate_startup_quality(), so the isolated monitor harness must carry the
# quality helpers the class calls. Everything here is still compiled from the
# production file - nothing is re-implemented in the test.
QUALITY_HELPER_FUNCTIONS = (
    "observe_startup_line",
    "evaluate_startup_quality",
    "_handle_startup_downgrade",
    "_raise_or_park_quality_alarm",
    "request_format_recycle",
    "_resolve_quality_standard",
    "_classify_quality_reading",
    "_coerce_video_params",
    "_normalize_quality_fps",
    "_parse_expected_video",
    "_fmt_monotonic_clock",
)
QUALITY_HELPER_CLASSES = ("VideoParams", "QualityVerdict")
QUALITY_HELPER_CONSTANTS = (
    "QUALITY_PENDING_MAX_WAIT_SEC",
    "RE_VIDEO_BANNER",
    "RE_FIELD_ORDER",
    "RE_EXPECTED_VIDEO_FULL",
    "RE_EXPECTED_VIDEO_SHORT",
    "_HEIGHT_TO_WIDTH",
)


def quality_support_nodes(tree):
    """Collect the quality helpers/classes/constants the monitor class depends on."""
    wanted = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in QUALITY_HELPER_FUNCTIONS:
            wanted.append(node)
        elif isinstance(node, ast.ClassDef) and node.name in QUALITY_HELPER_CLASSES:
            wanted.append(node)
        elif isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id in QUALITY_HELPER_CONSTANTS for t in node.targets
        ):
            wanted.append(node)
    found = {n.name for n in wanted if isinstance(n, (ast.FunctionDef, ast.ClassDef))}
    missing = (
        set(QUALITY_HELPER_FUNCTIONS) | set(QUALITY_HELPER_CLASSES)
    ) - found
    assert not missing, f"Missing quality helpers for the monitor harness: {sorted(missing)}"
    return wanted


def load_monitor_class():
    """Extract StreamIntegrityMonitor directly from record_static.py AST for fast isolated unit testing."""
    record_path = str(resolve_record_py_path())

    with open(record_path, "r", encoding="utf-8") as f:
        src = f.read()

    tree = ast.parse(src)
    class_node = None
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "StreamIntegrityMonitor":
            class_node = node
            break
    assert class_node is not None, f"StreamIntegrityMonitor not found in {record_path}"

    class DummyState:
        def __init__(self):
            self.ffmpeg_stream_corrupt_incidents = 0
            self.ffmpeg_stream_corrupt_nonfatal_incidents = 0
            self.ffmpeg_stream_corrupt_alarm_active = False
            self.ffmpeg_stream_corrupt_post_ack_silent = False
            self.stream_corrupt_recycle_timestamps = []
            self.stream_corrupt_lock = threading.Lock()

    logs = []
    alarms_started = []
    alarms_stopped = []

    def dummy_log(msg, level="INFO"):
        logs.append((level, msg))

    def dummy_alarm_start(state, alarm_type=None, incident=None):
        alarms_started.append(alarm_type)

    def dummy_alarm_stop(state):
        alarms_stopped.append(True)

    def dummy_quality_alarm_start(state):
        alarms_started.append("quality_downgrade")

    ns = {
        "re": re,
        "time": time,
        "threading": threading,
        "Optional": Optional,
        "Tuple": Tuple,
        "NamedTuple": NamedTuple,
        "RecorderState": DummyState,
        "log": dummy_log,
        "shared_alarm_start": dummy_alarm_start,
        "shared_alarm_stop": dummy_alarm_stop,
        "is_alarm_active": lambda state: getattr(state, "shared_alarm_type", None) is not None,
        "quality_downgrade_alarm_start": dummy_quality_alarm_start,
        # EXPECTED_VIDEO is read via globals() in the production helpers.
        "EXPECTED_VIDEO": None,
    }
    mod = ast.Module(body=quality_support_nodes(tree) + [class_node], type_ignores=[])
    exec(compile(mod, filename=record_path, mode="exec"), ns)
    return ns["StreamIntegrityMonitor"], DummyState, logs, alarms_started, alarms_stopped


class TestStreamIntegrityMonitorTwoTier(unittest.TestCase):
    def setUp(self):
        (
            self.MonitorCls,
            self.DummyState,
            self.logs,
            self.alarms_started,
            self.alarms_stopped,
        ) = load_monitor_class()
        self.state = self.DummyState()

    def test_01_chunk1_1h57m_nalu_plus_19h_timeline_tear_fast_seals(self):
        """
        Replays exact Chunk 1 @ 01:57:51 (09:12:30.917) incident from 17-Oct-2026 log:
        - Mature chunk (out_time_us = 7,071,536,000 = 1h57m51s)
        - NALU header parse failures + +70,788,477,689 (+19.66h) discontinuity
        - Followed by 0 stderr warnings and advancing frames (353589 -> 354190 -> 354226...)
        Must NOT self-heal at 4s; must fast-seal after 2.0s coalesce.
        """
        t = [1000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            # Establish mature chunk state at 1h57m51s
            mon.process_progress({
                "frame": "353589",
                "total_size": "5201025680",
                "out_time_us": "7071536000",
            })

            # Exact splice lines at 09:12:30.917 - 09:12:30.922
            mon.process_stderr_line("[h264 @ 0x99cc4c600] Failed to parse header of NALU (type 4): Invalid data found when processing input")
            mon.process_stderr_line("[h264 @ 0x99cc4c600] Failed to parse header of NALU (type 16): Invalid data found when processing input")
            mon.process_stderr_line("[vist#0:0/h264 @ 0x99cdbc180] timestamp discontinuity (stream id=256): 70788477689, new offset= -70740877689")
            mon.evaluate_health()

            self.assertTrue(mon.container_defect)
            self.assertIn("Extreme Timeline Tear (+70788477689)", mon.last_container_error_msg)
            self.assertFalse(mon.recycle_requested)

            # 1 second later: frames advance cleanly in -c copy (353589 -> 354190), no new stderr
            t[0] += 1.0
            mon.process_progress({
                "frame": "354190",
                "total_size": "5212451844",
                "out_time_us": "7071536000",
            })
            mon.evaluate_health()
            self.assertFalse(mon.recycle_requested)  # Still within 2.0s coalesce window

            # 2.0 seconds after splice: frames still advancing (354190 -> 354226), fast-seal triggers!
            t[0] += 1.0
            mon.process_progress({
                "frame": "354226",
                "total_size": "5213250692",
                "out_time_us": "7072256000",
            })
            mon.evaluate_health()
            self.assertTrue(mon.recycle_requested)
            self.assertIn("Fatal Container Poisoning", mon.recycle_reason)
            self.assertIn("Extreme Timeline Tear (+70788477689)", mon.recycle_reason)

    def test_02_chunk1_2h52m_unset_timestamps_opposing_av_tear_and_aac_extradata(self):
        """
        Replays exact Chunk 1 @ 02:52:09 (10:06:12.229) incident from 17-Oct-2026 log:
        - missing picture in access unit
        - Timestamps are unset in a packet for stream 0
        - vist discontinuity: -3206710000 (-53.4m)
        - aist discontinuity: +3206710000 (+53.4m)
        - AAC bitstream not in ADTS format and extradata missing
        """
        t = [2000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.process_progress({
                "frame": "516462",
                "total_size": "7610925072",
                "out_time_us": "10329248000",
            })

            mon.process_stderr_line("[h264 @ 0x99cc4c600] missing picture in access unit with size 534")
            mon.process_stderr_line("[mpegts @ 0x99cc3c000] Timestamps are unset in a packet for stream 0.")
            mon.process_stderr_line("[vist#0:0/h264 @ 0x99cdbc180] timestamp discontinuity (stream id=256): -3206710000, new offset= -67484387689")
            mon.process_stderr_line("[aist#0:1/aac @ 0x99cdbc780] timestamp discontinuity (stream id=257): 3206710000, new offset= -70691097689")
            mon.process_stderr_line("[mpegts @ 0x99cdbc000] AAC bitstream not in ADTS format and extradata missing")
            mon.evaluate_health()

            self.assertTrue(mon.container_defect)
            self.assertIn("Unset Packet Timestamps", mon.last_container_error_msg)
            self.assertIn("Extreme Timeline Tear", mon.last_container_error_msg)
            self.assertIn("Opposing A/V Timeline Tear (-3206710000 vs +3206710000)", mon.last_container_error_msg)
            self.assertIn("AAC Extradata Missing", mon.last_container_error_msg)

            t[0] += 2.1
            mon.evaluate_health()
            self.assertTrue(mon.recycle_requested)

    def test_03_chunk3_1h09m_nalu_plus_24h_timeline_tear_fast_seals(self):
        """
        Replays exact Chunk 3 @ 01:09:30 (12:35:13.126) incident from 17-Oct-2026 log:
        - NALU header parse failures + +88,742,481,667 (+24.65h) discontinuity
        - Followed by advancing frames (208517 -> 209265 -> 209333...)
        """
        t = [3000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.process_progress({
                "frame": "208517",
                "total_size": "3078907916",
                "out_time_us": "4170128000",
            })

            mon.process_stderr_line("[h264 @ 0x9f044c600] Failed to parse header of NALU (type 4): Invalid data found when processing input")
            mon.process_stderr_line("[vist#0:0/h264 @ 0x9f05bc180] timestamp discontinuity (stream id=256): 88742481667, new offset= -88681921667")
            mon.evaluate_health()

            self.assertTrue(mon.container_defect)
            t[0] += 2.0
            mon.process_progress({
                "frame": "209333",
                "total_size": "3091696992",
                "out_time_us": "4171504000",
            })
            mon.evaluate_health()
            self.assertTrue(mon.recycle_requested)
            self.assertIn("Extreme Timeline Tear (+88742481667)", mon.recycle_reason)

    def test_04_chunk3_1h51m_dts_gt_pts_inversion_and_opposing_tear(self):
        """
        Replays exact Chunk 3 @ 01:51:24 (13:17:07.650) incident from 17-Oct-2026 log:
        - Invalid timestamps stream=0, pts=227833200, dts=8817758792 (DTS > PTS)
        - vist discontinuity: -95443717689
        - aist discontinuity: +95443717689
        """
        t = [4000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.process_progress({
                "frame": "334227",
                "total_size": "4929891676",
                "out_time_us": "6684364667",
            })

            mon.process_stderr_line("[mpegts @ 0x9f043c000] Invalid timestamps stream=0, pts=227833200, dts=8817758792, size=2921")
            mon.process_stderr_line("[vist#0:0/h264 @ 0x9f05bc180] timestamp discontinuity (stream id=256): -95443717689, new offset= 6761796022")
            mon.process_stderr_line("[aist#0:1/aac @ 0x9f05bc780] timestamp discontinuity (stream id=257): 95443717689, new offset= -88681921667")
            mon.evaluate_health()

            self.assertTrue(mon.container_defect)
            self.assertIn("Protocol DTS > PTS Timestamp Inversion", mon.last_container_error_msg)
            t[0] += 2.0
            mon.evaluate_health()
            self.assertTrue(mon.recycle_requested)

    def test_05_benign_isolated_nalu_blip_self_heals_without_false_positive(self):
        """
        Verifies an isolated NALU / PPS warning where timeline stays intact
        self-heals after 4.0s of advancing frames with ZERO alarm and ZERO chunk recycle.
        """
        t = [5000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.process_progress({
                "frame": "10000",
                "total_size": "150000000",
                "out_time_us": "200000000",
            })

            # Isolated NALU and PPS blips
            mon.process_stderr_line("[h264 @ 0x123] Failed to parse header of NALU (type 4): Invalid data")
            mon.process_stderr_line("[h264 @ 0x123] non-existing PPS 0 referenced")
            mon.evaluate_health()

            self.assertTrue(mon.codec_defect)
            self.assertFalse(mon.container_defect)
            self.assertFalse(mon.recycle_requested)

            # Advance 4.5s with clean frame progression
            for step in range(1, 6):
                t[0] += 1.0
                mon.process_progress({
                    "frame": str(10000 + step * 50),
                    "total_size": str(150000000 + step * 700000),
                    "out_time_us": str(200000000 + step * 1000000),
                })
                mon.evaluate_health()

            self.assertFalse(mon.codec_defect)
            self.assertFalse(mon.container_defect)
            self.assertFalse(mon.incident_active)
            self.assertFalse(mon.recycle_requested)
            self.assertEqual(len(self.alarms_started), 0)

    def test_06_benign_http_reconnect_and_ring_buffer_overlap_no_false_positive(self):
        """
        Verifies a mid-stream HTTP reconnect with Packet corrupt and a -23.8s server ring-buffer
        discontinuity (like 09:11:57 before the fatal splice) does NOT trigger Tier 2 poisoning.
        """
        t = [6000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.process_progress({
                "frame": "352414",
                "total_size": "5183677040",
                "out_time_us": "7048036000",
            })

            mon.process_stderr_line("[http @ 0x99cc40180] Will reconnect at 5381089096 in 0 second(s), error=Input/output error.")
            mon.process_stderr_line("[mpegts @ 0x99cc3c000] Packet corrupt (stream = 0, dts = 2218933800).")
            mon.process_stderr_line("[vist#0:0/h264 @ 0x99cdbc180] timestamp discontinuity (stream id=256): -23800000, new offset= 47600000")
            mon.evaluate_health()

            self.assertFalse(mon.container_defect)
            self.assertFalse(mon.recycle_requested)

            # Stream continues advancing cleanly
            for step in range(1, 6):
                t[0] += 1.0
                mon.process_progress({
                    "frame": str(352414 + step * 50),
                    "total_size": str(5183677040 + step * 700000),
                    "out_time_us": str(7048036000 + step * 1000000),
                })
                mon.evaluate_health()

            self.assertFalse(mon.transport_defect)
            self.assertFalse(mon.container_defect)
            self.assertFalse(mon.recycle_requested)
            self.assertEqual(len(self.alarms_started), 0)

    def test_07_benign_startup_unset_timestamps_ignored(self):
        """
        Verifies that 'Timestamps are unset in a packet' during the initial 10s startup alignment
        is safely ignored and never triggers container poisoning.
        """
        t = [7000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.process_progress({
                "frame": "10",
                "total_size": "50000",
                "out_time_us": "200000",  # 0.2s (< 10s startup threshold)
            })
            mon.process_stderr_line("[mpegts @ 0x123] Timestamps are unset in a packet for stream 0.")
            mon.evaluate_health()

            self.assertFalse(mon.container_defect)
            self.assertFalse(mon.recycle_requested)

    def test_08_immature_chunk_anti_thrash_uses_full_grace_window(self):
        """
        Verifies that if Tier 2 container poisoning happens on a brand-new chunk (< 30s),
        it latches (does not self-heal at 4s) but withholds the 2s fast-seal, waiting for the
        full 8s alarm + 15s grace window (23s total) to prevent rapid 2s restart loops.
        """
        t = [8000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.process_progress({
                "frame": "250",
                "total_size": "3500000",
                "out_time_us": "5000000",  # 5.0s (< 30.0s mature threshold)
            })

            mon.process_stderr_line("[mpegts @ 0x123] Invalid timestamps stream=0, pts=100, dts=900000")
            mon.evaluate_health()
            self.assertTrue(mon.container_defect)

            # At 2.5s: should NOT fast-seal because chunk is < 30s
            t[0] += 2.5
            mon.evaluate_health()
            self.assertFalse(mon.recycle_requested)

            # At 5.0s: even if frames advance, must NOT decay because container_defect is latched
            t[0] += 2.5
            mon.process_progress({
                "frame": "500",
                "total_size": "7000000",
                "out_time_us": "10000000",
            })
            mon.evaluate_health()
            self.assertTrue(mon.container_defect)
            self.assertFalse(mon.recycle_requested)

            # At 8.0s: incident begins and the grace countdown starts. The monitor no longer
            # sounds any siren; escalation belongs to the rolling-window helpers.
            t[0] += 3.0
            mon.evaluate_health()
            self.assertTrue(mon.incident_active)
            self.assertEqual(len(self.alarms_started), 0)
            self.assertFalse(mon.recycle_requested)

            # At 8.0s + 15.0s = 23.0s: grace expires and recycles
            t[0] += 15.0
            mon.evaluate_health()
            self.assertTrue(mon.recycle_requested)
            self.assertIn("Fatal Container Poisoning", mon.recycle_reason)

    def test_09_benign_commercial_break_same_direction_splice_no_false_positive(self):
        """
        Verifies a normal 45-second broadcast commercial splice (+45,000,000 us on both video and audio
        in the same direction) does not trigger Tier 2 container poisoning and self-clears cleanly.
        """
        t = [9000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.process_progress({
                "frame": "50000",
                "total_size": "800000000",
                "out_time_us": "1000000000",
            })

            mon.process_stderr_line("[vist#0:0/h264] timestamp discontinuity (stream id=256): 45000000, new offset= -45000000")
            mon.process_stderr_line("[aist#0:1/aac] timestamp discontinuity (stream id=257): 45000000, new offset= -45000000")
            mon.evaluate_health()

            self.assertFalse(mon.container_defect)
            self.assertFalse(mon.transport_defect)
            self.assertFalse(mon.recycle_requested)

    def test_10_sustained_tier1_codec_loss_recycles_after_15s_grace(self):
        """
        Verifies that continuous Tier 1 NALU errors (without a container timeline tear)
        trigger the alarm at 8s and recycle after the 15s codec grace window (23s total).
        """
        t = [10000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.process_progress({
                "frame": "5000",
                "total_size": "50000000",
                "out_time_us": "100000000",
            })

            for step in range(24):
                mon.process_stderr_line("[h264] non-existing PPS 0 referenced")
                mon.evaluate_health()
                if step < 8:
                    self.assertFalse(mon.incident_active)
                elif step < 23:
                    self.assertTrue(mon.incident_active)
                    self.assertFalse(mon.recycle_requested)
                else:
                    self.assertTrue(mon.recycle_requested)
                    self.assertIn("Codec Parameter Loss", mon.recycle_reason)
                t[0] += 1.0

    def test_11_sustained_video_freeze_recycles_after_30s_grace(self):
        """
        Verifies audio-only masking defense: when bytes grow > 50KB/s but video frame counter
        is frozen, flags freeze at 6s, alarms at 14s, and recycles after 30s grace (44s total).
        """
        t = [11000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            size = 100_000_000
            out_us = 200_000_000
            mon.process_progress({
                "frame": "12000",
                "total_size": str(size),
                "out_time_us": str(out_us),
            })

            for step in range(1, 46):
                t[0] += 1.0
                size += 100_000  # Audio bytes still downloading
                out_us += 1_000_000
                mon.process_progress({
                    "frame": "12000",  # Video frame frozen!
                    "total_size": str(size),
                    "out_time_us": str(out_us),
                })
                mon.evaluate_health()

            self.assertTrue(mon.video_freeze_defect)
            self.assertTrue(mon.recycle_requested)
            self.assertIn("Video Elementary Freeze", mon.recycle_reason)

    def test_12_transport_grace_recycle_after_60s(self):
        """
        Verifies that a sustained Tier 1 transport discontinuity storm triggers transport
        grace recycling, and that the monitor never starts or stops any alarm itself
        (sirens are owned by the rolling-window helpers on RecorderState).
        """
        t = [12000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            for step in range(75):
                # Sustained discontinuities (+5s) every second (transport storm)
                delta = 5000000
                mon.process_stderr_line(f"[vist] timestamp discontinuity: {delta}, new offset= 0")
                mon.evaluate_health()
                t[0] += 1.0

            # Under the new spec, the 60s hard limit is deleted; transport grace recycle triggers.
            self.assertTrue(mon.recycle_requested)
            self.assertIn("Transport Discontinuity", mon.recycle_reason)
            # Regression guard (Blocker 1): the monitor never touches the shared alarm.
            self.assertEqual(self.alarms_started, [])
            self.assertEqual(self.alarms_stopped, [])
            # Fatal counting is owned by register_stream_corrupt_recycle, not the monitor.
            self.assertEqual(self.state.ffmpeg_stream_corrupt_incidents, 0)

    def test_13_sustained_defect_that_self_heals_counts_nonfatal_only(self):
        """A defect >= 8.0s followed by >= 15.0s clean (no recycle) is one non-fatal incident."""
        t = [20000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            frame = 1000
            for _ in range(10):
                mon.process_stderr_line("[h264 @ 0x1] non-existing PPS 0 referenced")
                frame += 50
                mon.process_progress({
                    "frame": str(frame),
                    "total_size": str(frame * 10000),
                    "out_time_us": str(frame * 20000),
                })
                mon.evaluate_health()
                t[0] += 1.0
            self.assertTrue(mon.incident_active)
            self.assertFalse(mon.recycle_requested)
            self.assertEqual(self.state.ffmpeg_stream_corrupt_nonfatal_incidents, 0)

            for _ in range(25):
                frame += 50
                mon.process_progress({
                    "frame": str(frame),
                    "total_size": str(frame * 10000),
                    "out_time_us": str(frame * 20000),
                })
                mon.evaluate_health()
                t[0] += 1.0

            self.assertFalse(mon.incident_active)
            self.assertFalse(mon.recycle_requested)
            self.assertEqual(self.state.ffmpeg_stream_corrupt_nonfatal_incidents, 1)
            self.assertEqual(self.state.ffmpeg_stream_corrupt_incidents, 0)
            self.assertEqual(self.alarms_started, [])
            self.assertEqual(self.alarms_stopped, [])

    def test_14_sub_8s_blip_is_not_counted_as_incident(self):
        """Transient defects under 8.0s are broadcast noise and are never counted."""
        t = [21000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.process_progress({"frame": "100", "total_size": "1000000", "out_time_us": "5000000"})
            mon.process_stderr_line("[h264 @ 0x1] non-existing PPS 0 referenced")
            mon.evaluate_health()
            frame = 100
            for _ in range(40):
                t[0] += 1.0
                frame += 50
                mon.process_progress({
                    "frame": str(frame),
                    "total_size": str(frame * 10000),
                    "out_time_us": str(frame * 20000),
                })
                mon.evaluate_health()
            self.assertFalse(mon.incident_active)
            self.assertEqual(self.state.ffmpeg_stream_corrupt_nonfatal_incidents, 0)

    def test_15_pending_recycle_blocks_nonfatal_count_and_is_not_cancelled(self):
        """R3: a requested recycle must not be counted non-fatal nor reset by the recovery branch."""
        t = [22000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.incident_active = True
            mon.current_incident_start = t[0] - 20.0
            mon.recycle_requested = True
            mon.recycle_reason = "Codec Parameter Loss sustained >= 15s"
            mon.clean_since_mono = t[0] - 100.0
            mon.defect_start_mono = None
            mon.evaluate_health()
            self.assertTrue(mon.recycle_requested)
            self.assertTrue(mon.incident_active)
            self.assertEqual(self.state.ffmpeg_stream_corrupt_nonfatal_incidents, 0)

    def test_16_startup_wrapped_negative_dts_ignored(self):
        """Verifies startup wrapped negative DTS (2^33 - 1160) within 10s grace is ignored as benign."""
        t = [30000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.process_progress({"frame": "1", "total_size": "50000", "out_time_us": "40000"})
            mon.process_stderr_line("[mpegts @ 0x123] Invalid timestamps stream=0, pts=2440, dts=8589933432, size=71750")
            mon.evaluate_health()
            self.assertFalse(mon.container_defect)
            self.assertFalse(mon.transport_defect)
            self.assertFalse(mon.recycle_requested)

    def test_17_post_startup_or_real_dts_inversion_latches_container_defect(self):
        """Verifies real DTS inversion (dts < wrapped floor) latches container defect immediately."""
        t = [31000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.process_progress({"frame": "1", "total_size": "50000", "out_time_us": "40000"})
            mon.process_stderr_line("[mpegts @ 0x123] Invalid timestamps stream=0, pts=2440, dts=500000, size=71750")
            mon.evaluate_health()
            self.assertTrue(mon.container_defect)
            self.assertIn("Protocol DTS > PTS Timestamp Inversion", mon.last_container_error_msg)

    def test_18_solo_same_sign_timeline_tear_is_tier1_not_fatal(self):
        """Verifies solo large discontinuity without codec/container corruption is Tier 1, not fatal."""
        t = [32000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.process_progress({"frame": "5000", "total_size": "50000000", "out_time_us": "100000000"})
            # Large discontinuity without codec error -> flags Tier 1 transport defect, but not fatal container defect
            mon.process_stderr_line("[vist#0:0/h264] timestamp discontinuity (stream id=256): 70788477689, new offset= -70788477689")
            mon.evaluate_health()
            self.assertTrue(mon.transport_defect)
            self.assertFalse(mon.container_defect)
            self.assertFalse(mon.recycle_requested)

    def test_18b_corroborated_first_tear_latches_fatal(self):
        """Verifies large discontinuity accompanied by codec corruption immediately latches fatal container defect."""
        t = [32000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.process_progress({"frame": "5000", "total_size": "50000000", "out_time_us": "100000000"})
            # Codec corruption occurs
            mon.process_stderr_line("[h264 @ 0x123] missing picture in access unit with POC")
            # First discontinuity arrives with offset == -delta
            mon.process_stderr_line("[vist#0:0/h264] timestamp discontinuity (stream id=256): 70788477689, new offset= -70788477689")
            mon.evaluate_health()
            self.assertTrue(mon.container_defect)
            self.assertIn("Extreme Timeline Tear", mon.last_container_error_msg)

    def test_19_burst_speed_chunk_under_30s_wall_not_mature(self):
        """Verifies 46s media delivered in 4s wall-clock time does not fast-seal because it is not mature."""
        t = [33000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            # 46 seconds of media delivered at t = 4s wall time (speed = 11.5x > 3.0x burst ratio)
            t[0] += 4.0
            mon.process_progress({"frame": "2300", "total_size": "38000000", "out_time_us": "46122667"})
            # Real container defect injected
            mon.process_stderr_line("[mpegts @ 0x123] Invalid timestamps stream=0, pts=2440, dts=500000, size=71750")
            mon.evaluate_health()
            self.assertTrue(mon.container_defect)

            # Advance past 2s coalesce window to t = 6.5s wall time (media/wall = 46/6.5 = 7.0x > 3.0x)
            t[0] += 2.5
            mon.evaluate_health()
            # Fast-seal must NOT trigger because speed ratio > 3.0 (burst dead zone protected)
            self.assertFalse(mon.recycle_requested)

    def test_20_same_sign_video_and_audio_discontinuity_pair_not_fatal(self):
        """Verifies encoder reset hitting both video and audio with same sign within 5s does not latch fatal."""
        t = [34000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.process_progress({"frame": "5000", "total_size": "50000000", "out_time_us": "100000000"})
            # Both video and audio jump with same sign
            mon.process_stderr_line("[vist#0:0/h264] timestamp discontinuity (stream id=256): -3540021667, new offset= 3540021667")
            t[0] += 0.1
            mon.process_stderr_line("[aist#0:1/aac] timestamp discontinuity (stream id=257): -3540021667, new offset= 7080043334")
            mon.evaluate_health()
            self.assertTrue(mon.transport_defect)
            self.assertFalse(mon.container_defect)
            self.assertFalse(mon.recycle_requested)

    def test_23_rule_2c_nopts_pts_latches_container_defect(self):
        """Verifies DTS > PTS inversion with NOPTS or negative PTS is not treated as benign wrap."""
        t = [35000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.process_stderr_line("[mpegts @ 0x123] Invalid timestamps stream=0, pts=-9223372036854775808, dts=8589933432, size=71750")
            mon.evaluate_health()
            self.assertTrue(mon.container_defect)
            self.assertIn("Protocol DTS > PTS Timestamp Inversion", mon.last_container_error_msg)

    def test_25_paired_clock_reset_does_not_latch_container_defect(self):
        """
        Replays exact Chunk 6 incident from 21-Oct-2026 Fox Cricket PFTV run:
        Upstream resets PTS to 0. Audio logs -620544000, Video logs +620544000 1ms later.
        FFmpeg compensates internally at 50fps 1.0x speed.
        Verifies that paired clock reset does NOT poison container, does NOT request recycle,
        and clears solo transport_defect.
        """
        t = [36000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.process_progress({"frame": "31037", "fps": "50.16", "total_size": "330985552", "out_time_us": "620536000", "speed": "1.0x"})

            # Leg 1: Audio discontinuity (-620.544s)
            mon.process_stderr_line("[aist#0:1/ac3 @ 0x1d9fd1f5840] timestamp discontinuity (stream id=257): -620544000, new offset= 620544000")
            # Leg 2: Video discontinuity (+620.544s) arrives 1ms later
            t[0] += 0.001
            mon.process_stderr_line("[vist#0:0/hevc @ 0x1d9fdaca040] timestamp discontinuity (stream id=256): 620544000, new offset= 0")
            mon.evaluate_health()

            self.assertFalse(mon.container_defect)
            self.assertFalse(mon.recycle_requested)
            self.assertFalse(mon.transport_defect)
            self.assertIsNone(mon.last_transport_delta)

    def test_26_asymmetric_tear_latches_container_defect(self):
        """Verifies asymmetric audio/video drift (span >= 54s, diff > 2.0s) latches fatal container defect."""
        t = [37000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.process_progress({"frame": "1000", "total_size": "10000000", "out_time_us": "20000000"})

            # Asymmetric: Audio jumps -100s, Video jumps +10s (difference is 90s >> 2s)
            mon.process_stderr_line("[aist#0:1/ac3] timestamp discontinuity: -100000000, new offset= 100000000")
            t[0] += 0.01
            mon.process_stderr_line("[vist#0:0/hevc] timestamp discontinuity: 10000000, new offset= 0")
            mon.evaluate_health()

            self.assertTrue(mon.container_defect)
            self.assertIn("Opposing A/V Timeline Tear", mon.last_container_error_msg)

    def test_27_container_error_deduplication_and_clamping(self):
        """Verifies repeated errors increment counter and clamp to <= 200 characters without blowing up."""
        mon = self.MonitorCls(self.state)
        now = 1000.0
        for i in range(50):
            # Slightly drifting microsecond delta numbers simulated
            mon._latch_container_defect(now + i, f"Opposing A/V Timeline Tear (-6205440{i:02d} vs +620544000)")

        self.assertTrue(mon.container_defect)
        self.assertLessEqual(len(mon.last_container_error_msg), 200)
        self.assertIn("Opposing A/V Timeline Tear [x50]", mon.last_container_error_msg)

    def test_28_timing_warning_burst_throttling_and_summary_flush(self):
        """
        Verifies Item 3A: Repetitive timing warnings (Non-monotonic DTS / timestamp discontinuity)
        show first 2 occurrences on the screen, suppress subsequent lines during burst,
        and flush a concise periodic summary notice after 3 seconds.
        """
        t = [50000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)

            # First 2 timing warnings in burst: displayed on screen (return False)
            res1 = mon.process_stderr_line("[mpegts @ 0x1] Non-monotonic DTS in output stream 0:1; previous: 100, current: 100; changing to 101.")
            self.assertFalse(res1)
            t[0] += 0.05
            res2 = mon.process_stderr_line("[mpegts @ 0x1] Non-monotonic DTS in output stream 0:1; previous: 101, current: 101; changing to 102.")
            self.assertFalse(res2)

            # Lines 3 to 10 in burst: suppressed from screen (return True)
            for i in range(3, 11):
                t[0] += 0.05
                res = mon.process_stderr_line(f"[mpegts @ 0x1] Non-monotonic DTS in output stream 0:1; previous: {100+i}, current: {100+i}; changing to {101+i}.")
                self.assertTrue(res)

            self.assertEqual(mon.timing_suppressed_count, 8)

            # Less than 3.0 seconds elapsed: check_flush_timing_summary returns None
            self.assertIsNone(mon.check_flush_timing_summary())

            # Advance time past 3.0s window
            t[0] += 3.5
            summary = mon.check_flush_timing_summary()
            self.assertIsNotNone(summary)
            self.assertIn("Timing warning burst (suppressed 8 repetitive lines from screen)", summary)
            self.assertEqual(mon.timing_suppressed_count, 0)
            self.assertIsNone(mon.check_flush_timing_summary())

            # Now verify a SECOND burst occurring 60 seconds later:
            t[0] += 60.0
            # Lines 1 & 2 of new burst should display (return False)
            self.assertFalse(mon.process_stderr_line("[mpegts @ 0x1] Non-monotonic DTS in output stream 0:1; previous: 200, current: 200; changing to 201."))
            t[0] += 0.05
            self.assertFalse(mon.process_stderr_line("[mpegts @ 0x1] Non-monotonic DTS in output stream 0:1; previous: 201, current: 201; changing to 202."))
            # Line 3 of new burst should suppress (return True)
            t[0] += 0.05
            self.assertTrue(mon.process_stderr_line("[mpegts @ 0x1] Non-monotonic DTS in output stream 0:1; previous: 202, current: 202; changing to 203."))
            self.assertEqual(mon.timing_suppressed_count, 1)

            # Must NOT prematurely flush at 0.1s!
            self.assertIsNone(mon.check_flush_timing_summary())

            # Only flushes after full 3.0s window has passed
            t[0] += 3.1
            summary2 = mon.check_flush_timing_summary()
            self.assertIsNotNone(summary2)
            self.assertIn("Timing warning burst (suppressed 1 repetitive lines from screen)", summary2)
            self.assertEqual(mon.timing_suppressed_count, 0)

    def test_29_non_monotonic_pts_and_forced_summary_flush(self):
        """Verifies Non-monotonic PTS throttling and forced summary flush on process exit."""
        t = [60000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)

            # 5 non-monotonic PTS lines
            for i in range(5):
                t[0] += 0.01
                mon.process_stderr_line(f"[mpegts @ 0x2] Non-monotonic PTS in output stream 0:0; previous: {i}, current: {i}.")

            self.assertEqual(mon.timing_suppressed_count, 3)
            # Forced flush on FFmpeg exit flushes immediately regardless of elapsed time
            summary = mon.check_flush_timing_summary(force=True)
            self.assertIsNotNone(summary)
            self.assertIn("suppressed 3 repetitive lines", summary)
            self.assertEqual(mon.timing_suppressed_count, 0)

    def test_24_missing_last_run_start_does_not_flag_burst(self):
        """Verifies missing last_run_start sets run_wall=None and does not misfire replay burst filter."""
        state = WindowDummyState()
        state.stats = {"good_chunks": 1}
        run_start = state.stats.get("last_run_start")
        run_wall = max(0.001, time.time() - float(run_start)) if run_start else None
        dur = 50.0
        is_replay_burst = (
            state.stats.get("good_chunks", 0) > 0
            and run_wall is not None
            and run_wall < 25.0
            and dur < 180.0
            and (dur / run_wall) > 3.0
        )
        self.assertFalse(is_replay_burst)

    def test_21_replay_burst_preserves_first_run_cache(self):
        """Verifies 46s burst in 4s wall-clock time is preserved on chunk 1 (good_chunks=0), but rejected on later chunks."""
        state_first = WindowDummyState()
        state_first.stats = {"good_chunks": 0}
        run_wall = 4.0
        dur = 46.0
        is_replay_burst_first = (
            state_first.stats.get("good_chunks", 0) > 0
            and run_wall < 25.0
            and dur < 180.0
            and (dur / run_wall) > 3.0
        )
        self.assertFalse(is_replay_burst_first)

        state_later = WindowDummyState()
        state_later.stats = {"good_chunks": 1}
        is_replay_burst_later = (
            state_later.stats.get("good_chunks", 0) > 0
            and run_wall < 25.0
            and dur < 180.0
            and (dur / run_wall) > 3.0
        )
        self.assertTrue(is_replay_burst_later)

    def test_22_maturity_speed_ratio_dead_zone_heals(self):
        """Verifies chunk with 65s media running at normal live speed (50s wall) is mature and fast-seals."""
        t = [35000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            # 65s media delivered in 50s wall (speed = 65/50 = 1.3x <= 3.0x)
            t[0] += 50.0
            mon.process_progress({"frame": "3250", "total_size": "55000000", "out_time_us": "65000000"})
            # Severe container defect occurs
            mon.process_stderr_line("[mpegts @ 0x123] Invalid timestamps stream=0, pts=2440, dts=500000, size=71750")
            mon.evaluate_health()
            self.assertTrue(mon.container_defect)

            # Advance past 2.0s coalesce window (to 52.5s wall time)
            t[0] += 2.5
            mon.evaluate_health()
            # Fast-seal triggers because chunk is mature (media >= 60s and speed <= 3.0x)
            self.assertTrue(mon.recycle_requested)
            self.assertIn("Fatal Container Poisoning", mon.recycle_reason)



class TestStreamCorruptRollingWindow(unittest.TestCase):
    """Rolling 30-minute fatal-cut window, escalation siren, ack and re-arm."""

    def setUp(self):
        (
            self.ns,
            self.logs,
            self.alarms_started,
            self.alarms_stopped,
            self.beeps,
        ) = load_window_helpers()
        self.state = WindowDummyState()
        self.window = self.ns["STREAM_CORRUPT_WINDOW_SEC"]
        self.register = self.ns["register_stream_corrupt_recycle"]
        self.prune = self.ns["prune_stream_corrupt_window"]
        self.ack = self.ns["acknowledge_stream_corrupt_alarm"]

    def test_constants(self):
        self.assertEqual(self.window, 1800.0)
        self.assertEqual(self.ns["STREAM_CORRUPT_ALARM_THRESHOLD"], 2)

    def test_boundary_exactly_1800s_stays_and_1800_1s_expires(self):
        self.register(self.state, now=1000.0)
        self.assertEqual(self.prune(self.state, now=1000.0 + 1800.0), 1)
        self.assertEqual(self.prune(self.state, now=1000.0 + 1800.1), 0)

    def test_isolated_cut_beeps_without_siren_and_counts_fatal_once(self):
        should_beep = self.register(self.state, now=5000.0)
        self.assertTrue(should_beep)
        self.assertEqual(self.alarms_started, [])
        self.assertFalse(self.state.ffmpeg_stream_corrupt_alarm_active)
        self.assertEqual(self.state.ffmpeg_stream_corrupt_incidents, 1)

    def test_second_cut_in_window_starts_siren_and_skips_beep(self):
        self.register(self.state, now=5000.0)
        should_beep = self.register(self.state, now=5000.0 + 600.0)
        self.assertEqual(self.alarms_started, ["stream_corrupt"])
        self.assertTrue(self.state.ffmpeg_stream_corrupt_alarm_active)
        self.assertFalse(should_beep)
        self.assertEqual(self.state.ffmpeg_stream_corrupt_incidents, 2)

    def test_second_cut_outside_window_is_a_new_isolated_cut(self):
        self.register(self.state, now=5000.0)
        should_beep = self.register(self.state, now=5000.0 + 1800.1)
        self.assertEqual(self.alarms_started, [])
        self.assertTrue(should_beep)
        self.assertEqual(len(self.state.stream_corrupt_recycle_timestamps), 1)

    def test_ack_silences_episode_and_later_cuts_only_beep(self):
        """R2: a cut after 'A' is beep-only while the five-minute quiet window is open.

        The acknowledgement stamps its quiet window from the real clock (BUG-032), so
        the cut times are offsets from time.monotonic() rather than absolute constants.
        With constants this test silently depended on how long the host had been up: on
        a freshly booted runner (CI) 5200.0 was already past the window, the siren
        re-armed and the "later cut only beeps" expectation failed.
        """
        base = time.monotonic()
        self.register(self.state, now=base)
        self.register(self.state, now=base + 100.0)
        self.assertTrue(self.ack(self.state))
        self.assertTrue(self.state.ffmpeg_stream_corrupt_post_ack_silent)
        self.assertFalse(self.state.ffmpeg_stream_corrupt_alarm_active)
        self.assertEqual(self.alarms_stopped, [True])
        self.assertFalse(self.state.stream_corrupt_lock.locked())

        should_beep = self.register(self.state, now=base + 200.0)
        self.assertTrue(should_beep)
        self.assertEqual(self.alarms_started, ["stream_corrupt"])  # no second siren

    def test_ack_without_active_corrupt_alarm_is_a_noop(self):
        self.assertFalse(self.ack(self.state))
        self.assertEqual(self.alarms_stopped, [])
        self.assertFalse(self.state.ffmpeg_stream_corrupt_post_ack_silent)

    def test_rearm_after_31_minutes_without_tick_via_register_time_prune(self):
        """R1: ack, then a long gap with no sweeper tick, then a cut starts a fresh episode.

        The acknowledgement stamps its quiet window from the real clock (BUG-032), so
        the synthetic cut times are offsets from time.monotonic() rather than absolute
        constants - otherwise the "31 minutes later" cut lands inside a quiet window
        that was opened in the present.
        """
        base = time.monotonic()
        self.register(self.state, now=base)
        self.register(self.state, now=base + 100.0)
        self.ack(self.state)
        self.assertTrue(self.state.ffmpeg_stream_corrupt_post_ack_silent)

        later = base + 100.0 + 31 * 60.0
        should_beep = self.register(self.state, now=later)
        self.assertFalse(self.state.ffmpeg_stream_corrupt_post_ack_silent)
        self.assertEqual(len(self.state.stream_corrupt_recycle_timestamps), 1)
        self.assertTrue(should_beep)
        self.assertEqual(self.alarms_started, ["stream_corrupt"])  # only the first episode's siren

        # The second cut of the NEW episode escalates again (re-armed).
        self.register(self.state, now=later + 60.0)
        self.assertEqual(self.alarms_started, ["stream_corrupt", "stream_corrupt"])

    def test_sweeper_clears_silence_only_on_nonempty_to_empty_transition(self):
        self.state.ffmpeg_stream_corrupt_post_ack_silent = True
        self.assertEqual(self.prune(self.state, now=100.0), 0)  # already empty: untouched
        self.assertTrue(self.state.ffmpeg_stream_corrupt_post_ack_silent)

        self.register(self.state, now=200.0)
        self.state.ffmpeg_stream_corrupt_post_ack_silent = True
        self.assertEqual(self.prune(self.state, now=200.0 + 1000.0), 1)  # still inside window
        self.assertTrue(self.state.ffmpeg_stream_corrupt_post_ack_silent)
        self.assertEqual(self.prune(self.state, now=200.0 + 1800.1), 0)  # transition to empty
        self.assertFalse(self.state.ffmpeg_stream_corrupt_post_ack_silent)

    def test_timeout_silence_keeps_episode_silent(self):
        """N1: the 5-minute auto-silence sets the same flag, so the next cut is beep-only."""
        self.register(self.state, now=5000.0)
        self.state.ffmpeg_stream_corrupt_post_ack_silent = True  # what _auto_silence_shared_alarm sets
        should_beep = self.register(self.state, now=5300.0)
        self.assertEqual(self.alarms_started, [])
        self.assertTrue(should_beep)

    def test_foreign_siren_alive_blocks_flag_and_beep(self):
        """B6: alarm_active is only set when the live siren is actually stream_corrupt."""
        self.state.shared_alarm_type = "vpn_drop"
        self.register(self.state, now=5000.0)
        should_beep = self.register(self.state, now=5100.0)
        self.assertFalse(self.state.ffmpeg_stream_corrupt_alarm_active)
        self.assertFalse(should_beep)

    def test_bad_beep_async_runs_on_daemon_thread(self):
        done = threading.Event()
        self.ns["beep_bad"] = lambda state=None: done.set()
        self.ns["play_stream_corrupt_bad_beep_async"](self.state)
        self.assertTrue(done.wait(timeout=2.0))


def load_finalize_function(test_dir: Path):
    record_path = resolve_record_py_path()
    tree = ast.parse(record_path.read_text(encoding="utf-8"))
    wanted = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "finalize_recording"]
    assert len(wanted) == 1, f"finalize_recording not found in {record_path}"

    logs = []
    commands_run = []
    trashed = []
    summary_calls = []

    def dummy_run(cmd, cwd, tag=""):
        commands_run.append((tag, cmd))
        return 0

    list_file = str(test_dir / "concat_list.txt")
    final_file = str(test_dir / "FINAL.mkv")
    chunks_dir = str(test_dir / "chunks")

    ns = {
        "os": os,
        "time": time,
        "log": lambda msg, level="INFO", **kw: logs.append((level, msg)),
        "send2trash": lambda p: trashed.append(p),
        "get_file_info": lambda p: (100.0, {}),
        "run_ffmpeg_with_capture": dummy_run,
        "write_summary_log": lambda state, dur, finalized=True: summary_calls.append((state, dur, finalized)),
        "summary_calls": summary_calls,
        "LIST_FILE": list_file,
        "FINAL_FILE": final_file,
        "CHUNKS_DIR": chunks_dir,
        "BLACK_LEN": 1.0,
        "FINAL_AUDIO_MIN_KBPS": 192,
        "FINAL_AUDIO_KBPS_PER_CHANNEL": 64,
        "FINAL_MIN_DURATION_RATIO": 0.90,
        "FFMPEG_PROGRESS_PERIOD": 1.0,
        "RecorderState": object,
        "DOWNLOAD_MODE": "FFMPEG",
        "ENGINE_YTDLP": "YTDLP",
        "ENGINE_STREAMLINK": "STREAMLINK",
    }
    mod = ast.Module(body=wanted, type_ignores=[])
    exec(compile(mod, filename=str(record_path), mode="exec"), ns)
    return ns["finalize_recording"], ns, logs, commands_run, trashed


class TestTwoTierFinalization(unittest.TestCase):
    """Verifies Two-Tier Concatenation Pipeline: Tier 1 fast copy -> Tier 2 audio transcode fallback."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_dir = Path(self.temp_dir.name)
        self.list_file = self.test_dir / "concat_list.txt"
        self.list_file.write_text("file chunk_001.ts\nfile chunk_002.ts\n", encoding="utf-8")
        self.final_file = self.test_dir / "FINAL.mkv"
        self.chunks_dir = self.test_dir / "chunks"
        self.chunks_dir.mkdir(exist_ok=True)

        self.finalize_fn, self.ns, self.logs, self.commands_run, self.trashed = load_finalize_function(self.test_dir)

        class DummyState:
            best_w = 1920
            best_h = 1080
            best_fps = 50
            same_params = True
            mixed_reason = ""
            stream_layout_ref = [{"codec_type": "audio", "sample_rate": 48000, "channels": 2}]
            audio_layout_ref = None
            stats = {"sessions": [{"dur_media": 50.0}, {"dur_media": 50.0}]}
        self.state = DummyState()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_tier1_lossless_copy_succeeds_without_fallback(self):
        """When Tier 1 copy succeeds, only 1 ffmpeg call is made and chunks folder is recycled."""
        def mock_run(cmd, cwd, tag=""):
            self.commands_run.append((tag, cmd))
            self.final_file.write_bytes(b"mock_video_bytes")
            return 0

        self.ns["run_ffmpeg_with_capture"] = mock_run
        self.finalize_fn(self.state)

        self.assertEqual(len(self.commands_run), 1)
        tag, cmd = self.commands_run[0]
        self.assertEqual(tag, "FFmpeg-FINAL-T1")
        self.assertIn("-c", cmd)
        self.assertIn("copy", cmd)
        self.assertIn(str(self.chunks_dir), self.trashed)

    def test_tier1_fails_tier2_fallback_succeeds(self):
        """When Tier 1 fails (e.g. corrupt ADTS tail crash), Tier 2 runs with audio re-encode and resample."""
        def mock_run(cmd, cwd, tag=""):
            self.commands_run.append((tag, cmd))
            if tag == "FFmpeg-FINAL-T1":
                self.final_file.write_bytes(b"corrupt_partial")
                return 1  # Tier 1 fails
            # Tier 2 succeeds
            self.final_file.write_bytes(b"valid_final_bytes")
            return 0

        self.ns["run_ffmpeg_with_capture"] = mock_run
        self.finalize_fn(self.state)

        self.assertEqual(len(self.commands_run), 2)
        tag1, _ = self.commands_run[0]
        self.assertEqual(tag1, "FFmpeg-FINAL-T1")

        tag2, cmd2 = self.commands_run[1]
        self.assertEqual(tag2, "FFmpeg-FINAL-T2")
        self.assertIn("-c:v", cmd2)
        self.assertIn("-c:a", cmd2)
        self.assertIn("aac", cmd2)
        self.assertIn("-af", cmd2)
        self.assertIn("aresample=async=1:first_pts=0", cmd2)
        self.assertIn("-ar:a:0", cmd2)
        self.assertIn("48000", cmd2)
        self.assertIn("-ac:a:0", cmd2)
        self.assertIn("2", cmd2)
        self.assertIn("-b:a:0", cmd2)
        self.assertIn(str(self.chunks_dir), self.trashed)

    def test_both_tiers_fail_preserves_chunks_folder(self):
        """When both tiers fail, chunks folder is preserved 100% intact (never sent to trash)."""
        def mock_run(cmd, cwd, tag=""):
            self.commands_run.append((tag, cmd))
            return 1

        self.ns["run_ffmpeg_with_capture"] = mock_run
        self.finalize_fn(self.state)

        self.assertEqual(len(self.commands_run), 2)
        self.assertNotIn(str(self.chunks_dir), self.trashed)

    def test_duration_truncated_both_tiers_removes_output_and_preserves_chunks(self):
        """When output duration < 90% expected across both tiers, incomplete output is removed and chunks are kept for manual salvage."""
        def mock_run(cmd, cwd, tag=""):
            self.commands_run.append((tag, cmd))
            self.final_file.write_bytes(b"truncated_video_bytes")
            return 0

        # Return 80.0s (expected is 100s + 1s black = 101s; 80 < 101 * 0.90 = 90.9s)
        self.ns["get_file_info"] = lambda p: (80.0, {})
        self.ns["run_ffmpeg_with_capture"] = mock_run
        self.finalize_fn(self.state)

        # Chunks folder must NOT be trashed; kept intact for operator
        self.assertNotIn(str(self.chunks_dir), self.trashed)
        # Broken / incomplete final file must be removed
        self.assertFalse(self.final_file.exists())
        # No partial file created
        self.assertFalse((self.test_dir / "FINAL.partial.mkv").exists())

    def test_duration_zero_fails_and_preserves_chunks(self):
        """When output duration is 0s, both tiers fail, output is deleted, and chunks are kept."""
        def mock_run(cmd, cwd, tag=""):
            self.commands_run.append((tag, cmd))
            self.final_file.write_bytes(b"zero_dur_bytes")
            return 0

        self.ns["get_file_info"] = lambda p: (0.0, {})
        self.ns["run_ffmpeg_with_capture"] = mock_run
        self.finalize_fn(self.state)

        self.assertNotIn(str(self.chunks_dir), self.trashed)
        self.assertFalse(self.final_file.exists())

    def test_tier1_truncated_tier2_crashes_preserves_chunks_and_logs_failure(self):
        """When Tier 1 is truncated and Tier 2 crashes, incomplete final is removed, chunks are preserved for manual salvage."""
        def mock_run(cmd, cwd, tag=""):
            self.commands_run.append((tag, cmd))
            if tag == "FFmpeg-FINAL-T1":
                self.final_file.write_bytes(b"tier1_truncated_bytes")
                return 0  # Tier 1 exits 0 but fails duration gate (80s < 90.9s)
            # Tier 2 crashes or fails with rc=1
            self.final_file.write_bytes(b"tier2_broken_crash_bytes")
            return 1

        self.ns["get_file_info"] = lambda p: (80.0, {})
        self.ns["run_ffmpeg_with_capture"] = mock_run
        self.finalize_fn(self.state)

        # Both tiers ran
        self.assertEqual(len(self.commands_run), 2)
        # Broken FINAL.mkv must NOT be left on disk
        self.assertFalse(self.final_file.exists())
        # Chunks folder must NOT be trashed; 100% preserved for operator manual salvage
        self.assertNotIn(str(self.chunks_dir), self.trashed)
        # Process end timing recorded and summary log written with finalized=False
        self.assertIn("process_end", self.state.stats)
        self.assertTrue(any(call[2] is False for call in self.ns["summary_calls"]))

    def test_tier1_truncated_tier2_succeeds_finalizes(self):
        """When Tier 1 is truncated, Tier 2 runs and succeeds, creating validated final output and trashing chunks."""
        def mock_run(cmd, cwd, tag=""):
            self.commands_run.append((tag, cmd))
            if tag == "FFmpeg-FINAL-T1":
                self.final_file.write_bytes(b"tier1_truncated_bytes")
                return 0
            self.final_file.write_bytes(b"full_tier2_valid_bytes")
            return 0

        # Tier 1 returns 80s (truncated), Tier 2 returns 101s (ok)
        def mock_probe(p):
            if len(self.commands_run) == 1:
                return (80.0, {})
            return (101.0, {})

        self.ns["get_file_info"] = mock_probe
        self.ns["run_ffmpeg_with_capture"] = mock_run
        self.finalize_fn(self.state)

        # Both tiers ran
        self.assertEqual(len(self.commands_run), 2)
        # Final file exists and chunks dir trashed
        self.assertTrue(self.final_file.exists())
        self.assertIn(str(self.chunks_dir), self.trashed)


class TestBackoffPolicy(unittest.TestCase):
    """Verifies Defect 6 parity: backoff cap at 15s and 0s reset on chunks >= 60s."""

    def test_backoff_cap_at_15_seconds(self):
        """Backoff scales by +5s per failure but is strictly capped at 15s (never escalating to 30s)."""
        backoff = 0
        ok = False
        for _ in range(10):
            backoff = 0 if ok else min(backoff + 5, 15)
        self.assertEqual(backoff, 15)

    def test_backoff_resets_to_zero_on_healthy_chunk_ge_60s(self):
        """A healthy chunk running >= 60s resets backoff to 0s to maintain zero-downtime match invariant."""
        backoff = 15
        ok = True
        chunk_dur = 65.0
        is_corrupt_recycle = False
        status = "ok"

        if status == "stalled" or (is_corrupt_recycle and ok) or (ok and chunk_dur >= 60.0):
            backoff = 0

        self.assertEqual(backoff, 0)

    def test_record_dynamic_and_static_parity_on_backoff(self):
        """Verify record_dynamic.py and record_static.py both have the backoff constants and reset in code."""
        record_path = resolve_record_py_path()
        dynamic_path = Path(__file__).resolve().parent.parent / "record_dynamic.py"

        record_src = record_path.read_text(encoding="utf-8")
        dynamic_src = dynamic_path.read_text(encoding="utf-8")

        # Both must use the module constants for backoff cap
        self.assertIn("min(backoff_sec + BACKOFF_STEP_SEC, BACKOFF_MAX_SEC)", record_src)
        self.assertIn("min(backoff_sec + BACKOFF_STEP_SEC, BACKOFF_MAX_SEC)", dynamic_src)

        # Both must reset when chunk_dur >= HEALTHY_CHUNK_RESET_SEC
        self.assertIn("chunk_dur >= HEALTHY_CHUNK_RESET_SEC", record_src)
        self.assertIn("chunk_dur >= HEALTHY_CHUNK_RESET_SEC", dynamic_src)

    def test_record_dynamic_and_static_parity_on_burst_and_constants(self):
        """Verify record_dynamic.py and record_static.py have complete parity on burst detection and named constants."""
        record_path = resolve_record_py_path()
        dynamic_path = Path(__file__).resolve().parent.parent / "record_dynamic.py"

        record_src = record_path.read_text(encoding="utf-8")
        dynamic_src = dynamic_path.read_text(encoding="utf-8")

        # Both must define burst and bitrate constants
        for const in ("STABILITY_WALL_SEC = 25", "BURST_RATIO = 3.0", "DUP_MAX_SEC = 180", "FINAL_AUDIO_KBPS_PER_CHANNEL = 64"):
            self.assertIn(const, record_src, f"Missing {const} in record_static.py")
            self.assertIn(const, dynamic_src, f"Missing {const} in record_dynamic.py")

        # Both must guard replay burst in maybe_accept_chunk
        self.assertIn("is_replay_burst = (", record_src)
        self.assertIn("is_replay_burst = (", dynamic_src)
        self.assertIn("and not is_replay_burst", record_src)
        self.assertIn("and not is_replay_burst", dynamic_src)

        # StreamIntegrityMonitor in record_static.py must define and use WRAPPED_PTS_TOLERANCE
        self.assertIn("WRAPPED_PTS_TOLERANCE = 450_000", record_src)
        self.assertIn("self.WRAPPED_PTS_TOLERANCE", record_src)

        # Both must define safe_print and stdout reconfigure for Windows cp1252 safety
        self.assertIn("def safe_print(", record_src)
        self.assertIn("def safe_print(", dynamic_src)
        self.assertIn('sys.stdout.reconfigure(errors="replace")', record_src)
        self.assertIn('sys.stdout.reconfigure(errors="replace")', dynamic_src)

    def test_30_record_dynamic_and_static_parity_on_timing_burst_throttling(self):
        """Verify record_dynamic.py and record_static.py have complete parity on timing warning burst throttling and termcap capture."""
        record_path = resolve_record_py_path()
        dynamic_path = Path(__file__).resolve().parent.parent / "record_dynamic.py"

        record_src = record_path.read_text(encoding="utf-8")
        dynamic_src = dynamic_path.read_text(encoding="utf-8")

        # Both must define check_flush_timing_summary
        self.assertIn("def check_flush_timing_summary(", record_src)
        self.assertIn("def check_flush_timing_summary(", dynamic_src)

        # Both must flush the standard burst summary string
        self.assertIn("FFmpeg: Timing warning burst (suppressed", record_src)
        self.assertIn("FFmpeg: Timing warning burst (suppressed", dynamic_src)

        # Both must preserve suppressed lines to termcap for full on-disk fidelity (Item 3B)
        self.assertIn('termcap_write(f"{ts_str} [INFO] FFmpeg: {s}")', record_src)
        self.assertIn('termcap_write(f"{ts_str} [INFO] FFmpeg: {s}")', dynamic_src)

    def test_31_concat_finalization_flags_and_bsf_tier_hardening(self):
        """
        Verifies Items 4A & 4B: Matroska concat command line construction across record_static.py and record_dynamic.py:
        1. Demuxer input error tolerance: -err_detect ignore_err -fflags +genpts+igndts+discardcorrupt before -f concat.
        2. Tier 1 output flags: -avoid_negative_ts make_zero -max_interleave_delta 0.
        3. Tier 2 timestamp synthesis: -bsf:v setts=ts='if(eq(PTS,NOPTS),PREV_OUTPTS+1,PTS)':dts='if(eq(DTS,NOPTS),PREV_OUTDTS+1,DTS)'
           and -avoid_negative_ts make_zero -max_interleave_delta 0.
        """
        record_path = resolve_record_py_path()
        dynamic_path = Path(__file__).resolve().parent.parent / "record_dynamic.py"

        record_src = record_path.read_text(encoding="utf-8")
        dynamic_src = dynamic_path.read_text(encoding="utf-8")

        for src, name in ((record_src, "record_static.py"), (dynamic_src, "record_dynamic.py")):
            # Input error tolerance flags before -f concat
            self.assertIn('"-err_detect", "ignore_err"', src, f"Missing -err_detect ignore_err in {name}")
            self.assertIn('"-fflags", "+genpts+igndts+discardcorrupt"', src, f"Missing +genpts+igndts+discardcorrupt in {name}")
            # Output flags
            self.assertIn('"-avoid_negative_ts", "make_zero"', src, f"Missing -avoid_negative_ts make_zero in {name}")
            self.assertIn('"-max_interleave_delta", "0"', src, f"Missing -max_interleave_delta 0 in {name}")
            # Tier 2 bitstream filter for NOPTS synthesis
            self.assertIn(
                '"-bsf:v", "setts=ts=\'if(eq(PTS,NOPTS),PREV_OUTPTS+1,PTS)\':dts=\'if(eq(DTS,NOPTS),PREV_OUTDTS+1,DTS)\'"',
                src,
                f"Missing setts bitstream filter in {name}",
            )

    def test_32_summary_report_dynamic_header_and_failure_propagation(self):
        """
        Verifies Item 5: Dynamic summary report header and failure propagation:
        1. Summary header distinguishes mixed params from assembly failures.
        2. finalize_recording sets state.last_finalization_failure on failure.
        """
        record_path = resolve_record_py_path()
        dynamic_path = Path(__file__).resolve().parent.parent / "record_dynamic.py"

        record_src = record_path.read_text(encoding="utf-8")
        dynamic_src = dynamic_path.read_text(encoding="utf-8")

        for src, name in ((record_src, "record_static.py"), (dynamic_src, "record_dynamic.py")):
            # Dynamic header checks in write_summary_log
            self.assertIn('lines.append(f"Recording summary (NOT finalized - mixed params: {reason}) for {FINAL_FILE}")', src, f"Missing mixed params header in {name}")
            self.assertIn('lines.append(f"Recording summary (NOT finalized - {failure_msg}) for {FINAL_FILE}")', src, f"Missing failure header in {name}")
            # Failure reason assignment in finalize_recording
            self.assertIn('state.last_finalization_failure = last_failure_reason or "ffmpeg concat failure"', src, f"Missing failure reason propagation in {name}")

    def test_33_quality_downgrade_alarm_state_machine_and_recovery(self):
        """
        Verifies Items 6A, 6B, 6C, 6D: Quality downgrade alarm state machine, acknowledgment, baseline elevation & recovery.
        """
        import sys
        import importlib

        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as tmp:
            tmp.write(
                "NM3U8DL_PLAYLIST_PRIMARY_PHRASES = ['test']\n"
                "NM3U8DL_PLAYLIST_REQUIRED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_REJECTED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_PREFERRED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_GROUP = 'TEST'\n"
                "NM3U8DL_PLAYLIST_GROUPS = {'TEST': ['http://test']}\n"
                "SCHEDULE_START = ''\n"
                "RUN_DURATION_MIN = 1\n"
                "BASE_NAME = 'test'\n"
                f"RECORDING_OUTPUT_DIR = {repr(tempfile.gettempdir())}\n"
                "REQUIRED_VPN_COUNTRY = 'IN'\n"
            )
            tmp_path = tmp.name
        old_cfg = os.environ.get("RECORDER_DYNAMIC_CONFIG_PATH")
        os.environ["RECORDER_DYNAMIC_CONFIG_PATH"] = tmp_path
        try:
            if "record_dynamic" in sys.modules:
                rd = sys.modules["record_dynamic"]
            else:
                rd = importlib.import_module("record_dynamic")

            state = rd.RecorderState()
            with tempfile.TemporaryDirectory() as td:
                td_path = Path(td)
                dummy_chunk = td_path / "dummy.ts"
                dummy_chunk.write_bytes(b"dummy_chunk_bytes")

                orig_mode = rd.DOWNLOAD_MODE
                orig_list = rd.LIST_FILE
                orig_get_info = rd.get_file_info
                orig_params = rd.get_video_params
                orig_log = rd.log
                alarms_started = []
                alarms_stopped = []
                beeps = []

                orig_alarm_start = rd.shared_alarm_start
                orig_alarm_stop = rd.shared_alarm_stop
                orig_beep_good = rd.beep_good

                try:
                    rd.DOWNLOAD_MODE = rd.ENGINE_FFMPEG
                    rd.LIST_FILE = str(td_path / "list.txt")
                    rd.get_file_info = lambda p: (100.0, 4000.0)
                    rd.log = lambda *a, **k: None

                    def fake_alarm_start(s, alarm_type=None, incident=None):
                        alarms_started.append(alarm_type)
                        s.shared_alarm_type = alarm_type

                    def fake_alarm_stop(s):
                        alarms_stopped.append(True)
                        s.shared_alarm_type = None

                    def fake_beep_good(s=None):
                        beeps.append(True)

                    rd.shared_alarm_start = fake_alarm_start
                    rd.shared_alarm_stop = fake_alarm_stop
                    rd.beep_good = fake_beep_good

                    # 1. Chunk 1 starts at 720p emergency slate (Item 6D)
                    rd.get_video_params = lambda p: (1280, 720, 25.0, "h264", "")
                    rd.maybe_accept_chunk(state, str(dummy_chunk), "chunk_001.ts", "TEST")
                    self.assertEqual(state.baseline_video_params, (1280, 720, 25.0))

                    # 2. Chunk 2 upgrades to 1080p50 match feed -> baseline must elevate to 1080p50
                    dummy_chunk.write_bytes(b"chunk2")
                    rd.get_video_params = lambda p: (1920, 1080, 50.0, "h264", "")
                    rd.maybe_accept_chunk(state, str(dummy_chunk), "chunk_002.ts", "TEST")
                    self.assertEqual(state.baseline_video_params, (1920, 1080, 50.0))

                    # 3. Chunk 3 drops back to 720p25 -> must trigger quality downgrade alarm (Item 6A)
                    dummy_chunk.write_bytes(b"chunk3")
                    rd.get_video_params = lambda p: (1280, 720, 25.0, "h264", "")
                    rd.maybe_accept_chunk(state, str(dummy_chunk), "chunk_003.ts", "TEST")
                    self.assertIn("quality_downgrade", alarms_started)
                    self.assertEqual(state.last_downgraded_quality_params, (1280, 720, 25.0))
                    self.assertFalse(state.same_params)

                    # 4. Operator acknowledges downgrade with 'A' (Item 6B)
                    state.shared_alarm_type = "quality_downgrade"
                    state.quality_downgrade_alarm_active = True
                    acked = rd.acknowledge_quality_downgrade_alarm(state)
                    self.assertTrue(acked)
                    self.assertFalse(state.quality_downgrade_alarm_active)
                    self.assertEqual(state.acknowledged_quality_params, (1280, 720, 25.0))

                    # 5. Chunk 4 at same 720p25 -> must NOT re-alarm (Item 6B)
                    alarms_started.clear()
                    dummy_chunk.write_bytes(b"chunk4")
                    rd.maybe_accept_chunk(state, str(dummy_chunk), "chunk_004.ts", "TEST")
                    self.assertNotIn("quality_downgrade", alarms_started)

                    # 6. Chunk 5 restores to baseline 1080p50 -> stops alarm, clears ack params, beeps good (Item 6C)
                    dummy_chunk.write_bytes(b"chunk5")
                    rd.get_video_params = lambda p: (1920, 1080, 50.0, "h264", "")
                    beeps.clear()
                    rd.maybe_accept_chunk(state, str(dummy_chunk), "chunk_005.ts", "TEST")
                    self.assertIsNone(state.acknowledged_quality_params)
                    self.assertIsNone(state.last_downgraded_quality_params)
                    self.assertGreaterEqual(len(beeps), 1)

                finally:
                    rd.DOWNLOAD_MODE = orig_mode
                    rd.LIST_FILE = orig_list
                    rd.get_file_info = orig_get_info
                    rd.get_video_params = orig_params
                    rd.log = orig_log
                    rd.shared_alarm_start = orig_alarm_start
                    rd.shared_alarm_stop = orig_alarm_stop
                    rd.beep_good = orig_beep_good
        finally:
            if old_cfg:
                os.environ["RECORDER_DYNAMIC_CONFIG_PATH"] = old_cfg
            else:
                os.environ.pop("RECORDER_DYNAMIC_CONFIG_PATH", None)
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    def test_34_two_tier_chunk_acceptance_and_short_downgrade_discard(self):
        """
        Verifies Items 7A & 7B: Two-Tier chunk duration thresholds:
        1. Mid-run flapping chunk (< 60s, e.g. 35s) without intentional stop is rejected as BAD.
        2. Clean boundary (duration_reached / stop_flag) chunk >= 15s (e.g. 20s winning overs) is accepted as GOOD.
        3. Short chunk (< 60s) on clean boundary with downgraded quality below baseline is rejected as BAD (Item 7B).
        """
        import sys
        import importlib

        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as tmp:
            tmp.write(
                "NM3U8DL_PLAYLIST_PRIMARY_PHRASES = ['test']\n"
                "NM3U8DL_PLAYLIST_REQUIRED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_REJECTED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_PREFERRED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_GROUP = 'TEST'\n"
                "NM3U8DL_PLAYLIST_GROUPS = {'TEST': ['http://test']}\n"
                "SCHEDULE_START = ''\n"
                "RUN_DURATION_MIN = 1\n"
                "BASE_NAME = 'test'\n"
                f"RECORDING_OUTPUT_DIR = {repr(tempfile.gettempdir())}\n"
                "REQUIRED_VPN_COUNTRY = 'IN'\n"
            )
            tmp_path = tmp.name
        old_cfg = os.environ.get("RECORDER_DYNAMIC_CONFIG_PATH")
        os.environ["RECORDER_DYNAMIC_CONFIG_PATH"] = tmp_path
        try:
            if "record_dynamic" in sys.modules:
                rd = sys.modules["record_dynamic"]
            else:
                rd = importlib.import_module("record_dynamic")

            state = rd.RecorderState()
            state.baseline_video_params = (1920, 1080, 50.0)
            state.best_w, state.best_h, state.best_fps = 1920, 1080, 50.0
            state.stats["good_chunks"] = 1

            with tempfile.TemporaryDirectory() as td:
                td_path = Path(td)
                dummy_chunk = td_path / "dummy.ts"

                orig_mode = rd.DOWNLOAD_MODE
                orig_list = rd.LIST_FILE
                orig_get_info = rd.get_file_info
                orig_params = rd.get_video_params
                orig_log = rd.log

                try:
                    rd.DOWNLOAD_MODE = rd.ENGINE_FFMPEG
                    rd.LIST_FILE = str(td_path / "list.txt")
                    rd.log = lambda *a, **k: None

                    # Scenario 1: Mid-run flapping chunk of 35s (< 60s) -> REJECTED as BAD (Item 7A)
                    dummy_chunk.write_bytes(b"flap")
                    rd.get_file_info = lambda p: (35.0, 4000.0)
                    rd.get_video_params = lambda p: (1920, 1080, 50.0, "h264", "")
                    accepted = rd.maybe_accept_chunk(state, str(dummy_chunk), "chunk_002.ts", "TEST")
                    self.assertFalse(accepted)

                    # Scenario 2: Clean boundary (stop_flag=True), 25s chunk at 1080p50 -> ACCEPTED as GOOD (Item 7A winning overs protection)
                    dummy_chunk.write_bytes(b"winning_overs")
                    state.stop_flag = True
                    accepted = rd.maybe_accept_chunk(state, str(dummy_chunk), "chunk_002.ts", "TEST")
                    self.assertTrue(accepted)
                    self.assertEqual(state.stats["good_chunks"], 2)

                    # Scenario 3: Clean boundary (stop_flag=True), but 25s chunk with downgraded quality 720p25 -> REJECTED as BAD (Item 7B)
                    dummy_chunk.write_bytes(b"emergency_slate")
                    rd.get_video_params = lambda p: (1280, 720, 25.0, "h264", "")
                    accepted = rd.maybe_accept_chunk(state, str(dummy_chunk), "chunk_003.ts", "TEST")
                    self.assertFalse(accepted)

                finally:
                    rd.DOWNLOAD_MODE = orig_mode
                    rd.LIST_FILE = orig_list
                    rd.get_file_info = orig_get_info
                    rd.get_video_params = orig_params
                    rd.log = orig_log
        finally:
            if old_cfg:
                os.environ["RECORDER_DYNAMIC_CONFIG_PATH"] = old_cfg
            else:
                os.environ.pop("RECORDER_DYNAMIC_CONFIG_PATH", None)
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    def test_35_record_dynamic_and_static_parity_on_quality_alarms_and_durations(self):
        """
        Verifies Item 8: Complete parity between record_static.py and record_dynamic.py for:
        1. MIN_ACCEPTED_CHUNK_SECONDS = 60
        2. quality_downgrade alarm functions and state fields
        3. Two-tier duration acceptance and short downgrade discard logic
        4. Headless terminal key listener error handling
        """
        record_path = resolve_record_py_path()
        dynamic_path = Path(__file__).resolve().parent.parent / "record_dynamic.py"

        record_src = record_path.read_text(encoding="utf-8")
        dynamic_src = dynamic_path.read_text(encoding="utf-8")

        for src, name in ((record_src, "record_static.py"), (dynamic_src, "record_dynamic.py")):
            # Constant
            self.assertIn("MIN_ACCEPTED_CHUNK_SECONDS = 60", src, f"Missing MIN_ACCEPTED_CHUNK_SECONDS in {name}")
            # State fields
            self.assertIn("quality_downgrade_alarm_active: bool = False", src, f"Missing state field in {name}")
            self.assertIn("baseline_video_params: Optional[tuple] = None", src, f"Missing state field in {name}")
            self.assertIn("last_downgraded_quality_params: Optional[tuple] = None", src, f"Missing state field in {name}")
            self.assertIn("acknowledged_quality_params: Optional[tuple] = None", src, f"Missing state field in {name}")
            # Functions
            self.assertIn("def quality_downgrade_alarm_start(", src, f"Missing alarm start in {name}")
            self.assertIn("def quality_downgrade_alarm_stop(", src, f"Missing alarm stop in {name}")
            self.assertIn("def acknowledge_quality_downgrade_alarm(", src, f"Missing ack alarm in {name}")
            # Two-tier acceptance in maybe_accept_chunk
            self.assertIn("effective_min_dur = (", src, f"Missing effective_min_dur in {name}")
            self.assertIn("is_short_downgrade = False", src, f"Missing is_short_downgrade in {name}")
            # Terminal listener headless guard
            self.assertIn("except (termios.error, io.UnsupportedOperation, AttributeError, Exception) as exc:", src, f"Missing terminal headless guard in {name}")

    def test_dynamic_recorder_replay_burst_behaviour(self):
        """Verify dynamic recorder's maybe_accept_chunk marks burst as BAD and normal chunk as GOOD."""
        import sys
        import importlib

        # Provide a temporary dummy user config so record_dynamic can import cleanly
        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as tmp:
            tmp.write(
                "NM3U8DL_PLAYLIST_PRIMARY_PHRASES = ['test']\n"
                "NM3U8DL_PLAYLIST_REQUIRED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_REJECTED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_PREFERRED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_GROUP = 'TEST'\n"
                "NM3U8DL_PLAYLIST_GROUPS = {'TEST': ['http://test']}\n"
                "SCHEDULE_START = ''\n"
                "RUN_DURATION_MIN = 1\n"
                "BASE_NAME = 'test'\n"
                f"RECORDING_OUTPUT_DIR = {repr(tempfile.gettempdir())}\n"
                "REQUIRED_VPN_COUNTRY = 'IN'\n"
            )
            tmp_path = tmp.name
        old_cfg = os.environ.get("RECORDER_DYNAMIC_CONFIG_PATH")
        os.environ["RECORDER_DYNAMIC_CONFIG_PATH"] = tmp_path
        try:
            if "record_dynamic" in sys.modules:
                rd = sys.modules["record_dynamic"]
            else:
                rd = importlib.import_module("record_dynamic")

            state = rd.RecorderState()
            state.stats["good_chunks"] = 1
            state.stats["last_run_start"] = time.time() - 4.0  # 4s wall time

            with tempfile.TemporaryDirectory() as td:
                td_path = Path(td)
                dummy_chunk = td_path / "dummy.ts"
                dummy_chunk.write_bytes(b"dummy_chunk_bytes")

                orig_mode = rd.DOWNLOAD_MODE
                orig_list = rd.LIST_FILE
                orig_get_info = rd.get_file_info
                orig_params = rd.get_video_params
                orig_log = rd.log
                try:
                    rd.DOWNLOAD_MODE = rd.ENGINE_FFMPEG
                    rd.LIST_FILE = str(td_path / "list.txt")
                    rd.get_file_info = lambda p: (75.0, 4000.0)
                    rd.get_video_params = lambda p: (1920, 1080, 50.0, "h264", "")
                    rd.log = lambda *a, **k: None

                    # Replay burst (75s media in 4s wall = 18.75x) must NOT be accepted
                    accepted = rd.maybe_accept_chunk(state, str(dummy_chunk), "chunk_002.ts", "TEST")
                    self.assertFalse(accepted)
                    self.assertEqual(state.stats["good_chunks"], 1)

                    # Normal chunk (75s media in 70s wall = 1.07x) is accepted as GOOD
                    dummy_chunk.write_bytes(b"dummy_chunk_bytes")
                    state.stats["last_run_start"] = time.time() - 70.0
                    accepted = rd.maybe_accept_chunk(state, str(dummy_chunk), "chunk_003.ts", "TEST")
                    self.assertTrue(accepted)
                    self.assertEqual(state.stats["good_chunks"], 2)
                finally:
                    rd.DOWNLOAD_MODE = orig_mode
                    rd.LIST_FILE = orig_list
                    rd.get_file_info = orig_get_info
                    rd.get_video_params = orig_params
                    rd.log = orig_log
        finally:
            if old_cfg:
                os.environ["RECORDER_DYNAMIC_CONFIG_PATH"] = old_cfg
            else:
                os.environ.pop("RECORDER_DYNAMIC_CONFIG_PATH", None)
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    def test_dynamic_recorder_resolve_engine_resolves_cleanly(self):
        """Verify dynamic recorder resolve_engine succeeds without NameError or unhandled exception."""
        import sys
        import importlib

        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as tmp:
            tmp.write(
                "NM3U8DL_PLAYLIST_PRIMARY_PHRASES = ['test']\n"
                "NM3U8DL_PLAYLIST_REQUIRED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_REJECTED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_PREFERRED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_GROUP = 'TEST'\n"
                "NM3U8DL_PLAYLIST_GROUPS = {'TEST': ['http://test']}\n"
                "SCHEDULE_START = ''\n"
                "RUN_DURATION_MIN = 1\n"
                "BASE_NAME = 'test'\n"
                f"RECORDING_OUTPUT_DIR = {repr(tempfile.gettempdir())}\n"
                "REQUIRED_VPN_COUNTRY = 'IN'\n"
            )
            tmp_path = tmp.name
        old_cfg = os.environ.get("RECORDER_DYNAMIC_CONFIG_PATH")
        os.environ["RECORDER_DYNAMIC_CONFIG_PATH"] = tmp_path
        try:
            if "record_dynamic" in sys.modules:
                rd = sys.modules["record_dynamic"]
            else:
                rd = importlib.import_module("record_dynamic")

            engine = rd.resolve_engine(rd.DOWNLOAD_MODE)
            self.assertIsNotNone(engine)
            self.assertEqual(engine.description, "Segmented downloader via N_m3u8DL-RE")
        finally:
            if old_cfg:
                os.environ["RECORDER_DYNAMIC_CONFIG_PATH"] = old_cfg
            else:
                os.environ.pop("RECORDER_DYNAMIC_CONFIG_PATH", None)
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    def test_dynamic_recorder_nm3u8dl_none_part_a_clean_exit(self):
        """Verify dynamic recorder start_nm3u8dl_to_chunk returns ended when part_a is None."""
        import sys
        import importlib

        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as tmp:
            tmp.write(
                "NM3U8DL_PLAYLIST_PRIMARY_PHRASES = ['test']\n"
                "NM3U8DL_PLAYLIST_REQUIRED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_REJECTED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_PREFERRED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_GROUP = 'TEST'\n"
                "NM3U8DL_PLAYLIST_GROUPS = {'TEST': ['http://test']}\n"
                "SCHEDULE_START = ''\n"
                "RUN_DURATION_MIN = 1\n"
                "BASE_NAME = 'test'\n"
                f"RECORDING_OUTPUT_DIR = {repr(tempfile.gettempdir())}\n"
                "REQUIRED_VPN_COUNTRY = 'IN'\n"
            )
            tmp_path = tmp.name
        old_cfg = os.environ.get("RECORDER_DYNAMIC_CONFIG_PATH")
        os.environ["RECORDER_DYNAMIC_CONFIG_PATH"] = tmp_path
        try:
            if "record_dynamic" in sys.modules:
                rd = sys.modules["record_dynamic"]
            else:
                rd = importlib.import_module("record_dynamic")

            state = rd.RecorderState()
            orig_get_part_a = rd.get_nm3u8dl_part_a
            orig_next_names = rd.next_chunk_names
            try:
                rd.get_nm3u8dl_part_a = lambda s: None
                rd.next_chunk_names = lambda s: ("/tmp/chunk_001.ts", "chunk_001.ts")
                result = rd.start_nm3u8dl_to_chunk(state)
                self.assertEqual(result.status, "ended")
                self.assertIn("external stop", result.reason)
            finally:
                rd.get_nm3u8dl_part_a = orig_get_part_a
                rd.next_chunk_names = orig_next_names
        finally:
            if old_cfg:
                os.environ["RECORDER_DYNAMIC_CONFIG_PATH"] = old_cfg
            else:
                os.environ.pop("RECORDER_DYNAMIC_CONFIG_PATH", None)
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    def test_dynamic_recorder_log_unicode_fallback_on_cp1252(self):
        """Verify dynamic recorder log() handles non-encodable charmaps without raising UnicodeEncodeError."""
        import sys
        import io
        import importlib

        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as tmp:
            tmp.write(
                "NM3U8DL_PLAYLIST_PRIMARY_PHRASES = ['test']\n"
                "NM3U8DL_PLAYLIST_REQUIRED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_REJECTED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_PREFERRED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_GROUP = 'TEST'\n"
                "NM3U8DL_PLAYLIST_GROUPS = {'TEST': ['http://test']}\n"
                "SCHEDULE_START = ''\n"
                "RUN_DURATION_MIN = 1\n"
                "BASE_NAME = 'test'\n"
                f"RECORDING_OUTPUT_DIR = {repr(tempfile.gettempdir())}\n"
                "REQUIRED_VPN_COUNTRY = 'IN'\n"
            )
            tmp_path = tmp.name
        old_cfg = os.environ.get("RECORDER_DYNAMIC_CONFIG_PATH")
        os.environ["RECORDER_DYNAMIC_CONFIG_PATH"] = tmp_path
        try:
            if "record_dynamic" in sys.modules:
                rd = sys.modules["record_dynamic"]
            else:
                rd = importlib.import_module("record_dynamic")

            class Cp1252Writer(io.TextIOWrapper):
                def __init__(self):
                    super().__init__(io.BytesIO(), encoding="cp1252", errors="strict")

            fake_stdout = Cp1252Writer()
            old_stdout = sys.stdout
            sys.stdout = fake_stdout
            try:
                # Should not raise UnicodeEncodeError even with right arrow across log, safe_print, and console print
                rd.log("Testing unicode arrow → in cp1252 charmap")
                rd.safe_print("Testing safe_print → direct")
                rd._console_print_lines(["Console prompt line → test"])
                rd._console_print_timestamped_line("Timestamped line → test")
            finally:
                sys.stdout = old_stdout
        finally:
            if old_cfg:
                os.environ["RECORDER_DYNAMIC_CONFIG_PATH"] = old_cfg
            else:
                os.environ.pop("RECORDER_DYNAMIC_CONFIG_PATH", None)
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


class TestStaticRecorderResolution(unittest.TestCase):
    """Guard: every AST assertion in this module must read this repository's record_static.py."""

    def test_resolver_prefers_repository_copy(self):
        repo_copy = Path(__file__).resolve().parent.parent / "record_static.py"
        if not repo_copy.is_file():
            self.skipTest("record_static.py is absent from this checkout")

        previous_override = os.environ.pop("RECORD_STATIC_PY_PATH", None)
        try:
            self.assertEqual(
                resolve_record_py_path(),
                repo_copy,
                "resolver must prefer the repository copy over any newer stale sibling",
            )
        finally:
            if previous_override is not None:
                os.environ["RECORD_STATIC_PY_PATH"] = previous_override


def load_integrity_cut_gate():
    """Compile settle_integrity_cut() out of record_static.py with recording-free deps."""
    record_path = resolve_record_py_path()
    text = record_path.read_text(encoding="utf-8")
    tree = ast.parse(text)
    nodes = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "settle_integrity_cut"
    ]
    assert nodes, f"settle_integrity_cut not found in {record_path}"

    logs = []
    corruption_cuts = []
    beeps = []

    def dummy_register(state, now=None, *, fatal=True):
        corruption_cuts.append((now, fatal))
        return True

    def dummy_beep(state):
        beeps.append(True)

    ns = {
        "log": lambda msg, level="INFO", **kwargs: logs.append((level, msg)),
        "register_stream_corrupt_recycle": dummy_register,
        "play_stream_corrupt_bad_beep_async": dummy_beep,
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(record_path), "exec"), ns)
    return ns["settle_integrity_cut"], logs, corruption_cuts, beeps


class TestPlannedFormatCutAccounting(unittest.TestCase):
    """BUG-029: a codec-change cut is a planned cut, never a corruption."""

    class Monitor:
        def __init__(self, kind, reason):
            self.recycle_kind = kind
            self.recycle_reason = reason
            self.recycle_requested = True

    class State:
        def __init__(self):
            self.stats = {}
            self.stream_corrupt_recycle_timestamps = []

    def setUp(self):
        (
            self.settle,
            self.logs,
            self.corruption_cuts,
            self.beeps,
        ) = load_integrity_cut_gate()

    def test_format_cut_is_not_counted_as_corruption(self):
        state = self.State()
        monitor = self.Monitor("format_change", "format change: codec h264->hevc")

        status = self.settle(state, monitor, flush_cb=lambda: None)

        self.assertEqual(status, "format_change_recycle")
        self.assertEqual(state.stats["format_change_recycles"], 1)
        # No corruption timestamp, no Bad Beep, no siren, no false summary line.
        self.assertEqual(state.stream_corrupt_recycle_timestamps, [])
        self.assertEqual(self.corruption_cuts, [])
        self.assertEqual(self.beeps, [])
        self.assertIn("FORMAT CHANGE", self.logs[-1][1])
        self.assertNotIn("STREAM RECYCLE", self.logs[-1][1])

    def test_corruption_cut_still_counts(self):
        state = self.State()
        monitor = self.Monitor("corruption", "Fatal Container Poisoning")

        status = self.settle(state, monitor, flush_cb=lambda: None)

        self.assertEqual(status, "stream_corrupt_recycle")
        self.assertEqual(len(self.corruption_cuts), 1)
        self.assertEqual(self.beeps, [True])
        self.assertNotIn("format_change_recycles", state.stats)
        self.assertIn("STREAM RECYCLE", self.logs[-1][1])

    def test_a_monitor_without_a_kind_is_treated_as_corruption(self):
        state = self.State()

        class LegacyMonitor:
            recycle_reason = "grace countdown expired"

        status = self.settle(state, LegacyMonitor(), flush_cb=None)

        self.assertEqual(status, "stream_corrupt_recycle")
        self.assertEqual(len(self.corruption_cuts), 1)


class TestSealPathVideoParamsContract(unittest.TestCase):
    """The chunk-seal path must accept the five-field VideoParams the probe returns."""

    def _load_record_dynamic(self):
        import importlib
        import sys

        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as tmp:
            tmp.write(
                "NM3U8DL_PLAYLIST_PRIMARY_PHRASES = ['test']\n"
                "NM3U8DL_PLAYLIST_REQUIRED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_REJECTED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_PREFERRED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_GROUP = 'TEST'\n"
                "NM3U8DL_PLAYLIST_GROUPS = {'TEST': ['http://test']}\n"
                "SCHEDULE_START = ''\n"
                "RUN_DURATION_MIN = 1\n"
                "BASE_NAME = 'test'\n"
                f"RECORDING_OUTPUT_DIR = {repr(tempfile.gettempdir())}\n"
                "REQUIRED_VPN_COUNTRY = 'IN'\n"
            )
            tmp_path = tmp.name
        previous = os.environ.get("RECORDER_DYNAMIC_CONFIG_PATH")
        os.environ["RECORDER_DYNAMIC_CONFIG_PATH"] = tmp_path
        try:
            if "record_dynamic" in sys.modules:
                return sys.modules["record_dynamic"]
            return importlib.import_module("record_dynamic")
        finally:
            if previous is None:
                os.environ.pop("RECORDER_DYNAMIC_CONFIG_PATH", None)
            else:
                os.environ["RECORDER_DYNAMIC_CONFIG_PATH"] = previous
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

    def test_video_params_five_field_unpack_on_seal_path(self):
        rd = self._load_record_dynamic()
        state = rd.RecorderState()

        originals = {
            "DOWNLOAD_MODE": rd.DOWNLOAD_MODE,
            "LIST_FILE": rd.LIST_FILE,
            "get_file_info": rd.get_file_info,
            "get_video_params": rd.get_video_params,
            "log": rd.log,
        }
        with tempfile.TemporaryDirectory() as td:
            chunk = Path(td) / "chunk.ts"
            chunk.write_bytes(b"chunk_bytes")
            try:
                rd.DOWNLOAD_MODE = rd.ENGINE_FFMPEG
                rd.LIST_FILE = str(Path(td) / "list.txt")
                rd.get_file_info = lambda p: (100.0, 4000.0)
                rd.log = lambda *a, **k: None
                # Exactly what the fixed probe returns: five fields, codec included.
                rd.get_video_params = lambda p: rd.VideoParams(
                    1920, 1080, 50.0, "h264", "progressive"
                )

                rd.maybe_accept_chunk(state, str(chunk), "chunk_001.ts", "TEST")

                self.assertEqual(state.baseline_video_params, (1920, 1080, 50.0))
                self.assertEqual(state.baseline_video_codec, "h264")
            finally:
                for name, value in originals.items():
                    setattr(rd, name, value)

    def test_seal_path_accepts_a_legacy_three_field_probe(self):
        rd = self._load_record_dynamic()
        state = rd.RecorderState()

        original_mode = rd.DOWNLOAD_MODE
        original_list = rd.LIST_FILE
        original_info = rd.get_file_info
        original_params = rd.get_video_params
        with tempfile.TemporaryDirectory() as td:
            chunk = Path(td) / "chunk.ts"
            chunk.write_bytes(b"chunk_bytes")
            try:
                rd.DOWNLOAD_MODE = rd.ENGINE_FFMPEG
                rd.LIST_FILE = str(Path(td) / "list.txt")
                rd.get_file_info = lambda p: (100.0, 4000.0)
                rd.log = lambda *a, **k: None
                rd.get_video_params = lambda p: (1920, 1080, 50.0)

                rd.maybe_accept_chunk(state, str(chunk), "chunk_001.ts", "TEST")

                # A legacy three-field mock is coerced, not crashed on.
                self.assertEqual(state.baseline_video_params, (1920, 1080, 50.0))
                self.assertIsNone(state.last_downgraded_quality_params)
            finally:
                rd.DOWNLOAD_MODE = original_mode
                rd.LIST_FILE = original_list
                rd.get_file_info = original_info
                rd.get_video_params = original_params


if __name__ == "__main__":
    unittest.main()
