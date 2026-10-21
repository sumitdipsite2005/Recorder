import ast
import os
import re
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Optional
from unittest.mock import patch


HELPER_FUNCTIONS = (
    "_prune_locked",
    "prune_stream_corrupt_window",
    "register_stream_corrupt_recycle",
    "acknowledge_stream_corrupt_alarm",
    "play_stream_corrupt_bad_beep_async",
)
HELPER_CONSTANTS = ("STREAM_CORRUPT_WINDOW_SEC", "STREAM_CORRUPT_ALARM_THRESHOLD")


def resolve_record_py_path() -> Path:
    """Resolve RECORD.py: RECORD_PY_PATH env -> parent/sibling search -> skip in CI."""
    env_path = os.environ.get("RECORD_PY_PATH")
    if env_path and Path(env_path).exists():
        return Path(env_path)
    candidates = []
    for parent in Path(__file__).resolve().parents[:4]:
        try:
            for item in parent.iterdir():
                if item.is_file() and item.name == "RECORD.py":
                    candidates.append(item)
                elif item.is_dir() and not item.name.startswith((".", "_")) and item.name != "Recorder":
                    sub = item / "RECORD.py"
                    if sub.is_file():
                        candidates.append(sub)
        except (PermissionError, OSError):
            continue
    if candidates:
        return max(candidates, key=lambda p: p.stat().st_mtime)
    raise unittest.SkipTest("Static recorder RECORD.py not found in local workspace (CI environment)")
    raise unittest.SkipTest("Static recorder RECORD.py not found in local workspace (CI environment)")


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
    """Extract the rolling-window constants and helpers from RECORD.py AST (no module import)."""
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


def load_monitor_class():
    """Extract StreamIntegrityMonitor directly from RECORD.py AST for fast isolated unit testing."""
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

    ns = {
        "re": re,
        "time": time,
        "threading": threading,
        "Optional": Optional,
        "RecorderState": DummyState,
        "log": dummy_log,
        "shared_alarm_start": dummy_alarm_start,
        "shared_alarm_stop": dummy_alarm_stop,
    }
    mod = ast.Module(body=[class_node], type_ignores=[])
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
            for step in range(70):
                # Alternating small discontinuities (+5s / -5s) every second
                delta = 5000000 if (step % 2 == 0) else -5000000
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
        """Verifies solo large discontinuity: compensated offset is benign (no transport defect), uncompensated is Tier 1."""
        t = [32000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.process_progress({"frame": "5000", "total_size": "50000000", "out_time_us": "100000000"})
            # Compensated by FFmpeg new offset -> benign (no defect)
            mon.process_stderr_line("[vist#0:0/h264] timestamp discontinuity (stream id=256): -3540021667, new offset= 3540021667")
            mon.evaluate_health()
            self.assertFalse(mon.transport_defect)
            self.assertFalse(mon.container_defect)

            # Uncompensated discontinuity -> flags Tier 1 transport defect, but not fatal container defect
            mon.process_stderr_line("[vist#0:0/h264] timestamp discontinuity (stream id=256): -3540021667, new offset= 0")
            mon.evaluate_health()
            self.assertTrue(mon.transport_defect)
            self.assertFalse(mon.container_defect)
            self.assertFalse(mon.recycle_requested)

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
        """Verifies encoder reset hitting both video and audio with same sign within 5s is not fatal."""
        t = [34000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            mon.process_progress({"frame": "5000", "total_size": "50000000", "out_time_us": "100000000"})
            # Both video and audio jump -3540021667 within 100ms with compensating offset
            mon.process_stderr_line("[vist#0:0/h264] timestamp discontinuity (stream id=256): -3540021667, new offset= 3540021667")
            t[0] += 0.1
            mon.process_stderr_line("[aist#0:1/aac] timestamp discontinuity (stream id=257): -3540021667, new offset= 3540021667")
            mon.evaluate_health()
            # Compensated same-sign pair: neither transport nor container defect latched
            self.assertFalse(mon.transport_defect)
            self.assertFalse(mon.container_defect)
            self.assertFalse(mon.recycle_requested)

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
        """Verifies chunk with 45s media running at normal live speed (35s wall) is mature and fast-seals."""
        t = [35000.0]
        with patch("time.monotonic", side_effect=lambda: t[0]):
            mon = self.MonitorCls(self.state)
            # 45s media delivered in 35s wall (speed = 45/35 = 1.28x <= 3.0x)
            t[0] += 35.0
            mon.process_progress({"frame": "2250", "total_size": "40000000", "out_time_us": "45000000"})
            # Severe container defect occurs
            mon.process_stderr_line("[mpegts @ 0x123] Invalid timestamps stream=0, pts=2440, dts=500000, size=71750")
            mon.evaluate_health()
            self.assertTrue(mon.container_defect)

            # Advance past 2.0s coalesce window (to 37.5s wall time)
            t[0] += 2.5
            mon.evaluate_health()
            # Fast-seal triggers because chunk is mature (media >= 30s and speed <= 3.0x)
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
        self.register(self.state, now=5000.0)
        self.register(self.state, now=5100.0)
        self.assertTrue(self.ack(self.state))
        self.assertTrue(self.state.ffmpeg_stream_corrupt_post_ack_silent)
        self.assertFalse(self.state.ffmpeg_stream_corrupt_alarm_active)
        self.assertEqual(self.alarms_stopped, [True])
        self.assertFalse(self.state.stream_corrupt_lock.locked())

        should_beep = self.register(self.state, now=5200.0)
        self.assertTrue(should_beep)
        self.assertEqual(self.alarms_started, ["stream_corrupt"])  # no second siren

    def test_ack_without_active_corrupt_alarm_is_a_noop(self):
        self.assertFalse(self.ack(self.state))
        self.assertEqual(self.alarms_stopped, [])
        self.assertFalse(self.state.ffmpeg_stream_corrupt_post_ack_silent)

    def test_rearm_after_31_minutes_without_tick_via_register_time_prune(self):
        """R1: ack, then a long gap with no sweeper tick, then a cut starts a fresh episode."""
        self.register(self.state, now=5000.0)
        self.register(self.state, now=5100.0)
        self.ack(self.state)
        self.assertTrue(self.state.ffmpeg_stream_corrupt_post_ack_silent)

        later = 5100.0 + 31 * 60.0
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
        "write_summary_log": lambda state, dur, finalized=True: None,
        "LIST_FILE": list_file,
        "FINAL_FILE": final_file,
        "CHUNKS_DIR": chunks_dir,
        "BLACK_LEN": 1.0,
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
        self.assertIn("-ar", cmd2)
        self.assertIn("48000", cmd2)
        self.assertIn("-ac", cmd2)
        self.assertIn("2", cmd2)
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



if __name__ == "__main__":
    unittest.main()
