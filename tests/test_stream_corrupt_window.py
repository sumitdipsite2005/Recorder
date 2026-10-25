from __future__ import annotations

import ast
import threading
import time
import unittest
from pathlib import Path
from typing import Optional

from test_alarm_sound import LogSink, recorder_path


# BUG-032: 'A' must buy five minutes of silence, not a permanent deafness.
WINDOW_HELPERS = (
    "_fmt_monotonic_clock",
    "acknowledge_stream_corrupt_alarm",
    "_prune_locked",
    "_start_corrupt_siren_if_due",
    "prune_stream_corrupt_window",
    "register_stream_corrupt_recycle",
    "_auto_silence_shared_alarm",
)
WINDOW_CONSTANTS = (
    "STREAM_CORRUPT_WINDOW_SEC",
    "STREAM_CORRUPT_ALARM_THRESHOLD",
    "STREAM_CORRUPT_ACK_QUIET_SEC",
)


class DummyState:
    """Minimal RecorderState stand-in for the corruption window helpers."""

    def __init__(self, lock=None):
        self.ffmpeg_stream_corrupt_incidents = 0
        self.ffmpeg_stream_corrupt_nonfatal_incidents = 0
        self.ffmpeg_stream_corrupt_alarm_active = False
        self.ffmpeg_stream_corrupt_post_ack_silent = False
        self.ffmpeg_stream_corrupt_quiet_until = None
        self.ffmpeg_stream_corrupt_quiet_fatal_snapshot = 0
        self.ffmpeg_stream_corrupt_quiet_nonfatal_snapshot = 0
        self.ffmpeg_stream_corrupt_fatal_cuts = 0
        self.stream_corrupt_recycle_timestamps = []
        self.stream_corrupt_lock = lock or threading.Lock()
        self.shared_alarm_type = None
        self.shared_alarm_thread = None
        self.shared_alarm_activity_id = None
        self.alarm_ack_requested = False
        self.alarm_linger_until = None


class HeldRecorder:
    """shared_alarm_start stand-in that records whether the lock was held."""

    def __init__(self):
        self.calls = []

    def __call__(self, state, alarm_type=None, incident=None):
        self.calls.append((alarm_type, getattr(state.stream_corrupt_lock, "held", None)))
        state.shared_alarm_type = alarm_type
        state.ffmpeg_stream_corrupt_alarm_active = True


class TrackingLock:
    """Lock proxy that records whether it is held at a given moment."""

    def __init__(self):
        self._lock = threading.Lock()
        self.held = False

    def __enter__(self):
        self._lock.acquire()
        self.held = True
        return self

    def __exit__(self, *exc):
        self.held = False
        self._lock.release()
        return False


def load_window_helpers():
    path = recorder_path()
    tree = ast.parse(path.read_text(encoding="utf-8"))
    wanted = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in WINDOW_HELPERS:
            wanted.append(node)
        elif isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id in WINDOW_CONSTANTS
            for target in node.targets
        ):
            wanted.append(node)
    found = {n.name for n in wanted if isinstance(n, ast.FunctionDef)}
    missing = sorted(set(WINDOW_HELPERS) - found)
    if missing:
        raise AssertionError(f"record_static.py is missing window helpers: {missing}")
    module = ast.Module(body=wanted, type_ignores=[])
    namespace = {
        "__file__": str(path),
        "time": time,
        "Optional": Optional,
        "log": LogSink(),
        "shared_alarm_start": HeldRecorder(),
        "shared_alarm_stop": lambda state: None,
        "is_alarm_active": lambda state: bool(
            getattr(state, "shared_alarm_type", None)
            or getattr(state, "ffmpeg_stream_corrupt_alarm_active", False)
        ),
        "shared_alarm_is_active": lambda state: True,
        "set_terminal_activity_context": lambda activity_id: None,
        "CONTINUOUS_ALARM_MAX_SEC": 300,
    }
    exec(compile(module, str(path), "exec"), namespace)
    return namespace


class StreamCorruptWindowTests(unittest.TestCase):
    def setUp(self):
        self.ns = load_window_helpers()
        self.state = DummyState(lock=TrackingLock())
        self.register = self.ns["register_stream_corrupt_recycle"]
        self.siren = self.ns["shared_alarm_start"]
        self.base = time.monotonic()

    def _acked_episode(self):
        """Two fatal cuts then 'A': the reported overnight failure."""
        self.register(self.state, now=self.base)
        self.register(self.state, now=self.base + 10)
        self.assertTrue(self.state.ffmpeg_stream_corrupt_alarm_active)
        self.assertTrue(self.ns["acknowledge_stream_corrupt_alarm"](self.state))
        self.assertEqual(len(self.siren.calls), 1)
        return self.state.ffmpeg_stream_corrupt_quiet_until

    def test_ack_opens_quiet_window_and_snapshots_counters(self):
        self.state.ffmpeg_stream_corrupt_fatal_cuts = 4
        self.state.ffmpeg_stream_corrupt_nonfatal_incidents = 2
        self.state.ffmpeg_stream_corrupt_alarm_active = True
        self.state.shared_alarm_type = "stream_corrupt"
        before = time.monotonic()
        self.assertTrue(self.ns["acknowledge_stream_corrupt_alarm"](self.state))
        quiet = self.state.ffmpeg_stream_corrupt_quiet_until
        self.assertAlmostEqual(
            quiet, before + self.ns["STREAM_CORRUPT_ACK_QUIET_SEC"], delta=1.0
        )
        self.assertEqual(self.state.ffmpeg_stream_corrupt_quiet_fatal_snapshot, 4)
        self.assertEqual(self.state.ffmpeg_stream_corrupt_quiet_nonfatal_snapshot, 2)
        self.assertTrue(self.state.ffmpeg_stream_corrupt_post_ack_silent)

    def test_fatal_cut_during_quiet_window_is_withheld_not_rearmed(self):
        quiet = self._acked_episode()
        warns_before = len(self.ns["log"].messages(level="WARN"))
        self.register(self.state, now=self.base + 30)
        self.assertEqual(self.state.ffmpeg_stream_corrupt_quiet_until, quiet)
        self.assertTrue(self.state.ffmpeg_stream_corrupt_post_ack_silent)
        self.assertEqual(len(self.siren.calls), 1, "no second siren while the window is open")
        withheld = self.ns["log"].messages(level="WARN")[warns_before:]
        self.assertEqual(len(withheld), 1, withheld)
        self.assertIn("siren withheld", withheld[0])

    def test_quiet_window_is_never_closed_early(self):
        quiet = self._acked_episode()
        self.register(self.state, now=self.base + 30)
        self.register(self.state, now=self.base + 120)
        for tick in (self.base + 60, self.base + 180, self.base + 299):
            self.ns["prune_stream_corrupt_window"](self.state, now=tick)
            self.assertEqual(self.state.ffmpeg_stream_corrupt_quiet_until, quiet)
        self.assertEqual(len(self.siren.calls), 1)

    def test_nonfatal_cut_during_quiet_window_does_not_rearm(self):
        self._acked_episode()
        incidents = self.state.ffmpeg_stream_corrupt_incidents
        timestamps = list(self.state.stream_corrupt_recycle_timestamps)
        self.assertFalse(self.register(self.state, now=self.base + 30, fatal=False))
        self.assertEqual(self.state.ffmpeg_stream_corrupt_incidents, incidents)
        self.assertEqual(self.state.stream_corrupt_recycle_timestamps, timestamps)
        self.assertEqual(self.state.ffmpeg_stream_corrupt_nonfatal_incidents, 1)
        self.assertIsNotNone(self.state.ffmpeg_stream_corrupt_quiet_until)

    def test_nonfatal_event_never_counts_toward_the_fatal_threshold(self):
        self.register(self.state, now=self.base, fatal=False)
        self.register(self.state, now=self.base + 1, fatal=True)
        self.assertEqual(len(self.state.stream_corrupt_recycle_timestamps), 1)
        self.assertEqual(self.state.ffmpeg_stream_corrupt_incidents, 1)
        self.assertEqual(self.state.ffmpeg_stream_corrupt_nonfatal_incidents, 1)
        self.assertEqual(self.siren.calls, [], "one glitch plus one cut is not a siren")

    def test_quiet_window_expiry_without_cuts_rearms_silently(self):
        quiet = self._acked_episode()
        infos_before = len(self.ns["log"].messages(level="INFO"))
        self.ns["prune_stream_corrupt_window"](self.state, now=quiet + 1)
        self.assertIsNone(self.state.ffmpeg_stream_corrupt_quiet_until)
        self.assertFalse(self.state.ffmpeg_stream_corrupt_post_ack_silent)
        self.assertEqual(len(self.siren.calls), 1, "a clean five minutes must not ring")
        infos = self.ns["log"].messages(level="INFO")[infos_before:]
        self.assertTrue(any("expired with no fatal cuts" in line for line in infos), infos)

    def test_quiet_window_expiry_with_cuts_rearms_siren(self):
        quiet = self._acked_episode()
        self.register(self.state, now=self.base + 30)
        self.ns["prune_stream_corrupt_window"](self.state, now=quiet + 1)
        self.assertIsNone(self.state.ffmpeg_stream_corrupt_quiet_until)
        self.assertFalse(self.state.ffmpeg_stream_corrupt_post_ack_silent)
        self.assertEqual(len(self.siren.calls), 2)
        self.assertEqual(self.siren.calls[-1][0], "stream_corrupt")
        self.assertTrue(
            any("1 fatal cut(s) unaddressed" in line for line in self.ns["log"].messages()),
            self.ns["log"].messages(),
        )

    def test_expiry_rearm_survives_aged_out_cuts(self):
        quiet = self._acked_episode()
        # One cut landed inside the acknowledged window, but the older cuts aged out of
        # the 1800s rolling window while the 300s quiet window was running.
        self.state.stream_corrupt_recycle_timestamps = [self.base + 30]
        self.state.ffmpeg_stream_corrupt_fatal_cuts = self.state.ffmpeg_stream_corrupt_quiet_fatal_snapshot + 1
        self.ns["prune_stream_corrupt_window"](self.state, now=quiet + 1)
        self.assertEqual(len(self.siren.calls), 2, "the expiry siren must bypass the 2-cut threshold")
        self.assertEqual(self.siren.calls[-1][0], "stream_corrupt")

    def test_expiry_rearm_not_bypassed_for_a_fresh_threshold(self):
        self.ns["_start_corrupt_siren_if_due"](self.state, 1, reason="threshold")
        self.assertEqual(self.siren.calls, [])

    def test_expiry_rearm_started_outside_the_lock(self):
        quiet = self._acked_episode()
        self.register(self.state, now=self.base + 30)
        self.ns["prune_stream_corrupt_window"](self.state, now=quiet + 1)
        self.assertEqual(len(self.siren.calls), 2)
        self.assertFalse(
            self.siren.calls[-1][1],
            "shared_alarm_start must never be called while stream_corrupt_lock is held",
        )

    def test_fmt_monotonic_clock_returns_wall_clock_not_monotonic(self):
        fmt = self.ns["_fmt_monotonic_clock"]
        self.assertEqual(fmt(None), "-")
        deadline = time.monotonic() + 600
        rendered = fmt(deadline)
        self.assertRegex(rendered, r"^\d{2}:\d{2}:\d{2}$")
        rendered_secs = sum(
            int(part) * mult for part, mult in zip(rendered.split(":"), (3600, 60, 1))
        )
        expected = time.localtime(time.time() + 600)
        expected_secs = expected.tm_hour * 3600 + expected.tm_min * 60 + expected.tm_sec
        self.assertLessEqual(abs(rendered_secs - expected_secs), 60)

    def test_tick_runs_under_nm3u8dl(self):
        """The N_m3u8DL monitor loop must prune too, or a window never expires there."""
        path = recorder_path()
        tree = ast.parse(path.read_text(encoding="utf-8"))
        node = next(
            n
            for n in tree.body
            if isinstance(n, ast.FunctionDef) and n.name == "start_nm3u8dl_to_chunk"
        )
        called = [
            sub.func.id
            for sub in ast.walk(node)
            if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name)
        ]
        self.assertIn("prune_stream_corrupt_window", called)

    def test_alarm_timeout_closes_quiet_window(self):
        self._acked_episode()
        self.ns["_auto_silence_shared_alarm"](self.state, "stream_corrupt")
        self.assertIsNone(self.state.ffmpeg_stream_corrupt_quiet_until)
        self.assertTrue(
            any("quiet window closed by alarm timeout" in line for line in self.ns["log"].messages()),
            self.ns["log"].messages(),
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
