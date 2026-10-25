from __future__ import annotations

import ast
import inspect
import os
import subprocess
import sys
import threading
import time
import unittest
from pathlib import Path
from typing import Optional
from unittest.mock import patch


# BUG-038: the alarm must be audible on every platform, and a siren that opens but
# cannot play must be detected instead of silently respawning forever.
SOUND_HELPERS = (
    "_resolve_alarm_sound_path",
    "_reap_afplay",
    "_afplay_play_once",
    "_alarm_fallback_hit",
    "_alarm_beep_fallback_loop",
    "_shared_alarm_sound_loop",
    "check_alarm_sound_at_launch",
)
SOUND_CONSTANTS = (
    "NM3U8DL_ALARM_BEEP_HZ",
    "NM3U8DL_ALARM_BEEP_MS",
    "NM3U8DL_ALARM_BEEP_GAP_SEC",
    "NM3U8DL_ALARM_BEEP_BURST_PAUSE_SEC",
    "ALARM_FALLBACK_BURST_HITS",
    "ALARM_FALLBACK_MACOS_SOUND",
    "ALARM_FALLBACK_MACOS_HIT_GAP_SEC",
    "ALARM_FALLBACK_RESPAWN_FLOOR_SEC",
    "ALARM_FALLBACK_MAX_CONSECUTIVE_FAILURES",
    "AFPLAY_TERMINATE_GRACE_SEC",
    "AFPLAY_MIN_AUDIBLE_SEC",
    "ALARM_SOUND_FILENAME",
)


def recorder_path(name: str = "record_static.py") -> Path:
    path = Path(__file__).resolve().parent.parent / name
    if not path.is_file():
        raise unittest.SkipTest(f"{name} not present in this checkout")
    return path


class LogSink:
    """Records log() calls so a test can assert on level and message."""

    def __init__(self):
        self.calls = []

    def __call__(self, message, level=None, **kwargs):
        self.calls.append((message, level, kwargs))

    def messages(self, level=None):
        return [msg for msg, lvl, _ in self.calls if level is None or lvl == level]


class StopEventStub:
    """threading.Event stand-in whose wait() can end the loop after N calls."""

    def __init__(self, stop_after_waits=None, started_set=False):
        self._set = started_set
        self._stop_after = stop_after_waits
        self._waits = 0
        self.wait_timeouts = []

    def is_set(self):
        return self._set

    def set(self):
        self._set = True

    def wait(self, timeout=None):
        self.wait_timeouts.append(timeout)
        self._waits += 1
        if self._stop_after is not None and self._waits >= self._stop_after:
            self._set = True
            return True
        return self._set


def load_sound_helpers(name: str = "record_static.py") -> dict:
    """Extract the alarm-audio helpers from a recorder file without importing it.

    ``record_static.py`` refuses to import without the operator's user config, so the
    helpers are compiled from their own AST nodes — the same pattern already used by
    ``test_stream_integrity.py`` for the rolling-window helpers.
    """
    path = recorder_path(name)
    tree = ast.parse(path.read_text(encoding="utf-8"))
    wanted = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in SOUND_HELPERS:
            wanted.append(node)
        elif isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id in SOUND_CONSTANTS
            for target in node.targets
        ):
            wanted.append(node)
    found = {n.name for n in wanted if isinstance(n, ast.FunctionDef)}
    missing = sorted(set(SOUND_HELPERS) - found)
    if missing:
        raise AssertionError(f"{name} is missing alarm-audio helpers: {missing}")
    module = ast.Module(body=wanted, type_ignores=[])
    namespace = {
        "__file__": str(path),
        "__name__": "recorder_alarm_sound_stub",
        "sys": sys,
        "os": os,
        "time": time,
        "subprocess": subprocess,
        "threading": threading,
        "winsound": None,
        "Optional": Optional,
        "RecorderState": object,
        "log": LogSink(),
        "is_sound_snoozed": lambda state, now_ts=None: False,
    }
    exec(compile(module, str(path), "exec"), namespace)
    return namespace


class ProcStub:
    """subprocess.Popen stand-in: reports a return code, and records termination."""

    def __init__(self, returncode=0, running=False, wait_raises_first=False):
        self.returncode = returncode
        self._running = running
        self.terminated = 0
        self.killed = 0
        self.wait_timeouts = []
        self._wait_raises_first = wait_raises_first
        self._wait_calls = 0

    def poll(self):
        return None if self._running else self.returncode

    def terminate(self):
        self.terminated += 1

    def kill(self):
        self.killed += 1

    def wait(self, timeout=None):
        self.wait_timeouts.append(timeout)
        self._wait_calls += 1
        if self._wait_raises_first and self._wait_calls == 1:
            raise subprocess.TimeoutExpired(cmd="afplay", timeout=timeout)
        self._running = False
        return self.returncode


class AlarmSoundResolutionTests(unittest.TestCase):
    def setUp(self):
        self.ns = load_sound_helpers()

    def _script_dir(self):
        return os.path.dirname(os.path.abspath(self.ns["__file__"]))

    def test_resolver_prefers_sounds_dir_over_script_dir(self):
        script_dir = self._script_dir()
        sounds_path = os.path.join(script_dir, "sounds", self.ns["ALARM_SOUND_FILENAME"])
        beside_path = os.path.join(script_dir, self.ns["ALARM_SOUND_FILENAME"])
        with patch("os.path.isfile", side_effect=lambda p: p in (sounds_path, beside_path)), patch(
            "os.path.getsize", return_value=1024
        ):
            resolved = self.ns["_resolve_alarm_sound_path"]()
        self.assertEqual(resolved, sounds_path)

    def test_resolver_returns_none_when_nothing_exists(self):
        self.ns["winsound"] = None
        with patch("os.path.isfile", return_value=False), patch.object(sys, "platform", "darwin"):
            mode = self.ns["check_alarm_sound_at_launch"]()
        self.assertEqual(mode, "none")
        errors = self.ns["log"].messages(level="ERROR")
        self.assertEqual(len(errors), 1, errors)
        self.assertIn("ALARM_SOUND_UNAVAILABLE", errors[0])

    def test_zero_byte_siren_is_not_a_siren(self):
        with patch("os.path.isfile", return_value=True), patch("os.path.getsize", return_value=0):
            self.assertIsNone(self.ns["_resolve_alarm_sound_path"]())

    def test_launch_check_names_the_voice_that_will_play(self):
        with patch("os.path.isfile", return_value=True), patch(
            "os.path.getsize", return_value=64
        ), patch.object(sys, "platform", "darwin"):
            self.assertEqual(self.ns["check_alarm_sound_at_launch"](), "siren")
            self.assertIn("ALARM_AUDIO_READY", " ".join(self.ns["log"].messages()))
        # macOS fallback is promised only when Sosumi actually exists on disk.
        with patch("os.path.isfile", side_effect=lambda p: p.endswith("Sosumi.aiff")), patch.object(
            sys, "platform", "darwin"
        ):
            self.assertEqual(self.ns["check_alarm_sound_at_launch"](), "fallback")
        with patch("os.path.isfile", return_value=False), patch.object(sys, "platform", "darwin"):
            self.assertEqual(self.ns["check_alarm_sound_at_launch"](), "none")

    def test_alarm_audio_constants_are_identical_in_both_recorders(self):
        static = load_sound_helpers("record_static.py")
        dynamic = load_sound_helpers("record_dynamic.py")
        for name in SOUND_CONSTANTS:
            self.assertEqual(
                static[name],
                dynamic[name],
                f"{name} differs between record_static.py and record_dynamic.py",
            )


class AlarmSirenPlaybackTests(unittest.TestCase):
    def setUp(self):
        self.ns = load_sound_helpers()

    def test_afplay_nonzero_exit_is_a_failure(self):
        proc = ProcStub(returncode=1, running=False)
        with patch("subprocess.Popen", return_value=proc):
            outcome = self.ns["_afplay_play_once"](object(), StopEventStub(), "/tmp/siren.wav")
        self.assertEqual(outcome, "failed")

    def test_three_failures_switch_to_fallback(self):
        fallback_calls = []
        self.ns["_resolve_alarm_sound_path"] = lambda: "/tmp/siren.wav"
        self.ns["_afplay_play_once"] = lambda *args, **kwargs: "failed"
        self.ns["_alarm_beep_fallback_loop"] = lambda *args, **kwargs: fallback_calls.append(args)
        with patch.object(sys, "platform", "darwin"):
            self.ns["_shared_alarm_sound_loop"](StopEventStub(), object(), activity_id="test")
        self.assertEqual(len(fallback_calls), 1, "siren that never plays must fall back exactly once")
        self.assertIn(
            "ALARM_FALLBACK_MAX_CONSECUTIVE_FAILURES",
            self.ns,
        )

    def test_respawn_floor_prevents_spin(self):
        self.ns["_resolve_alarm_sound_path"] = lambda: "/tmp/siren.wav"
        self.ns["_afplay_play_once"] = lambda *args, **kwargs: "played"
        event = StopEventStub(stop_after_waits=2)
        with patch.object(sys, "platform", "darwin"):
            self.ns["_shared_alarm_sound_loop"](event, object(), activity_id="test")
        self.assertTrue(event.wait_timeouts)
        self.assertEqual(
            set(event.wait_timeouts),
            {self.ns["ALARM_FALLBACK_RESPAWN_FLOOR_SEC"]},
            "every respawn must be separated by the floor wait",
        )

    def test_stop_reaps_afplay_child(self):
        proc = ProcStub(running=True)
        event = StopEventStub()
        event.set()
        with patch("subprocess.Popen", return_value=proc):
            outcome = self.ns["_afplay_play_once"](object(), event, "/tmp/siren.wav")
        self.assertEqual(outcome, "stopped")
        self.assertEqual(proc.terminated, 1)
        self.assertEqual(proc.wait_timeouts, [self.ns["AFPLAY_TERMINATE_GRACE_SEC"]])

    def test_snooze_reaps_afplay_child(self):
        proc = ProcStub(running=True)
        self.ns["is_sound_snoozed"] = lambda state, now_ts=None: True
        with patch("subprocess.Popen", return_value=proc):
            outcome = self.ns["_afplay_play_once"](object(), StopEventStub(), "/tmp/siren.wav")
        self.assertEqual(outcome, "snoozed")
        self.assertEqual(proc.terminated, 1)
        self.assertTrue(proc.wait_timeouts)

    def test_unresponsive_afplay_is_killed_and_reaped(self):
        proc = ProcStub(running=True, wait_raises_first=True)
        self.ns["_reap_afplay"](proc)
        self.assertEqual(proc.killed, 1)
        self.assertEqual(len(proc.wait_timeouts), 2, "wait must be retried after the kill")


class AlarmFallbackVoiceTests(unittest.TestCase):
    def setUp(self):
        self.ns = load_sound_helpers()

    def test_fallback_uses_sosumi_on_darwin(self):
        played = []

        def fake_run(args, **kwargs):
            played.append(args[1])
            return ProcStub(returncode=0)

        with patch("subprocess.run", side_effect=fake_run), patch.object(sys, "platform", "darwin"):
            self.assertTrue(self.ns["_alarm_fallback_hit"](StopEventStub()))
        self.assertEqual(len(played), self.ns["ALARM_FALLBACK_BURST_HITS"])
        for path in played:
            self.assertTrue(path.endswith("Sosumi.aiff"), path)
            self.assertNotIn("Basso", path)
            self.assertNotIn("Glass", path)

    def test_fallback_burst_is_interruptible(self):
        played = []
        event = StopEventStub()

        def fake_run(args, **kwargs):
            played.append(args[0])
            event.set()
            return ProcStub(returncode=0)

        with patch("subprocess.run", side_effect=fake_run), patch.object(sys, "platform", "darwin"):
            self.assertFalse(self.ns["_alarm_fallback_hit"](event))
        self.assertEqual(len(played), 1, "a set stop event must silence the remaining hits")

    def test_fallback_burst_returns_true_when_uninterrupted(self):
        with patch("subprocess.run", return_value=ProcStub(returncode=0)), patch.object(
            sys, "platform", "darwin"
        ):
            self.assertTrue(self.ns["_alarm_fallback_hit"](StopEventStub()))

    def test_fallback_loop_terminates_on_stop_event(self):
        started = time.monotonic()
        self.ns["_alarm_beep_fallback_loop"](StopEventStub(started_set=True), object())
        self.assertLess(time.monotonic() - started, 0.5)

    def test_fallback_loop_stops_when_no_voice_exists(self):
        self.ns["winsound"] = None
        with patch.object(sys, "platform", "linux"):
            self.ns["_alarm_beep_fallback_loop"](StopEventStub(), object(), activity_id="test")
        errors = self.ns["log"].messages(level="ERROR")
        self.assertEqual(len(errors), 1, errors)
        self.assertIn("ALARM_SOUND_UNAVAILABLE", errors[0])

    def test_fallback_loop_signature_matches_production(self):
        params = list(inspect.signature(self.ns["_alarm_beep_fallback_loop"]).parameters)
        self.assertEqual(params, ["stop_event", "state", "activity_id"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
