from __future__ import annotations

import ast
import sys
import threading
import time
import unittest

from test_alarm_sound import LogSink, recorder_path
from test_stream_corrupt_window import DummyState, load_window_helpers


# BUG-032: the acknowledged quiet state has no sounding alarm, so an `A` branch that
# only runs `if alarm_active:` can never reach it. These tests drive handle_key itself
# rather than the acknowledgement, because a direct call passes even when the key path
# is unreachable.
class AliveThread:
    def is_alive(self):
        return True


class DeadThread:
    def is_alive(self):
        return False


def load_handle_key():
    """Compile the nested handle_key of record_static.py as a module-level function."""
    path = recorder_path()
    tree = ast.parse(path.read_text(encoding="utf-8"))
    handle_key = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "handle_key":
            handle_key = node
            break
    if handle_key is None:
        raise AssertionError("record_static.py has no handle_key")

    window = load_window_helpers()
    log = LogSink()
    window["log"] = log  # the real acknowledgement now logs into the sink we observe
    ack_calls = []

    def _acknowledge(state):
        ack_calls.append("stream_corrupt")
        return window["acknowledge_stream_corrupt_alarm"](state)

    namespace = {
        "time": time,
        "sys": sys,
        # handle_key closes over `state` from the enclosing listener; compiling the
        # nested function at module level turns that into a global, so it is bound here.
        "state": None,
        "_CONSOLE_LOCK": threading.Lock(),
        "log": log,
        "handle_runtime_control_key": lambda state, ch: False,
        "acknowledge_stream_corrupt_alarm": _acknowledge,
        "acknowledge_nm3u8dl_stall_alarm": lambda state: ack_calls.append("nm3u8dl"),
        "acknowledge_live_stall_alarm": lambda state: ack_calls.append("live_stall"),
        "acknowledge_quality_downgrade_alarm": lambda state: ack_calls.append("quality"),
        "acknowledge_vpn_drop_alarm": lambda state: ack_calls.append("vpn_drop"),
        "acknowledge_internet_drop_alarm": lambda state: ack_calls.append("internet_drop"),
        "acknowledge_token_expired_alarm": lambda state: ack_calls.append("token_expired"),
    }
    exec(compile(ast.Module(body=[handle_key], type_ignores=[]), str(path), "exec"), namespace)
    return namespace, log, ack_calls


class AlarmKeyDispatchTests(unittest.TestCase):
    def setUp(self):
        self.ns, self.log, self.ack_calls = load_handle_key()
        self.state = DummyState(lock=threading.Lock())
        self.state.token_hold_active = False
        self.ns["state"] = self.state
        self.handle_key = self.ns["handle_key"]

    def test_key_handler_reaches_quiet_acknowledgement(self):
        self.state.ffmpeg_stream_corrupt_quiet_until = time.monotonic() + 120
        self.handle_key("a")
        self.assertEqual(self.ack_calls, ["stream_corrupt"])
        self.assertTrue(
            any("'A' ignored" in message for message in self.log.messages()),
            self.log.messages(),
        )
        # The press must not extend or close the window.
        self.assertGreater(self.state.ffmpeg_stream_corrupt_quiet_until, time.monotonic())

    def test_key_handler_still_acks_sounding_stream_corrupt(self):
        self.state.shared_alarm_thread = AliveThread()
        self.state.shared_alarm_type = "stream_corrupt"
        self.state.ffmpeg_stream_corrupt_alarm_active = True
        self.handle_key("A")
        self.assertEqual(self.ack_calls, ["stream_corrupt"])
        self.assertIsNotNone(self.state.ffmpeg_stream_corrupt_quiet_until)
        self.assertTrue(self.state.ffmpeg_stream_corrupt_post_ack_silent)

    def test_key_handler_is_a_no_op_when_nothing_sounds_or_waits(self):
        # The pre-gate always asks the acknowledgement; with no siren and no window it
        # must answer False and leave every flag and the log untouched.
        self.state.shared_alarm_thread = DeadThread()
        self.state.shared_alarm_type = None
        self.handle_key("a")
        self.assertIsNone(self.state.ffmpeg_stream_corrupt_quiet_until)
        self.assertFalse(self.state.ffmpeg_stream_corrupt_post_ack_silent)
        self.assertEqual(self.log.calls, [])
        self.assertFalse(self.state.alarm_ack_requested)


if __name__ == "__main__":
    unittest.main(verbosity=2)
