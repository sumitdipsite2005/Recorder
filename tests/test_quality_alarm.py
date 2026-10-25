"""BUG-029 — quality-downgrade standard, startup verdict, alarm and planned cut.

The recorder modules cannot be imported without a user config, so the pure
helpers are compiled from the production AST (the same technique the
stream-integrity tests use) and the two integration tests import
``record_dynamic`` against a throwaway config.
"""

from __future__ import annotations

import ast
import __future__
import io
import json
import os
import re
import sys
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any, NamedTuple, Optional, Tuple

ROOT = Path(__file__).resolve().parent.parent

COMPILE_FLAGS = __future__.annotations.compiler_flag

QUALITY_FUNCTIONS = (
    "observe_startup_line",
    "evaluate_startup_quality",
    "_handle_startup_downgrade",
    "_raise_or_park_quality_alarm",
    "evaluate_pending_quality_alarm",
    "begin_quality_observation",
    "request_format_recycle",
    "_startup_vid_reading",
    "maybe_probe_startup_quality",
    "_probe_video_params",
    "_live_chunk_size",
    "_resolve_quality_standard",
    "_classify_quality_reading",
    "_coerce_video_params",
    "_normalize_quality_fps",
    "_parse_expected_video",
    "_fmt_monotonic_clock",
)
QUALITY_CLASSES = ("VideoParams", "QualityVerdict")
QUALITY_CONSTANTS = (
    "QUALITY_PENDING_MAX_WAIT_SEC",
    "QUALITY_PROBE_MIN_AGE_SEC",
    "QUALITY_PROBE_MIN_BYTES",
    "RE_VIDEO_BANNER",
    "RE_FIELD_ORDER",
    "RE_EXPECTED_VIDEO_FULL",
    "RE_EXPECTED_VIDEO_SHORT",
    "RE_NM3U8DL_VID_ROW",
    "RE_NM3U8DL_VID_FPS",
    "RE_NM3U8DL_VID_CODEC",
    "RE_NM3U8DL_VID_TIME",
    "_HEIGHT_TO_WIDTH",
    "_NM3U8DL_CODEC_ALIASES",
)
# EXPECTED_VIDEO is not extracted: its module-level assignment reads the user config
# file. The namespace supplies the value, exactly as the recorder's globals() does.


class LogSink:
    def __init__(self):
        self.lines = []

    def __call__(self, msg, level="INFO", **kwargs):
        self.lines.append((level, str(msg)))

    @property
    def text(self):
        return "\n".join(message for _level, message in self.lines)

    def has(self, needle):
        return any(needle in message for _level, message in self.lines)

    def level_of(self, needle):
        for level, message in self.lines:
            if needle in message:
                return level
        return None


class AlarmRecorder:
    def __init__(self):
        self.started = []

    def __call__(self, state):
        self.started.append("quality_downgrade")
        state.quality_downgrade_alarm_active = True
        state.shared_alarm_type = "quality_downgrade"


class QualityState:
    """Minimal RecorderState stand-in carrying the BUG-029 fields."""

    def __init__(self, **overrides):
        self.baseline_video_params = None
        self.baseline_video_codec = None
        self.last_downgraded_quality_params = None
        self.last_downgraded_codec = None
        self.acknowledged_quality_params = None
        self.acknowledged_quality_codec = None
        self.quality_downgrade_alarm_active = False
        self.quality_banner_reading = None
        self.quality_banner_consumed = False
        self.quality_probe_done = False
        self.quality_startup_checked = False
        self.quality_expected_unset_logged = False
        self.quality_pending_downgrade = None
        self.quality_pending_since_mono = None
        self.shared_alarm_type = None
        self.shared_alarm_activity_id = None
        self.alarm_ack_requested = False
        self.alarm_linger_until = None
        self.integrity_monitor = None
        for key, value in overrides.items():
            setattr(self, key, value)


class MonitorStub:
    def __init__(self):
        self.recycle_requested = False
        self.recycle_kind = "corruption"
        self.recycle_reason = ""


def _alarm_active(state):
    return bool(getattr(state, "shared_alarm_type", None)) or bool(
        getattr(state, "quality_downgrade_alarm_active", False)
    )


def load_quality(filename="record_static.py", expected_video=None):
    """Compile the BUG-029 helpers from a recorder file into an isolated namespace."""
    path = ROOT / filename
    text = path.read_text(encoding="utf-8")
    tree = ast.parse(text)

    wanted = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in QUALITY_FUNCTIONS:
            wanted.append(node)
        elif isinstance(node, ast.ClassDef) and node.name in QUALITY_CLASSES:
            wanted.append(node)
        elif isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id in QUALITY_CONSTANTS
            for target in node.targets
        ):
            wanted.append(node)

    found = {node.name for node in wanted if isinstance(node, (ast.FunctionDef, ast.ClassDef))}
    missing = (set(QUALITY_FUNCTIONS) | set(QUALITY_CLASSES)) - found
    assert not missing, f"{filename}: quality helpers missing {sorted(missing)}"

    log = LogSink()
    alarms = AlarmRecorder()
    ns = {
        "re": re,
        "json": json,
        "os": os,
        "time": time,
        "Optional": Optional,
        "Tuple": Tuple,
        "Any": Any,
        "NamedTuple": NamedTuple,
        "RecorderState": QualityState,
        "log": log,
        "EXPECTED_VIDEO": expected_video,
        "is_alarm_active": _alarm_active,
        "quality_downgrade_alarm_start": alarms,
    }
    module = ast.Module(body=wanted, type_ignores=[])
    exec(compile(module, str(path), "exec", flags=COMPILE_FLAGS), ns)
    return ns, log, alarms


def load_function(filename, name, extra_ns):
    """Compile one top-level function from a recorder file with caller-supplied deps."""
    path = ROOT / filename
    tree = ast.parse(path.read_text(encoding="utf-8"))
    nodes = [
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name
    ]
    assert nodes, f"{filename} has no {name}"
    ns = dict(extra_ns)
    exec(
        compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec", flags=COMPILE_FLAGS),
        ns,
    )
    return ns[name]


class QualityStandardTests(unittest.TestCase):
    """The standard, the readings and the rounding rules (BLOCKER 1 items 1-3, 6)."""

    def setUp(self):
        self.ns, self.log, self.alarms = load_quality()
        self.VideoParams = self.ns["VideoParams"]

    def test_frame_rate_is_rounded_once_at_every_boundary(self):
        normalize = self.ns["_normalize_quality_fps"]
        self.assertEqual(normalize(59.94), 60.0)
        self.assertEqual(normalize(49.94), 50.0)
        self.assertEqual(normalize("50"), 50.0)
        self.assertEqual(normalize(0), 0.0)
        self.assertEqual(normalize(-1), 0.0)
        self.assertEqual(normalize(None), 0.0)
        # A config written as 59.94 must equal what ffprobe reports as 60.0.
        self.assertEqual(self.ns["_parse_expected_video"]("1080p59.94"), (1920, 1080, 60.0))

    def test_expected_video_accepts_every_documented_form(self):
        parse = self.ns["_parse_expected_video"]
        self.assertEqual(parse("1920x1080@50"), (1920, 1080, 50.0))
        self.assertEqual(parse("1920x1080"), (1920, 1080, None))
        self.assertEqual(parse("1080p50"), (1920, 1080, 50.0))
        self.assertEqual(parse("1080p"), (1920, 1080, None))
        self.assertEqual(parse("720p50"), (1280, 720, 50.0))
        self.assertEqual(parse(" 1080p50 "), (1920, 1080, 50.0))
        self.assertIsNone(parse(None))
        self.assertIsNone(parse(""))
        # A typo must never stop a recording or start a siren.
        self.assertIsNone(parse("full hd please"))
        self.assertEqual(self.log.level_of("is not understood"), "WARN")

    def test_coerce_video_params_accepts_legacy_three_field_readings(self):
        coerce = self.ns["_coerce_video_params"]
        legacy = coerce((1280, 720, 25.0))
        self.assertEqual(legacy[:3], (1280, 720, 25.0))
        self.assertEqual(legacy.codec, "")
        self.assertEqual(legacy.field_order, "")
        self.assertEqual(
            coerce((1280, 720, 59.94, "h264")), self.VideoParams(1280, 720, 60.0, "h264", "")
        )

    def test_pixel_rate_rule_does_not_call_a_resolution_drop_an_upgrade(self):
        classify = self.ns["_classify_quality_reading"]
        standard = (1920, 1080, 50.0, None)
        # Halved resolution at a higher frame rate is a DOWNGRADE, not an upgrade
        # (the BUG-027 rule): 1280x720@60 is 55.3M px/s against 103.7M px/s.
        verdict = classify(self.VideoParams(1280, 720, 60.0, "", ""), standard)
        self.assertEqual(verdict.geometry, "below")
        self.assertFalse(verdict.is_upgrade)

        below = classify(self.VideoParams(1280, 720, 25.0, "", ""), standard)
        self.assertTrue(below.is_downgrade)
        self.assertIn("geometry below standard", below.reason)

        # Exactly the standard is "at"; a higher pixel rate is the only upgrade.
        self.assertEqual(classify(self.VideoParams(1920, 1080, 50.0, "", ""), standard).geometry, "at")
        above = classify(self.VideoParams(1920, 1080, 60.0, "", ""), standard)
        self.assertTrue(above.is_upgrade)

    def test_codec_and_interlace_changes_are_reported(self):
        classify = self.ns["_classify_quality_reading"]
        verdict = classify(
            self.VideoParams(1920, 1080, 50.0, "hevc", "tt"),
            (1920, 1080, 50.0, "h264"),
        )
        self.assertTrue(verdict.is_codec_change)
        self.assertTrue(verdict.interlace_changed)
        self.assertIn("codec h264 -> hevc", verdict.reason)
        self.assertIn("interlaced source", verdict.reason)

    def test_banner_observation_takes_the_first_match_only(self):
        state = QualityState()
        observe = self.ns["observe_startup_line"]
        first = observe(
            state,
            "Stream #0:0: Video: h264 (High), yuv420p, 1920x1080 [SAR 1:1], 59.94 fps, 60 tbr",
        )
        self.assertIsNotNone(first)
        self.assertEqual(first.width, 1920)
        self.assertEqual(first.fps, 60.0)
        self.assertEqual(first.codec, "h264")
        self.assertTrue(state.quality_banner_consumed)
        # A later banner (an input/output duplicate or a re-open) must not replace it.
        self.assertIsNone(
            observe(state, "Stream #0:1: Video: hevc, yuv420p, 1280x720, 25 fps")
        )
        self.assertEqual(state.quality_banner_reading.width, 1920)

    def test_unknown_fps_reading_still_judges_geometry_and_acks_at_standard_fps(self):
        ns, log, alarms = load_quality(expected_video="1080p50")
        state = QualityState()

        ns["evaluate_startup_quality"](state, ns["VideoParams"](1280, 720, 0.0, "", ""), "nm3u8dl")

        self.assertEqual(alarms.started, ["quality_downgrade"])
        self.assertIn("QUALITY_VERDICT source=nm3u8dl", log.text)
        # Item 3: the ACK key carries the standard's rate, not the missing 0.0.
        self.assertEqual(state.last_downgraded_quality_params, (1280, 720, 50.0))
        # The downgrade must not become the standard.
        self.assertEqual(state.baseline_video_params, (1920, 1080, 50.0))

    def test_startup_verdict_is_idempotent_per_chunk(self):
        ns, log, alarms = load_quality(expected_video="1080p50")
        state = QualityState()
        evaluate = ns["evaluate_startup_quality"]
        evaluate(state, ns["VideoParams"](1280, 720, 50.0, "h264", ""), "ffmpeg")
        evaluate(state, ns["VideoParams"](1280, 720, 50.0, "h264", ""), "probe")
        self.assertEqual(alarms.started, ["quality_downgrade"])

        # begin_quality_observation re-arms the slot for the next chunk run.
        ns["begin_quality_observation"](state)
        self.assertFalse(state.quality_startup_checked)
        self.assertFalse(state.quality_banner_consumed)


class QualityAlarmParkingTests(unittest.TestCase):
    """Parking, promotion, expiry and acknowledgement (BLOCKER 2, items 4-7, 9-10)."""

    def setUp(self):
        self.ns, self.log, self.alarms = load_quality(expected_video="1080p50")

    def test_quality_alarm_never_parks_behind_itself(self):
        state = QualityState(shared_alarm_type="quality_downgrade")
        self.ns["_raise_or_park_quality_alarm"](state, "geometry below", source="chunk_seal")
        self.assertIsNone(state.quality_pending_downgrade)
        self.assertEqual(self.alarms.started, [])

        state = QualityState(quality_downgrade_alarm_active=True)
        self.ns["_raise_or_park_quality_alarm"](state, "geometry below", source="chunk_seal")
        self.assertIsNone(state.quality_pending_downgrade)
        self.assertEqual(self.alarms.started, [])

    def test_parked_alarm_promotes_when_the_other_alarm_clears(self):
        state = QualityState(shared_alarm_type="vpn_drop")
        state.last_downgraded_quality_params = (1280, 720, 50.0)
        state.last_downgraded_codec = "h264"

        self.ns["_raise_or_park_quality_alarm"](state, "geometry below", source="ffmpeg")
        self.assertEqual(self.alarms.started, [])
        self.assertEqual(state.quality_pending_downgrade["reason"], "geometry below")
        self.assertEqual(state.quality_pending_downgrade["source"], "ffmpeg")
        self.assertEqual(state.quality_pending_downgrade["codec"], "h264")
        self.assertIn("alarm deferred", self.log.text)

        # Still busy: the tick must not promote yet.
        self.ns["evaluate_pending_quality_alarm"](state)
        self.assertEqual(self.alarms.started, [])

        state.shared_alarm_type = None
        self.ns["evaluate_pending_quality_alarm"](state)
        self.assertEqual(self.alarms.started, ["quality_downgrade"])
        self.assertIsNone(state.quality_pending_downgrade)
        self.assertIn("promoted after prior alarm cleared", self.log.text)

    def test_parked_alarm_is_withheld_after_its_window_even_while_the_other_alarm_is_busy(self):
        state = QualityState(shared_alarm_type="vpn_drop")
        state.last_downgraded_quality_params = (1280, 720, 50.0)
        self.ns["_raise_or_park_quality_alarm"](state, "geometry below", source="ffmpeg")
        max_wait = self.ns["QUALITY_PENDING_MAX_WAIT_SEC"]

        self.ns["evaluate_pending_quality_alarm"](state, now=time.monotonic() + max_wait + 1)
        self.assertIsNone(state.quality_pending_downgrade)
        self.assertEqual(self.alarms.started, [])
        self.assertIn("QUALITY_DOWNGRADE_PENDING_EXPIRED", self.log.text)

        # The other alarm finally clears: a withheld alarm must not fire late.
        state.shared_alarm_type = None
        self.ns["evaluate_pending_quality_alarm"](state)
        self.assertEqual(self.alarms.started, [])

    # Spec-named coverage for the two parking outcomes (the record says what was
    # withheld - a silently dropped alarm is what BUG-029 was reported as).
    def test_pending_quality_alarm_sounds_when_bus_frees(self):
        state = QualityState(shared_alarm_type="stream_corrupt")
        state.last_downgraded_quality_params = (1280, 720, 50.0)
        state.last_downgraded_codec = "h264"
        self.ns["_raise_or_park_quality_alarm"](state, "geometry below", source="chunk_seal")

        state.shared_alarm_type = None
        self.ns["evaluate_pending_quality_alarm"](state)

        self.assertEqual(self.alarms.started, ["quality_downgrade"])

    def test_pending_quality_alarm_expires_with_explicit_withheld_line(self):
        state = QualityState(shared_alarm_type="stream_corrupt")
        state.last_downgraded_quality_params = (1280, 720, 50.0)
        self.ns["_raise_or_park_quality_alarm"](state, "geometry below", source="ffmpeg")

        self.ns["evaluate_pending_quality_alarm"](
            state, now=time.monotonic() + self.ns["QUALITY_PENDING_MAX_WAIT_SEC"] + 1
        )

        self.assertEqual(self.alarms.started, [])
        self.assertEqual(self.log.level_of("QUALITY_DOWNGRADE_PENDING_EXPIRED"), "ERROR")
        self.assertIn("geometry below", self.log.text)
        self.assertIn("withheld", self.log.text)

    def test_parked_alarm_is_not_promoted_after_the_operator_acknowledged_it(self):
        state = QualityState(shared_alarm_type="vpn_drop")
        state.last_downgraded_quality_params = (1280, 720, 50.0)
        state.last_downgraded_codec = "h264"
        self.ns["_raise_or_park_quality_alarm"](state, "geometry below", source="ffmpeg")

        state.acknowledged_quality_params = (1280, 720, 50.0)
        state.acknowledged_quality_codec = "h264"
        state.shared_alarm_type = None
        self.ns["evaluate_pending_quality_alarm"](state)

        self.assertEqual(self.alarms.started, [])
        self.assertIsNone(state.quality_pending_downgrade)

    def test_a_new_reading_replaces_a_parked_one_without_restarting_the_window(self):
        state = QualityState(shared_alarm_type="vpn_drop")
        state.last_downgraded_quality_params = (1280, 720, 50.0)
        self.ns["_raise_or_park_quality_alarm"](state, "geometry below", source="ffmpeg")
        first_since = state.quality_pending_since_mono

        state.last_downgraded_quality_params = (960, 540, 50.0)
        self.ns["_raise_or_park_quality_alarm"](state, "geometry far below", source="probe")

        self.assertEqual(state.quality_pending_downgrade["reason"], "geometry far below")
        self.assertEqual(state.quality_pending_downgrade["w"], 960)
        self.assertEqual(state.quality_pending_since_mono, first_since)


class StartupAckSuppressionTests(unittest.TestCase):
    """An 'A' press taken at startup stays quiet on the chunk it acknowledged."""

    def setUp(self):
        self.ns, self.log, self.alarms = load_quality(expected_video="1080p50")
        self.evaluate = self.ns["evaluate_startup_quality"]

    def _ack_what_is_ringing(self, state):
        state.acknowledged_quality_params = state.last_downgraded_quality_params
        state.acknowledged_quality_codec = state.last_downgraded_codec
        state.shared_alarm_type = None
        state.quality_downgrade_alarm_active = False

    def test_startup_ack_suppresses_the_same_reading_on_a_later_chunk(self):
        state = QualityState()
        self.evaluate(state, self.ns["VideoParams"](1280, 720, 59.94, "h264", ""), "ffmpeg")
        self.assertEqual(self.alarms.started, ["quality_downgrade"])
        self._ack_what_is_ringing(state)

        self.ns["begin_quality_observation"](state)
        self.evaluate(state, self.ns["VideoParams"](1280, 720, 60.0, "h264", ""), "probe")

        # Even the 59.94 -> 60 rounding difference must not resurrect it.
        self.assertEqual(self.alarms.started, ["quality_downgrade"])
        self.assertIn("QUALITY_DOWNGRADE_SUPPRESSED", self.log.text)

    def test_ack_taken_from_a_codec_less_reading_locks_the_codec_in(self):
        state = QualityState()
        # An N_m3u8DL Vid row publishes geometry only: the ACK carries no codec.
        self.evaluate(state, self.ns["VideoParams"](1280, 720, 0.0, "", ""), "nm3u8dl")
        self._ack_what_is_ringing(state)
        self.assertIsNone(state.acknowledged_quality_codec)

        # The probe now reads the same downgrade and knows the codec.
        self.ns["begin_quality_observation"](state)
        self.evaluate(state, self.ns["VideoParams"](1280, 720, 50.0, "h264", ""), "probe")

        self.assertEqual(self.alarms.started, ["quality_downgrade"])
        self.assertEqual(state.acknowledged_quality_codec, "h264")

    def test_a_codec_change_is_not_covered_by_a_geometry_acknowledgement(self):
        state = QualityState(integrity_monitor=MonitorStub())
        self.evaluate(state, self.ns["VideoParams"](1280, 720, 50.0, "h264", ""), "ffmpeg")
        self._ack_what_is_ringing(state)

        self.ns["begin_quality_observation"](state)
        self.evaluate(state, self.ns["VideoParams"](1280, 720, 50.0, "hevc", ""), "probe")

        self.assertIn("codec h264 -> hevc", self.log.text)
        self.assertEqual(state.baseline_video_codec, "hevc")
        self.assertEqual(state.integrity_monitor.recycle_kind, "format_change")


class PlannedCutTests(unittest.TestCase):
    """A codec change cuts the chunk as a planned format change, not as corruption."""

    def setUp(self):
        self.ns, self.log, self.alarms = load_quality()

    def test_request_format_recycle_marks_a_planned_cut(self):
        state = QualityState(integrity_monitor=MonitorStub())
        self.ns["request_format_recycle"](state, reason="codec h264->hevc")
        monitor = state.integrity_monitor
        self.assertTrue(monitor.recycle_requested)
        self.assertEqual(monitor.recycle_kind, "format_change")
        self.assertEqual(monitor.recycle_reason, "format change: codec h264->hevc")
        self.assertEqual(self.alarms.started, [])

        # A second request never overwrites the pending reason.
        self.ns["request_format_recycle"](state, reason="codec hevc->av1")
        self.assertIn("codec h264->hevc", monitor.recycle_reason)

    def test_request_format_recycle_without_a_monitor_is_a_logged_no_op(self):
        state = QualityState()
        self.ns["request_format_recycle"](state, reason="codec h264->hevc")
        self.assertIn("no monitor attached", self.log.text)
        self.assertEqual(self.log.level_of("no monitor attached"), "WARN")

    def test_codec_change_adopts_the_new_codec_and_cuts_the_chunk(self):
        state = QualityState(integrity_monitor=MonitorStub())
        state.baseline_video_params = (1920, 1080, 50.0)
        state.baseline_video_codec = "h264"

        self.ns["evaluate_startup_quality"](
            state, self.ns["VideoParams"](1920, 1080, 50.0, "hevc", ""), "ffmpeg"
        )

        self.assertEqual(state.baseline_video_codec, "hevc")
        # The geometry floor is untouched by a codec path.
        self.assertEqual(state.baseline_video_params, (1920, 1080, 50.0))
        self.assertEqual(state.integrity_monitor.recycle_kind, "format_change")
        self.assertEqual(self.alarms.started, [])


BANNER_1080P50 = (
    "Stream #0:0: Video: h264 (High), yuv420p(tv, progressive), 1920x1080 [SAR 1:1], "
    "50 fps, 50 tbr, 90k tbn"
)


class ExpectedVideoParsingTests(unittest.TestCase):
    """The configured expectation: lenient parsing, never a crash, highest rung wins."""

    def test_parse_expected_video_accepts_all_documented_forms(self):
        ns, _log, _alarms = load_quality()
        parse = ns["_parse_expected_video"]
        for text in ("1080p50", "1920x1080@50", "1080p", "1080P50", " 1080p50 ", "1920x1080"):
            with self.subTest(text=text):
                parsed = parse(text)
                self.assertIsNotNone(parsed, text)
                self.assertEqual(parsed[0], 1920, text)
                self.assertEqual(parsed[1], 1080, text)

    def test_parse_expected_video_rejects_garbage_without_raising(self):
        ns, _log, _alarms = load_quality()
        parse = ns["_parse_expected_video"]
        for text in ("", None, "best", "1080p50x", "abc", "0p0", "-1080p", "1080p9999"):
            with self.subTest(text=text):
                self.assertIsNone(parse(text), text)

    def test_expected_video_overrides_learned_baseline(self):
        ns, log, alarms = load_quality(expected_video="1080p50")
        state = QualityState()
        # A learned 720p floor must never judge a 720p feed as normal once the
        # operator has stated 1080p50.
        state.baseline_video_params = (1280, 720, 50.0)

        ns["evaluate_startup_quality"](state, ns["VideoParams"](1280, 720, 50.0, "h264", ""), "ffmpeg")

        self.assertEqual(alarms.started, ["quality_downgrade"])
        self.assertIn("QUALITY_VERDICT source=ffmpeg", log.text)
        self.assertEqual(state.baseline_video_params, (1280, 720, 50.0))


class BannerAndVidReadingTests(unittest.TestCase):
    """The two startup reading sources: the FFmpeg banner and the N_m3u8DL Vid row."""

    def setUp(self):
        self.ns, self.log, self.alarms = load_quality()

    def test_banner_parse_extracts_codec_geometry_fps(self):
        state = QualityState()
        reading = self.ns["observe_startup_line"](state, BANNER_1080P50)
        self.assertEqual(reading, self.ns["VideoParams"](1920, 1080, 50.0, "h264", "progressive"))
        self.assertIn("FORMAT_BANNER source=ffmpeg 1920x1080@50fps codec=h264", self.log.text)

    def test_banner_parse_ignores_second_banner_in_same_chunk(self):
        state = QualityState()
        observe = self.ns["observe_startup_line"]
        self.assertIsNotNone(observe(state, BANNER_1080P50))
        # The output banner of the same run, or a mid-chunk re-open, must not be used.
        self.assertIsNone(
            observe(state, "Stream #0:1: Video: hevc, yuv420p, 1280x720, 25 fps, 25 tbr")
        )
        self.assertEqual(state.quality_banner_reading, (1920, 1080, 50.0, "h264", "progressive"))

    def test_nm3u8dl_vid_row_reaches_verdict(self):
        state = QualityState()
        state.baseline_video_params = (1920, 1080, 50.0)
        state.baseline_video_codec = "h264"

        reading = self.ns["_startup_vid_reading"](
            "Vid 1280x720 | 2500 Kbps | 50 fps | avc1.64001f"
        )
        self.assertEqual(reading, self.ns["VideoParams"](1280, 720, 50.0, "h264", ""))

        self.ns["evaluate_startup_quality"](state, reading, "nm3u8dl")

        self.assertEqual(self.alarms.started, ["quality_downgrade"])
        self.assertIn("QUALITY_VERDICT source=nm3u8dl", self.log.text)
        self.assertIn("geometry below standard 1280x720@50 vs 1920x1080@50", self.log.text)

    def test_nm3u8dl_progress_row_never_invents_a_frame_rate(self):
        # A progress row's trailing number is a byte/segment count. Reading it as a
        # frame rate would raise a siren on a healthy feed.
        reading = self.ns["_startup_vid_reading"](
            "Vid 1920x1080 | 5000 Kbps | 00:12:34 | 128"
        )
        self.assertIsNotNone(reading)
        self.assertEqual(reading.fps, 0.0)
        self.assertIsNone(self.ns["_startup_vid_reading"]("Vid | Progress | 45%"))


class ProbeFallbackTests(unittest.TestCase):
    """No banner at all: the growing chunk is probed once, and never twice."""

    def setUp(self):
        self.ns, self.log, self.alarms = load_quality()
        self.calls = []

    def _arm(self, probed):
        def fake_probe(path):
            self.calls.append(path)
            return probed

        self.ns["_probe_video_params"] = fake_probe
        self.ns["_live_chunk_size"] = lambda path: 2 * 1024 * 1024

    def test_probe_fallback_fires_without_banner(self):
        state = QualityState()
        state.baseline_video_params = (1920, 1080, 50.0)
        state.baseline_video_codec = "h264"
        probed = self.ns["VideoParams"](1280, 720, 50.0, "h264", "")
        self._arm(probed)

        result = self.ns["maybe_probe_startup_quality"](state, "chunk_001.ts", 0.0, now=1.0)

        self.assertEqual(result, probed)
        self.assertEqual(self.calls, ["chunk_001.ts"])
        self.assertTrue(state.quality_probe_done)
        self.assertIn("FORMAT_BANNER source=probe 1280x720@50fps", self.log.text)
        self.assertEqual(self.alarms.started, ["quality_downgrade"])

    def test_probe_fallback_never_runs_twice(self):
        state = QualityState()
        self._arm(self.ns["VideoParams"](1920, 1080, 50.0, "h264", ""))
        self.ns["maybe_probe_startup_quality"](state, "chunk_001.ts", 0.0, now=1.0)
        self.assertEqual(len(self.calls), 1)

        # The next tick of the same chunk must not probe again, even when asked again.
        self.assertIsNone(
            self.ns["maybe_probe_startup_quality"](state, "chunk_001.ts", 0.0, now=30.0)
        )
        self.assertEqual(len(self.calls), 1)

    def test_probe_fallback_waits_for_the_window_and_skips_a_banner_reading(self):
        state = QualityState()
        self._arm(self.ns["VideoParams"](1920, 1080, 50.0, "h264", ""))
        self.ns["_live_chunk_size"] = lambda path: 1024

        # Neither the age nor the byte threshold is met yet.
        self.assertIsNone(
            self.ns["maybe_probe_startup_quality"](state, "chunk_001.ts", 0.0, now=1.0)
        )
        self.assertEqual(self.calls, [])
        self.assertFalse(state.quality_probe_done)

        # A banner already answered the question: no probe.
        state.quality_banner_consumed = True
        self.assertIsNone(
            self.ns["maybe_probe_startup_quality"](state, "chunk_001.ts", 0.0, now=600.0)
        )
        self.assertEqual(self.calls, [])

    def test_probe_failure_is_silent_and_never_breaks_the_run(self):
        state = QualityState()
        self._arm(None)
        self.assertIsNone(
            self.ns["maybe_probe_startup_quality"](state, "chunk_001.ts", 0.0, now=1.0)
        )
        self.assertTrue(state.quality_probe_done)
        self.assertEqual(self.alarms.started, [])


def _entry_points(filename):
    """Every start_*_to_chunk engine entry point in one recorder, by name -> source."""
    path = ROOT / filename
    text = path.read_text(encoding="utf-8")
    tree = ast.parse(text)
    return {
        node.name: ast.get_source_segment(text, node) or ""
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name.startswith("start_")
        and node.name.endswith("_to_chunk")
    }


class ChunkObservationTests(unittest.TestCase):
    """Every chunk run starts with fresh startup-verdict slots, in both recorders."""

    def setUp(self):
        self.ns, self.log, self.alarms = load_quality()

    def test_nm3u8dl_chunk_start_resets_quality_slots(self):
        state = QualityState()
        # Chunk 1 ended with everything consumed.
        state.quality_banner_reading = self.ns["VideoParams"](1920, 1080, 50.0, "h264", "")
        state.quality_banner_consumed = True
        state.quality_probe_done = True
        state.quality_startup_checked = True

        self.ns["begin_quality_observation"](state)

        self.assertIsNone(state.quality_banner_reading)
        self.assertFalse(state.quality_banner_consumed)
        self.assertFalse(state.quality_probe_done)
        self.assertFalse(state.quality_startup_checked)

        entries = _entry_points("record_static.py")
        self.assertIn("begin_quality_observation(state)", entries["start_nm3u8dl_to_chunk"])

    def test_every_engine_entry_point_begins_observation(self):
        for filename in ("record_static.py", "record_dynamic.py"):
            entries = _entry_points(filename)
            self.assertEqual(
                sorted(entries),
                ["start_ffmpeg_to_chunk", "start_nm3u8dl_to_chunk"],
                filename,
            )
            for name, source in entries.items():
                with self.subTest(module=filename, entry=name):
                    self.assertIn("begin_quality_observation(state)", source)


class CodecChangeTests(unittest.TestCase):
    """A codec flip cuts once, adopts the new codec, and never moves the floor."""

    def setUp(self):
        self.ns, self.log, self.alarms = load_quality()

    def test_codec_change_rearms_once_and_adopts_codec(self):
        state = QualityState(integrity_monitor=MonitorStub())
        state.baseline_video_params = (1920, 1080, 50.0)
        state.baseline_video_codec = "h264"
        evaluate = self.ns["evaluate_startup_quality"]

        evaluate(state, self.ns["VideoParams"](1920, 1080, 50.0, "hevc", ""), "ffmpeg")
        self.assertEqual(state.baseline_video_codec, "hevc")
        self.assertTrue(state.integrity_monitor.recycle_requested)

        # The next chunk of the same run must not cut again for the codec it adopted.
        self.ns["begin_quality_observation"](state)
        state.integrity_monitor = MonitorStub()
        evaluate(state, self.ns["VideoParams"](1920, 1080, 50.0, "hevc", ""), "ffmpeg")

        self.assertFalse(state.integrity_monitor.recycle_requested)
        self.assertIn("FORMAT_CHANGE codec h264 -> hevc", self.log.text)
        self.assertEqual(self.log.text.count("FORMAT_CHANGE codec"), 1)
        self.assertEqual(self.alarms.started, [])

    def test_codec_change_does_not_move_geometry_floor(self):
        state = QualityState(integrity_monitor=MonitorStub())
        state.baseline_video_params = (1920, 1080, 50.0)
        state.baseline_video_codec = "h264"

        self.ns["evaluate_startup_quality"](
            state, self.ns["VideoParams"](1280, 720, 50.0, "hevc", ""), "ffmpeg"
        )

        # Both facts are reported: the codec flips the format, and the geometry
        # downgrade still raises its own siren.
        self.assertEqual(state.baseline_video_params, (1920, 1080, 50.0))
        self.assertEqual(state.baseline_video_codec, "hevc")
        self.assertEqual(state.integrity_monitor.recycle_kind, "format_change")
        self.assertEqual(self.alarms.started, ["quality_downgrade"])
        self.assertIn("geometry below standard", self.log.text)
