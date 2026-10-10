"""Regression checks for Recorder Issue 3 quality-upgrade safeguards.

The main recorder reads machine-specific OneDrive configuration at import, so
its narrow IDET helper is exercised from its AST without starting a recorder.
"""
from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace
from typing import Callable, Optional
import unittest
from unittest.mock import Mock

from recorder_source.models import SelectionPolicy, SourceCandidate
from recorder_source.selection import (
    bitrate_only_upgrade_below_minimum,
    select_quality_upgrade,
)


def source(bitrate: int, **changes) -> SourceCandidate:
    values = dict(
        entry_title="stream",
        quality_known=True,
        video_width=1920,
        video_height=1080,
        video_fps=50.0,
        video_scan_type="progressive",
        video_bitrate_bps=bitrate,
        expiry=100000.0,
        launchable=True,
    )
    values.update(changes)
    return SourceCandidate(**values)


class BitrateUpgradeSafeguardTests(unittest.TestCase):
    def test_below_ten_percent_is_rejected(self):
        self.assertTrue(
            bitrate_only_upgrade_below_minimum(
                source(4_380_000), source(4_478_000),
                motion_cap_fps=50,
            )
        )

    def test_exact_ten_percent_passes_without_rounding(self):
        self.assertFalse(
            bitrate_only_upgrade_below_minimum(
                source(4_380_000), source(4_818_000),
                motion_cap_fps=50,
            )
        )

    def test_above_ten_percent_passes(self):
        self.assertFalse(
            bitrate_only_upgrade_below_minimum(
                source(4_380_000), source(5_100_000),
                motion_cap_fps=50,
            )
        )

    def test_unknown_current_or_alternative_bitrate_preserves_behavior(self):
        for current, alternative in ((0, 5_000_000), (4_000_000, 0)):
            with self.subTest(current=current, alternative=alternative):
                self.assertFalse(
                    bitrate_only_upgrade_below_minimum(
                        source(current), source(alternative),
                        motion_cap_fps=50,
                    )
                )

    def test_other_video_quality_improvements_are_not_throttled(self):
        cases = [
            (source(4_000_000, video_fps=25), source(4_100_000)),
            (source(4_000_000, video_width=1280, video_height=720),
             source(4_100_000)),
            (source(4_000_000, video_scan_type="interlaced"),
             source(4_100_000)),
        ]
        for current, alternative in cases:
            with self.subTest(current=current, alternative=alternative):
                self.assertFalse(
                    bitrate_only_upgrade_below_minimum(
                        current, alternative, motion_cap_fps=50,
                    )
                )

    def test_original_selection_still_ranks_eligible_candidates(self):
        policy = SelectionPolicy(900, 900, allow_unknown_expiry=False)
        current = source(4_000_000)
        small = source(4_100_000, entry_title="small")
        meaningful = source(4_500_000, entry_title="meaningful")
        selection = select_quality_upgrade(
            current, [small, meaningful], 50, policy, now_ts=1000,
        )
        self.assertEqual(selection.selected.entry_title, "meaningful")
        # The mature selector is untouched; the guard acts only on the chosen
        # bitrate-only upgrade.
        self.assertTrue(
            bitrate_only_upgrade_below_minimum(
                current, small, motion_cap_fps=50,
            )
        )


class IDETConfirmationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        root = Path(__file__).resolve().parents[1]
        tree = ast.parse((root / "record_dynamic.py").read_text(encoding="utf-8"))
        declaration = next(
            node for node in tree.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "_confirm_nm3u8dl_quality_upgrade_idet"
        )
        cls.compiled_helper = compile(
            ast.Module(body=[declaration], type_ignores=[]),
            str(root / "record_dynamic.py"),
            "exec",
        )

    def make_helper(self, observed="progressive", sample=None):
        self.detect = Mock(return_value=observed)
        self.prepare = Mock(return_value=sample or (
            "dash-selected-sample.mp4", {}, "", "dash-selected-sample.mp4"
        ))
        self.remove = Mock()
        env = {
            "Optional": Optional,
            "Callable": Callable,
            "os": SimpleNamespace(remove=self.remove),
            "_normalize_nm3u8dl_video_scan_type": lambda x: x or "",
            "get_nm3u8dl_effective_headers": lambda h, emit_logs=False: h,
            "_nm3u8dl_known_media_probe_keys": lambda c, q, h: [""],
            "_detect_nm3u8dl_stream_scan_type_with_idet": self.detect,
            "_prepare_nm3u8dl_selected_dash_media_sample": self.prepare,
            "_format_candidate_timeout_route": lambda p, u: "test-route",
        }
        exec(self.compiled_helper, env)
        return env["_confirm_nm3u8dl_quality_upgrade_idet"]

    def candidate(self, kind="HLS", **changes):
        data = {
            "stream_type": kind,
            "video_scan_type": "progressive",
            "video_scan_type_source": "idet",
            "stream_url": "https://test.invalid/master.m3u8",
            "manifest_variant_url": "https://test.invalid/variant-1080.m3u8",
            "manifest_final_url": "https://test.invalid/master.m3u8",
            "effective_headers": {},
            "playlist_url": "https://test.invalid/playlist.m3u",
        }
        data.update(changes)
        return data

    def test_hls_agrees_on_same_selected_variant_without_cache(self):
        confirm = self.make_helper()
        self.assertEqual(confirm(self.candidate()), "confirmed")
        args, kwargs = self.detect.call_args
        self.assertEqual(args[0], "https://test.invalid/variant-1080.m3u8")
        self.assertIsNone(kwargs["scan_type_cache"])

    def test_idet_disagreement_rejects_and_inconclusive_rejects(self):
        self.assertEqual(
            self.make_helper(observed="interlaced")(self.candidate()), "failed"
        )
        self.assertEqual(
            self.make_helper(observed="")(self.candidate()), "inconclusive"
        )

    def test_non_idet_sources_never_reprobe(self):
        confirm = self.make_helper()
        result = confirm(self.candidate(video_scan_type_source="manifest"))
        self.assertEqual(result, "not_required")
        self.detect.assert_not_called()

    def test_dash_rechecks_selected_sample_not_whole_mpd_and_cleans_up(self):
        confirm = self.make_helper()
        self.assertEqual(confirm(self.candidate(kind="DASH")), "confirmed")
        self.assertEqual(self.detect.call_args.args[0], "dash-selected-sample.mp4")
        self.assertIsNone(self.detect.call_args.kwargs["scan_type_cache"])
        self.remove.assert_called_once_with("dash-selected-sample.mp4")

    def test_cancelled_confirmation_cannot_approve_upgrade(self):
        confirm = self.make_helper()
        self.assertEqual(
            confirm(self.candidate(), stop_requested=lambda: True), "cancelled"
        )
        self.detect.assert_not_called()


class UpgradeDecisionIntegrationTests(unittest.TestCase):
    """Exercise the actual upgrade-decision block without recorder startup."""

    @classmethod
    def setUpClass(cls):
        root = Path(__file__).resolve().parents[1]
        tree = ast.parse((root / "record_dynamic.py").read_text(encoding="utf-8"))
        monitor = next(
            node for node in tree.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "monitor_nm3u8dl_playlist_renewal"
        )
        decision_if = next(
            node for node in ast.walk(monitor)
            if isinstance(node, ast.If)
            and isinstance(node.test, ast.Name)
            and node.test.id == "quality_evaluation_due"
        )
        harness = ast.parse("def run_decision():\\n    pass\\n".replace("\\n", "\n"))
        harness.body[0].body = decision_if.body
        cls.decision_code = compile(
            ast.fix_missing_locations(harness),
            str(root / "record_dynamic.py"),
            "exec",
        )

    def evaluate(self, current, alternatives, confirm=None, stopped=False):
        import time as time_module
        from recorder_source import selection as selection_module

        running = current.to_mapping()
        candidate_pool = [x.to_mapping() for x in alternatives]
        logs = []
        approvals = []
        state = SimpleNamespace(
            nm3u8dl_running_source=running,
            nm3u8dl_pending_source=None,
            nm3u8dl_rollover_reason=None,
            nm3u8dl_renewal_rollover_requested=False,
            stop_flag=False,
        )

        def pick(running_source, pool, target_fps, min_remaining,
                 allow_unknown_expiry=False, now_ts=None):
            return select_quality_upgrade(
                SourceCandidate.from_mapping(running_source),
                [SourceCandidate.from_mapping(item) for item in pool],
                target_fps,
                SelectionPolicy(
                    mandatory_min_remaining_sec=900,
                    upgrade_min_remaining_sec=900,
                    allow_unknown_expiry=allow_unknown_expiry,
                ),
                now_ts=1000,
            )

        def choose_confirmation(candidate, stop_requested=None):
            if confirm is None:
                return "not_required"
            result = confirm(candidate)
            if stopped:
                state.stop_flag = True
            return result

        env = {
            "stop_event": SimpleNamespace(is_set=lambda: False),
            "state": state,
            "running_source": running,
            "candidate_pool": candidate_pool,
            "resolution": {},
            "check_time": 1000.0,
            "quality_upgrade_target_fps": 50.0,
            "quality_min_remaining_min": 15,
            "allow_unknown_expiry": False,
            "NM3U8DL_QUALITY_RANKING_MOTION_CAP_FPS": 50.0,
            "source_selection": selection_module,
            "time": SimpleNamespace(time=lambda: 1000),
            "get_nm3u8dl_quality_upgrade_selection_decision": pick,
            "_confirm_nm3u8dl_quality_upgrade_idet": choose_confirmation,
            "log": lambda message, level="INFO": logs.append(message),
            "log_quality_upgrade_found": (
                lambda candidate, check_time: approvals.append(candidate)
            ),
            "_nm3u8dl_video_quality_rank": (
                lambda item: selection_module.video_quality_rank(
                    item, motion_cap_fps=50
                )
            ),
            "_nm3u8dl_ranking_motion_fps": (
                lambda item: selection_module.ranking_motion_fps(
                    item, motion_cap_fps=50
                )
            ),
            "get_nm3u8dl_candidate_quality_rank": (
                lambda item: (
                    int(item.get("preferred_qualifier_score") or 0),
                    *selection_module.video_quality_rank(item, motion_cap_fps=50),
                )
            ),
            "format_nm3u8dl_candidate_quality": (
                lambda item: item.get("entry_title") or "stream"
            ),
            "fmt_hms": lambda seconds: str(seconds),
        }
        exec(self.decision_code, env)
        env["run_decision"]()
        return state, approvals, logs

    def test_failed_idet_rechecks_alternative_without_switching_to_failed(self):
        current = source(4_000_000, entry_title="current")
        first = source(
            5_200_000, entry_title="failed-idet",
            video_scan_type_source="idet",
        )
        second = source(
            4_600_000, entry_title="alternative",
            video_scan_type_source="manifest",
        )
        state, approvals, logs = self.evaluate(
            current, [second, first],
            confirm=lambda candidate: (
                "failed" if candidate["entry_title"] == "failed-idet"
                else "not_required"
            ),
        )
        self.assertEqual(state.nm3u8dl_pending_source["entry_title"], "alternative")
        self.assertEqual([x["entry_title"] for x in approvals], ["alternative"])
        self.assertTrue(any("FAILED" in item for item in logs))
        self.assertTrue(any("alternative candidate" in item for item in logs))

    def test_small_bitrate_gain_keeps_recording(self):
        current = source(4_380_000, entry_title="current")
        slight = source(
            4_478_000, entry_title="slight",
            video_scan_type_source="manifest",
        )
        state, approvals, logs = self.evaluate(current, [slight])
        self.assertIsNone(state.nm3u8dl_pending_source)
        self.assertEqual(approvals, [])
        self.assertTrue(any("less than 10%" in item for item in logs))

    def test_recovery_started_during_confirmation_never_commits_upgrade(self):
        current = source(4_000_000)
        alternative = source(
            4_800_000, video_scan_type_source="idet",
        )
        state, approvals, _ = self.evaluate(
            current, [alternative], confirm=lambda c: "confirmed", stopped=True,
        )
        self.assertIsNone(state.nm3u8dl_pending_source)
        self.assertEqual(approvals, [])


if __name__ == "__main__":
    unittest.main()
