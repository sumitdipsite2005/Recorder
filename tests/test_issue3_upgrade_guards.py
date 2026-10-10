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


if __name__ == "__main__":
    unittest.main()
