from __future__ import annotations

from datetime import datetime
import unittest

from recorder_coordinator.launch import (
    _safe_base_name,
    build_all_launch_plan,
    build_manual_launch_plan,
)
from recorder_coordinator.models import (
    DashboardSnapshot,
    IdentityBlock,
    IdentityTarget,
    POLICY_ALL,
    POLICY_MANUAL,
    TargetView,
)
from recorder_source.identity import derive_feed_identity
from recorder_source.models import SourceCandidate


def sony_candidate(*, title: str, fps: float, bitrate: int, playlist: str) -> SourceCandidate:
    return SourceCandidate(
        playlist_url=playlist,
        matching_entry_index=1,
        entry_title=title,
        stream_url=(
            "https://sony.example/hls/live/feed123/channel1/english/"
            f"variant_{int(fps)}.m3u8"
        ),
        final_stream_url=(
            "https://cdn.example/hls/live/feed123/channel1/english/"
            f"variant_{int(fps)}.m3u8"
        ),
        launchable=True,
        video_width=1920,
        video_height=1080,
        video_fps=fps,
        video_bitrate_bps=bitrate,
        extra={
            "provider": "SONYLIV",
            "source_group": "SONYLIV_EVENTS",
        },
    )


class ManualLaunchPlanTests(unittest.TestCase):
    def test_fancode_filename_marker_uses_numeric_identity_sub_id(self):
        item = SourceCandidate(
            entry_title="Presidents Cup 2026 [English]",
            group_title="FanCode",
        )
        self.assertEqual(
            _safe_base_name(
                item,
                "fallback",
                "/mumbai/4249106_english_hls_b86f41b4c015704_1ta-di_h264",
            ),
            "FanCode - Presidents Cup 2026 [English] - 4249106",
        )

    def test_coordinator_filename_does_not_use_tvg_name(self):
        item = SourceCandidate(
            entry_title="",
            tvg_name="Very Long TVG Name That Must Not Enter The Filename",
            group_title="FanCode",
        )
        self.assertEqual(
            _safe_base_name(
                item,
                "Presidents Cup",
                "/mumbai/4249106_english_hls_b86f41b4c015704_1ta-di_h264",
            ),
            "FanCode - Presidents Cup - 4249106",
        )

    def snapshot_for(self, targets, candidates_by_target):
        all_candidates = [
            candidate
            for target in targets
            for candidate in candidates_by_target[target.name]
        ]
        identity = derive_feed_identity(all_candidates[0], "SONYLIV")
        block = IdentityBlock(
            policy=POLICY_MANUAL,
            identity=identity,
            target_names=[target.name for target in targets],
            candidates=list(all_candidates),
            best_candidate=all_candidates[-1],
            overall_state="AVAILABLE",
        )
        return identity, DashboardSnapshot(
            created_at=datetime.now(),
            target_views=tuple(
                TargetView(target, "ACTIVE", datetime.now(), None)
                for target in targets
            ),
            coordinator_window=None,
            blocks={(POLICY_MANUAL, identity.serialized): block},
            candidates_by_target={
                name: tuple(items)
                for name, items in candidates_by_target.items()
            },
        )

    def test_best_stream_wins_and_its_target_supplies_duration(self):
        lower = sony_candidate(
            title="Example Event",
            fps=25.0,
            bitrate=4_000_000,
            playlist="https://example.test/a.m3u",
        )
        better = sony_candidate(
            title="Example Event",
            fps=50.0,
            bitrate=6_000_000,
            playlist="https://example.test/b.m3u",
        )
        target_a = IdentityTarget(
            name="Target A",
            policy=POLICY_MANUAL,
            source_groups=("SONYLIV_EVENTS",),
            primary=("example",),
            worker_recording_duration_min=60,
        )
        target_b = IdentityTarget(
            name="Target B",
            policy=POLICY_MANUAL,
            source_groups=("SONYLIV_EVENTS",),
            primary=("example",),
            worker_recording_duration_min=120,
        )
        identity, snapshot = self.snapshot_for(
            (target_a, target_b),
            {
                "Target A": (lower,),
                "Target B": (better,),
            },
        )

        plan = build_manual_launch_plan(snapshot, identity.serialized, now_ts=1.0)

        self.assertEqual(plan.selected_candidate.stream_url, better.stream_url)
        self.assertEqual(tuple(item.name for item in plan.target_intents), ("Target B",))
        self.assertEqual(plan.recording_duration_min, 120.0)

    def test_all_policy_reuses_same_identity_launch_planner(self):
        candidate = sony_candidate(
            title="Auto Event",
            fps=50.0,
            bitrate=6_000_000,
            playlist="https://example.test/a.m3u",
        )
        target = IdentityTarget(
            name="Auto Target",
            policy=POLICY_ALL,
            source_groups=("SONYLIV_EVENTS",),
            primary=("auto",),
            worker_recording_duration_min=90,
        )
        identity = derive_feed_identity(candidate, "SONYLIV")
        block = IdentityBlock(
            policy=POLICY_ALL,
            identity=identity,
            target_names=[target.name],
            candidates=[candidate],
            best_candidate=candidate,
            overall_state="AVAILABLE",
        )
        snapshot = DashboardSnapshot(
            created_at=datetime.now(),
            target_views=(
                TargetView(target, "ACTIVE", datetime.now(), None),
            ),
            coordinator_window=None,
            blocks={(POLICY_ALL, identity.serialized): block},
            candidates_by_target={target.name: (candidate,)},
        )

        plan = build_all_launch_plan(
            snapshot,
            identity.serialized,
            now_ts=1.0,
        )

        self.assertEqual(plan.identity.serialized, identity.serialized)
        self.assertEqual(plan.selected_candidate.stream_url, candidate.stream_url)
        self.assertEqual(tuple(item.name for item in plan.target_intents), ("Auto Target",))
        self.assertEqual(plan.recording_duration_min, 90.0)

    def test_same_exact_winning_stream_keeps_tied_target_intents(self):
        candidate = sony_candidate(
            title='Final: Team A / Team B',
            fps=50.0,
            bitrate=6_000_000,
            playlist="https://example.test/a.m3u",
        )
        target_a = IdentityTarget(
            name="Target A",
            policy=POLICY_MANUAL,
            source_groups=("SONYLIV_EVENTS",),
            primary=("final",),
            worker_recording_duration_min=60,
        )
        target_b = IdentityTarget(
            name="Target B",
            policy=POLICY_MANUAL,
            source_groups=("SONYLIV_EVENTS",),
            primary=("team",),
            worker_recording_duration_min=None,
        )
        identity, snapshot = self.snapshot_for(
            (target_a, target_b),
            {
                "Target A": (candidate,),
                "Target B": (candidate,),
            },
        )

        plan = build_manual_launch_plan(snapshot, identity.serialized, now_ts=1.0)

        self.assertEqual(
            tuple(item.name for item in plan.target_intents),
            ("Target A", "Target B"),
        )
        self.assertIsNone(plan.recording_duration_min)
        self.assertEqual(plan.base_name, "Final_ Team A _ Team B")


if __name__ == "__main__":
    unittest.main(verbosity=2)
