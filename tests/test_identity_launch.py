from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from recorder_runtime.identity_launch import (
    FrozenTargetIntent,
    IdentityLaunchRequest,
    read_launch_request,
    target_intents_for_recovery_playlist,
    write_launch_request_temp,
)
from recorder_source.models import SourceCandidate


class IdentityLaunchRequestTests(unittest.TestCase):
    def make_request(self) -> IdentityLaunchRequest:
        candidate = SourceCandidate(
            playlist_url="https://example.test/list.m3u",
            matching_entry_index=7,
            entry_title="Example Event",
            stream_url="https://cdn.example.test/live/master.m3u8",
            headers={"Referer": "https://example.test/"},
            keys=("00112233445566778899aabbccddeeff:ffeeddccbbaa99887766554433221100",),
            launchable=True,
            video_width=1920,
            video_height=1080,
            video_fps=50.0,
            extra={
                "provider": "SONYLIV",
                "source_group": "SONYLIV_EVENTS",
            },
        )
        target = FrozenTargetIntent(
            name="Example",
            source_groups=("SONYLIV_EVENTS",),
            recovery_playlist_urls=("https://example.test/list.m3u",),
            primary=(("example", "event"),),
            required=("english",),
            worker_recording_duration_min=120.0,
        )
        return IdentityLaunchRequest(
            registry_session_id="session-1",
            identity_key="SONYLIV|lane:abc/live/english",
            provider="SONYLIV",
            selected_source_group="SONYLIV_EVENTS",
            selected_candidate=candidate,
            target_intents=(target,),
            recovery_playlist_urls=("https://example.test/list.m3u",),
            recording_duration_min=120.0,
            base_name="Example Event",
        )

    def test_round_trip_preserves_exact_start_source_and_frozen_intent(self):
        request = self.make_request()

        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "request.json"
            path.write_text(
                __import__("json").dumps(request.to_mapping()),
                encoding="utf-8",
            )
            loaded = read_launch_request(path)

        self.assertEqual(loaded.identity_key, request.identity_key)
        self.assertEqual(
            loaded.selected_candidate.stream_url,
            request.selected_candidate.stream_url,
        )
        self.assertEqual(
            dict(loaded.selected_candidate.headers),
            dict(request.selected_candidate.headers),
        )
        self.assertEqual(
            loaded.target_intents[0].primary,
            request.target_intents[0].primary,
        )
        self.assertEqual(
            loaded.target_intents[0].recovery_playlist_urls,
            ("https://example.test/list.m3u",),
        )
        self.assertEqual(
            loaded.recovery_playlist_urls,
            ("https://example.test/list.m3u",),
        )
        self.assertEqual(loaded.recording_duration_min, 120.0)

    def test_recovery_playlist_keeps_only_its_own_frozen_searches(self):
        target_a = FrozenTargetIntent(
            name="A",
            source_groups=("SONYLIV_EVENTS",),
            recovery_playlist_urls=("https://a.test/list.m3u",),
            primary=("boxing",),
        )
        target_b = FrozenTargetIntent(
            name="B",
            source_groups=("SONY_TV",),
            recovery_playlist_urls=("https://b.test/list.m3u",),
            primary=("india",),
        )

        self.assertEqual(
            tuple(
                item.name
                for item in target_intents_for_recovery_playlist(
                    (target_a, target_b),
                    "https://a.test/list.m3u",
                )
            ),
            ("A",),
        )
        self.assertEqual(
            tuple(
                item.name
                for item in target_intents_for_recovery_playlist(
                    (target_a, target_b),
                    "https://b.test/list.m3u",
                )
            ),
            ("B",),
        )

    def test_temp_handoff_can_be_consumed_and_deleted(self):
        request = self.make_request()

        path = write_launch_request_temp(request)
        self.assertTrue(path.exists())

        loaded = read_launch_request(path, delete_after_read=True)

        self.assertEqual(loaded.base_name, "Example Event")
        self.assertFalse(path.exists())

    def test_nonlaunchable_candidate_is_rejected(self):
        request = self.make_request()
        with self.assertRaisesRegex(ValueError, "must already be launchable"):
            IdentityLaunchRequest(
                registry_session_id=request.registry_session_id,
                identity_key=request.identity_key,
                provider=request.provider,
                selected_source_group=request.selected_source_group,
                selected_candidate=SourceCandidate(
                    stream_url=request.selected_candidate.stream_url,
                    launchable=False,
                ),
                target_intents=request.target_intents,
                recovery_playlist_urls=request.recovery_playlist_urls,
                recording_duration_min=request.recording_duration_min,
                base_name=request.base_name,
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
