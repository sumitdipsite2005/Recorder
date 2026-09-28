"""Checkpoint 0 regression protection for the mature record_dynamic.py.

These tests are deliberately offline. They protect mature matching, ranking,
lifetime, upgrade, and recording-local source identity/fingerprint behavior
before shared-core extraction begins.
"""

from __future__ import annotations

import copy
import importlib
import json
from datetime import datetime
import os
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from urllib.error import HTTPError

from recorder_source import discovery as shared_discovery
from recorder_source import quality as shared_quality


HERE = Path(__file__).resolve().parent
RECORDER_DIR = HERE.parent


def _load_recorder():
    """Import the mature recorder with a temporary, minimal test config."""
    temp_root = tempfile.TemporaryDirectory(prefix="recorder-checkpoint0-")
    recorder_config_dir = Path(temp_root.name) / "RECORDER"
    recorder_config_dir.mkdir(parents=True, exist_ok=True)
    config_path = recorder_config_dir / "recorder_dynamic_user_config.py"
    config_path.write_text(
        "\n".join(
            [
                'NM3U8DL_PLAYLIST_PRIMARY_PHRASES = [["India"]]',
                "NM3U8DL_PLAYLIST_REQUIRED_QUALIFIERS = []",
                "NM3U8DL_PLAYLIST_REJECTED_QUALIFIERS = []",
                "NM3U8DL_PLAYLIST_PREFERRED_QUALIFIERS = []",
                'NM3U8DL_PLAYLIST_GROUP = "SONYLIV_EVENTS"',
                'NM3U8DL_PLAYLIST_GROUPS = {"COMMON": [], "SONYLIV_EVENTS": []}',
                "SCHEDULE_START = None",
                "RUN_DURATION_MIN = 1",
                'BASE_NAME = "CHECKPOINT0"',
                f"RECORDING_OUTPUT_DIR = {str(Path(temp_root.name) / 'Manual Recordings')!r}",
                "",
            ]
        ),
        encoding="utf-8",
    )

    old_onedrive = os.environ.get("OneDrive")
    old_os_name = os.name
    os.environ["OneDrive"] = temp_root.name
    if str(RECORDER_DIR) not in sys.path:
        sys.path.insert(0, str(RECORDER_DIR))

    # The production recorder supports Windows/macOS only. The CI/container used
    # for these offline tests is Linux, so emulate only the config-location branch
    # during import. The recorder itself is not modified.
    if os.name != "nt":
        os.name = "nt"

    try:
        sys.modules.pop("record_dynamic", None)
        module = importlib.import_module("record_dynamic")
    finally:
        os.name = old_os_name
        if old_onedrive is None:
            os.environ.pop("OneDrive", None)
        else:
            os.environ["OneDrive"] = old_onedrive

    # Keep the TemporaryDirectory alive for as long as the imported module exists.
    module._checkpoint0_temp_root = temp_root
    return module


RECORDER = _load_recorder()


def make_candidate(
    name: str,
    *,
    fps: float = 25.0,
    width: int = 1920,
    height: int = 1080,
    bitrate: int = 5_000_000,
    expiry: float | None = None,
    preferred: int = 0,
    scan_type: str = "progressive",
) -> dict:
    return {
        "name": name,
        "video_fps": fps,
        "video_width": width,
        "video_height": height,
        "video_bitrate_bps": bitrate,
        "video_scan_type": scan_type,
        "quality_known": True,
        "expiry": expiry,
        "preferred_qualifier_score": preferred,
    }


class RecorderSafetyBaselineTests(unittest.TestCase):
    def setUp(self):
        self.saved = {
            "NM3U8DL_PLAYLIST_PRIMARY_PHRASES": copy.deepcopy(
                RECORDER.NM3U8DL_PLAYLIST_PRIMARY_PHRASES
            ),
            "NM3U8DL_PLAYLIST_REQUIRED_QUALIFIERS": copy.deepcopy(
                RECORDER.NM3U8DL_PLAYLIST_REQUIRED_QUALIFIERS
            ),
            "NM3U8DL_PLAYLIST_REJECTED_QUALIFIERS": copy.deepcopy(
                RECORDER.NM3U8DL_PLAYLIST_REJECTED_QUALIFIERS
            ),
            "NM3U8DL_PLAYLIST_PREFERRED_QUALIFIERS": copy.deepcopy(
                RECORDER.NM3U8DL_PLAYLIST_PREFERRED_QUALIFIERS
            ),
            "NM3U8DL_PLAYLIST_GROUP": RECORDER.NM3U8DL_PLAYLIST_GROUP,
            "NM3U8DL_NEW_SOURCE_MIN_REMAINING_MIN": (
                RECORDER.NM3U8DL_NEW_SOURCE_MIN_REMAINING_MIN
            ),
        }

    def tearDown(self):
        for name, value in self.saved.items():
            setattr(RECORDER, name, value)

    def set_match_rules(
        self,
        *,
        group: str,
        primary: list,
        required: list | None = None,
        rejected: list | None = None,
        preferred: list | None = None,
    ) -> None:
        RECORDER.NM3U8DL_PLAYLIST_GROUP = group
        RECORDER.NM3U8DL_PLAYLIST_PRIMARY_PHRASES = primary
        RECORDER.NM3U8DL_PLAYLIST_REQUIRED_QUALIFIERS = required or []
        RECORDER.NM3U8DL_PLAYLIST_REJECTED_QUALIFIERS = rejected or []
        RECORDER.NM3U8DL_PLAYLIST_PREFERRED_QUALIFIERS = preferred or []

    def test_manual_header_and_runtime_status_share_recording_start(self):
        state = RECORDER.RecorderState()
        state.identity_launch_request = SimpleNamespace(
            target_intents=(SimpleNamespace(name="Asian Games"),)
        )
        published = []
        state.identity_status_callback = published.append
        engine = RECORDER.build_engine_registry()[RECORDER.ENGINE_NM3U8DL]
        start_ts = datetime(2026, 9, 25, 20, 53, 4).timestamp()
        logged = []

        with (
            patch.object(RECORDER, "SCHEDULE_START", None),
            patch.object(RECORDER, "RUN_DURATION_MIN", None),
            patch.object(RECORDER.time, "time", return_value=start_ts),
            patch.object(
                RECORDER,
                "log",
                side_effect=lambda message="", *args, **kwargs: logged.append(str(message)),
            ),
        ):
            RECORDER.wait_until_start(state, engine)
            RECORDER._publish_identity_runtime_status(
                state,
                "WAITING_FOR_SOURCE",
                reason="test",
            )

        self.assertEqual(state.stats["process_start"], start_ts)
        self.assertIn(
            "Recording start   : 2026-09-25 20:53:04",
            logged,
        )
        self.assertEqual(
            published[-1]["recording_started_at"],
            start_ts,
        )

    def test_event_matching_keeps_and_or_and_qualifier_semantics(self):
        self.set_match_rules(
            group="SONYLIV_EVENTS",
            primary=[["Asian Games"], ["India", "Bharat"]],
            required=[["English", "ENG"]],
            rejected=[["Hindi", "HIN"]],
            preferred=[["50fps", "50 FPS"], ["Main"]],
        )
        playlist = """#EXTM3U
#EXTINF:-1 tvg-name="Asian Games India English Main 50fps" group-title="Cricket",Asian Games India English Main 50fps
https://example.test/one.m3u8
#EXTINF:-1 tvg-name="Asian Games Bharat ENG" group-title="Hockey",Asian Games Bharat ENG
https://example.test/two.m3u8
#EXTINF:-1 tvg-name="Asian Games India English Hindi" group-title="Cricket",Asian Games India English Hindi
https://example.test/rejected.m3u8
#EXTINF:-1 tvg-name="Asian Games English" group-title="Cricket",Asian Games English
https://example.test/missing-primary-group.m3u8
"""
        matches = RECORDER.find_nm3u8dl_playlist_entries(playlist)
        self.assertEqual([item["stream_url"] for item in matches], [
            "https://example.test/one.m3u8",
            "https://example.test/two.m3u8",
        ])
        self.assertEqual(matches[0]["preferred_qualifier_score"], 2)
        self.assertEqual(matches[1]["preferred_qualifier_score"], 0)

    def test_fixed_channel_matching_remains_distinct_from_event_matching(self):
        self.set_match_rules(
            group="SONY_TV",
            primary=[["Sony Sports Ten 1"]],
            rejected=[["Hindi"]],
            preferred=[["HD"]],
        )
        playlist = """#EXTM3U
#EXTINF:-1 tvg-name="Sony Sports Ten 1 HD" group-title="Sports",Sony Sports Ten 1 HD East
https://example.test/valid.m3u8
#EXTINF:-1 tvg-name="Sony Sports Ten 10 HD" group-title="Sports",Sony Sports Ten 10 HD
https://example.test/ten.m3u8
#EXTINF:-1 tvg-name="Other Channel" group-title="Sony Sports Ten 1 HD",Other Channel
https://example.test/group-only.m3u8
#EXTINF:-1 tvg-name="Sony Sports Ten 1 Hindi HD" group-title="Sports",Sony Sports Ten 1 Hindi HD
https://example.test/rejected.m3u8
"""
        matches = RECORDER.find_nm3u8dl_playlist_entries(playlist)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["stream_url"], "https://example.test/valid.m3u8")
        self.assertEqual(matches[0]["preferred_qualifier_score"], 1)

    def test_mandatory_join_prefers_normal_minimum_lifetime(self):
        now = 1_000_000.0
        RECORDER.NM3U8DL_NEW_SOURCE_MIN_REMAINING_MIN = 15
        short_better = make_candidate(
            "short-1080p50", fps=50, expiry=now + 4 * 60
        )
        safe_lower = make_candidate(
            "safe-720p25", fps=25, height=720, width=1280, expiry=now + 20 * 60
        )
        selected = RECORDER.get_nm3u8dl_join_candidate(
            [short_better, safe_lower], now_ts=now
        )
        self.assertIs(selected, safe_lower)

    def test_mandatory_join_falls_back_to_best_shorter_lived_candidate(self):
        now = 1_000_000.0
        RECORDER.NM3U8DL_NEW_SOURCE_MIN_REMAINING_MIN = 15
        lower = make_candidate("720p25", height=720, width=1280, expiry=now + 8 * 60)
        better = make_candidate("1080p50", fps=50, expiry=now + 4 * 60)
        selected = RECORDER.get_nm3u8dl_join_candidate([lower, better], now_ts=now)
        self.assertIs(selected, better)

    def test_preferred_qualifier_influences_mandatory_selection(self):
        now = 1_000_000.0
        preferred_lower_quality = make_candidate(
            "preferred-720p25",
            width=1280,
            height=720,
            preferred=1,
            expiry=now + 30 * 60,
        )
        unpreferred_higher_quality = make_candidate(
            "unpreferred-1080p50",
            fps=50,
            preferred=0,
            expiry=now + 30 * 60,
        )
        selected = RECORDER.get_nm3u8dl_join_candidate(
            [unpreferred_higher_quality, preferred_lower_quality], now_ts=now
        )
        self.assertIs(selected, preferred_lower_quality)

    def test_mature_nonselection_reason_matches_shared_selection_service(self):
        from recorder_source.models import SourceCandidate
        from recorder_source.selection import selection_nonselection_reason

        now = 1_000_000.0
        RECORDER.NM3U8DL_PLAYLIST_GROUP = "SONYLIV_EVENTS"
        selected = make_candidate(
            "selected-1080p50",
            fps=50,
            expiry=now + 30 * 60,
        )
        selected.update({
            "launchable": True,
            "playlist_url": "https://selected.test/list.m3u",
            "matching_entry_index": 1,
        })
        lower = make_candidate(
            "lower-1080p25",
            fps=25,
            expiry=now + 30 * 60,
        )
        lower.update({
            "launchable": True,
            "playlist_url": "https://lower.test/list.m3u",
            "matching_entry_index": 1,
        })

        mature_reason = RECORDER._nm3u8dl_nonselection_reason(
            lower,
            selected,
            now_ts=now,
        )
        shared_reason = selection_nonselection_reason(
            SourceCandidate.from_mapping(lower),
            SourceCandidate.from_mapping(selected),
            RECORDER._get_nm3u8dl_selection_policy(),
            now_ts=now,
        )

        self.assertEqual(mature_reason, shared_reason)
        self.assertEqual(mature_reason, "not selected: lower quality")

    def test_mature_quality_ordering_is_preserved(self):
        p1080_25 = make_candidate("1080p25", fps=25)
        p1080_50 = make_candidate("1080p50", fps=50)
        p720_50 = make_candidate("720p50", fps=50, width=1280, height=720)
        i1080_25 = make_candidate("1080i25", fps=25, scan_type="interlaced")

        self.assertGreater(
            RECORDER._nm3u8dl_video_quality_rank(p1080_50),
            RECORDER._nm3u8dl_video_quality_rank(p1080_25),
        )
        self.assertGreater(
            RECORDER._nm3u8dl_video_quality_rank(p1080_50),
            RECORDER._nm3u8dl_video_quality_rank(p720_50),
        )
        self.assertGreater(
            RECORDER._nm3u8dl_video_quality_rank(p1080_50),
            RECORDER._nm3u8dl_video_quality_rank(i1080_25),
        )

    def test_unknown_expiry_profile_behavior_is_preserved(self):
        equal_unknown = make_candidate("unknown", expiry=None)
        equal_known = make_candidate("known", expiry=2_000_000)

        RECORDER.NM3U8DL_PLAYLIST_GROUP = "FANCODE"
        self.assertGreater(
            RECORDER.get_nm3u8dl_candidate_quality_rank(equal_unknown),
            RECORDER.get_nm3u8dl_candidate_quality_rank(equal_known),
        )
        self.assertTrue(RECORDER.get_nm3u8dl_playlist_profile()["allow_unknown_expiry"])

        RECORDER.NM3U8DL_PLAYLIST_GROUP = "HOTSTAR_EVENTS"
        self.assertGreater(
            RECORDER.get_nm3u8dl_candidate_quality_rank(equal_known),
            RECORDER.get_nm3u8dl_candidate_quality_rank(equal_unknown),
        )
        self.assertFalse(RECORDER.get_nm3u8dl_playlist_profile()["allow_unknown_expiry"])

    def test_optional_upgrade_keeps_stricter_lifetime_and_preference_rules(self):
        now = 1_000_000.0
        running = make_candidate(
            "running-1080p25", fps=25, preferred=1, expiry=now + 40 * 60
        )
        too_short = make_candidate(
            "too-short-1080p50", fps=50, preferred=1, expiry=now + 10 * 60
        )
        loses_preference = make_candidate(
            "unpreferred-1080p50", fps=50, preferred=0, expiry=now + 30 * 60
        )
        good_upgrade = make_candidate(
            "good-1080p50", fps=50, preferred=1, expiry=now + 30 * 60
        )

        self.assertIsNone(
            RECORDER.get_nm3u8dl_quality_upgrade_candidate(
                running, [too_short, loses_preference], 50, 15, now_ts=now
            )
        )
        selected = RECORDER.get_nm3u8dl_quality_upgrade_candidate(
            running, [too_short, loses_preference, good_upgrade], 50, 15, now_ts=now
        )
        self.assertEqual(selected["name"], "good-1080p50")

    def test_same_motion_class_can_upgrade_720p50_to_1080p50(self):
        now = 1_000_000.0
        running = make_candidate(
            "running-720p50", fps=50, width=1280, height=720, expiry=now + 40 * 60
        )
        upgrade = make_candidate(
            "upgrade-1080p50", fps=50, width=1920, height=1080, expiry=now + 40 * 60
        )
        selected = RECORDER.get_nm3u8dl_quality_upgrade_candidate(
            running, [upgrade], 50, 15, now_ts=now
        )
        self.assertEqual(selected["name"], "upgrade-1080p50")

    def test_jio_star_sports_stops_quality_scans_at_1080p50(self):
        with patch.object(
            RECORDER,
            "NM3U8DL_PLAYLIST_GROUP",
            "JIO_STAR_SPORTS",
        ):
            profile = RECORDER.get_nm3u8dl_playlist_profile()
            self.assertTrue(profile["quality_upgrade_enabled"])
            self.assertTrue(profile["quality_upgrade_1080p50_ceiling"])

            self.assertTrue(
                RECORDER._nm3u8dl_quality_upgrade_cutoff_reached(
                    {
                        "video_width": 1920,
                        "video_height": 1080,
                        "video_fps": 50,
                        "video_scan_type": "progressive",
                    },
                    profile,
                )
            )
            self.assertFalse(
                RECORDER._nm3u8dl_quality_upgrade_cutoff_reached(
                    {
                        "video_width": 1280,
                        "video_height": 720,
                        "video_fps": 50,
                        "video_scan_type": "progressive",
                    },
                    profile,
                )
            )

    def test_failover_fingerprint_changes_with_effective_session_state(self):
        base = {
            "manifest_final_url": "https://media.example.test/live/master.m3u8?token=one",
            "effective_headers": {
                "Authorization": "Bearer A",
                "Cookie": "session=one",
                "Referer": "https://example.test/",
                "Origin": "https://example.test",
            },
        }
        same = copy.deepcopy(base)
        changed_auth = copy.deepcopy(base)
        changed_auth["effective_headers"]["Authorization"] = "Bearer B"
        changed_url = copy.deepcopy(base)
        changed_url["manifest_final_url"] = (
            "https://media.example.test/live/master.m3u8?token=two"
        )

        self.assertEqual(
            RECORDER.get_nm3u8dl_stream_fingerprint(base),
            RECORDER.get_nm3u8dl_stream_fingerprint(same),
        )
        self.assertNotEqual(
            RECORDER.get_nm3u8dl_stream_fingerprint(base),
            RECORDER.get_nm3u8dl_stream_fingerprint(changed_auth),
        )
        self.assertNotEqual(
            RECORDER.get_nm3u8dl_stream_fingerprint(base),
            RECORDER.get_nm3u8dl_stream_fingerprint(changed_url),
        )
        self.assertEqual(RECORDER.get_nm3u8dl_stream_fingerprint({}), "")

    def test_rotating_redirector_is_quarantined_after_two_failed_sessions(self):
        state = RECORDER.RecorderState()
        exposed_url = "https://wrapper.test/live/channel.m3u8"
        headers = {"Referer": "https://wrapper.test/"}

        def source(final_url):
            return {
                "stream_url": exposed_url,
                "manifest_final_url": final_url,
                "manifest_reachable": True,
                "headers": dict(headers),
                "effective_headers": dict(headers),
                "stream_type": "HLS",
            }

        with patch.object(RECORDER, "log"):
            first = source("https://edge.test/session-a/master.m3u8")
            RECORDER._nm3u8dl_set_running_stream_identity(state, first)
            self.assertEqual(
                RECORDER._nm3u8dl_handle_playlist_stream_failure(
                    state,
                    "NO_FILE_APPEAR",
                ),
                "retry",
            )
            self.assertEqual(
                RECORDER._nm3u8dl_handle_playlist_stream_failure(
                    state,
                    "NO_FILE_APPEAR",
                ),
                "rescan",
            )
            self.assertFalse(state.nm3u8dl_bad_stream_routes)

            second = source("https://edge.test/session-b/master.m3u8")
            RECORDER._nm3u8dl_set_running_stream_identity(state, second)
            RECORDER._nm3u8dl_handle_playlist_stream_failure(
                state,
                "NO_FILE_APPEAR",
            )
            RECORDER._nm3u8dl_handle_playlist_stream_failure(
                state,
                "NO_FILE_APPEAR",
            )

        route_key = RECORDER.get_nm3u8dl_stream_route_fingerprint(second)
        self.assertIn(route_key, state.nm3u8dl_bad_stream_routes)

        third = source("https://edge.test/session-c/master.m3u8")
        RECORDER._nm3u8dl_mark_bad_fingerprint_candidates(state, [third])
        self.assertTrue(third["failover_excluded"])
        self.assertIn(
            "redirecting source repeatedly failed",
            third["failover_exclusion_reason"],
        )
        self.assertNotIn(
            RECORDER.get_nm3u8dl_stream_fingerprint(third),
            state.nm3u8dl_bad_stream_fingerprints,
        )

        unrelated = {
            **source("https://edge.test/session-d/master.m3u8"),
            "stream_url": "https://other-wrapper.test/live/channel.m3u8",
        }
        RECORDER._nm3u8dl_mark_bad_fingerprint_candidates(
            state,
            [unrelated],
        )
        self.assertFalse(unrelated.get("failover_excluded", False))

    def test_literal_ip_failure_excludes_all_same_ip_candidates(self):
        state = RECORDER.RecorderState()
        failed_ip = "103.211.103.215"
        headers = {"User-Agent": "test"}

        failed_source = {
            "stream_url": "http://mag.diamondtv.one/live/a/302025.m3u8",
            "manifest_final_url": (
                f"http://{failed_ip}:61980/live/play/token-a/302025"
            ),
            "manifest_reachable": True,
            "headers": dict(headers),
            "effective_headers": dict(headers),
            "stream_type": "HLS",
        }

        with patch.object(RECORDER, "log"):
            RECORDER._nm3u8dl_set_running_stream_identity(
                state,
                failed_source,
            )
            self.assertEqual(
                RECORDER._nm3u8dl_handle_playlist_stream_failure(
                    state,
                    "NO_FILE_APPEAR",
                ),
                "retry",
            )
            self.assertEqual(
                RECORDER._nm3u8dl_handle_playlist_stream_failure(
                    state,
                    "NO_FILE_APPEAR",
                ),
                "rescan",
            )

        self.assertIn(failed_ip, state.nm3u8dl_bad_stream_ips)

        same_ip_candidates = [
            {
                "stream_url": "http://wrapper-one.test/live/channel.m3u8",
                "manifest_final_url": (
                    f"http://{failed_ip}:61980/live/play/token-b/302025"
                ),
                "headers": {},
                "effective_headers": {},
            },
            {
                "stream_url": "http://wrapper-two.test/live/channel.m3u8",
                "selected_media_final_url": (
                    f"http://{failed_ip}:61980/live/play/token-c/302025"
                ),
                "manifest_final_url": "http://wrapper-two.test/final.m3u8",
                "headers": {},
                "effective_headers": {},
            },
            {
                "stream_url": (
                    f"http://{failed_ip}:61980/live/play/token-d/302025"
                ),
                "headers": {},
                "effective_headers": {},
            },
        ]
        other_ip = {
            "stream_url": "http://wrapper-three.test/live/channel.m3u8",
            "manifest_final_url": (
                "http://103.211.103.216:61980/live/play/token-e/302025"
            ),
            "headers": {},
            "effective_headers": {},
        }

        RECORDER._nm3u8dl_mark_bad_fingerprint_candidates(
            state,
            same_ip_candidates + [other_ip],
        )

        for candidate in same_ip_candidates:
            self.assertTrue(candidate["failover_excluded"])
            self.assertIn(
                failed_ip,
                candidate["failover_exclusion_reason"],
            )
        self.assertFalse(other_ip.get("failover_excluded", False))

        fresh_wrapper = {
            "stream_url": "http://new-wrapper.test/live/channel.m3u8",
            "playlist_url": "https://example.test/pocket.m3u",
            "headers": {},
            "keys": [],
        }
        redirected_final = (
            f"http://{failed_ip}:61980/live/play/token-f/302025"
        )

        with patch.object(
            RECORDER,
            "_fetch_nm3u8dl_stream_manifest_text",
            return_value=(
                "#EXTM3U\n#EXT-X-TARGETDURATION:6\n",
                redirected_final,
            ),
        ), patch.object(
            RECORDER.source_quality,
            "inspect_manifest_probe_evidence",
        ) as manifest_inspector, patch.object(
            RECORDER,
            "_ffprobe_nm3u8dl_stream_quality",
        ) as ffprobe, patch.object(
            RECORDER,
            "_sample_nm3u8dl_stream_video_bitrate",
        ) as bitrate_sampler, patch.object(
            RECORDER,
            "_detect_nm3u8dl_stream_scan_type_with_idet",
        ) as idet:
            quality = RECORDER._probe_nm3u8dl_candidate_quality(
                fresh_wrapper,
                excluded_literal_ips=set(
                    state.nm3u8dl_bad_stream_ips
                ),
            )

        self.assertEqual(
            quality["manifest_final_url"],
            redirected_final,
        )
        self.assertTrue(quality["failover_excluded"])
        self.assertIn(
            failed_ip,
            quality["failover_exclusion_reason"],
        )
        manifest_inspector.assert_not_called()
        ffprobe.assert_not_called()
        bitrate_sampler.assert_not_called()
        idet.assert_not_called()

    def test_launch_uses_validated_final_manifest_url(self):
        source = {
            "stream_url": "https://wrapper.test/channel.m3u8",
            "manifest_final_url": "https://edge.test/session/master.m3u8",
            "manifest_reachable": True,
        }
        self.assertEqual(
            RECORDER.get_nm3u8dl_launch_stream_url(source),
            "https://edge.test/session/master.m3u8",
        )
        source["manifest_reachable"] = False
        self.assertEqual(
            RECORDER.get_nm3u8dl_launch_stream_url(source),
            "https://wrapper.test/channel.m3u8",
        )

    def test_launch_uses_validated_extensionless_final_manifest_url(self):
        source = {
            "stream_url": "http://wrapper.test/channel.m3u8",
            "manifest_final_url": "http://103.211.103.215:61980/live/play/token/302025",
            "manifest_reachable": True,
            "stream_type": "HLS",
        }
        self.assertEqual(
            RECORDER.get_nm3u8dl_launch_stream_url(source),
            source["manifest_final_url"],
        )

    def test_redirected_literal_ip_stream_skips_idet_probe(self):
        wrapper_url = "http://mag.diamondtv.one/live/test/302025.m3u8"
        final_url = "http://103.211.103.215:61980/live/play/token/302025"

        inspection = {
            "manifest_reachable": True,
            "stream_type": "HLS",
            "video_width": 1920,
            "video_height": 1080,
            "video_fps": 50.0,
            "video_scan_type": "",
            "video_bitrate_bps": 0,
        }
        ffprobe_quality = {
            "quality_known": True,
            "video_width": 1920,
            "video_height": 1080,
            "video_fps": 50.0,
            "video_scan_type": "",
            "video_bitrate_bps": 0,
        }
        candidate = {
            "stream_url": wrapper_url,
            "playlist_url": "https://example.test/pocket.m3u",
            "headers": {},
            "keys": [],
        }

        with patch.object(
            RECORDER,
            "_fetch_nm3u8dl_stream_manifest_text",
            return_value=("#EXTM3U\n#EXT-X-TARGETDURATION:6\n", final_url),
        ), patch.object(
            RECORDER.source_quality,
            "inspect_manifest_probe_evidence",
            return_value=(inspection, {"stream_type": "HLS"}, None),
        ), patch.object(
            RECORDER,
            "_ffprobe_nm3u8dl_stream_quality",
            return_value=ffprobe_quality,
        ), patch.object(
            RECORDER,
            "_detect_nm3u8dl_stream_scan_type_with_idet",
        ) as idet:
            quality = RECORDER._probe_nm3u8dl_candidate_quality(candidate)

        self.assertEqual(quality["manifest_final_url"], final_url)
        idet.assert_not_called()

    def test_external_probe_text_decode_replaces_invalid_bytes(self):
        captured = {}

        def fake_run(command, **kwargs):
            captured.update(kwargs)
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        with patch.object(RECORDER.subprocess, "run", side_effect=fake_run):
            RECORDER._run_nm3u8dl_external_capture_redacted(
                ["ffprobe", "-version"],
                raw_tool="ffprobe",
                raw_context="decode test",
                timeout=1,
            )

        self.assertTrue(captured["text"])
        self.assertEqual(captured["errors"], "replace")

    def test_mature_and_shared_effective_headers_are_identical(self):
        cases = (
            ("HOTSTAR_EVENTS", "HOTSTAR"),
            ("JIO_STAR_SPORTS", "JIO"),
            ("KHEL", "KHEL"),
            ("SONYLIV_EVENTS", "SONYLIV"),
            ("FANCODE", "FANCODE"),
        )
        metadata = {
            "Referer": "https://override.test/",
            "Cookie": "session=one",
            "User-Agent": "",
        }
        for group, provider in cases:
            with self.subTest(group=group):
                RECORDER.NM3U8DL_PLAYLIST_GROUP = group
                profile = RECORDER.get_nm3u8dl_playlist_profile()
                mature = RECORDER.get_nm3u8dl_effective_headers(metadata)
                shared = shared_discovery.build_effective_probe_headers(
                    provider,
                    {
                        "Referer": "https://override.test/",
                        "Cookie": "session=one",
                    },
                    base_headers=profile["added_headers"],
                    default_user_agent=RECORDER.NM3U8DL_PLAYLIST_USER_AGENTS["DEFAULT"],
                )
                self.assertEqual(mature, shared)

    def test_mature_and_shared_clearkey_normalization_are_identical(self):
        payload = json.dumps({
            "keys": [{
                "kty": "oct",
                "kid": "uoiW1gUkaHGsQkh4SR2GoQ",
                "k": "hgDUFTA0s8vIUvE-pLdILA",
            }]
        })
        self.assertEqual(
            RECORDER.normalize_nm3u8dl_playlist_license_key(payload),
            list(shared_discovery.normalize_playlist_license_key(payload)),
        )

    def test_mature_and_shared_dash_parser_are_identical(self):
        manifest = """<?xml version="1.0"?>
<MPD type="static">
  <Period>
    <AdaptationSet contentType="video">
      <Representation id="v25" width="1280" height="720" bandwidth="2500000" frameRate="25"/>
      <Representation id="v50" width="1920" height="1080" bandwidth="4500000" frameRate="50" scanType="progressive">
        <BaseURL>video/</BaseURL>
        <SegmentTemplate initialization="init-$RepresentationID$.mp4" media="seg-$Number$.m4s" startNumber="3" duration="2" timescale="1"/>
      </Representation>
    </AdaptationSet>
  </Period>
</MPD>"""
        mature = RECORDER._parse_nm3u8dl_dash_manifest_quality(
            manifest,
            "https://cdn.test/live/manifest.mpd",
        )
        shared = shared_quality.parse_dash_manifest_quality(
            manifest,
            "https://cdn.test/live/manifest.mpd",
            motion_cap_fps=RECORDER.NM3U8DL_QUALITY_RANKING_MOTION_CAP_FPS,
        )
        self.assertEqual(mature, shared)

    def test_mature_and_shared_ffprobe_bitrate_completion_are_identical(self):
        stdout = json.dumps({
            "streams": [{
                "index": 2,
                "codec_name": "h264",
                "width": 1920,
                "height": 1080,
                "avg_frame_rate": "25/1",
                "r_frame_rate": "25/1",
                "bit_rate": "0",
                "field_order": "progressive",
            }],
            "format": {"bit_rate": "0"},
        })
        result = SimpleNamespace(returncode=1, stdout=stdout, stderr="late warning")
        shared_sampled_streams = []

        shared = shared_quality.probe_stream_quality_ffprobe(
            "https://cdn.test/live/video.m3u8",
            {"Referer": "https://www.fancode.com/"},
            sample_missing_bitrate=True,
            bitrate_sample_callback=lambda stream_index: (
                shared_sampled_streams.append(stream_index) or 3_523_000
            ),
            runner=lambda args, timeout: result,
        )

        with patch.object(
            RECORDER,
            "_run_nm3u8dl_external_capture_redacted",
            return_value=result,
        ), patch.object(
            RECORDER,
            "_sample_nm3u8dl_stream_video_bitrate",
            return_value=3_523_000,
        ) as mature_sampler:
            mature = RECORDER._ffprobe_nm3u8dl_stream_quality(
                "https://cdn.test/live/video.m3u8",
                {"Referer": "https://www.fancode.com/"},
                sample_missing_bitrate=True,
            )

        keys = (
            "quality_known",
            "video_width",
            "video_height",
            "video_fps",
            "video_scan_type",
            "video_bitrate_bps",
            "video_bitrate_source",
            "_ffprobe_stream_index",
        )
        self.assertEqual(
            {key: mature.get(key) for key in keys},
            {key: shared.get(key) for key in keys},
        )
        self.assertEqual(shared_sampled_streams, [2])
        self.assertEqual(
            mature_sampler.call_args.kwargs["stream_index"],
            2,
        )

    def test_mature_access_classification_uses_shared_rule(self):
        RECORDER.NM3U8DL_PLAYLIST_GROUP = "FANCODE"
        error = HTTPError("https://x", 403, "Forbidden", {}, None)
        candidate = {
            "stream_url": "https://x/live.m3u8",
            "headers": {},
            "keys": [],
        }
        with patch.object(
            RECORDER,
            "_fetch_nm3u8dl_stream_manifest_text",
            side_effect=error,
        ):
            quality = RECORDER._probe_nm3u8dl_candidate_quality(candidate)
        self.assertTrue(quality["access_blocked"])
        self.assertEqual(quality["access_block_kind"], "vpn_route_suspected")
        self.assertEqual(quality["access_block_http_status"], 403)

    def test_manual_rejection_signature_is_broader_than_one_url(self):
        first = make_candidate("first", fps=50)
        first.update({
            "stream_type": "HLS",
            "stream_url": "https://dishmt.slivcdn.com/live/a/master.m3u8",
            "selected_media_final_url": "https://dishmt.slivcdn.com/live/a/1080.m3u8",
        })
        second = copy.deepcopy(first)
        second["stream_url"] = "https://dishmt.slivcdn.com/live/b/master.m3u8"
        second["selected_media_final_url"] = "https://dishmt.slivcdn.com/live/b/1080.m3u8"
        changed_quality = copy.deepcopy(second)
        changed_quality["video_fps"] = 25.0

        first_sig = RECORDER.get_nm3u8dl_manual_feed_signature(first)
        second_sig = RECORDER.get_nm3u8dl_manual_feed_signature(second)
        changed_sig = RECORDER.get_nm3u8dl_manual_feed_signature(changed_quality)

        self.assertIsNotNone(first_sig)
        self.assertEqual(first_sig["key"], second_sig["key"])
        self.assertNotEqual(first_sig["key"], changed_sig["key"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
