from __future__ import annotations

import json
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from recorder_source.discovery import (
    adapt_json_playlist_text,
    discover_playlist_text,
    fetch_playlist_documents,
    parse_extinf_metadata,
    parse_playlist_text,
    probe_candidate_hls,
)
from recorder_source.quality import parse_ffprobe_quality_output
from recorder_source.matching import (
    build_match_groups,
    evaluate_match,
    make_match_definition,
    normalize_match_text,
)
from recorder_source.models import (
    MATCH_MODE_EVENT_PHRASE,
    MATCH_MODE_EXACT_CHANNEL,
    PlaylistSourceSpec,
    SelectionPolicy,
    SourceAcquisitionRequest,
    SourceCandidate,
)
from recorder_source.selection import (
    candidate_quality_rank,
    comparable_motion_fps,
    select_join_candidate,
    select_quality_upgrade,
    video_quality_rank,
)


def candidate(name: str = "x", **kw) -> SourceCandidate:
    data = dict(
        entry_title=name,
        quality_known=True,
        video_width=1920,
        video_height=1080,
        video_fps=25.0,
        video_bitrate_bps=5_000_000,
        video_scan_type="progressive",
        launchable=True,
        probe_status="working",
    )
    data.update(kw)
    return SourceCandidate(**data)


class MatchingTests(unittest.TestCase):
    def test_normalization_handles_unicode_punctuation_and_apostrophes(self):
        self.assertEqual(normalize_match_text("Women’s—T20, Final!"), "womens t20 final")

    def test_build_match_groups_keeps_and_of_or_shape(self):
        self.assertEqual(
            build_match_groups([["Asian Games", "AG"], "India", ["", "ENG"]]),
            (("Asian Games", "AG"), ("India",), ("ENG",)),
        )

    def test_event_match_supports_and_or_required_rejected_preferred(self):
        definition = make_match_definition(
            mode=MATCH_MODE_EVENT_PHRASE,
            primary=[["Asian Games", "AG"], ["India", "Bharat"]],
            required=[["English", "ENG"]],
            rejected=[["Hindi", "HIN"]],
            preferred=[["Main"], ["50fps", "50 FPS"]],
        )
        result = evaluate_match(
            definition,
            tvg_name="Asian Games India English Main 50fps",
            group_title="Sports",
            entry_title="Asian Games India English Main 50fps",
            stream_url="https://example.test/live.m3u8",
        )
        self.assertTrue(result.matches)
        self.assertEqual(result.preferred_qualifier_score, 2)

    def test_event_match_can_find_primary_in_stream_path(self):
        definition = make_match_definition(
            mode=MATCH_MODE_EVENT_PHRASE,
            primary=[["cricodi2308"]],
        )
        result = evaluate_match(
            definition,
            tvg_name="Other",
            group_title="Sports",
            entry_title="Other",
            stream_url="https://cdn.test/hls/live/123/cricodi2308/ENG/master.m3u8",
        )
        self.assertTrue(result.matches)

    def test_exact_channel_does_not_match_group_title_only(self):
        definition = make_match_definition(
            mode=MATCH_MODE_EXACT_CHANNEL,
            primary=[["Sony Sports Ten 1"]],
        )
        result = evaluate_match(
            definition,
            tvg_name="Other Channel",
            group_title="Sony Sports Ten 1 HD",
            entry_title="Other Channel",
            stream_url="https://example.test/live.m3u8",
        )
        self.assertFalse(result.matches)

    def test_exact_channel_does_not_confuse_ten_1_with_ten_10(self):
        definition = make_match_definition(
            mode=MATCH_MODE_EXACT_CHANNEL,
            primary=[["Sony Sports Ten 1"]],
        )
        result = evaluate_match(
            definition,
            tvg_name="Sony Sports Ten 10 HD",
            group_title="Sports",
            entry_title="Sony Sports Ten 10 HD",
            stream_url="https://example.test/live.m3u8",
        )
        self.assertFalse(result.matches)

    def test_match_all_allows_empty_primary_but_keeps_qualifier_gates(self):
        definition = make_match_definition(
            mode=MATCH_MODE_EVENT_PHRASE,
            primary=(),
            required=[["ENG"]],
            rejected=[["HIN"]],
            match_all=True,
        )
        self.assertTrue(evaluate_match(
            definition,
            tvg_name="Feed ENG",
            group_title="Sports",
            entry_title="Anything",
            stream_url="https://example.test/live.m3u8",
        ).matches)
        self.assertFalse(evaluate_match(
            definition,
            tvg_name="Feed ENG HIN",
            group_title="Sports",
            entry_title="Anything",
            stream_url="https://example.test/live.m3u8",
        ).matches)


class SelectionTests(unittest.TestCase):
    def setUp(self):
        self.policy = SelectionPolicy(900, 900, allow_unknown_expiry=False)
        self.now = 1_000_000.0

    def test_join_prefers_safe_lifetime_over_better_short_candidate(self):
        short = candidate("short", video_fps=50, expiry=self.now + 300)
        safe = candidate("safe", video_width=1280, video_height=720, expiry=self.now + 1200)
        result = select_join_candidate([short, safe], self.policy, now_ts=self.now)
        self.assertEqual(result.selected.entry_title, "safe")
        self.assertFalse(result.fallback_used)

    def test_join_falls_back_when_all_candidates_are_short(self):
        lower = candidate("lower", video_width=1280, video_height=720, expiry=self.now + 600)
        higher = candidate("higher", video_fps=50, expiry=self.now + 300)
        result = select_join_candidate([lower, higher], self.policy, now_ts=self.now)
        self.assertEqual(result.selected.entry_title, "higher")
        self.assertTrue(result.fallback_used)

    def test_unknown_expiry_rejected_when_policy_disallows(self):
        result = select_join_candidate([candidate("unknown", expiry=None)], self.policy, now_ts=self.now)
        self.assertIsNone(result.selected)
        self.assertEqual(result.decision_type, "no_eligible_candidates")

    def test_unknown_expiry_allowed_when_policy_allows(self):
        policy = SelectionPolicy(900, 900, allow_unknown_expiry=True)
        result = select_join_candidate([candidate("unknown", expiry=None)], policy, now_ts=self.now)
        self.assertEqual(result.selected.entry_title, "unknown")

    def test_fancode_style_policy_can_prefer_unknown_expiry_on_equal_quality(self):
        policy = SelectionPolicy(
            900,
            900,
            allow_unknown_expiry=True,
            prefer_unknown_expiry_on_equal_quality=True,
        )
        unknown = candidate("unknown", expiry=None)
        known = candidate("known", expiry=self.now + 3600)
        self.assertGreater(candidate_quality_rank(unknown, policy), candidate_quality_rank(known, policy))

    def test_preferred_qualifier_outranks_quality(self):
        preferred = candidate(
            "preferred",
            video_width=1280,
            video_height=720,
            preferred_qualifier_score=1,
            expiry=self.now + 3600,
        )
        higher = candidate("higher", video_fps=50, preferred_qualifier_score=0, expiry=self.now + 3600)
        result = select_join_candidate([higher, preferred], self.policy, now_ts=self.now)
        self.assertEqual(result.selected.entry_title, "preferred")

    def test_1080p50_ranks_above_1080p25(self):
        high = candidate(video_fps=50)
        low = candidate(video_fps=25)
        self.assertGreater(video_quality_rank(high, motion_cap_fps=50), video_quality_rank(low, motion_cap_fps=50))

    def test_1080p50_ranks_above_720p50(self):
        high = candidate(video_fps=50, video_width=1920, video_height=1080)
        low = candidate(video_fps=50, video_width=1280, video_height=720)
        self.assertGreater(video_quality_rank(high, motion_cap_fps=50), video_quality_rank(low, motion_cap_fps=50))

    def test_interlaced_25_is_comparable_to_50_motion(self):
        interlaced = candidate(video_fps=25, video_scan_type="interlaced")
        self.assertEqual(comparable_motion_fps(interlaced, motion_cap_fps=50), 50)

    def test_upgrade_requires_minimum_lifetime(self):
        running = candidate("running", video_fps=25, expiry=self.now + 3600)
        short = candidate("short50", video_fps=50, expiry=self.now + 600)
        result = select_quality_upgrade(running, [short], 50, self.policy, now_ts=self.now)
        self.assertIsNone(result.selected)

    def test_upgrade_does_not_lose_preferred_qualifier(self):
        running = candidate("running", video_fps=25, preferred_qualifier_score=1, expiry=self.now + 3600)
        unpreferred = candidate("50", video_fps=50, preferred_qualifier_score=0, expiry=self.now + 3600)
        result = select_quality_upgrade(running, [unpreferred], 50, self.policy, now_ts=self.now)
        self.assertIsNone(result.selected)

    def test_upgrade_can_move_720p50_to_1080p50(self):
        running = candidate("720", video_fps=50, video_width=1280, video_height=720, expiry=self.now + 3600)
        upgrade = candidate("1080", video_fps=50, video_width=1920, video_height=1080, expiry=self.now + 3600)
        result = select_quality_upgrade(running, [upgrade], 50, self.policy, now_ts=self.now)
        self.assertEqual(result.selected.entry_title, "1080")


class DiscoveryTests(unittest.TestCase):
    def test_parse_extinf_metadata(self):
        parsed = parse_extinf_metadata('#EXTINF:-1 tvg-name="Name" group-title="Group",Entry Title')
        self.assertEqual(parsed, {"tvg_name": "Name", "group_title": "Group", "entry_title": "Entry Title"})

    def test_parse_playlist_preserves_metadata_only_observation(self):
        text = '#EXTM3U\n#EXTINF:-1 tvg-name="Asian Games" group-title="Sports",Asian Games\n'
        result = parse_playlist_text(text, playlist_url="https://src.test/list.m3u", source_name="src", provider="SONYLIV")
        self.assertEqual(len(result.candidates), 1)
        self.assertEqual(result.candidates[0].stream_url, "")
        self.assertEqual(result.candidates[0].entry_title, "Asian Games")

    def test_parse_playlist_preserves_source_provenance(self):
        text = '#EXTM3U\n#EXTINF:-1 tvg-name="A" group-title="G",A\nhttps://cdn.test/live.m3u8\n'
        result = parse_playlist_text(text, playlist_url="https://src.test/list.m3u", source_name="Source A", source_group="SONYLIV_EVENTS", provider="SONYLIV")
        c = result.candidates[0]
        self.assertEqual(c.playlist_url, "https://src.test/list.m3u")
        self.assertEqual(c.extra["source_name"], "Source A")
        self.assertEqual(c.extra["source_group"], "SONYLIV_EVENTS")
        self.assertEqual(c.extra["provider"], "SONYLIV")

    def test_header_precedence_pipe_over_exthttp_over_extvlc(self):
        text = '''#EXTM3U
#EXTINF:-1 tvg-name="A",A
#EXTVLCOPT:http-user-agent=VLC-UA
#EXTVLCOPT:http-referrer=https://vlc.test/
#EXTHTTP:{"User-Agent":"HTTP-UA","Referer":"https://http.test/","Cookie":"a=1"}
https://cdn.test/live.m3u8|User-Agent=PIPE-UA&Cookie=b%3D2
'''
        c = parse_playlist_text(text).candidates[0]
        self.assertEqual(c.headers["User-Agent"], "PIPE-UA")
        self.assertEqual(c.headers["Cookie"], "b=2")
        self.assertEqual(c.headers["Referer"], "https://http.test/")

    def test_playlist_stream_headers_are_retained_when_entry_has_no_override(self):
        text = '#EXTM3U\n#EXTINF:-1 tvg-name="A",A\nhttps://cdn.test/live.m3u8\n'
        c = parse_playlist_text(text, stream_headers={"User-Agent": "Configured-UA"}).candidates[0]
        self.assertEqual(c.headers["User-Agent"], "Configured-UA")

    def test_clearkey_metadata_is_parsed(self):
        text = '''#EXTM3U
#EXTINF:-1 tvg-name="A",A
#KODIPROP:inputstream.adaptive.license_type=clearkey
#KODIPROP:inputstream.adaptive.license_key=abc:def
https://cdn.test/live.mpd
'''
        c = parse_playlist_text(text).candidates[0]
        self.assertEqual(c.license_type, "clearkey")
        self.assertEqual(c.keys, ("abc:def",))
        self.assertEqual(c.stream_type, "DASH")

    def test_widevine_playlist_metadata_is_marked_unsupported(self):
        text = '''#EXTM3U
#EXTINF:-1 tvg-name="A",A
#KODIPROP:inputstream.adaptive.license_type=com.widevine.alpha
https://cdn.test/live.mpd
'''
        c = parse_playlist_text(text).candidates[0]
        self.assertEqual(c.unsupported_drm, "Widevine")

    def test_json_adapter_keeps_metadata_only_record(self):
        payload = json.dumps({"channels": [{"name": "A", "group": "Sports"}]})
        adapted, diag = adapt_json_playlist_text(payload)
        self.assertIn('tvg-name="A"', adapted)
        self.assertEqual(diag["metadata_only_record_count"], 1)
        self.assertEqual(diag["playable_record_count"], 0)

    def test_json_adapter_preserves_headers_and_key(self):
        payload = json.dumps({"items": [{
            "name": "A",
            "url": "https://cdn.test/a.mpd",
            "key_id": "kid",
            "key": "key",
            "headers": {"Authorization": "Bearer X"},
        }]})
        c = parse_playlist_text(payload).candidates[0]
        self.assertEqual(c.keys, ("kid:key",))
        self.assertEqual(c.headers["Authorization"], "Bearer X")

    def test_parse_acquisition_is_independent_of_target_matching(self):
        text = '#EXTM3U\n#EXTINF:-1 tvg-name="Completely Different",Different\nhttps://cdn.test/live.m3u8\n'
        acquired = parse_playlist_text(text)
        self.assertEqual(len(acquired.candidates), 1)
        definition = make_match_definition(mode=MATCH_MODE_EVENT_PHRASE, primary=[["Asian Games"]])
        filtered = discover_playlist_text(text, SourceAcquisitionRequest(match=definition))
        self.assertEqual(len(filtered.candidates), 0)

    def test_fetch_playlist_documents_keeps_success_when_other_source_fails(self):
        class Headers:
            def get_content_charset(self):
                return "utf-8"
        class Response:
            headers = Headers()
            def __init__(self, url): self.url = url
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def read(self): return b"#EXTM3U\n"
            def geturl(self): return self.url + "?final=1"
        def fake_urlopen(request, timeout=0):
            url = request.full_url
            if "bad" in url:
                raise OSError("boom")
            return Response(url)
        sources = [
            PlaylistSourceSpec("https://good.test/list.m3u", name="good"),
            PlaylistSourceSpec("https://bad.test/list.m3u", name="bad"),
        ]
        with patch("recorder_source.discovery.urlopen", side_effect=fake_urlopen):
            docs, errors, diag = fetch_playlist_documents(sources, max_workers=2)
        self.assertIn("https://good.test/list.m3u", docs)
        self.assertEqual(len(errors), 1)
        self.assertTrue(diag["https://good.test/list.m3u"]["ok"])
        self.assertFalse(diag["https://bad.test/list.m3u"]["ok"])

    def test_probe_no_playable_source_does_not_make_network_request(self):
        c = candidate(stream_url="", launchable=False, probe_status="unprobed")
        with patch("recorder_source.discovery.urlopen") as mocked:
            out = probe_candidate_hls(c)
        mocked.assert_not_called()
        self.assertEqual(out.probe_status, "no_playable_source")

    def test_probe_expired_url_is_rejected_before_network(self):
        expired = int(time.time()) - 60
        c = candidate(stream_url=f"https://cdn.test/live.m3u8?exp={expired}")
        with patch("recorder_source.discovery.urlopen") as mocked:
            out = probe_candidate_hls(c)
        mocked.assert_not_called()
        self.assertEqual(out.probe_status, "expired")

    def test_probe_hls_parses_best_quality_and_final_url(self):
        body = b'''#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=2500000,RESOLUTION=1280x720,FRAME-RATE=25\n720.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080,FRAME-RATE=50\n1080.m3u8\n'''
        class Headers:
            def get(self, name, default=None): return "application/vnd.apple.mpegurl"
        class Response:
            headers = Headers()
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def read(self, n=-1): return body
            def geturl(self): return "https://final.test/master.m3u8"
        with patch("recorder_source.discovery.urlopen", return_value=Response()):
            out = probe_candidate_hls(candidate(stream_url="https://src.test/master.m3u8", extra={"provider":"SONYLIV"}))
        self.assertTrue(out.launchable)
        self.assertEqual((out.video_width, out.video_height, out.video_fps), (1920, 1080, 50.0))
        self.assertEqual(out.final_stream_url, "https://final.test/master.m3u8")
        self.assertTrue(out.playback_fingerprint)

    def test_probe_dash_parses_quality(self):
        body = b'''<?xml version="1.0"?><MPD><Period><AdaptationSet><Representation width="1920" height="1080" bandwidth="4500000" frameRate="50"/></AdaptationSet></Period></MPD>'''
        class Headers:
            def get(self, name, default=None): return "application/dash+xml"
        class Response:
            headers = Headers()
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def read(self, n=-1): return body
            def geturl(self): return "https://final.test/manifest.mpd"
        with patch("recorder_source.discovery.urlopen", return_value=Response()):
            out = probe_candidate_hls(candidate(stream_url="https://src.test/manifest.mpd"))
        self.assertTrue(out.launchable)
        self.assertEqual(out.stream_type, "DASH")
        self.assertEqual((out.video_width, out.video_height, out.video_fps), (1920, 1080, 50.0))

    def test_probe_hls_fills_missing_fps_from_shared_ffprobe(self):
        body = b'''#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=3322000,RESOLUTION=1920x1080\nvideo.m3u8\n'''
        class Headers:
            def get(self, name, default=None):
                return "application/vnd.apple.mpegurl"
        class Response:
            headers = Headers()
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def read(self, n=-1): return body
            def geturl(self): return "https://final.test/master.m3u8"

        ffprobe = {
            "quality_known": True,
            "video_width": 1920,
            "video_height": 1080,
            "video_fps": 50.0,
            "video_bitrate_bps": 3322000,
            "video_scan_type": "progressive",
        }
        with patch("recorder_source.discovery.urlopen", return_value=Response()), patch(
            "recorder_source.discovery.probe_stream_quality_ffprobe",
            return_value=ffprobe,
        ) as probe:
            out = probe_candidate_hls(
                candidate(
                    stream_url="https://src.test/master.m3u8",
                    extra={"provider":"FANCODE"},
                )
            )
        probe.assert_called_once()
        self.assertEqual(
            probe.call_args.args[0],
            "https://final.test/video.m3u8",
        )
        self.assertEqual(out.video_fps, 50.0)
        self.assertEqual(out.extra["video_fps_source"], "ffprobe")
        self.assertEqual(out.extra["quality_source"], "manifest+ffprobe")
        self.assertEqual(
            out.extra["manifest_variant_url"],
            "https://final.test/video.m3u8",
        )

    def test_shared_hls_parser_returns_selected_variant_url(self):
        from recorder_source.quality import parse_hls_manifest_quality

        manifest = """#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=2500000,RESOLUTION=1280x720
720.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=3322000,RESOLUTION=1920x1080
1080.m3u8
"""
        quality = parse_hls_manifest_quality(
            manifest,
            "https://cdn.test/path/master.m3u8",
            motion_cap_fps=50,
        )
        self.assertEqual(quality["video_width"],1920)
        self.assertEqual(quality["video_height"],1080)
        self.assertEqual(
            quality["manifest_variant_url"],
            "https://cdn.test/path/1080.m3u8",
        )

    def test_shared_ffprobe_parser_uses_same_quality_ranking(self):
        payload = json.dumps({
            "streams": [
                {
                    "index": 0,
                    "width": 1920,
                    "height": 1080,
                    "avg_frame_rate": "25/1",
                    "bit_rate": "5000000",
                    "field_order": "progressive",
                },
                {
                    "index": 1,
                    "width": 1920,
                    "height": 1080,
                    "avg_frame_rate": "50/1",
                    "bit_rate": "4000000",
                    "field_order": "progressive",
                },
            ],
            "format": {},
        })
        quality = parse_ffprobe_quality_output(
            payload,
            target_quality={"video_width":1920,"video_height":1080},
            motion_cap_fps=50,
        )
        self.assertEqual(quality["video_fps"], 50.0)
        self.assertEqual(quality["_ffprobe_stream_index"], 1)

    def test_probe_http_403_is_access_blocked(self):
        error = HTTPError("https://x", 403, "Forbidden", {}, None)
        with patch("recorder_source.discovery.urlopen", side_effect=error):
            out = probe_candidate_hls(candidate(stream_url="https://x/live.m3u8"))
        self.assertTrue(out.access_blocked)
        self.assertEqual(out.probe_status, "access_blocked")


if __name__ == "__main__":
    unittest.main(verbosity=2)
