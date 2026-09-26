from __future__ import annotations

from dataclasses import replace
from datetime import datetime
import json
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from urllib.error import HTTPError

from recorder_source.headers import canonicalize_header_name
from recorder_source.manifest import manifest_type_from_text, is_manifest_text
from recorder_source.playback import playback_fingerprint
from recorder_source.playlist_headers import (
    MATURE_PLAYLIST_HEADER_POLICY,
    NORMALIZED_PLAYLIST_HEADER_POLICY,
    parse_stream_url_and_headers,
)
from recorder_source.discovery import (
    _github_file_commit_timestamp,
    adapt_json_playlist_text,
    discover_playlist_text,
    fetch_playlist_documents,
    build_effective_probe_headers,
    normalize_playlist_license_key,
    parse_extinf_metadata,
    parse_playlist_text,
    probe_candidate_hls,
    probe_candidates,
    resolve_playlist_source_freshness,
)
from recorder_source.quality import (
    extract_auth_expiry,
    format_candidate_quality,
    merge_ffprobe_quality_evidence,
    inspect_dash_manifest_drm,
    parse_dash_manifest_quality,
    QUALITY_FFPROBE_TIMEOUT_SEC,
    parse_ffprobe_quality_output,
    probe_stream_quality_ffprobe,
    quality_probe_identity,
)
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
from recorder_source import transport as source_transport
from recorder_source.policy import (
    is_vpn_route_suspected_403,
    selection_policy_for_provider,
)
from recorder_source.selection import (
    candidate_quality_rank,
    comparable_motion_fps,
    select_join_candidate,
    select_quality_upgrade,
    selection_nonselection_reason,
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

    def test_provider_policy_resolver_uses_shared_provider_defaults(self):
        sony = selection_policy_for_provider("SONYLIV")
        hotstar = selection_policy_for_provider("HOTSTAR")
        self.assertTrue(sony.allow_unknown_expiry)
        self.assertFalse(hotstar.allow_unknown_expiry)
        self.assertEqual(sony.mandatory_min_remaining_sec, 900)

    def test_shared_nonselection_reason_reports_lower_quality(self):
        selected = candidate(
            "selected",
            playlist_url="https://selected.test/list.m3u",
            matching_entry_index=1,
            video_fps=50,
            expiry=self.now + 3600,
        )
        lower = candidate(
            "lower",
            playlist_url="https://lower.test/list.m3u",
            matching_entry_index=1,
            video_fps=25,
            expiry=self.now + 3600,
        )
        self.assertEqual(
            selection_nonselection_reason(
                lower,
                selected,
                self.policy,
                now_ts=self.now,
            ),
            "not selected: lower quality",
        )

    def test_shared_nonselection_reason_reports_equivalent_alternative(self):
        selected = candidate(
            "selected",
            playlist_url="https://selected.test/list.m3u",
            matching_entry_index=1,
            expiry=self.now + 3600,
        )
        equivalent = candidate(
            "equivalent",
            playlist_url="https://equivalent.test/list.m3u",
            matching_entry_index=1,
            expiry=self.now + 3600,
        )
        self.assertEqual(
            selection_nonselection_reason(
                equivalent,
                selected,
                self.policy,
                now_ts=self.now,
            ),
            "not selected: equivalent alternative",
        )

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

    def test_shared_manifest_content_classifier(self):
        self.assertEqual(
            manifest_type_from_text("  #EXTM3U\\n#EXT-X-VERSION:3"),
            "HLS",
        )
        self.assertEqual(
            manifest_type_from_text(
                '<?xml version="1.0"?><dash:MPD xmlns:dash="urn:mpeg:dash:schema:mpd:2011"></dash:MPD>'
            ),
            "DASH",
        )
        self.assertFalse(is_manifest_text("<html>not a manifest</html>"))

    def test_shared_playlist_header_parser_preserves_both_existing_compatibility_profiles(self):
        raw = "https://cdn.test/live.mpd?|Cookie=b%3D2"
        mature_url, mature_headers = parse_stream_url_and_headers(
            raw,
            (),
            policy=MATURE_PLAYLIST_HEADER_POLICY,
        )
        normalized_url, normalized_headers = parse_stream_url_and_headers(
            raw,
            (),
            policy=NORMALIZED_PLAYLIST_HEADER_POLICY,
        )
        self.assertEqual(mature_url, "https://cdn.test/live.mpd")
        self.assertEqual(mature_headers["Cookie"], "b%3D2")
        self.assertEqual(normalized_url, "https://cdn.test/live.mpd?")
        self.assertEqual(normalized_headers["Cookie"], "b=2")

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

    def test_only_direct_drmlive_dash_widevine_wrapper_is_marked_unsupported(self):
        text = '''#EXTM3U
#EXTINF:-1 tvg-name="A",A
#KODIPROP:inputstream.adaptive.license_type=com.widevine.alpha
https://edge.drmlive.net/live.mpd
'''
        c = parse_playlist_text(text).candidates[0]
        self.assertEqual(c.unsupported_drm, "Widevine")

        ordinary = text.replace(
            "https://edge.drmlive.net/live.mpd",
            "https://cdn.test/live.mpd",
        )
        c2 = parse_playlist_text(ordinary).candidates[0]
        self.assertEqual(c2.unsupported_drm, "")

    def test_clearkey_jwk_normalization_matches_mature_internal_format(self):
        payload = {
            "keys": [{
                "kty": "oct",
                "kid": "uoiW1gUkaHGsQkh4SR2GoQ",
                "k": "hgDUFTA0s8vIUvE-pLdILA",
            }]
        }
        self.assertEqual(
            normalize_playlist_license_key(json.dumps(payload)),
            ("ba8896d605246871ac424878491d86a1:8600d4153034b3cbc852f13ea4b7482c",),
        )

    def test_effective_probe_headers_use_mature_provider_defaults(self):
        headers = build_effective_probe_headers(
            "SONYLIV",
            {"Referer": "https://override.test/", "User-Agent": ""},
        )
        self.assertEqual(headers["Origin"], "https://www.sonyliv.com")
        self.assertEqual(headers["Referer"], "https://override.test/")
        self.assertIn("Chrome/142.0.0.0", headers["User-Agent"])

        hotstar = build_effective_probe_headers("HOTSTAR", {})
        self.assertIn("Chrome/141.0.0.0", hotstar["User-Agent"])

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
            def get(self, name, default=None):
                if name == "Last-Modified":
                    return "Thu, 24 Sep 2026 15:30:00 GMT"
                if name == "ETag":
                    return '"abc"'
                return default
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
        self.assertEqual(
            diag["https://good.test/list.m3u"]["last_modified"],
            "Thu, 24 Sep 2026 15:30:00 GMT",
        )
        self.assertEqual(diag["https://good.test/list.m3u"]["etag"], '"abc"')
        self.assertFalse(diag["https://bad.test/list.m3u"]["ok"])

    def test_github_commit_lookup_retries_transient_timeout(self):
        class Response:
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def read(self):
                return json.dumps([{
                    "commit":{
                        "committer":{"date":"2026-09-25T03:17:48Z"}
                    }
                }]).encode("utf-8")
        with patch(
            "recorder_source.discovery.urlopen",
            side_effect=[TimeoutError("slow"), Response()],
        ) as opened, patch("recorder_source.discovery.time.sleep"):
            value=_github_file_commit_timestamp(
                "https://raw.githubusercontent.com/user/repo/main/list.m3u",
                timeout_sec=0.01,
                retry_base_sec=0,
            )
        self.assertIsNotNone(value)
        self.assertEqual(opened.call_count,2)

    def test_same_github_document_retries_weaker_fallback_until_commit_is_known(self):
        previous={
            "timestamp":1000.0,
            "source":"generated",
            "content_hash":__import__("hashlib").sha256(
                b"#EXTM3U\n"
            ).hexdigest(),
        }
        with patch(
            "recorder_source.discovery._github_file_commit_timestamp",
            return_value=2000.0,
        ):
            result=resolve_playlist_source_freshness(
                "https://raw.githubusercontent.com/user/repo/main/list.m3u",
                "#EXTM3U\n",
                previous=previous,
            )
        self.assertEqual(result["source"],"commit")
        self.assertEqual(result["timestamp"],2000.0)

    def test_playlist_freshness_prefers_github_commit_on_first_observation(self):
        with patch(
            "recorder_source.discovery._github_file_commit_timestamp",
            return_value=1234.0,
        ):
            result=resolve_playlist_source_freshness(
                "https://raw.githubusercontent.com/user/repo/main/list.m3u",
                "# generated 2026-09-24 12:00:00\n",
                {"last_modified":"Thu, 24 Sep 2026 11:00:00 GMT"},
            )
        self.assertEqual(result["timestamp"],1234.0)
        self.assertEqual(result["source"],"commit")

    def test_playlist_freshness_uses_generated_before_last_modified(self):
        with patch(
            "recorder_source.discovery._github_file_commit_timestamp",
            return_value=None,
        ):
            result=resolve_playlist_source_freshness(
                "https://example.test/list.m3u",
                "# Generated at 2026-09-24 12:30:00+00:00\n#EXTM3U\n",
                {"last_modified":"Thu, 24 Sep 2026 11:00:00 GMT"},
            )
        self.assertEqual(result["source"],"generated")
        self.assertEqual(
            result["timestamp"],
            datetime.fromisoformat("2026-09-24 12:30:00+00:00").timestamp(),
        )

    def test_playlist_freshness_uses_last_modified_when_no_stronger_signal(self):
        with patch(
            "recorder_source.discovery._github_file_commit_timestamp",
            return_value=None,
        ):
            result=resolve_playlist_source_freshness(
                "https://example.test/list.m3u",
                "#EXTM3U\n",
                {"last_modified":"Thu, 24 Sep 2026 11:00:00 GMT"},
            )
        self.assertEqual(result["source"],"last-modified")

    def test_playlist_freshness_reuses_same_document_and_observes_changed_document(self):
        with patch(
            "recorder_source.discovery._github_file_commit_timestamp",
            return_value=None,
        ):
            first=resolve_playlist_source_freshness(
                "https://example.test/list.m3u",
                "#EXTM3U\n",
                {},
            )
        same=resolve_playlist_source_freshness(
            "https://example.test/list.m3u",
            "#EXTM3U\n",
            {},
            previous=first,
            now_ts=2000,
        )
        changed=resolve_playlist_source_freshness(
            "https://example.test/list.m3u",
            "#EXTM3U\n# changed\n",
            {},
            previous=first,
            now_ts=2000,
        )
        self.assertEqual(same["source"],"unknown")
        self.assertEqual(changed["source"],"observed")
        self.assertEqual(changed["timestamp"],2000.0)

    def test_shared_probe_transport_retries_transient_timeout_once(self):
        calls = []

        def operation():
            calls.append(1)
            if len(calls) == 1:
                raise TimeoutError("timed out")
            return "ok"

        result = source_transport.run_retryable_http_get(
            operation,
            sleep_fn=lambda _: None,
        )
        self.assertEqual(result, "ok")
        self.assertEqual(len(calls), 2)

    def test_shared_probe_transport_does_not_retry_nontransient_http_error(self):
        calls = []

        def operation():
            calls.append(1)
            raise HTTPError("https://x", 404, "Not Found", {}, None)

        with self.assertRaises(HTTPError):
            source_transport.run_retryable_http_get(
                operation,
                sleep_fn=lambda _: None,
            )
        self.assertEqual(len(calls), 1)

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
            def __init__(self): self._read_done = False
            def read(self, n=-1):
                if self._read_done:
                    return b""
                self._read_done = True
                return body
            def geturl(self): return "https://final.test/master.m3u8"
        with patch(
            "recorder_source.discovery.urlopen",
            side_effect=lambda *args, **kwargs: Response(),
        ):
            out = probe_candidate_hls(candidate(stream_url="https://src.test/master.m3u8", extra={"provider":"SONYLIV"}))
        self.assertTrue(out.launchable)
        self.assertEqual((out.video_width, out.video_height, out.video_fps), (1920, 1080, 50.0))
        self.assertEqual(out.final_stream_url, "https://final.test/master.m3u8")
        self.assertTrue(out.playback_fingerprint)

    def test_shared_auth_expiry_uses_mature_syntax(self):
        self.assertEqual(
            extract_auth_expiry(
                "https://cdn.test/live.m3u8?exp=2000",
                "Cookie=foo;expires=1500",
            ),
            1500,
        )
        self.assertIsNone(
            extract_auth_expiry("https://cdn.test/live.m3u8?expiry=1234")
        )

    def test_shared_dash_parser_preserves_mature_representation_addressing(self):
        manifest = """<?xml version="1.0"?>
<MPD type="static">
  <Period>
    <AdaptationSet contentType="video">
      <Representation id="v25" width="1920" height="1080" bandwidth="5000000" frameRate="25">
        <BaseURL>video/</BaseURL>
        <SegmentTemplate initialization="init-$RepresentationID$.mp4" media="seg-$Number%03d$.m4s" startNumber="7" duration="2" timescale="1"/>
      </Representation>
      <Representation id="v50" width="1920" height="1080" bandwidth="4500000" frameRate="50" scanType="progressive">
        <BaseURL>video50/</BaseURL>
        <SegmentTemplate initialization="init-$RepresentationID$.mp4" media="seg-$Number%03d$.m4s" startNumber="11" duration="2" timescale="1"/>
      </Representation>
    </AdaptationSet>
  </Period>
</MPD>"""
        quality = parse_dash_manifest_quality(
            manifest,
            "https://cdn.test/live/manifest.mpd",
            motion_cap_fps=50.0,
            now_ts=1000.0,
        )
        self.assertEqual(quality["video_fps"], 50.0)
        self.assertEqual(quality["_dash_representation_id"], "v50")
        self.assertEqual(
            quality["_dash_initialization_url"],
            "https://cdn.test/live/video50/init-v50.mp4",
        )
        self.assertEqual(
            quality["_dash_media_urls"][0],
            "https://cdn.test/live/video50/seg-011.m4s",
        )

    def test_shared_dash_drm_inspection_marks_content_protection_key_required(self):
        manifest = '''<?xml version="1.0"?><MPD><Period><AdaptationSet><ContentProtection schemeIdUri="urn:uuid:edef8ba9-79d6-4ace-a3c8-27dcd51d21ed" value="Widevine"/><Representation width="1920" height="1080" bandwidth="4500000" frameRate="25"/></AdaptationSet></Period></MPD>'''
        drm = inspect_dash_manifest_drm(manifest)
        self.assertTrue(drm["drm_protected"])
        self.assertTrue(drm["drm_key_required"])

    def test_probe_dash_clearkey_is_launchable_with_widevine_signaling(self):
        body = b'''<?xml version="1.0"?><MPD><Period><AdaptationSet><ContentProtection schemeIdUri="urn:uuid:edef8ba9-79d6-4ace-a3c8-27dcd51d21ed" value="Widevine"/><Representation width="1920" height="1080" bandwidth="8200000" frameRate="25"/></AdaptationSet></Period></MPD>'''
        class Headers:
            def get(self, name, default=None): return "application/dash+xml"
        class Response:
            headers = Headers()
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def __init__(self): self._read_done = False
            def read(self, n=-1):
                if self._read_done:
                    return b""
                self._read_done = True
                return body
            def geturl(self): return "https://final.test/manifest.mpd"
        clear_key = "ba8896d605246871ac424878491d86a1:8600d4153034b3cbc852f13ea4b7482c"
        with patch(
            "recorder_source.discovery.urlopen",
            side_effect=lambda *args, **kwargs: Response(),
        ):
            out = probe_candidate_hls(candidate(
                stream_url="https://src.test/manifest.mpd",
                license_type="clearkey",
                keys=(clear_key,),
            ))
        self.assertTrue(out.launchable)
        self.assertEqual(out.probe_status,"working")
        self.assertEqual(out.unsupported_drm,"")
        self.assertTrue(out.extra["drm_key_required"])
        self.assertFalse(out.extra["drm_key_missing"])

    def test_probe_dash_drm_without_key_is_reported_as_key_missing(self):
        body = b'''<?xml version="1.0"?><MPD><Period><AdaptationSet><ContentProtection schemeIdUri="urn:uuid:edef8ba9-79d6-4ace-a3c8-27dcd51d21ed" value="Widevine"/><Representation width="1920" height="1080" bandwidth="8200000" frameRate="25"/></AdaptationSet></Period></MPD>'''
        class Headers:
            def get(self, name, default=None): return "application/dash+xml"
        class Response:
            headers = Headers()
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def __init__(self): self._read_done = False
            def read(self, n=-1):
                if self._read_done:
                    return b""
                self._read_done = True
                return body
            def geturl(self): return "https://final.test/manifest.mpd"
        with patch(
            "recorder_source.discovery.urlopen",
            side_effect=lambda *args, **kwargs: Response(),
        ):
            out = probe_candidate_hls(candidate(stream_url="https://src.test/manifest.mpd"))
        self.assertFalse(out.launchable)
        self.assertEqual(out.probe_status,"drm_key_missing")
        self.assertTrue(out.extra["drm_key_missing"])

    def test_probe_dash_parses_quality(self):
        body = b'''<?xml version="1.0"?><MPD><Period><AdaptationSet><Representation width="1920" height="1080" bandwidth="4500000" frameRate="50"/></AdaptationSet></Period></MPD>'''
        class Headers:
            def get(self, name, default=None): return "application/dash+xml"
        class Response:
            headers = Headers()
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def __init__(self): self._read_done = False
            def read(self, n=-1):
                if self._read_done:
                    return b""
                self._read_done = True
                return body
            def geturl(self): return "https://final.test/manifest.mpd"
        with patch(
            "recorder_source.discovery.urlopen",
            side_effect=lambda *args, **kwargs: Response(),
        ):
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
            def __init__(self): self._read_done = False
            def read(self, n=-1):
                if self._read_done:
                    return b""
                self._read_done = True
                return body
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
        self.assertEqual(out.video_fps_source, "ffprobe")
        self.assertEqual(out.quality_source, "manifest+ffprobe")
        self.assertEqual(
            out.extra["manifest_variant_url"],
            "https://final.test/video.m3u8",
        )

    def test_shared_ffprobe_completion_samples_selected_stream_when_bitrate_missing(self):
        stdout = json.dumps({
            "streams": [{
                "index": 3,
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
        sampled_streams = []

        def runner(args, timeout):
            return SimpleNamespace(returncode=1, stdout=stdout, stderr="late warning")

        def sample_bitrate(stream_index):
            sampled_streams.append(stream_index)
            return 3_456_000

        quality = probe_stream_quality_ffprobe(
            "https://final.test/video.m3u8",
            {"Referer": "https://example.test/"},
            sample_missing_bitrate=True,
            bitrate_sample_callback=sample_bitrate,
            runner=runner,
        )

        self.assertEqual(sampled_streams, [3])
        self.assertEqual(quality["video_bitrate_bps"], 3_456_000)
        self.assertEqual(quality["video_bitrate_source"], "sample")
        self.assertTrue(quality["quality_known"])

    def test_shared_ffprobe_completion_keeps_ffprobe_quality_if_bitrate_sample_fails(self):
        stdout = json.dumps({
            "streams": [{
                "index": 1,
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

        def fail_sample(stream_index):
            raise TimeoutError("sample timeout")

        quality = probe_stream_quality_ffprobe(
            "https://final.test/video.m3u8",
            {},
            sample_missing_bitrate=True,
            bitrate_sample_callback=fail_sample,
            runner=lambda args, timeout: SimpleNamespace(
                returncode=0,
                stdout=stdout,
                stderr="",
            ),
        )

        self.assertEqual((quality["video_width"], quality["video_height"]), (1920, 1080))
        self.assertEqual(quality["video_fps"], 25.0)
        self.assertEqual(quality["video_bitrate_bps"], 0)
        self.assertIn("sample timeout", quality["_bitrate_sample_failure"])

    def test_probe_hls_uses_shared_ffprobe_completion_when_bitrate_missing(self):
        body = b'''#EXTM3U\n#EXT-X-STREAM-INF:RESOLUTION=1920x1080,FRAME-RATE=25\nvideo.m3u8\n'''
        class Headers:
            def get(self, name, default=None):
                return "application/vnd.apple.mpegurl"
        class Response:
            headers = Headers()
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def __init__(self): self._read_done = False
            def read(self, n=-1):
                if self._read_done:
                    return b""
                self._read_done = True
                return body
            def geturl(self): return "https://final.test/master.m3u8"

        ffprobe = {
            "quality_known": True,
            "video_width": 1920,
            "video_height": 1080,
            "video_fps": 25.0,
            "video_bitrate_bps": 3_456_000,
            "video_bitrate_source": "sample",
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
        self.assertTrue(probe.call_args.kwargs["sample_missing_bitrate"])
        self.assertEqual(
            probe.call_args.kwargs["timeout_sec"],
            QUALITY_FFPROBE_TIMEOUT_SEC,
        )
        self.assertEqual(out.video_bitrate_bps, 3_456_000)
        self.assertEqual(out.video_bitrate_source, "sample")

    def test_shared_ffprobe_evidence_merge_fills_only_missing_facts(self):
        current = {
            "quality_known": True,
            "quality_source": "manifest",
            "video_fps": 0.0,
            "video_fps_source": "",
            "video_width": 1920,
            "video_height": 0,
            "video_resolution_source": "manifest",
            "video_bitrate_bps": 0,
            "video_bitrate_source": "",
            "video_scan_type": "",
            "video_scan_type_source": "",
        }
        ffprobe = {
            "video_fps": 25.0,
            "video_width": 1280,
            "video_height": 1080,
            "video_bitrate_bps": 3_523_000,
            "video_bitrate_source": "sample",
            "video_scan_type": "progressive",
        }

        merged = merge_ffprobe_quality_evidence(
            current,
            ffprobe,
            include_scan_type=True,
            include_sample_in_quality_source=True,
            default_bitrate_source="ffprobe",
        )

        self.assertEqual(merged["video_width"], 1920)
        self.assertEqual(merged["video_height"], 1080)
        self.assertEqual(merged["video_fps"], 25.0)
        self.assertEqual(merged["video_bitrate_bps"], 3_523_000)
        self.assertEqual(merged["video_bitrate_source"], "sample")
        self.assertEqual(merged["video_scan_type"], "progressive")
        self.assertEqual(
            merged["video_resolution_source"],
            "manifest+ffprobe",
        )
        self.assertEqual(
            merged["quality_source"],
            "manifest+ffprobe+sample",
        )

        mature_label = merge_ffprobe_quality_evidence(
            current,
            ffprobe,
            include_scan_type=True,
            include_sample_in_quality_source=False,
            default_bitrate_source="",
        )
        self.assertEqual(
            mature_label["quality_source"],
            "manifest+ffprobe",
        )

    def test_shared_quality_formatter_preserves_value_evidence(self):
        item = candidate(
            video_fps=50.0,
            video_fps_source="manifest",
            video_scan_type="progressive",
            video_scan_type_source="event-policy",
            video_resolution_source="manifest",
            video_bitrate_bps=3_456_000,
            video_bitrate_source="sample",
        )
        self.assertEqual(
            format_candidate_quality(item),
            "1920x1080 | 50p [manifest, event-policy] | ~3456 Kbps [FFmpeg sample]",
        )

    def test_provider_specific_403_policy_lives_in_policy_layer(self):
        self.assertTrue(
            is_vpn_route_suspected_403(provider="FANCODE")
        )
        self.assertTrue(
            is_vpn_route_suspected_403(source_group="JIO_STAR_SPORTS")
        )
        self.assertFalse(
            is_vpn_route_suspected_403(provider="SONYLIV")
        )

    def test_shared_header_canonicalization_matches_mature_alias_rules(self):
        self.assertEqual(canonicalize_header_name("user_agent"), "User-Agent")
        self.assertEqual(canonicalize_header_name("referrer"), "Referer")
        self.assertEqual(canonicalize_header_name("AUTHORIZATION"), "Authorization")
        self.assertEqual(canonicalize_header_name("X-Custom"), "X-Custom")

    def test_shared_playback_fingerprint_contract(self):
        base = {
            "Cookie": "session=one",
            "Authorization": "Bearer A",
            "Referer": "https://example.test/",
            "Origin": "https://example.test",
            "User-Agent": "ignored-one",
        }
        same_effective = dict(base, **{"User-Agent": "ignored-two"})
        changed_auth = dict(base, **{"Authorization": "Bearer B"})
        url = "https://media.example.test/live/master.m3u8?token=one"

        self.assertEqual(
            playback_fingerprint(url, base),
            playback_fingerprint(url, same_effective),
        )
        self.assertNotEqual(
            playback_fingerprint(url, base),
            playback_fingerprint(url, changed_auth),
        )
        self.assertNotEqual(
            playback_fingerprint(url, base),
            playback_fingerprint(
                "https://media.example.test/live/master.m3u8?token=two",
                base,
            ),
        )
        self.assertEqual(playback_fingerprint("", base), "")

    def test_quality_probe_identity_ignores_playlist_provenance(self):
        a = candidate(
            playlist_url="https://one.test/list.m3u",
            matching_entry_index=1,
            stream_url="https://cdn.test/live/master.m3u8",
            headers={"Referer": "https://example.test/"},
        )
        b = candidate(
            playlist_url="https://two.test/list.m3u",
            matching_entry_index=9,
            stream_url="https://cdn.test/live/master.m3u8",
            headers={"Referer": "https://example.test/"},
        )
        self.assertEqual(
            quality_probe_identity(a),
            quality_probe_identity(b),
        )

    def test_probe_candidates_reuses_one_probe_without_erasing_sources(self):
        a = candidate(
            playlist_url="https://one.test/list.m3u",
            matching_entry_index=1,
            stream_url="https://cdn.test/live/master.m3u8",
            extra={"provider": "SONYLIV", "source_name": "one", "source_group": "SONYLIV_EVENTS"},
        )
        b = replace(
            a,
            playlist_url="https://two.test/list.m3u",
            matching_entry_index=2,
            extra={"provider": "SONYLIV", "source_name": "two", "source_group": "SONYLIV_EVENTS"},
        )
        probed = replace(
            a,
            quality_known=True,
            quality_source="manifest",
            video_width=1920,
            video_height=1080,
            video_resolution_source="manifest",
            video_fps=50.0,
            video_fps_source="manifest",
            video_scan_type="progressive",
            video_scan_type_source="event-policy",
            video_bitrate_bps=4_963_000,
            video_bitrate_source="manifest",
            launchable=True,
            probe_status="working",
            extra={
                **dict(a.extra),
                "manifest_final_url": a.stream_url,
                "manifest_expiry": None,
                "probe_transport_launchable": True,
            },
        )
        with patch(
            "recorder_source.discovery.probe_candidate_hls",
            return_value=probed,
        ) as probe:
            out = probe_candidates((a, b))

        probe.assert_called_once()
        self.assertEqual(out[0].extra["source_name"], "one")
        self.assertEqual(out[1].extra["source_name"], "two")
        self.assertEqual(out[0].video_fps_source, "manifest")
        self.assertEqual(out[1].video_fps_source, "manifest")
        self.assertEqual(out[0].video_bitrate_bps, out[1].video_bitrate_bps)

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

    def test_probe_hls_child_404_is_variant_unavailable_not_drm(self):
        master = """#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=4963000,RESOLUTION=1920x1080,FRAME-RATE=50
child.m3u8
"""
        child_error = HTTPError(
            "https://final.test/child.m3u8",
            404,
            "Not Found",
            {},
            None,
        )
        with patch(
            "recorder_source.discovery.source_transport.fetch_stream_manifest_text",
            side_effect=[
                (master, "https://final.test/master.m3u8"),
                child_error,
            ],
        ):
            out = probe_candidate_hls(
                candidate(
                    stream_url="https://src.test/master.m3u8",
                    extra={"provider": "SONYLIV", "source_group": "SONYLIV_EVENTS"},
                )
            )

        self.assertFalse(out.launchable)
        self.assertEqual(out.probe_status, "hls_variant_unavailable")
        self.assertIn("HTTP 404 Not Found", out.reason)
        self.assertIn("selected HLS variant/path unavailable", out.reason)
        self.assertEqual(out.extra["drm_inspection_failure"], "")
        self.assertEqual(out.extra["hls_variant_probe_status"], "hls_variant_unavailable")
        self.assertEqual(out.unsupported_drm, "")

    def test_probe_hls_child_403_is_access_failure_not_drm(self):
        master = """#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=4963000,RESOLUTION=1920x1080,FRAME-RATE=50
child.m3u8
"""
        child_error = HTTPError(
            "https://final.test/child.m3u8",
            403,
            "Forbidden",
            {},
            None,
        )
        with patch(
            "recorder_source.discovery.source_transport.fetch_stream_manifest_text",
            side_effect=[
                (master, "https://final.test/master.m3u8"),
                child_error,
            ],
        ), patch(
            "recorder_source.discovery.source_transport.fetch_hls_child_with_master_cookie_session",
            side_effect=child_error,
        ):
            out = probe_candidate_hls(
                candidate(
                    stream_url="https://src.test/master.m3u8",
                    extra={"provider": "SONYLIV", "source_group": "SONYLIV_EVENTS"},
                )
            )

        self.assertFalse(out.launchable)
        self.assertEqual(out.probe_status, "hls_variant_access_failed")
        self.assertIn("HTTP 403 Forbidden", out.reason)
        self.assertEqual(out.extra["drm_inspection_failure"], "")

    def test_probe_fancode_http_403_uses_mature_access_classification(self):
        error = HTTPError("https://x", 403, "Forbidden", {}, None)
        with patch("recorder_source.discovery.urlopen", side_effect=error):
            out = probe_candidate_hls(candidate(
                stream_url="https://x/live.m3u8",
                extra={"provider": "FANCODE", "source_group": "FANCODE"},
            ))
        self.assertTrue(out.access_blocked)
        self.assertEqual(out.probe_status, "access_blocked")
        self.assertEqual(out.extra["access_block_kind"], "vpn_route_suspected")

    def test_probe_generic_http_403_is_not_invented_as_vpn_block(self):
        error = HTTPError("https://x", 403, "Forbidden", {}, None)
        with patch("recorder_source.discovery.urlopen", side_effect=error):
            out = probe_candidate_hls(candidate(stream_url="https://x/live.m3u8"))
        self.assertFalse(out.access_blocked)
        self.assertEqual(out.probe_status, "probe_failed")


if __name__ == "__main__":
    unittest.main(verbosity=2)
