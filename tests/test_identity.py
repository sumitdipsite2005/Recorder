from __future__ import annotations

import unittest

from recorder_source.identity import (
    CanonicalFeedIdentity,
    derive_feed_identity,
    filename_sub_id,
    group_candidates_by_identity,
)
from recorder_source.models import SourceCandidate


def candidate(
    url: str,
    *,
    provider: str = "SONYLIV",
    title: str = "Event",
    playlist: str = "https://source.test/list.m3u",
    index: int = 1,
) -> SourceCandidate:
    return SourceCandidate(
        playlist_url=playlist,
        matching_entry_index=index,
        entry_title=title,
        stream_url=url,
        final_stream_url=url,
        extra={"provider": provider},
    )


class IdentityTests(unittest.TestCase):
    def test_same_sony_parent_path_different_manifest_leaves_group_once(self):
        a = candidate(
            "https://a.cdn/hls/live/2120305/AG_Strea2309/ENG/std_lrh-800300010.m3u8"
        )
        b = candidate(
            "https://b.cdn/hls/live/2120305/AG_Strea2309/ENG/std_mdh-800300010.m3u8"
        )
        self.assertEqual(
            derive_feed_identity(a, "SONYLIV"),
            derive_feed_identity(b, "SONYLIV"),
        )

    def test_sony_identity_is_readable_parent_path(self):
        item = candidate(
            "https://sonydaimenew.akamaized.net/hls/live/2120305/"
            "UNL_Czec2609/ENG/std_lrh-800300010.m3u8"
            "?hdnea=exp=1~hmac=one"
        )
        identity = derive_feed_identity(item, "SONYLIV")
        self.assertEqual(
            identity.lane_key,
            "/hls/live/2120305/UNL_Czec2609/ENG",
        )

    def test_same_sony_lane_token_refresh_stays_same_identity(self):
        a = candidate(
            "https://a.cdn/hls/live/2120305/AG_Strea2309/ENG/master.m3u8?token=one"
        )
        b = candidate(
            "https://b.cdn/hls/live/2120305/AG_Strea2309/ENG/master.m3u8?token=two"
        )
        self.assertEqual(
            derive_feed_identity(a, "SONYLIV"),
            derive_feed_identity(b, "SONYLIV"),
        )

    def test_fancode_identity_is_readable_parent_path_and_ignores_token(self):
        a = candidate(
            "https://in-mc-flive.fancode.com/mumbai/"
            "4249106_english_hls_b86f41b4c015704_1ta-di_h264/"
            "1080p.m3u8?hdntl=Expires=1~Signature=one",
            provider="FANCODE",
        )
        b = candidate(
            "https://other-cdn.fancode.com/mumbai/"
            "4249106_english_hls_b86f41b4c015704_1ta-di_h264/"
            "1080p.m3u8?hdntl=Expires=2~Signature=two",
            provider="FANCODE",
        )
        identity = derive_feed_identity(a, "FANCODE")
        self.assertEqual(
            identity.lane_key,
            "/mumbai/4249106_english_hls_b86f41b4c015704_1ta-di_h264",
        )
        self.assertEqual(identity, derive_feed_identity(b, "FANCODE"))

    def test_language_path_components_remain_part_of_identity(self):
        eng = candidate(
            "https://a.cdn/hls/live/2120305/AG_Strea2309/ENG/master.m3u8"
        )
        hin = candidate(
            "https://a.cdn/hls/live/2120305/AG_Strea2309/HIN/master.m3u8"
        )
        self.assertNotEqual(
            derive_feed_identity(eng, "SONYLIV"),
            derive_feed_identity(hin, "SONYLIV"),
        )

    def test_distinct_feed_paths_remain_separate(self):
        a = candidate(
            "https://a.cdn/hls/live/2120305/AG_Strea2309/ENG/master.m3u8"
        )
        b = candidate(
            "https://a.cdn/hls/live/2120999/AG_Strea2309/ENG/master.m3u8"
        )
        self.assertNotEqual(
            derive_feed_identity(a, "SONYLIV"),
            derive_feed_identity(b, "SONYLIV"),
        )

    def test_transport_path_components_are_preserved(self):
        hls = candidate(
            "https://a.cdn/hls/live/2120305/AG_Strea2309/ENG/master.m3u8"
        )
        dash = candidate(
            "https://b.cdn/dash/live/2120305/AG_Strea2309/ENG/manifest.mpd"
        )
        self.assertNotEqual(
            derive_feed_identity(hls, "SONYLIV"),
            derive_feed_identity(dash, "SONYLIV"),
        )

    def test_dash_client_parent_path_ignores_host_query_and_leaf(self):
        urls = [
            "https://one.cdn/clients/dash/enc/LANE-A/out/v1/OUTPUT-1/manifest.mpd",
            "https://two.cdn/clients/dash/enc/LANE-A/out/v1/OUTPUT-1/manifest.mpd?x=1",
            "https://three.example/clients/dash/enc/LANE-A/out/v1/OUTPUT-1/chunk.mpd",
        ]
        ids = {
            derive_feed_identity(candidate(url), "SONYLIV").serialized
            for url in urls
        }
        self.assertEqual(len(ids), 1)
        self.assertIn(
            "SONYLIV|/clients/dash/enc/LANE-A/out/v1/OUTPUT-1",
            ids,
        )

    def test_provider_namespace_keeps_same_path_separate(self):
        url = "https://cdn.test/live/123/master.m3u8?token=one"
        sony = candidate(url, provider="SONYLIV")
        fancode = candidate(url, provider="FANCODE")
        self.assertNotEqual(
            derive_feed_identity(sony, "SONYLIV"),
            derive_feed_identity(fancode, "FANCODE"),
        )

    def test_same_provider_same_parent_path_ignores_host_and_query(self):
        a = candidate(
            "https://one.test/live/123/master.m3u8?token=one",
            provider="CUSTOM",
        )
        b = candidate(
            "https://two.test/live/123/variant.m3u8?token=two",
            provider="CUSTOM",
        )
        self.assertEqual(
            derive_feed_identity(a, "CUSTOM"),
            derive_feed_identity(b, "CUSTOM"),
        )

    def test_provider_identity_hint_is_fallback_when_parent_path_unavailable(self):
        a = SourceCandidate(
            stream_url="https://a.test/master.m3u8",
            extra={"provider_identity_hint": "lane-123"},
        )
        b = SourceCandidate(
            stream_url="https://b.test/other.mpd",
            extra={"provider_identity_hint": "lane-123"},
        )
        self.assertEqual(
            derive_feed_identity(a, "CUSTOM"),
            derive_feed_identity(b, "CUSTOM"),
        )

    def test_identity_equality_uses_provider_and_lane_not_diagnostics(self):
        a = CanonicalFeedIdentity(
            "SONYLIV",
            "/hls/live/1/A/ENG",
            confidence="path",
            evidence="one",
        )
        b = CanonicalFeedIdentity(
            "SONYLIV",
            "/hls/live/1/A/ENG",
            confidence="other",
            evidence="two",
        )
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))

    def test_group_candidates_by_identity_deduplicates_same_parent(self):
        candidates = [
            candidate(
                "https://a/hls/live/2120305/AG_Strea2309/ENG/one.m3u8"
            ),
            candidate(
                "https://b/hls/live/2120305/AG_Strea2309/ENG/two.m3u8"
            ),
        ]
        grouped = group_candidates_by_identity(
            candidates,
            provider="SONYLIV",
        )
        self.assertEqual(len(grouped), 1)
        self.assertEqual(len(next(iter(grouped.values()))), 2)

    def test_filename_sub_id_uses_first_all_numeric_slash_or_underscore_part(self):
        self.assertEqual(
            filename_sub_id("/hls/live/2120305/UNL_Czec2609/ENG"),
            "2120305",
        )
        self.assertEqual(
            filename_sub_id(
                "/mumbai/4249106_english_hls_b86f41b4c015704_1ta-di_h264"
            ),
            "4249106",
        )
        self.assertEqual(
            filename_sub_id("/live/channel/english"),
            "",
        )

    def test_no_url_fallback_uses_source_provenance_not_mutable_title(self):
        a = SourceCandidate(
            playlist_url="https://src/list",
            matching_entry_index=7,
            entry_title="Old",
        )
        b = SourceCandidate(
            playlist_url="https://src/list",
            matching_entry_index=7,
            entry_title="New",
        )
        self.assertEqual(
            derive_feed_identity(a, "UNKNOWN"),
            derive_feed_identity(b, "UNKNOWN"),
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
