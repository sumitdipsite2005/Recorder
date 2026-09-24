from __future__ import annotations

import unittest

from recorder_source.identity import CanonicalFeedIdentity, derive_feed_identity, group_candidates_by_identity
from recorder_source.models import SourceCandidate


def sony(url: str, *, title="Event", playlist="https://source.test/list.m3u", index=1) -> SourceCandidate:
    return SourceCandidate(
        playlist_url=playlist,
        matching_entry_index=index,
        entry_title=title,
        stream_url=url,
        final_stream_url=url,
        extra={"provider": "SONYLIV"},
    )


class IdentityTests(unittest.TestCase):
    def test_same_sony_lane_25_and_50_variants_group_once(self):
        a = sony("https://a.cdn/hls/live/2120305/AG_Strea2309/ENG/25/master.m3u8")
        b = sony("https://b.cdn/hls/live/2120305/AG_Strea2309/ENG/50/master.m3u8")
        self.assertEqual(derive_feed_identity(a, "SONYLIV"), derive_feed_identity(b, "SONYLIV"))

    def test_same_sony_lane_token_refresh_stays_same_identity(self):
        a = sony("https://a.cdn/hls/live/2120305/AG_Strea2309/ENG/master.m3u8?token=one")
        b = sony("https://a.cdn/hls/live/2120305/AG_Strea2309/ENG/master.m3u8?token=two")
        self.assertEqual(derive_feed_identity(a, "SONYLIV"), derive_feed_identity(b, "SONYLIV"))

    def test_same_sony_lane_title_change_stays_same_identity(self):
        a = sony("https://a.cdn/hls/live/2120305/AG_Strea2309/ENG/master.m3u8", title="Shooting")
        b = sony("https://a.cdn/hls/live/2120305/AG_Strea2309/ENG/master.m3u8", title="Athletics")
        self.assertEqual(derive_feed_identity(a, "SONYLIV"), derive_feed_identity(b, "SONYLIV"))

    def test_sony_language_lanes_remain_separate(self):
        eng = sony("https://a.cdn/hls/live/2120305/AG_Strea2309/ENG/master.m3u8")
        hin = sony("https://a.cdn/hls/live/2120305/AG_Strea2309/HIN/master.m3u8")
        self.assertNotEqual(derive_feed_identity(eng, "SONYLIV"), derive_feed_identity(hin, "SONYLIV"))

    def test_distinct_sony_feed_ids_remain_separate(self):
        a = sony("https://a.cdn/hls/live/2120305/AG_Strea2309/ENG/master.m3u8")
        b = sony("https://a.cdn/hls/live/2120999/AG_Strea2309/ENG/master.m3u8")
        self.assertNotEqual(derive_feed_identity(a, "SONYLIV"), derive_feed_identity(b, "SONYLIV"))

    def test_hls_and_dash_live_variants_same_lane_group(self):
        hls = sony("https://a.cdn/hls/live/2120305/AG_Strea2309/ENG/master.m3u8")
        dash = sony("https://b.cdn/dash/live/2120305/AG_Strea2309/ENG/manifest.mpd")
        self.assertEqual(derive_feed_identity(hls, "SONYLIV"), derive_feed_identity(dash, "SONYLIV"))

    def test_sony_dash_client_lane_ignores_cdn_hostname(self):
        urls = [
            "https://one.cdn/clients/dash/enc/LANE-A/out/v1/OUTPUT-1/manifest.mpd",
            "https://two.cdn/clients/dash/enc/LANE-A/out/v1/OUTPUT-1/manifest.mpd",
            "https://three.example/clients/dash/enc/LANE-A/out/v1/OUTPUT-1/manifest.mpd?x=1",
            "https://four.example/clients/dash/enc/LANE-A/out/v1/OUTPUT-1/chunk.mpd",
        ]
        ids = {derive_feed_identity(sony(url), "SONYLIV").serialized for url in urls}
        self.assertEqual(len(ids), 1)

    def test_sony_dash_client_distinct_output_remains_separate(self):
        a = sony("https://one/clients/dash/enc/LANE-A/out/v1/OUTPUT-1/manifest.mpd")
        b = sony("https://one/clients/dash/enc/LANE-A/out/v1/OUTPUT-2/manifest.mpd")
        self.assertNotEqual(derive_feed_identity(a, "SONYLIV"), derive_feed_identity(b, "SONYLIV"))

    def test_unknown_provider_is_conservative_about_query_changes(self):
        a = SourceCandidate(stream_url="https://cdn.test/live.m3u8?token=one")
        b = SourceCandidate(stream_url="https://cdn.test/live.m3u8?token=two")
        self.assertNotEqual(derive_feed_identity(a, "UNKNOWN"), derive_feed_identity(b, "UNKNOWN"))

    def test_unknown_provider_does_not_merge_by_title(self):
        a = SourceCandidate(entry_title="Same Event", stream_url="https://a.test/live.m3u8")
        b = SourceCandidate(entry_title="Same Event", stream_url="https://b.test/live.m3u8")
        self.assertNotEqual(derive_feed_identity(a, "UNKNOWN"), derive_feed_identity(b, "UNKNOWN"))

    def test_provider_identity_hint_can_supply_stable_native_identity(self):
        a = SourceCandidate(stream_url="https://a.test/token1", extra={"provider_identity_hint":"lane-123"})
        b = SourceCandidate(stream_url="https://b.test/token2", extra={"provider_identity_hint":"lane-123"})
        self.assertEqual(derive_feed_identity(a, "CUSTOM"), derive_feed_identity(b, "CUSTOM"))

    def test_identity_equality_uses_provider_and_lane_not_diagnostics(self):
        a = CanonicalFeedIdentity("SONYLIV", "lane:x", confidence="provider", evidence="one")
        b = CanonicalFeedIdentity("SONYLIV", "lane:x", confidence="conservative", evidence="two")
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))

    def test_group_candidates_by_identity_deduplicates_same_lane(self):
        candidates = [
            sony("https://a/hls/live/2120305/AG_Strea2309/ENG/25/master.m3u8"),
            sony("https://b/hls/live/2120305/AG_Strea2309/ENG/50/master.m3u8"),
        ]
        grouped = group_candidates_by_identity(candidates, provider="SONYLIV")
        self.assertEqual(len(grouped), 1)
        self.assertEqual(len(next(iter(grouped.values()))), 2)

    def test_no_url_fallback_uses_source_provenance_not_mutable_title(self):
        a = SourceCandidate(playlist_url="https://src/list", matching_entry_index=7, entry_title="Old")
        b = SourceCandidate(playlist_url="https://src/list", matching_entry_index=7, entry_title="New")
        self.assertEqual(derive_feed_identity(a, "UNKNOWN"), derive_feed_identity(b, "UNKNOWN"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
