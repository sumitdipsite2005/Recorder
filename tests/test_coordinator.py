from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import recorder_event_coordinator as coord
from recorder_coordinator.snapshot import candidate_state
from recorder_source.models import SourceCandidate


def target(name="T", policy=coord.POLICY_MANUAL, **kw):
    data = dict(name=name, policy=policy, source_groups=("SONYLIV_EVENTS",), primary=("Asian Games",))
    data.update(kw)
    return coord.IdentityTarget(**data)


def view(t=None, status="ACTIVE", now=None):
    t = t or target()
    now = now or datetime(2026, 9, 24, 10, 0, 0)
    return coord.TargetView(t, status, now, None)


def sony_candidate(
    *,
    playlist="https://src1.test/list.m3u",
    source_name="src1",
    title="Asian Games",
    group="Sports",
    tvg="Asian Games",
    lane="2120305/AG_Strea2309/ENG",
    fps=50.0,
    width=1920,
    height=1080,
    launchable=True,
    status="working",
    ignored=False,
    reason="",
):
    feed, provider_path, lang = lane.split("/", 2)
    stream = f"https://cdn.test/hls/live/{feed}/{provider_path}/{lang}/master.m3u8"
    return SourceCandidate(
        playlist_url=playlist,
        matching_entry_index=1,
        tvg_name=tvg,
        group_title=group,
        entry_title=title,
        raw_stream_url=stream,
        stream_url=stream,
        final_stream_url=stream,
        quality_known=launchable,
        video_width=width if launchable else 0,
        video_height=height if launchable else 0,
        video_fps=fps if launchable else 0,
        video_bitrate_bps=5_000_000 if launchable else 0,
        video_scan_type="progressive" if launchable else "",
        launchable=launchable,
        probe_status=status,
        ignored=ignored,
        reason=reason,
        extra={"provider":"SONYLIV", "source_name":source_name, "source_group":"SONYLIV_EVENTS"},
    )


def snapshot(candidates, *, policy=coord.POLICY_MANUAL, target_name="T", now=None):
    t = target(name=target_name, policy=policy)
    return coord.build_snapshot((view(t, now=now),), {target_name: tuple(candidates)}, now=now or datetime(2026,9,24,10,0,0))


class TargetConfigTests(unittest.TestCase):
    def test_parse_manual_target(self):
        items = coord.parse_targets({"IDENTITY_COORDINATOR_TARGETS":[{
            "name":"Manual Asian Games", "policy":"MANUAL", "source_groups":["SONYLIV_EVENTS"], "primary":["Asian Games"]
        }]})
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].policy, coord.POLICY_MANUAL)

    def test_parse_all_alias(self):
        items = coord.parse_targets({"IDENTITY_COORDINATOR_TARGETS":[{
            "name":"All", "policy":"ALL", "source_scope":"SONYLIV_EVENTS", "match_all":True
        }]})
        self.assertEqual(items[0].policy, coord.POLICY_ALL)
        self.assertTrue(items[0].match_all)

    def test_intentional_empty_target_set_is_valid(self):
        self.assertEqual(coord.parse_targets({"IDENTITY_COORDINATOR_TARGETS":[]}), ())

    def test_duplicate_target_name_is_rejected(self):
        raw={"IDENTITY_COORDINATOR_TARGETS":[
            {"name":"X","policy":"MANUAL","source_groups":["SONYLIV_EVENTS"],"primary":["A"]},
            {"name":"X","policy":"MANUAL","source_groups":["SONYLIV_EVENTS"],"primary":["A"]},
        ]}
        with self.assertRaises(ValueError): coord.parse_targets(raw)

    def test_schedule_and_activity_window(self):
        start=datetime(2026,9,24,11,0,0)
        t=target(schedule_start=start, activity_duration_min=60)
        runtime=coord.TargetRuntime()
        self.assertEqual(coord.target_view(t,runtime,start-timedelta(minutes=1)).status,"SCHEDULED")
        self.assertEqual(coord.target_view(t,runtime,start+timedelta(minutes=1)).status,"ACTIVE")
        self.assertEqual(coord.target_view(t,runtime,start+timedelta(minutes=60)).status,"EXPIRED")

    def test_unscheduled_target_waits_for_coordinator_before_first_activation(self):
        runtime=coord.TargetRuntime()
        now=datetime(2026,9,24,10,0,0)
        v=coord.target_view(target(),runtime,now,coordinator_active=False)
        self.assertEqual(v.status,"WAITING_COORDINATOR")
        self.assertIsNone(runtime.first_activation)

    def test_sources_for_tv_group_uses_tv_bucket_and_common(self):
        raw={"NM3U8DL_PLAYLIST_GROUPS":{
            "COMMON":["https://common.test/list"],
            "TV":["https://tv.test/list"],
        }}
        specs=coord.sources_for_group(raw,"SONY_TV")
        self.assertEqual([s.url for s in specs],["https://common.test/list","https://tv.test/list"])
        self.assertTrue(all(s.provider=="SONYLIV" for s in specs))

    def test_playlist_user_agent_profile_is_resolved(self):
        raw={"NM3U8DL_PLAYLIST_GROUPS":{
            "COMMON":[],
            "SONYLIV_EVENTS":[{"url":"https://src/list","playlist_user_agent":"OTT_NAVIGATOR","stream_user_agent":"TIVIMATE"}],
        }}
        spec=coord.sources_for_group(raw,"SONYLIV_EVENTS")[0]
        self.assertEqual(spec.request_headers["User-Agent"], "OTT Navigator/1.7.1.4")
        self.assertEqual(spec.stream_headers["User-Agent"], "TiviMate")

    def test_invalid_reload_keeps_last_valid_config(self):
        with tempfile.TemporaryDirectory() as td:
            path=Path(td)/"config.py"
            path.write_text('''IDENTITY_COORDINATOR_TARGETS=[{"name":"T","policy":"MANUAL","source_groups":["SONYLIV_EVENTS"],"primary":["A"]}]
NM3U8DL_PLAYLIST_GROUPS={"COMMON":[],"SONYLIV_EVENTS":["https://src/list"]}
''',encoding="utf-8")
            state=coord.CoordinatorConfigState(path)
            state.reload(datetime(2026,9,24,10,0,0))
            self.assertEqual(state.targets[0].name,"T")
            path.write_text('IDENTITY_COORDINATOR_TARGETS="broken"\n',encoding="utf-8")
            messages,changed=state.reload(datetime(2026,9,24,10,1,0))
            self.assertFalse(changed)
            self.assertEqual(state.targets[0].name,"T")
            self.assertIn("rejected invalid reload", messages[0])

    def test_deleted_then_recreated_target_gets_fresh_runtime(self):
        with tempfile.TemporaryDirectory() as td:
            path=Path(td)/"config.py"
            def write(targets):
                path.write_text('IDENTITY_COORDINATOR_TARGETS='+repr(targets)+'\nNM3U8DL_PLAYLIST_GROUPS={"COMMON":[],"SONYLIV_EVENTS":["https://src/list"]}\n',encoding="utf-8")
            item={"name":"T","policy":"MANUAL","source_groups":["SONYLIV_EVENTS"],"primary":["A"]}
            state=coord.CoordinatorConfigState(path)
            write([item]); state.reload(datetime(2026,9,24,10,0,0)); state.target_views(datetime(2026,9,24,10,0,0))
            self.assertIsNotNone(state.target_runtime["T"].first_activation)
            write([]); state.reload(datetime(2026,9,24,10,1,0)); self.assertNotIn("T",state.target_runtime)
            write([item]); state.reload(datetime(2026,9,24,10,2,0)); self.assertIsNone(state.target_runtime["T"].first_activation)


class AcquisitionTests(unittest.TestCase):
    def _raw(self):
        return {"NM3U8DL_PLAYLIST_GROUPS":{
            "COMMON":[],
            "SONYLIV_EVENTS":[
                {"url":"https://good.test/list.m3u","name":"good"},
                {"url":"https://bad.test/list.m3u","name":"bad"},
            ],
        }}

    def test_partial_source_failure_does_not_abort_other_source(self):
        playlist='#EXTM3U\n#EXTINF:-1 tvg-name="Asian Games" group-title="Sports",Asian Games\nhttps://cdn.test/hls/live/2120305/AG_Strea2309/ENG/master.m3u8\n'
        def fake_probe(items):
            return tuple(replace(c, launchable=True, probe_status="working", quality_known=True, video_width=1920, video_height=1080, video_fps=50, video_bitrate_bps=5_000_000, final_stream_url=c.stream_url) for c in items)
        with patch.object(coord,"fetch_playlist_documents",return_value=({"https://good.test/list.m3u":playlist},("bad: OSError: boom",),{})), patch.object(coord,"probe_candidates",side_effect=fake_probe):
            found,errors=coord.acquire_active_targets(self._raw(),(view(),))
        self.assertEqual(len(found["T"]),1)
        self.assertEqual(errors,("bad: OSError: boom",))

    def test_metadata_only_matching_entry_remains_visible_unusable(self):
        raw={"NM3U8DL_PLAYLIST_GROUPS":{"COMMON":[],"SONYLIV_EVENTS":[{"url":"https://good.test/list.m3u","name":"good"}]}}
        playlist='#EXTM3U\n#EXTINF:-1 tvg-name="Asian Games" group-title="Sports",Asian Games\n'
        with patch.object(coord,"fetch_playlist_documents",return_value=({"https://good.test/list.m3u":playlist},(),{})):
            found,errors=coord.acquire_active_targets(raw,(view(),))
        self.assertEqual(errors,())
        self.assertEqual(len(found["T"]),1)
        self.assertEqual(found["T"][0].probe_status,"no_playable_source")
        self.assertFalse(found["T"][0].launchable)

    def test_same_identity_nonmatching_metadata_is_retained_as_context(self):
        raw={"NM3U8DL_PLAYLIST_GROUPS":{"COMMON":[],"SONYLIV_EVENTS":[
            {"url":"https://one.test/list.m3u","name":"one"},
            {"url":"https://two.test/list.m3u","name":"two"},
        ]}}
        one='#EXTM3U\n#EXTINF:-1 tvg-name="Asian Games" group-title="Sports",Asian Games\nhttps://a.test/hls/live/2120305/AG_Strea2309/ENG/master.m3u8\n'
        two='#EXTM3U\n#EXTINF:-1 tvg-name="Athletics" group-title="Sports",Athletics\nhttps://b.test/hls/live/2120305/AG_Strea2309/ENG/master.m3u8\n'
        def fake_probe(items):
            return tuple(replace(c, launchable=not c.ignored, probe_status="working" if not c.ignored else "no_playable_source", quality_known=not c.ignored, video_width=1920 if not c.ignored else 0, video_height=1080 if not c.ignored else 0, video_fps=50 if not c.ignored else 0, final_stream_url=c.stream_url) for c in items)
        with patch.object(coord,"fetch_playlist_documents",return_value=({"https://one.test/list.m3u":one,"https://two.test/list.m3u":two},(),{})), patch.object(coord,"probe_candidates",side_effect=fake_probe):
            found,_=coord.acquire_active_targets(raw,(view(),))
        self.assertEqual(len(found["T"]),2)
        context=[c for c in found["T"] if c.ignored]
        self.assertEqual(len(context),1)
        self.assertEqual(context[0].entry_title,"Athletics")
        self.assertIn("same feed identity context",context[0].reason)


class SnapshotAndChangeTests(unittest.TestCase):
    def test_same_identity_sources_group_into_one_block(self):
        a=sony_candidate(playlist="https://one/list",source_name="one",fps=25)
        b=sony_candidate(playlist="https://two/list",source_name="two",fps=50)
        snap=snapshot([a,b])
        self.assertEqual(len(snap.blocks),1)
        block=next(iter(snap.blocks.values()))
        self.assertEqual(len(block.observations),2)
        self.assertEqual(block.best_candidate.video_fps,50)

    def test_first_snapshot_is_baseline_not_new_event(self):
        self.assertEqual(coord.diff_snapshots(None,snapshot([sony_candidate()])),())

    def test_new_identity_emits_new_and_beeps(self):
        empty=snapshot([])
        current=snapshot([sony_candidate()])
        events=coord.diff_snapshots(empty,current)
        self.assertEqual([e.marker for e in events],["NEW"])
        self.assertTrue(events[0].beep)

    def test_identity_disappearance_emits_removed_without_beep(self):
        old=snapshot([sony_candidate()])
        new=snapshot([])
        events=coord.diff_snapshots(old,new)
        self.assertEqual([e.marker for e in events],["REMOVED"])
        self.assertFalse(events[0].beep)

    def test_source_addition_emits_source_plus_without_beep(self):
        old=snapshot([sony_candidate(playlist="https://one/list",source_name="one")])
        new=snapshot([
            sony_candidate(playlist="https://one/list",source_name="one"),
            sony_candidate(playlist="https://two/list",source_name="two"),
        ])
        events=coord.diff_snapshots(old,new)
        source=[e for e in events if e.marker=="SOURCE+"]
        self.assertEqual(len(source),1)
        self.assertFalse(source[0].beep)

    def test_source_removal_emits_source_minus_without_beep(self):
        old=snapshot([
            sony_candidate(playlist="https://one/list",source_name="one"),
            sony_candidate(playlist="https://two/list",source_name="two"),
        ])
        new=snapshot([sony_candidate(playlist="https://one/list",source_name="one")])
        events=coord.diff_snapshots(old,new)
        source=[e for e in events if e.marker=="SOURCE-"]
        self.assertEqual(len(source),1)
        self.assertFalse(source[0].beep)

    def test_metadata_change_emits_update_and_beeps(self):
        old=snapshot([sony_candidate(title="Shooting")])
        new=snapshot([sony_candidate(title="Athletics")])
        events=coord.diff_snapshots(old,new)
        update=[e for e in events if e.marker=="UPDATE"]
        self.assertEqual(len(update),1)
        self.assertTrue(update[0].beep)
        self.assertIn("Shooting",update[0].details[0])
        self.assertIn("Athletics",update[0].details[0])

    def test_quality_improvement_emits_quality_plus_and_beeps(self):
        old=snapshot([sony_candidate(fps=25)])
        new=snapshot([sony_candidate(fps=50)])
        events=coord.diff_snapshots(old,new)
        q=[e for e in events if e.marker=="QUALITY+"]
        self.assertEqual(len(q),1)
        self.assertTrue(q[0].beep)

    def test_quality_reduction_emits_quality_minus_without_beep(self):
        old=snapshot([sony_candidate(fps=50)])
        new=snapshot([sony_candidate(fps=25)])
        events=coord.diff_snapshots(old,new)
        q=[e for e in events if e.marker=="QUALITY-"]
        self.assertEqual(len(q),1)
        self.assertFalse(q[0].beep)

    def test_usability_transition_emits_update(self):
        old=snapshot([sony_candidate(launchable=False,status="probe_failed")])
        new=snapshot([sony_candidate(launchable=True,status="working")])
        events=coord.diff_snapshots(old,new)
        self.assertTrue(any(e.marker=="UPDATE" and e.beep for e in events))

    def test_moving_existing_identity_between_policies_does_not_invent_new_feed(self):
        c=sony_candidate()
        old=snapshot([c],policy=coord.POLICY_MANUAL)
        new=snapshot([c],policy=coord.POLICY_ALL)
        events=coord.diff_snapshots(old,new)
        self.assertFalse(any(e.marker=="NEW" for e in events))
        self.assertFalse(any(e.marker=="REMOVED" for e in events))

    def test_dashboard_renders_candidate_rows(self):
        snap=snapshot([sony_candidate()])
        rendered=coord.render_dashboard(snap,())
        self.assertIn("[ON] Asian Games",rendered)
        self.assertIn("1920x1080",rendered)
        self.assertIn("SONYLIV|lane:2120305/AG_Strea2309/ENG",rendered)

    def test_display_order_keeps_existing_rows_stable_and_new_at_top(self):
        a=sony_candidate(lane="1/A/ENG")
        b=sony_candidate(lane="2/B/ENG")
        first=snapshot([a])
        order=coord.update_display_order({coord.POLICY_MANUAL:[],coord.POLICY_ALL:[]},first)
        second=snapshot([a,b])
        order2=coord.update_display_order(order,second)
        ids=[block.identity.serialized for block in second.blocks.values()]
        a_id=next(x for x in ids if "lane:1/A/ENG" in x)
        b_id=next(x for x in ids if "lane:2/B/ENG" in x)
        self.assertEqual(order2[coord.POLICY_MANUAL],[b_id,a_id])


class TimingTests(unittest.TestCase):
    def test_watch_sleep_does_not_sleep_past_target_start(self):
        now=datetime(2026,9,24,10,0,0)
        t=target(schedule_start=now+timedelta(seconds=30))
        snap=coord.DashboardSnapshot(now,(coord.TargetView(t,"SCHEDULED",t.schedule_start,None),),None,{})
        self.assertEqual(coord.next_watch_sleep_seconds(snap,300,now=now),30)

    def test_watch_sleep_does_not_sleep_past_coordinator_end(self):
        now=datetime(2026,9,24,10,0,0)
        window=coord.CoordinatorWindow("ACTIVE",now-timedelta(minutes=1),now+timedelta(seconds=20))
        snap=coord.DashboardSnapshot(now,(),window,{})
        self.assertEqual(coord.next_watch_sleep_seconds(snap,300,now=now),20)

    def test_refresh_interval_used_when_no_earlier_boundary(self):
        now=datetime(2026,9,24,10,0,0)
        snap=coord.DashboardSnapshot(now,(),None,{})
        self.assertEqual(coord.next_watch_sleep_seconds(snap,120,now=now),120)


class AdditionalRegressionTests(unittest.TestCase):
    def test_duplicate_playlist_urls_are_deduplicated_across_common_and_group(self):
        raw={"NM3U8DL_PLAYLIST_GROUPS":{
            "COMMON":["https://same.test/list"],
            "SONYLIV_EVENTS":["https://same.test/list","https://other.test/list"],
        }}
        specs=coord.sources_for_group(raw,"SONYLIV_EVENTS")
        self.assertEqual([s.url for s in specs],["https://same.test/list","https://other.test/list"])

    def test_coordinator_window_transitions_waiting_active_expired(self):
        start=datetime(2026,9,24,11,0,0)
        state=coord.CoordinatorConfigState(Path("unused"))
        state.coordinator_schedule_start=start
        state.coordinator_run_duration_min=30
        state._coordinator_schedule_loaded=True
        self.assertEqual(state.coordinator_window(start-timedelta(seconds=1)).status,"WAITING")
        self.assertEqual(state.coordinator_window(start+timedelta(minutes=1)).status,"ACTIVE")
        self.assertEqual(state.coordinator_window(start+timedelta(minutes=30)).status,"EXPIRED")

    def test_invalid_refresh_interval_reload_keeps_previous_valid_value(self):
        with tempfile.TemporaryDirectory() as td:
            path=Path(td)/"config.py"
            path.write_text('IDENTITY_COORDINATOR_TARGETS=[]\nNM3U8DL_PLAYLIST_GROUPS={"COMMON":[]}\nIDENTITY_COORDINATOR_REFRESH_INTERVAL_SEC=60\n',encoding="utf-8")
            state=coord.CoordinatorConfigState(path)
            state.reload(datetime(2026,9,24,10,0,0))
            self.assertEqual(state.refresh_interval_sec,60)
            path.write_text('IDENTITY_COORDINATOR_TARGETS=[]\nNM3U8DL_PLAYLIST_GROUPS={"COMMON":[]}\nIDENTITY_COORDINATOR_REFRESH_INTERVAL_SEC=0\n',encoding="utf-8")
            messages,changed=state.reload(datetime(2026,9,24,10,1,0))
            self.assertFalse(changed)
            self.assertEqual(state.refresh_interval_sec,60)
            self.assertTrue(messages)

    def test_same_identity_matching_two_targets_is_one_block_with_two_target_names(self):
        c=sony_candidate()
        t1=target(name="T1")
        t2=target(name="T2")
        snap=coord.build_snapshot((view(t1),view(t2)),{"T1":(c,),"T2":(c,)},now=datetime(2026,9,24,10,0,0))
        self.assertEqual(len(snap.blocks),1)
        block=next(iter(snap.blocks.values()))
        self.assertEqual(block.target_names,["T1","T2"])

    def test_hotstar_unknown_expiry_is_auth_unknown_not_working(self):
        c=SourceCandidate(
            stream_url="https://hotstar.test/live.m3u8",
            launchable=True,
            probe_status="working",
            expiry=None,
            extra={"provider":"HOTSTAR"},
        )
        self.assertEqual(candidate_state(c),"AUTH_UNKNOWN")

    def test_identity_reappearance_after_absence_is_new_again(self):
        present=snapshot([sony_candidate()])
        absent=snapshot([])
        removed=coord.diff_snapshots(present,absent)
        self.assertTrue(any(e.marker=="REMOVED" for e in removed))
        reappeared=coord.diff_snapshots(absent,present)
        self.assertTrue(any(e.marker=="NEW" and e.beep for e in reappeared))


if __name__ == "__main__":
    unittest.main(verbosity=2)
