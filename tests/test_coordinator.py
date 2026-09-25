from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
import queue
import tempfile
import wave
import threading
import unittest
from unittest.mock import patch

import recorder_event_coordinator as coord
from recorder_coordinator import terminal as coordinator_terminal
from recorder_coordinator.snapshot import candidate_row_key, candidate_state
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


class OutputPathTests(unittest.TestCase):
    def test_coordinator_log_path_uses_configured_output_root(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Manual Recordings"
            config_path = Path(td) / "config.py"
            config_path.write_text(
                "RECORDING_OUTPUT_DIR = " + repr(str(root)) + "\n",
                encoding="utf-8",
            )
            started = datetime(2026, 9, 25, 0, 6, 7)

            log_path = coord._coordinator_log_path(config_path, started)

            self.assertEqual(
                log_path,
                root
                / "recorder_logs"
                / "coordinator_logs"
                / "IDENTITY_COORDINATOR_20260925_000607.log",
            )
            self.assertTrue(log_path.parent.is_dir())


class ManualRecordLaunchTests(unittest.TestCase):
    def test_registry_blocks_duplicate_launch_without_hiding_dashboard_identity(self):
        snap = snapshot([sony_candidate()])
        identity_key = next(
            identity
            for policy, identity in snap.blocks
            if policy == coord.POLICY_MANUAL
        )

        with tempfile.TemporaryDirectory() as td:
            paths = coord.build_recorder_output_paths(Path(td) / "Recordings")
            store = coord.IdentityRegistryStore(paths)
            status = store.prepare_session()
            display_order = {
                coord.POLICY_ALL: [],
                coord.POLICY_MANUAL: [identity_key],
            }

            self.assertEqual(
                coord._manual_record_choices(snap, display_order, store),
                ((1, identity_key),),
            )
            store.claim(
                identity_key=identity_key,
                provider="SONYLIV",
                display_name="Asian Games",
                expected_session_id=status.session_id,
            )

            self.assertEqual(
                coord._manual_record_choices(snap, display_order, store),
                (),
            )
            self.assertIn(
                (coord.POLICY_MANUAL, identity_key),
                snap.blocks,
            )

    def test_manual_record_choices_preserve_dashboard_numbers_when_first_is_blocked(self):
        first = sony_candidate(lane="1/A/ENG", title="First Event")
        second = sony_candidate(lane="2/B/ENG", title="Second Event")
        snap = snapshot([first, second])
        identities = [
            identity
            for policy, identity in snap.blocks
            if policy == coord.POLICY_MANUAL
        ]
        display_order = {
            coord.POLICY_ALL: [],
            coord.POLICY_MANUAL: identities,
        }

        with tempfile.TemporaryDirectory() as td:
            paths = coord.build_recorder_output_paths(Path(td) / "Recordings")
            store = coord.IdentityRegistryStore(paths)
            status = store.prepare_session()
            store.claim(
                identity_key=identities[0],
                provider="SONYLIV",
                display_name="First Event",
                expected_session_id=status.session_id,
            )

            choices = coord._manual_record_choices(
                snap,
                display_order,
                store,
            )

        self.assertEqual(choices, ((2, identities[1]),))
        rendered = coord.render_manual_record_menu(snap, choices)
        self.assertIn("  2. Second Event", rendered)
        self.assertNotIn("  1. First Event", rendered)

    def test_manual_launch_handoff_contains_selected_exact_source_and_identity(self):
        snap = snapshot([sony_candidate()])
        identity_key = next(
            identity
            for policy, identity in snap.blocks
            if policy == coord.POLICY_MANUAL
        )

        with tempfile.TemporaryDirectory() as td:
            paths = coord.build_recorder_output_paths(Path(td) / "Recordings")
            store = coord.IdentityRegistryStore(paths)
            status = store.prepare_session()
            captured = {}

            def fake_launch(request, registry_store, *, config_path):
                captured["request"] = request
                captured["store"] = registry_store
                captured["config_path"] = config_path
                return type("Result", (), {"pid": 4321})()

            with patch.object(coord, "launch_identity_worker", fake_launch):
                config_path = Path(td) / "config.py"
                config_path.write_text("# test config\n", encoding="utf-8")
                plan, result = coord._launch_manual_identity(
                    snap,
                    identity_key,
                    registry_session_id=status.session_id,
                    registry_store=store,
                    config_path=config_path,
                    raw_config={
                        "NM3U8DL_PLAYLIST_GROUPS": {
                            "COMMON": ["https://common.test/list.m3u"],
                            "SONYLIV_EVENTS": ["https://src1.test/list.m3u"],
                        },
                    },
                )

            request = captured["request"]
            self.assertEqual(result.pid, 4321)
            self.assertEqual(request.identity_key, identity_key)
            self.assertEqual(
                request.selected_candidate.stream_url,
                plan.selected_candidate.stream_url,
            )
            self.assertEqual(
                request.selected_source_group,
                "SONYLIV_EVENTS",
            )
            self.assertEqual(
                request.registry_session_id,
                status.session_id,
            )
            self.assertEqual(
                request.recovery_playlist_urls,
                ("https://src1.test/list.m3u",),
            )
            self.assertEqual(
                captured["config_path"],
                config_path,
            )


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

    def test_sources_for_tv_group_does_not_implicitly_include_common(self):
        raw={"NM3U8DL_PLAYLIST_GROUPS":{
            "COMMON":["https://common.test/list"],
            "TV":["https://tv.test/list"],
        }}
        specs=coord.sources_for_group(raw,"SONY_TV")
        self.assertEqual([s.url for s in specs],["https://tv.test/list"])
        self.assertTrue(all(s.provider=="SONYLIV" for s in specs))

    def test_explicit_common_inherits_single_target_provider_context(self):
        raw={"NM3U8DL_PLAYLIST_GROUPS":{
            "COMMON":["https://common.test/list"],
            "SONYLIV_EVENTS":["https://sony.test/list"],
        }}
        t=target(source_groups=("COMMON","SONYLIV_EVENTS"))
        coord.validate_target_source_scopes(raw,(t,))
        specs=coord.sources_for_group(raw,"COMMON",context_group="SONYLIV_EVENTS")
        self.assertEqual([s.url for s in specs],["https://common.test/list"])
        self.assertTrue(all(s.provider=="SONYLIV" for s in specs))
        self.assertTrue(all(s.group=="SONYLIV_EVENTS" for s in specs))

    def test_common_with_multiple_provider_groups_is_rejected_as_ambiguous(self):
        raw={"NM3U8DL_PLAYLIST_GROUPS":{
            "COMMON":["https://common.test/list"],
            "SONYLIV_EVENTS":["https://sony.test/list"],
            "FANCODE":["https://fancode.test/list"],
        }}
        t=target(source_groups=("COMMON","SONYLIV_EVENTS","FANCODE"))
        with self.assertRaisesRegex(ValueError,"COMMON.*exactly one non-COMMON"):
            coord.validate_target_source_scopes(raw,(t,))

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

    def test_newer_nonmatching_metadata_rejects_stale_matching_identity(self):
        raw={"NM3U8DL_PLAYLIST_GROUPS":{"COMMON":[],"SONYLIV_EVENTS":[
            {"url":"https://old.test/list.m3u","name":"old"},
            {"url":"https://new.test/list.m3u","name":"new"},
        ]}}
        matching='#EXTM3U\n#EXTINF:-1 tvg-name="Asian Games",Asian Games\nhttps://a.test/hls/live/2120305/AG_Strea2309/ENG/master.m3u8\n'
        moved='#EXTM3U\n#EXTINF:-1 tvg-name="Swimming",Swimming\nhttps://b.test/hls/live/2120305/AG_Strea2309/ENG/master.m3u8\n'
        def freshness(url,*args,**kwargs):
            return {
                "timestamp":1000.0 if "old.test" in url else 2000.0,
                "source":"commit",
                "content_hash":url,
            }
        with patch.object(
            coord,"fetch_playlist_documents",
            return_value=(
                {"https://old.test/list.m3u":matching,"https://new.test/list.m3u":moved},
                (),
                {},
            ),
        ), patch.object(
            coord,"resolve_playlist_source_freshness",side_effect=freshness
        ), patch.object(
            coord,"probe_candidates",side_effect=lambda items: tuple(items)
        ):
            found,_=coord.acquire_active_targets(raw,(view(),))
        self.assertEqual(found["T"],())

    def test_newer_matching_metadata_keeps_identity_and_context_routes(self):
        raw={"NM3U8DL_PLAYLIST_GROUPS":{"COMMON":[],"SONYLIV_EVENTS":[
            {"url":"https://old.test/list.m3u","name":"old"},
            {"url":"https://new.test/list.m3u","name":"new"},
        ]}}
        old='#EXTM3U\n#EXTINF:-1 tvg-name="Swimming",Swimming\nhttps://a.test/hls/live/2120305/AG_Strea2309/ENG/master.m3u8\n'
        matching='#EXTM3U\n#EXTINF:-1 tvg-name="Asian Games",Asian Games\nhttps://b.test/hls/live/2120305/AG_Strea2309/ENG/master.m3u8\n'
        def freshness(url,*args,**kwargs):
            return {
                "timestamp":1000.0 if "old.test" in url else 2000.0,
                "source":"commit",
                "content_hash":url,
            }
        with patch.object(
            coord,"fetch_playlist_documents",
            return_value=(
                {"https://old.test/list.m3u":old,"https://new.test/list.m3u":matching},
                (),
                {},
            ),
        ), patch.object(
            coord,"resolve_playlist_source_freshness",side_effect=freshness
        ), patch.object(
            coord,"probe_candidates",side_effect=lambda items: tuple(items)
        ):
            found,_=coord.acquire_active_targets(raw,(view(),))
        self.assertEqual(len(found["T"]),2)
        self.assertEqual(sum(not item.ignored for item in found["T"]),1)

    def test_conflicting_equal_freshness_keeps_identity_conservatively(self):
        raw={"NM3U8DL_PLAYLIST_GROUPS":{"COMMON":[],"SONYLIV_EVENTS":[
            {"url":"https://one.test/list.m3u","name":"one"},
            {"url":"https://two.test/list.m3u","name":"two"},
        ]}}
        matching='#EXTM3U\n#EXTINF:-1 tvg-name="Asian Games",Asian Games\nhttps://a.test/hls/live/2120305/AG_Strea2309/ENG/master.m3u8\n'
        moved='#EXTM3U\n#EXTINF:-1 tvg-name="Swimming",Swimming\nhttps://b.test/hls/live/2120305/AG_Strea2309/ENG/master.m3u8\n'
        with patch.object(
            coord,"fetch_playlist_documents",
            return_value=(
                {"https://one.test/list.m3u":matching,"https://two.test/list.m3u":moved},
                (),
                {},
            ),
        ), patch.object(
            coord,
            "resolve_playlist_source_freshness",
            return_value={"timestamp":2000.0,"source":"commit","content_hash":"x"},
        ), patch.object(
            coord,"probe_candidates",side_effect=lambda items: tuple(items)
        ):
            found,_=coord.acquire_active_targets(raw,(view(),))
        self.assertEqual(len(found["T"]),2)

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
        self.assertIn("Quality : 1920x1080 | 50p | 5000 Kbps",rendered)
        self.assertIn("Identity: lane:2120305/AG_Strea2309/ENG",rendered)

    def test_dashboard_overlays_registry_state_and_active_recordings(self):
        snap=snapshot([sony_candidate()])
        identity_key=next(
            identity
            for policy,identity in snap.blocks
            if policy==coord.POLICY_MANUAL
        )
        rendered=coord.render_dashboard(
            snap,
            (),
            registry_entries={
                identity_key:{
                    "identity":identity_key,
                    "provider":"SONYLIV",
                    "display_name":"ENG _ Asian Games",
                    "state":"ACTIVE",
                    "worker_pid":4321,
                }
            },
        )
        identity_line=next(
            line for line in rendered.splitlines() if "Identity:" in line
        )
        self.assertIn("[1] ACTIVE SONYLIV",identity_line)
        self.assertIn("ACTIVE RECORDINGS",rendered)
        self.assertIn("[ACTIVE] ENG _ Asian Games | SONYLIV | PID 4321",rendered)

        colored=coord.render_dashboard(
            snap,
            (),
            use_color=True,
            registry_entries={
                identity_key:{
                    "identity":identity_key,
                    "provider":"SONYLIV",
                    "display_name":"ENG _ Asian Games",
                    "state":"ACTIVE",
                    "worker_pid":4321,
                }
            },
        )
        self.assertIn(
            "\033[38;2;41;159;214mACTIVE RECORDINGS\033[0m",
            colored,
        )
        self.assertIn(
            "\033[38;2;255;135;3m[ACTIVE]\033[0m",
            colored,
        )

    def test_dashboard_terminal_registry_state_suppresses_available_label(self):
        snap=snapshot([sony_candidate()])
        identity_key=next(
            identity
            for policy,identity in snap.blocks
            if policy==coord.POLICY_MANUAL
        )
        rendered=coord.render_dashboard(
            snap,
            (),
            registry_entries={
                identity_key:{
                    "identity":identity_key,
                    "provider":"SONYLIV",
                    "display_name":"ENG _ Asian Games",
                    "state":"MANUALLY_STOPPED",
                    "worker_pid":4321,
                }
            },
        )
        identity_line=next(
            line for line in rendered.splitlines() if "Identity:" in line
        )
        self.assertIn("[1] MANUALLY_STOPPED SONYLIV",identity_line)
        self.assertNotIn("ACTIVE RECORDINGS",rendered)

    def test_dashboard_quality_shows_shared_quality_evidence(self):
        item=replace(
            sony_candidate(),
            video_resolution_source="manifest",
            video_fps_source="manifest",
            video_scan_type_source="event-policy",
            video_bitrate_source="sample",
        )
        rendered=coord.render_dashboard(snapshot([item]),())
        self.assertIn(
            "Quality : 1920x1080 | 50p [manifest, event-policy] | ~5000 Kbps [FFmpeg sample]",
            rendered,
        )

    def test_dashboard_header_shows_target_search_and_source_scope(self):
        t=target(
            name="Asian Games",
            primary=("Asian Games",),
            required=("ENG",),
            rejected=("Highlights",),
        )
        snap=coord.build_snapshot(
            (view(t),),
            {"Asian Games":(sony_candidate(),)},
            now=datetime(2026,9,24,10,0,0),
        )
        rendered=coord.render_dashboard(
            snap,
            (),
            config_path=Path("recorder_dynamic_user_config.py"),
            refresh_interval_sec=300,
        )
        self.assertIn("RECORDER EVENT COORDINATOR",rendered)
        self.assertIn("Target 1",rendered)
        self.assertIn("Asian Games | MANUAL | ACTIVE",rendered)
        self.assertIn("Search: Asian Games | required: ENG | exclude: Highlights",rendered)
        self.assertIn("Sources: SONYLIV_EVENTS",rendered)

    def test_dashboard_groups_quality_variants_under_one_identity(self):
        a=sony_candidate(playlist="https://one/list",source_name="one",fps=50)
        b=sony_candidate(playlist="https://two/list",source_name="two",fps=25)
        snap=snapshot([a,b])
        rendered=coord.render_dashboard(snap,())
        self.assertEqual(rendered.count("Quality : 1920x1080"),2)
        self.assertIn("Quality : 1920x1080 | 50p | 5000 Kbps [BEST]",rendered)
        self.assertIn("Quality : 1920x1080 | 25p | 5000 Kbps",rendered)
        self.assertEqual(rendered.count("[ON] Asian Games"),2)

    def test_single_quality_group_does_not_show_best_marker(self):
        snap=snapshot([sony_candidate()])
        rendered=coord.render_dashboard(snap,())
        self.assertNotIn("[BEST]",rendered)

    def test_source_plus_marker_is_on_added_source_row_only(self):
        old=snapshot([sony_candidate(playlist="https://one/list",source_name="one")])
        new=snapshot([
            sony_candidate(playlist="https://one/list",source_name="one"),
            sony_candidate(playlist="https://two/list",source_name="two"),
        ])
        events=coord.diff_snapshots(old,new)
        rendered=coord.render_dashboard(new,events)
        identity_line=next(line for line in rendered.splitlines() if "Identity:" in line)
        self.assertNotIn("[SOURCE+]",identity_line)
        source_lines=[line for line in rendered.splitlines() if "[ON]" in line]
        self.assertEqual(sum("[SOURCE+]" in line for line in source_lines),1)

    def test_update_marker_is_on_changed_source_row_with_delta(self):
        old=snapshot([sony_candidate(title="Shooting")])
        new=snapshot([sony_candidate(title="Athletics")])
        events=coord.diff_snapshots(old,new)
        rendered=coord.render_dashboard(new,events)
        identity_line=next(line for line in rendered.splitlines() if "Identity:" in line)
        self.assertNotIn("[UPDATE]",identity_line)
        self.assertIn("[UPDATE] [ON] Athletics",rendered)
        self.assertIn("Event Shooting -> Athletics",rendered)

    def test_source_minus_is_not_a_live_dashboard_marker(self):
        old=snapshot([
            sony_candidate(playlist="https://one/list",source_name="one"),
            sony_candidate(playlist="https://two/list",source_name="two"),
        ])
        new=snapshot([sony_candidate(playlist="https://one/list",source_name="one")])
        events=coord.diff_snapshots(old,new)
        rendered=coord.render_dashboard(new,events)
        self.assertNotIn("[SOURCE-]",rendered)
        self.assertNotIn("two",rendered)

    def test_last_updated_stays_stable_until_row_changes(self):
        registry={}
        t1=datetime(2026,9,24,10,0,0)
        t2=datetime(2026,9,24,10,5,0)
        t3=datetime(2026,9,24,10,7,0)
        first=coord.build_snapshot(
            (view(now=t1),),
            {"T":(sony_candidate(title="Shooting"),)},
            now=t1,
            row_update_registry=registry,
        )
        second=coord.build_snapshot(
            (view(now=t2),),
            {"T":(sony_candidate(title="Shooting"),)},
            now=t2,
            row_update_registry=registry,
        )
        third=coord.build_snapshot(
            (view(now=t3),),
            {"T":(sony_candidate(title="Athletics"),)},
            now=t3,
            row_update_registry=registry,
        )
        coord.diff_snapshots(second,third)
        self.assertIn("Last Updated 10:00",coord.render_dashboard(first,()))
        self.assertIn("Last Updated 10:00",coord.render_dashboard(second,()))
        self.assertIn("Last Updated 10:07",coord.render_dashboard(third,()))

    def test_header_keeps_each_target_on_one_logical_line(self):
        now=datetime(2026,9,24,17,28,57)
        t=target(
            name="Asian Games",
            primary=("Presidents cup",),
            rejected=(("TAM","Tamil"),),
            preferred=(("ENG","English"),),
        )
        window=coord.CoordinatorWindow("ACTIVE",now,None)
        snap=coord.build_snapshot(
            (coord.TargetView(t,"ACTIVE",now,None),),
            {"Asian Games":()},
            coordinator_window=window,
            now=now,
        )
        rendered=coord.render_dashboard(
            snap,
            (),
            config_path=Path("recorder_dynamic_user_config.py"),
            refresh_interval_sec=300,
        )
        target_lines=[line for line in rendered.splitlines() if line.startswith("Target 1")]
        self.assertEqual(len(target_lines),1)
        self.assertIn("Asian Games | MANUAL | ACTIVE until stopped",target_lines[0])
        self.assertIn("Search: Presidents cup",target_lines[0])
        self.assertIn("exclude: (TAM OR Tamil)",target_lines[0])
        self.assertIn("prefer: (ENG OR English)",target_lines[0])
        self.assertIn("Sources: SONYLIV_EVENTS",target_lines[0])
        self.assertNotIn("Mode         :",rendered)
        self.assertNotIn("mode=inspect",rendered)
        self.assertIn("Coordinator\n  Status    : ACTIVE",rendered)
        self.assertIn("  Started   : 2026-09-24 17:28:57",rendered)
        self.assertIn("  End       : until stopped",rendered)
        event_watch_lines=[
            line for line in rendered.splitlines()
            if line.startswith("EVENT WATCH ")
        ]
        self.assertEqual(len(event_watch_lines),1)
        self.assertIn("| Provider=- | Identity blocks=0",event_watch_lines[0])

    def test_compact_source_name_preserves_github_provenance(self):
        self.assertEqual(
            coord.compact_source_name(
                "https://raw.githubusercontent.com/user/repo/refs/heads/main/path/fancode.m3u"
            ),
            "github:user/repo@main/path/fancode.m3u",
        )
        self.assertEqual(
            coord.compact_source_name(
                "https://premiumplugx.com//VIP/pluglist.php"
            ),
            "premiumplugx.com/VIP/pluglist.php",
        )

    def test_initial_context_callback_runs_before_acquisition(self):
        order=[]
        now=datetime(2026,9,24,10,0,0)

        class FakeState:
            raw_config={}
            def reload(self, value):
                return (), False
            def coordinator_window(self, value):
                return coord.CoordinatorWindow("ACTIVE",value,None)
            def target_views(self, value, *, coordinator_active=True):
                return (view(now=value),)

        def context(snapshot):
            order.append("header")
        def acquire(*args, **kwargs):
            order.append("acquire")
            return {"T":()}, ()

        with patch.object(coord,"acquire_active_targets",side_effect=acquire):
            coord.run_once(FakeState(),None,context_callback=context)
        self.assertEqual(order[:2],["header","acquire"])

    def test_nextpvr_reference_palette_is_used_for_event_group_and_marker(self):
        snap=snapshot([sony_candidate(group="Hockey")])
        rendered=coord.render_dashboard(
            snap,
            (coord.ChangeEvent("NEW",(coord.POLICY_MANUAL,next(iter(snap.blocks))[1]),("x",),True),),
            use_color=True,
        )
        self.assertIn("\033[38;2;41;159;214mAsian Games\033[0m",rendered)
        self.assertIn("\033[38;2;41;159;214mHockey\033[0m",rendered)
        self.assertIn("\033[38;2;255;135;3m[NEW]\033[0m",rendered)

    def test_update_delta_is_highlighted_yellow(self):
        old=snapshot([sony_candidate(title="Shooting")])
        new=snapshot([sony_candidate(title="Athletics")])
        events=coord.diff_snapshots(old,new)
        rendered=coord.render_dashboard(new,events,use_color=True)
        self.assertIn(
            "\033[38;2;255;215;0mEvent Shooting -> Athletics\033[0m",
            rendered,
        )

    def test_rows_within_quality_group_sort_by_last_updated_newest_first(self):
        older=sony_candidate(
            playlist="https://older/list",
            source_name="older",
            title="Older",
        )
        newer=sony_candidate(
            playlist="https://newer/list",
            source_name="newer",
            title="Newer",
        )
        snap=snapshot([older,newer])
        block=next(iter(snap.blocks.values()))
        block.row_last_updated[candidate_row_key(older)]=datetime(2026,9,24,10,0,0)
        block.row_last_updated[candidate_row_key(newer)]=datetime(2026,9,24,10,5,0)
        rendered=coord.render_dashboard(snap,())
        self.assertLess(rendered.index("[ON] Newer"),rendered.index("[ON] Older"))

    def test_dashboard_shows_source_freshness_time_and_evidence(self):
        item=sony_candidate()
        source_time=datetime(2026,9,24,9,45,30).timestamp()
        item=replace(
            item,
            extra={
                **dict(item.extra),
                "source_freshness_ts":source_time,
                "source_freshness_source":"commit",
            },
        )
        rendered=coord.render_dashboard(snapshot([item]),())
        self.assertIn(
            "Source Updated 09:45 [commit]",
            rendered,
        )

    def test_timestamp_display_adds_date_only_for_another_day(self):
        item=sony_candidate()
        source_time=datetime(2026,9,23,23,55,30).timestamp()
        item=replace(
            item,
            extra={
                **dict(item.extra),
                "source_freshness_ts":source_time,
                "source_freshness_source":"commit",
            },
        )
        snap=snapshot([item],now=datetime(2026,9,24,0,5,0))
        block=next(iter(snap.blocks.values()))
        block.row_last_updated[candidate_row_key(item)]=datetime(2026,9,23,23,58,45)
        rendered=coord.render_dashboard(snap,())
        self.assertIn("Last Updated 2026-09-23 23:58",rendered)
        self.assertIn("Source Updated 2026-09-23 23:55 [commit]",rendered)
        self.assertNotIn("23:55:30",rendered)

    def test_equal_last_updated_rows_use_source_freshness_as_tiebreaker(self):
        older=sony_candidate(
            playlist="https://older/list",
            source_name="older",
            title="Older source",
        )
        newer=sony_candidate(
            playlist="https://newer/list",
            source_name="newer",
            title="Newer source",
        )
        older=replace(
            older,
            extra={
                **dict(older.extra),
                "source_freshness_ts":1000.0,
                "source_freshness_source":"commit",
            },
        )
        newer=replace(
            newer,
            extra={
                **dict(newer.extra),
                "source_freshness_ts":2000.0,
                "source_freshness_source":"commit",
            },
        )
        snap=snapshot([older,newer])
        block=next(iter(snap.blocks.values()))
        tied=datetime(2026,9,24,10,0,0)
        block.row_last_updated[candidate_row_key(older)]=tied
        block.row_last_updated[candidate_row_key(newer)]=tied
        rendered=coord.render_dashboard(snap,())
        self.assertLess(
            rendered.index("[ON] Newer source"),
            rendered.index("[ON] Older source"),
        )

    def test_coordinator_info_does_not_repeat_info_entry_control(self):
        state=coord.SoundSnoozeState()
        rendered=coord.render_coordinator_controls(state)
        self.assertNotIn("I  Coordinator information & controls",rendered)
        self.assertIn("S  Sound / notification snooze",rendered)

    def test_watch_footer_advertises_record_and_f5_refresh(self):
        rendered=coord.watch_status_text(0.0,coord.time.monotonic()+60)
        self.assertIn("r=record",rendered)
        self.assertIn("i=info",rendered)
        self.assertNotIn("s=sound",rendered)
        self.assertIn("F5=refresh",rendered)
        self.assertNotIn("p=record",rendered)

    def test_sound_snooze_menu_has_coordinator_scopes_only(self):
        state=coord.SoundSnoozeState()
        rendered=coord.render_sound_snooze_menu(state)
        self.assertIn("M  Snooze for 15 minutes",rendered)
        self.assertIn("F  Snooze for full Coordinator run",rendered)
        self.assertIn("U  Unsnooze / restore sounds",rendered)
        self.assertNotIn("current RUN",rendered)

    def test_coordinator_notification_is_suppressed_while_snoozed(self):
        event=coord.ChangeEvent("NEW",(coord.POLICY_MANUAL,"id"),("appeared",),beep=True)
        state=coord.SoundSnoozeState()
        coord.runtime_sound.set_indefinite_sound_snooze(state,"coordinator_run")
        with patch("recorder_coordinator.terminal.winsound") as sound:
            coord.beep((event,),state)
        sound.PlaySound.assert_not_called()
        sound.Beep.assert_not_called()

    def test_notification_pcm_volume_is_reduced_to_seventy_five_percent(self):
        self.assertEqual(coordinator_terminal.COORDINATOR_NOTIFICATION_VOLUME,0.75)
        frames=(
            int(10000).to_bytes(2,"little",signed=True)
            + int(-10000).to_bytes(2,"little",signed=True)
        )
        scaled=coordinator_terminal._scale_pcm_frames(frames,2,0.75)
        values=[
            int.from_bytes(scaled[i:i+2],"little",signed=True)
            for i in range(0,len(scaled),2)
        ]
        self.assertEqual(values,[7500,-7500])

    def test_coordinator_notification_prefers_custom_wav(self):
        event=coord.ChangeEvent("NEW",(coord.POLICY_MANUAL,"id"),("appeared",),beep=True)
        with patch("recorder_coordinator.terminal.winsound") as sound, patch(
            "recorder_coordinator.terminal._coordinator_notification_sound_path"
        ) as path:
            path.return_value.is_file.return_value=True
            path.return_value.__str__.return_value="coordinator_notification.wav"
            coord.beep((event,))
        sound.PlaySound.assert_called_once()
        sound.Beep.assert_not_called()

    def test_coordinator_notification_falls_back_to_two_note_pattern(self):
        event=coord.ChangeEvent("NEW",(coord.POLICY_MANUAL,"id"),("appeared",),beep=True)
        with patch("recorder_coordinator.terminal.winsound") as sound, patch(
            "recorder_coordinator.terminal._coordinator_notification_sound_path"
        ) as path:
            path.return_value.is_file.return_value=False
            coord.beep((event,))
        self.assertEqual(
            [call.args for call in sound.Beep.call_args_list],
            [(523,180),(659,320)],
        )

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


    def test_windows_command_reader_accepts_sound_without_enter(self):
        q=queue.Queue()
        stop=threading.Event()

        class FakeMsvcrt:
            @staticmethod
            def kbhit():
                return True
            @staticmethod
            def getwch():
                stop.set()
                return "s"

        with patch.object(coord.os,"name","nt"), patch.object(coord,"msvcrt",FakeMsvcrt):
            coord._command_reader(q,stop)
        self.assertEqual(q.get_nowait(),"s")

    def test_windows_command_reader_accepts_r_without_enter(self):
        q=queue.Queue()
        stop=threading.Event()

        class FakeMsvcrt:
            @staticmethod
            def kbhit():
                return True
            @staticmethod
            def getwch():
                stop.set()
                return "r"

        with patch.object(coord.os,"name","nt"), patch.object(coord,"msvcrt",FakeMsvcrt):
            coord._command_reader(q,stop)
        self.assertEqual(q.get_nowait(),"r")

    def test_windows_command_reader_maps_f5_to_refresh_command(self):
        q=queue.Queue()
        stop=threading.Event()
        keys=iter(["\x00","\x3f"])

        class FakeMsvcrt:
            @staticmethod
            def kbhit():
                return True
            @staticmethod
            def getwch():
                value=next(keys)
                if value=="\x3f":
                    stop.set()
                return value

        with patch.object(coord.os,"name","nt"), patch.object(coord,"msvcrt",FakeMsvcrt):
            coord._command_reader(q,stop)
        self.assertEqual(q.get_nowait(),"__F5__")


class RegistryStateChangeTests(unittest.TestCase):
    def test_registry_state_changes_report_only_state_transitions(self):
        old={
            "id-a":{"state":"ACTIVE","reason":"started"},
            "id-b":{"state":"ACTIVE","reason":"started"},
        }
        new={
            "id-a":{"state":"WAITING_FOR_SOURCE","reason":"source unavailable"},
            "id-b":{"state":"ACTIVE","reason":"metadata changed"},
            "id-c":{"state":"LAUNCHING","reason":"launch requested"},
        }

        changes=coord._registry_state_changes(old,new)

        self.assertEqual(
            changes,
            (
                ("id-a","ACTIVE","WAITING_FOR_SOURCE","source unavailable"),
                ("id-c","-","LAUNCHING","launch requested"),
            ),
        )


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
    def test_group_lookup_uses_only_the_named_bucket(self):
        raw={"NM3U8DL_PLAYLIST_GROUPS":{
            "COMMON":["https://common.test/list"],
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

    def test_hls_variant_unavailable_state_is_not_drm(self):
        c=SourceCandidate(
            stream_url="https://cdn.test/live/master.m3u8",
            launchable=False,
            probe_status="hls_variant_unavailable",
            extra={
                "provider":"SONYLIV",
                "hls_variant_probe_status":"hls_variant_unavailable",
                "hls_variant_probe_failure":"HTTP 404 Not Found — selected HLS variant/path unavailable",
            },
        )
        self.assertEqual(candidate_state(c),"HLS_VARIANT_UNAVAILABLE")

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
