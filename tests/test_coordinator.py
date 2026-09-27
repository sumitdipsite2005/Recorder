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
from recorder_coordinator import acquisition as coord_acquisition
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


def runtime_candidate(
    candidate,
    *,
    selected=False,
    status="WORKING",
    reason="",
    classification="",
    quality="1920x1080 | 50p | 5000 Kbps",
):
    return {
        "playlist_url": candidate.playlist_url,
        "matching_entry_index": candidate.matching_entry_index,
        "stream_url": candidate.stream_url,
        "entry_title": candidate.entry_title,
        "tvg_name": candidate.tvg_name,
        "group_title": candidate.group_title,
        "status": status,
        "classification": classification,
        "selected": selected,
        "selection_reason": reason,
        "quality": quality,
        "expiry": candidate.expiry,
        "expiry_source": candidate.expiry_source,
        "launchable": candidate.launchable,
    }


class CoordinatorCancellationTests(unittest.TestCase):
    def test_record_cancel_restore_helper_redraws_dashboard_and_watch_line(self):
        snap=snapshot([sony_candidate()])
        with patch.object(coord,"render_dashboard",return_value="DASHBOARD"), patch.object(
            coord,"clear_live_status_line"
        ) as clear_status, patch.object(
            coord,"clear_dashboard_terminal"
        ) as clear_dashboard, patch.object(
            coord,"set_live_status_line"
        ) as set_status, patch("builtins.print") as print_mock:
            coord._restore_dashboard_after_temporary_menu(
                snap,
                {coord.POLICY_MANUAL:[],coord.POLICY_ALL:[]},
                config_path=Path("recorder_dynamic_user_config.py"),
                refresh_interval_sec=300,
                registry_entries={},
                runtime_statuses={},
                source_references={},
                watch_text="WATCH",
            )

        clear_status.assert_called_once()
        clear_dashboard.assert_called_once()
        print_mock.assert_called_once_with("DASHBOARD")
        set_status.assert_called_once_with("WATCH")

    def test_windows_ctrl_c_sets_stop_event_before_main_loop_reads_queue(self):
        class FakeMsvcrt:
            def kbhit(self):
                return True
            def getwch(self):
                return "\x03"

        command_queue = queue.Queue()
        stop_event = threading.Event()
        with patch.object(coord.os, "name", "nt"), patch.object(
            coord,
            "msvcrt",
            FakeMsvcrt(),
        ):
            coord._command_reader(command_queue, stop_event)

        self.assertTrue(stop_event.is_set())
        self.assertEqual(command_queue.get_nowait(), "__CTRL_C__")

    def test_run_once_forwards_scan_cancellation_to_acquisition(self):
        cancelled = lambda: False
        captured = {}

        class ConfigState:
            raw_config = {}
            def reload(self, now):
                return (), False
            def coordinator_window(self, now):
                return type("Window", (), {"status": "ACTIVE"})()
            def target_views(self, now, coordinator_active):
                return ()

        def fake_acquire(*args, **kwargs):
            captured["stop_requested"] = kwargs.get("stop_requested")
            return {}, ()

        with patch.object(coord, "acquire_active_targets", side_effect=fake_acquire), patch.object(
            coord,
            "build_snapshot",
            return_value=type("Snapshot", (), {})(),
        ), patch.object(coord, "diff_snapshots", return_value=()):
            coord.run_once(ConfigState(), None, stop_requested=cancelled)

        self.assertIs(captured["stop_requested"], cancelled)


class CoordinatorQualityPersistenceTests(unittest.TestCase):
    def test_run_once_forwards_same_quality_registry_to_acquisition(self):
        registry = {}
        captured = []

        class ConfigState:
            raw_config = {}
            def reload(self, now):
                return (), False
            def coordinator_window(self, now):
                return type("Window", (), {"status": "ACTIVE"})()
            def target_views(self, now, coordinator_active):
                return ()

        def fake_acquire(*args, **kwargs):
            captured.append(kwargs.get("quality_evidence_registry"))
            return {}, ()

        with patch.object(coord, "acquire_active_targets", side_effect=fake_acquire), patch.object(
            coord,
            "build_snapshot",
            return_value=type("Snapshot", (), {})(),
        ), patch.object(coord, "diff_snapshots", return_value=()):
            coord.run_once(
                ConfigState(),
                None,
                quality_evidence_registry=registry,
            )
            coord.run_once(
                ConfigState(),
                None,
                quality_evidence_registry=registry,
            )

        self.assertEqual(captured, [registry, registry])
        self.assertIs(captured[0], registry)
        self.assertIs(captured[1], registry)


class CoordinatorEventTransitionStateTests(unittest.TestCase):
    def test_run_once_forwards_same_event_transition_registry_to_acquisition(self):
        registry = {}
        captured = []

        class ConfigState:
            raw_config = {}
            def reload(self, now):
                return (), False
            def coordinator_window(self, now):
                return type("Window", (), {"status": "ACTIVE"})()
            def target_views(self, now, coordinator_active):
                return ()

        def fake_acquire(*args, **kwargs):
            captured.append(kwargs.get("event_transition_registry"))
            return {}, ()

        with patch.object(coord, "acquire_active_targets", side_effect=fake_acquire), patch.object(
            coord,
            "build_snapshot",
            return_value=type("Snapshot", (), {})(),
        ), patch.object(coord, "diff_snapshots", return_value=()):
            coord.run_once(
                ConfigState(),
                None,
                event_transition_registry=registry,
            )
            coord.run_once(
                ConfigState(),
                None,
                event_transition_registry=registry,
            )

        self.assertEqual(captured, [registry, registry])
        self.assertIs(captured[0], registry)
        self.assertIs(captured[1], registry)


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

    def test_manual_record_menu_reuses_event_and_identity_colors(self):
        item=sony_candidate(title="Example Event")
        snap=snapshot([item])
        identity_key=next(
            identity
            for policy,identity in snap.blocks
            if policy==coord.POLICY_MANUAL
        )
        rendered=coord.render_manual_record_menu(
            snap,
            ((1,identity_key),),
            use_color=True,
        )
        self.assertIn(
            "\033[38;2;41;159;214mExample Event\033[0m",
            rendered,
        )
        self.assertIn(
            "\033[38;2;145;153;160m/hls/live/2120305/AG_Strea2309/ENG\033[0m",
            rendered,
        )

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

            def fake_launch(
                request,
                registry_store,
                *,
                config_path,
                registry_transition_callback=None,
            ):
                captured["request"] = request
                captured["store"] = registry_store
                captured["config_path"] = config_path
                captured["registry_transition_callback"] = (
                    registry_transition_callback
                )
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
                request.target_intents[0].recovery_playlist_urls,
                ("https://src1.test/list.m3u",),
            )
            self.assertEqual(request.base_name, "Sports - Asian Games - 2120305")
            self.assertEqual(plan.base_name, "Sports - Asian Games - 2120305")
            self.assertEqual(
                captured["config_path"],
                config_path,
            )


    def test_tied_targets_keep_separate_recovery_source_scopes(self):
        candidate = sony_candidate()
        target_a = target(
            name="Target A",
            source_groups=("SONYLIV_EVENTS",),
            primary=("Asian",),
        )
        target_b = target(
            name="Target B",
            source_groups=("SONY_TV",),
            primary=("Games",),
        )
        now = datetime(2026, 9, 24, 10, 0, 0)
        snap = coord.build_snapshot(
            (view(target_a, now=now), view(target_b, now=now)),
            {
                "Target A": (candidate,),
                "Target B": (candidate,),
            },
            now=now,
        )
        identity_key = next(
            identity
            for policy, identity in snap.blocks
            if policy == coord.POLICY_MANUAL
        )
        plan = coord.build_manual_launch_plan(
            snap,
            identity_key,
            now_ts=now.timestamp(),
        )

        intents, all_urls = coord._freeze_manual_recovery_scope(
            snap,
            plan,
            {
                "NM3U8DL_PLAYLIST_GROUPS": {
                    "COMMON": [],
                    "SONYLIV_EVENTS": ["https://events.test/list.m3u"],
                    "TV": ["https://tv.test/list.m3u"],
                },
            },
        )

        self.assertEqual(
            {
                intent.name: intent.recovery_playlist_urls
                for intent in intents
            },
            {
                "Target A": ("https://events.test/list.m3u",),
                "Target B": ("https://tv.test/list.m3u",),
            },
        )
        self.assertEqual(
            all_urls,
            (
                "https://events.test/list.m3u",
                "https://tv.test/list.m3u",
            ),
        )
        self.assertEqual(
            {
                intent.name: tuple(
                    (scope.source_group, scope.match_mode)
                    for scope in intent.recovery_scopes
                )
                for intent in intents
            },
            {
                "Target A": (("SONYLIV_EVENTS", "EVENT_PHRASE"),),
                "Target B": (("SONY_TV", "EXACT_CHANNEL"),),
            },
        )


class AllIdentitiesLaunchTests(unittest.TestCase):
    def _all_snapshot(self, candidates):
        return snapshot(
            candidates,
            policy=coord.POLICY_ALL,
            target_name="Auto",
        )

    def test_all_launch_handoff_uses_same_identity_worker_path(self):
        snap = self._all_snapshot([sony_candidate()])
        identity_key = next(
            identity
            for policy, identity in snap.blocks
            if policy == coord.POLICY_ALL
        )

        with tempfile.TemporaryDirectory() as td:
            paths = coord.build_recorder_output_paths(Path(td) / "Recordings")
            store = coord.IdentityRegistryStore(paths)
            status = store.prepare_session()
            captured = {}

            def fake_launch(
                request,
                registry_store,
                *,
                config_path,
                registry_transition_callback=None,
            ):
                captured["request"] = request
                captured["store"] = registry_store
                return type("Result", (), {"pid": 5432})()

            with patch.object(coord, "launch_identity_worker", fake_launch):
                plan, result = coord._launch_identity(
                    snap,
                    identity_key,
                    launch_policy=coord.POLICY_ALL,
                    registry_session_id=status.session_id,
                    registry_store=store,
                    config_path=Path(td) / "config.py",
                    raw_config={
                        "NM3U8DL_PLAYLIST_GROUPS": {
                            "COMMON": [],
                            "SONYLIV_EVENTS": ["https://src1.test/list.m3u"],
                        },
                    },
                )

        request = captured["request"]
        self.assertEqual(result.pid, 5432)
        self.assertEqual(request.identity_key, identity_key)
        self.assertEqual(request.selected_candidate, plan.selected_candidate)
        self.assertEqual(request.target_intents[0].name, "Auto")
        self.assertEqual(
            request.recovery_playlist_urls,
            ("https://src1.test/list.m3u",),
        )

    def test_all_launch_skips_claimed_identity_and_continues_after_failure(self):
        first = sony_candidate(lane="1/A/ENG", title="First")
        blocked = sony_candidate(lane="2/B/ENG", title="Blocked")
        third = sony_candidate(lane="3/C/ENG", title="Third")
        snap = self._all_snapshot([first, blocked, third])
        identity_by_title = {
            block.best_candidate.entry_title: identity_key
            for (policy, identity_key), block in snap.blocks.items()
            if policy == coord.POLICY_ALL and block.best_candidate is not None
        }

        with tempfile.TemporaryDirectory() as td:
            paths = coord.build_recorder_output_paths(Path(td) / "Recordings")
            store = coord.IdentityRegistryStore(paths)
            status = store.prepare_session()
            store.claim(
                identity_key=identity_by_title["Blocked"],
                provider="SONYLIV",
                display_name="Blocked",
                expected_session_id=status.session_id,
            )
            attempted = []

            def fake_launch_identity(
                snapshot_value,
                identity_key,
                **kwargs,
            ):
                attempted.append(identity_key)
                if identity_key == identity_by_title["First"]:
                    raise RuntimeError("first failed")
                plan = type("Plan", (), {
                    "base_name": "Third",
                    "identity": type("Identity", (), {"serialized": identity_key})(),
                })()
                result = type("Result", (), {"pid": 3333})()
                return plan, result

            with patch.object(
                coord,
                "_launch_identity",
                side_effect=fake_launch_identity,
            ):
                outcomes = coord._launch_all_identities(
                    snap,
                    registry_session_id=status.session_id,
                    registry_store=store,
                    config_path=Path(td) / "config.py",
                    raw_config={},
                    log_path=Path(td) / "coordinator.log",
                )

        self.assertNotIn(identity_by_title["Blocked"], attempted)
        self.assertEqual(
            set(attempted),
            {identity_by_title["First"], identity_by_title["Third"]},
        )
        outcome_by_identity = {
            identity: state
            for identity, state, _detail in outcomes
        }
        self.assertEqual(
            outcome_by_identity[identity_by_title["First"]],
            "FAILED",
        )
        self.assertEqual(
            outcome_by_identity[identity_by_title["Third"]],
            "LAUNCHED",
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
        failed_source_keys=set()
        with patch.object(coord_acquisition,"fetch_playlist_documents",return_value=({"https://good.test/list.m3u":playlist},("bad: OSError: boom",),{})), patch.object(coord_acquisition,"probe_candidates",side_effect=fake_probe):
            found,errors=coord.acquire_active_targets(
                self._raw(),
                (view(),),
                failed_source_keys=failed_source_keys,
            )
        self.assertEqual(len(found["T"]),1)
        self.assertEqual(errors,("bad: OSError: boom",))
        self.assertEqual(
            failed_source_keys,
            {("https://bad.test/list.m3u","SONYLIV","SONYLIV_EVENTS")},
        )

    def test_token_only_context_refresh_does_not_make_identity_disappear(self):
        raw={"NM3U8DL_PLAYLIST_GROUPS":{"COMMON":[],"SONYLIV_EVENTS":[
            {"url":"https://matching.test/list.m3u","name":"matching"},
            {"url":"https://context.test/list.m3u","name":"context"},
        ]}}
        matching=(
            '#EXTM3U\n'
            '#EXTINF:-1 tvg-name="Asian Games",Asian Games\n'
            'https://cdn.test/hls/live/2120305/AG_Strea2309/ENG/1080p.m3u8?token=one\n'
        )
        context_one=(
            '#EXTM3U\n'
            '#EXTINF:-1 tvg-name="Day 3 World Feed",Day 3 World Feed\n'
            'https://cdn.test/hls/live/2120305/AG_Strea2309/ENG/1080p.m3u8?token=one\n'
        )
        context_two=context_one.replace("token=one","token=two")
        registry={}
        documents=[
            (
                {
                    "https://matching.test/list.m3u":matching,
                    "https://context.test/list.m3u":context_one,
                },
                (),
                {},
            ),
            (
                {
                    "https://matching.test/list.m3u":matching,
                    "https://context.test/list.m3u":context_two,
                },
                (),
                {},
            ),
        ]
        with patch.object(
            coord_acquisition,
            "fetch_playlist_documents",
            side_effect=documents,
        ), patch.object(
            coord_acquisition,
            "_github_file_commit_timestamp",
            return_value=None,
            create=True,
        ), patch(
            "recorder_source.discovery._github_file_commit_timestamp",
            return_value=None,
        ), patch.object(
            coord_acquisition,
            "probe_candidates",
            side_effect=lambda items: tuple(items),
        ):
            first,_=coord.acquire_active_targets(
                raw,
                (view(),),
                source_freshness_registry=registry,
            )
            second,_=coord.acquire_active_targets(
                raw,
                (view(),),
                source_freshness_registry=registry,
            )

        self.assertEqual(len(first["T"]),2)
        self.assertEqual(len(second["T"]),2)
        self.assertEqual(sum(not item.ignored for item in second["T"]),1)

    def test_single_newer_nonmatching_source_requires_persistence_before_reject(self):
        raw={"NM3U8DL_PLAYLIST_GROUPS":{"COMMON":[],"SONYLIV_EVENTS":[
            {"url":"https://old.test/list.m3u","name":"old"},
            {"url":"https://new.test/list.m3u","name":"new"},
        ]}}
        matching='#EXTM3U\n#EXTINF:-1 tvg-name="Asian Games",Asian Games\nhttps://a.test/hls/live/2120305/AG_Strea2309/ENG/master.m3u8\n'
        moved='#EXTM3U\n#EXTINF:-1 tvg-name="Swimming",Swimming\nhttps://b.test/hls/live/2120305/AG_Strea2309/ENG/master.m3u8\n'
        transition_registry={}
        def freshness(url,*args,**kwargs):
            return {
                "timestamp":1000.0 if "old.test" in url else 2000.0,
                "source":"commit",
                "content_hash":url,
            }
        with patch.object(coord_acquisition,"fetch_playlist_documents",
            return_value=(
                {"https://old.test/list.m3u":matching,"https://new.test/list.m3u":moved},
                (),
                {},
            ),
        ), patch.object(coord_acquisition,"resolve_playlist_source_freshness",side_effect=freshness
        ), patch.object(coord_acquisition,"probe_candidates",side_effect=lambda items: tuple(items)
        ):
            first,_=coord.acquire_active_targets(
                raw,
                (view(),),
                event_transition_registry=transition_registry,
            )
            second,_=coord.acquire_active_targets(
                raw,
                (view(),),
                event_transition_registry=transition_registry,
            )
        self.assertEqual(len(first["T"]),2)
        self.assertEqual(second["T"],())

    def test_two_newer_agreeing_sources_confirm_event_move_immediately(self):
        raw={"NM3U8DL_PLAYLIST_GROUPS":{"COMMON":[],"SONYLIV_EVENTS":[
            {"url":"https://old.test/list.m3u","name":"old"},
            {"url":"https://new1.test/list.m3u","name":"new1"},
            {"url":"https://new2.test/list.m3u","name":"new2"},
        ]}}
        matching='#EXTM3U\n#EXTINF:-1 tvg-name="Asian Games",Asian Games\nhttps://a.test/hls/live/2120305/AG_Strea2309/ENG/master.m3u8\n'
        moved1='#EXTM3U\n#EXTINF:-1 tvg-name="Swimming",Swimming\nhttps://b.test/hls/live/2120305/AG_Strea2309/ENG/master.m3u8\n'
        moved2='#EXTM3U\n#EXTINF:-1 tvg-name="Swimming",Swimming\nhttps://c.test/hls/live/2120305/AG_Strea2309/ENG/master.m3u8\n'
        def freshness(url,*args,**kwargs):
            return {
                "timestamp":1000.0 if "old.test" in url else 2000.0,
                "source":"commit",
                "content_hash":url,
            }
        with patch.object(coord_acquisition,"fetch_playlist_documents",
            return_value=(
                {
                    "https://old.test/list.m3u":matching,
                    "https://new1.test/list.m3u":moved1,
                    "https://new2.test/list.m3u":moved2,
                },
                (),
                {},
            ),
        ), patch.object(coord_acquisition,"resolve_playlist_source_freshness",side_effect=freshness
        ), patch.object(coord_acquisition,"probe_candidates",side_effect=lambda items: tuple(items)
        ):
            found,_=coord.acquire_active_targets(raw,(view(),))
        self.assertEqual(found["T"],())

    def test_fresh_matching_evidence_cancels_confirmed_event_move(self):
        raw={"NM3U8DL_PLAYLIST_GROUPS":{"COMMON":[],"SONYLIV_EVENTS":[
            {"url":"https://old.test/list.m3u","name":"old"},
            {"url":"https://new.test/list.m3u","name":"new"},
        ]}}
        matching='#EXTM3U\n#EXTINF:-1 tvg-name="Asian Games",Asian Games\nhttps://a.test/hls/live/2120305/AG_Strea2309/ENG/master.m3u8\n'
        moved='#EXTM3U\n#EXTINF:-1 tvg-name="Swimming",Swimming\nhttps://b.test/hls/live/2120305/AG_Strea2309/ENG/master.m3u8\n'
        transition_registry={}
        phase={"matching_ts":1000.0}
        def freshness(url,*args,**kwargs):
            return {
                "timestamp":phase["matching_ts"] if "old.test" in url else 2000.0,
                "source":"commit",
                "content_hash":url,
            }
        with patch.object(coord_acquisition,"fetch_playlist_documents",
            return_value=(
                {"https://old.test/list.m3u":matching,"https://new.test/list.m3u":moved},
                (),
                {},
            ),
        ), patch.object(coord_acquisition,"resolve_playlist_source_freshness",side_effect=freshness
        ), patch.object(coord_acquisition,"probe_candidates",side_effect=lambda items: tuple(items)
        ):
            first,_=coord.acquire_active_targets(
                raw,(view(),),event_transition_registry=transition_registry
            )
            second,_=coord.acquire_active_targets(
                raw,(view(),),event_transition_registry=transition_registry
            )
            phase["matching_ts"]=3000.0
            third,_=coord.acquire_active_targets(
                raw,(view(),),event_transition_registry=transition_registry
            )

        self.assertEqual(len(first["T"]),2)
        self.assertEqual(second["T"],())
        self.assertEqual(len(third["T"]),2)
        self.assertEqual(transition_registry,{})

    def test_newer_shorter_metadata_does_not_reject_matching_identity(self):
        raw={"NM3U8DL_PLAYLIST_GROUPS":{"COMMON":[],"SONYLIV_EVENTS":[
            {"url":"https://old.test/list.m3u","name":"old"},
            {"url":"https://new.test/list.m3u","name":"new"},
        ]}}
        matching=(
            '#EXTM3U\n'
            '#EXTINF:-1 tvg-name="Türkiye vs France - 26 Sep 2026 [ENG] - '
            'UEFA Nations League 2026-27",Türkiye vs France - 26 Sep 2026 '
            '[ENG] - UEFA Nations League 2026-27\n'
            'https://a.test/hls/live/2120299/Footlive2509/ENG/master.m3u8\n'
        )
        shorter=(
            '#EXTM3U\n'
            '#EXTINF:-1 tvg-name="Türkiye vs France - 26 Sep 2026 [ENG]",'
            'Türkiye vs France - 26 Sep 2026 [ENG]\n'
            'https://b.test/hls/live/2120299/Footlive2509/ENG/master.m3u8\n'
        )
        def freshness(url,*args,**kwargs):
            return {
                "timestamp":1000.0 if "old.test" in url else 2000.0,
                "source":"commit",
                "content_hash":url,
            }
        with patch.object(coord_acquisition,"fetch_playlist_documents",
            return_value=(
                {"https://old.test/list.m3u":matching,"https://new.test/list.m3u":shorter},
                (),
                {},
            ),
        ), patch.object(coord_acquisition,"resolve_playlist_source_freshness",side_effect=freshness
        ), patch.object(coord_acquisition,"probe_candidates",side_effect=lambda items: tuple(items)
        ):
            found,_=coord.acquire_active_targets(
                raw,
                (view(target(primary=("UEFA Nations League",))),),
            )
        self.assertEqual(len(found["T"]),2)
        self.assertEqual(sum(not item.ignored for item in found["T"]),1)
        context=next(item for item in found["T"] if item.ignored)
        self.assertFalse(
            context.extra["freshness_disqualifying_conflict"]
        )
        self.assertIn("less specific",context.reason)

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
        with patch.object(coord_acquisition,"fetch_playlist_documents",
            return_value=(
                {"https://old.test/list.m3u":old,"https://new.test/list.m3u":matching},
                (),
                {},
            ),
        ), patch.object(coord_acquisition,"resolve_playlist_source_freshness",side_effect=freshness
        ), patch.object(coord_acquisition,"probe_candidates",side_effect=lambda items: tuple(items)
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
        with patch.object(coord_acquisition,"fetch_playlist_documents",
            return_value=(
                {"https://one.test/list.m3u":matching,"https://two.test/list.m3u":moved},
                (),
                {},
            ),
        ), patch.object(coord_acquisition,"resolve_playlist_source_freshness",
            return_value={"timestamp":2000.0,"source":"commit","content_hash":"x"},
        ), patch.object(coord_acquisition,"probe_candidates",side_effect=lambda items: tuple(items)
        ):
            found,_=coord.acquire_active_targets(raw,(view(),))
        self.assertEqual(len(found["T"]),2)

    def test_metadata_only_matching_entry_remains_visible_unusable(self):
        raw={"NM3U8DL_PLAYLIST_GROUPS":{"COMMON":[],"SONYLIV_EVENTS":[{"url":"https://good.test/list.m3u","name":"good"}]}}
        playlist='#EXTM3U\n#EXTINF:-1 tvg-name="Asian Games" group-title="Sports",Asian Games\n'
        with patch.object(coord_acquisition,"fetch_playlist_documents",return_value=({"https://good.test/list.m3u":playlist},(),{})):
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
        with patch.object(coord_acquisition,"fetch_playlist_documents",return_value=({"https://one.test/list.m3u":one,"https://two.test/list.m3u":two},(),{})), patch.object(coord_acquisition,"probe_candidates",side_effect=fake_probe):
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

    def test_failed_source_retains_identity_as_unavailable_not_recordable(self):
        prior_candidate=sony_candidate(
            playlist="https://bad.test/list.m3u",
            source_name="bad",
        )
        old=snapshot([prior_candidate])
        retained=coord._retain_failed_source_observations(
            old,
            (view(),),
            {"T":()},
            {("https://bad.test/list.m3u","SONYLIV","SONYLIV_EVENTS")},
        )
        self.assertEqual(len(retained["T"]),1)
        carried=retained["T"][0]
        self.assertTrue(carried.ignored)
        self.assertFalse(carried.launchable)
        self.assertEqual(carried.probe_status,"source_unavailable")

        new=coord.build_snapshot(
            (view(),),
            retained,
            source_errors=("bad: HTTPError: HTTP Error 502: Bad Gateway",),
            now=datetime(2026,9,24,10,5,0),
        )
        self.assertEqual(len(new.blocks),1)
        self.assertIsNone(next(iter(new.blocks.values())).best_candidate)
        events=coord.diff_snapshots(old,new)
        self.assertFalse(any(event.marker=="REMOVED" for event in events))
        self.assertTrue(any(
            event.marker=="UPDATE"
            and "SOURCE_UNAVAILABLE" in " ".join(event.details)
            for event in events
        ))

    def test_successful_source_scan_can_remove_previously_retained_identity(self):
        prior_candidate=sony_candidate(
            playlist="https://bad.test/list.m3u",
            source_name="bad",
        )
        old=snapshot([prior_candidate])
        retained=coord._retain_failed_source_observations(
            old,
            (view(),),
            {"T":()},
            {("https://bad.test/list.m3u","SONYLIV","SONYLIV_EVENTS")},
        )
        retained_snapshot=coord.build_snapshot(
            (view(),),
            retained,
            now=datetime(2026,9,24,10,5,0),
        )
        recovered=coord._retain_failed_source_observations(
            retained_snapshot,
            (view(),),
            {"T":()},
            set(),
        )
        recovered_snapshot=coord.build_snapshot(
            (view(),),
            recovered,
            now=datetime(2026,9,24,10,10,0),
        )
        events=coord.diff_snapshots(retained_snapshot,recovered_snapshot)
        self.assertTrue(any(event.marker=="REMOVED" for event in events))

    def test_changed_target_definition_does_not_retain_failed_source_rows(self):
        prior_candidate=sony_candidate(
            playlist="https://bad.test/list.m3u",
            source_name="bad",
        )
        old=snapshot([prior_candidate])
        changed_view=view(target(primary=("Different Event",)))
        retained=coord._retain_failed_source_observations(
            old,
            (changed_view,),
            {"T":()},
            {("https://bad.test/list.m3u","SONYLIV","SONYLIV_EVENTS")},
        )
        self.assertEqual(retained["T"],())

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

    def test_change_log_collapses_identical_update_summaries_only(self):
        key=(coord.POLICY_MANUAL,"SONYLIV|feed")
        duplicate_a=coord.ChangeEvent(
            "UPDATE",
            key,
            ("Candidate states PROBE_FAILED -> WORKING",),
            beep=True,
            source_id="https://one.test/list.m3u",
        )
        duplicate_b=coord.ChangeEvent(
            "UPDATE",
            key,
            ("Candidate states PROBE_FAILED -> WORKING",),
            beep=True,
            source_id="https://two.test/list.m3u",
        )
        distinct=coord.ChangeEvent(
            "UPDATE",
            key,
            ("Event Old -> New",),
            beep=True,
            source_id="https://three.test/list.m3u",
        )
        source_add=coord.ChangeEvent(
            "SOURCE+",
            key,
            ("source added: four",),
            source_id="https://four.test/list.m3u",
        )
        logged=coord._change_events_for_log(
            (duplicate_a,duplicate_b,distinct,source_add)
        )
        self.assertEqual(logged,(duplicate_a,distinct,source_add))

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
        self.assertIn("Identity: /hls/live/2120305/AG_Strea2309/ENG",rendered)
        self.assertIn("| [S1] | expires unknown |", rendered)
        self.assertNotIn("src1 [S1]", rendered)
        self.assertIn("SOURCE REFERENCES", rendered)
        self.assertIn("[S1] https://src1.test/list.m3u", rendered)

    def test_dashboard_does_not_call_working_unknown_quality_no_working_candidate(self):
        item = replace(
            sony_candidate(),
            quality_known=False,
            video_width=0,
            video_height=0,
            video_fps=0.0,
            video_bitrate_bps=0,
            video_scan_type="",
            extra={
                "provider": "FANCODE",
                "source_name": "fancode",
                "source_group": "FANCODE",
            },
        )
        rendered = coord.render_dashboard(snapshot([item]), ())
        self.assertIn("AVAILABLE FANCODE", rendered)
        self.assertIn("Quality : unknown", rendered)
        self.assertIn("[ON] Asian Games", rendered)
        self.assertNotIn("Quality : no working candidate", rendered)

    def test_dashboard_overlays_recording_state_and_rich_recordings_summary(self):
        item=replace(
            sony_candidate(),
            video_resolution_source="manifest",
            video_fps_source="manifest",
            video_scan_type_source="event-policy",
            video_bitrate_source="manifest",
        )
        snap=snapshot([item])
        identity_key=next(
            identity
            for policy,identity in snap.blocks
            if policy==coord.POLICY_MANUAL
        )
        runtime_row=runtime_candidate(
            item,
            selected=True,
            status="SELECTED",
            quality="1920x1080 | 50p [manifest, event-policy] | 5000 Kbps",
        )
        registry_entries={
            identity_key:{
                "identity":identity_key,
                "provider":"SONYLIV",
                "display_name":"ENG _ Asian Games",
                "state":"RECORDING",
                "worker_pid":4321,
            }
        }
        recording_started_at=datetime(2026,9,25,20,53,4).timestamp()
        runtime_statuses={
            identity_key:{
                "worker_state":"RECORDING",
                "current_candidate":runtime_row,
                "candidates":[runtime_row],
                "target_names":["T"],
                "source_count":1,
                "recording_started_at":recording_started_at,
            }
        }
        rendered=coord.render_dashboard(
            snap,
            (),
            registry_entries=registry_entries,
            runtime_statuses=runtime_statuses,
        )
        identity_line=next(
            line for line in rendered.splitlines()
            if line.startswith("[1]")
        )
        self.assertIn("[1] RECORDING SONYLIV",identity_line)
        self.assertIn("RECORDINGS",rendered)
        recording_line=next(
            line for line in rendered.splitlines()
            if "[RECORDING]" in line
            and "Asian Games | Asian Games | Sports" in line
        )
        identity_detail_line=next(
            line for line in rendered.splitlines()
            if "Identity: /hls/live/2120305/AG_Strea2309/ENG" in line
            and "PID 4321" in line
        )
        self.assertIn("Asian Games | Asian Games | Sports | SONYLIV | [S1]",recording_line)
        self.assertIn(
            "1920x1080 | 50p [manifest, event-policy] | 5000 Kbps",
            recording_line,
        )
        self.assertIn(
            "Targets: T | Sources: 1 | Started: 2026-09-25 20:53:04 | PID 4321",
            identity_detail_line,
        )
        self.assertEqual(
            recording_line.index("Asian Games"),
            identity_detail_line.index("Identity:"),
        )
        combined_recording = recording_line + identity_detail_line
        self.assertNotIn("expires",combined_recording)
        self.assertNotIn("Last Updated",combined_recording)
        self.assertNotIn("Source Updated",combined_recording)

        colored=coord.render_dashboard(
            snap,
            (),
            use_color=True,
            registry_entries=registry_entries,
            runtime_statuses=runtime_statuses,
        )
        self.assertIn(
            "\033[38;2;41;159;214mRECORDINGS\033[0m",
            colored,
        )
        self.assertIn(
            "\033[38;2;255;135;3m[RECORDING]\033[0m",
            colored,
        )
        self.assertIn(
            "\033[38;2;255;135;3mRECORDING\033[0m",
            colored,
        )

    def test_recording_marker_survives_refreshed_discovery_row(self):
        original=replace(
            sony_candidate(),
            video_resolution_source="manifest",
            video_fps_source="manifest",
            video_scan_type_source="event-policy",
            video_bitrate_source="manifest",
        )
        refreshed=replace(
            original,
            matching_entry_index=99,
            group_title="Athletics",
        )
        snap=snapshot([refreshed])
        identity_key=next(
            identity
            for policy,identity in snap.blocks
            if policy==coord.POLICY_MANUAL
        )
        runtime_row=runtime_candidate(
            original,
            selected=True,
            status="SELECTED",
            quality="1920x1080 | 50p [manifest, event-policy] | 5000 Kbps",
        )
        rendered=coord.render_dashboard(
            snap,
            (),
            registry_entries={
                identity_key:{
                    "identity":identity_key,
                    "provider":"SONYLIV",
                    "display_name":"ENG _ Asian Games",
                    "state":"RECORDING",
                    "worker_pid":4321,
                }
            },
            runtime_statuses={
                identity_key:{
                    "worker_state":"RECORDING",
                    "current_candidate":runtime_row,
                    "candidates":[runtime_row],
                    "target_names":["T"],
                    "source_count":1,
                }
            },
        )
        refreshed_line=next(
            line for line in rendered.splitlines()
            if "Asian Games | Asian Games | Athletics" in line
        )
        self.assertIn("[RECORDING] [ON]",refreshed_line)

    def test_dashboard_uses_shared_color_mapping_for_waiting_state(self):
        snap=snapshot([sony_candidate()])
        identity_key=next(
            identity
            for policy,identity in snap.blocks
            if policy==coord.POLICY_MANUAL
        )
        colored=coord.render_dashboard(
            snap,
            (),
            use_color=True,
            registry_entries={
                identity_key:{
                    "identity":identity_key,
                    "provider":"SONYLIV",
                    "display_name":"ENG _ Asian Games",
                    "state":"WAITING_FOR_SOURCE",
                    "worker_pid":4321,
                }
            },
        )
        self.assertIn(
            "\033[38;2;255;135;3m[WAITING_FOR_SOURCE]\033[0m",
            colored,
        )
        self.assertIn(
            "\033[38;2;255;135;3mWAITING_FOR_SOURCE\033[0m",
            colored,
        )

    def test_source_reference_numbers_remain_stable_across_refreshes(self):
        first=snapshot([sony_candidate(
            playlist="https://src1.test/list.m3u",
            source_name="src1",
        )])
        refs=coordinator_terminal.update_source_reference_registry({},first)
        second=snapshot([
            sony_candidate(
                playlist="https://src2.test/list.m3u",
                source_name="src2",
                lane="2120306/AG_Strea2309/ENG",
            ),
            sony_candidate(
                playlist="https://src1.test/list.m3u",
                source_name="src1",
            ),
        ])
        refs=coordinator_terminal.update_source_reference_registry(refs,second)
        self.assertEqual(refs["https://src1.test/list.m3u"],1)
        self.assertEqual(refs["https://src2.test/list.m3u"],2)

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
        self.assertIn("[1] [MANUALLY_STOPPED] SONYLIV",identity_line)
        self.assertIn("suppressed for this registry/session",identity_line)
        self.assertNotIn("RECORDINGS",rendered)

    def test_terminal_registry_states_and_suppression_use_warning_style(self):
        snap=snapshot([sony_candidate()])
        identity_key=next(
            identity
            for policy,identity in snap.blocks
            if policy==coord.POLICY_MANUAL
        )
        for terminal_state in ("ENDED","MANUALLY_STOPPED","CRASHED"):
            with self.subTest(state=terminal_state):
                rendered=coord.render_dashboard(
                    snap,
                    (),
                    registry_entries={
                        identity_key:{
                            "identity":identity_key,
                            "provider":"SONYLIV",
                            "display_name":"ENG _ Asian Games",
                            "state":terminal_state,
                            "worker_pid":4321,
                        }
                    },
                    use_color=True,
                )
                self.assertIn(
                    f"\033[1;93m[{terminal_state}]\033[0m",
                    rendered,
                )
                self.assertIn(
                    "\033[1;93m | suppressed for this registry/session\033[0m",
                    rendered,
                )

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

    def test_dashboard_merges_master_and_direct_same_rendition_evidence(self):
        identity_path="/mumbai/4249106_english_hls_b86f41b4c015704_1ta-di_h264"
        direct_url=f"https://direct.test{identity_path}/1080p.m3u8?token=one"
        master_url=f"https://proxy.test{identity_path}/index.m3u8?token=two"
        master_child=f"https://proxy.test{identity_path}/1080p.m3u8?token=two"
        lower_url=f"https://direct.test{identity_path}/720p.m3u8?token=three"

        direct=replace(
            sony_candidate(fps=25),
            playlist_url="https://source1.test/list.m3u",
            raw_stream_url=direct_url,
            stream_url=direct_url,
            final_stream_url=direct_url,
            video_bitrate_bps=3_169_000,
            video_bitrate_source="sample",
            extra={
                "provider":"FANCODE",
                "source_name":"direct",
                "source_group":"FANCODE",
            },
        )
        master=replace(
            sony_candidate(
                playlist="https://source2.test/list.m3u",
                source_name="master",
                title="Day 3 World Feed",
                tvg="Day 3 World Feed",
                fps=25,
                ignored=True,
            ),
            raw_stream_url=master_url,
            stream_url=master_url,
            final_stream_url=master_url,
            video_bitrate_bps=3_322_000,
            video_bitrate_source="manifest",
            extra={
                "provider":"FANCODE",
                "source_name":"master",
                "source_group":"FANCODE",
                "manifest_variant_url":master_child,
            },
        )
        lower=replace(
            sony_candidate(
                playlist="https://source3.test/list.m3u",
                source_name="lower",
                title="Day 3 World Feed",
                tvg="Day 3 World Feed",
                fps=25,
                width=1280,
                height=720,
                ignored=True,
            ),
            raw_stream_url=lower_url,
            stream_url=lower_url,
            final_stream_url=lower_url,
            video_bitrate_bps=1_847_000,
            video_bitrate_source="sample",
            extra={
                "provider":"FANCODE",
                "source_name":"lower",
                "source_group":"FANCODE",
            },
        )

        rendered=coord.render_dashboard(snapshot([direct,master,lower]),())

        self.assertEqual(rendered.count("Quality : "),2)
        self.assertIn(
            "Quality : 1920x1080 | 25p | 3322 Kbps [BEST]",
            rendered,
        )
        self.assertNotIn(
            "Quality : 1920x1080 | 25p | ~3169 Kbps",
            rendered,
        )
        self.assertIn(
            "Quality : 1280x720 | 25p | ~1847 Kbps [FFmpeg sample]",
            rendered,
        )

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
        self.assertIn("\033[38;2;255;215;0m[NEW]\033[0m",rendered)

    def test_launching_and_recording_share_user_facing_state_color(self):
        launching=coordinator_terminal._runtime_state_text(
            "LAUNCHING",
            True,
            bracketed=True,
        )
        recording=coordinator_terminal._runtime_state_text(
            "RECORDING",
            True,
            bracketed=True,
        )
        self.assertEqual(
            launching.replace("[LAUNCHING]","[STATE]"),
            recording.replace("[RECORDING]","[STATE]"),
        )
        self.assertIn("\033[38;2;255;135;3m[LAUNCHING]\033[0m",launching)
        self.assertIn("\033[38;2;255;135;3m[RECORDING]\033[0m",recording)

    def test_normal_on_is_uncolored_and_off_is_bright_red(self):
        on_rendered=coord.render_dashboard(
            snapshot([sony_candidate()]),
            (),
            use_color=True,
        )
        self.assertIn("[ON]",on_rendered)
        self.assertNotIn("\033[1;92m[ON]\033[0m",on_rendered)

        expired=replace(
            sony_candidate(launchable=False,status="expired"),
            expiry=datetime(2026,9,24,9,30,0).timestamp(),
            expiry_source="manifest",
        )
        off_rendered=coord.render_dashboard(
            snapshot([expired]),
            (),
            use_color=True,
        )
        self.assertIn("\033[1;91m[OFF]\033[0m",off_rendered)

    def test_transient_update_marker_uses_change_gold(self):
        old=snapshot([sony_candidate(title="Shooting")])
        new=snapshot([sony_candidate(title="Athletics")])
        events=coord.diff_snapshots(old,new)
        rendered=coord.render_dashboard(new,events,use_color=True)
        self.assertIn("\033[38;2;255;215;0m[UPDATE]\033[0m",rendered)

    def test_source_reference_and_footer_share_muted_treatment_without_row_source_name(self):
        snap=snapshot([sony_candidate()])
        rendered=coord.render_dashboard(snap,(),use_color=True)
        muted="\033[38;2;118;118;118m"
        reset="\033[0m"
        self.assertIn(f"{muted}[S1]{reset}",rendered)
        self.assertIn("\033[1;93mexpires unknown\033[0m",rendered)
        self.assertNotIn(f"{muted}src1{reset}",rendered)
        self.assertIn(
            f"{muted}  [S1] https://src1.test/list.m3u{reset}",
            rendered,
        )
        self.assertIn(
            f"{muted}SOURCE REFERENCES{reset}",
            rendered,
        )

    def test_update_delta_dims_old_value_and_highlights_arrow_and_new_value(self):
        old=snapshot([sony_candidate(title="Shooting")])
        new=snapshot([sony_candidate(title="Athletics")])
        events=coord.diff_snapshots(old,new)
        rendered=coord.render_dashboard(new,events,use_color=True)
        self.assertIn(
            "\033[38;2;176;176;176mEvent Shooting\033[0m"
            "\033[38;2;255;215;0m -> Athletics\033[0m",
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

    def test_plain_watch_does_not_show_selection_decisions(self):
        selected=sony_candidate(
            playlist="https://selected.test/list",
            source_name="selected-source",
            title="Selected Event",
            fps=50.0,
        )
        lower=sony_candidate(
            playlist="https://lower.test/list",
            source_name="lower-source",
            title="Lower Event",
            fps=25.0,
        )
        rendered=coord.render_dashboard(snapshot([lower,selected]),())
        self.assertNotIn("[SELECTED]",rendered)
        self.assertNotIn("[RECORDING]",rendered)
        self.assertNotIn("not selected:",rendered)

    def test_runtime_selected_source_and_working_loser_use_worker_decision(self):
        selected=sony_candidate(
            playlist="https://selected.test/list",
            source_name="selected-source",
            title="Selected Event",
            fps=50.0,
        )
        lower=sony_candidate(
            playlist="https://lower.test/list",
            source_name="lower-source",
            title="Lower Event",
            fps=25.0,
        )
        snap=snapshot([lower,selected])
        identity_key=next(
            identity for policy,identity in snap.blocks
            if policy==coord.POLICY_MANUAL
        )
        selected_row=runtime_candidate(
            selected,
            selected=True,
            status="SELECTED",
        )
        selected_row["stream_url"] = "https://rotated-token.test/master.m3u8"
        lower_row=runtime_candidate(
            lower,
            reason="not selected: lower quality",
        )
        rendered=coord.render_dashboard(
            snap,
            (),
            use_color=True,
            registry_entries={
                identity_key:{
                    "state":"LAUNCHING",
                    "provider":"SONYLIV",
                    "worker_pid":4321,
                }
            },
            runtime_statuses={
                identity_key:{
                    "worker_state":"SELECTED",
                    "current_candidate":selected_row,
                    "candidates":[selected_row,lower_row],
                    "target_names":["T"],
                    "source_count":2,
                }
            },
        )
        selected_line=next(
            line for line in rendered.splitlines()
            if "Selected Event" in line
        )
        lower_line=next(
            line for line in rendered.splitlines()
            if "Lower Event" in line
        )
        self.assertIn("\033[1;97;42m[SELECTED]\033[0m",selected_line)
        self.assertIn("[ON]",selected_line)
        self.assertNotIn("\033[1;92m[ON]\033[0m",selected_line)
        self.assertLess(selected_line.index("[SELECTED]"),selected_line.index("[ON]"))
        self.assertIn("not selected: lower quality",lower_line)

    def test_same_feed_context_reason_is_not_user_facing(self):
        context=sony_candidate(
            playlist="https://context.test/list",
            source_name="context-source",
            title="Men's Marathon - Athletics - 26 Sep 2026 [ENG]",
            tvg="",
            ignored=True,
            reason=(
                "same feed identity context; source metadata is compatible "
                "but less specific than a matching observation"
            ),
        )
        snap=snapshot([context])
        identity_key=next(
            identity
            for policy,identity in snap.blocks
            if policy==coord.POLICY_MANUAL
        )
        runtime_row=runtime_candidate(
            context,
            status="IGNORED",
            reason=(
                "HLS (example.test) — same feed identity context; "
                "source metadata is compatible but less specific than a matching observation"
            ),
            classification="IGNORED",
        )
        rendered=coord.render_dashboard(
            snap,
            (),
            registry_entries={
                identity_key:{
                    "state":"RECORDING",
                    "provider":"SONYLIV",
                    "worker_pid":4321,
                }
            },
            runtime_statuses={
                identity_key:{
                    "worker_state":"RECORDING",
                    "current_candidate":None,
                    "candidates":[runtime_row],
                    "target_names":["T"],
                    "source_count":1,
                }
            },
        )
        self.assertIn("[ON] Men's Marathon",rendered)
        self.assertNotIn("same feed identity context",rendered)

    def test_unavailable_expiry_classification_is_front_loaded(self):
        expiry=datetime(2026,9,24,9,30,0).timestamp()
        item=replace(
            sony_candidate(launchable=False,status="expired"),
            expiry=expiry,
            expiry_source="manifest + URL/header",
        )
        rendered=coord.render_dashboard(snapshot([item]),())
        row=next(line for line in rendered.splitlines() if "[OFF]" in line)
        self.assertIn("[OFF] EXPIRED — Asian Games",row)
        self.assertIn(
            "expired 2026-09-24 09:30:00 [manifest + URL/header]",
            row,
        )

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
        a_id=next(x for x in ids if "/hls/live/1/A/ENG" in x)
        b_id=next(x for x in ids if "/hls/live/2/B/ENG" in x)
        self.assertEqual(order2[coord.POLICY_MANUAL],[b_id,a_id])

    def test_display_order_ranks_identities_by_unusable_observation_count(self):
        clean=[
            sony_candidate(
                lane="1/Clean/ENG",
                playlist="https://clean-1/list",
                source_name="clean-1",
            ),
            sony_candidate(
                lane="1/Clean/ENG",
                playlist="https://clean-2/list",
                source_name="clean-2",
            ),
        ]
        partial_one=[
            sony_candidate(
                lane="2/PartialOne/ENG",
                playlist="https://partial-one-on/list",
                source_name="partial-one-on",
            ),
            sony_candidate(
                lane="2/PartialOne/ENG",
                playlist="https://partial-one-off/list",
                source_name="partial-one-off",
                launchable=False,
                status="hls_variant_unavailable",
            ),
        ]
        partial_two=[
            sony_candidate(
                lane="3/PartialTwo/ENG",
                playlist="https://partial-two-on/list",
                source_name="partial-two-on",
            ),
            sony_candidate(
                lane="3/PartialTwo/ENG",
                playlist="https://partial-two-off-1/list",
                source_name="partial-two-off-1",
                launchable=False,
                status="hls_variant_unavailable",
            ),
            sony_candidate(
                lane="3/PartialTwo/ENG",
                playlist="https://partial-two-off-2/list",
                source_name="partial-two-off-2",
                launchable=False,
                status="hls_variant_unavailable",
            ),
        ]
        all_off=[
            sony_candidate(
                lane="4/AllOff/ENG",
                playlist="https://all-off-1/list",
                source_name="all-off-1",
                launchable=False,
                status="hls_variant_unavailable",
            ),
            sony_candidate(
                lane="4/AllOff/ENG",
                playlist="https://all-off-2/list",
                source_name="all-off-2",
                launchable=False,
                status="hls_variant_unavailable",
            ),
        ]
        snap=snapshot(all_off+partial_two+partial_one+clean)
        order=coord.update_display_order(
            {coord.POLICY_MANUAL:[],coord.POLICY_ALL:[]},
            snap,
        )

        self.assertEqual(
            order[coord.POLICY_MANUAL],
            [
                next(
                    identity
                    for identity in order[coord.POLICY_MANUAL]
                    if "/hls/live/1/Clean/ENG" in identity
                ),
                next(
                    identity
                    for identity in order[coord.POLICY_MANUAL]
                    if "/hls/live/2/PartialOne/ENG" in identity
                ),
                next(
                    identity
                    for identity in order[coord.POLICY_MANUAL]
                    if "/hls/live/3/PartialTwo/ENG" in identity
                ),
                next(
                    identity
                    for identity in order[coord.POLICY_MANUAL]
                    if "/hls/live/4/AllOff/ENG" in identity
                ),
            ],
        )


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
        kbhit_calls=[]

        class FakeMsvcrt:
            @staticmethod
            def kbhit():
                kbhit_calls.append(True)
                return len(kbhit_calls)==1
            @staticmethod
            def getwch():
                value=next(keys)
                if value=="\x3f":
                    stop.set()
                return value

        with patch.object(coord.os,"name","nt"), patch.object(coord,"msvcrt",FakeMsvcrt):
            coord._command_reader(q,stop)
        self.assertEqual(q.get_nowait(),"__F5__")
        self.assertEqual(len(kbhit_calls),1)


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
