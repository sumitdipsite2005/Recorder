from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import tempfile
import unittest

from recorder_runtime.registry import (
    IdentityLaunchBlocked,
    IdentityRegistryStore,
    InvalidRegistryTransition,
    MAX_ACTIVE_IDENTITY_WORKERS,
    RegistryError,
    STATE_RECORDING,
    STATE_CRASHED,
    STATE_LAUNCHING,
    STATE_RECORDING,
)
from recorder_runtime.paths import build_recorder_output_paths


FIXED_TIME = datetime(2026, 9, 25, 12, 0, 0, tzinfo=timezone.utc)


class IdentityRegistryTests(unittest.TestCase):
    def make_store(self, td: str) -> IdentityRegistryStore:
        root = Path(td) / "Manual Recordings"
        paths = build_recorder_output_paths(root)
        return IdentityRegistryStore(paths)

    def test_first_session_creates_current_inside_registry_folder(self):
        with tempfile.TemporaryDirectory() as td:
            store = self.make_store(td)

            status = store.prepare_session(now=FIXED_TIME)

            self.assertEqual(status.action, "CREATED")
            self.assertTrue(store.paths.current.exists())
            self.assertTrue(store.paths.archive.is_dir())
            self.assertEqual(
                store.paths.current.parent.name,
                "identity_registry",
            )
            self.assertEqual(
                store.paths.archive.parent,
                store.paths.current.parent,
            )

    def test_terminal_only_registry_rolls_to_archive_on_next_start(self):
        with tempfile.TemporaryDirectory() as td:
            store = self.make_store(td)
            first = store.prepare_session(now=FIXED_TIME)
            store.claim(
                identity_key="sony|lane-a|english",
                provider="SONY",
                display_name="Lane A",
                now=FIXED_TIME,
            )
            store.transition(
                identity_key="sony|lane-a|english",
                new_state=STATE_CRASHED,
                reason="test terminal state",
                now=FIXED_TIME,
            )

            second = store.prepare_session(now=FIXED_TIME)

            self.assertEqual(second.action, "ROLLED_OVER")
            self.assertNotEqual(second.session_id, first.session_id)
            archived = list(store.paths.archive.glob("identity_registry_*.json"))
            self.assertEqual(len(archived), 1)
            self.assertTrue(store.paths.current.exists())

    def test_live_owned_worker_keeps_existing_session(self):
        with tempfile.TemporaryDirectory() as td:
            store = self.make_store(td)
            first = store.prepare_session(now=FIXED_TIME)
            store.claim(
                identity_key="sony|lane-a|english",
                provider="SONY",
                display_name="Lane A",
                now=FIXED_TIME,
            )
            store.transition(
                identity_key="sony|lane-a|english",
                new_state=STATE_RECORDING,
                worker_pid=12345,
                now=FIXED_TIME,
            )

            second = store.prepare_session(
                worker_liveness=lambda pid: pid == 12345,
                now=FIXED_TIME,
            )

            self.assertEqual(second.action, "CONTINUED")
            self.assertEqual(second.session_id, first.session_id)
            self.assertEqual(list(store.paths.archive.iterdir()), [])

    def test_unresolved_launch_claim_keeps_existing_session(self):
        with tempfile.TemporaryDirectory() as td:
            store = self.make_store(td)
            first = store.prepare_session(now=FIXED_TIME)
            store.claim(
                identity_key="sony|lane-a|english",
                provider="SONY",
                display_name="Lane A",
                now=FIXED_TIME,
            )

            second = store.prepare_session(now=FIXED_TIME)

            self.assertEqual(second.action, "CONTINUED")
            self.assertEqual(second.session_id, first.session_id)
            self.assertEqual(
                second.unresolved_identities,
                ("sony|lane-a|english",),
            )
            self.assertEqual(list(store.paths.archive.iterdir()), [])

    def test_definitively_dead_owned_worker_blocks_session_rollover(self):
        with tempfile.TemporaryDirectory() as td:
            store = self.make_store(td)
            first = store.prepare_session(now=FIXED_TIME)
            store.claim(
                identity_key="sony|lane-a|english",
                provider="SONY",
                display_name="Lane A",
                now=FIXED_TIME,
            )
            store.transition(
                identity_key="sony|lane-a|english",
                new_state=STATE_RECORDING,
                worker_pid=12345,
                now=FIXED_TIME,
            )

            second = store.prepare_session(
                worker_liveness=lambda _pid: False,
                now=FIXED_TIME,
            )

            self.assertEqual(second.action, "CONTINUED")
            self.assertEqual(second.session_id, first.session_id)
            self.assertEqual(
                second.unresolved_identities,
                ("sony|lane-a|english",),
            )
            self.assertEqual(list(store.paths.archive.iterdir()), [])

    def test_claim_is_atomic_duplicate_prevention_boundary(self):
        with tempfile.TemporaryDirectory() as td:
            store = self.make_store(td)
            store.prepare_session(now=FIXED_TIME)
            store.claim(
                identity_key="sony|lane-a|english",
                provider="SONY",
                display_name="Lane A",
                now=FIXED_TIME,
            )

            with self.assertRaises(IdentityLaunchBlocked):
                store.claim(
                    identity_key="sony|lane-a|english",
                    provider="SONY",
                    display_name="Lane A duplicate",
                    now=FIXED_TIME,
                )

            registry = store.read()
            entry = registry["entries"]["sony|lane-a|english"]
            self.assertEqual(entry["state"], STATE_LAUNCHING)

    def test_active_worker_limit_blocks_twenty_first_claim(self):
        with tempfile.TemporaryDirectory() as td:
            store = self.make_store(td)
            status = store.prepare_session(now=FIXED_TIME)

            for index in range(MAX_ACTIVE_IDENTITY_WORKERS):
                store.claim(
                    identity_key=f"sony|lane-{index}|english",
                    provider="SONY",
                    display_name=f"Lane {index}",
                    expected_session_id=status.session_id,
                    now=FIXED_TIME,
                )

            with self.assertRaisesRegex(
                IdentityLaunchBlocked,
                r"active recording limit reached \(20/20\)",
            ):
                store.claim(
                    identity_key="sony|lane-over-limit|english",
                    provider="SONY",
                    display_name="Over limit",
                    expected_session_id=status.session_id,
                    now=FIXED_TIME,
                )

            registry = store.read()
            self.assertEqual(len(registry["entries"]), MAX_ACTIVE_IDENTITY_WORKERS)

    def test_terminal_entry_does_not_consume_active_worker_capacity(self):
        with tempfile.TemporaryDirectory() as td:
            store = self.make_store(td)
            status = store.prepare_session(now=FIXED_TIME)

            for index in range(MAX_ACTIVE_IDENTITY_WORKERS):
                identity_key=f"sony|lane-{index}|english"
                store.claim(
                    identity_key=identity_key,
                    provider="SONY",
                    display_name=f"Lane {index}",
                    expected_session_id=status.session_id,
                    now=FIXED_TIME,
                )

            store.transition(
                identity_key="sony|lane-0|english",
                new_state=STATE_CRASHED,
                expected_session_id=status.session_id,
                now=FIXED_TIME,
            )

            entry = store.claim(
                identity_key="sony|replacement|english",
                provider="SONY",
                display_name="Replacement",
                expected_session_id=status.session_id,
                now=FIXED_TIME,
            )

            self.assertEqual(entry["state"], STATE_LAUNCHING)

    def test_session_guard_rejects_stale_worker_update(self):
        with tempfile.TemporaryDirectory() as td:
            store = self.make_store(td)
            status = store.prepare_session(now=FIXED_TIME)
            store.claim(
                identity_key="sony|lane-a|english",
                provider="SONY",
                display_name="Lane A",
                expected_session_id=status.session_id,
                now=FIXED_TIME,
            )

            with self.assertRaises(RegistryError):
                store.transition(
                    identity_key="sony|lane-a|english",
                    new_state=STATE_RECORDING,
                    worker_pid=12345,
                    expected_session_id="stale-session",
                    now=FIXED_TIME,
                )

    def test_state_machine_rejects_invalid_terminal_transition(self):
        with tempfile.TemporaryDirectory() as td:
            store = self.make_store(td)
            store.prepare_session(now=FIXED_TIME)
            store.claim(
                identity_key="sony|lane-a|english",
                provider="SONY",
                display_name="Lane A",
                now=FIXED_TIME,
            )
            store.transition(
                identity_key="sony|lane-a|english",
                new_state=STATE_CRASHED,
                now=FIXED_TIME,
            )

            with self.assertRaises(InvalidRegistryTransition):
                store.transition(
                    identity_key="sony|lane-a|english",
                    new_state=STATE_RECORDING,
                    worker_pid=12345,
                    now=FIXED_TIME,
                )


    def test_legacy_active_registry_migrates_to_recording(self):
        with tempfile.TemporaryDirectory() as td:
            store = self.make_store(td)
            status = store.prepare_session(now=FIXED_TIME)
            store.claim(
                identity_key="sony|lane-a|english",
                provider="SONY",
                display_name="Lane A",
                expected_session_id=status.session_id,
                now=FIXED_TIME,
            )
            store.bind_worker_pid(
                identity_key="sony|lane-a|english",
                worker_pid=12345,
                expected_session_id=status.session_id,
                now=FIXED_TIME,
            )
            store.transition(
                identity_key="sony|lane-a|english",
                new_state=STATE_RECORDING,
                worker_pid=12345,
                expected_session_id=status.session_id,
                now=FIXED_TIME,
            )

            data = store.read()
            data["schema_version"] = 1
            data["entries"]["sony|lane-a|english"]["state"] = "ACTIVE"
            store.paths.current.write_text(
                json.dumps(data),
                encoding="utf-8",
            )

            continued = store.prepare_session(
                worker_liveness=lambda pid: pid == 12345,
                now=FIXED_TIME,
            )

            self.assertEqual(continued.action, "CONTINUED")
            migrated = json.loads(store.paths.current.read_text(encoding="utf-8"))
            self.assertEqual(migrated["schema_version"], 2)
            self.assertEqual(
                migrated["entries"]["sony|lane-a|english"]["state"],
                STATE_RECORDING,
            )

    def test_bind_worker_pid_keeps_launching_state(self):
        with tempfile.TemporaryDirectory() as td:
            store = self.make_store(td)
            status = store.prepare_session(now=FIXED_TIME)
            store.claim(
                identity_key="sony|lane-a|english",
                provider="SONY",
                display_name="Lane A",
                expected_session_id=status.session_id,
                now=FIXED_TIME,
            )
            entry = store.bind_worker_pid(
                identity_key="sony|lane-a|english",
                worker_pid=12345,
                expected_session_id=status.session_id,
                now=FIXED_TIME,
            )
            self.assertEqual(entry["state"], STATE_LAUNCHING)
            self.assertEqual(entry["worker_pid"], 12345)

if __name__ == "__main__":
    unittest.main(verbosity=2)
