from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from recorder_coordinator.registry import (
    IdentityLaunchBlocked,
    IdentityRegistryStore,
    InvalidRegistryTransition,
    RegistryError,
    STATE_ACTIVE,
    STATE_CRASHED,
    STATE_LAUNCHING,
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
                new_state=STATE_ACTIVE,
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

    def test_definitively_dead_owned_worker_allows_new_session(self):
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
                new_state=STATE_ACTIVE,
                worker_pid=12345,
                now=FIXED_TIME,
            )

            second = store.prepare_session(
                worker_liveness=lambda _pid: False,
                now=FIXED_TIME,
            )

            self.assertEqual(second.action, "ROLLED_OVER")
            self.assertNotEqual(second.session_id, first.session_id)
            self.assertEqual(
                len(list(store.paths.archive.glob("identity_registry_*.json"))),
                1,
            )

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
                    new_state=STATE_ACTIVE,
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
                    new_state=STATE_ACTIVE,
                    worker_pid=12345,
                    now=FIXED_TIME,
                )


if __name__ == "__main__":
    unittest.main(verbosity=2)
