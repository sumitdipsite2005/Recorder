from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from recorder_coordinator.registry import IdentityRegistryStore
from recorder_coordinator.worker import launch_identity_worker
from recorder_runtime.identity_launch import (
    FrozenTargetIntent,
    IdentityLaunchRequest,
)
from recorder_runtime.paths import build_recorder_output_paths
from recorder_source.models import SourceCandidate


class WorkerLaunchTests(unittest.TestCase):
    def make_store_and_request(self, td: str):
        paths = build_recorder_output_paths(Path(td) / "Manual Recordings")
        store = IdentityRegistryStore(paths)
        status = store.prepare_session()
        request = IdentityLaunchRequest(
            registry_session_id=status.session_id,
            identity_key="SONYLIV|lane:feed123/channel1/english",
            provider="SONYLIV",
            selected_source_group="SONYLIV_EVENTS",
            selected_candidate=SourceCandidate(
                playlist_url="https://example.test/list.m3u",
                entry_title="Example Event",
                stream_url=(
                    "https://example.test/hls/live/feed123/"
                    "channel1/english/master.m3u8"
                ),
                launchable=True,
                extra={
                    "provider": "SONYLIV",
                    "source_group": "SONYLIV_EVENTS",
                },
            ),
            target_intents=(
                FrozenTargetIntent(
                    name="Example",
                    source_groups=("SONYLIV_EVENTS",),
                    primary=("example",),
                ),
            ),
            recording_duration_min=60,
            base_name="Example Event",
        )
        return store, request

    def test_process_is_created_only_after_registry_claim(self):
        with tempfile.TemporaryDirectory() as td:
            store, request = self.make_store_and_request(td)
            captured = {}

            class FakeProcess:
                pid = 4321

            def fake_popen(command, **kwargs):
                registry = store.read()
                captured["state_at_spawn"] = registry["entries"][
                    request.identity_key
                ]["state"]
                captured["command"] = command
                captured["kwargs"] = kwargs
                return FakeProcess()

            result = launch_identity_worker(
                request,
                store,
                popen_factory=fake_popen,
            )
            request_path = Path(captured["command"][-1])
            try:
                self.assertEqual(result.pid, 4321)
                self.assertEqual(captured["state_at_spawn"], "LAUNCHING")
                self.assertEqual(
                    Path(captured["command"][1]).name,
                    "recorder_identity_worker.py",
                )
                self.assertTrue(request_path.exists())
                if os.name == "nt":
                    self.assertIn("creationflags", captured["kwargs"])
                else:
                    self.assertTrue(captured["kwargs"].get("start_new_session"))
            finally:
                request_path.unlink(missing_ok=True)

    def test_process_creation_failure_marks_identity_crashed(self):
        with tempfile.TemporaryDirectory() as td:
            store, request = self.make_store_and_request(td)
            captured_path = None

            def failing_popen(command, **_kwargs):
                nonlocal captured_path
                captured_path = Path(command[-1])
                raise OSError("test spawn failure")

            with self.assertRaises(OSError):
                launch_identity_worker(
                    request,
                    store,
                    popen_factory=failing_popen,
                )

            registry = store.read()
            self.assertEqual(
                registry["entries"][request.identity_key]["state"],
                "CRASHED",
            )
            self.assertIsNotNone(captured_path)
            self.assertFalse(captured_path.exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
