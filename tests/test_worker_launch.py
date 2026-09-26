from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from recorder_runtime.registry import IdentityRegistryStore
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
            recovery_playlist_urls=("https://example.test/list.m3u",),
            recording_duration_min=60,
            base_name="Example Event",
        )
        config_path = Path(td) / "config.py"
        config_path.write_text("# test config\n", encoding="utf-8")
        return store, request, config_path

    def test_terminal_tab_is_created_only_after_registry_claim(self):
        with tempfile.TemporaryDirectory() as td:
            store, request, config_path = self.make_store_and_request(td)
            captured = {}
            transitions = []

            def fake_terminal_launcher(command, *, title, cwd, post_exit_cwd):
                registry = store.read()
                captured["state_at_launch"] = registry["entries"][
                    request.identity_key
                ]["state"]
                captured["command"] = command
                captured["title"] = title
                captured["cwd"] = cwd
                captured["post_exit_cwd"] = post_exit_cwd
                store.bind_worker_pid(
                    identity_key=request.identity_key,
                    worker_pid=4321,
                    reason="test worker started",
                    expected_session_id=request.registry_session_id,
                )
                return object()

            result = launch_identity_worker(
                request,
                store,
                config_path=config_path,
                terminal_launcher=fake_terminal_launcher,
                startup_timeout_sec=1.0,
                registry_transition_callback=(
                    lambda identity_key, previous_state, entry: transitions.append(
                        (
                            identity_key,
                            previous_state,
                            entry["state"],
                            entry.get("reason"),
                        )
                    )
                ),
            )

            request_index = captured["command"].index("--request") + 1
            config_index = captured["command"].index("--config") + 1
            request_path = Path(captured["command"][request_index])
            try:
                self.assertEqual(result.pid, 4321)
                self.assertEqual(captured["state_at_launch"], "LAUNCHING")
                self.assertEqual(
                    transitions,
                    [(
                        request.identity_key,
                        "-",
                        "LAUNCHING",
                        "identity worker launch requested",
                    )],
                )
                self.assertEqual(
                    Path(captured["command"][1]).name,
                    "recorder_identity_worker.py",
                )
                self.assertEqual(
                    Path(captured["command"][config_index]),
                    config_path.resolve(),
                )
                self.assertIn("Example Event", captured["title"])
                self.assertEqual(
                    captured["post_exit_cwd"],
                    Path(td) / "Manual Recordings",
                )
            finally:
                request_path.unlink(missing_ok=True)

    def test_terminal_launch_failure_marks_identity_crashed(self):
        with tempfile.TemporaryDirectory() as td:
            store, request, config_path = self.make_store_and_request(td)
            captured_path = None

            def failing_terminal_launcher(command, *, title, cwd, post_exit_cwd):
                nonlocal captured_path
                captured_path = Path(
                    command[command.index("--request") + 1]
                )
                raise OSError("test terminal launch failure")

            with self.assertRaises(OSError):
                launch_identity_worker(
                    request,
                    store,
                    config_path=config_path,
                    terminal_launcher=failing_terminal_launcher,
                    startup_timeout_sec=1.0,
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
