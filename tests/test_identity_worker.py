from __future__ import annotations

import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import recorder_identity_worker as worker
from recorder_coordinator.registry import IdentityRegistryStore
from recorder_runtime.identity_launch import (
    FrozenTargetIntent,
    IdentityLaunchRequest,
    write_launch_request_temp,
)
from recorder_runtime.paths import build_recorder_output_paths
from recorder_source.models import SourceCandidate


class IdentityWorkerLifecycleTests(unittest.TestCase):
    def make_case(self, td: str):
        output_paths = build_recorder_output_paths(Path(td) / "Recordings")
        store = IdentityRegistryStore(output_paths)
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
        store.claim(
            identity_key=request.identity_key,
            provider=request.provider,
            display_name=request.base_name,
            expected_session_id=request.registry_session_id,
        )
        config_path = Path(td) / "config.py"
        config_path.write_text("# test config\n", encoding="utf-8")
        request_path = write_launch_request_temp(request)
        return store, request, config_path, request_path, output_paths

    def run_worker_with_outcome(self, td: str, outcome):
        store, request, config_path, request_path, output_paths = self.make_case(td)
        fake_record_dynamic = types.SimpleNamespace(
            OUTPUT_PATHS=output_paths,
            run_recorder_process=lambda **_kwargs: outcome,
        )
        with patch.dict(sys.modules, {"record_dynamic": fake_record_dynamic}):
            rc = worker.main([
                "--request",
                str(request_path),
                "--config",
                str(config_path),
            ])
        return rc, store, request

    def test_ctrl_c_outcome_becomes_manually_stopped(self):
        with tempfile.TemporaryDirectory() as td:
            outcome = types.SimpleNamespace(
                status="manual_stopped",
                reason="Ctrl-C requested by user",
            )
            rc, store, request = self.run_worker_with_outcome(td, outcome)

            self.assertEqual(rc, 0)
            entry = store.read()["entries"][request.identity_key]
            self.assertEqual(entry["state"], "MANUALLY_STOPPED")
            self.assertIn("Ctrl-C", entry["reason"])

    def test_normal_recorder_end_becomes_ended(self):
        with tempfile.TemporaryDirectory() as td:
            outcome = types.SimpleNamespace(
                status="ended",
                reason="duration_reached",
            )
            rc, store, request = self.run_worker_with_outcome(td, outcome)

            self.assertEqual(rc, 0)
            entry = store.read()["entries"][request.identity_key]
            self.assertEqual(entry["state"], "ENDED")

    def test_exceptional_worker_failure_becomes_crashed(self):
        with tempfile.TemporaryDirectory() as td:
            store, request, config_path, request_path, output_paths = self.make_case(td)

            def fail(**_kwargs):
                raise RuntimeError("test recorder failure")

            fake_record_dynamic = types.SimpleNamespace(
                OUTPUT_PATHS=output_paths,
                run_recorder_process=fail,
            )
            with patch.dict(sys.modules, {"record_dynamic": fake_record_dynamic}):
                with self.assertRaisesRegex(RuntimeError, "test recorder failure"):
                    worker.main([
                        "--request",
                        str(request_path),
                        "--config",
                        str(config_path),
                    ])

            entry = store.read()["entries"][request.identity_key]
            self.assertEqual(entry["state"], "CRASHED")


if __name__ == "__main__":
    unittest.main(verbosity=2)
