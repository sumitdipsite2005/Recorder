from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from recorder_runtime.identity_status import IdentityRuntimeStatusStore
from recorder_runtime.paths import build_recorder_output_paths


class IdentityRuntimeStatusTests(unittest.TestCase):
    def test_status_write_is_read_back_for_current_session(self):
        with tempfile.TemporaryDirectory() as td:
            paths = build_recorder_output_paths(Path(td) / "Recordings")
            store = IdentityRuntimeStatusStore(paths, "session-a")

            store.write(
                identity_key="SONYLIV|lane:test/one/ENG",
                provider="SONYLIV",
                worker_pid=4321,
                worker_state="RECORDING",
                payload={
                    "current_candidate": {
                        "playlist_url": "https://example.test/list.m3u",
                        "entry_title": "Example Event",
                    },
                    "target_names": ["Example"],
                    "source_count": 3,
                },
                sequence=4,
            )

            statuses = store.read_all()
            status = statuses["SONYLIV|lane:test/one/ENG"]
            self.assertEqual(status["worker_state"], "RECORDING")
            self.assertEqual(status["worker_pid"], 4321)
            self.assertEqual(status["sequence"], 4)
            self.assertEqual(status["source_count"], 3)
            self.assertEqual(
                status["current_candidate"]["entry_title"],
                "Example Event",
            )

    def test_other_registry_session_is_isolated(self):
        with tempfile.TemporaryDirectory() as td:
            paths = build_recorder_output_paths(Path(td) / "Recordings")
            first = IdentityRuntimeStatusStore(paths, "session-a")
            second = IdentityRuntimeStatusStore(paths, "session-b")

            first.write(
                identity_key="SONYLIV|lane:test/one/ENG",
                provider="SONYLIV",
                worker_pid=4321,
                worker_state="RECORDING",
                sequence=1,
            )

            self.assertEqual(second.read_all(), {})


if __name__ == "__main__":
    unittest.main(verbosity=2)
