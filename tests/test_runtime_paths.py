from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from recorder_runtime.paths import build_recorder_output_paths


class RecorderOutputPathTests(unittest.TestCase):
    def test_fixed_layout_is_derived_from_one_root(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "Manual Recordings"

            paths = build_recorder_output_paths(root)

            self.assertEqual(paths.root, root)
            self.assertEqual(paths.recorder_logs, root / "recorder_logs")
            self.assertEqual(
                paths.recording_logs,
                root / "recorder_logs" / "recording_logs",
            )
            self.assertEqual(
                paths.playlist_history,
                root / "recorder_logs" / "playlist_history",
            )
            self.assertEqual(
                paths.coordinator_logs,
                root / "recorder_logs" / "coordinator_logs",
            )

    def test_missing_output_root_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "RECORDING_OUTPUT_DIR is required"):
            build_recorder_output_paths(None)

    def test_relative_output_root_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "must be an absolute path"):
            build_recorder_output_paths("Manual Recordings")


if __name__ == "__main__":
    unittest.main(verbosity=2)
