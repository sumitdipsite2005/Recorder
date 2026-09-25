from __future__ import annotations

import base64
from pathlib import Path, PurePosixPath
import unittest

from recorder_runtime.terminal_host import build_terminal_tab_argv


class TerminalHostTests(unittest.TestCase):
    def test_windows_worker_uses_existing_terminal_window_new_tab(self):
        worker = [
            r"C:\Python\python.exe",
            r"C:\Recorder\recorder_identity_worker.py",
            "--request",
            r"C:\Temp\request.json",
        ]

        argv = build_terminal_tab_argv(
            worker,
            title="Recorder — Example",
            cwd=Path(r"C:\Recorder"),
            post_exit_cwd=Path(r"C:\My PC Recordings\Manual Recordings"),
            platform_name="win32",
            windows_shell="cmd.exe",
        )

        self.assertEqual(argv[:4], ["wt.exe", "-w", "0", "new-tab"])
        self.assertIn("--title", argv)
        self.assertIn("cmd.exe", argv)
        self.assertIn("/k", argv)
        self.assertNotIn("CREATE_NEW_CONSOLE", " ".join(argv))
        self.assertIn("recorder_identity_worker.py", argv[-1])
        self.assertIn(
            r'cd /d "C:\My PC Recordings\Manual Recordings"',
            argv[-1],
        )

    def test_windows_worker_matches_powershell_coordinator_shell(self):
        worker = [
            r"C:\Python\python.exe",
            r"C:\Recorder\recorder_identity_worker.py",
            "--request",
            r"C:\Temp\request.json",
        ]

        argv = build_terminal_tab_argv(
            worker,
            title="Recorder — Example",
            cwd=Path(r"C:\Recorder"),
            post_exit_cwd=Path(r"C:\My PC Recordings\Manual Recordings"),
            platform_name="win32",
            windows_shell="powershell.exe",
        )

        self.assertIn("powershell.exe", argv)
        self.assertIn("-NoExit", argv)
        self.assertIn("-EncodedCommand", argv)
        self.assertNotIn("-Command", argv)
        self.assertNotIn("cmd.exe", argv)
        decoded = base64.b64decode(argv[-1]).decode("utf-16-le")
        self.assertIn("recorder_identity_worker.py", decoded)
        self.assertIn(
            "Set-Location -LiteralPath 'C:\\My PC Recordings\\Manual Recordings'",
            decoded,
        )
        self.assertFalse(any("Set-Location" in argument for argument in argv[:-1]))

    def test_macos_worker_opens_new_terminal_tab_and_keeps_shell(self):
        worker = [
            "/usr/bin/python3",
            "/Users/test/Recorder/recorder_identity_worker.py",
            "--request",
            "/tmp/request.json",
        ]

        argv = build_terminal_tab_argv(
            worker,
            title="Recorder — Example",
            cwd=PurePosixPath("/Users/test/Recorder"),
            platform_name="darwin",
        )

        self.assertEqual(argv[:2], ["osascript", "-e"])
        script = argv[2]
        self.assertIn('keystroke "t" using command down', script)
        self.assertIn("selected tab of front window", script)
        self.assertIn("recorder_identity_worker.py", script)
        self.assertIn("cd /Users/test/Recorder", script)


if __name__ == "__main__":
    unittest.main(verbosity=2)
