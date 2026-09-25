"""Cross-platform terminal-tab hosting for interactive recorder workers."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Callable, Mapping, Optional, Sequence


class TerminalHostError(RuntimeError):
    """Raised when an interactive worker tab cannot be created safely."""


def _apple_script_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _windows_tab_argv(
    worker_command: Sequence[str],
    *,
    title: str,
) -> list[str]:
    command_line = subprocess.list2cmdline(list(worker_command))
    return [
        "wt.exe",
        "-w",
        "0",
        "new-tab",
        "--title",
        title,
        "--suppressApplicationTitle",
        "cmd.exe",
        "/k",
        command_line,
    ]


def _macos_tab_argv(
    worker_command: Sequence[str],
    *,
    title: str,
    cwd: Path,
) -> list[str]:
    safe_title = shlex.quote(str(title))
    shell_command = (
        f"cd {shlex.quote(str(cwd))} && "
        f"printf '\\033]0;%s\\007' {safe_title} && "
        f"{shlex.join(list(worker_command))}"
    )
    escaped = _apple_script_string(shell_command)
    script = (
        'tell application "Terminal" to activate\n'
        'tell application "System Events"\n'
        '  tell process "Terminal"\n'
        '    keystroke "t" using command down\n'
        '  end tell\n'
        'end tell\n'
        'delay 0.2\n'
        'tell application "Terminal"\n'
        f'  do script "{escaped}" in selected tab of front window\n'
        'end tell'
    )
    return ["osascript", "-e", script]


def build_terminal_tab_argv(
    worker_command: Sequence[str],
    *,
    title: str,
    cwd: Path,
    platform_name: Optional[str] = None,
) -> list[str]:
    platform_value = platform_name or sys.platform
    if platform_value.startswith("win"):
        return _windows_tab_argv(worker_command, title=title)
    if platform_value == "darwin":
        return _macos_tab_argv(worker_command, title=title, cwd=cwd)
    raise TerminalHostError(
        "Identity-worker terminal tabs are currently supported on "
        "Windows Terminal and macOS Terminal."
    )


def launch_terminal_tab(
    worker_command: Sequence[str],
    *,
    title: str,
    cwd: Path,
    popen_factory: Optional[Callable[..., object]] = None,
    environ: Optional[Mapping[str, str]] = None,
) -> object:
    environment = dict(os.environ if environ is None else environ)

    if sys.platform.startswith("win"):
        if shutil.which("wt.exe", path=environment.get("PATH")) is None:
            raise TerminalHostError(
                "Windows Terminal (wt.exe) is required to host identity "
                "workers as tabs instead of separate console windows."
            )
    elif sys.platform == "darwin":
        if shutil.which("osascript", path=environment.get("PATH")) is None:
            raise TerminalHostError(
                "macOS osascript is unavailable; cannot open a Terminal tab."
            )
    else:
        raise TerminalHostError(
            "Identity-worker terminal tabs are currently supported on "
            "Windows Terminal and macOS Terminal."
        )

    argv = build_terminal_tab_argv(
        worker_command,
        title=title,
        cwd=cwd,
    )
    popen = popen_factory or subprocess.Popen
    return popen(
        argv,
        cwd=str(cwd),
        env=environment,
    )
