"""Cross-platform terminal-tab hosting for interactive recorder workers."""

from __future__ import annotations

import base64
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


def _powershell_quote(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _detect_windows_shell() -> str:
    """Detect the shell hosting the Coordinator process; fail safely to CMD."""
    query_shell = shutil.which("powershell.exe") or shutil.which("pwsh.exe")
    if query_shell is None:
        return "cmd.exe"
    try:
        completed = subprocess.run(
            [
                query_shell,
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                f"(Get-Process -Id {os.getppid()}).ProcessName",
            ],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
    except Exception:
        return "cmd.exe"

    process_name = str(completed.stdout or "").strip().splitlines()
    if not process_name:
        return "cmd.exe"
    normalized = process_name[-1].strip().casefold()
    if normalized in {"powershell", "powershell.exe"}:
        return "powershell.exe"
    if normalized in {"pwsh", "pwsh.exe"}:
        return "pwsh.exe"
    if normalized in {"cmd", "cmd.exe"}:
        return "cmd.exe"
    return "cmd.exe"


def _windows_tab_argv(
    worker_command: Sequence[str],
    *,
    title: str,
    post_exit_cwd: Path,
    windows_shell: Optional[str] = None,
) -> list[str]:
    shell = str(windows_shell or _detect_windows_shell()).strip().casefold()
    if shell in {"powershell", "powershell.exe", "pwsh", "pwsh.exe"}:
        executable = "pwsh.exe" if shell.startswith("pwsh") else "powershell.exe"
        invocation = "& " + " ".join(
            _powershell_quote(argument)
            for argument in worker_command
        )
        shell_command = (
            f"{invocation}; "
            f"Set-Location -LiteralPath {_powershell_quote(post_exit_cwd)}"
        )
        encoded_command = base64.b64encode(
            shell_command.encode("utf-16-le")
        ).decode("ascii")
        return [
            "wt.exe",
            "-w",
            "0",
            "new-tab",
            "--title",
            title,
            "--suppressApplicationTitle",
            executable,
            "-NoExit",
            "-EncodedCommand",
            encoded_command,
        ]

    command_line = subprocess.list2cmdline(list(worker_command))
    post_exit = subprocess.list2cmdline([str(post_exit_cwd)])
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
        f"{command_line} & cd /d {post_exit}",
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
        'tell application "System Events"\n'
        '  set terminalWasRunning to exists process "Terminal"\n'
        'end tell\n'
        'set hadWindow to false\n'
        'if terminalWasRunning then\n'
        '  tell application "Terminal"\n'
        '    set hadWindow to (count of windows) > 0\n'
        '  end tell\n'
        'end if\n'
        'tell application "Terminal" to activate\n'
        'delay 0.2\n'
        'if hadWindow then\n'
        '  tell application "System Events"\n'
        '    tell process "Terminal"\n'
        '      keystroke "t" using command down\n'
        '    end tell\n'
        '  end tell\n'
        '  delay 0.2\n'
        '  tell application "Terminal"\n'
        f'    do script "{escaped}" in selected tab of front window\n'
        '  end tell\n'
        'else\n'
        '  tell application "Terminal"\n'
        '    if (count of windows) = 0 then\n'
        f'      do script "{escaped}"\n'
        '    else\n'
        f'      do script "{escaped}" in selected tab of front window\n'
        '    end if\n'
        '  end tell\n'
        'end if'
    )
    return ["osascript", "-e", script]


def build_terminal_tab_argv(
    worker_command: Sequence[str],
    *,
    title: str,
    cwd: Path,
    post_exit_cwd: Optional[Path] = None,
    platform_name: Optional[str] = None,
    windows_shell: Optional[str] = None,
) -> list[str]:
    platform_value = platform_name or sys.platform
    if platform_value.startswith("win"):
        return _windows_tab_argv(
            worker_command,
            title=title,
            post_exit_cwd=Path(post_exit_cwd or cwd),
            windows_shell=windows_shell,
        )
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
    post_exit_cwd: Optional[Path] = None,
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
        post_exit_cwd=post_exit_cwd,
    )
    popen = popen_factory or subprocess.Popen
    return popen(
        argv,
        cwd=str(cwd),
        env=environment,
    )
