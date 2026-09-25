"""Process launch boundary for independent identity-bound recorder workers."""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from recorder_runtime.identity_launch import (
    IdentityLaunchRequest,
    write_launch_request_temp,
)
from recorder_runtime.terminal_host import launch_terminal_tab

from .registry import (
    IdentityRegistryStore,
    STATE_ACTIVE,
    STATE_CRASHED,
    STATE_ENDED,
    STATE_MANUALLY_STOPPED,
    STATE_WAITING_FOR_SOURCE,
)


@dataclass(frozen=True)
class WorkerLaunchResult:
    pid: int


def _worker_script_path() -> Path:
    return Path(__file__).resolve().parents[1] / "recorder_identity_worker.py"


def launch_identity_worker(
    request: IdentityLaunchRequest,
    registry_store: IdentityRegistryStore,
    *,
    config_path: Path,
    terminal_launcher: Optional[Callable[..., object]] = None,
    startup_timeout_sec: float = 15.0,
) -> WorkerLaunchResult:
    """Claim one identity and create its independent recorder process."""
    request_path = write_launch_request_temp(request)
    claimed = False
    try:
        registry_store.claim(
            identity_key=request.identity_key,
            provider=request.provider,
            display_name=request.base_name,
            reason="identity worker launch requested",
            expected_session_id=request.registry_session_id,
        )
        claimed = True

        resolved_config = Path(config_path).resolve()
        if not resolved_config.is_file():
            raise RuntimeError(
                f"Recorder config not found: {resolved_config}"
            )

        command = [
            sys.executable,
            str(_worker_script_path()),
            "--request",
            str(request_path),
            "--config",
            str(resolved_config),
        ]
        launcher = terminal_launcher or launch_terminal_tab
        launcher(
            command,
            title=f"Recorder — {request.base_name}",
            cwd=_worker_script_path().parent,
            post_exit_cwd=registry_store.paths.root.parent.parent,
        )

        deadline = time.monotonic() + max(0.1, float(startup_timeout_sec))
        terminal_states = {
            STATE_ACTIVE,
            STATE_WAITING_FOR_SOURCE,
            STATE_ENDED,
            STATE_MANUALLY_STOPPED,
            STATE_CRASHED,
        }
        while time.monotonic() < deadline:
            registry = registry_store.read()
            entry = registry["entries"].get(request.identity_key)
            if isinstance(entry, dict):
                state = entry.get("state")
                pid = entry.get("worker_pid")
                if state in terminal_states and isinstance(pid, int) and pid > 0:
                    if state == STATE_CRASHED:
                        raise RuntimeError(
                            "identity worker entered CRASHED during startup: "
                            f"{entry.get('reason') or 'unknown reason'}"
                        )
                    return WorkerLaunchResult(pid=pid)
            time.sleep(0.1)

        raise RuntimeError(
            "identity worker did not report startup within "
            f"{startup_timeout_sec:g}s"
        )

    except Exception as error:
        try:
            request_path.unlink()
        except FileNotFoundError:
            pass

        if claimed:
            try:
                registry = registry_store.read()
                entry = registry["entries"].get(request.identity_key)
                if isinstance(entry, dict) and entry.get("state") == "LAUNCHING":
                    registry_store.transition(
                        identity_key=request.identity_key,
                        new_state=STATE_CRASHED,
                        reason=(
                            "worker creation failed: "
                            f"{type(error).__name__}: {error}"
                        ),
                        expected_session_id=request.registry_session_id,
                    )
            except Exception:
                # Preserve the original launch failure. Any unresolved ownership
                # will be surfaced by the registry's fail-safe startup checks.
                pass
        raise
