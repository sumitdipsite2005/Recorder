"""Process launch boundary for independent identity-bound recorder workers."""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from recorder_runtime.identity_launch import (
    IdentityLaunchRequest,
    write_launch_request_temp,
)

from .registry import (
    IdentityRegistryStore,
    STATE_CRASHED,
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
    popen_factory: Optional[Callable[..., object]] = None,
) -> WorkerLaunchResult:
    """Claim one identity and create its independent recorder process."""
    request_path = write_launch_request_temp(request)
    claimed = False
    popen = popen_factory or subprocess.Popen

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
        kwargs = {
            "cwd": str(_worker_script_path().parent),
        }
        if os.name == "nt":
            kwargs["creationflags"] = subprocess.CREATE_NEW_CONSOLE
        else:
            kwargs["start_new_session"] = True

        process = popen(command, **kwargs)
        pid = int(getattr(process, "pid"))
        if pid <= 0:
            raise RuntimeError("identity worker process returned an invalid pid")
        return WorkerLaunchResult(pid=pid)

    except Exception as error:
        try:
            request_path.unlink()
        except FileNotFoundError:
            pass

        if claimed:
            registry_store.transition(
                identity_key=request.identity_key,
                new_state=STATE_CRASHED,
                reason=f"worker creation failed: {type(error).__name__}: {error}",
                expected_session_id=request.registry_session_id,
            )
        raise
