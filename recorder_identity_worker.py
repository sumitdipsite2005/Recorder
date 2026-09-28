"""Identity-bound worker process.

The Coordinator owns discovery/launch authority. This process consumes one
explicit launch request, binds the claimed worker PID, and then enters the
same mature dynamic recorder execution path used by direct ONE BEST runs.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Optional, Sequence

from recorder_runtime.registry import (
    IdentityRegistryStore,
    STATE_CRASHED,
    STATE_ENDED,
    STATE_LAUNCHING,
    STATE_MANUALLY_STOPPED,
    STATE_RECORDING,
    STATE_WAITING_FOR_SOURCE,
)
from recorder_runtime.identity_status import IdentityRuntimeStatusStore
from recorder_runtime.identity_launch import read_launch_request


def parse_args(argv: Optional[Sequence[str]] = None):
    parser = argparse.ArgumentParser(description="Run one identity-bound recorder worker")
    parser.add_argument(
        "--request",
        type=Path,
        required=True,
        help="temporary identity launch request JSON",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="same recorder_dynamic_user_config.py used by the Coordinator",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    request = read_launch_request(args.request, delete_after_read=True)
    config_path = args.config.resolve()
    if not config_path.is_file():
        raise RuntimeError(f"Recorder config not found: {config_path}")

    # record_dynamic still owns its normal configuration load. The worker only
    # points that existing loader at the exact config file used by Coordinator.
    os.environ["RECORDER_DYNAMIC_CONFIG_PATH"] = str(config_path)

    # Import only inside the worker process. The Coordinator itself remains
    # decoupled from the mature recorder-sized module.
    import record_dynamic

    registry_store = IdentityRegistryStore(record_dynamic.OUTPUT_PATHS)
    registry_store.bind_worker_pid(
        identity_key=request.identity_key,
        worker_pid=os.getpid(),
        reason="identity worker process started",
        expected_session_id=request.registry_session_id,
    )
    status_store = IdentityRuntimeStatusStore(
        record_dynamic.OUTPUT_PATHS,
        request.registry_session_id,
    )
    status_sequence = 0

    def publish_runtime_status(payload):
        nonlocal status_sequence
        data = dict(payload or {})
        worker_state = str(data.pop("worker_state", "") or "").strip().upper()
        if not worker_state:
            return

        registry = registry_store.read()
        entry = registry["entries"].get(request.identity_key)
        current_state = (
            str(entry.get("state") or "").strip().upper()
            if isinstance(entry, dict)
            else ""
        )
        desired_state = {
            "RECORDING": STATE_RECORDING,
            "WAITING_FOR_SOURCE": STATE_WAITING_FOR_SOURCE,
        }.get(worker_state)

        if desired_state and current_state != desired_state:
            registry_store.transition(
                identity_key=request.identity_key,
                new_state=desired_state,
                worker_pid=os.getpid(),
                reason=str(data.get("reason") or ""),
                expected_session_id=request.registry_session_id,
            )

        status_sequence += 1
        status_store.write(
            identity_key=request.identity_key,
            provider=request.provider,
            worker_pid=os.getpid(),
            worker_state=worker_state,
            payload=data,
            sequence=status_sequence,
        )

    try:
        outcome = record_dynamic.run_recorder_process(
            identity_launch_request=request,
            identity_status_callback=publish_runtime_status,
        )

        if outcome.status == "manual_stopped":
            status_sequence += 1
            status_store.write(
                identity_key=request.identity_key,
                provider=request.provider,
                worker_pid=os.getpid(),
                worker_state=STATE_MANUALLY_STOPPED,
                payload={"current_candidate": None, "reason": outcome.reason},
                sequence=status_sequence,
            )
            registry_store.transition(
                identity_key=request.identity_key,
                new_state=STATE_MANUALLY_STOPPED,
                worker_pid=os.getpid(),
                reason=outcome.reason,
                expected_session_id=request.registry_session_id,
            )
            return 0

        if outcome.status == "ended":
            status_sequence += 1
            status_store.write(
                identity_key=request.identity_key,
                provider=request.provider,
                worker_pid=os.getpid(),
                worker_state=STATE_ENDED,
                payload={"current_candidate": None, "reason": outcome.reason},
                sequence=status_sequence,
            )
            registry_store.transition(
                identity_key=request.identity_key,
                new_state=STATE_ENDED,
                worker_pid=os.getpid(),
                reason=outcome.reason,
                expected_session_id=request.registry_session_id,
            )
            return 0

        raise RuntimeError(
            "mature recorder returned a non-terminal-success outcome: "
            f"{outcome.status}: {outcome.reason}"
        )

    except BaseException as error:
        try:
            registry = registry_store.read()
            entry = registry["entries"].get(request.identity_key)
            if (
                isinstance(entry, dict)
                and entry.get("state") in {
                    STATE_LAUNCHING,
                    STATE_RECORDING,
                    STATE_WAITING_FOR_SOURCE,
                }
            ):
                status_sequence += 1
                status_store.write(
                    identity_key=request.identity_key,
                    provider=request.provider,
                    worker_pid=os.getpid(),
                    worker_state=STATE_CRASHED,
                    payload={
                        "current_candidate": None,
                        "reason": f"worker failed: {type(error).__name__}: {error}",
                    },
                    sequence=status_sequence,
                )
                registry_store.transition(
                    identity_key=request.identity_key,
                    new_state=STATE_CRASHED,
                    worker_pid=os.getpid(),
                    reason=f"worker failed: {type(error).__name__}: {error}",
                    expected_session_id=request.registry_session_id,
                )
        except Exception:
            pass
        raise


if __name__ == "__main__":
    raise SystemExit(main())
