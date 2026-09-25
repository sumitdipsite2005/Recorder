"""Identity-bound worker process.

The Coordinator owns discovery/launch authority. This process consumes one
explicit launch request, marks its claimed identity ACTIVE, and then enters the
same mature dynamic recorder execution path used by direct ONE BEST runs.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Optional, Sequence

from recorder_coordinator.registry import (
    IdentityRegistryStore,
    STATE_ACTIVE,
    STATE_CRASHED,
)
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
    registry_store.transition(
        identity_key=request.identity_key,
        new_state=STATE_ACTIVE,
        worker_pid=os.getpid(),
        reason="identity worker process started",
        expected_session_id=request.registry_session_id,
    )

    try:
        record_dynamic.run_recorder_process(
            identity_launch_request=request,
        )
    except Exception as error:
        registry_store.transition(
            identity_key=request.identity_key,
            new_state=STATE_CRASHED,
            worker_pid=os.getpid(),
            reason=f"worker failed: {type(error).__name__}: {error}",
            expected_session_id=request.registry_session_id,
        )
        raise

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
