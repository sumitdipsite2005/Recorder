"""Shared runtime output-directory layout for recorder entrypoints."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union


PathValue = Optional[Union[str, os.PathLike]]


@dataclass(frozen=True)
class RecorderOutputPaths:
    """Derived runtime locations beneath one user-configured output root."""

    root: Path
    recorder_logs: Path
    recording_logs: Path
    playlist_history: Path
    coordinator_logs: Path
    identity_registry: Path
    identity_registry_archive: Path


def build_recorder_output_paths(value: PathValue) -> RecorderOutputPaths:
    """Validate RECORDING_OUTPUT_DIR and derive the fixed runtime layout."""
    text = str(value or "").strip()
    if not text:
        raise ValueError(
            "RECORDING_OUTPUT_DIR is required in recorder_dynamic_user_config.py"
        )

    expanded = os.path.expandvars(os.path.expanduser(text))
    root = Path(expanded)
    if not root.is_absolute():
        raise ValueError(
            "RECORDING_OUTPUT_DIR must be an absolute path; "
            f"got {text!r}"
        )

    recorder_logs = root / "recorder_logs"
    identity_registry = recorder_logs / "identity_registry"
    return RecorderOutputPaths(
        root=root,
        recorder_logs=recorder_logs,
        recording_logs=recorder_logs / "recording_logs",
        playlist_history=recorder_logs / "playlist_history",
        coordinator_logs=recorder_logs / "coordinator_logs",
        identity_registry=identity_registry,
        identity_registry_archive=identity_registry / "archive",
    )
