"""Atomic worker-owned runtime status for Identity Coordinator presentation."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Mapping, Optional

from recorder_runtime.paths import RecorderOutputPaths


SCHEMA_VERSION = 1


def _utc_text(now: Optional[datetime] = None) -> str:
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _status_root(output_paths: RecorderOutputPaths) -> Path:
    return output_paths.identity_registry.parent / "identity_runtime_status"


def _identity_filename(identity_key: str) -> str:
    digest = hashlib.sha256(identity_key.encode("utf-8")).hexdigest()
    return f"{digest}.json"


class IdentityRuntimeStatusStore:
    """One atomic status file per identity within one registry session.

    Each identity is owned by at most one worker in a registry session, so the
    per-identity file is single-writer and needs no cross-worker lock.
    """

    def __init__(
        self,
        output_paths: RecorderOutputPaths,
        registry_session_id: str,
    ) -> None:
        session_id = str(registry_session_id or "").strip()
        if not session_id:
            raise ValueError("registry_session_id is required")
        self.registry_session_id = session_id
        self.root = _status_root(output_paths) / session_id

    def _path(self, identity_key: str) -> Path:
        key = str(identity_key or "").strip()
        if not key:
            raise ValueError("identity_key is required")
        return self.root / _identity_filename(key)

    def write(
        self,
        *,
        identity_key: str,
        provider: str,
        worker_pid: int,
        worker_state: str,
        payload: Optional[Mapping[str, object]] = None,
        sequence: int = 0,
    ) -> Dict[str, object]:
        if isinstance(worker_pid, bool) or not isinstance(worker_pid, int) or worker_pid <= 0:
            raise ValueError("worker_pid must be a positive integer")

        data: Dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "registry_session_id": self.registry_session_id,
            "identity": str(identity_key),
            "provider": str(provider or "").strip().upper(),
            "worker_pid": worker_pid,
            "worker_state": str(worker_state or "").strip().upper(),
            "sequence": int(sequence),
            "updated_at": _utc_text(),
        }
        if payload:
            for key, value in payload.items():
                if key not in data:
                    data[str(key)] = value

        path = self._path(identity_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path: Optional[Path] = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="\n",
                dir=path.parent,
                prefix=f".{path.name}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temp_path = Path(handle.name)
                json.dump(data, handle, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, path)
            temp_path = None
        finally:
            if temp_path is not None:
                try:
                    temp_path.unlink()
                except FileNotFoundError:
                    pass
        return data

    def read_all(self) -> Dict[str, Dict[str, object]]:
        result: Dict[str, Dict[str, object]] = {}
        if not self.root.is_dir():
            return result

        for path in sorted(self.root.glob("*.json")):
            try:
                with path.open("r", encoding="utf-8") as handle:
                    data = json.load(handle)
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(data, dict):
                continue
            if data.get("schema_version") != SCHEMA_VERSION:
                continue
            if data.get("registry_session_id") != self.registry_session_id:
                continue
            identity_key = data.get("identity")
            if not isinstance(identity_key, str) or not identity_key:
                continue
            result[identity_key] = data

        return result
