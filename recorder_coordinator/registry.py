"""Persistent ownership state for identity-based recorder orchestration.

The registry is intentionally narrow: it prevents duplicate MANUAL/ALL
identity workers and preserves orchestration state across Coordinator restarts.
It does not store discovery candidates or recorder runtime internals.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterator, Mapping, Optional, Tuple

from recorder_runtime.paths import RecorderOutputPaths


SCHEMA_VERSION = 1

STATE_LAUNCHING = "LAUNCHING"
STATE_ACTIVE = "ACTIVE"
STATE_WAITING_FOR_SOURCE = "WAITING_FOR_SOURCE"
STATE_ENDED = "ENDED"
STATE_MANUALLY_STOPPED = "MANUALLY_STOPPED"
STATE_CRASHED = "CRASHED"

OWNERSHIP_STATES = frozenset(
    {STATE_LAUNCHING, STATE_ACTIVE, STATE_WAITING_FOR_SOURCE}
)
TERMINAL_STATES = frozenset(
    {STATE_ENDED, STATE_MANUALLY_STOPPED, STATE_CRASHED}
)
VALID_STATES = OWNERSHIP_STATES | TERMINAL_STATES

_ALLOWED_TRANSITIONS = {
    STATE_LAUNCHING: frozenset({STATE_ACTIVE, STATE_CRASHED}),
    STATE_ACTIVE: frozenset(
        {
            STATE_WAITING_FOR_SOURCE,
            STATE_ENDED,
            STATE_MANUALLY_STOPPED,
            STATE_CRASHED,
        }
    ),
    STATE_WAITING_FOR_SOURCE: frozenset(
        {
            STATE_ACTIVE,
            STATE_ENDED,
            STATE_MANUALLY_STOPPED,
            STATE_CRASHED,
        }
    ),
    STATE_ENDED: frozenset(),
    STATE_MANUALLY_STOPPED: frozenset(),
    STATE_CRASHED: frozenset(),
}


class RegistryError(RuntimeError):
    """Base exception for unsafe or invalid registry operations."""


class RegistryLockedError(RegistryError):
    """Raised when the registry update lock cannot be acquired."""


class RegistryValidationError(RegistryError):
    """Raised when current.json is malformed or violates the registry schema."""


class IdentityLaunchBlocked(RegistryError):
    """Raised when an identity is already owned or suppressed in this session."""


class InvalidRegistryTransition(RegistryError):
    """Raised when a caller attempts an invalid state transition."""


@dataclass(frozen=True)
class IdentityRegistryPaths:
    root: Path
    current: Path
    archive: Path
    lock: Path


@dataclass(frozen=True)
class RegistrySessionStatus:
    session_id: str
    action: str
    unresolved_identities: Tuple[str, ...] = ()


def registry_paths(output_paths: RecorderOutputPaths) -> IdentityRegistryPaths:
    root = output_paths.identity_registry
    return IdentityRegistryPaths(
        root=root,
        current=root / "current.json",
        archive=output_paths.identity_registry_archive,
        lock=root / ".registry.lock",
    )


def _utc_text(now: Optional[datetime] = None) -> str:
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _new_registry(now: Optional[datetime] = None) -> Dict[str, object]:
    stamp = _utc_text(now)
    return {
        "schema_version": SCHEMA_VERSION,
        "session_id": uuid.uuid4().hex,
        "created_at": stamp,
        "updated_at": stamp,
        "entries": {},
    }


def _validate_registry(data: object) -> Dict[str, object]:
    if not isinstance(data, dict):
        raise RegistryValidationError("identity registry root must be a JSON object")
    if data.get("schema_version") != SCHEMA_VERSION:
        raise RegistryValidationError(
            f"unsupported identity registry schema_version {data.get('schema_version')!r}"
        )
    session_id = data.get("session_id")
    if not isinstance(session_id, str) or not session_id.strip():
        raise RegistryValidationError("identity registry session_id is required")
    entries = data.get("entries")
    if not isinstance(entries, dict):
        raise RegistryValidationError("identity registry entries must be an object")

    for identity_key, entry in entries.items():
        if not isinstance(identity_key, str) or not identity_key:
            raise RegistryValidationError(
                "identity registry keys must be non-empty strings"
            )
        if not isinstance(entry, dict):
            raise RegistryValidationError(
                f"identity registry entry {identity_key!r} must be an object"
            )
        if entry.get("identity") != identity_key:
            raise RegistryValidationError(
                f"identity registry entry {identity_key!r} has mismatched identity"
            )
        state = entry.get("state")
        if state not in VALID_STATES:
            raise RegistryValidationError(
                f"identity registry entry {identity_key!r} has invalid state {state!r}"
            )
        worker_pid = entry.get("worker_pid")
        if worker_pid is not None and (
            isinstance(worker_pid, bool)
            or not isinstance(worker_pid, int)
            or worker_pid <= 0
        ):
            raise RegistryValidationError(
                f"identity registry entry {identity_key!r} has invalid worker_pid"
            )
    return data


def _read_unlocked(path: Path) -> Dict[str, object]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return _validate_registry(json.load(handle))
    except FileNotFoundError:
        raise
    except RegistryValidationError:
        raise
    except (OSError, json.JSONDecodeError) as error:
        raise RegistryValidationError(
            f"could not read valid identity registry {path}: {error}"
        ) from error


def _write_atomic(path: Path, data: Mapping[str, object]) -> None:
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


def _lock_is_stale(path: Path, stale_after_sec: float) -> bool:
    try:
        return time.time() - path.stat().st_mtime > stale_after_sec
    except FileNotFoundError:
        return False


@contextmanager
def _registry_lock(
    paths: IdentityRegistryPaths,
    *,
    timeout_sec: float = 10.0,
    stale_after_sec: float = 300.0,
) -> Iterator[None]:
    paths.root.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + max(0.0, timeout_sec)
    fd: Optional[int] = None

    while fd is None:
        try:
            fd = os.open(
                str(paths.lock),
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
            payload = (
                f"pid={os.getpid()}\ncreated={_utc_text()}\n"
            ).encode("utf-8")
            os.write(fd, payload)
            os.fsync(fd)
        except FileExistsError:
            if _lock_is_stale(paths.lock, stale_after_sec):
                try:
                    paths.lock.unlink()
                    continue
                except FileNotFoundError:
                    continue
            if time.monotonic() >= deadline:
                raise RegistryLockedError(
                    f"identity registry is busy: {paths.lock}"
                )
            time.sleep(0.05)

    try:
        yield
    finally:
        if fd is not None:
            os.close(fd)
        try:
            paths.lock.unlink()
        except FileNotFoundError:
            pass


def process_liveness(pid: int) -> Optional[bool]:
    """Return True/False when process existence is knowable, otherwise None."""
    if pid <= 0:
        return False
    if pid == os.getpid():
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return None
    return True


class IdentityRegistryStore:
    """Safe local access to one current identity-orchestration registry."""

    def __init__(self, output_paths: RecorderOutputPaths) -> None:
        self.paths = registry_paths(output_paths)

    def read(self) -> Dict[str, object]:
        return _read_unlocked(self.paths.current)

    def prepare_session(
        self,
        *,
        worker_liveness: Callable[[int], Optional[bool]] = process_liveness,
        now: Optional[datetime] = None,
    ) -> RegistrySessionStatus:
        """Create, continue, or roll the current orchestration session.

        The old registry is archived only when no live worker and no unresolved
        ownership remains. Coordinator restart therefore continues a still-live
        session instead of creating a duplicate-owner session.
        """
        self.paths.archive.mkdir(parents=True, exist_ok=True)

        with _registry_lock(self.paths):
            if not self.paths.current.exists():
                registry = _new_registry(now)
                _write_atomic(self.paths.current, registry)
                return RegistrySessionStatus(
                    session_id=str(registry["session_id"]),
                    action="CREATED",
                )

            registry = _read_unlocked(self.paths.current)
            live, unresolved = self._ownership_status(
                registry,
                worker_liveness,
            )
            if live or unresolved:
                return RegistrySessionStatus(
                    session_id=str(registry["session_id"]),
                    action="CONTINUED",
                    unresolved_identities=unresolved,
                )

            archive_path = self._next_archive_path(registry, now)
            os.replace(self.paths.current, archive_path)
            new_registry = _new_registry(now)
            _write_atomic(self.paths.current, new_registry)
            return RegistrySessionStatus(
                session_id=str(new_registry["session_id"]),
                action="ROLLED_OVER",
            )

    def launch_blocked(self, identity_key: str) -> bool:
        registry = self.read()
        entries = registry["entries"]
        assert isinstance(entries, dict)
        return identity_key in entries

    def claim(
        self,
        *,
        identity_key: str,
        provider: str,
        display_name: str,
        reason: str = "",
        now: Optional[datetime] = None,
    ) -> Dict[str, object]:
        """Atomically claim an identity as LAUNCHING before worker creation."""
        if not identity_key:
            raise ValueError("identity_key is required")
        stamp = _utc_text(now)

        with _registry_lock(self.paths):
            registry = _read_unlocked(self.paths.current)
            entries = registry["entries"]
            assert isinstance(entries, dict)
            if identity_key in entries:
                existing = entries[identity_key]
                assert isinstance(existing, dict)
                raise IdentityLaunchBlocked(
                    f"{identity_key} is already {existing.get('state')} "
                    "in the current registry session"
                )

            entry: Dict[str, object] = {
                "identity": identity_key,
                "provider": provider,
                "display_name": display_name,
                "state": STATE_LAUNCHING,
                "worker_pid": None,
                "updated_at": stamp,
            }
            if reason:
                entry["reason"] = reason
            entries[identity_key] = entry
            registry["updated_at"] = stamp
            _write_atomic(self.paths.current, registry)
            return dict(entry)

    def transition(
        self,
        *,
        identity_key: str,
        new_state: str,
        worker_pid: Optional[int] = None,
        reason: str = "",
        now: Optional[datetime] = None,
    ) -> Dict[str, object]:
        """Atomically move one identity through settled orchestration states."""
        if new_state not in VALID_STATES:
            raise InvalidRegistryTransition(
                f"invalid registry state {new_state!r}"
            )
        if worker_pid is not None and (
            isinstance(worker_pid, bool)
            or not isinstance(worker_pid, int)
            or worker_pid <= 0
        ):
            raise ValueError("worker_pid must be a positive integer")

        stamp = _utc_text(now)
        with _registry_lock(self.paths):
            registry = _read_unlocked(self.paths.current)
            entries = registry["entries"]
            assert isinstance(entries, dict)
            entry = entries.get(identity_key)
            if not isinstance(entry, dict):
                raise InvalidRegistryTransition(
                    f"{identity_key} is not claimed in the current registry session"
                )

            old_state = entry["state"]
            if new_state not in _ALLOWED_TRANSITIONS[old_state]:
                raise InvalidRegistryTransition(
                    f"identity registry transition {old_state} -> "
                    f"{new_state} is not allowed"
                )

            if new_state in {STATE_ACTIVE, STATE_WAITING_FOR_SOURCE}:
                effective_pid = (
                    worker_pid
                    if worker_pid is not None
                    else entry.get("worker_pid")
                )
                if (
                    isinstance(effective_pid, bool)
                    or not isinstance(effective_pid, int)
                    or effective_pid <= 0
                ):
                    raise InvalidRegistryTransition(
                        f"{new_state} requires a valid worker_pid"
                    )
                entry["worker_pid"] = effective_pid
            elif worker_pid is not None:
                entry["worker_pid"] = worker_pid

            entry["state"] = new_state
            entry["updated_at"] = stamp
            if reason:
                entry["reason"] = reason
            else:
                entry.pop("reason", None)
            registry["updated_at"] = stamp
            _write_atomic(self.paths.current, registry)
            return dict(entry)

    def _ownership_status(
        self,
        registry: Mapping[str, object],
        worker_liveness: Callable[[int], Optional[bool]],
    ) -> Tuple[bool, Tuple[str, ...]]:
        has_live_worker = False
        unresolved = []
        entries = registry["entries"]
        assert isinstance(entries, dict)

        for identity_key, entry in entries.items():
            assert isinstance(entry, dict)
            if entry["state"] not in OWNERSHIP_STATES:
                continue

            pid = entry.get("worker_pid")
            if (
                isinstance(pid, bool)
                or not isinstance(pid, int)
                or pid <= 0
            ):
                unresolved.append(identity_key)
                continue

            live = worker_liveness(pid)
            if live is True:
                has_live_worker = True
            elif live is None:
                unresolved.append(identity_key)

        return has_live_worker, tuple(sorted(unresolved))

    def _next_archive_path(
        self,
        registry: Mapping[str, object],
        now: Optional[datetime],
    ) -> Path:
        value = now or datetime.now(timezone.utc)
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        stamp = value.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S")
        session_id = str(registry["session_id"])
        base = f"identity_registry_{stamp}_{session_id[:8]}"
        candidate = self.paths.archive / f"{base}.json"
        suffix = 1
        while candidate.exists():
            candidate = self.paths.archive / f"{base}_{suffix}.json"
            suffix += 1
        return candidate
