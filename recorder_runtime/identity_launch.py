"""Explicit handoff contract for identity-bound recorder workers."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, Tuple

from recorder_source.models import SourceCandidate


LAUNCH_REQUEST_VERSION = 1


def _freeze(value: object) -> object:
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, Mapping):
        return {
            str(key): _freeze(item)
            for key, item in value.items()
        }
    return value


def _json_value(value: object) -> object:
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, Mapping):
        return {
            str(key): _json_value(item)
            for key, item in value.items()
        }
    return value


@dataclass(frozen=True)
class FrozenTargetIntent:
    """One launch-time target/search definition retained for worker recovery."""

    name: str
    source_groups: Tuple[str, ...]
    primary: Tuple[object, ...] = ()
    required: Tuple[object, ...] = ()
    rejected: Tuple[object, ...] = ()
    preferred: Tuple[object, ...] = ()
    match_all: bool = False
    worker_recording_duration_min: Optional[float] = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "FrozenTargetIntent":
        return cls(
            name=str(value.get("name") or "").strip(),
            source_groups=tuple(
                str(item).strip().upper()
                for item in (value.get("source_groups") or ())
                if str(item).strip()
            ),
            primary=tuple(_freeze(item) for item in (value.get("primary") or ())),
            required=tuple(_freeze(item) for item in (value.get("required") or ())),
            rejected=tuple(_freeze(item) for item in (value.get("rejected") or ())),
            preferred=tuple(_freeze(item) for item in (value.get("preferred") or ())),
            match_all=bool(value.get("match_all")),
            worker_recording_duration_min=(
                float(value["worker_recording_duration_min"])
                if value.get("worker_recording_duration_min") is not None
                else None
            ),
        )

    def to_mapping(self) -> dict:
        return {
            "name": self.name,
            "source_groups": list(self.source_groups),
            "primary": _json_value(self.primary),
            "required": _json_value(self.required),
            "rejected": _json_value(self.rejected),
            "preferred": _json_value(self.preferred),
            "match_all": self.match_all,
            "worker_recording_duration_min": self.worker_recording_duration_min,
        }


@dataclass(frozen=True)
class IdentityLaunchRequest:
    """Everything a worker needs to start now and recover within fixed intent."""

    registry_session_id: str
    identity_key: str
    provider: str
    selected_source_group: str
    selected_candidate: SourceCandidate
    target_intents: Tuple[FrozenTargetIntent, ...]
    recovery_playlist_urls: Tuple[str, ...]
    recording_duration_min: Optional[float]
    base_name: str
    initial_candidate_pool: Tuple[SourceCandidate, ...] = ()
    version: int = LAUNCH_REQUEST_VERSION

    def __post_init__(self) -> None:
        if self.version != LAUNCH_REQUEST_VERSION:
            raise ValueError(
                f"unsupported identity launch request version {self.version!r}"
            )
        if not self.registry_session_id.strip():
            raise ValueError("registry_session_id is required")
        if not self.identity_key.strip():
            raise ValueError("identity_key is required")
        if not self.provider.strip():
            raise ValueError("provider is required")
        if not self.selected_source_group.strip():
            raise ValueError("selected_source_group is required")
        if not self.target_intents:
            raise ValueError("at least one frozen target intent is required")
        if not self.recovery_playlist_urls:
            raise ValueError("at least one frozen recovery playlist URL is required")
        if any(not str(url).strip() for url in self.recovery_playlist_urls):
            raise ValueError("recovery playlist URLs must be non-empty")
        if not self.base_name.strip():
            raise ValueError("base_name is required")
        if not self.selected_candidate.stream_url.strip():
            raise ValueError("selected candidate stream_url is required")
        if not self.selected_candidate.launchable:
            raise ValueError("selected candidate must already be launchable")
        if (
            self.recording_duration_min is not None
            and self.recording_duration_min <= 0
        ):
            raise ValueError("recording_duration_min must be > 0 when supplied")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "IdentityLaunchRequest":
        candidate_value = value.get("selected_candidate")
        if not isinstance(candidate_value, Mapping):
            raise ValueError("selected_candidate must be an object")
        target_values = value.get("target_intents")
        if not isinstance(target_values, Sequence) or isinstance(
            target_values, (str, bytes)
        ):
            raise ValueError("target_intents must be an array")

        return cls(
            version=int(value.get("version") or 0),
            registry_session_id=str(value.get("registry_session_id") or ""),
            identity_key=str(value.get("identity_key") or ""),
            provider=str(value.get("provider") or "").strip().upper(),
            selected_source_group=str(
                value.get("selected_source_group") or ""
            ).strip().upper(),
            selected_candidate=SourceCandidate.from_mapping(candidate_value),
            initial_candidate_pool=tuple(
                SourceCandidate.from_mapping(item)
                for item in (value.get("initial_candidate_pool") or ())
                if isinstance(item, Mapping)
            ),
            target_intents=tuple(
                FrozenTargetIntent.from_mapping(item)
                for item in target_values
                if isinstance(item, Mapping)
            ),
            recovery_playlist_urls=tuple(
                str(item).strip()
                for item in (value.get("recovery_playlist_urls") or ())
                if str(item).strip()
            ),
            recording_duration_min=(
                float(value["recording_duration_min"])
                if value.get("recording_duration_min") is not None
                else None
            ),
            base_name=str(value.get("base_name") or ""),
        )

    def to_mapping(self) -> dict:
        return {
            "version": self.version,
            "registry_session_id": self.registry_session_id,
            "identity_key": self.identity_key,
            "provider": self.provider,
            "selected_source_group": self.selected_source_group,
            "selected_candidate": self.selected_candidate.to_mapping(),
            "initial_candidate_pool": [
                candidate.to_mapping()
                for candidate in self.initial_candidate_pool
            ],
            "target_intents": [
                target.to_mapping()
                for target in self.target_intents
            ],
            "recovery_playlist_urls": list(self.recovery_playlist_urls),
            "recording_duration_min": self.recording_duration_min,
            "base_name": self.base_name,
        }


def write_launch_request_temp(request: IdentityLaunchRequest) -> Path:
    """Write a complete local handoff file before worker process creation."""
    fd, raw_path = tempfile.mkstemp(
        prefix="recorder_identity_launch_",
        suffix=".json",
        text=True,
    )
    path = Path(raw_path)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(request.to_mapping(), handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        raise
    return path


def read_launch_request(
    path: os.PathLike[str] | str,
    *,
    delete_after_read: bool = False,
) -> IdentityLaunchRequest:
    request_path = Path(path)
    try:
        with request_path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
        if not isinstance(value, Mapping):
            raise ValueError("identity launch request root must be an object")
        return IdentityLaunchRequest.from_mapping(value)
    finally:
        if delete_after_read:
            try:
                request_path.unlink()
            except FileNotFoundError:
                pass
