"""Identity Coordinator configuration, scheduling, and source-scope rules."""

from __future__ import annotations

import math
import runpy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Set, Tuple

from recorder_coordinator.models import (
    CoordinatorWindow,
    IdentityTarget,
    POLICY_ALL,
    POLICY_MANUAL,
    TargetRuntime,
    TargetView,
)
from recorder_coordinator.snapshot import compact_source_name
from recorder_source.matching import make_match_definition
from recorder_source.models import PlaylistSourceSpec
from recorder_source.policy import (
    PLAYLIST_GROUP_MATCH_MODES as GROUP_MATCH_MODE,
    PLAYLIST_GROUP_PROFILES as GROUP_PROVIDER,
    PLAYLIST_GROUP_SOURCE_BUCKETS as GROUP_SOURCE_BUCKET,
    PLAYLIST_USER_AGENTS,
)

DEFAULT_REFRESH_INTERVAL_SEC = 300

class CoordinatorConfigState:
    """Keep the last valid dynamic target configuration active."""

    def __init__(self, config_path: Path) -> None:
        self.config_path = Path(config_path)
        self.raw_config: Optional[dict] = None
        self.targets: Tuple[IdentityTarget, ...] = ()
        self.refresh_interval_sec = DEFAULT_REFRESH_INTERVAL_SEC
        self.last_error_signature = ""
        self.target_runtime: Dict[str, TargetRuntime] = {}
        self.coordinator_schedule_start: Optional[datetime] = None
        self.coordinator_run_duration_min: Optional[float] = None
        self.coordinator_actual_activation: Optional[datetime] = None
        self._coordinator_schedule_loaded = False

    def reload(self, now: datetime) -> Tuple[Tuple[str, ...], bool]:
        try:
            raw = runpy.run_path(str(self.config_path))
            targets = parse_targets(raw)
            validate_target_source_scopes(raw, targets)
            refresh = float(
                raw.get(
                    "IDENTITY_COORDINATOR_REFRESH_INTERVAL_SEC",
                    DEFAULT_REFRESH_INTERVAL_SEC,
                )
            )
            if not math.isfinite(refresh) or refresh <= 0:
                raise ValueError(
                    "IDENTITY_COORDINATOR_REFRESH_INTERVAL_SEC must be a finite value > 0"
                )
            coordinator_schedule_start = _parse_schedule_start(
                raw.get("IDENTITY_COORDINATOR_SCHEDULE_START")
            )
            coordinator_run_duration_min = _parse_optional_minutes(
                raw.get("IDENTITY_COORDINATOR_RUN_DURATION_MIN"),
                "IDENTITY_COORDINATOR_RUN_DURATION_MIN",
            )
        except Exception as error:
            signature = f"{type(error).__name__}: {error}"
            if self.raw_config is None:
                raise RuntimeError(
                    f"Identity Coordinator config could not be loaded: {signature}"
                ) from error
            if signature == self.last_error_signature:
                return (), False
            self.last_error_signature = signature
            return (
                (
                    "CONFIG WARNING: rejected invalid reload; keeping last valid config "
                    f"({signature})"
                ),
            ), False

        self.last_error_signature = ""
        previous_targets = self.targets
        messages = (
            diff_target_configs(previous_targets, targets)
            if self.raw_config is not None
            else ()
        )
        changed = (
            self.raw_config is None
            or targets != previous_targets
            or refresh != self.refresh_interval_sec
        )
        removed_target_names = (
            {target.name for target in previous_targets}
            - {target.name for target in targets}
        )
        for removed_name in removed_target_names:
            self.target_runtime.pop(removed_name, None)
        self.raw_config = raw
        self.targets = targets
        self.refresh_interval_sec = refresh
        if not self._coordinator_schedule_loaded:
            self.coordinator_schedule_start = coordinator_schedule_start
            self.coordinator_run_duration_min = coordinator_run_duration_min
            self._coordinator_schedule_loaded = True

        for target in targets:
            self.target_runtime.setdefault(target.name, TargetRuntime())
        return tuple(messages), changed

    def target_views(
        self,
        now: datetime,
        *,
        coordinator_active: bool = True,
    ) -> Tuple[TargetView, ...]:
        coordinator_active_from = (
            self.coordinator_schedule_start
            if self.coordinator_schedule_start is not None
            else self.coordinator_actual_activation
        )
        return tuple(
            target_view(
                target,
                self.target_runtime.setdefault(target.name, TargetRuntime()),
                now,
                coordinator_active=coordinator_active,
                coordinator_active_from=coordinator_active_from,
            )
            for target in self.targets
        )

    def coordinator_window(self, now: datetime) -> CoordinatorWindow:
        if self.coordinator_schedule_start is not None:
            active_from = self.coordinator_schedule_start
        else:
            if self.coordinator_actual_activation is None:
                self.coordinator_actual_activation = now
            active_from = self.coordinator_actual_activation

        active_until = (
            active_from + timedelta(minutes=self.coordinator_run_duration_min)
            if self.coordinator_run_duration_min is not None
            else None
        )
        if now < active_from:
            return CoordinatorWindow("WAITING", active_from, active_until)
        if active_until is not None and now >= active_until:
            return CoordinatorWindow("EXPIRED", active_from, active_until)
        return CoordinatorWindow("ACTIVE", active_from, active_until)


def _normalize_policy(value: object) -> str:
    text = str(value or "").strip().upper().replace(" ", "_")
    if text in {"ALL", "ALL_ID", "ALL_IDS", "ALL_IDENTITIES"}:
        return POLICY_ALL
    if text == "MANUAL":
        return POLICY_MANUAL
    raise ValueError(f"unsupported identity policy {value!r}; use MANUAL or ALL_IDENTITIES")


def _parse_optional_minutes(value: object, field_name: str) -> Optional[float]:
    if value is None or value == "":
        return None
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{field_name} must be a finite value > 0 when supplied")
    return number


def _parse_schedule_start(value: object) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.astimezone().replace(tzinfo=None) if value.tzinfo is not None else value
    text = str(value).strip()
    for candidate in (text, text.replace("Z", "+00:00")):
        try:
            parsed = datetime.fromisoformat(candidate)
            return (
                parsed.astimezone().replace(tzinfo=None)
                if parsed.tzinfo is not None
                else parsed
            )
        except ValueError:
            pass
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError(f"invalid schedule_start {value!r}; use ISO date/time")


def _tuple_config(value: object) -> Tuple[object, ...]:
    if value is None:
        return ()
    if isinstance(value, tuple):
        return value
    if isinstance(value, list):
        return tuple(value)
    return (value,)


def parse_targets(raw: Mapping[str, object]) -> Tuple[IdentityTarget, ...]:
    if "IDENTITY_COORDINATOR_TARGETS" not in raw:
        raise ValueError(
            "IDENTITY_COORDINATOR_TARGETS is required; configure explicit MANUAL/ALL targets"
        )
    configured = raw.get("IDENTITY_COORDINATOR_TARGETS")
    targets: List[IdentityTarget] = []

    if not isinstance(configured, (list, tuple)):
        raise ValueError(
            "IDENTITY_COORDINATOR_TARGETS must be a list of explicit MANUAL/ALL targets"
        )

    # Target names identify active runtime targets. Disabled declarations do not
    # participate in duplicate-name enforcement and must never suppress or
    # replace an enabled target with the same name.
    enabled_names: Set[str] = set()
    for index, item in enumerate(configured, start=1):
        if not isinstance(item, Mapping):
            raise ValueError(f"target {index} must be a dictionary")
        name = str(item.get("name") or "").strip()
        if not name:
            raise ValueError(f"target {index} needs a stable unique name")
        if not bool(item.get("enabled", True)):
            continue
        if name in enabled_names:
            raise ValueError(f"duplicate enabled target name {name!r}")
        enabled_names.add(name)

    retained_disabled_names: Set[str] = set()
    for index, item in enumerate(configured, start=1):
        name = str(item.get("name") or "").strip()
        enabled = bool(item.get("enabled", True))

        # If an enabled target exists with this name, any disabled declaration
        # is inert configuration and is ignored entirely.
        if not enabled and name in enabled_names:
            continue

        # Multiple disabled declarations with the same name are also inert.
        # Keep one for DISABLED presentation/config-diff behavior without
        # creating duplicate runtime-name entries.
        if not enabled and name in retained_disabled_names:
            continue

        source_groups = tuple(
            str(group).strip().upper()
            for group in _tuple_config(item.get("source_groups") or item.get("source_scope"))
            if str(group).strip()
        )
        if not source_groups:
            raise ValueError(f"{name}: source_groups/source_scope cannot be empty")

        match_all = bool(item.get("match_all", False))
        primary = _tuple_config(item.get("primary"))
        if not match_all and not primary:
            raise ValueError(f"{name}: primary search is required unless match_all=True")

        parsed_target = IdentityTarget(
            name=name,
            policy=_normalize_policy(item.get("policy")),
            source_groups=source_groups,
            primary=primary,
            required=_tuple_config(item.get("required")),
            rejected=_tuple_config(item.get("rejected")),
            preferred=_tuple_config(item.get("preferred")),
            match_all=match_all,
            enabled=enabled,
            schedule_start=_parse_schedule_start(item.get("schedule_start")),
            activity_duration_min=_parse_optional_minutes(
                item.get("activity_duration_min"), "activity_duration_min"
            ),
            worker_recording_duration_min=_parse_optional_minutes(
                item.get("worker_recording_duration_min"), "worker_recording_duration_min"
            ),
        )
        targets.append(parsed_target)
        if not enabled:
            retained_disabled_names.add(name)

    return tuple(targets)

def diff_target_configs(
    previous: Sequence[IdentityTarget],
    current: Sequence[IdentityTarget],
) -> Tuple[str, ...]:
    old = {target.name: target for target in previous}
    new = {target.name: target for target in current}
    messages: List[str] = []
    for name in sorted(new.keys() - old.keys()):
        messages.append(f"CONFIG: added target {name}")
    for name in sorted(old.keys() - new.keys()):
        messages.append(f"CONFIG: removed target {name}")
    for name in sorted(old.keys() & new.keys()):
        if old[name] == new[name]:
            continue
        changed_fields: List[str] = []
        for field_name in IdentityTarget.__dataclass_fields__:
            if field_name == "name":
                continue
            if getattr(old[name], field_name) != getattr(new[name], field_name):
                changed_fields.append(field_name)
        messages.append(f"CONFIG: updated target {name} ({', '.join(changed_fields)})")
    return tuple(messages)


def target_view(
    target: IdentityTarget,
    runtime: TargetRuntime,
    now: datetime,
    *,
    coordinator_active: bool = True,
    coordinator_active_from: Optional[datetime] = None,
) -> TargetView:
    if not target.enabled:
        return TargetView(target, "DISABLED", None, None)

    if target.schedule_start is not None:
        active_from = target.schedule_start
        if coordinator_active_from is not None and active_from <= coordinator_active_from:
            active_from = coordinator_active_from
    else:
        if runtime.first_activation is None:
            if not coordinator_active:
                return TargetView(target, "WAITING_COORDINATOR", None, None)
            runtime.first_activation = now
        active_from = runtime.first_activation

    active_until = (
        active_from + timedelta(minutes=target.activity_duration_min)
        if target.activity_duration_min is not None
        else None
    )

    if not coordinator_active:
        if coordinator_active_from is not None and active_from <= coordinator_active_from:
            return TargetView(target, "WAITING_COORDINATOR", active_from, active_until)
        if now < active_from:
            return TargetView(target, "SCHEDULED", active_from, active_until)
        return TargetView(target, "WAITING_COORDINATOR", active_from, active_until)

    if now < active_from:
        return TargetView(target, "SCHEDULED", active_from, active_until)
    if active_until is not None and now >= active_until:
        return TargetView(target, "EXPIRED", active_from, active_until)
    return TargetView(target, "ACTIVE", active_from, active_until)


def _normalize_playlist_source(
    source: object,
) -> Tuple[str, str, Mapping[str, str], Mapping[str, str]]:
    if isinstance(source, Mapping):
        url = str(source.get("url") or "").strip()
        name = str(source.get("name") or "").strip()
        raw_headers = source.get("playlist_headers") or {}
        request_headers = dict(raw_headers) if isinstance(raw_headers, Mapping) else {}
        playlist_ua_profile = str(source.get("playlist_user_agent") or "").strip().upper()
        stream_ua_profile = str(source.get("stream_user_agent") or "").strip().upper()
        if playlist_ua_profile:
            playlist_ua = PLAYLIST_USER_AGENTS.get(playlist_ua_profile)
            if playlist_ua is None:
                raise ValueError(f"unknown playlist_user_agent profile {playlist_ua_profile!r}")
            request_headers.setdefault("User-Agent", playlist_ua)
        stream_headers: Dict[str, str] = {}
        if stream_ua_profile:
            stream_ua = PLAYLIST_USER_AGENTS.get(stream_ua_profile)
            if stream_ua is None:
                raise ValueError(f"unknown stream_user_agent profile {stream_ua_profile!r}")
            stream_headers["User-Agent"] = stream_ua
        return url, name, request_headers, stream_headers
    return str(source or "").strip(), "", {}, {}


def _source_context_group_for_target(
    target: IdentityTarget,
    source_group: str,
) -> str:
    """Resolve provider/match semantics for one explicitly configured source bucket."""
    group_name = str(source_group or "").strip().upper()
    if group_name != "COMMON":
        return group_name

    provider_groups = tuple(
        dict.fromkeys(
            str(group).strip().upper()
            for group in target.source_groups
            if str(group).strip() and str(group).strip().upper() != "COMMON"
        )
    )
    if len(provider_groups) != 1:
        raise ValueError(
            f"{target.name}: COMMON must be paired with exactly one non-COMMON "
            "source group so provider/match behavior is unambiguous"
        )
    return provider_groups[0]


def validate_target_source_scopes(
    raw: Mapping[str, object],
    targets: Sequence[IdentityTarget],
) -> None:
    """Reject a reload before it can replace the last valid target set."""
    for target in targets:
        for group in target.source_groups:
            context_group = _source_context_group_for_target(target, group)
            sources_for_group(raw, group, context_group=context_group)
            _match_definition_for_target(target, context_group)


def sources_for_group(
    raw: Mapping[str, object],
    group: str,
    *,
    context_group: Optional[str] = None,
) -> Tuple[PlaylistSourceSpec, ...]:
    """Return only the explicitly named source bucket.

    COMMON is no longer silently injected. When COMMON is explicitly listed on
    a target, context_group preserves that target's provider/match semantics
    while the playlist URLs still come only from the COMMON bucket.
    """
    group_name = str(group or "").strip().upper()
    context_name = str(context_group or group_name).strip().upper()
    buckets = raw.get("NM3U8DL_PLAYLIST_GROUPS")
    if not isinstance(buckets, Mapping):
        raise ValueError("NM3U8DL_PLAYLIST_GROUPS is missing/invalid")
    bucket = GROUP_SOURCE_BUCKET.get(group_name, group_name)
    provider = GROUP_PROVIDER.get(context_name, context_name or "UNKNOWN")
    values = buckets.get(bucket, ())
    if not isinstance(values, (list, tuple)):
        raise ValueError(f"playlist source bucket {bucket!r} must be a list")

    result: List[PlaylistSourceSpec] = []
    seen: Set[str] = set()
    for item in values:
        url, name, headers, stream_headers = _normalize_playlist_source(item)
        if not url or url in seen:
            continue
        seen.add(url)
        result.append(
            PlaylistSourceSpec(
                url=url,
                name=name or compact_source_name(url),
                group=context_name,
                provider=provider,
                request_headers=headers,
                stream_headers=stream_headers,
            )
        )
    if not result:
        raise ValueError(f"no playlist sources configured for group {group_name}")
    return tuple(result)


def _match_definition_for_target(target: IdentityTarget, source_group: str):
    mode = GROUP_MATCH_MODE.get(source_group, "EVENT_PHRASE")
    return make_match_definition(
        mode=mode,
        primary=target.primary,
        required=target.required,
        rejected=target.rejected,
        preferred=target.preferred,
        match_all=target.match_all,
    )

