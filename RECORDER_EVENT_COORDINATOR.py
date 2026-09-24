"""Identity Coordinator — Inspect / Watch checkpoint.

Checkpoint 2 deliberately stops before recording launch.  It dynamically reads
MANUAL/ALL target definitions, discovers qualifying playlist observations,
canonicalizes provider feed identities, and presents meaningful NEW/UPDATE/
SOURCE+ changes.
"""

from __future__ import annotations

import argparse
import math
import os
import runpy
import sys
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple
from urllib.parse import urlsplit

try:
    import winsound
except ImportError:  # pragma: no cover - non-Windows development/test hosts
    winsound = None

from recorder_source.discovery import (
    fetch_playlist_documents,
    parse_playlist_text,
    probe_candidates,
)
from recorder_source.identity import CanonicalFeedIdentity, derive_feed_identity
from recorder_source.matching import evaluate_match, make_match_definition
from recorder_source.models import PlaylistSourceSpec, SourceCandidate
from recorder_source.policy import (
    DEFAULT_SELECTION_POLICY,
    PLAYLIST_GROUP_MATCH_MODES as GROUP_MATCH_MODE,
    PLAYLIST_GROUP_PROFILES as GROUP_PROVIDER,
    PLAYLIST_GROUP_SOURCE_BUCKETS as GROUP_SOURCE_BUCKET,
    PLAYLIST_USER_AGENTS,
    PROVIDER_SELECTION_POLICIES as PROVIDER_SELECTION_POLICY,
)
from recorder_source.selection import select_join_candidate, video_quality_rank


POLICY_MANUAL = "MANUAL"
POLICY_ALL = "ALL_IDENTITIES"
VALID_POLICIES = frozenset({POLICY_MANUAL, POLICY_ALL})

DEFAULT_REFRESH_INTERVAL_SEC = 300


@dataclass(frozen=True)
class IdentityTarget:
    name: str
    policy: str
    source_groups: Tuple[str, ...]
    primary: Tuple[object, ...] = ()
    required: Tuple[object, ...] = ()
    rejected: Tuple[object, ...] = ()
    preferred: Tuple[object, ...] = ()
    match_all: bool = False
    enabled: bool = True
    schedule_start: Optional[datetime] = None
    activity_duration_min: Optional[float] = None
    worker_recording_duration_min: Optional[float] = None


@dataclass
class TargetRuntime:
    first_activation: Optional[datetime] = None


@dataclass(frozen=True)
class TargetView:
    target: IdentityTarget
    status: str
    active_from: Optional[datetime]
    active_until: Optional[datetime]


@dataclass(frozen=True)
class CoordinatorWindow:
    status: str
    active_from: datetime
    active_until: Optional[datetime]


@dataclass(frozen=True)
class SourceObservation:
    source_id: str
    source_name: str
    candidates: Tuple[SourceCandidate, ...]
    event_names: Tuple[str, ...]
    tvg_names: Tuple[str, ...]
    group_titles: Tuple[str, ...]
    candidate_states: Tuple[str, ...]
    state: str
    best_candidate: Optional[SourceCandidate]

    @property
    def meaningful_signature(self) -> Tuple[object, ...]:
        quality = _quality_signature(self.best_candidate)
        return (
            self.event_names,
            self.tvg_names,
            self.group_titles,
            self.candidate_states,
            self.state,
            quality,
        )


@dataclass
class IdentityBlock:
    policy: str
    identity: CanonicalFeedIdentity
    target_names: List[str] = field(default_factory=list)
    candidates: List[SourceCandidate] = field(default_factory=list)
    observations: Dict[str, SourceObservation] = field(default_factory=dict)
    best_candidate: Optional[SourceCandidate] = None
    overall_state: str = "UNUSABLE"


@dataclass(frozen=True)
class ChangeEvent:
    marker: str
    block_key: Tuple[str, str]
    details: Tuple[str, ...]
    beep: bool = False


@dataclass
class DashboardSnapshot:
    created_at: datetime
    target_views: Tuple[TargetView, ...]
    coordinator_window: Optional[CoordinatorWindow]
    blocks: Dict[Tuple[str, str], IdentityBlock]
    source_errors: Tuple[str, ...] = ()
    config_messages: Tuple[str, ...] = ()


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
        return tuple(
            target_view(
                target,
                self.target_runtime.setdefault(target.name, TargetRuntime()),
                now,
                coordinator_active=coordinator_active,
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


def _default_config_path() -> Path:
    if sys.platform == "darwin":
        root = Path.home() / "Library/CloudStorage/OneDrive-Personal/RECORDER"
    elif os.name == "nt":
        onedrive = os.environ.get("OneDrive")
        if not onedrive:
            raise RuntimeError("Windows OneDrive folder could not be located")
        root = Path(onedrive) / "RECORDER"
    else:
        raise RuntimeError("Use --config on this operating system")
    return root / "recorder_dynamic_user_config.py"


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

    seen_names: Set[str] = set()
    for index, item in enumerate(configured, start=1):
        if not isinstance(item, Mapping):
            raise ValueError(f"target {index} must be a dictionary")
        name = str(item.get("name") or "").strip()
        if not name:
            raise ValueError(f"target {index} needs a stable unique name")
        if name in seen_names:
            raise ValueError(f"duplicate target name {name!r}")
        seen_names.add(name)

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

        targets.append(
            IdentityTarget(
                name=name,
                policy=_normalize_policy(item.get("policy")),
                source_groups=source_groups,
                primary=primary,
                required=_tuple_config(item.get("required")),
                rejected=_tuple_config(item.get("rejected")),
                preferred=_tuple_config(item.get("preferred")),
                match_all=match_all,
                enabled=bool(item.get("enabled", True)),
                schedule_start=_parse_schedule_start(item.get("schedule_start")),
                activity_duration_min=_parse_optional_minutes(
                    item.get("activity_duration_min"), "activity_duration_min"
                ),
                worker_recording_duration_min=_parse_optional_minutes(
                    item.get("worker_recording_duration_min"), "worker_recording_duration_min"
                ),
            )
        )

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
) -> TargetView:
    if not target.enabled:
        return TargetView(target, "DISABLED", None, None)

    if target.schedule_start is not None:
        active_from = target.schedule_start
    else:
        if runtime.first_activation is None:
            if not coordinator_active:
                return TargetView(target, "WAITING_COORDINATOR", None, None)
            runtime.first_activation = now
        active_from = runtime.first_activation

    if now < active_from:
        active_until = (
            active_from + timedelta(minutes=target.activity_duration_min)
            if target.activity_duration_min is not None
            else None
        )
        return TargetView(target, "SCHEDULED", active_from, active_until)

    active_until = (
        active_from + timedelta(minutes=target.activity_duration_min)
        if target.activity_duration_min is not None
        else None
    )
    if active_until is not None and now >= active_until:
        return TargetView(target, "EXPIRED", active_from, active_until)
    if not coordinator_active:
        return TargetView(target, "WAITING_COORDINATOR", active_from, active_until)
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


def _compact_source_name(url: str) -> str:
    try:
        parsed = urlsplit(url)
        path = parsed.path.rstrip("/")
        tail = "/".join(path.split("/")[-3:])
        return f"{parsed.netloc}/{tail}" if tail else parsed.netloc
    except Exception:
        return url


def validate_target_source_scopes(
    raw: Mapping[str, object],
    targets: Sequence[IdentityTarget],
) -> None:
    """Reject a reload before it can replace the last valid target set."""
    for target in targets:
        for group in target.source_groups:
            sources_for_group(raw, group)
            _match_definition_for_target(target, group)


def sources_for_group(raw: Mapping[str, object], group: str) -> Tuple[PlaylistSourceSpec, ...]:
    group_name = str(group or "").strip().upper()
    buckets = raw.get("NM3U8DL_PLAYLIST_GROUPS")
    if not isinstance(buckets, Mapping):
        raise ValueError("NM3U8DL_PLAYLIST_GROUPS is missing/invalid")
    bucket = GROUP_SOURCE_BUCKET.get(group_name, group_name)
    provider = GROUP_PROVIDER.get(group_name, group_name or "UNKNOWN")
    result: List[PlaylistSourceSpec] = []
    seen: Set[str] = set()
    for source_group in ("COMMON", bucket):
        values = buckets.get(source_group, ())
        if not isinstance(values, (list, tuple)):
            raise ValueError(f"playlist source bucket {source_group!r} must be a list")
        for item in values:
            url, name, headers, stream_headers = _normalize_playlist_source(item)
            if not url or url in seen:
                continue
            seen.add(url)
            result.append(
                PlaylistSourceSpec(
                    url=url,
                    name=name or _compact_source_name(url),
                    group=group_name,
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


def _probe_key(candidate: SourceCandidate) -> Tuple[object, ...]:
    return (
        candidate.playlist_url,
        str(candidate.extra.get("provider") or "UNKNOWN"),
        candidate.stream_url,
        candidate.matching_entry_index,
        tuple(sorted((str(k), str(v)) for k, v in dict(candidate.headers).items())),
    )


def _matching_candidate(candidate: SourceCandidate, definition) -> Optional[SourceCandidate]:
    evaluation = evaluate_match(
        definition,
        tvg_name=candidate.tvg_name,
        group_title=candidate.group_title,
        entry_title=candidate.entry_title,
        stream_url=candidate.stream_url,
    )
    if not evaluation.matches:
        return None
    return replace(
        candidate,
        preferred_qualifier_score=evaluation.preferred_qualifier_score,
        ignored=False,
        reason="",
    )


def _context_candidate_for_target(
    candidate: SourceCandidate,
    target: IdentityTarget,
    source_group: str,
) -> Optional[SourceCandidate]:
    """Keep same-identity source disagreement visible without widening eligibility.

    Once at least one observation genuinely matches a target, another observation
    of that same canonical feed may carry stale/different primary metadata.  It is
    useful dashboard context, but it must never become a recordable alternative
    unless it independently satisfies the target's full search definition.
    Required/rejected qualifier gates still apply to context rows.
    """
    gate_definition = make_match_definition(
        mode=GROUP_MATCH_MODE.get(source_group, "EVENT_PHRASE"),
        primary=(),
        required=target.required,
        rejected=target.rejected,
        preferred=target.preferred,
        match_all=True,
    )
    evaluation = evaluate_match(
        gate_definition,
        tvg_name=candidate.tvg_name,
        group_title=candidate.group_title,
        entry_title=candidate.entry_title,
        stream_url=candidate.stream_url,
    )
    if not evaluation.matches:
        return None
    return replace(
        candidate,
        preferred_qualifier_score=evaluation.preferred_qualifier_score,
        ignored=True,
        reason=(
            "same feed identity context; this source's current primary metadata "
            "does not match the target"
        ),
    )


def _identity_serialized(candidate: SourceCandidate) -> str:
    provider = str(candidate.extra.get("provider") or "UNKNOWN").upper()
    return derive_feed_identity(candidate, provider).serialized


def acquire_active_targets(
    raw_config: Mapping[str, object],
    target_views: Sequence[TargetView],
) -> Tuple[Dict[str, Tuple[SourceCandidate, ...]], Tuple[str, ...]]:
    active = [view for view in target_views if view.status == "ACTIVE"]
    if not active:
        return {}, ()

    source_specs_by_key: Dict[Tuple[str, str, str], PlaylistSourceSpec] = {}
    target_group_sources: Dict[Tuple[str, str], Tuple[PlaylistSourceSpec, ...]] = {}
    for view in active:
        for group in view.target.source_groups:
            specs = sources_for_group(raw_config, group)
            target_group_sources[(view.target.name, group)] = specs
            for spec in specs:
                source_specs_by_key[(spec.url, spec.provider, spec.group)] = spec

    # Fetch each configured URL once. Parsing/normalization is performed once per
    # source/provider context, then target matching is applied afterward.
    url_source_specs: Dict[str, PlaylistSourceSpec] = {}
    for spec in source_specs_by_key.values():
        url_source_specs.setdefault(spec.url, spec)
    documents, fetch_errors, _ = fetch_playlist_documents(tuple(url_source_specs.values()))

    parsed_by_key: Dict[Tuple[str, str, str], Tuple[SourceCandidate, ...]] = {}
    errors: List[str] = list(fetch_errors)
    for key, spec in source_specs_by_key.items():
        text = documents.get(spec.url)
        if text is None:
            continue
        try:
            result = parse_playlist_text(
                text,
                playlist_url=spec.url,
                source_name=spec.name,
                source_group=spec.group,
                provider=spec.provider,
                stream_headers=spec.stream_headers,
            )
        except Exception as error:
            errors.append(
                f"{spec.name}: discovery parse failed ({type(error).__name__}: {error})"
            )
            continue
        parsed_by_key[key] = result.candidates

    raw_candidates_by_target: Dict[str, List[SourceCandidate]] = {
        view.target.name: [] for view in active
    }
    matched_identity_keys: Dict[str, Set[str]] = {
        view.target.name: set() for view in active
    }

    # First establish which identities genuinely qualify for each target.
    for view in active:
        target = view.target
        for group in target.source_groups:
            definition = _match_definition_for_target(target, group)
            for spec in target_group_sources[(target.name, group)]:
                key = (spec.url, spec.provider, spec.group)
                for candidate in parsed_by_key.get(key, ()):
                    matched = _matching_candidate(candidate, definition)
                    if matched is None:
                        continue
                    raw_candidates_by_target[target.name].append(matched)
                    matched_identity_keys[target.name].add(_identity_serialized(matched))

    # Then retain same-identity observations that disagree only on primary
    # metadata. This makes stale/current source disagreement visible without
    # allowing the stale observation to become a selectable recording candidate.
    for view in active:
        target = view.target
        target_identities = matched_identity_keys[target.name]
        if not target_identities:
            continue
        existing_keys = {
            _probe_key(candidate) for candidate in raw_candidates_by_target[target.name]
        }
        for group in target.source_groups:
            definition = _match_definition_for_target(target, group)
            for spec in target_group_sources[(target.name, group)]:
                key = (spec.url, spec.provider, spec.group)
                for candidate in parsed_by_key.get(key, ()):
                    if _probe_key(candidate) in existing_keys:
                        continue
                    if _identity_serialized(candidate) not in target_identities:
                        continue
                    if _matching_candidate(candidate, definition) is not None:
                        continue
                    context = _context_candidate_for_target(candidate, target, group)
                    if context is None:
                        continue
                    raw_candidates_by_target[target.name].append(context)
                    existing_keys.add(_probe_key(context))

    # Probe equivalent candidate/session observations once and reuse the result
    # across targets. Metadata-only observations are retained and come back as
    # NO_PLAYABLE_SOURCE rather than being discarded.
    unique_by_key: Dict[Tuple[object, ...], SourceCandidate] = {}
    for candidates in raw_candidates_by_target.values():
        for candidate in candidates:
            unique_by_key.setdefault(_probe_key(candidate), candidate)
    probed = probe_candidates(tuple(unique_by_key.values()))
    probed_by_key = {_probe_key(candidate): candidate for candidate in probed}

    final: Dict[str, Tuple[SourceCandidate, ...]] = {}
    for name, candidates in raw_candidates_by_target.items():
        resolved = tuple(
            probed_by_key.get(_probe_key(candidate), candidate)
            for candidate in candidates
        )
        eligible_identities = {
            _identity_serialized(candidate)
            for candidate in resolved
            if not candidate.ignored
        }
        final[name] = tuple(
            candidate
            for candidate in resolved
            if not candidate.ignored
            or _identity_serialized(candidate) in eligible_identities
        )
    return final, tuple(dict.fromkeys(errors))


def _candidate_source_id(candidate: SourceCandidate) -> str:
    return str(candidate.playlist_url or candidate.extra.get("source_name") or "unknown-source")


def _candidate_source_name(candidate: SourceCandidate) -> str:
    return str(candidate.extra.get("source_name") or _compact_source_name(candidate.playlist_url))


def _candidate_observation_key(candidate: SourceCandidate) -> Tuple[object, ...]:
    """Identify one source observation independent of which targets matched it."""
    return (
        _candidate_source_id(candidate),
        candidate.stream_url,
        tuple(
            sorted(
                (str(key).casefold(), str(value))
                for key, value in candidate.headers.items()
            )
        ),
        candidate.tvg_name,
        candidate.group_title,
        candidate.entry_title,
    )


def _candidate_state(candidate: SourceCandidate) -> str:
    provider = str(candidate.extra.get("provider") or "UNKNOWN").upper()
    policy = PROVIDER_SELECTION_POLICY.get(provider, DEFAULT_SELECTION_POLICY)
    if (
        candidate.launchable
        and candidate.expiry is None
        and policy is not None
        and not policy.allow_unknown_expiry
    ):
        return "AUTH_UNKNOWN"
    if candidate.launchable:
        return "WORKING"
    if candidate.probe_status == "expired":
        return "EXPIRED"
    if candidate.access_blocked or candidate.probe_status == "access_blocked":
        return "ACCESS_BLOCKED"
    if candidate.unsupported_drm:
        return "UNSUPPORTED_DRM"
    if candidate.probe_status == "probe_failed":
        return "PROBE_FAILED"
    return str(candidate.probe_status or "UNUSABLE").upper()


def _quality_signature(candidate: Optional[SourceCandidate]) -> Tuple[object, ...]:
    if candidate is None:
        return ()
    return (
        int(candidate.video_width or 0),
        int(candidate.video_height or 0),
        round(float(candidate.video_fps or 0.0), 3),
        int(candidate.video_bitrate_bps or 0),
        str(candidate.video_scan_type or ""),
    )


def _quality_text(candidate: Optional[SourceCandidate]) -> str:
    if candidate is None:
        return "no working candidate"
    width, height = int(candidate.video_width or 0), int(candidate.video_height or 0)
    fps = float(candidate.video_fps or 0.0)
    bitrate = int(candidate.video_bitrate_bps or 0)
    parts: List[str] = []
    if width and height:
        parts.append(f"{width}x{height}")
    if fps:
        parts.append(f"{fps:g}p")
    if bitrate:
        parts.append(f"{round(bitrate / 1000):d} Kbps")
    return " | ".join(parts) if parts else "quality unknown"


def _best_candidate(
    candidates: Sequence[SourceCandidate],
    provider: str,
) -> Optional[SourceCandidate]:
    working = [
        candidate
        for candidate in candidates
        if candidate.launchable and not candidate.ignored
    ]
    if not working:
        return None
    policy = PROVIDER_SELECTION_POLICY.get(provider, DEFAULT_SELECTION_POLICY)
    decision = select_join_candidate(working, policy, now_ts=time.time())
    return decision.selected


def _build_source_observation(
    source_id: str,
    candidates: Sequence[SourceCandidate],
    provider: str,
) -> SourceObservation:
    event_names = tuple(
        dict.fromkeys(
            str(item.entry_title or "").strip()
            for item in candidates
            if str(item.entry_title or "").strip()
        )
    )
    tvg_names = tuple(
        dict.fromkeys(
            str(item.tvg_name or "").strip()
            for item in candidates
            if str(item.tvg_name or "").strip()
        )
    )
    group_titles = tuple(
        dict.fromkeys(
            str(item.group_title or "").strip()
            for item in candidates
            if str(item.group_title or "").strip()
        )
    )
    best = _best_candidate(candidates, provider)
    candidate_states = tuple(sorted(_candidate_state(item) for item in candidates))
    states = set(candidate_states)
    state = "WORKING" if "WORKING" in states else sorted(states)[0] if states else "UNUSABLE"
    return SourceObservation(
        source_id=source_id,
        source_name=_candidate_source_name(candidates[0]) if candidates else source_id,
        candidates=tuple(candidates),
        event_names=event_names,
        tvg_names=tvg_names,
        group_titles=group_titles,
        candidate_states=candidate_states,
        state=state,
        best_candidate=best,
    )


def build_snapshot(
    target_views: Sequence[TargetView],
    candidates_by_target: Mapping[str, Sequence[SourceCandidate]],
    *,
    source_errors: Sequence[str] = (),
    config_messages: Sequence[str] = (),
    coordinator_window: Optional[CoordinatorWindow] = None,
    now: Optional[datetime] = None,
) -> DashboardSnapshot:
    blocks: Dict[Tuple[str, str], IdentityBlock] = {}
    target_by_name = {view.target.name: view.target for view in target_views}

    for target_name, candidates in candidates_by_target.items():
        target = target_by_name[target_name]
        for candidate in candidates:
            provider = str(candidate.extra.get("provider") or "UNKNOWN").upper()
            identity = derive_feed_identity(candidate, provider)
            key = (target.policy, identity.serialized)
            block = blocks.get(key)
            if block is None:
                block = IdentityBlock(policy=target.policy, identity=identity)
                blocks[key] = block
            if target_name not in block.target_names:
                block.target_names.append(target_name)
            observation_key = _candidate_observation_key(candidate)
            existing_index = next(
                (
                    index
                    for index, existing in enumerate(block.candidates)
                    if _candidate_observation_key(existing) == observation_key
                ),
                None,
            )
            if existing_index is None:
                block.candidates.append(candidate)
            else:
                existing = block.candidates[existing_index]
                if existing.ignored and not candidate.ignored:
                    block.candidates[existing_index] = candidate
                elif (
                    existing.ignored == candidate.ignored
                    and candidate.preferred_qualifier_score
                    > existing.preferred_qualifier_score
                ):
                    block.candidates[existing_index] = candidate

    for block in blocks.values():
        by_source: Dict[str, List[SourceCandidate]] = {}
        for candidate in block.candidates:
            by_source.setdefault(_candidate_source_id(candidate), []).append(candidate)
        block.observations = {
            source_id: _build_source_observation(source_id, candidates, block.identity.provider)
            for source_id, candidates in by_source.items()
        }
        block.best_candidate = _best_candidate(block.candidates, block.identity.provider)
        block.overall_state = "AVAILABLE" if block.best_candidate is not None else "UNUSABLE"

    return DashboardSnapshot(
        created_at=now or datetime.now(),
        target_views=tuple(target_views),
        coordinator_window=coordinator_window,
        blocks=blocks,
        source_errors=tuple(source_errors),
        config_messages=tuple(config_messages),
    )


def diff_snapshots(
    previous: Optional[DashboardSnapshot],
    current: DashboardSnapshot,
) -> Tuple[ChangeEvent, ...]:
    if previous is None:
        return ()

    events: List[ChangeEvent] = []
    previous_keys = set(previous.blocks)
    current_keys = set(current.blocks)
    previous_identities = {key[1] for key in previous_keys}
    current_identities = {key[1] for key in current_keys}

    # NEW belongs to the feed identity itself, not to the MANUAL/ALL presentation
    # block. Moving an existing target between policies must not invent a NEW feed.
    for key in current_keys - previous_keys:
        if key[1] not in previous_identities:
            events.append(ChangeEvent("NEW", key, ("feed identity appeared",), beep=True))

    for key in previous_keys - current_keys:
        if key[1] not in current_identities:
            events.append(
                ChangeEvent(
                    "REMOVED",
                    key,
                    ("feed identity no longer present",),
                    beep=False,
                )
            )

    for key in current_keys & previous_keys:
        old = previous.blocks[key]
        new = current.blocks[key]
        old_sources = set(old.observations)
        new_sources = set(new.observations)

        for source_id in sorted(new_sources - old_sources):
            events.append(
                ChangeEvent(
                    "SOURCE+",
                    key,
                    (f"source added: {new.observations[source_id].source_name}",),
                    beep=False,
                )
            )
        for source_id in sorted(old_sources - new_sources):
            events.append(
                ChangeEvent(
                    "SOURCE-",
                    key,
                    (f"source removed: {old.observations[source_id].source_name}",),
                    beep=False,
                )
            )

        metadata_details: List[str] = []
        state_changed = False
        for source_id in sorted(old_sources & new_sources):
            before = old.observations[source_id]
            after = new.observations[source_id]
            if before.event_names != after.event_names:
                metadata_details.append(
                    f"{after.source_name}: Event {_joined(before.event_names)} "
                    f"-> {_joined(after.event_names)}"
                )
            if before.group_titles != after.group_titles:
                metadata_details.append(
                    f"{after.source_name}: Group {_joined(before.group_titles)} "
                    f"-> {_joined(after.group_titles)}"
                )
            if before.tvg_names != after.tvg_names:
                metadata_details.append(
                    f"{after.source_name}: TVG {_joined(before.tvg_names)} "
                    f"-> {_joined(after.tvg_names)}"
                )
            if before.candidate_states != after.candidate_states:
                state_changed = True
                metadata_details.append(
                    f"{after.source_name}: Candidate states "
                    f"{_joined(before.candidate_states)} -> {_joined(after.candidate_states)}"
                )
            elif before.state != after.state:
                state_changed = True
                metadata_details.append(
                    f"{after.source_name}: State {before.state} -> {after.state}"
                )

        if old.overall_state != new.overall_state:
            state_changed = True
            metadata_details.append(f"Identity state {old.overall_state} -> {new.overall_state}")

        if metadata_details:
            events.append(ChangeEvent("UPDATE", key, tuple(metadata_details), beep=True))

        old_quality = _quality_signature(old.best_candidate)
        new_quality = _quality_signature(new.best_candidate)
        if (
            old_quality != new_quality
            and old.best_candidate is not None
            and new.best_candidate is not None
        ):
            policy = PROVIDER_SELECTION_POLICY.get(new.identity.provider, DEFAULT_SELECTION_POLICY)
            old_rank = video_quality_rank(
                old.best_candidate, motion_cap_fps=policy.motion_cap_fps
            )
            new_rank = video_quality_rank(
                new.best_candidate, motion_cap_fps=policy.motion_cap_fps
            )
            if new_rank > old_rank:
                events.append(
                    ChangeEvent(
                        "QUALITY+",
                        key,
                        (
                            "effective quality "
                            f"{_quality_text(old.best_candidate)} -> "
                            f"{_quality_text(new.best_candidate)}",
                        ),
                        beep=True,
                    )
                )
            else:
                events.append(
                    ChangeEvent(
                        "QUALITY-",
                        key,
                        (
                            "effective quality "
                            f"{_quality_text(old.best_candidate)} -> "
                            f"{_quality_text(new.best_candidate)}",
                        ),
                        beep=False,
                    )
                )

    return tuple(events)


def _joined(values: Sequence[str]) -> str:
    return " / ".join(values) if values else "-"


def _event_markers(events: Sequence[ChangeEvent]) -> Dict[Tuple[str, str], List[ChangeEvent]]:
    result: Dict[Tuple[str, str], List[ChangeEvent]] = {}
    for event in events:
        if event.marker in {"REMOVED", "SOURCE-"}:
            continue
        result.setdefault(event.block_key, []).append(event)
    return result


def render_dashboard(
    snapshot: DashboardSnapshot,
    events: Sequence[ChangeEvent],
    display_order: Optional[Mapping[str, Sequence[str]]] = None,
) -> str:
    markers = _event_markers(events)
    lines: List[str] = []
    lines.append("=" * 96)
    lines.append(
        "IDENTITY COORDINATOR — INSPECT / WATCH   "
        f"{snapshot.created_at:%Y-%m-%d %H:%M:%S}"
    )
    lines.append("=" * 96)
    if snapshot.coordinator_window is not None:
        window = snapshot.coordinator_window
        timing = f" from {window.active_from:%Y-%m-%d %H:%M:%S}"
        if window.active_until is not None:
            timing += f" until {window.active_until:%Y-%m-%d %H:%M:%S}"
        lines.append(f"Coordinator: {window.status}{timing}")
    for view in snapshot.target_views:
        timing = ""
        if view.status == "SCHEDULED" and view.active_from is not None:
            timing = f" — starts {view.active_from:%Y-%m-%d %H:%M:%S}"
        elif view.status == "ACTIVE" and view.active_until is not None:
            timing = f" — active until {view.active_until:%Y-%m-%d %H:%M:%S}"
        lines.append(
            f"Target: {view.target.name} | {view.target.policy} | {view.status}{timing}"
        )
    if snapshot.config_messages:
        lines.append("")
        lines.extend(snapshot.config_messages)

    for policy, heading in ((POLICY_ALL, "ALL IDENTITIES"), (POLICY_MANUAL, "MANUAL")):
        policy_blocks = [block for key, block in snapshot.blocks.items() if key[0] == policy]
        if display_order is not None:
            position = {
                identity: index
                for index, identity in enumerate(display_order.get(policy, ()))
            }
            policy_blocks.sort(
                key=lambda block: position.get(block.identity.serialized, len(position))
            )
        if not policy_blocks:
            continue
        lines.append("")
        lines.append(f"--- {heading} ---")
        for index, block in enumerate(policy_blocks, start=1):
            key = (block.policy, block.identity.serialized)
            block_events = markers.get(key, [])
            marker_text = " ".join(f"[{event.marker}]" for event in block_events)
            if marker_text:
                marker_text += " "
            lines.append(
                f"{marker_text}[{index}] {block.overall_state} {block.identity.serialized} "
                f"| Targets: {', '.join(block.target_names)} | Sources: {len(block.observations)}"
            )
            lines.append(f"    Best: {_quality_text(block.best_candidate)}")
            for source in block.observations.values():
                for candidate in source.candidates:
                    candidate_state = _candidate_state(candidate)
                    on_off = "ON" if candidate_state == "WORKING" else "OFF"
                    event_name = candidate.entry_title or candidate.tvg_name or "-"
                    group_name = candidate.group_title or "-"
                    context_text = " | CONTEXT" if candidate.ignored else ""
                    lines.append(
                        f"    [{on_off}] {event_name} | {group_name} "
                        f"| {_quality_text(candidate if candidate.quality_known else None)} "
                        f"| {candidate_state}{context_text} | {source.source_name}"
                    )
                    if candidate_state != "WORKING" or candidate.ignored:
                        reason = candidate.reason
                        if candidate.ignored and not reason:
                            reason = (
                                "same feed identity context; this source's current primary "
                                "metadata does not match the target"
                            )
                        if candidate_state == "AUTH_UNKNOWN" and not reason:
                            reason = "authorization expiry is unknown for this provider profile"
                        if reason:
                            lines.append(f"        Reason: {reason}")
            for event in block_events:
                for detail in event.details:
                    lines.append(f"        [{event.marker}] {detail}")

    if snapshot.source_errors:
        lines.append("")
        lines.append("Source warnings:")
        for error in snapshot.source_errors:
            lines.append(f"  - {error}")

    if not snapshot.blocks:
        lines.append("")
        lines.append("No qualifying identities in the current active target scope.")
    return "\n".join(lines)



def update_display_order(
    previous_order: Mapping[str, Sequence[str]],
    snapshot: DashboardSnapshot,
) -> Dict[str, List[str]]:
    """Keep existing identities stable; place genuinely new identities at top."""
    result: Dict[str, List[str]] = {}
    for policy in (POLICY_ALL, POLICY_MANUAL):
        current = [
            key[1]
            for key in snapshot.blocks
            if key[0] == policy
        ]
        current_set = set(current)
        old = [identity for identity in previous_order.get(policy, ()) if identity in current_set]
        new = [identity for identity in current if identity not in old]
        result[policy] = new + old
    return result


def _terminal_is_interactive() -> bool:
    try:
        return bool(sys.stdout.isatty())
    except Exception:
        return False


def _clear_dashboard_terminal() -> None:
    if not _terminal_is_interactive():
        return
    os.system("cls" if os.name == "nt" else "clear")

def _write_log(path: Path, text: str) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(text.rstrip() + "\n")


def _beep(events: Sequence[ChangeEvent]) -> None:
    if winsound is None or not any(event.beep for event in events):
        return
    try:
        winsound.Beep(880, 180)
        winsound.Beep(1175, 220)
    except Exception:
        pass


def run_once(
    config_state: CoordinatorConfigState,
    previous: Optional[DashboardSnapshot],
) -> Tuple[DashboardSnapshot, Tuple[ChangeEvent, ...]]:
    now = datetime.now()
    config_messages, _ = config_state.reload(now)
    window = config_state.coordinator_window(now)
    target_views = config_state.target_views(
        now,
        coordinator_active=(window.status == "ACTIVE"),
    )
    raw = config_state.raw_config or {}
    if window.status == "ACTIVE":
        candidates_by_target, source_errors = acquire_active_targets(raw, target_views)
    else:
        candidates_by_target, source_errors = {}, ()
    snapshot = build_snapshot(
        target_views,
        candidates_by_target,
        source_errors=source_errors,
        config_messages=config_messages,
        coordinator_window=window,
        now=now,
    )
    events = diff_snapshots(previous, snapshot)
    return snapshot, events


def next_watch_sleep_seconds(
    snapshot: DashboardSnapshot,
    refresh_interval_sec: float,
    *,
    now: Optional[datetime] = None,
) -> float:
    """Return the next watch delay without sleeping past known timing boundaries."""
    current = now or datetime.now()
    delays = [max(1.0, float(refresh_interval_sec))]

    window = snapshot.coordinator_window
    if window is not None:
        if window.status == "WAITING" and window.active_from > current:
            delays.append(max(1.0, (window.active_from - current).total_seconds()))
        elif (
            window.status == "ACTIVE"
            and window.active_until is not None
            and window.active_until > current
        ):
            delays.append(max(1.0, (window.active_until - current).total_seconds()))

    for view in snapshot.target_views:
        if (
            view.status == "SCHEDULED"
            and view.active_from is not None
            and view.active_from > current
        ):
            delays.append(max(1.0, (view.active_from - current).total_seconds()))
        elif (
            view.status == "ACTIVE"
            and view.active_until is not None
            and view.active_until > current
        ):
            delays.append(max(1.0, (view.active_until - current).total_seconds()))

    return min(delays)


def run(config_path: Path, *, once: bool = False) -> int:
    state = CoordinatorConfigState(config_path)
    previous: Optional[DashboardSnapshot] = None
    display_order: Dict[str, List[str]] = {POLICY_ALL: [], POLICY_MANUAL: []}
    started = datetime.now()
    log_path = Path.cwd() / f"IDENTITY_COORDINATOR_{started:%Y%m%d_%H%M%S}.log"

    while True:
        try:
            snapshot, events = run_once(state, previous)
        except KeyboardInterrupt:
            print("\nIdentity Coordinator stopped by user.")
            return 0
        except Exception as error:
            message = f"Identity Coordinator scan failed: {type(error).__name__}: {error}"
            print(message)
            _write_log(log_path, f"{datetime.now():%Y-%m-%d %H:%M:%S} {message}")
            if once:
                return 1
            time.sleep(max(5.0, float(state.refresh_interval_sec)))
            continue

        errors_changed = previous is None or snapshot.source_errors != previous.source_errors
        meaningful = (
            previous is None
            or bool(events)
            or bool(snapshot.config_messages)
            or errors_changed
        )
        display_order = update_display_order(display_order, snapshot)
        if meaningful:
            text = render_dashboard(snapshot, events, display_order)
            _clear_dashboard_terminal()
            print(text)
            print(f"\nLog: {log_path}")
            _write_log(log_path, text)
            if events:
                for event in events:
                    _write_log(
                        log_path,
                        (
                            f"CHANGE {event.marker} {event.block_key[1]} :: "
                            f"{'; '.join(event.details)}"
                        ),
                    )
            _beep(events)
        else:
            print(f"{datetime.now():%H:%M:%S} Watch scan complete — no meaningful change")

        previous = snapshot
        if (
            snapshot.coordinator_window is not None
            and snapshot.coordinator_window.status == "EXPIRED"
        ):
            print("Identity Coordinator schedule complete.")
            return 0
        if once:
            return 0

        sleep_for = next_watch_sleep_seconds(
            snapshot,
            state.refresh_interval_sec,
            now=datetime.now(),
        )
        try:
            time.sleep(sleep_for)
        except KeyboardInterrupt:
            print("\nIdentity Coordinator stopped by user.")
            return 0


def parse_args(argv: Optional[Sequence[str]] = None):
    parser = argparse.ArgumentParser(description="Inspect/watch identity-based recorder targets")
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="override recorder_dynamic_user_config.py path",
    )
    parser.add_argument("--once", action="store_true", help="perform one scan and exit")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    config_path = args.config or _default_config_path()
    if not config_path.is_file():
        raise RuntimeError(f"Recorder config not found: {config_path}")
    return run(config_path, once=args.once)


if __name__ == "__main__":
    raise SystemExit(main())
