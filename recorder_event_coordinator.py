"""Identity Coordinator — Inspect / Watch / MANUAL launch orchestration.

The Coordinator discovers qualifying identities, presents watch state, and for
MANUAL policy launches an independently running mature-recorder worker only
after the user explicitly chooses an identity.

State/change intelligence, launch planning, worker creation, registry ownership,
and terminal presentation stay behind focused modules so this entry point
remains orchestration-focused.
"""

from __future__ import annotations

import argparse
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
import runpy
import sys
import threading
import time
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

try:
    import msvcrt
except ImportError:  # pragma: no cover - Windows is the production terminal
    msvcrt = None

from recorder_runtime import sound as runtime_sound
from recorder_runtime.identity_launch import IdentityLaunchRequest
from recorder_runtime.paths import build_recorder_output_paths
from recorder_runtime.sound import SoundSnoozeState

from recorder_coordinator.launch import build_manual_launch_plan
from recorder_coordinator.registry import IdentityRegistryStore
from recorder_coordinator.worker import launch_identity_worker
from recorder_coordinator.models import (
    ChangeEvent,
    CoordinatorWindow,
    DashboardSnapshot,
    IdentityTarget,
    POLICY_ALL,
    POLICY_MANUAL,
    TargetRuntime,
    TargetView,
    VALID_POLICIES,
)
from recorder_coordinator.snapshot import (
    build_snapshot,
    compact_source_name,
    diff_snapshots,
)
from recorder_coordinator.terminal import (
    beep,
    clear_dashboard_terminal,
    clear_live_status_line,
    render_coordinator_controls,
    render_dashboard,
    render_header,
    render_manual_record_menu,
    render_sound_snooze_menu,
    set_live_status_line,
    update_display_order,
    watch_status_text,
    write_log,
)
from recorder_source.discovery import (
    fetch_playlist_documents,
    parse_playlist_text,
    probe_candidates,
    resolve_playlist_source_freshness,
)
from recorder_source.identity import derive_feed_identity
from recorder_source.matching import evaluate_match, make_match_definition
from recorder_source.models import PlaylistSourceSpec, SourceCandidate
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


def _coordinator_output_paths(config_path: Path):
    raw = runpy.run_path(str(config_path))
    return build_recorder_output_paths(raw.get("RECORDING_OUTPUT_DIR"))


def _coordinator_log_path(
    config_path: Path,
    started: datetime,
    *,
    output_paths=None,
) -> Path:
    paths = output_paths or _coordinator_output_paths(config_path)
    paths.coordinator_logs.mkdir(parents=True, exist_ok=True)
    return (
        paths.coordinator_logs
        / f"IDENTITY_COORDINATOR_{started:%Y%m%d_%H%M%S}.log"
    )


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


def _observation_key(candidate: SourceCandidate) -> Tuple[object, ...]:
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


def _freshness_eligible_identity_keys(
    candidates: Sequence[SourceCandidate],
) -> Set[str]:
    """Return identities that remain target-eligible after freshness review.

    Matching rows are marked ignored=False; same-identity context rows whose
    primary metadata does not match are ignored=True. Known freshest metadata
    can disqualify an identity only when every observation at the newest
    credible timestamp is non-matching. Unknown freshness or a tie containing
    both matching and non-matching observations is conservative: keep.
    """
    by_identity: Dict[str, List[SourceCandidate]] = {}
    for candidate in candidates:
        by_identity.setdefault(_identity_serialized(candidate), []).append(candidate)

    eligible: Set[str] = set()
    for identity_key, identity_candidates in by_identity.items():
        known = []
        for candidate in identity_candidates:
            value = candidate.extra.get("source_freshness_ts")
            try:
                timestamp = float(value)
            except (TypeError, ValueError):
                continue
            known.append((timestamp, candidate))

        if not known:
            eligible.add(identity_key)
            continue

        newest = max(timestamp for timestamp, _ in known)
        freshest = [
            candidate
            for timestamp, candidate in known
            if timestamp == newest
        ]
        if any(not candidate.ignored for candidate in freshest):
            eligible.add(identity_key)

    return eligible


def acquire_active_targets(
    raw_config: Mapping[str, object],
    target_views: Sequence[TargetView],
    *,
    progress_callback: Optional[Callable[[str], None]] = None,
    source_freshness_registry: Optional[Dict[str, Mapping[str, object]]] = None,
) -> Tuple[Dict[str, Tuple[SourceCandidate, ...]], Tuple[str, ...]]:
    active = [view for view in target_views if view.status == "ACTIVE"]
    if not active:
        return {}, ()

    source_specs_by_key: Dict[Tuple[str, str, str], PlaylistSourceSpec] = {}
    target_group_sources: Dict[Tuple[str, str], Tuple[PlaylistSourceSpec, ...]] = {}
    target_group_context: Dict[Tuple[str, str], str] = {}
    for view in active:
        for group in view.target.source_groups:
            context_group = _source_context_group_for_target(view.target, group)
            specs = sources_for_group(
                raw_config,
                group,
                context_group=context_group,
            )
            target_group_sources[(view.target.name, group)] = specs
            target_group_context[(view.target.name, group)] = context_group
            for spec in specs:
                source_specs_by_key[(spec.url, spec.provider, spec.group)] = spec

    # Fetch each configured URL once. Parsing/normalization is performed once per
    # source/provider context, then target matching is applied afterward.
    url_source_specs: Dict[str, PlaylistSourceSpec] = {}
    for spec in source_specs_by_key.values():
        url_source_specs.setdefault(spec.url, spec)
    source_specs = tuple(url_source_specs.values())
    if progress_callback is None:
        documents, fetch_errors, fetch_diagnostics = fetch_playlist_documents(source_specs)
    else:
        progress_callback(f"Scanning playlists 0/{len(source_specs)}")
        documents, fetch_errors, fetch_diagnostics = fetch_playlist_documents(
            source_specs,
            progress_callback=(
                lambda done, total: progress_callback(
                    f"Scanning playlists {done}/{total}"
                )
            ),
        )

    freshness_by_url: Dict[str, Mapping[str, object]] = {}
    freshness_now = time.time()

    def resolve_freshness(spec: PlaylistSourceSpec):
        text = documents.get(spec.url)
        if text is None:
            return spec.url, None
        previous_freshness = (
            source_freshness_registry.get(spec.url)
            if source_freshness_registry is not None
            else None
        )
        return (
            spec.url,
            resolve_playlist_source_freshness(
                spec.url,
                text,
                fetch_diagnostics.get(spec.url),
                previous=previous_freshness,
                now_ts=freshness_now,
            ),
        )

    # GitHub commit lookups are independent network calls. Resolve source
    # freshness concurrently so one slow repository does not serialize startup.
    freshness_workers = min(6, max(1, len(source_specs)))
    with ThreadPoolExecutor(
        max_workers=freshness_workers,
        thread_name_prefix="source_freshness",
    ) as executor:
        futures = [executor.submit(resolve_freshness, spec) for spec in source_specs]
        for future in as_completed(futures):
            source_url, freshness = future.result()
            if freshness is None:
                continue
            freshness_by_url[source_url] = freshness
            if source_freshness_registry is not None:
                source_freshness_registry[source_url] = freshness

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
        freshness = freshness_by_url.get(
            spec.url,
            {"timestamp": None, "source": "unknown"},
        )
        parsed_by_key[key] = tuple(
            replace(
                candidate,
                extra={
                    **dict(candidate.extra),
                    "source_freshness_ts": freshness.get("timestamp"),
                    "source_freshness_source": freshness.get("source") or "unknown",
                },
            )
            for candidate in result.candidates
        )

    raw_candidates_by_target: Dict[str, List[SourceCandidate]] = {
        view.target.name: [] for view in active
    }
    matched_identity_keys: Dict[str, Set[str]] = {
        view.target.name: set() for view in active
    }

    # First establish which identities genuinely qualify for each target.
    for view in active:
        target = view.target
        matched_observation_keys: Set[Tuple[object, ...]] = set()
        for group in target.source_groups:
            context_group = target_group_context[(target.name, group)]
            definition = _match_definition_for_target(target, context_group)
            for spec in target_group_sources[(target.name, group)]:
                key = (spec.url, spec.provider, spec.group)
                for candidate in parsed_by_key.get(key, ()):
                    matched = _matching_candidate(candidate, definition)
                    if matched is None:
                        continue
                    probe_key = _observation_key(matched)
                    if probe_key in matched_observation_keys:
                        continue
                    matched_observation_keys.add(probe_key)
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
            _observation_key(candidate) for candidate in raw_candidates_by_target[target.name]
        }
        for group in target.source_groups:
            context_group = target_group_context[(target.name, group)]
            definition = _match_definition_for_target(target, context_group)
            for spec in target_group_sources[(target.name, group)]:
                key = (spec.url, spec.provider, spec.group)
                for candidate in parsed_by_key.get(key, ()):
                    if _observation_key(candidate) in existing_keys:
                        continue
                    if _identity_serialized(candidate) not in target_identities:
                        continue
                    if _matching_candidate(candidate, definition) is not None:
                        continue
                    context = _context_candidate_for_target(
                        candidate,
                        target,
                        context_group,
                    )
                    if context is None:
                        continue
                    raw_candidates_by_target[target.name].append(context)
                    existing_keys.add(_observation_key(context))

    # A stale matching row must not keep an identity eligible when newer
    # credible metadata for that same identity has moved to another event.
    # Ambiguous ties and identities with no usable freshness remain visible.
    for target_name, candidates in raw_candidates_by_target.items():
        eligible_identity_keys = _freshness_eligible_identity_keys(candidates)
        raw_candidates_by_target[target_name] = [
            candidate
            for candidate in candidates
            if _identity_serialized(candidate) in eligible_identity_keys
        ]

    # Preserve each source observation separately here. The shared probing
    # boundary groups equivalent effective streams internally, probes once, and
    # copies the probe facts back without erasing source provenance.
    unique_by_key: Dict[Tuple[object, ...], SourceCandidate] = {}
    for candidates in raw_candidates_by_target.values():
        for candidate in candidates:
            unique_by_key.setdefault(_observation_key(candidate), candidate)
    probe_pool = tuple(unique_by_key.values())
    if progress_callback is None:
        probed = probe_candidates(probe_pool)
    else:
        progress_callback(f"Checking candidates 0/{len(probe_pool)}")
        probed = probe_candidates(
            probe_pool,
            progress_callback=(
                lambda done, total: progress_callback(
                    f"Checking candidates {done}/{total}"
                )
            ),
        )
    probed_by_key = {_observation_key(candidate): candidate for candidate in probed}

    final: Dict[str, Tuple[SourceCandidate, ...]] = {}
    for name, candidates in raw_candidates_by_target.items():
        resolved = tuple(
            probed_by_key.get(_observation_key(candidate), candidate)
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



def run_once(
    config_state: CoordinatorConfigState,
    previous: Optional[DashboardSnapshot],
    *,
    progress_callback: Optional[Callable[[str], None]] = None,
    context_callback: Optional[Callable[[DashboardSnapshot], None]] = None,
    row_update_registry: Optional[Dict[Tuple[object, ...], Tuple[Tuple[object, ...], datetime]]] = None,
    source_freshness_registry: Optional[Dict[str, Mapping[str, object]]] = None,
) -> Tuple[DashboardSnapshot, Tuple[ChangeEvent, ...]]:
    now = datetime.now()
    config_messages, _ = config_state.reload(now)
    window = config_state.coordinator_window(now)
    target_views = config_state.target_views(
        now,
        coordinator_active=(window.status == "ACTIVE"),
    )
    raw = config_state.raw_config or {}
    if context_callback is not None:
        context_callback(
            DashboardSnapshot(
                created_at=now,
                target_views=tuple(target_views),
                coordinator_window=window,
                blocks={},
            )
        )
    if window.status == "ACTIVE":
        candidates_by_target, source_errors = acquire_active_targets(
            raw,
            target_views,
            progress_callback=progress_callback,
            source_freshness_registry=source_freshness_registry,
        )
    else:
        candidates_by_target, source_errors = {}, ()

    if progress_callback is not None:
        progress_callback("Building identities...")

    snapshot = build_snapshot(
        target_views,
        candidates_by_target,
        source_errors=source_errors,
        config_messages=config_messages,
        coordinator_window=window,
        now=now,
        row_update_registry=row_update_registry,
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


def _manual_record_choices(
    snapshot: DashboardSnapshot,
    display_order: Mapping[str, Sequence[str]],
    registry_store: IdentityRegistryStore,
) -> Tuple[str, ...]:
    registry = registry_store.read()
    entries = registry.get("entries")
    blocked = set(entries) if isinstance(entries, Mapping) else set()

    ordered = list(display_order.get(POLICY_MANUAL, ()))
    ordered.extend(
        identity_key
        for policy, identity_key in snapshot.blocks
        if policy == POLICY_MANUAL and identity_key not in ordered
    )
    return tuple(
        identity_key
        for identity_key in ordered
        if (
            identity_key not in blocked
            and (POLICY_MANUAL, identity_key) in snapshot.blocks
            and snapshot.blocks[(POLICY_MANUAL, identity_key)].best_candidate
            is not None
        )
    )


def _launch_manual_identity(
    snapshot: DashboardSnapshot,
    identity_key: str,
    *,
    registry_session_id: str,
    registry_store: IdentityRegistryStore,
):
    plan = build_manual_launch_plan(snapshot, identity_key)
    request = IdentityLaunchRequest(
        registry_session_id=registry_session_id,
        identity_key=plan.identity.serialized,
        provider=plan.identity.provider,
        selected_source_group=plan.selected_source_group,
        selected_candidate=plan.selected_candidate,
        target_intents=plan.target_intents,
        recording_duration_min=plan.recording_duration_min,
        base_name=plan.base_name,
    )
    result = launch_identity_worker(request, registry_store)
    return plan, result


def _command_reader(
    command_queue: "queue.Queue[str]",
    stop_event: threading.Event,
) -> None:
    if os.name == "nt" and msvcrt is not None:
        number_buffer = ""
        while not stop_event.is_set():
            if not msvcrt.kbhit():
                time.sleep(0.05)
                continue
            key = msvcrt.getwch()
            if key in ("\x00", "\xe0"):
                if msvcrt.kbhit():
                    msvcrt.getwch()
                continue
            if key.isdigit():
                number_buffer += key
                command_queue.put(f"__NUMBER_BUFFER__:{number_buffer}")
                continue
            if key == "\b":
                number_buffer = number_buffer[:-1]
                command_queue.put(f"__NUMBER_BUFFER__:{number_buffer}")
                continue
            if key in ("\r", "\n"):
                command_queue.put(f"__NUMBER_SUBMIT__:{number_buffer}")
                number_buffer = ""
                continue

            number_buffer = ""
            if key.casefold() in {"p", "r", "i", "s", "m", "f", "u"}:
                command_queue.put(key.casefold())
            elif key == "\x1b":
                command_queue.put("__ESC__")
            elif key == "\x03":
                command_queue.put("__CTRL_C__")
        return

    while not stop_event.is_set():
        try:
            value = input().strip()
        except (EOFError, KeyboardInterrupt):
            return
        command_queue.put(value)


def _has_visible_transient(
    snapshot: DashboardSnapshot,
    events: Sequence[ChangeEvent],
) -> bool:
    if snapshot.config_messages:
        return True
    return any(
        event.marker not in {"REMOVED", "SOURCE-"}
        for event in events
    )


def run(config_path: Path, *, once: bool = False) -> int:
    state = CoordinatorConfigState(config_path)
    previous: Optional[DashboardSnapshot] = None
    display_order: Dict[str, List[str]] = {POLICY_ALL: [], POLICY_MANUAL: []}
    row_update_registry: Dict[Tuple[object, ...], Tuple[Tuple[object, ...], datetime]] = {}
    source_freshness_registry: Dict[str, Mapping[str, object]] = {}
    started = datetime.now()
    output_paths = _coordinator_output_paths(config_path)
    log_path = _coordinator_log_path(
        config_path,
        started,
        output_paths=output_paths,
    )
    registry_store = IdentityRegistryStore(output_paths)
    registry_status = registry_store.prepare_session()
    write_log(
        log_path,
        (
            f"IDENTITY REGISTRY {registry_status.action} "
            f"session={registry_status.session_id}"
        ),
    )
    if registry_status.unresolved_identities:
        unresolved = ", ".join(registry_status.unresolved_identities)
        warning = (
            "IDENTITY REGISTRY WARNING: unresolved ownership remains for "
            f"{unresolved}; identity-based launch must remain blocked until "
            "that registry state is resolved."
        )
        print(warning)
        write_log(log_path, warning)

    command_queue: "queue.Queue[str]" = queue.Queue()
    stop_event = threading.Event()
    if not once:
        threading.Thread(
            target=_command_reader,
            args=(command_queue, stop_event),
            daemon=True,
        ).start()

    force_refresh = True
    next_refresh_monotonic = 0.0
    last_scan_wall_time: Optional[float] = None
    dashboard_has_transient = False
    sound_state = SoundSnoozeState()
    sound_menu_open = False
    record_menu_open = False
    record_choices: Tuple[str, ...] = ()

    try:
        while not stop_event.is_set():
            if sound_state.sound_snooze_mode == "timed":
                before = sound_state.sound_snooze_mode
                if not runtime_sound.is_sound_snoozed(
                    sound_state,
                    indefinite_modes=("coordinator_run",),
                ) and before == "timed":
                    write_log(
                        log_path,
                        f"{datetime.now():%Y-%m-%d %H:%M:%S} "
                        "SOUND SNOOZE ENDED — 15-minute snooze expired; sound restored",
                    )

            now_monotonic = time.monotonic()
            if force_refresh or now_monotonic >= next_refresh_monotonic:
                try:
                    def show_initial_header(preview: DashboardSnapshot) -> None:
                        clear_live_status_line()
                        clear_dashboard_terminal()
                        print(
                            render_header(
                                preview,
                                config_path=config_path,
                                refresh_interval_sec=state.refresh_interval_sec,
                            )
                        )

                    snapshot, events = run_once(
                        state,
                        previous,
                        progress_callback=(None if once else set_live_status_line),
                        context_callback=(
                            show_initial_header
                            if previous is None and not once
                            else None
                        ),
                        row_update_registry=row_update_registry,
                        source_freshness_registry=source_freshness_registry,
                    )
                except KeyboardInterrupt:
                    raise
                except Exception as error:
                    clear_live_status_line()
                    message = (
                        f"Identity Coordinator scan failed: "
                        f"{type(error).__name__}: {error}"
                    )
                    print(message)
                    write_log(
                        log_path,
                        f"{datetime.now():%Y-%m-%d %H:%M:%S} {message}",
                    )
                    if once:
                        return 1
                    next_refresh_monotonic = (
                        time.monotonic() + max(5.0, float(state.refresh_interval_sec))
                    )
                    last_scan_wall_time = time.time()
                    force_refresh = False
                    set_live_status_line(
                        watch_status_text(last_scan_wall_time, next_refresh_monotonic)
                    )
                    continue

                errors_changed = (
                    previous is None
                    or snapshot.source_errors != previous.source_errors
                )
                header_changed = (
                    previous is None
                    or snapshot.target_views != previous.target_views
                    or snapshot.coordinator_window != previous.coordinator_window
                )
                meaningful = (
                    previous is None
                    or bool(events)
                    or bool(snapshot.config_messages)
                    or errors_changed
                    or header_changed
                )
                clear_transient_only = (not meaningful) and dashboard_has_transient

                display_order = update_display_order(display_order, snapshot)
                if meaningful or clear_transient_only:
                    clear_live_status_line()
                    terminal_events = events if meaningful else ()
                    terminal_text = render_dashboard(
                        snapshot,
                        terminal_events,
                        display_order,
                        config_path=config_path,
                        refresh_interval_sec=state.refresh_interval_sec,
                        use_color=True,
                    )
                    clear_dashboard_terminal()
                    print(terminal_text)

                    if meaningful:
                        log_text = render_dashboard(
                            snapshot,
                            terminal_events,
                            display_order,
                            config_path=config_path,
                            refresh_interval_sec=state.refresh_interval_sec,
                            use_color=False,
                        )
                        write_log(log_path, log_text)
                        for event in events:
                            write_log(
                                log_path,
                                (
                                    f"CHANGE {event.marker} {event.block_key[1]} :: "
                                    f"{'; '.join(event.details)}"
                                ),
                            )
                        beep(events, sound_state)

                    dashboard_has_transient = (
                        _has_visible_transient(snapshot, terminal_events)
                        if meaningful
                        else False
                    )

                previous = snapshot
                if (
                    snapshot.coordinator_window is not None
                    and snapshot.coordinator_window.status == "EXPIRED"
                ):
                    clear_live_status_line()
                    print("Identity Coordinator schedule complete.")
                    return 0
                if once:
                    return 0

                last_scan_wall_time = time.time()
                sleep_for = next_watch_sleep_seconds(
                    snapshot,
                    state.refresh_interval_sec,
                    now=datetime.now(),
                )
                next_refresh_monotonic = time.monotonic() + sleep_for
                force_refresh = False
                set_live_status_line(
                    watch_status_text(last_scan_wall_time, next_refresh_monotonic)
                )

            timeout = max(
                0.1,
                min(0.5, next_refresh_monotonic - time.monotonic()),
            )
            try:
                command = command_queue.get(timeout=timeout)
            except queue.Empty:
                if next_refresh_monotonic > 0:
                    set_live_status_line(
                        watch_status_text(last_scan_wall_time, next_refresh_monotonic)
                    )
                continue

            normalized = " ".join(command.split()).casefold()
            if normalized == "__ctrl_c__":
                clear_live_status_line()
                print("\nIdentity Coordinator stopped by user.")
                return 0
            if sound_menu_open:
                if normalized in {"__esc__", "esc", "cancel"}:
                    sound_menu_open = False
                    clear_live_status_line()
                    print("Sound control cancelled; sound state is unchanged.")
                elif normalized in {"m", "15", "15m"}:
                    runtime_sound.set_timed_sound_snooze(
                        sound_state,
                        duration_sec=15 * 60.0,
                    )
                    sound_menu_open = False
                    clear_live_status_line()
                    message = (
                        "Sound snoozed for 15 minutes. Coordinator scanning "
                        "and change detection continue normally."
                    )
                    print(message)
                    write_log(
                        log_path,
                        f"{datetime.now():%Y-%m-%d %H:%M:%S} SOUND SNOOZE — 15 minutes",
                    )
                elif normalized in {"f", "full"}:
                    runtime_sound.set_indefinite_sound_snooze(
                        sound_state,
                        "coordinator_run",
                    )
                    sound_menu_open = False
                    clear_live_status_line()
                    message = (
                        "Sound snoozed for the full Coordinator run. Coordinator "
                        "scanning and change detection continue normally."
                    )
                    print(message)
                    write_log(
                        log_path,
                        f"{datetime.now():%Y-%m-%d %H:%M:%S} "
                        "SOUND SNOOZE — full Coordinator run",
                    )
                elif normalized in {"u", "unsnooze", "restore"}:
                    runtime_sound.clear_sound_snooze(sound_state)
                    sound_menu_open = False
                    clear_live_status_line()
                    print("Sound restored.")
                    write_log(
                        log_path,
                        f"{datetime.now():%Y-%m-%d %H:%M:%S} SOUND RESTORED",
                    )
                else:
                    set_live_status_line("Select M/F/U or Esc")
                    continue

                if next_refresh_monotonic > 0:
                    set_live_status_line(
                        watch_status_text(last_scan_wall_time, next_refresh_monotonic)
                    )
                continue

            if record_menu_open:
                if normalized in {"__esc__", "esc", "cancel"}:
                    record_menu_open = False
                    record_choices = ()
                    clear_live_status_line()
                    print("Record selection cancelled.")
                else:
                    number_text = ""
                    if command.startswith("__NUMBER_BUFFER__:"):
                        number_text = command.split(":", 1)[1]
                        set_live_status_line(
                            f"Enter identity number: {number_text}"
                        )
                        continue
                    if command.startswith("__NUMBER_SUBMIT__:"):
                        number_text = command.split(":", 1)[1]
                    elif normalized.isdigit():
                        number_text = normalized
                    else:
                        set_live_status_line(
                            "Enter identity number and press Enter, or Esc to cancel"
                        )
                        continue

                    if not number_text:
                        set_live_status_line(
                            "Enter identity number and press Enter, or Esc to cancel"
                        )
                        continue

                    selected_number = int(number_text)
                    if not 1 <= selected_number <= len(record_choices):
                        set_live_status_line(
                            f"Choose 1-{len(record_choices)} or Esc"
                        )
                        continue

                    identity_key = record_choices[selected_number - 1]
                    record_menu_open = False
                    record_choices = ()
                    clear_live_status_line()
                    try:
                        if previous is None:
                            raise RuntimeError(
                                "Coordinator has no completed discovery snapshot yet"
                            )
                        plan, launch_result = _launch_manual_identity(
                            previous,
                            identity_key,
                            registry_session_id=registry_status.session_id,
                            registry_store=registry_store,
                        )
                        message = (
                            f"Recording launched: {plan.base_name} "
                            f"(PID {launch_result.pid})"
                        )
                        print(message)
                        write_log(
                            log_path,
                            (
                                f"{datetime.now():%Y-%m-%d %H:%M:%S} "
                                f"MANUAL LAUNCH — {plan.identity.serialized} — "
                                f"{plan.base_name} — PID {launch_result.pid}"
                            ),
                        )
                    except Exception as error:
                        message = (
                            "Record launch failed: "
                            f"{type(error).__name__}: {error}"
                        )
                        print(message)
                        write_log(
                            log_path,
                            (
                                f"{datetime.now():%Y-%m-%d %H:%M:%S} "
                                f"MANUAL LAUNCH FAILED — {identity_key} — "
                                f"{type(error).__name__}: {error}"
                            ),
                        )

                    force_refresh = True
                    continue

                if next_refresh_monotonic > 0:
                    set_live_status_line(
                        watch_status_text(last_scan_wall_time, next_refresh_monotonic)
                    )
                continue

            if normalized in {"p", "record"}:
                if previous is None:
                    set_live_status_line(
                        "No completed discovery snapshot yet"
                    )
                    continue
                try:
                    record_choices = _manual_record_choices(
                        previous,
                        display_order,
                        registry_store,
                    )
                except Exception as error:
                    clear_live_status_line()
                    print(
                        "Could not read identity registry for Record: "
                        f"{type(error).__name__}: {error}"
                    )
                    continue
                if not record_choices:
                    clear_live_status_line()
                    print("No MANUAL identities are currently available to record.")
                    if next_refresh_monotonic > 0:
                        set_live_status_line(
                            watch_status_text(
                                last_scan_wall_time,
                                next_refresh_monotonic,
                            )
                        )
                    continue

                record_menu_open = True
                clear_live_status_line()
                print(render_manual_record_menu(previous, record_choices))
                set_live_status_line(
                    "Enter identity number and press Enter, or Esc to cancel"
                )
                continue

            if normalized in {"r", "refresh"}:
                force_refresh = True
                continue

            if normalized in {"i", "info"}:
                clear_live_status_line()
                print(render_coordinator_controls(sound_state))
                if next_refresh_monotonic > 0:
                    set_live_status_line(
                        watch_status_text(last_scan_wall_time, next_refresh_monotonic)
                    )
                continue

            if normalized in {"s", "sound"}:
                sound_menu_open = True
                clear_live_status_line()
                print(render_sound_snooze_menu(sound_state))
                set_live_status_line("Select M/F/U or Esc")
                continue

            if normalized:
                set_live_status_line(
                    "Use p=record | i=info | s=sound | r=refresh | Ctrl+C=exit"
                )
            elif next_refresh_monotonic > 0:
                set_live_status_line(
                    watch_status_text(last_scan_wall_time, next_refresh_monotonic)
                )

    except KeyboardInterrupt:
        clear_live_status_line()
        print("\nIdentity Coordinator stopped by user.")
        return 0
    finally:
        stop_event.set()


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
