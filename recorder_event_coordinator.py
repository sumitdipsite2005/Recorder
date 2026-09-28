"""Identity Coordinator — Inspect / Watch / identity launch orchestration.

The Coordinator discovers qualifying identities and presents watch state.
MANUAL launches only after user selection; ALL_IDENTITIES automatically launches
each newly eligible canonical identity through the same worker path.

State/change intelligence, launch planning, worker creation, registry ownership,
and terminal presentation stay behind focused modules so this entry point
remains orchestration-focused.
"""

from __future__ import annotations

import argparse
import os
import queue
import runpy
import sys
import threading
import time
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

try:
    import msvcrt
except ImportError:  # pragma: no cover - Windows is the production terminal
    msvcrt = None

from recorder_runtime import sound as runtime_sound
from recorder_runtime.identity_launch import (
    FrozenRecoveryScope,
    FrozenTargetIntent,
    IdentityLaunchRequest,
)
from recorder_runtime.identity_status import IdentityRuntimeStatusStore
from recorder_runtime.paths import build_recorder_output_paths
from recorder_runtime.sound import SoundSnoozeState

from recorder_coordinator.acquisition import acquire_active_targets
from recorder_coordinator.configuration import (
    DEFAULT_REFRESH_INTERVAL_SEC,
    CoordinatorConfigState,
    _match_definition_for_target,
    _source_context_group_for_target,
    diff_target_configs,
    parse_targets,
    sources_for_group,
    target_view,
    validate_target_source_scopes,
)
from recorder_coordinator.launch import (
    build_identity_launch_plan,
    build_manual_launch_plan,
)
from recorder_runtime.registry import IdentityLaunchBlocked, IdentityRegistryStore
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
    update_source_reference_registry,
    watch_status_text,
    write_log,
)
from recorder_source.models import SourceCandidate
from recorder_source.policy import (
    PLAYLIST_GROUP_MATCH_MODES as GROUP_MATCH_MODE,
    PLAYLIST_GROUP_PROFILES as GROUP_PROVIDER,
)


REGISTRY_REFRESH_INTERVAL_SEC = 1.0


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


def _registry_entries_snapshot(
    registry_store: IdentityRegistryStore,
) -> Dict[str, Mapping[str, object]]:
    registry = registry_store.read()
    entries = registry.get("entries")
    if not isinstance(entries, Mapping):
        raise RuntimeError("identity registry entries are missing or invalid")
    return {
        str(identity_key): dict(entry)
        for identity_key, entry in entries.items()
        if isinstance(identity_key, str) and isinstance(entry, Mapping)
    }


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


def _coordinator_source_key(
    candidate: SourceCandidate,
) -> Tuple[str, str, str]:
    return (
        str(candidate.playlist_url or ""),
        str(candidate.extra.get("provider") or "UNKNOWN").strip().upper(),
        str(candidate.extra.get("source_group") or "").strip().upper(),
    )


def _retained_failed_source_candidate(
    candidate: SourceCandidate,
) -> SourceCandidate:
    detail = "playlist source unavailable in current Coordinator scan"
    return replace(
        candidate,
        launchable=False,
        probe_status="source_unavailable",
        probe_error=detail,
        ignored=True,
        reason=detail + "; previous observation retained",
        extra={
            **dict(candidate.extra),
            "coordinator_source_scan_status": "unavailable",
            "coordinator_retained_observation": True,
        },
    )


def _retain_failed_source_observations(
    previous: Optional[DashboardSnapshot],
    target_views: Sequence[TargetView],
    candidates_by_target: Mapping[str, Sequence[SourceCandidate]],
    failed_source_keys: Set[Tuple[str, str, str]],
) -> Dict[str, Tuple[SourceCandidate, ...]]:
    current = {
        str(name): list(items)
        for name, items in candidates_by_target.items()
    }
    if previous is None or not failed_source_keys:
        return {
            name: tuple(items)
            for name, items in current.items()
        }

    previous_targets = {
        view.target.name: view.target
        for view in previous.target_views
    }

    def observation_key(candidate: SourceCandidate) -> Tuple[object, ...]:
        return (
            _coordinator_source_key(candidate),
            int(candidate.matching_entry_index or 0),
            str(candidate.stream_url or candidate.raw_stream_url or ""),
            str(candidate.tvg_name or ""),
            str(candidate.group_title or ""),
            str(candidate.entry_title or ""),
        )

    for view in target_views:
        if view.status != "ACTIVE":
            continue
        target_name = view.target.name
        if previous_targets.get(target_name) != view.target:
            continue

        target_items = current.setdefault(target_name, [])
        existing = {
            observation_key(candidate)
            for candidate in target_items
        }
        for candidate in previous.candidates_by_target.get(target_name, ()):
            if _coordinator_source_key(candidate) not in failed_source_keys:
                continue
            retained = _retained_failed_source_candidate(candidate)
            key = observation_key(retained)
            if key in existing:
                continue
            target_items.append(retained)
            existing.add(key)

    return {
        name: tuple(items)
        for name, items in current.items()
    }


def _change_events_for_log(
    events: Sequence[ChangeEvent],
) -> Tuple[ChangeEvent, ...]:
    """Collapse duplicate identity UPDATE summaries without changing dashboard events."""
    result: List[ChangeEvent] = []
    seen_updates = set()
    for event in events:
        if event.marker == "UPDATE":
            signature = (event.block_key, event.details)
            if signature in seen_updates:
                continue
            seen_updates.add(signature)
        result.append(event)
    return tuple(result)


def run_once(
    config_state: CoordinatorConfigState,
    previous: Optional[DashboardSnapshot],
    *,
    progress_callback: Optional[Callable[[str], None]] = None,
    context_callback: Optional[Callable[[DashboardSnapshot], None]] = None,
    row_update_registry: Optional[Dict[Tuple[object, ...], Tuple[Tuple[object, ...], datetime]]] = None,
    source_freshness_registry: Optional[Dict[str, Mapping[str, object]]] = None,
    event_transition_registry: Optional[
        Dict[Tuple[str, str], Mapping[str, object]]
    ] = None,
    quality_evidence_registry: Optional[Dict[str, Mapping[str, object]]] = None,
    stop_requested: Optional[Callable[[], bool]] = None,
) -> Tuple[DashboardSnapshot, Tuple[ChangeEvent, ...]]:
    if stop_requested is not None and stop_requested():
        raise RuntimeError("Coordinator scan cancelled by stop request")
    now = datetime.now()
    config_messages, _ = config_state.reload(now)
    window = config_state.coordinator_window(now)
    target_views = config_state.target_views(
        now,
        coordinator_active=(window.status == "ACTIVE"),
    )
    raw = config_state.raw_config or {}
    failed_source_keys: Set[Tuple[str, str, str]] = set()
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
        acquisition_kwargs = (
            {"quality_evidence_registry": quality_evidence_registry}
            if quality_evidence_registry is not None
            else {}
        )
        candidates_by_target, source_errors = acquire_active_targets(
            raw,
            target_views,
            progress_callback=progress_callback,
            source_freshness_registry=source_freshness_registry,
            event_transition_registry=event_transition_registry,
            failed_source_keys=failed_source_keys,
            stop_requested=stop_requested,
            **acquisition_kwargs,
        )
        candidates_by_target = _retain_failed_source_observations(
            previous,
            target_views,
            candidates_by_target,
            failed_source_keys,
        )
    else:
        candidates_by_target, source_errors = {}, ()

    if stop_requested is not None and stop_requested():
        raise RuntimeError("Coordinator scan cancelled by stop request")

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
) -> Tuple[Tuple[int, str], ...]:
    """Return selectable identities with their stable dashboard row numbers."""
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
        (number, identity_key)
        for number, identity_key in enumerate(ordered, start=1)
        if (
            identity_key not in blocked
            and (POLICY_MANUAL, identity_key) in snapshot.blocks
            and snapshot.blocks[(POLICY_MANUAL, identity_key)].best_candidate
            is not None
        )
    )


def _registry_state_changes(
    previous_entries: Mapping[str, Mapping[str, object]],
    current_entries: Mapping[str, Mapping[str, object]],
) -> Tuple[Tuple[str, str, str, str], ...]:
    """Describe registry state transitions observed by the fast status poll."""
    changes: List[Tuple[str, str, str, str]] = []
    for identity_key in sorted(set(previous_entries) | set(current_entries)):
        previous_entry = previous_entries.get(identity_key)
        current_entry = current_entries.get(identity_key)
        previous_state = (
            str(previous_entry.get("state") or "").strip().upper()
            if isinstance(previous_entry, Mapping)
            else ""
        )
        current_state = (
            str(current_entry.get("state") or "").strip().upper()
            if isinstance(current_entry, Mapping)
            else ""
        )
        if previous_state == current_state:
            continue
        reason_entry = current_entry if isinstance(current_entry, Mapping) else previous_entry
        reason = (
            str(reason_entry.get("reason") or "-")
            if isinstance(reason_entry, Mapping)
            else "-"
        )
        changes.append(
            (
                identity_key,
                previous_state or "-",
                current_state or "-",
                reason,
            )
        )
    return tuple(changes)


def _write_registry_state_change(
    log_path: Path,
    identity_key: str,
    previous_state: str,
    current_state: str,
    reason: str,
) -> None:
    write_log(
        log_path,
        (
            f"{datetime.now():%Y-%m-%d %H:%M:%S} "
            f"REGISTRY STATE — {identity_key} — "
            f"{previous_state} -> {current_state} — {reason}"
        ),
    )


def _freeze_identity_recovery_scope(
    snapshot: DashboardSnapshot,
    plan,
    raw_config: Mapping[str, object],
) -> Tuple[Tuple[FrozenTargetIntent, ...], Tuple[str, ...]]:
    """Freeze each winning search together with its own launch-time source URLs."""
    target_by_name = {
        view.target.name: view.target
        for view in snapshot.target_views
    }
    frozen_intents: List[FrozenTargetIntent] = []
    all_urls: List[str] = []
    all_seen: Set[str] = set()

    for intent in plan.target_intents:
        target = target_by_name.get(intent.name)
        if target is None:
            raise RuntimeError(
                f"Selected identity target {intent.name!r} is no longer available"
            )

        intent_urls: List[str] = []
        intent_seen: Set[str] = set()
        intent_scopes: List[FrozenRecoveryScope] = []
        for group in intent.source_groups:
            context_group = _source_context_group_for_target(target, group)
            provider = str(
                GROUP_PROVIDER.get(context_group, context_group)
            ).strip().upper()
            if provider != plan.identity.provider:
                continue

            group_urls: List[str] = []
            group_seen: Set[str] = set()
            for spec in sources_for_group(
                raw_config,
                group,
                context_group=context_group,
            ):
                if spec.url not in group_seen:
                    group_seen.add(spec.url)
                    group_urls.append(spec.url)
                if spec.url not in intent_seen:
                    intent_seen.add(spec.url)
                    intent_urls.append(spec.url)
                if spec.url not in all_seen:
                    all_seen.add(spec.url)
                    all_urls.append(spec.url)

            if group_urls:
                intent_scopes.append(
                    FrozenRecoveryScope(
                        source_group=str(group).strip().upper(),
                        playlist_urls=tuple(group_urls),
                        match_mode=GROUP_MATCH_MODE.get(
                            context_group,
                            "EVENT_PHRASE",
                        ),
                    )
                )

        if not intent_urls:
            raise RuntimeError(
                f"Selected identity target {intent.name!r} has no frozen "
                "recovery playlist source scope"
            )

        frozen_intents.append(
            replace(
                intent,
                recovery_playlist_urls=tuple(intent_urls),
                recovery_scopes=tuple(intent_scopes),
            )
        )

    if not frozen_intents or not all_urls:
        raise RuntimeError(
            "Selected identity has no frozen recovery playlist source scope"
        )

    return tuple(frozen_intents), tuple(all_urls)

def _launch_identity(
    snapshot: DashboardSnapshot,
    identity_key: str,
    *,
    launch_policy: str,
    registry_session_id: str,
    registry_store: IdentityRegistryStore,
    config_path: Path,
    raw_config: Mapping[str, object],
    registry_transition_callback: Optional[
        Callable[[str, str, Mapping[str, object]], None]
    ] = None,
):
    plan = build_identity_launch_plan(
        snapshot,
        identity_key,
        launch_policy=launch_policy,
    )
    target_intents, recovery_playlist_urls = _freeze_identity_recovery_scope(
        snapshot,
        plan,
        raw_config,
    )
    request = IdentityLaunchRequest(
        registry_session_id=registry_session_id,
        identity_key=plan.identity.serialized,
        provider=plan.identity.provider,
        selected_source_group=plan.selected_source_group,
        selected_candidate=plan.selected_candidate,
        initial_candidate_pool=plan.candidate_pool,
        target_intents=target_intents,
        recovery_playlist_urls=recovery_playlist_urls,
        recording_duration_min=plan.recording_duration_min,
        base_name=plan.base_name,
    )
    result = launch_identity_worker(
        request,
        registry_store,
        config_path=config_path,
        registry_transition_callback=registry_transition_callback,
    )
    return plan, result


def _launch_manual_identity(
    snapshot: DashboardSnapshot,
    identity_key: str,
    *,
    registry_session_id: str,
    registry_store: IdentityRegistryStore,
    config_path: Path,
    raw_config: Mapping[str, object],
    registry_transition_callback: Optional[
        Callable[[str, str, Mapping[str, object]], None]
    ] = None,
):
    return _launch_identity(
        snapshot,
        identity_key,
        launch_policy=POLICY_MANUAL,
        registry_session_id=registry_session_id,
        registry_store=registry_store,
        config_path=config_path,
        raw_config=raw_config,
        registry_transition_callback=registry_transition_callback,
    )


# Compatibility for existing tests/callers that inspect the MANUAL freeze helper.
def _freeze_manual_recovery_scope(
    snapshot: DashboardSnapshot,
    plan,
    raw_config: Mapping[str, object],
) -> Tuple[Tuple[FrozenTargetIntent, ...], Tuple[str, ...]]:
    return _freeze_identity_recovery_scope(snapshot, plan, raw_config)


def _launch_all_identities(
    snapshot: DashboardSnapshot,
    *,
    registry_session_id: str,
    registry_store: IdentityRegistryStore,
    config_path: Path,
    raw_config: Mapping[str, object],
    log_path: Path,
    registry_transition_callback: Optional[
        Callable[[str, str, Mapping[str, object]], None]
    ] = None,
) -> Tuple[Tuple[str, str, str], ...]:
    """Launch every currently eligible ALL identity once for this registry session."""
    registry = registry_store.read()
    entries = registry.get("entries")
    blocked = set(entries) if isinstance(entries, Mapping) else set()

    identity_keys = sorted(
        identity_key
        for (policy, identity_key), block in snapshot.blocks.items()
        if (
            policy == POLICY_ALL
            and identity_key not in blocked
            and block.best_candidate is not None
        )
    )

    outcomes: List[Tuple[str, str, str]] = []
    for identity_key in identity_keys:
        try:
            plan, result = _launch_identity(
                snapshot,
                identity_key,
                launch_policy=POLICY_ALL,
                registry_session_id=registry_session_id,
                registry_store=registry_store,
                config_path=config_path,
                raw_config=raw_config,
                registry_transition_callback=registry_transition_callback,
            )
        except IdentityLaunchBlocked as error:
            detail = str(error)
            outcomes.append((identity_key, "SUPPRESSED", detail))
            write_log(
                log_path,
                (
                    f"{datetime.now():%Y-%m-%d %H:%M:%S} "
                    f"ALL LAUNCH SUPPRESSED — {identity_key} — {detail}"
                ),
            )
            continue
        except Exception as error:
            detail = f"{type(error).__name__}: {error}"
            outcomes.append((identity_key, "FAILED", detail))
            write_log(
                log_path,
                (
                    f"{datetime.now():%Y-%m-%d %H:%M:%S} "
                    f"ALL LAUNCH FAILED — {identity_key} — {detail}"
                ),
            )
            continue

        detail = f"{plan.base_name} — PID {result.pid}"
        outcomes.append((identity_key, "LAUNCHED", detail))
        write_log(
            log_path,
            (
                f"{datetime.now():%Y-%m-%d %H:%M:%S} "
                f"ALL LAUNCH — {plan.identity.serialized} — "
                f"{plan.base_name} — PID {result.pid}"
            ),
        )

    return tuple(outcomes)


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
                extended_key = msvcrt.getwch()
                if extended_key == "\x3f":  # F5
                    number_buffer = ""
                    command_queue.put("__F5__")
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
            if key.casefold() in {"r", "i", "s", "m", "f", "u"}:
                command_queue.put(key.casefold())
            elif key == "\x1b":
                command_queue.put("__ESC__")
            elif key == "\x03":
                stop_event.set()
                command_queue.put("__CTRL_C__")
        return

    while not stop_event.is_set():
        try:
            value = input().strip()
        except EOFError:
            return
        except KeyboardInterrupt:
            stop_event.set()
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


def _restore_dashboard_after_temporary_menu(
    snapshot: Optional[DashboardSnapshot],
    display_order: Mapping[str, Sequence[str]],
    *,
    config_path: Path,
    refresh_interval_sec: float,
    registry_entries: Mapping[str, Mapping[str, object]],
    runtime_statuses: Mapping[str, Mapping[str, object]],
    source_references: Mapping[str, int],
    watch_text: str = "",
) -> None:
    """Remove a temporary menu and restore the last completed Coordinator view."""
    if snapshot is None:
        clear_live_status_line()
        return

    clear_live_status_line()
    terminal_text = render_dashboard(
        snapshot,
        (),
        display_order,
        config_path=config_path,
        refresh_interval_sec=refresh_interval_sec,
        use_color=True,
        registry_entries=registry_entries,
        runtime_statuses=runtime_statuses,
        source_references=source_references,
    )
    clear_dashboard_terminal()
    print(terminal_text)
    if watch_text:
        set_live_status_line(watch_text)


def run(config_path: Path, *, once: bool = False) -> int:
    state = CoordinatorConfigState(config_path)
    previous: Optional[DashboardSnapshot] = None
    display_order: Dict[str, List[str]] = {POLICY_ALL: [], POLICY_MANUAL: []}
    row_update_registry: Dict[Tuple[object, ...], Tuple[Tuple[object, ...], datetime]] = {}
    source_freshness_registry: Dict[str, Mapping[str, object]] = {}
    event_transition_registry: Dict[
        Tuple[str, str], Mapping[str, object]
    ] = {}
    quality_evidence_registry: Dict[str, Mapping[str, object]] = {}
    source_reference_registry: Dict[str, int] = {}
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

    registry_entries = _registry_entries_snapshot(registry_store)
    runtime_status_store = IdentityRuntimeStatusStore(
        output_paths,
        registry_status.session_id,
    )
    runtime_statuses = runtime_status_store.read_all()
    next_registry_refresh_monotonic = (
        time.monotonic() + REGISTRY_REFRESH_INTERVAL_SEC
    )
    registry_read_error_signature = ""

    def capture_launch_registry_transition(
        identity_key: str,
        previous_state: str,
        entry: Mapping[str, object],
    ) -> None:
        current_state = str(entry.get("state") or "-").strip().upper() or "-"
        reason = str(entry.get("reason") or "-")
        _write_registry_state_change(
            log_path,
            identity_key,
            previous_state,
            current_state,
            reason,
        )
        registry_entries[identity_key] = dict(entry)

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
    info_menu_open = False
    sound_menu_open = False
    record_menu_open = False
    record_choices: Tuple[Tuple[int, str], ...] = ()
    last_completed_raw_config: Mapping[str, object] = {}

    def process_record_control(
        command: str,
        normalized: str,
        *,
        during_scan: bool = False,
    ) -> bool:
        nonlocal record_menu_open, record_choices, next_registry_refresh_monotonic
        nonlocal dashboard_has_transient

        if record_menu_open:
            if normalized in {"__esc__", "esc", "cancel"}:
                record_menu_open = False
                record_choices = ()
                watch_text = ""
                if not during_scan and next_refresh_monotonic > 0:
                    watch_text = watch_status_text(
                        last_scan_wall_time,
                        next_refresh_monotonic,
                    )
                _restore_dashboard_after_temporary_menu(
                    previous,
                    display_order,
                    config_path=config_path,
                    refresh_interval_sec=state.refresh_interval_sec,
                    registry_entries=registry_entries,
                    runtime_statuses=runtime_statuses,
                    source_references=source_reference_registry,
                    watch_text=watch_text,
                )
                dashboard_has_transient = False
                return True

            number_text = ""
            if command.startswith("__NUMBER_BUFFER__:"):
                number_text = command.split(":", 1)[1]
                set_live_status_line(f"Identity number: {number_text}")
                return True
            if command.startswith("__NUMBER_SUBMIT__:"):
                number_text = command.split(":", 1)[1]
            elif normalized.isdigit():
                number_text = normalized
            else:
                set_live_status_line("Identity number: ")
                return True

            if not number_text:
                set_live_status_line("Identity number: ")
                return True

            selected_number = int(number_text)
            choice_by_number = dict(record_choices)
            if selected_number not in choice_by_number:
                valid_numbers = ", ".join(
                    str(number) for number, _ in record_choices
                )
                set_live_status_line(f"Choose {valid_numbers} or Esc")
                return True

            identity_key = choice_by_number[selected_number]
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
                    config_path=config_path,
                    raw_config=last_completed_raw_config,
                    registry_transition_callback=(
                        capture_launch_registry_transition
                    ),
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
            next_registry_refresh_monotonic = 0.0
            return True

        if normalized not in {"r", "record"}:
            return False

        if previous is None:
            set_live_status_line("No completed discovery snapshot yet")
            return True
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
            return True
        if not record_choices:
            clear_live_status_line()
            print("No MANUAL identities are currently available to record.")
            return True

        record_menu_open = True
        clear_live_status_line()
        print(render_manual_record_menu(previous, record_choices))
        set_live_status_line("Identity number: ")
        return True

    try:
        while not stop_event.is_set():
            now_monotonic = time.monotonic()
            if now_monotonic >= next_registry_refresh_monotonic:
                next_registry_refresh_monotonic = (
                    now_monotonic + REGISTRY_REFRESH_INTERVAL_SEC
                )
                try:
                    fresh_registry_entries = _registry_entries_snapshot(
                        registry_store
                    )
                    fresh_runtime_statuses = runtime_status_store.read_all()
                except Exception as error:
                    signature = f"{type(error).__name__}: {error}"
                    if signature != registry_read_error_signature:
                        write_log(
                            log_path,
                            (
                                f"{datetime.now():%Y-%m-%d %H:%M:%S} "
                                "REGISTRY STATUS READ WARNING — "
                                f"{signature}"
                            ),
                        )
                    registry_read_error_signature = signature
                else:
                    registry_read_error_signature = ""
                    registry_changed = fresh_registry_entries != registry_entries
                    runtime_status_changed = fresh_runtime_statuses != runtime_statuses
                    if registry_changed:
                        for (
                            identity_key,
                            previous_state,
                            current_state,
                            reason,
                        ) in _registry_state_changes(
                            registry_entries,
                            fresh_registry_entries,
                        ):
                            _write_registry_state_change(
                                log_path,
                                identity_key,
                                previous_state,
                                current_state,
                                reason,
                            )
                        registry_entries = fresh_registry_entries
                    if runtime_status_changed:
                        runtime_statuses = fresh_runtime_statuses
                    if previous is not None and registry_changed:
                        display_order = update_display_order(
                            display_order,
                            previous,
                            registry_entries=registry_entries,
                        )
                    if previous is not None and runtime_status_changed:
                        source_reference_registry = update_source_reference_registry(
                            source_reference_registry,
                            previous,
                            runtime_statuses,
                        )
                    if (
                            (registry_changed or runtime_status_changed)
                            and previous is not None
                            and not force_refresh
                            and not record_menu_open
                            and not info_menu_open
                            and not sound_menu_open
                        ):
                            clear_live_status_line()
                            terminal_text = render_dashboard(
                                previous,
                                (),
                                display_order,
                                config_path=config_path,
                                refresh_interval_sec=state.refresh_interval_sec,
                                use_color=True,
                                registry_entries=registry_entries,
                                runtime_statuses=runtime_statuses,
                                source_references=source_reference_registry,
                            )
                            clear_dashboard_terminal()
                            print(terminal_text)
                            dashboard_has_transient = False
                            if next_refresh_monotonic > 0:
                                set_live_status_line(
                                    watch_status_text(
                                        last_scan_wall_time,
                                        next_refresh_monotonic,
                                    )
                                )

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
            if (
                not record_menu_open
                and not info_menu_open
                and not sound_menu_open
                and (force_refresh or now_monotonic >= next_refresh_monotonic)
            ):
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

                    scan_results: "queue.Queue[Tuple[str, object]]" = queue.Queue()
                    scan_progress = {"text": ""}
                    deferred_scan_commands: List[str] = []
                    refresh_requested_during_scan = False

                    def scan_progress_callback(text: str) -> None:
                        scan_progress["text"] = str(text)

                    def scan_worker() -> None:
                        try:
                            result = run_once(
                                state,
                                previous,
                                progress_callback=(
                                    None if once else scan_progress_callback
                                ),
                                context_callback=(
                                    show_initial_header
                                    if previous is None and not once
                                    else None
                                ),
                                row_update_registry=row_update_registry,
                                source_freshness_registry=source_freshness_registry,
                                event_transition_registry=event_transition_registry,
                                quality_evidence_registry=quality_evidence_registry,
                                stop_requested=stop_event.is_set,
                            )
                        except BaseException as error:
                            scan_results.put(("error", error))
                        else:
                            scan_results.put(("result", result))

                    scan_thread = threading.Thread(
                        target=scan_worker,
                        daemon=True,
                        name="coordinator_scan",
                    )
                    scan_thread.start()

                    while scan_thread.is_alive():
                        if stop_event.is_set():
                            clear_live_status_line()
                            print("\nIdentity Coordinator stopped by user.")
                            return 0

                        if (
                            not record_menu_open
                            and not sound_menu_open
                            and scan_progress["text"]
                        ):
                            set_live_status_line(scan_progress["text"])

                        try:
                            scan_command = command_queue.get(timeout=0.10)
                        except queue.Empty:
                            continue

                        scan_normalized = " ".join(
                            scan_command.split()
                        ).casefold()
                        if scan_normalized == "__ctrl_c__":
                            stop_event.set()
                            clear_live_status_line()
                            print("\nIdentity Coordinator stopped by user.")
                            return 0

                        if process_record_control(
                            scan_command,
                            scan_normalized,
                            during_scan=True,
                        ):
                            continue

                        if scan_normalized in {
                            "__f5__",
                            "f5",
                            "refresh",
                        }:
                            refresh_requested_during_scan = True
                            continue

                        deferred_scan_commands.append(scan_command)

                    result_kind, result_payload = scan_results.get()
                    for deferred_command in deferred_scan_commands:
                        command_queue.put(deferred_command)

                    if result_kind == "error":
                        raise result_payload
                    snapshot, events = result_payload
                    last_completed_raw_config = dict(state.raw_config or {})
                except KeyboardInterrupt:
                    raise
                except Exception as error:
                    if stop_event.is_set():
                        clear_live_status_line()
                        print("\nIdentity Coordinator stopped by user.")
                        return 0
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

                if stop_event.is_set():
                    clear_live_status_line()
                    print("\nIdentity Coordinator stopped by user.")
                    return 0

                auto_launch_outcomes = _launch_all_identities(
                    snapshot,
                    registry_session_id=registry_status.session_id,
                    registry_store=registry_store,
                    config_path=config_path,
                    raw_config=last_completed_raw_config,
                    log_path=log_path,
                    registry_transition_callback=capture_launch_registry_transition,
                )
                if auto_launch_outcomes:
                    try:
                        fresh_registry_entries = _registry_entries_snapshot(
                            registry_store
                        )
                        fresh_runtime_statuses = runtime_status_store.read_all()
                    except Exception as error:
                        write_log(
                            log_path,
                            (
                                f"{datetime.now():%Y-%m-%d %H:%M:%S} "
                                "POST-AUTO STATUS READ WARNING — "
                                f"{type(error).__name__}: {error}"
                            ),
                        )
                    else:
                        for (
                            identity_key,
                            previous_state,
                            current_state,
                            reason,
                        ) in _registry_state_changes(
                            registry_entries,
                            fresh_registry_entries,
                        ):
                            _write_registry_state_change(
                                log_path,
                                identity_key,
                                previous_state,
                                current_state,
                                reason,
                            )
                        registry_entries = fresh_registry_entries
                        runtime_statuses = fresh_runtime_statuses

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

                display_order = update_display_order(
                    display_order,
                    snapshot,
                    registry_entries=registry_entries,
                )
                source_reference_registry = update_source_reference_registry(
                    source_reference_registry,
                    snapshot,
                    runtime_statuses,
                )
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
                        registry_entries=registry_entries,
                        runtime_statuses=runtime_statuses,
                        source_references=source_reference_registry,
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
                            registry_entries=registry_entries,
                            runtime_statuses=runtime_statuses,
                            source_references=source_reference_registry,
                        )
                        write_log(log_path, log_text)
                        for event in _change_events_for_log(events):
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
                force_refresh = refresh_requested_during_scan
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
                if (
                    next_refresh_monotonic > 0
                    and not record_menu_open
                    and not info_menu_open
                    and not sound_menu_open
                ):
                    set_live_status_line(
                        watch_status_text(last_scan_wall_time, next_refresh_monotonic)
                    )
                continue

            normalized = " ".join(command.split()).casefold()
            if normalized == "__ctrl_c__":
                clear_live_status_line()
                print("\nIdentity Coordinator stopped by user.")
                return 0
            if info_menu_open:
                if normalized in {"i", "info", "__esc__", "esc", "cancel"}:
                    info_menu_open = False
                    watch_text = (
                        watch_status_text(last_scan_wall_time, next_refresh_monotonic)
                        if next_refresh_monotonic > 0
                        else ""
                    )
                    _restore_dashboard_after_temporary_menu(
                        previous,
                        display_order,
                        config_path=config_path,
                        refresh_interval_sec=state.refresh_interval_sec,
                        registry_entries=registry_entries,
                        runtime_statuses=runtime_statuses,
                        source_references=source_reference_registry,
                        watch_text=watch_text,
                    )
                    dashboard_has_transient = False
                else:
                    set_live_status_line("Press I or Esc to close")
                continue

            if sound_menu_open:
                message = ""
                if normalized in {"__esc__", "esc", "cancel"}:
                    sound_menu_open = False
                    message = "Sound control cancelled; sound state is unchanged."
                elif normalized in {"m", "15", "15m"}:
                    runtime_sound.set_timed_sound_snooze(
                        sound_state,
                        duration_sec=15 * 60.0,
                    )
                    sound_menu_open = False
                    message = (
                        "Sound snoozed for 15 minutes. Coordinator scanning "
                        "and change detection continue normally."
                    )
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
                    message = (
                        "Sound snoozed for the full Coordinator run. Coordinator "
                        "scanning and change detection continue normally."
                    )
                    write_log(
                        log_path,
                        f"{datetime.now():%Y-%m-%d %H:%M:%S} "
                        "SOUND SNOOZE — full Coordinator run",
                    )
                elif normalized in {"u", "unsnooze", "restore"}:
                    runtime_sound.clear_sound_snooze(sound_state)
                    sound_menu_open = False
                    message = "Sound restored."
                    write_log(
                        log_path,
                        f"{datetime.now():%Y-%m-%d %H:%M:%S} SOUND RESTORED",
                    )
                else:
                    set_live_status_line("Select M/F/U or Esc")
                    continue

                _restore_dashboard_after_temporary_menu(
                    previous,
                    display_order,
                    config_path=config_path,
                    refresh_interval_sec=state.refresh_interval_sec,
                    registry_entries=registry_entries,
                    runtime_statuses=runtime_statuses,
                    source_references=source_reference_registry,
                    watch_text=message,
                )
                dashboard_has_transient = False
                continue

            if process_record_control(command, normalized):
                continue

            if normalized in {"__f5__", "f5", "refresh"}:
                force_refresh = True
                continue

            if normalized in {"i", "info"}:
                info_menu_open = True
                clear_live_status_line()
                clear_dashboard_terminal()
                print(render_coordinator_controls(sound_state))
                set_live_status_line("Press I or Esc to close")
                continue

            if normalized in {"s", "sound"}:
                sound_menu_open = True
                clear_live_status_line()
                clear_dashboard_terminal()
                print(render_sound_snooze_menu(sound_state))
                set_live_status_line("Select M/F/U or Esc")
                continue

            if normalized:
                set_live_status_line(
                    "Use r=record | i=info | s=sound | F5=refresh | Ctrl+C=exit"
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
