"""Terminal presentation and terminal side effects for the Identity Coordinator."""

from __future__ import annotations

import hashlib
import os
import sys
import tempfile
import wave
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

try:
    import winsound
except ImportError:  # pragma: no cover - non-Windows development/test hosts
    winsound = None

from recorder_runtime import sound as runtime_sound
from recorder_runtime.sound import SoundSnoozeState
from recorder_source.policy import selection_policy_for_provider
from recorder_source.selection import (
    same_selection_candidate,
    selection_nonselection_reason,
)

from .models import (
    ChangeEvent,
    DashboardSnapshot,
    IdentityTarget,
    POLICY_ALL,
    POLICY_MANUAL,
    TargetView,
)
from .snapshot import candidate_row_key, candidate_state, quality_text


_LIVE_STATUS_ACTIVE = False
_LIVE_STATUS_TEXT = ""

_RUNTIME_REGISTRY_STATES = frozenset(
    {"LAUNCHING", "ACTIVE", "WAITING_FOR_SOURCE"}
)


def _terminal_is_interactive() -> bool:
    try:
        return bool(sys.stdout.isatty())
    except Exception:
        return False


# Palette sampled from the user's NextPVR reference and deliberately kept soft.
_EVENT_RGB = (41, 159, 214)       # #299FD6
_MARKER_RGB = (255, 135, 3)       # #FF8703
_CHANGE_DETAIL_RGB = (255, 215, 0)
_SECONDARY_RGB = (176, 176, 176)
_MUTED_RGB = (118, 118, 118)
_IDENTITY_RGB = (145, 153, 160)
_ERROR_RGB = (224, 82, 82)
_IMPORTANT_RGB = (220, 220, 220)

_RUNTIME_STATE_RGB = {
    "AVAILABLE": _IMPORTANT_RGB,
    "LAUNCHING": _EVENT_RGB,
    "ACTIVE": _CHANGE_DETAIL_RGB,
    "WAITING_FOR_SOURCE": _MARKER_RGB,
    "ENDED": _MUTED_RGB,
    "MANUALLY_STOPPED": _MUTED_RGB,
    "CRASHED": _ERROR_RGB,
}


def _paint_rgb(text: str, rgb: Tuple[int, int, int], use_color: bool) -> str:
    if not use_color:
        return text
    red, green, blue = rgb
    return f"\033[38;2;{red};{green};{blue}m{text}\033[0m"


def _marker(text: str, use_color: bool) -> str:
    return _paint_rgb(text, _MARKER_RGB, use_color)


def _event_title(text: str, use_color: bool) -> str:
    return _paint_rgb(text, _EVENT_RGB, use_color)


def _identity_text(text: str, use_color: bool) -> str:
    return _paint_rgb(text, _IDENTITY_RGB, use_color)


def _group_text(text: str, use_color: bool) -> str:
    return _paint_rgb(text, _EVENT_RGB, use_color)


def _change_detail_text(text: str, use_color: bool) -> str:
    return _paint_rgb(text, _CHANGE_DETAIL_RGB, use_color)


def _important_text(text: str, use_color: bool) -> str:
    return _paint_rgb(text, _IMPORTANT_RGB, use_color)


def _secondary_text(text: str, use_color: bool) -> str:
    return _paint_rgb(text, _SECONDARY_RGB, use_color)


def _muted_text(text: str, use_color: bool) -> str:
    return _paint_rgb(text, _MUTED_RGB, use_color)


def _source_reference_text(text: str, use_color: bool) -> str:
    return _paint_rgb(text, _MUTED_RGB, use_color)


def _compact_timestamp(value: datetime, reference: datetime) -> str:
    """Show HH:MM today; include the date only when it differs from today."""
    if value.date() == reference.date():
        return value.strftime("%H:%M")
    return value.strftime("%Y-%m-%d %H:%M")


def _off_text(text: str, use_color: bool) -> str:
    return _paint_rgb(text, _ERROR_RGB, use_color)


def _runtime_state_text(
    state: str,
    use_color: bool,
    *,
    bracketed: bool = False,
) -> str:
    normalized = str(state or "").strip().upper()
    text = f"[{normalized}]" if bracketed else normalized
    rgb = _RUNTIME_STATE_RGB.get(normalized, _ERROR_RGB)
    return _paint_rgb(text, rgb, use_color)


def _phrase_group_text(value: object) -> str:
    if isinstance(value, (list, tuple)):
        choices = [str(item).strip() for item in value if str(item).strip()]
        if not choices:
            return ""
        joined = " OR ".join(choices)
        return f"({joined})" if len(choices) > 1 else joined
    return str(value or "").strip()


def _target_search_text(target: IdentityTarget) -> str:
    if target.match_all:
        primary = "MATCH ALL"
    else:
        groups = [_phrase_group_text(item) for item in target.primary]
        primary = " AND ".join(item for item in groups if item) or "-"
    extras: List[str] = []
    if target.required:
        extras.append(
            "required: " + " AND ".join(
                item for item in (_phrase_group_text(value) for value in target.required) if item
            )
        )
    if target.rejected:
        extras.append(
            "exclude: " + " OR ".join(
                item for item in (_phrase_group_text(value) for value in target.rejected) if item
            )
        )
    if target.preferred:
        extras.append(
            "prefer: " + " OR ".join(
                item for item in (_phrase_group_text(value) for value in target.preferred) if item
            )
        )
    return " | ".join([primary] + extras)


def _target_status_text(view: TargetView) -> str:
    if view.status == "ACTIVE":
        if view.active_until is None:
            return "ACTIVE until stopped"
        return f"ACTIVE until {view.active_until:%Y-%m-%d %H:%M:%S}"
    if view.status == "SCHEDULED" and view.active_from is not None:
        text = f"SCHEDULED from {view.active_from:%Y-%m-%d %H:%M:%S}"
        if view.active_until is not None:
            text += f" until {view.active_until:%Y-%m-%d %H:%M:%S}"
        return text
    if view.status == "EXPIRED" and view.active_until is not None:
        return f"EXPIRED at {view.active_until:%Y-%m-%d %H:%M:%S}"
    if view.status == "WAITING_COORDINATOR":
        return "WAITING for coordinator"
    return view.status


def _coordinator_status_text(snapshot: DashboardSnapshot) -> str:
    window = snapshot.coordinator_window
    if window is None:
        return "status unavailable"
    if window.status == "WAITING":
        text = f"WAITING | Starts: {window.active_from:%Y-%m-%d %H:%M:%S}"
        if window.active_until is not None:
            text += f" | End: {window.active_until:%Y-%m-%d %H:%M:%S}"
        return text
    if window.status == "ACTIVE":
        end_text = (
            f"{window.active_until:%Y-%m-%d %H:%M:%S}"
            if window.active_until is not None
            else "until stopped"
        )
        return (
            f"ACTIVE | Started: {window.active_from:%Y-%m-%d %H:%M:%S} "
            f"| End: {end_text}"
        )
    if window.status == "EXPIRED":
        end_text = (
            f"{window.active_until:%Y-%m-%d %H:%M:%S}"
            if window.active_until is not None
            else "unknown"
        )
        return (
            f"EXPIRED | Started: {window.active_from:%Y-%m-%d %H:%M:%S} "
            f"| End: {end_text}"
        )
    return window.status


def _header_lines(
    snapshot: DashboardSnapshot,
    *,
    config_path: Optional[Path],
    refresh_interval_sec: Optional[float],
) -> List[str]:
    lines = ["RECORDER EVENT COORDINATOR"]
    if config_path is not None:
        lines.append(f"Config       : {Path(config_path).name}")
    if refresh_interval_sec is not None:
        lines.append(f"Refresh      : every {refresh_interval_sec:g}s + manual F5")
    if snapshot.coordinator_window is not None:
        window = snapshot.coordinator_window
        lines.append("Coordinator")
        lines.append(f"  Status    : {window.status}")
        lines.append(f"  Started   : {window.active_from:%Y-%m-%d %H:%M:%S}")
        lines.append(
            "  End       : "
            + (
                f"{window.active_until:%Y-%m-%d %H:%M:%S}"
                if window.active_until is not None
                else "until stopped"
            )
        )

    for index, view in enumerate(snapshot.target_views, start=1):
        target = view.target
        lines.append(
            f"Target {index:<2}    : {target.name} | {target.policy} | "
            f"{_target_status_text(view)} | Search: {_target_search_text(target)} "
            f"| Sources: {', '.join(target.source_groups) or '-'}"
        )
    return lines


def render_header(
    snapshot: DashboardSnapshot,
    *,
    config_path: Optional[Path] = None,
    refresh_interval_sec: Optional[float] = None,
) -> str:
    return "\n".join(
        _header_lines(
            snapshot,
            config_path=config_path,
            refresh_interval_sec=refresh_interval_sec,
        )
    )


def _quality_key(candidate) -> Tuple[int, int, float, int, str]:
    if candidate is None or not candidate.quality_known:
        return (0, 0, 0.0, 0, "")
    return (
        int(candidate.video_width or 0),
        int(candidate.video_height or 0),
        round(float(candidate.video_fps or 0.0), 3),
        int(candidate.video_bitrate_bps or 0),
        str(candidate.video_scan_type or ""),
    )


def _event_maps(events: Sequence[ChangeEvent]):
    identity: Dict[Tuple[str, str], List[ChangeEvent]] = {}
    source: Dict[Tuple[Tuple[str, str], str], List[ChangeEvent]] = {}
    quality: Dict[Tuple[str, str], List[ChangeEvent]] = {}
    for event in events:
        if event.marker in {"REMOVED", "SOURCE-"}:
            continue
        if event.marker in {"QUALITY+", "QUALITY-"}:
            quality.setdefault(event.block_key, []).append(event)
        elif event.source_id:
            source.setdefault((event.block_key, event.source_id), []).append(event)
        else:
            identity.setdefault(event.block_key, []).append(event)
    return identity, source, quality


def _marker_text(events: Sequence[ChangeEvent], use_color: bool) -> str:
    if not events:
        return ""
    return " ".join(_marker(f"[{event.marker}]", use_color) for event in events) + " "


def _provider_summary(snapshot: DashboardSnapshot) -> str:
    providers = sorted({block.identity.provider for block in snapshot.blocks.values()})
    if not providers:
        return "Provider=-"
    label = "Provider" if len(providers) == 1 else "Providers"
    return f"{label}={','.join(providers)}"


def _dashboard_source_ids(snapshot: DashboardSnapshot) -> Tuple[str, ...]:
    source_ids: List[str] = []
    seen = set()
    for block in snapshot.blocks.values():
        for source in block.observations.values():
            source_id = str(source.source_id or "").strip()
            if source_id and source_id not in seen:
                seen.add(source_id)
                source_ids.append(source_id)
    return tuple(source_ids)


def update_source_reference_registry(
    previous: Mapping[str, int],
    snapshot: DashboardSnapshot,
) -> Dict[str, int]:
    """Keep source reference numbers stable for the life of a Coordinator run."""
    result = {
        str(source_id): int(number)
        for source_id, number in previous.items()
        if str(source_id).strip() and int(number) > 0
    }
    next_number = max(result.values(), default=0) + 1
    for source_id in _dashboard_source_ids(snapshot):
        if source_id not in result:
            result[source_id] = next_number
            next_number += 1
    return result


def render_dashboard(
    snapshot: DashboardSnapshot,
    events: Sequence[ChangeEvent],
    display_order: Optional[Mapping[str, Sequence[str]]] = None,
    *,
    config_path: Optional[Path] = None,
    refresh_interval_sec: Optional[float] = None,
    use_color: Optional[bool] = None,
    registry_entries: Optional[Mapping[str, Mapping[str, object]]] = None,
    source_references: Optional[Mapping[str, int]] = None,
) -> str:
    color = _terminal_is_interactive() if use_color is None else bool(use_color)
    identity_events, source_events, quality_events = _event_maps(events)
    effective_source_references = (
        dict(source_references)
        if source_references is not None
        else update_source_reference_registry({}, snapshot)
    )
    current_source_ids = _dashboard_source_ids(snapshot)

    lines: List[str] = _header_lines(
        snapshot,
        config_path=config_path,
        refresh_interval_sec=refresh_interval_sec,
    )

    current_registry = registry_entries or {}
    active_entries = []
    for identity_key, entry in current_registry.items():
        if not isinstance(entry, Mapping):
            continue
        state = str(entry.get("state") or "").strip().upper()
        if state not in _RUNTIME_REGISTRY_STATES:
            continue
        active_entries.append((identity_key, entry, state))

    if active_entries:
        lines.append("")
        lines.append(_event_title("ACTIVE RECORDINGS", color))
        for identity_key, entry, state in sorted(
            active_entries,
            key=lambda item: (
                str(item[1].get("display_name") or item[0]).casefold(),
                item[0],
            ),
        ):
            display_name = str(entry.get("display_name") or identity_key)
            provider = str(entry.get("provider") or "-")
            pid = entry.get("worker_pid")
            pid_text = (
                str(pid)
                if isinstance(pid, int) and not isinstance(pid, bool)
                else "-"
            )
            state_flag = _runtime_state_text(state, color, bracketed=True)
            lines.append(
                f"  {state_flag} {display_name} | {provider} | PID {pid_text}"
            )

    lines.append("")
    lines.append("=" * 88)
    lines.append(
        f"EVENT WATCH {snapshot.created_at:%Y-%m-%d %H:%M:%S} "
        f"| {_provider_summary(snapshot)} "
        f"| Identity blocks={len(snapshot.blocks)}"
    )
    lines.append("=" * 88)

    if snapshot.config_messages:
        lines.append("")
        lines.extend(snapshot.config_messages)

    for policy, heading in ((POLICY_MANUAL, "MANUAL"), (POLICY_ALL, "ALL IDENTITIES")):
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
            block_key = (block.policy, block.identity.serialized)
            serialized = block.identity.serialized
            provider_prefix = f"{block.identity.provider}|"
            identity_value = (
                serialized[len(provider_prefix):]
                if serialized.startswith(provider_prefix)
                else serialized
            )
            identity_marker = _marker_text(identity_events.get(block_key, ()), color)
            registry_entry = current_registry.get(serialized)
            registry_state = (
                str(registry_entry.get("state") or "").strip().upper()
                if isinstance(registry_entry, Mapping)
                else ""
            )
            visible_state = registry_state or block.overall_state
            state_label = _runtime_state_text(visible_state, color)
            lines.append(
                f"{identity_marker}[{index}] {state_label} {block.identity.provider} "
                f"| Identity: {_identity_text(identity_value, color)} "
                f"| Targets: {', '.join(block.target_names)} "
                f"| Sources: {len(block.observations)}"
            )

            for event in identity_events.get(block_key, ()):
                for detail in event.details:
                    lines.append(f"    {_secondary_text(detail, color)}")

            grouped: Dict[Tuple[int, int, float, int, str], List[Tuple[object, object]]] = {}
            seen_rows = set()
            for source in block.observations.values():
                for candidate in source.candidates:
                    key = _quality_key(candidate)
                    row_identity = (
                        source.source_id,
                        candidate.entry_title,
                        candidate.tvg_name,
                        candidate.group_title,
                        candidate_state(candidate),
                        key,
                    )
                    if row_identity in seen_rows:
                        continue
                    seen_rows.add(row_identity)
                    grouped.setdefault(key, []).append((source, candidate))

            best_key = _quality_key(block.best_candidate)
            quality_keys = sorted(
                grouped,
                key=lambda key: (
                    0 if key == best_key and block.best_candidate is not None else 1,
                    -(key[0] * key[1]),
                    -key[2],
                    -key[3],
                ),
            )
            source_marker_consumed = set()
            for quality_key in quality_keys:
                rows = grouped[quality_key]
                rows.sort(
                    key=lambda item: (
                        block.row_last_updated.get(
                            candidate_row_key(item[1]),
                            datetime.min,
                        ),
                        float(item[1].extra.get("source_freshness_ts") or 0.0),
                    ),
                    reverse=True,
                )
                representative = rows[0][1]
                quality_label = quality_text(
                    representative if representative.quality_known else None
                )
                suffix: List[str] = []
                if block.best_candidate is not None and quality_key == best_key:
                    if len(quality_keys) > 1:
                        suffix.append(_marker("[BEST]", color))
                    suffix.extend(
                        _marker(f"[{event.marker}]", color)
                        for event in quality_events.get(block_key, ())
                    )
                suffix_text = " " + " ".join(suffix) if suffix else ""
                quality_display = (
                    _off_text(quality_label, color)
                    if quality_label == "no working candidate"
                    else _important_text(quality_label, color)
                )
                lines.append(
                    f"    Quality : {quality_display}{suffix_text}"
                )
                if block.best_candidate is not None and quality_key == best_key:
                    for event in quality_events.get(block_key, ()):
                        for detail in event.details:
                            lines.append(
                                f"              {_change_detail_text(detail, color)}"
                            )

                for source, candidate in rows:
                    row_events = source_events.get((block_key, source.source_id), ())
                    marker_prefix = ""
                    details_to_show: List[str] = []
                    if source.source_id not in source_marker_consumed and row_events:
                        marker_prefix = _marker_text(row_events, color)
                        source_marker_consumed.add(source.source_id)
                        for event in row_events:
                            if event.marker == "UPDATE":
                                details_to_show.extend(event.details)

                    state_text = candidate_state(candidate)
                    on_off = "[ON]" if state_text == "WORKING" else _off_text("[OFF]", color)
                    selection_marker = ""
                    selection_reason = ""
                    if state_text == "WORKING" and block.best_candidate is not None:
                        if same_selection_candidate(candidate, block.best_candidate):
                            selection_marker = " " + _marker("[SELECTED]", color)
                        else:
                            selection_reason = selection_nonselection_reason(
                                candidate,
                                block.best_candidate,
                                selection_policy_for_provider(block.identity.provider),
                                now_ts=snapshot.created_at.timestamp(),
                            )
                    event_name = candidate.entry_title or candidate.tvg_name or "-"
                    tvg_name = candidate.tvg_name or "-"
                    group_name = candidate.group_title or "-"
                    last_updated = block.row_last_updated.get(candidate_row_key(candidate))
                    last_updated_text = (
                        f"Last Updated {_compact_timestamp(last_updated, snapshot.created_at)}"
                        if last_updated is not None
                        else "Last Updated -"
                    )
                    freshness_ts = candidate.extra.get("source_freshness_ts")
                    freshness_source = str(
                        candidate.extra.get("source_freshness_source") or "unknown"
                    )
                    if freshness_ts is not None:
                        try:
                            freshness_time = datetime.fromtimestamp(float(freshness_ts))
                            freshness_text = (
                                "Source Updated "
                                f"{_compact_timestamp(freshness_time, snapshot.created_at)} "
                                f"[{freshness_source}]"
                            )
                        except (TypeError, ValueError, OSError, OverflowError):
                            freshness_text = "Source Updated - [unknown]"
                    else:
                        freshness_text = f"Source Updated - [{freshness_source}]"
                    trailing_state = (
                        ""
                        if state_text == "WORKING"
                        else " | " + _off_text(state_text, color)
                    )
                    trailing_selection = ""
                    if selection_marker:
                        trailing_selection = " | " + selection_marker.strip()
                    elif selection_reason:
                        trailing_selection = (
                            " | " + _secondary_text(selection_reason, color)
                        )
                    source_reference = effective_source_references.get(
                        str(source.source_id)
                    )
                    source_reference_text = (
                        " " + _source_reference_text(f"[S{source_reference}]", color)
                        if source_reference is not None
                        else ""
                    )
                    lines.append(
                        "        "
                        f"{marker_prefix}{on_off} "
                        f"{_event_title(event_name, color)} | "
                        f"{_secondary_text(tvg_name, color)} | "
                        f"{_group_text(group_name, color)} | "
                        f"{_source_reference_text(source.source_name, color)}{source_reference_text} | "
                        f"{_secondary_text(last_updated_text, color)} | "
                        f"{_secondary_text(freshness_text, color)}"
                        f"{trailing_state}"
                        f"{trailing_selection}"
                    )
                    for detail in details_to_show:
                        lines.append(
                            f"             {_change_detail_text(detail, color)}"
                        )

            if index != len(policy_blocks):
                lines.append("")

    visible_source_references = [
        (effective_source_references[source_id], source_id)
        for source_id in current_source_ids
        if source_id in effective_source_references
    ]
    if visible_source_references:
        lines.append("")
        lines.append(_source_reference_text("SOURCE REFERENCES", color))
        for number, source_id in sorted(visible_source_references):
            lines.append(
                _source_reference_text(f"  [S{number}] {source_id}", color)
            )

    if snapshot.source_errors:
        lines.append("")
        lines.append("Source warnings:")
        for error in snapshot.source_errors:
            lines.append(f"  - {error}")

    if not snapshot.blocks:
        lines.append("")
        lines.append("No qualifying identities in the current active target scope.")

    return "\n".join(lines)


def _identity_usefulness_rank(block) -> Tuple[int, int]:
    """Rank identities by how much of their visible source evidence is usable."""
    states = [
        candidate_state(candidate)
        for candidate in block.candidates
    ]
    total = len(states)
    unusable = sum(state != "WORKING" for state in states)

    if unusable == 0:
        category = 0
    elif total > 0 and unusable < total:
        category = 1
    else:
        category = 2

    return category, unusable


def update_display_order(
    previous_order: Mapping[str, Sequence[str]],
    snapshot: DashboardSnapshot,
) -> Dict[str, List[str]]:
    """Order useful identities first while preserving stable order within ties."""
    result: Dict[str, List[str]] = {}
    for policy in (POLICY_ALL, POLICY_MANUAL):
        policy_blocks = [
            block
            for key, block in snapshot.blocks.items()
            if key[0] == policy
        ]
        current = [block.identity.serialized for block in policy_blocks]
        current_set = set(current)
        old = [
            identity
            for identity in previous_order.get(policy, ())
            if identity in current_set
        ]
        old_position = {
            identity: index
            for index, identity in enumerate(old)
        }
        new_position = {
            identity: index
            for index, identity in enumerate(
                identity
                for identity in current
                if identity not in old_position
            )
        }

        policy_blocks.sort(
            key=lambda block: (
                *_identity_usefulness_rank(block),
                0 if block.identity.serialized in new_position else 1,
                new_position.get(
                    block.identity.serialized,
                    old_position.get(block.identity.serialized, 0),
                ),
            )
        )
        result[policy] = [
            block.identity.serialized
            for block in policy_blocks
        ]
    return result


def clear_dashboard_terminal() -> None:
    if not _terminal_is_interactive():
        return
    os.system("cls" if os.name == "nt" else "clear")


def set_live_status_line(text: str) -> None:
    """Create/update one live WATCH row without scrolling the terminal."""
    global _LIVE_STATUS_ACTIVE, _LIVE_STATUS_TEXT
    if not _terminal_is_interactive():
        return
    text = str(text)
    if not _LIVE_STATUS_ACTIVE:
        print("")
        print(text)
        _LIVE_STATUS_ACTIVE = True
        _LIVE_STATUS_TEXT = text
        return
    if text == _LIVE_STATUS_TEXT:
        return
    sys.stdout.write("\033[s\033[1A\r\033[2K" + text + "\033[u")
    sys.stdout.flush()
    _LIVE_STATUS_TEXT = text


def clear_live_status_line() -> None:
    global _LIVE_STATUS_ACTIVE, _LIVE_STATUS_TEXT
    if not _LIVE_STATUS_ACTIVE:
        return
    if _terminal_is_interactive():
        sys.stdout.write("\033[s\033[1A\r\033[2K\033[u")
        sys.stdout.flush()
    _LIVE_STATUS_ACTIVE = False
    _LIVE_STATUS_TEXT = ""


def _format_countdown(seconds: float) -> str:
    remaining = max(0, int(seconds))
    minutes, secs = divmod(remaining, 60)
    return f"{minutes:02d}:{secs:02d}"


def watch_status_text(
    last_scan_wall_time: Optional[float],
    next_refresh_monotonic: float,
) -> str:
    from datetime import datetime
    import time

    last_scan = (
        datetime.fromtimestamp(last_scan_wall_time).strftime("%H:%M")
        if last_scan_wall_time is not None
        else "--:--"
    )
    return (
        f"Watching | Last scan {last_scan} | "
        f"Next scan {_format_countdown(next_refresh_monotonic - time.monotonic())} "
        "| r=record | i=info | F5=refresh | Ctrl+C=exit"
    )


def coordinator_sound_state_text(
    sound_state: SoundSnoozeState,
    *,
    now_ts: Optional[float] = None,
) -> str:
    if not runtime_sound.is_sound_snoozed(
        sound_state,
        now_ts=now_ts,
        indefinite_modes=("coordinator_run",),
    ):
        return "ON"
    if sound_state.sound_snooze_mode == "timed":
        remaining = runtime_sound.sound_snooze_remaining_seconds(
            sound_state,
            now_ts=now_ts,
        )
        return f"SNOOZED — {_format_countdown(remaining)} remaining"
    return "SNOOZED — full Coordinator run"


def render_coordinator_controls(sound_state: SoundSnoozeState) -> str:
    return "\n".join([
        "",
        "================ COORDINATOR INFORMATION & CONTROLS ================",
        f"Sound state : {coordinator_sound_state_text(sound_state)}",
        "",
        "  R  Record a MANUAL identity",
        "  S  Sound / notification snooze",
        "  F5 Refresh now",
        "  Ctrl+C  Exit",
        "====================================================================",
        "",
    ])


def render_manual_record_menu(
    snapshot: DashboardSnapshot,
    identity_choices: Sequence[Tuple[int, str]],
) -> str:
    lines = [
        "",
        "================ RECORD MANUAL IDENTITY ================",
    ]
    for number, identity_key in identity_choices:
        block = snapshot.blocks[(POLICY_MANUAL, identity_key)]
        candidate = block.best_candidate
        title = (
            str(candidate.entry_title or "").strip()
            if candidate is not None
            else ""
        ) or (
            str(candidate.tvg_name or "").strip()
            if candidate is not None
            else ""
        ) or identity_key
        quality = quality_text(candidate, include_provenance=False)
        lines.append(f"  {number}. {title}")
        lines.append(
            f"     {block.identity.provider} | "
            f"{block.identity.lane_key} | {quality}"
        )
    lines.extend([
        "",
        "Enter identity number and press Enter. Esc cancels.",
        "========================================================",
        "",
    ])
    return "\n".join(lines)


def render_sound_snooze_menu(sound_state: SoundSnoozeState) -> str:
    return "\n".join([
        "",
        "================ SOUND / NOTIFICATION SNOOZE ================",
        f"Current sound state : {coordinator_sound_state_text(sound_state)}",
        "",
        "  M  Snooze for 15 minutes",
        "  F  Snooze for full Coordinator run",
        "  U  Unsnooze / restore sounds",
        "  Esc  Cancel",
        "==============================================================",
        "",
    ])


def write_log(path: Path, text: str) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(text.rstrip() + "\n")


COORDINATOR_NOTIFICATION_SOUND_FILENAME = "happy_notification.wav"
COORDINATOR_NOTIFICATION_VOLUME = 0.75


def _coordinator_notification_sound_path() -> Path:
    """Resolve the optional one-shot Coordinator sound beside the repo scripts."""
    return Path(__file__).resolve().parent.parent / COORDINATOR_NOTIFICATION_SOUND_FILENAME


def _scale_pcm_frames(frames: bytes, sample_width: int, volume: float) -> bytes:
    """Scale integer PCM samples without changing the user's system volume."""
    factor = max(0.0, min(1.0, float(volume)))
    if factor >= 0.999 or not frames:
        return frames

    if sample_width == 1:
        output = bytearray(len(frames))
        for index, sample in enumerate(frames):
            scaled = int(round((sample - 128) * factor + 128))
            output[index] = max(0, min(255, scaled))
        return bytes(output)

    if sample_width not in (2, 3, 4):
        raise ValueError(f"unsupported PCM sample width: {sample_width}")

    bits = sample_width * 8
    minimum = -(1 << (bits - 1))
    maximum = (1 << (bits - 1)) - 1
    output = bytearray()
    for offset in range(0, len(frames), sample_width):
        chunk = frames[offset:offset + sample_width]
        if len(chunk) != sample_width:
            output.extend(chunk)
            continue
        sample = int.from_bytes(chunk, "little", signed=True)
        scaled = int(round(sample * factor))
        scaled = max(minimum, min(maximum, scaled))
        output.extend(scaled.to_bytes(sample_width, "little", signed=True))
    return bytes(output)


def _attenuated_notification_sound_path(
    source_path: Path,
    volume: float = COORDINATOR_NOTIFICATION_VOLUME,
) -> Path:
    """Create one cached quieter PCM WAV while preserving async file playback."""
    factor = max(0.0, min(1.0, float(volume)))
    if factor >= 0.999:
        return source_path

    try:
        stat = source_path.stat()
        cache_key = hashlib.sha256(
            (
                f"{source_path.resolve()}|{stat.st_mtime_ns}|{stat.st_size}|"
                f"{factor:.4f}"
            ).encode("utf-8")
        ).hexdigest()[:16]
        output_path = Path(tempfile.gettempdir()) / (
            f"recorder_coordinator_notification_{os.getpid()}_{cache_key}.wav"
        )
        if output_path.is_file():
            return output_path

        with wave.open(str(source_path), "rb") as source:
            if source.getcomptype() != "NONE":
                return source_path
            params = source.getparams()
            frames = source.readframes(source.getnframes())

        scaled_frames = _scale_pcm_frames(frames, params.sampwidth, factor)
        with wave.open(str(output_path), "wb") as output:
            output.setparams(params)
            output.writeframes(scaled_frames)
        return output_path
    except Exception:
        # If the custom file is an unusual WAV encoding, preserve notification
        # behavior rather than failing the alert entirely.
        return source_path


def beep(
    events: Sequence[ChangeEvent],
    sound_state: Optional[SoundSnoozeState] = None,
) -> None:
    if sound_state is not None and runtime_sound.is_sound_snoozed(
        sound_state,
        indefinite_modes=("coordinator_run",),
    ):
        return
    if winsound is None or not any(event.beep for event in events):
        return
    try:
        sound_path = _coordinator_notification_sound_path()
        if sound_path.is_file():
            playback_path = _attenuated_notification_sound_path(sound_path)
            winsound.PlaySound(
                str(playback_path),
                winsound.SND_FILENAME | winsound.SND_ASYNC,
            )
            return

        # Safe fallback when the custom WAV is not installed on this machine.
        winsound.Beep(523, 180)
        winsound.Beep(659, 320)
    except Exception:
        pass
