"""Terminal presentation and terminal side effects for the Identity Coordinator."""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

try:
    import winsound
except ImportError:  # pragma: no cover - non-Windows development/test hosts
    winsound = None

from recorder_runtime import sound as runtime_sound
from recorder_runtime.sound import SoundSnoozeState

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


def _off_text(text: str, use_color: bool) -> str:
    return _paint_rgb(text, _ERROR_RGB, use_color)


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
        lines.append(f"Refresh      : every {refresh_interval_sec:g}s + manual r")
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


def render_dashboard(
    snapshot: DashboardSnapshot,
    events: Sequence[ChangeEvent],
    display_order: Optional[Mapping[str, Sequence[str]]] = None,
    *,
    config_path: Optional[Path] = None,
    refresh_interval_sec: Optional[float] = None,
    use_color: Optional[bool] = None,
) -> str:
    color = _terminal_is_interactive() if use_color is None else bool(use_color)
    identity_events, source_events, quality_events = _event_maps(events)

    lines: List[str] = _header_lines(
        snapshot,
        config_path=config_path,
        refresh_interval_sec=refresh_interval_sec,
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
            state_label = (
                _important_text(block.overall_state, color)
                if block.overall_state == "AVAILABLE"
                else _off_text(block.overall_state, color)
            )
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
                    event_name = candidate.entry_title or candidate.tvg_name or "-"
                    tvg_name = candidate.tvg_name or "-"
                    group_name = candidate.group_title or "-"
                    last_updated = block.row_last_updated.get(candidate_row_key(candidate))
                    last_updated_text = (
                        f"Last Updated {last_updated:%H:%M}"
                        if last_updated is not None
                        else "Last Updated -"
                    )
                    trailing_state = (
                        ""
                        if state_text == "WORKING"
                        else " | " + _off_text(state_text, color)
                    )
                    lines.append(
                        "        "
                        f"{marker_prefix}{on_off} "
                        f"{_event_title(event_name, color)} | "
                        f"{_secondary_text(tvg_name, color)} | "
                        f"{_group_text(group_name, color)} | "
                        f"{_muted_text(source.source_name, color)} | "
                        f"{_secondary_text(last_updated_text, color)}"
                        f"{trailing_state}"
                    )
                    for detail in details_to_show:
                        lines.append(
                            f"             {_change_detail_text(detail, color)}"
                        )

            if index != len(policy_blocks):
                lines.append("")

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
        current = [key[1] for key in snapshot.blocks if key[0] == policy]
        current_set = set(current)
        old = [identity for identity in previous_order.get(policy, ()) if identity in current_set]
        new = [identity for identity in current if identity not in old]
        result[policy] = new + old
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
        "| i=info | r=refresh | Ctrl+C=exit"
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
        "  S  Sound / notification snooze",
        "  r  Refresh now",
        "  Ctrl+C  Exit",
        "====================================================================",
        "",
    ])


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


def _coordinator_notification_sound_path() -> Path:
    """Resolve the optional one-shot Coordinator sound beside the repo scripts."""
    return Path(__file__).resolve().parent.parent / COORDINATOR_NOTIFICATION_SOUND_FILENAME


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
            winsound.PlaySound(
                str(sound_path),
                winsound.SND_FILENAME | winsound.SND_ASYNC,
            )
            return

        # Safe fallback when the custom WAV is not installed on this machine.
        winsound.Beep(523, 180)
        winsound.Beep(659, 320)
    except Exception:
        pass
