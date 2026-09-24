"""Terminal presentation and terminal side effects for the Identity Coordinator."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

try:
    import winsound
except ImportError:  # pragma: no cover - non-Windows development/test hosts
    winsound = None

from .models import (
    ChangeEvent,
    DashboardSnapshot,
    POLICY_ALL,
    POLICY_MANUAL,
)
from .snapshot import candidate_state, quality_text


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
            lines.append(f"    Best: {quality_text(block.best_candidate)}")
            for source in block.observations.values():
                for candidate in source.candidates:
                    state_text = candidate_state(candidate)
                    on_off = "ON" if state_text == "WORKING" else "OFF"
                    event_name = candidate.entry_title or candidate.tvg_name or "-"
                    group_name = candidate.group_title or "-"
                    context_text = " | CONTEXT" if candidate.ignored else ""
                    lines.append(
                        f"    [{on_off}] {event_name} | {group_name} "
                        f"| {quality_text(candidate if candidate.quality_known else None)} "
                        f"| {state_text}{context_text} | {source.source_name}"
                    )
                    if state_text != "WORKING" or candidate.ignored:
                        reason = candidate.reason
                        if candidate.ignored and not reason:
                            reason = (
                                "same feed identity context; this source's current primary "
                                "metadata does not match the target"
                            )
                        if state_text == "AUTH_UNKNOWN" and not reason:
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


def clear_dashboard_terminal() -> None:
    if not _terminal_is_interactive():
        return
    os.system("cls" if os.name == "nt" else "clear")

def write_log(path: Path, text: str) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(text.rstrip() + "\n")


def beep(events: Sequence[ChangeEvent]) -> None:
    if winsound is None or not any(event.beep for event in events):
        return
    try:
        winsound.Beep(880, 180)
        winsound.Beep(1175, 220)
    except Exception:
        pass


