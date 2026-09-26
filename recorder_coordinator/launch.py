"""Build an explicit MANUAL identity launch plan from one Coordinator snapshot."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

from recorder_runtime.identity_launch import FrozenTargetIntent
from recorder_source.identity import CanonicalFeedIdentity, derive_feed_identity
from recorder_source.models import SourceCandidate
from recorder_source.policy import selection_policy_for_provider
from recorder_source.selection import candidate_quality_rank, select_join_candidate

from .models import DashboardSnapshot, IdentityTarget, POLICY_MANUAL


@dataclass(frozen=True)
class ManualLaunchPlan:
    identity: CanonicalFeedIdentity
    selected_candidate: SourceCandidate
    candidate_pool: Tuple[SourceCandidate, ...]
    selected_source_group: str
    target_intents: Tuple[FrozenTargetIntent, ...]
    recording_duration_min: Optional[float]
    base_name: str


def _candidate_exact_key(candidate: SourceCandidate) -> Tuple[object, ...]:
    """Identify the exact Coordinator candidate, not merely its feed identity."""
    return (
        candidate.playlist_url,
        candidate.matching_entry_index,
        candidate.stream_url,
        candidate.final_stream_url,
        tuple(sorted((str(k), str(v)) for k, v in candidate.headers.items())),
        tuple(candidate.keys),
    )


def _target_intent(target: IdentityTarget) -> FrozenTargetIntent:
    return FrozenTargetIntent(
        name=target.name,
        source_groups=tuple(target.source_groups),
        primary=tuple(target.primary),
        required=tuple(target.required),
        rejected=tuple(target.rejected),
        preferred=tuple(target.preferred),
        match_all=target.match_all,
        worker_recording_duration_min=target.worker_recording_duration_min,
    )


def _combined_duration(targets: Tuple[IdentityTarget, ...]) -> Optional[float]:
    durations = tuple(target.worker_recording_duration_min for target in targets)
    if any(value is None for value in durations):
        return None
    return max(float(value) for value in durations if value is not None)


def _safe_base_name(candidate: SourceCandidate, fallback: str) -> str:
    event_name = (
        str(candidate.entry_title or "").strip()
        or str(candidate.tvg_name or "").strip()
        or str(fallback or "").strip()
    )
    group_name = str(candidate.group_title or "").strip()

    if group_name and event_name and group_name.casefold() != event_name.casefold():
        raw = f"{group_name} - {event_name}"
    else:
        raw = event_name or group_name or "Recording"

    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", raw)
    cleaned = cleaned.rstrip(" .")
    if not cleaned:
        cleaned = "Recording"

    # Windows reserves these device names even when an extension is present.
    stem = cleaned.split(".", 1)[0].strip().upper()
    reserved = {
        "CON", "PRN", "AUX", "NUL",
        "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
        "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    }
    if stem in reserved:
        cleaned = f"_{cleaned}"
    return cleaned


def build_manual_launch_plan(
    snapshot: DashboardSnapshot,
    identity_key: str,
    *,
    now_ts: Optional[float] = None,
) -> ManualLaunchPlan:
    block = snapshot.blocks.get((POLICY_MANUAL, identity_key))
    if block is None:
        raise ValueError(f"MANUAL identity is no longer available: {identity_key}")
    if block.best_candidate is None:
        raise ValueError(f"MANUAL identity has no recordable candidate: {identity_key}")

    target_views = {
        view.target.name: view
        for view in snapshot.target_views
        if view.status == "ACTIVE" and view.target.policy == POLICY_MANUAL
    }

    contexts: List[Tuple[IdentityTarget, SourceCandidate]] = []
    for target_name in block.target_names:
        view = target_views.get(target_name)
        if view is None:
            continue
        for candidate in snapshot.candidates_by_target.get(target_name, ()):
            if not candidate.launchable or candidate.ignored:
                continue
            provider = str(candidate.extra.get("provider") or block.identity.provider)
            identity = derive_feed_identity(candidate, provider)
            if identity.serialized != identity_key:
                continue
            contexts.append((view.target, candidate))

    if not contexts:
        raise ValueError(
            f"MANUAL identity has no active target/candidate context: {identity_key}"
        )

    policy = selection_policy_for_provider(block.identity.provider)
    decision = select_join_candidate(
        [candidate for _, candidate in contexts],
        policy,
        now_ts=time.time() if now_ts is None else float(now_ts),
    )
    if decision.selected_index is None or decision.selected is None:
        raise ValueError(f"MANUAL identity has no selectable candidate: {identity_key}")

    selected_target, selected_candidate = contexts[decision.selected_index]
    selected_key = _candidate_exact_key(selected_candidate)
    selected_rank = candidate_quality_rank(selected_candidate, policy)

    winning_targets: List[IdentityTarget] = []
    seen_target_names = set()
    for target, candidate in contexts:
        if _candidate_exact_key(candidate) != selected_key:
            continue
        if candidate_quality_rank(candidate, policy) != selected_rank:
            continue
        if target.name in seen_target_names:
            continue
        seen_target_names.add(target.name)
        winning_targets.append(target)

    if not winning_targets:
        winning_targets = [selected_target]

    source_group = str(
        selected_candidate.extra.get("source_group") or ""
    ).strip().upper()
    if not source_group:
        raise ValueError(
            "selected Coordinator candidate has no source-group context"
        )

    targets_tuple = tuple(winning_targets)
    return ManualLaunchPlan(
        identity=block.identity,
        selected_candidate=selected_candidate,
        candidate_pool=tuple(block.candidates),
        selected_source_group=source_group,
        target_intents=tuple(_target_intent(target) for target in targets_tuple),
        recording_duration_min=_combined_duration(targets_tuple),
        base_name=_safe_base_name(selected_candidate, selected_target.name),
    )
