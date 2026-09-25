"""Current-state construction and change detection for Inspect / Watch.

This module contains the Coordinator's state intelligence only. Terminal
rendering and terminal side effects live in recorder_coordinator.terminal.
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlsplit

from recorder_source.identity import derive_feed_identity
from recorder_source.models import SourceCandidate
from recorder_source.policy import DEFAULT_SELECTION_POLICY, PROVIDER_SELECTION_POLICIES
from recorder_source.quality import format_candidate_quality
from recorder_source.selection import select_join_candidate, video_quality_rank

from .models import (
    ChangeEvent,
    DashboardSnapshot,
    IdentityBlock,
    SourceObservation,
    TargetView,
)


PROVIDER_SELECTION_POLICY = PROVIDER_SELECTION_POLICIES


def compact_source_name(url: str) -> str:
    """Keep source provenance recognizable without query/token noise."""
    try:
        parsed = urlsplit(url)
        host = parsed.netloc
        parts = [part for part in parsed.path.split("/") if part]
        if host.casefold() == "raw.githubusercontent.com" and len(parts) >= 3:
            owner, repo = parts[0], parts[1]
            rest = parts[2:]
            branch = ""
            if len(rest) >= 3 and rest[0:2] == ["refs", "heads"]:
                branch = rest[2]
                rest = rest[3:]
            elif rest:
                branch = rest[0]
                rest = rest[1:]
            suffix = "/".join(rest)
            branch_text = f"@{branch}" if branch else ""
            return f"github:{owner}/{repo}{branch_text}/{suffix}".rstrip("/")
        suffix = "/".join(parts)
        return f"{host}/{suffix}".rstrip("/") if suffix else host
    except Exception:
        return url


def _candidate_source_id(candidate: SourceCandidate) -> str:
    return str(candidate.playlist_url or candidate.extra.get("source_name") or "unknown-source")


def _candidate_source_name(candidate: SourceCandidate) -> str:
    return str(candidate.extra.get("source_name") or compact_source_name(candidate.playlist_url))


def candidate_row_key(candidate: SourceCandidate) -> Tuple[str, str, str, str]:
    """Presentation key for one source/metadata row."""
    return (
        _candidate_source_id(candidate),
        str(candidate.tvg_name or "").strip(),
        str(candidate.group_title or "").strip(),
        str(candidate.entry_title or "").strip(),
    )


def candidate_update_key(candidate: SourceCandidate) -> Tuple[str, int, str]:
    """Stable row identity used to retain Last Updated across refreshes."""
    return (
        _candidate_source_id(candidate),
        int(candidate.matching_entry_index or 0),
        str(candidate.stream_url or candidate.raw_stream_url or "").strip(),
    )


def candidate_update_signature(candidate: SourceCandidate) -> Tuple[object, ...]:
    return (
        str(candidate.extra.get("source_name") or "").strip(),
        str(candidate.tvg_name or "").strip(),
        str(candidate.group_title or "").strip(),
        str(candidate.entry_title or "").strip(),
        candidate_state(candidate),
    )


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


def candidate_state(candidate: SourceCandidate) -> str:
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


def quality_text(
    candidate: Optional[SourceCandidate],
    *,
    include_provenance: bool = True,
) -> str:
    if candidate is None:
        return "no working candidate"
    return format_candidate_quality(
        candidate,
        motion_cap_fps=50.0,
        include_provenance=include_provenance,
    )


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
    candidate_states = tuple(sorted(candidate_state(item) for item in candidates))
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
    row_update_registry: Optional[Dict[Tuple[object, ...], Tuple[Tuple[object, ...], datetime]]] = None,
) -> DashboardSnapshot:
    current_time = now or datetime.now()
    row_update_registry = row_update_registry if row_update_registry is not None else {}
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
        for candidate in block.candidates:
            row_key = candidate_row_key(candidate)
            global_key = (block.identity.serialized,) + candidate_update_key(candidate)
            signature = candidate_update_signature(candidate)
            previous_entry = row_update_registry.get(global_key)
            if previous_entry is None or previous_entry[0] != signature:
                row_update_registry[global_key] = (signature, current_time)
            block.row_last_updated[row_key] = row_update_registry[global_key][1]
        block.best_candidate = _best_candidate(block.candidates, block.identity.provider)
        block.overall_state = "AVAILABLE" if block.best_candidate is not None else "UNUSABLE"

    return DashboardSnapshot(
        created_at=current_time,
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
            for candidate in new.observations[source_id].candidates:
                new.row_last_updated[candidate_row_key(candidate)] = current.created_at
            events.append(
                ChangeEvent(
                    "SOURCE+",
                    key,
                    (f"source added: {new.observations[source_id].source_name}",),
                    beep=False,
                    source_id=source_id,
                )
            )
        for source_id in sorted(old_sources - new_sources):
            events.append(
                ChangeEvent(
                    "SOURCE-",
                    key,
                    (f"source removed: {old.observations[source_id].source_name}",),
                    beep=False,
                    source_id=source_id,
                )
            )

        source_state_update = False
        for source_id in sorted(old_sources & new_sources):
            before = old.observations[source_id]
            after = new.observations[source_id]
            details: List[str] = []
            if before.event_names != after.event_names:
                details.append(
                    f"Event {_joined(before.event_names)} -> {_joined(after.event_names)}"
                )
            if before.group_titles != after.group_titles:
                details.append(
                    f"Group {_joined(before.group_titles)} -> {_joined(after.group_titles)}"
                )
            if before.tvg_names != after.tvg_names:
                details.append(
                    f"TVG {_joined(before.tvg_names)} -> {_joined(after.tvg_names)}"
                )
            if before.candidate_states != after.candidate_states:
                source_state_update = True
                details.append(
                    "Candidate states "
                    f"{_joined(before.candidate_states)} -> {_joined(after.candidate_states)}"
                )
            elif before.state != after.state:
                source_state_update = True
                details.append(f"State {before.state} -> {after.state}")

            if details:
                for candidate in after.candidates:
                    new.row_last_updated[candidate_row_key(candidate)] = current.created_at
                events.append(
                    ChangeEvent(
                        "UPDATE",
                        key,
                        tuple(details),
                        beep=True,
                        source_id=source_id,
                    )
                )

        if old.overall_state != new.overall_state and not source_state_update:
            events.append(
                ChangeEvent(
                    "UPDATE",
                    key,
                    (f"Identity state {old.overall_state} -> {new.overall_state}",),
                    beep=True,
                )
            )

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
                            f"{quality_text(old.best_candidate)} -> "
                            f"{quality_text(new.best_candidate)}",
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
                            f"{quality_text(old.best_candidate)} -> "
                            f"{quality_text(new.best_candidate)}",
                        ),
                        beep=False,
                    )
                )

    return tuple(events)


def _joined(values: Sequence[str]) -> str:
    return " / ".join(values) if values else "-"


