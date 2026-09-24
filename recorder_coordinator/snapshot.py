"""Current-state construction and change detection for Inspect / Watch.

This module contains the Coordinator's state intelligence only. Terminal
rendering and terminal side effects live in recorder_coordinator.terminal.
"""

from __future__ import annotations

import time
from typing import Dict, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlsplit

from recorder_source.identity import derive_feed_identity
from recorder_source.models import SourceCandidate
from recorder_source.policy import DEFAULT_SELECTION_POLICY, PROVIDER_SELECTION_POLICIES
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
    try:
        parsed = urlsplit(url)
        path = parsed.path.rstrip("/")
        tail = "/".join(path.split("/")[-3:])
        return f"{parsed.netloc}/{tail}" if tail else parsed.netloc
    except Exception:
        return url


def _candidate_source_id(candidate: SourceCandidate) -> str:
    return str(candidate.playlist_url or candidate.extra.get("source_name") or "unknown-source")


def _candidate_source_name(candidate: SourceCandidate) -> str:
    return str(candidate.extra.get("source_name") or compact_source_name(candidate.playlist_url))


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


def quality_text(candidate: Optional[SourceCandidate]) -> str:
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


