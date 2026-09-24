"""Shared candidate quality/lifetime selection with explicit policy inputs."""

from __future__ import annotations

import time
from typing import Mapping, Optional, Sequence, Tuple, Union

from .models import SelectionDecision, SelectionPolicy, SourceCandidate


def normalize_video_scan_type(value: object) -> str:
    text = str(value or "").strip().casefold()
    if text in ("progressive", "p"):
        return "progressive"
    if text in ("interlaced", "interlace", "i", "tt", "bb", "tb", "bt"):
        return "interlaced"
    return ""


def has_quality_evidence(quality: Union[Mapping[str, object], SourceCandidate]) -> bool:
    candidate = _as_candidate(quality)
    return bool(
        candidate.quality_known
        or candidate.video_fps > 0
        or candidate.video_width > 0
        or candidate.video_height > 0
        or candidate.video_bitrate_bps > 0
    )


def video_scan_rank(quality: Union[Mapping[str, object], SourceCandidate]) -> int:
    scan_type = normalize_video_scan_type(_as_candidate(quality).video_scan_type)
    if scan_type == "progressive":
        return 2
    if scan_type == "interlaced":
        return 1
    return 0


def comparable_motion_fps(
    quality: Union[Mapping[str, object], SourceCandidate],
    *,
    motion_cap_fps: float,
) -> float:
    candidate = _as_candidate(quality)
    fps = float(candidate.video_fps or 0.0)
    if fps <= 0:
        return 0.0

    scan_type = normalize_video_scan_type(candidate.video_scan_type)
    if scan_type == "interlaced":
        half_cap = float(motion_cap_fps) / 2.0
        if fps <= half_cap + 0.01:
            return fps * 2.0
    return fps


def ranking_motion_fps(
    quality: Union[Mapping[str, object], SourceCandidate],
    *,
    motion_cap_fps: float,
) -> float:
    return min(
        comparable_motion_fps(quality, motion_cap_fps=motion_cap_fps),
        float(motion_cap_fps),
    )


def video_resolution_class(
    quality: Union[Mapping[str, object], SourceCandidate],
) -> int:
    candidate = _as_candidate(quality)
    width = int(candidate.video_width or 0)
    height = int(candidate.video_height or 0)
    if width <= 0 or height <= 0:
        return 0
    if height >= 2160:
        return 2160
    if height >= 1440:
        return 1440
    if height >= 1080:
        return 1080
    if height >= 720:
        return 720
    return 0


def video_quality_rank(
    quality: Union[Mapping[str, object], SourceCandidate],
    *,
    motion_cap_fps: float,
) -> Tuple[object, ...]:
    candidate = _as_candidate(quality)
    fps = float(candidate.video_fps or 0.0)
    width = int(candidate.video_width or 0)
    height = int(candidate.video_height or 0)
    bitrate = int(candidate.video_bitrate_bps or 0)
    scan_rank = video_scan_rank(candidate)
    motion_fps = ranking_motion_fps(candidate, motion_cap_fps=motion_cap_fps)
    motion_class = 2 if motion_fps >= float(motion_cap_fps) else 1 if motion_fps > 0 else 0
    resolution_class = video_resolution_class(candidate)
    quality_known = int(
        bool(candidate.quality_known or fps > 0 or (width > 0 and height > 0) or bitrate > 0)
    )
    return (
        quality_known,
        motion_class,
        resolution_class,
        motion_fps,
        scan_rank,
        width * height,
        height,
        width,
        bitrate,
    )


def candidate_quality_rank(
    candidate: Union[Mapping[str, object], SourceCandidate],
    policy: SelectionPolicy,
) -> Tuple[object, ...]:
    normalized = _as_candidate(candidate)
    expiry = normalized.expiry
    if policy.prefer_unknown_expiry_on_equal_quality:
        authorization_rank = (int(expiry is None), int(expiry or 0))
    else:
        authorization_rank = (0, int(expiry or 0))

    return (
        int(normalized.preferred_qualifier_score or 0),
        *video_quality_rank(normalized, motion_cap_fps=policy.motion_cap_fps),
        *authorization_rank,
    )


def select_join_candidate(
    candidates: Sequence[SourceCandidate],
    policy: SelectionPolicy,
    *,
    now_ts: Optional[float] = None,
) -> SelectionDecision:
    if not candidates:
        return SelectionDecision(
            selected=None,
            selected_index=None,
            decision_type="no_candidates",
            reason="no candidates supplied",
            fallback_used=False,
            considered_count=0,
            eligible_count=0,
        )

    now = time.time() if now_ts is None else float(now_ts)
    eligible_indexes = [
        index
        for index, candidate in enumerate(candidates)
        if candidate.expiry is not None or policy.allow_unknown_expiry
    ]
    if not eligible_indexes:
        return SelectionDecision(
            selected=None,
            selected_index=None,
            decision_type="no_eligible_candidates",
            reason="all candidates have unknown expiry and policy disallows unknown expiry",
            fallback_used=False,
            considered_count=len(candidates),
            eligible_count=0,
        )

    safe_indexes = [
        index
        for index in eligible_indexes
        if candidates[index].expiry is None
        or float(candidates[index].expiry) - now >= int(policy.mandatory_min_remaining_sec)
    ]
    pool_indexes = safe_indexes or eligible_indexes
    selected_index = max(
        pool_indexes,
        key=lambda index: candidate_quality_rank(candidates[index], policy),
    )
    fallback_used = not bool(safe_indexes)
    return SelectionDecision(
        selected=candidates[selected_index],
        selected_index=selected_index,
        decision_type="selected",
        reason=(
            "best candidate meeting mandatory minimum lifetime"
            if safe_indexes
            else "best shorter-lived candidate because none meet mandatory minimum lifetime"
        ),
        fallback_used=fallback_used,
        considered_count=len(candidates),
        eligible_count=len(pool_indexes),
    )


def select_quality_upgrade(
    running_source: SourceCandidate,
    candidates: Sequence[SourceCandidate],
    target_fps: float,
    policy: SelectionPolicy,
    *,
    now_ts: Optional[float] = None,
) -> SelectionDecision:
    now = time.time() if now_ts is None else float(now_ts)
    running_rank = video_quality_rank(
        running_source,
        motion_cap_fps=policy.motion_cap_fps,
    )
    running_preferred_score = int(running_source.preferred_qualifier_score or 0)
    eligible_indexes = []

    for index, candidate in enumerate(candidates):
        candidate_motion_fps = ranking_motion_fps(
            candidate,
            motion_cap_fps=policy.motion_cap_fps,
        )
        if candidate_motion_fps < target_fps:
            continue

        if candidate.expiry is None:
            if not policy.allow_unknown_expiry:
                continue
        elif float(candidate.expiry) - now < int(policy.upgrade_min_remaining_sec):
            continue

        if int(candidate.preferred_qualifier_score or 0) < running_preferred_score:
            continue
        if video_quality_rank(candidate, motion_cap_fps=policy.motion_cap_fps) <= running_rank:
            continue
        eligible_indexes.append(index)

    if not eligible_indexes:
        return SelectionDecision(
            selected=None,
            selected_index=None,
            decision_type="no_upgrade",
            reason="no candidate satisfies upgrade quality/lifetime/preference rules",
            fallback_used=False,
            considered_count=len(candidates),
            eligible_count=0,
        )

    selected_index = max(
        eligible_indexes,
        key=lambda index: candidate_quality_rank(candidates[index], policy),
    )
    return SelectionDecision(
        selected=candidates[selected_index],
        selected_index=selected_index,
        decision_type="upgrade_selected",
        reason="best eligible quality upgrade",
        fallback_used=False,
        considered_count=len(candidates),
        eligible_count=len(eligible_indexes),
    )


def _as_candidate(value: Union[Mapping[str, object], SourceCandidate]) -> SourceCandidate:
    if isinstance(value, SourceCandidate):
        return value
    return SourceCandidate.from_mapping(value)
