"""Identity Coordinator source acquisition, matching, and freshness evaluation."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from recorder_coordinator.configuration import (
    _match_definition_for_target,
    _source_context_group_for_target,
    sources_for_group,
)
from recorder_coordinator.models import IdentityTarget, TargetView
from recorder_source.discovery import (
    fetch_playlist_documents,
    parse_playlist_text,
    probe_candidates,
    resolve_playlist_source_freshness,
)
from recorder_source.identity import derive_feed_identity
from recorder_source.matching import evaluate_match, make_match_definition
from recorder_source.models import PlaylistSourceSpec, SourceCandidate
from recorder_source.policy import PLAYLIST_GROUP_MATCH_MODES as GROUP_MATCH_MODE

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


def _normalized_primary_metadata(value: object) -> str:
    text = str(value or "").casefold()
    normalized = "".join(
        character if character.isalnum() else " "
        for character in text
    )
    return " ".join(normalized.split())


def _primary_metadata_values(candidate: SourceCandidate) -> Tuple[str, ...]:
    values: List[str] = []
    for raw_value in (candidate.entry_title, candidate.tvg_name):
        normalized = _normalized_primary_metadata(raw_value)
        if normalized and normalized not in values:
            values.append(normalized)
    return tuple(values)


def _metadata_compatible_with_matching(
    candidate: SourceCandidate,
    matching_candidates: Sequence[SourceCandidate],
) -> bool:
    """Return True when same-feed metadata is compatible but less specific."""
    current_values = _primary_metadata_values(candidate)
    if not current_values:
        return True

    matching_values = tuple(
        value
        for matching_candidate in matching_candidates
        for value in _primary_metadata_values(matching_candidate)
    )
    if not matching_values:
        return True

    for current in current_values:
        for matching in matching_values:
            if current == matching:
                return True
            shorter, longer = (
                (current, matching)
                if len(current) <= len(matching)
                else (matching, current)
            )
            if len(shorter) >= 8 and shorter in longer:
                return True
    return False


def _context_candidate_for_target(
    candidate: SourceCandidate,
    target: IdentityTarget,
    source_group: str,
    *,
    matching_candidates: Sequence[SourceCandidate] = (),
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
    compatible_metadata = _metadata_compatible_with_matching(
        candidate,
        matching_candidates,
    )
    return replace(
        candidate,
        preferred_qualifier_score=evaluation.preferred_qualifier_score,
        ignored=True,
        reason=(
            "same feed identity context; source metadata is compatible but "
            "less specific than a matching observation"
            if compatible_metadata
            else (
                "same feed identity context; this source's current primary "
                "metadata conflicts with the target"
            )
        ),
        extra={
            **dict(candidate.extra),
            "freshness_disqualifying_conflict": not compatible_metadata,
        },
    )


def _identity_serialized(candidate: SourceCandidate) -> str:
    provider = str(candidate.extra.get("provider") or "UNKNOWN").upper()
    return derive_feed_identity(candidate, provider).serialized


def _freshness_eligible_identity_keys(
    candidates: Sequence[SourceCandidate],
) -> Set[str]:
    """Return identities that remain target-eligible after freshness review.

    Matching rows are marked ignored=False. Same-identity context rows are
    freshness-disqualifying only when their primary metadata actually conflicts
    with the matching observation. Shorter/incomplete metadata is not evidence
    that the feed moved to another event. Unknown freshness and ties containing
    matching evidence remain conservative: keep.
    """
    by_identity: Dict[str, List[SourceCandidate]] = {}
    for candidate in candidates:
        by_identity.setdefault(_identity_serialized(candidate), []).append(candidate)

    eligible: Set[str] = set()
    for identity_key, identity_candidates in by_identity.items():
        known = []
        for candidate in identity_candidates:
            if (
                candidate.ignored
                and candidate.extra.get(
                    "freshness_disqualifying_conflict"
                ) is False
            ):
                continue
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
    quality_evidence_registry: Optional[Dict[str, Mapping[str, object]]] = None,
    stop_requested: Optional[Callable[[], bool]] = None,
) -> Tuple[Dict[str, Tuple[SourceCandidate, ...]], Tuple[str, ...]]:
    def raise_if_cancelled() -> None:
        if stop_requested is not None and stop_requested():
            raise RuntimeError("Coordinator scan cancelled by stop request")

    cancellation_kwargs = (
        {"stop_requested": stop_requested}
        if stop_requested is not None
        else {}
    )
    quality_registry_kwargs = (
        {"quality_evidence_registry": quality_evidence_registry}
        if quality_evidence_registry is not None
        else {}
    )

    raise_if_cancelled()
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
        documents, fetch_errors, fetch_diagnostics = fetch_playlist_documents(
            source_specs,
            **cancellation_kwargs,
        )
    else:
        progress_callback(f"Scanning playlists 0/{len(source_specs)}")
        documents, fetch_errors, fetch_diagnostics = fetch_playlist_documents(
            source_specs,
            progress_callback=(
                lambda done, total: progress_callback(
                    f"Scanning playlists {done}/{total}"
                )
            ),
            **cancellation_kwargs,
        )

    raise_if_cancelled()

    freshness_by_url: Dict[str, Mapping[str, object]] = {}
    freshness_now = time.time()

    def resolve_freshness(spec: PlaylistSourceSpec):
        raise_if_cancelled()
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
                **cancellation_kwargs,
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
            raise_if_cancelled()
            source_url, freshness = future.result()
            if freshness is None:
                continue
            freshness_by_url[source_url] = freshness
            if source_freshness_registry is not None:
                source_freshness_registry[source_url] = freshness

    raise_if_cancelled()

    parsed_by_key: Dict[Tuple[str, str, str], Tuple[SourceCandidate, ...]] = {}
    errors: List[str] = list(fetch_errors)
    for key, spec in source_specs_by_key.items():
        raise_if_cancelled()
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
        raise_if_cancelled()
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
        matching_by_identity: Dict[str, List[SourceCandidate]] = {}
        for matching_candidate in raw_candidates_by_target[target.name]:
            if matching_candidate.ignored:
                continue
            matching_by_identity.setdefault(
                _identity_serialized(matching_candidate),
                [],
            ).append(matching_candidate)
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
                    identity_key = _identity_serialized(candidate)
                    context = _context_candidate_for_target(
                        candidate,
                        target,
                        context_group,
                        matching_candidates=matching_by_identity.get(
                            identity_key,
                            (),
                        ),
                    )
                    if context is None:
                        continue
                    raw_candidates_by_target[target.name].append(context)
                    existing_keys.add(_observation_key(context))

    # A stale matching row must not keep an identity eligible when newer
    # credible metadata for that same identity has moved to another event.
    # Ambiguous ties and identities with no usable freshness remain visible.
    for target_name, candidates in raw_candidates_by_target.items():
        raise_if_cancelled()
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
    raise_if_cancelled()
    if progress_callback is None:
        probed = probe_candidates(
            probe_pool,
            **cancellation_kwargs,
            **quality_registry_kwargs,
        )
    else:
        progress_callback(f"Checking candidates 0/{len(probe_pool)}")
        probed = probe_candidates(
            probe_pool,
            progress_callback=(
                lambda done, total: progress_callback(
                    f"Checking candidates {done}/{total}"
                )
            ),
            **cancellation_kwargs,
            **quality_registry_kwargs,
        )
    raise_if_cancelled()
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



