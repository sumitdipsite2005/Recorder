"""Shared, deterministic playlist matching rules."""

from __future__ import annotations

import re
import unicodedata
from urllib.parse import urlparse
from typing import Iterable, List, Sequence, Tuple

from .models import (
    MATCH_MODE_EXACT_CHANNEL,
    MatchDefinition,
    MatchEvaluation,
)


def normalize_match_text(value: str) -> str:
    """Normalize human-readable playlist text exactly as the mature recorder does."""
    text = unicodedata.normalize("NFKC", str(value)).casefold()
    apostrophe_chars = {"'", "’", "‘", "ʼ", "＇"}
    normalized_chars: List[str] = []

    for char in text:
        if char in apostrophe_chars:
            continue
        if unicodedata.category(char).startswith("P"):
            normalized_chars.append(" ")
        else:
            normalized_chars.append(char)

    return " ".join("".join(normalized_chars).split())


def build_match_groups(config_items: Iterable[object]) -> Tuple[Tuple[str, ...], ...]:
    """Convert config-style strings/lists into explicit AND-of-OR groups."""
    groups: List[Tuple[str, ...]] = []

    for item in config_items:
        raw_alternatives: Sequence[object]
        if isinstance(item, (list, tuple)):
            raw_alternatives = item
        else:
            raw_alternatives = (item,)

        alternatives = tuple(
            str(alternative).strip()
            for alternative in raw_alternatives
            if str(alternative).strip()
            and normalize_match_text(str(alternative).strip())
        )
        if alternatives:
            groups.append(alternatives)

    return tuple(groups)


def make_match_definition(
    *,
    mode: str,
    primary: Iterable[object],
    required: Iterable[object] = (),
    rejected: Iterable[object] = (),
    preferred: Iterable[object] = (),
    match_all: bool = False,
) -> MatchDefinition:
    primary_groups = build_match_groups(primary)
    if not match_all and not primary_groups:
        raise RuntimeError(
            "NM3U8DL_PLAYLIST_PRIMARY_PHRASES must contain at least one searchable phrase"
        )

    return MatchDefinition.from_groups(
        mode=mode,
        primary_groups=primary_groups,
        required_qualifier_groups=build_match_groups(required),
        rejected_qualifier_groups=build_match_groups(rejected),
        preferred_qualifier_groups=build_match_groups(preferred),
        match_all=match_all,
    )


def _normalized_groups(
    groups: Sequence[Sequence[str]],
) -> Tuple[Tuple[str, ...], ...]:
    return tuple(
        tuple(normalize_match_text(alternative) for alternative in alternatives)
        for alternatives in groups
    )


def _qualifier_group_matches(
    alternatives: Sequence[str],
    normalized_text: str,
) -> bool:
    return any(
        re.search(
            rf"(?<!\w){re.escape(alternative)}(?!\w)",
            normalized_text,
        )
        is not None
        for alternative in alternatives
    )


def _channel_primary_alternative_matches(
    alternative: str,
    normalized_text: str,
) -> bool:
    if not alternative:
        return False

    end_guard = r"(?=$|[^\w]|\d)" if alternative[-1].isalpha() else r"(?!\w)"
    return (
        re.search(
            rf"(?<!\w){re.escape(alternative)}{end_guard}",
            normalized_text,
        )
        is not None
    )


def evaluate_match(
    definition: MatchDefinition,
    *,
    tvg_name: str,
    group_title: str,
    entry_title: str,
    stream_url: str,
) -> MatchEvaluation:
    """Evaluate one candidate entry using only explicit match inputs."""
    primary_groups = _normalized_groups(definition.primary_groups)
    required_groups = _normalized_groups(definition.required_qualifier_groups)
    rejected_groups = _normalized_groups(definition.rejected_qualifier_groups)
    preferred_groups = _normalized_groups(definition.preferred_qualifier_groups)

    if definition.mode == MATCH_MODE_EXACT_CHANNEL:
        identity_text = " ".join(value for value in (tvg_name, entry_title) if value)
        identity_normalized = normalize_match_text(identity_text)
        rejected_match = any(
            _qualifier_group_matches(alternatives, identity_normalized)
            for alternatives in rejected_groups
        )

        field_scores: List[int] = []
        if not rejected_match:
            for field_value in (tvg_name, entry_title):
                normalized_field = normalize_match_text(field_value)
                if not normalized_field:
                    continue

                primary_match = bool(definition.match_all) or all(
                    any(
                        _channel_primary_alternative_matches(
                            alternative,
                            normalized_field,
                        )
                        for alternative in alternatives
                    )
                    for alternatives in primary_groups
                )
                if not primary_match:
                    continue

                required_match = all(
                    _qualifier_group_matches(alternatives, normalized_field)
                    for alternatives in required_groups
                )
                if not required_match:
                    continue

                preferred_score = sum(
                    1
                    for alternatives in preferred_groups
                    if _qualifier_group_matches(alternatives, normalized_field)
                )
                field_scores.append(preferred_score)

        return MatchEvaluation(
            matches=bool(field_scores),
            preferred_qualifier_score=max(field_scores) if field_scores else 0,
            primary_match=bool(field_scores),
            required_qualifier_match=bool(field_scores),
            rejected_qualifier_match=rejected_match,
        )

    searchable_text = " ".join(
        value for value in (tvg_name, group_title, entry_title) if value
    )
    extinf_normalized = normalize_match_text(searchable_text)
    stream_path_normalized = normalize_match_text(urlparse(stream_url).path)
    primary_search_normalized = " ".join(
        value for value in (extinf_normalized, stream_path_normalized) if value
    )

    primary_match = bool(definition.match_all) or all(
        any(alternative in primary_search_normalized for alternative in alternatives)
        for alternatives in primary_groups
    )
    required_match = all(
        _qualifier_group_matches(alternatives, extinf_normalized)
        for alternatives in required_groups
    )
    rejected_match = any(
        _qualifier_group_matches(alternatives, extinf_normalized)
        for alternatives in rejected_groups
    )
    preferred_score = sum(
        1
        for alternatives in preferred_groups
        if _qualifier_group_matches(alternatives, extinf_normalized)
    )

    return MatchEvaluation(
        matches=primary_match and required_match and not rejected_match,
        preferred_qualifier_score=preferred_score,
        primary_match=primary_match,
        required_qualifier_match=required_match,
        rejected_qualifier_match=rejected_match,
    )


def describe_match_definition(definition: MatchDefinition) -> str:
    def describe_groups(groups: Sequence[Sequence[str]]) -> str:
        parts: List[str] = []
        for alternatives in groups:
            text = " OR ".join(f'"{alternative}"' for alternative in alternatives)
            if len(alternatives) > 1:
                text = f"({text})"
            parts.append(text)
        return " AND ".join(parts)

    primary_description = (
        "MATCH ALL" if definition.match_all else describe_groups(definition.primary_groups)
    )
    if definition.mode == MATCH_MODE_EXACT_CHANNEL and not definition.match_all:
        primary_description = f"channel phrase {primary_description}"

    parts = [primary_description]
    if definition.required_qualifier_groups:
        parts.append(
            "required qualifiers "
            + describe_groups(definition.required_qualifier_groups)
        )
    if definition.rejected_qualifier_groups:
        parts.append(
            "rejected qualifiers "
            + describe_groups(definition.rejected_qualifier_groups)
        )
    if definition.preferred_qualifier_groups:
        parts.append(
            "preferred qualifiers "
            + describe_groups(definition.preferred_qualifier_groups)
        )
    return " with ".join(parts)
