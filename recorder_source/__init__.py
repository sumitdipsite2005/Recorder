"""Shared source acquisition and intelligence for the recorder project."""

from .identity import CanonicalFeedIdentity, derive_feed_identity, group_candidates_by_identity
from .models import (
    MatchDefinition,
    MatchEvaluation,
    PlaylistSourceSpec,
    SelectionDecision,
    SelectionPolicy,
    SourceAcquisitionRequest,
    SourceAcquisitionResult,
    SourceCandidate,
)

__all__ = [
    "CanonicalFeedIdentity",
    "MatchDefinition",
    "MatchEvaluation",
    "PlaylistSourceSpec",
    "SelectionDecision",
    "SelectionPolicy",
    "SourceAcquisitionRequest",
    "SourceAcquisitionResult",
    "SourceCandidate",
    "derive_feed_identity",
    "group_candidates_by_identity",
]
