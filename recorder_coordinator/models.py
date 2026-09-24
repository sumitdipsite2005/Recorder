"""Data contracts for Identity Coordinator configuration and watch state."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from recorder_source.identity import CanonicalFeedIdentity
from recorder_source.models import SourceCandidate


POLICY_MANUAL = "MANUAL"
POLICY_ALL = "ALL_IDENTITIES"
VALID_POLICIES = frozenset({POLICY_MANUAL, POLICY_ALL})


@dataclass(frozen=True)
class IdentityTarget:
    name: str
    policy: str
    source_groups: Tuple[str, ...]
    primary: Tuple[object, ...] = ()
    required: Tuple[object, ...] = ()
    rejected: Tuple[object, ...] = ()
    preferred: Tuple[object, ...] = ()
    match_all: bool = False
    enabled: bool = True
    schedule_start: Optional[datetime] = None
    activity_duration_min: Optional[float] = None
    worker_recording_duration_min: Optional[float] = None


@dataclass
class TargetRuntime:
    first_activation: Optional[datetime] = None


@dataclass(frozen=True)
class TargetView:
    target: IdentityTarget
    status: str
    active_from: Optional[datetime]
    active_until: Optional[datetime]


@dataclass(frozen=True)
class CoordinatorWindow:
    status: str
    active_from: datetime
    active_until: Optional[datetime]


@dataclass(frozen=True)
class SourceObservation:
    source_id: str
    source_name: str
    candidates: Tuple[SourceCandidate, ...]
    event_names: Tuple[str, ...]
    tvg_names: Tuple[str, ...]
    group_titles: Tuple[str, ...]
    candidate_states: Tuple[str, ...]
    state: str
    best_candidate: Optional[SourceCandidate]


@dataclass
class IdentityBlock:
    policy: str
    identity: CanonicalFeedIdentity
    target_names: List[str] = field(default_factory=list)
    candidates: List[SourceCandidate] = field(default_factory=list)
    observations: Dict[str, SourceObservation] = field(default_factory=dict)
    best_candidate: Optional[SourceCandidate] = None
    overall_state: str = "UNUSABLE"


@dataclass(frozen=True)
class ChangeEvent:
    marker: str
    block_key: Tuple[str, str]
    details: Tuple[str, ...]
    beep: bool = False


@dataclass
class DashboardSnapshot:
    created_at: datetime
    target_views: Tuple[TargetView, ...]
    coordinator_window: Optional[CoordinatorWindow]
    blocks: Dict[Tuple[str, str], IdentityBlock]
    source_errors: Tuple[str, ...] = ()
    config_messages: Tuple[str, ...] = ()
