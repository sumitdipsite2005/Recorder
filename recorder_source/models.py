"""Shared source-intelligence contracts for the recorder project.

The models in this module deliberately contain source/acquisition/selection facts
only. Recorder execution state (worker attempts, chunks, alarms, finalization,
etc.) stays outside this package.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple


MATCH_MODE_EXACT_CHANNEL = "EXACT_CHANNEL"
MATCH_MODE_EVENT_PHRASE = "EVENT_PHRASE"
VALID_MATCH_MODES = frozenset({MATCH_MODE_EXACT_CHANNEL, MATCH_MODE_EVENT_PHRASE})


def _freeze_groups(groups: Sequence[Sequence[str]]) -> Tuple[Tuple[str, ...], ...]:
    return tuple(tuple(str(item) for item in group) for group in groups)


@dataclass(frozen=True)
class MatchDefinition:
    """Explicit search/qualifier definition supplied to shared matching."""

    mode: str
    primary_groups: Tuple[Tuple[str, ...], ...]
    required_qualifier_groups: Tuple[Tuple[str, ...], ...] = ()
    rejected_qualifier_groups: Tuple[Tuple[str, ...], ...] = ()
    preferred_qualifier_groups: Tuple[Tuple[str, ...], ...] = ()
    match_all: bool = False

    def __post_init__(self) -> None:
        if self.mode not in VALID_MATCH_MODES:
            raise ValueError(f"Unsupported match mode: {self.mode!r}")
        if not self.match_all and not self.primary_groups:
            raise ValueError("MatchDefinition requires primary groups unless match_all=True")

    @classmethod
    def from_groups(
        cls,
        *,
        mode: str,
        primary_groups: Sequence[Sequence[str]],
        required_qualifier_groups: Sequence[Sequence[str]] = (),
        rejected_qualifier_groups: Sequence[Sequence[str]] = (),
        preferred_qualifier_groups: Sequence[Sequence[str]] = (),
        match_all: bool = False,
    ) -> "MatchDefinition":
        return cls(
            mode=mode,
            primary_groups=_freeze_groups(primary_groups),
            required_qualifier_groups=_freeze_groups(required_qualifier_groups),
            rejected_qualifier_groups=_freeze_groups(rejected_qualifier_groups),
            preferred_qualifier_groups=_freeze_groups(preferred_qualifier_groups),
            match_all=bool(match_all),
        )


@dataclass(frozen=True)
class MatchEvaluation:
    """Result of evaluating one discovered entry against a MatchDefinition."""

    matches: bool
    preferred_qualifier_score: int = 0
    primary_match: bool = False
    required_qualifier_match: bool = True
    rejected_qualifier_match: bool = False


@dataclass(frozen=True)
class SourceCandidate:
    """Normalized source/candidate facts shared across source intelligence.

    Only the fields required by the current extraction are first-class today.
    ``extra`` preserves additional normalized facts while the larger mature
    resolver is migrated incrementally, without making untyped dictionaries the
    public shared-core contract.
    """

    playlist_url: str = ""
    matching_entry_index: Optional[int] = None
    extinf: str = ""
    tvg_name: str = ""
    group_title: str = ""
    entry_title: str = ""
    option_lines: Tuple[str, ...] = ()
    raw_stream_url: str = ""
    stream_url: str = ""
    headers: Mapping[str, str] = field(default_factory=dict)
    keys: Tuple[str, ...] = ()
    license_type: str = ""
    unsupported_drm: str = ""
    stream_type: str = ""
    preferred_qualifier_score: int = 0
    expiry: Optional[float] = None
    expiry_source: str = ""
    quality_known: bool = False
    quality_source: str = ""
    video_fps: float = 0.0
    video_fps_source: str = ""
    video_width: int = 0
    video_height: int = 0
    video_resolution_source: str = ""
    video_bitrate_bps: int = 0
    video_bitrate_source: str = ""
    video_scan_type: str = ""
    video_scan_type_source: str = ""
    final_stream_url: str = ""
    launchable: bool = False
    probe_status: str = "unprobed"
    probe_error: str = ""
    access_blocked: bool = False
    identity_key: str = ""
    playback_fingerprint: str = ""
    ignored: bool = False
    reason: str = ""
    extra: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "SourceCandidate":
        known_fields = {
            "playlist_url",
            "matching_entry_index",
            "extinf",
            "tvg_name",
            "group_title",
            "entry_title",
            "option_lines",
            "raw_stream_url",
            "stream_url",
            "headers",
            "keys",
            "license_type",
            "unsupported_drm",
            "stream_type",
            "preferred_qualifier_score",
            "expiry",
            "expiry_source",
            "quality_known",
            "quality_source",
            "video_fps",
            "video_fps_source",
            "video_width",
            "video_height",
            "video_resolution_source",
            "video_bitrate_bps",
            "video_bitrate_source",
            "video_scan_type",
            "video_scan_type_source",
            "final_stream_url",
            "launchable",
            "probe_status",
            "probe_error",
            "access_blocked",
            "identity_key",
            "playback_fingerprint",
            "ignored",
            "reason",
        }
        return cls(
            playlist_url=str(value.get("playlist_url") or ""),
            matching_entry_index=(
                int(value["matching_entry_index"])
                if value.get("matching_entry_index") is not None
                else None
            ),
            extinf=str(value.get("extinf") or ""),
            tvg_name=str(value.get("tvg_name") or ""),
            group_title=str(value.get("group_title") or ""),
            entry_title=str(value.get("entry_title") or ""),
            option_lines=tuple(str(item) for item in (value.get("option_lines") or ())),
            raw_stream_url=str(value.get("raw_stream_url") or ""),
            stream_url=str(value.get("stream_url") or ""),
            headers=dict(value.get("headers") or {}),
            keys=tuple(str(item) for item in (value.get("keys") or ())),
            license_type=str(value.get("license_type") or ""),
            unsupported_drm=str(value.get("unsupported_drm") or ""),
            stream_type=str(value.get("stream_type") or ""),
            preferred_qualifier_score=int(value.get("preferred_qualifier_score") or 0),
            expiry=(float(value["expiry"]) if value.get("expiry") is not None else None),
            expiry_source=str(value.get("expiry_source") or ""),
            quality_known=bool(value.get("quality_known")),
            quality_source=str(value.get("quality_source") or ""),
            video_fps=float(value.get("video_fps") or 0.0),
            video_fps_source=str(value.get("video_fps_source") or ""),
            video_width=int(value.get("video_width") or 0),
            video_height=int(value.get("video_height") or 0),
            video_resolution_source=str(value.get("video_resolution_source") or ""),
            video_bitrate_bps=int(value.get("video_bitrate_bps") or 0),
            video_bitrate_source=str(value.get("video_bitrate_source") or ""),
            video_scan_type=str(value.get("video_scan_type") or ""),
            video_scan_type_source=str(value.get("video_scan_type_source") or ""),
            final_stream_url=str(
                value.get("final_stream_url")
                or value.get("manifest_final_url")
                or ""
            ),
            launchable=bool(value.get("launchable")),
            probe_status=str(value.get("probe_status") or "unprobed"),
            probe_error=str(value.get("probe_error") or ""),
            access_blocked=bool(value.get("access_blocked")),
            identity_key=str(value.get("identity_key") or ""),
            playback_fingerprint=str(value.get("playback_fingerprint") or ""),
            ignored=bool(value.get("ignored")),
            reason=str(value.get("reason") or ""),
            extra={key: item for key, item in value.items() if key not in known_fields},
        )

    def to_mapping(self) -> Dict[str, Any]:
        result: Dict[str, Any] = dict(self.extra)
        result.update(
            {
                "playlist_url": self.playlist_url,
                "matching_entry_index": self.matching_entry_index,
                "extinf": self.extinf,
                "tvg_name": self.tvg_name,
                "group_title": self.group_title,
                "entry_title": self.entry_title,
                "option_lines": list(self.option_lines),
                "raw_stream_url": self.raw_stream_url,
                "stream_url": self.stream_url,
                "headers": dict(self.headers),
                "keys": list(self.keys),
                "license_type": self.license_type,
                "unsupported_drm": self.unsupported_drm,
                "stream_type": self.stream_type,
                "preferred_qualifier_score": self.preferred_qualifier_score,
                "expiry": self.expiry,
                "expiry_source": self.expiry_source,
                "quality_known": self.quality_known,
                "quality_source": self.quality_source,
                "video_fps": self.video_fps,
                "video_fps_source": self.video_fps_source,
                "video_width": self.video_width,
                "video_height": self.video_height,
                "video_resolution_source": self.video_resolution_source,
                "video_bitrate_bps": self.video_bitrate_bps,
                "video_bitrate_source": self.video_bitrate_source,
                "video_scan_type": self.video_scan_type,
                "video_scan_type_source": self.video_scan_type_source,
                "final_stream_url": self.final_stream_url,
                "launchable": self.launchable,
                "probe_status": self.probe_status,
                "probe_error": self.probe_error,
                "access_blocked": self.access_blocked,
                "identity_key": self.identity_key,
                "playback_fingerprint": self.playback_fingerprint,
                "ignored": self.ignored,
                "reason": self.reason,
            }
        )
        return result


@dataclass(frozen=True)
class SelectionPolicy:
    """Explicit policy inputs used by shared candidate selection."""

    mandatory_min_remaining_sec: int
    upgrade_min_remaining_sec: int
    allow_unknown_expiry: bool = False
    prefer_unknown_expiry_on_equal_quality: bool = False
    motion_cap_fps: float = 50.0


@dataclass(frozen=True)
class SelectionDecision:
    """Structured outcome from shared selection logic."""

    selected: Optional[SourceCandidate]
    selected_index: Optional[int]
    decision_type: str
    reason: str
    fallback_used: bool
    considered_count: int
    eligible_count: int




@dataclass(frozen=True)
class PlaylistSourceSpec:
    """One configured playlist acquisition source."""

    url: str
    name: str = ""
    group: str = ""
    provider: str = "UNKNOWN"
    request_headers: Mapping[str, str] = field(default_factory=dict)
    stream_headers: Mapping[str, str] = field(default_factory=dict)

@dataclass(frozen=True)
class SourceAcquisitionRequest:
    """Minimum shared request contract for source discovery."""

    match: MatchDefinition
    source_names: Tuple[str, ...] = ()
    target_name: str = ""


@dataclass(frozen=True)
class SourceAcquisitionResult:
    """Normalized result returned by source-discovery boundaries."""

    candidates: Tuple[SourceCandidate, ...]
    source_errors: Tuple[str, ...] = ()
    diagnostics: Mapping[str, Any] = field(default_factory=dict)
