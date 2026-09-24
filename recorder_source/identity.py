"""Provider-scoped feed identity and candidate grouping.

Feed identity answers "which underlying provider lane is this?" and deliberately
stays separate from event metadata, quality variants, and playback/session
fingerprints.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Mapping, Sequence, Tuple
from urllib.parse import urlsplit, urlunsplit

from .models import SourceCandidate


SONY_PROVIDER_NAMES = frozenset({"SONYLIV", "SONY", "SONY_TV"})


@dataclass(frozen=True)
class CanonicalFeedIdentity:
    """Stable provider-scoped identity used for grouping and later ownership."""

    provider: str
    lane_key: str
    confidence: str = field(default="conservative", compare=False)
    evidence: str = field(default="", compare=False)

    @property
    def serialized(self) -> str:
        return f"{self.provider}|{self.lane_key}"


def normalize_provider_name(value: str) -> str:
    provider = str(value or "UNKNOWN").strip().upper() or "UNKNOWN"
    if provider in SONY_PROVIDER_NAMES:
        return "SONYLIV"
    return provider


def strip_transport_suffix(value: str) -> str:
    """Remove recorder-style ``URL|header`` transport suffixes only."""
    text = str(value or "").strip()
    if "|" in text:
        text = text.split("|", 1)[0].strip()
    return text


def _candidate_urls(candidate: SourceCandidate) -> Tuple[str, ...]:
    values: List[str] = []
    extra = candidate.extra if isinstance(candidate.extra, Mapping) else {}
    for value in (
        candidate.final_stream_url,
        extra.get("manifest_final_url"),
        extra.get("effective_url"),
        candidate.stream_url,
        candidate.raw_stream_url,
    ):
        text = strip_transport_suffix(str(value or ""))
        if text and text not in values:
            values.append(text)
    return tuple(values)


def _sony_lane_from_url(value: str) -> str:
    """Return Sony's stable provider lane, independent of CDN/manifest variant.

    Captured Sony delivery has two stable lane shapes relevant to the current
    project:

    * ``/(hls|dash)/live/<feed-id>/<provider-path>/<language>/...``
    * ``/.../clients/dash/enc/<lane>/out/v1/<output-id>/...``

    In both cases the CDN hostname and manifest leaf are delivery details, not
    feed identity.  These are Sony-specific rules; unknown providers remain
    conservative rather than using generic URL stripping.
    """
    try:
        path = re.sub(r"/{2,}", "/", urlsplit(value).path or "/")
    except Exception:
        return ""

    live_match = re.search(
        r"/(?:hls|dash)/live/"
        r"(?P<feed_id>[^/]+)/(?P<lane>[^/]+)/(?P<language>[^/]+)(?:/|$)",
        path,
        flags=re.IGNORECASE,
    )
    if live_match:
        feed_id = live_match.group("feed_id")
        lane = live_match.group("lane")
        language = live_match.group("language")
        return f"lane:{feed_id}/{lane}/{language}"

    dash_match = re.search(
        r"/clients/dash/enc/"
        r"(?P<lane>[^/]+)/out/v1/(?P<output_id>[^/]+)(?:/|$)",
        path,
        flags=re.IGNORECASE,
    )
    if dash_match:
        lane = dash_match.group("lane")
        output_id = dash_match.group("output_id")
        return f"dash-lane:{lane}/{output_id}"

    return ""


def _conservative_url_key(value: str) -> str:
    """Keep unknown-provider identity intentionally strict.

    No generic token/query stripping is performed. A refreshed URL may therefore
    remain separate until a provider-specific rule proves sameness, which is the
    safer failure mode for identity-based recording.
    """
    text = strip_transport_suffix(value)
    try:
        parsed = urlsplit(text)
    except Exception:
        return f"literal:{text}"

    if not parsed.scheme or not parsed.netloc:
        return f"literal:{text}"

    normalized = urlunsplit(
        (
            parsed.scheme.casefold(),
            parsed.netloc.casefold(),
            re.sub(r"/{2,}", "/", parsed.path or "/"),
            parsed.query,
            "",
        )
    )
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return f"opaque-url:{digest}"


def derive_feed_identity(
    candidate: SourceCandidate,
    provider: str,
) -> CanonicalFeedIdentity:
    """Derive one provider-scoped canonical identity conservatively."""
    provider_name = normalize_provider_name(provider)
    urls = _candidate_urls(candidate)

    if provider_name in SONY_PROVIDER_NAMES:
        for value in urls:
            lane_key = _sony_lane_from_url(value)
            if lane_key:
                return CanonicalFeedIdentity(
                    provider="SONYLIV",
                    lane_key=lane_key,
                    confidence="provider",
                    evidence="sony stable live lane",
                )

    # A provider/acquisition adapter may supply stable native lane evidence when
    # its delivery URL does not itself expose the provider lane.  The hint is
    # trusted only because it comes from the provider edge, not from display
    # metadata or generic URL stripping.
    extra = candidate.extra if isinstance(candidate.extra, Mapping) else {}
    provider_hint = str(extra.get("provider_identity_hint") or "").strip()
    if provider_hint:
        digest = hashlib.sha256(provider_hint.encode("utf-8")).hexdigest()
        return CanonicalFeedIdentity(
            provider=provider_name,
            lane_key=f"provider-hint:{digest}",
            confidence="provider",
            evidence="provider identity hint",
        )

    if urls:
        return CanonicalFeedIdentity(
            provider=provider_name,
            lane_key=_conservative_url_key(urls[0]),
            confidence="conservative",
            evidence="full provider-scoped URL retained",
        )

    # No URL evidence: keep observations conservative without making mutable
    # display metadata part of canonical identity. Source provenance plus
    # observation position is the narrowest available fallback evidence.
    digest_source = "|".join(
        (candidate.playlist_url, str(candidate.matching_entry_index or ""))
    )
    confidence = "unresolved"
    evidence = "source provenance only; no stable provider lane evidence"
    digest = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()
    return CanonicalFeedIdentity(
        provider=provider_name,
        lane_key=f"unresolved:{digest}",
        confidence=confidence,
        evidence=evidence,
    )


def group_candidates_by_identity(
    candidates: Sequence[SourceCandidate],
    *,
    provider: str,
) -> Dict[CanonicalFeedIdentity, Tuple[SourceCandidate, ...]]:
    """Group candidates before identity-based job counting or presentation."""
    grouped: Dict[CanonicalFeedIdentity, List[SourceCandidate]] = {}
    for candidate in candidates:
        identity = derive_feed_identity(candidate, provider)
        grouped.setdefault(identity, []).append(candidate)
    return {identity: tuple(items) for identity, items in grouped.items()}
