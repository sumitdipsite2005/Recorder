"""Shared sound-snooze state and timing helpers.

Caller-specific meanings such as recorder RUN/full recording or Coordinator
full-run remain in the caller. This module owns only the reusable state/timing
mechanics.
"""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Iterable, Optional


@dataclass
class SoundSnoozeState:
    sound_snooze_mode: Optional[str] = None
    sound_snooze_until_ts: Optional[float] = None


def clear_sound_snooze(state) -> None:
    state.sound_snooze_mode = None
    state.sound_snooze_until_ts = None


def set_timed_sound_snooze(
    state,
    *,
    duration_sec: float = 15 * 60.0,
    now_ts: Optional[float] = None,
) -> None:
    now = time.time() if now_ts is None else float(now_ts)
    state.sound_snooze_mode = "timed"
    state.sound_snooze_until_ts = now + max(0.0, float(duration_sec))


def set_indefinite_sound_snooze(state, mode: str) -> None:
    value = str(mode or "").strip()
    if not value or value == "timed":
        raise ValueError("indefinite sound-snooze mode must be a non-timed name")
    state.sound_snooze_mode = value
    state.sound_snooze_until_ts = None


def timed_sound_snoozed(
    state,
    *,
    now_ts: Optional[float] = None,
) -> Optional[bool]:
    """Return True/False for timed mode, or None when another mode is active."""
    if getattr(state, "sound_snooze_mode", None) != "timed":
        return None

    now = time.time() if now_ts is None else float(now_ts)
    until_ts = getattr(state, "sound_snooze_until_ts", None)
    if until_ts is not None and now < float(until_ts):
        return True

    clear_sound_snooze(state)
    return False


def is_sound_snoozed(
    state,
    *,
    now_ts: Optional[float] = None,
    indefinite_modes: Iterable[str] = (),
) -> bool:
    timed = timed_sound_snoozed(state, now_ts=now_ts)
    if timed is not None:
        return bool(timed)

    mode = getattr(state, "sound_snooze_mode", None)
    return mode in {str(value) for value in indefinite_modes}


def sound_snooze_remaining_seconds(
    state,
    *,
    now_ts: Optional[float] = None,
) -> float:
    if getattr(state, "sound_snooze_mode", None) != "timed":
        return 0.0
    now = time.time() if now_ts is None else float(now_ts)
    until_ts = float(getattr(state, "sound_snooze_until_ts", 0.0) or 0.0)
    return max(0.0, until_ts - now)
