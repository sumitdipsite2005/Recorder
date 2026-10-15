"""Shared sound-snooze state and timing helpers.

Caller-specific meanings such as recorder RUN/full recording or Coordinator
full-run remain in the caller. This module owns only the reusable state/timing
mechanics.
"""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Callable, Iterable, Optional


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


def acknowledge_vpn_drop_alarm(
    state,
    *,
    log_fn: Optional[Callable] = None,
    alarm_stop_fn: Optional[Callable] = None,
) -> bool:
    """Acknowledge the VPN drop alarm immediately."""
    alarm_type = getattr(state, "shared_alarm_type", None)
    if not (
        getattr(state, "lenient_alarm_active", False)
        or getattr(state, "network_guard_hold_active", False)
        or alarm_type == "vpn_drop"
    ):
        return False

    alarm_activity_id = getattr(state, "shared_alarm_activity_id", None)
    if log_fn:
        log_fn("ALARM_ACK", activity_id=alarm_activity_id)
        log_fn("ALARM_VPN_DROP_STOP reason=ack", activity_id=alarm_activity_id)
    state.vpn_drop_post_ack_silent = True
    state.alarm_ack_requested = False
    state.alarm_linger_until = None
    if alarm_stop_fn:
        alarm_stop_fn(state)
    return True


def acknowledge_internet_drop_alarm(
    state,
    *,
    log_fn: Optional[Callable] = None,
    alarm_stop_fn: Optional[Callable] = None,
) -> bool:
    """Acknowledge the internet drop alarm immediately."""
    alarm_type = getattr(state, "shared_alarm_type", None)
    if not (
        getattr(state, "internet_drop_active", False)
        or getattr(state, "internet_hold_active", False)
        or alarm_type == "internet_drop"
    ):
        return False

    alarm_activity_id = getattr(state, "shared_alarm_activity_id", None)
    if log_fn:
        log_fn("ALARM_ACK", activity_id=alarm_activity_id)
        log_fn("ALARM_INTERNET_DROP_STOP reason=ack", activity_id=alarm_activity_id)
    state.internet_drop_post_ack_silent = True
    state.alarm_ack_requested = False
    state.alarm_linger_until = None
    if alarm_stop_fn:
        alarm_stop_fn(state)
    return True

