"""Explicit process outcome returned by the mature recorder boundary."""

from __future__ import annotations

from dataclasses import dataclass


OUTCOME_MANUAL_STOP = "MANUAL_STOP"
OUTCOME_NORMAL_END = "NORMAL_END"
OUTCOME_FAILURE_STOP = "FAILURE_STOP"


@dataclass(frozen=True)
class RecorderProcessOutcome:
    kind: str
    reason: str

    def __post_init__(self) -> None:
        if self.kind not in {
            OUTCOME_MANUAL_STOP,
            OUTCOME_NORMAL_END,
            OUTCOME_FAILURE_STOP,
        }:
            raise ValueError(f"unknown recorder process outcome {self.kind!r}")
