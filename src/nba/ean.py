"""Research-only Extraordinary Achievement Norm v0.3 evaluator.

No historical rarity or impact estimates are inferred here; inputs must be
validated by independently versioned models. Never certifies awards.
"""
from dataclasses import dataclass
from enum import Enum
from math import isfinite
from typing import Optional


class Decision(str, Enum):
    QUALIFIED = "QUALIFIED"
    FAILED = "FAILED"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


@dataclass(frozen=True)
class EANConfig:
    eas_min: float = 95.0
    rarity_min: float = 95.0
    impact_min: float = 98.0
    sustained_min: float = 80.0
    participation_min: float = 0.70


@dataclass(frozen=True)
class Candidate:
    player_id: str
    season: int
    scope: str
    achievement_profile: str
    rarity: Optional[float]
    impact: Optional[float]
    sustained: Optional[float]
    participation: Optional[float]
    data_validated: bool
    coverage_tier: str = "UNKNOWN"


@dataclass(frozen=True)
class Result:
    decision: Decision
    eas: Optional[float]
    reason: str


def evaluate(candidate: Candidate, config: EANConfig = EANConfig()) -> Result:
    if candidate.scope != "regular":
        return Result(Decision.INSUFFICIENT_DATA, None, "postseason model not validated")
    values = (candidate.rarity, candidate.impact, candidate.sustained, candidate.participation)
    if not candidate.data_validated or any(v is None for v in values):
        return Result(Decision.INSUFFICIENT_DATA, None, "unvalidated or missing data")
    r, i, s, a = values
    if not all(isfinite(v) for v in values):
        return Result(Decision.INSUFFICIENT_DATA, None, "nonfinite input")
    if not all(0 <= v <= 100 for v in (r, i, s)) or not 0 <= a <= 1:
        raise ValueError("scores must be 0..100 and participation 0..1")
    eas = 0.45 * r + 0.40 * i + 0.15 * s
    passed = (
        eas >= config.eas_min
        and r >= config.rarity_min
        and i >= config.impact_min
        and s >= config.sustained_min
        and a >= config.participation_min
    )
    return Result(Decision.QUALIFIED if passed else Decision.FAILED, round(eas, 6),
                  "all gates met" if passed else "one or more gates failed")


def career_recipients(rows):
    """Return highest qualifying season per player, with deterministic ties."""
    winners = {}
    for candidate, result in rows:
        if result.decision != Decision.QUALIFIED:
            continue
        prior = winners.get(candidate.player_id)
        rank = (result.eas, -candidate.season, candidate.achievement_profile)
        if prior is None or rank > prior[0]:
            winners[candidate.player_id] = (rank, candidate, result)
    return {pid: (item[1], item[2]) for pid, item in sorted(winners.items())}
