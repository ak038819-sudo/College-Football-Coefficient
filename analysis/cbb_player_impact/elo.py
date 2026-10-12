"""Experimental result-only basketball Elo; no production imports or writes."""

from dataclasses import dataclass
from datetime import datetime
import math


@dataclass(frozen=True)
class Parameters:
    # Starting hypotheses, NOT fitted basketball parameters.
    initial: float = 1500.0
    scale: float = 400.0
    k: float = 20.0
    home: float = 0.0
    retention: float = 0.5

    def __post_init__(self):
        if not all(math.isfinite(v) for v in vars(self).values()):
            raise ValueError("Parameters must be finite")
        if self.scale <= 0 or self.k <= 0 or not 0 <= self.retention <= 1:
            raise ValueError("Require scale/k > 0 and retention in [0, 1]")


def replay(games, parameters=None):
    """Replay final CBBD games; return pregame predictions and final ratings.

    Caller must supply a complete relevant schedule for meaningful rankings.
    Team-only samples are plumbing tests, not national strength estimates.
    Uses provider IDs and ending-year seasons. Provider Elo is never read.
    """
    p = parameters or Parameters()
    finals, seen = [], set()
    for g in games:
        if g['id'] in seen:
            raise ValueError(f"Duplicate game {g['id']}")
        seen.add(g['id'])
        if g['status'] != 'final':
            continue
        scores = (g['homePoints'], g['awayPoints'])
        if any(type(s) is not int or s < 0 for s in scores) or scores[0] == scores[1]:
            raise ValueError(f"Invalid final score for {g['id']}")
        if g['homeTeamId'] == g['awayTeamId']:
            raise ValueError("A team cannot play itself")
        if type(g['neutralSite']) is not bool:
            raise ValueError("neutralSite must be explicit boolean")
        if type(g['season']) is not int:
            raise ValueError("season must be an ending-year integer")
        date = datetime.fromisoformat(g['startDate'].replace('Z', '+00:00'))
        if date.tzinfo is None:
            raise ValueError("Game timestamps must include timezone")
        finals.append((date, g))
    finals.sort(key=lambda item: (item[0], item[1]['id']))
    ratings, history, last_season = {}, [], None
    for _, g in finals:
        season = g['season']
        if last_season is not None and season < last_season:
            raise ValueError("Season order conflicts with chronology")
        if last_season is not None and season > last_season:
            for team in ratings:
                ratings[team] = p.initial + (ratings[team] - p.initial) * p.retention ** (season-last_season)
        last_season = season
        home, away = g['homeTeamId'], g['awayTeamId']
        rh, ra = ratings.get(home, p.initial), ratings.get(away, p.initial)
        gap = rh - ra + (0 if g['neutralSite'] else p.home)
        z = math.log(10) * gap / p.scale
        probability = 1 / (1 + math.exp(-z)) if z >= 0 else math.exp(z) / (1 + math.exp(z))
        outcome = int(g['homePoints'] > g['awayPoints'])
        delta = p.k * (outcome - probability)
        history.append(dict(game_id=g['id'], season=season, home_pre=rh,
                            away_pre=ra, home_probability=probability,
                            home_result=outcome, home_delta=delta))
        ratings[home], ratings[away] = rh + delta, ra - delta
    return history, ratings
