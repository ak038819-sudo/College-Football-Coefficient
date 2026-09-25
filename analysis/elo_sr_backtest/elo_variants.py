#!/usr/bin/env python3
"""
EXPERIMENT harness for the Success Rate vs MOV Elo backtest. Production code is
imported, never modified.

replay() is a line-for-line mirror of build_elo.run_elo with ONE change: the
margin multiplier M is a pluggable function. Everything else is identical --
initial 1500, offseason regression at each new season's first game, flat home
field on non-neutral games, ties (S = 0.5, M = 1), zero-sum updates, and the
chronological game order from build_elo.fetch_games_chronological. The
prediction for each game is recorded BEFORE that game updates any rating.

Model A (production MOV) must reproduce elo_game_history exactly; see
verify_model_a(). Any model differs from production only from `era_start` on:
before that every model uses production MOV, so all start the comparison
period from identical ratings.

Direction rule: the Elo change is K * (S - E) * M. The multiplier may change the
SIZE of an update but never its sign, so every multiplier is clamped at >= 0.
"""
from __future__ import annotations

import math
import sqlite3
import sys
from pathlib import Path
from typing import Callable, Dict, List, Optional

REPO = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO / "src"))
from build_elo import (effective_rating, expected_result, fetch_games_chronological,  # noqa: E402
                       load_config, mov_multiplier)

# multiplier(game, point_diff, winner_advantage) -> M   (ties never reach it: M = 1 like production)
Multiplier = Callable[[dict, int, float], float]
LOG_LOSS_EPS = 1e-12


def load_games(conn: sqlite3.Connection) -> List[dict]:
    prev = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        return [dict(g) for g in fetch_games_chronological(conn)]
    finally:
        conn.row_factory = prev


def production_multiplier(cfg: dict) -> Multiplier:
    c, d = cfg["mov_c"], cfg["mov_d"]
    return lambda g, point_diff, winner_adv: mov_multiplier(point_diff, winner_adv, c, d)


def pure_multiplier(g, point_diff, winner_adv) -> float:
    return 1.0


def replay(games: List[dict], cfg: dict, multiplier: Multiplier, era_start: Optional[int] = None,
           keep_rows: bool = False, state: Optional[tuple] = None, return_state: bool = False):
    """
    Returns (predictions, rows). predictions: one dict per game, recorded before
    the update: game_id, season, p_home (with home field, exactly as production
    predicts), s_home, and the pregame ratings. rows (if keep_rows) match
    elo_game_history's layout for verification.

    state / return_state: resume from (ratings, current_season) saved by an
    earlier call, so every model can share ONE replay of the identical
    pre-comparison years instead of recomputing them per configuration.
    """
    init, scale, k = cfg["initial_rating"], cfg["scale"], cfg["k"]
    hfa, keep = cfg["home_field"], cfg["offseason_retention"]
    prod = production_multiplier(cfg)
    ratings: Dict[int, float] = dict(state[0]) if state else {}
    current = state[1] if state else None
    preds, rows = [], []
    for g in games:
        season = g["season_year"]
        if current is None:
            current = season
        elif season != current:
            for t in ratings:
                ratings[t] = init + keep * (ratings[t] - init)
            current = season
        h, a = g["home_team_id"], g["away_team_id"]
        ratings.setdefault(h, init)
        ratings.setdefault(a, init)
        r_home, r_away = ratings[h], ratings[a]
        neutral = bool(g["neutral_site"])
        eff_home = effective_rating(r_home, True, neutral, hfa)
        eff_away = effective_rating(r_away, False, neutral, hfa)
        e_home = expected_result(eff_home, eff_away, scale)
        hs, aws = g["home_score"], g["away_score"]
        s_home = 1.0 if hs > aws else 0.0 if aws > hs else 0.5
        preds.append({"game_id": g["game_id"], "season": season, "p_home": e_home, "s_home": s_home,
                      "home_pre": r_home, "away_pre": r_away, "neutral": neutral})
        point_diff = abs(hs - aws)
        if hs == aws:
            m = 1.0
        else:
            winner_adv = eff_home - eff_away if hs > aws else eff_away - eff_home
            fn = multiplier if era_start is None or season >= era_start else prod
            m = max(0.0, fn(g, point_diff, winner_adv))          # never reverse the direction
        delta = k * (s_home - e_home) * m
        if keep_rows:
            rows.append((g["game_id"], h, r_home, r_away, e_home, m, delta, r_home + delta))
            rows.append((g["game_id"], a, r_away, r_home, 1.0 - e_home, m, -delta, r_away - delta))
        ratings[h] = r_home + delta
        ratings[a] = r_away - delta
    if return_state:
        return preds, rows, (ratings, current)
    return preds, rows


def verify_model_a(conn: sqlite3.Connection, cfg: dict) -> int:
    """Raises unless the harness's production replay equals elo_game_history exactly. Returns rows compared."""
    _, rows = replay(load_games(conn), cfg, production_multiplier(cfg), keep_rows=True)
    stored = conn.execute("""SELECT game_id, team_id, pregame_elo, opponent_pregame_elo, elo_expectation,
                                    mov_multiplier, elo_change, postgame_elo FROM elo_game_history""").fetchall()
    key = lambda r: (r[0], r[1])
    ours, theirs = sorted(rows, key=key), sorted(stored, key=key)
    if len(ours) != len(theirs):
        raise AssertionError(f"row count differs: harness {len(ours)} vs production {len(theirs)}")
    for x, y in zip(ours, theirs):
        if x[:2] != y[:2] or any(abs(p - q) > 1e-9 for p, q in zip(x[2:], y[2:])):
            raise AssertionError(f"harness differs from production at game {x[0]} team {x[1]}: {x} vs {y}")
    return len(ours)


# ---------------------------------------------------------------- scoring
def game_scores(p: float, y: float) -> tuple:
    """(brier, log loss, correct) for one game; probabilities clamped for log loss; ties score half-right."""
    pc = min(max(p, LOG_LOSS_EPS), 1.0 - LOG_LOSS_EPS)
    brier = (p - y) ** 2
    ll = -(y * math.log(pc) + (1.0 - y) * math.log(1.0 - pc))
    correct = 0.5 if y == 0.5 or p == 0.5 else float((p > 0.5) == (y == 1.0))
    return brier, ll, correct


def season_sums(preds: List[dict], seasons: Optional[set] = None, keep: Optional[Callable] = None) -> Dict[int, list]:
    """{season: [brier_sum, logloss_sum, correct_sum, n]} over games passing the filters."""
    out: Dict[int, list] = {}
    for p in preds:
        if seasons is not None and p["season"] not in seasons:
            continue
        if keep is not None and not keep(p):
            continue
        b, l, c = game_scores(p["p_home"], p["s_home"])
        acc = out.setdefault(p["season"], [0.0, 0.0, 0.0, 0])
        acc[0] += b
        acc[1] += l
        acc[2] += c
        acc[3] += 1
    return out


def summarize(sums: Dict[int, list], seasons=None) -> dict:
    tot = [0.0, 0.0, 0.0, 0]
    for s, v in sums.items():
        if seasons is None or s in seasons:
            for i in range(4):
                tot[i] += v[i]
    n = tot[3]
    return {"n": n, "brier": tot[0] / n if n else None, "log_loss": tot[1] / n if n else None,
            "accuracy": tot[2] / n if n else None}


def walk_forward(config_sums: Dict[str, Dict[int, list]], first_test: int, last_test: int, train_start: int,
                 criterion: str = "log_loss") -> dict:
    """
    For each test season T: choose the config with the best `criterion` on seasons
    train_start..T-1 ONLY, then score that config on season T. Never uses T or
    later to choose. Returns per-season choices and the pooled out-of-sample metrics.
    """
    per_season, pooled = [], [0.0, 0.0, 0.0, 0]
    for t in range(first_test, last_test + 1):
        train = set(range(train_start, t))
        best = min(config_sums, key=lambda c: (summarize(config_sums[c], train)[criterion] or float("inf"), c))
        test = config_sums[best].get(t)
        if not test:
            continue
        for i in range(4):
            pooled[i] += test[i]
        per_season.append({"season": t, "chosen": best, "n": test[3], "brier": test[0] / test[3],
                           "log_loss": test[1] / test[3], "accuracy": test[2] / test[3]})
    n = pooled[3]
    return {"per_season": per_season,
            "pooled": {"n": n, "brier": pooled[0] / n if n else None, "log_loss": pooled[1] / n if n else None,
                       "accuracy": pooled[2] / n if n else None}}


def elo_config() -> dict:
    return load_config(str(REPO / "config" / "model_config.json"))["elo"]
