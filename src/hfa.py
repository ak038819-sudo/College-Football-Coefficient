#!/usr/bin/env python3
"""
Team-specific home-field advantage (HFA), estimated empirically.

ANALYSIS ONLY. build_elo.py keeps using the flat config "elo.home_field";
nothing in this module changes production ratings or predictions.

------------------------------------------------------------------------
The question each estimate answers
------------------------------------------------------------------------
"Given how strong the two teams were entering the game, how often did this
team win at home, compared with how often it would have been expected to
win on a NEUTRAL field?"

For every qualifying home game g of team i (not neutral-site, completed,
both teams have a pregame Elo):

    P_g = neutral_win_probability(Elo_home_pre, Elo_away_pre)
        = expected_result(Elo_home_pre, Elo_away_pre, scale)   # engine's own
                                                               # function, NO HFA
    A_g = 1 win, 0 loss, 0.5 tie   (the Elo engine's convention)
    w_g = 2 ^ (-age_years_g / L)   (L = half_life_years)

    H_raw_i   = sum(w A) / sum(w P)
    H_FBS     = the same ratio over ALL qualifying home games (national baseline)
    N_eff_i   = sum(w)             (or Kish; see EFFECTIVE_N_METHODS)
    lambda_i  = N_eff_i / (N_eff_i + K)
    H_prior_i = alpha * H_FCS_i + (1 - alpha) * H_FBS   if FCS history exists
              = H_FBS                                    otherwise (today: always)
    H_adj_i   = lambda_i * H_raw_i + (1 - lambda_i) * H_prior_i

A team with no qualifying games has N_eff = 0 -> lambda = 0 -> pure prior.
There are no thresholds or program categories; the move from prior-driven
to evidence-driven is continuous in N_eff.

------------------------------------------------------------------------
Circularity -- why this is not estimating HFA from itself
------------------------------------------------------------------------
Two pipelines, kept apart:

  ESTIMATION (this module):   P_g uses pregame ratings with NO home bonus.
  PREDICTION (build_elo.py):  expected_result(R_home + HFA, R_away).

The pregame ratings come from elo_game_history, produced by build_elo.py with
the FLAT config home_field -- no team-specific HFA ever feeds them, so no
team's estimate depends on itself. If production Elo ever adopts these
team-specific values, estimation must keep reading ratings from a separate
flat-HFA reference run; otherwise each estimate would feed the ratings that
produce the next estimate.

------------------------------------------------------------------------
Point in time -- no look-ahead
------------------------------------------------------------------------
Every calculation takes an `as_of` date and uses only games dated strictly
before it. Game age is measured in days / 365.25 from `as_of`. A negative
age is a programming error and raises, so a future game can never leak in.

------------------------------------------------------------------------
Converting H into Elo points
------------------------------------------------------------------------
There is no exact transformation from the ratio H alone: the logistic curve
is nonlinear, so the Elo bonus needed to lift expected wins by a given
proportion depends on how lopsided the team's games were (a +10% lift is a
few points for a team playing coin flips and far more for a team that was
already a 90% favourite). So the conversion uses the games themselves:

    find D such that  sum(w * expected_result(Elo_home + D, Elo_away)) = target

    target = H_adj * sum(w P)    (the shrunk estimate)
    target = sum(w A)            (the raw estimate: D_raw)

sum(w * E(d + D)) is strictly increasing in D, so a safeguarded Newton search
(bisection whenever a Newton step would leave the bracket) always finds
the unique D inside `point_bounds`; a result at a bound is flagged (e.g. a
team that won every home game has no finite raw solution). A team with no
games gets the national D solved over all FBS home games for its prior.
The national D is a useful cross-check against the separately calibrated
flat "elo.home_field".
"""
from __future__ import annotations

import datetime as dt
import math
import sqlite3
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from build_elo import expected_result

DAYS_PER_YEAR = 365.25


# ---------------------------------------------------------------- inputs
@dataclass(frozen=True)
class HomeGame:
    game_id: int
    season: int
    date: dt.date
    team_id: int          # the home team
    opponent_id: int
    home_pre_elo: float
    away_pre_elo: float
    p_neutral: float      # neutral-field expectation, NO home bonus
    actual: float         # 1 / 0 / 0.5


def neutral_win_probability(home_pre_elo: float, away_pre_elo: float, scale: float) -> float:
    """The Elo engine's own expected_result(), deliberately WITHOUT any home bonus."""
    return expected_result(home_pre_elo, away_pre_elo, scale)


def load_home_games(conn: sqlite3.Connection, scale: float) -> Tuple[List[HomeGame], Dict[str, int]]:
    """
    Every completed, non-neutral game with a pregame Elo for BOTH teams, from
    the home team's perspective, oldest first. Games are excluded (and counted)
    rather than guessed at: neutral sites; missing scores (unplayed/canceled);
    a missing Elo row for either side. Postseason games are NOT excluded for
    being postseason -- only the neutral flag decides, so an on-campus CFP
    game counts and a neutral-site bowl does not. The games table only holds
    FBS-vs-FBS games with canonical team ids (aliases resolved at load), so
    FCS opponents and renamed programs need no handling here.
    """
    rows = conn.execute(
        """
        SELECT g.game_id, g.season_year, g.game_date, g.home_team_id, g.away_team_id,
               g.home_score, g.away_score, g.neutral_site,
               eh.pregame_elo AS home_pre, ea.pregame_elo AS away_pre
        FROM games g
        LEFT JOIN elo_game_history eh ON eh.game_id = g.game_id AND eh.team_id = g.home_team_id
        LEFT JOIN elo_game_history ea ON ea.game_id = g.game_id AND ea.team_id = g.away_team_id
        ORDER BY g.game_date, g.game_id
        """
    ).fetchall()
    games, skipped = [], {"neutral_site": 0, "not_completed": 0, "missing_elo": 0}
    for gid, season, date, home, away, hs, as_, neutral, home_pre, away_pre in rows:
        if neutral:
            skipped["neutral_site"] += 1
            continue
        if hs is None or as_ is None:
            skipped["not_completed"] += 1
            continue
        if home_pre is None or away_pre is None:
            skipped["missing_elo"] += 1
            continue
        actual = 1.0 if hs > as_ else 0.0 if hs < as_ else 0.5
        games.append(HomeGame(gid, season, dt.date.fromisoformat(str(date)[:10]), home, away,
                              home_pre, away_pre, neutral_win_probability(home_pre, away_pre, scale), actual))
    return games, skipped


# ---------------------------------------------------------------- weights
def game_age_years(game_date: dt.date, as_of: dt.date) -> float:
    return (as_of - game_date).days / DAYS_PER_YEAR


def recency_weight(age_years: float, half_life_years: float) -> float:
    """w = 2^(-age / L). A negative age means a future game: refuse it (look-ahead guard)."""
    if age_years < 0:
        raise ValueError(f"negative game age {age_years}: a future game reached an HFA calculation")
    if half_life_years <= 0:
        raise ValueError("half_life_years must be positive")
    return 2.0 ** (-age_years / half_life_years)


@dataclass
class WeightedSums:
    sum_w: float = 0.0
    sum_w2: float = 0.0
    sum_wa: float = 0.0     # weighted actual wins
    sum_wp: float = 0.0     # weighted neutral-expected wins
    games: int = 0
    oldest: Optional[dt.date] = None
    newest: Optional[dt.date] = None


def weighted_sums(games: Iterable[HomeGame], as_of: dt.date, half_life_years: float) -> WeightedSums:
    """Accumulate over games dated STRICTLY before as_of; later games are ignored, never weighted."""
    s = WeightedSums()
    for g in games:
        if g.date >= as_of:
            continue
        w = recency_weight(game_age_years(g.date, as_of), half_life_years)
        s.sum_w += w
        s.sum_w2 += w * w
        s.sum_wa += w * g.actual
        s.sum_wp += w * g.p_neutral
        s.games += 1
        s.oldest = g.date if s.oldest is None or g.date < s.oldest else s.oldest
        s.newest = g.date if s.newest is None or g.date > s.newest else s.newest
    return s


# ---------------------------------------------------------------- ratios
def calculate_raw_hfa(sums: WeightedSums) -> Optional[float]:
    """sum(wA) / sum(wP); None (never a divide-by-zero or a silent 0) when there's no expectation mass."""
    return sums.sum_wa / sums.sum_wp if sums.sum_wp > 0 else None


def _n_eff_sum_weights(s: WeightedSums) -> float:
    return s.sum_w


def _n_eff_kish(s: WeightedSums) -> float:
    return (s.sum_w ** 2) / s.sum_w2 if s.sum_w2 > 0 else 0.0


EFFECTIVE_N_METHODS: Dict[str, Callable[[WeightedSums], float]] = {
    "sum_weights": _n_eff_sum_weights,
    "kish": _n_eff_kish,
}


def calculate_effective_sample_size(sums: WeightedSums, method: str = "sum_weights") -> float:
    try:
        return EFFECTIVE_N_METHODS[method](sums)
    except KeyError:
        raise ValueError(f"unknown effective_n_method {method!r}; choose from {sorted(EFFECTIVE_N_METHODS)}") from None


def calculate_fbs_baseline(all_games: Sequence[HomeGame], as_of: dt.date, half_life_years: float) -> Optional[float]:
    """National H over every qualifying home game before as_of, weighted the same way."""
    return calculate_raw_hfa(weighted_sums(all_games, as_of, half_life_years))


def calculate_hfa_prior(fbs_baseline: float, fcs_hfa: Optional[float] = None, fcs_weight: float = 0.5) -> float:
    """
    alpha * H_FCS + (1 - alpha) * H_FBS when FCS history exists; otherwise H_FBS.
    Hook for transitioning programs (North Dakota State, Sacramento State, ...):
    the database holds FBS-vs-FBS games only, so fcs_hfa is None for everyone
    today and this safely returns the FBS baseline. No FCS value is ever invented.
    """
    if fcs_hfa is None:
        return fbs_baseline
    return fcs_weight * fcs_hfa + (1.0 - fcs_weight) * fbs_baseline


def apply_hfa_shrinkage(raw: Optional[float], prior: float, n_eff: float, k: float) -> Tuple[float, float]:
    """Returns (adjusted, lambda). No evidence (raw None or n_eff 0) -> exactly the prior."""
    if raw is None or n_eff <= 0:
        return prior, 0.0
    lam = n_eff / (n_eff + k)
    return lam * raw + (1.0 - lam) * prior, lam


# ---------------------------------------------------------------- Elo points
def convert_hfa_to_elo_points(games: Sequence[HomeGame], as_of: dt.date, half_life_years: float,
                              target_wins: float, scale: float,
                              bounds: Tuple[float, float] = (-400.0, 400.0)) -> Tuple[Optional[float], bool]:
    """
    The home bonus D (Elo points) with sum(w * E(home + D, away)) == target_wins,
    over the given games before as_of. Returns (D, at_bound). See module docstring
    for why H alone can't be converted without the games.
    """
    pts = [(recency_weight(game_age_years(g.date, as_of), half_life_years), g.home_pre_elo - g.away_pre_elo)
           for g in games if g.date < as_of]
    if not pts:
        return None, False

    ln10_over_scale = math.log(10.0) / scale

    def total_and_slope(bonus: float) -> Tuple[float, float]:
        t = dt_sum = 0.0
        for w, d in pts:
            e = expected_result(d + bonus, 0.0, scale)
            t += w * e
            dt_sum += w * e * (1.0 - e) * ln10_over_scale   # d/dD of the logistic
        return t, dt_sum

    lo, hi = bounds
    if target_wins <= total_and_slope(lo)[0]:
        return lo, True
    if target_wins >= total_and_slope(hi)[0]:
        return hi, True
    # Safeguarded Newton: Newton steps while they stay inside the bracket,
    # bisection otherwise. Same guarantee as pure bisection (the function is
    # strictly increasing, so the root is unique), a fraction of the work.
    x = 0.0 if lo < 0.0 < hi else (lo + hi) / 2.0
    for _ in range(100):
        f, slope = total_and_slope(x)
        f -= target_wins
        if abs(f) < 1e-10:
            break
        if f < 0:
            lo = x
        else:
            hi = x
        step = x - f / slope if slope > 0 else None
        x = step if step is not None and lo < step < hi else (lo + hi) / 2.0
        if hi - lo < 1e-9:
            break
    return x, False


# ---------------------------------------------------------------- one team
def calculate_team_hfa(team_games: Sequence[HomeGame], national_games: Sequence[HomeGame], as_of: dt.date,
                       cfg: dict, scale: float, fcs_hfa: Optional[float] = None,
                       national_points: Optional[float] = None) -> dict:
    """Every field of one team's estimate as of `as_of`. `national_points` may be passed in to avoid recomputing it."""
    L, K = cfg["half_life_years"], cfg["shrinkage_k"]
    bounds = tuple(cfg.get("point_bounds", (-400.0, 400.0)))
    nat = weighted_sums(national_games, as_of, L)
    baseline = calculate_raw_hfa(nat)
    if baseline is None:
        raise ValueError(f"no qualifying home games before {as_of}: no national baseline can be formed")
    prior = calculate_hfa_prior(baseline, fcs_hfa, cfg["fcs_prior_weight"])

    s = weighted_sums(team_games, as_of, L)
    raw = calculate_raw_hfa(s)
    n_eff = calculate_effective_sample_size(s, cfg.get("effective_n_method", "sum_weights"))
    adjusted, lam = apply_hfa_shrinkage(raw, prior, n_eff, K)

    if s.games:
        points, at_bound = convert_hfa_to_elo_points(team_games, as_of, L, adjusted * s.sum_wp, scale, bounds)
        raw_points, raw_at_bound = convert_hfa_to_elo_points(team_games, as_of, L, s.sum_wa, scale, bounds)
    else:
        if national_points is None:
            national_points, _ = convert_hfa_to_elo_points(national_games, as_of, L, prior * nat.sum_wp, scale, bounds)
        points, at_bound, raw_points, raw_at_bound = national_points, False, None, False
    return {
        "as_of": as_of.isoformat(),
        "raw_hfa": raw, "adjusted_hfa": adjusted, "prior_hfa": prior, "fbs_baseline": baseline,
        "fcs_prior_used": fcs_hfa is not None,
        "effective_n": n_eff, "lambda": lam, "games_used": s.games,
        "weighted_actual_wins": s.sum_wa, "weighted_expected_wins": s.sum_wp,
        "oldest_game_used": s.oldest.isoformat() if s.oldest else None,
        "newest_game_used": s.newest.isoformat() if s.newest else None,
        "elo_hfa_points": points, "elo_points_at_bound": at_bound,
        "raw_elo_hfa_points": raw_points, "raw_points_at_bound": raw_at_bound,
    }


# ---------------------------------------------------------------- audit trail
def generate_hfa_diagnostics(team_games: Sequence[HomeGame], as_of: dt.date, half_life_years: float,
                             opponent_names: Dict[int, str]) -> List[dict]:
    """One row per game contributing to a team's estimate: every term of the formula, for hand-checking."""
    out = []
    for g in team_games:
        if g.date >= as_of:
            continue
        age = game_age_years(g.date, as_of)
        w = recency_weight(age, half_life_years)
        out.append({
            "season": g.season, "date": g.date.isoformat(), "opponent": opponent_names.get(g.opponent_id, g.opponent_id),
            "home_pre_game_elo": round(g.home_pre_elo, 1), "away_pre_game_elo": round(g.away_pre_elo, 1),
            "neutral_expected_win_probability": round(g.p_neutral, 4), "actual_result": g.actual,
            "game_age_years": round(age, 3), "recency_weight": round(w, 5),
            "weighted_actual_win": round(w * g.actual, 5), "weighted_expected_win": round(w * g.p_neutral, 5),
        })
    return out
