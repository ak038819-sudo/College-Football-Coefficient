#!/usr/bin/env python3
"""
Fit and score the live win-probability model (src/win_probability.py).

Two of its parameters are fitted here, one is counted, and one is not fitted at
all because nothing in this database can fit it yet.

    points_per_elo  How many points of final margin a point of venue-adjusted
                    pregame Elo gap is worth. A least-squares fit of REGULATION
                    margin on `elo_game_history.elo_diff_adjusted`.
    sigma           The residual spread of that fit, in points: how far a game
                    lands from what the ratings implied.
    overtime_home_win
                    Counted, not fitted: the share of games tied at the end of
                    regulation that the home team won.
    tie_half_width  Solved, not assumed: the band around zero whose normal mass
                    equals how often games really end regulation level. The
                    geometric half point understates it by about half, because
                    margins pile up on 0, 3 and 7 rather than spreading
                    smoothly. Fitted on the overtime era only -- before 1996 a
                    tie simply stood, so those seasons measure a different
                    thing.
    time_exponent   NOT FITTED. See below.

------------------------------------------------------------------------
Regulation margin, not final margin
------------------------------------------------------------------------
A game that goes to overtime had a regulation margin of exactly zero, whatever
the final score says. The live model's random walk runs to the end of
regulation and hands a tie to the overtime term, so the quantity it has to
match is the regulation margin -- and using the final margin instead would
teach it that tie games end 31-28, which is the one thing they never do.

------------------------------------------------------------------------
Why the fit is on margins and not on outcomes
------------------------------------------------------------------------
Fitting the win/loss outcomes identifies only the RATIO of points_per_elo to
sigma: a model twice as confident in the margin and twice as uncertain about it
predicts exactly the same pregame probability. Margins identify both, and both
are needed, because the live number divides a margin by a spread that shrinks
as the clock runs down.

------------------------------------------------------------------------
What this cannot fit, and why it is honest about it
------------------------------------------------------------------------
`time_exponent` governs how fast the spread shrinks inside a game, so fitting
it needs the score at known points DURING games. This database holds final
scores only: `games.home_line_scores` exists as a column, but every committed
season CSV predates it and the archive is empty of per-quarter scores. So the
production value is 0.5, which is what a random walk gives -- an assumption,
recorded as one in config/model_config.json, not a measurement.

Everything else here IS measured, including two checks that bear on the
assumption:

  * the model's own pregame probabilities are scored against the rating
    engine's expectation on seasons the fit never saw, and
  * the tie mass the normal approximation implies at kickoff is compared with
    how often games actually end regulation level -- the single place where the
    shape of the distribution, rather than its width, is visible.

Run it:

    python3 src/calibrate_win_probability.py            # fit, hold out 2018+
    python3 src/calibrate_win_probability.py --holdout-from 2015
"""
from __future__ import annotations

import argparse
import csv
import math
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from win_probability import (  # noqa: E402  (path set above, as elsewhere in src/)
    DEFAULT_TIE_HALF_WIDTH,
    REGULATION_SECONDS,
    WinProbabilityParams,
    home_win_probability,
    tie_mass,
)

REPO = Path(__file__).resolve().parents[1]
DEFAULT_DB = REPO / "db" / "league.db"
DEFAULT_OUT = REPO / "data" / "processed" / "win_probability_calibration.csv"
DEFAULT_HOLDOUT_FROM = 2018
# Fixed, not fitted; see the module docstring.
TIME_EXPONENT = 0.5

GAMES_SQL = """
    SELECT g.season_year,
           g.home_score - g.away_score AS final_margin,
           g.went_ot,
           e.elo_diff_adjusted,
           e.elo_expectation
      FROM games g
      JOIN elo_game_history e
        ON e.game_id = g.game_id AND e.team_id = g.home_team_id
     WHERE g.home_score IS NOT NULL AND g.away_score IS NOT NULL
       AND e.elo_diff_adjusted IS NOT NULL
     ORDER BY g.season_year, g.game_id
"""


def load_games(conn: sqlite3.Connection) -> list[dict]:
    rows = []
    for season, final_margin, went_ot, elo_diff, elo_expectation in conn.execute(GAMES_SQL):
        rows.append({
            "season": int(season),
            # A game that reached overtime was level after four quarters.
            "margin": 0 if went_ot else int(final_margin),
            "went_ot": bool(went_ot),
            "elo_diff": float(elo_diff),
            "elo_expectation": float(elo_expectation),
            "home_result": 0.5 if final_margin == 0 else (1.0 if final_margin > 0 else 0.0),
        })
    return rows


def fit_margin_model(rows: list[dict]) -> tuple[float, float, float]:
    """Least squares: regulation margin on the venue-adjusted Elo gap.

    Returns (points_per_elo, intercept, sigma). The intercept is fitted and
    reported rather than forced to zero -- `elo_diff_adjusted` already carries
    the engine's home-field term, so an intercept far from zero would mean the
    rating engine's venue adjustment and the real home edge disagree, which is
    worth seeing instead of absorbing.
    """
    n = len(rows)
    if n < 2:
        raise ValueError("not enough games to fit")
    mean_x = sum(r["elo_diff"] for r in rows) / n
    mean_y = sum(r["margin"] for r in rows) / n
    sxx = sum((r["elo_diff"] - mean_x) ** 2 for r in rows)
    sxy = sum((r["elo_diff"] - mean_x) * (r["margin"] - mean_y) for r in rows)
    if sxx <= 0:
        raise ValueError("every game has the same Elo gap; nothing to fit")
    slope = sxy / sxx
    intercept = mean_y - slope * mean_x
    residual_sq = sum((r["margin"] - (intercept + slope * r["elo_diff"])) ** 2 for r in rows)
    sigma = math.sqrt(residual_sq / (n - 2))
    return slope, intercept, sigma


def overtime_home_share(rows: list[dict]) -> tuple[float, int]:
    ot = [r for r in rows if r["went_ot"]]
    if not ot:
        return 0.5, 0
    return sum(r["home_result"] for r in ot) / len(ot), len(ot)


def overtime_era_rows(rows: list[dict]) -> list[dict]:
    """Seasons from the first one that produced an overtime game onwards.

    Before 1996 a tie stood, so a level game was a RESULT rather than the start
    of an overtime period. Those seasons' tie rate measures a different sport
    and would drag the fitted band toward an era the live model never covers.
    """
    ot_seasons = [r["season"] for r in rows if r["went_ot"]]
    if not ot_seasons:
        return rows
    first = min(ot_seasons)
    return [r for r in rows if r["season"] >= first]


def fit_tie_half_width(rows: list[dict], points_per_elo: float, sigma: float,
                       tolerance: float = 1e-6) -> tuple[float, float, float]:
    """The band whose normal mass matches the observed rate of level games.

    Returns (width, model_rate_at_that_width, actual_rate). Bisection, because
    the mass is monotone in the width and the interval is known: zero width is
    zero mass, and a touchdown's width is far more mass than any season has
    ever seen. A sample with no level games at all leaves the geometric default
    rather than collapsing the band to nothing.
    """
    actual = sum(1 for r in rows if r["margin"] == 0) / len(rows)
    if actual <= 0:
        return DEFAULT_TIE_HALF_WIDTH, 0.0, 0.0

    def mass(width: float) -> float:
        params = WinProbabilityParams(points_per_elo=points_per_elo, sigma=sigma,
                                      time_exponent=TIME_EXPONENT, overtime_home_win=0.5,
                                      tie_half_width=width)
        return sum(tie_mass(points_per_elo * r["elo_diff"], sigma, params)
                   for r in rows) / len(rows)

    low, high = 0.0, 6.999
    if mass(high) < actual:
        # More level games than a seven-point band can account for: the normal
        # approximation is the wrong shape for this sample, and widening the
        # band further would call one-score leads ties to paper over it.
        return high, mass(high), actual
    while high - low > tolerance:
        middle = (low + high) / 2
        if mass(middle) < actual:
            low = middle
        else:
            high = middle
    width = (low + high) / 2
    return width, mass(width), actual


DRIVE_STATES_SQL = """
    SELECT g.season_year,
           e.elo_diff_adjusted,
           d.home_score_after - d.away_score_after AS margin,
           d.seconds_remaining,
           g.home_score - g.away_score AS final_margin
      FROM game_drives d
      JOIN games g ON g.game_id = d.game_id
      JOIN elo_game_history e
        ON e.game_id = g.game_id AND e.team_id = g.home_team_id
     WHERE g.home_score IS NOT NULL AND g.away_score IS NOT NULL
       AND e.elo_diff_adjusted IS NOT NULL
       AND d.seconds_remaining IS NOT NULL
       AND d.home_score_after IS NOT NULL AND d.away_score_after IS NOT NULL
"""


def load_drive_states(conn: sqlite3.Connection) -> list[dict]:
    """Every drive boundary as a game state with a known outcome.

    This is what `time_exponent` needs and what final scores cannot give: the
    score at a known moment inside a game. A drive's end is such a moment --
    the clock and both scores are recorded -- and the game's final result says
    how it turned out.

    States from the same game are heavily correlated (one game contributes two
    dozen of them, mostly with the same margin), so the fit is consistent but
    the effective sample is nearer the number of GAMES than the number of
    rows. Read the held-out log loss, not a standard error, as the evidence.
    """
    states = []
    for season, elo_diff, margin, secs, final_margin in conn.execute(DRIVE_STATES_SQL):
        if secs is None or not 0 <= secs <= REGULATION_SECONDS:
            continue
        states.append({
            "season": int(season),
            "elo_diff": float(elo_diff),
            "margin": int(margin),
            "secs": int(secs),
            "home_result": 0.5 if final_margin == 0 else (1.0 if final_margin > 0 else 0.0),
        })
    return states


def state_log_loss(states: list[dict], params: WinProbabilityParams) -> float:
    eps = 1e-12
    total = 0.0
    for state in states:
        p = home_win_probability(state["elo_diff"], state["margin"], state["secs"], params)
        y = state["home_result"]
        total -= y * math.log(max(p, eps)) + (1 - y) * math.log(max(1 - p, eps))
    return total / len(states)


def fit_time_exponent(states: list[dict], base: WinProbabilityParams,
                      low: float = 0.1, high: float = 1.5, tolerance: float = 1e-3) -> tuple[float, float]:
    """The exponent that best predicts in-game states, by golden section.

    Golden section rather than a grid because the loss is smooth and unimodal
    in this parameter -- a bigger exponent narrows the late-game spread, which
    is monotone in its effect on every state -- and because each evaluation
    walks every drive in the archive.

    Returns (exponent, log loss at it).
    """
    phi = (math.sqrt(5) - 1) / 2
    with_exponent = lambda e: WinProbabilityParams(
        points_per_elo=base.points_per_elo, sigma=base.sigma, time_exponent=e,
        overtime_home_win=base.overtime_home_win, tie_half_width=base.tie_half_width)
    a, b = low, high
    c, d = b - phi * (b - a), a + phi * (b - a)
    fc, fd = state_log_loss(states, with_exponent(c)), state_log_loss(states, with_exponent(d))
    while b - a > tolerance:
        if fc < fd:
            b, d, fd = d, c, fc
            c = b - phi * (b - a)
            fc = state_log_loss(states, with_exponent(c))
        else:
            a, c, fc = c, d, fd
            d = a + phi * (b - a)
            fd = state_log_loss(states, with_exponent(d))
    best = (a + b) / 2
    return best, state_log_loss(states, with_exponent(best))


def state_calibration(states: list[dict], params: WinProbabilityParams) -> list[dict]:
    """Held-out states grouped by how much of the game was left.

    The quarter a state belongs to is where the model's time curve shows: an
    exponent that is wrong late is invisible in a pooled number and obvious
    here.
    """
    buckets = {}
    for state in states:
        quarter = min(3, int((REGULATION_SECONDS - state["secs"]) / (REGULATION_SECONDS / 4)))
        buckets.setdefault(quarter, []).append(state)
    rows = []
    for quarter in sorted(buckets):
        group = buckets[quarter]
        pairs = [(home_win_probability(s["elo_diff"], s["margin"], s["secs"], params), s["home_result"])
                 for s in group]
        rows.append({"quarter": quarter + 1, "states": len(group),
                     "log_loss": log_loss(pairs), "brier": brier(pairs),
                     "mean_predicted": sum(p for p, _ in pairs) / len(pairs),
                     "actual": sum(y for _, y in pairs) / len(pairs)})
    return rows


def brier(pairs) -> float:
    pairs = list(pairs)
    return sum((p - y) ** 2 for p, y in pairs) / len(pairs)


def log_loss(pairs) -> float:
    pairs = list(pairs)
    eps = 1e-12
    return -sum(y * math.log(max(p, eps)) + (1 - y) * math.log(max(1 - p, eps))
                for p, y in pairs) / len(pairs)


def score(rows: list[dict], params: WinProbabilityParams) -> dict:
    """Pregame scores for this model and for the rating engine's own expectation.

    The engine's expectation is the benchmark that matters: this model is only
    allowed to drive the scoreboard if, with no score on the board, it says
    about what the ratings already said. A live model that quietly disagrees
    with the pregame number beside it is a bug the viewer can see.
    """
    mine = [(home_win_probability(r["elo_diff"], 0.0, REGULATION_SECONDS, params), r["home_result"])
            for r in rows]
    engine = [(r["elo_expectation"], r["home_result"]) for r in rows]
    tie_rate = sum(1 for r in rows if r["margin"] == 0) / len(rows)
    # The tie mass this model puts on each game at kickoff, averaged. Compared
    # with the real rate, this is a check on the SHAPE of the approximation.
    predicted_tie = sum(tie_mass(params.points_per_elo * r["elo_diff"], params.sigma, params)
                        for r in rows) / len(rows)
    return {
        "games": len(rows),
        "brier": brier(mine),
        "log_loss": log_loss(mine),
        "engine_brier": brier(engine),
        "engine_log_loss": log_loss(engine),
        "actual_tie_rate": tie_rate,
        "model_tie_rate": predicted_tie,
    }


def calibration_bins(rows: list[dict], params: WinProbabilityParams, bins: int = 10) -> list[dict]:
    buckets: dict[int, list] = {}
    for r in rows:
        p = home_win_probability(r["elo_diff"], 0.0, REGULATION_SECONDS, params)
        index = min(bins - 1, int(p * bins))
        buckets.setdefault(index, []).append((p, r["home_result"]))
    out = []
    for index in sorted(buckets):
        pairs = buckets[index]
        out.append({
            "bin": f"{index / bins:.1f}-{(index + 1) / bins:.1f}",
            "games": len(pairs),
            "mean_predicted": sum(p for p, _ in pairs) / len(pairs),
            "actual": sum(y for _, y in pairs) / len(pairs),
        })
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--holdout-from", type=int, default=DEFAULT_HOLDOUT_FROM,
                    help="first season withheld from the fit (default %(default)s)")
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    args = ap.parse_args(argv)

    conn_states = sqlite3.connect(args.db)
    rows = load_games(conn_states)
    if not conn_states.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='game_drives'").fetchone():
        conn_states.close()
        conn_states = None
    if not rows:
        print("No games with a pregame Elo gap; run build_elo.py first.", file=sys.stderr)
        return 1

    train = [r for r in rows if r["season"] < args.holdout_from]
    test = [r for r in rows if r["season"] >= args.holdout_from]
    if not train or not test:
        print(f"--holdout-from {args.holdout_from} leaves one side empty "
              f"({len(train)} train, {len(test)} test).", file=sys.stderr)
        return 1

    slope, intercept, sigma = fit_margin_model(train)
    ot_share, ot_games = overtime_home_share(train)
    width, width_mass, width_actual = fit_tie_half_width(overtime_era_rows(train), slope, sigma)
    params = WinProbabilityParams(points_per_elo=slope, sigma=sigma,
                                  time_exponent=TIME_EXPONENT, overtime_home_win=ot_share,
                                  tie_half_width=width)
    params.validate()

    full_slope, full_intercept, full_sigma = fit_margin_model(rows)
    full_ot, full_ot_games = overtime_home_share(rows)
    full_width, _, full_width_actual = fit_tie_half_width(overtime_era_rows(rows),
                                                          full_slope, full_sigma)

    print(f"Fitted on {len(train)} games, {train[0]['season']}-{args.holdout_from - 1}")
    print(f"  points_per_elo     {slope:.6f}  ({1 / slope:.1f} Elo points per point of margin)")
    print(f"  intercept          {intercept:+.3f} points  (0 = the engine's home-field term is right)")
    print(f"  sigma              {sigma:.3f} points")
    print(f"  overtime_home_win  {ot_share:.4f} over {ot_games} games tied after regulation")
    print(f"  tie_half_width     {width:.3f} points  (model {width_mass:.4f} vs actual "
          f"{width_actual:.4f} level games, overtime era only)")
    states = load_drive_states(conn_states) if conn_states else []
    train_states = [st for st in states if st["season"] < args.holdout_from]
    test_states = [st for st in states if st["season"] >= args.holdout_from]
    if train_states and test_states:
        exponent, fitted_loss = fit_time_exponent(train_states, params)
        params = WinProbabilityParams(points_per_elo=slope, sigma=sigma, time_exponent=exponent,
                                      overtime_home_win=ot_share, tie_half_width=width)
        walk = WinProbabilityParams(points_per_elo=slope, sigma=sigma, time_exponent=TIME_EXPONENT,
                                    overtime_home_win=ot_share, tie_half_width=width)
        print(f"  time_exponent      {exponent:.4f} fitted on {len(train_states)} in-game states "
              f"from {len({(st['season'], st['margin'], st['secs']) for st in train_states})} distinct states")
        print(f"                     train log loss {fitted_loss:.5f} against "
              f"{state_log_loss(train_states, walk):.5f} for a plain random walk (0.5)")
        print(f"                     held-out log loss {state_log_loss(test_states, params):.5f} against "
              f"{state_log_loss(test_states, walk):.5f}")
        print("\n  Held-out in-game states, by quarter")
        for row in state_calibration(test_states, params):
            print(f"    Q{row['quarter']}  n={row['states']:7d}  log loss {row['log_loss']:.5f}  "
                  f"predicted {row['mean_predicted']:.3f}  actual {row['actual']:.3f}")
    else:
        exponent = None
        print(f"  time_exponent      {TIME_EXPONENT} (ASSUMED: a random walk; no loaded drives to fit it)")

    scored = {"train": score(train, params), "test": score(test, params)}
    for name, s in scored.items():
        print(f"\n{name}: {s['games']} games")
        print(f"  this model   Brier {s['brier']:.5f}  log loss {s['log_loss']:.5f}")
        print(f"  rating Elo   Brier {s['engine_brier']:.5f}  log loss {s['engine_log_loss']:.5f}")
        print(f"  regulation ties: actual {s['actual_tie_rate']:.4f}, model {s['model_tie_rate']:.4f}")

    print("\nHeld-out calibration")
    for row in calibration_bins(test, params):
        print(f"  {row['bin']}  n={row['games']:5d}  predicted {row['mean_predicted']:.3f}  "
              f"actual {row['actual']:.3f}")

    # The production parameters use every season; the holdout above is what
    # shows the fit generalises, not what ships. The time exponent is refit the
    # same way, on every state and against the all-season margin model, because
    # an exponent fitted beside a different sigma is not the same number.
    full_params = WinProbabilityParams(points_per_elo=full_slope, sigma=full_sigma,
                                       time_exponent=TIME_EXPONENT,
                                       overtime_home_win=full_ot, tie_half_width=full_width)
    full_exponent = fit_time_exponent(states, full_params)[0] if states else None
    print("\nRefitted on every season (the production values):")
    print(f"  points_per_elo {full_slope:.6f}  intercept {full_intercept:+.3f}  "
          f"sigma {full_sigma:.3f}  overtime_home_win {full_ot:.4f} ({full_ot_games} games)  "
          f"tie_half_width {full_width:.3f} (actual {full_width_actual:.4f} level)")
    if full_exponent is not None:
        print(f"  time_exponent {full_exponent:.4f} on {len(states)} in-game states")

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["metric", "value", "games"])
        writer.writerow(["holdout_from", args.holdout_from, len(rows)])
        for key, value in (("points_per_elo", slope), ("intercept", intercept), ("sigma", sigma),
                           ("overtime_home_win", ot_share), ("tie_half_width", width),
                           ("time_exponent", exponent if exponent is not None else TIME_EXPONENT)):
            writer.writerow([f"train_{key}", f"{value:.6f}", len(train)])
        all_season_values = [("points_per_elo", full_slope), ("intercept", full_intercept),
                             ("sigma", full_sigma), ("overtime_home_win", full_ot),
                             ("tie_half_width", full_width)]
        if full_exponent is not None:
            all_season_values.append(("time_exponent", full_exponent))
        for key, value in all_season_values:
            writer.writerow([f"all_seasons_{key}", f"{value:.6f}", len(rows)])
        for name, s in scored.items():
            for key in ("brier", "log_loss", "engine_brier", "engine_log_loss",
                        "actual_tie_rate", "model_tie_rate"):
                writer.writerow([f"{name}_{key}", f"{s[key]:.6f}", s["games"]])
        for row in calibration_bins(test, params):
            writer.writerow([f"test_calibration_{row['bin']}",
                             f"{row['mean_predicted']:.6f}|{row['actual']:.6f}", row["games"]])
        if states:
            for row in state_calibration(test_states, params):
                writer.writerow([f"test_states_q{row['quarter']}_log_loss",
                                 f"{row['log_loss']:.6f}", row["states"]])
    if conn_states is not None:
        conn_states.close()
    print(f"\nWrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
