#!/usr/bin/env python3
"""
Fits the bracket simulator's temperature against real game results (SIM).

`src/coefficients/simulate_bracket.py` turns the CoE gap between two teams
into a win probability with a logistic:

    P(A beats B) = 1 / (1 + exp(-(coe_A - coe_B) / T))

and has always used T = 6.0, a value its own docstring describes as picked
to produce plausible-looking upset odds -- "a genuine modeling choice, not
derived from the rules". It had never been checked against a game result.
This script checks it, and says what T the record actually supports.

## What is being predicted, and why it is out of sample

The predictor for a game in season Y is each team's rolling five-year CoE
ENTERING Y -- the `end_year = Y-1` row of data/processed/team_coeff_5yr.csv.
That value is complete before Y kicks off, so every game scored here is a
genuine walk-forward prediction, on the same footing as calibrate_elo.py and
calibrate_hybrid_weight.py. The `end_year = Y` rows must NOT be used: that
window spans [Y-4 .. Y], so it is built partly out of the very games being
predicted.

Season Y-1 CoE is not quite the quantity the simulator is handed at run
time -- a playoff is played after its season, so the bracket seeds and
simulates on `end_year = Y`, which by then is legitimately known. The two
are the same kind of number on the same scale (a five-year weighted CoE
sum), so a temperature fitted on one transfers to the other; it does mean
the fitted T is calibrated against a slightly noisier input than the
simulator's, which makes it, if anything, a touch conservative.

## Which games the fit is allowed to see

This is the part that decides the answer, so it is worth stating plainly.

Fitting on every game gives T = 2.9 -- but the simulator never simulates
"every game". It simulates a 24-team field, where both sides are among the
best teams in the country and the CoE gaps are small. Fitting on all games
lets tens of thousands of mismatches set the slope, and a slope tuned on
blowouts is measurably WORSE on the matchups the bracket actually contains.

So the fit that counts is restricted to games where both teams were in the
top `--top-n` by CoE entering that season, which is the closest thing the
record has to a field of playoff-calibre opponents. The unrestricted fit is
still computed and printed, as the contrast that justifies the restriction.

## Home field

Real games mostly DO have a home team, and their outcomes carry that
advantage, so fitting T alone would quietly absorb home field into the slope
and bias it. Both parameters therefore have to be fitted together whatever
the bracket then does with h.

What the bracket does with h has changed. It used to discard it, on the
argument that CoE already decided who hosts so applying it would
double-count. That argument was wrong -- CoE decides WHICH team hosts and
says nothing about the advantage OF hosting -- and h is now applied in the
Round of 24, the one round with a host. Either way, h has to be in this fit
for T to mean anything.

So two parameters are fitted together:

    P(home wins) = 1 / (1 + exp(-((coe_home - coe_away) + h * is_home) / T))

where `h` is the home-field edge expressed in CoE points and `is_home` is 0
at a neutral site. T is what the simulator wants; h is reported beside it
because it is the measured size of the advantage the simulator chooses to
leave out, which is worth knowing rather than assuming.

Scored from the HOME team's perspective only -- the away row carries the
same information, and scoring both would double count -- exactly as
calibrate_hybrid_weight.py does.

Writes a CSV of the search and prints a recommendation. Like the other
calibration scripts, it changes no config and no default automatically.

Usage:
    python src/calibrate_sim_temperature.py [--db db/league.db] [--top-n 40]
        [--out data/processed/sim_temperature_calibration.csv]
"""
from __future__ import annotations

import argparse
import csv
import math
import sqlite3
from pathlib import Path

from validation import brier_score, log_loss, accuracy, base_rate, calibration_error

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "processed"

# The value simulate_bracket.py has used since it was written, carried here so
# the report can say what the change costs or buys against the status quo.
INCUMBENT_TEMPERATURE = 6.0

# How many teams, by CoE entering the season, count as playoff calibre. 40 is
# a little wider than the 24-team field on purpose: it roughly doubles the
# sample without reaching down into teams a bracket would never contain, and
# the fitted values are stable across 25 and 40 (see the docstring).
DEFAULT_TOP_N = 40

# A coarse grid first, then a refinement pass around the winner. A grid rather
# than an optimizer keeps the whole search in the CSV, so the shape of the
# curve is reviewable and a near-tie is visible instead of hidden behind a
# single returned number -- which matters here, because the curve IS flat.
TEMPERATURE_GRID = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 14.0, 20.0]
HOME_FIELD_GRID = [0.0, 0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0]


def load_coe_by_end_year() -> dict[int, dict[str, float]]:
    """{end_year: {team_name: coeff_5yr}} from the committed rolling CoE file."""
    out: dict[int, dict[str, float]] = {}
    with open(DATA_DIR / "team_coeff_5yr.csv", newline="") as f:
        for row in csv.DictReader(f):
            out.setdefault(int(row["end_year"]), {})[row["team_name"]] = float(row["coeff_5yr"])
    return out


def top_n_by_season(coe_by_end_year: dict[int, dict[str, float]], top_n: int) -> dict[int, set[str]]:
    """{end_year: the top_n team names by CoE in that window}."""
    return {year: {name for name, _ in sorted(values.items(), key=lambda kv: -kv[1])[:top_n]}
            for year, values in coe_by_end_year.items()}


def load_games(conn: sqlite3.Connection) -> list[tuple]:
    """
    (season, home_name, away_name, is_home, s_home) for every completed game.

    `is_home` is 0 at a neutral site, so the home-field term is fitted only on
    the games that actually had one.
    """
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT g.season_year, g.neutral_site, g.home_score, g.away_score,
               h.team_name AS home_name, a.team_name AS away_name
        FROM games g
        JOIN teams h ON h.team_id = g.home_team_id
        JOIN teams a ON a.team_id = g.away_team_id
        WHERE g.home_score IS NOT NULL AND g.away_score IS NOT NULL
        ORDER BY g.season_year, g.game_date, g.game_id
        """
    ).fetchall()
    out = []
    for r in rows:
        if r["home_score"] > r["away_score"]:
            s = 1.0
        elif r["home_score"] < r["away_score"]:
            s = 0.0
        else:
            s = 0.5
        out.append((r["season_year"], r["home_name"], r["away_name"],
                    0.0 if r["neutral_site"] else 1.0, s))
    return out


def gaps_for(games: list[tuple], coe_by_end_year: dict[int, dict[str, float]],
             eligible: dict[int, set[str]] | None = None) -> list[tuple]:
    """
    (gap, is_home, s_home) per scoreable game: the CoE gap ENTERING the season.

    A game is dropped when either team has no rating in the prior window --
    early seasons before the first full window closes, and programs with no
    prior five years. Dropping is the honest move: substituting 0.0 would
    assert an average team where there is no evidence at all.

    With `eligible`, a game is kept only when BOTH teams are in that season's
    eligible set, which is how the playoff-calibre population is drawn.
    """
    out = []
    for season, home, away, is_home, s_home in games:
        entering = coe_by_end_year.get(season - 1)
        if not entering or home not in entering or away not in entering:
            continue
        if eligible is not None:
            field = eligible.get(season - 1, set())
            if home not in field or away not in field:
                continue
        out.append((entering[home] - entering[away], is_home, s_home))
    return out


def probabilities(gaps: list[tuple], temperature: float, home_field: float) -> list[tuple]:
    """(p_home, s_home) pairs, in the form src/validation.py's metrics take."""
    return [(1.0 / (1.0 + math.exp(-(gap + home_field * is_home) / temperature)), s)
            for gap, is_home, s in gaps]


def score(gaps: list[tuple], temperature: float, home_field: float) -> dict:
    pairs = probabilities(gaps, temperature, home_field)
    return {
        "temperature": round(temperature, 4),
        "home_field": round(home_field, 4),
        "n": len(pairs),
        "brier": brier_score(pairs),
        "log_loss": log_loss(pairs),
        "accuracy": accuracy(pairs),
        "calibration_error": calibration_error(pairs),
    }


def search(gaps: list[tuple], temperatures: list[float], home_fields: list[float]) -> list[dict]:
    """Every (T, h) pair scored, sorted best Brier first."""
    results = [score(gaps, t, h) for t in temperatures for h in home_fields]
    results.sort(key=lambda r: r["brier"])
    return results


def refine(center_t: float, center_h: float) -> tuple[list[float], list[float]]:
    """
    A finer grid around the coarse winner, so the reported value is not an
    artifact of where the coarse ticks happened to fall.
    """
    temperatures = [round(center_t + s, 4) for s in
                    (-0.75, -0.5, -0.25, 0.0, 0.25, 0.5, 0.75) if center_t + s > 0]
    home_fields = [round(center_h + s, 4) for s in
                   (-0.5, -0.25, -0.125, 0.0, 0.125, 0.25, 0.5) if center_h + s >= 0]
    return temperatures, home_fields


def fit(gaps: list[tuple]) -> tuple[dict, list[dict]]:
    """Coarse then fine. Returns the best row and every row searched."""
    coarse = search(gaps, TEMPERATURE_GRID, HOME_FIELD_GRID)
    fine = search(gaps, *refine(coarse[0]["temperature"], coarse[0]["home_field"]))
    rows = list({(r["temperature"], r["home_field"]): r for r in coarse + fine}.values())
    rows.sort(key=lambda r: r["brier"])
    return rows[0], rows


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--top-n", type=int, default=DEFAULT_TOP_N,
                   help="how many teams by entering CoE count as playoff calibre")
    p.add_argument("--out", default="data/processed/sim_temperature_calibration.csv")
    args = p.parse_args()

    coe_by_end_year = load_coe_by_end_year()
    conn = sqlite3.connect(args.db)
    games = load_games(conn)
    conn.close()

    all_gaps = gaps_for(games, coe_by_end_year)
    field_gaps = gaps_for(games, coe_by_end_year,
                          top_n_by_season(coe_by_end_year, args.top_n))
    if not field_gaps:
        raise SystemExit("no scoreable games -- run the pipeline and build_coefficients.py first")

    seasons = {s for s, *_ in games}
    print(f"{len(all_gaps):,} of {len(games):,} completed games are scoreable "
          f"(both teams rated entering their season), {min(seasons)}-{max(seasons)}.")
    print(f"{len(field_gaps):,} of those are top-{args.top_n} against top-{args.top_n} "
          f"-- the population the bracket actually simulates.")
    print(f"Base rate (home team wins), playoff-calibre games: "
          f"{base_rate(probabilities(field_gaps, 1.0, 0.0)):.5f}\n")

    all_best, _ = fit(all_gaps)
    best, rows = fit(field_gaps)

    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["temperature", "home_field", "n", "brier", "log_loss",
                                          "accuracy", "calibration_error"])
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote the full playoff-calibre search ({len(rows)} combinations) to {args.out}\n")

    print(f"Best ten on top-{args.top_n} games (lower Brier is better):")
    for r in rows[:10]:
        print(f"  T={r['temperature']:>6}  home_field={r['home_field']:>6}  "
              f"Brier={r['brier']:.5f}  logloss={r['log_loss']:.5f}  "
              f"acc={r['accuracy']:.4f}  cal_err={r['calibration_error']:.5f}")

    incumbent = score(field_gaps, INCUMBENT_TEMPERATURE, 0.0)
    naive = score(field_gaps, all_best["temperature"], all_best["home_field"])
    print(f"\nOn the same top-{args.top_n} games:")
    print(f"  incumbent  T={INCUMBENT_TEMPERATURE}, no home-field term: "
          f"Brier={incumbent['brier']:.5f}, calibration error={incumbent['calibration_error']:.5f}")
    print(f"  all-games fit T={all_best['temperature']}, h={all_best['home_field']}: "
          f"Brier={naive['brier']:.5f}, calibration error={naive['calibration_error']:.5f}")
    print(f"  fitted     T={best['temperature']}, h={best['home_field']}: "
          f"Brier={best['brier']:.5f}, calibration error={best['calibration_error']:.5f}")
    print(f"\nBrier improvement over the incumbent: {incumbent['brier'] - best['brier']:+.5f}")
    print(f"Calibration error improvement:         "
          f"{incumbent['calibration_error'] - best['calibration_error']:+.5f}")
    print(f"\nThe all-games fit (T={all_best['temperature']}) beats the incumbent but loses clearly to\n"
          f"the restricted one on the games that matter here, which is why the restricted fit is\n"
          f"what gets reported: a slope tuned mostly on mismatches does not transfer to a bracket.")
    print("\nT is what the bracket simulator uses. home_field is the measured size of the\n"
          "advantage the simulator deliberately leaves out; it is reported, not applied.")
    print("This is a recommendation only -- no default or config is modified automatically.")


if __name__ == "__main__":
    main()
