#!/usr/bin/env python3
"""
Team statistics with the schedule taken out of them.

Every team number on the Stats pages is a raw total or a raw average: points
scored, points allowed, EPA per play, success rate. None of them knows who the
team played. So a defence that faced four of the best offences in the country
reads as worse than one that faced four of the worst, and a 38-point offence
built on three directional-school opponents outranks a 31-point offence that
played in the SEC. The table is accurate and the ranking it implies is wrong.

This module answers the question the raw table cannot: how would this team's
offence have done against an AVERAGE opponent, and how would an average
offence have done against this team's defence.

------------------------------------------------------------------------
The model
------------------------------------------------------------------------
For one season and one metric, every game gives two observations -- each
team's offence against the other's defence. Each is modelled as

    value = mu + off(team) + def(opponent) + home * side

where `mu` is the league's weighted mean, `off` and `def` are one value per
team, `side` is +0.5 at home, -0.5 away and 0 at a neutral site, and `home` is
a single league-wide scalar. A good offence has a positive `off`; a good
defence has a NEGATIVE `def`, because `def` is what the defence adds to what
offences get against it.

The published numbers put those back on the metric's own scale, so they can be
read beside the raw ones and sorted in the same direction:

    adjusted offence = mu + off(team)     what this offence would do against an
                                          average defence, at a neutral site
    adjusted defence = mu + def(team)     what an average offence would do
                                          against this defence

Fitted by weighted ridge regression (`LAMBDA`, in units of the metric's own
weight), solved by coordinate descent. The ridge term is not decoration: in
week 2 a team has one opponent, and an unpenalised fit would hand a team that
beat a bad defence by 40 an offensive rating no number of later games could
pull back. It pulls every team toward the league mean by an amount that
shrinks as the games accumulate, which is the honest statement of what one
game knows.

`mu`, being a weighted mean of the games actually in the fit, is also the
answer to "average" in both sentences above: average for THIS season, not for
all time. A 2026 adjusted 28.0 and a 1994 adjusted 28.0 are each 'what an
average opponent of its own season would have conceded', which is what makes
them comparable at all.

------------------------------------------------------------------------
What this is not
------------------------------------------------------------------------
* NOT a rating. Nothing in Elo, CoE, the hybrid layer, the playoff model or
  any prediction reads this module. It is a description of statistics that
  were already published, with one known distortion removed.

* NOT a margin model. The two sides are fitted separately on what each offence
  did, never on the difference, so a team is never credited on defence for
  what its offence scored.

* NOT a claim about games against opponents this project does not rate. An
  FBS-vs-FCS game has no opponent to adjust against -- the FCS side has no
  offence or defence in the fit, and giving it the league mean would price
  Mercer as an average FBS team and quietly reward every team that scheduled
  one. Those games are excluded from the fit and counted separately, exactly
  as the remaining-schedule average excludes them (see
  build_schedule_strength.py). A team's raw numbers here therefore cover the
  same games as its adjusted ones, so the two columns are always comparable;
  they will not match the Stats page's own raw averages, which include every
  opponent.

* NOT available for every metric in every season. CFBD's play-by-play metrics
  begin in 2001 and 2002 is nearly empty; points run as far back as the game
  archive does. A metric with too little of a season to fit is absent from
  that season rather than fitted on scraps.

Usage:
    python src/build_adjusted_stats.py              # the current season
    python src/build_adjusted_stats.py --year 2024
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))


# Ridge strength, per metric, in units of that metric's own weight: points are
# weighted one per game, so 2.0 is two games' worth of pull toward the mean;
# the play-by-play metrics are weighted by plays, so 200 is roughly three
# games of snaps.
#
# MEASURED, not assumed, by holding out a fifth of each season's games and
# scoring the fit's predictions on them across 2005-2026 -- see
# src/calibrate_adjusted_stats.py. Every value here beats the league mean alone
# by a wide margin (points 9.32 vs 11.29 mean absolute error) and beats an
# unpenalised fit (9.54). But the curve is FLAT around its floor: 1.5 to 2.5
# for points, and 180 to 250 for the play-by-play metrics, differ in the fourth
# decimal of an error measured in points per game. So these are round numbers
# inside the flat region, not a claim that 2.0 beats 1.9. Do not re-tune them
# on a fourth-decimal difference; a rerun's own noise is bigger than that.
LAMBDA = {"points": 2.0, "ppa": 200.0, "success_rate": 200.0}

# A fit needs enough of a season to mean anything. Below this many observations
# the metric is absent for that season rather than fitted on scraps.
MIN_OBSERVATIONS = 200
# A team needs its own games too: one game is an opponent, not a season.
MIN_TEAM_GAMES = 2

MAX_ITERATIONS = 500
TOLERANCE = 1e-10

# (key, label, how a number reads, which direction is good for the offence).
METRICS = [
    ("points", "Points per game", "points", 1),
    ("ppa", "EPA per play", "epa", 3),
    ("success_rate", "Success rate", "pct", 3),
]
METRIC_LABELS = {k: label for k, label, _, _ in METRICS}

POINTS_SQL = """
    SELECT home_team_id, away_team_id, home_score, away_score, neutral_site
      FROM games
     WHERE season_year = ?
       AND home_score IS NOT NULL
       AND away_score IS NOT NULL
"""

# The opponent comes from `games`, never from the advanced row itself: a row
# whose game is not in `games` is a game against an opponent this project does
# not rate, and the join is what excludes it.
ADVANCED_SQL = """
    SELECT a.team_id,
           CASE WHEN g.home_team_id = a.team_id THEN g.away_team_id ELSE g.home_team_id END,
           a.off_{metric},
           a.off_plays,
           g.neutral_site,
           CASE WHEN g.home_team_id = a.team_id THEN 1 ELSE 0 END
      FROM game_team_advanced a
      JOIN games g ON g.game_id = a.game_id
     WHERE a.season_year = ?
       AND g.home_score IS NOT NULL
       AND a.off_{metric} IS NOT NULL
       AND a.off_plays IS NOT NULL
       AND a.off_plays > 0
"""


def side_of(is_home: int, neutral: int) -> float:
    """+0.5 at home, -0.5 away, 0 at a neutral site.

    Split rather than a 0/1 home flag so that `home` is the full home-to-away
    gap and `mu` stays the league mean rather than the mean away performance.
    """
    if neutral:
        return 0.0
    return 0.5 if is_home else -0.5


def point_observations(conn: sqlite3.Connection, season: int) -> list[tuple]:
    """(team, opponent, value, weight, side) per team per completed FBS game."""
    out = []
    for home, away, hs, ascore, neutral in conn.execute(POINTS_SQL, (season,)):
        out.append((home, away, float(hs), 1.0, side_of(1, neutral)))
        out.append((away, home, float(ascore), 1.0, side_of(0, neutral)))
    return out


def advanced_observations(conn: sqlite3.Connection, season: int, metric: str) -> list[tuple]:
    """Same, from CFBD's per-game advanced table, weighted by offensive plays."""
    if not table_exists(conn, "game_team_advanced"):
        return []
    sql = ADVANCED_SQL.format(metric=metric)
    return [(team, opp, float(value), float(plays), side_of(is_home, neutral))
            for team, opp, value, plays, neutral, is_home in conn.execute(sql, (season,))]


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)
    ).fetchone() is not None


def observations(conn: sqlite3.Connection, season: int, metric: str) -> list[tuple]:
    if metric == "points":
        return point_observations(conn, season)
    return advanced_observations(conn, season, metric)


def solve(obs: list[tuple], lam: float) -> dict:
    """Fit mu, a league home effect, and one offence and defence per team.

    Coordinate descent on the weighted ridge objective

        sum_k w_k (v_k - mu - off_i - def_j - home * side_k)^2
          + lam * sum_i (off_i^2 + def_i^2)

    `mu` is the weighted mean and is not re-fitted: with off and def both
    penalised toward zero it is already the metric's centre, and leaving it
    free lets it trade against the other terms without changing any fitted
    value a reader sees.
    """
    if not obs:
        return {"mu": None, "home": 0.0, "off": {}, "def": {}, "iterations": 0, "observations": 0}
    total_w = sum(w for _, _, _, w, _ in obs)
    mu = sum(v * w for _, _, v, w, _ in obs) / total_w

    off: dict[int, float] = {}
    dfn: dict[int, float] = {}
    for team, opp, _, _, _ in obs:
        off.setdefault(team, 0.0)
        dfn.setdefault(opp, 0.0)
        # Every team that appears on one side is fitted on both, so a team with
        # only home games still has a defence and nothing drops out of a rank.
        off.setdefault(opp, 0.0)
        dfn.setdefault(team, 0.0)

    home = 0.0
    side_ss = sum(w * s * s for _, _, _, w, s in obs)
    iterations = 0
    for iterations in range(1, MAX_ITERATIONS + 1):
        moved = 0.0

        num: dict[int, float] = {}
        den: dict[int, float] = {}
        for team, opp, v, w, s in obs:
            num[team] = num.get(team, 0.0) + w * (v - mu - dfn[opp] - home * s)
            den[team] = den.get(team, 0.0) + w
        for team in off:
            # A team with no observations on this side (it only ever appears as
            # the opponent) has nothing to fit and stays at zero -- with lam = 0
            # the denominator would otherwise be zero.
            weight = den.get(team, 0.0) + lam
            new = num.get(team, 0.0) / weight if weight else 0.0
            moved = max(moved, abs(new - off[team]))
            off[team] = new

        num.clear()
        den.clear()
        for team, opp, v, w, s in obs:
            num[opp] = num.get(opp, 0.0) + w * (v - mu - off[team] - home * s)
            den[opp] = den.get(opp, 0.0) + w
        for team in dfn:
            weight = den.get(team, 0.0) + lam
            new = num.get(team, 0.0) / weight if weight else 0.0
            moved = max(moved, abs(new - dfn[team]))
            dfn[team] = new

        if side_ss > 0:
            new_home = sum(w * s * (v - mu - off[team] - dfn[opp])
                           for team, opp, v, w, s in obs) / side_ss
            moved = max(moved, abs(new_home - home))
            home = new_home

        if moved < TOLERANCE:
            break

    return {"mu": mu, "home": home, "off": off, "def": dfn,
            "iterations": iterations, "observations": len(obs)}


def raw_means(obs: list[tuple]) -> tuple[dict, dict, dict]:
    """Weighted raw mean for each team's offence and defence, and its game count.

    Over the SAME observations the fit used, so the raw and adjusted columns on
    a row always describe the same games.
    """
    off_n, off_d, def_n, def_d, played = {}, {}, {}, {}, {}
    for team, opp, v, w, _ in obs:
        off_n[team] = off_n.get(team, 0.0) + v * w
        off_d[team] = off_d.get(team, 0.0) + w
        def_n[opp] = def_n.get(opp, 0.0) + v * w
        def_d[opp] = def_d.get(opp, 0.0) + w
        played[team] = played.get(team, 0) + 1
        played[opp] = played.get(opp, 0) + 1
    off = {t: off_n[t] / off_d[t] for t in off_n if off_d[t]}
    dfn = {t: def_n[t] / def_d[t] for t in def_n if def_d[t]}
    # Each game gives one observation per side, so a team appears once as the
    # offence and once as the defence in every game it played.
    return off, dfn, {t: n // 2 for t, n in played.items()}


def ranked(values: dict[int, float], best_high: bool) -> dict[int, int]:
    """1 = best. Ties share a rank and skip the next one, as every table here does."""
    order = sorted(values.items(), key=lambda kv: (-kv[1] if best_high else kv[1], kv[0]))
    out, previous, previous_rank = {}, None, 0
    for i, (team, value) in enumerate(order):
        rank = previous_rank if value == previous else i + 1
        out[team] = rank
        previous, previous_rank = value, rank
    return out


def round_or_none(value, digits):
    return None if value is None else round(value, digits)


def build_metric(conn: sqlite3.Connection, season: int, metric: str, digits: int) -> dict | None:
    """One metric's fit for one season, or None when the season has too little of it."""
    obs = observations(conn, season, metric)
    if len(obs) < MIN_OBSERVATIONS:
        return None
    fit = solve(obs, LAMBDA[metric])
    raw_off, raw_def, games = raw_means(obs)

    keep = {t for t, n in games.items() if n >= MIN_TEAM_GAMES}
    if not keep:
        return None
    adj_off = {t: fit["mu"] + fit["off"][t] for t in keep}
    adj_def = {t: fit["mu"] + fit["def"][t] for t in keep}

    rank = {
        "off_raw": ranked({t: raw_off[t] for t in keep}, True),
        "off_adj": ranked(adj_off, True),
        "def_raw": ranked({t: raw_def[t] for t in keep}, False),
        "def_adj": ranked(adj_def, False),
    }
    teams = {}
    for t in keep:
        teams[t] = {
            "games": games[t],
            "off_raw": round_or_none(raw_off[t], digits),
            "off_adj": round_or_none(adj_off[t], digits),
            "def_raw": round_or_none(raw_def[t], digits),
            "def_adj": round_or_none(adj_def[t], digits),
            **{f"{k}_rank": rank[k][t] for k in rank},
        }
    return {"mu": round_or_none(fit["mu"], digits + 2),
            "home": round_or_none(fit["home"], digits + 2),
            "lambda": LAMBDA[metric],
            "observations": fit["observations"],
            "iterations": fit["iterations"],
            "teams": teams,
            "rated": len(keep)}


def unrated_games(conn: sqlite3.Connection, season: int) -> dict[int, int]:
    """Completed games against opponents this project does not rate, per team."""
    if not table_exists(conn, "non_fbs_games"):
        return {}
    return {team: n for team, n in conn.execute(
        "SELECT team_id, COUNT(*) FROM non_fbs_games WHERE season_year = ? GROUP BY team_id",
        (season,))}


def build_season(conn: sqlite3.Connection, season: int) -> dict:
    """The whole payload for one season's adjusted-statistics view."""
    fits = {}
    for key, _label, _unit, digits in METRICS:
        fit = build_metric(conn, season, key, digits)
        if fit is not None:
            fits[key] = fit

    names = {tid: name for tid, name in conn.execute("SELECT team_id, team_name FROM teams")}
    conferences = season_conferences(conn, season)
    unrated = unrated_games(conn, season)

    ids = sorted({t for fit in fits.values() for t in fit["teams"]})
    teams = []
    for tid in ids:
        row = {"id": tid, "name": names.get(tid, f"#{tid}"),
               "conference": conferences.get(tid),
               "unrated": unrated.get(tid, 0)}
        for key, fit in fits.items():
            if tid in fit["teams"]:
                row[key] = fit["teams"][tid]
        teams.append(row)
    teams.sort(key=lambda r: r["name"])

    return {
        "season": season,
        "metrics": [{"key": k, "label": label, "unit": unit, "digits": digits,
                     "mu": fits[k]["mu"], "home": fits[k]["home"],
                     "lambda": fits[k]["lambda"], "rated": fits[k]["rated"],
                     "observations": fits[k]["observations"]}
                    for k, label, unit, digits in METRICS if k in fits],
        "teams": teams,
    }


def season_conferences(conn: sqlite3.Connection, season: int) -> dict[int, str]:
    """Each team's conference in THIS season, the same source every other view reads."""
    if not table_exists(conn, "team_membership_by_season"):
        return {}
    return {tid: conf for tid, conf in conn.execute(
        "SELECT team_id, conference_real FROM team_membership_by_season "
        "WHERE season_year = ? AND conference_real IS NOT NULL", (season,))}


def latest_season(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT MAX(season_year) FROM games").fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default="db/league.db")
    ap.add_argument("--year", type=int)
    ap.add_argument("--json", action="store_true", help="print the whole payload")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    season = args.year or latest_season(conn)
    payload = build_season(conn, season)
    if args.json:
        print(json.dumps(payload, indent=2))
        return
    if not payload["metrics"]:
        print(f"{season}: no metric has enough of this season to fit.")
        return
    print(f"{season}: {len(payload['teams'])} teams")
    for m in payload["metrics"]:
        print(f"  {METRIC_LABELS[m['key']]}: mean {m['mu']}, home effect {m['home']}, "
              f"{m['observations']} observations, {m['rated']} teams")
    key = payload["metrics"][0]["key"]
    movers = sorted((r for r in payload["teams"] if key in r),
                    key=lambda r: r[key]["def_raw_rank"] - r[key]["def_adj_rank"])
    print(f"  defence helped most by its schedule ({METRIC_LABELS[key]}):")
    for r in movers[:5]:
        d = r[key]
        print(f"    {r['name']:<22} raw #{d['def_raw_rank']:<3} -> adjusted #{d['def_adj_rank']}")
    print("  defence hurt most by its schedule:")
    for r in movers[-5:][::-1]:
        d = r[key]
        print(f"    {r['name']:<22} raw #{d['def_raw_rank']:<3} -> adjusted #{d['def_adj_rank']}")


if __name__ == "__main__":
    main()
