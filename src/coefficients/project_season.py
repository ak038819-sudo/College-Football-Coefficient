#!/usr/bin/env python3
"""Rest-of-season projection: what the model thinks the season will END as.

WHAT WAS MISSING

Every playoff surface on this site described a season that had already
happened. select_cfp_field.select_field reads conference standings *to date*
and the ranking *as it stands today*, and simulate_cfp_bracket then runs the
bracket from that already-fixed field. In
week 7 that produced a page stating flatly that Indiana was the Big Ten
champion and giving a title chance to exactly twelve teams and zero to anyone
else, with 432 games still unplayed. Those odds answered "if the playoff
started today, who wins it" -- a question nobody asks in October.

This module answers the question people do ask. It plays the rest of the
season out, many times, and counts how often each team wins its conference,
reaches the field, earns a bye and wins the title.

HOW ONE SIMULATED SEASON IS BUILT

  1. Every unplayed game of the season is decided by a coin weighted with the
     Elo win probability the site already publishes for that game -- the same
     number predict_upcoming.py puts on the game's card, via the same
     game_expectation(). Nothing here invents a second notion of who is
     favoured.
  2. That result then MOVES both ratings, by build_elo's own update, so the
     next simulated game is played by the teams the earlier ones made. A team
     that loses its first three simulated games is a worse team in its fourth.
  3. Conference records are recomputed from the completed results plus the
     simulated ones, and each conference's champion is its leader under the
     standings tiebreak that derive_conference_standings_local.py already
     uses.
  4. The Elo each team carries out of that simulated season, and those
     simulated champions, go into select_field, unchanged, and the resulting
     bracket is played out by simulate_cfp_bracket.simulate_one_bracket,
     unchanged.

So a team's playoff chance moves for the two real reasons it should: it can
win its conference and take an automatic bid, and it can play its way up or
down the ranking the field is seeded from.

WHY ELO AND NOT THE COEFFICIENT

This module first rated each simulated season with the iterative coefficient
and seeded from the 5-year rolling window, because that was the committee
stand-in. It barely moved: four of the five seasons in that window had
already happened, so a 2-2 Oregon was 100% to make the field and the only
real movement on the page was in the conference races. The owner's call
(2026-10-12) replaced the stand-in with Elo, and this projection follows it,
which also makes it a far cheaper thing to run -- a chain of rating updates
instead of fifteen iterations over ~900 games per simulated season.

THE TWO LIMITS, STATED RATHER THAN HIDDEN

  * A SIMULATED GAME HAS NO SCORE, SO IT HAS NO PERFORMANCE MULTIPLIER. The
    live engine scales each update by how far the winner outplayed the
    pregame gap; a coin flip cannot answer that. Each simulated game takes
    the configured layer's own recent mean instead (see
    mean_performance_multiplier), so ratings move by a realistic amount on
    average while no single simulated game is allowed to be a blowout or a
    narrow escape. The spread of simulated end-of-season ratings is therefore
    narrower than a real season's.
  * A PROJECTED SEASON HAS NO POSTSEASON. Bowl and playoff matchups are not
    known, so a projected season carries only the games on the schedule. That
    is also what the ranking wants: the committee ranks teams on the season
    before the postseason, which is exactly what select_cfp_field reads for a
    completed season.
  * THE CHAMPION IS A STANDINGS LEADER, NOT A TITLE-GAME WINNER. That is the
    existing project-wide definition (see select_cfp_field), inherited here on
    purpose rather than reinvented.

Usage:
    python src/coefficients/project_season.py --year 2026 --sims 2000
"""
from __future__ import annotations

import argparse
import csv
import random
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from build_elo import load_config  # noqa: E402
from predict_upcoming import CONFIG_PATH, elo_config, game_expectation  # noqa: E402

from select_cfp_field import (  # noqa: E402
    BYE_SEEDS, INDEPENDENTS, load_team_elo, select_field,
)
from simulate_cfp_bracket import ROUND_ORDER, ROUND_RANK, simulate_one_bracket  # noqa: E402

DATA_DIR = SRC.parent / "data" / "processed"


# ------------------------------------------------------------------- the Elo

# How much a simulated game may move a rating, relative to a real one.
#
# build_elo scales every update by a performance multiplier M, which asks how
# far the winner outplayed what the pregame gap implied. A simulated game has
# a winner and no score, so there is no M to compute -- the question the layer
# answers does not exist here.
#
# M = 1 would be the no-information answer, and it would be wrong in a
# measurable direction: the configured layer's M averages about 1.7 on real
# games, so every simulated rating would move 40% less than a real result
# moves it, and a projected season would be too close to where it started.
# So the layer's own mean is used, measured from the stored chain rather than
# written down here, and MEAN_MULTIPLIER_FALLBACK only covers a database with
# no history to measure.
MEAN_MULTIPLIER_FALLBACK = 1.0

# Seasons to measure that mean over. The multiplier's scale belongs to the
# configured performance layer (xSRDiff's M averages 1.66 where the older MOV
# layer's averaged 2.43), so the window is recent rather than all of history.
MULTIPLIER_SEASONS = 5


def mean_performance_multiplier(conn: sqlite3.Connection, year: int,
                                seasons: int = MULTIPLIER_SEASONS) -> float:
    """The average M real games took recently, as the stand-in for a simulated one.

    Restricted to rows the CONFIGURED layer actually scored. A row that fell
    back to margin of victory carries a multiplier on another scale entirely,
    and averaging the two would describe neither.
    """
    row = conn.execute(
        """SELECT AVG(e.mov_multiplier)
           FROM elo_game_history e JOIN games g ON g.game_id = e.game_id
           WHERE g.season_year > ? AND g.season_year <= ?
             AND e.performance_model = ? AND e.mov_multiplier IS NOT NULL""",
        (year - seasons, year, load_config(str(CONFIG_PATH))["performance"]["modifier"]),
    ).fetchone()
    return float(row[0]) if row and row[0] else MEAN_MULTIPLIER_FALLBACK


def play_game(ratings: dict, home: int, away: int, neutral: bool, cfg: dict,
              k_times_m: float, rng: random.Random) -> bool:
    """Decide one simulated game and move both ratings by it. True if home won.

    The probability is predict_upcoming.game_expectation -- the number this
    site publishes on the game's card -- and the update is build_elo's own
    `delta = k * (actual - expected) * M`, applied to the home side and
    subtracted from the away side, which is how run_elo keeps a game zero-sum.

    So a team that loses three simulated games in a row is a worse team in its
    fourth. That is the point of doing it this way: until the committee
    stand-in became Elo, this projection held every rating at today's value,
    and a collapse could not show up in the field at all.
    """
    r_home = ratings.get(home, cfg["initial_rating"])
    r_away = ratings.get(away, cfg["initial_rating"])
    expected_home = game_expectation(r_home, r_away, neutral, cfg)
    home_won = rng.random() < expected_home
    delta = k_times_m * ((1.0 if home_won else 0.0) - expected_home)
    ratings[home] = r_home + delta
    ratings[away] = r_away - delta
    return home_won


# ------------------------------------------------------------- the schedule

def completed_results(conn: sqlite3.Connection, year: int):
    """What the season has already banked: (games_played, conf_records).

    A tie is dropped from nothing here -- it counts in the record exactly as
    compute_conference_team_records counts it. The ratings a simulated season
    starts from come from the Elo chain instead, so this reads results only
    for the record line and the games-played count.
    """
    rows = conn.execute(
        """
        SELECT g.home_team_id, g.away_team_id, g.home_score, g.away_score,
               g.home_conference, g.away_conference
        FROM v_games_enriched g
        WHERE g.season_year = ?
        """,
        (year,),
    ).fetchall()
    played = defaultdict(int)
    record = defaultdict(lambda: [0, 0, 0, 0, 0, 0, 0, 0])  # see RECORD_FIELDS
    for (hid, aid, hs, as_, hconf, aconf) in rows:
        if hs is None or as_ is None:
            continue
        played[hid] += 1
        played[aid] += 1
        bank_game(record, hid, aid, hconf, aconf,
                  0 if hs == as_ else (1 if hs > as_ else -1))
    return played, record


# A banked record: conference W, L, T, games, then overall W, L, T, games.
RECORD_FIELDS = 8


def bank_game(record, home_id, away_id, home_conf, away_conf, outcome: int) -> None:
    """Credit one decided game to both teams' records.

    `outcome` is 1 for a home win, -1 for an away win, 0 for a tie. Mirrors
    compute_conference_team_records.compute_records: a team with no conference
    that season has no record at all, and a game counts toward the conference
    record only when both sides are in the SAME conference.
    """
    conf_game = home_conf is not None and away_conf is not None and home_conf == away_conf
    for tid, conf, res in ((home_id, home_conf, outcome), (away_id, away_conf, -outcome)):
        if conf is None:
            continue
        r = record[(tid, conf)]
        if conf_game:
            r[0 if res > 0 else 1 if res < 0 else 2] += 1
            r[3] += 1
        r[4 if res > 0 else 5 if res < 0 else 6] += 1
        r[7] += 1


def remaining_games(conn: sqlite3.Connection, year: int):
    """The season's unplayed games, in the order they will be played.

    One row per game: (home_id, away_id, home_name, away_name, home_conf,
    away_conf, neutral). No probability: a game's odds depend on the ratings
    the simulated season has reached by the time it is played, so they are
    computed inside each run rather than once here. The ORDER therefore
    matters now, which is why it is the kickoff order and not an arbitrary one.
    """
    if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='scheduled_games'").fetchone():
        return []
    conference = {tid: conf for tid, conf in conn.execute(
        """SELECT team_id, conference_real FROM team_membership_by_season
           WHERE season_year = ? AND conference_real IS NOT NULL""", (year,))}
    names = dict(conn.execute("SELECT team_id, team_name FROM teams"))
    out = []
    for _gid, home, away, neutral in conn.execute(
            """SELECT game_id, home_team_id, away_team_id, neutral_site
               FROM scheduled_games
               WHERE season_year = ? AND game_id NOT IN (SELECT game_id FROM games)
               ORDER BY kickoff_utc IS NULL, kickoff_utc, week, game_id""", (year,)):
        if home not in names or away not in names:
            continue
        out.append((home, away, names[home], names[away],
                    conference.get(home), conference.get(away), bool(neutral)))
    return out


# ------------------------------------------------------------- the standings

def champions_from_records(record, names: dict) -> dict[str, str]:
    """conference -> its leader's team name, under the project's standings tiebreak.

    derive_conference_standings_local.derive_standings' own six keys, in its
    own order -- and the sixth is the team NAME, not its id. Two teams can
    reach the last key genuinely level (2021 Big 12: Oklahoma State and Baylor
    both 8-2 in conference, both 11-2 overall, identical on all five
    numbers), and there the alphabet decides. Ranking those by id instead
    picks a different champion, silently and only sometimes.

    Independents are skipped, because an independent cannot win a conference
    and so cannot hold an automatic bid.
    """
    by_conf = defaultdict(list)
    for (tid, conf), r in record.items():
        if conf == INDEPENDENTS or tid not in names:
            continue
        conf_pct = (r[0] + 0.5 * r[2]) / r[3] if r[3] else 0.0
        overall_pct = (r[4] + 0.5 * r[6]) / r[7] if r[7] else 0.0
        by_conf[conf].append((-conf_pct, -r[0], r[1], -overall_pct, -r[4], names[tid]))
    return {conf: min(rows)[-1] for conf, rows in by_conf.items() if rows}


# ------------------------------------------------------------ the projection

TALLIES = ("conference_title", "playoff", "bye", *ROUND_ORDER)


def project(db_path: str, year: int, n_sims: int, sim_seed: int = 0,
            cfg: dict | None = None, progress=None) -> dict:
    """Play the rest of `year` out `n_sims` times and count what happened.

    Returns {"n_sims", "counts", "wins", "elo", "unplayed", "games_played"}:
    counts[team] is a tally per TALLIES entry, wins[team] is total projected
    wins summed over runs and elo[team] is total projected end-of-season Elo,
    so the caller can report a mean of either.
    """
    cfg = cfg or elo_config()
    conn = sqlite3.connect(db_path)
    try:
        _played, banked = completed_results(conn, year)
        schedule = remaining_games(conn, year)
        names = dict(conn.execute("SELECT team_id, team_name FROM teams"))
        # Where every simulated season starts, and the same thing a completed
        # season is SELECTED by: each team's rating after its latest played
        # regular-season game of this year. For the season being played that is
        # simply its current rating.
        #
        # A team with no game yet this season is absent and starts at the
        # engine's prior. That is a guard rather than a path: every FBS team
        # has played by the second week of a season, and a projection of a
        # season nobody has started is not a thing this site publishes.
        start_elo = load_team_elo(conn, year, by_id=True)
        k_times_m = cfg["k"] * mean_performance_multiplier(conn, year)
    finally:
        conn.close()

    rng = random.Random(sim_seed)
    counts = defaultdict(lambda: dict.fromkeys(TALLIES, 0))
    wins = defaultdict(float)
    elo_total = defaultdict(float)
    banked_wins = {names[tid]: r[4] for (tid, _conf), r in banked.items() if tid in names}

    for i in range(n_sims):
        record = {k: list(v) for k, v in banked.items()}
        record = defaultdict(lambda: [0] * RECORD_FIELDS, record)
        season_wins = dict(banked_wins)
        ratings = dict(start_elo)

        for home_id, away_id, home, away, hconf, aconf, neutral in schedule:
            home_won = play_game(ratings, home_id, away_id, neutral, cfg, k_times_m, rng)
            season_wins[home if home_won else away] = \
                season_wins.get(home if home_won else away, 0) + 1
            bank_game(record, home_id, away_id, hconf, aconf, 1 if home_won else -1)

        conf_of = by_conference(record, names)
        # The ranking, by name, over the at-large pool this simulated season
        # has: the Elo each team would carry into the postseason of it.
        ranking = {name: ratings.get(tid, cfg["initial_rating"])
                   for tid, name in names.items() if name in conf_of}
        champions = champions_from_records(record, names)
        field = select_field_from(champions, ranking, conf_of)

        for team, n in season_wins.items():
            wins[team] += n
        for team, rating in ranking.items():
            elo_total[team] += rating
        for q in field:
            counts[q["team"]]["playoff"] += 1
            if q["seed"] <= BYE_SEEDS:
                counts[q["team"]]["bye"] += 1
        for team in champions.values():
            counts[team]["conference_title"] += 1
        for team, furthest in simulate_one_bracket(field, ranking, cfg, rng).items():
            for r in ROUND_ORDER:
                if ROUND_RANK[r] <= ROUND_RANK[furthest]:
                    counts[team][r] += 1
        if progress and (i + 1) % progress == 0:
            print(f"  {i + 1:,}/{n_sims:,} seasons", file=sys.stderr, flush=True)

    return {"n_sims": n_sims, "counts": dict(counts), "wins": dict(wins),
            "elo": dict(elo_total), "unplayed": len(schedule),
            "games_played": {names[tid]: r[7] for (tid, _c), r in banked.items() if tid in names}}


def by_conference(record, names) -> dict[str, str]:
    """team_name -> conference, for the at-large pool, from the records banked.

    select_cfp_field reads this from membership, which is the right source for
    a real field. Here the records already carry each team's conference for the
    season and are the same thing by construction, so the pool and the
    standings cannot disagree.
    """
    return {names[tid]: conf for (tid, conf) in record if tid in names}


def select_field_from(champions, team_coe, conf_of) -> list[dict]:
    """select_cfp_field.select_field, over a simulated season.

    select_field takes a connection because its two inputs live in the
    database. Here they are simulated, so they are passed directly to the same
    function through a stand-in that answers those two queries.
    """
    return select_field(_SimulatedSeason(champions, conf_of), 0, team_coe)


class _SimulatedSeason:
    """The two reads select_field makes, answered from a simulated season.

    Deliberately a stand-in for a connection rather than a copy of
    select_field's body: the selection rules -- five champion bids, seven
    at-large, straight seeding -- must not exist twice, or a change to the
    format would have to be made in two places and would silently not be.
    """

    def __init__(self, champions: dict[str, str], conf_of: dict[str, str]):
        self._champions = [(conf, team) for conf, team in champions.items()]
        self._conf_of = [(team, conf) for team, conf in conf_of.items()]

    def execute(self, sql, _params=()):
        if "conference_standings_by_year" in sql:
            return _Rows(self._champions)
        if "team_membership_by_season" in sql:
            return _Rows(self._conf_of)
        raise AssertionError(f"_SimulatedSeason was asked an unexpected query: {sql}")


class _Rows(list):
    """A cursor's worth of rows. select_field's helpers iterate and fetchall()."""

    def fetchall(self):
        return list(self)


def pct_digits(n_sims: int) -> int:
    """Decimals enough that ONE occurrence in n_sims runs is not a zero.

    A team that reaches the field in 3 of 10,000 simulated seasons has a
    0.03% chance. Rounded to the one decimal a page prints, that is 0.0 --
    which reads as "cannot happen" and, worse, is falsy, so the export's own
    "something still to play for" filter dropped the team from the payload
    altogether. The page has a "<0.1%" case for exactly this team, and it was
    unreachable.

    So the published number keeps whatever precision the simulation can
    actually distinguish. Rounding for display stays in the page, which is
    where a reader's decimal belongs.
    """
    smallest = 100 / max(1, n_sims)
    digits = 1
    while digits < 6 and round(smallest, digits) == 0:
        digits += 1
    return digits


def rows_for_report(projection: dict) -> list[dict]:
    """One row per team, strongest title chance first."""
    n = projection["n_sims"]
    d = pct_digits(n)
    rows = []
    for team, c in projection["counts"].items():
        rows.append({
            "team": team,
            "conference_title_pct": round(100 * c["conference_title"] / n, d),
            "playoff_pct": round(100 * c["playoff"] / n, d),
            "bye_pct": round(100 * c["bye"] / n, d),
            "semifinal_pct": round(100 * c["sf"] / n, d),
            "final_pct": round(100 * c["final"] / n, d),
            "champion_pct": round(100 * c["champion"] / n, d),
            "projected_wins": round(projection["wins"].get(team, 0.0) / n, 1),
            # The mean rating a team ends the season on, which is what decides
            # the field. Rounded to a whole number because that is how this
            # site displays an Elo rating everywhere else.
            "projected_elo": round(projection["elo"].get(team, 0.0) / n),
        })
    rows.sort(key=lambda r: (-r["champion_pct"], -r["playoff_pct"], r["team"]))
    return rows


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--sims", type=int, default=10000)
    p.add_argument("--sim-seed", type=int, default=0)
    p.add_argument("--top", type=int, default=25)
    args = p.parse_args()

    pr = project(args.db, args.year, args.sims, args.sim_seed,
                 progress=max(1, args.sims // 10))
    if not pr["unplayed"]:
        raise SystemExit(f"{args.year} has no unplayed games -- nothing to project")

    print(f"\n=== {args.year} rest of season, {pr['n_sims']:,} simulated seasons "
          f"({pr['unplayed']:,} games to play) ===\n")
    print(f"{'Team':<24} {'Rec':>6} {'Wins':>5} {'Elo':>5} {'Conf':>8} {'Playoff':>8} "
          f"{'Bye':>7} {'Title':>7}")
    for row in rows_for_report(pr)[:args.top]:
        print(f"{row['team']:<24} {'':>6} {row['projected_wins']:>5.1f} "
              f"{row['projected_elo']:>5} {row['conference_title_pct']:>7.1f}% "
              f"{row['playoff_pct']:>7.1f}% {row['bye_pct']:>6.1f}% "
              f"{row['champion_pct']:>6.1f}%")


if __name__ == "__main__":
    main()
