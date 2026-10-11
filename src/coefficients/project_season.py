#!/usr/bin/env python3
"""Rest-of-season projection: what the model thinks the season will END as.

WHAT WAS MISSING

Every playoff surface on this site described a season that had already
happened. select_cfp_field.select_field reads conference standings *to date*
and the 5-year rolling coefficient *as it stands today*, and
simulate_cfp_bracket then runs the bracket from that already-fixed field. In
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
  2. The season's completed results plus those simulated ones go back through
     the iterative rating -- the SAME arithmetic build_coefficients.py runs --
     so the season's own coefficient reflects the simulated results, and the
     5-year rolling coefficient is rebuilt on top of the four fixed prior
     seasons.
  3. Conference records are recomputed from the same combined results, and
     each conference's champion is its leader under the standings tiebreak
     that derive_conference_standings_local.py already uses.
  4. That simulated ranking and those simulated champions go into
     select_field, unchanged, and the resulting bracket is played out by
     simulate_cfp_bracket.simulate_one_bracket, unchanged.

So a team's playoff chance moves for the two real reasons it should: it can
win its conference and take an automatic bid, and it can play its way up or
down the ranking the field is seeded from.

THE THREE LIMITS, STATED RATHER THAN HIDDEN

  * ELO IS HELD AT TODAY'S VALUES. A simulated game does not feed back into
    the Elo ratings that decide later simulated games, so a team that loses
    its next five games is still favoured by the same amount in game six. This
    narrows the spread of outcomes: genuine collapses and genuine surges are
    both underrepresented. The coefficient side of the model does respond to
    simulated results; the game-by-game probabilities do not.
  * A PROJECTED SEASON HAS NO POSTSEASON. Bowl and playoff matchups are not
    known, so a projected season carries only the games on the schedule.
    Completed seasons in the historical record do include them, weighted 2x
    and 3x, so a projected coefficient is not perfectly comparable with a
    finished one. Within a single projection every team is treated the same
    way, which is what the ranking needs.
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

from build_coefficients import (  # noqa: E402
    CONFIDENCE_GAMES, ITERATIONS, ROLLING_YEARS, compute_prior_ratings,
    phase_weight, within_window_weight,
)
from predict_upcoming import current_ratings, elo_config, game_expectation  # noqa: E402

from select_cfp_field import INDEPENDENTS, select_field  # noqa: E402
from simulate_bracket import DEFAULT_HOME_FIELD, DEFAULT_TEMPERATURE  # noqa: E402
from simulate_cfp_bracket import ROUND_ORDER, ROUND_RANK, simulate_one_bracket  # noqa: E402

DATA_DIR = SRC.parent / "data" / "processed"

# build_coefficients.per_season_iterative_ratings' own loss credit. Named here
# because the fast path below reproduces that arithmetic and a test holds the
# two to identical output; see iterative_ratings().
LOSS_PENALTY = 0.15

# The prior-regression default build_coefficients' CLI uses. A projected season
# has to be rated the way the published season was rated, so this follows that
# default rather than choosing its own.
PRIOR_REGRESSION = 0.5


# ---------------------------------------------------------------- the rating

def iterative_ratings(results, teams, prior, confidence, iterations=ITERATIONS):
    """build_coefficients.per_season_iterative_ratings, over decided results.

    Same arithmetic, different input shape. The real builder reads rows with
    scores and works out the winner itself; a projection has already decided
    every game, so results arrive as (winner, loser, phase_weight) and the
    score comparison has nothing left to do. Taking that shape is what makes
    this affordable to run thousands of times -- the alternative is rebuilding
    a list of ~900 row objects per simulation.

    Duplicated arithmetic is a liability, so tests/test_project_season.py
    drives both functions over a real season's games and requires identical
    output. If the builder's model changes and this is not changed with it,
    that test fails.
    """
    ratings = {t: 1.0 for t in teams}
    n_teams = len(teams)
    if not n_teams:
        return {}
    for _ in range(iterations):
        new_scores = defaultdict(float)
        for winner, loser, w in results:
            new_scores[winner] += ratings[loser] * w
            new_scores[loser] += ratings[winner] * w * LOSS_PENALTY
        total = sum(new_scores.values())
        if total <= 0:
            break
        scale = n_teams / total
        for t in teams:
            raw_new = new_scores[t] * scale
            c = confidence.get(t, 0.0)
            p = prior.get(t, raw_new)
            ratings[t] = c * raw_new + (1 - c) * p
    return ratings


def season_rating(by_season: dict[int, dict[str, float]], year: int, results, teams,
                  confidence) -> dict[str, float]:
    """One season's rating, priored off the season before it.

    The only place PRIOR_REGRESSION is applied, so a test can pin it by
    reproducing a published season rather than by agreeing with itself: a test
    that computes the prior with the same constant the code uses passes at any
    value of it.
    """
    prior = compute_prior_ratings(by_season.get(year - 1, {}), PRIOR_REGRESSION)
    return iterative_ratings(results, teams, prior, confidence)


def load_season_ratings(path: Path | None = None) -> dict[int, dict[str, float]]:
    """season_year -> {team_name: that season's blended rating}.

    data/processed/team_ratings_by_season.csv, which build_coefficients wrote.
    The four seasons before the one being projected are fixed history and are
    read from here rather than recomputed.
    """
    out: dict[int, dict[str, float]] = defaultdict(dict)
    with open(path or (DATA_DIR / "team_ratings_by_season.csv"), newline="") as f:
        for r in csv.DictReader(f):
            out[int(r["season_year"])][r["team_name"]] = float(r["rating"])
    return dict(out)


def rolling_coe(by_season: dict[int, dict[str, float]], end_year: int,
                this_season: dict[str, float]) -> dict[str, float]:
    """The 5-year rolling coefficient for `end_year`, with a simulated final season.

    build_coefficients.write_rolling_5yr's own sum and decay, with `end_year`'s
    season rating supplied rather than read, because that is the one season a
    projection changes.
    """
    window = range(end_year - (ROLLING_YEARS - 1), end_year + 1)
    teams: set[str] = set(this_season)
    for y in window:
        teams |= set(by_season.get(y, {}))
    out = {}
    for team in teams:
        coeff = 0.0
        for y in window:
            season = this_season if y == end_year else by_season.get(y, {})
            coeff += season.get(team, 0.0) * within_window_weight(end_year, y)
        out[team] = coeff
    return out


# ------------------------------------------------------------- the schedule

def completed_results(conn: sqlite3.Connection, year: int):
    """((winner, loser, weight), ...) for the season's decided games, plus context.

    Returns (results, teams, games_played, conf_records), where conf_records is
    the running record every team has already banked. Ties are dropped from
    `results` exactly as the rating builder drops them, but still count in the
    record, exactly as compute_conference_team_records counts them.
    """
    rows = conn.execute(
        """
        SELECT g.home_team_id, g.away_team_id, ht.team_name AS home_team,
               at.team_name AS away_team, g.home_score, g.away_score,
               g.game_phase, g.home_conference, g.away_conference
        FROM v_games_enriched g
        JOIN teams ht ON ht.team_id = g.home_team_id
        JOIN teams at ON at.team_id = g.away_team_id
        WHERE g.season_year = ?
        """,
        (year,),
    ).fetchall()
    results, teams, played = [], set(), defaultdict(int)
    record = defaultdict(lambda: [0, 0, 0, 0, 0, 0, 0, 0])  # see RECORD_FIELDS
    for (hid, aid, home, away, hs, as_, phase, hconf, aconf) in rows:
        teams.add(home)
        teams.add(away)
        if hs is None or as_ is None:
            continue
        played[home] += 1
        played[away] += 1
        if hs != as_:
            winner, loser = (home, away) if hs > as_ else (away, home)
            results.append((winner, loser, phase_weight(phase)))
        bank_game(record, hid, aid, hconf, aconf,
                  0 if hs == as_ else (1 if hs > as_ else -1))
    return results, teams, played, record


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


def remaining_games(conn: sqlite3.Connection, year: int, cfg: dict):
    """The season's unplayed games, each with the probability the site publishes.

    One row per game: (home_id, away_id, home_name, away_name, home_conf,
    away_conf, p_home, weight). Nothing is simulated here -- this is the
    schedule and the odds, built once.
    """
    if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='scheduled_games'").fetchone():
        return []
    ratings, _, _ = current_ratings(conn, cfg, year)
    conference = {tid: conf for tid, conf in conn.execute(
        """SELECT team_id, conference_real FROM team_membership_by_season
           WHERE season_year = ? AND conference_real IS NOT NULL""", (year,))}
    names = dict(conn.execute("SELECT team_id, team_name FROM teams"))
    out = []
    for gid, home, away, neutral, phase in conn.execute(
            """SELECT game_id, home_team_id, away_team_id, neutral_site, game_phase
               FROM scheduled_games
               WHERE season_year = ? AND game_id NOT IN (SELECT game_id FROM games)
               ORDER BY kickoff_utc IS NULL, kickoff_utc, week, game_id""", (year,)):
        if home not in names or away not in names:
            continue
        r_home = ratings.get(home, cfg["initial_rating"])
        r_away = ratings.get(away, cfg["initial_rating"])
        out.append((home, away, names[home], names[away],
                    conference.get(home), conference.get(away),
                    game_expectation(r_home, r_away, neutral, cfg), phase_weight(phase)))
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


def project(db_path: str, year: int, n_sims: int, temperature: float = DEFAULT_TEMPERATURE,
            home_field: float = DEFAULT_HOME_FIELD, sim_seed: int = 0,
            season_ratings: dict[int, dict[str, float]] | None = None,
            progress=None) -> dict:
    """Play the rest of `year` out `n_sims` times and count what happened.

    Returns {"n_sims", "counts", "wins", "unplayed", "teams"}: counts[team] is
    a tally per TALLIES entry, and wins[team] is total projected wins summed
    over runs, so the caller can report a mean.
    """
    conn = sqlite3.connect(db_path)
    try:
        cfg = elo_config()
        fixed, teams, played, banked = completed_results(conn, year)
        schedule = remaining_games(conn, year, cfg)
        names = dict(conn.execute("SELECT team_id, team_name FROM teams"))
        by_season = season_ratings if season_ratings is not None else load_season_ratings()
    finally:
        conn.close()

    for (_h, _a, home, away, *_rest) in schedule:
        teams.add(home)
        teams.add(away)
    teams = frozenset(teams)

    # A projected season is a FINISHED season, so every scheduled game counts
    # toward the confidence a team's rating is given -- the damping in
    # build_coefficients exists for a half-played season, and a projection has
    # played the other half.
    total_played = dict(played)
    for (_h, _a, home, away, *_rest) in schedule:
        total_played[home] = total_played.get(home, 0) + 1
        total_played[away] = total_played.get(away, 0) + 1
    confidence = {t: min(1.0, n / CONFIDENCE_GAMES) if CONFIDENCE_GAMES > 0 else 1.0
                  for t, n in total_played.items()}

    rng = random.Random(sim_seed)
    counts = defaultdict(lambda: dict.fromkeys(TALLIES, 0))
    wins = defaultdict(float)
    banked_wins = {names[tid]: r[4] for (tid, _conf), r in banked.items() if tid in names}

    for i in range(n_sims):
        results = list(fixed)
        record = {k: list(v) for k, v in banked.items()}
        record = defaultdict(lambda: [0] * RECORD_FIELDS, record)
        season_wins = dict(banked_wins)
        for home_id, away_id, home, away, hconf, aconf, p_home, weight in schedule:
            home_won = rng.random() < p_home
            winner, loser = (home, away) if home_won else (away, home)
            results.append((winner, loser, weight))
            season_wins[winner] = season_wins.get(winner, 0) + 1
            bank_game(record, home_id, away_id, hconf, aconf, 1 if home_won else -1)

        this_season = season_rating(by_season, year, results, teams, confidence)
        team_coe = rolling_coe(by_season, year, this_season)
        champions = champions_from_records(record, names)
        field = select_field_from(champions, team_coe, by_conference(record, names))

        for team, n in season_wins.items():
            wins[team] += n
        for q in field:
            counts[q["team"]]["playoff"] += 1
            if q["seed"] <= 4:
                counts[q["team"]]["bye"] += 1
        for team in champions.values():
            counts[team]["conference_title"] += 1
        for team, furthest in simulate_one_bracket(
                field, team_coe, temperature, rng, home_field).items():
            for r in ROUND_ORDER:
                if ROUND_RANK[r] <= ROUND_RANK[furthest]:
                    counts[team][r] += 1
        if progress and (i + 1) % progress == 0:
            print(f"  {i + 1:,}/{n_sims:,} seasons", file=sys.stderr, flush=True)

    return {"n_sims": n_sims, "counts": dict(counts), "wins": dict(wins),
            "unplayed": len(schedule), "teams": teams,
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
        })
    rows.sort(key=lambda r: (-r["champion_pct"], -r["playoff_pct"], r["team"]))
    return rows


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--sims", type=int, default=10000)
    p.add_argument("--temperature", type=float, default=DEFAULT_TEMPERATURE)
    p.add_argument("--home-field", type=float, default=DEFAULT_HOME_FIELD)
    p.add_argument("--sim-seed", type=int, default=0)
    p.add_argument("--top", type=int, default=25)
    args = p.parse_args()

    pr = project(args.db, args.year, args.sims, args.temperature, args.home_field,
                 args.sim_seed, progress=max(1, args.sims // 10))
    rows = rows_for_report(pr)
    print(f"=== {args.year} rest-of-season projection ({pr['n_sims']:,} simulated seasons, "
          f"{pr['unplayed']:,} games unplayed) ===\n")
    print(f"{'Team':<22} {'Wins':>6} {'Conf':>7} {'Playoff':>8} {'Bye':>7} {'Final':>7} {'Title':>7}")
    for r in rows[:args.top]:
        print(f"{r['team']:<22} {r['projected_wins']:>6.1f} {r['conference_title_pct']:>6.1f}% "
              f"{r['playoff_pct']:>7.1f}% {r['bye_pct']:>6.1f}% {r['final_pct']:>6.1f}% "
              f"{r['champion_pct']:>6.1f}%")


if __name__ == "__main__":
    main()
