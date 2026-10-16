#!/usr/bin/env python3
"""
How hard a team's schedule has been, and how hard the rest of it is.

A team page already carries one strength-of-schedule number: the average Elo
of the opponents it has played, taken at the time it played them. That number
answers "was this record earned?" for one team and cannot be compared to
anybody else's without opening thirteen other pages.

It is also entirely backward-looking, and in October that is the less
interesting half. An undefeated team whose hard games are all still to come is
a different proposition from one that has already played them, and nothing on
this site could tell the two apart.

So this module measures three things per team-season and ranks each across the
league:

  PLAYED     the mean Elo of the opponents already faced, each taken at the
             rating it held entering that game. Identical to the team page's
             own number, deliberately: two pages disagreeing about one team's
             schedule would be worse than neither having it.

  REMAINING  the mean CURRENT Elo of the opponents still to be faced. There is
             no pregame rating for a game nobody has played, so this is the
             best available estimate and it moves as those opponents do.

             "Current" means what predict_upcoming.current_ratings() means:
             the rating every team would carry into its next game, replayed
             over every completed game in the database. NOT the rating a team
             took into the postseason, which is what the playoff selector
             reads -- that one is frozen at the end of the regular season and
             does not exist at all for a team yet to play this year, so a
             preseason table would have no remaining column and a team in a
             bowl would be priced at its November rating.

  FULL       the two pooled by game count, which is the season's schedule as
             it stands today.

Ranks are 1 = toughest, within a season.

------------------------------------------------------------------------
What this is not
------------------------------------------------------------------------
* NOT a rating input. Nothing in the Elo or CoE engines reads any of this; it
  is a description of a schedule, computed from ratings those engines produced.

* NOT comparable across eras without care. Elo is a within-season-chain
  number, so a 1,650 average opponent in 2026 and in 1994 are not the same
  claim about absolute quality. The view ranks within a season for that
  reason.

* NOT a count of games this project does not hold. A game against an unrated
  opponent has no Elo to average, so it is excluded and counted separately. A
  team whose remaining schedule is all unrated has no remaining number rather
  than a flattering one.

  There is a known limit here, and the page states it rather than papering
  over it: `fetch_cfbd_games.py` deliberately leaves an UPCOMING FBS-vs-FCS
  game out of schedule_<year>.csv, because that file feeds the upcoming-game
  cards and there is no rating to predict such a game with. Those games
  therefore never reach scheduled_games and cannot be counted here either. So
  a remaining schedule is the games left against RATED opponents, and a team
  with an FCS game still to come has one more game than this says. The
  remaining_unrated counter is still correct for anything that does reach it;
  it is simply zero on the real archive.

Usage:
    python src/build_schedule_strength.py            # prints the current season
    python src/build_schedule_strength.py --year 2024
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "src" / "coefficients"))

from predict_upcoming import current_ratings, elo_config  # noqa: E402

DEFAULT_DB = REPO / "db" / "league.db"

# The opponents a team has already played, with the rating each held entering
# the game -- the same column the team page averages.
PLAYED_SQL = """
    SELECT e.team_id, g.season_year, e.opponent_pregame_elo
      FROM elo_game_history e
      JOIN games g ON g.game_id = e.game_id
     WHERE e.opponent_pregame_elo IS NOT NULL
"""

# The games still to be played. scheduled_games holds only games that have not
# finished -- load_schedule.py drops one as soon as it has a result -- so this
# needs no date arithmetic and cannot double-count a played game.
SCHEDULED_SQL = """
    SELECT season_year, home_team_id, away_team_id
      FROM scheduled_games
     WHERE home_team_id IS NOT NULL AND away_team_id IS NOT NULL
"""


def scheduled_games(conn: sqlite3.Connection, season: int) -> int:
    """How many games the season still has left, counted once each."""
    if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='scheduled_games'").fetchone():
        return 0
    return conn.execute(
        "SELECT COUNT(*) FROM scheduled_games WHERE season_year = ?", (season,)).fetchone()[0]


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def rank_toughest_first(values: dict[int, float]) -> dict[int, int]:
    """team_id -> rank, 1 = toughest. Equal values share the lower rank.

    Ties are real here: two teams can genuinely face the same average
    opponent, and giving one of them the better rank would invent a difference
    the numbers do not contain.
    """
    ranks: dict[int, int] = {}
    for position, (team_id, value) in enumerate(
            sorted(values.items(), key=lambda kv: (-kv[1], kv[0])), start=1):
        ranks[team_id] = position
    # Collapse ties onto the first position that holds that value.
    first_at: dict[float, int] = {}
    for team_id, value in sorted(values.items(), key=lambda kv: (-kv[1], kv[0])):
        first_at.setdefault(value, ranks[team_id])
        ranks[team_id] = first_at[value]
    return ranks


def played_opponents(conn: sqlite3.Connection) -> dict[tuple[int, int], list[float]]:
    played: dict[tuple[int, int], list[float]] = {}
    for team_id, season, opponent_elo in conn.execute(PLAYED_SQL):
        played.setdefault((int(team_id), int(season)), []).append(float(opponent_elo))
    return played


def live_ratings(conn: sqlite3.Connection) -> dict[int, float]:
    """team_id -> the rating it would carry into its next game, right now.

    This is the whole season's chain replayed, postseason games included, so a
    team still playing is priced at what it is worth today and a team yet to
    play this year keeps last season's regressed rating instead of vanishing.
    """
    ratings, _, _ = current_ratings(conn, elo_config(), latest_season(conn))
    return ratings


def latest_season(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT MAX(season_year) FROM games").fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def remaining_opponents(conn: sqlite3.Connection, season: int,
                        elo: dict[int, float]) -> tuple[dict[int, list[float]], dict[int, int]]:
    """team_id -> the current Elo of each opponent it has left, plus a count of
    the ones with no rating at all.

    A scheduled opponent with no rating cannot contribute a number. Averaging
    it in at the engine's 1500 prior would quietly make such a schedule look
    average when it is the opposite, so it is excluded and counted.
    """
    if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='scheduled_games'").fetchone():
        return {}, {}
    remaining: dict[int, list[float]] = {}
    unrated: dict[int, int] = {}
    for season_year, home_id, away_id in conn.execute(SCHEDULED_SQL):
        if int(season_year) != season:
            continue
        for team_id, opponent_id in ((int(home_id), int(away_id)), (int(away_id), int(home_id))):
            opponent_elo = elo.get(opponent_id)
            if opponent_elo is None:
                unrated[team_id] = unrated.get(team_id, 0) + 1
            else:
                remaining.setdefault(team_id, []).append(float(opponent_elo))
    return remaining, unrated


def build_season(conn: sqlite3.Connection, season: int,
                 played: dict[tuple[int, int], list[float]] | None = None,
                 elo: dict[int, float] | None = None) -> dict:
    """One season's schedule-strength table, teams and conferences.

    `elo` is the live rating map, passed in by a caller measuring several
    seasons so the Elo chain is replayed once rather than once per season. It
    is only read for a season that still has games in it, which is why a
    finished season costs nothing.
    """
    played = played if played is not None else played_opponents(conn)
    if scheduled_games(conn, season):
        elo = live_ratings(conn) if elo is None else elo
    else:
        elo = {}
    remaining, unrated = remaining_opponents(conn, season, elo)
    names = {tid: name for tid, name in conn.execute("SELECT team_id, team_name FROM teams")}
    membership = {}
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='team_membership_by_season'").fetchone():
        membership = {int(tid): conf for tid, conf in conn.execute(
            "SELECT team_id, conference_real FROM team_membership_by_season "
            "WHERE season_year = ? AND conference_real IS NOT NULL", (season,))}

    teams: dict[int, dict] = {}
    for team_id in {tid for (tid, s) in played if s == season} | set(remaining) | set(unrated):
        faced = played.get((team_id, season), [])
        left = remaining.get(team_id, [])
        pooled = faced + left
        teams[team_id] = {
            "team_id": team_id,
            "team": names.get(team_id, f"#{team_id}"),
            "conference": membership.get(team_id),
            "played_games": len(faced),
            "played_sos": _mean(faced),
            "remaining_games": len(left),
            "remaining_sos": _mean(left),
            "remaining_unrated": unrated.get(team_id, 0),
            "full_sos": _mean(pooled),
        }

    for key, rank_key in (("played_sos", "played_rank"),
                          ("remaining_sos", "remaining_rank"),
                          ("full_sos", "full_rank")):
        ranks = rank_toughest_first(
            {tid: t[key] for tid, t in teams.items() if t[key] is not None})
        for team_id, team in teams.items():
            team[rank_key] = ranks.get(team_id)

    conferences: dict[str, dict] = {}
    for team in teams.values():
        conference = team["conference"]
        if not conference:
            continue
        row = conferences.setdefault(conference, {
            "conference": conference, "teams": 0,
            "_played": [], "_remaining": [], "_full": []})
        row["teams"] += 1
        for key, bucket in (("played_sos", "_played"), ("remaining_sos", "_remaining"),
                            ("full_sos", "_full")):
            if team[key] is not None:
                row[bucket].append(team[key])
    conference_rows = []
    for row in conferences.values():
        conference_rows.append({
            "conference": row["conference"], "teams": row["teams"],
            "played_sos": _mean(row["_played"]),
            "remaining_sos": _mean(row["_remaining"]),
            "full_sos": _mean(row["_full"]),
        })
    conference_rows.sort(key=lambda r: (-(r["full_sos"] if r["full_sos"] is not None else -1e9),
                                        r["conference"]))

    ordered = sorted(teams.values(),
                     key=lambda t: (-(t["full_sos"] if t["full_sos"] is not None else -1e9),
                                    t["team"]))
    return {
        "season": season,
        "teams": ordered,
        "conferences": conference_rows,
        # Whether the season still has games in it is what decides if the
        # remaining columns mean anything, so the page is told rather than
        # inferring it from a column of dashes.
        #
        # Each remaining game appears on BOTH teams, so summing the per-team
        # counts gives team-games, not games. Halving it would be wrong too:
        # a game against an unrated opponent counts for one team only. The
        # scheduled table is the only honest source for a game count.
        "remaining_team_games": sum(t["remaining_games"] for t in ordered),
        "remaining_games": scheduled_games(conn, season),
    }


def build_report(conn: sqlite3.Connection, seasons: list[int] | None = None) -> dict | None:
    """Every season this database can measure, newest first.

    None when there is no Elo history at all, so a checkout that has not built
    the ratings yet publishes no section rather than an empty one.
    """
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name='elo_game_history'").fetchone():
        return None
    played = played_opponents(conn)
    if not played:
        return None
    available = seasons if seasons is not None else sorted({s for (_, s) in played}, reverse=True)
    # Replayed once for the whole report, not once per season.
    elo = live_ratings(conn)
    return {"seasons": {str(season): build_season(conn, season, played, elo)
                        for season in available}}


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Measure schedule strength, played and remaining.")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--year", type=int, help="One season; default is the newest.")
    parser.add_argument("--json", action="store_true", help="Print the whole report as JSON.")
    args = parser.parse_args(argv)
    conn = sqlite3.connect(args.db)
    try:
        report = build_report(conn, [args.year] if args.year else None)
        if report is None:
            sys.exit("No Elo history in this database; build the ratings first.")
        if args.json:
            print(json.dumps(report, indent=1))
            return
        season = args.year or max(int(s) for s in report["seasons"])
        table = report["seasons"][str(season)]
        print(f"{season}: {len(table['teams'])} teams, "
              f"{table['remaining_games']} games left to play "
              f"({table['remaining_team_games']} team-games)")
        print(f"{'':>4} {'team':<26} {'played':>7} {'#':>4} {'left':>7} {'#':>4} {'full':>7} {'#':>4}")
        for row in table["teams"][:15]:
            def cell(value):
                return f"{value:7.1f}" if value is not None else f"{'—':>7}"
            print(f"{row['full_rank'] or '':>4} {row['team'][:26]:<26} "
                  f"{cell(row['played_sos'])} {row['played_rank'] or '':>4} "
                  f"{cell(row['remaining_sos'])} {row['remaining_rank'] or '':>4} "
                  f"{cell(row['full_sos'])} {row['full_rank'] or '':>4}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
