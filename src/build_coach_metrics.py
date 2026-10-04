#!/usr/bin/env python3
"""Head-coach metrics measured against the Elo expectation (v0.1.1 phase 5).

A coach page already shows a record. A record is mostly a statement about the
roster and the schedule: 9-3 at Alabama and 9-3 at Kansas are not the same
season's work. What this adds is the comparison the model can already make --
before each game the Elo rating gave that team a win probability, so the sum of
those probabilities is the season a *typical* coach of that team, against that
schedule, would have had. Wins above that is the part of the season that was not
already predicted.

    expected_wins       = sum over the season's games of the team's pregame
                          Elo win probability
    wins_above_expected = wins + ties/2 - expected_wins

ATTRIBUTION IS THE WHOLE PROBLEM, and it is why 361 tenures get no metric here.
CFBD's /coaches feed is one record per coach-season with a season-long games,
wins and losses count. It never says which GAME a coach was on the sideline for.
Where a team had one head coach all season, every game of that team's season is
that coach's and the sum is sound. Where a team had two -- a firing, an interim,
a resignation -- nothing in the feed divides the games between them, and this
project does not guess: both rows are written with attributed = 0 and the reason
named, so the page can say the season is not measured instead of crediting an
interim with a predecessor's wins.

The same stance applies to the identity itself. A coach here is a name-only
identity (CFBD has no coach id), so two head coaches who share a name are one
page and one career total. That is recorded in person_unresolved and said on the
page; it is not fixable from this feed. See docs/identity-problems.md 2.

COVERAGE. The measure uses the games this database holds, which are the games
the ratings were built from -- so expected and actual wins always count the same
set. CFBD's own season record can be larger, because it counts games against
opponents outside this FBS database. Both numbers are kept, and the page labels
which is which rather than quietly preferring one.

Usage:
    python src/build_coach_metrics.py [--db db/league.db]
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SCHEMA = REPO / "sql" / "coach_metric_tables.sql"
OUT_CSV = REPO / "data" / "processed" / "coach_season_metrics.csv"

SHARED_SEASON = "two head coaches this season, and the feed does not say which games"
NO_GAMES = "this database holds no games for that team that season"


def apply_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA.read_text(encoding="utf-8"))


def season_games(conn: sqlite3.Connection) -> dict:
    """{(team_id, season): [game, ...]} in played order, with the Elo view of each.

    One row per team per game, taken from elo_game_history, which is where the
    pregame rating and the expectation it implied were recorded when the ratings
    were built. Reading them back means the expectation is the one the model
    actually used, not one recomputed now from a rating that has since moved.
    """
    rows = conn.execute("""
        SELECT e.team_id, g.season_year, g.game_id, g.week, g.game_date,
               e.pregame_elo, e.postgame_elo, e.elo_expectation,
               g.home_team_id, g.home_score, g.away_score
        FROM elo_game_history e
        JOIN games g USING (game_id)
        WHERE g.home_score IS NOT NULL AND g.away_score IS NOT NULL
        -- Ordered by DATE, not by week. CFBD numbers some postseason games
        -- week 1, so ordering by week put a bowl game at the FRONT of the
        -- season: 1,353 of 5,287 seasons then read an Elo change of exactly
        -- zero, because the first row's pregame rating was the one the season
        -- had ENDED on. The stored chain proves the date order is the real one
        -- -- each game's postgame rating is the next game's pregame.
        ORDER BY g.season_year, e.team_id, g.game_date, g.week, g.game_id
    """).fetchall()
    seasons: dict = {}
    for (team_id, season, game_id, _week, _date, pregame, postgame, expectation,
         home_id, home_score, away_score) in rows:
        ours, theirs = ((home_score, away_score) if team_id == home_id
                        else (away_score, home_score))
        seasons.setdefault((team_id, season), []).append({
            "game_id": game_id,
            "pregame_elo": pregame,
            "postgame_elo": postgame,
            "expectation": expectation,
            "result": "W" if ours > theirs else ("L" if ours < theirs else "T"),
        })
    return seasons


def measure(games: list) -> dict:
    """What a season of games says about the coach who coached all of them."""
    wins = sum(1 for g in games if g["result"] == "W")
    losses = sum(1 for g in games if g["result"] == "L")
    ties = sum(1 for g in games if g["result"] == "T")
    # A game whose expectation was never recorded cannot be counted on either
    # side: dropping it from expected_wins alone would credit the coach with a
    # free win above expectation.
    rated = [g for g in games if g["expectation"] is not None]
    expected = sum(g["expectation"] for g in rated) if rated else None
    credit = wins + ties / 2
    pregame = games[0]["pregame_elo"]
    postgame = games[-1]["postgame_elo"]
    return {
        "games": len(games),
        "wins": wins,
        "losses": losses,
        "ties": ties,
        "expected_wins": expected,
        # Measured over the rated games only, so it is comparable with
        # expected_wins. With every game rated -- which is the normal case --
        # this is the season's own record against the season's own expectation.
        "wins_above_expected": (
            None if expected is None
            else sum(1 for g in rated if g["result"] == "W")
            + sum(1 for g in rated if g["result"] == "T") / 2 - expected),
        "pregame_elo": pregame,
        "postgame_elo": postgame,
        "elo_change": (None if pregame is None or postgame is None
                       else postgame - pregame),
        "_credit": credit,
    }


def build(conn: sqlite3.Connection) -> dict:
    apply_schema(conn)
    conn.execute("DELETE FROM coach_season_metrics")

    seasons = season_games(conn)
    coe2 = {(team_id, season): value for team_id, season, value in conn.execute(
        "SELECT team_id, season_year, season_coe2 FROM team_coe2_by_season")}

    shared: dict = {}
    for team_id, season, count in conn.execute(
            "SELECT team_id, season_year, COUNT(*) FROM coach_tenures "
            "GROUP BY team_id, season_year"):
        shared[(team_id, season)] = count

    written = {"attributed": 0, "shared": 0, "no games": 0}
    for coach_id, team_id, season in conn.execute(
            "SELECT coach_id, team_id, season_year FROM coach_tenures "
            "ORDER BY coach_id, season_year, team_id"):
        games = seasons.get((team_id, season))
        if shared.get((team_id, season), 1) > 1:
            reason, row = SHARED_SEASON, None
            written["shared"] += 1
        elif not games:
            reason, row = NO_GAMES, None
            written["no games"] += 1
        else:
            reason, row = None, measure(games)
            written["attributed"] += 1
        conn.execute(
            "INSERT INTO coach_season_metrics (coach_id, team_id, season_year, "
            "attributed, reason, games, wins, losses, ties, expected_wins, "
            "wins_above_expected, pregame_elo, postgame_elo, elo_change, season_coe2) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (coach_id, team_id, season, 0 if row is None else 1, reason,
             None if row is None else row["games"],
             None if row is None else row["wins"],
             None if row is None else row["losses"],
             None if row is None else row["ties"],
             None if row is None else row["expected_wins"],
             None if row is None else row["wins_above_expected"],
             None if row is None else row["pregame_elo"],
             None if row is None else row["postgame_elo"],
             None if row is None else row["elo_change"],
             None if row is None else coe2.get((team_id, season))))
    conn.commit()
    return written


def write_csv(conn: sqlite3.Connection, path: Path = OUT_CSV) -> int:
    """The same rows as a file, for the same reason every other builder writes
    one: a number on a page should be checkable without opening a database."""
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = conn.execute(
        "SELECT m.coach_id, c.display_name, t.team_name, m.season_year, m.attributed, "
        "m.reason, m.games, m.wins, m.losses, m.ties, m.expected_wins, "
        "m.wins_above_expected, m.elo_change, m.season_coe2 "
        "FROM coach_season_metrics m JOIN coaches c USING (coach_id) "
        "JOIN teams t USING (team_id) ORDER BY c.display_name, m.season_year").fetchall()
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["coach_id", "coach", "team", "season", "attributed", "reason",
                         "games", "wins", "losses", "ties", "expected_wins",
                         "wins_above_expected", "elo_change", "season_coe2"])
        writer.writerows(rows)
    return len(rows)


def main(argv: list | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=str(REPO / "db" / "league.db"))
    args = parser.parse_args(argv)

    conn = sqlite3.connect(args.db)
    # A fresh checkout has no coaching history: sync-people.yml fetches it and
    # it is not required for the ratings. Nothing to measure is not an error.
    if not conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' "
                        "AND name = 'coach_tenures'").fetchone():
        print("no coach_tenures table; nothing to measure")
        conn.close()
        return 0
    counts = build(conn)
    rows = write_csv(conn)
    total = sum(counts.values())
    print(f"coach_season_metrics: {total} tenures -- {counts['attributed']} measured, "
          f"{counts['shared']} shared with another head coach, "
          f"{counts['no games']} with no games here; wrote {rows} rows to {OUT_CSV}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
