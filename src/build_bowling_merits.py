#!/usr/bin/env python3
"""
Bowling Merits per game, for display beside the production Merits column.

Bowling Merits is a RESEARCH model. It lives in analysis/bowling_merits/ and
nothing here promotes it: this builder reads the canonical database, runs that
research engine unchanged, and writes the per-game awards to a table of their
own. No rating, selector or production Merits figure reads that table. The one
consumer is the Schedule tab's display column.

Why a table rather than a shadow database: the research pipeline's shadow DB is
built by a dispatch-only workflow and uploaded as a CI artifact, so it never
reaches the deploy that builds the site. The engine's award math needs nothing
but completed FBS-vs-FBS games, which the canonical database already holds, so
the site can show the model without the shadow database existing at all.

How the award differs from production Merits, which matters for rendering:

  * A regulation loss earns a NEGATIVE award (-loss_penalty * (1 - q)), where
    production Merits awards a flat 0. A Bowling Merits 0.000 is therefore not
    an outcome any result produces, and the column carries minus signs.
  * An overtime loss earns a flat 0.5.
  * A TIE is not scored at all. The research engine rejects a tied game
    outright, so ties are filtered out before it runs and reach the page as
    absent, exactly as a tie does in the production column.
  * Only rated games count. A game against an opponent this project holds no
    rating for lives in non_fbs_games, which this builder never reads, so such
    a game has no award -- the same five columns it already dashes.
  * `mature` is false while a later game can still move the award: q is drawn
    from the opponent's neighbouring games, so a team's most recent results
    keep changing until the opponent has played `future` more.

Unlike the production award there is no bootstrap window, because q comes from
the season in hand and not from prior seasons: every eligible season is scored,
1980 included.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ENGINE_DIR = REPO_ROOT / "analysis" / "bowling_merits"

TABLE = "bowling_merits_game_awards"

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    game_id     INTEGER NOT NULL,
    team_id     INTEGER NOT NULL,
    season_year INTEGER NOT NULL,
    merits      REAL    NOT NULL,
    opponent_quality REAL NOT NULL,
    mature      INTEGER NOT NULL,
    PRIMARY KEY (game_id, team_id)
);
CREATE INDEX IF NOT EXISTS idx_{TABLE}_team_season
    ON {TABLE} (team_id, season_year);
"""

# Completed, non-tied games. The research adapter in
# analysis/bowling_merits/canonical_adapter.py additionally joins
# team_membership_by_season to keep the set FBS-vs-FBS, and that join is
# deliberately NOT repeated here: `games` is already the rated set, with games
# against unrated opponents held in non_fbs_games instead, so the join drops
# nothing where membership data exists (measured: 0 of 9,085 games 2014-2025)
# and drops EVERYTHING where it does not. The snapshots only cover 2014-2025,
# so repeating the join would silently score no season before 2014 and -- the
# reason it was caught -- none of 2026 either. tests/test_bowling_merits.py
# pins the equivalence so the shortcut cannot rot into a real difference.
#
# Ties are excluded in SQL rather than in Python so that the count this builder
# reports is the count it scored.
GAMES_SQL = """
SELECT g.game_id, g.season_year, g.game_date,
       g.home_team_id, g.away_team_id,
       g.home_score, g.away_score, g.went_ot
FROM games g
WHERE g.home_score IS NOT NULL AND g.away_score IS NOT NULL
  AND g.home_score <> g.away_score
  AND g.game_date IS NOT NULL
  AND g.went_ot IN (0, 1)
ORDER BY g.season_year, g.game_date, g.game_id
"""

REQUIRED_TABLES = {"games"}


def _engine():
    """Import the research engine by path, without leaving it on sys.path.

    analysis/bowling_merits/ is a flat directory whose modules import each other
    by bare name, so it has to be importable as a directory. Putting it on
    sys.path permanently would let a research module shadow a src/ one of the
    same name (engine.py, pipeline.py and incremental.py are all plausible
    collisions), so the entry is removed again as soon as the import is done.
    """
    path = str(ENGINE_DIR)
    added = path not in sys.path
    if added:
        sys.path.insert(0, path)
    try:
        import engine  # noqa: PLC0415  (deliberately deferred; see docstring)
        return engine
    finally:
        if added and path in sys.path:
            sys.path.remove(path)


def load_eligible_games(conn: sqlite3.Connection) -> list[dict]:
    """Canonical games in the research engine's own shape, keyed by team id.

    The engine treats `home`/`away` as opaque keys, so team ids go in directly
    and the awards come back already joinable -- no name round trip, and so no
    chance of a rename splitting one program's season in two.
    """
    have = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    missing = REQUIRED_TABLES - have
    if missing:
        raise RuntimeError(f"Missing canonical tables: {sorted(missing)}")
    return [
        dict(id=str(gid), season=int(season), date=date,
             home=str(home), away=str(away),
             hs=int(hs), as_=int(as_), ot=bool(ot))
        for gid, season, date, home, away, hs, as_, ot in conn.execute(GAMES_SQL)
    ]


def build_awards(games: list[dict]) -> list[tuple]:
    """One award row per team per eligible game, for every season at once.

    The engine keys everything by (season, team), so seasons never bleed into
    one another and a single call is equivalent to one call per season.
    """
    engine = _engine()
    if not games:
        return []
    ledger = engine.calculate(games, engine.Rules(1.5, 0.5, 3))
    return [(int(row["id"]), int(row["team"]), int(row["season"]),
             round(row["merits"], 6), round(row["q"], 6), int(bool(row["mature"])))
            for row in ledger]


def write_awards(conn: sqlite3.Connection, rows: list[tuple]) -> None:
    """Replace the table's contents in one transaction.

    A full replace, not an upsert: a rescored season must not leave last run's
    awards behind for games that are no longer eligible, and an award is cheap
    to recompute from scratch.
    """
    conn.executescript(SCHEMA)
    with conn:
        conn.execute(f"DELETE FROM {TABLE}")
        conn.executemany(
            f"INSERT INTO {TABLE} "
            "(game_id, team_id, season_year, merits, opponent_quality, mature) "
            "VALUES (?, ?, ?, ?, ?, ?)", rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="db/league.db")
    args = parser.parse_args(argv)

    path = Path(args.db)
    if not path.is_file():
        print(f"No database at {path}; nothing to build.")
        return 1
    conn = sqlite3.connect(path)
    try:
        games = load_eligible_games(conn)
        rows = build_awards(games)
        write_awards(conn, rows)
    finally:
        conn.close()

    seasons = {r[2] for r in rows}
    provisional = sum(1 for r in rows if not r[5])
    print(f"Bowling Merits: {len(rows)} awards over {len(games)} games, "
          f"{min(seasons, default='-')}-{max(seasons, default='-')}, "
          f"{provisional} still provisional.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
