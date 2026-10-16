"""
Load data/raw/fcs_schedule_<year>.csv into scheduled_non_fbs_games.

One row per game an FBS team has still to play against an opponent no rating
in this project holds. The FBS side is resolved to a team_id; the other side
stays as the source's own spelling.

This exists to be counted, not predicted. The Schedule Strength page reports
how many games a team has left, and `scheduled_games` deliberately holds only
FBS-vs-FBS fixtures, so a team with two FCS games to come looked like it had
two fewer games than it has. There is still no rating for the opponent, so
nothing here feeds a projection or a win probability.

Which side was FBS, and whether a game has since been played, are decided by
the same two rules load_fcs_games.py uses, and by its code: the source's own
per-game classification, and the game's id in `games`. See its module
docstring for why neither can be read off the team directory.
"""
import argparse
import csv
import sqlite3
import sys
from pathlib import Path

from load_fcs_games import norm, rated_already, sides, team_id, to_bool01
from team_identity import resolve_database_name

DB_PATH = Path("db/league.db")
SCHEMA = Path(__file__).resolve().parent.parent / "sql/non_fbs_game_tables.sql"


def already_played(cur, game_id: int) -> bool:
    """Whether this fixture has a result somewhere, by id rather than by name.

    Two places can hold it. A game the source has reclassified as FBS-vs-FBS
    since the snapshot is in `games`; one played as it stood is in
    non_fbs_games, because load_fcs_games.py runs first. Either way it is not
    still to come, and counting it as such would overstate the schedule it is
    on. In practice the fetcher routes a completed game to the other file and
    the wholesale delete below clears it, so this is the second line of
    defence against a stale schedule file, not the first.
    """
    if rated_already(cur, game_id):
        return True
    return cur.execute(
        "SELECT 1 FROM non_fbs_games WHERE game_id = ?", (game_id,)).fetchone() is not None


def load(conn: sqlite3.Connection, csv_path: Path, year: int) -> dict:
    cur = conn.cursor()
    cur.executescript(SCHEMA.read_text())

    rows = list(csv.DictReader(csv_path.open(encoding="utf-8"))) if csv_path.exists() else []
    loaded, played, ambiguous, neither_known = 0, [], [], []

    # Replaced wholesale, like every other schedule load: a fixture that has
    # since been played, moved or cancelled must disappear from "still to
    # come", and an upsert would leave it there forever.
    cur.execute("DELETE FROM scheduled_non_fbs_games WHERE season_year = ?", (year,))

    for row in rows:
        gid = int(norm(row["game_id"]))
        label = f"{row['away_team']} at {row['home_team']}"
        if already_played(cur, gid):
            played.append(label)
            continue
        which = sides(cur, row, gid)
        if which is None:
            if resolve_database_name(cur, norm(row["home_team"])) \
                    or resolve_database_name(cur, norm(row["away_team"])):
                ambiguous.append(label)
            else:
                neither_known.append(label)
            continue
        fbs, other = which
        kickoff = norm(row.get("kickoff_utc"))
        week = norm(row.get("week"))
        cur.execute(
            """
            INSERT INTO scheduled_non_fbs_games
                (game_id, season_year, week, season_type, kickoff_utc,
                 start_time_tbd, team_id, opponent_name, opponent_conference,
                 opponent_classification, is_home, neutral_site, game_phase,
                 notes, venue)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                gid, year,
                int(week) if week else None,
                norm(row.get("season_type")) or None,
                kickoff or None,
                to_bool01(row.get("start_time_tbd")),
                team_id(cur, norm(row[f"{fbs}_team"])),
                norm(row[f"{other}_team"]),
                norm(row.get(f"{other}_conference")),
                norm(row.get(f"{other}_classification")),
                1 if fbs == "home" else 0,
                to_bool01(row.get("neutral_site")),
                norm(row.get("game_phase")) or "regular",
                norm(row.get("notes")),
                norm(row.get("venue")),
            ),
        )
        loaded += 1

    conn.commit()
    return {"loaded": loaded, "already_played": played,
            "ambiguous": ambiguous, "neither_known": neither_known,
            "read": len(rows)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("year", type=int)
    ap.add_argument("--db", default=str(DB_PATH))
    ap.add_argument("--csv", default=None)
    args = ap.parse_args(argv)

    csv_path = Path(args.csv) if args.csv else Path(f"data/raw/fcs_schedule_{args.year}.csv")
    conn = sqlite3.connect(args.db)
    try:
        result = load(conn, csv_path, args.year)
    finally:
        conn.close()

    if not csv_path.exists():
        print(f"No {csv_path}; cleared {args.year} and loaded nothing.")
        return 0
    print(f"{args.year}: loaded {result['loaded']} of {result['read']} rows from {csv_path}")
    # A fixture that is already in `games` is not an error -- the source was
    # fetched before kickoff and the game has since been played -- so it is
    # counted quietly. The other two are named: they mean the source said
    # something this table cannot represent.
    if result["already_played"]:
        print(f"  skipped {len(result['already_played'])} since played")
    for label, bad in (("no FBS side named", result["ambiguous"]),
                       ("neither team known", result["neither_known"])):
        if bad:
            print(f"  skipped {len(bad)} with {label}: {', '.join(bad[:5])}"
                  + (" ..." if len(bad) > 5 else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
