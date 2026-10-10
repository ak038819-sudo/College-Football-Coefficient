"""
Load data/raw/fcs_games_<year>.csv into non_fbs_games.

One row per completed game an FBS team played against an opponent this project
does not hold. The FBS side is resolved to a team_id through the same identity
registry every other loader uses; the other side is kept as the source's own
spelling and nothing is inferred from it.

Nothing here writes to `games`, so nothing here can reach a rating. See
sql/non_fbs_game_tables.sql for why that separation is structural.
"""
import argparse
import csv
import sqlite3
import sys
from pathlib import Path

from team_identity import resolve_database_name

DB_PATH = Path("db/league.db")
SCHEMA = Path(__file__).resolve().parent.parent / "sql/non_fbs_game_tables.sql"


def norm(s) -> str:
    return (s or "").strip()


def to_int(value, field: str, game_id: str):
    text = norm(value)
    if text == "":
        raise ValueError(f"game {game_id}: {field} is empty")
    return int(text)


def to_bool01(value) -> int:
    return 1 if norm(value).lower() in {"1", "true", "yes", "t", "y"} else 0


def sides(cur, row: dict) -> tuple[str, str] | None:
    """
    (fbs_side, other_side) as 'home'/'away', or None when this row is not the
    one-known-one-unknown shape the table is for.

    Both sides resolving means the source reclassified a game this project
    already rates, and loading it here would show it twice and double-count the
    record. Neither resolving means no FBS team is involved at all. Both are
    reported rather than guessed at.
    """
    home = resolve_database_name(cur, norm(row["home_team"]))
    away = resolve_database_name(cur, norm(row["away_team"]))
    if home and away:
        return None
    if home:
        return "home", "away"
    if away:
        return "away", "home"
    return None


def team_id(cur, name: str) -> int:
    canonical = resolve_database_name(cur, name)
    cur.execute("SELECT team_id FROM teams WHERE team_name = ?", (canonical,))
    found = cur.fetchone()
    if not found:
        raise ValueError(f"resolved '{name}' to '{canonical}', which is not in teams")
    return found[0]


def load(conn: sqlite3.Connection, csv_path: Path, year: int) -> dict:
    cur = conn.cursor()
    cur.executescript(SCHEMA.read_text())

    rows = list(csv.DictReader(csv_path.open(encoding="utf-8"))) if csv_path.exists() else []
    loaded, both_known, neither_known = 0, [], []

    # The season is replaced wholesale rather than upserted: a game the source
    # has reclassified out of this set must disappear, and an upsert would
    # leave it behind forever.
    cur.execute("DELETE FROM non_fbs_games WHERE season_year = ?", (year,))

    for row in rows:
        gid = norm(row["game_id"])
        which = sides(cur, row)
        if which is None:
            target = both_known if resolve_database_name(cur, norm(row["home_team"])) \
                else neither_known
            target.append(f"{row['away_team']} at {row['home_team']}")
            continue
        fbs, other = which
        cur.execute(
            """
            INSERT INTO non_fbs_games
                (game_id, season_year, week, game_date, team_id, opponent_name,
                 opponent_conference, opponent_classification, team_score,
                 opponent_score, is_home, neutral_site, went_ot, game_phase)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(gid), year,
                int(row["week"]) if norm(row["week"]) else None,
                norm(row["date"]),
                team_id(cur, norm(row[f"{fbs}_team"])),
                norm(row[f"{other}_team"]),
                norm(row.get(f"{other}_conference")),
                norm(row.get(f"{other}_classification")),
                to_int(row[f"{fbs}_score"], f"{fbs}_score", gid),
                to_int(row[f"{other}_score"], f"{other}_score", gid),
                1 if fbs == "home" else 0,
                to_bool01(row["neutral_site"]),
                to_bool01(row["went_ot"]),
                norm(row["game_phase"]) or "regular",
            ),
        )
        loaded += 1

    conn.commit()
    return {"loaded": loaded, "both_known": both_known, "neither_known": neither_known,
            "read": len(rows)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("year", type=int)
    ap.add_argument("--db", default=str(DB_PATH))
    ap.add_argument("--csv", default=None)
    args = ap.parse_args(argv)

    csv_path = Path(args.csv) if args.csv else Path(f"data/raw/fcs_games_{args.year}.csv")
    conn = sqlite3.connect(args.db)
    try:
        result = load(conn, csv_path, args.year)
    finally:
        conn.close()

    if not csv_path.exists():
        print(f"No {csv_path}; cleared {args.year} and loaded nothing.")
        return 0
    print(f"{args.year}: loaded {result['loaded']} of {result['read']} rows from {csv_path}")
    # A row this project already rates must never also appear here, so it is
    # named rather than counted: it means the source changed its mind about a
    # game, which is worth a human look.
    for label, bad in (("both teams known", result["both_known"]),
                       ("neither team known", result["neither_known"])):
        if bad:
            print(f"  skipped {len(bad)} with {label}: {', '.join(bad[:5])}"
                  + (" ..." if len(bad) > 5 else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
