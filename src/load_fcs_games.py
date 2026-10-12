"""
Load data/raw/fcs_games_<year>.csv into non_fbs_games.

One row per completed game an FBS team played that no rating in this project
holds. The FBS side is resolved to a team_id through the same identity registry
every other loader uses; the other side is kept as the source's own spelling and
nothing is inferred from it.

The other side is usually a school this project does not carry at all, but it
need not be: a program that has played in both divisions (Idaho, Delaware,
Jacksonville State) is in `teams` for its FBS seasons and is an unrated
opponent in the others. Which side was FBS comes from the source's own
per-game classification, and whether a game is already rated comes from its id
in `games` -- not from whether a name resolves, which cannot know a season.

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


def fbs_side_by_classification(row: dict) -> str | None:
    """
    'home'/'away' for the side the source labels FBS, or None when its own
    classification strings do not single one out.

    CFBD carries a per-game classification for both sides, and that is the only
    thing in the data that knows which division a team was in *that season*.
    A team's presence in `teams` cannot answer it: Idaho played FBS football
    through 2017 and FCS football after, so the table holds it either way.
    """
    labels = {side: norm(row.get(f"{side}_classification")).lower()
              for side in ("home", "away")}
    fbs = [side for side, label in labels.items() if label == "fbs"]
    if len(fbs) != 1:
        return None
    other = "away" if fbs[0] == "home" else "home"
    # One side labelled fbs and the other labelled nothing is not evidence of a
    # division gap, so it is not treated as one.
    return fbs[0] if labels[other] else None


def rated_already(cur, game_id: int) -> bool:
    """Whether the ratings already hold this game, by id rather than by name."""
    return cur.execute(
        "SELECT 1 FROM games WHERE game_id = ?", (game_id,)).fetchone() is not None


def sides(cur, row: dict, game_id: int) -> tuple[str, str] | None:
    """
    (project_side, other_side) as 'home'/'away', or None when this row is not
    one this table can hold.

    One side has to be a team this project holds, because that is what the row
    hangs off. The other side is kept as the source's own spelling.

    Both sides resolving used to be refused outright, on the reading that the
    source had reclassified a game the ratings already have. That reading was
    wrong 649 times across 1980-2026: a team can be in `teams` for its FBS
    seasons and play the same schools from the FCS later, and Utah 66, Idaho 14
    in 2026 is both names resolving while the game is in no rating at all. So
    the duplicate test is now the game's id in `games`, which is what "already
    rated" actually means, and the division gap is read from the source's own
    per-game classification.
    """
    home = resolve_database_name(cur, norm(row["home_team"]))
    away = resolve_database_name(cur, norm(row["away_team"]))
    if not (home or away):
        return None
    if home and away:
        if rated_already(cur, game_id):
            return None
        fbs = fbs_side_by_classification(row)
        if fbs is None:
            return None
        return fbs, ("away" if fbs == "home" else "home")
    # Exactly one side is a team this project holds, so it is the only side a
    # row can hang off, whatever the source's classification says about it.
    return ("home", "away") if home else ("away", "home")


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
    loaded, already_rated, ambiguous, neither_known = 0, [], [], []

    # The season is replaced wholesale rather than upserted: a game the source
    # has reclassified out of this set must disappear, and an upsert would
    # leave it behind forever.
    cur.execute("DELETE FROM non_fbs_games WHERE season_year = ?", (year,))

    for row in rows:
        gid = norm(row["game_id"])
        which = sides(cur, row, int(gid))
        if which is None:
            label = f"{row['away_team']} at {row['home_team']}"
            if rated_already(cur, int(gid)):
                already_rated.append(label)
            elif resolve_database_name(cur, norm(row["home_team"])) \
                    or resolve_database_name(cur, norm(row["away_team"])):
                ambiguous.append(label)
            else:
                neither_known.append(label)
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
    return {"loaded": loaded, "already_rated": already_rated,
            "ambiguous": ambiguous, "neither_known": neither_known,
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
    # game, which is worth a human look. So is a game between two teams this
    # project holds whose classification does not say which side was FBS: the
    # row could go on either team's schedule and the data does not say which.
    for label, bad in (("the game already rated", result["already_rated"]),
                       ("no FBS side named", result["ambiguous"]),
                       ("neither team known", result["neither_known"])):
        if bad:
            print(f"  skipped {len(bad)} with {label}: {', '.join(bad[:5])}"
                  + (" ..." if len(bad) > 5 else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
