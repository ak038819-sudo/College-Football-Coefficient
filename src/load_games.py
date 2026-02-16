import csv
import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path("db/league.db")


def norm(s: str) -> str:
    return (s or "").strip()


def resolve_team_name(cur, raw_name: str) -> str:
    """
    Map a raw name to a canonical teams.team_name using team_aliases if needed.
    """
    raw_name = norm(raw_name)

    # Direct hit
    cur.execute("SELECT team_name FROM teams WHERE team_name = ?", (raw_name,))
    if cur.fetchone():
        return raw_name

    # Alias hit
    cur.execute("SELECT team_name FROM team_aliases WHERE alias = ?", (raw_name,))
    row = cur.fetchone()
    if row:
        return row[0]

    raise ValueError(
        f"Unknown team name: '{raw_name}'. "
        f"Add it to teams.team_name or create an alias in team_aliases."
    )


def team_id(cur, canonical_name: str) -> int:
    cur.execute("SELECT team_id FROM teams WHERE team_name = ?", (canonical_name,))
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Canonical team not found in teams: '{canonical_name}'")
    return row[0]


def parse_bool01(val: str, field_name: str) -> int:
    """
    Convert common truthy/falsy representations to 0/1.
    Accepts: 0/1, true/false, yes/no, blank -> 0
    """
    v = norm(val).lower()
    if v == "":
        return 0
    if v in ("1", "true", "t", "yes", "y"):
        return 1
    if v in ("0", "false", "f", "no", "n"):
        return 0
    # last resort: try int
    try:
        return 1 if int(v) != 0 else 0
    except Exception as e:
        raise ValueError(f"{field_name} must be 0/1 or boolean-like. Got '{val}'") from e


def main(csv_path: str):
    csv_file = Path(csv_path)
    if not csv_file.exists():
        raise FileNotFoundError(f"Missing file: {csv_file}")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    inserted = 0
    skipped_dupes = 0
    skipped_non_fbs = 0


    with csv_file.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        required = [
            "season_year",
            "week",
            "date",
            "home_team",
            "away_team",
            "home_score",
            "away_score",
            "went_ot",
            "game_type",
        ]
        for col in required:
            if col not in (reader.fieldnames or []):
                raise ValueError(f"CSV missing required column: {col}")

        for row in reader:
            season_year = int(row["season_year"])

            week_raw = norm(row["week"])
            week = int(week_raw) if week_raw else None

            # date is required; keep strict format YYYY-MM-DD
            game_date = norm(row["date"])
            datetime.strptime(game_date, "%Y-%m-%d")

            # neutral_site is OPTIONAL (defaults to 0)
            neutral_site = parse_bool01(row.get("neutral_site", "0"), "neutral_site")

            try:
                home_name = resolve_team_name(cur, row["home_team"])
                away_name = resolve_team_name(cur, row["away_team"])
            except ValueError:
                skipped_non_fbs += 1
                continue


            home_score = int(row["home_score"])
            away_score = int(row["away_score"])

            went_ot = parse_bool01(row["went_ot"], "went_ot")

            game_type = (row.get("game_type") or "").lower()
            week = int(row.get("week") or 0)
            season_year = int(row.get("season_year") or 0)
            game_type = (row.get("game_type") or "").strip().lower()
            is_playoff = 1 if game_type == "playoff" else 0
            is_nit = 0



            home_id = team_id(cur, home_name)
            away_id = team_id(cur, away_name)

            try:
                cur.execute(
                    """
                    INSERT INTO games (
                        season_year, week, game_date, neutral_site,
                        home_team_id, away_team_id,
                        home_score, away_score,
                        went_ot, is_playoff, is_nit
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        season_year,
                        week,
                        game_date,
                        neutral_site,
                        home_id,
                        away_id,
                        home_score,
                        away_score,
                        went_ot,
                        is_playoff,
                        is_nit,
                    ),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                skipped_dupes += 1

            except sqlite3.IntegrityError:
                skipped_dupes += 1

    conn.commit()
    conn.close()

    print(f"Inserted: {inserted}")
    print(f"Skipped duplicates: {skipped_dupes}")
    print(f"Skipped non-FBS games: {skipped_non_fbs}")
    print(f"From file: {csv_file}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python src/load_games.py data/raw/games_2014.csv")
        raise SystemExit(2)
    main(sys.argv[1])

