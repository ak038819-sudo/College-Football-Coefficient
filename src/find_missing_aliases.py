import csv
import sqlite3
import sys
from pathlib import Path


def norm(s: str) -> str:
    return (s or "").strip()


def main(db_path: str, csv_path: str) -> int:
    db_path = str(db_path)
    csv_path = str(csv_path)

    if not Path(db_path).exists():
        print(f"DB not found: {db_path}")
        return 2
    if not Path(csv_path).exists():
        print(f"CSV not found: {csv_path}")
        return 2

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Everything we already recognize:
    cur.execute("SELECT team_name FROM teams")
    known = {norm(r[0]) for r in cur.fetchall()}

    cur.execute("SELECT alias, team_name FROM team_aliases")
    for a, tn in cur.fetchall():
        known.add(norm(a))
        known.add(norm(tn))

    missing = set()
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            h = norm(row.get("home_team", ""))
            a = norm(row.get("away_team", ""))
            if h and h not in known:
                missing.add(h)
            if a and a not in known:
                missing.add(a)

    for name in sorted(missing):
        print(name)

    conn.close()
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python src/find_missing_aliases.py db/league.db data/raw/games_2025.csv")
        raise SystemExit(2)
    raise SystemExit(main(sys.argv[1], sys.argv[2]))
