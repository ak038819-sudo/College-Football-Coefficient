"""Read-only reconciliation of raw CSV games with the canonical Elocast DB.

Run from repository root:
 python analysis/bowling_merits/eligibility_audit.py db/league.db data/raw/games_2019.csv data/raw/games_2026.csv
No tables are written. A raw-only game is a diagnostic, not automatically ineligible.
"""
import argparse
import csv
import sqlite3
from collections import Counter
from pathlib import Path

def audit(db,paths):
    con=sqlite3.connect(f"file:{Path(db).resolve()}?mode=ro",uri=True)
    con.row_factory=sqlite3.Row
    tables={r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "games" not in tables:
        raise RuntimeError("Missing canonical games table")
    for path in paths:
        with open(path,newline="",encoding="utf-8") as f:
            rows=list(csv.DictReader(f))
        years={int(r["season_year"]) for r in rows}
        if len(years)!=1: raise ValueError("Expected one season per CSV")
        year=years.pop()
        raw={int(r["game_id"]):r for r in rows if r["home_score"] and r["away_score"]}
        dbids={r[0] for r in con.execute("SELECT game_id FROM games WHERE season_year=? AND home_score IS NOT NULL AND away_score IS NOT NULL",(year,))}
        raw_only=sorted(set(raw)-dbids)
        db_only=sorted(dbids-set(raw))
        print(f"{year}: raw={len(raw)} db={len(dbids)} shared={len(set(raw)&dbids)} raw_only={len(raw_only)} db_only={len(db_only)}")
        for gid in raw_only[:25]:
            r=raw[gid]
            print(f"  RAW ONLY {gid}: {r['home_team']} vs {r['away_team']} ({r['date']})")
        if db_only: print("  DB ONLY IDs:",db_only[:25])
        if "team_membership_by_season" in tables:
            missing=con.execute("""SELECT t.team_name, m.conference_real, m.is_fbs
                FROM team_membership_by_season m JOIN teams t ON t.team_id=m.team_id
                WHERE m.season_year=? AND (m.is_fbs=0 OR m.conference_real IS NULL)
                ORDER BY t.team_name""",(year,)).fetchall()
            print("  Non-FBS or null-conference membership:",[tuple(r) for r in missing[:25]])
        else:
            print("  WARNING: membership table missing")
    con.close()

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("db")
    p.add_argument("csv",nargs="+")
    a=p.parse_args()
    audit(a.db,a.csv)
