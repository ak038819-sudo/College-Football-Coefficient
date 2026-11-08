"""Read-only Bowling Merits real-data preflight; no writes or ranking publication."""
import argparse
import json
import sqlite3
from pathlib import Path
from canonical_adapter import read_canonical,snapshot

def preflight(db,season):
    path=Path(db)
    if not path.is_file():raise FileNotFoundError(f"Canonical DB not available: {path}")
    with sqlite3.connect(f"file:{path.resolve()}?mode=ro",uri=True) as con:
        row=con.execute("""
          SELECT COUNT(*),
            COALESCE(SUM(CASE WHEN home_score IS NOT NULL AND away_score IS NOT NULL THEN 1 ELSE 0 END),0),
            COALESCE(SUM(CASE WHEN home_score IS NOT NULL AND away_score IS NOT NULL
                       AND (went_ot IS NULL OR went_ot NOT IN (0,1)) THEN 1 ELSE 0 END),0)
          FROM games WHERE season_year=?
        """,(season,)).fetchone()
        total,scored,unknown_ot=row
        missing_memberships=con.execute("""
          SELECT COUNT(*) FROM games g
          LEFT JOIN team_membership_by_season h ON h.team_id=g.home_team_id AND h.season_year=g.season_year
          LEFT JOIN team_membership_by_season a ON a.team_id=g.away_team_id AND a.season_year=g.season_year
          WHERE g.season_year=? AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL
            AND (h.team_id IS NULL OR a.team_id IS NULL)
        """,(season,)).fetchone()[0]
    games=read_canonical(db,season)
    result=snapshot(db,season)
    if result["awards"]!=2*len(games):
        raise AssertionError("Award/game parity mismatch")
    return {"season":season,"status":"research_preflight_only",
            "total_games":total,"scored_games":scored,
            "unknown_overtime_games":unknown_ot,
            "missing_membership_games":missing_memberships,
            "eligible_fbs_games":len(games),
            "awards":result["awards"],"provisional_awards":result["provisional_awards"],
            "teams":len(result["standings"]),
            "eligibility_reconciliation":"pending"}
if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--db",default="db/league.db")
    p.add_argument("--season",type=int,default=2025)
    a=p.parse_args()
    print(json.dumps(preflight(a.db,a.season),indent=2))
