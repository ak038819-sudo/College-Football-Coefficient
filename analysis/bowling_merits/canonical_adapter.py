"""Read-only canonical SQLite adapter for Bowling Merits.

No database writes. Only FBS-vs-FBS completed games are scored.
"""
import argparse
import json
import sqlite3
from pathlib import Path
from engine import Rules,calculate,standings

SQL="""
SELECT g.game_id,g.season_year,g.game_date,
       h.team_name AS home,a.team_name AS away,
       g.home_score,g.away_score,g.went_ot
FROM games g
JOIN teams h ON h.team_id=g.home_team_id
JOIN teams a ON a.team_id=g.away_team_id
JOIN team_membership_by_season hm
  ON hm.team_id=g.home_team_id AND hm.season_year=g.season_year
JOIN team_membership_by_season am
  ON am.team_id=g.away_team_id AND am.season_year=g.season_year
WHERE g.season_year=?
  AND hm.is_fbs=1 AND am.is_fbs=1
  AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL
  AND g.game_date IS NOT NULL AND g.went_ot IN (0,1)
ORDER BY g.game_date,g.game_id
"""

def read_canonical(db,season):
    path=Path(db)
    if not path.is_file(): raise FileNotFoundError(path)
    with sqlite3.connect(f"file:{path.resolve()}?mode=ro",uri=True) as con:
        tables={r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        required={"games","teams","team_membership_by_season"}
        if not required<=tables: raise RuntimeError(f"Missing canonical tables: {sorted(required-tables)}")
        rows=con.execute(SQL,(season,)).fetchall()
    games=[dict(id=str(i),season=int(y),date=d,home=h,away=a,
                hs=int(hs),as_=int(away_score),ot=bool(ot))
           for i,y,d,h,a,hs,away_score,ot in rows]
    if not games: raise ValueError(f"No eligible completed FBS games in {season}")
    return games

def snapshot(db,season,as_of=None):
    games=read_canonical(db,season)
    ledger=calculate(games,Rules(1.5,.5,3),as_of)
    return {"season":season,"as_of":as_of,"eligible_games":len(games),
            "awards":len(ledger),"provisional_awards":sum(not x["mature"] for x in ledger),
            "standings":standings(ledger),"ledger":ledger}

if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("--db",default="db/league.db")
    p.add_argument("--season",type=int,required=True);p.add_argument("--as-of")
    p.add_argument("--output")
    a=p.parse_args()
    result=snapshot(a.db,a.season,a.as_of)
    rendered=json.dumps(result,indent=2)
    if a.output: Path(a.output).write_text(rendered+"\n",encoding="utf-8")
    else: print(rendered)
