"""Read-only Bowling Merits preview data contract.

Consumes only an existing isolated shadow database. Never computes from
research CSVs, alters production standings, or writes to either database.
"""
import argparse
import json
import sqlite3
from collections import defaultdict
from pathlib import Path

def build_preview(shadow_db,season,limit=None):
    path=Path(shadow_db)
    if not path.is_file():
        raise FileNotFoundError(f"Bowling Merits shadow database missing: {path}")
    with sqlite3.connect(f"file:{path.resolve()}?mode=ro",uri=True) as con:
        rows=con.execute("""
          SELECT game_id,team,game_date,opponent,outcome,opponent_quality,
                 merits,future_available,mature
          FROM bowling_merits_awards WHERE season=?
          ORDER BY game_date,game_id,team
        """,(season,)).fetchall()
    if not rows:
        raise ValueError(f"No shadow awards available for season {season}")
    teams=defaultdict(lambda:{"total":0.0,"games":0,"wins":0,"losses":0,
                              "ot_losses":0,"provisional":0,"awards":[]})
    for game_id,team,date,opponent,outcome,q,merits,available,mature in rows:
        t=teams[team]
        t["total"]+=merits
        t["games"]+=1
        t["wins"]+=outcome=="W"
        t["ot_losses"]+=outcome=="OT L"
        t["losses"]+=outcome=="L"
        t["provisional"]+=not mature
        t["awards"].append({"game_id":game_id,"date":date,"opponent":opponent,
                            "outcome":outcome,"opponent_quality":round(q,6),
                            "merits":round(merits,6),"future_games_available":available,
                            "provisional":not bool(mature)})
    ranking=sorted(teams.items(),key=lambda item:(-item[1]["total"],item[0]))
    standings=[{"rank":i,"team":team,"merits":round(data["total"],6),
                "games":data["games"],"wins":data["wins"],
                "regulation_losses":data["losses"],"ot_losses":data["ot_losses"],
                "provisional_awards":data["provisional"],
                "provisional":data["provisional"]>0,
                "awards":data["awards"]}
               for i,(team,data) in enumerate(ranking,1)]
    return {"model":"Bowling Merits","status":"research_preview",
            "production_merits_unchanged":True,
            "eligibility_reconciliation":"deferred",
            "season":season,"teams":len(standings),
            "standings":standings[:limit] if limit is not None else standings}

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--shadow-db",default="db/bowling_merits_shadow.db")
    p.add_argument("--season",type=int,required=True)
    p.add_argument("--limit",type=int,default=None)
    p.add_argument("--output")
    a=p.parse_args()
    data=build_preview(a.shadow_db,a.season,a.limit)
    rendered=json.dumps(data,indent=2)+"\n"
    if a.output: Path(a.output).write_text(rendered,encoding="utf-8")
    else: print(rendered,end="")
