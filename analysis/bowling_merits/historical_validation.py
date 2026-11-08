"""Historical Bowling Merits validation: research only, no production writes.

Run: python analysis/bowling_merits/historical_validation.py
Optional: --output-dir /tmp/bowling-merits-study
"""
import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from engine import Rules, calculate, read_games, standings

ROOT=Path(__file__).resolve().parents[2]
YEARS=tuple(range(2014,2020))+tuple(range(2021,2026))
MODELS={"control_k3":Rules(2,0,3),"challenger_k3":Rules(1.5,.5,3),"challenger_k4":Rules(1.5,.5,4)}

def rankmap(ledger):
    return {team:i for i,(team,_) in enumerate(standings(ledger),1)}

def run(root=ROOT,output_dir=None):
    output_dir=Path(output_dir) if output_dir else None
    if output_dir: output_dir.mkdir(parents=True,exist_ok=True)
    rows=[]
    for year in YEARS:
        path=root/"data"/"raw"/f"games_{year}.csv"
        if not path.is_file():
            raise FileNotFoundError(f"Required historical season missing: {path}")
        games=read_games(path)
        if not games: raise ValueError(f"No completed games for {year}")
        if {g["season"] for g in games}!={year}: raise ValueError(f"Mixed/mislabeled season {year}")
        ledgers={name:calculate(games,rules) for name,rules in MODELS.items()}
        ranks={name:rankmap(ledger) for name,ledger in ledgers.items()}
        all_teams=set.union(*(set(x) for x in ranks.values()))
        if len(all_teams)!=len(ranks["control_k3"]): raise AssertionError("Model team set mismatch")
        totals={name:dict(standings(ledger)) for name,ledger in ledgers.items()}
        for team in sorted(all_teams):
            row={"season":year,"team":team,"games":sum(g["home"]==team or g["away"]==team for g in games)}
            for name in MODELS:
                row[name+"_rank"]=ranks[name][team]
                row[name+"_merits"]=round(totals[name][team],8)
            row["challenger_k3_rank_change"]=ranks["control_k3"][team]-ranks["challenger_k3"][team]
            row["challenger_k4_rank_change"]=ranks["challenger_k3"][team]-ranks["challenger_k4"][team]
            rows.append(row)
        print(json.dumps({"season":year,"games":len(games),"teams":len(all_teams),
                          "top5":{name:standings(ledgers[name])[:5] for name in MODELS},
                          "provisional_awards":{name:sum(not x["mature"] for x in ledger) for name,ledger in ledgers.items()}},sort_keys=True))
    if output_dir:
        fields=list(rows[0])
        with (output_dir/"bowling_merits_historical_comparison.csv").open("w",newline="",encoding="utf-8") as f:
            writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader();writer.writerows(rows)
        summary=[]
        for year in YEARS:
            season=[r for r in rows if r["season"]==year]
            for name in ("challenger_k3","challenger_k4"):
                base="control_k3" if name=="challenger_k3" else "challenger_k3"
                movers=sorted(season,key=lambda r:(-abs(r[base+"_rank"]-r[name+"_rank"]),r["team"]))[:15]
                summary.extend({"season":year,"comparison":base+" vs "+name,"team":r["team"],
                                "old_rank":r[base+"_rank"],"new_rank":r[name+"_rank"],
                                "delta":r[base+"_rank"]-r[name+"_rank"]} for r in movers)
        with (output_dir/"bowling_merits_rank_movers.csv").open("w",newline="",encoding="utf-8") as f:
            writer=csv.DictWriter(f,fieldnames=["season","comparison","team","old_rank","new_rank","delta"])
            writer.writeheader();writer.writerows(summary)
    return rows

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--output-dir")
    args=parser.parse_args()
    run(output_dir=args.output_dir)
