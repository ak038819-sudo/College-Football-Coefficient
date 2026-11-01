#!/usr/bin/env python3
"""Read-only research audit of connectedness and outlier-influence by metric."""
from __future__ import annotations
import argparse
import json
import sqlite3
from pathlib import Path
try:
    from src.build_matchup_intelligence import load_observations
    from src.audit_matchup_graph_outliers import schedule_graph,winsor_sensitivity
except ModuleNotFoundError:
    from build_matchup_intelligence import load_observations
    from audit_matchup_graph_outliers import schedule_graph,winsor_sensitivity

METRICS=("rush_success_rate","pass_success_rate","rush_ppa","pass_ppa",
         "rush_explosiveness","pass_explosiveness")


def evaluate(conn,*,season:int,as_of:str)->dict:
    result={"season":season,"as_of":as_of,
            "method":"FBS_game_graph_and_1_to_99pct_game_level_winsor_sensitivity",
            "metrics":{}}
    for m in METRICS:
        obs,coverage=load_observations(conn,season=season,as_of=as_of,metric=m)
        graph=schedule_graph(obs)
        influence=winsor_sensitivity(obs,lower_q=.01,upper_q=.99)
        result["metrics"][m]={
            "eligible_sides":len(obs),
            "graph":{key:value for key,value in graph.items() if key!="team_graph"},
            "teams_with_bridge_exposure":sum(
                d["incident_bridges"]>0 for d in graph["team_graph"].values()),
            "outliers":influence,
            "coverage":coverage,
        }
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db",default="db/league.db")
    p.add_argument("--season",type=int,required=True)
    p.add_argument("--as-of",required=True)
    p.add_argument("--out",required=True)
    args=p.parse_args()
    with sqlite3.connect(f"file:{Path(args.db).resolve().as_posix()}?mode=ro",
                         uri=True) as conn:
        r=evaluate(conn,season=args.season,as_of=args.as_of)
    dest=Path(args.out)
    dest.parent.mkdir(parents=True,exist_ok=True)
    dest.write_text(json.dumps(r,sort_keys=True,indent=2)+"\n")
    for metric,data in r["metrics"].items():
        print(metric,"sides=",data["eligible_sides"],
              "components=",data["graph"]["connected_components"],
              "bridges=",data["graph"]["bridge_matchups"],
              "clipped=",data["outliers"].get("clipped_observation_count"),
              "outlier_off_p90=",data["outliers"].get("percentile_shift",{}).get("offensive_percentile",{}).get("p90"))


if __name__=="__main__":
    main()
