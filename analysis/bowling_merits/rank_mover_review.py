"""Explain the largest control-vs-challenger ranking movements, with game-level evidence."""
import argparse
import csv
from collections import Counter
from pathlib import Path
from engine import Rules, calculate, read_games, standings
from historical_validation import YEARS, ROOT

def report(root=ROOT,limit=10,output=None):
    results=[]
    for year in YEARS:
        games=read_games(root/"data"/"raw"/f"games_{year}.csv")
        control=calculate(games,Rules(2,0,3))
        challenger=calculate(games,Rules(1.5,.5,3))
        ranks=[{team:i for i,(team,_) in enumerate(standings(ledger),1)} for ledger in (control,challenger)]
        totals=[dict(standings(ledger)) for ledger in (control,challenger)]
        per_team={}
        for entry in challenger: per_team.setdefault(entry["team"],[]).append(entry)
        for team,entries in per_team.items():
            outcomes=Counter(x["outcome"] for x in entries)
            wins=[x for x in entries if x["outcome"]=="W"]
            losses=[x for x in entries if x["outcome"]=="L"]
            results.append(dict(season=year,team=team,control_rank=ranks[0][team],
                challenger_rank=ranks[1][team],rank_gain=ranks[0][team]-ranks[1][team],
                control_merits=round(totals[0][team],6),
                challenger_merits=round(totals[1][team],6),
                wins=outcomes["W"],regulation_losses=outcomes["L"],ot_losses=outcomes["OT L"],
                mean_win_opponent_quality=round(sum(x["q"] for x in wins)/len(wins),4) if wins else None,
                mean_loss_opponent_quality=round(sum(x["q"] for x in losses)/len(losses),4) if losses else None))
    fields=list(results[0])
    if output:
        output=Path(output);output.parent.mkdir(parents=True,exist_ok=True)
        with output.open("w",newline="",encoding="utf-8") as f:
            writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader();writer.writerows(results)
    for year in YEARS:
        season=sorted((r for r in results if r["season"]==year),
                      key=lambda r:(-abs(r["rank_gain"]),r["team"]))[:limit]
        print(f"SEASON {year}: biggest control vs challenger movers")
        for r in season: print(f"  {r['team']}: {r['control_rank']} -> {r['challenger_rank']} "
            f"(gain {r['rank_gain']:+}); W={r['wins']} L={r['regulation_losses']} "
            f"OTL={r['ot_losses']} win-Q={r['mean_win_opponent_quality']}")
    return results

if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("--limit",type=int,default=10);p.add_argument("--output")
    a=p.parse_args();report(limit=a.limit,output=a.output)
