"""Research-only Bowling Merits engine; no production writes."""
import csv
from collections import defaultdict
from dataclasses import dataclass

@dataclass(frozen=True)
class Rules:
    floor: float = 1.5
    loss_penalty: float = .5
    future: int = 3
    def __post_init__(self):
        if not 0 <= self.floor <= 4 or self.loss_penalty < 0 or self.future not in (3,4):
            raise ValueError("Invalid research parameters")

def read_games(path):
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    out = []
    for r in rows:
        if not r["home_score"] or not r["away_score"]:
            continue
        if r.get("went_ot") not in ("0","1"):
            raise ValueError("Unknown overtime status: "+r["game_id"])
        out.append(dict(id=r["game_id"], season=int(r["season_year"]), date=r["date"],
                        home=r["home_team"], away=r["away_team"], hs=int(r["home_score"]),
                        as_=int(r["away_score"]), ot=r["went_ot"]=="1"))
    return out

def calculate(games, rules=Rules(), as_of=None):
    """Return deterministic game-level ledger; a date cutoff prevents future leakage."""
    ordered=sorted((g for g in games if as_of is None or g["date"]<=as_of),
                   key=lambda g:(g["season"],g["date"],g["id"]))
    schedule=defaultdict(list)
    records=defaultdict(lambda:[0,0])
    seen=set()
    for g in ordered:
        key=(g["season"],g["id"])
        if key in seen or not g["home"] or not g["away"] or g["home"]==g["away"] or g["hs"]==g["as_"] or min(g["hs"],g["as_"])<0:
            raise ValueError("Invalid/duplicate game: "+str(key))
        seen.add(key)
        for team,opp,won in ((g["home"],g["away"],g["hs"]>g["as_"]),
                             (g["away"],g["home"],g["as_"]>g["hs"])):
            w,l=records[(g["season"],opp)]
            q=(w+2)/(w+l+4)
            s=.5+.5*q if won else (.25 if g["ot"] else 0)+.5*q
            schedule[(g["season"],team)].append((g,opp,won,s))
        records[(g["season"],g["home"])][0 if g["hs"]>g["as_"] else 1]+=1
        records[(g["season"],g["away"])][0 if g["as_"]>g["hs"] else 1]+=1
    ledger=[]
    for (season,team),entries in schedule.items():
        for g,opp,won,_ in entries:
            other=schedule[(season,opp)]
            idx=next(i for i,x in enumerate(other) if x[0]["id"]==g["id"])
            available=min(rules.future,len(other)-idx-1)
            samples=[]
            for d,weight in enumerate((1,.75,.5),1):
                if idx>=d: samples.append((weight,other[idx-d][3]))
            for d,weight in enumerate((1,.75,.5,.25)[:rules.future],1):
                if idx+d<len(other): samples.append((weight,other[idx+d][3]))
            q=sum(w*s for w,s in samples)/sum(w for w,s in samples) if samples else .5
            merits=(rules.floor+(4-rules.floor)*q if won else
                    .5 if g["ot"] else -rules.loss_penalty*(1-q))
            ledger.append(dict(id=g["id"],season=season,date=g["date"],team=team,
                               opponent=opp,outcome="W" if won else "OT L" if g["ot"] else "L",
                               q=q,merits=merits,available=available,mature=available==rules.future))
    return sorted(ledger,key=lambda x:(x["season"],x["date"],x["id"],x["team"]))

def standings(ledger):
    totals=defaultdict(float)
    for row in ledger: totals[row["team"]]+=row["merits"]
    return sorted(totals.items(),key=lambda x:(-x[1],x[0]))

if __name__=="__main__":
    import argparse,json
    p=argparse.ArgumentParser()
    p.add_argument("csv",nargs="+")
    p.add_argument("--as-of")
    p.add_argument("--floor",type=float,default=1.5)
    p.add_argument("--loss-penalty",type=float,default=.5)
    p.add_argument("--future",type=int,default=3)
    a=p.parse_args()
    rules=Rules(a.floor,a.loss_penalty,a.future)
    for path in a.csv:
        games=read_games(path)
        ledger=calculate(games,rules,a.as_of)
        ranks=standings(ledger)
        print(json.dumps(dict(file=path,awards=len(ledger),mature=sum(x["mature"] for x in ledger),
             provisional=sum(not x["mature"] for x in ledger),
             BYU=next((s for t,s in ranks if t=="BYU"),None),top5=ranks[:5]),indent=2))
