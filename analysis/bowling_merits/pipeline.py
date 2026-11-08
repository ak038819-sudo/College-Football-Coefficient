"""Opt-in Bowling Merits shadow pipeline, isolated from production workflows.

Read canonical completed games, calculate Challenger A, and atomically replace
one season's awards in a separate shadow SQLite database. No production writes.
"""
import argparse
import json
from pathlib import Path
from canonical_adapter import snapshot
from shadow_storage import sync_shadow

def run(source_db,shadow_db,season,as_of=None,enable=False):
    if not enable:
        raise PermissionError("Shadow pipeline disabled; --enable-shadow-writes required")
    source=Path(source_db).resolve()
    target=Path(shadow_db).resolve()
    if source==target:
        raise ValueError("Shadow database must be separate from canonical source")
    result=snapshot(source,season,as_of)
    count=sync_shadow(target,result["ledger"],season,enabled=True)
    return {"season":season,"as_of":as_of,"awards":count,
            "provisional_awards":result["provisional_awards"],
            "source_db":str(source),"shadow_db":str(target)}

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--source-db",default="db/league.db")
    p.add_argument("--shadow-db",default="db/bowling_merits_shadow.db")
    p.add_argument("--season",type=int,required=True)
    p.add_argument("--as-of")
    p.add_argument("--enable-shadow-writes",action="store_true")
    a=p.parse_args()
    print(json.dumps(run(a.source_db,a.shadow_db,a.season,a.as_of,
                         enable=a.enable_shadow_writes),indent=2))
