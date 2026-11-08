"""Opt-in, transactional Bowling Merits shadow storage.

Never writes to production Merits tables. All persistence requires explicit
--enable-shadow-writes and uses an independent SQLite file by default.
"""
import argparse
import json
import sqlite3
from pathlib import Path

from canonical_adapter import snapshot
from incremental import award_key

SCHEMA="""
CREATE TABLE IF NOT EXISTS bowling_merits_awards (
  season INTEGER NOT NULL,
  game_id TEXT NOT NULL,
  team TEXT NOT NULL,
  game_date TEXT NOT NULL,
  opponent TEXT NOT NULL,
  outcome TEXT NOT NULL,
  opponent_quality REAL NOT NULL,
  merits REAL NOT NULL,
  future_available INTEGER NOT NULL,
  mature INTEGER NOT NULL CHECK (mature IN (0,1)),
  PRIMARY KEY(season,game_id,team)
);
CREATE INDEX IF NOT EXISTS idx_bowling_merits_season_team
 ON bowling_merits_awards(season,team);
"""

def sync_shadow(shadow_db, ledger, season, enabled=False):
    if not enabled:
        raise PermissionError("Shadow writes disabled; explicit opt-in required")
    target=Path(shadow_db)
    if target.resolve()==Path("db/league.db").resolve():
        raise ValueError("Refusing to write to the canonical league database")
    target.parent.mkdir(parents=True,exist_ok=True)
    with sqlite3.connect(target) as con:
        con.executescript(SCHEMA)
        # One transaction for season replacement; no partial season if a row fails.
        con.execute("BEGIN IMMEDIATE")
        try:
            con.execute("DELETE FROM bowling_merits_awards WHERE season=?",(season,))
            for r in ledger:
                if r["season"]!=season: raise ValueError("Mixed seasons in ledger")
                con.execute("""INSERT INTO bowling_merits_awards VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    (r["season"],r["id"],r["team"],r["date"],r["opponent"],
                     r["outcome"],r["q"],r["merits"],r["available"],int(r["mature"])))
            con.commit()
        except BaseException:
            con.rollback()
            raise
    return len(ledger)

def read_shadow(shadow_db,season):
    with sqlite3.connect(f"file:{Path(shadow_db).resolve()}?mode=ro",uri=True) as con:
        rows=con.execute("""SELECT game_id,team,merits FROM bowling_merits_awards
                             WHERE season=? ORDER BY game_id,team""",(season,)).fetchall()
    return rows

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--source-db",default="db/league.db")
    p.add_argument("--shadow-db",default="db/bowling_merits_shadow.db")
    p.add_argument("--season",type=int,required=True)
    p.add_argument("--as-of")
    p.add_argument("--enable-shadow-writes",action="store_true")
    a=p.parse_args()
    if not a.enable_shadow_writes:
        raise SystemExit("No writes performed. Pass --enable-shadow-writes to opt in.")
    result=snapshot(a.source_db,a.season,a.as_of)
    count=sync_shadow(a.shadow_db,result["ledger"],a.season,enabled=True)
    print(json.dumps({"season":a.season,"shadow_awards":count,
                      "provisional_awards":result["provisional_awards"],
                      "shadow_db":a.shadow_db}))
