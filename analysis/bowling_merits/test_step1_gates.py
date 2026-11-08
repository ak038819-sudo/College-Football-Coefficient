"""Step 1 parity and eligibility gates for Bowling Merits."""
import csv
import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent))
from engine import calculate,read_games,standings

ROOT=Path(__file__).resolve().parents[2]

class StepOneGates(unittest.TestCase):
    def test_incremental_replay_matches_full_rebuild_at_every_week(self):
        for year in (2019,2026):
            games=read_games(ROOT/"data"/"raw"/f"games_{year}.csv")
            dates=sorted({g["date"] for g in games})
            for date in dates:
                subset=[g for g in games if g["date"]<=date]
                expected=calculate(subset)
                actual=calculate(games,as_of=date)
                self.assertEqual(expected,actual,(year,date))
                self.assertEqual(standings(expected),standings(actual),(year,date))

    def test_no_future_leakage(self):
        games=read_games(ROOT/"data"/"raw"/"games_2026.csv")
        first=min(g["date"] for g in games)
        baseline=calculate(games,as_of=first)
        extra=dict(games[-1],id="future_fixture",date="2027-01-20")
        self.assertEqual(baseline,calculate(games+[extra],as_of=first))

    def test_2026_raw_unaffiliated_game_is_detected(self):
        path=ROOT/"data"/"raw"/"games_2026.csv"
        with path.open(newline="",encoding="utf-8") as f:
            rows=list(csv.DictReader(f))
        unaffiliated=[r for r in rows if not r.get("home_conference") and not r.get("away_conference")
                      and r.get("home_score") and r.get("away_score")]
        self.assertTrue(any({r["home_team"],r["away_team"]}=={"Schreiner","Oklahoma Panhandle"} for r in unaffiliated))

if __name__=="__main__": unittest.main()
