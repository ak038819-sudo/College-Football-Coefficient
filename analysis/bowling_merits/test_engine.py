"""Regression coverage for the research-only Bowling Merits engine."""
import csv
import sys
import unittest
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parent))
from engine import Rules, calculate, read_games, standings

def g(i,date,home,away,hs,away_score,ot=False,season=2026):
    return dict(id=str(i),date=date,home=home,away=away,hs=hs,as_=away_score,ot=ot,season=season)

class BowlingMeritsTests(unittest.TestCase):
    def test_ot_win_and_loss(self):
        a=calculate([g(1,"2026-09-01","A","B",24,21,True)])
        self.assertAlmostEqual(next(x["merits"] for x in a if x["team"]=="A"),2.75)
        self.assertAlmostEqual(next(x["merits"] for x in a if x["team"]=="B"),.5)

    def test_control_and_challenger(self):
        games=[g(1,"2026-09-01","A","B",24,21)]
        self.assertAlmostEqual(next(x["merits"] for x in calculate(games) if x["team"]=="B"),-.25)
        a=calculate(games,Rules(2,0))
        self.assertAlmostEqual(next(x["merits"] for x in a if x["team"]=="A"),3)
        self.assertAlmostEqual(next(x["merits"] for x in a if x["team"]=="B"),0)

    def test_ot_loss_quality_signal(self):
        first=g(1,"2026-09-01","A","B",20,10)
        reg=next(x["merits"] for x in calculate([first,g(2,"2026-09-08","C","B",24,21)]) if x["team"]=="A")
        ot=next(x["merits"] for x in calculate([first,g(2,"2026-09-08","C","B",24,21,True)]) if x["team"]=="A")
        self.assertGreater(ot,reg)

    def test_chronological_cutoff(self):
        games=[g(1,"2026-09-01","A","B",20,10),
               g(2,"2026-09-08","B","C",17,10),
               g(3,"2026-09-15","B","D",17,10),
               g(4,"2026-09-22","B","E",17,10)]
        initial=calculate(games,as_of="2026-09-01")
        final=calculate(games)
        early=next(x for x in initial if x["team"]=="A")
        later=next(x for x in final if x["team"]=="A")
        self.assertAlmostEqual(early["merits"],2.75)
        self.assertFalse(early["mature"])
        self.assertTrue(later["mature"])
        self.assertGreater(later["merits"],early["merits"])

    def test_order_independent(self):
        games=[g(1,"2026-09-01","A","B",20,10),g(2,"2026-09-08","B","C",17,10)]
        self.assertEqual(calculate(games),calculate(list(reversed(games))))

    def test_season_isolation(self):
        games=[g(1,"2025-09-01","A","B",20,10,season=2025),g(2,"2026-09-01","A","B",20,10)]
        self.assertEqual([x["q"] for x in calculate(games) if x["team"]=="A"],[.5,.5])

    def test_invalid_games(self):
        with self.assertRaises(ValueError): calculate([g(1,"2026-09-01","A","B",10,10)])
        x=g(1,"2026-09-01","A","B",10,7)
        with self.assertRaises(ValueError): calculate([x,x])

    def test_four_game_window(self):
        games=[g(i,"2026-09-%02d"%i,"B",f"C{i}",20,10) for i in range(1,6)]
        self.assertEqual(len(calculate(games,Rules(future=4))),10)

    def test_historical_fixtures_if_present(self):
        root=Path(__file__).resolve().parents[2]
        expected={2019:(774,1158,15.089198687115356),2026:(558,700,18.22364065332815)}
        for year,(count,mature,byu) in expected.items():
            path=root/"data"/"raw"/f"games_{year}.csv"
            if not path.exists(): continue
            games=read_games(path)
            self.assertEqual(len(games),count)
            ledger=calculate(games)
            self.assertEqual(sum(x["mature"] for x in ledger),mature)
            self.assertAlmostEqual(dict(standings(ledger))["BYU"],byu,places=8)

if __name__=="__main__": unittest.main()
