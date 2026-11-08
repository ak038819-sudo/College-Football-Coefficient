import sqlite3
import tempfile
import unittest
from pathlib import Path

from engine import Rules,calculate
from shadow_storage import read_shadow,sync_shadow

GAMES=[
    dict(id="1",season=2025,date="2025-09-01",home="A",away="B",hs=24,as_=17,ot=False),
    dict(id="2",season=2025,date="2025-09-08",home="B",away="A",hs=21,as_=24,ot=True),
]
LEDGER=calculate(GAMES,Rules(1.5,.5,3))

class ShadowStorageTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=Path(self.tmp.name)/"shadow.db"
    def tearDown(self): self.tmp.cleanup()
    def test_opt_in_required(self):
        with self.assertRaises(PermissionError):
            sync_shadow(self.path,LEDGER,2025)
        self.assertFalse(self.path.exists())
    def test_replay_idempotence(self):
        self.assertEqual(sync_shadow(self.path,LEDGER,2025,True),4)
        first=read_shadow(self.path,2025)
        sync_shadow(self.path,LEDGER,2025,True)
        self.assertEqual(first,read_shadow(self.path,2025))
    def test_season_replacement_removes_stale_awards(self):
        sync_shadow(self.path,LEDGER,2025,True)
        sync_shadow(self.path,LEDGER[:2],2025,True)
        self.assertEqual(len(read_shadow(self.path,2025)),2)
    def test_transaction_rolls_back_on_invalid_row(self):
        sync_shadow(self.path,LEDGER,2025,True)
        before=read_shadow(self.path,2025)
        bad=[dict(r) for r in LEDGER]
        bad[-1]["season"]=2024
        with self.assertRaises(ValueError):
            sync_shadow(self.path,bad,2025,True)
        self.assertEqual(read_shadow(self.path,2025),before)
    def test_separate_database(self):
        sync_shadow(self.path,LEDGER,2025,True)
        with sqlite3.connect(self.path) as con:
            self.assertEqual(con.execute("SELECT count(*) FROM bowling_merits_awards").fetchone()[0],4)
            self.assertFalse(con.execute("SELECT 1 FROM sqlite_master WHERE name='team_coefficient_by_year'").fetchone())

if __name__=="__main__":unittest.main()
