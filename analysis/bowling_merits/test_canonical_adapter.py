import sqlite3
import tempfile
import unittest
from pathlib import Path
from canonical_adapter import read_canonical,snapshot
from engine import calculate,Rules,standings

class CanonicalAdapterTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.db=Path(self.tmp.name)/"league.db"
        with sqlite3.connect(self.db) as c:
            c.executescript("""
            CREATE TABLE teams(team_id INTEGER PRIMARY KEY,team_name TEXT);
            CREATE TABLE team_membership_by_season(team_id INTEGER,season_year INTEGER,is_fbs INTEGER);
            CREATE TABLE games(game_id INTEGER PRIMARY KEY,season_year INTEGER,game_date TEXT,
              home_team_id INTEGER,away_team_id INTEGER,home_score INTEGER,away_score INTEGER,went_ot INTEGER);
            INSERT INTO teams VALUES(1,'A'),(2,'B'),(3,'Non-FBS');
            INSERT INTO team_membership_by_season VALUES(1,2025,1),(2,2025,1),(3,2025,0);
            INSERT INTO games VALUES
              (1,2025,'2025-09-01',1,2,28,14,0),
              (2,2025,'2025-09-08',2,1,21,24,1),
              (3,2025,'2025-09-15',1,3,35,7,0),
              (4,2025,'2025-09-22',1,2,NULL,NULL,0);
            """)
    def tearDown(self): self.tmp.cleanup()
    def test_only_completed_fbs_games(self):
        games=read_canonical(self.db,2025)
        self.assertEqual([g["id"] for g in games],["1","2"])
        self.assertEqual(len(snapshot(self.db,2025)["ledger"]),4)
    def test_rebuild_parity_and_cutoff(self):
        games=read_canonical(self.db,2025)
        full=snapshot(self.db,2025)
        self.assertEqual(full["standings"],standings(calculate(games,Rules(1.5,.5,3))))
        cutoff=snapshot(self.db,2025,"2025-09-01")
        self.assertEqual(cutoff["awards"],2)
        self.assertEqual(cutoff["standings"],standings(calculate(games,Rules(1.5,.5,3),"2025-09-01")))
    def test_missing_db_fails_closed(self):
        with self.assertRaises(FileNotFoundError):
            read_canonical(Path(self.tmp.name)/"missing.db",2025)

if __name__=="__main__": unittest.main()
