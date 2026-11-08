"""Step 3.4 end-to-end failure and recovery gates."""
import sqlite3
import tempfile
import unittest
from pathlib import Path
from pipeline import run
from shadow_storage import read_shadow

def seed(path):
    with sqlite3.connect(path) as con:
        con.executescript("""
        CREATE TABLE teams(team_id INTEGER PRIMARY KEY,team_name TEXT);
        CREATE TABLE team_membership_by_season(team_id INTEGER,season_year INTEGER,is_fbs INTEGER);
        CREATE TABLE games(game_id INTEGER PRIMARY KEY,season_year INTEGER,game_date TEXT,
          home_team_id INTEGER,away_team_id INTEGER,home_score INTEGER,away_score INTEGER,went_ot INTEGER);
        INSERT INTO teams VALUES(1,'A'),(2,'B'),(3,'C');
        INSERT INTO team_membership_by_season VALUES(1,2025,1),(2,2025,1),(3,2025,0);
        INSERT INTO games VALUES
          (1,2025,'2025-09-01',1,2,28,14,0),
          (2,2025,'2025-09-08',2,1,21,24,1),
          (3,2025,'2025-09-15',1,3,35,7,0);
        """)

class EndToEndGates(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name)
        self.source=self.root/"canonical.db"
        self.shadow=self.root/"shadow.db"
        seed(self.source)
    def tearDown(self):self.tmp.cleanup()
    def test_score_correction_rebuild_updates_awards(self):
        run(self.source,self.shadow,2025,enable=True)
        before=read_shadow(self.shadow,2025)
        with sqlite3.connect(self.source) as con:
            con.execute("UPDATE games SET home_score=7,away_score=14 WHERE game_id=1")
        run(self.source,self.shadow,2025,enable=True)
        after=read_shadow(self.shadow,2025)
        self.assertNotEqual(before,after)
        self.assertEqual(len(after),4)
    def test_invalid_game_preserves_previous_snapshot(self):
        run(self.source,self.shadow,2025,enable=True)
        before=read_shadow(self.shadow,2025)
        with sqlite3.connect(self.source) as con:
            con.execute("UPDATE games SET away_score=home_score WHERE game_id=1")
        with self.assertRaises(ValueError):
            run(self.source,self.shadow,2025,enable=True)
        self.assertEqual(before,read_shadow(self.shadow,2025))
    def test_missing_membership_excludes_game(self):
        with sqlite3.connect(self.source) as con:
            con.execute("DELETE FROM team_membership_by_season WHERE team_id=2")
        with self.assertRaises(ValueError):
            run(self.source,self.shadow,2025,enable=True)
        self.assertFalse(self.shadow.exists())
    def test_missing_source_preserves_previous_snapshot(self):
        run(self.source,self.shadow,2025,enable=True)
        before=read_shadow(self.shadow,2025)
        self.source.unlink()
        with self.assertRaises(FileNotFoundError):
            run(self.source,self.shadow,2025,enable=True)
        self.assertEqual(before,read_shadow(self.shadow,2025))
    def test_non_fbs_game_excluded(self):
        run(self.source,self.shadow,2025,enable=True)
        self.assertEqual(len(read_shadow(self.shadow,2025)),4)
    def test_recovery_after_invalid_game(self):
        run(self.source,self.shadow,2025,enable=True)
        before=read_shadow(self.shadow,2025)
        with sqlite3.connect(self.source) as con:
            con.execute("UPDATE games SET away_score=home_score WHERE game_id=1")
        with self.assertRaises(ValueError):
            run(self.source,self.shadow,2025,enable=True)
        with sqlite3.connect(self.source) as con:
            con.execute("UPDATE games SET away_score=14 WHERE game_id=1")
        run(self.source,self.shadow,2025,enable=True)
        self.assertEqual(before,read_shadow(self.shadow,2025))

if __name__=="__main__":unittest.main()
