import sqlite3
import tempfile
import unittest
from pathlib import Path
from pipeline import run
from shadow_storage import read_shadow

class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        root=Path(self.tmp.name)
        self.source=root/"league.db"
        self.shadow=root/"shadow.db"
        with sqlite3.connect(self.source) as c:
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
              (3,2025,'2025-09-15',1,3,35,7,0);
            """)
    def tearDown(self): self.tmp.cleanup()
    def test_disabled_by_default(self):
        with self.assertRaises(PermissionError):
            run(self.source,self.shadow,2025)
        self.assertFalse(self.shadow.exists())
    def test_end_to_end_and_replay(self):
        first=run(self.source,self.shadow,2025,enable=True)
        self.assertEqual(first["awards"],4)
        saved=read_shadow(self.shadow,2025)
        run(self.source,self.shadow,2025,enable=True)
        self.assertEqual(saved,read_shadow(self.shadow,2025))
    def test_cutoff_and_restore(self):
        run(self.source,self.shadow,2025,enable=True)
        run(self.source,self.shadow,2025,"2025-09-01",enable=True)
        self.assertEqual(len(read_shadow(self.shadow,2025)),2)
        run(self.source,self.shadow,2025,enable=True)
        self.assertEqual(len(read_shadow(self.shadow,2025)),4)
    def test_reject_same_database(self):
        with self.assertRaises(ValueError):
            run(self.source,self.source,2025,enable=True)
    def test_no_source_mutation(self):
        with sqlite3.connect(self.source) as c:
            before=c.execute("SELECT * FROM games ORDER BY game_id").fetchall()
        run(self.source,self.shadow,2025,enable=True)
        with sqlite3.connect(self.source) as c:
            after=c.execute("SELECT * FROM games ORDER BY game_id").fetchall()
        self.assertEqual(before,after)

if __name__=="__main__":unittest.main()
