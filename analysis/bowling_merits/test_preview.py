import tempfile
import unittest
from pathlib import Path
from engine import Rules,calculate
from preview import build_preview
from shadow_storage import sync_shadow

GAMES=[
 dict(id="1",season=2025,date="2025-09-01",home="A",away="B",hs=28,as_=14,ot=False),
 dict(id="2",season=2025,date="2025-09-08",home="B",away="A",hs=21,as_=24,ot=True),
]
class PreviewTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.db=Path(self.tmp.name)/"shadow.db"
        self.ledger=calculate(GAMES,Rules(1.5,.5,3))
        sync_shadow(self.db,self.ledger,2025,enabled=True)
    def tearDown(self):self.tmp.cleanup()
    def test_standings_match_ledger(self):
        data=build_preview(self.db,2025,limit=None)
        self.assertEqual(data["teams"],2)
        for team in data["standings"]:
            self.assertAlmostEqual(team["merits"],round(sum(x["merits"] for x in self.ledger if x["team"]==team["team"]),6))
            self.assertEqual(team["games"],2)
    def test_provisional_and_overtime_details(self):
        data=build_preview(self.db,2025)
        b=next(t for t in data["standings"] if t["team"]=="B")
        self.assertTrue(b["provisional"])
        self.assertEqual(b["ot_losses"],1)
        self.assertEqual(next(a for a in b["awards"] if a["outcome"]=="OT L")["merits"],.5)
    def test_limit_preserves_full_rank(self):
        data=build_preview(self.db,2025,limit=1)
        self.assertEqual(len(data["standings"]),1)
        self.assertEqual(data["teams"],2)
        self.assertEqual(data["standings"][0]["rank"],1)
    def test_missing_season_fails_closed(self):
        with self.assertRaises(ValueError):build_preview(self.db,2024)
    def test_missing_shadow_fails_closed(self):
        with self.assertRaises(FileNotFoundError):
            build_preview(Path(self.tmp.name)/"missing.db",2025)
    def test_read_only(self):
        before=self.db.read_bytes()
        build_preview(self.db,2025)
        self.assertEqual(before,self.db.read_bytes())

if __name__=="__main__":unittest.main()
