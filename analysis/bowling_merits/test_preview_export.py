"""Step 4 preview-export contract and frontend safety checks."""
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from engine import Rules,calculate
from preview import build_preview
from shadow_storage import sync_shadow

ROOT=Path(__file__).resolve().parent
PAGE=ROOT.parent.parent/"ui"/"bowling-merits-preview.html"
GAMES=[
 dict(id="1",season=2025,date="2025-09-01",home="A",away="B",hs=28,as_=14,ot=False),
 dict(id="2",season=2025,date="2025-09-08",home="B",away="A",hs=21,as_=24,ot=True),
]
# More teams than the retired top-25 cap, so a reinstated cap truncates and
# the full-coverage assertions below fail instead of passing vacuously.
WIDE_TEAMS=40
WIDE_GAMES=[dict(id=f"w{i}",season=2025,date="2025-09-%02d"%(i+1),
                 home=f"H{i:02d}",away=f"V{i:02d}",hs=20+i,as_=10,ot=False)
            for i in range(1,WIDE_TEAMS//2+1)]

class PreviewExportTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.db=Path(self.tmp.name)/"shadow.db"
        self.output=Path(self.tmp.name)/"bowling-merits-preview.json"
        sync_shadow(self.db,calculate(GAMES,Rules(1.5,.5,3)),2025,True)
        self.wide_db=Path(self.tmp.name)/"shadow_wide.db"
        sync_shadow(self.wide_db,calculate(WIDE_GAMES,Rules(1.5,.5,3)),2025,True)
    def tearDown(self):self.tmp.cleanup()
    def test_cli_export_matches_contract(self):
        subprocess.run([sys.executable,str(ROOT/"preview.py"),"--shadow-db",str(self.db),
                        "--season","2025","--output",str(self.output)],
                       check=True,capture_output=True,text=True)
        exported=json.loads(self.output.read_text())
        self.assertEqual(exported,build_preview(self.db,2025))
        self.assertEqual(exported["status"],"research_preview")
        self.assertTrue(exported["production_merits_unchanged"])
    def test_full_export_includes_every_team_and_reconciles_awards(self):
        data=build_preview(self.db,2025)
        self.assertEqual({row["team"] for row in data["standings"]},{"A","B"})
        self.assertEqual(data["teams"],len(data["standings"]))
        self.assertEqual(sum(row["games"] for row in data["standings"]),2*len(GAMES))
        self.assertEqual(sum(len(row["awards"]) for row in data["standings"]),2*len(GAMES))
        self.assertEqual(data["standings"],build_preview(self.db,2025,limit=None)["standings"])
        self.assertEqual(len(build_preview(self.db,2025,limit=1)["standings"]),1)
    def test_full_export_is_not_capped_at_25_teams(self):
        data=build_preview(self.wide_db,2025)
        self.assertEqual(len(data["standings"]),WIDE_TEAMS)
        self.assertEqual(data["teams"],WIDE_TEAMS)
        self.assertEqual(len({row["team"] for row in data["standings"]}),WIDE_TEAMS)
        self.assertEqual(sum(len(row["awards"]) for row in data["standings"]),
                         2*len(WIDE_GAMES))
        self.assertEqual(len(build_preview(self.wide_db,2025,limit=25)["standings"]),25)
    def test_cli_default_is_not_capped_at_25_teams(self):
        subprocess.run([sys.executable,str(ROOT/"preview.py"),"--shadow-db",str(self.wide_db),
                        "--season","2025","--output",str(self.output)],
                       check=True,capture_output=True,text=True)
        data=json.loads(self.output.read_text())
        self.assertEqual(len(data["standings"]),WIDE_TEAMS)
        self.assertEqual(data["teams"],WIDE_TEAMS)
    def test_cli_default_is_full_export(self):
        subprocess.run([sys.executable,str(ROOT/"preview.py"),"--shadow-db",str(self.db),
                        "--season","2025","--output",str(self.output)],
                       check=True,capture_output=True,text=True)
        data=json.loads(self.output.read_text())
        self.assertEqual(len(data["standings"]),data["teams"])
        self.assertEqual(sum(len(row["awards"]) for row in data["standings"]),2*len(GAMES))
    def test_export_is_deterministic(self):
        args=[sys.executable,str(ROOT/"preview.py"),"--shadow-db",str(self.db),
              "--season","2025","--output",str(self.output)]
        subprocess.run(args,check=True,capture_output=True)
        first=self.output.read_bytes()
        subprocess.run(args,check=True,capture_output=True)
        self.assertEqual(first,self.output.read_bytes())
    def test_missing_season_does_not_create_export(self):
        result=subprocess.run([sys.executable,str(ROOT/"preview.py"),"--shadow-db",str(self.db),
                               "--season","2024","--output",str(self.output)],
                              capture_output=True,text=True)
        self.assertNotEqual(result.returncode,0)
        self.assertFalse(self.output.exists())
    def test_frontend_requires_explicit_preview_flag(self):
        page=PAGE.read_text(encoding="utf-8")
        self.assertIn('params.get("preview")==="1"',page)
        self.assertIn('data.status!=="research_preview"',page)
        self.assertIn('textContent',page)
        self.assertNotIn('innerHTML',page)

if __name__=="__main__":unittest.main()
