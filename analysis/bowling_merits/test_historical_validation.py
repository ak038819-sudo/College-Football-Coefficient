"""Step 2 historical comparison contract tests."""
import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent))
from historical_validation import MODELS, YEARS, run

ROOT=Path(__file__).resolve().parents[2]

class HistoricalValidationTests(unittest.TestCase):
    def test_season_scope(self):
        self.assertEqual(YEARS,tuple(range(2014,2020))+tuple(range(2021,2026)))
        self.assertNotIn(2020,YEARS)

    def test_model_parameters(self):
        self.assertEqual((MODELS["control_k3"].floor,MODELS["control_k3"].loss_penalty,MODELS["control_k3"].future),(2,0,3))
        self.assertEqual((MODELS["challenger_k3"].floor,MODELS["challenger_k3"].loss_penalty,MODELS["challenger_k3"].future),(1.5,.5,3))
        self.assertEqual(MODELS["challenger_k4"].future,4)

    def test_missing_seasons_fail_loudly(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(FileNotFoundError,"2014"):
                run(root=Path(tmp))

    def test_all_required_season_files_present(self):
        missing=[year for year in YEARS if not (ROOT/"data"/"raw"/f"games_{year}.csv").is_file()]
        self.assertFalse(missing,f"Missing historical game CSVs: {missing}")

if __name__=="__main__": unittest.main()
