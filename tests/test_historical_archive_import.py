"""Historical archive ingestion contract tests; no network or production DB."""
import importlib.util
import sqlite3
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/import_historical_archive.py"
spec = importlib.util.spec_from_file_location("historical_import", SCRIPT)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def sample(gid=100, home=3, away=3):
    return {"id": gid, "season": 1901, "seasonType": "regular",
            "homeTeam": "Historical College", "awayTeam": "Old University",
            "homePoints": home, "awayPoints": away,
            "neutralSite": None, "homeClassification": None}


def test_ties_and_unknown_classification_are_preserved():
    with sqlite3.connect(":memory:") as db:
        mod.setup(db)
        mod.store(db, 1901, "regular", [sample()])
        row = db.execute("""SELECT home_score, away_score, neutral_site,
                            home_classification, home_name_raw
                            FROM archive_games""").fetchone()
        assert row == (3, 3, None, None, "Historical College")


def test_repeat_import_is_idempotent_and_refreshes_source():
    with sqlite3.connect(":memory:") as db:
        mod.setup(db)
        mod.store(db, 1901, "regular", [sample()])
        mod.store(db, 1901, "regular", [sample(home=4)])
        assert db.execute("SELECT COUNT(*) FROM archive_games").fetchone()[0] == 1
        assert db.execute("SELECT home_score FROM archive_games").fetchone()[0] == 4


def test_checkpoint_failure_rolls_back_the_games_insert(monkeypatch):
    """The games insert and its checkpoint share one transaction.

    Without that transaction the games would survive a checkpoint that never
    landed, leaving rows no fetch record accounts for. The failure is forced
    after executemany by making the checkpoint's timestamp raise.
    """
    class Exploding:
        @staticmethod
        def now(tz=None):
            raise RuntimeError("clock unavailable")

    with sqlite3.connect(":memory:") as db:
        mod.setup(db)
        monkeypatch.setattr(mod, "datetime", Exploding)
        try:
            mod.store(db, 1901, "regular", [sample()])
        except RuntimeError:
            pass
        else:
            raise AssertionError("A failing checkpoint should propagate")
        assert db.execute("SELECT COUNT(*) FROM archive_games").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM archive_fetches").fetchone()[0] == 0


def test_failed_payload_cannot_partially_import():
    with sqlite3.connect(":memory:") as db:
        mod.setup(db)
        try:
            mod.store(db, 1901, "regular", [sample(), {"homeTeam": "missing id"}])
        except ValueError:
            pass
        else:
            raise AssertionError("Missing source ID should fail")
        assert db.execute("SELECT COUNT(*) FROM archive_games").fetchone()[0] == 0


def test_checkpoint_resume_and_sparse_seasons():
    calls = []
    def fetch(year, kind, key):
        calls.append((year, kind))
        return [] if year == 1871 else [dict(sample(gid=year * 10 + (kind == "postseason")), season=year, seasonType=kind)]
    with sqlite3.connect(":memory:") as db:
        mod.run(db, 1870, 1871, "fake", pause=0, fetcher=fetch)
        mod.run(db, 1870, 1871, "fake", pause=0, fetcher=fetch)
        assert len(calls) == 4
        assert db.execute("SELECT COUNT(*) FROM archive_fetches").fetchone()[0] == 4
        assert db.execute("SELECT COUNT(*) FROM archive_games").fetchone()[0] == 2


def test_duplicate_ids_rejected_before_any_write():
    with sqlite3.connect(":memory:") as db:
        mod.setup(db)
        try:
            mod.store(db, 1901, "regular", [sample(), sample()])
        except ValueError:
            pass
        else:
            raise AssertionError("Duplicate IDs should fail")
        assert db.execute("SELECT COUNT(*) FROM archive_games").fetchone()[0] == 0


def test_wrong_season_and_type_rejected():
    with sqlite3.connect(":memory:") as db:
        mod.setup(db)
        for bad in (dict(sample(), season=1902), dict(sample(), seasonType="postseason")):
            try:
                mod.store(db, 1901, "regular", [bad])
            except ValueError:
                pass
            else:
                raise AssertionError("Mismatched season metadata should fail")
        assert db.execute("SELECT COUNT(*) FROM archive_games").fetchone()[0] == 0
