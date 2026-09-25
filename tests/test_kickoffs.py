"""
Kickoff times: display-only data that fixes Saturday-night games showing as Sunday.
Pins that the model's game_date is never touched, nothing is guessed, and no
rating engine reads kickoff times.
"""
import sqlite3
import subprocess
import sys
from pathlib import Path

import fetch_kickoffs as fk
from export_static_data import FIELDS, build_season_payloads
from load_kickoffs import load_kickoffs

REPO = Path(__file__).resolve().parent.parent


def test_rows_keep_cfbd_time_and_tbd_flag_and_skip_games_without_a_time():
    rows = fk.kickoff_rows([{"id": 1, "startDate": "2025-09-28T02:30:00.000Z", "startTimeTBD": False},
                            {"id": 2, "start_date": "2025-09-27T04:00:00.000Z", "start_time_tbd": True},
                            {"id": 3, "startDate": None}], 2025)
    assert rows == [{"game_id": 1, "season_year": 2025, "kickoff_utc": "2025-09-28T02:30:00.000Z", "start_time_tbd": 0},
                    {"game_id": 2, "season_year": 2025, "kickoff_utc": "2025-09-27T04:00:00.000Z", "start_time_tbd": 1}]


def test_a_failed_request_keeps_the_existing_file(tmp_path):
    import requests
    original = requests.get
    (tmp_path / "kickoffs_2025.csv").write_text("existing", encoding="utf-8")
    requests.get = lambda *a, **k: (_ for _ in ()).throw(requests.ConnectionError("offline"))
    try:
        assert fk.write_kickoffs(2025, {}, tmp_path) is None
    finally:
        requests.get = original
    assert (tmp_path / "kickoffs_2025.csv").read_text(encoding="utf-8") == "existing"


def _db(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "k.db"))
    return conn


def test_loader_is_idempotent_and_skips_unparseable_times(tmp_path):
    csv_path = tmp_path / "kickoffs_2025.csv"
    csv_path.write_text("game_id,season_year,kickoff_utc,start_time_tbd\n"
                        "1,2025,2025-09-28T02:30:00.000Z,0\n2,2025,not-a-time,0\n3,2025,2025-09-27T04:00:00.000Z,1\n",
                        encoding="utf-8")
    conn = _db(tmp_path)
    assert load_kickoffs(conn, str(csv_path)) == {"loaded": 2, "skipped": 1, "date_only": False}
    assert load_kickoffs(conn, str(csv_path)) == {"loaded": 2, "skipped": 1, "date_only": False}
    assert conn.execute("SELECT game_id, time_tbd FROM game_kickoffs ORDER BY game_id").fetchall() == [(1, 0), (3, 1)]


def _copy(db_path):
    """In-memory copy, so the test never depends on (or changes) what's in the real database."""
    src = sqlite3.connect(str(db_path))
    mem = sqlite3.connect(":memory:")
    src.backup(mem)
    src.close()
    mem.execute("DROP TABLE IF EXISTS game_kickoffs")
    return mem


def test_export_adds_kickoffs_without_touching_game_date(db_path):
    conn = _copy(db_path)
    gid, season, stored = conn.execute("SELECT game_id, season_year, game_date FROM games "
                                       "WHERE home_score IS NOT NULL AND season_year >= 2001 ORDER BY game_id LIMIT 1").fetchone()
    i = {f: n for n, f in enumerate(FIELDS)}
    row = lambda: next(r for r in build_season_payloads(conn, {"games": []})[season]["games"] if r[0] == gid)
    before = row()
    conn.executescript((REPO / "sql" / "kickoff_tables.sql").read_text(encoding="utf-8"))
    conn.execute("INSERT INTO game_kickoffs VALUES (?, ?, '2011-09-10T23:30:00.000Z', 0, 0)", (gid, season))
    after = row()
    assert before[i["kickoff_utc"]] is None and before[i["time_tbd"]] is None       # not fetched: nothing invented
    assert after[i["kickoff_utc"]] == "2011-09-10T23:30:00.000Z" and after[i["time_tbd"]] == 0
    assert after[i["date"]] == before[i["date"]] == str(stored)[:10]               # the model's date is untouched


def _season_csv(tmp_path, times, tbd=None):
    p = tmp_path / "kickoffs_x.csv"
    p.write_text("game_id,season_year,kickoff_utc,start_time_tbd\n" + "".join(
        f"{n},1985,{t},{(tbd or [0] * len(times))[n]}\n" for n, t in enumerate(times)), encoding="utf-8")
    return str(p)


def test_all_midnight_season_is_dates_only_but_real_midnight_kickoffs_are_kept(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "k.db"))
    assert load_kickoffs(conn, _season_csv(tmp_path, ["1985-09-07T00:00:00.000Z", "1985-09-14T00:00:00.000Z"]))["date_only"]
    assert conn.execute("SELECT SUM(date_only) FROM game_kickoffs").fetchone()[0] == 2
    mixed = ["1985-09-07T00:00:00.000Z", "1985-09-07T19:30:00.000Z"]            # 00:00Z = a real 8 PM ET kickoff
    assert not load_kickoffs(conn, _season_csv(tmp_path, mixed))["date_only"]
    assert conn.execute("SELECT SUM(date_only) FROM game_kickoffs").fetchone()[0] == 0


def test_date_only_seasons_send_no_fake_time_to_the_page(db_path):
    conn = _copy(db_path)
    gid, season = conn.execute("SELECT game_id, season_year FROM games WHERE home_score IS NOT NULL "
                               "AND season_year < 2001 ORDER BY game_id LIMIT 1").fetchone()
    conn.executescript((REPO / "sql" / "kickoff_tables.sql").read_text(encoding="utf-8"))
    conn.execute("INSERT INTO game_kickoffs VALUES (?, ?, '1985-09-07T00:00:00.000Z', 0, 1)", (gid, season))
    i = {f: n for n, f in enumerate(FIELDS)}
    r = next(r for r in build_season_payloads(conn, {"games": []})[season]["games"] if r[0] == gid)
    assert r[i["kickoff_utc"]] is None and r[i["time_tbd"]] == 1


def test_a_table_from_the_previous_version_is_upgraded(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "old.db"))
    conn.execute("CREATE TABLE game_kickoffs (game_id INTEGER PRIMARY KEY, season_year INTEGER NOT NULL, "
                 "kickoff_utc TEXT NOT NULL, time_tbd INTEGER NOT NULL)")
    stats = load_kickoffs(conn, _season_csv(tmp_path, ["1985-09-07T00:00:00.000Z"]))
    assert stats["loaded"] == 1 and stats["date_only"]


def test_no_rating_engine_reads_kickoff_times():
    engines = ["src/build_elo.py", "src/build_coefficients.py", "src/build_hybrid_coefficients.py",
               "src/build_conference_coe2.py", "src/predict_upcoming.py", "src/hfa.py",
               "src/coefficients/select_playoff_field_v2.py", "src/coefficients/select_nit_field.py"]
    for path in engines:
        assert "game_kickoffs" not in (REPO / path).read_text(encoding="utf-8"), path


def test_loading_kickoffs_never_needs_the_http_library():
    code = "import sys; sys.modules['requests'] = None; sys.path.insert(0, 'src'); import load_kickoffs, fetch_kickoffs"
    r = subprocess.run([sys.executable, "-c", code], cwd=str(REPO), capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
