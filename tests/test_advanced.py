"""
Milestone 7: CFBD advanced season stats -- display data only.
Pins: nested-field flattening, a real 0 is not "not reported", no requests for
pre-2001 seasons, alias mapping, direction-aware ranks, and that no rating
engine reads the table.
"""
import csv
import sqlite3
from pathlib import Path

import fetch_cfbd_advanced as fa
from export_team_pages import ADVANCED_DISPLAY, build_advanced
from load_advanced import load_advanced

REPO = Path(__file__).resolve().parent.parent
PAYLOAD = [
    {"team": "Alpha", "conference": "X",
     "offense": {"plays": 900, "ppa": 0.30, "successRate": 0.50, "havoc": {"total": 0.10}},
     "defense": {"plays": 850, "ppa": -0.10, "havoc": {"total": 0.20}}},
    {"team": "Bravo Alias", "conference": "X",
     "offense": {"plays": 800, "ppa": 0.0, "successRate": None},          # real zero + unreported
     "defense": {"plays": 820, "ppa": 0.15, "havoc": {"total": 0.12}}},
]


def test_flattening_keeps_zero_and_blanks_unreported():
    rows = {r["team"]: r for r in fa.advanced_rows(PAYLOAD, 2025)}
    assert rows["Alpha"]["off_ppa"] == 0.30 and rows["Alpha"]["def_havoc"] == 0.20
    assert rows["Bravo Alias"]["off_ppa"] == 0.0
    assert rows["Bravo Alias"]["off_success_rate"] == ""
    assert rows["Bravo Alias"]["off_explosiveness"] == ""
    assert all(r["garbage_time_excluded"] == 1 for r in rows.values())


def test_pre_2001_seasons_never_call_the_api():
    original = fa.requests.get
    fa.requests.get = lambda *a, **k: (_ for _ in ()).throw(AssertionError("API called for a pre-2001 season"))
    try:
        assert fa.fetch_advanced(2000, {}) is None
    finally:
        fa.requests.get = original


def _league(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "a.db"))
    conn.executescript("""CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT);
                          CREATE TABLE team_aliases (alias TEXT PRIMARY KEY, team_name TEXT);
                          INSERT INTO teams VALUES (1, 'Alpha'), (2, 'Bravo');
                          INSERT INTO team_aliases VALUES ('Bravo Alias', 'Bravo');""")
    path = tmp_path / "advanced_2025.csv"
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fa.CSV_FIELDS)
        w.writeheader()
        w.writerows(fa.advanced_rows(PAYLOAD + [{"team": "Unknown U", "offense": {}, "defense": {}}], 2025))
    return conn, str(path)


def test_loader_aliases_nulls_and_idempotency(tmp_path):
    conn, path = _league(tmp_path)
    stats = load_advanced(conn, path)
    load_advanced(conn, path)
    assert stats == {"loaded": 2, "unmatched": ["Unknown U"]}
    assert conn.execute("SELECT COUNT(*) FROM team_season_advanced").fetchone()[0] == 2
    assert conn.execute("SELECT off_ppa, off_success_rate FROM team_season_advanced WHERE team_id = 2").fetchone() == (0.0, None)


def test_ranks_point_the_right_way_and_skip_unreported(tmp_path):
    conn, path = _league(tmp_path)
    load_advanced(conn, path)
    adv = build_advanced(conn)
    idx = {m[0]: i for i, m in enumerate(ADVANCED_DISPLAY)}
    a, b = adv["teams"]["1"]["2025"], adv["teams"]["2"]["2025"]
    assert a[idx["off_ppa"]][1] == 1 and b[idx["off_ppa"]][1] == 2          # higher offensive EPA is better
    assert a[idx["def_ppa"]][1] == 1 and b[idx["def_ppa"]][1] == 2          # LOWER EPA allowed is better
    assert a[idx["def_havoc"]][1] == 1                                      # higher havoc is better
    assert b[idx["off_success_rate"]] == [None, None, None]                 # unreported: no value, no rank
    assert a[idx["off_success_rate"]][1:] == [1, 1]                         # ranked among reporters only


def test_no_rating_engine_reads_advanced_stats():
    engines = ["src/build_elo.py", "src/build_coefficients.py", "src/build_hybrid_coefficients.py",
               "src/build_conference_coe2.py", "src/predict_upcoming.py",
               "src/coefficients/select_playoff_field_v2.py", "src/coefficients/select_nit_field.py"]
    for path in engines:
        assert "team_season_advanced" not in (REPO / path).read_text(encoding="utf-8"), path
