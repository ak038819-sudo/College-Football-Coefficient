"""
Milestone C: game pages. The files they read must carry the model's stored values
exactly -- nothing recomputed, nothing lost -- and the browser must find each team
pair's series in the same file Python put it in.
"""
import re
import sqlite3
from pathlib import Path

from export_static_data import (DETAIL_FIELDS, FIELDS, SERIES_FIELDS, SERIES_SHARDS, build_game_details,
                                build_season_payloads, build_series, series_shard)
from export_team_pages import build_team_pages

REPO = Path(__file__).resolve().parent.parent


def test_series_holds_every_completed_game_once_in_the_right_pair(db_path):
    conn = sqlite3.connect(str(db_path))
    payloads = build_season_payloads(conn, {"games": []})
    shards = build_series(payloads)
    seen = {}
    for k, shard in shards.items():
        for pair, rows in shard["pairs"].items():
            lo, hi = map(int, pair.split("-"))
            assert lo < hi and series_shard(lo, hi) == k
            assert [r[1] for r in rows] == sorted(r[1] for r in rows)          # oldest first
            for r in rows:
                assert sorted((r[SERIES_FIELDS.index("home_id")], r[SERIES_FIELDS.index("away_id")])) == [lo, hi]
                seen[r[0]] = seen.get(r[0], 0) + 1
    completed = conn.execute("SELECT COUNT(*) FROM games WHERE home_score IS NOT NULL").fetchone()[0]
    assert len(seen) == completed and set(seen.values()) == {1}


def test_game_details_are_the_stored_hybrid_values(db_path):
    conn = sqlite3.connect(str(db_path))
    details = build_game_details(conn)
    stored = {(g, t): (hr, p, rt) for g, t, hr, p, rt in conn.execute(
        "SELECT game_id, team_id, hybrid_rating, hybrid_expectation, result_type FROM hybrid_game_ratings")}
    home_of = dict(conn.execute("SELECT game_id, home_team_id FROM games"))
    i = {f: n for n, f in enumerate(DETAIL_FIELDS)}
    checked = 0
    for season, p in details.items():
        for r in p["games"]:
            hr, pe, rt = stored[(r[0], home_of[r[0]])]
            assert r[i["home_hybrid_rating"]] == round(hr, 4) and r[i["home_hybrid_p"]] == round(pe, 4)
            assert r[i["home_result"]] == rt
            checked += 1
    assert checked > 0 and min(details) >= 1985                            # CoE 2.0 needs five prior seasons
    assert {"win_base", "difficulty_alpha"} <= set(next(iter(details.values()))["params"])


def test_browser_and_python_agree_on_the_series_file_for_a_pair():
    shell = (REPO / "ui" / "dashboard_shell.html").read_text(encoding="utf-8")
    assert "(lo * 131 + hi) % (STATIC_MANIFEST.series_shards || 32)" in shell   # same formula as series_shard()
    assert SERIES_SHARDS == 32
    for a, b in ((71, 77), (77, 71), (1, 140), (139, 2)):
        lo, hi = sorted((a, b))
        assert series_shard(a, b) == (lo * 131 + hi) % 32


def test_team_pages_append_kickoff_last_without_moving_existing_positions(db_path):
    conn = sqlite3.connect(str(db_path))
    data = build_team_pages(conn)
    g = data["games"][0]
    assert len(g) == 12                                                    # 11 original positions + kickoff_utc
    shell = (REPO / "ui" / "dashboard_shell.html").read_text(encoding="utf-8")
    assert re.search(r"kickoff_utc: g\[11\]", shell)
