"""
Milestone 6: national championships come from the hand-maintained
data/reference/national_champions.csv -- never inferred from results.
These tests validate that file (so a typo fails CI, not the website) and pin
the counting convention: shared seasons, vacated titles listed but uncounted.
"""
import csv
import sqlite3
from collections import defaultdict

import pytest

from export_team_pages import CHAMPIONS_PATH, load_national_champions, title_history


def _rows():
    with open(CHAMPIONS_PATH, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_every_season_has_a_champion_under_its_eras_rules():
    by_season = defaultdict(list)
    for r in _rows():
        by_season[int(r["season_year"])].append(r)
    first, last = min(by_season), max(by_season)
    assert first == 1980
    for season in range(first, last + 1):
        rows = by_season.get(season, [])
        systems = [r["system"] for r in rows]
        assert any(r["status"] == "awarded" for r in rows), f"{season}: no awarded title"
        if season >= 2014:
            assert systems.count("cfp") == 1, f"{season}: need exactly one CFP champion"
        elif season >= 1998:
            assert systems.count("bcs") == 1, f"{season}: need exactly one BCS title-game row"
        else:
            assert systems.count("ap") == 1 and systems.count("coaches") == 1, f"{season}: need one AP and one Coaches row"


def test_file_validates_against_the_team_table(db_path):
    conn = sqlite3.connect(str(db_path))
    champions = load_national_champions(conn)       # raises on any unknown team / bad system / era
    conn.close()
    assert len(champions) == len(_rows())


def test_cfp_champions_won_the_final_cfp_game_in_loaded_data(db_path):
    conn = sqlite3.connect(str(db_path))
    champions = load_national_champions(conn)
    checked = 0
    for c in champions:
        if c["system"] != "cfp":
            continue
        g = conn.execute("""SELECT home_team_id, away_team_id, home_score, away_score FROM games
                            WHERE season_year = ? AND game_phase = 'cfp' AND home_score IS NOT NULL
                            ORDER BY game_date DESC, game_id DESC LIMIT 1""", (c["season"],)).fetchone()
        if g is None:
            continue   # that season isn't loaded in this database
        winner = g[0] if g[2] > g[3] else g[1]
        assert winner == c["team_id"], f"{c['season']}: file says team {c['team_id']}, data says {winner}"
        checked += 1
    conn.close()
    if not checked:
        pytest.skip("no CFP-era title games loaded")


def test_counting_convention():
    champs = [
        {"season": 2004, "team_id": 7, "system": "bcs", "status": "vacated", "notes": ""},
        {"season": 2004, "team_id": 7, "system": "ap", "status": "awarded", "notes": ""},
        {"season": 2003, "team_id": 7, "system": "ap", "status": "awarded", "notes": ""},
        {"season": 2003, "team_id": 8, "system": "bcs", "status": "awarded", "notes": ""},
        {"season": 1999, "team_id": 9, "system": "bcs", "status": "vacated", "notes": ""},
    ]
    h = title_history(champs)
    assert h[7]["title_count"] == 2                       # 2004 counts via the AP despite the vacated BCS row
    assert h[8]["title_count"] == 1
    assert h[9]["title_count"] == 0                       # vacated-only: listed, never counted
    shared = {(t[0], t[1]): t[3] for t in h[7]["titles"]}
    assert shared[(2003, "ap")] is True and shared[(2004, "ap")] is False


def test_bad_rows_fail_loudly(tmp_path):
    conn = sqlite3.connect(":memory:")
    conn.executescript("""CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT);
                          CREATE TABLE team_aliases (alias TEXT PRIMARY KEY, team_name TEXT);
                          INSERT INTO teams VALUES (1, 'Alabama');""")
    header = "season_year,team_name,system,title_type,status,notes\n"
    for bad in ["2020,Alabama,bcs,championship_game,awarded,\n",      # BCS outside its era
                "2020,Not A Team,cfp,championship_game,awarded,\n",    # unknown team
                "2020,Alabama,cfp,final_poll,awarded,\n",              # system/title_type mismatch
                "2020,Alabama,cfp,championship_game,stripped,\n"]:     # invalid status
        p = tmp_path / "c.csv"
        p.write_text(header + bad, encoding="utf-8")
        with pytest.raises(ValueError):
            load_national_champions(conn, p)
