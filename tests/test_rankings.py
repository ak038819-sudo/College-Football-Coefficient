"""
Milestone 5: AP / CFP polls. Display data only -- these tests pin the capture
rules (AP + CFP only, a real 0 is not "not reported"), CFBD-name mapping through
the alias table, "latest release" selection, and that no rating engine reads polls.
"""
import csv
import sqlite3
from pathlib import Path

from export_dashboard_data import build_polls
from fetch_cfbd_games import poll_key, rankings_rows
from load_rankings import load_rankings

REPO = Path(__file__).resolve().parent.parent


def test_only_ap_and_cfp_polls_are_captured():
    assert poll_key("AP Top 25") == "ap"
    assert poll_key("Playoff Committee Rankings") == "cfp"
    assert poll_key("Coaches Poll") is None
    assert poll_key("FCS Coaches Poll") is None


def test_zero_votes_stay_zero_and_unreported_stays_blank():
    rows = rankings_rows([{"seasonType": "regular", "week": 4, "polls": [
        {"poll": "AP Top 25", "ranks": [{"rank": 1, "school": "A", "firstPlaceVotes": 40, "points": 1500},
                                        {"rank": 2, "school": "B", "firstPlaceVotes": 0, "points": 1400}]},
        {"poll": "Coaches Poll", "ranks": [{"rank": 1, "school": "B"}]},
        {"poll": "Playoff Committee Rankings", "ranks": [{"rank": 1, "school": "A"}]}]}], 2026)
    by = {(r["poll"], r["school"]): r for r in rows}
    assert set(by) == {("ap", "A"), ("ap", "B"), ("cfp", "A")}
    assert by[("ap", "B")]["first_place_votes"] == 0
    assert by[("cfp", "A")]["first_place_votes"] == "" and by[("cfp", "A")]["points"] == ""


def _db(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "r.db"))
    conn.executescript("""
        CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT NOT NULL);
        CREATE TABLE team_aliases (alias TEXT PRIMARY KEY, team_name TEXT NOT NULL);
        INSERT INTO teams VALUES (1, 'Indiana'), (2, 'Miami (FL)');
        INSERT INTO team_aliases VALUES ('Miami', 'Miami (FL)');
    """)
    return conn


def _csv(tmp_path, rows):
    p = tmp_path / "rankings_2026.csv"
    with p.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["season_year", "season_type", "week", "poll", "rank", "school", "conference", "first_place_votes", "points"])
        w.writerows(rows)
    return str(p)


ROWS = [
    [2026, "regular", 4, "ap", 1, "Indiana", "", 40, 1500],
    [2026, "regular", 4, "ap", 2, "Miami", "", 0, 1400],
    [2026, "regular", 9, "ap", 1, "Miami", "", 30, 1500],
    [2026, "regular", 9, "ap", 2, "Some FCS School", "", "", ""],
    [2026, "postseason", 1, "ap", 1, "Indiana", "", 60, 1550],
]


def test_loader_maps_aliases_keeps_unmatched_and_is_idempotent(tmp_path):
    conn = _db(tmp_path)
    path = _csv(tmp_path, ROWS)
    stats = load_rankings(conn, path)
    load_rankings(conn, path)
    assert conn.execute("SELECT COUNT(*) FROM poll_rankings").fetchone()[0] == len(ROWS)
    assert stats["unmatched"] == ["Some FCS School"]
    assert conn.execute("SELECT team_id FROM poll_rankings WHERE school='Miami' AND week=4").fetchone()[0] == 2
    fcs = conn.execute("SELECT team_id, first_place_votes, points FROM poll_rankings WHERE school='Some FCS School'").fetchone()
    assert fcs == (None, None, None)
    assert conn.execute("SELECT first_place_votes FROM poll_rankings WHERE school='Miami' AND week=4").fetchone()[0] == 0


def test_latest_release_wins_and_missing_cfp_is_absent_not_empty(tmp_path):
    conn = _db(tmp_path)
    load_rankings(conn, _csv(tmp_path, ROWS))
    conn.close()
    polls = build_polls(str(tmp_path / "r.db"), 2026)
    assert (polls["ap"]["season_type"], polls["ap"]["week"]) == ("postseason", 1)   # final beats week 9
    assert polls["ap"]["rows"] == [[1, 1, "Indiana", 60, 1550]]
    assert polls["cfp"] is None
    assert build_polls(str(tmp_path / "r.db"), 2025) == {"season": 2025, "ap": None, "cfp": None}


def test_no_rating_engine_reads_polls():
    engines = ["src/build_elo.py", "src/build_coefficients.py", "src/build_hybrid_coefficients.py",
               "src/build_conference_coe2.py", "src/predict_upcoming.py",
               "src/coefficients/select_playoff_field_v2.py", "src/coefficients/select_nit_field.py"]
    for path in engines:
        assert "poll_rankings" not in (REPO / path).read_text(encoding="utf-8"), path
