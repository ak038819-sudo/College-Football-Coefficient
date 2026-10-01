"""The public HFA table must use current estimates, not production Elo inputs."""
import sqlite3

from export_dashboard_data import build_current_hfa


def test_current_hfa_export_has_all_rows_and_evidence(tmp_path):
    db = tmp_path / "hfa.db"
    conn = sqlite3.connect(db)
    conn.executescript("""
        CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT);
        CREATE TABLE team_hfa_current (
            team_id INTEGER, season_year INTEGER, as_of TEXT, adjusted_hfa REAL,
            elo_hfa_points REAL, games_used INTEGER, effective_n REAL,
            lambda REAL, elo_points_at_bound INTEGER
        );
        INSERT INTO teams VALUES (1, 'BYU'), (2, 'Utah'), (3, 'Sacramento State');
        INSERT INTO team_hfa_current VALUES
            (1, 2026, '2026-09-28', 1.12, 60.123, 240, 54.37, 0.8, 0),
            (2, 2026, '2026-09-28', 0.98, -5.678, 210, 50, 0.7, 1),
            (3, 2026, '2026-09-28', 1.04, 20, 0, 0, 0, 0);
    """)
    conn.close()
    result = build_current_hfa(str(db))
    assert (result["season"], result["as_of"]) == (2026, "2026-09-28")
    assert [r["team"] for r in result["teams"]] == ["BYU", "Sacramento State", "Utah"]
    assert result["teams"][0]["elo_points"] == 60.1
    assert result["teams"][1]["games"] == 0
    assert result["teams"][2]["points_at_bound"] is True


def test_missing_hfa_table_has_explicit_empty_state(tmp_path):
    db = tmp_path / "old.db"
    sqlite3.connect(db).close()
    assert build_current_hfa(str(db)) == {"as_of": None, "season": None, "teams": []}
