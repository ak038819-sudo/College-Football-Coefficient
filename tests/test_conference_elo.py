"""Conference Elo uses historical membership and shows incomplete coverage."""
import sqlite3

from export_dashboard_data import build_conference_elo_by_year


def test_conference_elo_respects_season_membership_and_missing_ratings(tmp_path):
    db = tmp_path / "league.db"
    conn = sqlite3.connect(db)
    conn.executescript("""
        CREATE TABLE teams(team_id INTEGER PRIMARY KEY, team_name TEXT);
        CREATE TABLE team_membership_by_season(team_id INTEGER, season_year INTEGER, conference_real TEXT);
        INSERT INTO teams VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D');
        INSERT INTO team_membership_by_season VALUES
          (1, 2025, 'Old League'), (2, 2025, 'Old League'),
          (3, 2025, 'New League'), (4, 2025, 'FBS Independents'),
          (1, 2026, 'New League'), (2, 2026, 'Old League'),
          (3, 2026, 'New League'), (4, 2026, 'New League');
    """)
    conn.close()
    ratings = {
        2025: [{"team": "A", "elo": 1800}, {"team": "B", "elo": 1600},
               {"team": "C", "elo": 1700}, {"team": "D", "elo": 1900}],
        2026: [{"team": "A", "elo": 1750}, {"team": "B", "elo": 1650},
               {"team": "C", "elo": 1550}],
    }
    result = build_conference_elo_by_year(str(db), ratings)
    assert result["2025"] == [
        {"conference": "New League", "elo": 1700.0, "rated": 1, "members": 1},
        {"conference": "Old League", "elo": 1700.0, "rated": 2, "members": 2},
    ]
    assert result["2026"] == [
        {"conference": "New League", "elo": 1650.0, "rated": 2, "members": 3},
        {"conference": "Old League", "elo": 1650.0, "rated": 1, "members": 1},
    ]
