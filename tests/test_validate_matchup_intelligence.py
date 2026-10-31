"""Walk-forward validation tests on a self-contained game/advanced SQLite fixture."""
import sqlite3

from src.validate_matchup_intelligence import evaluate_walk_forward


def make_db():
    c = sqlite3.connect(":memory:")
    c.executescript("""
    CREATE TABLE games (
      game_id INTEGER PRIMARY KEY, season_year INTEGER, game_date TEXT,
      home_team_id INTEGER, away_team_id INTEGER,
      home_score INTEGER, away_score INTEGER
    );
    CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT);
    CREATE TABLE team_membership_by_season (
      team_id INTEGER, season_year INTEGER, is_fbs INTEGER
    );
    CREATE TABLE game_team_advanced (
      game_id INTEGER, team_id INTEGER, season_year INTEGER,
      off_success_rate REAL, off_ppa REAL, off_explosiveness REAL
    );
    """)
    for t in range(1, 5):
        c.execute("INSERT INTO teams VALUES (?, ?)", (t, f"Team {t}"))
        c.execute("INSERT INTO team_membership_by_season VALUES (?, 2026, 1)", (t,))
    gid = 100
    # Each team has sufficient prior offensive and opponent-facing observations.
    for week in range(1, 7):
        for home, away in [(1, 2), (3, 4), (1, 3), (2, 4)]:
            day = f"2026-09-{week:02d}"
            c.execute("INSERT INTO games VALUES (?,2026,?,?,?,?,?)",
                      (gid, day, home, away, 20, 17))
            for t, opponent in ((home, away), (away, home)):
                value = 0.40 + 0.02 * (t - opponent) + 0.001 * week
                c.execute("INSERT INTO game_team_advanced VALUES (?,?,?,?,?,?)",
                          (gid, t, 2026, value, value - 0.4, value + 0.2))
            gid += 1
    return c


def test_walk_forward_reports_matched_comparisons():
    db = make_db()
    result = evaluate_walk_forward(
        db, season=2026, metric="success_rate", min_training_games=4)
    assert result["evaluation_dates"] > 0
    assert result["considered_team_games"] > 0
    assert 0 < result["model_coverage"] <= 1
    assert result["opponent_adjusted"]["n"] == result["matched_national_baseline"]["n"]
    assert result["opponent_adjusted"]["n"] == result["matched_team_offense_baseline"]["n"]
    assert result["opponent_adjusted"]["rmse"] >= 0
    db.close()


def test_missing_metric_excluded_not_imputed_as_zero():
    db = make_db()
    db.execute("UPDATE game_team_advanced SET off_ppa=NULL WHERE game_id=100")
    result = evaluate_walk_forward(
        db, season=2026, metric="ppa", min_training_games=4)
    assert result["source_coverage"]["missing_value"] >= 2
    db.close()


def test_no_same_day_training():
    db = make_db()
    # With a larger minimum-training requirement the first dates cannot
    # contribute; future dates do not count toward training eligibility.
    result = evaluate_walk_forward(
        db, season=2026, metric="success_rate", min_training_games=1000)
    assert result["evaluation_dates"] == 0
    assert result["opponent_adjusted"] is None
    db.close()
