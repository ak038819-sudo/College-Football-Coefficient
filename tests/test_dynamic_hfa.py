"""An opt-in Elo replay can use frozen team HFA without touching live Elo."""
import sqlite3

import pytest

from build_elo import expected_result, run_elo
import compare_dynamic_hfa as experiment
from compare_dynamic_hfa import entering_home_bonuses, verify_flat_reference
from srdiff import build_layer

CFG = {"initial_rating": 1500, "scale": 400, "k": 20, "home_field": 50,
       "offseason_retention": 0.8, "mov_c": 2.2, "mov_d": 0.001}


def game(gid, season, date, home, away, neutral=False):
    return {"game_id": gid, "season_year": season, "game_date": date,
            "home_team_id": home, "away_team_id": away, "home_name": f"Team {home}",
            "away_name": f"Team {away}", "home_score": 24, "away_score": 17,
            "neutral_site": int(neutral)}


def test_opt_in_home_bonus_uses_only_the_home_team_and_neutral_is_unchanged():
    games = [game(1, 2026, "2026-09-01", 1, 2), game(2, 2026, "2026-09-02", 3, 4, True)]
    flat = run_elo(games, CFG)[0]
    calls = []
    def bonus(g):
        calls.append(g["home_team_id"])
        return 100
    dynamic = run_elo(games, CFG, home_bonus_for_game=bonus)[0]
    assert calls == [1], "neutral games do not consult or apply a home bonus"
    assert flat[0][4] == pytest.approx(expected_result(1550, 1500, 400))
    assert dynamic[0][4] == pytest.approx(expected_result(1600, 1500, 400))
    assert dynamic[1][4] == pytest.approx(1 - dynamic[0][4])
    assert dynamic[2][4] == flat[2][4] == 0.5
    assert dynamic[0][6] == pytest.approx(-dynamic[1][6])
    assert run_elo(games, CFG)[0] == flat, "the default production path stays flat"


def test_entering_estimates_reject_future_and_missing_teams():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE team_hfa_by_season (season_year INTEGER, team_id INTEGER, as_of TEXT, "
                 "elo_hfa_points REAL, elo_points_at_bound INTEGER)")
    games = [game(1, 2025, "2025-09-01", 1, 2), game(2, 2026, "2026-09-01", 1, 2)]
    conn.execute("INSERT INTO team_hfa_by_season VALUES (2026, 1, '2026-09-02', 80, 0)")
    with pytest.raises(ValueError, match="after its first game"):
        entering_home_bonuses(conn, games, 50)
    conn.execute("UPDATE team_hfa_by_season SET as_of='2026-08-31'")
    bonus, bounded = entering_home_bonuses(conn, games, 50)
    assert bonus(games[0]) == 50  # no historical baseline in the first season
    assert bonus(games[1]) == 80
    assert not bounded
    with pytest.raises(ValueError, match="Missing entering-season HFA"):
        bonus(game(3, 2026, "2026-09-02", 3, 2))


def test_bounded_conversion_uses_flat_fallback_not_a_clipped_extreme():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE team_hfa_by_season (season_year INTEGER, team_id INTEGER, as_of TEXT, "
                 "elo_hfa_points REAL, elo_points_at_bound INTEGER)")
    conn.execute("INSERT INTO team_hfa_by_season VALUES (2026, 1, '2026-08-31', 400, 1)")
    games = [game(1, 2025, "2025-09-01", 1, 2), game(2, 2026, "2026-09-01", 1, 2)]
    bonus, bounded = entering_home_bonuses(conn, games, 50)
    assert bonus(games[1]) == 50
    assert bounded == {(2026, 1)}


def test_flat_reference_mismatch_stops_comparison():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE elo_game_history (game_id INTEGER, team_id INTEGER, pregame_elo REAL, elo_expectation REAL)")
    rows = run_elo([game(1, 2025, "2025-09-01", 1, 2)], CFG)[0]
    conn.executemany("INSERT INTO elo_game_history VALUES (?,?,?,?)", [(r[0], r[1], r[2], r[4]) for r in rows])
    verify_flat_reference(conn, rows)
    conn.execute("UPDATE elo_game_history SET elo_expectation=0.9 WHERE team_id=1")
    with pytest.raises(ValueError, match="not this flat reference"):
        verify_flat_reference(conn, rows)


def test_comparison_is_read_only_and_reports_matched_forward_games(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE teams (team_id INTEGER, team_name TEXT);
        INSERT INTO teams VALUES (1,'Alpha'), (2,'Bravo');
        CREATE TABLE games (game_id INTEGER, season_year INTEGER, game_date TEXT,
            home_team_id INTEGER, away_team_id INTEGER, home_score INTEGER,
            away_score INTEGER, neutral_site INTEGER, went_ot INTEGER);
        CREATE TABLE elo_game_history (game_id INTEGER, team_id INTEGER,
            pregame_elo REAL, elo_expectation REAL);
        CREATE TABLE team_hfa_by_season (season_year INTEGER, team_id INTEGER,
            as_of TEXT, elo_hfa_points REAL, elo_points_at_bound INTEGER);
        INSERT INTO games VALUES (1,2025,'2025-09-01',1,2,24,17,0,0),
                                 (2,2026,'2026-09-01',2,1,24,17,0,0);
        INSERT INTO team_hfa_by_season VALUES (2026,2,'2026-09-01',100,0);
    """)
    conn.row_factory = sqlite3.Row
    from build_elo import fetch_games_chronological
    games = fetch_games_chronological(conn)
    layer = build_layer({"modifier": "mov", "fallback": "mov", "beta": 1.0,
                         "m_min": 0.5, "m_max": 1.5, "model_path": None}, CFG)
    rows = run_elo(games, CFG, layer)[0]
    conn.executemany("INSERT INTO elo_game_history VALUES (?,?,?,?)",
                     [(r[0], r[1], r[2], r[4]) for r in rows])
    monkeypatch.setattr(experiment, "performance_layer", lambda *a, **k: (layer, {}))
    result = experiment.compare(conn, CFG, 2026)
    assert result["overall"]["games"] == 1
    assert result["overall"]["flat_brier"] != result["overall"]["dynamic_brier"]
    assert result["by_season"]["2026"]["games"] == 1
    assert conn.execute("SELECT COUNT(*) FROM elo_game_history").fetchone()[0] == 4
