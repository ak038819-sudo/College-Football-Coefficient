"""
Milestone 4 (homepage): the sidebar boards must agree with the authoritative
outputs -- the Conference CoE board with the ordering that sets playoff bids,
the Elo Top 25 with the Elo engine's current ratings.
"""
import sqlite3

import pytest

from export_dashboard_data import build_conference_board, years_with_membership_data
from predict_upcoming import build_upcoming, current_ratings, elo_config
from select_playoff_field_v2 import load_conference_coe_rank


def test_conference_board_is_the_bid_setting_order(db_path):
    board = build_conference_board(str(db_path), years_with_membership_data(str(db_path)))
    if board["season"] is None:
        pytest.skip("no conference rankings computable in this database")
    conn = sqlite3.connect(str(db_path))
    expected = load_conference_coe_rank(conn, board["season"])
    real_confs = {r[0] for r in conn.execute(
        "SELECT DISTINCT conference FROM conference_standings_by_year WHERE season_year = ?", (board["season"],))}
    conn.close()
    assert [r[0] for r in board["rows"]] == [c for c, _ in expected]
    assert all(r[1] == pytest.approx(v, abs=5e-4) for r, (_, v) in zip(board["rows"], expected))
    assert "FBS Independents" not in {r[0] for r in board["rows"]}
    assert {r[0] for r in board["rows"]} <= real_confs, "no defunct/empty conference may appear"


def test_elo_board_matches_engine_current_ratings(db_path):
    conn = sqlite3.connect(str(db_path))
    has_games = conn.execute("SELECT COUNT(*) FROM games WHERE home_score IS NOT NULL").fetchone()[0]
    if not has_games:
        pytest.skip("no completed games loaded")
    cfg = elo_config()
    up = build_upcoming(conn, cfg)
    ratings, _, _ = current_ratings(conn, cfg, up["season"])
    conn.close()
    board = up["current_elo"]
    assert board, "an active season must produce a board"
    assert [r[2] for r in board] == list(range(1, len(board) + 1)), "ranks must be 1..n with no gaps"
    assert [r[1] for r in board] == sorted((r[1] for r in board), reverse=True)
    for tid, elo, _ in board:
        assert elo == pytest.approx(ratings.get(tid, cfg["initial_rating"]), abs=0.051)
