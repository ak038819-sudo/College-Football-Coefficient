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

    # Compared at the precision the board PUBLISHES, not with a tolerance.
    # This used to be approx(abs=5e-4) against the unrounded source, which is
    # the maximum error rounding to three decimals can produce -- the check sat
    # exactly on its own limit, so whether it passed depended on where that
    # week's numbers happened to fall. It failed the 2026-10-10 deploy with the
    # ACC 0.000454 away from its source and another conference a hair over, and
    # nothing was wrong: a failing test here blocks the deploy, so the site
    # stops publishing until someone reads the log. The board is correct when
    # it shows the source rounded, which is an exact statement.
    mismatched = [(c, published, round(v, 3), v)
                  for (c, published), (_, v) in zip(board["rows"], expected)
                  if published != round(v, 3)]
    assert not mismatched, "the board does not show the bid-setting values: " + ", ".join(
        f"{c} shows {published} for {v!r} (rounds to {want})"
        for c, published, want, v in mismatched)
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
