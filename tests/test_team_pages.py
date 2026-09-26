"""
Milestone 2 (team pages): export_team_pages.build_team_pages() computes every
number the team page displays. These tests pin the roadmap's data rules:
pregame-only expectations, chronological Elo, correctly-signed swings,
scheduled games excluded, and real vs. model history kept apart.
"""
import sqlite3

import pytest

from export_team_pages import build_team_pages

# team_season row layout (see export_team_pages module docstring)
TID, SEASON, GAMES, W, L, T, START, END, CHG, SOS, SOS_RANK, EXP, ACT, ABOVE, COE2, COE2_RANK = range(16)


def _db(tmp_path, games, elo):
    db = tmp_path / "tp.db"
    conn = sqlite3.connect(str(db))
    conn.execute("""CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER, game_date TEXT,
        week INTEGER, home_team_id INTEGER, away_team_id INTEGER, home_score INTEGER, away_score INTEGER,
        neutral_site INTEGER, game_phase TEXT, went_ot INTEGER)""")
    # mov_multiplier mirrors the real schema; left NULL here, which also exercises the export's
    # "missing value -> null, never a crash" path.
    conn.execute("""CREATE TABLE elo_game_history (game_id INTEGER, team_id INTEGER, pregame_elo REAL,
        opponent_pregame_elo REAL, elo_expectation REAL, elo_change REAL, postgame_elo REAL, mov_multiplier REAL)""")
    conn.executemany("INSERT INTO games VALUES (?,?,?,?,?,?,?,?,?,?,?)", games)
    conn.executemany("INSERT INTO elo_game_history (game_id, team_id, pregame_elo, opponent_pregame_elo, "
                     "elo_expectation, elo_change, postgame_elo) VALUES (?,?,?,?,?,?,?)", elo)
    conn.commit()
    return conn


@pytest.fixture
def synthetic(tmp_path):
    games = [
        # id, season, date, wk, home, away, hs, as, neutral, phase, ot
        (1, 2013, "2013-09-01", 1, 1, 2, 30, 20, 0, "regular", 0),   # team 1 W
        (2, 2013, "2014-01-06", 16, 1, 3, 10, 24, 1, "cfp", 0),      # pre-2014 "cfp" = BCS bowl, team 1 L
        (3, 2015, "2015-09-05", 1, 2, 1, 17, 17, 0, "regular", 0),   # tie
        (4, 2015, "2016-01-01", 17, 1, 3, 31, 28, 1, "cfp", 0),      # real CFP, team 1 W
        (5, 2015, "2015-12-01", 14, 1, 2, None, None, 0, "regular", 0),  # scheduled: no scores
    ]
    elo = [
        (1, 1, 1500.0, 1450.0, 0.64, 12.0, 1512.0), (1, 2, 1450.0, 1500.0, 0.36, -12.0, 1438.0),
        (2, 1, 1512.0, 1600.0, 0.40, -9.0, 1503.0), (2, 3, 1600.0, 1512.0, 0.60, 9.0, 1609.0),
        (3, 2, 1460.0, 1520.0, 0.50, 1.0, 1461.0), (3, 1, 1520.0, 1460.0, 0.50, -1.0, 1519.0),
        (4, 1, 1519.0, 1610.0, 0.30, 25.0, 1544.0), (4, 3, 1610.0, 1519.0, 0.70, -25.0, 1585.0),
    ]
    conn = _db(tmp_path, games, elo)
    yield build_team_pages(conn)
    conn.close()


def _season(data, tid, season):
    return next(r for r in data["team_seasons"] if r[TID] == tid and r[SEASON] == season)


def test_expected_wins_is_sum_of_pregame_expectation(synthetic):
    s = _season(synthetic, 1, 2013)
    assert s[EXP] == pytest.approx(0.64 + 0.40)
    assert s[SOS] == pytest.approx((1450.0 + 1600.0) / 2, abs=0.05)   # mean opponent PREGAME Elo


def test_ties_count_half_and_performance_is_actual_minus_expected(synthetic):
    s = _season(synthetic, 1, 2015)
    assert (s[W], s[L], s[T]) == (1, 0, 1)
    assert s[ACT] == 1.5
    assert s[ABOVE] == pytest.approx(1.5 - (0.50 + 0.30), abs=0.005)


def test_scheduled_games_are_excluded_everywhere(synthetic):
    assert 5 not in {g[0] for g in synthetic["games"]}
    assert 5 not in {e[0] for e in synthetic["elo"]}
    assert _season(synthetic, 1, 2015)[GAMES] == 2


def test_swings_signs_and_results(synthetic):
    sw = synthetic["swings"]["1"]
    elo_by = {(e[0], e[1]): e for e in synthetic["elo"]}
    assert sw["gains"] == [4, 1]          # largest gain first, wins only
    assert sw["losses"] == [2]            # the tie (game 3, -1.0) is excluded from both lists
    assert all(elo_by[(g, 1)][5] > 0 for g in sw["gains"])
    assert all(elo_by[(g, 1)][5] < 0 for g in sw["losses"])


def test_pre_2014_cfp_phase_counts_as_bowl_not_cfp(synthetic):
    h = synthetic["history"]["1"]
    assert h["bowl"] == [1, 0, 1]                   # 2013 BCS-era title game
    assert h["cfp_appearances"] == 1 and h["cfp_seasons"] == [2015]
    assert h["cfp_record"] == [1, 0]


def test_history_contains_only_real_results(synthetic):
    # Model (simulated) playoff data lives in dashboard_data.json; it must never leak in here.
    for h in synthetic["history"].values():
        assert set(h) == {"first_season", "last_season", "seasons", "record", "bowl",
                          "cfp_appearances", "cfp_seasons", "cfp_record",
                          "titles", "title_count"}   # titles: Milestone 6 reference list (real history)


# ---------------- invariants on the real database ----------------

@pytest.fixture(scope="module")
def real(db_path):
    conn = sqlite3.connect(str(db_path))
    has_elo = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='elo_game_history'").fetchone()[0]
    if not has_elo or not conn.execute("SELECT COUNT(*) FROM elo_game_history").fetchone()[0]:
        pytest.skip("elo_game_history not built -- run src/build_elo.py first")
    data = build_team_pages(conn)
    yield conn, data
    conn.close()


def test_elo_history_is_chronological_and_continuous(real):
    """Within a season, each game's pregame Elo must equal the previous game's postgame Elo --
    only true if the export's order matches the Elo engine's processing order exactly."""
    _, data = real
    season_of = {g[0]: g[1] for g in data["games"]}
    prev = {}
    for gid, tid, pre, _opp, _exp, _chg, post, *_later_fields in data["elo"]:
        key = (tid, season_of[gid])
        if key in prev:
            assert pre == pytest.approx(prev[key], abs=0.11), f"team {tid} season {key[1]} game {gid}"
        prev[key] = post


def test_expected_wins_match_database_on_every_team_season(real):
    conn, data = real
    direct = {(t, s): e for t, s, e in conn.execute("""
        SELECT e.team_id, g.season_year, SUM(e.elo_expectation)
        FROM elo_game_history e JOIN games g USING(game_id)
        WHERE g.home_score IS NOT NULL AND g.away_score IS NOT NULL
        GROUP BY 1, 2""")}
    assert len(direct) == len(data["team_seasons"])
    for r in data["team_seasons"]:
        assert r[EXP] == pytest.approx(direct[(r[TID], r[SEASON])], abs=0.006)


def test_real_swings_have_correct_signs(real):
    _, data = real
    chg = {(e[0], e[1]): e[5] for e in data["elo"]}
    for tid, sw in data["swings"].items():
        assert all(chg[(g, int(tid))] > 0 for g in sw["gains"])
        assert all(chg[(g, int(tid))] < 0 for g in sw["losses"])
