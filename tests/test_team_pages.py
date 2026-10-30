"""
Milestone 2 (team pages): export_team_pages.build_team_pages() computes every
number the team page displays. These tests pin the roadmap's data rules:
pregame-only expectations, chronological Elo, correctly-signed swings,
scheduled games excluded, and real vs. model history kept apart.
"""
import sqlite3

import pytest

from export_team_pages import build_team_pages, split_team_pages, write_team_pages

# team_season row layout (see export_team_pages module docstring)
(TID, SEASON, GAMES, W, L, T, START, END, CHG, SOS, SOS_RANK, EXP, ACT, ABOVE,
 COE2, COE2_RANK, CONF) = range(17)


def _db(tmp_path, games, elo, membership=()):
    db = tmp_path / "tp.db"
    conn = sqlite3.connect(str(db))
    conn.execute("""CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER, game_date TEXT,
        week INTEGER, home_team_id INTEGER, away_team_id INTEGER, home_score INTEGER, away_score INTEGER,
        neutral_site INTEGER, game_phase TEXT, went_ot INTEGER)""")
    # mov_multiplier mirrors the real schema; left NULL here, which also exercises the export's
    # "missing value -> null, never a crash" path.
    conn.execute("""CREATE TABLE elo_game_history (game_id INTEGER, team_id INTEGER, pregame_elo REAL,
        opponent_pregame_elo REAL, elo_expectation REAL, elo_change REAL, postgame_elo REAL, mov_multiplier REAL)""")
    # Milestone E: the team page names each season's own conference, so the export
    # reads this table too.
    conn.execute("""CREATE TABLE team_membership_by_season (team_id INTEGER, season_year INTEGER,
        conference_real TEXT, is_fbs INTEGER)""")
    conn.executemany("INSERT INTO team_membership_by_season VALUES (?,?,?,1)", membership)
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
    # Team 1 changes conference between 2013 and 2015; team 3 has no membership row.
    membership = [(1, 2013, "Old League"), (2, 2013, "Old League"),
                  (1, 2015, "New League"), (2, 2015, "Old League")]
    conn = _db(tmp_path, games, elo, membership)
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


def test_each_team_season_names_that_seasons_conference(synthetic):
    # Milestone E: a later realignment must never rewrite an older season's label.
    assert _season(synthetic, 1, 2013)[CONF] == "Old League"
    assert _season(synthetic, 1, 2015)[CONF] == "New League"
    # No membership row means no conference, never a guess carried over from another season.
    assert _season(synthetic, 3, 2013)[CONF] is None


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


def test_every_team_season_names_its_conference(real):
    """Guards export_team_pages.load_memberships' missing-table fallback: on the real
    database every rated team-season has a membership row, so a silently empty
    conference column would mean the team page lost its per-season conference."""
    conn, data = real
    if not conn.execute("SELECT COUNT(*) FROM team_membership_by_season").fetchone()[0]:
        pytest.skip("team_membership_by_season is empty -- run run_pipeline.py first")
    missing = [(r[TID], r[SEASON]) for r in data["team_seasons"] if r[CONF] is None]
    known = dict(conn.execute("SELECT team_id || '-' || season_year, conference_real "
                              "FROM team_membership_by_season WHERE conference_real IS NOT NULL"))
    assert [m for m in missing if f"{m[0]}-{m[1]}" in known] == []
    assert len(missing) < 0.01 * len(data["team_seasons"]), \
        f"{len(missing)} of {len(data['team_seasons'])} team-seasons have no conference: {missing[:10]}"


def test_real_swings_have_correct_signs(real):
    _, data = real
    chg = {(e[0], e[1]): e[5] for e in data["elo"]}
    for tid, sw in data["swings"].items():
        assert all(chg[(g, int(tid))] > 0 for g in sw["gains"])
        assert all(chg[(g, int(tid))] < 0 for g in sw["losses"])


# ---- games against unrated opponents (FCS and the like) ----
#
# The point of these is that the games are EXPORTED and SEPARATE: visible on a
# schedule and countable in a record, and absent from every rated number.

NON_FBS_SCHEMA = """CREATE TABLE non_fbs_games (
    game_id INTEGER PRIMARY KEY, season_year INTEGER, week INTEGER, game_date TEXT,
    team_id INTEGER, opponent_name TEXT, opponent_conference TEXT,
    opponent_classification TEXT, team_score INTEGER, opponent_score INTEGER,
    is_home INTEGER, neutral_site INTEGER, went_ot INTEGER, game_phase TEXT)"""

# The same rated fixture the tests above use, handed over as a connection so a
# test can add unrated games to it and compare before and after.
RATED_GAMES = [
    (1, 2013, "2013-09-01", 1, 1, 2, 30, 20, 0, "regular", 0),
    (3, 2015, "2015-09-05", 1, 2, 1, 17, 17, 0, "regular", 0),
]
RATED_ELO = [
    (1, 1, 1500.0, 1450.0, 0.64, 12.0, 1512.0), (1, 2, 1450.0, 1500.0, 0.36, -12.0, 1438.0),
    (3, 2, 1460.0, 1520.0, 0.50, 1.0, 1461.0), (3, 1, 1520.0, 1460.0, 0.50, -1.0, 1519.0),
]


@pytest.fixture
def rated_conn(tmp_path):
    conn = _db(tmp_path, RATED_GAMES, RATED_ELO)
    yield conn
    conn.close()


def _unrated(conn, rows):
    conn.execute(NON_FBS_SCHEMA)
    conn.executemany("INSERT INTO non_fbs_games VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    return conn


def _row(game_id=900, season=2015, week=3, date="2015-09-14", team_id=1, opp="Mercer",
         conf="SoCon", cls="fcs", mine=52, theirs=7, home=1, neutral=0, ot=0, phase="regular"):
    return (game_id, season, week, date, team_id, opp, conf, cls, mine, theirs,
            home, neutral, ot, phase)


def test_an_unrated_game_is_exported_with_its_own_fields(rated_conn):
    data = build_team_pages(_unrated(rated_conn, [_row()]))
    assert len(data["non_fbs"]) == 1
    assert data["non_fbs"][0] == [900, 2015, "2015-09-14", 3, 1, "Mercer", 52, 7, 1, 0, 0, 0, "fcs"]


def test_an_unrated_game_moves_no_rated_number(rated_conn):
    before = build_team_pages(rated_conn)
    after = build_team_pages(_unrated(rated_conn, [_row()]))
    # Team 1 has a real 2015 season here, so this comparison is not vacuous.
    assert any(r[TID] == 1 and r[SEASON] == 2015 for r in before["team_seasons"])
    assert after["team_seasons"] == before["team_seasons"]
    assert after["games"] == before["games"]
    assert after["elo"] == before["elo"]
    assert 900 not in [g[0] for g in after["games"]]


def test_unrated_records_count_wins_losses_and_ties_separately(rated_conn):
    data = build_team_pages(_unrated(rated_conn, [
        _row(game_id=900, mine=52, theirs=7),
        _row(game_id=901, date="2015-09-21", mine=10, theirs=24),
        _row(game_id=902, date="2015-09-28", mine=17, theirs=17),
        # A different season for the same team stays its own record.
        _row(game_id=903, season=2013, date="2013-09-02", mine=3, theirs=9, home=0),
    ]))
    assert data["non_fbs_records"]["1|2015"] == [1, 1, 1]
    assert data["non_fbs_records"]["1|2013"] == [0, 1, 0]


def test_unrated_records_are_kept_per_team(rated_conn):
    data = build_team_pages(_unrated(rated_conn, [
        _row(team_id=1, mine=52, theirs=7),
        _row(game_id=901, team_id=2, mine=7, theirs=52)]))
    assert data["non_fbs_records"]["1|2015"] == [1, 0, 0]
    assert data["non_fbs_records"]["2|2015"] == [0, 1, 0]


def test_a_postseason_phase_is_carried_as_its_code(rated_conn):
    data = build_team_pages(_unrated(rated_conn, [
        _row(phase="bowl"), _row(game_id=901, phase="cfp"), _row(game_id=902, phase="regular")]))
    assert {r[0]: r[10] for r in data["non_fbs"]} == {900: 1, 901: 2, 902: 0}


def test_a_missing_classification_is_exported_as_null_not_empty_string(rated_conn):
    # CFBD gives no division for some older opponents. The page distinguishes
    # "the source said FCS" from "the source said nothing", so '' must not
    # survive as a value the page would print as a division.
    data = build_team_pages(_unrated(rated_conn, [_row(cls="")]))
    assert data["non_fbs"][0][12] is None


def test_a_database_with_no_such_table_exports_an_empty_set(rated_conn):
    data = build_team_pages(rated_conn)
    assert data["non_fbs"] == []
    assert data["non_fbs_records"] == {}


# ---- The Merits award a game earned (the team page's Schedule column) ----
# The award is Python's, computed by build_hybrid_coefficients.py, and the page
# only formats it. What the export has to preserve is the difference between an
# award of zero -- which is what a regulation loss really earns -- and no award
# at all, which is what a bootstrap season and a tie have.
MERIT = 8


def _with_hybrid(tmp_path, awards):
    games = [(1, 2015, "2015-09-01", 1, 1, 2, 30, 20, 0, "regular", 0),
             (2, 2015, "2015-09-08", 2, 2, 1, 24, 10, 0, "regular", 0)]
    elo = [(1, 1, 1500.0, 1450.0, 0.64, 12.0, 1512.0), (1, 2, 1450.0, 1500.0, 0.36, -12.0, 1438.0),
           (2, 1, 1512.0, 1600.0, 0.40, -9.0, 1503.0), (2, 2, 1600.0, 1512.0, 0.60, 9.0, 1609.0)]
    conn = _db(tmp_path, games, elo)
    conn.execute("""CREATE TABLE hybrid_game_ratings (game_id INTEGER, team_id INTEGER, elo_z REAL,
        coe_z REAL, hybrid_strength REAL, hybrid_rating REAL, hybrid_expectation REAL,
        result_type TEXT, game_coe REAL, PRIMARY KEY (game_id, team_id))""")
    conn.executemany("INSERT INTO hybrid_game_ratings (game_id, team_id, game_coe) VALUES (?,?,?)", awards)
    conn.commit()
    return conn


def test_a_games_merits_award_rides_on_its_elo_row(tmp_path):
    conn = _with_hybrid(tmp_path, [(1, 1, 2.7183), (1, 2, 0.0), (2, 1, 0.0), (2, 2, 3.5)])
    try:
        by = {(e[0], e[1]): e for e in build_team_pages(conn)["elo"]}
    finally:
        conn.close()
    assert by[(1, 1)][MERIT] == pytest.approx(2.718)      # the win, rounded for the wire
    assert by[(2, 2)][MERIT] == pytest.approx(3.5)


def test_an_award_of_zero_is_exported_as_zero_and_not_as_absent(tmp_path):
    """A regulation loss earns 0.0, which is a measurement; absent means unvalued."""
    conn = _with_hybrid(tmp_path, [(1, 1, 2.0), (1, 2, 0.0), (2, 1, 0.0), (2, 2, 2.0)])
    try:
        by = {(e[0], e[1]): e for e in build_team_pages(conn)["elo"]}
    finally:
        conn.close()
    assert by[(1, 2)][MERIT] == 0.0
    assert by[(1, 2)][MERIT] is not None


def test_a_rated_game_the_hybrid_layer_never_valued_carries_no_award(tmp_path):
    """The bootstrap seasons have an Elo row and no hybrid row; that is a null."""
    conn = _with_hybrid(tmp_path, [(1, 1, 2.0), (1, 2, 0.0)])
    try:
        by = {(e[0], e[1]): e for e in build_team_pages(conn)["elo"]}
    finally:
        conn.close()
    assert by[(2, 1)][MERIT] is None
    assert by[(2, 2)][MERIT] is None


def test_a_database_with_no_hybrid_table_exports_every_award_as_null(tmp_path):
    """Older snapshots have no hybrid_game_ratings at all and must still export."""
    games = [(1, 2015, "2015-09-01", 1, 1, 2, 30, 20, 0, "regular", 0)]
    elo = [(1, 1, 1500.0, 1450.0, 0.64, 12.0, 1512.0), (1, 2, 1450.0, 1500.0, 0.36, -12.0, 1438.0)]
    conn = _db(tmp_path, games, elo)
    try:
        rows = build_team_pages(conn)["elo"]
    finally:
        conn.close()
    assert rows and all(r[MERIT] is None for r in rows)


def test_the_award_is_the_one_stored_for_that_team_not_its_opponents(tmp_path):
    """Both sides of a game have a row; joining on game alone would mix them up."""
    conn = _with_hybrid(tmp_path, [(1, 1, 3.9), (1, 2, 0.0), (2, 1, 0.0), (2, 2, 2.1)])
    try:
        by = {(e[0], e[1]): e for e in build_team_pages(conn)["elo"]}
    finally:
        conn.close()
    assert (by[(1, 1)][MERIT], by[(1, 2)][MERIT]) == (3.9, 0.0)
    assert (by[(2, 1)][MERIT], by[(2, 2)][MERIT]) == (0.0, 2.1)


# ---------------------------------------------------------------------------
# Sharding. The whole-league payload is re-filed under two indexes; these pin
# that nothing is lost, nothing is invented, and the two totals the browser
# can no longer count for itself are counted here.


def _payload():
    """A league of three teams over two seasons, built by hand.

    Hand-built rather than taken from build_team_pages, because a splitter
    tested against its own producer's output only proves the two agree.
    """
    return {
        "cfp_first_season": 2014,
        "titles_since": 1980,
        # game: [id, season, date, week, home, away, hs, as, neutral, phase, ot, kickoff]
        "games": [
            [1, 2024, "2024-09-01", 1, 10, 20, 24, 17, 0, 0, 0, None],
            [2, 2024, "2024-09-08", 2, 20, 30, 10, 13, 0, 0, 0, None],
            [3, 2025, "2025-09-01", 1, 10, 30, 31, 28, 0, 0, 1, None],
        ],
        # elo: [game_id, team_id, pre, opp_pre, exp, change, post, mov, coe]
        "elo": [
            [1, 10, 1500.0, 1400.0, 0.64, 9.0, 1509.0, None, None],
            [1, 20, 1400.0, 1500.0, 0.36, -9.0, 1391.0, None, None],
            [2, 20, 1391.0, 1450.0, 0.42, -8.0, 1383.0, None, None],
            [2, 30, 1450.0, 1391.0, 0.58, 8.0, 1458.0, None, None],
            [3, 10, 1509.0, 1458.0, 0.57, 7.0, 1516.0, None, None],
            [3, 30, 1458.0, 1509.0, 0.43, -7.0, 1451.0, None, None],
        ],
        # team_season: team, season, ... coe2 at index 14
        "team_seasons": [
            [10, 2024, 1, 1, 0, 0, 1500.0, 1509.0, 9.0, 1400.0, 1, 0.6, 1, 0.4, 2.5, 1, "ACC"],
            [20, 2024, 2, 0, 2, 0, 1400.0, 1383.0, -17.0, 1475.0, 2, 0.8, 0, -0.8, None, None, "ACC"],
            [30, 2024, 1, 1, 0, 0, 1450.0, 1458.0, 8.0, 1391.0, 3, 0.4, 1, 0.6, 1.5, 2, "SEC"],
            [10, 2025, 1, 1, 0, 0, 1509.0, 1516.0, 7.0, 1458.0, 1, 0.6, 1, 0.4, 3.0, 1, "ACC"],
            [30, 2025, 1, 0, 1, 0, 1458.0, 1451.0, -7.0, 1509.0, 2, 0.4, 0, -0.4, None, None, "SEC"],
        ],
        # non_fbs: [id, season, date, week, team, opponent, mine, theirs, home, neutral, phase, ot, class]
        "non_fbs": [[99, 2024, "2024-09-15", 3, 10, "Someone FCS", 45, 3, 1, 0, 0, 0, "fcs"]],
        "non_fbs_records": {"10|2024": {"w": 1, "l": 0, "t": 0}},
        "coe2_rollups": {"10|2024": {"total": 2.5}, "30|2024": {"total": 1.5}},
        "swings": {"10": {"gains": [1], "losses": []}},
        "history": {"10": {"titles": 1}},
        "advanced": {"metrics": ["a", "b"], "first_season": 2004,
                     "teams": {"10": {"x": 1}, "20": {"x": 2}}},
    }


def test_every_row_survives_the_split_exactly_once_per_index():
    """Not one row lost and not one invented, counted both ways."""
    data = _payload()
    split = split_team_pages(data)
    for index in ("teams", "seasons"):
        for key in ("elo", "team_seasons", "non_fbs"):
            moved = [row for scope in split[index].values() for row in scope[key]]
            assert len(moved) == len(data[key]), (index, key)
            # Same rows, not merely the same count.
            assert sorted(map(repr, moved)) == sorted(map(repr, data[key])), (index, key)


def test_a_game_is_filed_under_BOTH_clubs_and_its_season():
    """The one place duplication is deliberate: a game has two teams.

    The old layout kept one copy precisely so it was not duplicated; the whole
    point of the split is that a team page should not download the other 138
    teams' games to find its own.
    """
    split = split_team_pages(_payload())
    assert [g[0] for g in split["teams"][10]["games"]] == [1, 3]
    assert [g[0] for g in split["teams"][20]["games"]] == [1, 2]
    assert [g[0] for g in split["teams"][30]["games"]] == [2, 3]
    assert [g[0] for g in split["seasons"][2024]["games"]] == [1, 2]
    assert [g[0] for g in split["seasons"][2025]["games"]] == [3]
    # Across the team index a game appears exactly twice -- once per club.
    filed = [g[0] for scope in split["teams"].values() for g in scope["games"]]
    assert sorted(filed) == [1, 1, 2, 2, 3, 3]


def test_a_team_playing_itself_is_filed_once():
    """Guards the `game[5] != game[4]` check, which is otherwise invisible."""
    data = _payload()
    data["games"].append([4, 2025, "2025-10-01", 5, 10, 10, 7, 7, 0, 0, 0, None])
    split = split_team_pages(data)
    assert [g[0] for g in split["teams"][10]["games"]].count(4) == 1


def test_an_elo_row_reaches_its_season_through_the_game_it_names():
    # An Elo row carries no season of its own; the only way to it is the game.
    split = split_team_pages(_payload())
    assert sorted(r[0] for r in split["seasons"][2024]["elo"]) == [1, 1, 2, 2]
    assert sorted(r[0] for r in split["seasons"][2025]["elo"]) == [3, 3]
    assert sorted(r[0] for r in split["teams"][10]["elo"]) == [1, 3]


def test_an_elo_row_whose_game_is_missing_still_reaches_its_team():
    """Losing the season is survivable; losing the team loses a ledger row."""
    data = _payload()
    data["elo"].append([404, 10, 1500.0, 1400.0, 0.5, 0.0, 1500.0, None, None])
    split = split_team_pages(data)
    assert 404 in [r[0] for r in split["teams"][10]["elo"]]
    assert 404 not in [r[0] for scope in split["seasons"].values() for r in scope["elo"]]


def test_the_team_only_extras_go_to_their_team_and_nowhere_else():
    split = split_team_pages(_payload())
    assert split["teams"][10]["swings"] == {"gains": [1], "losses": []}
    assert split["teams"][10]["history"] == {"titles": 1}
    assert split["teams"][10]["advanced"] == {"x": 1}
    assert split["teams"][10]["coe2_rollups"] == {"10|2024": {"total": 2.5}}
    assert split["teams"][30]["coe2_rollups"] == {"30|2024": {"total": 1.5}}
    assert split["teams"][10]["non_fbs_records"] == {"10|2024": {"w": 1, "l": 0, "t": 0}}
    # A club with none of them simply has no key, rather than an empty object
    # that would read as "measured, and nothing there".
    assert "swings" not in split["teams"][20]
    assert "coe2_rollups" not in split["teams"][20]


def test_the_per_season_totals_are_counted_over_the_WHOLE_league():
    """The browser used to count these while indexing every team-season row.

    A shard holds one season or one team, so counting there would divide by a
    smaller number and quietly produce a wrong rank -- "#1 of 1" instead of
    "#1 of 3". They are counted here, once, and shipped in core.
    """
    core = split_team_pages(_payload())["core"]
    assert core["teams_per_season"] == {2024: 3, 2025: 2}
    # Only the team-seasons that HAVE a CoE 2.0 value count toward its
    # denominator, which is why it is a separate count and not the one above:
    # 2024 has two of its three, 2025 one of its two.
    assert core["coe2_per_season"] == {2024: 2, 2025: 1}


def test_core_carries_the_league_wide_advanced_metadata_but_no_team_numbers():
    core = split_team_pages(_payload())["core"]
    assert core["advanced"] == {"metrics": ["a", "b"], "first_season": 2004}
    assert "teams" not in core["advanced"]
    assert core["cfp_first_season"] == 2014
    assert core["titles_since"] == 1980


def test_an_empty_league_splits_into_nothing_rather_than_failing():
    split = split_team_pages({"games": [], "elo": [], "team_seasons": []})
    assert split["teams"] == {}
    assert split["seasons"] == {}
    assert split["core"]["teams_per_season"] == {}
    assert split["core"]["advanced"] is None


def test_the_written_files_name_their_own_scope(tmp_path):
    """Each file assigns into a keyed global, so loading two cannot clobber."""
    manifest = write_team_pages(_payload(), tmp_path)
    core = (tmp_path / "core.js").read_text()
    assert core.startswith("window.__TEAM_PAGES_CORE__=")
    team = (tmp_path / "team" / "10.js").read_text()
    assert team.startswith('(window.__TEAM_PAGES_TEAM__=window.__TEAM_PAGES_TEAM__||{})["10"]=')
    season = (tmp_path / "season" / "2024.js").read_text()
    assert season.startswith('(window.__TEAM_PAGES_SEASON__=window.__TEAM_PAGES_SEASON__||{})["2024"]=')
    assert manifest["teams"]["10"].startswith("data/team_pages/team/10.js?v=")
    assert manifest["seasons"]["2024"].startswith("data/team_pages/season/2024.js?v=")
    assert manifest["core"].startswith("data/team_pages/core.js?v=")


def test_the_version_changes_when_the_bytes_do(tmp_path):
    """A stamp nobody recomputes reads as a promise the file has not changed."""
    first = write_team_pages(_payload(), tmp_path)
    data = _payload()
    data["team_seasons"][0][7] = 1999.0
    second = write_team_pages(data, tmp_path)
    assert first["teams"]["10"] != second["teams"]["10"]
    # And a file the change did not touch keeps its stamp.
    assert first["teams"]["20"] == second["teams"]["20"]

