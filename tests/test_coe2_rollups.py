"""
ENG-13 (CoE 2.0 season rollups and explicit bonuses) and ENG-14 (conference
five-season aggregate and the bid-allocation input contract).

The guides name five fixtures between them -- season reconciliation, no-bonus,
multiple-bonus, conference reconciliation, realignment, and the allocation input
contract -- and they are the tests here, plus the invariants on the real
database.
"""
import sqlite3

import pytest

from build_coe2_rollups import (BONUS_CATEGORIES, build_conference_5yr, build_team_rollups,
                                conference_coe2_rank, conference_season_coe2, load_bonus_config)

# team_coe2_by_season row layout
TID, SEASON, GAMES_TOTAL, BONUS_TOTAL, TOTAL, N_GAMES, VERSION = range(7)
# team_coe2_bonuses row layout
B_TID, B_SEASON, B_CAT, B_COUNT, B_POINTS, B_DETAIL, B_VERSION = range(7)

ZERO = load_bonus_config({"version": "test_v1"})
PAID = load_bonus_config({"version": "test_v1", "bowl_appearance": 1.0, "bowl_win": 2.0,
                          "cfp_appearance": 3.0, "cfp_win": 4.0,
                          "conference_standings_first": 5.0, "national_title": 10.0})


def _db(tmp_path, games, hybrid, standings=()):
    conn = sqlite3.connect(str(tmp_path / "r.db"))
    conn.executescript("""
        CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER, game_phase TEXT,
            home_team_id INTEGER, away_team_id INTEGER, home_score INTEGER, away_score INTEGER);
        CREATE TABLE hybrid_game_ratings (game_id INTEGER, team_id INTEGER, game_coe REAL);
        CREATE TABLE elo_game_history (game_id INTEGER, team_id INTEGER);
        CREATE TABLE conference_standings_by_year (season_year INTEGER, conference TEXT,
            team_id INTEGER, conf_rank INTEGER);
        CREATE TABLE team_membership_by_season (team_id INTEGER, season_year INTEGER, conference_real TEXT);
    """)
    conn.executemany("INSERT INTO games VALUES (?,?,?,?,?,?,?)", games)
    conn.executemany("INSERT INTO hybrid_game_ratings VALUES (?,?,?)", hybrid)
    # Elo history mirrors the games, which is what the postseason counter walks.
    conn.executemany("INSERT INTO elo_game_history VALUES (?,?)",
                     [(g[0], t) for g in games for t in (g[3], g[4])])
    conn.executemany("INSERT INTO conference_standings_by_year VALUES (?,?,?,?)", standings)
    conn.commit()
    return conn


def _season(rows, tid, season):
    return next(r for r in rows if r[TID] == tid and r[SEASON] == season)


# ------------------------------------------------- ENG-13: season reconciliation

def test_a_season_total_is_exactly_its_game_awards_plus_its_bonuses(tmp_path):
    conn = _db(tmp_path,
               games=[(1, 2021, "regular", 1, 2, 30, 10), (2, 2021, "bowl", 1, 3, 24, 21)],
               hybrid=[(1, 1, 2.5), (1, 2, 0.0), (2, 1, 3.5), (2, 3, 0.0)])
    rows, bonuses = build_team_rollups(conn, PAID)
    s = _season(rows, 1, 2021)
    assert s[GAMES_TOTAL] == pytest.approx(6.0)
    assert s[N_GAMES] == 2
    mine = [b for b in bonuses if b[B_TID] == 1 and b[B_SEASON] == 2021]
    assert s[BONUS_TOTAL] == pytest.approx(sum(b[B_POINTS] for b in mine))
    assert s[TOTAL] == pytest.approx(s[GAMES_TOTAL] + s[BONUS_TOTAL])
    conn.close()


def test_no_bonus_season_still_reconciles(tmp_path):
    """The no-bonus fixture: a regular season only. bonus_total is 0 and the
    total is the game awards, with no bonus rows at all."""
    conn = _db(tmp_path, games=[(1, 2021, "regular", 1, 2, 30, 10)],
               hybrid=[(1, 1, 2.5), (1, 2, 0.0)])
    rows, bonuses = build_team_rollups(conn, PAID)
    s = _season(rows, 1, 2021)
    assert s[BONUS_TOTAL] == 0.0
    assert s[TOTAL] == pytest.approx(s[GAMES_TOTAL]) == pytest.approx(2.5)
    assert [b for b in bonuses if b[B_TID] == 1] == []
    conn.close()


def test_multiple_bonuses_accumulate_with_visible_provenance(tmp_path):
    """The multiple-bonus fixture: a CFP run plus a standings title plus a
    national title, each its own row naming the rule and the count."""
    champions = [{"team_id": 1, "season": 2021, "status": "awarded", "system": "ap"},
                 {"team_id": 1, "season": 2021, "status": "awarded", "system": "coaches"}]
    conn = _db(tmp_path,
               games=[(1, 2021, "cfp", 1, 2, 30, 10), (2, 2021, "cfp", 1, 3, 24, 21)],
               hybrid=[(1, 1, 4.0), (2, 1, 5.0)],
               standings=[(2021, "Alpha", 1, 1)])
    rows, bonuses = build_team_rollups(conn, PAID, champions)
    mine = {b[B_CAT]: b for b in bonuses if b[B_TID] == 1}
    assert mine["cfp_appearance"][B_COUNT] == 2 and mine["cfp_appearance"][B_POINTS] == pytest.approx(6.0)
    assert mine["cfp_win"][B_COUNT] == 2 and mine["cfp_win"][B_POINTS] == pytest.approx(8.0)
    assert mine["conference_standings_first"][B_POINTS] == pytest.approx(5.0)
    # One championship, two poll rows: paid once.
    assert mine["national_title"][B_COUNT] == 1 and mine["national_title"][B_POINTS] == pytest.approx(10.0)
    assert all(b[B_DETAIL] for b in mine.values()), "every bonus states its reason"
    s = _season(rows, 1, 2021)
    assert s[BONUS_TOTAL] == pytest.approx(29.0)
    assert s[TOTAL] == pytest.approx(9.0 + 29.0)
    conn.close()


def test_zero_valued_bonuses_keep_the_baseline_reproducible(tmp_path):
    """The shipped configuration: categories are recorded, points are 0, so a
    season total is exactly the sum of its game awards."""
    conn = _db(tmp_path, games=[(1, 2021, "bowl", 1, 2, 30, 10)], hybrid=[(1, 1, 2.5)],
               standings=[(2021, "Alpha", 1, 1)])
    rows, bonuses = build_team_rollups(conn, ZERO)
    s = _season(rows, 1, 2021)
    assert s[TOTAL] == pytest.approx(s[GAMES_TOTAL])
    assert s[BONUS_TOTAL] == 0.0
    # The provenance is still recorded, so turning values on later changes only magnitudes.
    assert {b[B_CAT] for b in bonuses if b[B_TID] == 1} == {"bowl_appearance", "bowl_win",
                                                            "conference_standings_first"}
    assert all(b[B_POINTS] == 0.0 for b in bonuses)
    conn.close()


def test_a_pre_2014_title_bowl_is_a_bowl_not_a_playoff_game(tmp_path):
    conn = _db(tmp_path, games=[(1, 2013, "cfp", 1, 2, 30, 10)], hybrid=[(1, 1, 2.0)])
    _, bonuses = build_team_rollups(conn, PAID)
    cats = {b[B_CAT] for b in bonuses if b[B_TID] == 1}
    assert cats == {"bowl_appearance", "bowl_win"}
    assert "cfp_appearance" not in cats
    conn.close()


def test_an_unknown_bonus_category_is_rejected():
    with pytest.raises(ValueError, match="unknown categories"):
        load_bonus_config({"bowl_appearanc": 1.0})          # a typo must not silently award nothing
    cfg = load_bonus_config({})
    assert set(cfg["values"]) == {k for k, _ in BONUS_CATEGORIES}
    assert all(v == 0.0 for v in cfg["values"].values())


# --------------------------------------- ENG-14: conference aggregate + contract

def _conf_db(tmp_path, membership, games, hybrid):
    conn = _db(tmp_path, games, hybrid)
    conn.executemany("INSERT INTO team_membership_by_season VALUES (?,?,?)", membership)
    conn.commit()
    return conn


def test_internal_conference_games_never_enter_conference_strength(tmp_path):
    conn = _conf_db(tmp_path,
                    membership=[(1, 2020, "Alpha"), (2, 2020, "Alpha"), (3, 2020, "Beta")],
                    games=[(1, 2020, "regular", 1, 2, 21, 14),      # Alpha vs Alpha: internal
                           (2, 2020, "regular", 1, 3, 28, 7)],      # Alpha vs Beta: external
                    hybrid=[(1, 1, 5.0), (1, 2, 0.0), (2, 1, 3.0), (2, 3, 0.0)])
    per_season = conference_season_coe2(conn)
    assert per_season[("Alpha", 2020)] == (3.0, 1), "only the external game counts"
    assert per_season[("Beta", 2020)] == (0.0, 1)
    conn.close()


def test_realignment_credits_the_conference_the_team_was_in_that_season(tmp_path):
    conn = _conf_db(tmp_path,
                    membership=[(1, 2020, "Alpha"), (1, 2021, "Beta"),
                                (2, 2020, "Beta"), (2, 2021, "Beta")],
                    games=[(1, 2020, "regular", 1, 2, 21, 14),      # Alpha vs Beta: external for both
                           (2, 2021, "regular", 1, 2, 21, 14)],     # both now Beta: internal
                    hybrid=[(1, 1, 4.0), (1, 2, 0.0), (2, 1, 6.0), (2, 2, 0.0)])
    per = conference_season_coe2(conn)
    assert per[("Alpha", 2020)] == (4.0, 1), "2020 credit belongs to the conference of the time"
    assert ("Alpha", 2021) not in per, "a team that left cannot keep earning for its old league"
    assert ("Beta", 2021) not in per, "the 2021 meeting is internal to Beta"
    conn.close()


def test_the_five_season_window_is_prior_seasons_only_and_decay_weighted():
    per_season = {("Alpha", y): (10.0, 2) for y in range(2015, 2021)}
    rows = build_conference_5yr(per_season, "test_v1", decay_base=0.5, rolling_years=5)
    by_year = {r[0]: r for r in rows}
    # Entering 2021: seasons 2016..2020, anchored at 2020.
    r = by_year[2021]
    assert (r[3], r[4]) == (2016, 2020), "window is Y-5..Y-1, never Y itself"
    assert r[5] == 5
    assert r[2] == pytest.approx(10 * (1 + 0.5 + 0.25 + 0.125 + 0.0625))
    assert r[6] == 10, "contributing games across the window"
    # The first data season has no prior history, so it gets no row at all.
    assert 2015 not in by_year

    # The season that matters most: one that HAS its own data. Entering 2020 must
    # use 2015..2019 and must NOT let 2020's own contribution in -- if it did, a
    # season's own results would shape the strength rating used to value them.
    r2020 = by_year[2020]
    assert (r2020[3], r2020[4]) == (2015, 2019)
    assert r2020[5] == 5
    assert r2020[2] == pytest.approx(10 * (1 + 0.5 + 0.25 + 0.125 + 0.0625))
    # Every window this builder emits ends strictly before the season it names.
    assert all(r[4] < r[0] for r in rows)
    # An extra season inside the window would change the value; pin the count too.
    assert all(r[5] <= 5 for r in rows)


def test_a_conference_absent_from_the_window_gets_no_row_rather_than_a_zero():
    per_season = {("Alpha", 2018): (5.0, 1), ("Gone", 2016): (5.0, 1)}
    rows = build_conference_5yr(per_season, "test_v1")
    entering_2023 = [r for r in rows if r[0] == 2023]
    assert {r[1] for r in entering_2023} == set(), "nothing in 2018..2022 -> no rows"
    entering_2019 = {r[1] for r in rows if r[0] == 2019}
    assert entering_2019 == {"Alpha", "Gone"}, "both contributed inside this window"


def test_the_bid_allocation_input_matches_cfp_v1s_contract(tmp_path):
    """The allocation-input contract test: same shape and same exclusions as
    select_playoff_field_v2.load_conference_coe_rank, so bid allocation can
    consume either model through one interface."""
    conn = _db(tmp_path, games=[], hybrid=[])
    conn.executescript(open("sql/coe2_rollup_tables.sql").read())
    conn.executemany(
        "INSERT INTO conference_coe2_5yr_by_season (season_year, conference, coe2_5yr, window_start_year,"
        " window_end_year, seasons_counted, external_games, formula_version) VALUES (?,?,?,?,?,?,?,?)",
        [(2021, "Alpha", 50.0, 2016, 2020, 5, 10, "v1"),
         (2021, "Beta", 80.0, 2016, 2020, 5, 10, "v1"),
         (2021, "FBS Independents", 99.0, 2016, 2020, 5, 10, "v1"),   # not a league
         (2021, "Defunct", 70.0, 2016, 2020, 5, 10, "v1")])           # no members in 2021
    conn.executemany("INSERT INTO conference_standings_by_year VALUES (?,?,?,?)",
                     [(2021, "Alpha", 1, 1), (2021, "Beta", 2, 1)])
    conn.commit()
    ranked = conference_coe2_rank(conn, 2021, "v1")
    assert [c for c, _ in ranked] == ["Beta", "Alpha"], "strongest first"
    assert all(isinstance(c, str) and isinstance(v, float) for c, v in ranked)
    assert "FBS Independents" not in dict(ranked)
    assert "Defunct" not in dict(ranked), "a conference with no members that season holds no rank"
    conn.close()


# ------------------------------------------- invariants on the real database

@pytest.fixture(scope="module")
def real(db_conn):
    if not db_conn.execute("SELECT COUNT(*) FROM hybrid_game_ratings").fetchone()[0]:
        pytest.skip("hybrid_game_ratings not built -- run src/build_hybrid_coefficients.py first")
    return db_conn


def test_every_real_season_total_reconciles(real):
    bad = real.execute("""SELECT COUNT(*) FROM team_coe2_by_season
                          WHERE ABS(season_coe2 - (game_coe_total + bonus_total)) > 1e-6""").fetchone()[0]
    assert bad == 0


def test_real_game_totals_match_the_game_awards_they_sum(real):
    mismatch = real.execute("""
        SELECT COUNT(*) FROM team_coe2_by_season t
        JOIN (SELECT h.team_id tid, g.season_year yr, SUM(h.game_coe) s, COUNT(*) n
              FROM hybrid_game_ratings h JOIN games g ON g.game_id = h.game_id
              WHERE h.game_coe IS NOT NULL GROUP BY 1, 2) x
          ON x.tid = t.team_id AND x.yr = t.season_year
        WHERE ABS(t.game_coe_total - x.s) > 1e-5 OR t.games_counted != x.n""").fetchone()[0]
    assert mismatch == 0


def test_real_bonus_totals_match_their_provenance_rows(real):
    bad = real.execute("""
        SELECT COUNT(*) FROM team_coe2_by_season t
        LEFT JOIN (SELECT team_id, season_year, SUM(points) p FROM team_coe2_bonuses GROUP BY 1, 2) b
          ON b.team_id = t.team_id AND b.season_year = t.season_year
        WHERE ABS(t.bonus_total - COALESCE(b.p, 0)) > 1e-6""").fetchone()[0]
    assert bad == 0


def test_a_real_national_title_is_never_counted_twice(real):
    """Most pre-BCS champions have an AP row and a Coaches row for one title."""
    assert real.execute("SELECT COUNT(*) FROM team_coe2_bonuses "
                        "WHERE category = 'national_title' AND count > 1").fetchone()[0] == 0
    total = real.execute("SELECT COALESCE(SUM(count), 0) FROM team_coe2_bonuses "
                         "WHERE category = 'national_title'").fetchone()[0]
    import csv as _csv
    from pathlib import Path
    rows = [r for r in _csv.DictReader(
        (Path(__file__).parent.parent / "data" / "reference" / "national_champions.csv")
        .open(encoding="utf-8")) if r["status"].strip() == "awarded"]
    assert total == len({(r["season_year"], r["team_name"]) for r in rows})


def test_the_real_conference_window_never_includes_its_own_season(real):
    bad = real.execute("""SELECT COUNT(*) FROM conference_coe2_5yr_by_season
                          WHERE window_end_year >= season_year""").fetchone()[0]
    assert bad == 0, "the entering-season value must be frozen before the season it names"


def test_the_real_conference_window_reconciles_to_its_season_contributions(real):
    """Each five-season value must be the decay-weighted sum of the per-season
    conference CoE 2.0 values inside its own window."""
    per = dict(real.execute(
        "SELECT conference || '|' || season_year, conference_coe2 FROM conference_coe2_by_season"))
    checked = 0
    for season, conf, value, start, end in real.execute(
            """SELECT season_year, conference, coe2_5yr, window_start_year, window_end_year
               FROM conference_coe2_5yr_by_season"""):
        expected = sum(per.get(f"{conf}|{y}", 0.0) * (0.92 ** (end - y)) for y in range(start, end + 1))
        assert value == pytest.approx(expected, abs=0.01), f"{conf} entering {season}"
        checked += 1
    assert checked > 300
