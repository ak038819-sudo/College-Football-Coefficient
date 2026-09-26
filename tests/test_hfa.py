"""
Team-specific home-field advantage (src/hfa.py, src/build_hfa.py): the validation
list from the HFA spec, plus consistency with the Elo engine. Analysis only --
nothing here touches production Elo.
"""
import datetime as dt
import sqlite3
from pathlib import Path

import pytest

from build_elo import expected_result, run_elo
from build_hfa import build, load_config, resolve_team
from hfa import (HomeGame, WeightedSums, apply_hfa_shrinkage, calculate_effective_sample_size,
                 calculate_hfa_prior, calculate_raw_hfa, calculate_team_hfa, convert_hfa_to_elo_points,
                 game_age_years, load_home_games, neutral_win_probability, recency_weight, weighted_sums)

REPO_ROOT = Path(__file__).resolve().parent.parent
CFG = load_config()
HCFG, SCALE = CFG["hfa"], CFG["elo"]["scale"]
AS_OF = dt.date(2026, 1, 1)


def G(i, date, p, actual, team=1, opp=2, d=0.0):
    return HomeGame(i, date.year, date, team, opp, 1500.0 + d, 1500.0, p, actual)


def test_neutral_probability_is_the_engine_formula_without_home_field():
    assert neutral_win_probability(1600, 1500, SCALE) == expected_result(1600, 1500, SCALE)
    assert neutral_win_probability(1600, 1500, SCALE) < expected_result(1600 + CFG["elo"]["home_field"], 1500, SCALE)


def test_raw_is_one_when_weighted_actual_equals_weighted_expected():
    games = [G(i, dt.date(2020, 9, 1 + i), 0.5, float(i % 2)) for i in range(20)]
    assert calculate_raw_hfa(weighted_sums(games, AS_OF, 10.0)) == pytest.approx(1.0, abs=0.02)


def test_one_half_life_is_half_weight_and_recent_games_count_more():
    assert recency_weight(10.0, 10.0) == pytest.approx(0.5)
    assert recency_weight(0.0, 10.0) == 1.0
    assert recency_weight(5.0, 10.0) == pytest.approx(0.7071, abs=1e-4)
    assert recency_weight(20.0, 10.0) == pytest.approx(0.25)
    assert game_age_years(dt.date(2016, 1, 1), AS_OF) == pytest.approx(10.0, abs=0.01)
    assert recency_weight(game_age_years(dt.date(2025, 9, 1), AS_OF), 10) > recency_weight(game_age_years(dt.date(1995, 9, 1), AS_OF), 10)


def test_future_games_never_count_and_negative_age_is_refused():
    games = [G(1, dt.date(2025, 9, 1), 0.5, 1.0), G(2, AS_OF, 0.5, 0.0), G(3, dt.date(2026, 9, 1), 0.5, 0.0)]
    s = weighted_sums(games, AS_OF, 10.0)
    assert s.games == 1 and s.newest == dt.date(2025, 9, 1)
    with pytest.raises(ValueError):
        recency_weight(-0.1, 10.0)


def test_team_with_zero_games_gets_exactly_the_prior():
    national = [G(i, dt.date(2024, 9, 1 + i), 0.5, 1.0 if i % 3 else 0.0, team=9) for i in range(24)]
    est = calculate_team_hfa([], national, AS_OF, HCFG, SCALE)
    assert est["raw_hfa"] is None and est["games_used"] == 0 and est["lambda"] == 0.0
    assert est["adjusted_hfa"] == est["prior_hfa"] == est["fbs_baseline"]
    assert est["elo_hfa_points"] is not None            # the national prior's bonus, never zero-by-default


def test_large_sample_is_dominated_by_observed_small_by_prior():
    adj, lam = apply_hfa_shrinkage(1.30, 1.10, n_eff=3000, k=30)
    assert lam > 0.99 and adj == pytest.approx(1.30, abs=0.003)
    adj, lam = apply_hfa_shrinkage(1.30, 1.10, n_eff=3, k=30)
    assert lam < 0.1 and abs(adj - 1.10) < abs(adj - 1.30)


def test_no_silent_divide_by_zero():
    assert calculate_raw_hfa(WeightedSums()) is None
    assert apply_hfa_shrinkage(None, 1.12, n_eff=0.0, k=30) == (1.12, 0.0)


def test_fcs_prior_falls_back_to_fbs_when_unavailable():
    assert calculate_hfa_prior(1.12) == 1.12
    assert calculate_hfa_prior(1.12, fcs_hfa=None, fcs_weight=0.9) == 1.12
    assert calculate_hfa_prior(1.12, fcs_hfa=1.30, fcs_weight=0.5) == pytest.approx(1.21)


def test_effective_sample_size_is_pluggable():
    s = weighted_sums([G(i, dt.date(2000 + i, 9, 1), 0.5, 1.0) for i in range(20)], AS_OF, 10.0)
    assert calculate_effective_sample_size(s, "sum_weights") == pytest.approx(s.sum_w)
    assert calculate_effective_sample_size(s, "kish") <= s.games
    with pytest.raises(ValueError):
        calculate_effective_sample_size(s, "made_up")


def test_elo_point_conversion_recovers_a_known_bonus():
    games = [G(i, dt.date(2015 + i % 10, 9, 1 + i % 25), 0.0, 1.0, d=(i * 37) % 300 - 150) for i in range(200)]
    for true_bonus in (-80.0, 0.0, 64.0, 150.0):
        target = sum(recency_weight(game_age_years(g.date, AS_OF), 10) *
                     expected_result(g.home_pre_elo + true_bonus, g.away_pre_elo, SCALE) for g in games)
        got, at_bound = convert_hfa_to_elo_points(games, AS_OF, 10.0, target, SCALE)
        assert not at_bound and got == pytest.approx(true_bonus, abs=1e-6)


# ---------------- pipeline-level tests on a tiny synthetic league ----------------

def _league(tmp_path, extra_games=(), name="hfa.db"):
    conn = sqlite3.connect(str(tmp_path / name))
    conn.executescript("""
        CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT NOT NULL);
        CREATE TABLE team_aliases (alias TEXT PRIMARY KEY, team_name TEXT NOT NULL);
        CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER, week INTEGER, game_date TEXT,
            home_team_id INTEGER, away_team_id INTEGER, home_score INTEGER, away_score INTEGER,
            went_ot INTEGER DEFAULT 0, neutral_site INTEGER DEFAULT 0, game_phase TEXT DEFAULT 'regular');
        CREATE TABLE elo_game_history (game_id INTEGER, team_id INTEGER, pregame_elo REAL, opponent_pregame_elo REAL,
            elo_expectation REAL, mov_multiplier REAL, elo_change REAL, postgame_elo REAL);
        INSERT INTO teams VALUES (1,'Alpha'),(2,'Bravo'),(3,'Charlie'),(4,'Delta');
        INSERT INTO team_aliases VALUES ('Alpha U', 'Alpha');
    """)
    gid, rows = 1, []
    for season in (2019, 2020, 2021):
        for wk in range(1, 9):
            h, a = (wk % 4) + 1, ((wk + 1) % 4) + 1
            rows.append((gid, season, wk, f"{season}-09-{wk + 1:02d}", h, a, 28 + (gid % 5), 21 + (gid % 11),
                         1 if wk == 8 else 0))
            gid += 1
    rows += list(extra_games)
    conn.executemany("INSERT INTO games (game_id, season_year, week, game_date, home_team_id, away_team_id,"
                     " home_score, away_score, neutral_site) VALUES (?,?,?,?,?,?,?,?,?)", rows)
    conn.row_factory = sqlite3.Row
    from build_elo import fetch_games_chronological
    elo_rows, _, _ = run_elo(fetch_games_chronological(conn), CFG["elo"])
    conn.row_factory = None
    # Name the columns and take only the original eight: run_elo's row also carries
    # the xSRDiff performance-layer fields (EXP-03), which HFA never reads, and a
    # positional insert would break every time that row grows.
    conn.executemany(
        "INSERT INTO elo_game_history (game_id, team_id, pregame_elo, opponent_pregame_elo,"
        " elo_expectation, mov_multiplier, elo_change, postgame_elo) VALUES (?,?,?,?,?,?,?,?)",
        [r[:8] for r in elo_rows])
    conn.commit()
    return conn


def test_neutral_site_games_are_excluded(tmp_path):
    conn = _league(tmp_path)
    games, skipped = load_home_games(conn, SCALE)
    neutral_ids = {r[0] for r in conn.execute("SELECT game_id FROM games WHERE neutral_site = 1")}
    assert neutral_ids and skipped["neutral_site"] == len(neutral_ids)
    assert not neutral_ids & {g.game_id for g in games}


def test_later_seasons_never_change_earlier_estimates(tmp_path):
    # Separate files rather than deleting and rebuilding one: Windows can't delete
    # a database file while a connection to it is open.
    conn = _league(tmp_path, name="before.db")
    base = {(r["team_id"], r["season_year"]): r for r in build(conn, CFG)[0]}
    conn.close()
    later = [(900 + i, 2022, i, f"2022-09-{i + 1:02d}", 1, 2, 70, 0, 0) for i in range(1, 6)]   # lopsided future games
    conn = _league(tmp_path, later, name="after.db")
    after = {(r["team_id"], r["season_year"]): r for r in build(conn, CFG)[0]}
    conn.close()
    for key, row in base.items():
        assert after[key] == row, f"estimate for {key} changed when LATER games were added"


def test_missing_elo_is_skipped_not_fatal(tmp_path):
    conn = _league(tmp_path)
    conn.execute("DELETE FROM elo_game_history WHERE game_id = 3")
    games, skipped = load_home_games(conn, SCALE)
    assert skipped["missing_elo"] == 1 and 3 not in {g.game_id for g in games}
    assert build(conn, CFG)[1]


def test_aliases_resolve_to_the_canonical_team(tmp_path):
    conn = _league(tmp_path)
    assert resolve_team(conn, "Alpha U") == resolve_team(conn, "Alpha") == 1


def test_same_input_gives_identical_output(tmp_path):
    conn = _league(tmp_path)
    assert build(conn, CFG) == build(conn, CFG)


# ---------------- real database ----------------

def test_real_estimates_never_use_games_on_or_after_as_of(db_path):
    conn = sqlite3.connect(str(db_path))
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='team_hfa_by_season'").fetchone():
        pytest.skip("team_hfa_by_season not built -- run src/build_hfa.py")
    bad = conn.execute("SELECT COUNT(*) FROM team_hfa_by_season WHERE newest_game_used >= as_of").fetchone()[0]
    assert bad == 0


# ---------------- the guide's validation list, on data rather than on formulas ----------------

def _hand_built(tmp_path, games, elo, name="hand.db"):
    """
    A database with pregame Elo written by hand, so a test can set the exact
    ratings it wants to reason about instead of whatever run_elo happens to
    produce. `games` rows: (game_id, date, home, away, home_score, away_score,
    neutral); `elo` maps game_id -> (home_pre, away_pre).
    """
    conn = sqlite3.connect(str(tmp_path / name))
    conn.executescript("""
        CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT NOT NULL);
        CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER, week INTEGER, game_date TEXT,
            home_team_id INTEGER, away_team_id INTEGER, home_score INTEGER, away_score INTEGER,
            went_ot INTEGER DEFAULT 0, neutral_site INTEGER DEFAULT 0, game_phase TEXT DEFAULT 'regular');
        CREATE TABLE elo_game_history (game_id INTEGER, team_id INTEGER, pregame_elo REAL,
            opponent_pregame_elo REAL, elo_expectation REAL, mov_multiplier REAL, elo_change REAL,
            postgame_elo REAL);
        INSERT INTO teams VALUES (1,'Alpha'),(2,'Bravo'),(3,'Charlie');
    """)
    for gid, date, home, away, hs, as_, neutral in games:
        conn.execute("INSERT INTO games (game_id, season_year, week, game_date, home_team_id, away_team_id,"
                     " home_score, away_score, neutral_site) VALUES (?,?,?,?,?,?,?,?,?)",
                     (gid, int(date[:4]), 1, date, home, away, hs, as_, neutral))
        h_pre, a_pre = elo[gid]
        for tid, pre, opp in ((home, h_pre, a_pre), (away, a_pre, h_pre)):
            conn.execute("INSERT INTO elo_game_history (game_id, team_id, pregame_elo, opponent_pregame_elo,"
                         " elo_expectation, mov_multiplier, elo_change, postgame_elo)"
                         " VALUES (?,?,?,?,?,?,?,?)", (gid, tid, pre, opp, 0.5, 1.0, 0.0, pre))
    conn.commit()
    return conn


def test_a_win_a_loss_and_a_tie_score_one_zero_and_a_half(tmp_path):
    conn = _hand_built(tmp_path,
                       [(1, "2020-09-05", 1, 2, 28, 21, 0),      # Alpha wins at home
                        (2, "2020-09-12", 1, 2, 14, 35, 0),      # Alpha loses at home
                        (3, "2020-09-19", 1, 2, 17, 17, 0)],     # tie
                       {1: (1500.0, 1500.0), 2: (1500.0, 1500.0), 3: (1500.0, 1500.0)})
    got = {g.game_id: g.actual for g in load_home_games(conn, SCALE)[0]}
    assert got == {1: 1.0, 2: 0.0, 3: 0.5}


def test_two_equally_rated_teams_give_a_neutral_expectation_of_exactly_one_half(tmp_path):
    """
    The circularity guard, checked on the value the pipeline actually carries
    rather than on the formula in isolation: if a home bonus ever leaks into the
    neutral expectation, this game's P stops being 0.5 and every team's HFA is
    measured against an expectation that already contains home field.
    """
    conn = _hand_built(tmp_path, [(1, "2020-09-05", 1, 2, 28, 21, 0)], {1: (1500.0, 1500.0)})
    g = load_home_games(conn, SCALE)[0][0]
    assert g.p_neutral == 0.5
    assert g.p_neutral <= 0.5          # specifically NOT above 0.5
    assert calculate_raw_hfa(weighted_sums([g], AS_OF, 10.0)) == pytest.approx(2.0)   # 1 win / 0.5 expected


def test_the_elo_favourite_is_expected_to_win_more_than_half_its_home_games(tmp_path):
    conn = _hand_built(tmp_path, [(1, "2020-09-05", 1, 2, 28, 21, 0)], {1: (1700.0, 1500.0)})
    g = load_home_games(conn, SCALE)[0][0]
    assert g.p_neutral > 0.5
    assert g.p_neutral == expected_result(1700.0, 1500.0, SCALE)


def test_a_seasons_before_min_season_are_excluded(tmp_path):
    conn = _hand_built(tmp_path,
                       [(1, "1975-09-05", 1, 2, 28, 21, 0), (2, "1985-09-05", 1, 2, 28, 21, 0)],
                       {1: (1500.0, 1500.0), 2: (1500.0, 1500.0)})
    games, skipped = load_home_games(conn, SCALE, min_season=1980)
    assert [g.game_id for g in games] == [2] and skipped["before_min_season"] == 1
    assert len(load_home_games(conn, SCALE)[0]) == 2          # no floor unless one is asked for


def test_a_brand_new_program_lands_on_the_national_prior_not_zero_or_one(tmp_path):
    """
    The guide's new-FBS-program case: North Dakota State, Sacramento State and the
    like must fall to the measured national baseline, not to 0, not to NULL, and
    not to 1.00 unless the baseline itself is 1.00.
    """
    # A league where home teams beat the neutral expectation, so the baseline is
    # clearly not 1.00 and a team defaulting to 1.00 would be visible.
    national = [G(i, dt.date(2024, 9, 1 + (i % 28)), 0.5, 1.0 if i % 4 else 0.0, team=9) for i in range(40)]
    est = calculate_team_hfa([], national, AS_OF, HCFG, SCALE)
    assert est["fbs_baseline"] == pytest.approx(1.5, abs=0.05)     # 3 wins in 4 against a coin flip
    assert est["adjusted_hfa"] == est["prior_hfa"] == est["fbs_baseline"]
    assert est["adjusted_hfa"] not in (0.0, 1.0) and est["adjusted_hfa"] is not None
    assert est["raw_hfa"] is None                                  # no evidence, and none invented


def test_the_fcs_prior_is_a_switch_and_off_by_default_in_the_config():
    assert calculate_hfa_prior(1.12, fcs_hfa=1.30, fcs_weight=0.5, use_fcs_prior=False) == 1.12
    assert calculate_hfa_prior(1.12, fcs_hfa=1.30, fcs_weight=0.5, use_fcs_prior=True) == pytest.approx(1.21)
    assert HCFG["use_fcs_prior"] is False
    # The FBS-only path is the one that has to work: no FCS history exists today.
    assert calculate_hfa_prior(1.12, fcs_hfa=None, use_fcs_prior=True) == 1.12


def test_the_national_baseline_is_measured_and_not_assumed_to_be_one(db_path):
    """
    The whole point of the model is how far home teams beat neutral expectations,
    so a baseline pinned at 1.00 would mean it had been assumed rather than fitted.
    """
    conn = sqlite3.connect(str(db_path))
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='team_hfa_current'").fetchone():
        pytest.skip("team_hfa_current not built -- run src/build_hfa.py")
    baselines = [r[0] for r in conn.execute("SELECT DISTINCT fbs_baseline FROM team_hfa_current")]
    assert len(baselines) == 1 and baselines[0] > 1.02, baselines


def test_the_neutral_expectation_is_not_the_stored_home_inclusive_one(db_path):
    """
    elo_game_history.elo_expectation already contains the engine's flat home bonus.
    Recomputing P from pregame_elo instead is what keeps home field out of the
    quantity this model measures home teams against -- so for a real non-neutral
    game the two numbers must differ, in the direction the bonus implies.
    """
    conn = sqlite3.connect(str(db_path))
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='elo_game_history'").fetchone():
        pytest.skip("elo_game_history not built -- run src/build_elo.py")
    games, _ = load_home_games(conn, SCALE, HCFG.get("min_season"))
    if not games:
        pytest.skip("no qualifying home games in this database")
    stored = dict(conn.execute("""SELECT g.game_id, eh.elo_expectation FROM games g
                                 JOIN elo_game_history eh ON eh.game_id = g.game_id
                                                         AND eh.team_id = g.home_team_id"""))
    assert CFG["elo"]["home_field"] > 0
    checked = 0
    for g in games[-500:]:
        assert g.p_neutral < stored[g.game_id], f"game {g.game_id} used the home-inclusive expectation"
        checked += 1
    assert checked == 500


def test_ci_builds_hfa_after_elo_and_never_before_it():
    """
    Every estimate reads pregame ratings out of elo_game_history, so a workflow
    that ran build_hfa.py first would silently produce nothing (every game skipped
    for missing Elo) rather than failing. Cannot be skipped: it reads the workflow.
    """
    text = (REPO_ROOT / ".github" / "workflows" / "ci-and-deploy.yml").read_text(encoding="utf-8")
    hfa = [i for i, ln in enumerate(text.splitlines()) if "src/build_hfa.py" in ln and "#" not in ln.split("python3")[0]]
    elo = [i for i, ln in enumerate(text.splitlines()) if "src/build_elo.py" in ln]
    assert hfa, "the workflow never builds the HFA tables"
    for h in hfa:
        assert any(e < h for e in elo), "build_hfa.py runs with no build_elo.py before it"
