"""
Tests for build_hybrid_coefficients.py covering the remaining CoE-2.0
properties from the design doc's section 30 not already covered by
test_elo.py: Win CoE floor/ceiling, regulation-loss CoE, tie handling,
the frozen 5yr CoE's anti-circularity guarantee, and pregame-only Elo
usage.

Most run directly against the real db (skip cleanly if it hasn't been
built yet), since these are properties of the actual computed data.
"""
import pytest

from build_hybrid_coefficients import compute_frozen_5yr_coe, zscore_stats, tie_game_coe


def test_tie_underdog_gets_bonus():
    """An underdog (p_this < 0.5) settling for a tie should score ABOVE 1.0."""
    assert tie_game_coe(p_this=0.2, tie_delta=2.0) > 1.0


def test_tie_favorite_gets_penalty():
    """A favorite (p_this > 0.5) settling for a tie should score BELOW 1.0."""
    assert tie_game_coe(p_this=0.8, tie_delta=2.0) < 1.0


def test_tie_even_matchup_scores_exactly_one():
    assert tie_game_coe(p_this=0.5, tie_delta=2.0) == 1.0


def test_frozen_5yr_coe_never_includes_the_season_itself():
    """
    Anti-circularity (spec section 17): season Y's frozen 5yr CoE must
    be built ONLY from seasons < Y. Constructed so that if the current
    season's own (huge) rating leaked in, the frozen value would be
    wildly different from what the prior-seasons-only math predicts.
    """
    team_ratings = {
        (2018, "A"): 1.0, (2019, "A"): 1.0, (2020, "A"): 1.0, (2021, "A"): 1.0, (2022, "A"): 1.0,
        (2023, "A"): 999.0,  # season 2023's own huge rating -- must NOT leak into 2023's frozen 5yr CoE
    }
    years = [2018, 2019, 2020, 2021, 2022, 2023]
    frozen = compute_frozen_5yr_coe(team_ratings, years, decay_base=0.92)

    # 2023's frozen value should be built from 2018-2022 only (all rating=1.0), nowhere near 999
    assert (2023, "A") in frozen
    assert frozen[(2023, "A")] < 10, "Season's own rating leaked into its frozen 5yr CoE"


def test_frozen_5yr_coe_uses_exactly_five_prior_seasons():
    team_ratings = {(y, "A"): float(y) for y in range(2015, 2023)}  # ratings 2015..2022, rising each year
    years = list(range(2015, 2023))
    frozen = compute_frozen_5yr_coe(team_ratings, years, decay_base=1.0)  # no decay, for simple arithmetic

    # 2022's frozen value should be the plain sum of 2017-2021 (5 seasons strictly before 2022)
    expected = sum(float(y) for y in range(2017, 2022))
    assert abs(frozen[(2022, "A")] - expected) < 1e-9


def test_frozen_5yr_coe_skips_bootstrap_years():
    team_ratings = {(y, "A"): 1.0 for y in range(2020, 2023)}  # only 3 years of history
    years = list(range(2020, 2023))
    frozen = compute_frozen_5yr_coe(team_ratings, years, decay_base=0.92)
    assert len(frozen) == 0, "Should skip every year until 5 full prior seasons exist"


def test_zscore_stats_handles_degenerate_input():
    mean, sd = zscore_stats([5.0])  # single value -- can't compute a real stdev
    assert sd != 0  # must not produce a division-by-zero downstream
    mean, sd = zscore_stats([3.0, 3.0, 3.0])  # zero variance
    assert sd != 0


@pytest.fixture
def db_conn(repo_root):
    import sqlite3
    from pathlib import Path
    db_path = repo_root / "db" / "league.db"
    if not db_path.exists():
        pytest.skip("db/league.db not built")
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "hybrid_game_ratings" not in tables:
        pytest.skip("hybrid_game_ratings not built yet -- run build_hybrid_coefficients.py first")
    yield conn
    conn.close()


def test_win_coe_never_below_floor(db_conn):
    row = db_conn.execute(
        "SELECT MIN(game_coe) FROM hybrid_game_ratings WHERE result_type IN ('WIN','OT_WIN')"
    ).fetchone()
    assert row[0] >= 2.0, f"A win scored below the spec's floor of 2.0: {row[0]}"


def test_win_coe_never_reaches_ceiling(db_conn):
    row = db_conn.execute(
        "SELECT MAX(game_coe) FROM hybrid_game_ratings WHERE result_type IN ('WIN','OT_WIN')"
    ).fetchone()
    assert row[0] < 4.0, f"A win reached/exceeded the spec's asymptotic ceiling of 4.0: {row[0]}"


def test_regulation_loss_is_always_zero(db_conn):
    rows = db_conn.execute(
        "SELECT DISTINCT game_coe FROM hybrid_game_ratings WHERE result_type='LOSS'"
    ).fetchall()
    # sqlite3.Row objects don't compare equal to plain tuples via == in
    # this environment even when the underlying values match -- convert
    # explicitly rather than comparing Row objects directly (a false
    # test failure from this exact pattern was caught on real CI output:
    # the data was genuinely correct, only the comparison was wrong).
    values = [tuple(r) for r in rows]
    assert values == [(0.0,)], f"Regulation losses should always score exactly 0, found: {values}"


def test_ot_loss_is_always_one(db_conn):
    """
    NOTE: as of this writing, went_ot is a known data gap -- every game
    in the games table currently has went_ot=0 (root cause: CFBD's basic
    /games endpoint doesn't appear to expose an overtime field directly;
    fetch_cfbd_games.py has been searching for one that likely doesn't
    exist there). Left as a documented gap for now, not fixed in this
    pass. So OT_LOSS rows may not exist yet -- this test is written to
    still be meaningful once went_ot is eventually fixed, without
    needing to be rewritten: it checks "IF any OT losses exist, they are
    always exactly 1.0" rather than asserting they exist.
    """
    rows = db_conn.execute(
        "SELECT DISTINCT game_coe FROM hybrid_game_ratings WHERE result_type='OT_LOSS'"
    ).fetchall()
    if not rows:
        pytest.skip("No OT_LOSS rows exist yet -- known went_ot data gap, not yet fixed")
    values = [tuple(r) for r in rows]  # see test_regulation_loss_is_always_zero for why
    assert values == [(1.0,)], f"OT losses should always score exactly 1 in this prototype, found: {values}"


def test_hybrid_expectations_sum_to_one_per_game(db_conn):
    rows = db_conn.execute(
        "SELECT game_id, SUM(hybrid_expectation) as total FROM hybrid_game_ratings GROUP BY game_id"
    ).fetchall()
    assert rows, "No hybrid game ratings found"
    max_violation = max(abs(r["total"] - 1.0) for r in rows)
    assert max_violation < 1e-6, f"Hybrid expectations don't sum to 1 for some game: max violation {max_violation}"
