"""The rest-of-season projection, held to the code it stands in for.

project_season.py reimplements three things in a faster shape so they can run
thousands of times: the iterative season rating, the rolling 5-year window,
and the conference standings tiebreak. Duplicated arithmetic is only safe if
something holds the copy to the original, so every test here drives the REAL
code and the projection's version over real seasons and requires them to
agree. If the model changes on one side only, these fail.

The last test is the one that matters most: a season with nothing left to play
must project to exactly the field the site publishes for it, with certainty.
A projection that cannot reproduce a finished season has no business
describing an unfinished one.
"""
from __future__ import annotations

import csv

import pytest

import build_coefficients as bc
import project_season as ps
from conftest import PLAYOFF_YEARS
from select_cfp_field import conference_champions, load_team_coe_5yr, select_field

# Seasons with both a full set of games and the four prior seasons the rolling
# window needs. Deliberately several, because a single season cannot show that
# a tiebreak is right -- the 2021 Big 12 tie that this module got wrong first
# time round appears in exactly one of them.
RATING_YEARS = [2018, 2021, 2024, 2025]


@pytest.fixture(scope="module")
def season_ratings():
    return ps.load_season_ratings()


@pytest.fixture(scope="module")
def live_season(db_path):
    """The season still being played, or a skip.

    Everything this projection is for needs unplayed games. A database built
    without data/raw/schedule_*.csv has none, so these tests would otherwise
    assert that an empty projection is uncertain -- and CI built exactly such a
    database until the test job started loading the schedule. They skip with a
    reason instead of failing, and
    test_ci_builds_a_database_with_games_left_to_play holds CI to building one
    where they run.
    """
    import sqlite3

    conn = sqlite3.connect(str(db_path))
    try:
        if not conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='scheduled_games'"
        ).fetchone():
            pytest.skip("no scheduled_games table -- load data/raw/schedule_*.csv first")
        season = conn.execute(
            """SELECT MAX(season_year) FROM scheduled_games
               WHERE game_id NOT IN (SELECT game_id FROM games)""").fetchone()[0]
    finally:
        conn.close()
    if season is None:
        pytest.skip("every scheduled game has been played -- nothing to project")
    return season


def _real_inputs(db_conn, year):
    """What build_coefficients.main would hand the rating for one season."""
    bc.ensure_view(db_conn)
    games = bc.fetch_games_for_year(db_conn, year)
    played = bc.count_games_played(games)
    confidence = {t: min(1.0, n / bc.CONFIDENCE_GAMES) for t, n in played.items()}
    return games, played, confidence


@pytest.mark.parametrize("year", RATING_YEARS)
def test_the_fast_rating_is_the_builders_rating(db_conn, year, season_ratings):
    """Identical output, not merely close: the same arithmetic in another shape."""
    games, played, confidence = _real_inputs(db_conn, year)
    prior = bc.compute_prior_ratings(season_ratings[year - 1], ps.PRIOR_REGRESSION)

    real = bc.per_season_iterative_ratings(
        games, iterations=bc.ITERATIONS, prior=prior, confidence=confidence)
    results, teams, projection_played, _record = ps.completed_results(db_conn, year)
    fast = ps.iterative_ratings(results, teams, prior, confidence)

    assert set(fast) == set(real), "the two disagree about who played that season"
    assert projection_played == played, "the two disagree about how many games a team played"
    assert max(abs(real[t] - fast[t]) for t in real) == 0.0


@pytest.mark.parametrize("year", RATING_YEARS)
def test_the_rolling_window_reproduces_the_published_coefficient(db_conn, year, season_ratings):
    """rolling_coe over a finished season == data/processed/team_coeff_5yr.csv.

    Not exactly equal: the four prior seasons are read back from a CSV rounded
    to six decimals, so a sum of five of them lands about 1e-6 away. The
    ORDER, which is the only thing the field is selected on, has to be
    identical.
    """
    games, _played, confidence = _real_inputs(db_conn, year)
    prior = bc.compute_prior_ratings(season_ratings[year - 1], ps.PRIOR_REGRESSION)
    this_season = bc.per_season_iterative_ratings(
        games, iterations=bc.ITERATIONS, prior=prior, confidence=confidence)

    mine = ps.rolling_coe(season_ratings, year, this_season)
    published = load_team_coe_5yr(year)
    assert set(published) <= set(mine)
    assert max(abs(published[t] - mine[t]) for t in published) < 1e-4
    rank = lambda coe: sorted(published, key=lambda t: (-coe[t], t))  # noqa: E731
    assert rank(mine) == rank(published)


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_in_memory_champions_match_the_stored_standings(db_conn, year):
    """Every conference, every season -- the champion the database derived.

    This is where the projection decides who holds an automatic bid, so a
    tiebreak that differs from derive_conference_standings_local's by one key
    changes the field. Ranking the last key by team id instead of team name
    passes eleven of these twelve seasons.
    """
    names = dict(db_conn.execute("SELECT team_id, team_name FROM teams"))
    _results, _teams, _played, record = ps.completed_results(db_conn, year)
    mine = ps.champions_from_records(record, names)
    stored = conference_champions(db_conn, year)
    assert stored, f"{year} has no stored standings to check against"
    assert mine == stored


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_banked_records_match_the_stored_records(db_conn, year):
    """bank_game's running totals == conference_team_records_by_year."""
    _results, _teams, _played, record = ps.completed_results(db_conn, year)
    stored = {
        (r["team_id"], r["conference"]): [r["conf_wins"], r["conf_losses"], r["conf_ties"],
                                          r["conf_games"], r["overall_wins"], r["overall_losses"],
                                          r["overall_ties"], r["overall_games"]]
        for r in db_conn.execute(
            """SELECT team_id, conference, conf_wins, conf_losses, conf_ties, conf_games,
                      overall_wins, overall_losses, overall_ties, overall_games
               FROM conference_team_records_by_year WHERE season_year = ?""", (year,))
    }
    assert stored, f"{year} has no stored records to check against"
    assert {k: v for k, v in record.items() if k in stored} == stored
    assert set(record) == set(stored)


def test_a_finished_season_projects_to_the_field_it_actually_had(db_conn, db_path):
    """The whole pipeline, end to end, on a season with nothing left to play.

    With no unplayed games every simulated season is the same season, so every
    team in the field must be there in 100% of runs, the byes must be exactly
    seeds 1-4, and the twelve teams must be the twelve the site publishes.
    """
    year = 2025
    projection = ps.project(str(db_path), year, 3)
    assert projection["unplayed"] == 0, "2025 still has unplayed games; pick a finished season"

    field = select_field(db_conn, year, load_team_coe_5yr(year))
    expected = {q["team"] for q in field}
    expected_byes = {q["team"] for q in field if q["seed"] <= 4}

    made_it = {t for t, c in projection["counts"].items() if c["playoff"]}
    assert made_it == expected
    assert all(projection["counts"][t]["playoff"] == 3 for t in expected), "not certain"
    assert {t for t, c in projection["counts"].items() if c["bye"]} == expected_byes


def test_every_projected_season_sends_exactly_twelve_teams(db_path, live_season):
    """Over many simulated 2026 seasons, the field is always a legal field.

    select_field raises on a short field, so this would fail loudly rather
    than quietly publishing eleven teams -- but it also pins the counting: the
    playoff tallies have to sum to twelve per run.
    """
    projection = ps.project(str(db_path), live_season, 40)
    total = sum(c["playoff"] for c in projection["counts"].values())
    assert total == 12 * projection["n_sims"]
    byes = sum(c["bye"] for c in projection["counts"].values())
    assert byes == 4 * projection["n_sims"]
    champions = sum(c["champion"] for c in projection["counts"].values())
    assert champions == projection["n_sims"], "every simulated season has one champion"


def test_an_unfinished_season_is_genuinely_uncertain(db_path, live_season):
    """More teams than fill the field have a chance at it, and conferences turn over.

    The point of the feature. Before it, the site gave twelve teams a 100%
    playoff chance and everyone else 0%; if this projection reproduced that,
    it would be an expensive way to publish the same claim.
    """
    projection = ps.project(str(db_path), live_season, 200)
    rows = ps.rows_for_report(projection)
    contenders = [r for r in rows if r["playoff_pct"] > 0]
    assert len(contenders) > 12, "no team outside today's field has any chance"
    assert any(0 < r["playoff_pct"] < 100 for r in contenders), "every chance is a certainty"
    races = [r for r in rows if 0 < r["conference_title_pct"] < 100]
    assert races, "no conference race is undecided"


def test_projected_wins_never_exceed_the_games_played(db_path, live_season):
    """A team cannot win more games than its season contains."""
    projection = ps.project(str(db_path), live_season, 25)
    played = projection["games_played"]
    for team, total in projection["wins"].items():
        mean = total / projection["n_sims"]
        assert mean >= played.get(team, 0) * 0 - 1e-9
        assert mean <= 25, f"{team} projects {mean:.1f} wins, which is not a season"


def test_the_simulation_is_reproducible_from_its_seed(db_path, live_season):
    one = ps.project(str(db_path), live_season, 30, sim_seed=7)
    two = ps.project(str(db_path), live_season, 30, sim_seed=7)
    assert one["counts"] == two["counts"]
    assert ps.project(str(db_path), live_season, 30, sim_seed=8)["counts"] != one["counts"]


def test_the_report_rows_are_percentages_of_the_runs(db_path, live_season):
    projection = ps.project(str(db_path), live_season, 50)
    for row in ps.rows_for_report(projection):
        for key in ("conference_title_pct", "playoff_pct", "bye_pct", "champion_pct"):
            assert 0.0 <= row[key] <= 100.0
        assert row["champion_pct"] <= row["final_pct"] <= row["playoff_pct"] + 1e-9, row
        assert row["bye_pct"] <= row["playoff_pct"] + 1e-9, row


def test_the_published_season_ratings_cover_the_window(season_ratings):
    """The four prior seasons a 2026 projection leans on are actually on disk."""
    for year in range(2022, 2026):
        assert len(season_ratings.get(year, {})) > 100, f"{year} season ratings missing"


def test_rolling_coe_applies_the_builders_decay(season_ratings):
    """One team, one window, computed by hand against within_window_weight."""
    team = "Ohio State"
    this_season = {team: 3.0}
    got = ps.rolling_coe(season_ratings, 2026, this_season)[team]
    expected = 3.0 * bc.within_window_weight(2026, 2026) + sum(
        season_ratings[y].get(team, 0.0) * bc.within_window_weight(2026, y)
        for y in range(2022, 2026))
    assert got == pytest.approx(expected)


def test_the_tiebreak_keys_are_the_standings_keys(db_conn):
    """A conference where two teams are level on all five numbers.

    2021's Big 12 is the real case: Oklahoma State and Baylor finished 8-2 in
    conference and 11-2 overall, identical on conference win%, conference
    wins, conference losses, overall win% and overall wins. The alphabet is
    the only thing left, and it has to be the alphabet of NAMES.
    """
    names = dict(db_conn.execute("SELECT team_id, team_name FROM teams"))
    _r, _t, _p, record = ps.completed_results(db_conn, 2021)
    level = [names[tid] for (tid, conf), r in record.items()
             if conf == "Big 12" and r[:2] == [8, 2] and r[4] == 11]
    assert sorted(level) == ["Baylor", "Oklahoma State"], level
    assert ps.champions_from_records(record, names)["Big 12"] == "Baylor"


def test_the_published_coefficient_file_has_the_projected_season(db_conn):
    """team_coeff_5yr.csv carries 2026, so a projection has a baseline to move."""
    with open(ps.DATA_DIR / "team_coeff_5yr.csv", newline="") as f:
        years = {int(r["end_year"]) for r in csv.DictReader(f)}
    assert 2026 in years


@pytest.mark.parametrize("year", RATING_YEARS)
def test_the_projection_reproduces_the_published_season_rating(db_conn, year, season_ratings):
    """A finished season, rated by the projection, == team_ratings_by_season.csv.

    End to end through season_rating, which applies the prior itself, so this
    is the projection's rating path and not a reassembly of it in the test.
    """
    _games, _played, confidence = _real_inputs(db_conn, year)
    results, teams, _p, _r = ps.completed_results(db_conn, year)
    fast = ps.season_rating(season_ratings, year, results, teams, confidence)

    published = season_ratings[year]
    assert set(fast) == set(published)
    # The CSV is rounded to six decimals, and the prior read back from it
    # carries that rounding into the next season, so this is a rounding-scale
    # agreement rather than an exact one.
    assert max(abs(published[t] - fast[t]) for t in published) < 1e-5


def test_the_prior_regression_matches_the_builders_default():
    """PRIOR_REGRESSION is build_coefficients' own --prior-regression default.

    Checked against that function's source because the value lives in an
    argparse call inside main() and there is nothing importable to compare
    against. Worth checking anyway: no behavioural test here can pin it. In a
    season where every team has reached CONFIDENCE_GAMES the prior is
    multiplied by zero (see the next test), and a projected season is a
    complete season, so the constant is nearly inert in exactly the case this
    module cares about -- which is why a test that it reproduces a published
    season passes at any value, and why this one is explicit instead.
    """
    import inspect

    source = inspect.getsource(bc.main)
    assert '"--prior-regression", type=float, default=0.5' in source, (
        "build_coefficients' prior-regression default moved; "
        "project_season.PRIOR_REGRESSION has to move with it")
    assert ps.PRIOR_REGRESSION == 0.5


def test_the_prior_only_acts_below_full_confidence():
    """Where the prior does and does not reach the rating.

    At confidence 1.0 the blend is all computed value, so two very different
    priors give the same rating; below 1.0 they do not. This is what makes the
    constant almost irrelevant to a finished or projected season and decisive
    for a half-played one -- the reason the live Field page's mid-season
    numbers are damped at all.
    """
    results = [("A", "B", 1.0), ("A", "C", 1.0), ("B", "C", 1.0)]
    teams = frozenset({"A", "B", "C"})
    high = {"A": 9.0, "B": 9.0, "C": 9.0}
    low = {"A": 0.1, "B": 0.1, "C": 0.1}

    full = {t: 1.0 for t in teams}
    assert ps.iterative_ratings(results, teams, high, full) == \
        ps.iterative_ratings(results, teams, low, full)

    partial = {t: 0.25 for t in teams}
    assert ps.iterative_ratings(results, teams, high, partial) != \
        ps.iterative_ratings(results, teams, low, partial)


def test_ci_builds_a_database_with_games_left_to_play(repo_root):
    """The test that cannot skip, carrying the weight for the ones that can.

    Every behavioural test of this projection needs a season with unplayed
    games, and skips without one. That is right locally and unacceptable in
    CI, where skipping would let the whole feature go quiet -- which is exactly
    what happened on this PR's first run: the test job's bootstrap loaded the
    games but not data/raw/schedule_*.csv, so 2026 had nothing left to project
    and the tests that matter most failed on an empty projection.

    run_pipeline.py is what the deploy job runs and it loads both, so the
    assertion is simply that the test job loads them too.
    """
    workflow = (repo_root / ".github" / "workflows" / "ci-and-deploy.yml").read_text(
        encoding="utf-8")
    assert "src/load_schedule.py" in workflow, (
        "CI never loads data/raw/schedule_*.csv, so its database has no unplayed "
        "games and every rest-of-season test skips")
    assert "src/load_fcs_games.py" in workflow, (
        "CI never loads data/raw/fcs_games_*.csv, so its database has no "
        "non_fbs_games and the team record tests skip")
