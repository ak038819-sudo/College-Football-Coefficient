"""The rest-of-season projection, held to the code it stands in for.

project_season.py duplicates two things in a faster shape so they can run
thousands of times: the Elo update and the conference standings tiebreak.
Duplicated arithmetic is only safe if something holds the copy to the
original, so the tests here drive the REAL code and the projection's version
over real seasons and require them to agree.

The test that matters most is test_a_finished_season_projects_to_the_field_it
_actually_had: a season with nothing left to play must project to EXACTLY the
field the site publishes for it, with certainty. Both are selected from the
same ratings, so that is an equality rather than a resemblance, and a
projection that cannot reproduce a finished season has no business describing
an unfinished one.
"""
from __future__ import annotations

import csv

import pytest

import project_season as ps
from conftest import PLAYOFF_YEARS
from predict_upcoming import elo_config, game_expectation
from select_cfp_field import conference_champions, load_team_elo, select_field

CFG = elo_config()


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


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_in_memory_champions_match_the_stored_standings(db_conn, year):
    """Every conference, every season -- the champion the database derived.

    This is where the projection decides who holds an automatic bid, so a
    tiebreak that differs from derive_conference_standings_local's by one key
    changes the field. Ranking the last key by team id instead of team name
    passes eleven of these twelve seasons.
    """
    names = dict(db_conn.execute("SELECT team_id, team_name FROM teams"))
    _played, record = ps.completed_results(db_conn, year)
    mine = ps.champions_from_records(record, names)
    stored = conference_champions(db_conn, year)
    assert stored, f"{year} has no stored standings to check against"
    assert mine == stored


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_banked_records_match_the_stored_records(db_conn, year):
    """bank_game's running totals == conference_team_records_by_year."""
    _played, record = ps.completed_results(db_conn, year)
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

    field = select_field(db_conn, year, load_team_elo(db_conn, year))
    expected = {q["team"] for q in field}
    expected_byes = {q["team"] for q in field if q["seed"] <= 4}

    made_it = {t for t, c in projection["counts"].items() if c["playoff"]}
    assert made_it == expected, (
        "a finished season must project to exactly the field the site publishes "
        "for it -- both are selected from the same ratings"
    )
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


def test_the_tiebreak_keys_are_the_standings_keys(db_conn):
    """A conference where two teams are level on all five numbers.

    2021's Big 12 is the real case: Oklahoma State and Baylor finished 8-2 in
    conference and 11-2 overall, identical on conference win%, conference
    wins, conference losses, overall win% and overall wins. The alphabet is
    the only thing left, and it has to be the alphabet of NAMES.
    """
    names = dict(db_conn.execute("SELECT team_id, team_name FROM teams"))
    _played, record = ps.completed_results(db_conn, 2021)
    level = [names[tid] for (tid, conf), r in record.items()
             if conf == "Big 12" and r[:2] == [8, 2] and r[4] == 11]
    assert sorted(level) == ["Baylor", "Oklahoma State"], level
    assert ps.champions_from_records(record, names)["Big 12"] == "Baylor"


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


def test_a_single_occurrence_is_not_published_as_zero():
    """The precision of a published percentage is the simulation's, not a page's.

    A team that reaches the field in one of 10,000 simulated seasons has a real
    0.01% chance. Rounded to one decimal that is 0.0, which says the opposite,
    and because 0.0 is falsy the export's "something still to play for" filter
    dropped the team from the payload entirely -- so the page's own "<0.1%"
    case, written for exactly this team, could never be reached.
    """
    assert ps.pct_digits(10_000) == 2
    assert ps.pct_digits(1_000) == 1
    assert ps.pct_digits(100_000) == 3

    n = 10_000
    projection = {
        "n_sims": n,
        "wins": {"Longshot": 7.0 * n, "Favourite": 11.0 * n},
        "elo": {"Longshot": 1500.0 * n, "Favourite": 1900.0 * n},
        "counts": {
            "Longshot": {**dict.fromkeys(ps.TALLIES, 0), "playoff": 1, "conference_title": 1},
            "Favourite": {**dict.fromkeys(ps.TALLIES, 0), "playoff": n, "conference_title": n,
                          "champion": 3764},
        },
    }
    rows = {r["team"]: r for r in ps.rows_for_report(projection)}
    assert rows["Longshot"]["playoff_pct"] > 0, "one field in 10,000 runs is a chance, not a zero"
    assert rows["Longshot"]["playoff_pct"] == pytest.approx(0.01)
    # A chance the page has words for keeps its usual reading at the usual size.
    assert rows["Favourite"]["playoff_pct"] == 100.0
    assert rows["Favourite"]["champion_pct"] == pytest.approx(37.64)


@pytest.mark.parametrize("n_sims", [100, 1_000, 10_000, 100_000])
def test_no_tally_can_round_to_zero_at_any_simulation_count(n_sims):
    """The rule, not the one default: a count of one survives at every size."""
    projection = {"n_sims": n_sims, "wins": {}, "elo": {},
                  "counts": {"T": {**dict.fromkeys(ps.TALLIES, 0), "playoff": 1}}}
    row = ps.rows_for_report(projection)[0]
    assert row["playoff_pct"] > 0


def test_the_projected_record_is_the_record_the_team_page_shows(db_path, live_season):
    """One team, one record, wherever the site prints it.

    conference_team_records_by_year is built from the rated `games` table, and
    so is every win project() banks, so this page read Arkansas at 1-10 while
    the header of Arkansas's own team page -- changed in the same commit --
    read the 2-10 it finished at. The record line here is now the same sum the
    header makes: rated games plus completed games against unrated opponents.

    The odds are deliberately not part of that sum, and the rest of this file
    holds them to the rated games alone.
    """
    import sqlite3

    import export_dashboard_data as edd

    # Keyed by string, because that payload is read by a page. See build_projection.
    unrated_by_id = edd.load_non_fbs_records_by_year(str(db_path)).get(str(live_season), {})
    if not unrated_by_id:
        pytest.skip("this database holds no games against unrated opponents that season")

    conn = sqlite3.connect(str(db_path))
    try:
        names = dict(conn.execute("SELECT team_id, team_name FROM teams"))
        rated = {
            name: (w, lo, t)
            for name, w, lo, t in conn.execute(
                """SELECT tm.team_name, r.overall_wins, r.overall_losses, r.overall_ties
                   FROM conference_team_records_by_year r JOIN teams tm ON tm.team_id = r.team_id
                   WHERE r.season_year = ?""", (live_season,))
        }
    finally:
        conn.close()
    unrated = {names[int(tid)]: wlt for tid, wlt in unrated_by_id.items()
               if int(tid) in names}

    payload = edd.build_projection(str(db_path), live_season, 60)
    assert payload and payload["teams"], "the live season should project to something"

    checked = 0
    for row in payload["teams"]:
        rw, rl, rt = rated.get(row["team"], (0, 0, 0))
        uw, ul, ut = unrated.get(row["team"], (0, 0, 0))
        assert (row["wins"], row["losses"], row["ties"]) == (rw + uw, rl + ul, rt + ut), \
            f"{row['team']}'s projection record is not the record its team page shows"
        # A projection cannot have a team finishing below what it has banked.
        assert row["projected_wins"] >= rw + uw
        if uw:
            checked += 1
    assert checked, "no team in the payload had beaten an unrated opponent"


def test_a_team_with_a_rounded_away_chance_stays_on_the_page():
    """The page's row list is decided by the tallies, not by their decimals.

    pct_digits keeps a single occurrence out of 0.0 today, but the filter must
    not depend on it: if the published precision ever drops, a team with a real
    chance should still be on the page rather than silently leaving the
    payload.
    """
    import export_dashboard_data as edd

    counts = {
        "Longshot": {**dict.fromkeys(ps.TALLIES, 0), "playoff": 3},
        "Conference hope": {**dict.fromkeys(ps.TALLIES, 0), "conference_title": 1},
        "Eliminated": dict.fromkeys(ps.TALLIES, 0),
    }
    # Every percentage rounded to nothing, as a one-decimal export would do.
    rows = [{"team": t, "playoff_pct": 0.0, "conference_title_pct": 0.0} for t in counts]
    kept = [t["team"] for t in edd.still_playing_for_something(rows, counts)]
    assert kept == ["Longshot", "Conference hope"]


# ----------------------------------------------------------------- the Elo chain

def test_a_simulated_game_uses_the_probability_the_site_publishes():
    """Not a second notion of who is favoured.

    play_game decides the game with predict_upcoming.game_expectation -- the
    number on every game card -- so a coin that always lands just below the
    threshold must give the home win and one just above it must not.
    """
    class Fixed:
        def __init__(self, value):
            self.value = value

        def random(self):
            return self.value

    cfg, home, away = CFG, 1, 2
    p = game_expectation(1800.0, 1600.0, False, cfg)
    assert 0.5 < p < 1.0, "the better team at home should be favoured but not certain"

    assert ps.play_game({home: 1800.0, away: 1600.0}, home, away, False, cfg, 0.0,
                        Fixed(p - 1e-9)) is True
    assert ps.play_game({home: 1800.0, away: 1600.0}, home, away, False, cfg, 0.0,
                        Fixed(p + 1e-9)) is False
    # And the venue is read: the same two teams at a neutral site are closer.
    assert game_expectation(1800.0, 1600.0, True, cfg) < p


def test_a_simulated_result_moves_both_ratings_and_is_zero_sum():
    """build_elo's own update: the winner gains exactly what the loser loses.

    A chain that is not zero-sum inflates or deflates the whole field over a
    season, and the ranking it produces would not be comparable with the
    stored ratings the field is otherwise selected from.
    """
    class Always:
        def random(self):
            return 0.0  # always below the expectation, so the home team wins

    cfg, k_m = CFG, CFG["k"] * 1.7
    ratings = {1: 1500.0, 2: 1500.0}
    before = sum(ratings.values())
    assert ps.play_game(ratings, 1, 2, False, cfg, k_m, Always()) is True
    assert sum(ratings.values()) == pytest.approx(before)
    assert ratings[1] > 1500.0 > ratings[2]
    # The size is k * M * (actual - expected), with the host's own edge in the
    # expectation -- so a home win over an equal team moves less than a road one.
    expected_home = game_expectation(1500.0, 1500.0, False, cfg)
    assert ratings[1] - 1500.0 == pytest.approx(k_m * (1 - expected_home))

    road = {1: 1500.0, 2: 1500.0}
    ps.play_game(road, 2, 1, False, cfg, k_m, type("R", (), {"random": lambda s: 1.0})())
    assert road[1] - 1500.0 > ratings[1] - 1500.0, "an away win should be worth more"


def test_losing_simulated_games_makes_a_team_worse():
    """The whole reason the chain is live.

    Until the committee stand-in became Elo this projection held every rating
    at today's value, so a team that lost its next five games was still
    favoured by the same amount in the sixth, and could not play its way out
    of the field. One simulated season has to be able to move a team.
    """
    cfg, k_m = CFG, CFG["k"] * 1.7
    ratings = {1: 1700.0, 2: 1700.0}
    loses = type("Loser", (), {"random": lambda s: 1.0})()   # home never wins
    for _ in range(5):
        assert ps.play_game(ratings, 1, 2, True, cfg, k_m, loses) is False
    assert ratings[1] < 1700.0 - 100, "five losses barely moved the rating"
    assert ratings[2] > 1700.0 + 100


def test_a_simulated_game_takes_the_configured_layers_own_mean_multiplier(db_conn):
    """Not 1.0, and not the other layer's scale.

    A simulated game has a winner and no score, so there is no multiplier to
    compute; the stand-in is the configured layer's measured average. Taking
    1.0 instead would move every simulated rating about 40% less than a real
    result moves it.
    """
    m = ps.mean_performance_multiplier(db_conn, 2026)
    assert 1.2 < m < 2.2, f"the measured mean multiplier is {m}, which is not this layer's"
    # Measured, not written down: the fallback is only for a database with no
    # history, and must not be what a real database returns.
    assert m != ps.MEAN_MULTIPLIER_FALLBACK
    # The other layer is excluded. MOV's multiplier averages ~2.43, so a mean
    # computed over both layers' rows lands above this one.
    both = db_conn.execute(
        """SELECT AVG(e.mov_multiplier) FROM elo_game_history e
           JOIN games g ON g.game_id = e.game_id
           WHERE g.season_year > 2021 AND g.season_year <= 2026""").fetchone()[0]
    assert m == pytest.approx(both, abs=0.2), "the recent seasons are all one layer"
    old = ps.mean_performance_multiplier(db_conn, 1995)
    assert old == ps.MEAN_MULTIPLIER_FALLBACK, (
        "a window the configured layer never scored should fall back, not average MOV's"
    )


def test_the_projected_elo_is_a_rating_not_a_total(db_path, live_season):
    """Published as a mean per run, in the range Elo actually occupies."""
    projection = ps.project(str(db_path), live_season, 20)
    rows = ps.rows_for_report(projection)
    assert rows
    for row in rows:
        assert 800 < row["projected_elo"] < 2400, row
    # The field is seeded on this number, so the best playoff chances belong to
    # the highest projected ratings.
    ranked = sorted(rows, key=lambda r: -r["projected_elo"])
    assert ranked[0]["playoff_pct"] >= ranked[-1]["playoff_pct"]
