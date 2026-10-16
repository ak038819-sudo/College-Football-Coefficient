"""
The ratings against the closing line.

Every number this produces is a claim about whether the model beats the market,
so the tests are built around the ways such a claim goes wrong quietly: a
flipped side on a spread pick, a push counted as a win, agreement left in the
straight-up comparison so that the difference drowns, and a market probability
read through the wrong curve. Each one would leave the page looking sensible
and saying the opposite of the truth.
"""
from __future__ import annotations

import math
import sqlite3

import pytest

import measure_vs_market as mm
from win_probability import WinProbabilityParams

PARAMS = WinProbabilityParams(points_per_elo=0.05, sigma=16.0, time_exponent=0.45,
                              overtime_home_win=0.5, tie_half_width=1.0)


def row(actual, market, model, *, season=2024, p_home=0.6, game_id=1):
    """One game: what happened, what the line was, what the model expected."""
    return {"game_id": game_id, "season": season, "actual_margin": actual,
            "provider": "consensus", "market_margin": market, "model_margin": model,
            "model_p_home": p_home, "market_p_home": None,
            "home_won": 1.0 if actual > 0 else (0.0 if actual < 0 else 0.5)}


class TestTheProviderChoice:
    def test_consensus_wins_whatever_order_the_database_returns(self):
        assert mm.best_line([("Bovada", 3.0), ("consensus", 3.5)]) == ("consensus", 3.5)
        assert mm.best_line([("consensus", 3.5), ("Bovada", 3.0)]) == ("consensus", 3.5)

    def test_an_unknown_provider_is_usable_but_sorts_last_and_deterministically(self):
        assert mm.best_line([("zzz Books", 1.0), ("aaa Books", 2.0)]) == ("aaa Books", 2.0)
        assert mm.best_line([("zzz Books", 1.0), ("Bovada", 2.0)]) == ("Bovada", 2.0)

    def test_no_lines_means_no_choice(self):
        assert mm.best_line([]) is None


class TestAgainstTheSpread:
    def test_the_model_backs_the_home_team_when_it_expects_more_than_the_line(self):
        # Line home by 7, model expects home by 10, home won by 14: covered.
        assert mm.against_the_spread([row(14, 7.0, 10.0)])["wins"] == 1
        # Same pick, home won by only 3: did not cover.
        assert mm.against_the_spread([row(3, 7.0, 10.0)])["losses"] == 1

    def test_the_model_backs_the_away_team_when_it_expects_less(self):
        # Line home by 7, model expects home by 3, home won by 3: away covered,
        # and the model was on the away side.
        assert mm.against_the_spread([row(3, 7.0, 3.0)])["wins"] == 1
        # A flipped side would call this one a win too, which is why both
        # directions are pinned.
        assert mm.against_the_spread([row(14, 7.0, 3.0)])["losses"] == 1

    def test_the_sides_work_with_the_away_team_favoured(self):
        """A negative line is where a sign error hides: the model expecting
        -3 against a line of -10 is a pick ON the home team."""
        assert mm.against_the_spread([row(-5, -10.0, -3.0)])["wins"] == 1
        assert mm.against_the_spread([row(-14, -10.0, -3.0)])["losses"] == 1

    def test_a_push_is_excluded_rather_than_scored(self):
        result = mm.against_the_spread([row(7, 7.0, 10.0), row(14, 7.0, 10.0)])
        assert (result["pushes"], result["wins"], result["losses"]) == (1, 1, 0)
        assert result["decided"] == 1
        assert result["rate"] == 1.0

    def test_the_rate_is_none_rather_than_zero_when_nothing_was_decided(self):
        result = mm.against_the_spread([row(7, 7.0, 10.0)])
        assert result["rate"] is None

    def test_the_break_even_with_vig_is_published_beside_the_rate(self):
        """50% is the break-even of this measurement, not of a wager."""
        result = mm.against_the_spread([row(14, 7.0, 10.0)])
        assert result["break_even_with_vig"] == pytest.approx(110 / 210)
        assert result["break_even_with_vig"] > 0.5


class TestStraightUpDisagreements:
    def test_only_the_games_where_the_favourites_differ_are_counted(self):
        rows = [
            row(10, 7.0, 10.0),      # both favour home: excluded
            row(-10, -7.0, -2.0),    # both favour away: excluded
            row(10, -3.0, 4.0),      # they differ, home won: the model was right
            row(-10, 3.0, -4.0),     # they differ, away won: the model was right
            row(-10, -3.0, 4.0),     # they differ, away won: the market was right
        ]
        result = mm.straight_up_disagreements(rows)
        assert result["games"] == 3
        assert (result["model_right"], result["market_right"]) == (2, 1)
        assert result["model_rate"] == pytest.approx(2 / 3)

    def test_a_pickem_is_not_a_market_favourite(self):
        assert mm.straight_up_disagreements([row(10, 0.0, -4.0)])["games"] == 0

    def test_a_tie_makes_neither_of_them_right(self):
        assert mm.straight_up_disagreements([row(0, -3.0, 4.0)])["games"] == 0

    def test_no_disagreements_reports_none_rather_than_zero(self):
        assert mm.straight_up_disagreements([row(10, 7.0, 10.0)])["model_rate"] is None


class TestTheProbabilityConversion:
    def test_a_bigger_spread_is_a_bigger_chance(self):
        rising = [mm.spread_to_probability(m, PARAMS, 14.0) for m in (-14, -7, 0, 7, 14)]
        assert rising == sorted(rising)
        assert rising[2] == pytest.approx(0.5, abs=0.01)   # a pick'em is a coin flip

    def test_a_narrower_curve_makes_the_same_spread_more_certain(self):
        """This is why the market's own error spread has to be measured: read
        through the model's wider curve, the market's numbers would be scored
        on probabilities it never implied."""
        assert (mm.spread_to_probability(7.0, PARAMS, 13.0)
                > mm.spread_to_probability(7.0, PARAMS, 20.0))

    def test_the_width_is_measured_from_the_markets_own_errors(self):
        rows = [row(10, 7.0, 3.0), row(0, 3.0, 1.0), row(-7, -4.0, -1.0)]
        sigma = mm.attach_market_probabilities(rows, PARAMS)
        errors = [10 - 7.0, 0 - 3.0, -7 - -4.0]
        assert sigma == pytest.approx(math.sqrt(sum(e * e for e in errors) / 3))
        assert all(r["market_p_home"] is not None for r in rows)
        # And the probabilities really were read through THAT width, not the
        # model's: scoring the market on probabilities it never implied would
        # flatter or punish it for nothing.
        assert rows[0]["market_p_home"] == pytest.approx(
            mm.spread_to_probability(rows[0]["market_margin"], PARAMS, sigma))
        assert rows[0]["market_p_home"] != pytest.approx(
            mm.spread_to_probability(rows[0]["market_margin"], PARAMS))

    def test_a_zero_width_is_refused_rather_than_dividing_by_it(self):
        with pytest.raises(ValueError):
            mm.spread_to_probability(7.0, PARAMS, 0.0)

    def test_a_market_that_never_missed_falls_back_to_the_model_width(self):
        """Three games is a small sample; a perfect one would otherwise make
        every market probability 0 or 1 and its log loss infinite."""
        rows = [row(7, 7.0, 3.0), row(-3, -3.0, 1.0)]
        assert mm.attach_market_probabilities(rows, PARAMS) == PARAMS.sigma


class TestTheScores:
    def test_margin_errors_score_both_sides_on_the_same_games(self):
        out = mm.margin_errors([row(10, 7.0, 3.0), row(-4, -1.0, -6.0)])
        assert out["market_mae"] == pytest.approx((3 + 3) / 2)
        assert out["model_mae"] == pytest.approx((7 + 2) / 2)
        assert out["model_rmse"] >= out["model_mae"]

    def test_the_bias_says_which_way_a_forecast_leans(self):
        """Both of these underestimated the home team, so both biases are
        positive; a sign error here would read as the opposite lean."""
        out = mm.margin_errors([row(10, 3.0, 1.0), row(20, 10.0, 6.0)])
        assert out["market_bias"] > 0 and out["model_bias"] > 0

    def test_probability_scores_wait_for_the_market_probability(self):
        rows = [row(10, 7.0, 3.0)]
        assert mm.probability_scores(rows) == {}
        mm.attach_market_probabilities(rows, PARAMS)
        scored = mm.probability_scores(rows)
        assert scored["scored"] == 1
        assert 0 <= scored["market_brier"] <= 1

    def test_a_confident_wrong_probability_scores_worse_than_a_hedged_one(self):
        sure = [row(-10, 0.0, 0.0, p_home=0.99)]
        hedged = [row(-10, 0.0, 0.0, p_home=0.55)]
        for rows in (sure, hedged):
            mm.attach_market_probabilities(rows, PARAMS)
        assert (mm.probability_scores(sure)["model_brier"]
                > mm.probability_scores(hedged)["model_brier"])


def _db(tmp_path, lines=True):
    """A four-game season with ratings and, optionally, a line archive."""
    conn = sqlite3.connect(str(tmp_path / "league.db"))
    conn.executescript("""
        CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER,
                            home_team_id INTEGER, away_team_id INTEGER,
                            home_score INTEGER, away_score INTEGER);
        CREATE TABLE elo_game_history (game_id INTEGER, team_id INTEGER,
                                       elo_diff_adjusted REAL, elo_expectation REAL);
        INSERT INTO games VALUES (1, 2024, 10, 20, 31, 17),
                                 (2, 2024, 20, 10, 10, 24),
                                 (3, 2024, 10, 30, 21, 20),
                                 (4, 2024, 30, 10, 14, 14);
        INSERT INTO elo_game_history VALUES
            (1, 10, 200.0, 0.75), (1, 20, -200.0, 0.25),
            (2, 20, -150.0, 0.30), (2, 10, 150.0, 0.70),
            (3, 10, 60.0, 0.58), (3, 30, -60.0, 0.42),
            (4, 30, -20.0, 0.47), (4, 10, 20.0, 0.53);
    """)
    if lines:
        conn.executescript("""
            CREATE TABLE game_lines (game_id INTEGER, provider TEXT, season_year INTEGER,
                                     market_home_margin REAL,
                                     snapshot_home_score INTEGER,
                                     snapshot_away_score INTEGER);
            INSERT INTO game_lines VALUES
                (1, 'consensus', 2024, 10.5, 31, 17), (1, 'Bovada', 2024, 11.0, 31, 17),
                (2, 'consensus', 2024, -7.0, 10, 24),
                (3, 'consensus', 2024, NULL, 21, 20),
                (4, 'Bovada', 2024, -2.0, 14, 14);
        """)
    conn.commit()
    return conn


class TestTheReport:
    def test_one_row_per_game_on_the_preferred_provider(self, tmp_path):
        rows = mm.load_market_games(_db(tmp_path), PARAMS)
        assert [r["game_id"] for r in rows] == [1, 2, 4]   # game 3 has no priced line
        assert {r["game_id"]: r["provider"] for r in rows} == {
            1: "consensus", 2: "consensus", 4: "Bovada"}
        assert {r["game_id"]: r["market_margin"] for r in rows}[1] == 10.5

    def test_the_model_margin_is_the_elo_gap_in_points(self, tmp_path):
        rows = mm.load_market_games(_db(tmp_path), PARAMS)
        assert rows[0]["model_margin"] == pytest.approx(200.0 * PARAMS.points_per_elo)

    def test_the_home_teams_own_rating_row_is_the_one_read(self, tmp_path):
        """Both teams have a row per game with opposite gaps; reading the away
        team's would invert every model pick."""
        rows = {r["game_id"]: r["model_margin"] for r in mm.load_market_games(_db(tmp_path), PARAMS)}
        # Game 1's home team is rated +200, game 2's is -150 and game 4's is
        # -20. Reading the away team's row would flip all three.
        assert rows[1] > 0
        assert rows[2] < 0 and rows[4] < 0

    def test_a_line_captured_before_the_game_ended_is_not_a_closing_line(self, tmp_path):
        """CFBD serves the CURRENT number before kickoff and the closing one
        after, in the same column. A mid-season file therefore carries weeks-
        early spreads for games still to come, and scoring one of those as a
        close the moment the game is played would quietly corrupt the whole
        comparison. The snapshot's own record of the result is what tells them
        apart: it cannot know a score the game had not produced yet."""
        conn = _db(tmp_path)
        # Game 2's only line was captured before the game ended.
        conn.execute("UPDATE game_lines SET snapshot_home_score = NULL, "
                     "snapshot_away_score = NULL WHERE game_id = 2")
        conn.commit()
        assert [r["game_id"] for r in mm.load_market_games(conn, PARAMS)] == [1, 4]

    def test_one_pre_kickoff_provider_does_not_cost_the_game_its_closing_line(self, tmp_path):
        """Game 1 has two providers. If one of them was snapshotted early the
        game still has a closing line from the other, and the early number must
        not be the one chosen even though its provider ranks first."""
        conn = _db(tmp_path)
        conn.execute("UPDATE game_lines SET snapshot_away_score = NULL "
                     "WHERE game_id = 1 AND provider = 'consensus'")
        conn.commit()
        rows = {r["game_id"]: r for r in mm.load_market_games(conn, PARAMS)}
        assert rows[1]["provider"] == "Bovada"
        assert rows[1]["market_margin"] == 11.0

    def test_a_checkout_with_no_line_archive_reports_nothing(self, tmp_path):
        conn = _db(tmp_path, lines=False)
        assert mm.load_market_games(conn, PARAMS) == []
        assert mm.build_report(conn, PARAMS) is None

    def test_the_report_carries_coverage_beside_every_figure(self, tmp_path):
        report = mm.build_report(_db(tmp_path), PARAMS)
        assert report["overall"]["games"] == 3
        coverage = {c["season"]: c for c in report["coverage"]}
        assert coverage[2024]["games"] == 4
        assert coverage[2024]["priced"] == 3
        assert coverage[2024]["share"] == pytest.approx(0.75)

    def test_the_report_says_which_width_it_read_the_market_through(self, tmp_path):
        report = mm.build_report(_db(tmp_path), PARAMS)
        assert report["market_sigma"] > 0
        assert report["points_per_elo"] == PARAMS.points_per_elo

    def test_every_season_is_summarised_as_well_as_the_whole(self, tmp_path):
        report = mm.build_report(_db(tmp_path), PARAMS)
        assert [s["season"] for s in report["by_season"]] == [2024]
        assert report["by_season"][0]["games"] == 3
        assert report["by_season"][0]["ats"]["decided"] <= 3

    def test_the_providers_actually_used_are_listed(self, tmp_path):
        report = mm.build_report(_db(tmp_path), PARAMS)
        assert {p["provider"]: p["games"] for p in report["providers"]} == {
            "consensus": 2, "Bovada": 1}
