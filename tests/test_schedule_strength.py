"""Schedule strength, played and remaining.

The numbers here are averages of other people's ratings, which makes them easy
to compute and easy to get subtly wrong. The three mistakes worth testing for:

  * reading the team's own rating instead of the opponent's, which would turn
    the column into a worse copy of the Elo table;
  * counting an unrated opponent at the engine's 1500 prior, which would make
    a schedule full of them look average when it is the opposite;
  * disagreeing with the team page, which already publishes the played number.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "coefficients"))

import build_schedule_strength as ss  # noqa: E402


# The ratings a live chain would hold for the fixture's four teams. Passed in
# explicitly by the arithmetic tests below so each average is a round number
# that can be asserted as a literal: a test comparing against whatever the Elo
# engine happened to produce would pass whatever the maths did. Which rating
# source is the right one is a separate question, tested on its own in
# TestWhichRatingCountsAsCurrent.
ELO_TODAY = {1: 1540.0, 2: 1420.0, 3: 1580.0, 4: 1280.0}


def _league(tmp_path, scheduled=True):
    """Four rated teams, one season played in part, with games still to come.

    Ratings are chosen so every average is a round number and a swapped column
    is obvious: team 1 plays only strong opponents, team 4 only weak ones.
    """
    conn = sqlite3.connect(str(tmp_path / "league.db"))
    conn.executescript("""
        CREATE TABLE teams (team_id INTEGER PRIMARY KEY, team_name TEXT);
        CREATE TABLE games (game_id INTEGER PRIMARY KEY, season_year INTEGER,
                            game_date TEXT, game_phase TEXT,
                            home_team_id INTEGER, away_team_id INTEGER,
                            home_score INTEGER, away_score INTEGER,
                            neutral_site INTEGER DEFAULT 0, went_ot INTEGER DEFAULT 0);
        CREATE TABLE elo_game_history (game_id INTEGER, team_id INTEGER,
                                       pregame_elo REAL, opponent_pregame_elo REAL,
                                       postgame_elo REAL);
        CREATE TABLE scheduled_games (game_id INTEGER PRIMARY KEY, season_year INTEGER,
                                      home_team_id INTEGER, away_team_id INTEGER);
        CREATE TABLE team_membership_by_season (team_id INTEGER, season_year INTEGER,
                                                conference_real TEXT);
        INSERT INTO teams VALUES (1, 'Alpha'), (2, 'Bravo'), (3, 'Charlie'), (4, 'Delta');
        INSERT INTO games (game_id, season_year, game_date, game_phase,
                           home_team_id, away_team_id, home_score, away_score) VALUES
            (10, 2026, '2026-09-05', 'regular', 1, 2, 30, 10),
            (11, 2026, '2026-09-12', 'regular', 1, 3, 20, 17),
            (12, 2026, '2026-09-19', 'regular', 4, 2, 7, 35);
        -- Opponent pregame Elo is the column that matters. Alpha faced 1800 and
        -- 1600 (mean 1700); Delta faced 1400 (mean 1400).
        INSERT INTO elo_game_history VALUES
            (10, 1, 1500, 1800, 1520), (10, 2, 1800, 1500, 1780),
            (11, 1, 1520, 1600, 1540), (11, 3, 1600, 1520, 1580),
            (12, 4, 1300, 1400, 1280), (12, 2, 1400, 1300, 1420);
        INSERT INTO team_membership_by_season VALUES
            (1, 2026, 'Big'), (2, 2026, 'Big'), (3, 2026, 'Small'), (4, 2026, 'Small');
    """)
    if scheduled:
        # Alpha's remaining game is against Delta (weak); Delta's two remaining
        # games are against Alpha and Bravo. Current ratings come off the last
        # regular-season game: Alpha 1540, Bravo 1420, Charlie 1580, Delta 1280.
        conn.executescript("""
            INSERT INTO scheduled_games VALUES (20, 2026, 1, 4), (21, 2026, 4, 2);
        """)
    conn.commit()
    return conn


class TestThePlayedNumber:
    def test_it_averages_the_opponents_rating_not_the_teams_own(self, tmp_path):
        """Reading pregame_elo instead of opponent_pregame_elo would make this
        column a worse copy of the Elo table. Alpha's own ratings were 1500 and
        1520; its opponents' were 1800 and 1600."""
        table = ss.build_season(_league(tmp_path), 2026)
        played = {t["team"]: t["played_sos"] for t in table["teams"]}
        assert played["Alpha"] == pytest.approx(1700.0)
        assert played["Delta"] == pytest.approx(1400.0)

    def test_the_toughest_schedule_ranks_first(self, tmp_path):
        table = ss.build_season(_league(tmp_path), 2026)
        ranks = {t["team"]: t["played_rank"] for t in table["teams"]}
        # Alpha 1700, Charlie 1520, Bravo 1400, Delta 1400.
        assert ranks["Alpha"] == 1
        assert ranks["Charlie"] == 2

    def test_a_tie_shares_the_better_rank_rather_than_inventing_an_order(self, tmp_path):
        """Bravo and Delta both faced a 1400 average. Giving one of them rank 3
        and the other 4 would report a difference the numbers do not hold."""
        table = ss.build_season(_league(tmp_path), 2026)
        ranks = {t["team"]: t["played_rank"] for t in table["teams"]}
        assert ranks["Bravo"] == ranks["Delta"] == 3

    def test_it_agrees_with_the_number_the_team_page_already_publishes(self, tmp_path):
        """export_team_pages averages the same column. Two pages disagreeing
        about one team's schedule would be worse than neither having it."""
        conn = _league(tmp_path)
        table = ss.build_season(conn, 2026)
        independently = {}
        for team_id, opponent in conn.execute(
                "SELECT e.team_id, e.opponent_pregame_elo FROM elo_game_history e "
                "JOIN games g ON g.game_id = e.game_id WHERE g.season_year = 2026"):
            independently.setdefault(team_id, []).append(opponent)
        for row in table["teams"]:
            faced = independently[row["team_id"]]
            assert row["played_sos"] == pytest.approx(sum(faced) / len(faced))
            assert row["played_games"] == len(faced)


class TestTheRemainingNumber:
    def test_it_uses_the_opponents_rating_today(self, tmp_path):
        """There is no pregame rating for a game nobody has played, so the
        current one is the only honest estimate. Alpha has Delta (1280) left."""
        table = ss.build_season(_league(tmp_path), 2026, elo=ELO_TODAY)
        remaining = {t["team"]: t["remaining_sos"] for t in table["teams"]}
        assert remaining["Alpha"] == pytest.approx(1280.0)
        # Delta has Alpha (1540) and Bravo (1420) left: mean 1480.
        assert remaining["Delta"] == pytest.approx(1480.0)

    def test_a_team_with_nothing_left_has_no_remaining_number(self, tmp_path):
        table = ss.build_season(_league(tmp_path), 2026, elo=ELO_TODAY)
        charlie = next(t for t in table["teams"] if t["team"] == "Charlie")
        assert charlie["remaining_games"] == 0
        assert charlie["remaining_sos"] is None
        assert charlie["remaining_rank"] is None

    def test_an_unrated_opponent_is_excluded_and_counted_not_averaged_at_1500(self, tmp_path):
        """Counting a team this project does not rate at the engine's 1500
        prior would make a schedule full of them look average. Here Alpha's
        extra opponent (id 99) has no rating at all."""
        conn = _league(tmp_path)
        conn.execute("INSERT INTO scheduled_games VALUES (22, 2026, 1, 99)")
        conn.commit()
        table = ss.build_season(conn, 2026, elo=ELO_TODAY)
        alpha = next(t for t in table["teams"] if t["team"] == "Alpha")
        # Still only Delta's 1280, NOT the mean of 1280 and 1500.
        assert alpha["remaining_sos"] == pytest.approx(1280.0)
        assert alpha["remaining_games"] == 1
        assert alpha["remaining_unrated"] == 1

    def test_a_finished_season_has_no_remaining_half_at_all(self, tmp_path):
        table = ss.build_season(_league(tmp_path, scheduled=False), 2026)
        assert table["remaining_games"] == 0
        assert table["remaining_team_games"] == 0
        for row in table["teams"]:
            assert row["remaining_sos"] is None
            # And the full schedule is then exactly the played one.
            assert row["full_sos"] == pytest.approx(row["played_sos"])


class TestTheFullNumber:
    def test_it_pools_the_games_rather_than_averaging_the_two_averages(self, tmp_path):
        """Alpha has played two games (1800, 1600) and has one left (1280), so
        the full average is 1560. Averaging 1700 and 1280 would give 1490 and
        would weight one remaining game as heavily as two played ones."""
        table = ss.build_season(_league(tmp_path), 2026, elo=ELO_TODAY)
        alpha = next(t for t in table["teams"] if t["team"] == "Alpha")
        assert alpha["full_sos"] == pytest.approx((1800 + 1600 + 1280) / 3)
        assert alpha["full_sos"] != pytest.approx((1700 + 1280) / 2)

    def test_the_table_is_ordered_by_the_full_schedule(self, tmp_path):
        table = ss.build_season(_league(tmp_path), 2026, elo=ELO_TODAY)
        values = [t["full_sos"] for t in table["teams"]]
        assert values == sorted(values, reverse=True)

    def test_a_remaining_game_is_counted_once_per_team_and_the_game_count_is_not_doubled(self, tmp_path):
        """Each scheduled game appears on both teams, so summing per-team
        counts gives team-games. Publishing that as a game count would double
        the season that is left."""
        table = ss.build_season(_league(tmp_path), 2026, elo=ELO_TODAY)
        assert table["remaining_games"] == 2        # two scheduled games
        assert table["remaining_team_games"] == 4   # each on two teams


def _with_fcs_fixtures(conn, rows):
    """Add scheduled_non_fbs_games, built from the real schema so the test
    tracks it, holding (game_id, season, team_id) rows."""
    conn.executescript(
        (Path(__file__).resolve().parents[1] / "sql/non_fbs_game_tables.sql").read_text())
    conn.executemany(
        """INSERT INTO scheduled_non_fbs_games
               (game_id, season_year, team_id, opponent_name, is_home, game_phase)
           VALUES (?, ?, ?, 'Mercer', 1, 'regular')""", rows)
    conn.commit()
    return conn


class TestTheNonFbsGamesStillToCome:
    """A game left against an FCS opponent is a game left.

    It is not in scheduled_games -- that table feeds predict_upcoming.py, which
    has no rating to price it with -- so it comes from its own table. It must
    be counted and must not be averaged: 1500 is the engine's prior for a team
    it has never seen, not an estimate of an FCS program.
    """

    def test_it_counts_towards_the_games_a_team_has_left(self, tmp_path):
        conn = _with_fcs_fixtures(_league(tmp_path), [(30, 2026, 3)])
        table = ss.build_season(conn, 2026, elo=ELO_TODAY)
        charlie = next(t for t in table["teams"] if t["team"] == "Charlie")
        # Charlie had nothing left in scheduled_games and now has one unrated
        # game. Reporting nothing left would be the bug this table fixes.
        assert charlie["remaining_unrated"] == 1

    def test_it_does_not_move_the_remaining_average(self, tmp_path):
        conn = _with_fcs_fixtures(_league(tmp_path), [(30, 2026, 1)])
        table = ss.build_season(conn, 2026, elo=ELO_TODAY)
        alpha = next(t for t in table["teams"] if t["team"] == "Alpha")
        assert alpha["remaining_sos"] == pytest.approx(1280.0)   # Delta alone
        assert alpha["remaining_games"] == 1                     # rated games only
        assert alpha["remaining_unrated"] == 1

    def test_a_team_with_only_unrated_games_left_has_no_remaining_number(self, tmp_path):
        conn = _with_fcs_fixtures(_league(tmp_path), [(30, 2026, 3)])
        table = ss.build_season(conn, 2026, elo=ELO_TODAY)
        charlie = next(t for t in table["teams"] if t["team"] == "Charlie")
        assert charlie["remaining_sos"] is None
        assert charlie["remaining_rank"] is None

    def test_it_is_added_to_the_seasons_game_count_once_each(self, tmp_path):
        """One row per game in each table, and a non-FBS fixture sits on
        exactly one rated team, so the two counts add without halving."""
        conn = _with_fcs_fixtures(_league(tmp_path), [(30, 2026, 1), (31, 2026, 3)])
        table = ss.build_season(conn, 2026, elo=ELO_TODAY)
        assert table["remaining_games"] == 4            # 2 scheduled + 2 non-FBS
        assert table["remaining_team_games"] == 6       # 2 x 2 + 2 x 1
        assert table["remaining_unrated"] == 2

    def test_another_season_is_not_counted_into_this_one(self, tmp_path):
        conn = _with_fcs_fixtures(_league(tmp_path), [(30, 2026, 1), (31, 2025, 1)])
        table = ss.build_season(conn, 2026, elo=ELO_TODAY)
        assert table["remaining_unrated"] == 1
        assert table["remaining_games"] == 3

    def test_an_unrated_fbs_opponent_and_an_fcs_one_are_one_counter(self, tmp_path):
        """They are the same fact to a reader -- a game left this page cannot
        price -- so a team with one of each has two, not one of each kind."""
        conn = _league(tmp_path)
        conn.execute("INSERT INTO scheduled_games VALUES (22, 2026, 1, 99)")
        conn.commit()
        _with_fcs_fixtures(conn, [(30, 2026, 1)])
        table = ss.build_season(conn, 2026, elo=ELO_TODAY)
        alpha = next(t for t in table["teams"] if t["team"] == "Alpha")
        assert alpha["remaining_unrated"] == 2

    def test_a_database_without_the_table_measures_everything_else(self, tmp_path):
        """An old checkout, or one that has not run the loader. The season
        still has its rated games left; it simply does not know about these."""
        table = ss.build_season(_league(tmp_path), 2026, elo=ELO_TODAY)
        assert table["remaining_games"] == 2
        assert table["remaining_unrated"] == 0

    def test_a_season_whose_only_games_left_are_unrated_still_reports_them(self, tmp_path):
        """scheduled_games is empty, so the Elo chain is never replayed -- and
        the count must survive that shortcut."""
        conn = _with_fcs_fixtures(_league(tmp_path, scheduled=False), [(30, 2026, 1)])
        table = ss.build_season(conn, 2026)
        assert table["remaining_games"] == 1
        assert table["remaining_unrated"] == 1
        assert next(t for t in table["teams"]
                    if t["team"] == "Alpha")["remaining_unrated"] == 1


class TestTheConferenceRollup:
    def test_each_conference_averages_its_own_members(self, tmp_path):
        table = ss.build_season(_league(tmp_path), 2026, elo=ELO_TODAY)
        rows = {c["conference"]: c for c in table["conferences"]}
        # Big is Alpha (1700) and Bravo (1400).
        assert rows["Big"]["played_sos"] == pytest.approx(1550.0)
        assert rows["Big"]["teams"] == 2
        # Small is Charlie (1520) and Delta (1400).
        assert rows["Small"]["played_sos"] == pytest.approx(1460.0)

    def test_conferences_are_ordered_by_the_full_schedule_not_the_played_half(self, tmp_path):
        """Big has played the tougher schedule (1550 against 1460) and Small
        has the tougher one left, which puts Small ahead on the full figure
        (1487 against 1460). Ordering by the played column would reverse them,
        and the column the table is sorted by is the one it is headed with."""
        table = ss.build_season(_league(tmp_path), 2026, elo=ELO_TODAY)
        rows = {c["conference"]: c for c in table["conferences"]}
        assert rows["Big"]["played_sos"] > rows["Small"]["played_sos"]
        assert rows["Small"]["full_sos"] > rows["Big"]["full_sos"]
        assert [c["conference"] for c in table["conferences"]] == ["Small", "Big"]


class TestWhichRatingCountsAsCurrent:
    """The remaining column promises the opponents' ratings TODAY.

    The playoff selector's load_team_elo() reads the rating a team took into
    the postseason, which is a different thing in two cases that both reach
    this page: a team that has not played yet this season has no such rating
    at all, and a team playing in the postseason is frozen at its November
    one. Both would be wrong here.
    """

    def test_a_team_yet_to_play_this_season_is_still_a_rated_opponent(self, tmp_path):
        """In week zero nobody has a rating "entering the postseason", so the
        whole remaining column would be empty -- a preseason table of dashes.
        The live chain carries last season's rating forward instead."""
        conn = _league(tmp_path)
        # Move every game into 2025 and leave 2026 entirely unplayed.
        conn.execute("UPDATE games SET season_year = 2025, game_date = "
                     "REPLACE(game_date, '2026', '2025')")
        conn.execute("DELETE FROM team_membership_by_season")
        conn.commit()
        elo = ss.live_ratings(conn)
        assert len(elo) == 4, "every team that has ever played keeps a rating"
        table = ss.build_season(conn, 2026)
        alpha = next(t for t in table["teams"] if t["team"] == "Alpha")
        assert alpha["played_games"] == 0
        assert alpha["played_sos"] is None
        # The point: a remaining number EXISTS before the season starts.
        assert alpha["remaining_sos"] is not None
        assert alpha["remaining_unrated"] == 0

    def test_a_postseason_result_moves_the_rating_the_remaining_column_uses(self, tmp_path):
        """A team in a bowl is priced at what it is worth today, not at what it
        was worth in November. load_team_elo() would ignore this game."""
        conn = _league(tmp_path)
        before = ss.live_ratings(conn)[2]
        conn.execute("INSERT INTO games (game_id, season_year, game_date, game_phase, "
                     "home_team_id, away_team_id, home_score, away_score) "
                     "VALUES (13, 2026, '2026-12-28', 'postseason', 2, 3, 3, 45)")
        conn.commit()
        after = ss.live_ratings(conn)[2]
        assert after < before, "Bravo lost a bowl by 42 and must be rated lower for it"

    def test_a_finished_season_needs_no_rating_replay_at_all(self, tmp_path, monkeypatch):
        """Replaying the Elo chain costs real time, and a season with nothing
        left to play never reads it. 47 seasons would otherwise mean 47
        replays on every export."""
        conn = _league(tmp_path, scheduled=False)
        called = []
        monkeypatch.setattr(ss, "live_ratings", lambda c: called.append(1) or {})
        ss.build_season(conn, 2026)
        assert called == []


class TestTheReport:
    def test_every_season_with_ratings_gets_a_table(self, tmp_path):
        report = ss.build_report(_league(tmp_path))
        assert set(report["seasons"]) == {"2026"}
        assert len(report["seasons"]["2026"]["teams"]) == 4

    def test_a_database_with_no_ratings_publishes_nothing_rather_than_an_empty_table(self, tmp_path):
        conn = sqlite3.connect(str(tmp_path / "empty.db"))
        conn.executescript("CREATE TABLE teams (team_id INTEGER, team_name TEXT);")
        assert ss.build_report(conn) is None

    def test_a_database_with_no_schedule_table_still_measures_what_was_played(self, tmp_path):
        """A checkout that has not loaded the schedule is the common case for an
        old season, and it must not take the played half down with it."""
        conn = _league(tmp_path)
        conn.execute("DROP TABLE scheduled_games")
        conn.commit()
        table = ss.build_season(conn, 2026)
        assert next(t for t in table["teams"] if t["team"] == "Alpha")["played_sos"] == 1700.0
        assert table["remaining_games"] == 0
