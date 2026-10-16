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
                            home_score INTEGER, away_score INTEGER);
        CREATE TABLE elo_game_history (game_id INTEGER, team_id INTEGER,
                                       pregame_elo REAL, opponent_pregame_elo REAL,
                                       postgame_elo REAL);
        CREATE TABLE scheduled_games (game_id INTEGER PRIMARY KEY, season_year INTEGER,
                                      home_team_id INTEGER, away_team_id INTEGER);
        CREATE TABLE team_membership_by_season (team_id INTEGER, season_year INTEGER,
                                                conference_real TEXT);
        INSERT INTO teams VALUES (1, 'Alpha'), (2, 'Bravo'), (3, 'Charlie'), (4, 'Delta');
        INSERT INTO games VALUES
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
        table = ss.build_season(_league(tmp_path), 2026)
        remaining = {t["team"]: t["remaining_sos"] for t in table["teams"]}
        assert remaining["Alpha"] == pytest.approx(1280.0)
        # Delta has Alpha (1540) and Bravo (1420) left: mean 1480.
        assert remaining["Delta"] == pytest.approx(1480.0)

    def test_a_team_with_nothing_left_has_no_remaining_number(self, tmp_path):
        table = ss.build_season(_league(tmp_path), 2026)
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
        table = ss.build_season(conn, 2026)
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
        table = ss.build_season(_league(tmp_path), 2026)
        alpha = next(t for t in table["teams"] if t["team"] == "Alpha")
        assert alpha["full_sos"] == pytest.approx((1800 + 1600 + 1280) / 3)
        assert alpha["full_sos"] != pytest.approx((1700 + 1280) / 2)

    def test_the_table_is_ordered_by_the_full_schedule(self, tmp_path):
        table = ss.build_season(_league(tmp_path), 2026)
        values = [t["full_sos"] for t in table["teams"]]
        assert values == sorted(values, reverse=True)

    def test_a_remaining_game_is_counted_once_per_team_and_the_game_count_is_not_doubled(self, tmp_path):
        """Each scheduled game appears on both teams, so summing per-team
        counts gives team-games. Publishing that as a game count would double
        the season that is left."""
        table = ss.build_season(_league(tmp_path), 2026)
        assert table["remaining_games"] == 2        # two scheduled games
        assert table["remaining_team_games"] == 4   # each on two teams


class TestTheConferenceRollup:
    def test_each_conference_averages_its_own_members(self, tmp_path):
        table = ss.build_season(_league(tmp_path), 2026)
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
        table = ss.build_season(_league(tmp_path), 2026)
        rows = {c["conference"]: c for c in table["conferences"]}
        assert rows["Big"]["played_sos"] > rows["Small"]["played_sos"]
        assert rows["Small"]["full_sos"] > rows["Big"]["full_sos"]
        assert [c["conference"] for c in table["conferences"]] == ["Small", "Big"]


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
