"""The playoff bracket, derived from games the archive already has.

Each test drives `src/nba/series.py` with games shaped the way
`src/nba/export.py` hands them over, and each of the three traps named in
that module's docstring has a test that fails when its rule is removed.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.nba import series  # noqa: E402

EAST = 'Eastern Conference'
WEST = 'Western Conference'


def game(home, away, home_points, away_points, date, *, kind='postseason',
         game_id=None):
    return dict(id=game_id or f'{date}-{home}-{away}', date=date, type=kind,
                home=home, away=away, home_points=home_points,
                away_points=away_points)


def best_of(home, away, results, start=1, month='04'):
    """One series: `results` is True where the first club won that game."""
    rows = []
    for index, home_won in enumerate(results):
        # The clubs alternate hosting, which is what makes the pair key earn
        # its place: a series is one series however the venue moves.
        at_home = index % 2 == 0
        host, guest = (home, away) if at_home else (away, home)
        winner_is_host = home_won if at_home else not home_won
        points = (110, 100) if winner_is_host else (100, 110)
        rows.append(game(host, guest, points[0], points[1],
                         f'2026-{month}-{start + index * 2:02d}'))
    return rows


def board(*rows):
    """Board rows as `export.board` builds them: conference and record."""
    return [dict(team=team, conference=conference, wins=wins, losses=losses)
            for team, conference, wins, losses in rows]


class TestWhatCountsAsASeriesGame:
    def test_a_game_with_no_score_is_not_part_of_a_series(self):
        """TRAP 1. The 2020 bubble left nine rows that were scheduled,
        postponed and never played, beside the game that replaced them."""
        played = game('A', 'B', 110, 100, '2026-04-20')
        phantom = game('A', 'B', None, None, '2026-04-18')
        assert series.played([played, phantom]) == [played]

    def test_a_regular_season_game_is_not_part_of_a_series(self):
        assert series.played(
            [game('A', 'B', 110, 100, '2026-01-02', kind='regular')]) == []

    def test_a_play_in_game_is_not_part_of_a_series(self):
        """The play-in is not a round of the bracket: see `playin`."""
        assert series.played(
            [game('A', 'B', 110, 100, '2026-04-15', kind='playin')]) == []

    def test_a_series_is_one_series_however_the_venue_moves(self):
        rows = best_of('A', 'B', [True, True, False, True])
        grouped = series.group(rows)
        assert len(grouped) == 1
        assert grouped[0][0] == ('A', 'B')
        assert len(grouped[0][1]) == 4

    def test_the_games_of_a_series_come_back_in_date_order(self):
        rows = best_of('A', 'B', [True, False, True, True])
        shuffled = [rows[2], rows[0], rows[3], rows[1]]
        dates = [row['date'] for row in series.group(shuffled)[0][1]]
        assert dates == sorted(dates)


class TestWhoWonTheSeries:
    def test_the_result_is_counted_and_not_assumed_to_be_four_wins(self):
        """TRAP 2. The 2002 first round was best-of-five: eight series were
        won with three wins, and a 3-0 sweep is three games."""
        rows = best_of('A', 'B', [True, True, True])
        won, lost, wins = series.result(('A', 'B'), rows)
        assert (won, lost) == ('A', 'B')
        assert wins == {'A': 3, 'B': 0}

    def test_a_best_of_seven_is_read_the_same_way(self):
        rows = best_of('A', 'B', [True, False, False, True, False, False])
        won, lost, wins = series.result(('A', 'B'), rows)
        assert (won, lost) == ('B', 'A')
        assert wins == {'A': 2, 'B': 4}

    def test_a_level_series_has_no_winner_rather_than_a_broken_tie(self):
        rows = best_of('A', 'B', [True, False])
        won, lost, wins = series.result(('A', 'B'), rows)
        assert won is None and lost is None
        assert wins == {'A': 1, 'B': 1}


class TestWhichRoundASeriesIs:
    """TRAP 3. The feed's own labels are absent for 2002 and 2003, say only
    "PLAYOFFS" for 316 games and spell each round four ways, so the round is
    derived structurally: the k-th series a club plays is round k."""

    def _bracket(self):
        # Four clubs: A beats B and C beats D, then A meets C.
        rows = (best_of('A', 'B', [True, True, True], month='04')
                + best_of('C', 'D', [True, True, True], start=2, month='04')
                + best_of('A', 'C', [True, True, True], month='05'))
        return series.group(rows)

    def test_the_kth_series_a_club_plays_is_round_k(self):
        numbers = series.rounds(self._bracket())
        assert numbers[('A', 'B')] == 1
        assert numbers[('C', 'D')] == 1
        assert numbers[('A', 'C')] == 2

    def test_no_round_label_is_read_from_the_feed(self):
        """A game whose notes claim another round changes nothing, because
        the notes are never consulted."""
        rows = best_of('A', 'B', [True, True, True])
        for row in rows:
            row['notes'] = 'NBA FINALS'
        assert series.rounds(series.group(rows))[('A', 'B')] == 1

    def test_clubs_on_different_rounds_are_reported(self):
        # B plays twice before meeting A, which cannot happen in a bracket.
        rows = (best_of('B', 'C', [True, True, True], month='04')
                + best_of('A', 'B', [True, True, True], month='05'))
        assert series.disagreements(series.group(rows)) == [('A', 'B')]

    def test_a_proper_bracket_reports_no_disagreement(self):
        assert series.disagreements(self._bracket()) == []


class TestTheShapeOfTheBracket:
    def test_a_bracket_of_the_wrong_shape_is_refused_not_drawn(self):
        reasons = series.shape({1: [1, 2], 2: [1], 3: [1], 4: [1]})
        assert reasons == ['round 1 has 2 series, not 8',
                           'round 2 has 1 series, not 4',
                           'round 3 has 1 series, not 2']

    def test_a_round_a_16_team_bracket_does_not_have_is_refused(self):
        reasons = series.shape({1: [0] * 8, 2: [0] * 4, 3: [0] * 2,
                                4: [0], 5: [0]})
        assert reasons == ['there is a round 5, which a 16-team bracket '
                           'does not have']

    def test_the_right_shape_is_not_refused(self):
        assert series.shape({1: [0] * 8, 2: [0] * 4, 3: [0] * 2,
                             4: [0]}) == []


class TestSeeding:
    def test_a_seed_is_the_rank_by_record_inside_the_conference(self):
        rows = board(('A', EAST, 60, 22), ('B', EAST, 50, 32),
                     ('C', WEST, 58, 24), ('D', WEST, 40, 42))
        assert series.seeds(rows) == {'A': 1, 'B': 2, 'C': 1, 'D': 2}

    def test_a_seed_is_not_the_rank_by_rating(self):
        """A club the standings had seventh may be rated second, and the
        bracket is seeded on the standings."""
        rows = board(('A', EAST, 40, 42), ('B', EAST, 60, 22))
        for row in rows:
            row['rating'] = 1800 if row['team'] == 'A' else 1500
        assert series.seeds(rows)['B'] == 1

    def test_clubs_level_on_record_share_a_seed(self):
        """The league's tiebreakers are a cascade this archive cannot
        reproduce, so a tie is left as one and the next seed is skipped."""
        rows = board(('A', EAST, 46, 36), ('B', EAST, 46, 36),
                     ('C', EAST, 45, 37))
        assert series.seeds(rows) == {'A': 1, 'B': 1, 'C': 3}

    def test_a_club_with_no_conference_is_left_unseeded(self):
        rows = board(('A', EAST, 60, 22))
        rows.append(dict(team='Z', conference=None, wins=50, losses=32))
        assert series.seeds(rows) == {'A': 1}

    def test_a_club_that_has_played_fewer_games_is_ranked_on_its_rate(self):
        rows = board(('A', EAST, 30, 10), ('B', EAST, 50, 32))
        assert series.seeds(rows)['A'] == 1

    def test_a_club_with_no_games_is_ranked_last_rather_than_first(self):
        rows = board(('A', EAST, 0, 0), ('B', EAST, 1, 81))
        assert series.seeds(rows) == {'B': 1, 'A': 2}


class TestBuildingOneSeason:
    def _season(self):
        rows = []
        # Eight first-round series, in two conferences of four clubs.
        pairs = [('E1', 'E8'), ('E2', 'E7'), ('E3', 'E6'), ('E4', 'E5'),
                 ('W1', 'W8'), ('W2', 'W7'), ('W3', 'W6'), ('W4', 'W5')]
        for index, (one, two) in enumerate(pairs):
            rows += best_of(one, two, [True] * 4, start=1 + index, month='04')
        for index, (one, two) in enumerate(
                [('E1', 'E4'), ('E2', 'E3'), ('W1', 'W4'), ('W2', 'W3')]):
            rows += best_of(one, two, [True] * 4, start=1 + index, month='05')
        for index, (one, two) in enumerate([('E1', 'E2'), ('W1', 'W2')]):
            rows += best_of(one, two, [True] * 4, start=1 + index, month='06')
        rows += best_of('E1', 'W1', [True] * 4, start=10, month='06')
        return rows

    def test_a_season_comes_out_as_four_rounds_of_the_right_size(self):
        built = series.build(self._season())
        assert [len(rows) for rows in built['rounds']] == [8, 4, 2, 1]
        assert built['ok'] == 1
        assert built['why'] == []
        assert built['names'] == list(series.ROUND_NAMES)

    def test_a_series_carries_its_clubs_its_wins_and_its_winner(self):
        built = series.build(self._season())
        final = built['rounds'][3][0]
        assert final['teams'] == ['E1', 'W1']
        assert final['wins'] == [4, 0]
        assert final['won'] == 'E1'
        assert final['lost'] == 'W1'
        assert len(final['games']) == 4
        assert final['dates'] == ['2026-06-10', '2026-06-16']

    def test_the_seeds_ride_along_where_the_board_has_them(self):
        rows = board(('E1', EAST, 60, 22), ('E2', EAST, 50, 32))
        built = series.build(self._season(), rows)
        wanted = [entry for entry in built['rounds'][2]
                  if entry['teams'] == ['E1', 'E2']]
        assert wanted[0]['seeds'] == [1, 2]

    def test_a_series_whose_clubs_the_board_does_not_seed_carries_none(self):
        built = series.build(self._season(), [])
        assert 'seeds' not in built['rounds'][0][0]

    def test_a_season_with_no_postseason_is_none_rather_than_empty(self):
        """The season in progress. None is what makes a page say there is no
        bracket yet instead of drawing an empty one."""
        assert series.build([game('A', 'B', 110, 100, '2026-01-02',
                                  kind='regular')]) is None
        assert series.build([]) is None

    def test_a_bracket_of_the_wrong_shape_says_so_in_the_payload(self):
        built = series.build(best_of('A', 'B', [True] * 4))
        assert built['ok'] == 0
        assert 'round 1 has 1 series, not 8' in built['why']


class TestThePlayIn:
    def test_the_play_in_is_listed_as_games_and_not_as_a_bracket(self):
        """TWO FORMATS: one game in the 2020 bubble, six a season from 2021.
        So these are games, not a shape."""
        rows = [game('A', 'B', 110, 100, '2026-04-15', kind='playin'),
                game('C', 'D', 99, 101, '2026-04-14', kind='playin'),
                game('E', 'F', 110, 100, '2026-04-20')]
        got = series.playin(rows)
        assert [entry['teams'] for entry in got] == [['C', 'D'], ['A', 'B']]
        assert got[0]['points'] == [99, 101]

    def test_a_single_game_play_in_is_as_valid_as_a_six_game_one(self):
        rows = [game('A', 'B', 126, 122, '2020-08-15', kind='playin')]
        assert len(series.playin(rows)) == 1

    def test_a_season_with_no_play_in_is_none(self):
        assert series.playin(
            [game('A', 'B', 110, 100, '2026-04-20')]) is None


class TestAgainstTheArchive:
    """The one test that reads the real database, so that a rule which holds
    on fixtures and not on 25 seasons of games cannot pass."""

    def test_every_season_the_archive_has_makes_a_16_team_bracket(self):
        import sqlite3
        path = Path(__file__).resolve().parents[1] / 'db' / 'nba.db'
        if not path.exists():
            pytest.skip('no NBA archive in this checkout')
        connection = sqlite3.connect(path)
        connection.row_factory = sqlite3.Row
        from src.nba import export
        model = export.model_row(connection)
        if model is None:
            pytest.skip('no model in this archive')
        index = export.teams(connection)
        checked = 0
        for row in connection.execute(
                'SELECT DISTINCT season FROM game'
                " WHERE season_type = 'postseason' ORDER BY season"):
            season = row['season']
            games = export.season_games(connection, season,
                                        model['model_id'])
            built = series.build(games, export.board(
                connection, season, model['model_id'], games,
                names=export.season_names(connection, season), index=index))
            assert built is not None, season
            assert built['why'] == [], (season, built['why'])
            assert [len(rows) for rows in built['rounds']] == [8, 4, 2, 1], \
                season
            checked += 1
        # A guard on the guard: a database with one postseason in it would
        # otherwise make this test look like it covered the archive.
        assert checked >= 20, f'only {checked} seasons checked'
