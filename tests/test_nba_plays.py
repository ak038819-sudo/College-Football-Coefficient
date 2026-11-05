"""The NBA play-by-play reduction: what is kept, and what is refused.

The fixture is a hand-built game whose every quantity can be counted by eye,
because the point of these tests is that the reduction's arithmetic is the
arithmetic the schema's comments claim. A fixture read off the real feed would
only prove that the code agrees with itself.
"""

import pytest

from src.nba import plays, store
from src.nba.identity import game_key, team_key

HOME_ID, AWAY_ID = 1, 2
HOME, AWAY = team_key(HOME_ID), team_key(AWAY_ID)
PROVIDER = '401999001'
KEY = game_key(PROVIDER)


def row(number, period, clock, home, away, *, points=0, scoring=False,
        team=None, kind=None, text=None, scorer=None, scorer_id=None):
    return dict(game_id=PROVIDER, game_play_number=number,
                sequence_number=number, period_number=period,
                clock_display_value=clock, home_score=home, away_score=away,
                scoring_play=scoring, score_value=points,
                team_id=team, type_text=kind, text=text,
                athlete_name_1=scorer, athlete_id_1=scorer_id,
                home_team_id=HOME_ID, away_team_id=AWAY_ID)


def basket(number, period, clock, home, away, *, team, points=2, **kwargs):
    return row(number, period, clock, home, away, points=points,
               scoring=True, team=team, kind='Jump Shot',
               text='A jump shot', scorer='A. Player', scorer_id=77,
               **kwargs)


@pytest.fixture
def game_rows():
    """A four-period game: the away side leads, the home side takes it late.

    By eye: two lead changes (away ahead at 2-0, home ahead at 3-2, away
    ahead at 3-4, home ahead at 6-4 — three changes of leader after the
    first lead), one tie, a biggest home run of 3 and a biggest away run of
    2, and a 6-4 final.
    """
    return [
        row(1, 1, '12:00', 0, 0),
        basket(2, 1, '11:30', 0, 2, team=AWAY_ID),
        basket(3, 1, '11:00', 2, 2, team=HOME_ID),
        basket(4, 2, '6:00', 3, 2, team=HOME_ID, points=1),
        basket(5, 3, '5:00', 3, 4, team=AWAY_ID, points=2),
        basket(6, 4, '1:00', 5, 4, team=HOME_ID, points=2),
        basket(7, 4, '0:10', 6, 4, team=HOME_ID, points=1),
        row(8, 4, '0:00', 6, 4, kind='End Game'),
    ]


class TestClock:
    def test_a_period_is_twelve_minutes_and_an_overtime_is_five(self):
        assert plays.period_length(1) == 720
        assert plays.period_length(4) == 720
        assert plays.period_length(5) == 300

    def test_regulation_seconds_left_counts_down_across_four_periods(self):
        assert plays.seconds_left(1, '12:00') == 2880
        assert plays.seconds_left(2, '12:00') == 2160
        assert plays.seconds_left(4, '6:00') == 360
        assert plays.seconds_left(4, '0:00') == 0

    def test_overtime_is_pinned_at_zero_regulation_seconds(self):
        # The feed counts 300 down to 0 again in every overtime period, so a
        # fourth-period state and a first-overtime state can both read 240.
        # The model's walk ends at the end of regulation, and this is what
        # keeps the two apart.
        assert plays.seconds_left(5, '4:00') == 0
        assert plays.seconds_left(6, '0:30') == 0

    def test_elapsed_keeps_running_through_overtime(self):
        assert plays.elapsed_seconds(1, '12:00') == 0
        assert plays.elapsed_seconds(4, '0:00') == 2880
        assert plays.elapsed_seconds(5, '5:00') == 2880
        assert plays.elapsed_seconds(5, '0:00') == 3180
        assert plays.elapsed_seconds(6, '0:00') == 3480

    def test_a_clock_with_tenths_is_read_and_a_bad_one_is_not(self):
        assert plays.clock_seconds('0:48.7') == 49
        assert plays.clock_seconds('11:45') == 705
        assert plays.clock_seconds('') is None
        assert plays.clock_seconds('quarter') is None
        assert plays.clock_seconds('1:75') is None
        assert plays.seconds_left(1, 'nonsense') is None


class TestReduceGame:
    def test_the_line_score_is_the_step_between_cumulative_totals(self,
                                                                  game_rows):
        periods, _, _, _ = plays.reduce_game(PROVIDER, game_rows)
        assert [(row['period'], row['home_points'], row['away_points'])
                for row in periods] == [(1, 2, 2), (2, 1, 0), (3, 0, 2),
                                        (4, 3, 0)]

    def test_a_duplicated_run_of_rows_does_not_double_the_line_score(self,
                                                                    game_rows):
        # The archive carries a 2010 game whose rows appear twice. Summing
        # each scoring play's delta gave it a 213-234 line score for a game
        # that ended 127-141; the step between cumulative totals is right
        # either way, which is why the line score is built that way.
        doubled = game_rows + [dict(entry, game_play_number=entry[
            'game_play_number'] + 100) for entry in game_rows]
        periods, _, _, _ = plays.reduce_game(PROVIDER, doubled)
        assert sum(row['home_points'] for row in periods) == 6
        assert sum(row['away_points'] for row in periods) == 4

    def test_the_shape_counts_lead_changes_ties_leads_and_runs(self,
                                                              game_rows):
        _, shape, _, _ = plays.reduce_game(PROVIDER, game_rows)
        assert shape['lead_changes'] == 3
        assert shape['ties'] == 1
        assert shape['home_biggest_lead'] == 2
        assert shape['away_biggest_lead'] == 2
        assert shape['home_biggest_run'] == 3
        assert shape['away_biggest_run'] == 2
        assert shape['scoring_plays'] == 6
        assert shape['periods'] == 4
        assert shape['overtime'] == 0

    def test_time_in_front_adds_up_to_the_length_of_the_game(self, game_rows):
        _, shape, _, _ = plays.reduce_game(PROVIDER, game_rows)
        total = (shape['home_time_leading'] + shape['away_time_leading']
                 + shape['time_tied'])
        assert total == plays.REGULATION_SECONDS

    def test_an_overtime_game_is_marked_and_its_clock_keeps_running(self):
        rows = [row(1, 1, '12:00', 0, 0),
                basket(2, 4, '0:01', 2, 2, team=HOME_ID),
                basket(3, 5, '0:30', 4, 2, team=HOME_ID),
                row(4, 5, '0:00', 4, 2, kind='End Game')]
        periods, shape, states, _ = plays.reduce_game(PROVIDER, rows)
        assert shape['overtime'] == 1
        assert shape['periods'] == 5
        assert [entry['period'] for entry in periods] == [1, 4, 5]
        overtime = [state for state in states if state['period'] == 5]
        assert overtime and all(state['seconds_left'] == 0
                                for state in overtime)
        assert all(state['elapsed'] >= plays.REGULATION_SECONDS
                   for state in overtime)

    def test_the_curve_is_thinned_but_still_ends_at_the_final_score(self):
        # Twelve seconds apart is closer than CURVE_SECONDS, so most of these
        # are dropped -- and the last one never is, because a curve that
        # stops short does not reach the score the page prints beside it.
        rows = [row(1, 1, '12:00', 0, 0)]
        home = 0
        for index in range(40):
            home += 2
            rows.append(basket(index + 2, 1, f'{11 - index // 5}:'
                               f'{59 - (index % 5) * 12:02d}', home, 0,
                               team=HOME_ID))
        _, _, states, _ = plays.reduce_game(PROVIDER, rows)
        assert len(states) < len(rows)
        assert (states[-1]['home_score'], states[-1]['away_score']) == (80, 0)

    def test_the_scoring_side_is_read_from_the_score_not_the_play(self):
        # A row whose `team_id` names the wrong side: the score moved on the
        # away total, so the points are the away side's whatever the column
        # says. This is what keeps a goaltending call off the defence.
        rows = [row(1, 1, '12:00', 0, 0),
                basket(2, 1, '11:00', 0, 2, team=HOME_ID),
                row(3, 1, '0:00', 0, 2, kind='End Game')]
        _, shape, _, _ = plays.reduce_game(PROVIDER, rows)
        assert shape['away_biggest_run'] == 2
        assert shape['home_biggest_run'] == 0

    def test_rows_with_no_readable_state_reduce_to_nothing(self):
        assert plays.reduce_game(PROVIDER, [
            row(1, None, None, None, None)]) is None


class TestKeyPlays:
    def test_a_late_lead_change_and_the_last_score_are_kept(self, game_rows):
        _, _, _, kept = plays.reduce_game(PROVIDER, game_rows)
        reasons = {play['sequence']: play['reason'] for play in kept}
        # Play 6 put the home side in front in the fourth period; play 7 was
        # the last score of the game.
        assert reasons[6] == 'lead'
        # Play 7 was the last score of the game AND a score in the closing
        # two minutes of a one-possession game. The more specific reason
        # wins, so `final` is what a blowout's last basket carries.
        assert reasons[7] == 'late'

    def test_an_early_lead_change_is_not_a_key_play(self, game_rows):
        _, _, _, kept = plays.reduce_game(PROVIDER, game_rows)
        # Play 3 tied it and play 4 took the lead, both in the first half.
        assert all(play['sequence'] not in (2, 3, 4) for play in kept)

    def test_a_blowout_keeps_only_its_last_score(self):
        rows = [row(1, 1, '12:00', 0, 0)]
        home = 0
        for index in range(20):
            home += 2
            rows.append(basket(index + 2, (index // 5) + 1, '6:00', home, 0,
                               team=HOME_ID))
        _, _, _, kept = plays.reduce_game(PROVIDER, rows)
        assert [play['reason'] for play in kept] == ['final']

    def test_no_game_keeps_more_than_the_cap(self):
        rows = [row(1, 1, '12:00', 0, 0)]
        home = away = 0
        number = 2
        for index in range(60):
            # A basket each in turn in the fourth period: every one of them
            # ties the game or takes the lead.
            if index % 2:
                home += 2
            else:
                away += 2
            rows.append(basket(number, 4, '1:00', home, away,
                               team=HOME_ID if index % 2 else AWAY_ID))
            number += 1
        _, _, _, kept = plays.reduce_game(PROVIDER, rows)
        assert len(kept) == plays.MAX_KEY_PLAYS
        # The LATEST are the ones kept: a game with sixty lead changes in the
        # fourth period is a game whose closing minutes matter most.
        assert kept[-1]['sequence'] == number - 1


class TestGroupGames:
    def test_an_exhibition_game_is_dropped_rather_than_failing_the_run(self):
        # The 2026 file carries the All-Star mini-tournament, whose sides are
        # not franchises and whose games are not in the archive at all.
        rows = [dict(row(1, 1, '12:00', 0, 0), game_id='401838140',
                     home_team_id=132374, away_team_id=132375),
                row(2, 1, '12:00', 0, 0)]
        games, dropped = plays.group_games(rows)
        assert list(games) == [PROVIDER]
        assert dropped == 1

    def test_rows_are_put_in_play_order_not_file_order(self):
        rows = [row(10, 1, '11:00', 2, 0), row(2, 1, '12:00', 0, 0)]
        games, _ = plays.group_games(rows)
        assert [entry['game_play_number']
                for entry in games[PROVIDER]] == [2, 10]


def _store(tmp_path, final=(6, 4)):
    connection = store.connect(tmp_path / 'nba.db')
    connection.execute(
        'INSERT INTO team (team_key, team_id, name) VALUES (?, ?, ?)',
        (HOME, HOME_ID, 'Home Club'))
    connection.execute(
        'INSERT INTO team (team_key, team_id, name) VALUES (?, ?, ?)',
        (AWAY, AWAY_ID, 'Away Club'))
    connection.execute(
        'INSERT INTO game (game_key, provider_id, season, season_label,'
        ' season_type, game_date, home_team_key, away_team_key,'
        ' home_team_id, away_team_id, home_points, away_points)'
        ' VALUES (?, ?, 2026, "2025-26", "regular", "2026-01-20", ?, ?,'
        ' ?, ?, ?, ?)',
        (KEY, PROVIDER, HOME, AWAY, HOME_ID, AWAY_ID, final[0], final[1]))
    connection.commit()
    return connection


class TestStore:
    def test_a_season_is_stored_and_re_storing_it_changes_nothing(
            self, tmp_path, game_rows):
        connection = _store(tmp_path)
        reduced = plays.reduce_season(game_rows)
        first = plays.replace_season(connection, 2026, *reduced[:4])
        second = plays.replace_season(connection, 2026, *reduced[:4])
        assert first == second
        assert connection.execute(
            'SELECT COUNT(*) FROM game_period').fetchone()[0] == 4
        assert connection.execute(
            'SELECT COUNT(*) FROM game_shape').fetchone()[0] == 1

    def test_a_period_that_has_stopped_existing_stops_existing_here(
            self, tmp_path, game_rows):
        connection = _store(tmp_path)
        plays.replace_season(connection, 2026,
                             *plays.reduce_season(game_rows)[:4])
        shorter = [entry for entry in game_rows
                   if entry['period_number'] < 4]
        plays.replace_season(connection, 2026,
                             *plays.reduce_season(shorter)[:4])
        assert [row[0] for row in connection.execute(
            'SELECT period FROM game_period ORDER BY period')] == [1, 2, 3]

    def test_a_game_the_archive_does_not_have_is_dropped_and_counted(
            self, tmp_path, game_rows):
        connection = store.connect(tmp_path / 'nba.db')
        stored = plays.replace_season(
            connection, 2026, *plays.reduce_season(game_rows)[:4])
        assert stored['shapes'] == 0
        assert stored['dropped'] == 1

    def test_a_reduction_that_adds_up_is_marked_reconciled(self, tmp_path,
                                                           game_rows):
        connection = _store(tmp_path)
        plays.replace_season(connection, 2026,
                             *plays.reduce_season(game_rows)[:4])
        assert connection.execute(
            'SELECT reconciled FROM game_shape').fetchone()[0] == 1

    def test_a_reduction_short_of_the_final_is_marked_unreconciled(
            self, tmp_path, game_rows):
        # 1,390 of 31,216 games really are like this, 1,317 of them in
        # 2002-2006, where the early feed's play log is simply short of the
        # box score. The page shows the box score and says the period detail
        # is incomplete.
        connection = _store(tmp_path, final=(6, 7))
        stored = plays.replace_season(
            connection, 2026, *plays.reduce_season(game_rows)[:4])
        assert connection.execute(
            'SELECT reconciled FROM game_shape').fetchone()[0] == 0
        assert stored['unreconciled'] == 1

    def test_a_game_with_no_final_score_has_nothing_to_reconcile_against(
            self, tmp_path, game_rows):
        connection = _store(tmp_path, final=(None, None))
        plays.replace_season(connection, 2026,
                             *plays.reduce_season(game_rows)[:4])
        assert connection.execute(
            'SELECT reconciled FROM game_shape').fetchone()[0] is None


class TestIngest:
    def test_a_fetched_season_is_reduced_and_reported(self, tmp_path,
                                                      game_rows):
        connection = _store(tmp_path)

        def fetcher(seasons, **kwargs):
            for season in seasons:
                yield season, game_rows, 1234

        report = plays.ingest(connection, [2026], fetcher=fetcher)
        assert report['seasons'] == 1
        assert report['shapes'] == 1
        assert report['bytes'] == 1234
        assert report['periods'] == 4
        assert report['per_season'][0]['season'] == 2026
