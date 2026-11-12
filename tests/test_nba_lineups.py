"""The 2017-18 lineup pilot: rebuilding stints from the ESPN play-by-play.

Every fixture here is written by hand in the feed's shape. Nothing in this
file reaches the network or the archive.
"""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis.nba_lineups import collect, stints as lineups

HOME = 26
AWAY = 7
HOME_FIVE = [101, 102, 103, 104, 105]
AWAY_FIVE = [201, 202, 203, 204, 205]
HOME_BENCH = [106, 107]
AWAY_BENCH = [206, 207]
GAME = '400974766'


def play(period, clock, kind, **extra):
    row = {'game_id': GAME, 'period_number': period,
           'clock_display_value': clock, 'type_text': kind, 'text': '',
           'team_id': None, 'athlete_id_1': None, 'athlete_id_2': None,
           'athlete_id_3': None, 'home_score': None, 'away_score': None,
           'shooting_play': False, 'scoring_play': False}
    row.update(extra)
    return row


def numbered(plays):
    for index, row in enumerate(plays, start=1):
        row['game_play_number'] = index
    return plays


def missed(period, clock, team, shooter):
    return play(period, clock, 'Jump Shot', team_id=team,
                athlete_id_1=shooter, shooting_play=True)


def swap(period, clock, team, enter, leave, **extra):
    return play(period, clock, lineups.SUBSTITUTION, team_id=team,
                athlete_id_1=enter, athlete_id_2=leave,
                text='%s enters the game for %s' % (enter, leave), **extra)


def made(period, clock, team, shooter, home_score, away_score, points=2):
    return play(period, clock, 'Jump Shot', team_id=team,
                athlete_id_1=shooter, shooting_play=True, scoring_play=True,
                score_value=points, home_score=home_score,
                away_score=away_score)


def starters():
    return {HOME: set(HOME_FIVE), AWAY: set(AWAY_FIVE)}


def team_of():
    teams = {player: HOME for player in HOME_FIVE + HOME_BENCH}
    teams.update({player: AWAY for player in AWAY_FIVE + AWAY_BENCH})
    return teams


def box_line(player, team, minutes, *, starter=False, side='home'):
    return {'game_id': GAME, 'athlete_id': player, 'team_id': team,
            'starter': starter, 'minutes': minutes, 'home_away': side}


def player_box(minutes=None):
    minutes = minutes or {}
    rows = []
    for player in HOME_FIVE:
        rows.append(box_line(player, HOME, minutes.get(player, 12.0),
                             starter=True, side='home'))
    for player in HOME_BENCH:
        rows.append(box_line(player, HOME, minutes.get(player, 0.0),
                             side='home'))
    for player in AWAY_FIVE:
        rows.append(box_line(player, AWAY, minutes.get(player, 12.0),
                             starter=True, side='away'))
    for player in AWAY_BENCH:
        rows.append(box_line(player, AWAY, minutes.get(player, 0.0),
                             side='away'))
    return rows


class TestReadingTheClock:

    def test_a_period_starts_where_the_last_one_ended(self):
        assert lineups.period_start(1) == 0
        assert lineups.period_start(2) == 720
        assert lineups.period_start(5) == 2880

    def test_overtime_is_five_minutes_not_twelve(self):
        assert lineups.period_length(4) == 720
        assert lineups.period_length(5) == 300
        assert lineups.period_start(6) == 2880 + 300

    def test_an_overtime_clock_does_not_read_as_the_fourth_quarter(self):
        fourth = lineups.elapsed(4, '4:00')
        first_overtime = lineups.elapsed(5, '4:00')
        assert fourth == 720 * 3 + 480
        assert first_overtime == 2880 + 60
        assert first_overtime > fourth

    def test_a_clock_under_a_minute_carries_tenths(self):
        assert lineups.remaining('0:10.0') == 10.0
        assert lineups.remaining('11:59') == 719

    def test_a_missing_clock_is_not_a_zero(self):
        assert lineups.remaining(None) is None
        assert lineups.remaining('') is None
        assert lineups.elapsed(1, None) is None


class TestWhichPlaysEndAPossession:

    def test_a_turnover_whose_name_does_not_say_so(self):
        assert lineups.is_turnover('Traveling')

    def test_the_play_called_no_turnover_is_not_one(self):
        assert not lineups.is_turnover('No Turnover')

    def test_team_turnovers_count(self):
        assert lineups.is_turnover('Shot Clock Turnover')
        assert lineups.is_turnover('Bad Pass\nTurnover')

    def test_a_defensive_rebound_ends_the_other_teams_possession(self):
        row = play(1, '5:00', 'Defensive Rebound', team_id=HOME)
        assert lineups.offensive_team(row, [AWAY, HOME]) == AWAY

    def test_an_offensive_rebound_does_not_change_hands(self):
        row = play(1, '5:00', 'Offensive Rebound', team_id=HOME)
        assert lineups.offensive_team(row, [AWAY, HOME]) == HOME

    def test_a_foul_says_nothing_about_who_has_the_ball(self):
        row = play(1, '5:00', 'Personal Foul', team_id=HOME)
        assert lineups.offensive_team(row, [AWAY, HOME]) is None


class TestTheOneSidedSubstitutions:

    def test_a_complete_row_reads_entrant_then_exit(self):
        row = swap(1, '6:00', HOME, 106, 101)
        assert lineups._substitution(row) == (106, 101)

    def test_a_row_missing_the_exit_keeps_its_entrant(self):
        row = play(1, '6:00', lineups.SUBSTITUTION, team_id=HOME,
                   athlete_id_1=106, text='Someone enters the game for ')
        assert lineups._substitution(row) == (106, None)

    def test_a_row_missing_the_entrant_puts_the_leaver_in_the_first_slot(self):
        # ' enters the game for Y' names only Y, who is LEAVING, and the
        # feed still files him under athlete_id_1. Reading that column as
        # the entrant swaps the wrong player onto the floor.
        row = play(1, '6:00', lineups.SUBSTITUTION, team_id=HOME,
                   athlete_id_1=101, text='enters the game for Someone')
        assert lineups._substitution(row) == (None, 101)

    def test_two_halves_at_one_moment_are_joined_into_one_swap(self):
        plays = numbered([
            play(1, '6:00', lineups.SUBSTITUTION, team_id=HOME,
                 athlete_id_1=106, text='A enters the game for '),
            play(1, '6:00', lineups.SUBSTITUTION, team_id=HOME,
                 athlete_id_1=101, text='enters the game for B'),
            play(1, '0:00', lineups.PERIOD_END),
        ])
        moves = lineups.substitutions(plays, [HOME, AWAY])
        assert len(moves) == 1
        assert (moves[0]['enter'], moves[0]['leave']) == (106, 101)
        assert moves[0]['paired']

    def test_two_halves_of_the_same_kind_are_not_joined(self):
        plays = numbered([
            play(1, '6:00', lineups.SUBSTITUTION, team_id=HOME,
                 athlete_id_1=106, text='A enters the game for '),
            play(1, '6:00', lineups.SUBSTITUTION, team_id=HOME,
                 athlete_id_1=107, text='C enters the game for '),
        ])
        moves = lineups.substitutions(plays, [HOME, AWAY])
        assert len(moves) == 2
        assert all(move['leave'] is None for move in moves)

    def test_an_unnamed_entrant_is_recovered_from_the_short_floor(self):
        # This is 400974766's own case: Kenneth Faried came in for Mason
        # Plumlee on a row that named only Plumlee, and the only record
        # that Faried was on the floor is that he then played.
        plays = numbered([
            play(1, '6:00', lineups.SUBSTITUTION, team_id=HOME,
                 athlete_id_1=101, text='enters the game for A'),
            play(1, '5:00', 'Jump Shot', team_id=HOME, athlete_id_1=106,
                 shooting_play=True),
            play(1, '0:00', lineups.PERIOD_END),
        ])
        moves, notes = lineups.resolve(plays, starters(), team_of())
        assert (moves[0]['enter'], moves[0]['leave']) == (106, 101)
        assert notes == []

    def test_an_unnamed_entrant_is_recovered_when_he_is_subbed_out(self):
        plays = numbered([
            play(1, '6:00', lineups.SUBSTITUTION, team_id=HOME,
                 athlete_id_1=101, text='enters the game for A'),
            swap(1, '3:00', HOME, 101, 106),
            play(1, '0:00', lineups.PERIOD_END),
        ])
        moves, notes = lineups.resolve(plays, starters(), team_of())
        assert moves[0]['enter'] == 106
        assert notes == []

    def test_an_unnamed_exit_is_recovered_from_the_long_floor(self):
        # 106 came in for nobody, so six Home players are on the floor
        # until 101 enters again -- which he can only do if he had left.
        plays = numbered([
            play(1, '6:00', lineups.SUBSTITUTION, team_id=HOME,
                 athlete_id_1=106, text='A enters the game for '),
            swap(1, '3:00', HOME, 101, 107),
            play(1, '0:00', lineups.PERIOD_END),
        ])
        moves, notes = lineups.resolve(plays, starters(), team_of())
        assert moves[0]['leave'] == 101
        assert notes == []

    def test_what_nothing_identifies_is_left_unresolved_and_said_so(self):
        plays = numbered([
            play(1, '6:00', lineups.SUBSTITUTION, team_id=HOME,
                 athlete_id_1=101, text='enters the game for A'),
            play(1, '0:00', lineups.PERIOD_END),
        ])
        moves, notes = lineups.resolve(plays, starters(), team_of())
        assert moves[0]['enter'] is None
        assert len(notes) == 1
        spans, _ = lineups.intervals(plays, starters(), team_of())
        assert spans[-1]['resolved'] is False
        assert lineups.quality(spans[-1], {HOME: 0}, {HOME: 1}) \
            == lineups.UNRESOLVED


class TestTheStintsThemselves:

    def test_a_game_with_no_substitutions_is_one_stint_a_period(self):
        plays = numbered([
            made(1, '6:00', HOME, 101, 2, 0),
            play(1, '0:00', lineups.PERIOD_END),
            made(2, '6:00', HOME, 101, 4, 0),
            play(2, '0:00', lineups.PERIOD_END),
        ])
        spans, notes = lineups.intervals(plays, starters(), team_of())
        assert [span['period'] for span in spans] == [1, 2]
        assert all(len(five) == 5 for span in spans
                   for five in span['lineups'].values())

    def test_a_substitution_splits_the_period_at_its_own_clock(self):
        plays = numbered([
            swap(1, '6:00', HOME, 106, 101),
            play(1, '0:00', lineups.PERIOD_END),
        ])
        spans, _ = lineups.intervals(plays, starters(), team_of())
        assert [(span['start'], span['end']) for span in spans] \
            == [(0.0, 360.0), (360.0, 720.0)]
        assert 101 in spans[0]['lineups'][HOME]
        assert 106 in spans[1]['lineups'][HOME]
        assert 101 not in spans[1]['lineups'][HOME]

    def test_a_later_period_opens_with_the_previous_closing_five(self):
        plays = numbered([
            swap(1, '6:00', HOME, 106, 101),
            play(1, '0:00', lineups.PERIOD_END),
            made(2, '6:00', HOME, 106, 2, 0),
            play(2, '0:00', lineups.PERIOD_END),
        ])
        spans, _ = lineups.intervals(plays, starters(), team_of())
        second = [span for span in spans if span['period'] == 2]
        assert len(second) == 1
        assert 106 in second[0]['lineups'][HOME]

    def test_overtime_gets_its_own_five_minute_stint(self):
        plays = numbered([
            play(4, '0:00', lineups.PERIOD_END),
            made(5, '2:00', HOME, 101, 2, 0),
            play(5, '0:00', lineups.PERIOD_END),
        ])
        spans, _ = lineups.intervals(plays, starters(), team_of())
        overtime = [span for span in spans if span['period'] == 5]
        assert len(overtime) == 1
        assert overtime[0]['end'] - overtime[0]['start'] == 300

    def test_minutes_come_out_of_the_stints_alone(self):
        plays = numbered([
            swap(1, '6:00', HOME, 106, 101),
            play(1, '0:00', lineups.PERIOD_END),
        ])
        spans, _ = lineups.intervals(plays, starters(), team_of())
        played = lineups.minutes_played(spans)
        assert played[101] == 6.0
        assert played[106] == 6.0
        assert played[102] == 12.0


class TestPointsAndPossessions:

    def test_a_stints_first_basket_belongs_to_that_stint(self):
        # The score columns already contain the play, so a stint's opening
        # score is the one BEFORE its first play. Snapshotting after drops
        # every stint's first basket and the game ends short of its final.
        plays = numbered([
            made(1, '11:00', HOME, 101, 2, 0),
            swap(1, '6:00', HOME, 106, 101),
            made(1, '5:00', HOME, 106, 4, 0),
            made(1, '4:00', AWAY, 201, 4, 3, points=3),
            play(1, '0:00', lineups.PERIOD_END),
        ])
        spans, _ = lineups.intervals(plays, starters(), team_of())
        points, _ = lineups.measure(plays, spans, [AWAY, HOME],
                                    home=HOME, away=AWAY)
        assert points[0][HOME] == 2
        assert points[1][HOME] == 2
        assert points[1][AWAY] == 3
        assert sum(one[HOME] for one in points) == 4
        assert sum(one[AWAY] for one in points) == 3

    def test_an_and_one_is_one_possession_not_two(self):
        plays = numbered([
            made(1, '11:00', HOME, 101, 2, 0),
            play(1, '11:00', 'Shooting Foul', team_id=AWAY,
                 athlete_id_1=201),
            play(1, '11:00', 'Free Throw - 1 of 1', team_id=HOME,
                 athlete_id_1=101, shooting_play=True, scoring_play=True,
                 home_score=3, away_score=0),
            play(1, '0:00', lineups.PERIOD_END),
        ])
        spans, _ = lineups.intervals(plays, starters(), team_of())
        _, possessions = lineups.measure(plays, spans, [AWAY, HOME],
                                         home=HOME, away=AWAY)
        assert possessions[0][HOME] == 1
        assert possessions[0][AWAY] == 0

    def test_a_possession_is_credited_where_its_basket_was_scored(self):
        # 400974447's own shape: a basket, the and-one missed, a
        # substitution, and only then the rebound that finally ends the
        # possession. The points are in the first stint, so the
        # possession has to be too.
        plays = numbered([
            made(1, '7:00', HOME, 101, 2, 0),
            play(1, '7:00', 'Free Throw - 1 of 1', team_id=HOME,
                 athlete_id_1=101, shooting_play=True, home_score=2,
                 away_score=0),
            swap(1, '6:00', HOME, 106, 102, home_score=2, away_score=0),
            play(1, '5:00', 'Offensive Rebound', team_id=HOME,
                 athlete_id_1=103, home_score=2, away_score=0),
            made(1, '4:00', AWAY, 201, 2, 2),
            play(1, '0:00', lineups.PERIOD_END),
        ])
        spans, _ = lineups.intervals(plays, starters(), team_of())
        points, possessions = lineups.measure(plays, spans, [AWAY, HOME],
                                              home=HOME, away=AWAY)
        assert points[0][HOME] == 2
        assert possessions[0][HOME] == 1
        assert possessions[1][HOME] == 0

    def test_a_technical_free_throw_is_not_a_possession(self):
        plays = numbered([
            made(1, '11:00', HOME, 101, 2, 0),
            play(1, '10:00', 'Defensive 3-Seconds Technical', team_id=AWAY,
                 athlete_id_1=201, home_score=2, away_score=0),
            play(1, '10:00', 'Free Throw - Technical', team_id=HOME,
                 athlete_id_1=101, shooting_play=True, scoring_play=True,
                 home_score=3, away_score=0),
            play(1, '0:00', lineups.PERIOD_END),
        ])
        row = plays[2]
        assert lineups.offensive_team(row, [AWAY, HOME]) is None
        spans, _ = lineups.intervals(plays, starters(), team_of())
        points, possessions = lineups.measure(plays, spans, [AWAY, HOME],
                                              home=HOME, away=AWAY)
        assert points[0][HOME] == 3
        assert possessions[0][HOME] == 1

    def test_the_ball_changing_hands_is_counted_once_each_way(self):
        plays = numbered([
            made(1, '11:00', HOME, 101, 2, 0),
            made(1, '10:00', AWAY, 201, 2, 2),
            play(1, '9:00', 'Lost Ball Turnover', team_id=HOME,
                 athlete_id_1=101, home_score=2, away_score=2),
            play(1, '0:00', lineups.PERIOD_END),
        ])
        spans, _ = lineups.intervals(plays, starters(), team_of())
        _, possessions = lineups.measure(plays, spans, [AWAY, HOME],
                                         home=HOME, away=AWAY)
        assert possessions[0][HOME] == 2
        assert possessions[0][AWAY] == 1

    def test_a_possession_belongs_to_the_stint_that_ended_it(self):
        plays = numbered([
            made(1, '11:00', HOME, 101, 2, 0),
            missed(1, '10:00', AWAY, 201),
            play(1, '9:50', 'Defensive Rebound', team_id=HOME,
                 athlete_id_1=102),
            swap(1, '6:00', HOME, 106, 101),
            made(1, '5:00', HOME, 106, 4, 0),
            play(1, '0:00', lineups.PERIOD_END),
        ])
        spans, _ = lineups.intervals(plays, starters(), team_of())
        _, possessions = lineups.measure(plays, spans, [AWAY, HOME],
                                         home=HOME, away=AWAY)
        assert possessions[0][HOME] == 1
        assert possessions[0][AWAY] == 1
        assert possessions[1][HOME] == 1

    def test_a_possession_crossing_a_substitution_lands_with_its_points(self):
        # The basket that ends a possession can fall on the other side of a
        # substitution from the shot that began it. Crediting the start
        # leaves the later stint with points and nothing to divide them by.
        plays = numbered([
            made(1, '11:00', HOME, 101, 2, 0),
            missed(1, '7:00', AWAY, 201),
            play(1, '6:50', 'Offensive Rebound', team_id=AWAY,
                 athlete_id_1=202),
            swap(1, '6:00', HOME, 106, 101),
            made(1, '5:00', AWAY, 201, 2, 2),
            play(1, '0:00', lineups.PERIOD_END),
        ])
        spans, _ = lineups.intervals(plays, starters(), team_of())
        points, possessions = lineups.measure(plays, spans, [AWAY, HOME],
                                              home=HOME, away=AWAY)
        assert points[1][AWAY] == 2
        assert possessions[1][AWAY] == 1
        assert possessions[0][AWAY] == 0
        assert lineups.quality(spans[1], points[1], possessions[1]) \
            == lineups.VERIFIED

    def test_a_run_does_not_carry_across_the_period_break(self):
        plays = numbered([
            made(1, '1:00', HOME, 101, 2, 0),
            play(1, '0:00', lineups.PERIOD_END),
            made(2, '11:00', HOME, 101, 4, 0),
            play(2, '0:00', lineups.PERIOD_END),
        ])
        spans, _ = lineups.intervals(plays, starters(), team_of())
        _, possessions = lineups.measure(plays, spans, [AWAY, HOME],
                                         home=HOME, away=AWAY)
        assert [one[HOME] for one in possessions] == [1, 1]


class TestWhatAStintIsWorth:

    def _span(self):
        return {'start': 0.0, 'end': 720.0, 'period': 1, 'resolved': True,
                'lineups': {HOME: frozenset(HOME_FIVE),
                            AWAY: frozenset(AWAY_FIVE)}}

    def test_a_clean_stint_is_verified(self):
        assert lineups.quality(self._span(), {HOME: 10, AWAY: 8},
                               {HOME: 9, AWAY: 9}) == lineups.VERIFIED

    def test_points_with_no_possession_is_called_out_not_dropped(self):
        assert lineups.quality(self._span(), {HOME: 2, AWAY: 0},
                               {HOME: 0, AWAY: 1}) \
            == lineups.SCORED_WITHOUT_POSSESSION

    def test_no_points_and_no_possessions_is_just_a_short_stint(self):
        assert lineups.quality(self._span(), {HOME: 0, AWAY: 0},
                               {HOME: 0, AWAY: 0}) == lineups.VERIFIED

    def test_a_stint_holding_a_player_whose_minutes_are_wrong_says_so(self):
        assert lineups.quality(self._span(), {HOME: 10, AWAY: 8},
                               {HOME: 9, AWAY: 9}, adrift={104}) \
            == lineups.MINUTES_UNRECONCILED

    def test_an_unresolved_lineup_outranks_every_other_complaint(self):
        span = self._span()
        span['resolved'] = False
        assert lineups.quality(span, {HOME: 2, AWAY: 0}, {HOME: 0, AWAY: 1},
                               adrift={104}) == lineups.UNRESOLVED


class TestReadingTheBoxScore:

    def test_a_stated_zero_minute_appearance_is_kept(self):
        # Three Spurs entered 400974440 with ten seconds left and the box
        # rounds each to 0.0. Treating that as "did not appear" made the
        # rebuild look wrong in games where it was right.
        rows = player_box({106: 0.0})
        assert collect.box_minutes(rows, GAME)[106] == 0.0

    def test_a_player_who_did_not_play_is_left_out(self):
        rows = player_box()
        rows.append(box_line(108, HOME, None, side='home'))
        assert 108 not in collect.box_minutes(rows, GAME)

    def test_the_sides_come_from_the_boxs_own_labelling(self):
        assert collect.home_away(player_box(), GAME) == (HOME, AWAY)

    def test_the_possession_estimate_weights_free_throws(self):
        team_box = [{'game_id': GAME, 'team_id': HOME,
                     'field_goals_attempted': 80,
                     'free_throws_attempted': 25,
                     'offensive_rebounds': 10, 'turnovers': 14}]
        estimate = collect.box_possessions(team_box, GAME)
        assert estimate[HOME] == pytest.approx(80 + 0.44 * 25 - 10 + 14)


class TestTheRowsThatGetWritten:

    def _game(self):
        plays = numbered([
            made(1, '11:00', HOME, 101, 2, 0),
            missed(1, '10:00', AWAY, 201),
            play(1, '9:50', 'Defensive Rebound', team_id=HOME,
                 athlete_id_1=102),
            swap(1, '6:00', HOME, 106, 101),
            made(1, '5:00', HOME, 106, 4, 0),
            made(1, '4:00', AWAY, 201, 4, 3, points=3),
            play(1, '0:00', lineups.PERIOD_END),
        ])
        return plays, player_box({101: 6.0, 106: 6.0})

    def test_every_row_carries_five_players_a_side(self):
        plays, box = self._game()
        rows, notes, _ = collect.build_game(plays, box, GAME,
                                            date='2017-10-18', venue='Arena')
        assert rows
        for row in rows:
            assert len(row['home_player_ids'].split()) == 5
            assert len(row['away_player_ids'].split()) == 5
            assert row['home_team'] == HOME and row['away_team'] == AWAY
            assert row['date'] == '2017-10-18' and row['venue'] == 'Arena'

    def test_the_clocks_run_down_inside_a_period(self):
        plays, box = self._game()
        rows, _, _ = collect.build_game(plays, box, GAME)
        assert rows[0]['start_clock'] == '12:00'
        assert rows[0]['end_clock'] == '6:00'
        assert rows[1]['start_clock'] == '6:00'
        assert rows[1]['end_clock'] == '0:00'

    def test_the_stint_points_add_up_to_the_games_own_score(self):
        plays, box = self._game()
        rows, _, _ = collect.build_game(plays, box, GAME)
        assert sum(row['home_points'] for row in rows) == 4
        assert sum(row['away_points'] for row in rows) == 3

    def test_an_all_star_game_is_not_a_lineup(self):
        # Both of 2017-18's All-Star games carry season_type 2 and both
        # have full play-by-play, so only the teams give them away.
        plays, box = self._game()
        exhibition = []
        for row in box:
            row = dict(row)
            if row['team_id'] == HOME:
                row['team_id'] = 111353            # Team LeBron
            exhibition.append(row)
        rows, notes, _ = collect.build_game(plays, exhibition, GAME)
        assert rows == []
        assert 'exhibition' in notes[0]

    def test_a_game_the_box_gives_no_starters_for_is_skipped(self):
        plays, box = self._game()
        thin = [row for row in box if not row['starter']]
        rows, notes, _ = collect.build_game(plays, thin, GAME)
        assert rows == []
        assert notes

    def test_the_written_file_names_its_fields_and_hashes_itself(
            self, tmp_path):
        plays, box = self._game()
        rows, _, rebuilt = collect.build_game(plays, box, GAME)
        team_box = [{'game_id': GAME, 'team_id': HOME,
                     'field_goals_attempted': 2, 'free_throws_attempted': 0,
                     'offensive_rebounds': 0, 'turnovers': 0},
                    {'game_id': GAME, 'team_id': AWAY,
                     'field_goals_attempted': 1, 'free_throws_attempted': 0,
                     'offensive_rebounds': 0, 'turnovers': 0}]
        report = collect.reconcile(rows, rebuilt, box, team_box, GAME)
        summary = collect.coverage(rows, [report])
        manifest = collect.write(rows, [report], summary, tmp_path,
                                 season=2018, revision='test')
        table = tmp_path / 'stints_2018.csv'
        assert table.exists()
        assert table.read_text().splitlines()[0] == ','.join(collect.FIELDS)
        assert manifest['published'] is False
        assert manifest['identifiers'] == 'ESPN'
        assert len(manifest['stint_file_sha256']) == 64
        saved = json.loads((tmp_path / 'manifest.json').read_text())
        assert saved['summary']['stints'] == len(rows)

    def test_a_game_whose_minutes_disagree_is_not_called_verified(self):
        plays, box = self._game()
        wrong = [dict(row) for row in box]
        for row in wrong:
            if row['athlete_id'] == 101:
                row['minutes'] = 12.0        # the feed subbed him out at 6:00
        rows, _, _ = collect.build_game(plays, wrong, GAME)
        assert any(row['quality_status'] == lineups.MINUTES_UNRECONCILED
                   for row in rows)

    def test_the_reconciliation_states_both_possession_counts(self):
        plays, box = self._game()
        rows, _, rebuilt = collect.build_game(plays, box, GAME)
        team_box = [{'game_id': GAME, 'team_id': HOME,
                     'field_goals_attempted': 2, 'free_throws_attempted': 0,
                     'offensive_rebounds': 0, 'turnovers': 0},
                    {'game_id': GAME, 'team_id': AWAY,
                     'field_goals_attempted': 1, 'free_throws_attempted': 0,
                     'offensive_rebounds': 0, 'turnovers': 0}]
        report = collect.reconcile(rows, rebuilt, box, team_box, GAME)
        assert report['possessions_counted'][HOME] == 2
        assert report['possessions_estimated'][HOME] == 2.0
        assert report['minutes_agree'] is True
        assert report['points_rebuilt'][HOME] == 4

    def test_the_coverage_summary_counts_what_is_trustworthy(self):
        plays, box = self._game()
        rows, _, rebuilt = collect.build_game(plays, box, GAME)
        team_box = [{'game_id': GAME, 'team_id': HOME,
                     'field_goals_attempted': 2, 'free_throws_attempted': 0,
                     'offensive_rebounds': 0, 'turnovers': 0},
                    {'game_id': GAME, 'team_id': AWAY,
                     'field_goals_attempted': 1, 'free_throws_attempted': 0,
                     'offensive_rebounds': 0, 'turnovers': 0}]
        report = collect.reconcile(rows, rebuilt, box, team_box, GAME)
        summary = collect.coverage(rows, [report, {'game_id': 'x',
                                                   'stints': 0}])
        assert summary['games'] == 2
        assert summary['games_rebuilt'] == 1
        assert summary['games_without_stints'] == 1
        assert summary['stints'] == len(rows)
        assert summary['verified_share'] == 1.0

    def test_a_season_is_grouped_once_not_scanned_once_a_game(self):
        plays, box = self._game()
        grouped = collect.by_game(plays)
        assert list(grouped) == [GAME]
        assert collect._of_game(grouped, GAME) == plays
        assert collect._of_game(plays, GAME) == plays
        assert collect._of_game(grouped, 'nothing') == []
