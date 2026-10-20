"""Minute and score reconciliation gates. Offline synthetic data, no network.

These tests pin the B01 gate: the box score feed and the lineup feed must agree
about floor time before paired-lineup reconstruction is trusted. Synthetic data
cannot establish real coverage; the measured run is recorded in the pilot docs.
"""

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis.cbb_player_impact.reconcile import (clock_ordering_issues,
                                                  expected_player_minutes,
                                                  filtered_team_id,
                                                  lineup_seconds_by_team,
                                                  period_count, reconcile_game,
                                                  reconcile_team_box_score)


def game(periods=2, home=70, away=60):
    return dict(id=7, homeTeamId=1, awayTeamId=2, homePoints=home, awayPoints=away,
                homePeriodPoints=[home] + [0] * (periods - 1),
                awayPeriodPoints=[away] + [0] * (periods - 1))


def box(team=1, minutes=40.0, points=70, bench_minutes=0.0, starters=5, players=6):
    rows = [dict(athleteId=team * 100 + i, starter=i < starters, minutes=minutes,
                 points=points if i == 0 else 0) for i in range(players)]
    for row in rows[starters:]:
        row['minutes'] = bench_minutes
    return dict(gameId=7, teamId=team, players=rows)


def test_regulation_and_overtime_sizing():
    assert expected_player_minutes(2) == 200
    assert expected_player_minutes(3) == 225
    assert expected_player_minutes(5) == 275
    with pytest.raises(ValueError):
        expected_player_minutes(1)
    assert period_count(game(3)) == 3
    assert period_count(dict(homePeriodPoints=[1, 2], awayPeriodPoints=[1])) is None
    assert period_count(dict(homePeriodPoints=None, awayPeriodPoints=None)) is None


def test_a_clean_box_score_reconciles():
    result = reconcile_team_box_score(game(), box(), lineup_seconds=2400)
    assert result['reconciled'], result['issues']
    assert result['minutes'] == 200
    assert result['expected_minutes'] == 200
    assert result['minutes_from_lineups'] == pytest.approx(200)
    assert result['points'] == 70


def test_minutes_that_miss_the_game_length_are_reported():
    result = reconcile_team_box_score(game(), box(minutes=38.0))
    assert not result['reconciled']
    assert any('do not total the expected' in issue for issue in result['issues'])


def test_box_score_disagreeing_with_the_lineup_feed_blocks_stint_work():
    # 2,100 lineup seconds implies 175 player-minutes, not 200.
    result = reconcile_team_box_score(game(), box(), lineup_seconds=2100)
    assert not result['reconciled']
    assert any('stints cannot be trusted' in issue for issue in result['issues'])


def test_points_must_equal_the_final_score():
    result = reconcile_team_box_score(game(), box(points=68))
    assert not result['reconciled']
    assert any('do not equal final score' in issue for issue in result['issues'])


def test_five_starters_required():
    result = reconcile_team_box_score(game(), box(starters=4, players=6))
    assert not result['reconciled']
    assert any('starters' in issue for issue in result['issues'])


def test_missing_minutes_are_counted_not_treated_as_zero():
    row = box()
    row['players'][5]['minutes'] = None
    result = reconcile_team_box_score(game(), row, lineup_seconds=2400)
    assert result['players_without_minutes'] == 1
    # The five starters still total 200, so a null bench minute is not a shortfall.
    assert result['reconciled'], result['issues']
    assert result['minutes'] == 200


def test_a_boolean_is_not_a_minute_count():
    # isinstance(True, int) is True, so an unguarded check would add one minute.
    row = box()
    row['players'][0]['minutes'] = True
    result = reconcile_team_box_score(game(), row, lineup_seconds=2400)
    assert result['players_without_minutes'] == 1
    assert result['minutes'] == 160
    assert not result['reconciled']


def test_duplicate_and_unusable_athlete_ids_are_rejected():
    row = box()
    row['players'][1]['athleteId'] = row['players'][0]['athleteId']
    assert any('Duplicate athlete id' in i
               for i in reconcile_team_box_score(game(), row)['issues'])
    row = box()
    row['players'][0]['athleteId'] = 'byu-guard'
    assert any('usable athlete id' in i
               for i in reconcile_team_box_score(game(), row)['issues'])


def test_overtime_box_score_reconciles_against_the_longer_game():
    g, row = game(3), box(minutes=45.0)
    result = reconcile_team_box_score(g, row, lineup_seconds=2700)
    assert result['expected_minutes'] == 225
    assert result['reconciled'], result['issues']


def test_unverifiable_game_length_is_stated_not_assumed():
    g = game()
    g['homePeriodPoints'] = None
    result = reconcile_team_box_score(g, box())
    assert not result['reconciled']
    assert any('Cannot size the game' in issue for issue in result['issues'])
    assert result['expected_minutes'] is None


def test_a_box_score_from_another_game_is_refused():
    result = reconcile_team_box_score(game(), dict(teamId=99, players=[box()]))
    assert not result['reconciled']
    assert result['issues'] == ['Box score team is not in this game']


def test_game_level_reconciliation_scopes_to_the_filtered_team():
    g = game()
    seconds = {1: 2400, 2: 2400}
    only_home = reconcile_game(g, [box(team=1)], seconds, {1})
    assert only_home['reconciled']
    assert only_home['reconciled_teams'] == [1]
    # Demanding both sides reports the opponent's absence rather than passing.
    both = reconcile_game(g, [box(team=1)], seconds)
    assert not both['reconciled']
    assert both['issues'] == ['2: no box score for this game']


def test_lineup_seconds_skip_unusable_rows():
    rows = [dict(teamId=1, totalSeconds=1200), dict(teamId=1, totalSeconds=1200),
            dict(teamId=2, totalSeconds=None), dict(teamId=2, totalSeconds=0),
            dict(teamId=None, totalSeconds=600)]
    assert lineup_seconds_by_team(rows) == {1: 2400}


def test_filtered_team_id_requires_exactly_one_team():
    assert filtered_team_id([box(team=4), box(team=4)]) == 4
    with pytest.raises(ValueError):
        filtered_team_id([box(team=1), box(team=2)])
    with pytest.raises(ValueError):
        filtered_team_id([])


def test_clock_must_count_down_within_a_period():
    good = [dict(period=1, secondsRemaining=1200), dict(period=1, secondsRemaining=1100),
            dict(period=2, secondsRemaining=1200)]
    assert clock_ordering_issues(good, 'plays') == []
    bad = [dict(period=1, secondsRemaining=1100), dict(period=1, secondsRemaining=1200)]
    assert len(clock_ordering_issues(bad, 'plays')) == 1
    missing = [dict(period=None, secondsRemaining=None)]
    assert len(clock_ordering_issues(missing, 'subs')) == 1


def test_equal_clock_substitutions_are_allowed():
    # Five players can enter at one dead ball; equal clocks are not an error.
    same = [dict(period=1, secondsRemaining=600)] * 5
    assert clock_ordering_issues(same, 'substitutions') == []


def test_summary_reports_missing_snapshot_rather_than_claiming_success(tmp_path):
    from analysis.cbb_player_impact.summarize import summarize
    lines = summarize(tmp_path)
    assert any('No snapshot produced' in line for line in lines)
    assert not any('reconciled' in line for line in lines)


def test_summary_states_the_sample_is_not_national(tmp_path):
    import json
    from analysis.cbb_player_impact.summarize import summarize
    (tmp_path / 'manifest.json').write_text(json.dumps(dict(
        status='collected; player-model validation pending', season=2026, team='BYU',
        retrieved_at='2026-10-20T03:16:17+00:00', schedule_games=35,
        completed_games=35, sampled_games=3, box_score_games=35)))
    (tmp_path / 'reconciliation.json').write_text(json.dumps([reconcile_game(
        game(), [box(team=1)], {1: 2400}, {1})]))
    text = '\n'.join(summarize(tmp_path))
    assert 'Minute reconciliation: 1/1 games' in text
    assert 'must never be published as national coverage' in text
    assert 'minutes 200.0 vs expected 200 vs lineups 200.0' in text
