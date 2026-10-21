"""Rosters and box scores: the choices that would corrupt the people layer.

The schedule layer's cap is a record count; this layer's cap is a GAME count,
on a different endpoint, and a window sized for one is wrong for the other.
Everything here pins a way that difference, or an identity, could be lost in
silence.
"""

from datetime import date, timedelta
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ncaambb.http import CbbdError
from src.ncaambb.people import (BOX_SCORE_RECORD_CAP,
                                BOX_SCORE_WINDOW_DAYS, box_score_day,
                                box_score_people, box_score_windows,
                                fetch_box_scores, fetch_rosters,
                                merge_box_scores, one_sided_games,
                                roster_players)
from src.ncaambb.games import season_window


def line(athlete=101, name='A Guard', points=12, minutes=28, starter=True):
    return dict(athleteId=athlete, name=name, position='G', starter=starter,
                minutes=minutes, points=points)


def box(game=1, team=10, players=None, day='2025-11-07T19:00:00Z'):
    return dict(gameId=game, teamId=team, team='Alpha', isHome=True,
                startDate=day, players=players if players is not None
                else [line()])


def roster(team=10, players=None):
    return dict(teamId=team, team='Alpha',
                players=players if players is not None
                else [dict(id=101, name='A Guard', jersey='3', position='G',
                           height=75.0, weight=180.0)])


def recorder(rows_for):
    """A getter that records the windows it was asked for."""
    asked = []

    def getter(path, params, **kwargs):
        asked.append((path, params['startDateRange'][:10],
                      params['endDateRange'][:10]))
        return rows_for(params['startDateRange'][:10],
                        params['endDateRange'][:10])

    return getter, asked


# --- the roster request -----------------------------------------------------

def test_a_season_of_rosters_is_one_request_because_team_is_optional():
    calls = []

    def getter(path, params, **kwargs):
        calls.append((path, dict(params)))
        return [roster()]

    assert fetch_rosters(2026, getter=getter) == [roster()]
    assert calls == [('/teams/roster', {'season': 2026})]


# --- the game cap, which is not the record cap ------------------------------

def test_the_box_score_cap_counts_rows_because_that_is_what_was_measured():
    """Measured on the provider: 2025-11-13..27 holds 821 games and 1,642
    rows, and one request for the whole window returns exactly 1,000 rows
    covering 501 games. An earlier version capped on distinct GAMES at 1,000,
    a threshold no response ever reaches, so every busy window was truncated
    in silence."""
    assert BOX_SCORE_RECORD_CAP == 1000


def test_a_window_at_the_game_cap_is_split_until_it_is_not():
    """A busy stretch exceeds the game cap inside one window, so the window is
    halved until every request that is actually used came back under it, and
    the windows that are used still tile the season exactly."""
    busy = (date(2025, 11, 1), date(2025, 11, 30))

    def per_day(day):
        return 150 if busy[0] <= day <= busy[1] else 2

    def rows_for(start, end):
        first, last = date.fromisoformat(start), date.fromisoformat(end)
        rows, day = [], first
        while day <= last:
            rows += [box(game=f'{day}-{n}', day=f'{day}T19:00:00Z')
                     for n in range(per_day(day))]
            day += timedelta(days=1)
        return rows

    getter, asked = recorder(rows_for)
    rows, windows = fetch_box_scores(2026, getter=getter, window_days=15)

    used = [(date.fromisoformat(start), date.fromisoformat(end))
            for _, start, end in asked
            if len({row['gameId'] for row in rows_for(start, end)})
            < BOX_SCORE_RECORD_CAP]
    assert windows == len(used)
    # A 15-day November window holds 2,250 games, so splitting had to happen.
    assert len(used) > len(box_score_windows(*season_window(2026), days=15))
    # The windows that were used tile the whole season, with no gap and no
    # overlap: nothing was requested, found over the cap and then forgotten.
    covered = sorted(used)
    assert covered[0][0] == season_window(2026)[0]
    assert covered[-1][1] == season_window(2026)[1]
    for (_, left), (right, _) in zip(covered, covered[1:]):
        assert (right - left).days == 1
    expected = sum(per_day(day) for day in
                   [covered[0][0] + timedelta(days=n)
                    for n in range((covered[-1][1] - covered[0][0]).days + 1)])
    assert len({row['gameId'] for row in rows}) == expected


def test_a_window_over_the_row_cap_splits_though_its_games_are_fewer():
    """900 games is 1,800 rows and well over a 1,000-ROW cap. Measuring games
    against that cap is what let the real truncation through: 501 games was
    never 1,000 of anything."""
    def rows_for(start, end):
        day = date.fromisoformat(start)
        span = (date.fromisoformat(end) - day).days + 1
        per = max(1, 900 // 15)
        return [box(game=f'{day}-{n}', team=team, day=f'{day}T19:00:00Z')
                for n in range(per * span) for team in (10, 20)]

    getter, asked = recorder(rows_for)
    rows, windows = fetch_box_scores(2026, getter=getter, window_days=15)
    assert windows > len(box_score_windows(*season_window(2026), days=15))
    # Every request whose rows were kept came back under the cap.
    kept = [(s, e) for _, s, e in asked
            if len(rows_for(s, e)) < BOX_SCORE_RECORD_CAP]
    assert kept, 'something must have been small enough to keep'
    assert one_sided_games(rows) == [], 'a complete fetch has no half game'


def test_the_default_window_is_small_enough_for_the_busiest_day():
    """The busiest measured day runs to about 200 rows, so a window of five
    days stays under 1,000 without needing a split at all."""
    assert BOX_SCORE_WINDOW_DAYS * 200 <= BOX_SCORE_RECORD_CAP


def test_a_game_with_one_side_is_reported_as_a_half_game():
    """The row cap cuts a game in half. That half is the direct evidence a
    response was truncated, so it must be visible rather than stored quietly."""
    assert one_sided_games([box(game=1, team=10), box(game=1, team=20),
                            box(game=2, team=10)]) == [2]


def test_a_game_whose_two_rows_name_one_team_is_also_a_half_game():
    assert one_sided_games([box(game=3, team=10), box(game=3, team=10)]) == [3]


def test_a_single_day_over_the_cap_fails_rather_than_truncating():
    def rows_for(start, end):
        return [box(game=n, day=f'{start}T19:00:00Z')
                for n in range(BOX_SCORE_RECORD_CAP + 5)]

    getter, _ = recorder(rows_for)
    with pytest.raises(CbbdError, match='cannot be split'):
        fetch_box_scores(2026, getter=getter, window_days=1)


def test_a_row_outside_the_requested_window_fails_the_fetch():
    """If the provider ignores the date filter, a window's rows are not that
    window's rows and the splitting logic is measuring the wrong thing."""
    getter, _ = recorder(lambda start, end: [box(day='2030-01-01T00:00:00Z')])
    with pytest.raises(CbbdError, match='outside the requested window'):
        fetch_box_scores(2026, getter=getter, window_days=30)


# --- windows ----------------------------------------------------------------

def test_windows_cover_the_span_with_no_gap_and_no_overlap():
    windows = box_score_windows(date(2025, 11, 1), date(2025, 12, 5), days=10)
    assert windows[0][0] == date(2025, 11, 1)
    assert windows[-1][1] == date(2025, 12, 5)
    for (_, end), (start, _) in zip(windows, windows[1:]):
        assert (start - end).days == 1


def test_a_window_spans_the_number_of_days_it_was_asked_for():
    """Off by one here makes every window a day longer than the cap was sized
    for, which is how a window silently grows back over the cap."""
    windows = box_score_windows(date(2025, 11, 1), date(2026, 3, 1), days=15)
    assert all((end - start).days + 1 == 15 for start, end in windows[:-1])


def test_a_single_day_span_is_one_window():
    assert box_score_windows(date(2025, 11, 1), date(2025, 11, 1), days=15) == [
        (date(2025, 11, 1), date(2025, 11, 1))]


def test_a_zero_length_window_is_refused_rather_than_looping_forever():
    with pytest.raises(ValueError, match='at least one day'):
        box_score_windows(date(2025, 11, 1), date(2025, 12, 1), days=0)


def test_a_backwards_span_is_refused():
    with pytest.raises(ValueError, match='before it starts'):
        box_score_windows(date(2025, 12, 1), date(2025, 11, 1))


def test_the_whole_season_window_is_covered_by_box_score_windows():
    getter, asked = recorder(lambda start, end: [])
    fetch_box_scores(2026, getter=getter, window_days=15)
    assert asked[0][1] == '2025-07-01'
    assert asked[-1][2] == '2026-06-30'


def test_a_box_score_day_reads_the_same_field_as_a_game():
    assert box_score_day(box(day='2025-11-07T02:00:00Z')) == date(2025, 11, 7)
    assert box_score_day(dict(gameId=1)) is None


# --- deduplication ----------------------------------------------------------

def test_an_identical_repeat_across_window_edges_collapses():
    assert len(merge_box_scores([box(), box()])) == 1


def test_two_rows_for_one_team_game_that_disagree_fail():
    with pytest.raises(ValueError, match='conflicting'):
        merge_box_scores([box(), box(players=[line(points=99)])])


def test_both_sides_of_one_game_are_kept():
    merged = merge_box_scores([box(team=10), box(team=20)])
    assert {row['teamId'] for row in merged} == {10, 20}


def test_a_box_score_with_no_team_is_refused_not_keyed_on_none():
    with pytest.raises(ValueError, match='without a game or team id'):
        merge_box_scores([dict(gameId=1, players=[])])


# --- identity ---------------------------------------------------------------

def test_a_roster_flattens_into_player_and_team_pairs():
    pairs = roster_players([roster()])
    assert [(p['id'], t) for p, t in pairs] == [(101, 10)]


def test_a_transfer_keeps_both_memberships_and_is_not_merged():
    both = roster_players([roster(10), roster(20)])
    assert len(both) == 2
    assert {t for _, t in both} == {10, 20}


def test_two_players_sharing_a_name_stay_two_people():
    pairs = roster_players([roster(10, [
        dict(id=101, name='A Guard'), dict(id=202, name='A Guard')])])
    assert {p['id'] for p, _ in pairs} == {101, 202}


def test_a_roster_row_with_no_integer_player_id_is_dropped_not_guessed():
    pairs = roster_players([roster(10, [dict(id=None, name='A Guard'),
                                        dict(id='101', name='B Guard')])])
    assert pairs == []


def test_a_roster_with_no_team_id_is_dropped_rather_than_attributed():
    assert roster_players([dict(team='Alpha', players=[dict(id=101)])]) == []


def test_a_roster_with_no_players_does_not_fail():
    assert roster_players([roster(10, [])]) == []


def test_roster_teams_are_distinct_and_carry_the_feeds_own_name():
    from src.ncaambb.people import roster_teams
    teams = roster_teams([roster(10), roster(10), roster(20)])
    assert [(t['id'], t['school']) for t in teams] == [(10, 'Alpha'),
                                                       (20, 'Alpha')]


def test_a_roster_with_no_integer_team_id_names_no_team():
    from src.ncaambb.people import roster_teams
    assert roster_teams([dict(teamId=None, team='Alpha'),
                         dict(teamId='10', team='Alpha')]) == []


def test_a_roster_team_with_no_name_gets_its_id_and_nothing_invented():
    from src.ncaambb.people import roster_teams
    assert roster_teams([dict(teamId=777, team=None)]) == [
        dict(id=777, school='Team 777')]


def test_a_player_who_appears_only_in_a_box_score_still_gets_an_identity():
    """Rosters are a snapshot and a stat line is a fact. Taking identities from
    rosters alone would drop the line rather than the gap."""
    people = box_score_people([box(players=[line(athlete=999, name='Walk On')])])
    assert [(p['id'], p['name']) for p in people] == [(999, 'Walk On')]


def test_a_player_in_many_box_scores_yields_one_identity():
    people = box_score_people([box(game=1), box(game=2)])
    assert len(people) == 1


def test_a_box_score_line_with_no_athlete_id_yields_no_identity():
    people = box_score_people([box(players=[line(athlete=None),
                                            line(athlete='101')])])
    assert people == []
