"""The people coverage audit: that it separates a provider gap from our own.

The audit exists to answer one question the raw counts cannot: a missing box
score is either the provider having none or our windowed fetch having lost one,
and those have opposite remedies. A report that blurs the two is worse than no
report, so each test here pins the classification rather than the wording.
"""

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ncaambb.audit_people import (box_score_gap, gap_by_window,
                                      partial_games, report,
                                      score_disagreements, seasons,
                                      unrostered_players)
from src.ncaambb.store import (connect, load_season, upsert_box_scores,
                               upsert_player_team_seasons, upsert_players)
from test_ncaambb_store import box_line, box_score, game, player, team


@pytest.fixture
def db(tmp_path):
    connection = connect(tmp_path / 'ncaambb.db')
    yield connection
    connection.close()


def seeded(db, games, teams, boxes=(), people=(), memberships=()):
    load_season(db, 2026, games, teams)
    if people:
        upsert_players(db, people)
    if memberships:
        upsert_player_team_seasons(db, 2026, memberships)
    if boxes:
        upsert_box_scores(db, list(boxes))
    return db


def test_a_game_against_an_unlisted_opponent_is_a_provider_boundary(db):
    """The provider lists no roster for a team it does not carry, so it has no
    box score either. That is its boundary, not our loss."""
    seeded(db, [game(1, home=10, away=99)], [team(10)])
    missing, both_di = box_score_gap(db, 2026)
    assert len(missing) == 1 and both_di == []


def test_a_division_one_game_with_no_box_score_is_our_loss(db):
    seeded(db, [game(1, home=10, away=20)], [team(10), team(20, 'Beta')])
    missing, both_di = box_score_gap(db, 2026)
    assert len(missing) == 1 and len(both_di) == 1


def test_a_game_whose_box_score_is_stored_is_not_missing(db):
    seeded(db, [game(1, home=10, away=20)], [team(10), team(20, 'Beta')],
           boxes=[box_score(1, 10, [box_line(101, points=70)]),
                  box_score(1, 20, [box_line(101, points=60)], isHome=False)],
           people=[player(101)])
    assert box_score_gap(db, 2026) == ([], [])


def test_one_side_of_a_game_is_reported_as_partial(db):
    """A game with one team's box score is not covered and not missing; it is
    broken, and summing games would hide it."""
    seeded(db, [game(1, home=10, away=20)], [team(10), team(20, 'Beta')],
           boxes=[box_score(1, 10, [box_line(101)])], people=[player(101)])
    assert [row['sides'] for row in partial_games(db, 2026)] == [1]


def test_a_gap_spread_over_the_calendar_is_not_blamed_on_one_window(db):
    """Truncation loses a stretch of one window. The report therefore has to
    show WHICH window, so a thin spread cannot be read as a truncation."""
    teams = [team(10), team(20, 'Beta')]
    games = [game(1, day='2025-11-07T19:00:00Z'),
             game(2, day='2026-01-20T19:00:00Z'),
             game(3, day='2026-03-02T19:00:00Z')]
    seeded(db, games, teams)
    windows = gap_by_window(db, 2026)
    assert [total for *_, total in windows] == [1, 1, 1]
    assert len(windows) == 3, 'three separate windows, not one bad one'


def test_a_gap_concentrated_in_one_window_shows_as_one_window(db):
    teams = [team(10), team(20, 'Beta')]
    games = [game(n, day=f'2025-11-{n:02d}T19:00:00Z') for n in (5, 6, 7)]
    seeded(db, games, teams)
    assert [total for *_, total in gap_by_window(db, 2026)] == [3]


def test_an_unrostered_player_is_split_by_his_teams_division(db):
    """The provider's roster feed carries Division I teams. A player who only
    ever appeared for an unlisted opponent is expected to have no roster row;
    a Division I player without one is a roster gap worth knowing about."""
    seeded(db, [game(1, home=10, away=99)], [team(10)],
           people=[player(101), player(202, 'B Forward'), player(303, 'C Wing')],
           boxes=[box_score(1, 10, [box_line(101)]),
                  box_score(1, 99, [box_line(202), box_line(303)],
                            isHome=False)])
    total, counts = unrostered_players(db, 2026)
    assert total == 3
    # Deliberately lopsided: equal counts would survive the labels being
    # swapped, and this test would then pin nothing.
    assert counts == {'Division I': 1, 'non-Division I': 2}


def test_a_rostered_player_is_not_counted_as_unrostered(db):
    seeded(db, [game(1, home=10, away=20)], [team(10), team(20, 'Beta')],
           people=[player(101)], memberships=[(player(101), 10)],
           boxes=[box_score(1, 10, [box_line(101)])])
    assert unrostered_players(db, 2026) == (0, {})


def test_a_disagreement_names_the_game_and_both_numbers(db):
    """A count alone cannot be checked against anything. The report has to say
    which game and by how much, or nobody can tell a rounding from a rout."""
    seeded(db, [game(1, home_points=70, away_points=60)],
           [team(10), team(20, 'Beta')], people=[player(101)],
           boxes=[box_score(1, 10, [box_line(101, points=55)])])
    rows = score_disagreements(db, 2026)
    assert [(row['box'], row['final']) for row in rows] == [(55, 70)]


def test_a_box_score_with_no_measured_points_is_not_a_disagreement(db):
    seeded(db, [game(1)], [team(10), team(20, 'Beta')], people=[player(101)],
           boxes=[box_score(1, 10, [box_line(101, points=None)])])
    assert score_disagreements(db, 2026) == []


def test_coverage_counts_the_same_games_the_denominator_counts(db):
    """A box score for a cancelled game over a denominator of finals printed
    100.3% coverage, which is not a percentage of anything."""
    seeded(db, [game(1, status='final'),
                game(2, status='cancelled', home_points=None,
                     away_points=None, day='2025-11-08T19:00:00Z')],
           [team(10), team(20, 'Beta')], people=[player(101)],
           boxes=[box_score(1, 10, [box_line(101, points=70)]),
                  box_score(1, 20, [box_line(101, points=60)], isHome=False),
                  box_score(2, 10, [box_line(101, points=5)])])
    text = '\n'.join(report(db, 2026))
    assert '- finals: 1; with a box score: 1 (100.0%)' in text
    assert '- box scores for games that are not final: 1' in text


def test_the_report_runs_and_names_the_season(db):
    seeded(db, [game(1)], [team(10), team(20, 'Beta')])
    text = '\n'.join(report(db, 2026))
    assert '## Season 2026' in text
    assert seasons(db) == [2026]
