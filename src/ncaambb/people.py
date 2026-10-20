"""Rosters and box scores: the people layer of national ingestion.

`/games/players` caps at 1,000 RECORDS, which is MEASURED, not assumed: the
15 days of 2025-11-13..27 hold 821 games and 1,642 team rows, and one request
for the whole window returns exactly 1,000 rows covering 501 games. The cap is
therefore on rows and it cuts the 501st game in half, which is why a one-sided
box score is evidence of truncation rather than of a half-played game.

An earlier version of this module capped on distinct GAMES at 1,000. That was
wrong twice: the wrong quantity, and a threshold no response ever reached, so
every window above about 500 games was silently truncated and 1,535 Division I
games of one season had no box score at all.

Windows are therefore small enough that a busy stretch stays under 1,000 rows
before any split is needed; the split path still covers a window busier than
that. A player is identified by the provider's athlete id, never by name. Two
people with one name stay two people, and one person who transfers stays one
person with two team-season rows.
"""

from datetime import timedelta

from .games import fetch_windowed, game_day, season_window
from .http import get_json

# `/games/players` returns at most 1,000 ROWS, one per team per game.
# Measured: see this module's docstring.
BOX_SCORE_RECORD_CAP = 1000
# The busiest single day of a season runs to about 200 rows, so five days stay
# under the cap; the split path still covers a window busier than that.
BOX_SCORE_WINDOW_DAYS = 5


def fetch_rosters(season, *, getter=None, **kwargs):
    """Every team's roster for a season. `team` is optional, so one request."""
    fetch = getter if getter is not None else get_json
    return fetch('/teams/roster', {'season': season}, **kwargs)


def box_score_windows(first, last, days=None):
    """Split a span into fixed-length windows, inclusive at both ends."""
    step = days if days is not None else BOX_SCORE_WINDOW_DAYS
    if step < 1:
        raise ValueError('A window must span at least one day')
    if last < first:
        raise ValueError('Window ends before it starts')
    windows, start = [], first
    while start <= last:
        end = min(start + timedelta(days=step - 1), last)
        windows.append((start, end))
        start = end + timedelta(days=1)
    return windows


def box_score_day(row):
    """A box score row's UTC date. Same field name as a game's."""
    return game_day(row)


def one_sided_games(rows):
    """Games with only one team's box score.

    The row cap cuts a game in half, so this is the direct evidence that a
    response was truncated and the split did not catch it. It is reported
    rather than raised, because the provider could also simply hold one side.
    """
    sides = {}
    for row in rows:
        sides.setdefault(row.get('gameId'), set()).add(row.get('teamId'))
    return sorted(game for game, teams in sides.items() if len(teams) != 2)


def fetch_box_scores(season, *, getter=None, window_days=None, **kwargs):
    """Every team-game box score of a season, under the 1,000-row cap.

    Returns (rows, windows_requested). Each row is one team's side of one game.
    """
    first, last = season_window(season)
    collected, windows = [], 0
    for start, end in box_score_windows(first, last, window_days):
        rows, count = fetch_windowed(
            '/games/players', season, start, end, cap=BOX_SCORE_RECORD_CAP,
            day_of=box_score_day, unit='records', getter=getter, **kwargs)
        collected.extend(rows)
        windows += count
    return merge_box_scores(collected), windows


def merge_box_scores(rows):
    """Deduplicate by (game, team), refusing two rows that disagree."""
    seen, conflicts = {}, []
    for row in rows:
        key = (row.get('gameId'), row.get('teamId'))
        if None in key:
            raise ValueError('A box score arrived without a game or team id')
        if key not in seen:
            seen[key] = row
        elif seen[key] != row:
            conflicts.append(key)
    if conflicts:
        raise ValueError(f'{len(conflicts)} box scores returned conflicting '
                         f'duplicates (for example {sorted(conflicts)[:3]})')
    return [seen[key] for key in sorted(seen)]


def roster_players(rosters):
    """Flatten rosters into (player, team_id) pairs, keeping ids distinct.

    A player appearing on two rosters in one season keeps both memberships;
    nothing is merged and nothing is equated by name.
    """
    pairs = []
    for roster in rosters:
        team = roster.get('teamId')
        if type(team) is not int:
            continue
        for player in roster.get('players') or []:
            if type(player.get('id')) is int:
                pairs.append((player, team))
    return pairs


def roster_teams(rosters):
    """Teams the roster feed names, as rows `upsert_teams` can store.

    The feed carries teams the season's `/teams` list does not, and a
    membership cannot be stored against a team that has no row. Registering
    them from the roster is what keeps the result independent of the order
    seasons happen to be loaded in: skipping them meant a later season's
    placeholder could make a membership appear on the second run.
    """
    teams = {}
    for roster in rosters:
        identifier = roster.get('teamId')
        if type(identifier) is not int or identifier in teams:
            continue
        name = roster.get('team')
        teams[identifier] = dict(id=identifier,
                                 school=name if isinstance(name, str) and name
                                 else f'Team {identifier}')
    return list(teams.values())


def box_score_people(boxes):
    """Players who appear in a box score, as identity rows.

    A player can record a stat line without being in the roster snapshot, so
    their identity comes from the box score rather than the line being dropped.
    """
    people = {}
    for box in boxes:
        for player in box.get('players') or []:
            identifier = player.get('athleteId')
            if type(identifier) is not int or identifier in people:
                continue
            people[identifier] = dict(id=identifier, name=player.get('name'),
                                      position=player.get('position'))
    return list(people.values())
