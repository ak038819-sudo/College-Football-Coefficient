"""Reading the NBA schedule files into rows this repository is willing to store.

Every row is checked before it is kept. A game whose teams are unknown, whose
date is missing, or which lists one team twice is REFUSED, not repaired — a
silently mended row is a wrong row that no longer looks wrong.

Three things about this source drive most of what is unusual here.

**AN UNPLAYED GAME IS WRITTEN 0-0, NOT NULL.** The source fills both score
columns with zero for every scheduled game that has not been played — 1,108
of them in the season in progress. Read naively, next April's fixtures are
1,108 draws, and a rating replay would take every one of them as evidence.
`status_type_completed` is the only field that tells the truth, so a score is
read only when it is set, and the three other leagues' habit of checking
`is not None` is not enough here.

**ZERO ATTENDANCE IS AN ABSENCE, NOT AN EMPTY ARENA.** It appears in 174
games of 2011, which had no empty arenas, as well as in the seasons that did.
There is no way to tell a missing reading from a real zero, so all of them are
stored as NULL rather than asserting a crowd of nobody.

**AN ALL-STAR GAME LOOKS LIKE A REGULAR-SEASON GAME.** It carries
`season_type = 2`. What marks it is that its sides are not franchises. Those
games are skipped and counted, never stored: see `identity.EXHIBITION_IDS`.
"""

import json
from datetime import date as date_type, datetime

from .identity import (EXHIBITION_IDS, AFTER_REGULAR_SEASON, FRANCHISE_IDS,
                       game_key, is_franchise, season_label, season_type_of,
                       team_key)

# Refuse the file outright if any of these is gone: a missing column would
# otherwise read as a column full of blanks, which is a silent half-import.
REQUIRED_COLUMNS = (
    'id', 'season', 'season_type', 'date', 'game_date', 'neutral_site',
    'status_type_completed', 'home_id', 'home_score', 'away_id',
    'away_score', 'venue_id', 'venue_full_name', 'venue_address_city',
    'venue_address_state', 'attendance', 'notes_headline', 'time_valid',
)


class ArchiveError(ValueError):
    """A schedule file is not shaped the way this loader requires."""


def _clean(value):
    """A string with whitespace collapsed, or None.

    The collapse is not cosmetic: the 2006 and 2007 files spell the Hornets'
    location with a literal newline inside it ('NO/Oklahoma City\\n Hornets'),
    and that newline would reach a page.
    """
    if value is None:
        return None
    text = ' '.join(str(value).split())
    return text or None


def _int(value):
    if value is None or value == '':
        return None
    if isinstance(value, bool):
        raise ArchiveError('A boolean is not a number here')
    return int(value)


def _date(value):
    """The game's local date as YYYY-MM-DD."""
    if isinstance(value, date_type) and not isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    text = (str(value) if value is not None else '').strip()
    if not text:
        return None
    try:
        return datetime.strptime(text[:10], '%Y-%m-%d').date().isoformat()
    except ValueError:
        return None


def _tipoff(value):
    """The tip-off as an ISO-8601 UTC string, or None.

    The source's `date` column is already UTC ('2027-04-12T00:30Z'). It is
    normalised to a full timestamp rather than stored as given, so that two
    files written in different years sort against each other.
    """
    text = _clean(value)
    if not text:
        return None
    try:
        moment = datetime.fromisoformat(text.replace('Z', '+00:00'))
    except ValueError:
        return None
    return moment.isoformat().replace('+00:00', 'Z')


def is_exhibition(row):
    """Whether this row is an All-Star or placeholder fixture.

    True when EITHER side is not a franchise. Both sides are checked rather
    than one, because a row with one real team and one unknown id is not an
    exhibition — it is a fault, and `parse_row` should refuse it.
    """
    sides = (row.get('home_id'), row.get('away_id'))
    return all(_int(side) in EXHIBITION_IDS for side in sides)


def parse_row(row):
    """One schedule row as a stored game. Raises on anything contradictory."""
    provider_id = _clean(row.get('id'))
    if not provider_id:
        raise ArchiveError('A schedule row has no id')
    home_id, away_id = _int(row.get('home_id')), _int(row.get('away_id'))
    for side in (home_id, away_id):
        if side not in FRANCHISE_IDS:
            raise ArchiveError(f'{provider_id}: {side} is not a franchise')
    if home_id == away_id:
        raise ArchiveError(f'{provider_id}: a team cannot play itself')
    season = _int(row.get('season'))
    if season is None:
        raise ArchiveError(f'{provider_id}: no season')
    game_date = _date(row.get('game_date')) or _date(row.get('date'))
    if not game_date:
        raise ArchiveError(f'{provider_id}: no date')
    # The note matters as well as the type: the NBA Cup final is sent as
    # a regular-season game and does not count in the standings.
    kind = season_type_of(_int(row.get('season_type')),
                          row.get('notes_headline'))
    completed = bool(row.get('status_type_completed'))
    # THE 0-0 TRAP. Both score columns are filled with zero for a game that
    # has not been played, so the completion flag decides whether there is a
    # score at all. Reading the columns first and checking afterwards is how
    # a fixture becomes a draw.
    home_points = _int(row.get('home_score')) if completed else None
    away_points = _int(row.get('away_score')) if completed else None
    if completed and (home_points is None or away_points is None):
        raise ArchiveError(f'{provider_id}: completed with no score')
    if completed and home_points == away_points:
        # The NBA plays overtime until somebody wins. A level final score on
        # a completed game is a fault in the data, not a draw, and rating it
        # as half a win would be inventing a result.
        raise ArchiveError(f'{provider_id}: completed {home_points}-'
                           f'{away_points}, and an NBA game cannot be drawn')
    attendance = _int(row.get('attendance'))
    rateable, exclusions = _classify(completed, home_points, away_points)
    return dict(
        game_key=game_key(provider_id),
        provider_id=provider_id,
        season=season,
        season_label=season_label(season),
        season_type=kind,
        postseason=1 if kind in AFTER_REGULAR_SEASON else 0,
        game_date=game_date,
        tipoff_utc=_tipoff(row.get('date')) if row.get('time_valid') else None,
        tipoff_tbd=0 if row.get('time_valid') else 1,
        neutral_site=1 if row.get('neutral_site') else 0,
        completed=1 if completed else 0,
        home_team_key=team_key(home_id),
        away_team_key=team_key(away_id),
        home_team_id=home_id,
        away_team_id=away_id,
        home_points=home_points,
        away_points=away_points,
        venue_id=_clean(row.get('venue_id')),
        venue=_clean(row.get('venue_full_name')),
        venue_city=_clean(row.get('venue_address_city')),
        venue_state=_clean(row.get('venue_address_state')),
        # Zero is an absence here, never an empty arena. See the module note.
        attendance=attendance if attendance else None,
        notes=_clean(row.get('notes_headline')),
        rateable=1 if rateable else 0,
        exclusions=exclusions,
    )


def _classify(completed, home_points, away_points):
    """Whether this game may move a rating, and every reason it may not."""
    exclusions = []
    if not completed:
        exclusions.append('not_played')
    elif home_points is None or away_points is None:
        exclusions.append('no_final_score')
    return (not exclusions), exclusions


def parse(rows):
    """Every franchise game in a schedule file, validated.

    Exhibitions are dropped rather than stored, and the count comes back
    beside the games so a caller can report what it skipped instead of a
    number quietly going missing.
    """
    if not isinstance(rows, list):
        raise ArchiveError('Expected a list of schedule rows')
    if rows:
        missing = [name for name in REQUIRED_COLUMNS if name not in rows[0]]
        if missing:
            raise ArchiveError('Schedule is missing columns: '
                               + ', '.join(missing))
    games, seen, skipped = [], set(), 0
    for row in rows:
        if is_exhibition(row):
            skipped += 1
            continue
        game = parse_row(row)
        if game['provider_id'] in seen:
            raise ArchiveError(f'{game["provider_id"]}: appears twice')
        seen.add(game['provider_id'])
        games.append(game)
    return games, skipped


def sort_key(game):
    """Tip-off order, with a stable tiebreak for games sharing a date.

    By DATE, never by any week or round number. Ordering a season by round
    put a postseason game first in the football archive and zeroed 1,353
    season rating changes; the same shape of mistake is available here,
    because the play-in is played between the regular season and the
    playoffs and sorts before both under any naive ordering of its name.
    """
    return (game['game_date'], game['tipoff_utc'] or '', game['provider_id'])


def in_order(games):
    return sorted(games, key=sort_key)


def coverage(games):
    """Per-season counts, for the ingest report and the export's coverage."""
    seasons = {}
    for game in games:
        row = seasons.setdefault(game['season'], dict(
            season=game['season'], label=game['season_label'], games=0,
            played=0, rateable=0, postseason=0, playin=0, cup_final=0,
            neutral=0, no_tipoff_time=0, teams=set()))
        row['games'] += 1
        row['teams'].add(game['home_team_id'])
        row['teams'].add(game['away_team_id'])
        row['played'] += game['completed']
        row['rateable'] += game['rateable']
        row['neutral'] += game['neutral_site']
        row['no_tipoff_time'] += game['tipoff_tbd']
        if game['season_type'] == 'postseason':
            row['postseason'] += 1
        elif game['season_type'] == 'playin':
            row['playin'] += 1
        elif game['season_type'] == 'cup_final':
            row['cup_final'] += 1
    for row in seasons.values():
        row['teams'] = len(row['teams'])
    return [seasons[key] for key in sorted(seasons)]


def exclusions_json(game):
    return json.dumps(game['exclusions'])


def teams_from_schedule(rows, season):
    """The per-season name of every franchise in one schedule file.

    The names come from the schedule rather than from a team file because
    this source publishes no team file. A franchise appears on both sides of
    the season, so either side names it; the home side is read first and the
    away side fills in anyone who never hosted.
    """
    found = {}
    for row in rows:
        if is_exhibition(row):
            continue
        for side in ('home', 'away'):
            team_id = _int(row.get(f'{side}_id'))
            if not is_franchise(team_id) or team_id in found:
                continue
            found[team_id] = dict(
                team_key=team_key(team_id),
                team_id=team_id,
                season=int(season),
                abbreviation=_clean(row.get(f'{side}_abbreviation')),
                location=_clean(row.get(f'{side}_location')),
                nickname=_clean(row.get(f'{side}_name')),
                name=_clean(row.get(f'{side}_display_name')),
                logo=_clean(row.get(f'{side}_logo')),
                logo_dark=_clean(row.get(f'{side}_logo_dark')),
            )
    return [found[key] for key in sorted(found)]
