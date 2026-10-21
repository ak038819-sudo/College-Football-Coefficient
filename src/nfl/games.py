"""Reading the NFL archive into rows this repository is willing to store.

Every row is checked before it is kept. A game whose score contradicts the
source's own `result` column, whose team code is unknown, whose date is
missing, or which lists one team twice is REFUSED, not repaired — a silently
mended row is a wrong row that no longer looks wrong.

Two NFL facts drive most of what is unusual here:

**A tie is a real result.** The archive holds 16 of them since 1999. The
basketball loader excludes a level final from rating, because a tied college
basketball game means the data is wrong. An NFL tie means the teams tied, and
a rating that ignores it throws away the most informative kind of game there
is. Ties are rateable, scored as half a win.

**A team code is not a team.** `STL` and `LA` are one franchise either side of
a move. Every row therefore carries both the code the source used and the
franchise key the rating follows. See `identity.RELOCATIONS`.
"""

import csv
import io
from datetime import datetime, time as clock_time
from zoneinfo import ZoneInfo

from .identity import TEAM_CODES, entity_key, franchise_key, season_label

# Refuse the file outright if any of these is gone: a missing column would
# otherwise read as a column full of blanks, which is a silent half-import.
REQUIRED_COLUMNS = ('game_id', 'season', 'game_type', 'week', 'gameday',
                    'gametime', 'away_team', 'away_score', 'home_team',
                    'home_score', 'location', 'result', 'overtime')

SEASON_TYPES = {
    'REG': 'regular',
    'WC': 'wildcard',
    'DIV': 'divisional',
    'CON': 'conference',
    'SB': 'superbowl',
}
POSTSEASON = frozenset({'wildcard', 'divisional', 'conference', 'superbowl'})

# Kickoff times in this archive are US Eastern, including for games played in
# London and Munich. Storing the local date alongside is what lets a page show
# "Sunday" correctly without this assumption having to be right.
SOURCE_TIMEZONE = ZoneInfo('America/New_York')


class ArchiveError(ValueError):
    """The archive is not shaped the way this loader requires."""


def _int(value):
    if value is None or value == '':
        return None
    return int(value)


def kickoff_utc(gameday, gametime):
    """ISO-8601 UTC kickoff, or None when the source gives no time.

    A missing kickoff time stays missing. Defaulting it to midnight would put
    the game on the wrong day in half the world's timezones and would sort it
    before every real kickoff, which is exactly the trap the basketball
    schedule's 44% of unscheduled tip-offs set.
    """
    if not gametime:
        return None
    hour, _, minute = gametime.partition(':')
    local = datetime.combine(
        datetime.strptime(gameday, '%Y-%m-%d').date(),
        clock_time(int(hour), int(minute or 0)), SOURCE_TIMEZONE)
    return local.astimezone(ZoneInfo('UTC')).isoformat().replace('+00:00', 'Z')


def _classify(row, home, away, home_score, away_score):
    """Whether this game may move a rating, and every reason it may not."""
    exclusions = []
    if home_score is None or away_score is None:
        exclusions.append('no_final_score')
    if home == away:
        exclusions.append('self_match')
    return (not exclusions), exclusions


def parse_row(row):
    """One archive row as a stored game. Raises on anything contradictory."""
    game_id = (row.get('game_id') or '').strip()
    if not game_id:
        raise ArchiveError('A game row has no game_id')
    kind = (row.get('game_type') or '').strip()
    if kind not in SEASON_TYPES:
        raise ArchiveError(f'{game_id}: unknown game_type {kind!r}')
    home = (row.get('home_team') or '').strip()
    away = (row.get('away_team') or '').strip()
    for code in (home, away):
        if code not in TEAM_CODES:
            raise ArchiveError(f'{game_id}: unknown team code {code!r}')
    if home == away:
        raise ArchiveError(f'{game_id}: a team cannot play itself')
    gameday = (row.get('gameday') or '').strip()
    if not gameday:
        raise ArchiveError(f'{game_id}: no date')
    try:
        datetime.strptime(gameday, '%Y-%m-%d')
    except ValueError as error:
        raise ArchiveError(f'{game_id}: unreadable date {gameday!r}') from error
    season = _int(row.get('season'))
    if season is None:
        raise ArchiveError(f'{game_id}: no season')
    home_score = _int(row.get('home_score'))
    away_score = _int(row.get('away_score'))
    if (home_score is None) != (away_score is None):
        raise ArchiveError(f'{game_id}: one score present and one missing')
    # The archive states the margin separately. Where both exist they must
    # agree; a disagreement means one of the three numbers is wrong and this
    # loader cannot know which.
    stated = _int(row.get('result'))
    if home_score is not None and stated is not None \
            and stated != home_score - away_score:
        raise ArchiveError(
            f'{game_id}: result {stated} contradicts {home_score}-{away_score}')
    location = (row.get('location') or '').strip()
    if location not in ('Home', 'Neutral'):
        raise ArchiveError(f'{game_id}: unknown location {location!r}')
    gametime = (row.get('gametime') or '').strip()
    rateable, exclusions = _classify(row, home, away, home_score, away_score)
    season_type = SEASON_TYPES[kind]
    return dict(
        game_key=entity_key('game', game_id),
        provider_id=game_id,
        season=season,
        season_label=season_label(season),
        season_type=season_type,
        postseason=1 if season_type in POSTSEASON else 0,
        week=_int(row.get('week')),
        game_date=gameday,
        kickoff_utc=kickoff_utc(gameday, gametime),
        kickoff_tbd=0 if gametime else 1,
        neutral_site=1 if location == 'Neutral' else 0,
        home_code=home,
        away_code=away,
        home_team_key=franchise_key(home),
        away_team_key=franchise_key(away),
        home_points=home_score,
        away_points=away_score,
        overtime=_int(row.get('overtime')),
        divisional_game=_int(row.get('div_game')),
        roof=(row.get('roof') or '').strip() or None,
        surface=(row.get('surface') or '').strip() or None,
        stadium_id=(row.get('stadium_id') or '').strip() or None,
        stadium=(row.get('stadium') or '').strip() or None,
        rateable=1 if rateable else 0,
        exclusions=exclusions,
    )


def parse(text):
    """Every game in the archive, in file order, validated.

    Ordering for a replay is by kickoff, not by week: the archive's own week
    numbers restart in the postseason, and the lesson that cost 1,353 football
    season rating changes was exactly this — order by date, never by week.
    """
    reader = csv.DictReader(io.StringIO(text))
    missing = [name for name in REQUIRED_COLUMNS
               if name not in (reader.fieldnames or [])]
    if missing:
        raise ArchiveError('Archive is missing columns: ' + ', '.join(missing))
    games, seen = [], set()
    for row in reader:
        game = parse_row(row)
        if game['provider_id'] in seen:
            raise ArchiveError(f'{game["provider_id"]}: appears twice')
        seen.add(game['provider_id'])
        games.append(game)
    if not games:
        raise ArchiveError('Archive holds no games')
    return games


def sort_key(game):
    """Kickoff order, with a stable tiebreak for games sharing a date."""
    return (game['game_date'], game['kickoff_utc'] or '', game['provider_id'])


def in_order(games):
    return sorted(games, key=sort_key)


def coverage(games):
    """Per-season counts, for the probe and for the export's coverage rows."""
    seasons = {}
    for game in games:
        row = seasons.setdefault(game['season'], dict(
            season=game['season'], games=0, finals=0, rateable=0, ties=0,
            neutral=0, postseason=0, teams=set(), no_kickoff_time=0))
        row['games'] += 1
        row['teams'].add(game['home_code'])
        row['teams'].add(game['away_code'])
        if game['home_points'] is not None:
            row['finals'] += 1
            if game['home_points'] == game['away_points']:
                row['ties'] += 1
        row['rateable'] += game['rateable']
        row['neutral'] += game['neutral_site']
        row['postseason'] += game['postseason']
        row['no_kickoff_time'] += game['kickoff_tbd']
    for row in seasons.values():
        row['teams'] = len(row['teams'])
    return [seasons[key] for key in sorted(seasons)]
