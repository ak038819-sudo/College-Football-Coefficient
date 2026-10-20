"""Cap-safe national schedule fetch.

A Division I season runs to several thousand games while `/games` returns at
most 3,000 records, so one request per season would truncate silently. The
season is therefore fetched in date windows, and any window that comes back at
the cap is split until it is not. A window that cannot be split further is a
hard failure, never a quiet partial answer.
"""

from datetime import date, datetime, timedelta, timezone

from .http import CbbdError, get_json

# `/games` returns at most 3,000 records. Reaching it means truncation.
GAMES_RECORD_CAP = 3000
# A season labelled by its ending year starts the previous calendar year. The
# window is deliberately wider than the playing season so an early exhibition
# or a late championship cannot fall outside it.
SEASON_START_MONTH = 7
SEASON_END_MONTH = 6
SEASON_END_DAY = 30


def season_window(ending_year):
    """The (first, last) UTC dates that can hold a game in this season."""
    if type(ending_year) is not int or not 1900 <= ending_year <= 2200:
        raise ValueError('Expected a basketball ending-year season')
    return (date(ending_year - 1, SEASON_START_MONTH, 1),
            date(ending_year, SEASON_END_MONTH, SEASON_END_DAY))


def month_windows(first, last):
    """Split a date span into calendar-month windows, inclusive at both ends."""
    if last < first:
        raise ValueError('Window ends before it starts')
    windows, start = [], first
    while start <= last:
        if start.month == 12:
            following = date(start.year + 1, 1, 1)
        else:
            following = date(start.year, start.month + 1, 1)
        windows.append((start, min(following - timedelta(days=1), last)))
        start = following
    return windows


def halve(first, last):
    """Split a window into two. Raises when it is already a single day."""
    span = (last - first).days
    if span < 1:
        raise ValueError('Cannot split a single-day window')
    middle = first + timedelta(days=span // 2)
    return (first, middle), (middle + timedelta(days=1), last)


def _stamp(day, end):
    """An ISO 8601 UTC instant covering the whole of `day`."""
    moment = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    if end:
        moment = moment.replace(hour=23, minute=59, second=59)
    return moment.isoformat().replace('+00:00', 'Z')


def game_day(game):
    """The UTC date a game starts on, or None when the provider omits it."""
    raw = game.get('startDate')
    if not isinstance(raw, str) or not raw:
        return None
    try:
        moment = datetime.fromisoformat(raw.replace('Z', '+00:00'))
    except ValueError:
        return None
    if moment.tzinfo is None:
        return None
    return moment.astimezone(timezone.utc).date()


def distinct_games(rows):
    """How many distinct games a response covers, for a per-game cap."""
    return len({row.get('gameId') for row in rows})


def fetch_windowed(path, season, first, last, *, cap, count=len, day_of=None,
                   unit='records', getter=None, **kwargs):
    """Fetch one date window from `path`, splitting it if it reaches `cap`.

    `count` measures the quantity the cap applies to: `/games` caps at 3,000
    RECORDS, while `/games/players` caps at 1,000 GAMES, which is a different
    limit on a different quantity. Returns (rows, windows_requested).

    Every row is checked to fall inside the window actually requested, so a
    provider that ignores the date filter is caught rather than trusted.
    """
    fetch = getter if getter is not None else get_json
    locate = day_of if day_of is not None else game_day
    rows = fetch(path, {'season': season,
                        'startDateRange': _stamp(first, False),
                        'endDateRange': _stamp(last, True)}, **kwargs)
    measured = count(rows)
    if measured >= cap:
        try:
            left, right = halve(first, last)
        except ValueError:
            raise CbbdError(
                f'{first} returned {measured} {unit} from {path}, at or above '
                f'the {cap} {unit} cap, and a single day cannot be split '
                'further; partition by conference instead') from None
        shared = dict(cap=cap, count=count, day_of=locate, unit=unit,
                      getter=fetch, **kwargs)
        first_rows, first_count = fetch_windowed(path, season, *left, **shared)
        second_rows, second_count = fetch_windowed(path, season, *right, **shared)
        return first_rows + second_rows, first_count + second_count
    outside = sum(1 for row in rows
                  if (day := locate(row)) is None or not first <= day <= last)
    if outside:
        raise CbbdError(f'{outside} rows from {path} fell outside the requested '
                        f'window {first}..{last}; the date filter cannot be '
                        'trusted')
    return rows, 1


def fetch_window(season, first, last, *, getter=None, **kwargs):
    """One date window of `/games`, under the 3,000-record cap."""
    return fetch_windowed('/games', season, first, last, cap=GAMES_RECORD_CAP,
                          unit='records', getter=getter, **kwargs)


def merge(rows):
    """Deduplicate games by id, rejecting two rows that disagree.

    Inclusive window boundaries mean one game can arrive twice. An identical
    repeat is the expected overlap; a conflicting repeat is a provider problem
    and must not be silently resolved by keeping whichever came last.
    """
    games, conflicts = {}, []
    for row in rows:
        identifier = row.get('id')
        if type(identifier) is not int:
            raise CbbdError('A game arrived without an integer id')
        seen = games.get(identifier)
        if seen is None:
            games[identifier] = row
        elif seen != row:
            conflicts.append(identifier)
    if conflicts:
        raise CbbdError(f'{len(conflicts)} games returned conflicting duplicates '
                        f'(for example {sorted(conflicts)[:3]})')
    return [games[key] for key in sorted(games)]


def fetch_season(season, *, getter=None, **kwargs):
    """Every game of one season, fetched cap-safely and deduplicated.

    Returns (games, stats). `stats` records how many windows were requested, so
    a caller can show that partitioning happened rather than assume it.
    """
    first, last = season_window(season)
    collected, windows = [], 0
    for window in month_windows(first, last):
        rows, count = fetch_window(season, *window, getter=getter, **kwargs)
        collected.extend(rows)
        windows += count
    games = merge(collected)
    stats = dict(season=season, games=len(games), rows_returned=len(collected),
                 windows_requested=windows,
                 duplicates=len(collected) - len(games))
    return games, stats
