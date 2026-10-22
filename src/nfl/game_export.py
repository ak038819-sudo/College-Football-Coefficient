"""The per-game layer of the export: scoring summaries, drives, win probability.

ONE FILE A WEEK, `games/<season>/<week>.js`, fetched lazily when a reader
opens a game. A season in one file measured 0.75 MB — 190 KB gzipped — to
read one game of it, and a refresh in October would rewrite the whole thing
every morning. A week is about sixteen games, 48 KB, 13 KB gzipped, and it is
the unit a reader is already looking at: the page shows a week of cards, so
every game they can click is in the file they just fetched.

The page builds that path itself from the game's own week, which the season
payload already carries, so no index is downloaded to find a game. The
manifest states ONE cache token per season rather than one per file, because
560 hashes would be a manifest heavier than the data.

WHAT IS NOT HERE IS THE POINT. The source for all of this is 2.4 GB of
play-by-play across 28 seasons, and not one play is stored or published. A
game page shows the scoring summary, the shape of each drive and how the win
probability moved, and those come from about 36 rows a game.

THE WIN PROBABILITY IS COMPUTED HERE, NOT IN THE BROWSER. Every NFL game in
this archive is finished, so there is nothing for a page to recompute as a
score changes, and one implementation of a fitted curve is better than two.
The college side computes its live number in the browser because it has a
live feed; when the NFL gets one, this is the number the feed would replace,
not a second model.

RECONCILIATION IS STATED, NOT ASSUMED. 18 of 7,366 games have a play-by-play
scoring log that does not add up to the final the archive states — sixteen of
them Jacksonville home games of 2001 and 2002, where the source's own running
score is wrong. Those games carry `ok: 0` and the page says the summary is
incomplete rather than drawing a scoreboard that contradicts the score above
it.
"""

from collections import defaultdict

from .win_probability import (REGULATION_SECONDS, home_win_probability,
                              load_params)

# The order a page prints them in, and the index the packed rows carry.
KINDS = ('TD', 'FG', 'XP', '2PT', 'SAFETY', 'D2PT')

DRIVE_SQL = """
    SELECT d.game_key, d.drive, d.team_key, d.quarter_start, d.seconds_left,
           d.start_yard_line, d.plays, d.yards, d.time_of_possession,
           d.result, d.home_score, d.away_score
      FROM game_drive d JOIN game g ON g.game_key = d.game_key
     WHERE g.season = ? ORDER BY d.game_key, d.drive
"""
SCORING_SQL = """
    SELECT s.game_key, s.team_key, s.quarter, s.clock, s.kind, s.points,
           s.scorer, s.kick_distance, s.description, s.home_score,
           s.away_score
      FROM scoring_play s JOIN game g ON g.game_key = s.game_key
     WHERE g.season = ? ORDER BY s.game_key, s.play_id
"""
GAME_SQL = """
    SELECT g.game_key, g.home_team_key, g.away_team_key, g.home_points,
           g.away_points, g.week, g.postseason, r.home_expected
      FROM game g LEFT JOIN rating_game r ON r.game_key = g.game_key
     WHERE g.season = ?
"""

# The postseason is one file however many rounds it ran to, because its weeks
# are small and a reader who opens the Super Bowl has usually just looked at
# the divisional round.
POSTSEASON = 'post'


def week_order(weeks):
    """Week keys in the order a season is played: 1, 2, ... then `post`.

    The order is load-bearing, not cosmetic: the season's cache token is the
    hash of its week files concatenated in this order, and the gate recomputes
    it. A set's iteration order would make the token depend on how the
    dictionary happened to be built.
    """
    return sorted(weeks, key=lambda week: (week == POSTSEASON,
                                           int(week) if week.isdigit() else 0))


def week_key(game):
    """The file a game's detail lives in. The page derives the same string.

    A regular-season game is keyed by its week number and everything after it
    by `post`. A game with no week at all — the archive has none, but a feed
    can always surprise — is keyed `post` rather than dropped, so it is
    somewhere a reader can reach instead of nowhere.
    """
    if game.get('postseason') or game.get('week') in (None, ''):
        return POSTSEASON
    return str(int(game['week']))


def has_plays(connection):
    """Whether this database carries the play-by-play layer at all.

    An export built before the reduction has run must say the layer is
    absent rather than publish empty game files, exactly as the people layer
    does: a page can then say the export predates it.
    """
    return connection.execute(
        'SELECT 1 FROM game_drive LIMIT 1').fetchone() is not None


def _side(team_key, home, away):
    """1 for the home team, 0 for the away team, None for neither.

    None is not an error to swallow: 93 drives in the archive have no
    possessing team, and a drive drawn on the wrong side of a game page is
    worse than a drive drawn on no side.
    """
    if team_key == home:
        return 1
    if team_key == away:
        return 0
    return None


def _per_mille(probability):
    """A probability as an integer 0-1000.

    Three digits is the precision a page can draw and a tenth of a point is
    already below what the model can claim. Floats would be 17 characters
    each across 170,000 drives.
    """
    return int(round(probability * 1000))


def _intern(vocabulary, value):
    if value is None:
        return None
    index = vocabulary.get(value)
    if index is None:
        index = vocabulary[value] = len(vocabulary)
    return index


def build(connection, season, *, params=None):
    """A season's game detail as {week key: payload}, or None where it has none."""
    if params is None:
        params = load_params()
    games = {}
    for row in connection.execute(GAME_SQL, (season,)):
        games[row['game_key']] = dict(
            home=row['home_team_key'], away=row['away_team_key'],
            home_points=row['home_points'], away_points=row['away_points'],
            week=row['week'], postseason=row['postseason'],
            expected=row['home_expected'])
    results = {}
    drives = defaultdict(list)
    for row in connection.execute(DRIVE_SQL, (season,)):
        game = games.get(row['game_key'])
        if game is None:
            continue
        secs = row['seconds_left']
        home_score, away_score = row['home_score'], row['away_score']
        probability = None
        if (secs is not None and home_score is not None
                and away_score is not None and game['expected'] is not None):
            probability = _per_mille(home_win_probability(
                _elo_diff(game['expected']), home_score - away_score, secs,
                params))
        drives[row['game_key']].append([
            row['drive'], _side(row['team_key'], game['home'], game['away']),
            row['quarter_start'], secs, row['start_yard_line'], row['plays'],
            row['yards'], row['time_of_possession'],
            _intern(results, row['result']), home_score, away_score,
            probability])
    scoring = defaultdict(list)
    points = defaultdict(lambda: [0, 0])
    for row in connection.execute(SCORING_SQL, (season,)):
        game = games.get(row['game_key'])
        if game is None:
            continue
        side = _side(row['team_key'], game['home'], game['away'])
        if side is not None:
            points[row['game_key']][side] += row['points']
        scoring[row['game_key']].append([
            side, row['quarter'], row['clock'],
            KINDS.index(row['kind']) if row['kind'] in KINDS else None,
            row['points'], row['scorer'], row['kick_distance'],
            row['home_score'], row['away_score'], row['description']])
    weeks = defaultdict(dict)
    for key, game in games.items():
        if key not in drives and key not in scoring:
            continue
        entry = {}
        if game['expected'] is not None:
            entry['wp'] = _per_mille(home_win_probability(
                _elo_diff(game['expected']), 0, REGULATION_SECONDS, params))
        if key in drives:
            entry['d'] = drives[key]
        if key in scoring:
            entry['s'] = scoring[key]
            entry['ok'] = 1 if _reconciles(game, points[key]) else 0
        weeks[week_key(game)][key] = entry
    if not weeks:
        return None
    vocabulary = [None] * len(results)
    for value, index in results.items():
        vocabulary[index] = value
    return {week: dict(season=season, week=week, kinds=list(KINDS),
                       results=vocabulary, games=entries)
            for week, entries in weeks.items()}


def _elo_diff(expected, scale=400.0):
    """The venue-adjusted rating gap, read back off the stored expectation.

    The same inversion the fit uses, and for the same reason: the rating
    engine already applied the home-field term, and re-deriving it here
    would be a second implementation of the venue rule.
    """
    import math
    if not 0.0 < expected < 1.0:
        return 0.0
    return -scale * math.log10(1.0 / expected - 1.0)


def _reconciles(game, totals):
    """Do the scoring plays add up to the final the archive states?"""
    if game['home_points'] is None or game['away_points'] is None:
        return False
    return (totals[1] == game['home_points']
            and totals[0] == game['away_points'])


def coverage(connection):
    """How much of the archive the play-by-play layer reaches, for the docs."""
    played = connection.execute(
        'SELECT COUNT(*) FROM game WHERE home_points IS NOT NULL'
    ).fetchone()[0]
    with_drives = connection.execute(
        'SELECT COUNT(DISTINCT game_key) FROM game_drive').fetchone()[0]
    return dict(played=played, with_drives=with_drives,
                drives=connection.execute(
                    'SELECT COUNT(*) FROM game_drive').fetchone()[0],
                scoring=connection.execute(
                    'SELECT COUNT(*) FROM scoring_play').fetchone()[0])
