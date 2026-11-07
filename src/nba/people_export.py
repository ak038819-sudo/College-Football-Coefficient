"""The people layer as the page reads it: an index, and a file per player.

ONE FILE PER PLAYER, not a shard holding dozens of them. The archive has
2,635 players, so a file each is 2,635 files and a reader who opens a player
page fetches exactly his own — about 30 KB, a third of that over the wire.
The football archive sharded its 99,813 people because a file each was out of
the question; at this size sharding would mean fetching a megabyte to read
one career.

ONE CACHE TOKEN FOR THE WHOLE LAYER, for the reason the game layer has one
per season: the path is derived by the page from the player's own id, so the
manifest does not name 2,635 files. The index carries its own token, because
it is the one file a reader fetches before knowing which player he wants.

WHAT A PAGE MAY SAY, AND WHAT IT MAY NOT. Three things in the payload exist
so that a page can be honest about the archive rather than about the sport:

- `first` and `last` are the first and last season the ARCHIVE has him
  playing, which for a man who debuted in 1996 is 2002. The page says "in
  this archive" rather than implying a rookie year.
- `fg` on a season row is the feed's own count of games played, and `mf`
  whether it agrees with the number of lines stored. They disagree for 1,231
  of 14,500 regular-season rows, almost all of them before 2015, and almost
  always by one or two games. A page shows the archive's count and says the
  feed counts more, rather than quietly choosing one.
- `al` holds the other provider ids that belong to the same man. Six players
  have one. Nothing is summed across it: the page links the two careers and
  says the record is split.
"""

import json
from collections import defaultdict

# The season types a line can carry, in the order a page prints them. The
# payload holds the index, not the word, and the play-in is its own type
# because the archive's own schedule treats it as one.
SEASON_TYPES = ('regular', 'postseason', 'playin')

# The counted columns, in the order both the season rows and the game rows
# carry them. THIS TUPLE IS THE CONTRACT: the page reads them by position, so
# the order is not cosmetic and a column may not be inserted in the middle.
COUNTS = ('points', 'rebounds', 'offensive_rebounds', 'defensive_rebounds',
          'assists', 'steals', 'blocks', 'turnovers', 'fouls',
          'field_goals_made', 'field_goals_attempted', 'threes_made',
          'threes_attempted', 'free_throws_made', 'free_throws_attempted',
          'plus_minus')

PERSON_SQL = """
    SELECT person_key, athlete_id, full_name, display_name, short_name,
           position, position_short, jersey, height_inches, weight_pounds,
           date_of_birth, birth_city, birth_state, birth_country,
           draft_year, draft_round, draft_selection, experience_years,
           first_season, last_season
      FROM person
     ORDER BY athlete_id
"""
SEASON_SQL = """
    SELECT person_key, season, season_type, team_key, games, starts, minutes,
           points, rebounds, offensive_rebounds, defensive_rebounds, assists,
           steals, blocks, turnovers, fouls, field_goals_made,
           field_goals_attempted, threes_made, threes_attempted,
           free_throws_made, free_throws_attempted, plus_minus, feed_games,
           matches_feed
      FROM person_season
     ORDER BY person_key, season, season_type
"""
GAME_SQL = """
    SELECT g.person_key, g.game_key, g.season, g.season_type, g.game_date,
           g.team_key, g.opponent_key, g.home_away, g.started, g.minutes,
           g.points, g.rebounds, g.offensive_rebounds, g.defensive_rebounds,
           g.assists, g.steals, g.blocks, g.turnovers, g.fouls,
           g.field_goals_made, g.field_goals_attempted, g.threes_made,
           g.threes_attempted, g.free_throws_made, g.free_throws_attempted,
           g.plus_minus
      FROM person_game g
     WHERE g.played = 1
     ORDER BY g.person_key, g.season, g.game_date, g.game_key
"""
ALIAS_SQL = 'SELECT person_key, other_key FROM person_alias'


def has_people(connection):
    """Whether this database carries the people layer at all.

    An export taken before the loader has run must say the layer is absent
    rather than publish 2,635 empty files, which is the rule the game layer
    follows: a page learns the layer is missing from the manifest, not from
    an empty file it has already fetched.
    """
    return connection.execute(
        'SELECT 1 FROM person_game LIMIT 1').fetchone() is not None


def short_id(key):
    """`nba:espn:player:1966` as the integer 1966.

    The payload and the route carry the provider's id rather than the whole
    key, for the reason the game layer carries a short game id: 2,635 copies
    of `nba:espn:player:` in one index is 44 KB of prefix.
    """
    if key is None:
        return None
    try:
        return int(str(key).rsplit(':', 1)[-1])
    except ValueError:
        return None


def _number(value):
    """A JSON number, or None. Integers stay integers, so that a payload
    does not carry `30.0` where the box score said 30."""
    if value is None:
        return None
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _type_index(name):
    try:
        return SEASON_TYPES.index(name)
    except ValueError:
        return None


def bio(row):
    """A player's own details, with nothing absent invented.

    Keys are short because every one of them appears 2,635 times across the
    index and the detail files: `n` name, `p` position, `j` jersey, `h`
    height in inches, `w` pounds, `b` date of birth, `c` birthplace, `d`
    draft, `x` seasons of experience.
    """
    place = [row['birth_city'], row['birth_state'], row['birth_country']]
    draft = [row['draft_year'], row['draft_round'], row['draft_selection']]
    entry = dict(i=row['athlete_id'],
                 n=row['full_name'],
                 s=row['short_name'] or row['display_name'],
                 p=row['position'],
                 pa=row['position_short'],
                 j=row['jersey'],
                 h=row['height_inches'],
                 w=row['weight_pounds'],
                 b=row['date_of_birth'],
                 x=row['experience_years'],
                 first=row['first_season'],
                 last=row['last_season'])
    if any(part for part in place):
        entry['c'] = [part or None for part in place]
    if row['draft_year'] is not None:
        entry['d'] = draft
    return {key: value for key, value in entry.items() if value is not None}


def season_row(row):
    """One season with one club: `[season, type, team, games, starts, min,
    ...counts, feed games, matches]`."""
    return ([row['season'], _type_index(row['season_type']),
             short_id(row['team_key']), row['games'], row['starts'],
             _number(row['minutes'])]
            + [_number(row[name]) for name in COUNTS]
            + [row['feed_games'], row['matches_feed']])


def game_row(row):
    """One game: `[game, date, team, opponent, home, started, min,
    ...counts]`.

    The date is the whole date rather than a day within the season, because a
    season straddles two calendar years and a page that prints "01-20" has to
    know which of them it is in to say so.
    """
    return ([short_id(row['game_key']), row['game_date'],
             short_id(row['team_key']), short_id(row['opponent_key']),
             1 if row['home_away'] == 'home' else 0,
             row['started'], _number(row['minutes'])]
            + [_number(row[name]) for name in COUNTS])


def build(connection):
    """The whole people layer as `(index, {athlete id: detail})`, or None.

    One pass over each table rather than a query per player: 2,635 queries
    for the seasons and 2,635 more for the games is the shape that makes an
    export take minutes instead of seconds.
    """
    if not has_people(connection):
        return None
    people = {row['person_key']: row for row in
              connection.execute(PERSON_SQL)}
    aliases = defaultdict(list)
    for row in connection.execute(ALIAS_SQL):
        other = short_id(row['other_key'])
        if other is not None:
            aliases[row['person_key']].append(other)
    seasons = defaultdict(list)
    for row in connection.execute(SEASON_SQL):
        if row['person_key'] in people:
            seasons[row['person_key']].append(season_row(row))
    games = defaultdict(lambda: defaultdict(list))
    for row in connection.execute(GAME_SQL):
        if row['person_key'] not in people:
            continue
        key = (row['season'], _type_index(row['season_type']))
        games[row['person_key']][key].append(game_row(row))
    index, detail = [], {}
    for key, row in people.items():
        athlete_id = row['athlete_id']
        own = bio(row)
        played = sum(entry[3] for entry in seasons[key]
                     if entry[1] == 0)
        points = sum(entry[6] or 0 for entry in seasons[key]
                     if entry[1] == 0)
        # THE INDEX IS A SEARCH LIST, not a statistics table: name, position,
        # the span the archive has and enough to sort by. Everything else is
        # in the player's own file, which is fetched when he is opened.
        entry = [athlete_id, own['n'], own.get('pa'), row['first_season'],
                 row['last_season'], played, points]
        index.append(entry)
        payload = dict(b=own, s=seasons[key])
        log = [[season, season_type, rows]
               for (season, season_type), rows
               in sorted(games[key].items())]
        if log:
            payload['g'] = log
        if aliases[key]:
            payload['al'] = sorted(aliases[key])
        detail[athlete_id] = payload
    index.sort(key=lambda entry: (entry[1] or '', entry[0]))
    return dict(count=len(index), types=list(SEASON_TYPES),
                players=index), detail


def coverage(connection):
    """What the layer carries, for the export's own summary line."""
    row = connection.execute(
        'SELECT (SELECT COUNT(*) FROM person) AS people,'
        '       (SELECT COUNT(*) FROM person_game WHERE played = 1) AS games,'
        '       (SELECT COUNT(*) FROM person_season) AS seasons,'
        '       (SELECT COUNT(*) FROM person_alias) AS aliases,'
        '       (SELECT COUNT(*) FROM person_season WHERE matches_feed = 0)'
        '           AS mismatched').fetchone()
    return dict(row) if row else {}


def body(payload):
    """One player's file, as the page's own global expects it."""
    return json.dumps(payload, separators=(',', ':'), sort_keys=True)
