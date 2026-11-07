"""Load the NBA people layer: who played, and what he did in each game.

`python -m src.nba.people` fetches a season's player core, box scores and
season statistics, reduces them, stores them and DELETES the files before
fetching the next season, the same stream-and-discard pattern
`src/nba/plays.py` uses on the play-by-play. 851,463 box-score lines across
26 seasons are 17 MB of Parquet; keeping them all on disk at once buys
nothing, because each season is reduced on its own.

WHAT THE ARCHIVE IS. 2,635 players hold at least one line, 2002 to the season
in progress. 588 of them appear in a single season and the longest career in
the archive runs 24 of them. That is small enough that a player page can
carry every game he played rather than a sample, which is the whole reason
this layer is shaped the way it is.

FIVE TRAPS, each measured before it was written down:

1. THE ALL-STAR GAME IS FILED AS A REGULAR-SEASON GAME. LeBron James has 83
   rows for 2017-18 under `season_type` 2, and the 83rd is for "Team LeBron".
   Summing them makes a man play 83 regular-season games. Every row whose
   team is not one of the thirty franchises is dropped, which is the rule
   `identity.is_franchise` already applies to the schedule; it is why the
   derived totals agree with the feed's own and the naive sum does not.

2. MINUTES, NOT `did_not_play`, SAY WHETHER A MAN PLAYED. The flag is true
   for 4,800-6,800 rows in most seasons and for 299 in 2009-10 — while
   10,767 rows that season carry no minutes at all, with `did_not_play`
   false and `active` false. A line that carries minutes is a line for a game
   he played, in every one of the 26 seasons.

3. THE FEED'S OWN SEASON STATISTICS ARE REGULAR SEASON ONLY. Compared across
   every season type they look wildly wrong; compared against the regular
   season alone they agree on points for 440 of 440 players in 2002 and 439
   of 441 in 2010. So the rollups here keep the season type, and the feed's
   numbers are used as a CHECK rather than as the source.

4. THE FEED'S COUNT OF GAMES PLAYED DISAGREES, IN BOTH DIRECTIONS. For
   1,006 of the 12,819 regular seasons it carries a number for it does not
   match the lines stored: 966 where it counts more — 43 lines with minutes
   against 54 games played for one 2009-10 Clipper — and 40 where it counts
   fewer, which is not the same defect and is not corrected either. 876 of
   the 1,006 are before 2015 and 870 of them are by one or two games. The
   count of lines is what is stored, the feed's count is kept beside it in
   `feed_games`, and `matches_feed` states whether the two agree. Nothing is
   reconciled behind the reader's back.

5. THE BOX FEED CALLS THE NBA CUP FINAL A REGULAR-SEASON GAME. The
   schedule calls it `cup_final` and the export already keeps it out of
   won-lost records, so the feed's numeric type would have put a game in a
   man's regular season that the rest of the archive leaves out of it: 86
   lines across the three finals, 69 of them played, and LeBron James's
   2023-24 at 72 games and 1,846 points instead of 71 and 1,822. A stored
   line takes its season type from the SCHEDULE, for every type rather than
   only for this one. See `known_games`.

THE DRAFT FEED IS A DIFFERENT ID SPACE, so it is not read here. Its
`athlete_id` shares not one value with the player ids: 0 of 60 picks join in
each of 2010, 2018 and 2019. Its names do not close the gap either, because
it drops the suffixes the player feed keeps — Mohamed Bamba for Mo Bamba,
Kevin Knox for Kevin Knox II, Robert Williams for Robert Williams III — so a
name join would attribute a first-round pick to the wrong man rather than
leave it unattributed. Where a player page says a man was drafted, the round
and the pick come from his own `player_core` row, which joins by
construction and covers 74-84% of players a season, undrafted men being the
rest.

SIX MEN HOLD TWO PROVIDER IDS EACH. Same name, same date of birth, and in
four of the six cases a career that splits across the two records; Corey
Brewer's 2019 is in both. They are linked symmetrically in `person_alias`
and nothing is summed across the link, which is how the football archive's
74 split careers were settled.
"""

import argparse
from collections import defaultdict
from datetime import datetime, timezone

from . import identity, source as source_module, store as store_module

# The datasets this loader reads, and whether a missing file fails the run.
# `player_core` and `player_box` are the layer; season statistics are the
# check on it, which is not worth failing a season that has its box scores
# in hand.
REQUIRED = ('player_core', 'player_box')
OPTIONAL = ('player_season_stats',)

# The countable columns, source name -> stored name. Percentages and
# made-attempted strings are NOT stored: a percentage is a quotient of two
# numbers that are both here, and storing it as well invites a page that
# draws one number from the feed and the other from arithmetic.
COUNTS = (
    ('points', 'points'),
    ('rebounds', 'rebounds'),
    ('offensive_rebounds', 'offensive_rebounds'),
    ('defensive_rebounds', 'defensive_rebounds'),
    ('assists', 'assists'),
    ('steals', 'steals'),
    ('blocks', 'blocks'),
    ('turnovers', 'turnovers'),
    ('fouls', 'fouls'),
    ('field_goals_made', 'field_goals_made'),
    ('field_goals_attempted', 'field_goals_attempted'),
    ('three_point_field_goals_made', 'threes_made'),
    ('three_point_field_goals_attempted', 'threes_attempted'),
    ('free_throws_made', 'free_throws_made'),
    ('free_throws_attempted', 'free_throws_attempted'),
    ('plus_minus', 'plus_minus'),
)
SUMMED = tuple(stored for _, stored in COUNTS)

PERSON_COLUMNS = (
    'person_key', 'athlete_id', 'full_name', 'display_name', 'short_name',
    'first_name', 'last_name', 'position', 'position_short', 'jersey',
    'height_inches', 'weight_pounds', 'date_of_birth', 'birth_city',
    'birth_state', 'birth_country', 'college_id', 'draft_year',
    'draft_round', 'draft_selection', 'experience_years',
)
GAME_COLUMNS = (
    'person_key', 'game_key', 'season', 'season_type', 'game_date',
    'team_key', 'opponent_key', 'home_away', 'played', 'started', 'minutes',
) + SUMMED
SEASON_COLUMNS = (
    'person_key', 'season', 'season_type', 'team_key', 'games', 'starts',
    'minutes',
) + SUMMED + ('feed_games', 'matches_feed')


def _int(value):
    """An integer, or None. `'3'`, `3.0` and `None` are all handled."""
    if value is None or value == '':
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _float(value):
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _str(value):
    if value is None:
        return None
    text = ' '.join(str(value).split())
    return text or None


def height_inches(display):
    """`6' 5"` as 77 inches. Returns None for anything else.

    The feed publishes height only as this string; `player_core` also carries
    a numeric `height`, but it is absent on some rows where the display
    string is present, so the string is the source and this is the parser.
    """
    text = _str(display)
    if not text:
        return None
    feet, _, rest = text.partition("'")
    feet = _int(feet)
    if feet is None:
        return None
    inches = _int(rest.replace('"', '').strip()) or 0
    return feet * 12 + inches


def weight_pounds(display):
    """`205 lbs` as 205."""
    text = _str(display)
    if not text:
        return None
    return _int(text.split()[0])


def person_key(athlete_id):
    """`nba:espn:player:1966`, or None for a row with no id.

    34 of the 851,463 box-score lines carry no athlete id at all. A line
    with no one to attribute it to is dropped rather than given a made-up
    key, and the count is reported.
    """
    value = _int(athlete_id)
    if value is None:
        return None
    return identity.entity_key('player', value)


def parse_people(rows):
    """`player_core` rows as person records, newest row per id winning."""
    found = {}
    for row in rows:
        athlete_id = _int(row.get('athlete_id'))
        if athlete_id is None:
            continue
        name = _str(row.get('full_name')) or _str(row.get('display_name'))
        if not name:
            continue
        found[athlete_id] = dict(
            person_key=identity.entity_key('player', athlete_id),
            athlete_id=athlete_id,
            full_name=name,
            display_name=_str(row.get('display_name')),
            short_name=_str(row.get('short_name')),
            first_name=_str(row.get('first_name')),
            last_name=_str(row.get('last_name')),
            position=_str(row.get('position_name')),
            position_short=_str(row.get('position_abbreviation')),
            jersey=_str(row.get('jersey')),
            height_inches=height_inches(row.get('display_height')),
            weight_pounds=weight_pounds(row.get('display_weight')),
            date_of_birth=_str(row.get('date_of_birth')),
            birth_city=_str(row.get('birth_city')),
            birth_state=_str(row.get('birth_state')),
            birth_country=_str(row.get('birth_country')),
            college_id=_int(row.get('college_id')),
            draft_year=_int(row.get('draft_year')),
            draft_round=_int(row.get('draft_round')),
            draft_selection=_int(row.get('draft_selection')),
            experience_years=_int(row.get('experience_years')),
        )
    return list(found.values())


def _franchise(team_id):
    """A franchise's team key, or None for an All-Star or unknown side."""
    try:
        return identity.team_key(team_id)
    except ValueError:
        return None


def parse_games(rows, season):
    """`player_box` rows as per-game lines, with what was refused counted.

    Returns `(lines, counts)`. A line is refused when it has no player, no
    game, or a team that is not one of the thirty franchises — the last of
    which is the All-Star trap, and is counted separately so that an export
    can state how many rows the exhibition cost rather than losing them
    silently.
    """
    counts = dict(rows=len(rows), no_person=0, no_game=0, exhibition=0,
                  duplicate=0)
    lines = {}
    for row in rows:
        key = person_key(row.get('athlete_id'))
        if key is None:
            counts['no_person'] += 1
            continue
        provider_game = _str(row.get('game_id'))
        if not provider_game:
            counts['no_game'] += 1
            continue
        team_key = _franchise(row.get('team_id'))
        if team_key is None:
            counts['exhibition'] += 1
            continue
        game_key = identity.game_key(provider_game)
        pair = (key, game_key)
        if pair in lines:
            counts['duplicate'] += 1
            continue
        minutes = _float(row.get('minutes'))
        line = dict(
            person_key=key, game_key=game_key, season=int(season),
            season_type=identity.season_type(_int(row.get('season_type'))),
            game_date=_str(row.get('game_date')),
            team_key=team_key,
            opponent_key=_franchise(row.get('opponent_team_id')),
            home_away=_str(row.get('home_away')),
            # RULE 2: minutes are what say he played.
            played=1 if minutes is not None else 0,
            started=1 if str(row.get('starter')).lower() == 'true' else 0,
            minutes=minutes,
        )
        for source_name, stored in COUNTS:
            line[stored] = _int(row.get(source_name))
        lines[pair] = line
    return list(lines.values()), counts


def feed_games(rows):
    """`{athlete id: games played}` from the feed's own season statistics.

    Read for one number only. The feed publishes three categories of season
    statistic — averages, totals and a miscellaneous set — and every
    countable one of them is derivable from the per-game lines, so the only
    thing taken from it is the count of games, which is the one quantity the
    lines and the feed disagree about.
    """
    found = {}
    for row in rows:
        if row.get('category') != 'averages':
            continue
        if row.get('stat_name') != 'gamesPlayed':
            continue
        athlete_id = _int(row.get('athlete_id'))
        value = _float(row.get('value'))
        if athlete_id is None or value is None:
            continue
        found[athlete_id] = int(round(value))
    return found


def roll_up(lines, *, feed=None):
    """Per-game lines as one row per man, season, season type and club.

    Only PLAYED lines are counted, so `games` is the number of games the
    archive carries a line with minutes for. `feed_games` is the feed's own
    count for the regular season and `matches_feed` says whether the two
    agree; both are NULL where the feed says nothing, which is every season
    type but the regular one.
    """
    feed = feed or {}
    grouped = defaultdict(lambda: defaultdict(float))
    counted = defaultdict(lambda: dict(games=0, starts=0))
    for line in lines:
        if not line['played']:
            continue
        key = (line['person_key'], line['season'], line['season_type'],
               line['team_key'])
        counted[key]['games'] += 1
        counted[key]['starts'] += 1 if line.get('started') else 0
        if line.get('minutes') is not None:
            grouped[key]['minutes'] += line['minutes']
        for stored in SUMMED:
            value = line.get(stored)
            if value is not None:
                grouped[key][stored] += value
    # The feed's count is for a man's whole regular season, not his season
    # with one club, so it is compared against the sum across his clubs.
    regular = defaultdict(int)
    for (person, season, season_type, _), totals in counted.items():
        if season_type == 'regular':
            regular[(person, season)] += totals['games']
    rows = []
    for key, totals in sorted(counted.items()):
        person, season, season_type, team = key
        athlete_id = _int(person.rsplit(':', 1)[-1])
        said = feed.get(athlete_id) if season_type == 'regular' else None
        row = dict(person_key=person, season=season, season_type=season_type,
                   team_key=team, games=totals['games'],
                   starts=totals['starts'],
                   feed_games=said,
                   matches_feed=(None if said is None
                                 else int(regular[(person, season)] == said)))
        sums = grouped[key]
        row['minutes'] = round(sums['minutes'], 1) if 'minutes' in sums else None
        for stored in SUMMED:
            row[stored] = int(sums[stored]) if stored in sums else None
        rows.append(row)
    return rows


def known_games(connection):
    """`{game key: season type}` for every game the schedule carries.

    A box-score line for a game the schedule does not have is dropped. The
    alternative is a per-game line that no page can reach, because every
    route into one goes through a game or a season the schedule defines.

    THE TYPE COMES BACK WITH THE KEY BECAUSE THE SCHEDULE IS RIGHT ABOUT IT
    AND THE BOX FEED IS NOT. The box feed's numeric season type calls the
    NBA Cup final a regular-season game — 86 lines across the three finals
    the archive has, 69 of them played — while the schedule classifies it
    `cup_final` and `src/nba/export.py` already keeps it out of won-lost
    records. Taking the feed's word put LeBron James's 2023-24 regular
    season at 72 games and 1,846 points where it is 71 and 1,822. So a
    stored line's season type is the schedule's, for every type, not only
    for this one: a second feed disagreeing again is then a fact the
    archive already has an answer for.
    """
    return {row[0]: row[1] for row in
            connection.execute('SELECT game_key, season_type FROM game')}


def replace_season(connection, season, people, lines, *, feed=None,
                   snapshot_id=None):
    """Store one season of the people layer, replacing whatever was there.

    THE ROLLUPS ARE DERIVED HERE, from the lines that are actually stored
    rather than from every line parsed. Rolling up first and filtering after
    made a man's season count games the schedule does not have, which is a
    number no page could reconcile with his own game log.

    DELETE then INSERT for everything keyed by the season, because the source
    revises these files: a line that has stopped existing must stop existing
    here. `person` is upserted rather than replaced — a man's row belongs to
    his whole career, not to one season of it — and his archive span is
    recomputed from the lines afterwards.
    """
    types = known_games(connection)
    kept = [line for line in lines if line['game_key'] in types]
    unknown = len(lines) - len(kept)
    # The schedule's classification wins: see `known_games`.
    for line in kept:
        line['season_type'] = types[line['game_key']]
    known_people = {person['person_key'] for person in people}
    # A line whose man has no `player_core` row cannot be stored: the table
    # has a foreign key to `person`, and a page would have a game log with
    # nobody to put it on. Measured at zero across all 26 seasons, and
    # counted rather than assumed.
    orphans = [line for line in kept if line['person_key'] not in known_people]
    kept = [line for line in kept if line['person_key'] in known_people]
    seasons = roll_up(kept, feed=feed)
    if people:
        names = ', '.join(PERSON_COLUMNS + ('snapshot_id',))
        marks = ', '.join('?' * (len(PERSON_COLUMNS) + 1))
        updates = ', '.join(f'{column} = excluded.{column}'
                            for column in PERSON_COLUMNS
                            if column != 'person_key')
        connection.executemany(
            f'INSERT INTO person ({names}) VALUES ({marks}) '
            f'ON CONFLICT(person_key) DO UPDATE SET {updates}, '
            'snapshot_id = excluded.snapshot_id',
            [tuple(person[column] for column in PERSON_COLUMNS)
             + (snapshot_id,) for person in people])
    connection.execute('DELETE FROM person_game WHERE season = ?', (season,))
    connection.execute('DELETE FROM person_season WHERE season = ?', (season,))
    for table, columns, rows in (
            ('person_game', GAME_COLUMNS, kept),
            ('person_season', SEASON_COLUMNS, seasons)):
        if not rows:
            continue
        names = ', '.join(columns + ('snapshot_id',))
        marks = ', '.join('?' * (len(columns) + 1))
        connection.executemany(
            f'INSERT INTO {table} ({names}) VALUES ({marks})',
            [tuple(row[column] for column in columns) + (snapshot_id,)
             for row in rows])
    connection.commit()
    return dict(people=len(people), lines=len(kept), rollups=len(seasons),
                unknown_games=unknown,
                orphans=len(orphans),
                played=sum(1 for line in kept if line['played']))


def refresh_spans(connection):
    """Recompute every man's first and last season from his stored lines.

    Derived from the lines rather than from a biography: a career that began
    in 1996 begins in 2002 here, and a page that says so is telling the truth
    about the archive rather than about the man.
    """
    connection.execute(
        'UPDATE person SET first_season = ('
        '  SELECT MIN(season) FROM person_game g'
        '   WHERE g.person_key = person.person_key AND g.played = 1), '
        'last_season = ('
        '  SELECT MAX(season) FROM person_game g'
        '   WHERE g.person_key = person.person_key AND g.played = 1)')
    connection.commit()
    return connection.execute(
        'SELECT COUNT(*) FROM person WHERE first_season IS NOT NULL'
    ).fetchone()[0]


def link_same_people(connection, *, snapshot_id=None):
    """Link the men who hold two provider ids, both ways round.

    The basis is a name and a date of birth that match exactly. Six men in
    the archive are like this, which was measured over every season's
    `player_core`; the football archive's 74 such cases were settled the same
    way, and for the same reason. Nothing is merged and nothing is summed: a
    page reached by either id shows that career and a link to the other.
    """
    connection.execute('DELETE FROM person_alias')
    rows = connection.execute(
        'SELECT person_key, LOWER(TRIM(full_name)) AS name, date_of_birth'
        '  FROM person WHERE date_of_birth IS NOT NULL'
        '   AND TRIM(full_name) <> ""').fetchall()
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row['name'], row['date_of_birth'])].append(row['person_key'])
    pairs = []
    for (name, _), keys in grouped.items():
        if len(keys) < 2:
            continue
        for one in keys:
            for other in keys:
                if one != other:
                    pairs.append((one, other, 'name and date of birth'))
    if pairs:
        connection.executemany(
            'INSERT INTO person_alias (person_key, other_key, basis, '
            'snapshot_id) VALUES (?, ?, ?, ?)',
            [pair + (snapshot_id,) for pair in pairs])
    connection.commit()
    return len(pairs)


def _rows(dataset, season, *, root, reuse_cache, opener, sleep):
    path, origin = source_module.load_asset(
        dataset, season, root=root, reuse_cache=reuse_cache, opener=opener,
        sleep=sleep)
    try:
        return source_module.read_parquet(path), origin, path
    except Exception:
        path.unlink(missing_ok=True)
        raise


def ingest_season(connection, season, *, raw_root=None, reuse_cache=False,
                  opener=None, sleep=None, now=None, given=None):
    """One season: fetch, reduce, store, and delete what was fetched.

    `given` is a dict of dataset name to rows, which is how the tests drive
    this without a network. A required dataset that will not fetch fails the
    season; an optional one that will not fetch leaves its part empty, which
    is the rule the schedule ingest already follows for standings.
    """
    stamp = (now or datetime.now(timezone.utc)).isoformat()
    given = given or {}
    fetched = {}
    paths = []
    try:
        for dataset in REQUIRED:
            if dataset in given:
                fetched[dataset] = given[dataset]
                continue
            rows, _, path = _rows(dataset, season, root=raw_root,
                                  reuse_cache=reuse_cache, opener=opener,
                                  sleep=sleep)
            fetched[dataset], _ = rows, paths.append(path)
        for dataset in OPTIONAL:
            if dataset in given:
                fetched[dataset] = given[dataset]
                continue
            try:
                rows, _, path = _rows(dataset, season, root=raw_root,
                                      reuse_cache=reuse_cache, opener=opener,
                                      sleep=sleep)
            except source_module.NbaSourceError:
                fetched[dataset] = []
            else:
                fetched[dataset], _ = rows, paths.append(path)
        people = parse_people(fetched['player_core'])
        lines, counts = parse_games(fetched['player_box'], season)
        snapshot_id = store_module.record_snapshot(
            connection, 'people', stamp, season=season, source='network',
            source_revision=source_module.source_revision(),
            rows_seen=counts['rows'],
            notes=source_module.asset_url('player_box', season))
        stored = replace_season(
            connection, int(season), people, lines,
            feed=feed_games(fetched['player_season_stats']),
            snapshot_id=snapshot_id)
    finally:
        for path in paths:
            path.unlink(missing_ok=True)
    return dict(season=int(season), **counts, **stored)


def ingest(connection, seasons, *, raw_root=None, reuse_cache=False,
           opener=None, sleep=None, now=None):
    """Every season named, oldest first. A failure stops the run."""
    results = []
    for season in seasons:
        results.append(ingest_season(
            connection, season, raw_root=raw_root, reuse_cache=reuse_cache,
            opener=opener, sleep=sleep, now=now))
    results.append(dict(season=None,
                        spans=refresh_spans(connection),
                        aliases=link_same_people(connection)))
    return results


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Load the NBA people layer')
    parser.add_argument('--db', default=None)
    parser.add_argument('--raw-root', default=None)
    parser.add_argument('--seasons', default='',
                        help='e.g. 2026 or 2024-2027; default is all of them')
    parser.add_argument('--reuse-cache', action='store_true')
    args = parser.parse_args(argv)
    from .ingest import parse_seasons
    seasons = parse_seasons(args.seasons)
    connection = store_module.connect(args.db)
    try:
        results = ingest(connection, seasons, raw_root=args.raw_root,
                         reuse_cache=args.reuse_cache)
    finally:
        pass
    final = results[-1]
    per_season = [row for row in results if row.get('season') is not None]
    lines = sum(row['lines'] for row in per_season)
    played = sum(row['played'] for row in per_season)
    exhibition = sum(row['exhibition'] for row in per_season)
    no_person = sum(row['no_person'] for row in per_season)
    unknown = sum(row['unknown_games'] for row in per_season)
    people = connection.execute('SELECT COUNT(*) FROM person').fetchone()[0]
    mismatched = connection.execute(
        'SELECT COUNT(*) FROM person_season WHERE matches_feed = 0'
    ).fetchone()[0]
    print(f'{people} players, {lines} lines ({played} played) across '
          f'{len(per_season)} seasons')
    print(f'  dropped: {exhibition} exhibition rows, {no_person} with no '
          f'player, {unknown} for games the schedule does not have')
    print(f'  {final["aliases"] // 2} players hold two provider ids')
    print(f'  {mismatched} player-seasons where the feed\'s count of games '
          'played and the lines stored disagree, in either direction')
    for row in per_season:
        print(f'  {row["season"]:>6}  people {row["people"]:>4}  '
              f'lines {row["lines"]:>6}  played {row["played"]:>6}  '
              f'[exhibition {row["exhibition"]}, '
              f'no player {row["no_person"]}, unknown game '
              f'{row["unknown_games"]}]')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
