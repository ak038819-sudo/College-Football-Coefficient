"""The NFL people layer: players, rosters, season statistics and the draft.

This module owns the people tables end to end — it parses the four nflverse
people files and it writes them — because the rules that make the parsing
correct are the same rules that decide what a row means, and splitting them
across two files is how the loader and the schema drift apart.

Four things in here were measured against the whole archive on 2026-10-21 and
are the reason the code is shaped as it is:

1. **1,789 of the 69,533 roster rows name a GSIS id the player file does not
   carry**, and 9 statistics rows do the same. Dropping them would lose real
   seasons of real players; assuming the player file covers everything would
   fail a foreign key. So a roster or statistics row for an unknown id
   creates a STUB player from the row's own name, position and measurements,
   flagged `stub = 1` so nothing later mistakes it for a full biography.

2. **25 roster rows and 51 statistics rows carry no GSIS id at all.** Those
   cannot be attributed to anybody and are counted and dropped, never stored
   under a made-up key.

3. **1,309 players have two roster rows in one season** because they changed
   team, and 31 have two for the same team. The first is the data being right
   and is stored as two rows; the second is a duplicate and the later row
   wins.

4. **The statistics files spell five teams the GSIS way** (ARZ, BLT, CLV,
   HST, SL) and **the draft file spells nine the Pro-Football-Reference way**
   (GNB, KAN, LAR, ...). `identity.resolve_code` holds the table and the
   evidence for it.

Statistics are stored LONG, a zero is not stored, and a regular season and a
postseason are never summed. See `schema.sql` for why each of those is the
case.
"""

import argparse
import sys
from datetime import datetime, timezone

from . import people_source
from .identity import (FIRST_SEASON, current_season, people_franchise_key,
                       player_key, seasons_through)
from .store import connect, record_snapshot

# Columns of a statistics row that describe WHO and WHEN rather than what he
# did. `games` is here because it belongs to `player_stat_season`: it is the
# denominator of every other number in the row, not one more number in it.
STAT_CONTEXT = frozenset({
    'player_id', 'player_name', 'player_display_name', 'position',
    'position_group', 'headshot_url', 'season', 'season_type', 'recent_team',
    'games',
})


def is_text_stat(column):
    """Columns holding a LIST rather than a number.

    `fg_made_list` is "23;41;52" and `fg_made_distance` the same again. They
    are per-kick detail, not a season statistic, and float() on them raises,
    so they are recognised by shape rather than listed: a new one appearing
    in a later release is then skipped rather than crashing a refresh.
    """
    return column.endswith('_list') or column.endswith('_distance')


# Statistics that are an AVERAGE or a SHARE, which must never be added up
# across seasons. A career `fg_pct` is made from career makes and attempts,
# not from the mean of twelve season percentages, and a career `target_share`
# is not a thing at all.
MEAN_STATS = frozenset({
    'passing_cpoe', 'pacr', 'racr', 'target_share', 'air_yards_share',
    'wopr', 'fg_pct', 'pat_pct',
})
# Statistics that are a BEST, where a career value is the largest season one.
MAX_STATS = frozenset({'fg_long', 'pt_long'})


def _int(value):
    """An integer, or None. Tolerates '12.0', refuses '12th'."""
    text = (value or '').strip() if isinstance(value, str) else value
    if text in (None, ''):
        return None
    try:
        number = float(text)
    except (TypeError, ValueError):
        return None
    if number != int(number):
        return None
    return int(number)


def _real(value):
    text = (value or '').strip() if isinstance(value, str) else value
    if text in (None, ''):
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _str(value):
    text = (value or '').strip() if isinstance(value, str) else value
    return text or None


def _team(code):
    """A franchise key from a people file's code, or None where it is blank.

    An unknown code RAISES, out of `identity.resolve_code`. That is deliberate:
    the alias table was measured, and a code outside it is a team this archive
    does not know about, which a person has to look at.
    """
    cleaned = _str(code)
    return people_franchise_key(cleaned) if cleaned else None


def parse_players(rows):
    """The player file as player rows, keyed and resolved.

    Only players whose career reaches 1999 are kept. The archive's games,
    ratings, rosters and statistics all start there, so a player who last
    played in 1991 would get a page with nothing on it and no way to reach it.
    A player the file dates before 1999 but a roster row contradicts is picked
    up by `parse_roster` as a stub and then filled in by this row anyway,
    because `upsert_players` merges on the key.
    """
    out, skipped = [], dict(no_id=0, before_archive=0)
    for row in rows:
        gsis = _str(row.get('gsis_id'))
        if not gsis:
            skipped['no_id'] += 1
            continue
        last = _int(row.get('last_season'))
        if last is not None and last < FIRST_SEASON:
            skipped['before_archive'] += 1
            continue
        out.append(dict(
            player_key=player_key(gsis),
            gsis_id=gsis,
            display_name=(_str(row.get('display_name'))
                          or ' '.join(part for part in
                                      (_str(row.get('first_name')),
                                       _str(row.get('last_name')))
                                      if part) or gsis),
            first_name=_str(row.get('first_name')),
            last_name=_str(row.get('last_name')),
            position=_str(row.get('position')),
            position_group=_str(row.get('position_group')),
            height=_int(row.get('height')),
            weight=_int(row.get('weight')),
            birth_date=_str(row.get('birth_date')),
            college=_str(row.get('college_name')),
            college_conference=_str(row.get('college_conference')),
            jersey_number=_int(row.get('jersey_number')),
            rookie_season=_int(row.get('rookie_season')),
            last_season=last,
            latest_team=_team(row.get('latest_team')),
            status=_str(row.get('status')),
            years_experience=_int(row.get('years_of_experience')),
            draft_season=_int(row.get('draft_year')),
            draft_round=_int(row.get('draft_round')),
            draft_pick=_int(row.get('draft_pick')),
            draft_team=_team(row.get('draft_team')),
            stub=0,
        ))
    return out, skipped


def _stub(gsis, *, name, position=None, height=None, weight=None,
          birth_date=None, college=None, season=None):
    """A player row built from a roster or statistics row.

    Everything the row happens to carry is kept; everything else is NULL. The
    `stub` flag is what tells a reader that this biography came from a team's
    roster sheet rather than from the player file.
    """
    return dict(
        player_key=player_key(gsis), gsis_id=gsis,
        display_name=name or gsis, first_name=None, last_name=None,
        position=position, position_group=None, height=height, weight=weight,
        birth_date=birth_date, college=college, college_conference=None,
        jersey_number=None, rookie_season=None, last_season=season,
        latest_team=None, status=None, years_experience=None,
        draft_season=None, draft_round=None, draft_pick=None, draft_team=None,
        stub=1)


def parse_roster(season, rows):
    """One season's roster as `player_season` rows, plus any stubs it needs.

    A player with two rows for two teams keeps both: he played for both. A
    player with two rows for ONE team is a duplicate and the later row wins,
    which is the same rule every other loader here uses.
    """
    seasons, stubs = {}, {}
    skipped = dict(no_id=0, duplicate=0)
    for row in rows:
        gsis = _str(row.get('gsis_id'))
        if not gsis:
            skipped['no_id'] += 1
            continue
        key = player_key(gsis)
        team = _team(row.get('team'))
        if team is None:
            skipped['no_id'] += 1
            continue
        stubs.setdefault(key, _stub(
            gsis, name=_str(row.get('full_name')),
            position=_str(row.get('position')),
            height=_int(row.get('height')), weight=_int(row.get('weight')),
            birth_date=_str(row.get('birth_date')),
            college=_str(row.get('college')), season=season))
        identity = (key, season, team)
        if identity in seasons:
            skipped['duplicate'] += 1
        seasons[identity] = dict(
            player_key=key, season=season, team_key=team,
            code=_str(row.get('team')),
            position=_str(row.get('position')),
            depth_position=_str(row.get('depth_chart_position')),
            jersey_number=_int(row.get('jersey_number')),
            status=_str(row.get('status')),
            status_label=_str(row.get('status_description_abbr')),
            height=_int(row.get('height')),
            weight=_int(row.get('weight')),
            years_experience=_int(row.get('years_exp')),
            last_week=_int(row.get('week')),
            last_game_type=_str(row.get('game_type')),
            entry_season=_int(row.get('entry_year')),
            rookie_season=_int(row.get('rookie_year')),
        )
    return list(seasons.values()), list(stubs.values()), skipped


def parse_stats(season, season_type, rows):
    """One statistics file as a season row and a long list of values.

    A value of exactly zero is NOT emitted. A counting stat of zero and a
    missing counting stat are the same statement, and emitting the zeros would
    be twelve times the rows to say nothing: across the archive 775,102 of
    the cells are non-zero.
    """
    contexts, values, stubs = {}, [], {}
    skipped = dict(no_id=0, nonnumeric=0)
    for row in rows:
        gsis = _str(row.get('player_id'))
        if not gsis:
            skipped['no_id'] += 1
            continue
        key = player_key(gsis)
        code = _str(row.get('recent_team'))
        stubs.setdefault(key, _stub(
            gsis, name=(_str(row.get('player_display_name'))
                        or _str(row.get('player_name'))),
            position=_str(row.get('position')), season=season))
        contexts[(key, season, season_type)] = dict(
            player_key=key, season=season, season_type=season_type,
            team_key=_team(code) if code else None, code=code,
            position=_str(row.get('position')),
            games=_int(row.get('games')))
        for column, raw in row.items():
            if column in STAT_CONTEXT or is_text_stat(column):
                continue
            text = _str(raw)
            if text is None:
                continue
            number = _real(text)
            if number is None:
                skipped['nonnumeric'] += 1
                continue
            if number == 0:
                continue
            values.append(dict(player_key=key, season=season,
                               season_type=season_type, stat=column,
                               value=number))
    return list(contexts.values()), values, list(stubs.values()), skipped


def parse_draft(rows, *, first_season=FIRST_SEASON):
    """The draft file as `draft_pick` rows, 1999 onwards.

    A pick with no GSIS id keeps its row and gets no `player_key`: 313 of the
    7,154 modern picks are in that position, nearly all of them players who
    never appeared in a game. Leaving the pick out would make a draft board
    with holes in it.
    """
    out = []
    skipped = dict(before_archive=0, unplaceable=0)
    for row in rows:
        season = _int(row.get('season'))
        rnd, pick = _int(row.get('round')), _int(row.get('pick'))
        if season is None or season < first_season:
            skipped['before_archive'] += 1
            continue
        team = _team(row.get('team'))
        if rnd is None or pick is None or team is None:
            skipped['unplaceable'] += 1
            continue
        gsis = _str(row.get('gsis_id'))
        out.append(dict(
            season=season, round=rnd, pick=pick, team_key=team,
            code=_str(row.get('team')),
            player_key=player_key(gsis) if gsis else None,
            name=_str(row.get('pfr_player_name')) or f'pick {pick}',
            position=_str(row.get('position')),
            category=_str(row.get('category')),
            side=_str(row.get('side')),
            college=_str(row.get('college')),
            age=_real(row.get('age')),
            final_season=_int(row.get('to')),
            hall_of_fame=1 if _str(row.get('hof')) in ('1', 'TRUE',
                                                       'True', 'true') else 0,
            all_pro=_int(row.get('allpro')),
            pro_bowls=_int(row.get('probowls')),
            seasons_started=_int(row.get('seasons_started')),
            games=_int(row.get('games')),
            weighted_av=_int(row.get('w_av')),
            career_av=_int(row.get('car_av')),
            draft_av=_int(row.get('dr_av')),
        ))
    return out, skipped


PLAYER_COLUMNS = (
    'player_key', 'gsis_id', 'display_name', 'first_name', 'last_name',
    'position', 'position_group', 'height', 'weight', 'birth_date', 'college',
    'college_conference', 'jersey_number', 'rookie_season', 'last_season',
    'latest_team', 'status', 'years_experience', 'draft_season',
    'draft_round', 'draft_pick', 'draft_team', 'stub',
)
SEASON_COLUMNS = (
    'player_key', 'season', 'team_key', 'code', 'position', 'depth_position',
    'jersey_number', 'status', 'status_label', 'height', 'weight',
    'years_experience', 'last_week', 'last_game_type', 'entry_season',
    'rookie_season',
)
STAT_SEASON_COLUMNS = ('player_key', 'season', 'season_type', 'team_key',
                       'code', 'position', 'games')
DRAFT_COLUMNS = (
    'season', 'round', 'pick', 'team_key', 'code', 'player_key', 'name',
    'position', 'category', 'side', 'college', 'age', 'final_season',
    'hall_of_fame', 'all_pro', 'pro_bowls', 'seasons_started', 'games',
    'weighted_av', 'career_av', 'draft_av',
)


def _upsert(connection, table, columns, keys, rows, *, snapshot_id=None,
            keep=(), prefer_existing=(), merged=None):
    """One generated upsert for every people table.

    The column list, the placeholders, the conflict clause and the value tuple
    all come from `columns`, because four hand-written copies of the same list
    is how a row gets stored with its fields shifted by one.

    `keep` names columns an update fills only where the incoming value is
    present: a NULL never erases a value. `prefer_existing` names columns an
    update fills only where the STORED value is absent: the stored value
    wins. `merged` maps a column to its own SQL expression.
    """
    names = ', '.join(columns + ('snapshot_id',))
    placeholders = ', '.join('?' * (len(columns) + 1))
    sets = []
    for column in columns:
        if column in keys:
            continue
        if merged and column in merged:
            sets.append(f'{column} = {merged[column]}')
        elif column in prefer_existing:
            sets.append(f'{column} = COALESCE({table}.{column}, '
                        f'excluded.{column})')
        elif column in keep:
            sets.append(f'{column} = COALESCE(excluded.{column}, '
                        f'{table}.{column})')
        else:
            sets.append(f'{column} = excluded.{column}')
    sets.append('snapshot_id = excluded.snapshot_id')
    statement = (f'INSERT INTO {table} ({names}) VALUES ({placeholders})'
                 f' ON CONFLICT({", ".join(keys)}) DO UPDATE SET '
                 + ', '.join(sets))
    for row in rows:
        missing = [name for name in columns if name not in row]
        if missing:
            raise KeyError(f'a {table} row is missing '
                           f'{", ".join(missing)}')
        connection.execute(statement,
                           tuple(row[name] for name in columns)
                           + (snapshot_id,))
    return len(rows)


def upsert_players(connection, rows, *, snapshot_id=None):
    """Store players. A full biography never loses to a stub.

    The two kinds of row merge in opposite directions, whichever order they
    arrive in:

    - a row from the PLAYER FILE is the authority, so its values replace what
      is stored, and only a NULL in it leaves a stored value alone;
    - a STUB, built from a roster sheet, fills gaps and overwrites nothing,
      its name included.

    `stub` is the AND of the two rows, so a player the file does carry stops
    being a stub the moment the file is read and never becomes one again.
    """
    nullable = tuple(name for name in PLAYER_COLUMNS
                     if name not in ('player_key', 'gsis_id', 'stub'))
    stub_flag = {'stub': 'MIN(player.stub, excluded.stub)'}
    full = [row for row in rows if not row['stub']]
    stubs = [row for row in rows if row['stub']]
    stored = _upsert(connection, 'player', PLAYER_COLUMNS, ('player_key',),
                     full, snapshot_id=snapshot_id,
                     keep=tuple(name for name in nullable
                                if name != 'display_name'),
                     merged=stub_flag)
    stored += _upsert(connection, 'player', PLAYER_COLUMNS, ('player_key',),
                      stubs, snapshot_id=snapshot_id,
                      prefer_existing=nullable, merged=stub_flag)
    connection.commit()
    return stored


def upsert_player_seasons(connection, rows, *, snapshot_id=None):
    stored = _upsert(connection, 'player_season', SEASON_COLUMNS,
                     ('player_key', 'season', 'team_key'), rows,
                     snapshot_id=snapshot_id)
    connection.commit()
    return stored


def upsert_stat_seasons(connection, rows, *, snapshot_id=None):
    stored = _upsert(connection, 'player_stat_season', STAT_SEASON_COLUMNS,
                     ('player_key', 'season', 'season_type'), rows,
                     snapshot_id=snapshot_id)
    connection.commit()
    return stored


def upsert_draft(connection, rows, *, snapshot_id=None):
    stored = _upsert(connection, 'draft_pick', DRAFT_COLUMNS,
                     ('season', 'round', 'pick'), rows,
                     snapshot_id=snapshot_id)
    connection.commit()
    return stored


def replace_stats(connection, season, season_type, rows):
    """Store one file's statistics, replacing whatever that file stored before.

    DELETE then INSERT rather than an upsert, because a stat that has stopped
    being non-zero must stop existing: the source revises these files, and an
    upsert would leave last week's value behind forever as a row nothing ever
    overwrites.
    """
    connection.execute(
        'DELETE FROM player_stat WHERE season = ? AND season_type = ?',
        (season, season_type))
    connection.executemany(
        'INSERT INTO player_stat (player_key, season, season_type, stat,'
        ' value) VALUES (?, ?, ?, ?, ?)',
        [(row['player_key'], row['season'], row['season_type'], row['stat'],
          row['value']) for row in rows])
    connection.commit()
    return len(rows)


def ingest(connection, bundle, *, snapshot_id=None):
    """Store a whole people bundle. Idempotent, and ordered so keys resolve.

    Players first (including every stub the other files imply), because
    `player_season`, `player_stat_season` and `draft_pick` all reference
    `player(player_key)` and foreign keys are ON.
    """
    report = dict(players=0, stubs=0, player_seasons=0, stat_seasons=0,
                  stat_values=0, draft_picks=0, skipped={})
    players, skipped = parse_players(bundle.get('players') or [])
    report['skipped']['players'] = skipped
    seasons, stat_seasons, stat_values, stubs = [], [], {}, {}
    for season, rows in sorted((bundle.get('rosters') or {}).items()):
        got, season_stubs, skips = parse_roster(season, rows)
        seasons.extend(got)
        for stub in season_stubs:
            stubs.setdefault(stub['player_key'], stub)
        report['skipped'][f'roster_{season}'] = skips
    for (season, season_type), rows in sorted(
            (bundle.get('stats') or {}).items()):
        contexts, values, stat_stubs, skips = parse_stats(
            season, season_type, rows)
        stat_seasons.extend(contexts)
        stat_values[(season, season_type)] = values
        for stub in stat_stubs:
            stubs.setdefault(stub['player_key'], stub)
        report['skipped'][f'stats_{season_type}_{season}'] = skips
    draft, draft_skips = parse_draft(bundle.get('draft') or [])
    report['skipped']['draft'] = draft_skips

    known = {row['player_key'] for row in players}
    extra = [stub for key, stub in sorted(stubs.items()) if key not in known]
    report['players'] = upsert_players(connection, players,
                                       snapshot_id=snapshot_id)
    report['stubs'] = upsert_players(connection, extra,
                                     snapshot_id=snapshot_id)
    stored = {row['player_key'] for row in players} | {
        row['player_key'] for row in extra}
    # A draft pick may name a player whose career never reached 1999 and who
    # is therefore not stored. The pick keeps its row; the link is dropped,
    # because a foreign key pointing at nothing is worse than no link.
    report['draft_unlinked'] = sum(
        1 for row in draft
        if row['player_key'] and row['player_key'] not in stored)
    for row in draft:
        if row['player_key'] and row['player_key'] not in stored:
            row['player_key'] = None
    report['player_seasons'] = upsert_player_seasons(
        connection, seasons, snapshot_id=snapshot_id)
    report['stat_seasons'] = upsert_stat_seasons(
        connection, stat_seasons, snapshot_id=snapshot_id)
    for (season, season_type), values in sorted(stat_values.items()):
        report['stat_values'] += replace_stats(connection, season,
                                               season_type, values)
    report['draft_picks'] = upsert_draft(connection, draft,
                                         snapshot_id=snapshot_id)
    return report


def coverage(connection):
    """What actually resolved, season by season. Measured, never assumed."""
    return [dict(row) for row in connection.execute(
        'SELECT s.season,'
        '       (SELECT COUNT(*) FROM player_season p'
        '          WHERE p.season = s.season) AS roster_rows,'
        '       (SELECT COUNT(DISTINCT p.player_key) FROM player_season p'
        '          WHERE p.season = s.season) AS players,'
        '       (SELECT COUNT(*) FROM player_stat_season t'
        "          WHERE t.season = s.season AND t.season_type = 'reg')"
        '         AS stat_rows,'
        '       (SELECT COUNT(*) FROM player_stat v'
        '          WHERE v.season = s.season) AS stat_values,'
        '       (SELECT COUNT(*) FROM draft_pick d'
        '          WHERE d.season = s.season) AS draft_picks'
        '  FROM (SELECT DISTINCT season FROM player_season) s'
        '  ORDER BY s.season')]


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Load the NFL people archive into db/nfl.db')
    parser.add_argument('--db', default=None)
    parser.add_argument('--raw-root', default=None)
    parser.add_argument('--seasons', default=None)
    parser.add_argument('--reuse-cache', action='store_true')
    args = parser.parse_args(argv)
    wanted = ([int(part) for part in args.seasons.split(',') if part.strip()]
              if args.seasons else None)
    moment = datetime.now(timezone.utc)
    try:
        bundle, origins = people_source.load(
            seasons=wanted, root=args.raw_root,
            reuse_cache=args.reuse_cache, now=moment)
    except people_source.NflSourceError as error:
        print(f'Cannot fetch the people archive: {error}', file=sys.stderr)
        return 1
    connection = connect(args.db)
    try:
        snapshot_id = record_snapshot(
            connection, 'people', moment.isoformat(),
            source=people_source.RELEASE,
            rows_seen=len(bundle.get('players') or []),
            notes=f'{len(bundle.get("rosters") or {})} rosters, '
                  f'{len(bundle.get("stats") or {})} statistics files')
        report = ingest(connection, bundle, snapshot_id=snapshot_id)
        rows = coverage(connection)
    finally:
        connection.close()
    print(f'{report["players"]} players, {report["stubs"]} stubs, '
          f'{report["player_seasons"]} roster rows, '
          f'{report["stat_seasons"]} statistics rows, '
          f'{report["stat_values"]} statistics, '
          f'{report["draft_picks"]} draft picks')
    dropped = {name: skips for name, skips in report['skipped'].items()
               if any(skips.values())}
    for name, skips in sorted(dropped.items()):
        print(f'  {name}: ' + ', '.join(f'{k} {v}' for k, v in skips.items()
                                        if v))
    if report['draft_unlinked']:
        print(f'  {report["draft_unlinked"]} draft picks name a player whose '
              'career never reached the archive; stored without a link')
    absent = sorted(name for name, origin in origins.items()
                    if origin == 'absent')
    if absent:
        print(f'  not published yet: {", ".join(absent)}')
    for row in rows:
        print(f'  {row["season"]}  roster {row["roster_rows"]:>4}  people '
              f'{row["players"]:>4}  stat rows {row["stat_rows"]:>4}  '
              f'values {row["stat_values"]:>6}  draft '
              f'{row["draft_picks"]:>3}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
