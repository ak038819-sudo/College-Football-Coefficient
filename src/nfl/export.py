"""Export the NFL archive as static files, with a manifest and a gate.

Writes `ui/data/nfl/` and nothing else: no college football file, no
basketball file, no shared global. A reader of these files can always answer
three questions before it draws a number — which model made it, what that
model scored on games it had never seen, and how much of the league is in
here — because the manifest is refused unless it can.

The gate in `src/nfl/publish_gate.py` runs BEFORE the first byte is written.
An export that would fail it is not written at all, because a half-written
export directory is itself a way to publish a sample.
"""

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from . import market as market_module
from . import people_export
from . import playoff as playoff_module
from .coaches import careers
from .identity import ALIGNMENT_FROM, CONFERENCES, current_season
from .publish_gate import SCHEMA_VERSION, check, render
from .store import connect

OUT_DIR = Path('ui/data/nfl')
MANIFEST = 'manifest.json'


def model_row(connection, name=None):
    """The newest stored model, or the one named. Never two, never a guess."""
    sql = ('SELECT model_id, name, config, fitted_on, held_out, holdout_brier,'
           '       holdout_games, created_at FROM rating_model')
    if name:
        row = connection.execute(sql + ' WHERE name = ?', (name,)).fetchone()
    else:
        row = connection.execute(
            sql + ' ORDER BY model_id DESC LIMIT 1').fetchone()
    if row is None:
        raise ValueError('No rating model is stored; run build_ratings first')
    return dict(row)


def _seasons_json(value):
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def model_section(row):
    config = json.loads(row['config'])
    canonical = json.dumps(config, sort_keys=True,
                           separators=(',', ':')).encode('utf-8')
    return dict(
        name=row['name'],
        config=config,
        config_sha256=hashlib.sha256(canonical).hexdigest(),
        fitted_on=_seasons_json(row['fitted_on']),
        held_out=_seasons_json(row['held_out']),
        holdout_brier=row['holdout_brier'],
        holdout_games=row['holdout_games'],
        built_at=row['created_at'],
    )


def teams(connection):
    """Every franchise, keyed by franchise key.

    The historical codes are not keys here: `STL` is the Rams, and a page that
    wants to print what a 2015 box score said reads the game's own `home_code`.
    """
    rows = connection.execute(
        'SELECT team_key, code, name, location, nickname, conference,'
        '       division, first_season, last_season FROM team'
        ' ORDER BY team_key').fetchall()
    return {row['team_key']: dict(
        code=row['code'], name=row['name'], location=row['location'],
        nickname=row['nickname'], conference=row['conference'],
        division=row['division'], first_season=row['first_season'],
        last_season=row['last_season'],
    ) for row in rows}


def season_games(connection, season):
    rows = connection.execute(
        'SELECT game_key, provider_id, season_type, postseason, week,'
        '       game_date, kickoff_utc, kickoff_tbd, neutral_site, home_code,'
        '       away_code, home_team_key, away_team_key, home_points,'
        '       away_points, overtime, divisional_game, stadium, roof,'
        '       surface, temp, wind, home_rest, away_rest, home_coach,'
        '       away_coach, home_qb_name, away_qb_name, spread_line,'
        '       total_line, home_moneyline, away_moneyline, referee, rateable'
        '  FROM game WHERE season = ?'
        "  ORDER BY game_date, COALESCE(kickoff_utc, ''), provider_id",
        (season,)).fetchall()
    out = []
    for row in rows:
        game = dict(
            id=row['game_key'],
            date=row['game_date'],
            at=row['kickoff_utc'],
            week=row['week'],
            type=row['season_type'],
            home=row['home_team_key'],
            away=row['away_team_key'],
            # A missing score stays missing. A zero is a measurement.
            home_points=row['home_points'],
            away_points=row['away_points'],
            rateable=row['rateable'],
        )
        # The code the source used at the time, carried only where it differs
        # from the franchise's current one, so a 2015 game can still read STL
        # without every row repeating what the team index already says.
        if row['home_code'] != teams_code(row['home_team_key']):
            game['home_code'] = row['home_code']
        if row['away_code'] != teams_code(row['away_team_key']):
            game['away_code'] = row['away_code']
        if row['neutral_site']:
            game['neutral'] = 1
        if row['kickoff_tbd']:
            game['time_tbd'] = 1
        if row['postseason']:
            game['postseason'] = 1
        if row['overtime']:
            game['ot'] = 1
        if row['divisional_game']:
            game['div'] = 1
        if row['stadium']:
            game['venue'] = row['stadium']
        # The context the archive already carried and schema 1 discarded. Each
        # field is emitted only where it is present, so a dome game has no
        # temperature rather than a zero, and a 1999 game with no posted line
        # has no line rather than a pick'em.
        for field, column in (('roof', 'roof'), ('surface', 'surface'),
                              ('temp', 'temp'), ('wind', 'wind'),
                              ('home_rest', 'home_rest'),
                              ('away_rest', 'away_rest'),
                              ('home_coach', 'home_coach'),
                              ('away_coach', 'away_coach'),
                              ('home_qb', 'home_qb_name'),
                              ('away_qb', 'away_qb_name'),
                              ('spread', 'spread_line'),
                              ('total_line', 'total_line'),
                              ('home_ml', 'home_moneyline'),
                              ('away_ml', 'away_moneyline'),
                              ('referee', 'referee')):
            if row[column] is not None and row[column] != '':
                game[field] = row[column]
        out.append(game)
    return out


def teams_code(team_key):
    """The franchise's current code, read from its key rather than a lookup."""
    return team_key.rsplit(':', 1)[-1]


def records(games):
    """Each franchise's won-lost-tied, from finals only.

    Ties are their own column. Folding a tie into a loss, or dropping it, is
    how 16 games since 1999 would quietly stop existing.
    """
    tally = defaultdict(lambda: dict(w=0, l=0, t=0))
    for game in games:
        home, away = game['home_points'], game['away_points']
        if home is None or away is None:
            continue
        if home == away:
            tally[game['home']]['t'] += 1
            tally[game['away']]['t'] += 1
            continue
        winner = game['home'] if home > away else game['away']
        loser = game['away'] if home > away else game['home']
        tally[winner]['w'] += 1
        tally[loser]['l'] += 1
    return tally


def board(connection, season, model_id, games):
    """The season's rating board: one row a franchise, ranked.

    Ranked here rather than in the page, so every view of this season agrees
    on what #7 is.
    """
    ratings = {row['team_key']: row for row in connection.execute(
        'SELECT team_key, rating, games_counted FROM team_rating'
        ' WHERE season = ? AND model_id = ?', (season, model_id)).fetchall()}
    alignment = {row['code']: row for row in connection.execute(
        'SELECT code, conference, division FROM team_code WHERE season = ?',
        (season,)).fetchall()}
    codes = {}
    for game in games:
        codes.setdefault(game['home'], game.get('home_code')
                         or teams_code(game['home']))
        codes.setdefault(game['away'], game.get('away_code')
                         or teams_code(game['away']))
    tally = records(games)
    rows = []
    for key, rating in ratings.items():
        record = tally.get(key, dict(w=0, l=0, t=0))
        code = codes.get(key, teams_code(key))
        division = alignment.get(code)
        rows.append(dict(
            team=key,
            code=code,
            conference=division['conference'] if division else None,
            division=division['division'] if division else None,
            # Stored to one decimal: the display rounds to a whole number, and
            # a rounded value must never be what the next calculation reads.
            rating=round(rating['rating'], 1),
            rated_games=rating['games_counted'],
            wins=record['w'], losses=record['l'], ties=record['t'],
        ))
    rows.sort(key=lambda row: (-row['rating'], row['team']))
    for index, row in enumerate(rows, start=1):
        row['rank'] = index
    return rows


def played_teams(games):
    """Franchises with at least one finished, rateable game.

    The denominator for a season in progress. In August every team has none,
    so measuring against the whole league would call a correct export empty.
    """
    seen = set()
    for game in games:
        if not game['rateable'] or game['home_points'] is None:
            continue
        seen.update((game['home'], game['away']))
    return len(seen)


def season_teams(connection, season):
    """How many franchises played in a season, from the games themselves."""
    row = connection.execute(
        'SELECT COUNT(*) FROM (SELECT home_team_key AS k FROM game'
        '  WHERE season = ? UNION SELECT away_team_key FROM game'
        '  WHERE season = ?)', (season, season)).fetchone()
    return row[0]


def coverage_row(connection, season, games, rows, *, complete=True):
    finals = sum(1 for game in games if game['home_points'] is not None)
    return dict(
        season=season,
        season_label=str(season),
        teams=season_teams(connection, season),
        rated_teams=len(rows),
        games=len(games),
        finals=finals,
        rateable=sum(1 for game in games if game['rateable']),
        # Stated by the code that knows when the export was generated, rather
        # than guessed by the gate from the numbers.
        complete=bool(complete),
        played_teams=played_teams(games),
    )


def champions(connection):
    """Who won each Super Bowl and each conference championship game.

    Taken from the games, not from a list typed out here: a champion is the
    team that won the game the archive calls a Super Bowl, so a season still
    being played simply has none.
    """
    rows = connection.execute(
        'SELECT season, season_type, home_team_key, away_team_key,'
        '       home_points, away_points FROM game'
        "  WHERE season_type IN ('superbowl', 'conference')"
        '    AND home_points IS NOT NULL AND away_points IS NOT NULL'
        '  ORDER BY season').fetchall()
    out = {}
    for row in rows:
        if row['home_points'] == row['away_points']:
            # A tied championship game cannot happen under the league's
            # overtime rules, and if the data says it did, naming a winner
            # would be inventing one.
            continue
        winner = (row['home_team_key'] if row['home_points']
                  > row['away_points'] else row['away_team_key'])
        loser = (row['away_team_key'] if row['home_points']
                 > row['away_points'] else row['home_team_key'])
        season = out.setdefault(row['season'],
                                dict(champion=None, runner_up=None,
                                     conference_champions=[]))
        if row['season_type'] == 'superbowl':
            season['champion'] = winner
            season['runner_up'] = loser
        else:
            season['conference_champions'].append(winner)
    for season in out.values():
        season['conference_champions'].sort()
    return out


def _mean(values):
    return sum(values) / len(values) if values else None


def _median(values):
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def conference_history(connection, model_id):
    """Each conference and division's rating, season by season.

    No new data: this is the same `team_rating` rows the one-season board
    reads, grouped by the alignment already stored per season. The college
    side's conference history was built the same way and for the same reason —
    a trend over 25 seasons is the thing a single board cannot show.

    Seasons before the 2002 realignment carry no stored alignment, so they are
    left out rather than having today's eight divisions applied backwards.
    """
    rows = connection.execute(
        'SELECT r.season, r.rating, r.team_key, c.conference, c.division'
        '  FROM team_rating r'
        '  JOIN game g ON (g.home_team_key = r.team_key'
        '                  OR g.away_team_key = r.team_key)'
        '               AND g.season = r.season'
        '  JOIN team_code c ON c.season = r.season'
        '               AND c.code = (CASE WHEN g.home_team_key = r.team_key'
        '                                  THEN g.home_code ELSE g.away_code'
        '                             END)'
        ' WHERE r.model_id = ? AND c.conference IS NOT NULL'
        ' GROUP BY r.season, r.team_key'
        ' ORDER BY r.season', (model_id,)).fetchall()
    grouped = defaultdict(lambda: defaultdict(list))
    for row in rows:
        grouped[row['season']][(row['conference'], row['division'])].append(
            dict(team=row['team_key'], rating=row['rating']))
    titles = champions(connection)
    out = []
    for season in sorted(grouped):
        if season < ALIGNMENT_FROM:
            continue
        buckets = grouped[season]
        conferences = []
        for conference in CONFERENCES:
            members = [team for (conf, _), teams in buckets.items()
                       if conf == conference for team in teams]
            if not members:
                continue
            divisions = []
            for (conf, division), teams in sorted(buckets.items()):
                if conf != conference:
                    continue
                best = max(teams, key=lambda row: row['rating'])
                divisions.append(dict(
                    division=division, teams=len(teams),
                    mean_rating=round(_mean([t['rating'] for t in teams]), 1),
                    best=best['team'], best_rating=round(best['rating'], 1)))
            best = max(members, key=lambda row: row['rating'])
            conferences.append(dict(
                conference=conference, teams=len(members),
                mean_rating=round(_mean([t['rating'] for t in members]), 1),
                median_rating=round(_median([t['rating']
                                             for t in members]), 1),
                best=best['team'], best_rating=round(best['rating'], 1),
                divisions=divisions))
        season_titles = titles.get(season, {})
        out.append(dict(season=season, conferences=conferences,
                        champion=season_titles.get('champion'),
                        runner_up=season_titles.get('runner_up'),
                        conference_champions=season_titles.get(
                            'conference_champions', [])))
    return out


def coach_rows(connection, model_id, *, season=None):
    """Stored coach-season rows, rounded for display."""
    query = ('SELECT coach_key, coach_name, season, team_key, games, wins,'
             '       losses, ties, postseason_games, postseason_wins,'
             '       expected_wins, wins_above, elo_change FROM coach_season'
             ' WHERE model_id = ?')
    params = [model_id]
    if season is not None:
        query += ' AND season = ?'
        params.append(season)
    query += ' ORDER BY season, wins_above DESC, coach_name'
    out = []
    for row in connection.execute(query, params):
        row = dict(row)
        row['expected_wins'] = round(row['expected_wins'], 2)
        row['wins_above'] = round(row['wins_above'], 2)
        row['elo_change'] = round(row['elo_change'], 1)
        out.append(row)
    return out


def coach_leaderboard(connection, model_id, *, limit=None):
    """Career rows, summed from the stored seasons by `coaches.careers`."""
    rows = careers(coach_rows(connection, model_id))
    out = []
    for row in rows:
        out.append(dict(
            coach=row['coach_key'], name=row['coach_name'],
            seasons=row['seasons'], teams=row['teams'],
            first_season=row['first_season'], last_season=row['last_season'],
            games=row['games'], wins=row['wins'], losses=row['losses'],
            ties=row['ties'], postseason_games=row['postseason_games'],
            postseason_wins=row['postseason_wins'],
            expected_wins=round(row['expected_wins'], 2),
            wins_above=round(row['wins_above'], 2)))
    return out[:limit] if limit else out


def track_record(connection, model_id):
    """The model's measured record, season by season, and against the market.

    Read from `market_season`, which `src/nfl/market.py` writes. A season with
    no stored row is simply absent: this page must never print a Brier it
    computed on the way past.
    """
    rows = [dict(row) for row in connection.execute(
        'SELECT season, rated_games, model_brier, model_accuracy,'
        '       priced_games, model_brier_priced, market_brier,'
        '       market_accuracy, spread_games, ats_wins, ats_losses,'
        '       ats_pushes, model_margin_mae, market_margin_mae'
        '  FROM market_season WHERE model_id = ? ORDER BY season',
        (model_id,))]
    return dict(seasons=rows, totals=market_module.totals(rows) if rows
                else None)


def build(connection, *, model_name=None, seasons=None, now=None):
    """Everything an export writes, and the manifest that describes it.

    Built whole before anything is written, so the gate can refuse it while
    the output directory is still untouched.
    """
    model = model_row(connection, model_name)
    available = [row[0] for row in connection.execute(
        'SELECT DISTINCT season FROM team_rating WHERE model_id = ?'
        ' ORDER BY season', (model['model_id'],)).fetchall()]
    wanted = sorted(set(seasons) & set(available)) if seasons else available
    if not wanted:
        raise ValueError('No season has a stored rating for this model')
    moment = (now or datetime.now(timezone.utc)).replace(microsecond=0)
    in_progress = current_season(moment)
    payloads, coverage = {}, []
    for season in wanted:
        games = season_games(connection, season)
        rows = board(connection, season, model['model_id'], games)
        payload = dict(season=season, board=rows, games=games,
                       coaches=coach_rows(connection, model['model_id'],
                                          season=season))
        # The real field, in the format that season played. None for a season
        # the stored alignment cannot speak for, which the page shows as "not
        # computed" rather than as an empty bracket.
        field = playoff_module.field(rows, games, season)
        if field is not None:
            payload['playoff'] = field
        # The season's statistical leaders, from the people layer. Absent,
        # not empty, for a season the people archive does not cover, so the
        # page can say so rather than draw an empty leaderboard.
        season_leaders = people_export.leaders(connection, season)
        if season_leaders:
            payload['leaders'] = season_leaders
        payloads[season] = payload
        coverage.append(coverage_row(connection, season, games, rows,
                                     complete=season < in_progress))
    history = dict(
        conferences=conference_history(connection, model['model_id']),
        track_record=track_record(connection, model['model_id']),
        coaches=coach_leaderboard(connection, model['model_id']),
        # Every coach-season, so a career opens without the reader having to
        # load the right seasons first. The per-season payload carries the
        # same rows for that season, which is what a season view wants; this
        # file is the one a coach view reads. The page fetches it lazily, on
        # the first tab that needs it, so a reader who came for the scoreboard
        # never downloads it.
        coach_seasons=coach_rows(connection, model['model_id']))
    manifest = dict(
        schema_version=SCHEMA_VERSION,
        sport='nfl',
        sport_label='NFL',
        generated_at=moment.isoformat().replace('+00:00', 'Z'),
        model=model_section(model),
        coverage=coverage,
        latest_season=wanted[-1],
        seasons={},
        files={},
    )
    return manifest, payloads, teams(connection), history


def build_people(connection, manifest):
    """The people files for this export, split at the season in progress.

    The split is read from the manifest's own as-of time rather than from the
    clock again, so the season the coverage rows call in progress and the
    season the people files hold apart are decided by the same instant.
    """
    moment = datetime.fromisoformat(
        manifest['generated_at'].replace('Z', '+00:00'))
    has_people = connection.execute(
        'SELECT 1 FROM player LIMIT 1').fetchone()
    if has_people is None:
        return None
    return people_export.build(connection, now=moment)


def write(manifest, payloads, team_index, history=None, out_dir=OUT_DIR, *,
          gate=check, people=None):
    """Write the export, but only if the gate passes, then check the disk."""
    out_dir = Path(out_dir)
    staged = {}
    for path, (variable, key, payload) in (people or {}).items():
        staged[path] = people_export.render(variable, key, payload)
    for season, payload in payloads.items():
        body = json.dumps(payload, separators=(',', ':'), sort_keys=True)
        text = (f'window.NFL_SEASONS=(window.NFL_SEASONS||{{}});'
                f'window.NFL_SEASONS[{season}]={body};\n')
        staged[f'seasons/{season}.js'] = text
    staged['teams.js'] = ('window.NFL_TEAMS=' + json.dumps(
        team_index, separators=(',', ':'), sort_keys=True) + ';\n')
    staged['history.js'] = ('window.NFL_HISTORY=' + json.dumps(
        history or {}, separators=(',', ':'), sort_keys=True) + ';\n')

    def stamp(text):
        return hashlib.sha256(text.encode('utf-8')).hexdigest()[:10]

    manifest = dict(manifest)
    manifest['seasons'] = {
        str(season):
            f'seasons/{season}.js?v={stamp(staged[f"seasons/{season}.js"])}'
        for season in payloads}
    manifest['files'] = dict(
        teams=f'teams.js?v={stamp(staged["teams.js"])}',
        history=f'history.js?v={stamp(staged["history.js"])}')
    if people:
        def stamped(path):
            return f'{path}?v={stamp(staged[path])}'
        careers = sorted((path for path in people
                          if path.startswith('people/careers/')),
                         key=lambda path: int(path.rsplit('/', 1)[1][:-3]))
        manifest['files']['people'] = dict(
            index=stamped('people/index.js'),
            current=stamped('people/current.js'),
            draft=stamped('people/draft.js'),
            season=people['people/current.js'][2]['season'],
            shards=len(careers),
            careers=[stamped(path) for path in careers])
    reasons = gate(manifest, root=None)
    if reasons:
        raise PublishRefused(reasons)
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, text in staged.items():
        target = out_dir / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding='utf-8')
    (out_dir / MANIFEST).write_text(
        json.dumps(manifest, separators=(',', ':'), sort_keys=True) + '\n',
        encoding='utf-8')
    written = gate(manifest, root=out_dir)
    if written:
        raise PublishRefused(written)
    return manifest


class PublishRefused(Exception):
    """Raised instead of writing, or instead of keeping, a refused export."""

    def __init__(self, reasons):
        super().__init__(render(reasons))
        self.reasons = list(reasons)


def summarise(manifest, out_dir):
    model = manifest['model']
    lines = ['# NFL export', '',
             f'Schema {manifest["schema_version"]}, as of '
             f'{manifest["generated_at"]}, written to `{out_dir}`.', '',
             f'Model `{model["name"]}` fitted on {model["fitted_on"]}, held '
             f'out {model["held_out"]}, holdout Brier {model["holdout_brier"]}'
             f' on {model["holdout_games"]} games.', '',
             '| season | teams | rated | games | finals |',
             '| --- | --- | --- | --- | --- |']
    for row in manifest['coverage']:
        lines.append(f'| {row["season_label"]} | {row["teams"]} | '
                     f'{row["rated_teams"]} | {row["games"]} | '
                     f'{row["finals"]} |')
    lines += ['', 'Publish gate PASSED.']
    return '\n'.join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Export the NFL archive, if the gate allows it')
    parser.add_argument('--out', default=str(OUT_DIR))
    parser.add_argument('--database', default=None)
    parser.add_argument('--model', default=None,
                        help='model name; the newest stored model by default')
    parser.add_argument('--seasons', default=None,
                        help='comma-separated seasons; every rated season by '
                             'default')
    args = parser.parse_args(argv)
    seasons = ([int(part) for part in args.seasons.split(',') if part.strip()]
               if args.seasons else None)
    connection = connect(args.database)
    try:
        manifest, payloads, team_index, history = build(
            connection, seasons=seasons, model_name=args.model)
    finally:
        connection.close()
    connection = connect(args.database)
    try:
        people = build_people(connection, manifest)
    finally:
        connection.close()
    try:
        manifest = write(manifest, payloads, team_index, history,
                         Path(args.out), people=people)
    except PublishRefused as refused:
        print(str(refused))
        return 1
    print(summarise(manifest, args.out))
    return 0


if __name__ == '__main__':
    sys.exit(main())
