"""Export the basketball archive as static files, with a manifest and a gate.

Release gate 4's second half. Writes `ui/data/ncaambb/` and nothing else: no
football file, no football manifest entry, no shared global. A reader of these
files can always answer three questions before it draws a number — which model
made it, what that model scored on games it had never seen, and how much of the
country is in here — because the manifest is refused unless it can.

The gate in `src/ncaambb/publish_gate.py` runs BEFORE the first byte is
written. An export that would fail it is not written at all, rather than written
and then rejected, because a half-written export directory is itself a way to
publish a sample.

This does not commit and does not deploy. Whether these files are served is
decided by release gate 6, which does not exist yet.
"""

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from . import export_people
from .elo import SCALE, expected_home
from .identity import current_season
from .publish_gate import SCHEMA_VERSION, check, render
from .store import connect

OUT_DIR = Path('ui/data/ncaambb')
MANIFEST = 'manifest.json'


def _write_js(path, global_expr, payload):
    """Write a payload as an assignment, the convention football already uses.

    Returns the ten-character version stamp the manifest carries.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(payload, separators=(',', ':'), sort_keys=True)
    path.write_text(f'{global_expr}={body};\n', encoding='utf-8')
    return hashlib.sha256(path.read_bytes()).hexdigest()[:10]


def model_row(connection, name=None):
    """The newest stored model, or the one named.

    An export states the model it read; it never averages two, and it never
    guesses when the database holds several.
    """
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


def model_section(row):
    """What the manifest says about the model, including the hash of its exact
    configuration, so two exports claiming one model can be compared."""
    config = json.loads(row['config'])
    canonical = json.dumps(config, sort_keys=True,
                           separators=(',', ':')).encode('utf-8')
    return dict(
        name=row['name'],
        config=config,
        config_sha256=hashlib.sha256(canonical).hexdigest(),
        fitted_on=json.loads(row['fitted_on'] or '[]'),
        held_out=json.loads(row['held_out'] or '[]'),
        holdout_brier=row['holdout_brier'],
        holdout_games=row['holdout_games'],
        built_at=row['created_at'],
    )


def teams(connection):
    """Every team any exported season refers to, Division I or not.

    A non-Division I opponent is named here because a Division I team's game
    against it is shown; it carries no rating and no board row.
    """
    rows = connection.execute(
        'SELECT team_key, school, mascot, abbreviation, display_name,'
        '       short_name, primary_color, secondary_color, logo_id'
        '  FROM team ORDER BY team_key').fetchall()
    return {row['team_key']: dict(
        name=row['display_name'] or row['school'],
        school=row['school'],
        mascot=row['mascot'],
        abbr=row['abbreviation'],
        short=row['short_name'],
        colors=[row['primary_color'], row['secondary_color']],
        # The id of this school's mark, NOT a path: the files are named from
        # it (`assets/ncaambb/2561.png`), the page has one place that builds
        # that path, and a school with no mark carries no key at all rather
        # than a null the page would have to test for separately.
        **({'logo': row['logo_id']} if row['logo_id'] else {}),
    ) for row in rows}


def data_as_of(connection, season):
    """When the provider last answered about this season's schedule.

    NOT when the export was built. A run that reloads a committed cache
    because the provider refused builds a perfectly good export over data
    that may be weeks old, and `generated_at` cannot say so: it is the build
    time and it is always now. A reader's scoreboard depends on the season
    being played, so that season's schedule snapshot is the one that decides
    how fresh the page may claim to be.

    None when no snapshot records it, which is true of any cache written
    before the ingestion began recording it. An unknown age is reported as
    unknown; the page then says 'last updated: unknown' and marks it stale,
    which is the honest answer and the safe one.
    """
    row = connection.execute(
        'SELECT data_fetched_at FROM snapshot'
        " WHERE kind = 'schedule' AND season = ?"
        # Belt and braces: DESC already sorts SQLite's NULLs last, and a
        # season whose every row is NULL returns NULL either way. The clause
        # states the intent so a later change to the ordering cannot start
        # returning an unrecorded row as the answer.
        ' AND data_fetched_at IS NOT NULL'
        ' ORDER BY data_fetched_at DESC LIMIT 1', (season,)).fetchone()
    return row['data_fetched_at'] if row else None


def season_games(connection, season, model_id=None, config=None):
    """One season's games, in tip-off order, with the model's own chain.

    `model_id` adds what the model knew before tip-off and what the result did
    to it. Without one the games come out as they always did, which is what a
    caller wanting the schedule alone asks for.

    TWO DIFFERENT NUMBERS SHARE THESE FIELDS, and the difference is the game's
    own status. A FINAL game carries the STORED chain: the ratings the model
    held before tip-off, never recomputed, so a 2015 game shows what the model
    believed in 2015. A game NOT YET PLAYED has no stored chain at all --
    `rateable` requires a final score, so the replay never saw it -- and
    carries the ratings the clubs hold NOW, with the expectation those imply.
    A page must say which it is showing, and the one on a game page does.
    """
    rows = connection.execute(
        'SELECT game_key, provider_id, season_type, tournament, status,'
        '       start_date_utc, playing_date, start_at_utc, start_time_tbd,'
        '       neutral_site, conference_game,'
        '       home_team_key, away_team_key, home_points, away_points,'
        '       venue, city, state, rateable, division_one'
        '  FROM game WHERE season = ?'
        # Ordered by the instant, so two games on one night come out in the
        # order they were played rather than in the order of their keys.
        ' ORDER BY COALESCE(start_at_utc, playing_date, start_date_utc),'
        ' game_key',
        (season,)).fetchall()
    # The stored per-game chain, not a number recomputed from today's ratings:
    # a 2015 game shows what the model believed in 2015. `counted = 0` is a
    # game the model declined to learn from, so its change is a real zero.
    chain, standing = {}, {}
    if model_id is not None:
        chain = {row['game_key']: row for row in connection.execute(
            'SELECT r.game_key, r.home_pregame, r.away_pregame,'
            '       r.home_expected, r.home_postgame, r.away_postgame,'
            '       r.rating_change, r.counted'
            '  FROM rating_game r JOIN game g ON g.game_key = r.game_key'
            ' WHERE g.season = ? AND r.model_id = ?', (season, model_id))}
        # Where the clubs stand now, for the games still to be played. Without
        # this an upcoming game had no win chance at all, which is the one
        # place a reader most wants one.
        standing = {row['team_key']: row['rating'] for row in
                    connection.execute(
                        'SELECT team_key, rating FROM team_rating'
                        ' WHERE season = ? AND model_id = ?',
                        (season, model_id))}
    out = []
    for row in rows:
        game = dict(
            id=row['game_key'],
            # The night it is played, not the UTC date. A game with no
            # Eastern date stored is one loaded before schema 3; falling back
            # to the UTC date keeps it on the scoreboard rather than dropping
            # it, and a reload puts it right.
            at=row['playing_date'] or row['start_date_utc'],
            status=row['status'],
            home=row['home_team_key'],
            away=row['away_team_key'],
            # A missing score stays missing. Football taught this project what
            # a zero standing in for "no score" costs downstream.
            home_points=row['home_points'],
            away_points=row['away_points'],
            type=row['season_type'],
            d1=row['division_one'],
            rateable=row['rateable'],
        )
        if row['neutral_site']:
            game['neutral'] = 1
        if row['start_time_tbd']:
            game['time_tbd'] = 1
        # The tip-off time, as an instant, only when one has been decided. A
        # reader's browser turns it into their own local time; the export does
        # not pick a timezone on their behalf.
        if row['start_at_utc'] and not row['start_time_tbd']:
            game['tip'] = row['start_at_utc']
        if row['conference_game']:
            game['conference_game'] = 1
        if row['tournament']:
            game['tournament'] = row['tournament']
        if row['venue']:
            game['venue'] = row['venue']
        if row['city']:
            game['city'] = row['city']
        if row['state']:
            game['state'] = row['state']
        stored = chain.get(row['game_key'])
        if stored is not None:
            # Named as college football names them, so one game page can read
            # either league's export without a translation table in the middle.
            game['home_elo'] = round(stored['home_pregame'], 1)
            game['away_elo'] = round(stored['away_pregame'], 1)
            game['home_wp'] = round(stored['home_expected'], 4)
            if stored['counted']:
                game['home_elo_post'] = round(stored['home_postgame'], 1)
                game['away_elo_post'] = round(stored['away_postgame'], 1)
                game['elo_change'] = round(stored['rating_change'], 2)
        elif row['status'] != 'final' and config:
            # Only for a game NOT played. A final game the replay skipped --
            # one side is not Division I -- is left with no numbers rather
            # than given today's ratings, which would read as a pregame
            # expectation the model never formed.
            home = standing.get(row['home_team_key'])
            away = standing.get(row['away_team_key'])
            if home is not None and away is not None:
                game['home_elo'] = round(home, 1)
                game['away_elo'] = round(away, 1)
                game['home_wp'] = round(expected_home(
                    home, away, neutral=bool(row['neutral_site']),
                    home_advantage=config['home_advantage'],
                    scale=config.get('scale', SCALE)), 4)
                # Said out loud, so a reader of the export cannot mistake a
                # live number for a stored one.
                game['wp_live'] = 1
        out.append(game)
    return out


def records(games):
    """Each team's won-lost, from finals only, counting every opponent.

    Two records are kept: every game played, and Division I games alone. The
    football header taught this project that a reader expects the record they
    see quoted elsewhere, which includes the non-Division I games, while a
    rating only ever learned from the Division I ones.
    """
    tally = defaultdict(lambda: dict(w=0, l=0, d1w=0, d1l=0))
    for game in games:
        if game['status'] != 'final':
            continue
        home, away = game['home_points'], game['away_points']
        if home is None or away is None or home == away:
            continue
        winner = game['home'] if home > away else game['away']
        loser = game['away'] if home > away else game['home']
        tally[winner]['w'] += 1
        tally[loser]['l'] += 1
        if game['d1']:
            tally[winner]['d1w'] += 1
            tally[loser]['d1l'] += 1
    return tally


def board(connection, season, model_id, games):
    """The season's rating board: one row a Division I team, ranked.

    Ranked here rather than in the page, so every view of this season agrees on
    what #7 is. A team with no rating is absent from the board rather than
    shown at the bottom with a blank.
    """
    memberships = {row['team_key']: row for row in connection.execute(
        'SELECT team_key, conference, conference_id, division_one'
        '  FROM team_season WHERE season = ?', (season,)).fetchall()}
    ratings = {row['team_key']: row for row in connection.execute(
        'SELECT team_key, rating, games_counted FROM team_rating'
        ' WHERE season = ? AND model_id = ?', (season, model_id)).fetchall()}
    tally = records(games)
    rows = []
    for key, membership in memberships.items():
        if not membership['division_one'] or key not in ratings:
            continue
        rating = ratings[key]
        record = tally.get(key, dict(w=0, l=0, d1w=0, d1l=0))
        rows.append(dict(
            team=key,
            conference=membership['conference'],
            conference_id=membership['conference_id'],
            # Stored to one decimal: the display rounds to a whole number, and
            # a rounded value must never be what the next calculation reads.
            rating=round(rating['rating'], 1),
            rated_games=rating['games_counted'],
            wins=record['w'], losses=record['l'],
            d1_wins=record['d1w'], d1_losses=record['d1l'],
        ))
    rows.sort(key=lambda row: (-row['rating'], row['team']))
    for index, row in enumerate(rows, start=1):
        row['rank'] = index
    return rows


def played_teams(games):
    """Division I teams with at least one finished, rateable game.

    The denominator for the rated fraction of a season still in progress. On
    opening night almost every team has none, so measuring against the whole
    Division I team list would call a correct export 0% rated.
    """
    seen = set()
    for game in games:
        if game['status'] != 'final' or not game['rateable']:
            continue
        seen.update((game['home'], game['away']))
    return len(seen)


def coverage_row(connection, season, games, rows, *, complete=True):
    division_one = connection.execute(
        'SELECT COUNT(*) FROM team_season'
        ' WHERE season = ? AND division_one = 1', (season,)).fetchone()[0]
    finals = sum(1 for game in games if game['status'] == 'final')
    return dict(
        season=season,
        season_label=connection.execute(
            'SELECT season_label FROM game WHERE season = ? LIMIT 1',
            (season,)).fetchone()[0],
        division_one_teams=division_one,
        rated_teams=len(rows),
        games=len(games),
        finals=finals,
        rateable=sum(1 for game in games if game['rateable']),
        # The gate judges a finished season on a full season's worth of games
        # and a rated fraction of the whole Division I list. Neither question
        # can be asked of a season in progress: in November a correct export
        # has a few hundred games and most teams unrated. Stated here, by the
        # code that knows when the export was generated, rather than guessed
        # by the gate from the numbers.
        complete=bool(complete),
        played_teams=played_teams(games),
    )


def build(connection, *, model_name=None, seasons=None, now=None,
          people_seasons=None):
    """Everything an export writes, and the manifest that describes it.

    Built whole before anything is written, so the gate can refuse it while the
    output directory is still untouched.
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
        # The stored configuration IS the elo configuration -- see
        # build_ratings.record_rating_model -- so the expectation an upcoming
        # game carries is computed with the same home advantage and scale the
        # replay used, not with a default standing in for them.
        games = season_games(connection, season, model['model_id'],
                             config=json.loads(model['config']))
        rows = board(connection, season, model['model_id'], games)
        payloads[season] = dict(season=season, board=rows, games=games)
        coverage.append(coverage_row(connection, season, games, rows,
                                     complete=season < in_progress))
    # The people layer covers only the seasons whose rosters and box scores
    # have been fetched, which is fewer than the seasons that have ratings.
    # An absent season is said so in the manifest rather than guessed at by a
    # page that finds no file.
    people, people_summary = export_people.payloads(
        connection, wanted, limit=people_seasons)
    manifest = dict(
        schema_version=SCHEMA_VERSION,
        sport='ncaambb',
        sport_label="Men's Basketball",
        generated_at=moment.isoformat().replace('+00:00', 'Z'),
        # The age of the DATA, which is what a reader's badge is about. See
        # data_as_of above for why generated_at cannot answer that.
        data_as_of=data_as_of(connection, wanted[-1]),
        model=model_section(model),
        coverage=coverage,
        latest_season=wanted[-1],
        seasons={},
        files={},
        people_summary=people_summary,
    )
    return manifest, payloads, teams(connection), people


def write(manifest, payloads, team_index, out_dir=OUT_DIR, *, people=None,
          gate=check):
    """Write the export, but only if the gate passes.

    The manifest is completed first — the files it names, with the hash each
    will have — so the gate sees the same manifest a reader will. Nothing is
    written until it passes, and the gate is run again on what was written,
    because a check against a plan is not a check against a disk.
    """
    out_dir = Path(out_dir)
    staged = {}
    for season, payload in payloads.items():
        body = json.dumps(payload, separators=(',', ':'), sort_keys=True)
        text = (f'window.NCAAMBB_SEASONS=(window.NCAAMBB_SEASONS||{{}});'
                f'window.NCAAMBB_SEASONS[{season}]={body};\n')
        staged[f'seasons/{season}.js'] = text
    staged['teams.js'] = ('window.NCAAMBB_TEAMS=' + json.dumps(
        team_index, separators=(',', ':'), sort_keys=True) + ';\n')
    for path, (global_expr, payload) in (people or {}).items():
        staged[path] = export_people.render(global_expr, payload)

    def stamp(text):
        return hashlib.sha256(text.encode('utf-8')).hexdigest()[:10]

    manifest = dict(manifest)
    manifest['seasons'] = {
        str(season): f'seasons/{season}.js?v={stamp(staged[f"seasons/{season}.js"])}'
        for season in payloads}
    manifest['files'] = dict(
        teams=f'teams.js?v={stamp(staged["teams.js"])}')
    if people:
        manifest['files']['people'] = export_people.manifest_section(
            people, lambda path: stamp(staged[path]))
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
    lines = [f'# Basketball export', '',
             f'Schema {manifest["schema_version"]}, as of '
             f'{manifest["generated_at"]}, written to `{out_dir}`.', '',
             f'Model `{model["name"]}` fitted on {model["fitted_on"]}, held out '
             f'{model["held_out"]}, holdout Brier {model["holdout_brier"]} on '
             f'{model["holdout_games"]} games.', '',
             '| season | D-I teams | rated | games | finals | rateable |',
             '| --- | --- | --- | --- | --- | --- |']
    for row in manifest['coverage']:
        lines.append(f'| {row["season_label"]} | {row["division_one_teams"]} | '
                     f'{row["rated_teams"]} | {row["games"]} | '
                     f'{row["finals"]} | {row["rateable"]} |')
    people = manifest.get('people_summary') or []
    if people:
        lines += ['', '| season | players | clubs | game lines |',
                  '| --- | --- | --- | --- |']
        for row in people:
            lines.append(f'| {row["season"]} | {row["players"]} | '
                         f'{row["clubs"]} | {row["lines"]} |')
    else:
        lines += ['', 'No season in this export has people data; the People '
                      'tab will say so rather than draw an empty panel.']
    lines += ['', 'Publish gate PASSED; this export describes national '
                  'coverage of a model scored on a season it never saw.']
    return '\n'.join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Export the basketball archive, if the gate allows it')
    parser.add_argument('--out', default=str(OUT_DIR))
    parser.add_argument('--model', default=None,
                        help='model name; the newest stored model by default')
    parser.add_argument('--seasons', default=None,
                        help='comma-separated ending years; every rated '
                             'season by default')
    parser.add_argument('--people-seasons', type=int, default=None,
                        help='how many of the most recent seasons get player '
                             f'files (default {export_people.DEFAULT_SEASONS}, '
                             '0 for every season that has people). A season is '
                             'around 127,500 player-game lines, so this is '
                             'what keeps the published payload bounded.')
    args = parser.parse_args(argv)
    seasons = ([int(part) for part in args.seasons.split(',') if part.strip()]
               if args.seasons else None)
    connection = connect()
    try:
        manifest, payloads, team_index, people = build(
            connection, seasons=seasons, model_name=args.model,
            people_seasons=args.people_seasons)
    finally:
        connection.close()
    try:
        manifest = write(manifest, payloads, team_index, Path(args.out),
                         people=people)
    except PublishRefused as refused:
        print(str(refused))
        return 1
    print(summarise(manifest, args.out))
    return 0


if __name__ == '__main__':
    sys.exit(main())
