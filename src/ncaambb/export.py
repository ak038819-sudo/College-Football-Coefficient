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
        '       short_name, primary_color, secondary_color'
        '  FROM team ORDER BY team_key').fetchall()
    return {row['team_key']: dict(
        name=row['display_name'] or row['school'],
        school=row['school'],
        mascot=row['mascot'],
        abbr=row['abbreviation'],
        short=row['short_name'],
        colors=[row['primary_color'], row['secondary_color']],
    ) for row in rows}


def season_games(connection, season):
    rows = connection.execute(
        'SELECT game_key, provider_id, season_type, tournament, status,'
        '       start_date_utc, start_time_tbd, neutral_site, conference_game,'
        '       home_team_key, away_team_key, home_points, away_points,'
        '       venue, city, state, rateable, division_one'
        '  FROM game WHERE season = ? ORDER BY start_date_utc, game_key',
        (season,)).fetchall()
    out = []
    for row in rows:
        game = dict(
            id=row['game_key'],
            at=row['start_date_utc'],
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


def build(connection, *, model_name=None, seasons=None, now=None):
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
        games = season_games(connection, season)
        rows = board(connection, season, model['model_id'], games)
        payloads[season] = dict(season=season, board=rows, games=games)
        coverage.append(coverage_row(connection, season, games, rows,
                                     complete=season < in_progress))
    manifest = dict(
        schema_version=SCHEMA_VERSION,
        sport='ncaambb',
        sport_label="Men's Basketball",
        generated_at=moment.isoformat().replace('+00:00', 'Z'),
        model=model_section(model),
        coverage=coverage,
        latest_season=wanted[-1],
        seasons={},
        files={},
    )
    return manifest, payloads, teams(connection)


def write(manifest, payloads, team_index, out_dir=OUT_DIR, *, gate=check):
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

    def stamp(text):
        return hashlib.sha256(text.encode('utf-8')).hexdigest()[:10]

    manifest = dict(manifest)
    manifest['seasons'] = {
        str(season): f'seasons/{season}.js?v={stamp(staged[f"seasons/{season}.js"])}'
        for season in payloads}
    manifest['files'] = dict(
        teams=f'teams.js?v={stamp(staged["teams.js"])}')
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
    args = parser.parse_args(argv)
    seasons = ([int(part) for part in args.seasons.split(',') if part.strip()]
               if args.seasons else None)
    connection = connect()
    try:
        manifest, payloads, team_index = build(connection, seasons=seasons,
                                               model_name=args.model)
    finally:
        connection.close()
    try:
        manifest = write(manifest, payloads, team_index, Path(args.out))
    except PublishRefused as refused:
        print(str(refused))
        return 1
    print(summarise(manifest, args.out))
    return 0


if __name__ == '__main__':
    sys.exit(main())
