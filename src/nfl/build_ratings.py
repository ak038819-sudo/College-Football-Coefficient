"""Replay the NFL archive with the fitted model and store the result.

Reads the configuration from `config/nfl_model.json`, replays every rateable
game in kickoff order, and writes three things: the model and what it scored
on seasons it was held out from, each franchise's rating at the end of each
season, and the per-game chain behind those ratings.

The configuration is NOT hardcoded here. `src/nfl/fit_elo.py` chooses it and
this writes what it chose, so changing the model is editing a JSON file and
re-running, never editing this file.

The held-out claim is committed beside the parameters and pinned to them by
hash. A refresh fits nothing — it replays the committed parameters on a fresh
runner where no fit report exists — so without the committed claim the export
gate would refuse every refresh, which is how the basketball refresh shipped
unable to publish anything.
"""

import argparse
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from .elo import replay, score
from .fit_elo import load_games
from .store import (connect, record_rating_model, record_snapshot,
                    upsert_rating_games, upsert_team_ratings)

CONFIG_PATH = Path('config/nfl_model.json')
MODEL_NAME = 'nfl_elo_v1'
REQUIRED = ('k', 'home_advantage', 'offseason_retention', 'margin_model',
            'reference_margin')
# The NFL is the hardest of the three leagues to predict and its own home
# teams win only about 55% of the time, so a coin flip (0.25) is a weak thing
# to have to beat. The floor is the home-base-rate baseline instead, measured
# at 0.24715 on 2021-2025: a model that cannot beat "the home team wins" has
# no business being published as a rating.
MAX_HOLDOUT_BRIER = 0.245


def load_config(path=None):
    """Read the fitted model. Resolved inside the function, never as a default."""
    target = Path(path) if path is not None else CONFIG_PATH
    payload = json.loads(target.read_text(encoding='utf-8'))
    elo = payload.get('elo')
    if not isinstance(elo, dict):
        raise ValueError(f'{target} has no "elo" section')
    missing = [key for key in REQUIRED if key not in elo]
    if missing:
        raise ValueError(f'{target} is missing fitted parameters: {missing}')
    if elo['margin_model'] != 'result_only' and elo['reference_margin'] <= 0:
        raise ValueError('reference_margin must be the measured mean winning '
                         'margin, not a placeholder')
    return payload


def elo_digest(elo):
    """The hash of the exact parameters, canonically."""
    canonical = json.dumps(elo, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def check_validation(payload, target='the configuration'):
    """Refuse a configuration whose held-out claim is missing or stale."""
    validation = payload.get('validation')
    if not isinstance(validation, dict):
        raise ValueError(
            f'{target} has no "validation" section. A published rating must '
            'say what the model scored on held-out seasons, and a replay has '
            'no fit report to take it from.')
    fitted = list(validation.get('fit_seasons') or [])
    held = list(validation.get('holdout_seasons') or [])
    if not held:
        raise ValueError(f'{target} names no held-out season')
    leaked = sorted(set(fitted) & set(held))
    if leaked:
        raise ValueError(f'{target} both fits on and holds out {leaked}; '
                         'the holdout leaked')
    holdout = validation.get('holdout')
    if not isinstance(holdout, dict):
        raise ValueError(f'{target} has no held-out measurement')
    brier, games = holdout.get('brier'), holdout.get('games')
    if not isinstance(brier, (int, float)) or isinstance(brier, bool) \
            or not 0 < brier < MAX_HOLDOUT_BRIER:
        raise ValueError(
            f'{target} reports a held-out Brier of {brier!r}, which is '
            f'missing or no better than picking the home team '
            f'(the floor is {MAX_HOLDOUT_BRIER})')
    if not isinstance(games, int) or isinstance(games, bool) or games <= 0:
        raise ValueError(f'{target} reports {games!r} held-out games')
    measured = validation.get('measured_on_elo_sha256')
    actual = elo_digest(payload['elo'])
    if measured != actual:
        raise ValueError(
            f'{target} carries a validation measured on parameters whose hash '
            f'is {measured!r}, but its "elo" section hashes to {actual}. '
            'Re-fit and re-score, or the published rating would claim a score '
            'the committed parameters never earned.')
    return validation


def report_from_config(payload):
    """The committed validation in the shape `build()` wants a report in."""
    validation = check_validation(payload)
    return dict(fit_seasons=list(validation.get('fit_seasons') or []),
                holdout_seasons=list(validation['holdout_seasons']),
                holdout=dict(validation['holdout']))


def season_ratings(rows, final_ratings):
    """Each franchise's rating at the end of each season, from the chain."""
    latest, counted = {}, Counter()
    for row in rows:
        if not row['counted']:
            continue
        for side in ('home', 'away'):
            key = (row[f'{side}_team_key'], row['season'])
            latest[key] = row[f'{side}_postgame']
            counted[key] += 1
    return [dict(team_key=team, season=season, rating=rating,
                 games_counted=counted[(team, season)])
            for (team, season), rating in sorted(latest.items())]


def build(connection, config, *, model_name=MODEL_NAME, report=None,
          snapshot_id=None):
    games = load_games(connection)
    if not games:
        raise ValueError('No rateable game is loaded; run the ingestion first')
    elo = config['elo']
    # A fit's own report wins when there is one. Without one this is a replay,
    # and the committed validation is the only honest source for the score.
    if report is None:
        report = report_from_config(config)
    rows, final = replay(games, elo)
    model_id = record_rating_model(
        connection, model_name, elo,
        fitted_on=json.dumps(report.get('fit_seasons')),
        held_out=json.dumps(report.get('holdout_seasons')),
        holdout_brier=report.get('holdout', {}).get('brier'),
        holdout_games=report.get('holdout', {}).get('games'),
        snapshot_id=snapshot_id)
    ratings = season_ratings(rows, final)
    stored_ratings = upsert_team_ratings(connection, model_id, ratings)
    stored_games = upsert_rating_games(connection, model_id, rows)
    seasons = sorted({row['season'] for row in rows})
    return dict(model_id=model_id, model_name=model_name, seasons=seasons,
                team_seasons=stored_ratings, games=stored_games,
                counted=sum(1 for row in rows if row['counted']),
                declined=sum(1 for row in rows if not row['counted']),
                **{key: value for key, value in score(rows).items()
                   if key not in ('calibration', 'games')})


def leaders(connection, model_id, season, *, limit=10):
    return connection.execute(
        'SELECT t.name, r.rating, r.games_counted FROM team_rating r'
        '  JOIN team t USING (team_key)'
        ' WHERE r.model_id = ? AND r.season = ?'
        ' ORDER BY r.rating DESC LIMIT ?', (model_id, season, limit)).fetchall()


def main(argv=None):
    parser = argparse.ArgumentParser(description='Replay the NFL archive')
    parser.add_argument('--database', default=None, help='Defaults to db/nfl.db')
    parser.add_argument('--config', default=None, help=f'Defaults to {CONFIG_PATH}')
    parser.add_argument('--report', default=None,
                        help="The fit's JSON report, so the stored model "
                             'carries what it scored held out')
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f'Cannot read the model configuration: {exc}', file=sys.stderr)
        return 1
    report = None
    if args.report:
        try:
            report = json.loads(Path(args.report).read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError) as exc:
            print(f'Cannot read the fit report: {exc}', file=sys.stderr)
            return 1
    connection = connect(args.database)
    try:
        snapshot_id = record_snapshot(
            connection, 'ratings', datetime.now(timezone.utc).isoformat(),
            notes=f'replayed with {MODEL_NAME}')
        summary = build(connection, config, report=report,
                        snapshot_id=snapshot_id)
    except ValueError as exc:
        print(f'Cannot build ratings: {exc}', file=sys.stderr)
        return 1
    finally:
        connection.close()
    print(f'{summary["games"]} games replayed, {summary["counted"]} counted, '
          f'{summary["declined"]} declined; {summary["team_seasons"]} '
          f'team-seasons across {summary["seasons"][0]}-'
          f'{summary["seasons"][-1]}')
    print(f'in-sample Brier {summary["brier"]:.5f}, '
          f'accuracy {summary["accuracy"]:.4f}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
