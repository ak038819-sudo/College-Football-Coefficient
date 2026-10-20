"""Replay the basketball archive with the fitted model and store the result.

Reads the configuration from `config/ncaambb_model.json`, replays every
rateable game in date order, and writes three things: the model and what it
scored on the season it was held out from, each team's rating at the end of
each season, and the per-game chain behind those ratings.

The configuration is NOT hardcoded here. `src/ncaambb/fit_elo.py` chooses it
and this writes what it chose, so changing the model is editing a JSON file
and re-running, never editing this.

Nothing here publishes. It writes `db/ncaambb.db` and makes no commit; the
B04 export gate does not exist yet.
"""

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from .elo import replay, score
from .fit_elo import load_games
from .store import (connect, record_rating_model, record_snapshot,
                    upsert_rating_games, upsert_team_ratings)

CONFIG_PATH = Path('config/ncaambb_model.json')
MODEL_NAME = 'ncaambb_elo_v1'
REQUIRED = ('k', 'home_advantage', 'offseason_retention', 'margin_model',
            'reference_margin')


def load_config(path=None):
    """Read the fitted model. Resolved inside the function, never as a default.

    A missing key is refused rather than defaulted: a silent default is how a
    parameter nobody fitted ends up in a published rating.
    """
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


def season_ratings(rows, final_ratings):
    """Each team's rating at the end of each season, from the game chain.

    Taken from the chain rather than from the replay's running totals, because
    the chain is what is stored and auditable: a team's last counted game of a
    season carries its end-of-season rating.
    """
    latest, counted = {}, Counter()
    for row in rows:
        if not row['counted']:
            continue
        for side in ('home', 'away'):
            key = (row[f'{side}_team_key'], row['season'])
            latest[key] = row[f'{side}_postgame']
            counted[key] += 1
    return [(team, season, rating, counted[(team, season)])
            for (team, season), rating in sorted(latest.items())]


def build(connection, config, *, model_name=MODEL_NAME, report=None,
          revision=None, snapshot_id=None):
    games = load_games(connection)
    if not games:
        raise ValueError('No rateable game is loaded; run the ingestion first')
    elo = config['elo']
    rows, final = replay(games, elo)
    model_id = record_rating_model(
        connection, model_name, elo,
        fitted_on=(report or {}).get('fit_seasons'),
        held_out=(report or {}).get('holdout_seasons'),
        holdout_brier=(report or {}).get('holdout', {}).get('brier'),
        holdout_games=(report or {}).get('holdout', {}).get('games'),
        snapshot_id=snapshot_id)
    ratings = season_ratings(rows, final)
    stored_ratings = upsert_team_ratings(connection, model_id, ratings)
    stored_games = upsert_rating_games(connection, model_id, rows)
    seasons = sorted({row['season'] for row in rows})
    return dict(model_id=model_id, model_name=model_name, seasons=seasons,
                team_seasons=stored_ratings, games=stored_games,
                counted=sum(1 for row in rows if row['counted']),
                declined=sum(1 for row in rows if not row['counted']),
                # `score` also reports a game count, which is the counted
                # games again under a name that would shadow the stored-row
                # count above. Dropped rather than renamed at the call site.
                **{key: value for key, value in score(rows).items()
                   if key not in ('calibration', 'games')})


def leaders(connection, model_id, season, *, limit=10):
    return connection.execute(
        'SELECT t.school, r.rating, r.games_counted FROM team_rating r'
        '  JOIN team t USING (team_key)'
        ' WHERE r.model_id = ? AND r.season = ?'
        ' ORDER BY r.rating DESC LIMIT ?', (model_id, season, limit)).fetchall()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', default=None, help='Defaults to db/ncaambb.db')
    parser.add_argument('--config', default=None,
                        help=f'Defaults to {CONFIG_PATH}')
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
        print(f'Model {summary["model_name"]} (id {summary["model_id"]})')
        print(f'Seasons {summary["seasons"][0]}-{summary["seasons"][-1]}:'
              f' {summary["counted"]} games counted,'
              f' {summary["declined"]} declined,'
              f' {summary["team_seasons"]} team-seasons rated')
        print(f'In-sample Brier {summary["brier"]:.5f},'
              f' log loss {summary["log_loss"]:.5f},'
              f' accuracy {summary["accuracy"]:.4f}')
        print('\nTop of the latest season:')
        for row in leaders(connection, summary['model_id'],
                           summary['seasons'][-1]):
            print(f'  {row["school"]:<28} {row["rating"]:7.1f}'
                  f'  ({row["games_counted"]} games)')
    except ValueError as exc:
        print(f'Cannot build ratings: {exc}', file=sys.stderr)
        return 1
    finally:
        connection.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
