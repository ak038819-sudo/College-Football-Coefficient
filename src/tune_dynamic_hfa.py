#!/usr/bin/env python3
"""Reproducible walk-forward search for a centered, team-specific Elo bonus.

Each candidate replays the complete Elo history; only its Brier/log loss in
2018-22 selects a candidate. 2023-25 is reported separately, 2026 separately
because it is still in progress. This is research, not a production switch.
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
from collections import defaultdict
from pathlib import Path

from build_elo import fetch_games_chronological, load_config, run_elo
from compare_dynamic_hfa import verify_flat_reference
from dynamic_hfa import home_games_from_flat, pregame_bonus_provider
from predict_upcoming import performance_layer


def score(games, rows):
    totals = defaultdict(lambda: [0, 0.0, 0.0])
    for game, row in zip(games, rows[::2]):
        year = game['season_year']
        if year < 2018:
            continue
        actual = 1.0 if game['home_score'] > game['away_score'] else (
            0.0 if game['home_score'] < game['away_score'] else 0.5)
        p = max(1e-12, min(1 - 1e-12, row[4]))
        region = 'train' if year <= 2022 else 'validation' if year <= 2025 else 'current'
        for key in (str(year), region):
            rec = totals[key]
            rec[0] += 1
            rec[1] += (p - actual) ** 2
            rec[2] -= actual * math.log(p) + (1 - actual) * math.log(1 - p)
    return {key: {'games': n, 'brier': round(b / n, 6), 'log_loss': round(ll / n, 6)}
            for key, (n, b, ll) in totals.items() if n}


def tune(conn, config_path, half_lives=(10.0,), shrinkages=(30.0,),
         alphas=(0.25, 0.5, 1.0), caps=(40.0, 80.0)):
    if (not all((half_lives, shrinkages, alphas, caps)) or
            any(v <= 0 for v in half_lives) or any(v < 0 for v in shrinkages) or
            any(not 0 <= v <= 1 for v in alphas) or any(v < 0 for v in caps)):
        raise ValueError('Use positive half-lives, nonnegative shrinkage/caps, and alphas in [0,1]')
    raw = load_config(config_path)
    cfg = raw['elo']
    conn.row_factory = sqlite3.Row
    games = fetch_games_chronological(conn)
    layer, success_rates = performance_layer(conn, cfg, path=config_path)
    flat = run_elo(games, cfg, layer, success_rates)[0]
    verify_flat_reference(conn, flat)
    reference = home_games_from_flat(games, flat, cfg['scale'])
    baseline = score(games, flat)
    candidates = []
    for half_life in half_lives:
        for shrinkage in shrinkages:
            hfa = {**raw['hfa'], 'half_life_years': half_life, 'shrinkage_k': shrinkage}
            # Compute the untuned team and national estimates once for this
            # half-life/shrinkage pair, then reuse them for each blend/cap.
            base = pregame_bonus_provider(reference, hfa, cfg['scale'], cfg['home_field'])
            cached = {}
            def estimates(game):
                key = game['game_date'], game['home_team_id']
                if key not in cached:
                    national = base({**game, 'home_team_id': -1})
                    cached[key] = base(game), national
                return cached[key]
            for alpha in alphas:
                for cap in caps:
                    def bonus(game):
                        team, national = estimates(game)
                        return cfg['home_field'] + max(-cap, min(cap, alpha * (team - national)))
                    result = score(games, run_elo(games, cfg, layer, success_rates,
                                                  home_bonus_for_game=bonus)[0])
                    item = {'half_life': half_life, 'shrinkage_k': shrinkage,
                            'alpha': alpha, 'cap': cap, 'scores': result}
                    candidates.append(item)
                    print(json.dumps({'candidate': {k: item[k] for k in ('half_life', 'shrinkage_k', 'alpha', 'cap')},
                                      'train': result.get('train'), 'validation': result.get('validation')}), flush=True)
    winner = min(candidates, key=lambda x: (x['scores']['train']['brier'],
                                           x['scores']['train']['log_loss']))
    return {'baseline': baseline, 'candidates': candidates,
            'selected_by_train_only': {k: winner[k] for k in ('half_life', 'shrinkage_k', 'alpha', 'cap')},
            'selected_scores': winner['scores'],
            'caveat': '2023-26 were inspected in a prior exploratory probe; a genuinely untouched final test requires future games.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', default='db/league.db')
    parser.add_argument('--config', type=Path, default=Path('config/model_config.json'))
    parser.add_argument('--half-lives', type=float, nargs='+', default=[10.0])
    parser.add_argument('--shrinkages', type=float, nargs='+', default=[30.0])
    parser.add_argument('--alphas', type=float, nargs='+', default=[0.25, 0.5, 1.0])
    parser.add_argument('--caps', type=float, nargs='+', default=[40.0, 80.0])
    parser.add_argument('--out', type=Path, default=Path('data/processed/dynamic_hfa_tuning.json'))
    args = parser.parse_args()
    with sqlite3.connect(args.db) as conn:
        result = tune(conn, args.config, args.half_lives, args.shrinkages, args.alphas, args.caps)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + '\n')
    print(f'Wrote {args.out}; train-selected candidate: {result["selected_by_train_only"]}')


if __name__ == '__main__':
    main()
