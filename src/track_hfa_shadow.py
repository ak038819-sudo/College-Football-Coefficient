#!/usr/bin/env python3
"""Freeze pregame flat and candidate HFA forecasts, then score final games.

Run after the full Elo rebuild. Previously recorded probabilities never change;
the script only adds upcoming games and settles completed ones. No production
rating, prediction, or dashboard export is modified.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
from pathlib import Path

from build_elo import expected_result, fetch_games_chronological, load_config, run_elo
from compare_dynamic_hfa import verify_flat_reference
from dynamic_hfa import home_games_from_flat, pregame_bonus_provider
from predict_upcoming import game_expectation, performance_layer

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / 'config/hfa_shadow.json'
OUT = ROOT / 'data/processed/dynamic_hfa_shadow.json'


def parse_time(value):
    return dt.datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(dt.timezone.utc)


def settle(records, final_games, now):
    """Score only the exact game and teams recorded before kickoff."""
    settled = 0
    for gid, record in records.items():
        game = final_games.get(int(gid))
        if game is None or 'result' in record:
            continue
        if (game['home_team_id'], game['away_team_id'], game['season_year']) != (
                record['home_id'], record['away_id'], record['season']):
            record['invalid_reason'] = 'Game identity changed; forecast not scored'
            continue
        if parse_time(record['recorded_at']) >= parse_time(record['kickoff_utc']):
            record['invalid_reason'] = 'Forecast was not recorded before kickoff'
            continue
        actual = 1.0 if game['home_score'] > game['away_score'] else (
            0.0 if game['home_score'] < game['away_score'] else 0.5)
        record['result'] = actual
        record['flat_brier'] = round((record['p_flat'] - actual) ** 2, 8)
        record['candidate_brier'] = round((record['p_candidate'] - actual) ** 2, 8)
        record['scored_at'] = now.isoformat().replace('+00:00', 'Z')
        settled += 1
    return settled


def summary(records):
    scored = [r for r in records.values() if 'result' in r]
    if not scored:
        return {'games': 0, 'flat_brier': None, 'candidate_brier': None}
    n = len(scored)
    return {'games': n,
            'flat_brier': round(sum(r['flat_brier'] for r in scored) / n, 6),
            'candidate_brier': round(sum(r['candidate_brier'] for r in scored) / n, 6)}


def update(conn, records, now, raw_cfg, frozen, horizon_days=7):
    """Return counts; the caller persists records atomically after success."""
    conn.row_factory = sqlite3.Row
    final_games = {r['game_id']: r for r in conn.execute(
        'SELECT game_id, home_team_id, away_team_id, season_year, home_score, away_score FROM games')}
    settled = settle(records, final_games, now)
    schedule = []
    for row in conn.execute('SELECT game_id, season_year, kickoff_utc, start_time_tbd, '
                            'home_team_id, away_team_id, neutral_site FROM scheduled_games'):
        gid, season, kickoff, tbd, home, away, neutral = row
        if str(gid) in records or not kickoff or tbd:
            continue
        try:
            start = parse_time(kickoff)
        except (TypeError, ValueError):
            continue
        if now < start <= now + dt.timedelta(days=horizon_days):
            schedule.append((gid, season, kickoff, home, away, neutral))
    if not schedule:
        return 0, settled

    cfg = raw_cfg['elo']
    if float(cfg['home_field']) != float(frozen['baseline_elo_points']):
        raise ValueError('Flat Elo intercept changed; revalidate the frozen candidate')
    games = fetch_games_chronological(conn)
    layer, success_rates = performance_layer(conn, cfg)
    flat_rows, flat_ratings, _ = run_elo(games, cfg, layer, success_rates)
    verify_flat_reference(conn, flat_rows)
    hfa = {**raw_cfg['hfa'], 'half_life_years': frozen['half_life_years'],
           'shrinkage_k': frozen['shrinkage_k']}
    ref = home_games_from_flat(games, flat_rows, cfg['scale'])
    bonus = pregame_bonus_provider(ref, hfa, cfg['scale'], cfg['home_field'],
                                   centered_alpha=frozen['alpha'],
                                   max_deviation=frozen['max_deviation'])
    _, candidate_ratings, _ = run_elo(games, cfg, layer, success_rates, home_bonus_for_game=bonus)
    last_season = games[-1]['season_year'] if games else None
    now_iso = now.isoformat().replace('+00:00', 'Z')
    count = 0
    for gid, season, kickoff, home, away, neutral in schedule:
        def rating(table, tid):
            r = table.get(tid, cfg['initial_rating'])
            if last_season is not None and season > last_season and tid in table:
                r = cfg['initial_rating'] + cfg['offseason_retention'] * (r - cfg['initial_rating'])
            return r
        flat_home, flat_away = rating(flat_ratings, home), rating(flat_ratings, away)
        dyn_home, dyn_away = rating(candidate_ratings, home), rating(candidate_ratings, away)
        game = {'game_date': kickoff[:10], 'home_team_id': home}
        home_bonus = 0.0 if neutral else bonus(game)
        p_flat = game_expectation(flat_home, flat_away, bool(neutral), cfg)
        p_candidate = expected_result(dyn_home + home_bonus, dyn_away, cfg['scale'])
        records[str(gid)] = {'season': season, 'kickoff_utc': kickoff, 'recorded_at': now_iso,
                             'home_id': home, 'away_id': away, 'neutral': bool(neutral),
                             'home_bonus': round(home_bonus, 3),
                             'p_flat': round(p_flat, 6), 'p_candidate': round(p_candidate, 6)}
        count += 1
    return count, settled


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', default=str(ROOT / 'db/league.db'))
    parser.add_argument('--out', type=Path, default=OUT)
    parser.add_argument('--now', help='UTC timestamp for deterministic local checks')
    args = parser.parse_args()
    now = parse_time(args.now) if args.now else dt.datetime.now(dt.timezone.utc)
    frozen = json.loads(CONFIG.read_text())
    prior = json.loads(args.out.read_text()) if args.out.exists() else {}
    if prior and (prior.get('version') != frozen['version'] or prior.get('frozen') != frozen):
        raise ValueError('Shadow model changed; preserve old forecasts under a new version/file')
    records = prior.get('forecasts', {})
    with sqlite3.connect(args.db) as conn:
        count, settled = update(conn, records, now, load_config(str(ROOT / 'config/model_config.json')), frozen)
    payload = {'version': frozen['version'], 'frozen': frozen, 'forecasts': records,
               'summary': summary(records)}
    if payload != prior:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        temp = args.out.with_suffix('.tmp')
        temp.write_text(json.dumps(payload, indent=2, sort_keys=True) + '\n')
        temp.replace(args.out)
    print(f'Shadow HFA: {count} new pregame forecasts, {settled} finals scored, '
          f'{payload["summary"]["games"]} prospective games total')


if __name__ == '__main__':
    main()
