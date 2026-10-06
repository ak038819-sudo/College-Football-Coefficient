#!/usr/bin/env python3
"""Read-only audit of published Elo transfers, not a fitted replacement model."""
import csv
import json
import re
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def read_export(path):
    text = path.read_text()
    match = re.search(r'=\s*(\{)', text)
    if not match:
        raise ValueError(f'No exported object: {path}')
    return json.JSONDecoder().raw_decode(text[match.start(1):])[0]


def group(conf, team):
    if not isinstance(conf, str) or not conf.strip():
        raise ValueError(f'Missing conference for team {team}')
    # Independents do not form a shared conference pool.
    return f'{conf} / {team}' if 'independent' in conf.lower() else conf


def audit(seasons=range(2018, 2027)):
    published = read_export(ROOT / 'ui/data/team_pages.js')
    deltas = {(r[0], r[1]): r[5] for r in published['elo']}
    rows, summaries = [], []
    for season in seasons:
        payload = read_export(ROOT / f'ui/data/games/{season}.js')
        buckets = defaultdict(lambda: defaultdict(float))
        totals = defaultdict(float)
        rated_games = []
        missing = set()
        for values in payload['games']:
            g = dict(zip(payload['fields'], values))
            if not g['completed']:
                continue
            home, away = g['home_id'], g['away_id']
            if (g['game_id'], home) not in deltas or (g['game_id'], away) not in deltas:
                totals['unrated_completed'] += 1
                continue
            rated_games.append(g)
            for team in (home, away):
                conf = payload['conferences'].get(str(team))
                if not isinstance(conf, str) or not conf.strip():
                    missing.add(team)
        # Exclude the whole season before publishing any classifications or
        # Brier splits; dropping only affected games would bias the remainder.
        if missing:
            summaries.append({'season': season, 'status': 'excluded',
                              'reason': 'missing_conference_membership',
                              'missing_team_ids': sorted(missing)})
            continue
        for g in rated_games:
            home, away = g['home_id'], g['away_id']
            h = group(payload['conferences'].get(str(home)), home)
            a = group(payload['conferences'].get(str(away)), away)
            dh, da = deltas[g['game_id'], home], deltas[g['game_id'], away]
            totals['rated_games'] += 1
            totals['max_pair_residual'] = max(totals['max_pair_residual'], abs(dh + da))
            internal = h == a
            phase = 'postseason' if g['phase'] else 'early' if g['week'] is not None and g['week'] <= 5 else 'late' if g['week'] is not None else 'unknown_week'
            totals[f'{phase}_{"internal" if internal else "cross"}_games'] += 1
            if internal:
                buckets[h]['internal_games'] += 1
                buckets[h]['internal_net'] += dh + da
            else:
                for name, delta in [(h, dh), (a, da)]:
                    buckets[name]['cross_games'] += 1
                    buckets[name]['cross_net'] += delta
                    buckets[name][f'{phase}_cross_games'] += 1
                    buckets[name][f'{phase}_cross_net'] += delta
            p = g['p_home']
            if p is not None:
                score = 1 if g['home_score'] > g['away_score'] else 0 if g['home_score'] < g['away_score'] else 0.5
                key = 'internal' if internal else 'cross'
                totals[f'{key}_brier_sum'] += (p - score) ** 2
                totals[f'{key}_predictions'] += 1
        for name, metrics in sorted(buckets.items()):
            rows.append({'season': season, 'conference': name, **{k: round(v, 4) for k, v in metrics.items()}})
        for key in ['internal', 'cross']:
            n = totals.get(f'{key}_predictions', 0)
            totals[f'{key}_brier'] = totals[f'{key}_brier_sum'] / n if n else None
        summaries.append({'season': season, **dict(totals)})
    return rows, summaries


def main():
    rows, summaries = audit()
    out = Path(__file__).resolve().parent
    columns = ['season', 'conference', 'internal_games', 'internal_net', 'cross_games', 'cross_net',
               'early_cross_games', 'early_cross_net', 'late_cross_games', 'late_cross_net',
               'postseason_cross_games', 'postseason_cross_net', 'unknown_week_cross_games', 'unknown_week_cross_net']
    with (out / 'conference_transfers.csv').open('w') as f:
        writer = csv.DictWriter(f, fieldnames=columns, lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)
    (out / 'season_summary.json').write_text(json.dumps(summaries, indent=2) + '\n')
    print(json.dumps([r for r in summaries if r['season'] >= 2025], indent=2))


if __name__ == '__main__':
    main()
