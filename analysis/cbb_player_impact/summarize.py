"""Render a pilot snapshot as markdown. Prints evidence; never prints the key.

The summary goes to stdout as well as the job summary so the measured numbers
survive in the run log, which outlives the 30-day artifact and is readable
where artifact blob storage is not.
"""

import argparse
import json
from pathlib import Path


def load(root, name):
    path = root / name
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding='utf-8'))


def summarize(root):
    lines = ['## College basketball pilot', '']
    manifest = load(root, 'manifest.json')
    if manifest is None:
        lines.append('No snapshot produced. Inspect the failed step for access '
                     'or validation errors.')
        return lines
    lines += [f'Collection status: {manifest["status"]}',
              f'Season {manifest["season"]} ({manifest["team"]}), retrieved '
              f'{manifest["retrieved_at"]}',
              f'Schedule games: {manifest["schedule_games"]}; completed: '
              f'{manifest["completed_games"]}; sampled: {manifest["sampled_games"]}']
    if 'box_score_games' in manifest:
        lines.append(f'Box score games returned: {manifest["box_score_games"]}')

    audits = load(root, 'audit.json')
    if audits:
        passed = sum(row['structural_checks_passed'] for row in audits)
        lines += ['', f'### Lineup structure: {passed}/{len(audits)} games passed']
        for row in audits:
            events = [f'{k.replace("_events", "")} {row[k]}'
                      for k in ('plays_events', 'substitutions_events') if k in row]
            lines.append(f'- Game {row["game_id"]}: '
                         + (', '.join(events) if events else 'no event counts'))
            for issue in row['issues']:
                lines.append(f'  - {issue}')

    reconciliation = load(root, 'reconciliation.json')
    if reconciliation:
        passed = sum(row['reconciled'] for row in reconciliation)
        lines += ['', f'### Minute reconciliation: {passed}/{len(reconciliation)} games']
        for row in reconciliation:
            lines.append(f'- Game {row["game_id"]}'
                         f' (teams {row["reconciled_teams"]}):'
                         f' {"reconciled" if row["reconciled"] else "NOT reconciled"}')
            for issue in row['issues']:
                lines.append(f'  - {issue}')
            for team in row['teams']:
                lines.append(
                    f'  - team {team["team_id"]}: {team["players"]} players,'
                    f' {team["starters"]} starters, minutes {team["minutes"]}'
                    f' vs expected {team["expected_minutes"]}'
                    f' vs lineups {team["minutes_from_lineups"]},'
                    f' points {team["points"]} vs final {team["final_score"]},'
                    f' periods {team["periods"]}')
                for issue in team['issues']:
                    lines.append(f'    - {issue}')

    lines += ['', 'Player-model readiness: pending paired-lineup and possession '
              'validation.', 'A successful collection is not a validated player rating.',
              'This is a single-team sample and must never be published as '
              'national coverage.']
    return lines


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, required=True)
    args = parser.parse_args()
    print('\n'.join(summarize(args.snapshot)))


if __name__ == '__main__':
    main()
