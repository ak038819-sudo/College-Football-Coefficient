#!/usr/bin/env python3
"""Season-by-season major-college eligibility for the 1946-1979 archive.

Membership moved. A school that was major-college in 1952 need not have
been in 1968, and the plan is explicit that present-day FBS says nothing
about either -- so nothing here reads the modern registry, by design and
by test.

What it reads instead is what the provider recorded AT THE TIME: the
classification carried on each game row, and the classification on that
team's record line for that season. Where the two disagree the season is
reported as a conflict and left unresolved. Where neither says anything
the season is UNKNOWN, which is not the same as 'no' and is never read as
'yes' -- it simply cannot be rated as major-college on this evidence.

Both databases are opened read-only. Nothing is promoted.
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import defaultdict
from pathlib import Path

FIRST, LAST = 1946, 1979
# The provider's own label for the top tier. Everything else it says is a
# lower division; an empty string is silence, not a division.
MAJOR = 'fbs'

MAJOR_COLLEGE = 'major_college'
LOWER_DIVISION = 'lower_division'
UNKNOWN = 'unknown'
CONFLICT = 'conflict'


def from_games(archive, first=FIRST, last=LAST):
    """What each team's own game rows say about it, season by season."""
    seen = defaultdict(lambda: {'games': 0, 'classified': 0,
                                'classifications': set()})
    rows = archive.execute(
        """SELECT season, home_name_raw, away_name_raw,
                  home_classification, away_classification
           FROM archive_games WHERE season BETWEEN ? AND ?""",
        (first, last)).fetchall()
    for season, home, away, home_class, away_class in rows:
        for name, classification in ((home, home_class), (away, away_class)):
            if not name:
                continue
            entry = seen[(int(season), str(name))]
            entry['games'] += 1
            if classification:
                entry['classified'] += 1
                entry['classifications'].add(str(classification))
    return seen


def from_records(records, first=FIRST, last=LAST):
    try:
        rows = records.execute(
            """SELECT season, team_raw, classification
               FROM archive_team_records WHERE season BETWEEN ? AND ?""",
            (first, last)).fetchall()
    except sqlite3.OperationalError:
        return {}
    return {(int(season), str(name)): (classification or '')
            for season, name, classification in rows}


def verdict(game_classes, record_class):
    """Major only where the evidence says so, and only for that season."""
    said = {value for value in game_classes if value}
    if record_class:
        said.add(record_class)
    if not said:
        return UNKNOWN
    if len(said) > 1:
        # Both a major and a lower label for one season, or two lower ones.
        # A tier is not something to average.
        return CONFLICT
    return MAJOR_COLLEGE if said == {MAJOR} else LOWER_DIVISION


def build(archive, records, first=FIRST, last=LAST):
    if first < FIRST or last > LAST or last < first:
        raise ValueError('Phase 3 range must be within 1946-1979')
    games = from_games(archive, first, last)
    lines = from_records(records, first, last)
    rows = []
    for key in sorted(set(games) | set(lines)):
        season, name = key
        entry = games.get(key, {'games': 0, 'classified': 0,
                                'classifications': set()})
        record_class = lines.get(key, '')
        call = verdict(entry['classifications'], record_class)
        rows.append({
            'season': season,
            'team_raw': name,
            'eligibility': call,
            'games': entry['games'],
            'games_classified': entry['classified'],
            'game_classifications': ';'.join(sorted(
                value for value in entry['classifications'] if value)),
            'record_classification': record_class,
            'has_record_line': key in lines,
        })
    return rows


def summarize(rows, first, last):
    by_season = defaultdict(lambda: defaultdict(int))
    totals = defaultdict(int)
    for row in rows:
        by_season[row['season']][row['eligibility']] += 1
        totals[row['eligibility']] += 1
    return {
        'years': [first, last],
        'team_seasons': len(rows),
        'by_eligibility': dict(sorted(totals.items())),
        'major_college_by_season': {
            str(season): counts.get(MAJOR_COLLEGE, 0)
            for season, counts in sorted(by_season.items())},
        'conflicts': [
            {'season': row['season'], 'team_raw': row['team_raw'],
             'game_classifications': row['game_classifications'],
             'record_classification': row['record_classification']}
            for row in rows if row['eligibility'] == CONFLICT],
        'production_promotion_allowed': False,
        'note': 'Eligibility is decided per season from what the provider '
                'recorded at the time. Unknown is not no, and is never read '
                'as yes. Present-day membership is not consulted.',
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--staging', type=Path, required=True)
    p.add_argument('--records', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--start', type=int, default=FIRST)
    p.add_argument('--end', type=int, default=LAST)
    args = p.parse_args()
    for path in (args.staging, args.records):
        if path.resolve().name == 'league.db':
            p.error('Refusing production database')
    if not args.staging.exists():
        p.error(f'No staged archive at {args.staging}')

    with sqlite3.connect(
            f'file:{args.staging.resolve()}?mode=ro', uri=True) as archive:
        if args.records.exists():
            with sqlite3.connect(f'file:{args.records.resolve()}?mode=ro',
                                 uri=True) as records:
                rows = build(archive, records, args.start, args.end)
        else:
            print('No records database: game rows are the only evidence.')
            rows = build(archive, sqlite3.connect(':memory:'),
                         args.start, args.end)
    if not rows:
        raise SystemExit(f'Nothing staged in {args.start}-{args.end}')

    args.out.mkdir(parents=True, exist_ok=True)
    with (args.out / 'historical_eligibility.csv').open('w', newline='') as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summary = summarize(rows, args.start, args.end)
    (args.out / 'historical_eligibility_summary.json').write_text(
        json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
