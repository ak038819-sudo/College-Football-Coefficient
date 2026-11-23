#!/usr/bin/env python3
"""Phase 4's first question: what does the provider actually have before 1946?

A PROBE, not an import. It asks the provider for every season in the range
and writes down the SHAPE of each answer -- how many games, how many carry
scores, dates, venues, classifications, which school names appear -- and
then stops. Nothing is staged, no database is written, and no identity,
eligibility or rating is decided anywhere in here.

The plan's own warning governs the output: **an empty response is not
evidence that no games occurred.** So a season the provider returns nothing
for is reported as a GAP TO CONFIRM against another source, never as a
season without football. The two are not the same claim and this report
never conflates them.

A season the provider FAILS on is a third thing again, and is counted
apart from both: an outage must not be able to masquerade as a gap. If any
season failed, the report is still written and the run then exits non-zero,
so a partial probe cannot be read as a complete one.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from import_historical_archive import fetch, value  # noqa: E402

# Phase 4's range. 1869 is the first season anyone played; 1946 is where
# Phase 3's archive already begins, so the probe stops short of it.
FIRST, LAST = 1869, 1945
SEASON_TYPES = ('regular', 'postseason')

COMPLETE, FAILED = 'complete', 'failed'


def shape(games, year):
    """What one season's answer looks like, counted field by field."""
    counts = {
        'records': len(games),
        'with_home_score': 0,
        'with_away_score': 0,
        'with_both_scores': 0,
        'ties': 0,
        'with_date': 0,
        'with_venue': 0,
        'with_home_classification': 0,
        'with_away_classification': 0,
        'neutral_true': 0,
        'neutral_false': 0,
        'neutral_unknown': 0,
        'missing_id': 0,
        'season_disagrees': 0,
    }
    names, classifications, conferences, ids = set(), set(), set(), []
    for row in games:
        gid = value(row, 'id', 'game_id')
        if gid is None or str(gid).strip() == '':
            counts['missing_id'] += 1
        else:
            ids.append(str(gid))
        source_year = value(row, 'season', 'year')
        if source_year is not None and int(source_year) != year:
            counts['season_disagrees'] += 1

        home = value(row, 'homePoints', 'home_points')
        away = value(row, 'awayPoints', 'away_points')
        counts['with_home_score'] += home is not None
        counts['with_away_score'] += away is not None
        if home is not None and away is not None:
            counts['with_both_scores'] += 1
            if int(home) == int(away):
                counts['ties'] += 1
        counts['with_date'] += bool(value(row, 'startDate', 'start_date'))
        counts['with_venue'] += bool(value(row, 'venue'))

        neutral = value(row, 'neutralSite', 'neutral_site')
        if neutral is None:
            counts['neutral_unknown'] += 1
        elif neutral:
            counts['neutral_true'] += 1
        else:
            counts['neutral_false'] += 1

        for side in ('home', 'away'):
            name = value(row, f'{side}Team', f'{side}_team')
            if name:
                names.add(str(name))
            label = value(row, f'{side}Classification',
                          f'{side}_classification')
            if label:
                counts[f'with_{side}_classification'] += 1
                classifications.add(str(label))
            conference = value(row, f'{side}Conference', f'{side}_conference')
            if conference:
                conferences.add(str(conference))

    counts['duplicate_ids'] = len(ids) - len(set(ids))
    counts['distinct_schools'] = len(names)
    return counts, names, classifications, conferences


def probe(first=FIRST, last=LAST, key=None, fetcher=None, pause=0.5,
          sleeper=None):
    if first < FIRST or last > LAST or last < first:
        raise ValueError(f'Phase 4 is {FIRST}-{LAST}, and start must not be '
                         f'after end: got {first}-{last}')
    # Resolved HERE, not in the signature: a default argument binds at def
    # time, so `fetcher=fetch` would make the module attribute impossible
    # to replace and every test of main() would quietly hit the network.
    if fetcher is None:
        fetcher = fetch
    if sleeper is None:
        import time
        sleeper = time.sleep
    rows = []
    schools, classifications, conferences = set(), set(), set()
    for year in range(first, last + 1):
        for kind in SEASON_TYPES:
            row = {'season': year, 'season_type': kind, 'status': COMPLETE,
                   'error': ''}
            try:
                games = fetcher(year, kind, key)
            except Exception as exc:
                # Counted apart from an empty answer: an outage must not be
                # able to look like a season the provider has nothing for.
                row['status'] = FAILED
                row['error'] = f'{type(exc).__name__}: {exc}'[:300]
                row.update({name: 0 for name in shape([], year)[0]})
                rows.append(row)
                print(f'FAILED {year} {kind}: {exc}', flush=True)
                continue
            counts, names, labels, leagues = shape(games, year)
            schools |= names
            classifications |= labels
            conferences |= leagues
            row.update(counts)
            rows.append(row)
            print(f'OK {year} {kind}: {counts["records"]} records', flush=True)
            if pause:
                sleeper(pause)
    return rows, schools, classifications, conferences


def summarize(rows, schools, classifications, conferences, first, last):
    done = [row for row in rows if row['status'] == COMPLETE]
    failed = [row for row in rows if row['status'] == FAILED]
    by_season = {}
    for row in done:
        by_season.setdefault(row['season'], 0)
        by_season[row['season']] += row['records']
    # A season is only 'empty' if every one of its requests SUCCEEDED and
    # came back with nothing. A season with a failed request is unknown.
    attempted = {}
    for row in rows:
        attempted.setdefault(row['season'], []).append(row['status'])
    empty = sorted(season for season, count in by_season.items()
                   if count == 0
                   and all(s == COMPLETE for s in attempted[season]))
    with_games = sorted(season for season, count in by_season.items()
                        if count > 0)
    incomplete = sorted({row['season'] for row in failed})

    def total(name):
        return sum(row[name] for row in done)

    return {
        'years': [first, last],
        'requests': {'made': len(rows), 'complete': len(done),
                     'failed': len(failed)},
        'seasons': {
            'requested': last - first + 1,
            'with_games': len(with_games),
            'empty': len(empty),
            'incomplete': len(incomplete),
        },
        'first_season_with_games': with_games[0] if with_games else None,
        'last_season_with_games': with_games[-1] if with_games else None,
        'empty_seasons': empty,
        'incomplete_seasons': incomplete,
        'games': total('records'),
        'games_by_season': {str(season): count
                            for season, count in sorted(by_season.items())},
        'fields': {
            'with_both_scores': total('with_both_scores'),
            'ties': total('ties'),
            'with_date': total('with_date'),
            'with_venue': total('with_venue'),
            'with_home_classification': total('with_home_classification'),
            'with_away_classification': total('with_away_classification'),
            'neutral_true': total('neutral_true'),
            'neutral_false': total('neutral_false'),
            'neutral_unknown': total('neutral_unknown'),
        },
        'integrity': {
            'missing_id': total('missing_id'),
            'duplicate_ids': total('duplicate_ids'),
            'season_disagrees': total('season_disagrees'),
        },
        'distinct_schools': len(schools),
        'classifications': sorted(classifications),
        'conferences': len(conferences),
        'staged': False,
        'production_promotion_allowed': False,
        'note': 'A probe. Nothing is staged and nothing is decided. An '
                'empty season is a gap to confirm against another source, '
                'not evidence that no games were played; a season whose '
                'request failed is neither, and is counted apart.',
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--start', type=int, default=FIRST)
    p.add_argument('--end', type=int, default=LAST)
    p.add_argument('--pause', type=float, default=0.5)
    args = p.parse_args()
    key = os.environ.get('CFBD_API_KEY')
    if not key:
        p.error('CFBD_API_KEY is required')

    rows, schools, classifications, conferences = probe(
        args.start, args.end, key, pause=args.pause)

    args.out.mkdir(parents=True, exist_ok=True)
    with (args.out / 'early_coverage.csv').open('w', newline='') as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    (args.out / 'early_schools.csv').write_text(
        'raw_name\n' + '\n'.join(sorted(schools)) + '\n')
    summary = summarize(rows, schools, classifications, conferences,
                        args.start, args.end)
    (args.out / 'early_coverage_summary.json').write_text(
        json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2))
    if summary['requests']['failed']:
        raise SystemExit(
            f"{summary['requests']['failed']} requests failed: this probe is "
            'partial and its empty seasons cannot be read as gaps')


if __name__ == '__main__':
    main()
