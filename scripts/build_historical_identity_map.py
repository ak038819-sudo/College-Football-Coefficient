#!/usr/bin/env python3
"""Candidate identity map for the schools in the 1946-1979 archive.

The archive stores the names the provider gave, verbatim. Before any of it
can be rated, each name has to be tied to a school -- including the ones
that were renamed, merged, dropped the sport, or merely look like another
program ("Trinity (Texas)", "Miami (OH)").

THIS SCRIPT DECIDES NOTHING. It gathers the evidence for each name and
sorts the names into those the provider itself carries as a team, those
our own reviewed registry already knows, and those a person has to rule
on. Where a name resembles a known one it says so in a column called
`suggestion`, which is a lead to check and never a resolution: a
suggestion is never written into `canonical_name`.

Both databases are opened read-only. Nothing is promoted and no rating is
built.
"""
from __future__ import annotations

import argparse
import csv
import difflib
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

FIRST, LAST = 1946, 1979
# Below this, a resemblance is noise. It is a reading aid either way: no
# status depends on it.
SUGGESTION_FLOOR = 0.82

RECORDS_MATCHED = "records_matched"
REGISTRY_MATCHED = "registry_matched"
UNRESOLVED = "unresolved"


def normalize(value):
    """The registry's own normalisation, so the two agree on what matches."""
    from src.team_identity import normalize as registry_normalize
    return registry_normalize(value)


def registry_names():
    """Every reviewed name and alias, mapped to its canonical school."""
    from src.team_identity import registry
    rows, by_name = registry()
    return {key: row['canonical_name'] for key, row in by_name.items()}


def appearances(archive, first=FIRST, last=LAST):
    """Every distinct raw school name in the range, with its evidence."""
    found = defaultdict(lambda: {
        'seasons': set(), 'games': 0, 'as_home': 0, 'as_away': 0,
        'classifications': set(), 'conferences': set(), 'opponents': set()})
    rows = archive.execute(
        """SELECT season, home_name_raw, away_name_raw,
                  home_classification, away_classification,
                  home_conference, away_conference
           FROM archive_games WHERE season BETWEEN ? AND ?""",
        (first, last)).fetchall()
    for season, home, away, home_class, away_class, home_conf, away_conf in rows:
        for name, side, kind, conference, other in (
                (home, 'as_home', home_class, home_conf, away),
                (away, 'as_away', away_class, away_conf, home)):
            if not name:
                continue
            entry = found[str(name)]
            entry['seasons'].add(int(season))
            entry['games'] += 1
            entry[side] += 1
            if kind:
                entry['classifications'].add(str(kind))
            if conference:
                entry['conferences'].add(str(conference))
            if other:
                entry['opponents'].add(str(other))
    return found


def record_lines(records, first=FIRST, last=LAST):
    """What the records feed carries for each name: its ids and seasons."""
    carried = defaultdict(lambda: {'seasons': set(), 'ids': set(),
                                   'classifications': set()})
    try:
        rows = records.execute(
            """SELECT team_raw, season, team_source_id, classification
               FROM archive_team_records WHERE season BETWEEN ? AND ?""",
            (first, last)).fetchall()
    except sqlite3.OperationalError:
        # No records collected yet. Every name then falls to review, which
        # is the honest answer rather than a crash.
        return carried
    for name, season, team_id, classification in rows:
        entry = carried[str(name)]
        entry['seasons'].add(int(season))
        if team_id:
            entry['ids'].add(str(team_id))
        if classification:
            entry['classifications'].add(str(classification))
    return carried


def suggestion(name, known, floor=SUGGESTION_FLOOR):
    """The closest known name, as a lead for a reviewer. Never applied."""
    if not known:
        return '', 0.0
    matches = difflib.get_close_matches(normalize(name), list(known), n=1,
                                        cutoff=floor)
    if not matches:
        return '', 0.0
    ratio = difflib.SequenceMatcher(None, normalize(name), matches[0]).ratio()
    return known[matches[0]], round(ratio, 3)


def build(archive, records, first=FIRST, last=LAST):
    if first < FIRST or last > LAST or last < first:
        raise ValueError("Phase 3 range must be within 1946-1979")
    staged = appearances(archive, first, last)
    carried = record_lines(records, first, last)
    known = registry_names()
    # The provider's own team names are leads too, for a name the records
    # feed spells differently from the games feed.
    leads = dict(known)
    leads.update({normalize(name): name for name in carried})

    rows = []
    for name in sorted(staged):
        entry = staged[name]
        line = carried.get(name)
        registry_match = known.get(normalize(name), '')
        if line:
            status, canonical = RECORDS_MATCHED, name
        elif registry_match:
            status, canonical = REGISTRY_MATCHED, registry_match
        else:
            status, canonical = UNRESOLVED, ''
        closest, ratio = ('', 0.0) if status != UNRESOLVED else suggestion(
            name, leads)
        seasons = sorted(entry['seasons'])
        rows.append({
            'raw_name': name,
            'status': status,
            'canonical_name': canonical,
            'cfbd_team_ids': ';'.join(sorted((line or {}).get('ids', ()))),
            'first_season': seasons[0],
            'last_season': seasons[-1],
            'seasons': len(seasons),
            'games': entry['games'],
            'as_home': entry['as_home'],
            'as_away': entry['as_away'],
            'record_seasons': len((line or {}).get('seasons', ())),
            'classifications': ';'.join(sorted(entry['classifications'])),
            'conferences': ';'.join(sorted(entry['conferences'])),
            'distinct_opponents': len(entry['opponents']),
            'suggestion': closest,
            'suggestion_ratio': ratio,
            'verdict': '',
            'verdict_note': '',
        })
    return rows


def summarize(rows, first, last):
    counts = defaultdict(int)
    for row in rows:
        counts[row['status']] += 1
    unresolved = [row for row in rows if row['status'] == UNRESOLVED]
    unresolved.sort(key=lambda row: (-row['seasons'], row['raw_name']))
    return {
        'years': [first, last],
        'schools': len(rows),
        'by_status': dict(sorted(counts.items())),
        'unresolved_games': sum(row['games'] for row in unresolved),
        'unresolved': [
            {'raw_name': row['raw_name'], 'seasons': row['seasons'],
             'games': row['games'], 'first_season': row['first_season'],
             'last_season': row['last_season'],
             'suggestion': row['suggestion'],
             'suggestion_ratio': row['suggestion_ratio']}
            for row in unresolved],
        'production_promotion_allowed': False,
        'note': 'Candidates only. A suggestion is a lead for review and is '
                'never a resolution; nothing here renames, merges or drops '
                'a school.',
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
            print('No records database: every name falls to review.')
            rows = build(archive, sqlite3.connect(':memory:'),
                         args.start, args.end)
    if not rows:
        raise SystemExit(f'No staged school names in {args.start}-{args.end}')

    args.out.mkdir(parents=True, exist_ok=True)
    with (args.out / 'historical_schools.csv').open('w', newline='') as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summary = summarize(rows, args.start, args.end)
    (args.out / 'historical_schools_summary.json').write_text(
        json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
