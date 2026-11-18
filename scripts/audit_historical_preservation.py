#!/usr/bin/env python3
"""Phase 3's preservation gate: does the staged archive still say what the
provider said?

The importer keeps each game's whole response body in `source_payload_json`
with a sha256 beside it, and copies a handful of fields out into columns for
querying. Every later gate -- reconciliation, identity, eligibility -- reads
the COLUMNS. So the columns are load-bearing, and nothing has ever checked
them against the payload they came from.

This does, and it does it WITHOUT importing the importer. Reading the payload
again with the same function that wrote the row would only prove the function
is deterministic. The field mapping below is written out a second time, from
the provider's field names, so that a wrong mapping has to be wrong twice to
go unnoticed.

Three questions, in order of how much they matter:

1. Is the payload itself intact? sha256 it again and compare.
2. Does every column still match the payload? A column that drifted from its
   payload is a silent rewrite of the source, which rule 1 of the plan
   forbids outright.
3. What did the provider actually leave uncertain? Ties, missing scores,
   missing dates, unknown classification and -- the one most easily lost --
   neutral site, which is only honestly three-valued if the provider ever
   says nothing. The census answers that instead of assuming it, and it
   counts from the PAYLOADS, not the columns: a column under audit must
   not get to answer a question about the source.

A column that is NULL where the payload carries a value is a LOSS. A column
that carries a value the payload does not have is an INVENTION. Both are
reported per row, neither is repaired: this audit opens the database
read-only and fixes nothing.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
from collections import Counter
from pathlib import Path

FIRST, LAST = 1946, 1979

# The provider's field names, written out again rather than imported. Each
# column names the keys it may come from; snake_case and camelCase are both
# in the wild and the importer accepts either, so this must too.
FIELDS = {
    # Provenance first. Every later gate assigns a game to a team-season by
    # the STORED season, so a season that drifted would move evidence from
    # one year to another without touching a score.
    'source_game_id': ('id', 'game_id'),
    'season': ('season', 'year'),
    'season_type': ('seasonType', 'season_type'),
    'start_date': ('startDate', 'start_date'),
    'home_name_raw': ('homeTeam', 'home_team'),
    'away_name_raw': ('awayTeam', 'away_team'),
    'home_score': ('homePoints', 'home_points'),
    'away_score': ('awayPoints', 'away_points'),
    'neutral_site': ('neutralSite', 'neutral_site'),
    'home_classification': ('homeClassification', 'home_classification'),
    'away_classification': ('awayClassification', 'away_classification'),
    'home_conference': ('homeConference', 'home_conference'),
    'away_conference': ('awayConference', 'away_conference'),
    'venue': ('venue',),
}

# The two columns the importer takes from the REQUEST rather than the
# response: it writes the season and season type it asked for. A payload
# silent about either is therefore not a disagreement. A payload that names
# a different one still is.
REQUEST_SHAPED = ('season', 'season_type')

INTEGER_COLUMNS = ('home_score', 'away_score', 'season')

# How many disagreeing rows are written out in full. The counts are exact
# either way; this only bounds the file.
FINDINGS_SHOWN = 500


def payload_value(payload, column):
    """What the payload says for a column, or None if it says nothing."""
    for key in FIELDS[column]:
        if key in payload:
            return payload[key]
    return None


def comparable(column, value):
    """One shape for a column's value, so that 0 and False are one thing.

    Returns None for 'nothing said'. An empty string is nothing said too:
    the provider writes '' for an unknown classification, and the importer
    stores that '' unchanged, so the two agree without either asserting a
    division.
    """
    if value is None:
        return None
    if column == 'neutral_site':
        return int(bool(value))
    if column in INTEGER_COLUMNS:
        return int(value)
    text = str(value)
    return text if text != '' else None


def row_findings(row, payload):
    """Every column of one row that no longer matches its payload."""
    found = []
    for column in FIELDS:
        stored = comparable(column, row[column])
        source = comparable(column, payload_value(payload, column))
        if stored == source:
            continue
        if column in REQUEST_SHAPED and source is None:
            continue
        kind = ('loss' if stored is None else
                'invention' if source is None else 'mismatch')
        found.append({
            'source_game_id': row['source_game_id'],
            'season': row['season'],
            'column': column,
            'kind': kind,
            'stored': '' if stored is None else str(stored),
            'payload': '' if source is None else str(source),
        })
    return found


def census(payload):
    """What the PROVIDER left uncertain, in the gate's own vocabulary.

    Read from the payload, never from the columns. The columns are what
    this audit is checking, so counting uncertainty out of them would let a
    drifted column answer a question about the source: a stored 7-6 over a
    payload's 7-7 would report no tie, and a stored 0 over a payload's null
    would report that the provider is certain about a neutral site it never
    spoke to.
    """
    marks = []
    home = comparable('home_score', payload_value(payload, 'home_score'))
    away = comparable('away_score', payload_value(payload, 'away_score'))
    if home is None or away is None:
        marks.append('missing_score')
    elif home == away:
        marks.append('tie')
    if not comparable('start_date', payload_value(payload, 'start_date')):
        marks.append('missing_date')
    for side in ('home', 'away'):
        column = f'{side}_classification'
        if not comparable(column, payload_value(payload, column)):
            marks.append(f'{side}_classification_unknown')
    neutral = comparable('neutral_site',
                         payload_value(payload, 'neutral_site'))
    if neutral is None:
        marks.append('neutral_site_unknown')
    elif neutral:
        marks.append('neutral_site_true')
    else:
        marks.append('neutral_site_false')
    if not comparable('venue', payload_value(payload, 'venue')):
        marks.append('missing_venue')
    return marks


def audit(archive, first=FIRST, last=LAST):
    if first < FIRST or last > LAST or last < first:
        raise ValueError('Phase 3 range must be within 1946-1979')
    archive.row_factory = sqlite3.Row
    rows = archive.execute(
        'SELECT * FROM archive_games WHERE season BETWEEN ? AND ?'
        ' ORDER BY season, source_game_id', (first, last)).fetchall()

    games = 0
    counted = 0
    corrupt_payloads = []
    unreadable_payloads = []
    findings = []
    marks = Counter()
    seasons = Counter()
    for row in rows:
        games += 1
        seasons[int(row['season'])] += 1
        raw = row['source_payload_json']
        if hashlib.sha256(raw.encode()).hexdigest() != row['payload_sha256']:
            corrupt_payloads.append({
                'source_game_id': row['source_game_id'],
                'season': row['season']})
            # A payload that does not match its own digest is not evidence
            # of anything, so it is not read further.
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            unreadable_payloads.append({
                'source_game_id': row['source_game_id'],
                'season': row['season'], 'error': str(exc)})
            continue
        if not isinstance(payload, dict):
            unreadable_payloads.append({
                'source_game_id': row['source_game_id'],
                'season': row['season'],
                'error': f'payload is {type(payload).__name__}, not an object'})
            continue
        findings.extend(row_findings(row, payload))
        marks.update(census(payload))
        counted += 1
    return {
        'games': games,
        # Only verified payloads reach the census: a body that failed its
        # own digest is not evidence of what the provider said.
        'census_games': counted,
        'seasons': seasons,
        'corrupt_payloads': corrupt_payloads,
        'unreadable_payloads': unreadable_payloads,
        'findings': findings,
        'marks': marks,
    }


def summarize(result, first, last):
    findings = result['findings']
    by_kind = Counter(item['kind'] for item in findings)
    by_column = Counter(item['column'] for item in findings)
    marks = result['marks']
    neutral = {
        'true': marks.get('neutral_site_true', 0),
        'false': marks.get('neutral_site_false', 0),
        'unknown': marks.get('neutral_site_unknown', 0),
    }
    return {
        'years': [first, last],
        'games': result['games'],
        'seasons_present': len(result['seasons']),
        'payloads': {
            'corrupt': len(result['corrupt_payloads']),
            'unreadable': len(result['unreadable_payloads']),
            'verified': result['games'] - len(result['corrupt_payloads'])
                        - len(result['unreadable_payloads']),
        },
        'column_findings': {
            'total': len(findings),
            'by_kind': dict(sorted(by_kind.items())),
            'by_column': dict(sorted(by_column.items())),
        },
        'census_games': result['census_games'],
        'preserved': {
            'ties': marks.get('tie', 0),
            'missing_score': marks.get('missing_score', 0),
            'missing_date': marks.get('missing_date', 0),
            'missing_venue': marks.get('missing_venue', 0),
            'home_classification_unknown':
                marks.get('home_classification_unknown', 0),
            'away_classification_unknown':
                marks.get('away_classification_unknown', 0),
        },
        'neutral_site': neutral,
        'neutral_site_is_three_valued': neutral['unknown'] > 0,
        'production_promotion_allowed': False,
        'note': 'Read-only. Columns are checked against the payload they '
                'came from, by a mapping written independently of the '
                'importer, and the uncertainty census is counted from '
                'those payloads rather than from the columns under audit. '
                'Nothing is repaired and nothing is promoted.',
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--staging', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--start', type=int, default=FIRST)
    p.add_argument('--end', type=int, default=LAST)
    args = p.parse_args()
    if args.staging.resolve().name == 'league.db':
        p.error('Refusing production database')
    if not args.staging.exists():
        p.error(f'No staged archive at {args.staging}')

    with sqlite3.connect(f'file:{args.staging.resolve()}?mode=ro',
                         uri=True) as archive:
        result = audit(archive, args.start, args.end)
    if not result['games']:
        raise SystemExit(f'Nothing staged in {args.start}-{args.end}')

    args.out.mkdir(parents=True, exist_ok=True)
    findings = result['findings']
    header = ['source_game_id', 'season', 'column', 'kind', 'stored',
              'payload']
    with (args.out / 'historical_preservation_findings.csv').open(
            'w', newline='') as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        writer.writeheader()
        writer.writerows(findings[:FINDINGS_SHOWN])
    summary = summarize(result, args.start, args.end)
    (args.out / 'historical_preservation_summary.json').write_text(
        json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
