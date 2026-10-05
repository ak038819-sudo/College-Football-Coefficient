"""Reviewed, exact team identities shared by ingestion and UI generation."""
from __future__ import annotations

import json
import unicodedata
from functools import lru_cache
from pathlib import Path

REGISTRY = Path(__file__).resolve().parent.parent / 'data/reference/team_identities.json'


def normalize(value: str) -> str:
    value = unicodedata.normalize('NFKD', str(value or '')).casefold()
    value = ''.join(c for c in value if not unicodedata.combining(c))
    return ' '.join(value.replace('’', '').replace("'", '').split())


def validate_registry(rows: list[dict]) -> dict[str, dict]:
    names, ids, slugs, providers = {}, set(), set(), {}
    for row in rows:
        if row['team_id'] in ids or row['slug'] in slugs:
            raise ValueError('Duplicate internal team ID or slug')
        ids.add(row['team_id'])
        slugs.add(row['slug'])
        for provider, value in row['provider_ids'].items():
            key = (provider, str(value))
            if key in providers:
                raise ValueError(f'Duplicate provider team ID: {key}')
            providers[key] = row['canonical_name']
        for alias in [row['canonical_name'], *row['aliases']]:
            key = normalize(alias)
            if not key or key in names and names[key]['team_id'] != row['team_id']:
                raise ValueError(f'Ambiguous team alias: {alias}')
            names[key] = row
    return names


@lru_cache(maxsize=1)
def registry() -> tuple[list[dict], dict[str, dict]]:
    rows = json.loads(REGISTRY.read_text())['teams']
    return rows, validate_registry(rows)


def canonical_name(value: str) -> str | None:
    row = registry()[1].get(normalize(value))
    return row['canonical_name'] if row else None


def resolve_database_name(cur, value: str) -> str | None:
    """Keep database aliases compatible, but reject conflicts with reviewed names.

    Registry IDs are audited references, never used to insert/rekey a database.
    A verified alias only resolves if its canonical school exists in that DB.
    """
    raw = str(value or '').strip()
    known = canonical_name(raw)
    matches = {r[0] for r in cur.execute(
        'SELECT team_name FROM teams WHERE team_name=? UNION '
        'SELECT t.team_name FROM team_aliases a JOIN teams t ON t.team_name=a.team_name '
        'WHERE TRIM(a.alias)=?', (raw, raw))}
    if known and cur.execute('SELECT 1 FROM teams WHERE team_name=?', (known,)).fetchone():
        matches.add(known)
    if len(matches) > 1:
        raise ValueError(f'Conflicting team identity: {raw}')
    return next(iter(matches), None)
