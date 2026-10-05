import copy
import sqlite3

import pytest

from audit_team_identities import audit
from export_team_identities import ROOT, render
from load_games import resolve_team_name
from person_identity import resolve_team_id
from team_identity import canonical_name, registry, validate_registry


def test_every_reviewed_name_and_alias_has_one_identity():
    rows, names = registry()
    assert len(rows) == 139
    for row in rows:
        for name in [row['canonical_name'], *row['aliases']]:
            assert canonical_name(name) == row['canonical_name']
    assert canonical_name('Texas Southern Tigers') is None
    assert canonical_name('Charlotte Saints') is None
    assert canonical_name('Troy Vikings') is None


@pytest.mark.parametrize('field,value', [('aliases', ['Texas']), ('team_id', 25), ('slug', 'air-force')])
def test_registry_rejects_identity_collisions(field, value):
    rows = copy.deepcopy(registry()[0])
    rows[1][field] = value
    with pytest.raises(ValueError):
        validate_registry(rows)


def test_games_and_people_share_exact_aliases_without_changing_database_ids():
    conn = sqlite3.connect(':memory:')
    conn.executescript('CREATE TABLE teams(team_id INTEGER,team_name TEXT);'
                       'CREATE TABLE team_aliases(alias TEXT,team_name TEXT);'
                       "INSERT INTO teams VALUES (9001,'Massachusetts'),(9002,'FAU'),(9003,'Florida');")
    assert resolve_team_name(conn.cursor(), 'UMass') == 'Massachusetts'
    assert resolve_team_id(conn, 'UMass') == 9001
    assert resolve_team_id(conn, 'Florida Atlantic Owls') == 9002
    assert resolve_team_id(conn, 'Texas Southern Tigers') is None
    conn.execute("INSERT INTO team_aliases VALUES ('Florida Atlantic Owls','Florida')")
    with pytest.raises(ValueError, match='Conflicting'):
        resolve_team_id(conn, 'Florida Atlantic Owls')


def test_browser_identity_asset_is_generated_from_reviewed_registry():
    assert (ROOT / 'ui/live_teams.js').read_text() == render()


def test_all_committed_exports_have_consistent_team_identities():
    result = audit()
    assert result['counts']['supported_teams'] == 139
    assert result['counts']['roster_rows'] > 200000
    assert result['errors'] == []
