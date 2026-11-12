"""The cross-league institution join.

The assertions that matter here are the negative ones: that a provider id is
never accepted as a cross-league key, and that an unknown school fails closed.
Those are the two ways a reader lands on another school's page.
"""
import gzip
import json
from pathlib import Path

import pytest

from institution_identity import (
    espn_id_from_logos,
    institutions,
    load_basketball_teams,
    load_football_teams,
    resolve,
    unresolved_basketball_teams,
)

SEASON = 2026


def _logo(espn_id, size=500):
    return f'https://cdn.collegefootballdata.com/cbb-logos/{size}/{espn_id}.png'


def _fixture(tmp_path, football, basketball):
    """A miniature repository root, so no test reads the real snapshots."""
    (tmp_path / 'data/reference').mkdir(parents=True)
    (tmp_path / f'data/ncaambb/raw/{SEASON}').mkdir(parents=True)
    (tmp_path / 'data/reference/team_identities.json').write_text(
        json.dumps({'teams': football}), encoding='utf-8')
    with gzip.open(tmp_path / f'data/ncaambb/raw/{SEASON}/teams.json.gz', 'wt', encoding='utf-8') as handle:
        json.dump(basketball, handle)
    return tmp_path


# --- the join key -----------------------------------------------------------

def test_espn_id_is_read_from_the_logo_path_not_the_size_segment():
    assert espn_id_from_logos([_logo(2561, size=500)]) == 2561
    assert espn_id_from_logos([_logo(66, size=128)]) == 66


def test_a_team_with_no_usable_logo_yields_no_id_rather_than_raising():
    assert espn_id_from_logos([]) is None
    assert espn_id_from_logos(None) is None
    assert espn_id_from_logos(['https://example.test/logo.svg']) is None
    assert espn_id_from_logos([None, 7]) is None


def test_a_cbbd_id_that_is_also_a_football_id_does_not_join_the_two(tmp_path):
    """The measured hazard: 81 CBBD ids are valid CFBD football ids elsewhere.

    Here CBBD's Florida carries id 87, which is Notre Dame's football id. The
    join must pair Florida with Florida on the ESPN id and must not pair
    anything with Notre Dame.
    """
    root = _fixture(
        tmp_path,
        football=[
            {'canonical_name': 'Florida', 'provider_ids': {'espn': 57, 'cfbd_scoreboard': 57}},
            {'canonical_name': 'Notre Dame', 'provider_ids': {'espn': 87, 'cfbd_scoreboard': 87}},
        ],
        basketball=[{'displayName': 'Florida Gators', 'id': 87, 'logos': [_logo(57)]}],
    )
    rows = institutions(SEASON, root)
    assert rows[57]['football']['canonical_name'] == 'Florida'
    assert rows[57]['basketball']['displayName'] == 'Florida Gators'
    assert rows[87]['football']['canonical_name'] == 'Notre Dame'
    assert rows[87]['basketball'] is None, 'a provider id was accepted as a cross-league key'


def test_names_are_not_used_as_a_fallback_key(tmp_path):
    """Idaho and Idaho State are the near-miss a name join gets wrong."""
    root = _fixture(
        tmp_path,
        football=[{'canonical_name': 'Idaho', 'provider_ids': {'espn': 70, 'cfbd_scoreboard': 70}}],
        basketball=[{'displayName': 'Idaho State Bengals', 'id': 304, 'logos': [_logo(304)]}],
    )
    rows = institutions(SEASON, root)
    assert rows[70]['basketball'] is None
    assert rows[304]['football'] is None


# --- failing closed ---------------------------------------------------------

@pytest.mark.parametrize('bad', [None, 'Michigan', 12.0, True, [57]])
def test_resolve_refuses_anything_that_is_not_an_espn_integer(bad, tmp_path):
    root = _fixture(tmp_path, football=[
        {'canonical_name': 'Florida', 'provider_ids': {'espn': 57, 'cfbd_scoreboard': 57}}], basketball=[])
    assert resolve(bad, SEASON, root) is None


def test_resolve_returns_none_for_a_school_in_neither_feed(tmp_path):
    root = _fixture(tmp_path, football=[
        {'canonical_name': 'Florida', 'provider_ids': {'espn': 57, 'cfbd_scoreboard': 57}}], basketball=[])
    assert resolve(999999, SEASON, root) is None
    assert resolve(57, SEASON, root)['football']['canonical_name'] == 'Florida'


def test_two_basketball_teams_claiming_one_espn_id_are_both_dropped_and_reported(tmp_path):
    root = _fixture(
        tmp_path,
        football=[{'canonical_name': 'Florida', 'provider_ids': {'espn': 57, 'cfbd_scoreboard': 57}}],
        basketball=[
            {'displayName': 'Florida Gators', 'id': 87, 'logos': [_logo(57)]},
            {'displayName': 'Impostor', 'id': 88, 'logos': [_logo(57)]},
        ],
    )
    assert load_basketball_teams(SEASON, root) == {}
    reasons = unresolved_basketball_teams(SEASON, root)
    assert {row['team'] for row in reasons} == {'Florida Gators', 'Impostor'}
    assert all('claimed by 2 teams' in row['reason'] for row in reasons)


def test_a_duplicate_espn_id_in_the_football_registry_is_an_error(tmp_path):
    root = _fixture(
        tmp_path,
        football=[
            {'canonical_name': 'Florida', 'provider_ids': {'espn': 57, 'cfbd_scoreboard': 57}},
            {'canonical_name': 'Florida Again', 'provider_ids': {'espn': 57, 'cfbd_scoreboard': 58}},
        ],
        basketball=[],
    )
    with pytest.raises(ValueError, match='two teams for ESPN id 57'):
        load_football_teams(root)


def test_a_basketball_team_with_no_espn_id_is_reported_not_silently_dropped(tmp_path):
    root = _fixture(
        tmp_path,
        football=[],
        basketball=[{'displayName': 'No Logo State', 'id': 1, 'logos': []}],
    )
    assert load_basketball_teams(SEASON, root) == {}
    reasons = unresolved_basketball_teams(SEASON, root)
    assert reasons == [{'team': 'No Logo State', 'cbbd_id': 1,
                        'reason': 'no ESPN id in any logo URL'}]


# --- against the real snapshots ---------------------------------------------

def test_every_fbs_football_team_pairs_with_a_basketball_team():
    """Measured 2026-11-08: 139 of 139, nothing football-only.

    Literals, not a constant, so moving a constant cannot satisfy this. If a
    future snapshot changes the FBS count this should be re-measured and the
    number here updated deliberately.
    """
    if not (Path(__file__).resolve().parents[1] / f'data/ncaambb/raw/{SEASON}/teams.json.gz').exists():
        pytest.skip('basketball snapshots not present')
    rows = institutions(SEASON)
    football_only = [r for r in rows.values() if r['football'] and not r['basketball']]
    both = [r for r in rows.values() if r['football'] and r['basketball']]
    assert football_only == []
    assert len(both) == 139


def test_no_real_school_pairs_on_the_provider_id():
    """0 of 139 on the real data. If this ever starts passing by coincidence the
    module must still not rely on it."""
    if not (Path(__file__).resolve().parents[1] / f'data/ncaambb/raw/{SEASON}/teams.json.gz').exists():
        pytest.skip('basketball snapshots not present')
    rows = institutions(SEASON)
    agreeing = [
        r for r in rows.values()
        if r['football'] and r['basketball']
        and r['basketball'].get('id') == (r['football'].get('provider_ids') or {}).get('cfbd_scoreboard')
    ]
    assert agreeing == []
