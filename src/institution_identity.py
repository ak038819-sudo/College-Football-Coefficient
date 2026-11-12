#!/usr/bin/env python3
"""Join a school's football team to its basketball team.

This is the thing My Teams needs and the repository did not have: a reader who
favourites Michigan should get Michigan football and Michigan basketball, and
nothing in `src/` previously referenced both leagues' identities at once.

WHAT THE JOIN KEY IS, AND WHY IT IS NOT THE PROVIDER ID. Measured on the 2026
snapshots (docs/identity-cross-league-measurement.md):

  * CBBD team id == CFBD team id for **0 of 139** schools present in both.
    The two feeds number teams independently even though both are CFBD-family.
  * **81 CBBD ids are also valid CFBD football ids belonging to a different
    school.** So a naive id join does not fail — it silently returns the wrong
    team. CBBD's Florida (87) lands on football's Notre Dame; CBBD's Colorado
    State (57) lands on football's Florida.

That is the worst possible failure shape for an identity layer, so the provider
id is refused as a cross-league key here, the way each league's own
`identity.py` refuses a bare provider id as an entity key.

The key used instead is the **ESPN team id**, which both sides already carry:
`data/reference/team_identities.json` stores it per football team, and CBBD
embeds it in its logo URLs (`.../cbb-logos/500/<espn_id>.png`) for 365 of 365
teams. On the 2026 data it pairs 139 of 139 FBS football schools with a
basketball team, with no football team left unmatched.

Display names happen to agree for all 139 pairs, so a name join would also have
worked today. It is still not used: a name is a label the provider may restyle,
and "Idaho" / "Idaho State" / "Illinois State" are the kind of near-miss that
makes name matching quietly wrong. The id is free, so there is no reason to
spend a guess.

The resolver FAILS CLOSED. An unknown or ambiguous school returns None rather
than a best guess, because the roadmap's own rule for this risk is "fail closed
and require reviewed mapping", and a wrong team association is worse to a
reader than a missing one.
"""
from __future__ import annotations

import gzip
import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
FOOTBALL_REGISTRY = 'data/reference/team_identities.json'
BASKETBALL_TEAMS = 'data/ncaambb/raw/{season}/teams.json.gz'

# `.../cbb-logos/500/2561.png` and the dark and 128/256 variants all end in the
# ESPN id. Anchored at the path separator so a size ("500") cannot be read as
# the id.
_LOGO_ESPN = re.compile(r'/(\d+)\.png$')


def espn_id_from_logos(logos) -> int | None:
    """The ESPN team id CBBD embeds in a logo URL, or None.

    Returns None rather than raising for a team with no usable logo, since that
    is a coverage gap in the feed and not a programming error.
    """
    for url in logos or []:
        if not isinstance(url, str):
            continue
        found = _LOGO_ESPN.search(url)
        if found:
            return int(found.group(1))
    return None


def _read_json(path: Path):
    if path.suffix == '.gz':
        with gzip.open(path, 'rt', encoding='utf-8') as handle:
            return json.load(handle)
    return json.loads(path.read_text(encoding='utf-8'))


def load_football_teams(root: Path | None = None) -> dict[int, dict]:
    """ESPN id -> football team, from the reviewed registry.

    The path is resolved inside the function, not bound as a default argument,
    so a test can point this at a fixture. A default argument binds at def time
    and has made tests in this repository pass vacuously more than once.
    """
    root = root or REPO_ROOT
    rows = _read_json(root / FOOTBALL_REGISTRY)['teams']
    teams: dict[int, dict] = {}
    for row in rows:
        espn = (row.get('provider_ids') or {}).get('espn')
        if espn is None:
            continue
        espn = int(espn)
        if espn in teams:
            raise ValueError(f'football registry has two teams for ESPN id {espn}')
        teams[espn] = row
    return teams


def load_basketball_teams(season: int, root: Path | None = None) -> dict[int, dict]:
    """ESPN id -> basketball team, for one season's CBBD snapshot.

    A duplicate ESPN id is dropped rather than overwriting, and both are
    reported by `unresolved_basketball_teams`, because picking one silently is
    how a reader ends up on another school's page.
    """
    root = root or REPO_ROOT
    rows = _read_json(root / BASKETBALL_TEAMS.format(season=season))
    teams: dict[int, dict] = {}
    duplicates: set[int] = set()
    for row in rows:
        espn = espn_id_from_logos(row.get('logos'))
        if espn is None:
            continue
        if espn in teams:
            duplicates.add(espn)
            continue
        teams[espn] = row
    for espn in duplicates:
        teams.pop(espn, None)
    return teams


def institutions(season: int, root: Path | None = None) -> dict[int, dict]:
    """ESPN id -> {espn_id, football, basketball} for every school in either feed.

    A school present in only one league is included with the other side None.
    That is the honest shape: 226 of the 2026 basketball teams have no FBS
    football team, and a My Teams entry for one of them is still valid.
    """
    football = load_football_teams(root)
    basketball = load_basketball_teams(season, root)
    out: dict[int, dict] = {}
    for espn in sorted(set(football) | set(basketball)):
        out[espn] = {
            'espn_id': espn,
            'football': football.get(espn),
            'basketball': basketball.get(espn),
        }
    return out


def resolve(espn_id, season: int, root: Path | None = None) -> dict | None:
    """One school's two teams, or None if the id is not in either feed.

    Fails closed: no name fallback, no nearest match.
    """
    if isinstance(espn_id, bool) or not isinstance(espn_id, int):
        return None
    return institutions(season, root).get(espn_id)


def unresolved_basketball_teams(season: int, root: Path | None = None) -> list[dict]:
    """Basketball teams this module cannot key, with the reason.

    Kept as a reported quantity rather than a silent drop, following
    docs/identity-problems.md: a coverage gap that nobody counts becomes a
    coverage gap that nobody fixes.
    """
    root = root or REPO_ROOT
    rows = _read_json(root / BASKETBALL_TEAMS.format(season=season))
    seen: dict[int, dict] = {}
    duplicated: dict[int, list[dict]] = {}
    out: list[dict] = []
    for row in rows:
        espn = espn_id_from_logos(row.get('logos'))
        if espn is None:
            out.append({'team': row.get('displayName'), 'cbbd_id': row.get('id'),
                        'reason': 'no ESPN id in any logo URL'})
            continue
        if espn in seen:
            duplicated.setdefault(espn, [seen[espn]]).append(row)
        else:
            seen[espn] = row
    for espn, rows_for_id in duplicated.items():
        for row in rows_for_id:
            out.append({'team': row.get('displayName'), 'cbbd_id': row.get('id'),
                        'reason': f'ESPN id {espn} claimed by {len(rows_for_id)} teams'})
    return out


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description='Report the cross-league institution join.')
    parser.add_argument('--season', type=int, default=2026)
    args = parser.parse_args()
    rows = institutions(args.season)
    both = [r for r in rows.values() if r['football'] and r['basketball']]
    fb_only = [r for r in rows.values() if r['football'] and not r['basketball']]
    bb_only = [r for r in rows.values() if r['basketball'] and not r['football']]
    print(f'season {args.season}: {len(rows)} institutions')
    print(f'  both leagues:    {len(both)}')
    print(f'  football only:   {len(fb_only)}')
    print(f'  basketball only: {len(bb_only)}')
    unresolved = unresolved_basketball_teams(args.season)
    print(f'  unkeyable basketball teams: {len(unresolved)}')
    for row in unresolved[:10]:
        print(f'    {row["team"]}: {row["reason"]}')


if __name__ == '__main__':
    main()
