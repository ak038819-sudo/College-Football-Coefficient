"""Namespaced basketball identities.

A bare provider id is never a key here. Football and basketball both come from
a CFBD-family provider that numbers teams and athletes from small integers, so
`18` is BYU's basketball id and also some football team's id. Every stored key
carries its sport, provider and entity kind, which makes a cross-sport join
impossible to write by accident rather than merely discouraged.

This is the production copy. `analysis/cbb_player_impact/contract.py` keeps its
own for the research pilot; src/ never imports from analysis/.
"""

SPORT = 'ncaambb'
PROVIDER = 'cbbd'
KINDS = frozenset({'team', 'game', 'player', 'venue', 'conference'})


def entity_key(kind, provider_id):
    """`ncaambb:cbbd:team:18`. Raises rather than coercing a doubtful id."""
    if kind not in KINDS:
        raise ValueError(f'Unknown entity kind {kind!r}')
    if type(provider_id) is not int or isinstance(provider_id, bool) or provider_id <= 0:
        raise ValueError('Provider ids must be positive integers')
    return f'{SPORT}:{PROVIDER}:{kind}:{provider_id}'


def season_label(ending_year):
    """2026 means the 2025-26 season. Never infer a season from a date alone."""
    if type(ending_year) is not int or isinstance(ending_year, bool) \
            or not 1900 <= ending_year <= 2200:
        raise ValueError('Expected a basketball ending-year season')
    return f'{ending_year - 1}-{str(ending_year)[-2:]}'
