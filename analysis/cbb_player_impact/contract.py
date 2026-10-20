"""Versioned basketball pilot export. Not consumed by the production site."""

from datetime import datetime

SPORT = 'ncaambb'
PROVIDER = 'cbbd'
STATUSES = {'scheduled', 'in_progress', 'final', 'postponed', 'cancelled'}


def entity_key(kind, provider_id):
    """Never join football and basketball using bare numeric provider IDs."""
    if kind not in {'game', 'team', 'player'}:
        raise ValueError('Unknown entity kind')
    if type(provider_id) is not int or provider_id <= 0:
        raise ValueError('Provider IDs must be positive integers')
    return f'{SPORT}:{PROVIDER}:{kind}:{provider_id}'


def season_label(ending_year):
    if type(ending_year) is not int or not 1900 <= ending_year <= 2200:
        raise ValueError('Expected a basketball ending-year season')
    return f'{ending_year - 1}-{str(ending_year)[-2:]}'


def timestamp(value):
    date = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if date.tzinfo is None:
        raise ValueError('Timestamps require an explicit timezone')
    return value


def pilot_export(games, *, season, team, retrieved_at):
    """Normalize the pilot schedule without claiming national/model readiness.

    This adapter is intentionally NOT a generic football adapter. It does not
    import football current-season rules, model config, IDs or database helpers.
    A future production publisher must require a different, validated scope.
    """
    label = season_label(season)
    if not isinstance(team, str) or not team.strip():
        raise ValueError('Pilot team is required')
    rows, seen = [], set()
    for game in games:
        key = entity_key('game', game['id'])
        if key in seen:
            raise ValueError('Duplicate game ID')
        seen.add(key)
        if game['season'] != season:
            raise ValueError('Mixed seasons in pilot export')
        status = game['status']
        if status not in STATUSES:
            raise ValueError('Unknown game status')
        home = entity_key('team', game['homeTeamId'])
        away = entity_key('team', game['awayTeamId'])
        if home == away:
            raise ValueError('A team cannot play itself')
        if type(game['neutralSite']) is not bool:
            raise ValueError('neutralSite must be explicit')
        scores = [game['homePoints'], game['awayPoints']]
        if any(score is not None and (type(score) is not int or score < 0) for score in scores):
            raise ValueError('Invalid score')
        if status == 'final' and (None in scores or scores[0] == scores[1]):
            raise ValueError('Final game requires a non-tied score')
        rows.append(dict(id=key, provider_id=game['id'], season_end_year=season,
                         starts_at=timestamp(game['startDate']), status=status,
                         neutral_site=game['neutralSite'], home_team_id=home,
                         away_team_id=away, home_score=scores[0], away_score=scores[1]))
    return dict(schema_version=1, sport=SPORT, provider=PROVIDER,
                season_end_year=season, season_label=label,
                retrieved_at=timestamp(retrieved_at),
                scope={'kind': 'team_sample', 'team': team},
                readiness={'public_release': False, 'national_rankings': False,
                           'player_impact': False},
                games=rows)
