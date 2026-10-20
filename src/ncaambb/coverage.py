"""Season and team coverage measurement.

The point of this module is to make absence visible. A count of games is not
coverage: a season is only accounted for when every game is classified, every
final has a usable score, and the games excluded from Division I rating are
named rather than quietly dropped.
"""

from .games import game_day

STATUSES = ('scheduled', 'in_progress', 'final', 'postponed', 'cancelled')
SEASON_TYPES = ('preseason', 'regular', 'postseason')


def _score(value):
    return type(value) is int and value >= 0


def classify_game(game, division_one_ids):
    """Describe one game's usability without changing or filling it.

    `division_one_ids` is the set of team ids the provider lists for that
    season. A game against a team outside it is retained for schedules and
    records and excluded from Division I rating, which is a decision recorded
    per game, not a silent filter.
    """
    home, away = game.get('homeTeamId'), game.get('awayTeamId')
    known = {home, away} <= set(division_one_ids)
    status = game.get('status')
    scores = (game.get('homePoints'), game.get('awayPoints'))
    has_scores = all(_score(value) for value in scores)
    reasons = []
    if status not in STATUSES:
        reasons.append('unknown status')
    if game.get('seasonType') not in SEASON_TYPES:
        reasons.append('unknown season type')
    if type(home) is not int or type(away) is not int:
        reasons.append('missing team id')
    elif home == away:
        reasons.append('team plays itself')
    if not known:
        reasons.append('non-Division I opponent')
    if status == 'final':
        if not has_scores:
            reasons.append('final without a usable score')
        elif scores[0] == scores[1]:
            reasons.append('final recorded as a tie')
    if game_day(game) is None:
        reasons.append('missing or naive start date')
    rateable = (status == 'final' and has_scores and not reasons
                and game.get('seasonType') != 'preseason')
    return dict(game_id=game.get('id'), status=status,
                season_type=game.get('seasonType'),
                division_one=known, rateable=rateable,
                start_time_tbd=game.get('startTimeTbd') is True,
                neutral_site=game.get('neutralSite') is True,
                exclusions=reasons)


def summarize_season(season, games, division_one_ids):
    """Account for every game of a season. Totals must add up, by construction."""
    rows = [classify_game(game, division_one_ids) for game in games]
    by_status, by_type, exclusions = {}, {}, {}
    teams = set()
    for game, row in zip(games, rows):
        by_status[row['status']] = by_status.get(row['status'], 0) + 1
        by_type[row['season_type']] = by_type.get(row['season_type'], 0) + 1
        for reason in row['exclusions']:
            exclusions[reason] = exclusions.get(reason, 0) + 1
        for side in ('homeTeamId', 'awayTeamId'):
            if type(game.get(side)) is int:
                teams.add(game[side])
    rateable = sum(row['rateable'] for row in rows)
    finals = by_status.get('final', 0)
    unplayed = sum(by_status.get(status, 0)
                   for status in ('scheduled', 'in_progress'))
    abandoned = sum(by_status.get(status, 0)
                    for status in ('postponed', 'cancelled'))
    accounted = rateable + (finals - rateable) + unplayed + abandoned
    if accounted != len(games):
        raise AssertionError('Coverage does not account for every game')
    return dict(
        season=season, games=len(games), finals=finals, rateable=rateable,
        finals_not_rateable=finals - rateable, unplayed=unplayed,
        postponed_or_cancelled=abandoned,
        by_status=dict(sorted(by_status.items(), key=lambda kv: str(kv[0]))),
        by_season_type=dict(sorted(by_type.items(), key=lambda kv: str(kv[0]))),
        exclusions=dict(sorted(exclusions.items())),
        teams_in_games=len(teams),
        division_one_teams=len(set(division_one_ids)),
        teams_outside_division_one=len(teams - set(division_one_ids)),
        start_time_tbd=sum(row['start_time_tbd'] for row in rows),
        neutral_site=sum(row['neutral_site'] for row in rows),
    )


def render(summaries):
    """A short markdown table plus the exclusions behind it."""
    lines = ['| Season | Games | Finals | Rateable | Unplayed | Off | D-I teams | Non-D-I |',
             '| --- | --- | --- | --- | --- | --- | --- | --- |']
    for row in summaries:
        lines.append(
            f'| {row["season"]} | {row["games"]} | {row["finals"]} |'
            f' {row["rateable"]} | {row["unplayed"]} |'
            f' {row["postponed_or_cancelled"]} | {row["division_one_teams"]} |'
            f' {row["teams_outside_division_one"]} |')
    for row in summaries:
        lines += ['', f'### Season {row["season"]}',
                  f'- status: {row["by_status"]}',
                  f'- season type: {row["by_season_type"]}',
                  f'- teams appearing in games: {row["teams_in_games"]}',
                  f'- start time to be decided: {row["start_time_tbd"]};'
                  f' neutral site: {row["neutral_site"]}']
        if row['exclusions']:
            lines.append('- excluded from Division I rating:')
            for reason, count in row['exclusions'].items():
                lines.append(f'  - {reason}: {count}')
        else:
            lines.append('- no game excluded from Division I rating')
    return lines
