"""Compute PxP+ and opportunity grades from NBA regular-season team box scores."""
import argparse
from collections import Counter, defaultdict
import csv
from datetime import date
from itertools import groupby
from pathlib import Path

BOX_FIELDS = ('points', 'field_goal_attempts', 'free_throw_attempts', 'offensive_rebounds', 'turnovers')
INPUT_FIELDS = {'season', 'team', 'opponent', 'game_date', *BOX_FIELDS}
FREE_THROW_POSSESSION_WEIGHT = .44
TEAM_PRIOR_POSSESSIONS = 1500
LEAGUE_PRIOR_POSSESSIONS = 6000


def read_games(path):
    pairs = defaultdict(dict)
    seasons = set()
    with path.open(newline='') as stream:
        reader = csv.DictReader(stream)
        missing = INPUT_FIELDS.difference(reader.fieldnames or ())
        if missing:
            raise ValueError(f'{path}: missing columns: {", ".join(sorted(missing))}')
        for row in reader:
            seasons.add(row['season'])
            team, opponent = row['team'], row['opponent']
            day = date.fromisoformat(row['game_date']).isoformat()
            if not team or not opponent or team == opponent:
                raise ValueError(f'{path}: invalid matchup on {day}')
            key = day, *sorted((team, opponent))
            if team in pairs[key]:
                raise ValueError(f'Duplicate box score: {key}')
            box = {key: int(row[key]) for key in BOX_FIELDS}
            if min(box.values()) < 0:
                raise ValueError(f'{path}: negative box-score total for {team} on {day}')
            pairs[key][team] = box
    if len(seasons) != 1 or not all(seasons):
        raise ValueError(f'{path}: expected one nonempty season')
    games = []
    for (day, a, b), sides in sorted(pairs.items()):
        if len(sides) != 2:
            raise ValueError(f'{path}: missing opposing box score for {a} vs. {b} on {day}')
        box_a, box_b = sides[a], sides[b]
        estimates = [box['field_goal_attempts'] + FREE_THROW_POSSESSION_WEIGHT * box['free_throw_attempts']
                     - box['offensive_rebounds'] + box['turnovers'] for box in (box_a, box_b)]
        possessions = sum(estimates) / 2
        if possessions <= 0:
            raise ValueError(f'{path}: invalid possession estimate for {a} vs. {b} on {day}')
        games.append((day, a, b, possessions, box_a, box_b))
    return games


def extra(box):
    return box['offensive_rebounds'] - box['turnovers']


def rate(total, prior, exposure):
    chances, possessions = total
    return (100 * chances + prior * exposure) / (possessions + exposure)


def record(total, chances, possessions):
    total[0] += chances
    total[1] += possessions


def expected_rate(profiles, team, opponent, league_rate):
    offense = rate(profiles[team, 'offense'], league_rate, TEAM_PRIOR_POSSESSIONS)
    allowed = rate(profiles[opponent, 'allowed'], league_rate, TEAM_PRIOR_POSSESSIONS)
    return offense + allowed - league_rate


def evaluate(games, prior_league_rate):
    league = [0., 0.]
    profiles = defaultdict(lambda: [0., 0.])
    seasons = defaultdict(lambda: Counter())
    game_grades = []
    for day, group in groupby(games, key=lambda g: g[0]):
        today = list(group)
        league_rate = rate(league, prior_league_rate, LEAGUE_PRIOR_POSSESSIONS)
        for _, a, b, p, x, y in today:
            for team, opponent, box, other in ((a, b, x, y), (b, a, y, x)):
                own, opp = extra(box), extra(other)
                expected = expected_rate(profiles, team, opponent, league_rate)
                expected_opp = expected_rate(profiles, opponent, team, league_rate)
                own_residual = 100 * own / p - expected
                opponent_residual = 100 * opp / p - expected_opp
                grade = own_residual - opponent_residual
                total = seasons[team]
                total.update(dict(games=1, possessions=p, points=box['points'], offensive_rebounds=box['offensive_rebounds'],
                                  turnovers=box['turnovers'], own_extra=own, opp_extra=opp, weighted_grade=grade*p))
                game_grades.append(dict(date=day, team=team, opponent=opponent,
                                      points=box['points'], opponent_points=other['points'],
                                      offensive_rebounds=box['offensive_rebounds'], turnovers=box['turnovers'],
                                      opponent_offensive_rebounds=other['offensive_rebounds'],
                                      opponent_turnovers=other['turnovers'], possessions=p,
                                      opportunities_per_100=100 + 100*own/p, expected_opportunities_per_100=100 + expected,
                                      opponent_opportunities_per_100=100 + 100*opp/p,
                                      expected_opponent_opportunities_per_100=100 + expected_opp,
                                      opportunity_edge_above_expected_per_100=grade))
        for _, a, b, p, x, y in today:
            for team, opponent, box in ((a, b, x), (b, a, y)):
                value = extra(box)
                record(league, value, p)
                record(profiles[team, 'offense'], value, p)
                record(profiles[opponent, 'allowed'], value, p)
    ppp = sum(x['points'] for x in seasons.values()) / sum(x['possessions'] for x in seasons.values())
    if ppp <= 0:
        raise ValueError('PxP+ requires a positive league scoring rate')
    season_rows = []
    for team, s in seasons.items():
        p = s['possessions']
        season_rows.append(dict(team=team, games=s['games'], offensive_rebounds_per_100=100*s['offensive_rebounds']/p,
                             turnovers_per_100=100*s['turnovers']/p,
                             opportunities_per_100=100 + 100*s['own_extra']/p,
                             opponent_opportunities_per_100=100 + 100*s['opp_extra']/p,
                             net_opportunities_per_100=100*(s['own_extra']-s['opp_extra'])/p,
                             net_opportunities_above_expected_per_100=s['weighted_grade']/p,
                             pxp_plus=100*(s['points']/p)/ppp))
    season_rows.sort(key=lambda r: -r['net_opportunities_per_100'])
    for key, label in (('opportunities_per_100', 'offensive_opportunity_rank'),
                       ('net_opportunities_per_100', 'net_opportunity_rank'),
                       ('net_opportunities_above_expected_per_100', 'net_opportunities_above_expected_rank'),
                       ('pxp_plus', 'pxp_plus_rank')):
        for rank, row in enumerate(sorted(season_rows, key=lambda r: -r[key]), start=1):
            row[label] = rank
    assert abs(sum(s['weighted_grade'] for s in seasons.values())) < 1e-7
    return season_rows, game_grades, ppp, 100 + 100 * league[0] / league[1]


def save(path, rows):
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('previous', type=Path, help='Prior-season team game CSV')
    parser.add_argument('season', type=Path, help='Target-season team game CSV, complete or in progress')
    parser.add_argument('--out', type=Path, required=True, help='Directory for the two output CSVs')
    args = parser.parse_args()
    past = read_games(args.previous)
    target = read_games(args.season)
    if past[-1][0] >= target[0][0]:
        parser.error('the prior-season CSV must precede all target-season games')
    prior_rate = 100 * sum(extra(x) + extra(y) for _, _, _, _, x, y in past) / sum(2*p for _, _, _, p, *_ in past)
    seasons, games, ppp, league_opportunity = evaluate(target, prior_rate)
    args.out.mkdir(parents=True, exist_ok=True)
    save(args.out / 'team_metrics.csv', seasons)
    save(args.out / 'game_metrics.csv', games)
    print(f'{len(target):,} games, {len(seasons)} teams, league PPP {ppp:.4f}, '
          f'league opportunities per 100 {league_opportunity:.2f}')


if __name__ == '__main__':
    main()
