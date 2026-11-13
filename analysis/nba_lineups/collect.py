"""Building the 2017-18 stint file, and auditing one game before the rest.

Read-only. It fetches the ESPN season assets through `src/nba/source.py`,
rebuilds every game's stints with `stints.py`, reconciles each game against
its own box score, and writes a CSV plus a manifest. It touches no database,
publishes nothing, and feeds no rating.

    python analysis/nba_lineups/collect.py --season 2018 --game 400974766
    python analysis/nba_lineups/collect.py --season 2018 \
        --out analysis/nba_lineups/snapshots/2018

**THE AUDIT GAME COMES FIRST.** Utah's 2017-18 opener against Denver is ESPN
400974766 (2017-10-18, Utah 106 Denver 96, Utah at home); the handoff names
it by its NBA.com id, 0021700010, and that provider is unreachable from here
([[nba-data-source]]), so every id in this pilot is an ESPN id and the
crosswalk to `src/nba/people.py` identities is left for the step that
attaches results to a player page.

**TURNOVERS, IN 2017-18 ONLY.** The season's team box carries no
`total_turnovers` at all ([[nba-team-statistics]]), so the box-score
possession estimate reconciled against here uses the narrower player-sum
`turnovers` and is low by the team turnovers -- about 0.6 a team-game.
The stint possessions are counted from events and include them.
"""

import argparse
import csv
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from analysis.nba_lineups import stints as lineups  # noqa: E402
from src.nba import source  # noqa: E402
from src.nba.identity import EXHIBITION_IDS  # noqa: E402

FREE_THROW_WEIGHT = 0.44
# A minute of box score is rounded to a whole number, so a rebuild that
# agrees to within half a minute agrees exactly.
MINUTE_TOLERANCE = 0.5
# Possessions counted from events run about two and a half below the box
# estimate, measured below; three is the slack that tolerance allows.
POSSESSION_TOLERANCE = 6.0

FIELDS = ('game_id', 'date', 'period', 'stint_id', 'home_team', 'away_team',
          'home_player_ids', 'away_player_ids', 'start_clock', 'end_clock',
          'home_points', 'away_points', 'home_possessions',
          'away_possessions', 'venue', 'quality_status')


def _clock(period, when):
    """A display clock for an elapsed-seconds mark."""
    left = lineups.period_start(period) + lineups.period_length(period) - when
    left = max(0.0, left)
    minutes, seconds = divmod(int(round(left)), 60)
    return '%d:%02d' % (minutes, seconds)


def by_game(rows):
    """Rows grouped by game id. A season is read once, not once a game."""
    grouped = {}
    for row in rows:
        grouped.setdefault(str(row.get('game_id')), []).append(row)
    return grouped


def _of_game(rows, game_id):
    """Just the rows of one game, in the order the feed lists them.

    Takes either a flat sequence or the result of `by_game`, so a one-game
    audit can hand it a season and a season build hands it an index.
    """
    game_id = str(game_id)
    if isinstance(rows, dict):
        return rows.get(game_id, [])
    return [row for row in rows if str(row.get('game_id')) == game_id]


def game_inputs(plays, player_box, game_id):
    """(plays, starters, team_of) for one game."""
    rows = sorted(_of_game(plays, game_id),
                  key=lambda row: (int(row.get('period_number') or 0),
                                   int(row.get('game_play_number') or 0)))
    team_of, starters = {}, {}
    for line in _of_game(player_box, game_id):
        player = lineups._identifier(line.get('athlete_id'))
        team = lineups._identifier(line.get('team_id'))
        if player is None or team is None:
            continue
        team_of[player] = team
        starters.setdefault(team, set())
        if str(line.get('starter')).lower() in ('true', '1'):
            starters[team].add(player)
    return rows, starters, team_of


def home_away(player_box, game_id):
    """(home team id, away team id) from the box score's own labelling."""
    sides = {}
    for line in _of_game(player_box, game_id):
        side = str(line.get('home_away')).lower()
        team = lineups._identifier(line.get('team_id'))
        if team is not None:
            sides[side] = team
    return sides.get('home'), sides.get('away')


def box_possessions(team_box, game_id):
    """The box-score possession estimate for each side of a game."""
    estimate = {}
    for line in _of_game(team_box, game_id):
        team = lineups._identifier(line.get('team_id'))
        if team is None:
            continue
        attempts = lineups._number(line.get('field_goals_attempted')) or 0.0
        throws = lineups._number(line.get('free_throws_attempted')) or 0.0
        rebounds = lineups._number(line.get('offensive_rebounds')) or 0.0
        turnovers = lineups._number(line.get('turnovers')) or 0.0
        estimate[team] = (attempts + FREE_THROW_WEIGHT * throws
                          - rebounds + turnovers)
    return estimate


def box_minutes(player_box, game_id):
    """Minutes from the box score, for the players it says appeared.

    A ZERO IS A STATED ZERO. Three Spurs entered 400974440 with ten
    seconds left and the box rounds each of them to 0.0 minutes; reading
    that as "did not appear" made the rebuild look wrong in exactly the
    games where it was right. Only a missing minute (did not play) is
    left out.
    """
    played = {}
    for line in _of_game(player_box, game_id):
        player = lineups._identifier(line.get('athlete_id'))
        minutes = lineups._number(line.get('minutes'))
        if player is not None and minutes is not None:
            played[player] = minutes
    return played


def exhibition(starters):
    """Whether a game is an exhibition, by the teams playing in it.

    **THE ALL-STAR TRAP AGAIN.** 2017-18's All-Star Game (401001760, Team
    LeBron against Team Stephen) and Rising Stars game (401018033, USA
    against World) both carry `season_type` 2, so a filter on season type
    keeps them -- and both have full play-by-play, so the rebuild happily
    produced 56 stints of them. `identity.EXHIBITION_IDS` holds TEAM ids,
    and those four clubs are in it.
    """
    return any(team in EXHIBITION_IDS for team in starters)


def build_game(plays, player_box, game_id, *, date=None, venue=None):
    """Every stint of one game, as records in the handoff's shape."""
    rows, starters, team_of = game_inputs(plays, player_box, game_id)
    if exhibition(starters):
        return [], ['an exhibition, by the teams playing in it'], {}
    if not rows or any(len(five) != lineups.LINEUP_SIZE
                       for five in starters.values()) \
            or len(starters) != 2:
        return [], ['no usable starters or plays'], {}
    home, away = home_away(player_box, game_id)
    teams = sorted(starters)
    if home not in teams or away not in teams:
        home, away = teams[0], teams[1]
    spans, notes = lineups.intervals(rows, starters, team_of)
    points, possessions = lineups.measure(rows, spans, teams,
                                          home=home, away=away)
    adrift = minutes_adrift(spans, player_box, game_id)
    # A SUBSTITUTION SPLIT BY A PLAY LEAVES A STINT OF NO LENGTH. Three
    # changes at one dead ball with a free throw among them are two groups,
    # so the floor between them holds that free throw and no clock at all.
    # One holding nothing is noise and is dropped; one holding the free
    # throw is the only row that free throw belongs to.
    keep = [index for index, span in enumerate(spans)
            if span['end'] > span['start']
            or any(points[index].values()) or any(possessions[index].values())]
    records = []
    for place, index in enumerate(keep):
        span = spans[index]
        status = lineups.quality(span, points[index], possessions[index],
                                 adrift=adrift)
        records.append({
            'game_id': str(game_id),
            'date': date or '',
            'period': span['period'],
            'stint_id': place + 1,
            'home_team': home,
            'away_team': away,
            'home_player_ids': ' '.join(str(one) for one in
                                        sorted(span['lineups'].get(home, ()))),
            'away_player_ids': ' '.join(str(one) for one in
                                        sorted(span['lineups'].get(away, ()))),
            'start_clock': _clock(span['period'], span['start']),
            'end_clock': _clock(span['period'], span['end']),
            'home_points': points[index].get(home, 0.0),
            'away_points': points[index].get(away, 0.0),
            'home_possessions': possessions[index].get(home, 0),
            'away_possessions': possessions[index].get(away, 0),
            'venue': venue or '',
            'quality_status': status,
        })
    return records, notes, {'spans': spans, 'teams': (home, away)}


def minutes_adrift(spans, player_box, game_id):
    """The players whose rebuilt minutes do not match the box score.

    A stint holding one of them is not trustworthy even when it holds five
    players a side, because the substitution that went missing put someone
    on the floor who was not there.
    """
    rebuilt = lineups.minutes_played(spans)
    stated = box_minutes(player_box, game_id)
    adrift = set()
    for player in set(rebuilt) | set(stated):
        gap = rebuilt.get(player, 0.0) - stated.get(player, 0.0)
        if round(abs(gap), 2) > MINUTE_TOLERANCE:
            adrift.add(player)
    return adrift


def reconcile(records, rebuilt, player_box, team_box, game_id):
    """What the rebuild got wrong about a game, measured against the box."""
    home, away = rebuilt['teams']
    rebuilt_minutes = lineups.minutes_played(rebuilt['spans'])
    stated = box_minutes(player_box, game_id)
    short = []
    for player, minutes in sorted(stated.items()):
        gap = rebuilt_minutes.get(player, 0.0) - minutes
        if round(abs(gap), 2) > MINUTE_TOLERANCE:
            short.append({
                'athlete_id': player, 'box': minutes,
                'rebuilt': round(rebuilt_minutes.get(player, 0.0), 2),
                'difference': round(gap, 2)})
    phantom = sorted(set(rebuilt_minutes) - set(stated))
    estimate = box_possessions(team_box, game_id)
    counted = {home: sum(row['home_possessions'] for row in records),
               away: sum(row['away_possessions'] for row in records)}
    scored = {home: sum(row['home_points'] for row in records),
              away: sum(row['away_points'] for row in records)}
    gaps = {team: round(counted[team] - estimate.get(team, 0.0), 2)
            for team in (home, away)}
    return {
        'game_id': str(game_id),
        'stints': len(records),
        'minutes_disagreements': short,
        'players_on_the_floor_with_no_box_minutes': phantom,
        'possessions_counted': counted,
        'possessions_estimated': {team: round(estimate.get(team, 0.0), 2)
                                  for team in (home, away)},
        'possession_gap': gaps,
        'points_rebuilt': scored,
        'possessions_within_tolerance': all(
            abs(gap) <= POSSESSION_TOLERANCE for gap in gaps.values()),
        'minutes_agree': not short and not phantom,
        'statuses': _tally(records),
    }


def _tally(records):
    counted = {}
    for row in records:
        counted[row['quality_status']] = \
            counted.get(row['quality_status'], 0) + 1
    return counted


def audit(plays, player_box, team_box, game_id, *, date=None, venue=None):
    """The single-game report the audit step asks for."""
    records, notes, rebuilt = build_game(plays, player_box, game_id,
                                         date=date, venue=venue)
    if not records:
        return {'game_id': str(game_id), 'notes': notes, 'stints': 0}
    report = reconcile(records, rebuilt, player_box, team_box, game_id)
    report['notes'] = notes
    report['periods'] = sorted({row['period'] for row in records})
    report['five_a_side'] = all(
        len(row['home_player_ids'].split()) == lineups.LINEUP_SIZE
        and len(row['away_player_ids'].split()) == lineups.LINEUP_SIZE
        for row in records)
    report['ordered'] = all(
        (records[index]['period'], records[index]['stint_id'])
        < (records[index + 1]['period'], records[index + 1]['stint_id'])
        or records[index]['period'] == records[index + 1]['period']
        for index in range(len(records) - 1))
    report['records'] = records
    return report


def schedule_facts(schedule):
    """game id -> (date, venue), for stamping the rows."""
    facts = {}
    for row in schedule:
        facts[str(row.get('id'))] = (str(row.get('game_date') or ''),
                                     str(row.get('venue_full_name') or ''))
    return facts


def expected_games(schedule):
    """The completed, non-exhibition games a season OUGHT to rebuild.

    **A GAME WITH NO PLAY-BY-PLAY IS THE ONE A PLAY INDEX CANNOT SHOW
    YOU.** Taking the game list from the plays makes a missing feed look
    like a season that never had the game: four completed 2017-18
    regular-season games have no play-by-play at all (400975670 and
    400975617, both Sacramento; 400974444; 400974773), and they have to
    be in the denominator to be noticed.
    """
    wanted = []
    for row in schedule:
        if not str(row.get('status_type_completed')).lower() in ('true', '1'):
            continue
        teams = (lineups._identifier(row.get('home_id')),
                 lineups._identifier(row.get('away_id')))
        if any(team in EXHIBITION_IDS for team in teams if team is not None):
            continue
        wanted.append(str(row.get('id')))
    return wanted


def build_season(plays, player_box, team_box, schedule, *, limit=None):
    """Every game of a season, with one reconciliation report each."""
    facts = schedule_facts(schedule)
    plays, player_box = by_game(plays), by_game(player_box)
    team_box = by_game(team_box)
    ids = sorted(set(expected_games(schedule)) | set(plays))
    if limit:
        ids = ids[:int(limit)]
    rows, reports = [], []
    for game_id in ids:
        date, venue = facts.get(game_id, ('', ''))
        if game_id not in plays:
            reports.append({'game_id': game_id, 'stints': 0,
                            'date': date, 'plays': 0,
                            'notes': ['no play-by-play was published for '
                                      'this game']})
            continue
        records, notes, rebuilt = build_game(plays, player_box, game_id,
                                             date=date, venue=venue)
        if not records:
            reports.append({'game_id': game_id, 'stints': 0, 'notes': notes})
            continue
        report = reconcile(records, rebuilt, player_box, team_box, game_id)
        report['notes'] = notes
        reports.append(report)
        rows.extend(records)
    return rows, reports


def coverage(rows, reports):
    """One summary of what the season's rebuild is worth."""
    statuses = _tally(rows)
    usable = [one for one in reports if one.get('stints')]
    missing = [one for one in reports if one.get('plays') == 0]
    return {
        'games': len(reports),
        'games_rebuilt': len(usable),
        'games_without_stints': len(reports) - len(usable),
        'games_without_play_by_play': len(missing),
        'games_without_play_by_play_ids': [one['game_id'] for one in missing],
        'stints': len(rows),
        'stints_a_game': round(len(rows) / len(usable), 2) if usable else 0,
        'statuses': statuses,
        'verified_share': round(
            statuses.get(lineups.VERIFIED, 0) / len(rows), 4) if rows else 0,
        'games_whose_minutes_agree':
            sum(1 for one in usable if one['minutes_agree']),
        'games_whose_possessions_agree':
            sum(1 for one in usable if one['possessions_within_tolerance']),
        'possession_gap_mean': round(
            sum(gap for one in usable
                for gap in one['possession_gap'].values())
            / max(1, 2 * len(usable)), 3),
    }


def write(rows, reports, summary, out, *, season, revision):
    """The CSV, the per-game reconciliation, and a manifest beside them."""
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    table = out / ('stints_%d.csv' % int(season))
    with table.open('w', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    (out / ('reconciliation_%d.json' % int(season))).write_text(
        json.dumps(reports, indent=1, sort_keys=True) + '\n')
    digest = hashlib.sha256(table.read_bytes()).hexdigest()
    manifest = {
        'pilot': 'nba-lineups',
        'season': int(season),
        'built_at': datetime.now(timezone.utc).isoformat(),
        'source_revision': revision,
        'identifiers': 'ESPN',
        'stint_file': table.name,
        'stint_file_sha256': digest,
        'summary': summary,
        'published': False,
    }
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=1,
                                                  sort_keys=True) + '\n')
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--season', type=int, default=2018)
    parser.add_argument('--game', default=None,
                        help='audit one game and print the report')
    parser.add_argument('--limit', type=int, default=None)
    parser.add_argument('--out', default=None)
    parser.add_argument('--reuse-cache', action='store_true', default=True)
    args = parser.parse_args(argv)

    frames = {}
    for dataset in ('pbp', 'player_box', 'team_box', 'schedule'):
        path, how = source.load_asset(dataset, args.season,
                                      reuse_cache=args.reuse_cache)
        print('%-11s %s (%s)' % (dataset, path, how), file=sys.stderr)
        frames[dataset] = source.read_parquet(path)

    if args.game:
        facts = schedule_facts(frames['schedule'])
        date, venue = facts.get(str(args.game), ('', ''))
        report = audit(frames['pbp'], frames['player_box'],
                       frames['team_box'], args.game, date=date, venue=venue)
        records = report.pop('records', [])
        print(json.dumps(report, indent=1, sort_keys=True))
        for row in records:
            print('P%d #%-3d %5s-%5s  %s | %s  %s' % (
                row['period'], row['stint_id'], row['start_clock'],
                row['end_clock'], row['home_player_ids'],
                row['away_player_ids'], row['quality_status']))
        return 0

    rows, reports = build_season(frames['pbp'], frames['player_box'],
                                 frames['team_box'], frames['schedule'],
                                 limit=args.limit)
    summary = coverage(rows, reports)
    print(json.dumps(summary, indent=1, sort_keys=True))
    if args.out:
        manifest = write(rows, reports, summary, args.out,
                         season=args.season, revision=source.source_revision())
        print(json.dumps(manifest['stint_file_sha256']))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
