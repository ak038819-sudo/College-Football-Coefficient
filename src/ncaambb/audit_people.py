"""What the people layer does and does not cover, measured from the database.

Box scores do not cover every game the schedule holds. That gap is either the
provider having no box score for a game or our own windowed fetch having
truncated one, and those two have opposite remedies, so this measures which it
is instead of reporting a percentage and leaving the cause open.

The same question applies to people: a player with a stat line and no roster
row is either a roster snapshot that excludes his team or an identity we
invented. Both are measured here by whether his team is Division I at all.

Read-only. Writes nothing and fetches nothing.
"""

import argparse
import sys
from collections import Counter

from .people import BOX_SCORE_WINDOW_DAYS, box_score_windows
from .games import season_window
from .store import connect


def seasons(connection):
    return [row['season'] for row in
            connection.execute('SELECT DISTINCT season FROM team_season'
                               ' ORDER BY season')]


def box_score_gap(connection, season):
    """Finals with no box score, split by whether both sides are Division I.

    A game against a team the provider does not list for the season is one we
    store as a placeholder, and the provider has no player data for that side.
    That is a provider boundary. A Division I game missing its box score is
    not, and would mean our fetch lost it.
    """
    rows = connection.execute(
        'SELECT g.game_key, g.start_date, g.home_team_key, g.away_team_key,'
        '       h.division_one AS home_di, a.division_one AS away_di'
        '  FROM game g'
        '  JOIN team_season h ON h.team_key = g.home_team_key'
        '                    AND h.season = g.season'
        '  JOIN team_season a ON a.team_key = g.away_team_key'
        '                    AND a.season = g.season'
        ' WHERE g.season = ? AND g.status = ?'
        '   AND g.game_key NOT IN (SELECT game_key FROM team_game_stat)',
        (season, 'final')).fetchall()
    both_di = [row for row in rows if row['home_di'] and row['away_di']]
    return rows, both_di


def partial_games(connection, season):
    """Games with exactly one side's box score, which is never right."""
    return connection.execute(
        'SELECT game_key, COUNT(*) AS sides FROM team_game_stat'
        ' WHERE game_key IN (SELECT game_key FROM game WHERE season = ?)'
        ' GROUP BY game_key HAVING sides != 2', (season,)).fetchall()


def gap_by_window(connection, season):
    """Missing Division I box scores per fetch window.

    Truncation loses whole stretches of a window, so a gap that is a handful of
    games spread over the calendar is the provider and a gap concentrated in
    one window is us.
    """
    _, both_di = box_score_gap(connection, season)
    missing = Counter(row['start_date'][:10] for row in both_di)
    first, last = season_window(season)
    out = []
    for start, end in box_score_windows(first, last, BOX_SCORE_WINDOW_DAYS):
        total = sum(count for day, count in missing.items()
                    if str(start) <= day <= str(end))
        if total:
            out.append((str(start), str(end), total))
    return out


def unrostered_players(connection, season):
    """Players with a stat line and no roster row, by their team's division."""
    rows = connection.execute(
        'SELECT DISTINCT s.player_key, t.division_one AS di'
        '  FROM player_game_stat s'
        '  JOIN game g ON g.game_key = s.game_key'
        '  JOIN team_season t ON t.team_key = s.team_key AND t.season = g.season'
        ' WHERE g.season = ?'
        '   AND s.player_key NOT IN (SELECT player_key FROM player_team_season'
        '                             WHERE season = ?)', (season, season)
    ).fetchall()
    counts = Counter('Division I' if row['di'] else 'non-Division I'
                     for row in rows)
    return len(rows), counts


def score_disagreements(connection, season):
    """Box scores whose player points do not add up to the final score."""
    return connection.execute(
        'SELECT g.game_key, g.start_date, s.team_key, s.points AS box,'
        '       CASE WHEN s.team_key = g.home_team_key THEN g.home_points'
        '            ELSE g.away_points END AS final'
        '  FROM team_game_stat s JOIN game g USING (game_key)'
        ' WHERE g.season = ? AND s.points IS NOT NULL'
        '   AND CASE WHEN s.team_key = g.home_team_key THEN g.home_points'
        '            ELSE g.away_points END IS NOT NULL'
        '   AND s.points != CASE WHEN s.team_key = g.home_team_key'
        '                        THEN g.home_points ELSE g.away_points END'
        ' ORDER BY g.start_date', (season,)).fetchall()


def report(connection, season):
    lines = [f'## Season {season}']
    finals = connection.execute(
        'SELECT COUNT(*) AS n FROM game WHERE season = ? AND status = ?',
        (season, 'final')).fetchone()['n']
    # Counted against the same games the denominator counts. Counting every
    # game with a box score over a denominator of finals reported 100.3%.
    covered = connection.execute(
        'SELECT COUNT(DISTINCT game_key) AS n FROM team_game_stat WHERE'
        ' game_key IN (SELECT game_key FROM game WHERE season = ?'
        '              AND status = ?)', (season, 'final')).fetchone()['n']
    unplayed = connection.execute(
        'SELECT COUNT(DISTINCT game_key) AS n FROM team_game_stat WHERE'
        ' game_key IN (SELECT game_key FROM game WHERE season = ?'
        '              AND status != ?)', (season, 'final')).fetchone()['n']
    missing, both_di = box_score_gap(connection, season)
    lines.append(f'- finals: {finals}; with a box score: {covered}'
                 f' ({covered / finals:.1%})' if finals else '- no finals')
    lines.append(f'- box scores for games that are not final: {unplayed}')
    lines.append(f'- finals with no box score: {len(missing)}, of which'
                 f' {len(both_di)} are Division I against Division I')
    partial = partial_games(connection, season)
    lines.append(f'- games with only one side\'s box score: {len(partial)}')

    windows = gap_by_window(connection, season)
    if windows:
        lines.append('- missing Division I box scores by fetch window:')
        for start, end, total in windows:
            lines.append(f'  - {start}..{end}: {total}')
    else:
        lines.append('- no Division I game is missing its box score')

    total, counts = unrostered_players(connection, season)
    lines.append(f'- players with a stat line and no roster row: {total}'
                 f' ({dict(counts)})')
    rostered = connection.execute(
        'SELECT COUNT(*) AS n FROM player_team_season WHERE season = ?',
        (season,)).fetchone()['n']
    lines.append(f'- roster memberships stored: {rostered}')

    disagree = score_disagreements(connection, season)
    lines.append(f'- box scores disagreeing with the final score:'
                 f' {len(disagree)}')
    for row in disagree[:20]:
        lines.append(f'  - {row["start_date"][:10]} {row["team_key"]}:'
                     f' box {row["box"]} vs final {row["final"]}')
    nulls = connection.execute(
        'SELECT COUNT(*) AS n FROM team_game_stat s JOIN game g USING'
        ' (game_key) WHERE g.season = ? AND s.points IS NULL',
        (season,)).fetchone()['n']
    lines.append(f'- box scores with no measured points at all: {nulls}')
    return lines


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', default=None, help='Defaults to db/ncaambb.db')
    parser.add_argument('--seasons', default=None,
                        help='Comma-separated ending years; default every season')
    args = parser.parse_args(argv)
    connection = connect(args.database)
    try:
        wanted = ([int(p) for p in args.seasons.split(',') if p.strip()]
                  if args.seasons else seasons(connection))
        if not wanted:
            print('No seasons are loaded', file=sys.stderr)
            return 1
        print('# People coverage audit\n')
        for season in wanted:
            print('\n'.join(report(connection, season)))
            print()
    finally:
        connection.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
