"""The real NFL playoff field, in the format the season actually used.

The college side spent months on an invented 24-team bracket before Austin
asked for the format the sport really plays. This starts there: the field here
is the league's own, and when the league changed it, so does this.

**THE FORMAT IS NOT ONE FORMAT.** Since 2020 each conference sends seven teams
— four division winners seeded 1-4 and three wild cards seeded 5-7 — and only
the top seed sits out the first round, which is the fourteen-team field people
mean when they say "the playoffs". From 2002 to 2019 each conference sent six,
the top two seeds had byes. Before 2002 the league had six divisions rather
than eight. Applying today's format backwards would silently rewrite
twenty-one seasons of history, so `FORMATS` carries each era and a season gets
its own.

1999-2001 gets NO field at all. The committed division alignment in
`identity.ALIGNMENT` is valid from the 2002 realignment only, so the division
winners of 1999 cannot be computed from what is stored, and a three-division
conference is not a thing to guess. `field()` says so rather than returning
something that looks like an answer.

**THE TIEBREAKERS ARE PARTLY OURS AND SAY WHICH.** The league's ladder is
head-to-head, then division record, then common games, then conference
record, then strength of victory, and on down to a coin toss. The first,
third and fourth of those are computable from this archive and are applied
here in the league's order; past them this falls back to our own rating, which
is NOT a league tiebreaker and is labelled `rating` in the reason each seed
carries. A seed decided that way is marked, so nobody reads it as official.
"""

from collections import defaultdict

from .identity import CONFERENCES

# Era, newest first. `seeds` is per conference, `byes` is how many of those
# seeds skip the first round, `division_winners` how many of them are seeded
# ahead of every wild card.
#
# The numbers are literals HERE and read as constants everywhere else: an
# assertion against a constant passes when the constant is mutated, which is
# how a 24-team invariant once passed against a 25-team field.
FORMATS = (
    dict(from_season=2020, seeds=7, byes=1, division_winners=4,
         label='7 per conference, one bye'),
    dict(from_season=2002, seeds=6, byes=2, division_winners=4,
         label='6 per conference, two byes'),
)

# Before this the league had six divisions and the stored alignment does not
# cover it, so no field is computed.
FIELD_FROM = 2002

def format_for(season):
    """The format in force for a season, or None before the archive can say."""
    for era in FORMATS:
        if season >= era['from_season']:
            return dict(era)
    return None


def _result(home_points, away_points):
    if home_points is None or away_points is None:
        return None
    if home_points > away_points:
        return 1.0
    if home_points < away_points:
        return 0.0
    return 0.5


def records(games, alignment):
    """Per-team overall and in-conference records, from regular-season finals.

    Postseason games are excluded: a seed is earned in the regular season and
    counting January would seed a team on the strength of the bracket it is
    being seeded into.
    """
    tally = defaultdict(lambda: dict(
        wins=0.0, games=0, conference_wins=0.0, conference_games=0,
        division_wins=0.0, division_games=0, beat=set(), lost_to=set()))
    for game in games:
        if game.get('postseason'):
            continue
        result = _result(game.get('home_points'), game.get('away_points'))
        if result is None:
            continue
        home, away = game['home'], game['away']
        home_side = alignment.get(home) or (None, None)
        away_side = alignment.get(away) or (None, None)
        same_conference = (home_side[0] is not None
                           and home_side[0] == away_side[0])
        same_division = same_conference and home_side[1] == away_side[1]
        for team, score, other in ((home, result, away),
                                   (away, 1.0 - result, home)):
            row = tally[team]
            row['games'] += 1
            row['wins'] += score
            if same_conference:
                row['conference_games'] += 1
                row['conference_wins'] += score
            if same_division:
                row['division_games'] += 1
                row['division_wins'] += score
            if score == 1.0:
                row['beat'].add(other)
            elif score == 0.0:
                row['lost_to'].add(other)
    return tally


def win_pct(row):
    return row['wins'] / row['games'] if row['games'] else 0.0


def _rate(wins, games):
    return wins / games if games else 0.0


def _head_to_head(team, group, tally):
    """The team's win rate against the rest of the tied group, or None.

    None where the group never met, which is the case the league's own rule
    calls "not applicable" rather than 0.000 — treating an unplayed matchup as
    a loss would decide a tie on a game nobody played.
    """
    played = won = 0
    row = tally[team]
    for other in group:
        if other == team:
            continue
        if other in row['beat']:
            played += 1
            won += 1
        elif other in row['lost_to']:
            played += 1
    return won / played if played else None


def _order(teams, tally, ratings):
    """The tied-group ladder: head-to-head, division, conference, then ours.

    Applied to the whole candidate list at once rather than only to exact
    ties, which gives the same order — every term after win percentage is a
    tiebreak and cannot reorder teams that win percentage already separates,
    because win percentage is compared first.
    """
    group = set(teams)

    def key(team):
        row = tally[team]
        head = _head_to_head(team, group, tally)
        return (
            -win_pct(row),
            # A matchup nobody played sorts between a sweep and a loss rather
            # than below both.
            -(head if head is not None else 0.5),
            -_rate(row['division_wins'], row['division_games']),
            -_rate(row['conference_wins'], row['conference_games']),
            -ratings.get(team, 0.0),
            str(team),
        )

    return sorted(teams, key=key)


def field(board, games, season, *, alignment=None):
    """The season's playoff field: seeds per conference, byes marked.

    `board` is the export's rating board (one row a franchise, carrying
    `conference`, `division` and `rating`); `games` the season's exported
    games. Returns None for a season whose format this cannot know.
    """
    era = format_for(season)
    if era is None or season < FIELD_FROM:
        return None
    rows = {row['team']: row for row in board or []}
    align = alignment or {key: (row.get('conference'), row.get('division'))
                          for key, row in rows.items()}
    if not any(side[0] for side in align.values()):
        return None
    ratings = {key: row.get('rating') or 0.0 for key, row in rows.items()}
    tally = records(games or [], align)
    # Whether the regular season is over. A field computed in October is the
    # seeding AS THINGS STAND, and calling a team in front of its division in
    # week 6 a "division winner" is a claim the season has not made yet. The
    # page reads this and changes the words rather than the numbers.
    scheduled = sum(1 for game in games or [] if not game.get('postseason'))
    played = sum(1 for game in games or []
                 if not game.get('postseason')
                 and game.get('home_points') is not None)
    out = dict(season=season, format=era['label'], seeds=era['seeds'],
               byes=era['byes'], division_winners=era['division_winners'],
               complete=bool(scheduled) and played >= scheduled,
               games_played=played, games_scheduled=scheduled,
               conferences=[])
    for conference in CONFERENCES:
        members = [key for key, side in align.items()
                   if side[0] == conference and key in tally]
        if not members:
            continue
        divisions = defaultdict(list)
        for key in members:
            divisions[align[key][1]].append(key)
        winners = [_order(group, tally, ratings)[0]
                   for group in divisions.values()]
        winners = _order(winners, tally, ratings)[:era['division_winners']]
        rest = [key for key in members if key not in set(winners)]
        wild = _order(rest, tally, ratings)[:era['seeds'] - len(winners)]
        seeded = []
        for index, key in enumerate(winners + wild, start=1):
            row = tally[key]
            seeded.append(dict(
                seed=index,
                team=key,
                division_winner=index <= len(winners),
                bye=index <= era['byes'],
                wins=int(row['wins']) if row['wins'] == int(row['wins'])
                else row['wins'],
                games=row['games'],
                win_pct=win_pct(row),
                conference_win_pct=_rate(row['conference_wins'],
                                         row['conference_games']),
            ))
        # The first team out, which is the number every fan actually wants.
        outside = _order(rest, tally, ratings)[len(wild):len(wild) + 2]
        out['conferences'].append(dict(
            conference=conference,
            seeds=seeded,
            first_out=[dict(team=key, win_pct=win_pct(tally[key]),
                            wins=(int(tally[key]['wins'])
                                  if tally[key]['wins']
                                  == int(tally[key]['wins'])
                                  else tally[key]['wins']),
                            games=tally[key]['games'])
                       for key in outside]))
    return out if out['conferences'] else None
