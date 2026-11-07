"""The playoff bracket, derived from the games the archive already has.

NO NEW DATA. The schedule carries every postseason game back to 2002 — 2,094
of them, which group into 375 series, exactly 15 a season in all 25 seasons,
which is a 16-team bracket with nothing missing. So this module is pure
functions over the games and the board `src/nba/export.py` has already
built, and it adds not one query and not one byte of fetched data.

A SERIES IS A SEASON AND AN UNORDERED PAIR OF CLUBS. Two clubs can meet at
most once in a bracket, so the pair identifies the series and no round label
is needed to group the games.

THREE TRAPS, each measured before it was written down:

1. THE 2020 BUBBLE LEFT PHANTOM ROWS. Games that were scheduled, postponed
   and never played are still in the schedule with no score, sitting beside
   the game that replaced them: nine such rows, which made three series look
   eight or nine games long. A game with no score is not part of a series
   here, which also keeps a season in progress from drawing a bracket out of
   fixtures nobody has played.

2. THE 2002 FIRST ROUND WAS BEST-OF-FIVE. Eight of that season's series were
   won with three wins, and a 3-0 sweep is three games; from 2003 on every
   series is best-of-seven. So a series' result is READ off the games rather
   than assumed to be first-to-four, which would call the 2002 first round
   unfinished.

3. THE FEED'S OWN ROUND LABELS CANNOT BE USED. `notes` is absent for all of
   2002 and 2003, says only "PLAYOFFS" for 316 games, and spells the same
   round four ways ("WEST 1ST ROUND", "WESTERN CONFERENCE 1ST ROUND", and
   the same pair for the East). The round is derived STRUCTURALLY instead:
   the k-th series a club plays in a postseason is round k, because the
   winner of a round plays in the next one. Checked across the archive —
   every season comes out 8/4/2/1 and the two clubs in a series never
   disagree about which round it is.

SEEDS ARE DERIVED, NOT STORED. Nothing in the feed says a club was the
fourth seed. A seed here is its rank by regular-season record within its
conference, which is the quantity the real seeding is built on, and a page
says "by record" rather than implying the league's own tiebreakers were
applied: the NBA's are a cascade of head-to-head, division and conference
records that this archive cannot reproduce, so clubs level on record are
left level and share the label.
"""

from collections import defaultdict

# The round names a 16-team bracket has, by the round's own number. The
# conference finals is round 3 and the final round 4, and the names carry no
# conference because a series knows its clubs and a page can read the
# conference off them.
ROUND_NAMES = ('first round', 'conference semifinals', 'conference finals',
               'finals')

# A bracket's shape: how many series each round holds. THIS IS A CHECK, not a
# layout -- a season that does not match it is reported rather than drawn, so
# a format change arrives as a stated refusal instead of a wrong picture.
ROUND_SIZES = (8, 4, 2, 1)


def played(games):
    """The postseason games that were actually played.

    TRAP 1. A score is what says a game happened: the 2020 bubble's
    postponed rows carry none, and neither does a fixture in a season whose
    postseason has not been reached.
    """
    return [game for game in games
            if game.get('type') == 'postseason'
            and game.get('home_points') is not None
            and game.get('away_points') is not None]


def pair_of(game):
    """A series key: the two clubs, in a fixed order, so that home and away
    swapping between games does not make two series out of one."""
    return tuple(sorted((game['home'], game['away'])))


def group(games):
    """`[(pair, [games in date order])]`, the series in the order they began.

    Ordered by the first game rather than by the round, because the round is
    not known until the series are in order -- which is what trap 3 is
    about.
    """
    grouped = defaultdict(list)
    for game in played(games):
        grouped[pair_of(game)].append(game)
    for rows in grouped.values():
        rows.sort(key=lambda game: (game.get('date') or '', game.get('id')))
    return sorted(grouped.items(),
                  key=lambda item: (item[1][0].get('date') or '',
                                    item[0]))


def result(pair, games):
    """Who won the series and by how many games.

    TRAP 2: the wins are counted and compared, never measured against a
    best-of length. A tie is left as a tie with no winner rather than broken,
    which is what a series still being played looks like.
    """
    first, second = pair
    wins = {first: 0, second: 0}
    for game in games:
        if game['home_points'] == game['away_points']:
            continue
        winner = (game['home'] if game['home_points'] > game['away_points']
                  else game['away'])
        if winner in wins:
            wins[winner] += 1
    if wins[first] == wins[second]:
        return None, None, wins
    if wins[first] > wins[second]:
        return first, second, wins
    return second, first, wins


def rounds(ordered):
    """`{pair: round number}` by the structural rule of trap 3.

    The k-th series a club plays is round k. Where the two clubs disagree —
    which does not happen anywhere in this archive, and would mean the
    bracket is not a bracket — the later of the two is taken and the
    disagreement is reported by `shape`.
    """
    seen = defaultdict(int)
    numbers = {}
    for pair, _ in ordered:
        first, second = pair
        numbers[pair] = max(seen[first], seen[second]) + 1
        seen[first] += 1
        seen[second] += 1
    return numbers


def disagreements(ordered):
    """The series whose two clubs are on different rounds. Empty everywhere
    in this archive; reported rather than assumed away."""
    seen = defaultdict(int)
    odd = []
    for pair, _ in ordered:
        first, second = pair
        if seen[first] != seen[second]:
            odd.append(pair)
        seen[first] += 1
        seen[second] += 1
    return odd


def seeds(board_rows):
    """`{team key: seed}` within each conference, by regular-season record.

    By RECORD, which is the quantity the league seeds on, and not by the
    rating, which would put a club the standings had seventh at number two.
    Clubs level on record share a seed: see the module note on tiebreakers.
    A club with no conference is left unseeded rather than filed under a
    guess.
    """
    by_conference = defaultdict(list)
    for row in board_rows or ():
        if row.get('conference'):
            by_conference[row['conference']].append(row)
    out = {}
    for rows in by_conference.values():
        def rank_key(row):
            wins, losses = row.get('wins') or 0, row.get('losses') or 0
            played_games = wins + losses
            # Win rate, not wins: a club that has played fewer games is not
            # behind for having played fewer.
            return -(wins / played_games if played_games else 0.0)
        rows.sort(key=lambda row: (rank_key(row), row['team']))
        seed, previous = 0, None
        for position, row in enumerate(rows, start=1):
            here = rank_key(row)
            if previous is None or here != previous:
                seed = position
                previous = here
            out[row['team']] = seed
    return out


def shape(by_round):
    """What is wrong with this bracket's shape, as a list of plain reasons.

    A season that does not hold 8, 4, 2 and 1 series is REPORTED rather than
    drawn: if the league changes the format, a reader should be told the
    archive no longer knows the shape instead of being shown a picture that
    quietly drops a series.
    """
    reasons = []
    for number, expected in enumerate(ROUND_SIZES, start=1):
        found = len(by_round.get(number, ()))
        if found != expected:
            reasons.append(f'round {number} has {found} series, not '
                           f'{expected}')
    extra = sorted(number for number in by_round
                   if number > len(ROUND_SIZES))
    for number in extra:
        reasons.append(f'there is a round {number}, which a 16-team bracket '
                       'does not have')
    return reasons


def build(games, board_rows=None):
    """One season's bracket, or None where the season has no postseason yet.

    `{'rounds': [[series, ...], ...], 'ok': 0|1, 'why': [...]}`, each series
    `{'teams': [a, b], 'wins': [a's, b's], 'won': key|None, 'games': [...]}`.
    `ok` is 0 where `shape` found something, and then a page says the
    archive cannot draw this bracket rather than drawing a wrong one.
    """
    ordered = group(games)
    if not ordered:
        return None
    numbers = rounds(ordered)
    seeded = seeds(board_rows)
    by_round = defaultdict(list)
    for pair, rows in ordered:
        won, lost, wins = result(pair, rows)
        first, second = pair
        entry = dict(
            teams=[first, second],
            wins=[wins[first], wins[second]],
            won=won,
            lost=lost,
            games=[game['id'] for game in rows],
            dates=[rows[0].get('date'), rows[-1].get('date')],
        )
        if seeded.get(first) or seeded.get(second):
            entry['seeds'] = [seeded.get(first), seeded.get(second)]
        by_round[numbers[pair]].append(entry)
    reasons = shape(by_round)
    for pair in disagreements(ordered):
        reasons.append('the two clubs of one series are on different rounds')
        break
    return dict(
        rounds=[by_round[number] for number in sorted(by_round)],
        names=list(ROUND_NAMES),
        ok=0 if reasons else 1,
        why=reasons,
    )


def playin(games):
    """The play-in games, which are not a bracket and are not drawn as one.

    TWO FORMATS, not one: the 2020 bubble had a SINGLE game, and every
    season from 2021 has six, four clubs a conference. So these are listed
    as games with what each decided left unsaid, rather than fitted to a
    shape the archive has held for only part of its span.
    """
    rows = [game for game in games if game.get('type') == 'playin']
    rows.sort(key=lambda game: (game.get('date') or '', game.get('id')))
    return [dict(id=game['id'], date=game.get('date'),
                 teams=[game['home'], game['away']],
                 points=[game.get('home_points'), game.get('away_points')])
            for game in rows] or None
