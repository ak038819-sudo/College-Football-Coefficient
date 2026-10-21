"""Namespaced NFL identities, and the franchise rule that relocations need.

A bare provider id is never a key. The NFL source numbers nothing — it names
teams with short codes like `KC` — but a code is still a provider's word for a
team, so it is namespaced the same way basketball's integers are:
`nfl:nflverse:team:KC`.

THE RELOCATION RULE IS THE WHOLE POINT OF THIS MODULE. The source changes a
team's code when the team moves city: Oakland is `OAK` through 2019 and `LV`
from 2020, San Diego is `SD` through 2016 and `LAC` from 2017, St. Louis is
`STL` through 2015 and `LA` from 2016. Those are three relocations, not six
teams. Treating the codes as separate teams would hand the Raiders, Chargers
and Rams a fresh 1500 rating on the day they moved, which is both wrong and
invisible: the ratings would still look like ratings.

So a code resolves to a FRANCHISE, and the franchise is what carries a rating
across seasons. The code is kept as well, because a 2015 box score really does
say `STL` and rewriting history to `LA` would be a different lie.
"""

SPORT = 'nfl'
PROVIDER = 'nflverse'
# `coach` is keyed by the source's name string, because the archive numbers
# coaches nowhere. A name is a weak id and this one is load-bearing, so it is
# normalised (collapsed whitespace) and never guessed at: two coaches sharing
# a name would share a key and nothing in this data could separate them.
KINDS = frozenset({'team', 'franchise', 'game', 'coach'})

# code -> the franchise code it belongs to. Only moves that CHANGE THE CODE
# appear here; a team that renames without the source renaming it needs no
# entry, and inventing one would merge two franchises that were never one.
# Measured against the source on 2026-10-21: 35 codes appear across
# 1999-2026, and exactly these three pairs have disjoint season ranges
# (OAK 1999-2019 / LV 2020-, SD 1999-2016 / LAC 2017-, STL 1999-2015 /
# LA 2016-). Houston is NOT here: HOU is an expansion team from 2002 with no
# predecessor code in this data, and the 1999-2001 Oilers/Titans are TEN
# throughout.
RELOCATIONS = {
    'OAK': 'LV',
    'SD': 'LAC',
    'STL': 'LA',
}

# Every code that may appear, including the three retired ones. A code outside
# this set is refused rather than stored: a new code is either an expansion
# team or a relocation, and both need a person to decide which.
TEAM_CODES = frozenset({
    'ARI', 'ATL', 'BAL', 'BUF', 'CAR', 'CHI', 'CIN', 'CLE', 'DAL', 'DEN',
    'DET', 'GB', 'HOU', 'IND', 'JAX', 'KC', 'LA', 'LAC', 'LV', 'MIA', 'MIN',
    'NE', 'NO', 'NYG', 'NYJ', 'OAK', 'PHI', 'PIT', 'SD', 'SEA', 'SF', 'STL',
    'TB', 'TEN', 'WAS',
})

FRANCHISE_CODES = frozenset(TEAM_CODES - set(RELOCATIONS))


def franchise_code(code):
    """The current code of the franchise a historical code belongs to.

    `franchise_code('OAK') == 'LV'`. An unknown code raises: guessing that it
    is its own franchise is how a relocation becomes two teams.
    """
    if code not in TEAM_CODES:
        raise ValueError(f'Unknown NFL team code {code!r}')
    return RELOCATIONS.get(code, code)


def entity_key(kind, provider_id):
    """`nfl:nflverse:team:KC`. Raises rather than coercing a doubtful id."""
    if kind not in KINDS:
        raise ValueError(f'Unknown entity kind {kind!r}')
    if not isinstance(provider_id, str) or not provider_id.strip():
        raise ValueError('Provider ids must be non-empty strings')
    if kind in ('team', 'franchise') and provider_id not in TEAM_CODES:
        raise ValueError(f'Unknown NFL team code {provider_id!r}')
    if kind == 'franchise' and provider_id in RELOCATIONS:
        raise ValueError(
            f'{provider_id!r} is a retired code; its franchise is '
            f'{RELOCATIONS[provider_id]!r}')
    return f'{SPORT}:{PROVIDER}:{kind}:{provider_id}'


def coach_key(name):
    """`nfl:nflverse:coach:Andy Reid`, with whitespace collapsed.

    The archive writes a coach's name with the odd double space, and 'Andy
    Reid' and 'Andy  Reid' must not become two coaches with half a career
    each. An empty name raises rather than becoming a key nobody can read.
    """
    cleaned = ' '.join((name or '').split())
    if not cleaned:
        raise ValueError('A coach name cannot be empty')
    return entity_key('coach', cleaned), cleaned


def franchise_key(code):
    """The franchise key a team code belongs to, relocations resolved."""
    return entity_key('franchise', franchise_code(code))


FIRST_SEASON = 1999

# An NFL season is named for the calendar year it STARTS, and its playoffs run
# into January and February of the next one. So the turnover is not New Year:
# a game played in January 2027 belongs to season 2026. March is the safe
# line — the league year opens then, nothing is played across it, and a run in
# September and a run the following January agree about which season is
# current.
SEASON_TURNS_OVER_IN = 3


def current_season(now):
    """The season in progress at `now`, named for the year it started."""
    return now.year if now.month >= SEASON_TURNS_OVER_IN else now.year - 1


def seasons_through(now, first=FIRST_SEASON):
    """Every season from `first` to the season in progress."""
    return list(range(first, current_season(now) + 1))


def season_label(season):
    """The NFL names a season after one year, unlike college basketball."""
    if type(season) is not int or isinstance(season, bool) \
            or not 1900 <= season <= 2200:
        raise ValueError('Expected an NFL season year')
    return str(season)


# The current eight-division alignment, which has held since the 2002
# realignment that added Houston and moved Seattle to the NFC. It is written
# down here rather than fetched because the archive's team file does not carry
# it and a division is not a thing to guess from a schedule.
#
# IT IS VALID FROM 2002 ONLY. The 1999-2001 league had six divisions and
# Seattle in the AFC West; a season before 2002 therefore gets NO alignment
# rather than this one applied backwards, which would quietly rewrite three
# seasons of standings.
ALIGNMENT_FROM = 2002
CONFERENCES = ('AFC', 'NFC')
DIVISIONS = ('East', 'North', 'South', 'West')
ALIGNMENT = {
    'BUF': ('AFC', 'East'), 'MIA': ('AFC', 'East'), 'NE': ('AFC', 'East'),
    'NYJ': ('AFC', 'East'),
    'BAL': ('AFC', 'North'), 'CIN': ('AFC', 'North'), 'CLE': ('AFC', 'North'),
    'PIT': ('AFC', 'North'),
    'HOU': ('AFC', 'South'), 'IND': ('AFC', 'South'), 'JAX': ('AFC', 'South'),
    'TEN': ('AFC', 'South'),
    'DEN': ('AFC', 'West'), 'KC': ('AFC', 'West'), 'LAC': ('AFC', 'West'),
    'LV': ('AFC', 'West'),
    'DAL': ('NFC', 'East'), 'NYG': ('NFC', 'East'), 'PHI': ('NFC', 'East'),
    'WAS': ('NFC', 'East'),
    'CHI': ('NFC', 'North'), 'DET': ('NFC', 'North'), 'GB': ('NFC', 'North'),
    'MIN': ('NFC', 'North'),
    'ATL': ('NFC', 'South'), 'CAR': ('NFC', 'South'), 'NO': ('NFC', 'South'),
    'TB': ('NFC', 'South'),
    'ARI': ('NFC', 'West'), 'LA': ('NFC', 'West'), 'SEA': ('NFC', 'West'),
    'SF': ('NFC', 'West'),
}


def alignment(code, season):
    """(conference, division) for a franchise in a season, or (None, None)."""
    if season < ALIGNMENT_FROM:
        return (None, None)
    return ALIGNMENT.get(franchise_code(code), (None, None))
