"""Namespaced NBA identities, and the two traps this archive sets.

A bare provider id is never a key. ESPN numbers its teams and games, so a
franchise is `nba:espn:team:25` and a game `nba:espn:game:401585612`.

**TRAP ONE: THE NAME IS NOT THE TEAM.** Two different franchises in this
archive are both spelled `CHA` / "Charlotte Hornets":

- team 3 is the Hornets franchise — Charlotte in 2002, New Orleans from 2003
  (playing in Oklahoma City in 2006 and 2007), the Pelicans from 2014.
- team 30 is the 2005 expansion team — the Bobcats through 2014, and renamed
  the Charlotte Hornets in 2015 when the Pelicans gave the name up.

Join those on the name and the Pelicans' first eleven seasons are filed under
the Hornets of today. Nothing in the data would look wrong afterwards. The id
is the only safe key, which is why this module refuses to resolve a team by
name at all.

**TRAP TWO: AN ALL-STAR GAME IS A REGULAR-SEASON GAME.** The All-Star game
and its modern variants (Team LeBron, Team Giannis, USA v World, Stars v
Stripes) carry `season_type = 2`, the same value as a game in Cleveland in
January. Filtering the exhibitions by season type does not work. What
separates them is that their sides are not franchises: ids 31, 32 and a dozen
six-figure ids. Measured across 2002-2027 — 39 such games, and no real
franchise ever plays one.

There is no relocation table here, unlike the NFL's. ESPN keeps a franchise's
id across a move: Seattle and Oklahoma City are both team 25, New Jersey and
Brooklyn both team 17. The per-season name is kept in `team_season` so that a
2007 box score still says SuperSonics.
"""

SPORT = 'nba'
PROVIDER = 'espn'
KINDS = frozenset({'team', 'game', 'player', 'coach'})

# The thirty franchises, by ESPN team id. Measured from every schedule file
# 2002-2027 on 2026-11-03: ids 1-29 appear in every season and id 30 (the
# Charlotte expansion team) from 2005, and no other id belongs to a
# franchise.
FRANCHISE_IDS = frozenset(range(1, 31))

# Ids that appear in the schedule and are NOT franchises. Listed rather than
# derived so that a new one shows up as a refusal instead of being skipped
# silently: a franchise this module has never seen is a thing a person should
# look at, not a row to drop.
#
# Measured across 2002-2027: the two conference All-Star sides (31, 32), the
# captain-drafted and exhibition sides that replaced them from 2018, and the
# -1 / -2 placeholders the source uses for a scheduled playoff game whose
# teams are not known yet.
EXHIBITION_IDS = frozenset({
    -2, -1,                      # TBD placeholders in an unfinished bracket
    31, 32,                      # East / West All-Stars
    111353, 111354,              # Team LeBron, Team Stephen
    111386, 111387,              # World, USA
    112151, 123554,              # Team Giannis, Team Durant
    130579, 130580, 130581, 130754,   # Chuck, Shaq, Kenny, Candace
    132374, 132375,              # Team Stars, Team Stripes
})

FIRST_SEASON = 2002

# An NBA season is named here for the year it ENDS, which is what the source
# does: `2027` is 2026-27. September is the safe turnover — the Finals are
# over and no game of the new season has been played — so a run in October
# and a run the following May agree about which season is current.
SEASON_TURNS_OVER_IN = 9

# The source's own `season_type`, measured across the whole archive: 31,365
# rows of 2, 2,094 of 3 and 37 of 5. There is no preseason in this data.
SEASON_TYPES = {
    2: 'regular',
    3: 'postseason',
    5: 'playin',
}
# The play-in is not the postseason — a team can lose one and have no
# playoff at all — but both are played after the regular season, and a page
# that lists "the regular season" should list neither.
AFTER_REGULAR_SEASON = frozenset({'postseason', 'playin'})

# THE NBA CUP FINAL DOES NOT COUNT IN THE STANDINGS, AND THE SOURCE SAYS IT
# IS A REGULAR-SEASON GAME.
#
# Every other game of the in-season tournament counts: the group games and
# the knockout rounds are scheduled regular-season games. The championship
# alone is not, which is why the two finalists end a season having played 83
# games while everybody else played 82. Measured across the archive: exactly
# two teams exceed 82 in each of 2024, 2025 and 2026, and none in any other
# season.
#
# Left as the source has it, a standings page would show the two finalists
# with one more win or loss than the NBA credits them, and the page would
# simply disagree with every other source about their record.
#
# It is rateable all the same. It is a real game between two teams trying to
# win, and the NBA excludes it from the standings, not from history.
#
# TWO SPELLINGS, because the event was renamed. 2023-24 says "NBA In-Season
# Tournament Championship" and the two seasons after it say "NBA Cup
# Championship" — a search for "Cup" alone finds only two of the three. The
# string is the source's and could change again, so
# `tests/test_nba_rating.py` asserts the invariant behind it: in a season
# with a final, every team played exactly 82 regular-season games.
CUP_FINAL_NOTES = frozenset({
    'nba cup championship',
    'nba in-season tournament championship',
})


def season_type_of(value, notes=None):
    """The season type of a game, reading its note as well as its type.

    The note is what separates the one tournament game that does not count
    from the hundreds that do.
    """
    kind = season_type(value)
    if kind == 'regular' and notes \
            and ' '.join(str(notes).split()).lower() in CUP_FINAL_NOTES:
        return 'cup_final'
    return kind


def is_franchise(team_id):
    """Whether this id is one of the thirty, rather than an All-Star side."""
    return _as_id(team_id) in FRANCHISE_IDS


def is_known(team_id):
    """Whether this id has been seen at all, as a franchise or otherwise."""
    value = _as_id(team_id)
    return value in FRANCHISE_IDS or value in EXHIBITION_IDS


def _as_id(team_id):
    if isinstance(team_id, bool):
        raise ValueError('A team id cannot be a boolean')
    if isinstance(team_id, int):
        return team_id
    text = str(team_id).strip()
    if not text:
        raise ValueError('A team id cannot be empty')
    try:
        return int(text)
    except ValueError as error:
        raise ValueError(f'Unreadable team id {team_id!r}') from error


def entity_key(kind, provider_id):
    """`nba:espn:team:25`. Raises rather than coercing a doubtful id."""
    if kind not in KINDS:
        raise ValueError(f'Unknown entity kind {kind!r}')
    text = str(provider_id).strip() if provider_id is not None else ''
    if not text:
        raise ValueError('Provider ids must be non-empty')
    return f'{SPORT}:{PROVIDER}:{kind}:{text}'


def team_key(team_id):
    """The franchise key for an ESPN team id.

    An All-Star side raises rather than becoming a team: it has no history,
    no next season and no rating, and storing one would put "Team Giannis"
    in the standings.
    """
    value = _as_id(team_id)
    if value not in FRANCHISE_IDS:
        if value in EXHIBITION_IDS:
            raise ValueError(f'Team id {value} is an exhibition side, not a '
                             'franchise')
        raise ValueError(f'Unknown NBA team id {value}')
    return entity_key('team', value)


def game_key(game_id):
    return entity_key('game', game_id)


def season_type(value):
    """The source's integer season type as a name. Unknown values raise."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f'Unreadable season type {value!r}')
    if value not in SEASON_TYPES:
        raise ValueError(f'Unknown NBA season type {value}')
    return SEASON_TYPES[value]


def season_label(season):
    """`2027` is shown as `2026-27`, the way a season that straddles two
    calendar years is written."""
    if type(season) is not int or isinstance(season, bool) \
            or not 1947 <= season <= 2200:
        raise ValueError('Expected an NBA season year')
    return f'{season - 1}-{season % 100:02d}'


def current_season(now):
    """The season in progress at `now`, named for the year it ends."""
    return now.year + 1 if now.month >= SEASON_TURNS_OVER_IN else now.year


def seasons_through(now, first=FIRST_SEASON):
    """Every season from `first` to the season in progress."""
    return list(range(first, current_season(now) + 1))
