# NBA lineups: rebuilding 2017-18 stints

Milestone 2 of the NBA team-statistics and Player Elo handoff, 2026-11-12.
Milestone 1 shipped as PR #204 ([[nba-team-statistics]]); this is the lineup
audit it asked for next, and the input milestone 3's possession-weighted
Player Elo needs.

Nothing here is published. It writes no database, feeds no rating, changes
no page, and runs in no workflow. Every identifier in it is an **ESPN** id.

## What it answers

Can a stint file — five players a side, with the points and possessions that
belong to them — be rebuilt for a full season from the data this repository
can already reach?

Yes, for 92.9% of 2017-18's stints, and the other 7.1% say which of four
things is wrong with them rather than being dropped or quietly kept.

## The source, and why it is this one

The handoff's own stint archive is keyed by **NBA.com game ids**
(`0021700010`). That provider is unreachable from this container and from
Actions — `cdn.nba.com` answers 403 and `stats.nba.com` times out, three
times out of three ([[nba-data-source]]) — so none of it can be loaded or
checked here.

What can be reached is the ESPN play-by-play already declared in
`src/nba/source.py`. The handoff's game is **ESPN 400974766** (2017-10-18,
Utah 106 Denver 96, Utah at home), the same game its NBA.com id names.

## How to run it

```sh
python analysis/nba_lineups/collect.py --season 2018 --game 400974766
python analysis/nba_lineups/collect.py --season 2018 \
    --out analysis/nba_lineups/snapshots/2018
python -m pytest tests/test_nba_lineups.py -q
```

The first prints the audit report and every stint of that game. The second
rebuilds the season — about three minutes — and writes `stints_2018.csv`,
`reconciliation_2018.json` and a `manifest.json` that hashes the table.
Snapshots are gitignored. Four assets are fetched (play-by-play, player box,
team box, schedule) and cached under `data/nba/raw/`; `--reuse-cache` is on.

`tests/test_nba_lineups.py` is 60 tests on hand-written rows in the feed's
shape. Nothing in it touches the network or the archive.

## The audit: ESPN 400974766

Every check the handoff asked for passes.

| | |
| --- | --- |
| stints | 30, all `verified` |
| five a side | every stint, both sides |
| periods | 1-4 in order, no overtime |
| points rebuilt | **Utah 106, Denver 96** — the box score's own final |
| minutes | agree with the box for all 26 players, inside its own rounding |
| possessions counted | Utah 94, Denver 94 |
| possessions estimated from the box | Utah 97.0, Denver 95.3 |

The game also contains one of the feed's one-sided substitution rows:
`enters the game for Mason Plumlee`, naming nobody as the entrant. Kenneth
Faried came in, and the only record of it is that he then played. Reading
`athlete_id_1` as the entrant would have put **Plumlee** back on the floor
he had just left and left Faried's six minutes nowhere.

## The season

1,314 completed, non-exhibition games were expected. 1,306 were rebuilt into
**39,541 stints**, 30.3 a game.

| | |
| --- | --- |
| regular-season games | 1,224 of 1,230, holding 36,949 stints |
| postseason games | 82 |
| `verified` | 36,749 (92.9%) |
| `minutes_unreconciled` | 2,175 (5.5%), from 237 player-games in 134 games |
| `unresolved_lineup` | 488 (1.2%) |
| `scored_without_possession` | 129 (0.33%) |
| games whose minutes agree exactly | 1,172 of 1,306 |
| possessions counted less box estimate | -2.51 a team-game, 2.40 median absolute |

**THE JAZZ COUNT CORROBORATES THE HANDOFF FROM A DIFFERENT PROVIDER.** The
handoff's NBA.com sample found **2,655 stints in 82 Jazz games**. This
rebuild finds **2,657 in the same 82**, with 2,497 verified. Two independent
feeds agreeing to two stints across a season is the strongest evidence here
that the reconstruction is sound.

## The four quality statuses

A row is never silently repaired and never silently dropped.

- **`verified`** — five a side, every substitution of the game accounted
  for, the players' minutes agree with the box score, and any points in the
  stint have a possession to divide them by.
- **`minutes_unreconciled`** — the stint holds a player whose rebuilt
  minutes miss the box score by more than half a minute. Half a minute is
  the box's own rounding, so this is a substitution the feed never wrote:
  the stint reads as five against five and is still wrong. 237 player-games.
- **`unresolved_lineup`** — a one-sided substitution row that nothing in the
  game identified, so one seat is unknown from that moment on.
- **`scored_without_possession`** — a side scored in the stint with no
  possession of its own in it. 129 stints, from three causes read by hand:
  points that appear on a row carrying no scoring play at all (the feed's
  own gap, as in 400974772), a technical free throw, which scores without
  anybody having the ball, and two baskets by one team with no opposing
  event logged in between.

The handoff asked that the Jazz stints with points and no recorded
possession be kept in the reconciliation, investigated, and attributed only
if the possession and lineup can be verified. **They are kept**: six Jazz
stints carry this status. Most of the ones in the first pass turned out to
be an attribution artefact rather than missing data and were fixed, not
flagged — see below.

## What a reader of the file has to know

1. **A stint boundary is a clock AND a play number.** A dead ball at 6:00
   holds the foul, the changes and the free throw all stamped 6:00, so a
   boundary that knows only the clock cannot say which side of the change
   a play falls on; matching on the clock alone credited that free throw
   to the five that had just left the floor. A change therefore takes
   effect at the play number of its own group, and the plays of one dead
   ball are split by it exactly where the feed puts them. 227 rows
   consequently span no clock at all: a floor that stood for one free
   throw between two changes. An empty one is dropped; one holding a
   free throw is the only row that free throw belongs to.
2. **A boundary never falls inside a free-throw trip**, because one
   possession cannot have two denominators. A change between the first and
   second free throw is real — the entering players stand on the floor for
   the second — but splitting there leaves one of the trip's points in a
   stint with no possession to divide it by, which over 2017-18 is 1,264
   stints. The and-one is the same shape: the basket, the foul, the
   change, then the free throw. The trip stays with the floor whose shot
   opened the possession and the change takes effect when the trip ends.
3. **A possession is credited where its basket was scored**, and, scoring
   none, where it ended. A substitution inside a possession splits it
   between two fives and no single choice is right; what a rate fit cannot
   survive is the points landing in one stint and the possession in
   another. Crediting the first play of the possession left 325 such
   stints, crediting the last left 488, and crediting the basket leaves
   129.
4. **Possessions are counted, not estimated.** A possession is a run of the
   offensive team, so an and-one is one possession, and no 0.44 appears
   anywhere. The count runs **2.51 below** the box-score estimate a
   team-game, 2.6% of a game's possessions, and the file states both so the
   fit can choose. For 2017-18 the estimate uses the narrower player-sum
   `turnovers`, because the season's team box carries no `total_turnovers`
   at all ([[nba-team-statistics]]), which leaves it low by the team
   turnovers — about 0.6 a team-game.
5. **A stated zero is a zero.** Three Spurs entered 400974440 with ten
   seconds left and the box rounds each to 0.0 minutes. Reading that as
   "did not appear" made the rebuild look wrong in games where it was right.
6. **Exhibitions hide in `season_type` 2.** 2017-18's All-Star Game
   (401001760) and Rising Stars game (401018033) both carry season type 2
   and both have full play-by-play, so the first run produced 56 stints of
   them. They are filtered by the TEAMS playing, through
   `identity.EXHIBITION_IDS`, which is the rule the rest of the NBA layer
   already follows.
7. **Four completed regular-season games have no play-by-play at all**:
   400975670 and 400975617 (both Sacramento), 400974444 (New Orleans) and
   400974773 (Washington). The game list is taken from the SCHEDULE rather
   than from the plays, because a game with no play-by-play is exactly the
   one a play index cannot show you; the four are named in the manifest's
   `games_without_play_by_play_ids`. Two more have play-by-play that cannot
   be used: 400975285 has no player box score, and 400975023's box marks
   **eight** Minnesota players as starters.

## What milestone 3 needs before it starts

- **The crosswalk.** Every id here is an ESPN id, which is what
  `src/nba/people.py` keys people on, but the mapping has to be verified
  before any of this reaches a player page rather than assumed from the
  shape of the numbers.
- **A decision about the flagged stints.** 7.1% of the stints are not
  `verified`. A ridge fit can drop them, or weight them by the possessions
  it does trust; this file does not decide, and the status column is there
  so the fit can state which it did.
- **The possession gap.** 2.51 possessions a team-game between a count and
  an estimate is small, but a rate per 100 possessions divides by it. The
  fit should use the counted possessions it is weighting by, not a rescaled
  box estimate, and say so.

See [[nba-team-metrics-handoff]], [[nba-team-statistics]], [[nba-game-layer]],
[[nba-people-layer]]. `analysis/cbb_player_impact/` planned the same
reconstruction for the college feed and is blocked on quota
([[cbb-launch-blocked-quota]]).
