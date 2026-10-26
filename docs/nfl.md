# The NFL in Elocast

A third league, built to the pattern the basketball track established
(`docs/basketball-integration.md`): its own database, its own identity
namespace, an Elo fitted and held out on its own games, an export with two
independent gates, its own refresh workflow with its own health light, and a
page of its own. No NFL row is joinable to a college football or basketball
row, and no league's failure can stop another's refresh.

## The source, and what is known about using it

`https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv` —
every NFL game from 1999 to the one being played this week, with final
scores, dates, kickoff times, venue neutrality, overtime and the stated
margin. 2.2 MB, one file, **no key and no request allowance**.

That last point is why this source was chosen over a metered API. The
basketball track lost a week to a rolling seven-day cap that a fresh key did
not reset (`docs/basketball-refresh.md`); there is nothing here to exhaust, so
the refresh can simply re-read the file every morning and the ingest can run
on any machine, including a laptop.

Team names come from `teams.csv` in the same repository, which starts in 2002.
The archive starts in 1999, so three seasons' franchises are code stubs until
a team-file row names them.

**Licensing, stated plainly because it is not resolved by a LICENSE file.**
Neither `nfldata` nor `nflverse-data` carries a data licence; the nflverse
*code* packages are MIT. What this repository publishes is derived ratings and
factual game results, not a copy of their files, and the site credits nflverse
as the source. That is the honest position rather than a permission anyone
granted, and it is Austin's call whether to seek something firmer before this
goes out widely. Nothing here redistributes the archive itself: the raw file is
gitignored and refetched.

## A franchise is not a team code

The source renames a team when it moves: `OAK` through 2019 and `LV` from 2020,
`SD` through 2016 and `LAC` from 2017, `STL` through 2015 and `LA` from 2016.
Three relocations, not six teams.

`src/nfl/identity.py` resolves a code to a franchise and every rating follows
the franchise. Without that the Raiders, Chargers and Rams would each restart
at 1500 on the day they moved — wrong, and invisible, because the ratings would
still look like ratings. The code the source used is kept on each game, so a
2015 box score still reads `STL`.

Houston is deliberately **not** in that table: `HOU` is an expansion team from
2002 with no predecessor code in this data, and the 1999-2001 Oilers are `TEN`
throughout. The eight-division alignment is committed in the same module and
applies **from 2002 only**; before the realignment the league had six divisions
and Seattle in the AFC, so an earlier season gets no alignment rather than this
one applied backwards.

## A tie is a result

The NFL has tied 16 times since 1999. The basketball replay declines a level
final, because a tied college basketball game means the data is wrong; here it
means the teams tied, and it is the most informative kind of game there is.
`src/nfl/elo.py` scores a tie as 0.5 and updates both ratings, and the Brier,
log loss, accuracy and calibration all take a half outcome. A tie takes no
margin multiplier, because there is no winning margin to compare with an
average one.

An unplayed game is `None`, never 0.5. The two must never look alike, and a
test pins that.

## The model

Fitted by `src/nfl/fit_elo.py` on **1999-2020** (5,852 rateable games) and
scored once on **2021-2025** (1,424 games), which the search never saw. The
2026 season is in progress and is neither fitted nor scored; completeness is
read from the data — a finished season has a Super Bowl in it — rather than
from the calendar.

The holdout is five seasons rather than one because an NFL season is 285 games
against college basketball's 5,752, and the differences between candidate
models here are smaller than a single season's error bar.

| parameter | value |
| --- | --- |
| k | 48 |
| home_advantage | 50 |
| offseason_retention | 0.5 |
| margin_model | mov |
| margin_exponent | 0.75 |
| reference_margin | 11.6725 (measured) |
| points_per_elo | 0.0527 (measured) |

Held out on 2021-2025:

| model | Brier | log loss | accuracy | calib err |
| --- | --- | --- | --- | --- |
| this fit | 0.22372 | 0.63998 | 0.6327 | 0.04191 |
| college football's parameters, unchanged | 0.22582 | 0.64566 | 0.6320 | 0.04589 |
| college basketball's parameters, unchanged | 0.23520 | 0.67178 | 0.6236 | 0.10173 |
| home team at its base rate 0.5463 | 0.24715 | 0.68884 | 0.5463 | 0.00000 |
| coin flip | 0.24930 | 0.69315 | 0.5000 | 0.04635 |

Two things worth saying about that table rather than leaving them to be
noticed. **The margin over college football's parameters is small** — 0.002 of
Brier. The NFL is a tightly matched league and 0.2237 against a 0.2472
base rate is what a good rating looks like here, not an underperformance.
And **home advantage came out at 50**, the same number college football ships,
which is a coincidence of two different fits, not an inheritance: home teams
won only 54.6% of the held-out games, well down on the league's historical
figure.

Both `k` and `offseason_retention` were chosen at the edge of the first grid.
A parameter chosen at a grid's boundary is not a fitted parameter, so the grid
was widened until they sat inside it; the comment in `fit_elo.py` says to do
the same again if it ever recurs.

The committed claim lives in `config/nfl_model.json` beside the parameters and
is **pinned to them by hash**. A refresh fits nothing — it replays the
committed parameters on a runner with no fit report — so editing `k` without
re-fitting fails the build rather than publishing a Brier the parameters never
earned. The basketball refresh shipped unable to publish anything for exactly
the want of this.

To re-measure: `python -m src.nfl.ingest && python -m src.nfl.fit_elo --out
elo_fit.json`, which needs no credentials.

## The export, and the two gates

`python -m src.nfl.export` writes `ui/data/nfl/`: a manifest, `teams.js`, and
one file a season. 28 seasons come to 2.2 MB, and a daily refresh changes only
the season in progress, because each season is its own file.

Two gates refuse separately, and neither can catch the other's failure:

- `src/nfl/publish_gate.py` runs at build time, before the first byte. The
  export is built whole in memory first, so a refusal leaves the output
  directory untouched — a half-written directory is itself a way to publish a
  sample.
- `ui/nfl_loader.js` runs in the reader's browser against whatever the network
  returned: a stale deploy, a cached manifest from before a schema change, a
  half-served directory, a hand-edited file.

The NFL lets both be stricter than their college equivalents, because the
league is a **fixed size**. The team check is an equality — 32 franchises from
2002, 31 before — so an export missing one franchise fails instead of clearing
a floor comfortably. A finished season must rate every team; a season in
progress must rate every team that has finished a game.

The Brier floor is **0.245**, the home-base-rate baseline, not 0.25. A coin
flip is a weak thing to beat in a league where the home team wins 55% of the
time. A test asserts the Python and JavaScript floors are the same number,
because two gates that disagree are one gate.

Staleness is said, never enforced: an export hours old is the last good
snapshot and stays publishable, badged with its age.

## The refresh

`.github/workflows/nfl-refresh.yml`, with its own concurrency group
(`nfl-refresh`, never the deploy's) and its own health light scope. It runs
the NFL tests before it touches anything published, ingests, replays, measures the
coaches and the market, builds the export into a temporary directory, and swaps it in only if
`src/nfl/publish.py` agrees. That module refuses an export *older* than the
live one — valid, and a step backwards — and stages-and-renames rather than
emptying and refilling, so a publish that dies halfway leaves the previous
export whole.

It commits `ui/data/nfl` and nothing else. Unlike the basketball refresh it
does **not** commit the raw archive: with no allowance to husband, refetching
is free and a 600 KB blob committed every morning is not.

Note the repository's standing gotcha: a new `workflow_dispatch` file is not
dispatchable until it is on the default branch, so the first export has to be
produced by dispatching this workflow after the merge.

## Phase 1 of parity: the context the archive already carried

`games.csv` — the file the refresh has always fetched — carries far more than
schema 1 of the loader stored. Everything in this section cost **no new
ingestion at all**: the coaches, the quarterbacks, the betting lines, the
referee, the rest days, the roof, the temperature and the wind were already
being downloaded every morning and thrown away on the floor of `parse_row`.

Schema 2 of the database keeps them. `store.GAME_COLUMNS` is the one list the
INSERT, the conflict clause and the value tuple are all generated from, and a
test asserts it matches `schema.sql` exactly and in order; three places used
to list these and two of them drifted. `migrate()` ALTERs an existing table,
because `CREATE TABLE IF NOT EXISTS` does nothing to a table that already
exists and the symptom of that is an error on a column the schema plainly
declares.

**Coaches** (`src/nfl/coaches.py`). One row per coach per season per team,
which is the shape that holds a midseason change and a coach who moves
without averaging either away. Wins above expectation is actual wins minus
the sum of the model's **pregame** probability that the team won each game, a
tie counting half on both sides. It says the model was wrong in that team's
favour; it is not a measure of coaching, and the page prints that sentence.
The college side cannot do this cleanly — CFBD never says whose games were
whose, so 361 of its 5,657 tenures are unmeasured — while every one of the
7,548 games here names both coaches.

Measured on the committed archive: 936 coach-season-team rows, 177 coaches.
Belichick +21.3, Reid +16.9, Payton +14.8.

**The model against the market** (`src/nfl/market.py`). The market's
probability is a **de-vigged moneyline, never a spread**. A spread becomes a
probability only by way of a model — ours, most likely — so a "market Brier"
built that way would measure our curve against itself. The cost is coverage:
moneylines are on 72% of the archive and 90%+ from 2007, so both Brier scores
are accumulated on the same priced subset and `model_brier_priced` exists for
exactly that reason.

Measured 1999–2026: on the 5,388 priced games the model scores 0.21893 and
the market 0.21101, so **the market is ahead**. Margin MAE 10.61 against the
spread's 10.26. Against the spread the model is 3655-3513-201, which is
51.0% — below the ~52.4% a bettor needs to clear a standard vig, and the page
says so in those words rather than letting a 51% record read as an edge.

**The real playoff field** (`src/nfl/playoff.py`). Not one format: seven per
conference with one bye since 2020, six with two byes from 2002 to 2019, and
**no field at all before 2002**, because the committed alignment begins at
the realignment and a three-division conference is not a thing to guess.

Win percentage separates the groups, and **each group of clubs on the same
record is broken on its own**, by the league's own first terms: head to head
among the tied clubs only (a sweep or a reverse sweep — a 1-1 split is
explicitly not a tiebreak), then division record **only where the tied clubs
share a division**, then conference record. Past those it falls back on our
rating, labelled as ours because it is not a league tiebreaker. Common games,
strength of victory and the rest of the ladder are not applied, and neither
is the league's re-start of the whole procedure each time a club is
eliminated from a group of three or more.

Both of those scoping rules came out of review on this PR, and the first
version had both wrong: head to head was computed over every candidate, so
games against clubs on other records could decide a tie between two clubs
that never played them, and division record was compared across divisions,
which sets two records against two different sets of opponents.

Verified against the real fields: 2024 (KC, BUF, BAL, HOU, LAC, PIT, DEN /
DET, PHI, TB, LA, MIN, WAS, GB), 2023, 2019 and 2015 all come out exactly
right.

**Conference history and the track record.** Both from data already stored:
conference and division mean ratings per season from `team_rating` and the
stored alignment, champions from the games the archive calls a Super Bowl or
a conference championship, and the per-season model-versus-market table from
`market_season`. They live in `ui/data/nfl/history.js`, which the page
fetches **lazily** on the first tab that needs it — 348 KB raw, 36 KB over
the wire, and a reader who came for the scoreboard never downloads it.

**On a game card**: the weather, the rest and the posted line. Absent is
absent throughout — a dome game has no temperature rather than a zero, which
is the `Number(null)` trap in another language.

The export is **schema 2**, and both gates accept 1 and 2. Refusing 1 would
fail the publish check against the version 1 export already on main and leave
the site unable to update at all.

## Phase 2 of parity: the people layer

Players, rosters, season statistics and the draft, 1999-2026, from four
nflverse release assets: `players/players.csv`, `rosters/roster_<season>.csv`,
`stats_player/stats_player_<reg|post>_<season>.csv` and
`draft_picks/draft_picks.csv`. No key, no cap, about 54 MB a run, fetched by
`src/nfl/people_source.py` and loaded by `src/nfl/people.py`. The raw files
are gitignored like the game archive.

**The release tag matters.** An older `player_stats` release still serves
`stats_player_reg_2024.csv` and 404s from 2025 on, which reads as "not
published yet". The live tag is `stats_player` (the one `nflreadr` builds its
URLs from), and under it every season 1999-2026 is present. Its header is 148
columns and wider than the old one, which is why statistics are stored long.

**Identity.** A player is `nfl:nflverse:player:<gsis_id>`. Measured on
2026-10-21:

| | |
| --- | --- |
| players stored (career reaching 1999) | 18,109 |
| of which stubs (an id the player file lacks) | 1,669 |
| players with a roster or statistics row, so a page | 16,608 |
| roster rows | 69,477 |
| statistics rows (player, season, half) | 63,188 |
| non-zero statistics stored | 711,914 |
| draft picks 1999-2026 | 7,154, 6,840 linked to a player |

1,789 roster rows and 9 statistics rows name a GSIS id the player file does
not carry. They become **stubs** built from the row's own name, position and
measurements and flagged `stub = 1`; the page says the biography came from a
roster sheet. A stub fills gaps in a real biography and never overwrites one,
whichever arrives first. 25 roster rows and 51 statistics rows carry no id at
all and are counted and dropped.

1,309 players have two roster rows in one season because they were traded;
both are kept. 31 have two rows for the same team, and the later one wins.
From 2016 the roster files include practice squads and reserve lists, which
is why a season has about 3,000 roster rows from then on against 2,000
before. The roster file is an end-of-season snapshot, not a week-by-week
history.

**Five teams are spelled the GSIS way in the roster files** (ARZ, BLT, CLV,
HST, SL) **and nine the Pro-Football-Reference way in the draft file** (GNB,
KAN, LAR, LVR, NOR, NWE, SDG, SFO, TAM). Each GSIS spelling appears in
2002-2015 only and never in the same season as the code it stands for, which
is the evidence that it is a spelling and not a second team.
`identity.resolve_code` holds the table. The game loader does not use it and
stays strict.

**Statistics** are stored long, one row per stat, with zeros not stored. A
regular season and a postseason are never summed together. A career total
adds counting stats, takes the best `fg_long` and `pt_long`, and leaves
averages and shares out (`passing_cpoe`, `target_share` and the like): a
career target share is not a thing. Every non-zero value is stored, and an
allowlist in `people_export.STAT_TABLES` decides what reaches the browser, in
the page's order rather than alphabetical.

**The export** adds `people/index.js` (16,608 players, 252 KB gzipped, loaded
on the first search), 64 career shards in `people/careers/` (about 35 KB
gzipped each, one loaded per player opened), `people/current.js` and
`people/draft.js`. Settled seasons go in the shards and the season in progress
goes in `current.js`, so a refresh during the season rewrites one file rather
than all 64. Each season payload also carries `leaders`, the regular season's
top ten in eleven categories, with ties at the cut all kept. Export schema 3;
both gates still accept 1 and 2, and the build gate refuses a people layer
that is missing a shard or its index. A publish that would REMOVE a layer the
live site has is refused.

**The page.** A Players tab: the season's leaders, the header's search box
(which now searches players and takes the reader to the tab), a player page
with a table per kind of statistic for the regular season and the postseason,
a career row, and the seasons he was on a roster. The Draft view shows a
season's board, linking each pick that has a page. Linemen have roster rows
and no statistics: the archive counts what a player did with the ball.

## Phase 3 of parity: play-by-play, and what it buys

The expensive phase, and the one with the most thrown away. nflverse
publishes a play-by-play file per season: 372 columns, about 46,000 plays, 65
to 99 MB of CSV, **2.4 GB across 1999-2026**. Not one play of it is stored or
committed.

What is kept is what a game page draws:

| | |
| --- | --- |
| drives | 170,528 |
| scoring plays | 94,574 |
| games covered | 7,366 of the 7,369 the archive has finals for |
| seasons | 1999-2026 |

Three games — two in 1999, one in 2000 — have a final score and no plays at
all. They are the source's gaps, not dropped rows: a drive whose game the
archive does not carry is counted and reported rather than discarded quietly.

**Only a confirmed 404 counts as "not published yet."** The season in
progress may have no file in September, and tolerating that is how the
refresh keeps working; tolerating a timeout or a 503 the same way would let
a run succeed while the published export quietly lost the current season's
game detail. `NflNotPublished` is a separate exception for exactly that
difference.

`src/nfl/pbp_source.py` streams one season to a temporary file, and
`src/nfl/plays.py` reduces it and deletes it before the next one is fetched,
so the peak disk cost is one season rather than 2.4 GB. A download that is
cut short leaves no file: a truncated CSV is the one thing the reader
refuses, and a retry that found one would read a season short.

### The scoreboard decides who scored

The obvious reading of a scoring play is its `posteam`, and it is wrong 438
times. Reducing the whole archive that way left **463 of 7,366 games whose
scoring plays did not add up to the final**, and 438 of those were out by
exactly two points in both directions: every safety in the archive, credited
to the team that conceded it.

`defteam` fixes most of them and breaks eleven others. A safety conceded
*after possession changed on the play* — a punt returner downed in his own
end zone, a tackle at the end of an interception return — is scored by the
team that was on offence, and nothing in the row separates that case from an
ordinary sack in the end zone.

So the scoreboard itself is the authority. A row carries the score after the
play, so the side whose total rose by exactly this play's points is the side
that scored, and the team columns are only the fallback. That leaves **18
games of 7,366** that still do not reconcile:

- sixteen are **Jacksonville home games of 2001 and 2002**, where the
  source's own running score is wrong — it credits Mark Brunell's touchdown
  passes to Pittsburgh and adds extra points to the wrong column;
- two are isolated (1999 week 9 at Carolina, 2011 week 13 at New Orleans).

Those games are **marked, not corrected**. The export sets `ok: 0` and the
page says the play log does not add up to the final above it, rather than
drawing a scoreboard that contradicts the score beside it.

### The drive fields are not on the drive's first play

`drive_start_yard_line` and its fourteen companions are blank on the opening
row of a drive and populated a play or two later. A reduction that reads the
first row gets a drive with a result and no start — which is what the first
version of this module produced. Every field is taken as the first non-blank
value anywhere in the drive. The SCORE is the opposite case: it is read from
the first play with a possessing team, because the score before the drive is
exactly what the drive started from.

### A measured win-probability curve

The college side's live model (`src/win_probability.py`) is pure
mathematics — remaining margin as a random walk the pregame rating tilts,
with an exact-tie term — and the NFL's clock is the same shape, so it is
imported rather than copied. The parameters are the NFL's own and are fitted
in `src/nfl/fit_win_probability.py` on **real in-game states**: the score,
the clock and the pregame rating at the start of every drive since 1999.

That is the parameter the college side could not measure. Its config records
`time_exponent` as an assumption of 0.5, the random walk's value, because
that database holds final scores only. Here there are 166,897 drive states,
so it is searched.

| | |
| --- | --- |
| `points_per_elo` | 0.0527 — **not fitted here**, read from `config/nfl_model.json` |
| `sigma` | 13.60 points |
| `time_exponent` | **0.34**, searched on 136,234 states over 1999-2020 |
| `overtime_home_win` | 0.5507, counted over 355 games level after four quarters |
| `tie_half_width` | 1.19 points, solved against how often games end level |

Scored once on 30,663 states over 2021-2025, which the search never saw:

| | Brier |
| --- | --- |
| this curve | **0.15658** |
| the pregame number, held constant | 0.22508 |
| the same curve with the random walk's 0.5 | 0.15751 |

The gain over the pregame number is large and is the point of the layer: a
scoreboard showing a 21-point favourite's kickoff chance while it trails by
ten in the fourth quarter is showing the one number the moment is no longer
about. The gain over 0.5 is small, and saying so is more useful than
overselling it — what 0.34 buys is a spread that shrinks faster early and
slower late than a pure walk does.

`points_per_elo` is deliberately not refitted: it is the Elo model's own
answer to what a rating point is worth in points, and a second estimate here
would be a second answer to one question.

**What the curve does not know:** possession, field position, down, distance
and timeouts. A tie game with two minutes left reads the same whether the
home team has the ball on the opponent's 20 or has just punted. That is
stated on the page under the chart, not only here.

### The game layer in the export

Export schema **4**, and all of 1, 2, 3 and 4 are still accepted by both
gates — refusing an older one would fail the publish check against the export
already on main.

One file a week, `games/<season>/<week>.js`, fetched lazily when a reader
opens a game. A season in one file measured 0.75 MB (190 KB gzipped) to read
one game of it; a week is about sixteen games, 44 KB, **13 KB gzipped**, and
it is the unit a reader is already looking at. The postseason is one file
however many rounds it ran to.

The page **derives** that path from the game's own week, which the season
payload already carries, so no index is downloaded to find a game. The
manifest therefore states one cache token per season rather than one per
file: 497 hashes would make the manifest, which every reader fetches,
heavier than the week file only a reader opening a game fetches. The gate
recomputes the token from the files on disk, in playing order, and refuses an
export whose weeks are listed out of that order — a token nobody can
reproduce is not a cache stamp.

**An overtime drive carries no number.** The model's walk runs to the end of
regulation and hands a tie to one measured overtime share, so `seconds_left`
is 0 inside overtime and the model would answer 1 or 0 by the score alone —
the drive after the first overtime field goal drawn as a certainty while the
opponent still has a possession coming. Those drives are left unpriced, the
same rule the fit applies to the same states, and the page says so under the
chart.

The win probability is computed **in Python at export time**, one number per
drive start. Every NFL game in this archive is finished, so there is nothing
for a page to recompute, and one implementation of a fitted curve is better
than two. When the NFL gets a live feed, that is the number the feed
replaces, not a second model.

### The page

A game card in the Games tab is a button where there is something behind it —
a finished game, in an export that carries the layer — and opens in place,
like a coach. It shows:

- the **win-probability trace**, as straight segments between drive starts
  and a closing point at the result. Straight, not smoothed: the model says
  nothing about what happened between two drive starts. The home code, 50%
  and the away code label the scale, because a line with no scale is a shape
  rather than a measurement.
- the **scoring summary**: quarter, clock, team, the play's own words and the
  running score. The clock is stripped from the description, which already
  states it — and only a clock, after a first version ate the "(Shotgun) "
  off every description that carried no clock in front of it.
- the **drives**: start, plays, yards, time of possession, result, the score
  the drive began from and the home team's win probability. The column is
  named after the home team, because a bare "Win prob" beside a row naming
  Baltimore reads as Baltimore's chance and it is Kansas City's.

## Phase 4 of parity: who was on the field, and who was fit to be

Two more nflverse release files, both small, both free, both stored whole
rather than reduced: they are already one row per player per game or per
week, which is what the pages show.

| what | asset | seasons | rows |
| --- | --- | --- | ---: |
| snap counts | `snap_counts/snap_counts_<season>.csv` | 2013-2026 | 333,375 |
| the injury report | `injuries/injuries_<season>.csv` | 2009-2026 | 92,890 |

### The snap file has no GSIS id

Its person id is Pro-Football-Reference's (`BrowSp00`), not the id every
other people file here uses, so a snap row reaches a player page only through
`player.pfr_id` — a column the player file carries for 91.3% of players and
that schema 5 adds. It resolves 99.93% of snap rows: 333,129 of 333,375. The
246 it does not, across 29 people, are dropped rather than stored, because a
snap count with nobody to attribute it to has no page to be shown on. The
count is reported by the ingest, so the day the crosswalk breaks is a number
on the run summary rather than a page that quietly empties.

Adding the column is a real migration. `CREATE TABLE IF NOT EXISTS` does
nothing to a table that exists, so a database built at schema 4 keeps a
`player` table with no `pfr_id`, every snap row fails to resolve, and the
page is simply blank. `store.ADDED_COLUMNS` carries it.

### Snap counts begin in 2013, and 2012 is the reason to be careful

The release has a `snap_counts_2012.csv`. It holds a header and nothing else.
**Published and empty is not the same as absent**, and neither of them is a
failure: an empty file yields no rows and says so. The fetcher asks for no
season before 2013 at all, so a 404 inside the measured range stays what it
should be — a real failure for every season but the one in progress.

### The injury report is not a game

It is filed before one, and one of the weeks here never produced a game.
Buffalo and Cincinnati both filed for week 17 of 2022; the game was abandoned
after Damar Hamlin's cardiac arrest and never played. Those 17 rows are the
only 17 of 92,890 that reach no game in this archive, and they are real
reports, so a report is keyed on its week and `game_key` is allowed to be
NULL rather than the report being thrown away for the game's absence.

The source also **repeats a player when a club updates its report**. Two
players in week 15 of 2024 appear twice, Questionable on the Saturday and Out
on the Sunday morning. The latest `date_modified` wins: the last thing the
club said before kickoff is what the report said.

### The report changed in 2016, and a count across that line is two reports

The league abolished the "Probable" designation after the 2015 season. The
archive has 17,400 of them and then none, and blank game statuses go from
about 150 a season to about 3,000: a player a club expects to play now
carries a practice line and no designation at all.

| | 2015 | 2016 |
| --- | ---: | ---: |
| Probable | 2,702 | 0 |
| no game status | 153 | 2,036 |

So the two are counted apart everywhere. A player page has a **Listed**
column for the weeks he appeared with no designation, and the game page lists
only the players a club designated — three thousand practice lines a season
would bury the handful who were doubtful — and says so under the table.

The column is headed **Reason**, not Injury: 4.8% of the archive's reasons
are not an injury, 1,861 of them illness and the rest a personal matter, a
rested starter or a coach's decision.

### A season's snap share is an average, not a ratio

The source gives a share per game and no team total, and this archive stores
no team snap total either, so **his snaps over the team's is not a number
that can be computed here**. The season figure is the mean of his per-game
shares over the games he was on that unit for, and the page says "per game"
rather than leaving a reader to assume a ratio. A game he spent on the bench
is not in the mean, because it would drag it towards a share he never played
at.

A unit he never played carries no row of noughts: a column of zeros beside
the unit he did play reads as a measurement rather than an absence.

### One player gained a page

`people_export.players` draws anybody with a roster or statistics row, and
now anybody with snaps or a report too. That clause is worth exactly one page
across the whole archive — Phil Bates, one game at wide receiver for Seattle
in 2014, the only one of 7,352 snapped players with neither of the other two.
It is here because the alternative is data the export holds and will not
draw, which is a hole that only ever grows.

### The refresh

A step between the people ingest and the play-by-play reduction, and **after
the people ingest, never before**: the snap join reads `player.pfr_id` out of
the database, so the player file has to be loaded first. A failure fails the
run rather than publishing without the layer, which is how the people layer
is handled for the same reason.

## The page, and what a card says

The NFL page was built phone-first and every check of it was made at 430px.
That is why it shipped with `nav.tabs` fixed to the foot of the window at
EVERY width: on a desktop screen it stranded eight items edge to edge across
the bottom, a long way from the content they switch. The bar is now a row of
tabs under the wordmark, and the fixed bar is a `max-width: 719px` rule — the
phone layout is unchanged.

A card used to be two names and a score. It now carries, for each side, its
rank on the season's board, its record, its pregame rating and its pregame
win probability, and it dims the side that lost. None of that is new data:

- **The pregame numbers are the stored chain**, `rating_game.home_pregame`,
  `away_pregame` and `home_expected`, carried into the exported game row.
  They are what the model believed BEFORE that game, so a 2015 card shows
  the 2015 view rather than today's.
- **An upcoming game has no chain row**, because the replay only rates games
  that have been played — and an upcoming game is the one card a reader most
  wants a number on. `export.expect_upcoming` prices those off the board's
  current ratings, which for a team's very next game IS its pregame rating,
  by the chain's own rule. It never overwrites a stored number and never
  prices a game that has already been played.
- The away side's probability is the home side's complement, computed once
  rather than rounded twice.

The three `<script src>` tags carry a `?v=sha256(file)[:12]` stamp, written
by `scripts/stamp_page_scripts.py` and pinned by a test. This is not
decoration. The service worker serves documents network-first and everything
else stale-while-revalidate, so without a stamp a returning reader can be
handed a FRESH `nfl.html` beside a CACHED `nfl_app.js` from before the
deploy — and a page calling a helper the cached module does not have throws
a TypeError and draws nothing until they reload. `ui/dashboard.html` never
had the problem because `build_dashboard.py` has always stamped its modules;
the NFL page had no generator and so had no stamps. `ui/basketball.html`
still has none.

One trap worth naming: `rating_model.config` **is** the elo block, not the
whole configuration file — `build_ratings` stores `config['elo']`. Reaching
for a nested `'elo'` key finds nothing and silently plays every upcoming game
at a home advantage of zero, which puts two equal teams at 50% instead of the
league's measured 57%. A test pins it.

## What is not here

- **No headshots.** The player file carries a `headshot` URL for 99.4% of
  players. It is not stored, let alone shown: a portrait is a licensing
  question rather than a data one, and the college side's portraits are
  blocked for the same reason.
- **No link to the college side.** The draft file's `cfb_player_id` is a
  Sports-Reference slug (`billy-sims-1`), not a CFBD athlete id, so an NFL
  career cannot be joined to the same player's college page from this data.
- **No plays.** The play-by-play is reduced to drives and scoring (phase 3
  above) and not one play is stored or published.
- **No awards.** nflverse has none, and every source that does — Pro Football
  Reference, ESPN — is a scraping-terms question rather than a data one. See
  `docs/nfl-awards-and-headshots.md`.
- **No depth charts, and no snap counts before 2013.** The depth-chart
  release exists and is not read; the snap archive starts where it starts.
- **No possession, field position or down in the win probability.** The curve
  knows the score, the clock and the pregame rating. A drive-level model
  would need the plays this layer deliberately throws away.
- **No routes of its own.** This page's tabs are buttons, not links, so a
  coach opens in place rather than at a URL. Deep-linking the NFL and
  basketball tabs is one change to both pages and is owned elsewhere.
- **No logos.** nflverse's logo file points at Wikipedia uploads; the
  sportsdataverse mark archive the college side fetches from does carry all
  32, so this is a fetch to write rather than a question to answer.
- **No favourites on this page.** The football shell's "My teams" has no
  counterpart here yet.
- **No live scores.** The export carries finals and schedules; there is no
  in-game clock, so a card says Final, Scheduled or Time TBD and never a zeroed
  clock. Live NFL data is a separate decision with a licensing cost attached,
  and the measured bottleneck for live data in this repository is GitHub's cron
  delivery rate, not the provider (`docs/live-data-options.md`).
- **No logos.** nflverse's logo file points at Wikipedia uploads; rehosting
  those is its own licensing question.
- **No playoff model.** The 12-team CFP machinery is college football's and
  has nothing to say about an NFL bracket.

## Standings, and what they are not

The standings group by division and order by record, with the rating as the
tiebreak. The league's own tiebreakers — head to head, then common games, then
several more — are not applied, and the page says so rather than implying an
official order.

The **playoff field** above them is a stricter thing and applies three of the
league's real tiebreakers before falling back on ours (see above). It is
still not official: strength of victory and the rest of the ladder are not
applied, and a field computed in October says "as things stand" and calls a
team in front of its division a division *lead*, not a winner.
