# The NBA archive

Stage N1 of the NBA tab: the source, the database and the loaders. Nothing
here is published yet — no export, no page, no entry in the league selector.
The scope this follows is the NBA tab scope doc, and the stages after this one
are the rating and the tab (N2), game pages (N3), people (N4) and the
postseason bracket (N5).

## The source

ESPN NBA data, 2002 to the season in progress, published by the
[sportsdataverse](https://github.com/sportsdataverse) project as static
Parquet files. No key, no request allowance, no paging: each dataset and
season is one public file. That is the same shape as the NFL tab's source and
a deliberate choice over a metered API — the rolling cap that stalled the
basketball track for eleven days cannot happen to a file anyone may fetch.

**Read the release assets, not the repository tree.** The same data appears in
two places and only one is complete:

| Mirror | Datasets published as Parquet |
| --- | --- |
| `sportsdataverse-data` release assets | all of them |
| `hoopR-nba-data` repository tree | schedules, play-by-play, team box, player box |

Measured on 2026-11-03. Rosters, standings, player core, season statistics and
the draft all 404 in the repository tree while being published as release
assets, so several plausible guesses against the tree returned 404 for data
that was in fact there. The URL shapes in `src/nba/source.py` were pinned from
`R/load_nba.R` in the hoopR package rather than guessed, and the stem is not
derivable from the tag: `espn_nba_schedules` holds `nba_schedule_2027.parquet`
while `espn_nba_standings` holds `standings_2026.parquet`.

CSV is not published, so Parquet is the only format and `pyarrow` is a real
dependency of the ingest. It is build-time only: the published site fetches
JSON.

**Licensing.** `sportsdataverse-data` carries the MIT licence, which permits
redistribution with attribution. The data underneath is ESPN-derived rather
than NBA official, so the site should credit both. The NBA's own tracking and
advanced statistics are not available at all — `stats.nba.com` answers a cloud
IP with a silent timeout, measured three times from a GitHub Actions runner.

**Seasons are named for the year they end.** `2027` is 2026-27, which is what
the source does and what `identity.season_label` renders as `2026-27`. The
turnover is September: the Finals are over and no game of the new season has
been played, so a run in October and a run the following May agree about which
season is current.

## Three traps, each of which produces data that looks right

### An unplayed game is written 0-0, not NULL

The source fills both score columns with zero for every scheduled fixture —
1,108 of them in the season in progress on the day this was written. Read
naively, next April's schedule is 1,108 draws, and a rating replay would take
every one of them as evidence that two teams are equal.

`status_type_completed` is the only field that tells the truth. So the score
is read **only when that flag is set**, and the habit the other three leagues
have of checking `is not None` is not enough here. `games.parse_row` is where
this is caught, and `tests/test_nba_data.py` drives it directly.

Two consequences worth stating: a completed game with no score is refused
rather than stored, and a completed game with a level score is refused too —
the NBA plays overtime until somebody wins, so a draw is a fault in the data,
not a result. There are none in the archive.

### Two different franchises are both "Charlotte Hornets"

| ESPN id | Seasons | What it is |
| --- | --- | --- |
| 3 | 2002 | Charlotte Hornets |
| 3 | 2003-2013 | New Orleans Hornets (playing in Oklahoma City, 2006-07) |
| 3 | 2014- | New Orleans Pelicans |
| 30 | 2005-2014 | Charlotte Bobcats |
| 30 | 2015- | Charlotte Hornets |

Join those on the name and the Pelicans' first eleven seasons are filed under
the Hornets of today, and nothing afterwards looks wrong. The id is the only
safe key, which is why `src/nba/identity.py` offers no way to resolve a team
by name.

There is no relocation table here, unlike the NFL's. ESPN keeps a franchise's
id across a move — Seattle and Oklahoma City are both team 25, New Jersey and
Brooklyn both team 17 — so the franchise needs no reassembling. What changes
is the name, and `team_season` keeps it so a 2007 box score still says
SuperSonics while the standings say Thunder.

### An All-Star game looks like a regular-season game

The All-Star game and its modern variants (Team LeBron, Team Giannis, USA v
World, Stars v Stripes) carry `season_type = 2`, the same value as a game in
Cleveland in January. Filtering the exhibitions by season type does not work.

What marks them is that their sides are not franchises: ids 31 and 32 and a
dozen six-figure ids, listed in `identity.EXHIBITION_IDS`. Measured across
2002-2027 there are 39 such games, and no franchise ever plays one. A row with
one real team and one unknown id is **not** an exhibition — it is a fault, and
it is refused rather than skipped.

## What is stored

`db/nba.db`, 13 MB, its own file and joinable to nothing in football, college
basketball or the NFL. The whole archive ingests in 22 seconds.

```
python -m src.nba.ingest                 # every season
python -m src.nba.ingest --seasons 2027  # one of them
```

| Season | Games | Played | Teams |
| --- | --- | --- | --- |
| 2001-02 | 1,260 | 1,260 | 29 |
| 2004-05 | 1,314 | 1,314 | 30 |
| 2011-12 | 1,074 | 1,074 | 30 |
| 2019-20 | 1,155 | 1,143 | 30 |
| 2025-26 | 1,326 | 1,322 | 30 |
| 2026-27 | 1,200 | 92 | 30 |

33,457 games across 26 seasons, 39 exhibitions skipped. The short seasons are
real: 2011-12 was the lockout (66 games a team) and 2019-20 lost games to the
pandemic. 29 teams until the Charlotte expansion team arrives in 2004-05.

**Conference comes from the standings feed, division does not.** The feed
groups teams by conference only, so `team_season.division` is left NULL rather
than filled from a table written down in code. The standings file for the
season in progress does not exist until the source writes one, and a missing
one leaves the conference NULL rather than failing an ingest that has the
games in hand — every other fetch failure fails the run, because a database
quietly short of a season makes every count taken from it wrong without
looking wrong.

**Zero attendance is an absence, not an empty arena.** It appears in 174 games
of 2011, which had no empty arenas, as well as in the seasons that did. There
is no way to tell a missing reading from a real zero, so all 2,300 of them are
stored as NULL rather than asserting a crowd of nobody.

## Ordering

By date, never by round. The play-in is played between the regular season and
the playoffs, so any ordering by the name or number of a round puts it in the
wrong place. Ordering a season by round once put a bowl game first in the
football archive and zeroed 1,353 season rating changes; the same shape of
mistake is available here.

## The rating

`config/nba_model.json` holds the fitted parameters and what they scored on
seasons they were never fitted on. `src/nba/fit_elo.py` chooses them;
`src/nba/build_ratings.py` replays with them and writes nothing else. A
refresh never refits.

| | value |
| --- | --- |
| k | 20 |
| home advantage | 80 |
| offseason retention | 0.6 |
| margin model | margin of victory, exponent 0.75 |
| reference margin | 11.039 points (MEASURED, the mean winning margin) |
| fitted on | 2001-02 to 2020-21, 25,585 games |
| held out | 2021-22 to 2025-26, 6,605 games |
| held-out Brier | **0.21809** |
| held-out accuracy | 0.65405 |
| home win rate on those games | 0.55534 |

**The floor it had to clear is measured on this league, not borrowed.** NBA
home teams won 55.53% of those 6,605 games, which puts "pick the home team
every time" at a Brier of 0.24694. That is the number `MAX_HOLDOUT_BRIER`
(0.246) comes from in both gates. Its closeness to the NFL's 0.245 is a
coincidence of two sports having a similar home edge, not a copied constant.

It also beats carrying over any model this repository already had, scored on
the same held-out games:

| settings carried over from | held-out Brier |
| --- | --- |
| **this fit** | **0.21809** |
| college football (k=45, home 50, retention 0.8, result only) | 0.22192 |
| NFL (k=48, home 50, retention 0.5, mov) | 0.22385 |
| college basketball (k=45, home 100, retention 0.8, mov) | 0.22714 |

No chosen parameter sits at the edge of its grid, so each is a fit rather
than a boundary; `fit_elo.py` reports `grid_edges` on every run and the fit
that produced this configuration reported none.

**It is over-confident at the top of its range.** In the held-out seasons
the most confident bucket predicted 0.846 and observed 0.782. That is
recorded in the configuration's own `note` rather than left for a reader to
find, and the calibration error (0.04173) is part of the committed
validation.

**The held-out claim is pinned to the parameters by hash.** A refresh runs on
a fresh runner with no fit report, so the committed `validation` block is the
only honest source for what the model scored. `measured_on_elo_sha256` is the
sha256 of the canonical `elo` block, and `check_validation` refuses a
configuration whose parameters have been edited since the score was measured.
Without the committed claim the gate would refuse every refresh, which is how
the basketball refresh once shipped unable to publish anything.

### The NBA Cup final

**The in-season tournament's championship game does not count in the
standings, and the source says it is a regular-season game.** Every other
game of the tournament does count: the group games and the knockout rounds
are scheduled regular-season games. The final alone is not, which is why the
two finalists end a season having played 83 while everybody else played 82 —
measured across the archive, exactly two teams exceed 82 in each of 2024,
2025 and 2026, and none in any other season.

Left as the source has it, a standings page shows two franchises one win or
loss ahead of the record the NBA credits them with.

It is rateable all the same. It is a real game between two teams trying to
win, and the NBA excludes it from the standings, not from history. So it is
replayed, exported and drawn, with its own season type `cup_final`, and kept
out of every won-lost record.

**Two spellings, because the event was renamed.** 2023-24 says "NBA In-Season
Tournament Championship" and the two seasons after it say "NBA Cup
Championship"; a search for "Cup" alone finds only two of the three. The
string is the source's and could change again, so `tests/test_nba_rating.py`
asserts the invariant behind it: in a season with a final, every team played
exactly 82 regular-season games.

## The export and the gate

`src/nba/export.py` writes `ui/data/nba/` and nothing else:

| file | what it is |
| --- | --- |
| `manifest.json` | the model, its held-out score, per-season coverage, and a version stamp for every file below |
| `teams.js` | the thirty franchises under their CURRENT names |
| `seasons/<season>.js` | one season's board, games and per-season names, about 440 KB, fetched one at a time |
| `history.js` | conference ratings season by season, and the champions |

The gate in `src/nba/publish_gate.py` runs BEFORE the first byte is written
and again against the directory afterwards. `ui/nba_loader.js` is the same
refusal in the browser, against whatever manifest the network returned: the
build gate stops a bad export being written, and the reader's gate stops a
stale deploy, a cached manifest from before a schema change or a half-served
directory being drawn as a rating.

Both are stricter than the college ones in one way: **the league is a fixed
size**, so the team check is an equality rather than a floor — 29 franchises
before 2004-05 and 30 after — and an export missing one franchise fails
instead of clearing a floor by a comfortable margin.

**Staleness is not a gate.** An export hours old is still the last good
snapshot and stays publishable, marked stale, which is what the page does. An
as-of time in the FUTURE is refused, because it means the clock or the
pipeline is wrong.

## The page

`ui/nba.html`, with `ui/nba_loader.js` (the refusal) and `ui/nba_app.js` (the
NBA's own rules). Four tabs of the shared five: Home, Stats, Standings,
Teams. No People, because schema 1 carries no people layer and an empty tab
is worse than an absent one.

Three rules live on this page and nowhere else in Elocast.

**A game's day is its LOCAL DATE.** A 7:30pm tip in Los Angeles is 03:30Z the
next day, and almost half an NBA night tips after midnight UTC. The
basketball page's `dayOf` reads the UTC timestamp, which is right for a noon
Saturday college game and wrong for most of this league, so the NBA has its
own.

**Standings are by conference, and say so.** There are no divisions in this
source. Where the standings feed has not published a season yet — which is
every season in progress — the board falls back to the franchise's current
conference and records which of the two it used in `conference_from`. The
conference HISTORY does not fall back: a trend may not be drawn from today's
alignment applied backwards.

**A franchise is not its name.** Team 25 is the SuperSonics in 2007 and the
Thunder now; team 3 and team 30 have both been the Charlotte Hornets. Every
lookup is by franchise key, and a season's own names come from that season's
payload.

## The refresh

`.github/workflows/nba-refresh.yml`, every morning through the season
(October to June) and on dispatch. Its own concurrency group, its own health
light, and no secret: the source is a set of public release assets with no
key and no cap, so the raw archive is refetched rather than committed.

pyarrow is installed by name in the workflow rather than left to
`requirements-dev.txt`: the source publishes Parquet and nothing else, so an
edit to that file could otherwise take this refresh offline silently.

A dispatch with `seasons` set ingests only those seasons and MAY NOT PUBLISH.
The database is built from scratch on every run, so a two-season run builds a
two-season export, which the gate would pass — it checks the seasons that are
there. The input is for rehearsing an ingest, and that guard is what keeps it
to that.

---

# The game layer (N3)

A game page: the period line score, how the game went, how the win
probability moved, and the plays a game story would print. Everything here
comes from the ESPN play-by-play, and **not one play is stored or
published.**

## The reduction

`src/nba/plays.py`. A season is fetched, reduced, and the file deleted
before the next season is fetched, so peak disk is one 22 MB file. Measured
over 2002-2027:

| | |
| --- | --- |
| seasons | 26 |
| plays read | 14,253,464 |
| fetched and discarded | 0.44 GB |
| games reduced | 31,230 |
| periods stored | 127,001 |
| curve points stored | 1,685,752 (54 a game) |
| key plays stored | 151,373 (4.8 a game) |

**THERE IS NO NBA DRIVE.** The NFL reduction is "drives and scoring plays"
because a drive is football's unit and the feed marks one. Basketball's unit
is the possession, and this feed does not mark possessions at all — there is
no column saying who has the ball, only who made each play. A reduction built
on a guessed possession boundary would be a derived quantity presented as a
stated one. So four tables, each holding something the feed really states:

- `game_period` — the line score, overtime periods included.
- `game_shape` — lead changes, ties, biggest lead and biggest run each way,
  and how long each side spent in front. The three times add up to the
  length of the game, which is a test.
- `game_state` — a thinned margin curve: the score at moments the clock
  really showed, at most one per 48 seconds of game clock (two shot clocks),
  which is 54 points for a regulation game.
- `game_play` — the key plays, by the rule below.

### Four traps, each measured

**OVERTIME SECONDS ARE NOT PINNED BY THE FEED.**
`start_game_seconds_remaining` counts 2880 down to 0 across regulation and
then counts 300 down to 0 again in *each* overtime period, so a
fourth-quarter state and a first-overtime state can both read 240.
`seconds_left` here is REGULATION seconds remaining and is 0 throughout
overtime, which is what `src/win_probability.py` means by it; `elapsed` is
the quantity that keeps running, and it is the chart's x-axis.

**THE LINE SCORE IS A DIFFERENCE OF TOTALS, NOT A SUM OF DELTAS.** Summing
each scoring play's points looks equivalent and is not: one 2010 game's rows
appear twice in the file, and summing deltas across them gave a 213-234 line
score for a game that ended 127-141. Taking the step between consecutive
periods' cumulative totals is right either way, and it also fixed two games
whose per-play deltas landed four points on the wrong side.

**A LEAD CHANGE PASSES THROUGH A TIE.** Counting a change whenever the sign
of the margin differs from the *previous state's* missed every change of the
shape 98-96, 98-98, 98-100 — which is most of them, and the archive-wide mean
read 4.4 a game instead of 6.5. What is tracked is the last side that was in
front.

**THE SCOREBOARD IS THE AUTHORITY ON WHO SCORED**, not the play's `team_id`,
for the same reason it is on the NFL side: the side whose total rose by
exactly this play's points is the side that scored.

### The plays that do not add up

**1,390 of 31,230 games have a play log that does not reach the final score
the box score states, and 1,317 of those are in 2002-2006.** Three read by
hand each end one to three points light on one side: the early ESPN feed is
simply short. `game_shape.reconciled` records it, and:

- a game page shows the box score's final, leaves the period line score out,
  and says everything below it ends where the plays do;
- `fit_win_probability.py` fits on none of them.

Separately, **five games of 2026 have no play-by-play at all** — four
postponed, and 401810469 (2026-01-20, a completed 110-138) which simply has
none published. Its page says so rather than drawing an empty flow.

## The win-probability curve

`src/nba/win_probability.py` and `src/nba/fit_win_probability.py`. The model
is the shared `src/win_probability.py` — the remaining margin as a random
walk the pregame rating tilts, with an exact-tie term — and what the NBA
passes in is **its own clock: 2880 seconds of regulation, where football
plays 3600.** That is the one mistake worth guarding: a state with 1440
seconds left would otherwise read as 40% of a game remaining instead of half
of one, and every number on every game page would be wrong by an amount no
reader could spot. A test asserts it.

**The parameters are the NBA's own.** `config/nba_win_probability.json`:

| parameter | value | how |
| --- | --- | --- |
| `points_per_elo` | 0.032804 | NOT fitted here — read from `config/nba_model.json`, the Elo model's own answer |
| `sigma` | 12.1823 points | the residual spread of the REGULATION margin about the rating's call |
| `overtime_home_win` | 0.5394 | counted, not fitted: 1,420 fit games ended regulation level, 6.1% |
| `tie_half_width` | 1.0418 points | solved so the model's tip-off tie mass matches that 6.1% |
| `time_exponent` | 0.2820 | FITTED, on real in-game states |

Fitted on 1,222,348 states over 2002-2021 (23,121 games); **held out on
352,034 states over 2022-2026**:

| | Brier |
| --- | --- |
| this model | **0.15981** |
| the pregame chance, never updated | 0.21708 |
| the same model at a pure random walk (0.5) | 0.16203 |
| calibration error | 0.02774 |

The exponent is well below the random walk's 0.5 and is not at a grid edge.
**The overtime share is 0.5394 across twenty seasons and 0.600 on 2026
alone** — a reminder, again, that a measurement on one season is not a claim
about the archive.

The search is coarse-to-fine (0.02 then 0.002) rather than one flat grid:
there are ten times as many states here as on the NFL side, because
basketball has no drives and the curve is sampled on the clock, and 161
passes over 1.2 million states is twenty minutes of work for an answer three
decimal places wide. The edge check is against the coarse grid, because that
is the boundary a fit can be pinned against.

## The export

`src/nba/game_export.py` writes **one file a month**,
`games/<season>/<YYYY-MM>.js`, fetched lazily when a reader opens a game.
The page derives the path with `date.slice(0, 7)` — arithmetic neither side
can get wrong — so no index is downloaded to find a game. A season in one
file would be 1.4 MB to read one game of it; a day would be 4,250 files
across the archive for an 8 KB saving on a fetch nobody makes twice. A month
is about 190 games, 1.5 KB each, 220 KB, which Pages serves gzipped at about
100 KB.

The manifest states **one cache token per season**, not one per file: 175
hashes in a file every reader fetches would cost more than the one month file
a reader who opens a game fetches.

The curve is **four parallel arrays** — elapsed, home, away, probability in
per mille — because the same points as objects would repeat four key names
1.68 million times. The shape row is read by the order `shape_fields` states,
so the page never hard-codes an index.

**The win probability is computed at build time, not in the browser.** Every
NBA game in this archive is finished, so there is nothing for a page to
recompute, and one implementation of a fitted curve is better than two.

**An overtime state is not priced.** Regulation seconds remaining is 0 there
and the model would answer 1 or 0 by the score alone, which in the middle of
a five-minute period is not an answer. Those points carry a null probability
— the same rule the fit applies to the same states — and the page says the
line stops at the end of regulation. It says it *only* where it is true:
every game's last point is the buzzer and carries no price either, and the
first browser check caught that sentence on a game that never went to
overtime.

The export is **schema 2**. Schema 1 is still supported: an export written
before the reduction ran is a complete export of everything it claims to
carry, and a page learns the game layer is absent from the missing
`files.games` rather than from the version number.

`ui/data/nba` is **56 MB**, 45 MB of it game detail (13.5 MB gzipped). For
comparison `ui/data/nfl/games` is 23 MB for 7,366 games; this is 31,230
games at 1.5 KB each.

## The key plays, and why they are chosen without the model

A play is kept when any of these is true, and the `reason` column says which:

| reason | rule |
| --- | --- |
| `lead` | it put a side in front that was not the side in front, in the fourth period or an overtime |
| `tie` | it levelled the game, in the fourth period or an overtime |
| `late` | it was scored in the last two minutes of the last period with the margin inside two possessions either side of it |
| `final` | it was the last score of the game |

`late` reads the **period** clock, not `seconds_left`. The two agree in the
fourth period and part company in overtime, where `seconds_left` is pinned at
0 by design: reading it there called every close overtime score late, 5,666 of
them across the archive, one at 3:46 of a second overtime.

Capped at twenty, keeping the latest. **Nothing here is chosen by the
win-probability model**, which is fitted on these same games: selecting the
plays by it and then drawing its own curve through them would be the model
marking its own homework. The page says so.

## The refresh

`python -m src.nba.plays` runs on every refresh, **before** the replay,
reducing the whole archive rather than only the season in progress: nothing
here is stateful, the database is built from scratch each run, and the
alternative is committing derived data and keeping it in step with the
source. A failure fails the run, because an export with no game layer would
be refused by `src/nba/publish.py` anyway — it may not drop a `files` key the
published manifest carries.

A dispatch with `pbp_seasons` set reduces only those seasons and **may not
publish**, for the same reason `seasons` may not.

# The people layer (N4)

Every man who has taken the floor in the archive, his seasons, and every
game of his career the archive carries a line for: **2,635 players**,
**850,950 box-score lines** across seasons 2001-02 to 2026-27, **670,667**
of them with minutes, and **20,132 season rollups** derived from them.

Loaded by `python -m src.nba.people`, which fetches one season of
`player_box` and `player_core` at a time and discards it, the way the
play-by-play reduction does. Nothing raw is stored and nothing raw is
committed.

## Five traps, each of which produces data that looks right

### The box feed calls the NBA Cup final a regular-season game

The schedule classifies it `cup_final` and the export already keeps it out
of won-lost records, so the box feed's numeric season type put a game into a
man's regular season that the rest of the archive leaves out of it: **86
lines** across the three finals the archive has, 69 of them played, and
LeBron James's 2023-24 at 72 games and 1,846 points where it is **71 and
1,822**.

**A stored line's season type is the schedule's, for every type rather than
only for this one.** `known_games` returns `{game key: season type}` and the
line adopts it, so a second feed disagreeing about a third type is a fact
the archive already has an answer for. The Cup final is then its own season
type end to end — its own rollup row, its own table and its own game-log
heading on a player page — rather than dropped, because it is a game of the
man's career.

### An All-Star game is filed as a regular-season game

`player_box` carries the All-Star game, the Rising Stars game and, in some
seasons, pre-season exhibitions, with `season_type` set to the regular
season's value. Left alone, an All-Star starter's regular-season line gains
a night played against nobody's defence, and his points per game with it.

**A line whose team is not one of the thirty franchises is dropped**, which
is `identity.is_franchise`, the same rule the schedule applies to itself:
"Team LeBron" is not a franchise. 479 lines go this way, and the derived
totals then agree with the feed's own where the naive sum does not. A
second filter keeps only lines for games **the schedule already has**
(`known_games`), which catches anything an exhibition's team id does not.

Neither filter removes the Cup final, and it should not: that is a real game
between two franchises and the schedule has it. What the Cup final needed
was the fix above — taking its type from the schedule.

### `did_not_play` is unreliable, and minutes are not

The feed's own "did not play" flag is absent or wrong through the early
seasons. **Minutes are what say he played**: `played = 1` where the line
carries minutes and 0 where it does not. 180,283 lines are on the roster and
off the floor, and a page shows none of them — a game a man did not play is
not a game of his career.

### The season-statistics feed covers the regular season only

`player_season_stats` has no postseason and no play-in rows at all, so a
rollup taken from it would lose every playoff career in the archive. The
rollups here are derived from **the stored per-game lines**, which is the one
source of truth the layer has, and the feed's own count of games played is
kept beside them as a stated check rather than as an alternative.

### The feed's count of games played disagrees, in both directions

For **1,006 of the 12,819** regular seasons the feed carries a count for,
`gamesPlayed` does not match the number of lines stored: **966** where the
feed counts more and **40** where it counts fewer. 876 of the 1,006 are
before 2015 and 870 of them are by one or two games. The two directions are
not the same defect and neither is corrected.

Nothing reconciles them. `person_season.feed_games` holds the feed's number
and `matches_feed` whether it agrees; a player page stars the count, says in
a tooltip what each side claims, and prints a note under the table. **The
count shown is the games the archive can show you.**

The first version of this section claimed the feed always counted more. It
does not, and the claim survived because the measurement behind it counted
`person_season` rows rather than man-seasons and looked only for the
direction it expected.

### And one that removed a table rather than inventing a join

The draft feed was going to carry draft position. It is a **separate id
space**: none of the 60 picks in the season sampled joined to a player in
that season's `player_core`. The table and its loader were deleted rather than
joined on a name. Draft year, round and pick come from `player_core` itself,
which has them for the men it has them for.

## Two ids, one man

Six players appear under two provider ids, each id holding part of one
career with the same name and the same date of birth. `person_alias` links
them **symmetrically** — neither id is primary — and **nothing is summed
across the link**. A player page says the archive holds the career under a
second id as well, links it, and prints only the seasons filed under the id
on screen. A doubled career is worse than a split one, and the archive
cannot say which id holds a given game without reading the game.

## What the page fetches

**One file per player, not a shard holding dozens.** At 2,635 players a file
each is 2,635 files, mean 19 KB, median 9 KB, largest 144 KB, and a reader
who opens a player page fetches exactly his own. The football archive
sharded its 99,813 people because a file each was out of the question; at
this size sharding would mean fetching a megabyte to read one career.

`people/index.js` is 119 KB and holds a **search list** — id, name,
position, the span the archive has him playing, games and points — not a
statistics table. It is sorted by name so a search can scan it; the page
sorts it by points to open on the leading scorers.

**One cache token covers every player file**, for the reason the game layer
has one per season: the path is derived by the page from the player's own id,
so the manifest does not name 2,635 files. The index carries its own token,
because it is the one file a reader fetches before knowing which player he
wants.

`files.people` is **optional** in the manifest. An export built before this
loader has run carries no such key, and then the People tab is absent rather
than empty — a page learns the layer is missing from the manifest, not from
a file it has already fetched.

## What a page may say, and what it may not

- `first` and `last` are the first and last season **the archive** has him
  playing, which for a man who debuted in 1996 is 2002. The page says "in
  this archive", never a rookie year.
- The career line under a season table sums **that man's rows of that season
  type** and nothing else. The regular season, the playoffs and the play-in
  are three tables with three career lines.
- The leading-scorers list says what its totals are: the archive's own count
  of each man's regular season, not a career record.

## The refresh

`python -m src.nba.people` runs on every refresh, **after** the ingest and
**before** the export: a box-score line for a game the schedule does not
carry is dropped rather than invented, and the export decides from
`person_game` whether there is a people layer to write at all. A failure
fails the run, because an export with no people layer would be refused by
`src/nba/publish.py` anyway — it may not drop a `files` key the published
manifest carries.

A dispatch with `people_seasons` set loads only those seasons and publishes
a partial archive of careers, for the same reason `seasons` does.

# The playoff bracket (N5)

Every playoff series the archive has, 2002-2026, drawn under
**Standings > Playoffs**, plus the play-in listed beneath it.

**NO NEW DATA, AND NO NEW FILE.** The schedule already carries every
postseason game: 2,094 of them, which group into **375 series**, exactly
**15 a season in all 25 seasons** — a 16-team bracket with nothing missing.
`src/nba/series.py` is pure functions over the games and the board the
export has already built, it adds no query, and the result rides in the
season's own payload at about **5 KB a season**, in a file a reader who
opens a season has already fetched.

A series is a season and an **unordered pair of clubs**. Two clubs can meet
at most once in a bracket, so the pair identifies the series and no round
label is needed to group the games.

## Three traps, each of which produces a bracket that looks right

### The 2020 bubble left phantom rows

Games that were scheduled, postponed and never played are still in the
schedule with no score, sitting beside the game that replaced them: **nine
such rows**, which made three series look eight or nine games long. A game
with no score is not part of a series here, which also keeps a season in
progress from drawing a bracket out of fixtures nobody has played.

### The 2002 first round was best-of-five

**Eight** of that season's series were won with three wins, and a 3-0 sweep
is three games; from 2003 on every series is best-of-seven. So a series'
result is **read off the games** rather than assumed to be first-to-four,
which would call the 2002 first round unfinished. The page says so.

### The feed's own round labels cannot be used

`notes` is absent for all of 2002 and 2003, says only "PLAYOFFS" for **316
games**, and spells the same round four ways — "WEST 1ST ROUND", "WESTERN
CONFERENCE 1ST ROUND", and the same pair for the East.

The round is derived **structurally** instead: the k-th series a club plays
in a postseason is round k, because the winner of a round plays in the next
one. Checked across the archive — every one of the 25 seasons comes out
**8/4/2/1**, and the two clubs of a series never disagree about which round
it is.

## Seeds are derived, and ties are not broken

Nothing in the feed says a club was the fourth seed. A seed here is its rank
by **regular-season record within its conference**, which is the quantity
the real seeding is built on — and not the rank by rating, which would put
a club the standings had seventh at number two.

**Clubs level on record share a seed.** The NBA's tiebreakers are a cascade
of head-to-head, division and conference records that this archive cannot
reproduce, so a tie is left as one and the next seed is skipped. In 2025-26
that is two fifth seeds and two sevenths in the East, which is what the
records say. A club with no conference in a season is left unseeded rather
than filed under a guess, and the page prints a blank, never "(0)".

## A bracket of the wrong shape is refused, not drawn

A season that does not hold 8, 4, 2 and 1 series is reported: the payload
carries `ok: 0` and the reasons, and the page says the archive cannot draw
that bracket. If the league changes the format, a reader is told the archive
no longer knows the shape instead of being shown a picture with a series
quietly missing.

## The play-in is games, not a bracket

**Two formats, not one:** a **single** game in the 2019-20 bubble, and
**six a season** since 2020-21, four clubs a conference. So the play-in is
listed as games with what each decided left unsaid, rather than fitted to a
shape the archive has held for only part of its span. Each date opens that
game's own page.

## A note on class names

The bracket's CSS classes all carry a `bk` prefix. `.club` is the club
directory's tile — a bordered, padded, filled box — and an unprefixed
`.club` inside a series drew every club name in a tile inside the series
box. Scoping the new rule to `#bracket-body` does not help, because the
*other* rule is unscoped and applies anyway. `tests/nba_page.test.cjs` has a
test that fails if a bracket element ever carries a class the rest of the
page styles.

# The club marks (N6)

Every other league on this site draws its clubs behind their logos; the NBA
tab drew bare names with an empty column beside them, which is what the
`nomark` class in `ui/nba.html` was holding open.

## The source is not the one the archive names

Every `team` row already carries ESPN's own logo URL, fetched with the club
— and `a.espncdn.com` is refused by the build container's egress proxy,
measured again while this was written. So the images come from
`sportsdataverse/sdv-assets`, whose manifest is on
`raw.githubusercontent.com`: the archive the college marks have come from
since 2026-10 and the NFL's since 2026-10-29. It carries 486 NBA rows, 30
clubs in sixteen variants.

## The join is ESPN's own id

The NFL side matches on the club name, because that is all its rows agree
on. Here the manifest's `entity_id` **is** the ESPN team id, which is the
number inside every team key (`nba:espn:team:1`), so the join is an identity
rather than a string match. Measured 2026-11-08: all 30 clubs resolve by id,
each with a `default` and a `dark` cut, each 500x500, with no duplicated
rows to disambiguate. The name beside each id is **checked** against the
name the database holds and a disagreement fails the run, because an id the
provider has reassigned would otherwise publish another club's mark in
silence.

## Thirty clubs cover twenty-five seasons

The archive stores a franchise, not a club-era: Seattle's rows and Oklahoma
City's share one key, as New Jersey's and Brooklyn's do. So a 2008 game
draws today's mark for the club that played it. That is a statement about
the franchise, which is what the rating is about too, and it is why this
needs no dated set like the college side's.

## Banked, not hotlinked

`python -m src.nba.logos` writes `ui/nba_logo_assets.json`: one shrunk
256 px data URI a theme, plus a `dim` list naming the themes the artwork was
measured to disappear on (`src/logo_contrast.py`, the same rule and the same
two backgrounds as the other leagues). Committing the images rather than
pointing at a CDN is what keeps a deploy from depending on somebody else's
service, keeps the installed app working offline, and keeps that CDN from
being told who reads the site.

The file lives **outside** `ui/data/nba/`, which the refresh replaces
wholesale, and it carries **no `?v=` stamp**: a stale module is missing a
function the fresh HTML calls, while a stale JSON of logos draws last
month's mark and nothing else.

`.github/workflows/fetch-nba-logos.yml` is dispatch-only and fetches it, for
the three reasons the NFL's job is separate — it is the only NBA job that
reaches the mark archive, it needs Pillow where the nightly refresh is
standard-library only, and a club redraws its mark about once a decade. The
page fetches the file lazily and re-renders when it lands; where it is
absent, every name draws exactly as it did before, and so does a club the
archive has no mark for.

**This is not a licence.** The archive grants nothing and claims nothing.
The marks belong to their clubs and are used nominatively, to identify the
club that played the game.

---

# The team statistics (schema 4)

PxP+, the two factors behind it, and the opportunity grades, computed from
the team box score. The definitions are in
[the NBA team statistics reference](nba-team-metrics.md); what follows is
how they are loaded, measured and published here.

`src/nba/team_box.py` loads the feed, `src/nba/schema.sql` stores COUNTS
and nothing derived, and **`src/nba/team_metrics.py` is the only module that
divides**. The season board and the game page read the same replay, so the
two cannot disagree.

## What reproduces

Against `team_box` for 2025-26, straight from the production feed:

| | |
| --- | --- |
| regular-season games | 1,230 |
| measured | 1,229 |
| franchises | 30 |
| league scoring rate | 1.12744 points per possession |
| highest PxP+ | Denver, 106.1 |
| highest raw Net Opportunities | Detroit, +3.47 |

Those are the pilot's own numbers, reproduced from this archive rather than
from the file it was built on. **The 1,230 is a coincidence worth stating.**
1,235 game ids carry season type 2; four are exhibitions (World, Team Stars,
Team Stripes) and the NBA Cup final is classified apart by the schedule,
which leaves 1,230 regular-season games — while one of them, 401810169
(Chicago at Orlando, 2025-12-01), has no turnover count for Chicago and
cannot be measured. The pilot's 1,230 was a different set with the same
size.

## Four traps, each of which produces data that looks right

**1. The feed calls the NBA Cup final a regular-season game.** The same trap
`src/nba/people.py` documents. A stored row takes the SCHEDULE's season
type, for every type, so the two finalists do not carry 83 games into a
season rate.

**2. The All-Star game is filed as a regular-season game too.** Four such
games in 2025-26. Season type cannot find them; `identity.team_key` refuses
their sides, because they are not franchises.

**3. `total_turnovers` means two different things in two eras.** The
identity `total = turnovers + team_turnovers` holds on 62,028 of the 62,051
rows that carry a total. But in **every season from 2001-02 to 2011-12**,
`team_turnovers` is a COPY of `turnovers` — equal on all 2,520 rows in
2001-02 and on all of them in each of the eleven seasons — so the total is
the team's turnovers counted twice: **28.9 a game against a true 14.4**.
Read at face value it inflates those seasons' possession estimates by about
12%, and because PxP+ is measured against the league it still centres on
100 and still looks right. **2017-18 is the opposite hole**: the total is
NULL on 2,620 of its 2,622 rows, the only season in the archive missing it.

So `turnover_basis` is decided per SEASON from that season's own rows — the
duplicate share is 1.000 for each of the twelve affected seasons and at most
0.0004 for every other — and stored beside the count it chose. The basis is
published, and a page drawing one of those seasons says the turnovers are
the narrower count. The sanity check is that possessions per game now runs
94 to 103 across the archive with no step at the 2012-13 boundary; before
the fix, 2011-12 read about 109.

**4. A count can simply be missing, and it costs the whole game.** The
possession denominator is SHARED, so a side short of a count makes its
opponent unmeasurable too. One game in 2025-26. Reported as unmeasured,
never imputed.

## What is published

The season payload carries a `team_stats` section — the league's own rate,
the exposure it was measured on, the pilot priors and what could not be
measured — and each board row carries `pxp`, `cci`, `ppc`,
`net_opportunities` and `measured_games`. A club with no measured game
carries **no keys at all**, so a reader can tell "no estimate" from "exactly
average".

The per-game half rides in the month files under `ts`, keyed `home` and
`away`: each side's PxP+, its opportunity rate, what the matchup expected of
it, its residual and the net grade. It is there rather than in the season
payload because a reader who opens a season fetches that and a reader who
opens a game fetches this. A game carrying statistics but no reduced
play-by-play is still written: the two layers are loaded by different
modules and either can be the one that has not run.

Rates go out at three decimals and the page draws one or two. The identity
`chance creation × points per chance ÷ 100 = PxP+` holds exactly at full
precision inside `team_metrics` and to within that rounding on the wire.

## The expectation is frozen by date

A game's expected opportunity rate is built from what both clubs and the
league had done BEFORE it: `team offensive + opponent allowed − league`, on
rates centred at 100. Every game on a date is expected from the state before
that date's first game, and the whole date is observed afterwards — so a
replay truncated anywhere leaves every earlier expectation bit-identical,
and no expectation depends on resolving two tip-off times within a day.

The priors are the trial's, not fitted here: a club starts a season with
1,500 possessions of exposure at the current league rate, and the league
starts with 6,000 at the previous season's. The export says so rather than
letting a tuned number read as a measured one.

## What the gate does and does not refuse

The gate refuses arithmetic that cannot be true of any archive — more games
measured than played, a negative count — and **does not impose a coverage
floor**. Which games can be measured is the feed's business, and refusing a
correct export because a provider dropped one field would take the site down
for one missing number. A shortfall is marked, in the coverage row and on
the page.

`src/nba/publish.py` has one check of its own, because these statistics have
**no `files` key**: they ride in the season payloads and month files that
`files` already names, so the layer-regression check cannot see them go. A
refresh whose `team_box` step failed would otherwise publish the same file
list with every PxP+ missing. The comparison is per season and
one-directional, so a season the live export never measured is not a loss.

## What is not here

The Elo performance modifier in the reference is **a trial and is not
wired in**. The rating is still win/loss Elo and these statistics change
nothing about it; the board exposes them beside the rating, which is what
keeps the two independent. The player scoring index is a separate research
note.

## The lineup pilot (not in the archive)

`analysis/nba_lineups/` rebuilds 2017-18's lineup stints from the ESPN
play-by-play: 39,541 stints over 1,306 games, 92.9% of them reconciled
against the box score player by player. It writes no table, feeds no
rating, and runs in no workflow; its README states the four quality
statuses and the five traps in the feed, the sharpest being that
`athlete_id_1` on a substitution row is the entrant only when the row
names both players.

It was built for a Player Elo fitted on lineups, which is not the route
the rating below took: the handoff settled on imported net points
instead. It stands as its own measurement — the only check here on the
stint archive the handoff quotes, which is keyed by NBA.com game ids, a
provider unreachable from here and from Actions.

# The player rating

`src/nba/net_points.py` imports ESPN's per-player, per-game net points;
`src/nba/player_elo.py` walks them into one rating a player;
`#section=people&view=elo` and the player pages draw it. Schema 5.

## It is a weighted average, not an Elo update

Whatever it is called on the page, there is no expectation and no error
term. A grade is already a measurement against the league, so the replay
has nothing to predict. What it has is an **exposure** problem — a
12-possession appearance and a 95-possession one are not equal evidence —
and that is what the weighting is in and what the half-life is measured in:

```
offence  = 100 * offensive net points / offensive team possessions
defence  = 100 * defensive net points / defensive team possessions
exposure = (offensive + defensive team possessions) / 2
decay    = 2 ** (-exposure / 2000)
weight  <- decay * weight  + (1 - decay)
offence <- decay * offence + (1 - decay) * this game's offence
defence <- decay * defence + (1 - decay) * this game's defence
impact   = offence / weight + defence / weight
rating   = 1500 + 25 * impact
```

**The two sides have different denominators** and neither may stand in for
the other: a player's team has the ball for one count of possessions while
he is on the floor and the opponent for another. The archive carries both.

**All three accumulators start at zero, so the first grade is the rating**
— the `(1 - decay)` factor divides out — and the rating *before* a man's
first graded game is NULL, never 1500. A page that drew 1500 there would
show a man arriving at exactly average and then moving, which never
happened.

## It carries across seasons, and that is measured

Taking the rating after a man's fifth graded game of a season and comparing
it with what he actually did over his next 500 possessions of exposure:

| season | carried across | reset each season |
| --- | --- | --- |
| 2024-25 | 2.26 | 4.28 |
| 2025-26 | 2.69 | 4.22 |

Root-mean-square error in net points per 100. A rating that starts every
season from nothing is about twice as wrong about what comes next, which is
why `offseason_retention` is not a thing here: the half-life does it, in
possessions rather than in months.

## The crosswalk, which is the part that can silently lie

The archive is keyed by **NBA** ids and ours by **ESPN** ids, so every
grade is resolved before it is stored, and anything unresolved is
quarantined in `player_net_points_unresolved` with its reason rather than
dropped:

- **A game** resolves on the date and the two franchises, through a
  30-row NBA-to-ESPN franchise map. A game the NBA lists at a neutral site
  has no home team, so ESPN's own `home` flag on the grades says which side
  is which. Regular season only.
- **A player** resolves on a name unique in that season, *checked against
  the games he played*. The name alone would be a guess. A name matching
  two people, or none, falls to the appearance fingerprint — the set of
  games played, which is nearly unique. Two men in this archive needed it:
  "Nathan Mensah"/"Nate Mensah" and "Cui Cui"/"Yongxi Cui".

One player is quarantined: Jaron Pierre Jr. (NBA `1643555`) has no ESPN
person here at all.

## A zero is a grade; a missing possession count is not

A net points of zero is the average performance, and reading it as missing
would drop every ordinary night in the archive. A row with no possessions
on one side of it is not a grade: the rate would divide by zero, and a man
on the floor for none of his team's offensive possessions has said nothing
about its offence. A did-not-play is not a grade either.

## The cutoff is a board's rule, not the rating's

A man is `rated` at 500 **current-season** minutes, `provisional` under it,
`unavailable` with no graded game. The board ranks the rated ones only — a
board that placed a man on forty minutes seventh in the league would be
making a claim the measurement cannot carry — but the payload carries the
provisional men too, because for the first six weeks of a season nobody is
over the cutoff and a payload holding only the rated would hide the whole
layer until December. Where none are ranked the page lists them **by floor
time**, saying so: six minutes of net points can read as +22.

## What reconciles, and what does not

The grades reconcile to ESPN's own season totals exactly on team
possessions and games played, and within 0.0053 of a net point on the
season, which is the rounding in the stored per-game values. Above 500
offensive possessions the per-100 rate agrees to 0.001 in 2023-24 and
2024-25.

**2025-26 is the exception.** Its days were saved as they happened and ESPN
has since revised some of them, so 387 of 452 players agree and the worst
is 0.87 per 100. A replay over it is reproducible from this archive but is
not ESPN's current published season number.

## Two things the gates will not let through

`publish_gate.check_player_ratings` refuses arithmetic no archive can
produce — more players carried than have a graded game at all, a negative
count, a cutoff that is not a positive number of minutes — and, as with the
team statistics, **imposes no coverage floor**: in October nobody has 500
minutes, and a gate that demanded a ranked board would refuse every export
of the first six weeks of a season.

`src/nba/publish.py` adds the layer-regression check these ratings need for
the same reason the team statistics need one: they have **no `files` key**,
so a refresh whose import step failed would otherwise publish the same file
list with every rating gone. Per season and one-directional.

## What is not here

Nothing rates a *club* from these. The player rating is beside the club
rating and feeds nothing into it; the rotation-change estimate is the
milestone after this one. The lineup reconstruction in `analysis/` is a
different measurement from a different provider and is not in the archive.
