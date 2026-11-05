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
| plays read | 14,246,266 |
| fetched and discarded | 0.44 GB |
| games reduced | 31,216 |
| periods stored | 126,943 |
| curve points stored | 1,684,983 (54 a game) |
| key plays stored | 155,820 (5.0 a game) |

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

**1,390 of 31,216 games have a play log that does not reach the final score
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
comparison `ui/data/nfl/games` is 23 MB for 7,366 games; this is 31,216
games at 1.5 KB each.

## The key plays, and why they are chosen without the model

A play is kept when any of these is true, and the `reason` column says which:

| reason | rule |
| --- | --- |
| `lead` | it put a side in front that was not the side in front, in the fourth period or an overtime |
| `tie` | it levelled the game, in the fourth period or an overtime |
| `late` | it was scored in the last two minutes of the last period with the margin inside two possessions either side of it |
| `final` | it was the last score of the game |

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
