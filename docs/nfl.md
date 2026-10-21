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

## What is not here

- **No players.** The archive behind this is schedules and scores. Coaches
  are there because every game names them; there are no player pages and the
  People tab is not offered for the NFL. A tab with nothing behind it is
  worse than an absent one.
- **No routes of its own.** This page's tabs are buttons, not links, so a
  coach opens in place rather than at a URL. Deep-linking the NFL and
  basketball tabs is one change to both pages and is owned elsewhere.
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
