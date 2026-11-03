# The basketball export contract, and the two gates around it

Release gate 4's second half. The plan's rule is that a BYU sample must never be
published as national coverage, and that a production loader must refuse a team
sample or an unsupported schema rather than show it as national rankings. This
is both halves of that refusal, and the contract they agree on.

Nothing here publishes. The export writes to a directory and the ratings
workflow writes it to a temporary one; whether these files are ever served is
release gate 6, which does not exist yet.

## Why there are two gates

They stop different things, and neither can stop the other's failure.

- **`src/ncaambb/publish_gate.py`** runs at build time, before the first byte is
  written. It stops a bad export from existing: a pilot-sized sample, a model
  with no held-out score, a holdout the fit could see, a manifest pointing at a
  file that is missing, empty or no longer the file it describes.
- **`ui/ncaambb_loader.js`** runs in the reader's browser, against whatever the
  network actually returned. It stops a stale deploy, a manifest cached from
  before a schema change, a half-served directory and a hand-edited file from
  being drawn as a national rating. The build gate never runs on the reader's
  machine, which is the whole reason this exists.

Both are pure functions over a manifest. Neither repairs anything, and neither
downgrades a failure to a warning.

## The contract

`manifest.json`, at the root of the export directory:

| field | what it is for |
| --- | --- |
| `schema_version` | 2. A reader states which versions it understands and refuses the rest, and still reads 1: the export on disk stays schema 1 until the next refresh runs, so a page that dropped it would go blank between the merge and that run. |
| `sport` | `ncaambb`. A manifest that does not say so is refused, in both gates, so this can never be read as football data. |
| `generated_at` | The as-of time, UTC, `Z`-suffixed. Missing or unparseable is a refusal; in the future is a refusal; old is not. |
| `model` | Name, the full fitted configuration, a sha256 of that configuration, which seasons it was fitted on, which were held out, and what it scored there. |
| `coverage` | Per season: Division I teams, how many are rated, games, finals, rateable games. |
| `latest_season` | Which season a page opens on. |
| `seasons` | Season to file, each with a `?v=` stamp. |
| `files` | Everything else, likewise stamped. |

Each `?v=` is `sha256(file bytes)[:10]`, the same convention football's manifest
uses, so a reviewer reads one convention across both sports. A page builds no
URL of its own: a season the manifest does not name has no URL to guess at,
because guessing is how a cached file gets served under a newer manifest.

The payload files assign their own globals — `window.NCAAMBB_SEASONS[<year>]`
and `window.NCAAMBB_TEAMS` — and share nothing with football. A test reads the
loader's source and fails if a football global appears in it.

## The floors, and why they are these numbers

| floor | value | why |
| --- | --- | --- |
| Division I teams in a published season | 300 | Division I measured 351 teams in 2015 and 365 in 2026. Every real season clears 300 comfortably; one team, one conference or one tournament field is far below it. |
| Rated share of those teams | 90% | Below that a board silently omits teams a reader expects to find. |
| Games in a published season | 2,000 | A real season is about 6,000. |
| Holdout Brier | better than 0.25 | 0.25 is a coin flip. A model that cannot beat one has not been validated. |
| Games the holdout was measured on | 2,000 | A sample-sized holdout is not validation. |
| Stale after | 180 minutes | A reader-side marker, never a refusal. |

The tests build a national-sized fixture — 320 teams, 2,240 games — rather than
lowering these floors. A test that relaxes the threshold it is testing proves
nothing, and this repository has shipped that mistake before.

## Staleness is said, not enforced

An export whose as-of time is hours or days old is still the last good snapshot
and must stay publishable, marked stale with its age. The plan's rule is that a
provider failure must not replace valid data with empty files; refusing to draw
an old-but-valid export would be the same mistake from the other direction. So
`accept()` returns `ok: true` with `stale: true` and an age label, and an
as-of time that will not parse is reported as unknown and stale rather than as
zero minutes ago — a page that claims to be current because a date failed to
parse is worse than one that admits it does not know.

An as-of time in the *future* is refused in both gates, because that means the
clock or the pipeline is wrong rather than that the data is old.

## What the export contains

Per season: the rating board (one row a Division I team, ranked at build time so
every view of the season agrees on what #7 is) and every game, including games
against non-Division I opponents, which are shown but never rated.

Two records are kept per team: every game played, and Division I games alone.
The football header taught this project that a reader expects the record they
see quoted elsewhere, which includes the non-Division I games, while a rating
only ever learned from the Division I ones.

A missing score stays missing. Football taught this project what a `0` standing
in for "no score" costs downstream, and a test drives it.

Ratings are stored to one decimal and rounded to whole numbers only at display,
the rule football settled on: a rounded value must never be what the next
calculation reads.

## The people files

Who played, and what they did. The data was loaded nationally by B02 and
nothing published it, so the People tab was drawn disabled from the day the
league launched.

Three kinds of file, because three screens ask three different questions and
one file answering all of them would be downloaded by readers who wanted none
of it:

| file | answers | read by |
| --- | --- | --- |
| `people/index.js` | who exists, and which seasons have people at all | the People tab, for search |
| `people/<season>/roster.js` | who is on this club, and their season line | a club's page |
| `people/<season>/<shard>.js` | this player's membership and every game he played | a player's page |

**The shards exist because a season is about 127,500 player-game lines.** A
player page needs one player's, so each season's lines are split sixteen ways
on the player's own id and a page fetches the one shard it needs. The split is
a FLOORED modulo written out longhand on both sides: Python's `%` is floored
and JavaScript's is not, so a negative provider id would land in shard -3 in
the browser and 13 in the exporter, which is a file that does not exist. The
Python test cannot tell the two forms apart — there they ARE the same
function — so the guard that has teeth is in `tests/ncaambb_app.test.cjs`.

**The manifest's `people` section nests under `files`**, which means the
publish gate's own file walk hashes every people file without knowing anything
about people: a missing, empty or stale one is refused for free. The shards
are listed in numeric order, and a reader matches the one it wants by NAME
rather than by position in that list.

**A line carries its own day and opponent.** Measured in Chromium against the
real export: a career spans four or five seasons and a player page loads one
season's schedule, so dating each game by looking it up in the season payload
dated one season and left the rest of the career a table of em dashes.
Fetching four 1.8 MB season files to date forty games is the wrong trade; a
date and two small integers per line cost a fraction of that and make a player
page self-contained. The day is the EASTERN playing date, the same one the
scoreboard groups by.

That is what `people` version **2** is. The page states which versions it can
read and treats an unknown one as no people at all, rather than decoding it on
a guess: a version 1 payload read by this page would draw a career of em
dashes and present it as the player's record.

**NOTHING HERE IS A SUM OF SOMETHING ELSE.** A season line is the player's own
box score lines added up, per club, so a mid-season transfer is two rows and
never one merged player. It is never reconciled against the team's score: the
pilot found 26 box scores that disagree with their game's final, and the right
answer there is to report the disagreement rather than average it away. A
total of nothing recorded is absent, not 0 — a player whose minutes nobody
wrote down and one who sat on the bench are different things, and 0 merges
them. A measured nil stays a measurement.

A player with a line but no roster snapshot is still put on his club's roster,
with no jersey. 29 Division I players in 2025-26 are in exactly that position
and their minutes are real.

**Only the most recent `export_people.DEFAULT_SEASONS` seasons get people
files**, six at the time of writing. A bound, not a preference: thirteen
seasons published in full is tens of megabytes added to a site already
measured at 461 MB against GitHub Pages' 1 GB cap (see
`docs/published-payload.md`), for seasons whose rosters nobody is looking up,
and six covers every player now playing for his whole career. `--people-seasons
0` publishes every season that has people; the manifest states which seasons
are carried either way, and the page says so rather than drawing a blank for
the rest. The refresh reads the same number from the exporter, so it does not
spend provider quota fetching people the export would not publish.

## Provider Elo is still never read

`/games` returns `homeTeamEloStart` and `homeTeamEloEnd`. They are not fetched,
not stored, and a test serialises the entire export and fails if any spelling of
a provider rating appears in it.

## Measured on the real archive

Run 39382139599, 2026-10-20, over the twelve ingested seasons. The export
passed the build gate, and the standalone gate passed again on the written
directory.

| season | D-I teams | rated | games | finals | rateable |
| --- | --- | --- | --- | --- | --- |
| 2014-15 | 351 | 351 | 5949 | 5932 | 5494 |
| 2015-16 | 351 | 351 | 5991 | 5951 | 5514 |
| 2016-17 | 351 | 351 | 5982 | 5964 | 5520 |
| 2017-18 | 351 | 351 | 5987 | 5968 | 5501 |
| 2018-19 | 353 | 353 | 6066 | 6049 | 5597 |
| 2019-20 | 353 | 353 | 5856 | 5767 | 5328 |
| 2020-21 | 356 | 347 | 5282 | 4285 | 3944 |
| 2021-22 | 358 | 358 | 6387 | 5966 | 5475 |
| 2022-23 | 363 | 363 | 6261 | 6222 | 5715 |
| 2023-24 | 362 | 362 | 6249 | 6243 | 5726 |
| 2024-25 | 364 | 364 | 6299 | 6292 | 5764 |
| 2025-26 | 365 | 365 | 6318 | 6300 | 5752 |

Every season but one rates every Division I team. 2020-21 rates 347 of 356,
which is the covid season showing up in the coverage column rather than being
smoothed over: nine teams played no rateable game the model could learn from.
It clears the 90% floor at 97.5%, so it publishes, and its coverage row says
what it is.

## Running it

```
python -m src.ncaambb.export --out <dir>        # refuses rather than writes
python -m src.ncaambb.publish_gate <dir>        # exit 1 if it must not publish
```

The ratings workflow runs both: the export to a temporary directory, then the
standalone gate on what was written, because a check against a plan is not a
check against a disk. The export is uploaded as an artifact and committed
nowhere.

## `data_as_of` — how old the data is, as against the export

`generated_at` is when the export was built. It is always now, so on its own
it says nothing about the data: a refresh that cannot reach the provider
rebuilds from the committed cache and stamps today's time over games that may
be weeks old.

`data_as_of` is when the provider last answered about the **latest season**,
the one a reader's scoreboard shows. A settled season refetched this morning
says nothing about tonight's scores, and a roster fetched today over last
month's schedule does not make the scores current, so only that season's
`schedule` snapshot counts.

- `null` when nothing records it, which is true of any cache written before
  the ingestion began recording its own age. The page then says
  `last updated: unknown` and marks it stale.
- The publish gate never refuses an export for being **old** — staleness is
  marked, not hidden, and refusing stale data is how a provider outage takes
  the whole page down. It does refuse one dated in the **future**, because a
  page draws a future moment as "updated just now", which is the one way this
  field can lie about freshness instead of reporting it.
- The age is recorded by the ingestion in `data/ncaambb/raw/<season>/fetched_at.json`,
  committed beside the gzipped responses. A season is dated by its **stalest**
  file, and a season only partly recorded has no age at all rather than a
  wrong one.
