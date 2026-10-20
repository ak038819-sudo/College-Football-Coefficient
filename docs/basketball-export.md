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
| `schema_version` | 1. A reader states which versions it understands and refuses the rest. |
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

## Provider Elo is still never read

`/games` returns `homeTeamEloStart` and `homeTeamEloEnd`. They are not fetched,
not stored, and a test serialises the entire export and fails if any spelling of
a provider rating appears in it.

## Running it

```
python -m src.ncaambb.export --out <dir>        # refuses rather than writes
python -m src.ncaambb.publish_gate <dir>        # exit 1 if it must not publish
```

The ratings workflow runs both: the export to a temporary directory, then the
standalone gate on what was written, because a check against a plan is not a
check against a disk. The export is uploaded as an artifact and committed
nowhere.
