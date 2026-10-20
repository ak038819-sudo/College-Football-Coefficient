# Basketball storage and ingestion

How basketball data is keyed, stored and reloaded. Measured coverage of the
provider is in [`basketball-coverage.md`](basketball-coverage.md); the pilot's
player-level findings are in
[`basketball-pilot-evidence.md`](basketball-pilot-evidence.md).

## Isolation

| | Football | Basketball |
| --- | --- | --- |
| Database | `db/league.db` | `db/ncaambb.db` |
| Raw cache | existing football paths | `data/ncaambb/raw/<season>/` |
| Code | existing modules | `src/ncaambb/` |
| Identity | football ids | `ncaambb:cbbd:<kind>:<id>` |

`src/ncaambb/` imports no football module, reads no football configuration and
writes no football table. It also does not import from
`analysis/cbb_player_impact/`: the research pilot keeps its own copy of the
identity rules, and the production copy in `src/ncaambb/identity.py` is a
deliberate promotion rather than a shared import.

## Why a bare provider id is never a key

Football and basketball both come from a CFBD-family provider that numbers
teams and athletes from small integers. BYU's basketball team id is 18, and
some football team is also 18. Every stored key is
`ncaambb:cbbd:team:18`, so a cross-sport join cannot be written by accident.
`entity_key` raises on a non-positive, non-integer or boolean id rather than
coercing it.

## Seasons

A season is stored as its provider ending year **and** a human label: 2026 and
`2025-26`. November 2025 and March 2026 are the same season, and nothing infers
a season from a calendar year. `season_window` spans 1 July of the previous year
to 30 June, deliberately wider than the playing season, so an early exhibition
or a late championship cannot fall outside it.

## What the schema will not hold

`/games` returns `homeTeamEloStart` and `homeTeamEloEnd`. The schema has no
column for them and a test asserts no column in `game`, `team` or `team_season`
has "elo" in its name. A provider rating must not be able to leak into ours.

## Absence is stored as absence

- A missing score is `NULL`, never 0. 44% of the 2026-27 schedule has no
  tip-off time, so `start_time_tbd` records that and the provider's own
  timestamp is kept verbatim rather than zeroed.
- A missing text value is `NULL`, not `"None"`.
- `minutes` on a player's game row is `NULL` when the provider recorded none,
  which is not the same as having played zero.
- A game with no usable, timezone-aware start date is **refused**, because the
  date is what assigns it to a season and a window.

## Non-Division I opponents are kept

Between 8.3% and 8.7% of finals are against a team the provider does not list
for that season. Those games are retained for schedules and records, so the game
row needs both sides to exist. The loader therefore stores a **placeholder**
team for an unlisted opponent, carrying the provider's own name for that side
and nothing invented, with `division_one = 0` on its `team_season` row. The game
carries `rateable = 0` and an `exclusions` list naming why. A game referencing a
team that was never stored is refused by a foreign key rather than orphaned.

## Idempotency and resumability

Every loader is an upsert on its natural key, so a repeated load leaves the same
rows. This is required rather than nice: a refresh re-reads windows it has
already seen, and a resumed run re-reads the window it died in.

Raw responses are cached per season under `data/ncaambb/raw/<season>/` as
gzipped JSON, written atomically through a temporary file so an interrupted run
leaves no half file to be reused. A season already in the cache costs **no
provider requests** to reload, which is what makes both a reload and a resume
cheap. `--refetch` ignores the cache.

A rerun also correctly advances a game: a `scheduled` game with no score
becomes `final` with a score and `rateable = 1`, in place, without a second row.

## Provenance

Every ingestion registers a `snapshot` row with its kind, season, retrieval
timestamp, the git revision it ran from, the number of requests and retries it
spent, the rows it saw, and whether it fetched or reused the cache. Every team,
team-season, game and stat row points at the snapshot that last wrote it.

## Failure kinds

A missing, rejected or unentitled key raises `CbbdAuthError` and stops the run
with exit code 2, storing nothing, because reporting an access failure as an
empty season would read as the provider having no games. A per-season data
failure is recorded and the other seasons still run, and the run exits 1. The
`CbbdAuthError` clause comes first precisely because it subclasses `CbbdError`;
a test pins that an access failure is not demoted to a failed season.

## Nothing here publishes

`src/ncaambb/ingest.py` writes the basketball database and the raw cache. It
makes no commit and writes nothing under `ui/`. The production export contract
and publisher gate (B04) do not exist yet, so the ingestion workflow keeps its
output inside the run and uploads it as an artifact. A basketball failure cannot
affect the football refresh.

## Two caps, both on rows, both measured

| Endpoint | Cap | How it was measured |
| --- | --- | --- |
| `/games` | 3,000 rows | every season exceeds twice it, so one request a season returns half a season |
| `/games/players` | 1,000 rows | see below |

A cap stated in the wrong quantity is worse than no cap, because the threshold
is then one no response ever reaches and every truncation passes as a complete
answer. This code made that mistake: it capped `/games/players` on distinct
**games** at 1,000. Run
[39372061291](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/39372061291)
settled it by requesting 2025-11-13..27 whole and then day by day:

| | rows | distinct games |
| --- | --- | --- |
| whole window, one request | 1,000 | 501 |
| the same 15 days, one request each | 1,642 | 821 |

The cap is on rows, and it cuts the 501st game in half. So every window above
about 500 games came back truncated and was treated as complete: season 2025-26
had **1,535 Division I games with no box score at all** and 14 games with only
one side. `src/ncaambb/probe_box_cap.py` is that measurement, kept so the
number can be rechecked rather than trusted.

Box score windows are therefore five days, which keeps the busiest measured day
(about 200 rows) well under 1,000 before any split is needed. A one-sided box
score is counted and reported as what it is: direct evidence of a truncated
response, not a half-played game.

## People

Player identities come from the rosters **and** from the box scores. A player
can record a stat line without appearing in the roster snapshot, and the line
must not be the thing that gets dropped. `/teams/roster` takes an optional
`team`, so a season's rosters are one request.

The roster feed also names teams the season's own `/teams` list does not carry.
Those teams are registered from the roster, as non-Division I for that season
and only where the season has no `team_season` row for them, because writing
`division_one = 0` over a team the provider does list would make a real team
unrateable. Skipping them instead made the load **order-dependent**: a later
season's placeholder created the team, and the same membership then stored on
the second run, so `player_team_season` grew by one row with no new data. A
team row alone is not enough to check against, because team rows are
season-agnostic.

Box score lines for a game the database does not hold are counted and reported
rather than skipped, because that count is the signal that the schedule load
was incomplete. `team_game_stat.points` is the sum of the box score's own
player points, which is a box-score measurement and not the authoritative final
score in `game`; where the two disagree the count is reported, and a team with
no measured player points stores `NULL`, because summing an empty list writes a
team held scoreless.

## What the people layer covers

`src/ncaambb/audit_people.py` measures it, and separates a provider boundary
from our own loss: a missing box score is split by whether both sides are
Division I, and the Division I half is reported per fetch window, because
truncation loses a stretch of one window while a provider boundary is spread
thin across the calendar. Measured on seasons 2024-25 and 2025-26 in run
[39372361073](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/39372361073):

| Check | 2025-26 |
| --- | --- |
| Finals with a box score | 6,300 of 6,300 (100%) |
| Games with only one side | 0 |
| Roster memberships stored | 5,643 |
| Players with a stat line and no roster row | 5,021, of which 28 Division I |
| Box scores disagreeing with the final score | 26 of 12,600 |
| Box scores with no measured points at all | 39 |

So the roster feed is effectively complete for Division I: 28 players out of
20,576 recorded a stat line without a roster row. The other 4,993 played only
for teams the provider does not list for the season, which is its boundary and
not a gap in the load.

The 26 disagreements are a provider inconsistency, not ours. Most are small
(the box score two points under the final) but a few are not: 10 against a
final of 77. The authoritative score stays the one in `game`; the count is
reported so the inconsistency is visible rather than averaged away.

## Validated against real data

Run [39369011851](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/39369011851)
ingested the schedules of seasons 2026 and 2027 and then loaded them again:

| Check | Result |
| --- | --- |
| Games stored | 12,333 (6,318 + 6,015) |
| Teams / team-seasons | 873 / 1,476 (728 + 748) |
| Second load | row counts unchanged, so loading is idempotent |
| Finals with a missing score | 0 |
| Rateable | 5,752 in 2026, 0 in 2027 |
| Games excluded, by reason | 11,191 none, 1,142 non-Division I opponent |
| Tip-off undecided | 0 in 2026, 2,669 in 2027 |

Every one of those figures matches what `probe_coverage.py` measured
independently through a different code path, which is the point of keeping the
two separate.

Run [39372361073](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/39372361073)
then ingested seasons 2025 and 2026 with their people and loaded them again:

| Check | Result |
| --- | --- |
| Games / teams / team-seasons | 12,617 / 1,544 / 3,088 |
| Players | 20,576 |
| Roster memberships | 20,853 |
| Team box scores / player lines | 25,230 / 254,821 |
| Second load | every row count unchanged |

## Commands

```
python -m src.ncaambb.probe_coverage --seasons 2024,2025,2026,2027
python -m src.ncaambb.ingest --seasons 2026,2027
python -m src.ncaambb.ingest --seasons 2027 --schedule-only
python -m src.ncaambb.ingest --seasons 2027 --refetch
python -m src.ncaambb.audit_people
python -m src.ncaambb.probe_box_cap --season 2026 \
    --first 2025-11-13 --last 2025-11-27
python -m pytest tests/test_ncaambb_ingestion.py tests/test_ncaambb_store.py \
    tests/test_ncaambb_people.py tests/test_ncaambb_audit.py \
    tests/test_ncaambb_box_cap.py -q
```

The key comes from the environment or the repository secret `CBBD_API_KEY`,
never from committed content, and no module prints it.

## Not built yet

Team Elo (B03), the export contract and publisher gate (B04), and the UI (B05).
Every table in the schema is now populated by a loader.
