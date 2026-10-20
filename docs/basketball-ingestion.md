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

## Validated against real data

Run [39369011851](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/39369011851)
ingested seasons 2026 and 2027 and then loaded them again:

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

## Commands

```
python -m src.ncaambb.probe_coverage --seasons 2024,2025,2026,2027
python -m src.ncaambb.ingest --seasons 2026,2027
python -m src.ncaambb.ingest --seasons 2027 --refetch
python -m pytest tests/test_ncaambb_ingestion.py tests/test_ncaambb_store.py -q
```

The key comes from the environment or the repository secret `CBBD_API_KEY`,
never from committed content, and no module prints it.

## Not built yet

Rosters and player identities at national scale, box scores at national scale,
team Elo (B03), the export contract and publisher gate (B04), and the UI (B05).
The `player`, `player_team_season`, `team_game_stat` and `player_game_stat`
tables exist and are not yet populated by any loader.
