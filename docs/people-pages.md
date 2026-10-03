# Player and coach pages

Work toward durable, linkable pages for players and head coaches, following the
v0.1.1 implementation guide. This page records what exists, what it deliberately
refuses to do, and where the data runs out. It is updated as each phase lands.

## Status

| Phase | What it covers | State |
| --- | --- | --- |
| 1 — Identity and schema | Person tables, identity resolution, CFBD roster and coaching ingestion | Landed |
| 2 — Player pages | Player export and detail pages, roster and box-score links, player search | Not started |
| 3 — Coach pages | Coach export and detail pages, team-page coach links | Not started |
| 4 — Stats integration | Season leaderboards linking into player pages | Not started |
| 5 — Coefficient metrics | Coach Elo and CoE analysis | Not started |
| 6 — Visual assets | Portraits, with licensing cleared before anything is committed | Not started |

Nothing in any phase is read by a rating engine. Elo, CoE, the xSRDiff
performance layer and the playoff builders are untouched by roster or coaching
data, and a missing snapshot changes no rating.

## The one rule

**A person is not a name.** Every link the site generates keys on an immutable
`player_id` or `coach_id`, never on the text displayed, because display names
change: nicknames, suffixes, a source correcting its own spelling. Names that
match are a reason to *look*, never a reason to merge.

Resolution is tried in a fixed order (`src/person_identity.py`):

1. **Source identifier.** A CFBD athlete id already recorded maps to its
   existing `player_id`. This is the only path that matches across teams and
   seasons, which is what makes a transfer one person rather than two.
2. **Strong contextual match.** With no source id, a row may attach to an
   existing person only on normalized name *and* team *and* season *and*
   position, and only when exactly one person matches. Two candidates is a
   collision, not a tie-break to be broken.
3. **Nothing.** The row goes to `person_unresolved` for review. No person is
   invented from a weak match.

`normalize_name` folds case, punctuation and generational suffixes, so
`Jerome Gaillard Jr.` and `Jerome Gaillard` share a lookup key, as do
`D.J. Uiagalelei` and `DJ Uiagalelei`. That key is for lookup only. Two
`Mike Williams` normalize identically, which is exactly why step 2 also requires
team, season and position, and refuses a double match.

## Tables

`sql/person_tables.sql`, applied with `CREATE TABLE IF NOT EXISTS` so it adds to
an existing `db/league.db` without dropping anything.

- `players`, `player_external_ids`, `player_name_aliases`
- `player_team_seasons` — one row per player per season per team, keyed
  `(player_id, season_year, team_id)`. A transfer is two rows for one person; a
  new jersey or position is season metadata on the row, not a new person.
- `coaches`, `coach_external_ids`, `coach_tenures`
- `person_unresolved` — every row an ingestion could not confidently attach,
  kept for review rather than guessed at.

## Data sources and what they cover

| Source | Used for | Coverage limit |
| --- | --- | --- |
| CFBD `/roster` | Rosters: identity, team-season, jersey, position, class, optional bio | CFBD's published schema starts at **2009**. Earlier seasons are refused rather than written as empty snapshots. |
| CFBD `/coaches` | Head-coaching history and season records | **Head coaches only.** The feed carries no coordinator or assistant history, and nothing here may be presented as though it did. It also carries **no coach identifier** — see below. |
| Existing player box-score archive | Game logs and box scores, 2004 onward | `data/raw/player_boxscores/` stores **names only**: the archive's cleaning step drops CFBD's athlete ids. See the known gap below. |

### Coach identity, with no source id

CFBD's coaching feed supplies no identifier, so `src/load_coaches.py` derives a
deterministic one:

    cfbd:<normalized name>|<hire date>

and, only when that key collides inside one snapshot, appends the record's first
season. Two people who share a name *and* a hire date are indistinguishable in
this feed; appending the first season keeps them two people, and the collision
is written to `person_unresolved` so the pair can be looked at rather than
trusted. The key is stored in `coach_external_ids` rather than assumed in code,
so a source with real coach ids can be added beside it without a migration.

### Known gap: the box-score archive carries no athlete ids

`clean_player_boxscores` keeps only `name` and `stat` per line, so the 2004-2026
archive cannot be joined to a person by id. Linking a box-score line to a player
page therefore depends on the contextual match (name + team + season +
position), which resolves a line only where a roster snapshot covers that team
and season — that is, 2009 onward, and only once those seasons are synced.
Lines that do not resolve display as plain text, exactly as they do today.
Carrying ids forward would mean re-fetching the archive; that is a decision for
phase 2, not a silent change to 237 MB of committed data.

## Running it

This repository's container cannot reach `api.collegefootballdata.com`, so every
fetch runs in Actions. The key lives only in the repository secret
`CFBD_API_KEY`.

- **Sync rosters and coaches** (`.github/workflows/sync-people.yml`), run by
  hand from the Actions tab. One roster season per commit, pushed as it lands,
  so a timeout costs the season in flight rather than the whole range; a failed
  season stops the run and names the year to resume from. The coaching history
  is a single all-or-nothing fetch, because a partial pull would erase careers
  from the snapshot rather than merely delay them.
- **Loading** happens in `run_pipeline.py`, from the committed snapshots. Both
  loaders are no-ops when no snapshot exists, so a fresh checkout builds
  identically with or without them.

Snapshots are committed rather than fetched at build time because `/roster` is a
*live* view of a team's roster. Without a committed snapshot, rebuilding an old
season's pages would ask CFBD what it thinks today.

Re-running any load is idempotent. A snapshot is the authority for the seasons it
covers, so its own rows are replaced wholesale: a player who left the roster
between fetches disappears from that season, and a season record CFBD has
corrected changes rather than accumulating.

## Portraits

No page may depend on a portrait. Headshot columns exist and stay empty until a
source with explicit redistribution rights is cleared; the fallback is a
team-coloured silhouette or initials. A stable image URL being technically
reachable is not redistribution permission, and no media-site headshot is
scraped into this repository.
