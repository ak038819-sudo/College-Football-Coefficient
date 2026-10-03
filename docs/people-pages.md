# Player and coach pages

Work toward durable, linkable pages for players and head coaches, following the
v0.1.1 implementation guide. This page records what exists, what it deliberately
refuses to do, and where the data runs out. It is updated as each phase lands.

## Status

| Phase | What it covers | State |
| --- | --- | --- |
| 1 — Identity and schema | Person tables, identity resolution, CFBD roster and coaching ingestion | Landed |
| 1 — Data synced | Rosters 2009-2026 (99,813 people, 271,131 player-seasons at schools in this database, 10,846 people at more than one school) and head coaches 1980-2026 (827 coaches, 5,657 tenures) | Landed |
| 2 — Player pages | Player export and detail pages, roster and box-score links, player search | Landed |
| 3 — Coach pages | Coach export and detail pages, team-page coach links | Landed |
| 4 — Stats integration | Season statistics on player pages; Stats tab names linked to people | Landed for 2025 |
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

### An id is a function of the source, not of the load order

"Immutable" is a promise about rebuilds, and the first version did not keep it.
`db/league.db` is not committed, so every deploy builds it from scratch, and an
autoincrement `player_id` therefore depended on the order the snapshots happened
to be read in: a database built 2026-first put `#player=16` on one person while
the pipeline's sorted 2009-first load put a different person there. A link
shared today would have pointed at someone else after the next deploy.

So the id now IS the source's identity:

- **Players**: the CFBD athlete id, exactly as the feed wrote it. All 350,670
  roster rows in the 2009-2026 archive carry one, and 29,162 of those ids are
  negative -- CFBD's own placeholder form, still one id per person -- so the
  sign is kept rather than folded onto an id that is already somebody's. The
  routes accept a negative id, and both the exporter and the page pick a shard
  with a *floored* modulo, because JavaScript's truncated `%` would send a
  negative id to a file that does not exist.
- **Coaches**: `person_identity.derived_id` of the `cfbd:<name>` key the loader
  already builds, because this feed has no coach id at all. The key is the same
  on every rebuild, so the id is too.
- **A person the source does not number**: derived from the context that
  identified them (name, team, season) in a range clear of every athlete id.
  Nothing in the archive takes this path today.

A database written before this holds people at load-ordered ids, and the loaders
would never notice -- they find a person by source id and reuse whatever id that
row already has -- so `migrate_player_ids` and the coach equivalent move them
before a snapshot is read. Verified against the real archive: the migrated
database and a from-scratch rebuild give identical id-to-person maps for all
99,813 players and 827 coaches.

`normalize_name` folds case, punctuation and generational suffixes, so
`Jerome Gaillard Jr.` and `Jerome Gaillard` share a lookup key, as do
`D.J. Uiagalelei`, `DJ Uiagalelei` and `D J Uiagalelei` — stripping the periods
leaves two tokens where the undotted spelling leaves one, so a run of single
characters is collapsed into a single token. A *lone* middle initial is left
alone (`John F Kennedy`), because gluing it to a real name would invent a token
no source ever wrote. That key is for lookup only. Two
`Mike Williams` normalize identically, which is exactly why step 2 also requires
team, season and position, and refuses a double match.

`identity_name` is the same folding with the generational suffix **kept**, and it
is used where the name *is* the identity rather than a hint beside a source id —
which is the coaching feed, and nothing else. Dropping a suffix is harmless when
an athlete id decides who someone is; it merges two people when the name is all
there is.

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
| CFBD `/coaches` | Head-coaching history and season records | **Head coaches only.** The feed carries no coordinator or assistant history, and nothing here may be presented as though it did. It carries **no coach identifier**, and its hire date belongs to the job rather than the person — see below. |
| Existing player box-score archive | Game logs and box scores, 2004 onward | `data/raw/player_boxscores/` stores **names only**: the archive's cleaning step drops CFBD's athlete ids. See the known gap below. |

### Coach identity, and what the feed cannot tell us

CFBD's coaching feed was not the shape this project first assumed. A full
1980-2026 pull settled it: **5,714 records carrying 5,716 seasons between them.**

- **Each record is one season**, not a career. `/coaches?year=N` returns that
  year's row for every coach who worked it.
- **`hire_date` belongs to the job, not the person.** Al Golden comes back with
  `2005-12-08` for his five Temple seasons and `2010-12-12` for his five at
  Miami.

So the name is the only *person-level* signal the feed carries, and the key is:

    cfbd:<normalized name, generational suffix kept>

The suffix is kept deliberately. Folding it merged Mike Sanford Sr. (UNLV,
2005-2009) with his son Mike Sanford Jr. (Western Kentucky and Colorado,
2017-2022) into one coach — a false statement that no later correction could
have detected, because the merged career looks perfectly ordinary. Dotted
initials are still folded, so `A.J.` and `AJ` are one key.

Grouping on name *and* hire date would split one person into one record per job
(Al Golden becomes two coaches); grouping on overlapping seasons would split
them into one record per season (Al Golden becomes ten). Both were tried before
the real data was in hand, and both were wrong.

A name is weak evidence by this project's own standard, so it is not dressed up
as anything stronger. `coach_external_ids.confidence` records it as
`name only`, and every career the feed cannot vouch for goes to
`person_unresolved` for review rather than being asserted:

| Flag | What it means | 2026 snapshot |
| --- | --- | --- |
| `name-only identity spanning N hire dates` | A coach who changed jobs, **or** two people sharing a name | 160 |
| `name-only identity with a gap after <year>` | A coach who returned years later, **or** two people sharing a name | 66 |
| `names differing only by a generational suffix` | A father and son, **or** one person a source spelled both ways | 1 |

The last flag is the inverse risk of keeping the suffix: a source that omits it
on some rows splits one person in two. Recording it makes that failure visible
where a silent merge would not be.

Both readings are genuinely possible, and neither is settled by guesswork. Two
coaches who truly share a name cannot be separated by this feed at all; saying
so plainly is better than inventing a distinction. A source with real coach ids
can be added beside this key later without a migration.

`hire_date` lives on `coach_tenures`, not `coaches`, because that is where it is
actually true: a single column on the person would have to pick one of a coach's
hire dates and be wrong about the rest.

The loader also migrates databases written by the first version of itself, which
keyed on `cfbd:<name>|<hire date>`. Those ids are rewritten to the current key,
keeping the lowest `coach_id` so an id survives wherever one can; where several
legacy rows collapse onto one key — a coach with two hire dates had one row per
job — the extras go, because that split is what this release undoes. Without the
migration the next run inserts a second coach per name and orphans the old row
under a supposedly immutable id. Reissuing a `coach_id` is acceptable only
because no page has published one yet; once coach URLs are live this must
migrate ids, never reissue them.

### The pages

| URL | What it shows |
| --- | --- |
| `#player=<player_id>` | Listed position, number, height, weight and hometown, and a row per season and team. A mid-year transfer has a row for each school. |
| `#coach=<coach_id>` | Record and win rate over the seasons this dataset covers, a season-by-season table with each poll finish and that job's hire date, and the identity caveat where the database recorded one. |
| `#team=<slug>&tab=roster` | That season's roster by jersey number, each name linking to its player page. The team header names the head coach. |

An id is the whole address. `navigation.js` accepts an integer and nothing else,
so `#player=Cade%20Klubnik` opens a page that says the id is unknown rather than
guessing at a person.

**No tabs on a person page, deliberately.** What is known about a person today —
who they are, and which teams and seasons they appear in — is one page's worth,
and per-game statistics are not attributed to people at all (below). Tabs would
be four addresses pointing at three empty panels. The `tab` parameter works for
every kind of page, so adding them with the stats layer will not change a URL
published before it.

### Where the data comes from

`src/export_people_pages.py` writes `ui/data/people/`, and
`src/export_static_data.py` records it under the static manifest's `people` key.
That call is guarded: a database built before the person tables exist produces a
manifest with no `people` key at all, which is how the UI tells "no data here"
from "no such feature".

| File | Loaded when | Why it is its own file |
| --- | --- | --- |
| `player_<n>.js` | a player page opens | 64 shards on `player_id % PLAYER_SHARDS`; one page loads one. The page reads the count from the manifest, so it can be raised as seasons accumulate: 16 shards over 2009-2026 made each file 3.0 MB, and 64 puts it near 750 KB |
| `index_<key>.js` | a name is typed in search | one shard per first letter of a name's words |
| `roster_<season>.js` | a Roster tab or a box score opens | carries the rows a roster table displays, so one team's roster does not depend on every player-detail shard |
| `coaches.js` | a coach page opens | every coach's whole career |
| `team_coaches.js` | a team page opens | season → team → coach id and name, so a line of text in a header does not cost 800 KB of careers |

A person with no season row is not exported. The roster that justified them is
gone, so the page would have nothing true to show, and an unresolved person must
not become an empty page.

### Searching for a person

People are not in the search index: there are 99,813 of them across 2009-2026. Search loads the shard for the letter
being typed and matches on word prefixes, the same way team suggestions work, so
a query may begin at any word of a name but never mid-word. A person is indexed
under the first letter of **each word** of their name, because sharding on the
whole name put Cade Klubnik in `c` alone and a search for "Klubnik" loaded shard
`k` and found nobody. Fewer than three characters is not a search; two letters
matches most of the country.

### An unusually long career is flagged, never corrected

Four hundred and forty-six of the 99,813 people carry a CFBD athlete id whose
roster rows span more than six seasons -- four years of eligibility, a redshirt
year and the free year the NCAA granted for 2020. The feed asserts this, so the
two readings are genuinely different and nothing in it says which applies:

- athlete 4571882 appears on thirteen roster rows from 2015 to 2024, at West
  Virginia, Kansas State and Baylor, every one reading "LB, #2, SR" -- an id
  reused, or a stale row repeated; and
- Cam McCormick really did play nine seasons, Oregon 2016-2022 then Miami, on
  injury waivers.

So `person_identity.flag_implausible_careers` keeps every row exactly as given
and records the person for review, and the player page says in plain words that
the career is longer than eligibility normally allows and the site is not
guessing. The flag is recomputed from scratch on each load, because a career is
only whole once the last season has been read, and it is keyed on `player_id`
rather than on the name: two players called John Smith must not share one
caveat.

## Statistics, and how they reach a person

A statistic attaches to a person by the source's athlete id or not at all.
There is no contextual fallback and there should not be one: a roster row with
no id can still be recognised by name AND team AND season AND position, because
a roster says who was PRESENT, but a statistic says what somebody DID, and a
wrong attachment puts one player's yards on another player's page.

`/stats/player/season` carries that id on every row, which is why phase 4 reads
it rather than the box-score archive. The archive covers 2004 onward but stores
only a name and a number, so it cannot identify anybody; this feed can, and the
237 MB re-fetch the archive would have needed is therefore not required for
season totals.

- `src/fetch_cfbd_player_season_stats.py` archives a season to
  `data/raw/player_season_stats/<year>.json`, in the feed's own long form (one
  row per person per category per stat type).
- `src/load_player_season_stats.py` loads it into `player_season_stats`,
  replacing the season's rows wholesale so a corrected statistic changes rather
  than accumulating. It runs AFTER the roster loaders, because the rosters are
  what put those people in the database.
- The exporter puts a player's own statistics in their detail payload, grouped
  by season and category, so a page renders them without re-reading a season.

Measured on the real 2025 snapshot: of 141,627 rows, 85,768 are stored for 8,819
people. 40,291 carry an athlete id with no roster row here and 15,568 belong to
a person here but name a school outside this FBS-only database -- a player who
has since moved to an FCS programme still appears in this feed. **Not one row
was lost for want of an identity**: every unknown athlete id in 2025 is at a
school this database does not carry. Both counts are written to
`person_unresolved` so that claim can be re-checked rather than believed.

Labels on the page are the feed's own -- `YDS`, `TD`, `PCT` -- because renaming
them would be this project asserting a reading of a statistic it did not
compute. `PCT` is the single exception and the page says so: the feed sends it
as a fraction between 0 and 1, and printing 0.686 under a header reading PCT
tells a reader two thirds of one percent.

### Linking the Stats tab to people

The Stats tab already had a sortable player table and leaders cards, built from
the box-score archive. Phase 4 did not add a second leaderboard beside them --
that would be two boards disagreeing. Instead the names in both now link to
person pages, through the SAME rule the box scores use: a name links only when
it matches exactly one player on that school's roster that season. One rule, so
the Stats page and a game page cannot give different answers about whose name it
is. On the 2025 Players table, 47 of the first 50 rows link; the three that do
not are names that match more than one person or none.

## Known gap: per-game statistics are not attributed to people

The player box-score archive identifies a player by **name only** — it carries no
athlete id — so no statistic is attached to a `player_id` anywhere on the site. A
player page says so instead of showing an empty Stats panel.

Box-score names on a game page do link to player pages, but only through the same
contextual rule the ingestion resolver uses: a name links when it matches
**exactly one** player on that school's roster for that season. Two players
sharing a name get no link, and neither does a school outside this dataset or a
season with no roster. On a 2026 game checked in the browser, 305 of 335 lines
linked; the 30 that did not were the team-total rows CFBD puts in every category
and players absent from the roster snapshot. The link is navigation, not
attribution: it says "this is probably the same person, go and look", and nothing
on either page claims the stat line as that person's record.

Closing this gap means re-fetching the archive with athlete ids, which is a
237 MB re-download and Austin's call.

## Known gap: a class year that is really a season

CFBD overloads the roster's `year` field. On the stub rows it returns for
players with no listed position, jersey or bio, it holds the **season** rather
than a class — 1,625 of the 31,382 rows in the 2026 snapshot carried `2026`
there. `class_year_label` accepts only 1-5 as a class and returns nothing for
any other number, so those players show no class year instead of a confident
`2026`. A non-numeric value (`Freshman`, `RS-FR`) is still kept as given.

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
