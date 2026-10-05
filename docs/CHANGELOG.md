# Site release history

## v0.1.1 — People, teams and statistics (2026-10-05)

- Release of the implemented People search, team Stats/Leaders tabs, sortable
  season rosters, chart date ranges and configurable national team tables.
- Collegiate typography, table gridlines, unified team cards, larger stadium
  maps, simplified Standings/Playoff menus, and game-panel spacing.
- Home discovery applies all filters together inside a funnel disclosure.
- All 139 supported team identities audited; exact provider mappings prevent
  similarly named schools from inheriting another team's ratings or links.
- Coach Elo changes and expectation formulas; no Hired column.
- Completed backfills and roster syncs automatically queue a site rebuild.
- People search/role survive shared URLs and return navigation; stale team-stat
  requests cannot overwrite a newer season selection.

This releases the current implementation as v0.1.1. Remaining requirements in
`release-v0.1.9.md` retain their v0.1.9 deadline; portraits remain gated by image
licensing. No production Elo formula changes are included in this release.


- `docs/identity-problems.md`: every known limit of CFBD's identity data, what
  each one costs a reader, what the code does about it and what is still an open
  decision, measured against the live database rather than estimated.
- Box-score lines for six teams now link. The archive carries CFBD's team names
  and a page keys on this database's, so UL Monroe, San José State, App State,
  Florida Atlantic, Florida International and plain Miami never matched and not
  one of their player lines linked to anybody, in any season. The six were
  already `team_aliases` rows; the export now reads them.
- A CFBD request that never got an answer is retried with an exponential
  backoff instead of ending the run. A reset killed the deploy's own fetch
  twice on 2026-10-03, so nothing published either time, and killed a
  23-season box-score backfill after one season. A request the API *answered*
  is not retried: 401 and 404 mean something, and repeating them only waits.
- Season statistics on player pages, from CFBD's season player feed, which
  numbers every row with an athlete id: a statistic is attached to a person
  because the source says so, never because two names matched. **2009 to 2025
  are loaded: 1,048,998 statistics for 45,805 people.** Snapshots are gzipped,
  which is what makes keeping the feed's own long form affordable -- 17 seasons
  are 11 MB in a checkout rather than about 400 MB.
- `defensive` and `fumbles` statistics begin in 2016; before that CFBD's feed
  carries eight categories, so a defensive player's page for 2009-2015 shows no
  statistics.
- A season CFBD splits across two athlete ids under one name at one school is
  marked on the page and the reason given, rather than printed as if whole: 172
  player-seasons are affected, and one of them read 17 carries for 41 yards
  where the source also held 23 carries, 101 yards and 3 touchdowns under its
  other id. The ids are not merged, because the feed cannot distinguish one
  person recorded twice from two players who share a name.
- Player names in the Stats tab's table and leaders cards link to person pages,
  by the same rule the box scores use. No second leaderboard was added: the
  Stats tab already has one, and two boards would disagree.

- Player and head-coach identity: person tables keyed on immutable ids, with
  source identifiers and name aliases stored separately so a display name can
  change without breaking a link. A transfer stays one person across schools.
- CFBD roster snapshots (2009 onward) and head-coaching history, fetched in
  Actions and committed to `data/raw`, loaded idempotently by the pipeline.
- Rows that cannot be confidently attached to a person or a team are recorded
  for review instead of guessed at; no person is created from a name match.
- Coach identity keyed on the name, which is the only person-level signal CFBD's
  coaching feed carries: each record is one season and its hire date belongs to
  the job, so the hire date lives on the tenure. The name keeps its generational
  suffix, so a father and son who both coached stay two people. Careers the feed
  cannot vouch for (a second hire date, a gap in seasons, or two names differing
  only by a suffix) are flagged for review rather than asserted.
- A roster class year is only accepted as 1-5. CFBD's stub rows put the season
  in that field, which displayed as a class year of "2026".
- Player and coach pages: `#player=<id>` and `#coach=<id>`. A player page gives
  their listed bio and a row per season and team; a coach page gives their
  record, a season-by-season table and, where the database flagged one, the
  identity caveat in plain words.
- A Roster tab on every team page, and the head coach named in the team header.
- Players and coaches in the header search, by first name or surname.
- Rosters backfilled to 2009: 99,813 people and 271,131 player-seasons, of whom
  10,846 played at more than one school. Player detail is sharded 64 ways so one
  page downloads about 750 KB rather than the 3.0 MB that 16 shards had become.
- A person's id is the source's own identity, so a `#player=` or `#coach=` link
  survives a rebuild: players carry their CFBD athlete id and coaches an id
  derived from their name key. The autoincrement ids they replace depended on
  the order the snapshots were read, and `db/league.db` is rebuilt from scratch
  on every deploy, so a shared link would have moved to a different person.
- An athlete id whose roster rows span more than six seasons is flagged for
  review and said so on the page, not corrected: 446 of them do, and some are
  real careers on NCAA injury waivers while others are the feed repeating a
  stale row. Every season the source gave is still shown.
- Box-score names on a game page link to player pages where the name matches
  exactly one player on that school's roster for that season. The archive
  carries no player id, so a shared name is left unlinked rather than guessed
  at, and no statistic is attributed to a person anywhere.
- No rating engine reads any of it. See
  [player and coach pages](people-pages.md) for coverage limits and known gaps.

## v0.1 — People and Places of the Game (in progress, local branch)

- Cross-season Find a Game page.
- Team home-field advantage table under Standings.
- Opt-in dynamic home-field Elo replay and comparison; production Elo remains unchanged.
- Canonical stadiums and season-specific team relationships, with conservative game venue resolution and stadium names on game cards and details.
- Stadium Explorer and verified venue links on game pages.
- Pregame, in-season team HFA comparison against flat Elo, using earlier games only. Production activation awaits held-out validation.
- Hourly finalization checks during regular-season game windows, with retries for late efficiency and player feeds.
- Player box-score archive and historical game-page display where CFBD supplies lines; older years require a backfill.

## v0.0 — Alpha (currently published)

- The published baseline before the People and Places of the Game update. This label describes the current live site; the new label appears on the site when v0.1 is published.

## Historical development notes

The earlier `v0.1-foundation` tag and the notes below refer to a development milestone, not the site release named v0.1 above.

### Data & Playoff Foundation
## Added:
- FBS-only filtering in fetch_cfbd_games.py
- game_phase classification (regular / bowl / cfp)
- Alias resolution system:
- Team membership ingestion by season
- 12-team playoff builder
- Playoff detection fallback logic for 2015/2016

## Fixed:
- CFP misclassification for early seasons
- SQLite heredoc command usage issues
- Schema mismatch for playoff_field_by_year

## Known Constraints:
- load_games.py requires game_phase column (intentional strict mode)
- Alias system maps alias → canonical team_name
