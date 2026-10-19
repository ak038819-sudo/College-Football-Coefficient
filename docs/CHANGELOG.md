# Site release history

## Unreleased — Elocast on a home screen (2026-10-19)

- **The site is installable.** Add to Home Screen on iOS or Install on Android
  gives a chromeless Elocast with its own icon, and it opens on the scoreboard.
  A manifest, a service worker and three icons rendered from the existing mark;
  no other change to how anything is served.
- **It works without a connection.** The dashboard carries its ratings payload
  inline, so an offline launch shows the last ratings that device saw, with a
  line saying that is what it is, rather than a browser error. Versioned assets
  are served from cache and everything else is revalidated, which is only safe
  because every generated file this site loads carries `?v=sha256(file)[:10]`:
  the URL changes when the bytes do. `tests/service_worker.test.cjs` drives the
  real worker to hold that rule in place.
- **Follow a team.** A star on a team page; the teams you follow appear as *My
  teams* at the top of the home page with their rating, record and next game.
  Stored on the device in `localStorage` and nowhere else: no account, no
  server, nothing personal collected.
- **Since last week**, a new tab on a team page: what the week that just
  finished did to the rating and the rank, the games that did it, and where the
  team stands in the playoff field. Every movement figure is read off the Elo
  timeline the engine already writes; nothing is recomputed in the browser.
  Seed and odds are shown as they stand today, and the panel says so — no
  week-ago copy of either is kept, so there is no honest change to subtract.
  `docs/installable-app.md` has what closing that would take.

## Unreleased — Who won what (2026-10-18)

- **Stats → Awards** lists every national trophy of a season with its winner,
  his school and what the trophy is for, and a **player page now carries an
  Awards panel** naming the trophies that player won. Travis Hunter's 2024
  reads as the six-trophy sweep it was.
- 26 seasons, 2000 through 2025, 28 trophies and 666 winners. This is the first
  feed in the project that is not CollegeFootballData, which has no awards
  endpoint at all; the source is ESPN, fetched by hand into `data/raw/awards`
  and committed, because rebuilding 2014's pages must not depend on what ESPN
  serves today.
- A winner is a link only where this project identified the person. Before
  2009, where the roster archive begins, that is nobody: those 188 winners are
  names, and the page says the name. Since 2009 it is 372 of 394.
- Getting to those 372 meant fixing what the first attempt got wrong. Matching
  a winner to a roster row had required his position to match the string the
  roster used, and the two feeds disagree per player: ESPN called Manti Te'o a
  LB where the roster says ILB, Aaron Donald a DT where it says DL, Jahdae
  Barron a CB where it says DB. That threw away 86 winners who were in the
  archive all along, every one of them a defender or a lineman — the offensive
  skill positions resolved because those are the words the two feeds happen to
  spell the same way. Position is now a tie-breaker between candidates of one
  name and never a filter on the only one.
- A surname is tried when a full name finds nobody, because ESPN prints the
  name a player is announced by and the roster prints the name he enrolled
  under: "Olu Oluwatimi" won the 2022 Rimington, and that roster says Olusegun.
  Held to the same rule as every other match — exactly one candidate, or none.
- The 37 trophies ESPN serves are not all the same kind of thing, and each is
  now named as what it is. Four are won by coaches, and not one of those four
  has the word "coach" in its name, so the pattern that used to decide this
  matched none of them and reported every coach as a player the project had
  failed to find. Five are FCS, Division II or Division III player awards,
  whose winners are not in an FBS roster archive and never will be; three are
  weekly honours, a different grain from everything else here. All of them stay
  in the database — the archive does not pretend a trophy was not given — and
  the counts no longer read a question of scope as a defect.
- 22 winners since 2009 are still names. ESPN gave no school at all for 12 of
  them, named no winner for 5, and the rest are a roster gap or two players of
  one name whom nothing but a coin flip would separate.
- Display data only. An award is a vote taken after the season, so it is
  already an opinion about the season; feeding it back into a rating would be
  circular, and nothing in Elo, CoE, the playoff model or any prediction reads
  it.

## Unreleased — Team statistics with the schedule taken out (2026-10-17)

- **Stats → Adjusted** answers the question the raw table cannot: how would
  this team's offense have done against an average opponent, and how would an
  average offense have done against this defense. Every other team number on
  the site is a raw total, so a defense that faced four of the best offenses in
  the country reads as worse than one that faced four of the worst.
- The raw column stays beside the adjusted one, with each value's rank and how
  far that rank moved. The gap between the two *is* the schedule, and dropping
  the raw number would turn a correction into an assertion.
- What it shows in 2025. Akron's scoring defense is 89th raw and 126th once its
  opponents are accounted for; Buffalo's 60th becomes 95th; Fresno State's 16th
  becomes 45th. In the other direction Florida's 70th becomes 32nd, UCLA's
  119th becomes 76th and Syracuse's 129th becomes 103rd. On offense, Alabama is
  55th raw and 21st adjusted.
- Three statistics: points per game for every season the game archive covers
  (1980 onwards), and CFBD's EPA per play and success rate from 2001, weighted
  by plays. A season with too little of a statistic has no fit for it rather
  than one built on scraps, so 2002 — which CFBD covers for 31 games — shows
  points only, as every season before 2001 does.
- The model fits a league average, one offense and one defense per team and a
  single league-wide home-field term all at once, by ridge regression on every
  game. Home field comes out at 3.9 points a game in 2025, which is the first
  time this project has measured it on scoring rather than on Elo.
- How hard to shrink was measured, not assumed: holding out a fifth of each
  season's games across 2005–2026, the fit predicts them to within 9.32 points
  of mean absolute error against 11.29 for the league average alone and 9.54
  with no shrinkage at all. The curve is flat around its floor, so the shipped
  values are round numbers inside the flat region rather than a claim that one
  decimal beats another.
- Games against opponents this project does not rate are excluded from both
  columns, so the two numbers on a row always cover the same games — which is
  also why these raw numbers can differ from the Teams table, which counts
  every opponent. Nothing here feeds Elo, CoE, the playoff model or any
  prediction.

## Unreleased — Conference strength over time (2026-10-17)

- **Standings → Conference History** charts every conference's average member
  Elo season by season, 1980–2026. The existing Conference Elo view answers
  "who is strong now"; this one answers "who was strong when", which is the
  question realignment makes interesting.
- The chosen conference is drawn in full, with every other conference of the
  same era behind it in grey. A conference rating only means something against
  the rest of the sport, so the chart draws the rest of the sport; the y-axis
  covers all of them and a test holds every point inside the plot.
- Underneath, one row per conference: how many seasons it lasted, its best and
  worst seasons with the years they fell in, and where it stands now. A
  conference that no longer exists has no "now" and sorts as absent rather
  than as the weakest in the sport.
- What it shows. The Pac-12 was 1,619 and second in the sport in 2015 and is
  1,460 and fifth now. The Pac-10 peaked at 1,673 in 1997, first. The Big 8's
  last season, 1995, was its best — 1,647 and first — immediately before it
  became the Big 12. The Big East was first in 2002 and sixth when it ended in
  2012. The SEC has been first in the most recent season and peaked at 1,677
  in 2014.
- No new data and no new export: every number comes from the
  `conference_elo_by_year` payload the one-season view already reads, which
  uses each season's own membership rather than today's. A rename appears as
  two rows (Pac-10 and Pac-12 are separate) because the source names them
  separately and the renames came with real changes of membership.

## Unreleased — Schedule strength, played and remaining (2026-10-16)

- **Standings → Schedule Strength** puts every team's schedule difficulty in
  one sortable table: the average Elo of the opponents already played, the
  average of the ones still to come, and the two pooled, each ranked within
  the season. Conferences get the same three numbers underneath.
- The forward-looking half is new. Strength of schedule existed only as one
  number on a team page, and only for games already played — so a team whose
  hard games were all still to come looked identical to one that had already
  played them. In 2026 the gap is stark: Auburn has faced the 39th-toughest
  schedule and has the 2nd-toughest left.
- **Played** takes each opponent at the rating it held entering that game,
  which is exactly the number a team page already shows — two pages
  disagreeing about one team's schedule would be worse than neither having it.
  **Remaining** takes opponents at their current rating, because a game nobody
  has played has no pregame rating.
- An opponent with no rating contributes nothing to a remaining average rather
  than being counted at the engine's 1500 prior, which would make a soft run
  look average when it is the opposite.
- **Games still to come against an FCS opponent are now counted.** They were
  not in the data at all: `fetch_cfbd_games.py` dropped an unplayed
  FBS-vs-FCS game rather than write it to the schedule file, which feeds
  `predict_upcoming.py` and has no rating to price such a game with. So every
  count of a team's remaining games was too low. Those fixtures are now
  fetched to `data/raw/fcs_schedule_<year>.csv` and loaded into their own
  `scheduled_non_fbs_games` table — counted, never predicted. The page says
  how many of the games left it cannot price, and marks them on each team's
  row; nothing feeds a projection or a win probability.
- The average still excludes them, and that is the point: 1500 is the Elo
  engine's prior for a team it has never seen, not an estimate of an FCS
  program, so averaging it in would make the softest run in the sport look
  ordinary.
- "Current" means the rating a team would carry into its next game, the whole
  chain replayed (`predict_upcoming.current_ratings`), not the rating it took
  into the postseason. The latter is frozen at the end of the regular season
  and does not exist at all for a team yet to play this year, so using it
  would leave a preseason table with no remaining column and price a team in
  a bowl at its November rating.
- Ranks are within a season, deliberately: Elo is a within-season chain, so a
  1,650 average opponent in 2026 and in 1994 are not the same claim.
- One file per season (`ui/data/schedule_strength/<season>.js`), loaded when
  the view opens. The whole history is 1.5 MB and the manifest is fetched on
  every page view.

## Unreleased — The ratings against the market (2026-10-16)

- The Track Record page now answers the question the baselines never could:
  how the ratings do against the **closing betting line**, game by game.
  Average margin miss for both sides, the model's record against the spread,
  who called the winner on the games where the two disagreed, and both Brier
  scores on one scale.
- **The market wins, and the page says so.** Over 10,151 games, 2013-2026, the
  ratings miss a margin by 13.32 points on average and the closing line by
  12.35; Brier 0.1846 against 0.1712. Read as a pick the ratings go
  4925–5015–211 against the spread (49.5%, where 52.4% is break-even), and on
  the 1,408 games where the two disagreed about the winner outright the
  ratings were right 41.4% of the time. That last number is the honest one:
  when this model disagrees with the market, the market is usually right.
- Closing lines are archived from CFBD (`game_lines`, one row per game per
  provider), 2013-2026 — every FBS game of every one of those seasons has a
  line. Nothing reads them as a rating input and nothing will — a benchmark
  the model can see is not a benchmark.
- Only **closing** lines are scored. CFBD serves the current number before
  kickoff and the closing one after, in the same column, so a mid-season file
  carries both; the measurement requires the snapshot's own record of the
  result, which a pre-kickoff capture cannot have. The current season's lines
  are also re-fetched on every deploy, beside the games.
- The page is honest about what the comparison is not: a closing line is not
  available when a bet would be placed, only priced games count (the season
  table says what share of each season that was), and break-even at the usual
  price is 52.4%, not 50%.
- See `docs/model-vs-market.md`, including the sign convention — CFBD writes a
  home favourite as −7 and every margin here runs the other way.

## Unreleased — Live win probability (2026-10-16)

- A game being played now shows a **live win chance** on each team, from the
  score, the clock and the pregame ratings, in place of a pregame number that
  stopped being true at kickoff. The pregame chance moves into the tooltip.
- The model behind it (`src/win_probability.py`) is fitted against every
  completed game since 1980: a point of margin is worth about 19 Elo points,
  and a game lands about 16.7 points either side of what the ratings implied.
  How fast the remaining uncertainty narrows is measured too — see below.
- Drive-by-drive data is now fetched and loaded (`game_drives`): 441,307
  drives, 2001-2026. CFBD's coverage is thin before 2005 (2002 has 31 games)
  and effectively complete from 2013.
- **A finished game page now tells the story of the game**: a scoring summary
  of every drive the scoreboard moved across, and the full drive list behind
  it. Points are credited by the score rather than by who had the ball, so a
  pick six counts for the team that scored it. A game CFBD has no drives for
  shows no section at all rather than an empty one.
- The last assumed parameter in the live model is now **measured**:
  `time_exponent` is 0.4395, fitted on 440,918 in-game states from the drive
  archive, in place of the 0.5 a plain random walk gives. Late-game margins
  swing more per second than early-game ones, and the live number now says so:
  a 400-Elo favourite trailing by 3 with a minute left reads 19% rather than
  14%. Held out on 2018-2026 states it scores 0.39929 against 0.40044.

## Unreleased — The real 12-team playoff (2026-10-10)

- The playoff is now the real College Football Playoff format: 12 teams, the
  5 highest-ranked conference champions on automatic bids, 7 at-large, seeds
  straight down the ranking, seeds 1-4 sitting out a first round played at
  the higher seed's home. The invented 24-team bracket, its pot draw and the
  16-team NIT are retained in the repository but no longer live.
- 5-year rolling team CoE is the ranking the format needs, in place of a
  selection committee. The Field page shows the automatic-bid cut with every
  conference champion's rating beside it.
- Title odds now describe the bracket on the page. The 24-team bracket's
  random draw meant they had to be averaged over draws that had not
  happened.
- Conference CoE no longer allocates playoff bids. The conference page's
  "Playoff bid rank" tile is now a conference strength rank, and the
  methodology page says what the ranking is and what it is not.

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
