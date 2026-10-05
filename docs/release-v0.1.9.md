# v0.1.9 acceptance plan

Updated 2026-10-04 from the owner's complete release request. This is a release gate, not a declaration that v0.1.9 is finished. “Implemented” means present on this branch; publishing and visual review remain separate. Do not close this milestone while any required item is open.

## Interface and navigation

| Requirement | State and acceptance |
| --- | --- |
| Remove coach Hired column; FBS record/schools wording | Implemented. No hire column; Record says “vs FBS Opponents”, Schools says “FBS programs coached”. |
| Link every player name to the correct player; show profile stats | Source athlete IDs now survive national-stat aggregation; game leaders use the same identity rule. Existing profiles show season stats. Open: audit remaining live-only names and absent profiles. Never link a namesake merely to make text clickable. |
| Collegiate typography | Graduate display headings and Roboto Slab display text added with system fallbacks. Visual approval required. |
| Merge Games / Box Score G | Team tables expose one Games column; player Games means recorded category appearances. Coverage is still stated briefly. |
| Richer coach profiles | Record, schools, expected wins, over/under expectation, per-season and aggregate Elo change, and formulas implemented. Recruiting remains open below. |
| Smooth team cards | Integrated card surface and rounded corners; directory logo-chip border removed. Desktop/mobile visual approval required. |
| Larger stadium map | Wider map column, taller viewport and bigger markers. Visual approval required. |
| Standings dropdown | Implemented; existing bookmarked views still resolve. |
| Stats redesign and readable tables | Gridlines, alternating rows, hover, aligned player cells, horizontal leader panels and wider content implemented. Visual approval required. |
| Players Recorded G -> Games; remove long coverage paragraph | Implemented. |
| Remove Matchup Elo-difference panel | Implemented; win probabilities and venue controls retained. |
| Playoff sub-subtabs -> dropdown | Implemented. |
| Home filter reloads, crowded controls, extra filters | Draft changes are retained until Apply. Funnel disclosure, responsive side-by-side fields and separate explorer link implemented. Season/week/team/conference/status supported. Opponent and additional filter choices remain open. |
| Home scoreboard/finder separation | Added spacing and separate panel. Finder stays on Home for now; full explorer is available through its link. Final relocation is a product decision, not a blocker to filter usability. |
| Roster layout | Sortable roster table implemented; season dropdown already available. |
| Background logos and coach overflow | Scoreboard watermarks clipped per team; flexible panels can shrink and wrap. Visual approval required. |
| Game layout | Player box scores grouped into two team columns (one on mobile); efficiency and Elo panels precede narrative. Additional spacing added. Team statistics already use separate away/home columns. Visual approval required. |
| Standings movement everywhere | Open. Export prior-week ranks for each ranking family, display signed position change, mark new/unranked distinctly, and show unavailable when no previous comparable snapshot exists. Never substitute year-over-year movement. |
| Expanded team statistics | More offense/rating fields in default overall table; column picker supports all currently populated team fields. Broader defensive/special-team fields remain limited by available source categories. |
| Easier player/coach access | People primary destination searches players and head coaches with role selector. Coordinators are not included until sourced. |
| Expanded player tables | Gridded category tables with counting/rate fields and Games. Full stat glossary and additional verified categories remain open. |
| Team roster/stats/leaders tabs | Season roster plus new Stats and Leaders tabs implemented. |
| Multiyear graph ranges | Start/end seasons added alongside single-season and all-time selector. |
| Team all-time highs/lows Elo, CoE, rank | Season-snapshot high/low values and rank extrema implemented; explicitly labeled. Open: game-level Elo extrema and associated game links, and clarify historical coverage. |
| User-defined team tables | Session column selection implemented, composes with filters/sorting. Saved presets/export remain optional follow-up. |

## Data quality, delivery and coverage

| Requirement | State and acceptance |
| --- | --- |
| ALL TEAM IDENTITIES / issue #33 | Reviewed 139 supported programs, 762 provider identities, internal references, asset ownership, ingestion aliases and people team references. Machine audit fails on mismatches. See `team-identity-audit.md`. |
| Remaining CFBD retries | Already addressed on main by PR #50. Shared retry helper covers optional feeds and required game requests; preserve bounded retries and immediate auth-error failure. |
| Backfill automatically publishes | workflow_run completion now queues main rebuild for Backfill player box scores and Sync rosters and coaches, including partial successful source commits before a failure. No extra CFBD fetch is required for this event. Verify one real workflow completion after merge. |
| Update during deploy becomes visible | Same completion trigger queues a subsequent rebuild; build job serializes and fetches current main after acquiring the lock. Verify with overlapping source/deploy runs after merge. |
| 361 split-year coach tenures | Open, source-dependent. Need team/year/person game-date boundaries backed by school releases or box scores. Assign game credit only inside verified boundaries; leave unresolved seasons explicitly unmeasured. |
| Coach namesakes; 160 hire-date discrepancies | Open, source-dependent. Hire-date differences alone do not prove separate people. Add reviewed identity overrides with sources and redirect history before splitting records. |
| 446 player IDs spanning >6 seasons | Decision: do not automatically split, merge or delete. Legitimate extended careers exist. Retain source records with caveat; verify individual cases, use dated roster evidence, and maintain redirect/provenance when a correction is justified. Case adjudication remains open. |
| Lower-division boxscore bloat | Compression/sharding already landed in PR #49; scope pruning remains open. Prune by exact archived game ID against an explicit retention policy, with dry-run counts, reversible backup and no loss of displayed game stats. FBS–FCS retention must be decided alongside the rating expansion. |
| Alternate live feed | Research recorded in `live-data-options.md`; trial and licensing decisions remain open. |
| Awards/achievements | Open. Need attributable award name, season, awarding organization, source URL and exact athlete ID. No inference from similar names. |
| Coach recruiting | Open. Ingest class rating/rank, source-year cutoff and recruit hometown state; distinguish team recruiting during tenure from personal coach credit. Compare ranks only within the same season and population. |
| Coach Elo formulas | Implemented: final postgame minus first pregame Elo per measured season; career sum excludes offseason regression. Wins above expectation = wins + half ties − sum pregame probabilities for rated games. Elo-residual model is a separate experiment, not this wins metric. |
| Player portraits | Blocked on documented image licensing, attribution, allowed uses and retention policy. Do not scrape headshots as a substitute. |
| Severe FBS loss-to-FCS penalty | Open model/input change. Current game model is FBS-vs-FBS. Add verified FCS opponents/results first; compare calibrated upset penalties in historical walk-forward tests before selecting a production setting. |
| Elo across football levels | Research item. Needs connected cross-division fixtures, division priors, promotion/reclassification histories and honest uncertainty for weakly connected populations. |

## Elo design question

In a fixed zero-sum rating pool, an internal conference game transfers points but cannot change that conference's total. A nonconference game moves points between pools. Uneven schedules, membership changes, postseason games and offseason regression mean an actual conference is not permanently sealed. Ratings can concentrate among its winners without new evidence about the conference's overall level.

Do not silently add points to every winning conference: that creates inflation without new cross-conference information. Compare the current model against (1) recency-weighted opponent-adjusted strength and (2) a hierarchical conference prior updated only from out-of-conference evidence. Evaluate held-out nonconference/postseason log loss, Brier score, calibration, conference mean drift and rank stability. Report results before changing the production model. A separate loss-to-FCS multiplier must also be tested for scale drift and repeated upsets.

## v1.0 full release gates

- Replace school logo chips with an intentional logo treatment across directories, scoreboards, team pages and tables. Preserve licensed artwork and historical identity.
- Establish and document a coherent visual identity: college typography, spacing, color, surfaces, tables, iconography, responsive rules, accessible contrast and keyboard states. Review the complete desktop/mobile journey, not isolated screenshots.
- All required v0.1.9 gates above must be closed or explicitly re-scoped by the owner before the version is advertised as complete.
