# Navigation and Stats renovation

The dashboard keeps its static exports and hash routing. Primary navigation is Home, Stats, Standings and Teams. No model, ingestion, live-state transition, identity or playoff-selection code changes in this milestone.

## Routes and compatibility

| Previous hash | Canonical destination |
| --- | --- |
| `#section=games` | `#section=home&view=games` |
| `#section=playoff&view=bracket` | `#section=stats&view=playoff&tool=bracket` |
| `#section=matchups` | `#section=stats&view=matchup` |
| `#section=stadiums` | `#section=teams&view=stadiums` |

Filters, selected games, venues and playoff subviews survive canonicalization. The existing feature controllers remain internal compatibility adapters. Path-style routes such as `/games` were not part of this static app and are not introduced. Data coverage and methodology remain secondary links; persistent fuzzy search is preserved.

## Home

Home includes the authoritative current scoreboard, importance ordering, live/upcoming/final groups, previous/next week links and season/week/team/conference/status discovery. A season plus a team finds historical games without knowing their week. Discovery results paginate in batches of 30. Historical all-season/head-to-head finder bookmarks still work through the relocated Games controller. A selected historical week does not replace the automatic default-week logic.

## Statistics

Stats has Overview, Teams, Players, Matchup and Playoff subnavigation. National leader cards deep-link into the matching filtered and sorted table. The existing analytical models are reused.

`ui/stats.js` owns counting-stat aggregation, numeric/string/percentage comparison, null-last ordering, filtering, and the adapter for existing tables. The Stats renderer uses this same comparator in a reusable `SortableTable` with 50-row pagination, accessible sort buttons, horizontal scrolling and a sticky identifying column. Existing tables keep their original order until the reader sorts them.

Season, category, filters, minimum, sort, direction and page live in the hash URL. Team links include a safe return URL to restore the statistical view. Player names expose a scoped identity hook for future profiles; the provider archive has names rather than stable player IDs, so this milestone does not invent IDs, positions or profile routes.

### Data boundaries

- Scores and records cover completed FBS-vs-FBS games in the season export, including postseason. They are not NCAA all-opponent totals.
- Player box scores are aggregated only when they match a completed archived game; absent categories and values remain unavailable.
- Player `Recorded G` counts games with entries in that player/category, not certified season participation.
- Rates are recomputed from summed numerators and denominators. Game averages and percentages are never added together.
- Team box-score totals use the recorded box scores. `Box score G` gives team coverage; per-recorded-game yardage uses that denominator.
- Completion-percentage overview leaders require 30 recorded attempts; the table preserves that threshold in its linked URL.
- Elo movement is the most recent exported game change, not a season-total gain/loss.
- Advanced metrics requiring unavailable denominators, player position filters, and FCS games are not fabricated.

## Geography

The stadium explorer uses a single Mercator projection with identical WGS84 coordinate transforms for boundaries and markers. Hawaii stays at its actual longitude and latitude in the same SVG as the mainland. Alaska can be included when northern stadium coordinates require it. Selecting a stadium keeps the map visible, highlights/focuses its marker and opens database-backed details with links to its home teams. Team pages use the existing verified stadium relationships.

State geometry comes from Leaflet's choropleth example, retrieved October 3, 2026:
https://github.com/Leaflet/Leaflet/blob/main/docs/examples/choropleth/us-states.js
The BSD notice is preserved in `docs/leaflet-geography-license.txt`. `ui/data/us_states.geojson` retains the state geometry; `ui/geography.js` packages that same geometry for the static browser. Puerto Rico is outside this milestone's US-state view. Alaska is omitted unless relevant, without relocating it.

## Validation

```sh
npm ci --ignore-scripts
npm run check
npm test
python -m pytest -q
python build_dashboard.py --reuse-exports
```

The build uses committed exports and does not recalculate Elo, coefficients or HFA. CI installs the test-only DOM dependency and runs the UI checks. Interaction tests load the actual generated dashboard and local assets without an external server. They cover navigation, archived finals, week browsing, combined filters, categories, conference and season filters, sorting, return state, matchup, playoff simulation, and bidirectional stadium navigation. Pure tests cover aggregation, percentages, nulls and geographic coordinates.

Local result: 88 JavaScript passes; 295 Python passes and 229 database-dependent skips. The existing CI database bootstrap provides the model data for those tests. JavaScript syntax checks, Python compilation, generated dashboard build and whitespace checks pass. Live visual browser preview is blocked in the execution environment; DOM smoke checks pass, but desktop/mobile visual review remains a release check.
