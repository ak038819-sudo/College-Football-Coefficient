# Physical stadium data (display only)

`data/stadiums_2026.tsv` seeds one primary venue relationship for each of the 138 canonical 2026 teams. The venue is a separate physical entity: `games.stadium_id` and `scheduled_games.stadium_id` do not determine the designated home side or the neutral-site flag. Stadium IDs remain stable when the seed changes a stadium's display name; previous names become aliases.

`python src/build_stadiums.py --db db/league.db` creates the additive tables and columns, seeds the 2026 relationships, resolves games, and writes `data/processed/stadium_resolution_report.json`. It is also called by `run_pipeline.py` after loading games and schedules. The report gives seeded/backfilled/unresolved counts, ambiguous names, and a sample of unresolved games. Running it again is safe; only newly resolved games count as backfilled.

When a `data/raw/venues.json` catalog is available, the builder imports CFBD venue IDs, locations, and capacities. Game-level `venue_id` takes precedence over name matching. A catalog request failure keeps the previous local snapshot. An older database gains a unique indexed `cfbd_venue_id` column during migration.

Resolution is deliberately conservative, in this order:

1. Match a game's CFBD venue ID to a catalog venue.
2. Match explicit upstream venue text to exactly one canonical stadium or alias.
3. Use a verified `game_venue_overrides` entry if there is one.
4. Infer from a season-valid primary `team_stadiums` relationship only for regular, nonneutral, unsuspicious true-home games with no upstream venue text or ID.
5. Leave `stadium_id` null. A designated home team is never enough to infer a neutral-site stadium or to project 2026 venues into earlier seasons.

For a verified game-specific exception, add the stadium (with its own unique `stadium_key` if needed), then add `game_venue_overrides(game_id, stadium_id, notes)` with a reviewable source in `notes`. Historical venue relationships can be added to `team_stadiums` with the correct inclusive `start_season` and `end_season`; do not copy the 2026 relationship backward. When two physical stadiums have the same name, an explicit name alone is ambiguous and requires a game-specific override. Check the report after every data refresh.

The static games export carries the resolved ID and name as appended fields. `ui/data/stadiums.js` contains searchable stadium pages with verified location/capacity when known, current primary FBS teams, up to five upcoming and five recent linked games, and a count of resolved completed games. The Stadiums section links those pages and plots venues that have coordinates. Archived game pages link the resolved venue and explain the flat Elo bonus; current team HFA evidence is labeled analysis only. Unknown venues have no stadium link. The archive count is not an all-time stadium record because many historical games lack a resolvable venue. Live CFBD views continue to show their explicit feed venue. None of the stadium tables or fields is read by Elo, HFA, CoE, standings, or playoff calculations.

Run the full pipeline and dashboard build against a populated league database to publish the catalog and pages. `python build_dashboard.py --reuse-exports` updates the local HTML shell but cannot produce stadium records from old exports. The branch currently has no fully built `db/league.db`, so the local preview shows the empty-data state until that build runs.
