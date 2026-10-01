# Physical stadium data (display only)

`data/stadiums_2026.tsv` seeds one primary venue relationship for each of the 138 canonical 2026 teams. The venue is a separate physical entity: `games.stadium_id` and `scheduled_games.stadium_id` do not determine the designated home side or the neutral-site flag. Stadium IDs remain stable when the seed changes a stadium's display name; previous names become aliases.

`python src/build_stadiums.py --db db/league.db` creates the additive tables and columns, seeds the 2026 relationships, resolves games, and writes `data/processed/stadium_resolution_report.json`. It is also called by `run_pipeline.py` after loading games and schedules. The report gives seeded/backfilled/unresolved counts, ambiguous names, and a sample of unresolved games. Running it again is safe; only newly resolved games count as backfilled.

Resolution is deliberately conservative, in this order:

1. Match a game's explicit upstream venue text to exactly one canonical stadium or alias.
2. Use a verified `game_venue_overrides` entry if there is one.
3. Infer from a season-valid primary `team_stadiums` relationship only for regular, nonneutral, unsuspicious true-home games with no upstream venue text.
4. Leave `stadium_id` null. A designated home team is never enough to infer a neutral-site stadium or to project 2026 venues into earlier seasons.

For a verified game-specific exception, add the stadium (with its own unique `stadium_key` if needed), then add `game_venue_overrides(game_id, stadium_id, notes)` with a reviewable source in `notes`. Historical venue relationships can be added to `team_stadiums` with the correct inclusive `start_season` and `end_season`; do not copy the 2026 relationship backward. When two physical stadiums have the same name, an explicit name alone is ambiguous and requires a game-specific override. Check the report after every data refresh.

The static games export carries the resolved ID and name as appended fields. Game cards and archived game pages display the name when known and omit it when unknown. Live CFBD views continue to show their explicit feed venue. None of the stadium tables or fields is read by Elo, HFA, CoE, standings, or playoff calculations.
