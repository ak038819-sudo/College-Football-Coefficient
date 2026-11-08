# Step 4 — Bowling Merits preview integration

This is a **research-only, read-only preview data contract**. It does not change
the Elocast production UI, existing Merits, or the canonical database.

## Produce preview data

After an explicitly authorized shadow rebuild with a vetted canonical DB:

```bash
python analysis/bowling_merits/pipeline.py --source-db db/league.db --shadow-db db/bowling_merits_shadow.db --season 2025 --enable-shadow-writes
python analysis/bowling_merits/preview.py --shadow-db db/bowling_merits_shadow.db --season 2025 --limit 25 --output /tmp/bowling_merits_preview.json
```

The preview fails closed when the shadow DB or requested season is missing.
The payload contains ordered standings with per-game award breakdowns,
opponent quality, maturity flags and the explicit `research_preview` status.

## UI proposal (not yet implemented)

Add a clearly labeled **Bowling Merits (Research Preview)** view under
Standings, behind a disabled-by-default UI flag. Display rank, team,
total Merits, games, wins, regulation losses, overtime losses, and a
provisional badge. Expanding a team shows each game, opponent, Q, award,
and whether the award can still change. Do not replace existing Merits.

## Outstanding release gates

- Reconcile canonical season-specific FBS eligibility (deliberately deferred).
- Run a vetted full-database rebuild and compare coverage, totals, and
  provisional awards against independent evidence.
- Confirm frontend data transport and feature flag in the actual Elocast UI.
- Obtain explicit authorization before merge, production deployment or
  promotion of Bowling Merits.
