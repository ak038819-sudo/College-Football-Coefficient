# Basketball integration: separate data, shared Elocast experience

## Current change (research only)

The pilot writes an `integration.json` beside its raw snapshots, containing a
versioned basketball schedule contract. It uses `ncaambb:cbbd:<kind>:<id>` keys,
explicit ending-year seasons and labels (2026 = 2025-26), timezone-aware timestamps,
and missing scores preserved as null. Unknown statuses and inconsistent finals
are rejected instead of silently mapped. Provider Elo is not imported.

The scope is always `team_sample`. Public-release, national-ranking and
player-impact readiness flags are always false. These are explicit metadata,
not yet a production publication gate: no production consumer exists. A successful
BYU pilot must not be presented as a national data feed or rating table.

This change does not edit the football schema, Elo config, build/deploy workflow,
dashboard, router, favorites, service worker or production exports. Existing
football URLs and its installed-app start URL remain unchanged.

## Recommended first integration, not implemented yet

Keep one repository, one domain and one installed app. Do not rewrite the football
engine into a generic multi-sport engine before the basketball data works.

| Boundary | Football stays | Basketball proposal |
| --- | --- | --- |
| Database | `db/league.db` | `db/ncaambb.db`, independent schema |
| Raw/derived files | Existing football paths | `data/ncaambb/raw/`, `data/ncaambb/processed/` |
| Model config | `config/model_config.json` | `config/ncaambb_model.json` |
| Published assets | Existing `ui/data/` files | `ui/data/ncaambb/` with versioned manifest |
| Page | `ui/dashboard.html` and existing hashes | `ui/basketball.html` with its own page state |
| Season | Existing football rules | Provider ending year; never calendar-year guessing |
| Refresh | Existing football workflows | Separate basketball fetch/build/validation job |
| Favorites/search | Existing football storage | Separate basketball storage or namespaced keys |

At the first UI release, a small sport selector links the two pages. Share visual
tokens and branding first; share rendering components only when their inputs have
explicit, tested contracts. Do not pass basketball records through football search,
conference/playoff rules, week selectors or person-ID lookup tables.

## Risks found in the current architecture

- Football navigation selects seasons from football configuration. A shared router
  without sport-specific state would cross-load seasons and teams.
- The site root currently redirects to football. Preserve it and existing deep links;
  add basketball as an opt-in destination before considering default-sport changes.
- The root service worker controls both pages. Its live-score network-first rule
  names football's file; basketball freshness and offline fallback need explicit tests.
  Versioned basketball assets must resolve to a consistent snapshot, not a mix of builds.
- Football publication currently stages broad `ui/data/`, `data/raw/` and
  `data/processed/` directories. Separate generators alone do NOT provide publication
  isolation. Before basketball writes published files, tighten the publisher's path
  ownership or use one serialized publication step with exact allowlists. Never have
  two independent jobs resolving generated-file conflicts by taking their own copy.
- Both sports use the same repository test suite. New research tests can block
  football deployment if merged broken (as the original import-path issue showed).
  Keep tests offline and validate both `pytest` and `python -m pytest` invocations.

## Release gates

1. Run the existing manual pilot on `main`, ending year 2026, with `CBBD_API_KEY`.
   Review access errors and artifact coverage; no successful collection assumed.
2. Reconcile one game's player minutes and paired lineups against box scores.
3. Build national ingestion independently: season membership, stable identities,
   request-cap handling, duplicate detection, and explicit non-D-I treatment.
4. Validate team Elo and export a national manifest with schema version, as-of time,
   model version and coverage. A production loader must reject team samples and
   unsupported schemas rather than show them as national rankings.
5. Add a private/unlinked basketball preview. Test football URL parity, basketball
   search/favorites isolation, season navigation, failed/missing feeds and mobile UI.
6. Validate publishing ownership, cache behavior and last-good snapshot rollback;
   then enable the sport selector. A basketball fetch failure must leave football
   refresh running and basketball's last valid snapshot visibly marked stale.
7. Player impact remains a separate experimental feature until paired-lineup
   coverage, uncertainty and held-out performance are established.

## Rollback

The initial research changes can be removed without touching production files.
For the later public release, disable the basketball link and retain the last good
basketball snapshot. Do not undo football commits, migrate its database, clear its
favorites, or reset the shared installed app to roll basketball back.
