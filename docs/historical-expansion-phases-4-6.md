# Elocast Historical Expansion — Phases 4–6

**Status:** Planning specification only. No historical imports, model rebuilds, schema migrations, or production deployment are authorized by this document.

**Program tracker:** #194  
**Predecessors:** Phase 1 baseline and coverage audit (#195, merged); Phase 2 isolated, resumable archive ingestion (#196); Phase 3 1946–1979 backfill readiness and integrity gates (#198, merged).

## Shared non-negotiable requirements

1. Keep original source records, names, scores, ties, dates, uncertainties, and provenance intact. Never rewrite raw source records to fit modern conventions.
2. Historical completeness, identity resolution, and model eligibility are separate questions. A source record may be preserved even when it is excluded from Elo or Merits.
3. Every transformation must be reproducible, reviewable, and idempotent. Report unresolved records and coverage gaps instead of silently dropping or inferring them.
4. Do not modify the production database or recompute production ratings without an approved migration plan, verified pre-change fingerprints, and before/after regression results.
5. Each phase has independent acceptance gates and a separate PR. CI passing is necessary but does not by itself establish historical accuracy.

---

## Phase 4 — Early College Football Archive (1869–1945)

### Objective

Extend Elocast's historical archive back to the earliest college football seasons, targeting **1869–1945**, without pretending early football followed modern scheduling, school identity, conference, or division rules.

### Scope and implementation

- **Source discovery and coverage matrix:** Determine which seasons and games are available from CFBD and which require independently licensed or otherwise usable alternative sources. Document provenance, usage rights, and season-level source coverage. An empty API response is not evidence that no games occurred.
- **Era-aware source ingestion:** Reuse Phase 2's staging architecture where applicable; add explicit source adapters for other datasets. Maintain source-specific IDs, original text, ingestion timestamps, payload fingerprints, and source URLs/attributions. Distinguish true duplicates from separate source reports of the same game.
- **Historical identity registry:** Track historical school/program names, discontinued teams, institutional changes, and disputed aliases. Preserve raw names alongside reviewed canonical identifiers; provide an unresolved-identity queue.
- **Historical semantics:** Preserve ties, missing or disputed results, neutral-site uncertainty, incomplete schedules, and varying classification conventions. Avoid applying modern FBS or 11-game/8-conference-game assumptions to the archive.
- **Cross-source reconciliation:** Compare season totals and game-level results against independent references where available. Explicitly report conflicts, uncertain dates, missing scores, and duplicated contests.
- **Staged batch processing:** Divide the period into manageable batches (suggested 1869–1899, 1900–1919, 1920–1939, 1940–1945), with resumable checkpoints, quality summaries, and human review at each gate. Batch boundaries may be revised based on actual source availability.

### Deliverables

- Source inventory and documented coverage by season, including known gaps.
- Staging imports for the years actually supported by validated sources.
- Historical team-identity and source-reconciliation review tables.
- Automated validation tests, batch reports, and unresolved-record inventory.
- Explicit recommendation on which records are suitable for downstream modeling.

### Acceptance criteria

- Every staged record has traceable provenance and a reproducible import path.
- Season totals and material source conflicts are reported; no unreviewed gaps are presented as complete coverage.
- Identity ambiguities and unknown classifications remain visible and are not silently coerced.
- No production Elo/Merits changes occur in Phase 4.

### Dependencies

Phase 2's importer and Phase 3's validation conventions should be stable. Phase 1 baseline verification remains mandatory before any later production promotion.

---

## Phase 5 — Historical Elo and Era-Aware Merits

### Objective

Research and validate historically meaningful ratings while protecting the current-season Elo and Merits behavior already used by Elocast.

### Scope and implementation

- **Eligibility framework:** Define season-specific analytical populations and confidence levels. Do not equate historical major-college status with modern FBS. Record why each program/game is included, excluded, or unresolved.
- **Historical Elo experiments:** Evaluate initial ratings, new-program entry, offseason regression, K-factor sensitivity, tie handling, missing-score policy, sparse schedules, home-field uncertainty, and era transitions. Test multiple candidate approaches before selecting one.
- **Merits adaptation:** Define era-appropriate conference membership, nonconference scheduling, opponent strength, and postseason treatment. Rules requiring modern conference structures or playoff formats must not be retroactively assumed for early seasons.
- **Reproducibility and leakage control:** Use deterministic season ordering, frozen source snapshots, configuration versioning, and documented historical data cutoffs. Separate descriptive ratings from predictive calibration.
- **Validation:** Compare candidate ratings against held-out game outcomes where feasible, inspect stability and sensitivity by era, audit ranking anomalies, and evaluate how missing data biases results. Include explicit tie and schedule-completeness tests.
- **Production compatibility:** Capture current Elo/Merits fingerprints and representative rankings before any integration. Use side-by-side reports and enforce a no-unapproved-change gate for modern production seasons.
- **Transparency:** Publish methodology, caveats, era eligibility, and confidence/coverage metadata with any proposed historical ratings.

### Deliverables

- Versioned historical Elo and Merits research specification.
- Candidate model configurations and reproducible experiment scripts.
- Evaluation report by era, including failure cases and uncertainty.
- Historical eligibility mappings and documented unresolved cases.
- Before/after comparison against frozen modern production baselines.
- A separate implementation PR only after a model choice is explicitly approved.

### Acceptance criteria

- Model behavior is empirically reviewed, reproducible, and transparent about incomplete data.
- No historical game receives fabricated scores, classification, conference affiliation, or venue.
- Historical and current-season ratings are clearly distinguishable where their methodologies differ.
- Current production Elo and Merits remain unchanged unless separately approved with regression evidence.

### Dependencies

Reviewed Phase 3/4 source coverage and identities, plus a completed Phase 1 production baseline audit.

---

## Phase 6 — Publish Historical Games and Coverage in Elocast

### Objective

Make the verified historical archive usable on the Elocast website, with transparent data coverage and appropriate separation between recorded history and model-derived rankings.

### Scope and implementation

- **Historical navigation:** Add season and game discovery across supported years, including early seasons, with clear era labels. Preserve the existing Home · Stats · Standings · Teams · People navigation and modern game experiences, and keep historical browsing inside the football league route rather than altering the league selector.
- **Game detail:** Show original participants, date, result, tie status, source attribution, and confidence/uncertainty flags. Missing statistics must display as unavailable, not zero.
- **Team history:** Show historical program aliases and seasons where identity is verified; distinguish discontinued or renamed programs without silently merging unrelated teams.
- **Coverage dashboard:** Display per-season counts, known missing data, unresolved identities, source provenance, and whether a season is archive-only or analytically eligible.
- **Historical rankings:** Show historical Elo/Merits only for seasons and teams explicitly approved in Phase 5. Label model version, coverage limitations, and methodological differences.
- **Performance and reliability:** Use paginated/filtered historical queries, caching or precomputed static assets as appropriate, accessible tables, and tests for very old years and sparse seasons.
- **Regression safety:** Ensure modern live scores, current-season standings, playoff simulation, player stats, and mobile navigation continue to function.
- **Release strategy:** Use feature flags or staged publication: verified archive browsing first, then historical ratings when approved. Avoid exposing unreviewed data as authoritative.

### Deliverables

- Historical season/game/team browsing UI and associated data interfaces.
- Public coverage and provenance indicators.
- Accessible empty/unknown states and clear historical caveats.
- End-to-end, performance, and mobile regression tests.
- Rollout and rollback plan with release notes.

### Acceptance criteria

- Visitors can browse approved historical records and understand what is known, unknown, and model-derived.
- No fabricated statistics or unreviewed rankings are displayed.
- Existing modern Elocast features pass regression checks.
- Publication is reversible and gated by documented data-quality signoff.

### Dependencies

Phase 4 source validation for early-era records; Phase 5 approval before historical ratings publication; Phase 1 production integrity baseline for any production data migration.

---

## Proposed release sequence

1. Finish Phase 2 pilot and Phase 3 source reconciliation independently.
2. Build and review Phase 4 archive coverage, with unresolved seasons clearly marked.
3. Research Phase 5 models on frozen, reviewed datasets; obtain explicit approval before production changes.
4. Deliver Phase 6 archive browsing separately from ratings; release ratings only after Phase 5 approval.

**Definition of program completion:** Elocast offers historically traceable game coverage across supported eras, publishes only approved ratings, makes missingness and uncertainty visible, and preserves verified modern behavior.
