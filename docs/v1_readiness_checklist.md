# v1 readiness — audited 2026-10-10

This file used to be an all-unchecked checklist from an earlier architecture.
Read literally it said nothing was done, which was false, and three of its
sections described software this project no longer contains:

- It gated on `build_all.py`. There is no such file; `run_pipeline.py` is the
  entry point.
- Its UI section described a Streamlit app on `localhost:8501`. The UI is a
  static dashboard built by `./build_dashboard.py` and published to GitHub
  Pages.
- It assumed the playoff field lived in database tables. No playoff table
  exists; the field is computed by `src/select_playoff_field_v2.py` and
  written into the static exports.

So it is replaced by an audit of the same concerns, each line either verified
with its evidence or named as open. **The live release gate is
[`release-v0.1.9.md`](release-v0.1.9.md)**, which tracks the owner's current
requirements line by line. This page is the longer-horizon v1 view, and does
not supersede it.

Every "verified" below was checked against the repository on 2026-10-10, not
carried over from the old file.

## 1. System rule enforcement

| Item | State |
| --- | --- |
| 24 teams selected every year | **Verified.** `tests/test_playoff_field.py::test_total_field_is_24` drives the real selector over all twelve seasons 2014–2025. |
| Exactly 8 byes | **Verified**, same file, same seasons. It is a regression test for a real bug that once produced 7. |
| Pot structure stable | **Verified.** Pot 1 and Pot 2 are each exactly 8, so a 1-to-1 draw pairing is always possible. This too guards a real bug (7 vs 9). |
| Conference champions auto-bid | **Verified** through `YEAR1_BIDS`/`YEAR2_BIDS` and `select_qualifiers`. |
| Independents never displace a champion | **Verified.** Two tests, one on real data for every season and one on a constructed independent made far stronger than the champion it would displace. |
| No independents in the database | **Superseded, by design.** Independents are a real part of the sport — 2026 has Notre Dame and UConn — and the model admits them through an explicit strength threshold rather than excluding them. The invariant that matters is the one above: a threshold may never cost a champion its bid. |
| All teams assigned to conferences | **Verified for 2026** as of today: 136 of 136 FBS programs have a membership row, each conference at its real size. Six were silently missing until the CFBD name-resolution fix of 2026-10-10. Idaho (FCS since 2018) and Sacramento State and North Dakota State (reclassifying upward) correctly have none. Earlier seasons are covered by committed snapshots plus `patch_known_membership_gaps.py`. |
| No duplicate selections | **Verified** incidentally — a duplicate would break the field-size and pot-size counts above. Not asserted directly. |
| Deterministic tiebreakers | **Open.** The selector is deterministic for a fixed database, but nothing asserts that two runs of the full pipeline produce an identical field. |
| 8 conference games enforced, no cross-division cupcakes | **Not applicable.** These are scheduling rules for a hypothetical league; this project rates games that were actually played. |
| Bye logic documented | **Verified** in `docs/coe_spec.md` and enforced by the bye test above. |

## 2. Coefficient engine stability

| Item | State |
| --- | --- |
| 2010–2013 present | **Verified.** The games table holds 1980–2026, 30,357 games, 7,509 of them in the 2010s. The old checklist's "backfill" framing predates that. |
| Rolling 5-year calculation | **Verified.** `team_coeff_5yr` covers 1984–2026, 5,085 team-seasons; the four-year lead-in is the window filling. |
| Conference aggregation | **Verified.** `conference_coeff_5yr`, 518 conference-seasons over the same range. |
| Stable across rebuilds | **Partly verified.** CI rebuilds the database from the backup plus committed CSVs on every run and the whole suite passes against it, which is a real reproducibility proof. A byte-for-byte comparison of two rebuilds is not asserted. |
| No NULL propagation | **Open as a general claim.** Specific guards exist and are tested (an unrated game is excluded rather than counted as zero; a missing Success Rate takes a recorded fallback rather than becoming 0), but nothing asserts the absence of NULL propagation across every table. |
| Formula version frozen as v1 | **Open.** CoE 2.0 bonuses carry a `coe2_bonus_v1` version tag, but production Elo moved as recently as 2026-10-09 (k 40 → 45). Freezing is a decision, not a defect. |
| Coefficient math documented | **Verified.** `docs/coe_spec.md`, with every fitted parameter and its fitting method recorded in `config/model_config.json`. |

## 3. Playoff engine integrity

| Item | State |
| --- | --- |
| Year 2 logic finalized | **Verified.** `YEAR2_BIDS` applies to every season after 2014. |
| Cross-year stability | **Verified** over 2014–2025 by the structural tests, which run per season rather than on a sampled few. |
| Identical output on re-run | **Open**, as under tiebreakers above. |
| Bye and pot allocation verified | **Verified** by the tests in section 1. |
| Edge cases (tie champions, weak champion) | **Partly verified.** The independent-threshold case is covered. A tied conference champion is not. |

## 4. Data integrity

| Item | State |
| --- | --- |
| Conference realignment documented | **Verified.** Per-season snapshots in `data/raw/membership_*.csv`, with the reviewed identity registry in `data/reference/team_identities.json` and the audit in `docs/team-identity-audit.md`. |
| No orphan teams, no games missing teams | **Verified** by the game-coverage audit (`src/audit_game_coverage.py`) in the scheduled build. |
| Conference championship games imported | **Verified** — postseason games carry a phase, and derived conference standings are computed per season. |
| Schema frozen | **Open**, and arguably should stay open while people, places and statistics layers are still landing. |

## 5. Documentation

Verified: `README.md`, `docs/CHANGELOG.md`, `docs/coe_spec.md`, and a rebuild
path in `docs/COMMANDS.md`. The known-limits pages are
`docs/identity-problems.md` and `docs/team-identity-audit.md`. Open: a
philosophy section, and a database schema reference.

## 6. Reproducibility

| Item | State |
| --- | --- |
| Clean build from scratch | **Verified.** CI bootstraps a database from `db/league_backup_before_playoff_migration.db` plus the committed CSVs on every run and the full suite passes against it. That path is the fresh-clone proof. |
| No manual SQL steps | **Verified** — the bootstrap applies `sql/*.sql` itself. |
| `build_all.py` works | **Superseded.** `run_pipeline.py`, then `./build_dashboard.py`. |
| Works from a fresh clone in a codespace | **Open.** Nothing exercises a codespace specifically. One known wrinkle: `src/fit_xsrdiff.py` reads `elo_game_history`, which `build_elo.py` produces, so on a clean database the curve cannot be refitted before the first Elo build and the committed curve is used instead. |

## 7. UI layer

The Streamlit gates are gone with the Streamlit app. Current equivalents,
verified in Chromium against a locally served `ui/` on 2026-10-10:

- Every route renders with no console errors: Home, Stats (Overview, Teams,
  Players, Matchup, Playoff), Standings (Elo, conference, home-field), Teams,
  Stadiums, a team page, People, Find a Game.
- The season selector is built from the data, offering 1980–2026.
- The 24-team field, byes and pots render for a selected season, and the
  bracket simulates.
- No year or ruleset is hardcoded in a page; season and view live in the hash
  URL.

Open, and tracked in `release-v0.1.9.md`: standings rank movement, an
intentional logo treatment, and a documented visual identity reviewed across
the whole desktop and mobile journey rather than in isolated screenshots.

## Known hazards found during this audit

- `src/build_playoff_field.py` implements a **12-team, 5-auto-bid** field and
  is referenced by nothing. The live selector is
  `src/select_playoff_field_v2.py` (24 teams, 8 byes). The file carries a
  header saying so; deleting it is the owner's call.
