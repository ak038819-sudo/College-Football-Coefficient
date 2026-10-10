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
| A fixed field size every year | **Verified.** The live format is 12 teams (see the closing section). `tests/test_cfp_field.py` drives the real selector over all thirteen seasons 2014–2026: twelve teams, seeded 1 to 12, no team twice. |
| A fixed number of byes | **Verified**, same file, same seasons: seeds 1–4 and nobody else, and each of them reaches the quarterfinals in 100% of simulated runs. |
| Bid structure stable | **Verified.** Exactly 5 automatic and 7 at-large bids every season, and the 5 automatic bids are exactly the 5 strongest conference champions. |
| Conference champions auto-bid | **Partly, by design.** The real format gives automatic bids to the five highest-ranked champions, not to all of them. Verified: an automatic bid never goes to a non-champion, and no at-large bid passes over a stronger eligible team, so a champion that misses out was genuinely out-ranked. |
| Independents handled correctly | **Verified.** An independent can be selected but never automatically, which is the real rule: no conference, no championship, no automatic bid. The 24-team model's special strength threshold is gone — independents are ordinary at-large candidates now. |
| Seeding is the ranking | **Verified.** Seeds run straight down 5-year rolling team CoE, which is the 2025 CFP rule change; under the 2024 rule the top four champions took the top four seeds wherever they ranked. A test asserts the ranking order, so the site cannot silently run the older variant. |
| The bracket keeps its halves | **Verified** twice, in Python and in the page's own copy: with the top two seeds made unbeatable they must both reach the final, which fails if the halves are wrong. |
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
- The 12-team field, the byes and the first round render for a selected
  season, and the bracket simulates.
- No year or ruleset is hardcoded in a page; season and view live in the hash
  URL.

Open, and tracked in `release-v0.1.9.md`: standings rank movement, an
intentional logo treatment, and a documented visual identity reviewed across
the whole desktop and mobile journey rather than in isolated screenshots.

## Which playoff format is live

The real **12-team** College Football Playoff, since 2026-10-10. Everything
on the site -- the Field, Bracket & Simulator, Title Odds and History pages,
the team pages' appearance and bye counts, and the methodology page -- runs
on `src/coefficients/select_cfp_field.py` and
`src/coefficients/simulate_cfp_bracket.py`. Every invariant in section 1
above is that format's. The write-up is
[`cfp-12-team-format.md`](cfp-12-team-format.md).

The invented 24-team model is **retained and still runs**, with its own
tests, but nothing live reads it: `select_playoff_field_v2.py` (field),
`draw_playoff_bracket_v2.py` (the pot draw), `simulate_bracket.py` (the
24-team simulation) and `select_nit_field.py` (the 16-team NIT, which has
never been exported or shown). Its ruleset is `coe_spec.md`, which now says
so at the top. `build_playoff_field.py` -- an older, cruder 12-team sketch
that wrote to a database table and picked champions by rating rather than
standings -- is superseded by `select_cfp_field.py` and is not used either.

Two things the earlier version of this section got wrong, recorded so they
are not re-derived: the move did **not** reach the CoE 2.0
`cfp_appearance`/`cfp_win` bonus categories, because those count real CFP
games (`phase == 'cfp'`) rather than model ones; and it did **not** change
published history, because the model's playoff field was never a stored
table -- it is computed per season at export time, so every season from 2014
on simply recomputes under the new format.

What the move did reach: the field selector, the bracket and simulator, the
title-odds export, the three playoff pages, the static manifest's model
parameters (`playoff_bids` became `playoff_format`), the conference page's
bid-rank tile, the footer, the methodology page, and `coe_spec.md`'s
framing.
