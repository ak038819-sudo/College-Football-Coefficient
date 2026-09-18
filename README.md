# Fixing College Football

A structural college football simulation engine that replaces the current postseason model with a coefficient-driven, standings-qualified 24-team playoff.

## Core Rules

1. Every FBS team must belong to a conference (no independents in the new format)
2. 11-game regular season: 8 conference games, 3 non-conference
3. All non-conference games played within FBS
4. No Top 25 poll — no subjective rankings
5. A Coefficient (CoE) system replaces the poll for playoff selection and seeding
6. 24-team playoff; all conference champions qualify
7. Top-8 (by conference strength) get byes to the Round of 16; the rest play in the Round of 24
8. Team CoE decides home-field advantage
9. Conference bid counts are set by conference-level coefficient ranking

Full original rules and the coefficient system's early design draft are in `docs/`.

## The Rating Model (source of truth)

**As of this rebuild, the project uses an iterative, opponent-strength rating model — not the discrete win/OT-loss/loss point system originally drafted in `docs/coe_spec.md`.** The old system (and everything built on it: bounty multiplier, conference/team point totals, qualifier and bracket scripts) is archived under `archive/discrete_coe_system_2026-09/` for reference, not deleted.

The live model, in `src/build_coefficients.py`:
- Every team starts a season at rating 1.0
- 15 iterations: a winner's rating increases by `opponent_rating × phase_weight` (phase weights: regular=1.0, bowl=2.0, CFP=3.0); the loser still gains a small `opponent_rating × phase_weight × 0.15` "loss penalty"
- Each iteration renormalizes to keep the scale stable
- **Team CoE** = that season's rating
- **Conference CoE** = sum of its member teams' ratings that season (conference membership comes from `team_membership_by_season`)
- **5-year rolling CoE** (both team and conference) = a decay-weighted sum over the trailing 5 seasons (`0.92^age`, more recent seasons weighted higher)

Team-level seeding and home-field use the 5-year rolling team CoE. Conference bid-count ranking uses the 5-year rolling conference CoE.

## Data Coverage

- **Games**: 2010–2025 (16 seasons, ~12,000 games), loaded from `data/raw/games_YYYY.csv`
- **Conference membership**: 2014–2025 only. There is no membership data for 2010–2013, so those 4 seasons can produce team/conference *ratings* but **cannot** be used for playoff qualification (which needs conference standings to know who takes each conference's bids). Team ratings for 2010–2013 still feed correctly into the 5-year rolling windows for 2014+.
- **Conference standings**: derived locally from the loaded games (win-loss record per team, per conference), using the same tiebreak heuristic as CFBD's own records endpoint (conference win% → conference wins → conference losses → overall win% → overall wins → team name). This is a stable proxy ordering, not each conference's actual official tiebreaker rules (head-to-head procedures, divisions, etc.) — see `src/coefficients/derive_conference_standings_local.py`.

## Playoff Format (current implementation)

**Bid allocation**, by conference's 5yr rolling CoE rank:

| Conference rank | Bids |
|---|---|
| 1–4 | 4 (top 4 teams by conference standings) |
| 5 | 3 |
| 6–10 | 1 (champion only) |

This is a project decision, differing slightly from the original spec's Year 1 draft (which gave rank 6 two bids) — Year 1 and Year 2+ are now identical, which also resolved a contradiction in the original spec between the Year 2+ seeding rule for rank 6 and the fixed "top-8 get byes" requirement.

**Seeding** (8 byes total, always):

| Conference rank | Byes | Pot 1 | Pot 2 |
|---|---|---|---|
| 1–2 | 1st & 2nd place | 3rd place | 4th place |
| 3–4 | Champion | Runner-up, 3rd place | 4th place |
| 5 | Champion | Runner-up, 3rd place* | — |
| 6–10 | Champion (rank 6 only) | — | Champion (ranks 7–10) |

*Rank 5's 3rd-place team was moved from Pot 2 to Pot 1 (project decision) to balance Pot 1 and Pot 2 at 8 teams each — the original spec's table put it in Pot 2, which made the pots unequal (7 vs 9) and broke a clean 1-to-1 draw pairing.

**"FBS Independents" are excluded from bid eligibility** — the new ruleset requires every team belong to a conference, so real-world independents (Massachusetts pre-2025, etc.) don't compete for bids in this model. This leaves exactly 10 bid-eligible conferences, matching the spec's 1–10 framework.

**Round of 24 draw**: Pot 1 vs Pot 2, randomized with a `--draw-seed` for reproducibility, backtracking to guarantee no same-conference matchup. Home field goes to whichever team has the higher 5-year rolling team CoE (ties broken by team name).

## Dashboard

A self-contained, publishable HTML dashboard — year selector, team/conference ratings, playoff field by conference, and the Round-of-24 bracket, all with embedded data (no server, no API calls needed to view it):

```bash
python build_dashboard.py --draw-seed 1
```

Writes `ui/dashboard.html`. Open it directly in a browser, or upload it anywhere that serves static files. To change the page's own design or layout, edit `ui/dashboard_shell.html` (the template) and rerun the build — the `__DATA_JSON__` placeholder gets replaced with a fresh export from `src/export_dashboard_data.py`.

## Running It

**One command, full pipeline:**
```bash
python run_pipeline.py                    # builds everything, shows the 2025 playoff field + bracket
python run_pipeline.py --year 2014        # same, for 2014 instead
python run_pipeline.py --force            # wipe db/league.db and rebuild from scratch
python run_pipeline.py --draw-seed 7      # use a specific bracket draw seed
```

That's the recommended way to run this — it replaces about 30 manual commands with one. See `run_pipeline.py`'s docstring for exactly what it does, in order.

**Individual pieces**, if you want to run or inspect one step at a time:
```bash
# Bootstrap the database (teams, aliases, 2014-2025 membership) from the backup DB
python src/bootstrap_league_db.py --backup db/league_backup_before_playoff_migration.db --out db/league.db

# Load one season of games
python src/load_games.py data/raw/games_2025.csv

# Patch known conference-membership gaps (Miami (FL), FAU, etc. -- see the script for sources)
python src/patch_known_membership_gaps.py --db db/league.db

# Run the rating model (all seasons at once)
python src/build_coefficients.py

# Conference standings for one season
python src/coefficients/compute_conference_team_records.py --year 2025 --no-validation
python src/coefficients/derive_conference_standings_local.py --year 2025

# Playoff field + bracket for one season
python src/coefficients/select_playoff_field_v2.py --year 2025
python src/coefficients/draw_playoff_bracket_v2.py --year 2025 --draw-seed 1
```

## Outputs

`data/processed/`:
- `team_ratings_by_season.csv` — every team's rating, every season
- `team_coeff_5yr.csv` — 5-year rolling team CoE
- `conference_ratings_by_season.csv` — every conference's rating (sum of member teams), every season
- `conference_coeff_5yr.csv` — 5-year rolling conference CoE

## Testing

```bash
pip install -r requirements-dev.txt
pytest
```

Most tests run against your real `db/league.db` (skip cleanly if it doesn't exist yet — build it with `run_pipeline.py` first) since the things worth checking here are properties of the actual data and algorithms, not synthetic toy cases. What's covered:

- The full SQL schema chain builds with no conflicts (would have caught the `team_aliases`-missing-from-schema bug and the duplicate-table-definition conflicts found earlier in this project)
- A fresh bootstrap never resurrects the three known-dead duplicate teams, and does include the real late additions (Idaho, Massachusetts)
- A completed season's ratings are stable regardless of the confidence-blending settings (`--confidence-games`, `--prior-regression`) — guards against an early-season fix accidentally changing historical years
- Every season's playoff field has exactly 8 byes, equal-sized Pot 1/Pot 2, and 24 total qualifiers (this test suite actually caught a real bug on its first run: the Pac-12 collapsing to 2 teams by 2024 while still ranking high enough for 3 bids — see `select_qualifiers()`'s carried-destiny fix)
- The independent-CoE-threshold rule never displaces a conference champion, even when directly provoked with a synthetic case designed to try
- The Round-of-24 draw never produces a same-conference matchup, checked across 20 different seeds per season (240 checks total)
- The draw is reproducible (same seed → identical pairing) and home field always goes to the higher-CoE team

## Project Status

✅ Real historical data loaded and verified (2010–2025, ~12,000 games)
✅ Iterative rating model implemented (team + conference level)
✅ Conference-membership gaps researched and patched with sourced history
✅ Conference standings derived locally (no external API dependency)
✅ Bid allocation by conference CoE rank
✅ 24-team qualifier selection with balanced 8/8 pot seeding
✅ Round-of-24 bracket draw (no-same-conference, CoE-based home field, reproducible via seed)
✅ One-command full-pipeline runner

### Not yet built
- Full bracket advancement past the Round of 24 (Round of 16 → quarters → semis → final) — needs a way to project/simulate game outcomes, a different kind of problem than everything above
- The NIT (a second, parallel bracket for teams that missed the main field)
- Official conference standings via a live CFBD API pull (current standings are a locally-computed, methodologically-identical substitute — see Data Coverage above)

## Database Design Notes

`team_membership_by_season` is keyed by `(team_id, season_year)`, which supports realignment (a team's conference can change year to year) and is what conference standings, bid allocation, and the conference-level rating rollup all depend on.

The pipeline is deterministic given a fixed `--draw-seed` and unchanged database state — rerunning produces identical output, which is what `run_pipeline.py`'s tests confirmed (same seed → identical bracket, different seed → different but still valid bracket).

## Legacy / Archived

`archive/discrete_coe_system_2026-09/` holds the original discrete win/OT-loss/loss point system (with bounty multiplier, conference champion derivation, and Year 1/Year 2 qualifier and draw scripts). It's fully superseded by the iterative model above but kept for reference. If reviving any piece of it, note that its SQL tables (`team_coefficient_by_year`, `conference_coefficient_by_year`, `playoff_field_by_year`, etc. — still defined in `sql/`) are no longer written to by anything in `src/`.
