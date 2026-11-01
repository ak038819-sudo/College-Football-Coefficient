# Fresh CFBD research backfill and model evidence — 2026-10-31

**Evidence:** isolated GitHub Actions research run
[41255752882](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/41255752882),
with staged-data artifact `matchup-cfbd-staged-2025-2026` (artifact ID
`13062014924`, 7-day retention). Fresh requests used the repository's existing
CFBD_API_KEY only inside the runner, with no commits or changes to production raw files.
The special auto-triggered research job was removed following this successful pass
to avoid repeated API charges. These are **staging-only source files**.

## Source and canonical FBS coverage

| Check | 2025 | 2026 |
|---|---:|---:|
| CFBD source team-games | 3316 | 2270 (changed during live season) |
| CFBD source paired games | 1658 | 1135 |
| Completed games in Elocast eligible FBS database | 808 | 485 |
| Canonical paired FBS games with every split metric | **808 / 808** | **485 / 485** |
| Canonical team-game sides with every split metric | **1616 / 1616** | **970 / 970** |
| Missing season membership | **0** | **0** |
| Unmatched advanced rows *among counted FBS game sides* | **0** | **0** |
| Coverage readiness gate | **PASS** | **PASS** |

The CFBD endpoint provides many observations outside the project's eligible
FBS-only game universe. These **must not be interpreted as extra FBS games**.
`data/raw/membership_2026.csv` originally omitted two actual 2026 FBS entrants:
North Dakota State (Mountain West) and Sacramento State (MAC). These now have
explicit 2026 records on the draft PR branch, based on the documented
conference transitions, their scheduled games and independent confirmation.
Without those corrected records, 15 games were wrongly excluded.

## Strict prior-date walk-forward validation

All comparisons use the **same** opponent-adjusted-eligible team-games in
each season. Values are **absolute error in the original metric scale**.
The exploratory split model currently uses the existing adjusted-statistics
solver with **provisional unit-game weight and ridge shrinkage=2**, *not*
a split-calibrated production model.

| Metric | 2025 adjusted MAE | 2025 previous-offense MAE | 2026 adjusted MAE | 2026 previous-offense MAE |
|---|---:|---:|---:|---:|
| Rush success rate | 0.08648 | 0.09000 | 0.08841 | 0.09237 |
| Rush PPA | 0.19574 | 0.20096 | 0.20862 | 0.21005 |
| Rush explosiveness | **0.29147** | **0.27401** | **0.29292** | **0.27066** |
| Pass success rate | 0.09141 | 0.09547 | 0.09220 | 0.09441 |
| Pass PPA | 0.29318 | 0.29507 | **0.29542** | **0.28838** |
| Pass explosiveness | **0.33949** | **0.33275** | **0.34449** | **0.32064** |

2025 matched evaluated team-games **1174** (~73.0% coverage of considered
team-games); 2026 **522** (~54.6%). Coverage limitations reflect eligibility
during early dates, not a missing data-field problem.

### Release consequences

- The *source data exists*, and FBS pair coverage is now verified. We can
  proceed with **method calibration**, not UI.
- Do **not** claim universal predictive improvement: both explosiveness
  splits worsen error in both seasons. 2026 passing PPA also worsens.
- Do **not** use a future 2026 holdout to optimize shrinkage if that same season
  is to be represented as an independent performance test. Compare candidate
  training-period settings on older seasons, then evaluate 2026 once.
- Calibrate per-metric shrinkage, weighting, and if appropriate outcome
  transformations; investigate explosive-play scale and sampling variance.
- Add uncertainty, early-season reliability states, and venue-aware baseline
  checks; use missing/unavailable where evidence is weak.
- No raw CSV promotion or any UI/production changes yet. **PR #161 stays draft.**
