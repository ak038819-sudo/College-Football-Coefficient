# Elocast Matchup Intelligence — held-out split calibration report

Generated via [GitHub Actions run 41256135712](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/41256135712), job `calibrate-staged`. This job completed successfully and used the staged CFBD data artifact from prior validated research run 41255752882. No fresh CFBD request, production CSV replacement, merge, or deployment was performed.

**Experiment:** Fit the established `build_adjusted_stats.solve()` ridge model for each split metric, train-only model-selection in 2025 using candidate lambdas 0.5, 2, 8, 24; strictly chronological refits each game date. Choose a 2025 improvement over own-team prior offense with >=50% matched coverage and >=0.2% relative MAE improvement; otherwise prefer the baseline. Score **2026 exactly once** using the selected parameters. 2026 does not select lambdas. Per-team minimum three games; all six 2026 metrics evaluated on 522 qualifying offense observations (~54.6% of considered team-games).

## Held-out 2026 results

| Metric | 2025 selection | Selected lambda | 2026 adjusted MAE | Matched prior-offense baseline MAE | Recommendation |
|---|---|---:|---:|---:|---|
| Rush success rate | adjusted | 2 | 0.08841 | 0.09237 | Further validation |
| Rush PPA | adjusted | 2 | 0.20862 | 0.21005 | Further validation, small gain |
| Rush explosiveness | baseline | 24 | 0.26088 | 0.26041 | **Research only** |
| Pass success rate | adjusted | 2 | 0.09220 | 0.09441 | Further validation |
| Pass PPA | adjusted | 8 | 0.28540 | 0.28371 | **Research only** |
| Pass explosiveness | adjusted | 24 | 0.30602 | 0.30352 | **Research only** |

**Interpretation:** Both split success rates beat the matched prior-offense baseline. Rush PPA offers only a small incremental improvement. Both explosiveness splits and pass PPA **fail** to outperform on truly held-out 2026 examples, despite tuning on 2025. They must not be advertised as improved opponent-adjusted predictors. In particular, stronger shrinkage greatly reduces explosiveness error relative to lambda=2 but does not justify release.

## Early-season reliability (implemented)

`src/matchup_intelligence.py` exposes `distinct_opponents` and a non-probabilistic `reliability` label based on **both** per-team observed games and opponent diversity:
- fewer than 3 games/opponents: insufficient
- fewer than 5 games/opponents: provisional
- fewer than 8 games or fewer than 7 distinct opponents: developing
- otherwise established

This does not represent a confidence interval, schedule-graph connectivity check, or cross-season reproducibility result. The 2026 eligible share remains ~55%, mainly due to strict minimum prior-game qualification. Do not extrapolate the reported model MAE to excluded early-season games. Model output may be exposed as research data with these limitations; no production UI or unqualified matchup-edge language is authorized.

## Remaining release gates

- Preserve/promote authenticated staged split CSVs only after data-integrity review. The source artifact expires under GitHub's short retention policy.
- Expand to several **earlier** training seasons, not retuning on 2026 after looking at its results. Reserve a *new* later-season holdout for another unbiased evaluation if further changes are made.
- Consider robust loss / alternative explosive-play distributions and measure predictive interval calibration; failing metrics stay with baseline or are omitted.
- Evaluate early-season fallback strategies, opponent graph connectivity, and interval coverage. Reliability labels are heuristics until validated.
- Run newest full CI, source exports and regression checks; PR #161 stays draft until validated and UI is ready.

The one-off workflow job was removed after the successful experiment to avoid reruns depending on an expiring artifact. The report artifact `matchup-calibrated-holdout` (ID 13062951570) remains available on run 41256135712 for its retention period.
