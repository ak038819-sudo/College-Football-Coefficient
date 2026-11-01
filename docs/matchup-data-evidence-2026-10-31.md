# Matchup Intelligence — Evidence review (2026-10-31)

Source: completed GitHub Actions run **41254935177**, `Test, Rebuild
Dashboard, and Deploy`, feature branch at
`b064b021c02d1f22c4242d242e9cf8468dacf37f`.
The PR test job was successful, and its log contained a real SQLite bootstrap,
2025/2026 per-game coverage audits, and strict prior-calendar-date success-rate
walk-forward results. The run did **not** deploy (draft PR, main-only deploy).

## Results from actual committed data

| Check | 2025 | 2026 to snapshot |
|---|---:|---:|
| Completed FBS games | 808 | 470 |
| Advanced games with both sides | 808 | 470 |
| Advanced team-game sides | 1616 | 940 |
| Missing-membership games | 0 | 15 |
| Advanced team-game rows unmatched to FBS game side | 0 | 30 |
| All rush/pass split metrics with observed sides | **0** | **0** |

The 2026 gaps concern 15 completed games with missing season FBS mapping,
and 30 per-game advanced team rows that cannot be tied to the eligible FBS
pairing. These warrant a mapping audit; do NOT silently invent FBS status.

## Strict prior-date overall success-rate evaluation

*Same matched subset for each of the three methods within a season.*

| Metric | 2025 | 2026 |
|---|---:|---:|
| Evaluated adjusted team-games | 1174 | 500 |
| Eligible share of considered team-games | 73.01% | 53.76% |
| Adjusted MAE | 0.064729 | 0.064339 |
| National average MAE (matched) | 0.073402 | 0.072935 |
| Prior-team-offense MAE (matched) | 0.070507 | 0.069501 |
| Adjusted RMSE | 0.082990 | 0.082143 |
| National average RMSE (matched) | 0.094999 | 0.091842 |
| Prior-team-offense RMSE (matched) | 0.090478 | 0.087236 |

Interpretation: historical *overall* success-rate point estimates show
lower out-of-sample error than either benchmark on those eligible examples.
This does **not** establish six rush/pass models, confidence intervals,
win-probability accuracy, or eligibility for every pregame matchup.

## Critical outstanding gates

1. Refresh `game_advanced_2025.csv` and `game_advanced_2026.csv` safely
   with true rush/pass splits; current committed files omit their columns.
2. Load staged data in disposable SQLite and inspect paired game coverage,
   missing values, team-name mapping, and 2026 membership omissions.
3. Backtest each rush/pass metric on strict prior-date folds. Tune any new
   split-stat shrinkage on earlier periods, not the season used for final
   scoring.
4. Confirm CI on the newest head, check statistical validity guards and
   compare model errors with matched baselines. Validate uncertainty separately.
5. Keep PR draft. No UI design or production publication until the evidence
   supports it.

Even after passing coverage gates, `production_ready` remains false until
data accuracy, leakage checks, and backtesting are independently verified.
