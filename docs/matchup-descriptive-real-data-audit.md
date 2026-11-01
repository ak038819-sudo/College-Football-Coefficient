# Descriptive Metric Integrity: first full real-data report

Research run: [GitHub Actions 41256966086](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/41256966086) (the test and evidence-preservation jobs both succeeded).

## Reproducibility and provenance

The independent, previously validated CFBD advanced game split files for 2025 and 2026 have been preserved on the feature branch at `data/research/matchup_cfbd/` along with their original coverage report, final FBS readiness gate, and two descriptive-integrity reports. `provenance.json` identifies source run 41255752882 and artifact 13062014924.

**This is research-only data.** The original `data/raw/game_advanced_2025.csv` and `data/raw/game_advanced_2026.csv` remain unchanged, and nothing on `main` or the live site has been updated. The one-off preservation workflow job was removed after successful execution.

## Real-data descriptive checks

| Field | 2025 complete | 2026 strictly before 2026-10-31 |
|---|---:|---:|
| Eligible FBS offensive team-game rows for each split metric | 1,616 | 904 |
| Teams with >=3 recorded offensive and defensive games and distinct opponents | 136 | 138 |
| Teams with >=8 games and >=7 distinct opponents (established) | 136 | **7** |
| All six split metrics satisfy algebraic decomposition and finite percentile bounds | Yes | Yes |
| Sampled teams per metric for opponent-removal sensitivity | 4 | 4 |

The 2026 **as-of** cutoff intentionally excludes every game on October 31, preventing same-day data leakage. Hence 904 team-game rows versus 970 rows in the full completed 2026 FBS coverage audit are expected, not necessarily missing source values.

## Observed leave-one-opponent-out sensitivity

The numbers below are the **largest** absolute percentile shifts observed across a deterministic sample of 4 teams per metric when removing a single opponent. They do **not** represent leaguewide maxima or confidence intervals.

| Metric | 2025 max offense / defense change (percentile points) | 2026 max offense / defense change |
|---|---:|---:|
| Rush success rate | 8.09 / 13.97 | 19.57 / 23.91 |
| Pass success rate | 13.24 / 18.38 | 15.94 / 15.94 |
| Rush PPA | 8.82 / 17.65 | 31.16 / 15.22 |
| Pass PPA | 13.24 / 12.50 | 28.26 / 18.12 |
| Rush explosiveness | 19.12 / 16.18 | 21.01 / 30.43 |
| Pass explosiveness | 22.79 / 20.59 | 17.39 / **37.68** |

**Assessment:** The math is internally consistent, but early-season schedule sensitivity can be substantial. Existing 'established' sample thresholds are necessary, not sufficient, for declaring a percentile robust. Unstable teams should receive an explicit sensitivity warning, or a raw figure rather than an unqualified adjusted rank. These estimates are for *description*, not a projected win likelihood.

## Next gates

1. Expand opponent-removal diagnostics to a representative or complete set of FBS teams (the first sample covers only four per metric), optimizing runtime before doing a full sweep.
2. Assess percentile rank variation across chronological week cutoffs and whether changes reflect added evidence rather than unstable estimation.
3. Validate distortions from low-variance/outlier-prone explosiveness, repeated opponents, neutral venues, and connected components of the FBS schedule graph. Avoid arbitrary stability cutoffs without empirical justification.
4. Adopt a transparent public display policy: sample tier **and** robustness diagnostics, never a single unsupported confidence score.
5. Run full regression suite on the archived research dataset. Keep PR #161 draft; no deployment or UI yet.
