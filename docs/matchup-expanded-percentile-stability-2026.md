# Broader descriptive-percentile sensitivity research — Oct 31, 2026

The one-off GitHub Actions research run
[41257232559](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/41257232559)
**passed** both feature tests and the independent research task. Source:
preserved, previously verified `data/research/matchup_cfbd/` CSVs (no CFBD
network call, no production edits). Downloadable raw reports:
[matchup-weekly-stability](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/41257232559/artifacts/13063902349).
The one-off research job was removed after completion.

## Method and boundaries

For **2025**, we deterministically sampled **20** rated teams per
metric *across its offensive percentile distribution*. For **2026**, 12
sampled teams per metric were evaluated at each strict prior-date cutoff:
September 26, October 10, and October 31. Each team is refitted after
removing every game against one opponent, for both home and away.
For each team the worst absolute offense/defense percentile movement is
recorded; the table below reports the *90th percentile of those team-level
worst movements*. This is **not** a full-league maximum or a confidence
interval. It includes changes to the adjusted comparison distribution
because each exclusion triggers a fresh fit.

## p90 of worst opponent-removal percentile shifts (percentile points)

| Metric | 2025 offense / defense | Oct 10, 2026 offense / defense | Oct 31, 2026 offense / defense |
|---|---:|---:|---:|
| Rush success | 12.50 / 16.25 | 35.65 / 27.68 | 16.38 / 23.77 |
| Pass success | 11.76 / 16.91 | 23.70 / 18.26 | 21.45 / 21.67 |
| Rush PPA | 17.94 / 17.28 | 27.61 / 28.18 | 24.35 / 13.70 |
| Pass PPA | 15.29 / 20.00 | 35.74 / 27.28 | 25.29 / 23.19 |
| Rush explosiveness | 24.41 / 27.28 | 36.52 / 24.86 | 35.22 / 31.88 |
| Pass explosiveness | 21.54 / 23.24 | 22.97 / 39.86 | **41.38 / 36.88** |

At the **September 26, 2026** cutoff, only **1–2 sampled teams per metric**
could even be re-rated after removing an opponent; the pregame sample is
therefore too thin to summarize as robust. This is missing-eligibility
information, **not** evidence of low volatility.

October 10 had 8–10 successfully re-rated teams out of 12 sampled per
metric; October 31 had 12/12. Consequently, early and later p90 numbers
have different denominators. Do not claim uniform improvement across all
categories or that the late-October sample is representative of every
rated team.

The research JSON also contains matched-team week-to-week percentile
movement. Such changes include actual games played, changes in schedule
composition and changes in the eligible ranking cohort; they are **not**
measurement error.

## What this changes for product readiness

1. Maintain **raw observed value** as the unambiguous statistic.
2. Show opponent-adjusted effects and unit schedule difficulty only with
   explicit **date**, sample coverage, opponent count, and reliability label.
3. Keep teams with sparse opponent schedules **unrated**; adding repeated
   games against the same opponent cannot create an established profile.
4. **Do not publish a precise confidence score or arbitrary universal
   10/20-point 'stable' badge.** Validate a cutoff with the full eligible
   population and compare stability by metric/season stage first.
5. Treat explosiveness as a descriptive statistic with elevated sensitivity
   and clear source definitions, not an unqualified elite/poor ranking.
6. Before UI design: test schedule-graph connectedness, outlier sensitivity,
   unit-specific eligibility/normalization and full regression suite.

The descriptive formulas themselves passed previous real-data integrity
checks, but broad *robustness* is still under investigation. PR #161 remains
a draft.
