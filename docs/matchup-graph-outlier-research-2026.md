# FBS schedule graph and extreme-observation sensitivity — October 31, 2026

Research source: successfully completed GitHub Actions run
[41257550785](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/41257550785),
artifact [matchup-graph-outlier-research](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/41257550785/artifacts/13063692359).
Both the feature-unit-test job and the data-research job concluded **success**.
The one-time job was removed after execution. No paid CFBD requests,
production data overwrites, UI changes, merge or deployment occurred.

## Design and tested population

Read-only ingestion from the preserved `data/research/matchup_cfbd/`
files and the existing FBS-only membership rules. The study uses completed
games strictly before the day supplied: 2025-12-31 and 2026-10-31.
The 2025 count below (1,584 team-game sides) reflects the cutoff,
so it differs from the later full-coverage 1,616 game-side audit.
The 2026 cutoff contains 904 team-game sides per metric.

An undirected team–opponent graph collapses repeated game observations to
one unique matchup edge, then reports connected components and **bridge
matchups** (edges whose removal disconnects a component).
Outlier sensitivity compares existing descriptive percentiles with a
research-only 1st/99th-percentile winsorized fit, on the **same eligible
rated-team population**. Neither step alters the production metric.

## Result 1: Connected FBS schedule graphs

| Metric observations | 2025 through Dec 30 | 2026 through Oct 30 |
|---|---:|---:|
| FBS offense team-game sides per split metric | 1,584 | 904 |
| Connected graph components per metric | **1** | **1** |
| Bridge matchup edges per metric | **0** | **0** |

The network is globally connected and has no single *game pairing* whose
removal splits it. This is favorable evidence about cross-team
comparability, **not** a proof that individual offense and defense
effects are well identified, that all teams have equally diverse
competition, or that schedules are equally informative.

## Result 2: Extreme outcome influence on offensive percentiles

Reported values are the **90th percentile across rated teams** of the
absolute offensive-percentile change after temporarily clipping the
lowest and highest 1% of observed metric values and refitting.

| Metric | 2025 offensive p90 change | 2026 offensive p90 change |
|---|---:|---:|
| Rush success rate | 1.47 | 2.90 |
| Pass success rate | 1.47 | 2.17 |
| Rush PPA | 3.31 | 2.39 |
| Pass PPA | 2.21 | 2.90 |
| Rush explosiveness | 4.41 | 5.80 |
| Pass explosiveness | 4.41 | 5.29 |

By contrast, the previous **opponent-removal** analysis found much
larger sampled p90 changes, especially in 2026 explosiveness.
The tests are **not directly commensurable**: one clips a small set of
game values, while the other removes all games involving a particular
opponent and changes the comparison population.

**Interpretation:** The observed volatility is more consistent with
schedule/sample sensitivity than a few isolated game-value tails, though
the tests alone cannot establish causation. Do **not** automatically
winsorize explosiveness, introduce a universal stability cutoff, or
treat graph connectedness as a confidence interval.

## Recommended next research acceptance gates

- Broaden from graph-level connectivity to **individual schedule
  information**, especially low opponent diversity and repeated rivals.
- Quantify sensitivity separately for raw and opponent-adjusted
  percentiles; raw values are an essential comparison.
- Measure sample reliability by played games/opponents and actual
  repeated-opponent exposure, with a consistent week cutoff.
- Verify opponent-unit interpretations of the schedule metric before
  determining public display policy.
- Keep the UI and production data path separate until a descriptive
  quality policy is set and the full repository regression suite passes.
