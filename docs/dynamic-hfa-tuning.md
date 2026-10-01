# Dynamic HFA tuning for v0.1

The production Elo home bonus remains **50 points**, flat for all teams.
This experiment asks whether team-specific home performance adds predictive
signal to that calibrated intercept.

## Candidate formula

For a game on date `d` at the designated home team's field:

`bonus = 50 + clamp(alpha × (team_estimate(d) - national_estimate(d)), -cap, +cap)`

Both estimates use the recency-weighted actual/neutral-expected home wins from
the independent flat-Elo reference and only games **before** `d`. The team
estimate already shrinks toward the national estimate for small samples.
Neutral games still use zero home bonus. The formula holds the league-wide
intercept at 50 rather than letting an untuned national estimate replace it.

## Walk-forward results

The search selected candidates on 2018–2022 Brier score. The 2023–2025
complete seasons are reported separately; 2026 is incomplete. Within the
small grid, the training-selected settings are a **5-year half-life**,
**shrinkage K = 60**, **alpha = 0.25**, and **±40 Elo cap**.

| Period | Games | Flat Brier | Candidate Brier | Flat log loss | Candidate log loss |
|---|---:|---:|---:|---:|---:|
| 2018–2022 | 3,626 | 0.185710 | 0.185683 | 0.548384 | 0.548256 |
| 2023–2025 | 2,398 | 0.190223 | 0.190000 | 0.558687 | 0.558027 |
| 2026 so far | 215 | 0.167914 | 0.167183 | 0.514500 | 0.512364 |

The candidate improved Brier in 2018, 2019, 2022, 2023, 2024, and 2025;
it worsened in 2020 and 2021. The gain is small. In the prior conversation,
the 2023–2026 aggregate was already inspected during an exploratory probe,
so those years cannot honestly be described as untouched confirmation.
The fixed 2026 candidate can be tracked prospectively from this point onward.

The machine-readable runs are `data/processed/dynamic_hfa_tuning.json` and
`dynamic_hfa_tuning_alternates.json`. Repeat with
`python3 src/tune_dynamic_hfa.py --half-lives 5 15 --shrinkages 60 --alphas 0.25 0.5 --caps 40`.
For the selected candidate, run
`python3 src/compare_dynamic_hfa.py --in-season --half-life 5 --shrinkage-k 60 --centered-alpha 0.25 --max-deviation 40`.

## Activation boundary

Keep this candidate isolated from production while the gain remains small and
partly examined before selection. A production switch must preserve an
independent flat reference for future HFA estimates, use the same bonus in
historical Elo and upcoming predictions, persist the bonus used for each game,
and recheck calibration and log loss on prospective games. The current site
must not present the experimental estimate as its live prediction input.
