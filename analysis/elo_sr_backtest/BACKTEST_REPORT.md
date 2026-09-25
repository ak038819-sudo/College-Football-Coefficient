# Success Rate Differential vs Margin of Victory in Elo: Backtest Report

**Experiment only.** Production Elo (`src/build_elo.py`) is untouched. Every
number here is reproducible with `python analysis/elo_sr_backtest/run_backtest.py`
(about a minute; deterministic), which also re-verifies that the harness
reproduces all 60,472 production Elo rows exactly before running anything.

## Bottom line

**Success Rate Differential does not beat the current MOV multiplier as a
replacement.** Across 16 out-of-sample seasons (2011–2026, 11,426 games) the
best SR model, chosen walk-forward, was 0.3% worse on Brier score and 0.3% worse
on log loss than production MOV. It was better in only 5 of 16 seasons, and the
gap is not statistically significant (z = +1.27), so the fair summary is "no
better," not "clearly worse." SR carries real information (every SR model beats
pure win/loss Elo by about 2%), but not more than the scoreboard margin already
provides.

## Setup

* **Current MOV Elo (the baseline):** exactly production. Start 1500, scale 400,
  K = 35, flat +50 home field unless neutral, `M = ln(|margin| + 1) * 2.2 / (2.2 +
  0.001 * winner's pregame edge)`, `dR = K (S - E) M` zero-sum, ties `S = 0.5, M = 1`,
  20% offseason regression to 1500. No FCS games are in the database, so FCS
  opponents never move Elo. No special postseason logic; only the neutral flag matters.
* **Identical start:** every model replays 1980–2007 with production MOV, so all
  enter 2008 (first season of >= 95% sustained coverage) with identical ratings.
  Models differ only in `M` from 2008 on. Predictions are recorded before each update.
* **Direction:** SRDiff is from the winner's side (`SR_winner - SR_loser`), and every
  multiplier is clamped at >= 0, so SR can shrink or grow an update but never
  reverse it.
* **Missing SR** (under 5% of 2008+ games) falls back to production MOV at
  production's update size.
* **K matters, and was handled explicitly.** Production's `M` averages 2.41, while
  `1 + b * SRDiff` averages far less, so at the same K = 35 an SR model takes much
  smaller steps for reasons unrelated to what SR knows. Each SR configuration was
  therefore run twice: at fixed K = 35 (as specified) and at a **normalized K** that
  matches production's average update size, measured on 2008–2010 only (before
  every test season, so no leakage). A K sweep on the leading model confirmed the
  normalized K is at the bottom of its U-shaped curve (see Q9–10).
* **Walk-forward validation:** for each test season T (2011–2026), the configuration
  is chosen by lowest log loss on 2008..T-1 only, then scored on T. The pooled
  results below are entirely out-of-sample.
* **Success Rate:** CFBD game-level offensive Success Rate, garbage time excluded
  (the primary variant; the all-plays variant scored slightly worse). Units
  confirmed as decimals.

## Model comparison (out-of-sample, walk-forward, 2011–2026)

| Model | Brier | Log loss | Accuracy | Chosen parameters | vs MOV (Brier) |
|---|---|---|---|---|---|
| **Current MOV Elo** | **0.18505** | **0.54563** | 0.7118 | production (K 35) | — |
| MOV, K re-chosen walk-forward | 0.18515 | 0.54590 | 0.7117 | K 35 (14 seasons), 40 (2) | −0.06% |
| Pure win/loss Elo | 0.18971 | 0.55689 | 0.7046 | K 90 (10), 80 (6) | −2.52% |
| Linear SRDiff | 0.18579 | 0.54747 | 0.7139 | beta 5 (14 seasons), beta 6 (2) | −0.40% |
| Dead-zone SRDiff | 0.18570 | 0.54721 | 0.7127 | beta 7–8 with 4–5pp zones typical | −0.35% |
| Bounded SRDiff | 0.18584 | 0.54750 | 0.7139 | floor 0.75 / ceiling 2.50 typical | −0.42% |
| Nonlinear SRDiff | 0.18562 | 0.54712 | 0.7149 | exp, beta 4, 0–2pp zone | −0.31% |
| **Best SR model overall** | 0.18564 | 0.54724 | 0.7142 | exp beta 4 in 12 of 16 seasons | −0.32% |
| Best SR model, fixed K = 35 | 0.18694 | 0.55056 | 0.7111 | exp beta 4 | −1.02% |

SR models post slightly higher straight-up accuracy than MOV, but they're worse
on both probability-quality metrics; accuracy was not the selection criterion.
Per-season numbers are in `season_model_comparison.csv`; every configuration's
in-sample score is in `parameter_results.csv`.

## Answers

**1. Earliest CFBD season with Success Rate?** 2001 (62% of games). 2002 is
nearly empty in CFBD's data (4%).

**2. Earliest seasons with sustained >= 90%, >= 95%, >= 99% coverage?** 2007
(90%), **2008 (95%, used for this backtest)**, 2017 (99%). Coverage stays at or
above each level through 2026 from those seasons on. Full table:
`cfbd_sr_coverage.csv`.

**3. Does SRDiff improve on MOV?** No. The best SR model is 0.32% worse on Brier
and 0.30% worse on log loss out-of-sample. The per-game log-loss difference
(SR − MOV) is +0.0016 ± 0.0013 (z = +1.27): not significant, and pointing the
wrong way.

**4. Does it outperform pure win/loss Elo?** Yes, clearly: log loss 0.5472 vs
0.5569, even when pure Elo gets its own walk-forward K. Success Rate is
informative; it just isn't more informative than margin.

**5. Best beta out-of-sample?** For linear SRM = 1 + beta·SRDiff, **beta = 5**
(chosen in 14 of 16 seasons, beta 6 in the other two). For the best overall
form, exponential, **beta = 4** in every season from 2015 on.

**6–7. Is a dead zone useful, and what size?** Marginally at most. Dead-zone
SRDiff beat plain linear by 0.0003 log loss, which is noise. Within that family,
walk-forward mostly chose **4–5pp zones paired with steeper slopes (beta 7–8)**:
ignoring small gaps lets the slope steepen on large ones, but the net gain is
negligible. The best overall model (exponential) settled on small 0–2pp zones.
There's no evidence tiny SR gaps need special treatment.

**8. Are caps/floors useful?** No. Bounded SRDiff (0.54750) is indistinguishable
from unbounded linear (0.54747). When chosen, the bounds are loose (floor 0.75,
ceiling 2.50) and rarely bind.

**9. Linear or nonlinear?** Nonlinear (exponential) is marginally best (0.54712
vs 0.54747), again within noise. A saturating (tanh) curve was tested too and
never chosen. Under the report's own rule (prefer complexity only for meaningful
gains), no form clears the bar.

**10. How consistent is the result across seasons?** The best SR model beat MOV in
5 of 16 seasons (2014, 2015, 2016, 2021, 2025). Its biggest losses came in 2011,
2012 and 2024. There's no stretch of years where SR is consistently better.

*K sensitivity:* varying the leading model's K from 0.7× to 1.3× its normalized
value (56.3) traces a U with the minimum exactly at 1.0×. Even there it scores
0.54694, still worse than MOV's 0.54563. The negative result isn't a K artifact.

**11. When MOV and Success Rate disagree, which predicts better?** Measured on
each team's next game after the disagreement (test seasons):

| Situation | Flagged games | Next games | Log loss MOV | Log loss SR | Better |
|---|---|---|---|---|---|
| Winner lost the SR battle | 2,690 | 4,260 | **0.5538** | 0.5561 | MOV |
| Close score (≤ 7) but SR gap ≥ 10pp | 863 | 1,523 | 0.5620 | **0.5592** | **SR** |
| Blowout (≥ 17) but SR gap ≤ 3pp | 471 | 850 | **0.5569** | 0.5575 | ~tie |
| SR gap ≥ 15pp | 3,084 | 4,943 | 0.5302 | 0.5300 | tie |

The one clear SR win is **close games where one team dominated on Success
Rate**: SR's read of "that team was better than the scoreboard shows" predicted
the next game better. When the winner lost the SR battle, the scoreboard was the
better guide. Individual flagged games are in `disagreement_games.csv`.

**12. Where does SR do noticeably worse?** For road favorites (log loss +0.0045
vs MOV), in conference games (+0.0023), and for home underdogs in the 20–40%
band (+0.0086). It does slightly better at neutral sites, in the postseason, and
in lopsided games (home win probability over 80%). It's also a little **more
overconfident**: favorites were predicted to win 72.7% (SR) vs 72.3% (MOV), and
actually won 71.2%. Details: `robustness.csv`.

**13. How unusual is BYU's +17pp at Colorado State?** Using the supplied +17pp,
larger than 78.5% of games' SR gaps: notable but common, and teams with a +15 to
+20pp edge win 91.6% of the time by a median of 21 points, so BYU's 18-point win
is typical for that edge. **CFBD's own numbers are larger:** +30pp with garbage
time excluded (95th percentile; only 41–42 plays each counted in a blowout) and
+24pp with all plays (90th percentile). CFBD has Colorado State at 36–37%
success, not the 45% supplied.

**14. Does Marshall's −3pp justify changing the Elo update?** No. A 3-point gap is
smaller than 80% of games' gaps (CFBD's −5.6pp is still below the median). Teams
on the wrong side of 0–5pp still win 43% of the time. The best model's dead zone
absorbs most of it: its multiplier is 0.96 on the supplied value, 0.87 on CFBD's.
Given that SR doesn't beat MOV overall, trimming this update isn't supported.

**15. Strongest SRDiff formula to test further?** `SRM = exp(4 × SRDiff_adj)`
with a 1–2pp dead zone and K about 56–59, garbage time excluded. But the
evidence points somewhere else: SR's value showed up **alongside** margin (close
games with lopsided SR), not **instead of** it. The natural next experiment is a
hybrid, `M = M_MOV × SRM^gamma`, walk-forward-tested against production MOV the
same way.

## Case studies (production pregame ratings; SR under the model chosen for 2026)

Both were road wins, so home field is in the expectation. SR uses the 2026
walk-forward choice (`exp`, beta 4, 2pp dead zone, K 59.4).

**BYU 41, Colorado State 23 (2026-09-19):** BYU pregame Elo 1744.6 vs 1328.8,
expected win probability 0.892.

| Source | BYU SRDiff | SR multiplier | SR Elo change | MOV multiplier | MOV Elo change |
|---|---|---|---|---|---|
| Supplied (62% vs 45%) | +17.0pp | 1.82 | +11.75 | 2.52 | +9.59 |
| CFBD, garbage time excluded | +30.1pp | 3.08 | +19.88 | 2.52 | +9.59 |
| CFBD, all plays | +23.7pp | 2.38 | +15.37 | 2.52 | +9.59 |

**Marshall 30, Missouri State 24 (2026-09-19):** Marshall pregame 1459.5 vs
1388.0, expected win probability 0.531.

| Source | Marshall SRDiff | SR multiplier | SR Elo change | MOV multiplier | MOV Elo change |
|---|---|---|---|---|---|
| Supplied (55% vs 58%) | −3.0pp | 0.96 | +26.78 | 1.93 | +31.64 |
| CFBD (both variants) | −5.6pp | 0.87 | +24.17 | 1.93 | +31.64 |

SR's larger K means even a below-1 multiplier still gives Marshall a sizable
gain. The direction never reverses.

## Other findings

* **SRDiff distribution (2008+, garbage time excluded):** the median absolute
  gap is 8.7pp; the 90th percentile is 24pp; the 95th is 30pp. Winners have a
  median edge of +7.1pp and lose the SR battle 24.4% of the time. Win
  percentage rises smoothly with SR edge, from 1.6% at ≤ −20pp to 98.4% at
  ≥ +20pp (`sr_distribution.csv`).
* **Play counts:** Success Rate is much noisier on few plays (team-game spread
  0.139 under 50 plays vs 0.065 at 80+), as expected from sampling. But the
  under-50 bucket is dominated by blowouts, where excluding garbage time removes
  many snaps, so play count and game script are confounded. A play-count
  weighting might help a future hybrid; nothing here justifies adding one now
  (`play_count_reliability.csv`).
* **Garbage time:** excluding it (primary) scored slightly better than all plays
  (log loss 0.54694 vs 0.54728 for the leading configuration).

## Limitations

* Walk-forward selection over about 3,000 configurations favors whatever fits
  recent seasons; the pooled result already reflects that.
* SR fallback games (under 5% since 2008) use MOV, which slightly helps the SR models.
* The margin constants (`mov_c`, `mov_d`) and Elo `scale` were never calibrated;
  a better-tuned MOV baseline would only widen MOV's lead.
* FCS opponents aren't in the data, so games against them never inform either model.

**Follow-up:** the hybrid suggested in Q15 was tested; see `HYBRID_REPORT.md`.
Success Rate *does* improve Elo when it adjusts the margin multiplier instead of
replacing it (0.8% better log loss, 15 of 16 seasons).

Production Elo was not modified. The decision on MOV vs SR remains yours.
