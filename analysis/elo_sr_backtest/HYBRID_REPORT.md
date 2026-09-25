# Follow-up: Success Rate *alongside* Margin of Victory

**Experiment only.** Production Elo is untouched. Reproduce everything with
`python analysis/elo_sr_backtest/run_hybrid.py` (deterministic; it re-verifies
that the harness matches production exactly before running).

## Bottom line

**Yes: Success Rate improves Elo when it adjusts the margin multiplier rather
than replacing it.** And the improvement is **not** just a side effect of
reshaping the margin curve: Success Rate adds its own, statistically clear
information on top of a re-tuned margin curve, in 15 of 16 out-of-sample seasons.

| Model (out-of-sample, walk-forward, 2011–2026, 11,426 games) | Log loss | Brier | Accuracy |
|---|---|---|---|
| Current production MOV | 0.54563 | 0.18505 | 0.7118 |
| SR as a **replacement** for MOV (previous report) | 0.54724 | 0.18564 | 0.7142 |
| Production MOV × exp(γ·SRDiff) | 0.54208 | 0.18354 | 0.7152 |
| Re-tuned margin curve, **no SR** | 0.54414 | 0.18438 | 0.7149 |
| **Re-tuned margin curve × exp(γ·SRDiff)** | **0.54129** | **0.18317** | **0.7180** |

Overall: **0.8% better log loss and 1.0% better Brier score than production.**
That's modest in absolute terms, which is typical for a refinement to a mature
Elo model, but consistent and well outside noise.

## Why this is SR, not just a better margin curve

Production's margin constants were never calibrated, so any extra flexibility
might help. To separate the two effects, the re-tuned margin curve,
`ln(|margin|+1)^p × c / (c + 0.001 × winner's pregame edge)`, was given its own
walk-forward selection over p and c, with **no** Success Rate. Its grid includes
production's own curve (p = 1, c = 2.2), so it could always fall back to production.

| Per-game log loss change (out-of-sample) | Change | Std. error | z | Seasons better |
|---|---|---|---|---|
| Re-tuned margin curve vs production | −0.0015 | 0.0008 | −1.89 | — |
| **Success Rate on top of re-tuned curve** | **−0.0028** | 0.0007 | **−3.89** | **15 of 16** |
| Both together vs production | −0.0043 | 0.0010 | −4.38 | — |

**Success Rate is the larger and more reliable of the two gains.** The steeper
margin curve helps too, though its evidence is borderline on its own. Notably,
once SR is included, walk-forward prefers a *less* steep margin curve (p =
1.25 vs 2.0 without SR): SR does part of the work a steeper curve was
approximating.

## How it was tested

Same protocol as `BACKTEST_REPORT.md`, same harness:

* All models share identical ratings entering 2008 (1980–2007 replayed once with
  production MOV). They differ only in the multiplier from 2008 on.
* `M = M_MOV × A`, where A is the SR adjustment. Every family includes γ = 0,
  which was verified to reproduce production **exactly**, so walk-forward could
  always fall back to production if SR added nothing.
* K normalized to production's average update size on 2008–2010 only (γ = 0 gives
  exactly K = 35). Fixed K = 35 results are reported too.
* Walk-forward: choose on 2008..T-1, score on T, for T = 2011–2026.
* SRDiff is from the winner's side, garbage time excluded. Games without SR use
  the margin multiplier alone.

### Adjustment forms tested

| Form | A | Out-of-sample log loss | Chosen |
|---|---|---|---|
| exponential | exp(γ·SRDiff) | 0.54208 | γ = 2.25–2.75, no dead zone |
| linear | 1 + γ·SRDiff | 0.54200 | γ = 3–4.5 with a 2–4pp dead zone |
| close-game weighted | exp(γ·SRDiff·e^(−margin/τ)) | 0.54437 | τ = 14 |

Weighting Success Rate toward close games, the hypothesis suggested by the
first report's disagreement analysis, **did worse** than applying it uniformly.
Exponential and linear are indistinguishable; exponential is used below because
it can never go negative.

**Not a K artifact:** the exponential production-curve hybrid still beats
production at a fixed K = 35 (log loss 0.54320 vs 0.54563).

### Parameter stability

The choices barely moved across the 16 walk-forward seasons. For the re-tuned
hybrid: γ = 2.0–2.5 in 14 of 16 seasons, favorite correction c = 1.1 in 15 of
16, margin exponent 1.25–1.5 in 14 of 16. On production's own curve, γ stayed
between 2.25 and 2.75 in 12 of 16.

## Robustness (production-curve hybrid vs production)

| Split | Games | Log loss change |
|---|---|---|
| All games | 11,426 | −0.0035 |
| Favorite at home | 6,517 | −0.0046 |
| Favorite on the road | 4,023 | −0.0005 |
| Neutral site | 886 | −0.0096 |
| Conference | 7,738 | −0.0028 |
| Nonconference | 3,688 | −0.0051 |
| Postseason | 585 | −0.0104 |
| Home win prob < 20% | 1,050 | +0.0016 |
| Home win prob 20–40% | 2,125 | +0.0003 |
| Home win prob 40–60% | 2,675 | −0.0039 |
| Home win prob 60–80% | 2,960 | −0.0076 |
| Home win prob > 80% | 2,616 | −0.0037 |

Better or level in every split except games with big **home underdogs**, where
it's very slightly worse. Unlike the replacement model, it doesn't lose on road
favorites or conference games. (`hybrid_robustness.csv`)

## Case studies (production-curve hybrid chosen for 2026: γ = 2.25, K = 27.9)

Production pregame ratings; both games were road wins.

| Game | SRDiff source | SRDiff | Multiplier: MOV → hybrid | Elo change: production → hybrid |
|---|---|---|---|---|
| BYU at Colorado State | supplied | +17.0pp | 2.52 → 3.70 | +9.59 → +11.22 |
| | CFBD, garbage time excluded | +30.1pp | 2.52 → 4.97 | +9.59 → +15.08 |
| | CFBD, all plays | +23.7pp | 2.52 → 4.30 | +9.59 → +13.04 |
| Marshall at Missouri State | supplied | −3.0pp | 1.93 → 1.80 | +31.64 → +23.60 |
| | CFBD (both variants) | −5.6pp | 1.93 → 1.70 | +31.64 → +22.28 |

The hybrid does exactly what the original hypothesis described: BYU's dominant
win counts for more, and Marshall's win, where it lost the Success Rate battle,
counts for less. Both still gain Elo. (The lower K offsets the larger average
multiplier, so typical updates stay production-sized.)

## Candidate formula

    M = ln(|margin| + 1)^1.25  ×  1.1 / (1.1 + 0.001 × winner's pregame edge)  ×  exp(2.5 × SRDiff_winner)
    ΔR = K × (S − E) × M,   K ≈ 22

with the same formula minus the exp(...) term when Success Rate isn't available.

## Before adopting it (decision is yours)

Production Elo was not changed. Adopting this would mean:

1. **A new production data dependency.** The weekly refresh would also fetch
   per-game Success Rate (`fetch_game_success_rates.py` already does this, with
   caching) and load it before Elo runs.
2. **Handling the pre-SR era.** Production replays from 1980, and SR only exists
   from 2001 (reliably from 2008). Earlier games would use the margin-only form,
   and the K interplay across that boundary needs its own check.
3. **Recalibrating together.** The home-field finding (national 64 Elo points
   vs the flat 50), Elo's never-calibrated `scale`, and this multiplier interact;
   they're best tuned jointly with this same walk-forward harness rather than
   one at a time.
4. **Everything downstream shifts slightly:** the Elo tab, win probabilities,
   the hybrid rating, and team-page history.

## Limitations

* About 380 configurations were searched in total (258 hybrid forms, 126 in the
  decomposition grids); walk-forward selection protects the reported numbers,
  but the grids were partly informed by the first report.
* The gain is 0.8% in log loss, real and consistent, but small per game.
* FCS games aren't in the data, so they inform neither model.

Files: `run_hybrid.py`, `hybrid_parameter_results.csv`,
`hybrid_walk_forward_results.csv`, `hybrid_season_comparison.csv`,
`hybrid_robustness.csv`, `hybrid_case_studies.csv`, `hybrid_results_summary.json`.
