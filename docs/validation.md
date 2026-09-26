# Model validation harness (MODEL-07)

One command scores every model this repository predicts with, and leaves a
machine-readable record behind:

```bash
python src/run_validation.py                      # whole history
python src/run_validation.py --from-season 2001   # past the Elo burn-in
```

Outputs, both rewritten in full on each run:

| file | what it holds |
|---|---|
| `data/processed/validation_report.json` | every metric, overall and per season, plus the calibration table and the rank diagnostics |
| `data/processed/validation_by_season.csv` | the per-model, per-season rows, for a chart or a spreadsheet |

The scheduled refresh runs it after building the rating tables and commits both
files, so the record accumulates over time rather than existing only on whoever
last ran the script. It reads the database and never writes to it, and it never
modifies `config/model_config.json`.

## Where the metrics live

`src/validation.py` owns them. Before MODEL-07, Brier was implemented three
times — once in `calibrate_elo.py`, a near-copy in `calibrate_hybrid_weight.py`,
and a third version inside `backtest_performance_layer.py` that also had log
loss and a calibration summary. Those three now call the shared module, so
"Brier" and "calibration error" mean one thing across the repository and a
variant's score in the backtest matrix is directly comparable to the report's.

The refactor is numerically identical where it should be: `calibrate_elo.py` on
the shipped config returns `0.17933395074551256` before and after, to the last
bit.

One behaviour did change on purpose. `backtest_performance_layer.py` used to
score both sides of every game. Brier, log loss and accuracy are symmetric and
came out the same either way, but the *binned* calibration error did not — the
bin holding the favourites is not the mirror of the bin holding the underdogs —
so its calibration column was not comparable with anything else. It now scores
the home side only, like everything else. Its Brier and mean |ΔElo| are
unchanged; its calibration column moved (0.00546 → 0.02457 for production MOV),
and the new number is the one that means what it says.

## What "walk-forward" rests on

Nothing here trains a model, so the harness cannot enforce
out-of-sample-ness — it scores predictions that were already made. The
guarantee comes from how they were made, and in this repository it holds:

* Elo's expectation for a game is formed from ratings built only from earlier
  games.
* The hybrid expectation adds a *frozen* prior-season CoE — completed seasons
  only, never the season in progress.
* The xSRDiff curve stores one fold per season, fitted strictly on earlier
  seasons.

So a single chronological pass is already walk-forward, which is the argument
`calibrate_elo.py` has always made. What the harness adds is the ability to
*see* it: every metric is broken down by season, so a model that looks fine
overall while drifting shows up as a row instead of hiding inside a mean. The
early seasons where every team still sits at a flat 1500 are visible the same
way — which is why `--from-season` is a flag you pass and a field recorded in
the report, not a silent burn-in rule.

## The metrics, and what each one catches

| metric | what it adds |
|---|---|
| `brier` | mean squared error. The headline number the calibration scripts already optimise. |
| `log_loss` | punishes confident mistakes far harder. A model that is usually right but occasionally certain and wrong looks fine on Brier and bad here. |
| `accuracy` | share of games the favourite won. Reported because people ask for it, not because it is a good target — it throws confidence away. |
| `base_rate` | observed rate of the scored side winning. Context for the rest. |
| `brier_skill` | improvement over always predicting the observed base rate. 0.0 means the model has added nothing. |
| `calibration_error` | do games predicted at 70% happen 70% of the time? `calibration` in the JSON is the bin table behind this one number, with a signed gap per bin, so over- and underconfidence at the extremes stay legible. |

A model can be perfectly calibrated and useless (predict the base rate for
everything), so no single column decides anything.

## Reading the output

On the committed data, whole history:

```
model                       n     Brier   log loss     acc    skill    calib
hybrid                 27,296   0.17776    0.53157   0.727    0.258  0.02170
elo_on_hybrid_games    27,296   0.17860    0.53359   0.726    0.255  0.02315
elo                    30,236   0.17933    0.53630   0.724    0.250  0.02457
baseline_home_rate     30,236   0.23904    0.67429   0.597    0.000  0.00000
baseline_coin_flip     30,236   0.24840    0.69315   0.500   -0.039  0.09679
```

Note `elo_on_hybrid_games`. The hybrid layer needs a frozen prior-season CoE,
so it has no prediction for the earliest seasons and covers 27,296 of 30,236
games. Comparing its Brier against `elo`'s full-history Brier would compare two
different populations; the restricted row is the like-for-like one, and the
report only emits it when the two sets really do differ.

`baseline_home_rate` is harder than it looks: it is handed the observed home-win
rate of the very games it is scored on, a fact no real forecaster would have
known in advance. Beating it means beating a baseline with hindsight.

## The rank diagnostics

CoE measures *achievement*, and the design doc is explicit that optimising it
for prediction accuracy is a category error — so it is not scored with the
metrics above at all. What can be checked is whether it behaves like a coherent
measure:

* **v1 vs 2.0 agreement** per season. Currently a mean rank correlation of
  **+0.833** across 41 seasons, over 5,199 eligible team-seasons. A
  disagreement is information, not a bug — the spec says so directly — so this
  is reported, not asserted.
* **Year-over-year stability** of each measure. Neither extreme is the goal: a
  measure that reshuffles the table every year is noise, one that never moves is
  not measuring the season.
* **Association with the schedule a team actually faced**, from mean *pregame*
  opponent Elo. Currently **+0.208**. Read the sign first: a negative value
  would mean the measure rewards farming weak opponents, which is the failure
  mode the design doc names.

Team-seasons with fewer than 8 games played are excluded, for the reason
`compare_models.py` documents at length — a team with one game produces a
spectacular and entirely spurious disagreement. The threshold is
`--confidence-games`, and it matches the one CoE v1's own early-season blend
uses.

## What this does not do

It does not choose parameters, adopt a recommendation, or switch anything. The
calibration scripts still print recommendations and still leave
`config/model_config.json` alone, and the live playoff model still runs entirely
on CoE v1. The harness exists so that a later decision to change any of that can
cite a number measured the same way twice.
