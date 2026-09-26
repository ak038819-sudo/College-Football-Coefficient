# Model validation harness (MODEL-07)

```bash
python src/run_validation.py --db db/league.db
python src/run_validation.py --db db/league.db --compare <earlier_hash>
```

Writes `data/processed/validation/<config_hash>.json`. The filename is a hash of
the model configuration that produced it (comments stripped), so re-running the
same config overwrites the same file with the same numbers and changing any
parameter writes a new one. Two runs can always be diffed.

## Two sections, never one score

**Predictive** — walk-forward Brier score, log loss, calibration error and
sharpness for anything that states a pregame probability (Elo, the hybrid
expectation). Sharpness sits beside calibration because always predicting 50% is
perfectly calibrated and useless.

**Achievement** — CoE diagnostics. CoE is not a forecast, and the spec forbids
tuning it to maximise prediction: a rating optimised to predict results is just
Elo. Each diagnostic asks a question with a defensible expected direction and
carries a `reading` explaining what its value means.

Merging the two into a single score is the failure mode this harness exists to
prevent, so it never computes one.

## Chronology

Folds only ever train on strictly earlier seasons, and `assert_chronological()`
runs before a report is written — a leaking configuration fails loudly instead
of producing a flattering number. Elo's own expectations are out-of-sample by
construction: the engine walks games in order, so each probability was formed
from ratings built only from earlier games.

## What the first run found

Against the current configuration, two structural properties worth knowing:

**Schedule strength barely moves season CoE 2.0.** Correlation between strength
of schedule and season CoE, measured *within* each win total, averages **+0.016**
across 14 win buckets — effectively zero, and it does not hold one sign.

The mechanism: `Game CoE = win_base + alpha x (1 - P)`, where `P` is *this
team's own* win expectation. `P` depends on both teams, so it measures how
unlikely the win was **for that team**, not how strong the opponent was in
absolute terms. Two 9-3 teams — one strong on a brutal schedule, one weak on a
soft one — face similar `P` values per game and earn similar awards. Whether
relative difficulty is the intent or absolute opponent quality should also count
is a modelling decision, not a bug to fix silently.

**Schedule padding is cheap.** Beating a top-20% opponent is worth **1.26x**
beating a bottom-20% one (means 3.07 vs 2.43), so roughly **1.3 soft wins equal
one elite win**. That ratio is structurally capped: with `win_base = 2.0` and
`difficulty_alpha = 2.0` an award ranges 2.0–4.0, so the ratio can never exceed
**2.0** whatever the opponent. Raising `difficulty_alpha` relative to `win_base`
is the lever if more resistance is wanted.

Both are reported, not acted on. The harness produces evidence; changing the
model is a separate, deliberate decision.
