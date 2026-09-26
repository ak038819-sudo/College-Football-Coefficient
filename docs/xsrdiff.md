# xSRDiff / SR+ performance layer (EXP-03)

Replaces the margin-of-victory multiplier in the Elo update with a
strength-adjusted Success Rate residual. Implemented; **dormant until per-game
Success Rate is loaded**, because that data is not in this repository yet.

## What it computes

    EloDiff*  = venue-adjusted pregame Elo_team - venue-adjusted Elo_opponent
    xSRDiff   = a + b * EloDiff*         fitted, never assumed
    SRDiff    = SR_team - SR_opponent    observed, for THAT game
    SR+       = SRDiff - xSRDiff
    M         = clamp(1 + beta * SR+, m_min, m_max)
    dElo      = K * (S - E) * M

M is computed once per game from the **winner's** SR+. SR+ is antisymmetric, so
a per-team M would give the two sides different multipliers and break Elo's
zero-sum property. It also makes the guide's acceptance scenarios come out
right: a favourite that wins while underperforming has SR+ < 0, so M < 1 and it
still gains, just less; an underdog that loses while playing nearly even softens
its own loss without it ever becoming a gain.

## Why it is dormant

`team_season_advanced` holds **season** Success Rate. xSRDiff needs the Success
Rate of the game being rated. A season figure includes that game and every game
after it, so using it would leak the future into a pregame expectation — exactly
what the guide forbids. Rather than approximate, the layer records that it had
no data and falls back.

With no per-game Success Rate the engine produces **bit-identical ratings to the
margin-of-victory model**, and `elo_game_history.performance_model` reads `mov`
for every game. That is the safety property: merging this changes nothing until
the data arrives.

## Turning it on

```bash
export CFBD_API_KEY=...                              # same key the season fetch uses
python src/fetch_cfbd_game_advanced.py 2001 2026     # -> data/raw/game_advanced_YYYY.csv
for f in data/raw/game_advanced_*.csv; do python src/load_game_advanced.py "$f"; done
python src/fit_xsrdiff.py --buckets                  # inspect the shape first
python src/fit_xsrdiff.py                            # -> data/processed/xsrdiff_model.json
python src/build_elo.py                              # now uses xSRDiff for 2001+
python src/backtest_performance_layer.py --from-season 2001
```

CI does the fetch/fit steps automatically on its scheduled refresh.

## Before trusting the numbers

`beta`, `m_min` and `m_max` in `config/model_config.json` are **placeholders, not
calibrated values**. Set them from `backtest_performance_layer.py`'s
walk-forward output, not from a handful of memorable games. That script's
metrics now come from the shared validation harness (`docs/validation.md`), so a
variant's score there is directly comparable to `run_validation.py`'s report on
the live models.

Watch the scale. MOV's multiplier averages well above 1; xSRDiff's sits near 1.
On a synthetic rehearsal the mean M was 2.43 under MOV and 1.07 under xSRDiff,
which compressed the whole rating range. **`k` almost certainly needs
recalibrating alongside any switch** — otherwise a Brier comparison is measuring
a quieter engine, not a better one.

## Point-in-time correctness

`xsrdiff_model.json` stores one fold per season, each fitted only on seasons
strictly before it, and `build_elo.py` selects the fold by the game's season. A
season with fewer than `MIN_TRAINING_ROWS` prior rows gets no fold and falls
back, rather than borrowing the all-data fit and calling the result a backtest.
`global_fit` exists only for forecasting games that have not been played.

## Where the values live

`elo_game_history`: `elo_diff_adjusted`, `success_rate_team`, `success_rate_opp`,
`sr_diff`, `xsr_diff`, `sr_plus`, `performance_model`. Exported per game to
`ui/data/details/<season>.js` and shown on a completed game page under
"Efficiency vs. expectation". Missing values are NULL and render as N/A, never 0.
