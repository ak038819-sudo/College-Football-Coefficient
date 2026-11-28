# Extraordinary Achievement Norm — research v0.3

This is a **research-only** scorer. It does not produce certified GM norms, does
not change the NBA site, and does not contain historical backtest results.

## Frozen experimental thresholds
EAS = 0.45 R + 0.40 I + 0.15 S.
Qualify only when EAS >= 95, R >= 95, I >= 98, S >= 80,
and regular-season participation >= 70%. One EAN maximum per player.
Ten historical recipients is a diagnostic target, not a quota.

## Input contract
`scripts/backtest_ean.py` accepts one JSON object per line with:
`player_id`, `season`, `scope` (currently `regular`),
`achievement_profile`, `rarity`, `impact`, `sustained`,
`participation`, `data_validated`, and `coverage_tier`.
Only use scores independently validated with versioned source data.

## Not yet implemented
- NBA historical ingestion and stable player identity crosswalk
- Predeclared positive-performance achievement profiles
- Era-relative feature engineering and pace adjustments
- Covariance-aware joint rarity, multiple-testing correction, extreme-tail validation
- Season-specific player-impact percentiles and historical coverage audit
- Game-level elite-impact consistency and postseason scoring
- Model uncertainty, era holdouts, and sensitivity analyses
- Dataset provenance hashes and immutable research runs
- GM norm ledger integration and explicit certification workflow

The ESPN Net Points Player Elo handoff in `docs/nba-player-elo-handoff.md`
is a contemporary player-rating pilot, **not** a validated all-era impact source.
Do not use its displayed Elo as a substitute for historical EAN impact.
Missing or unvalidated data yields INSUFFICIENT_DATA, not a failed norm.

Run tests: `pytest tests/test_nba_ean.py`.
Run candidate evaluation:
`python scripts/backtest_ean.py validated-candidates.jsonl --out ean-report.json`.

A historical backtest is not complete until all eligible seasons are enumerated,
coverage and tail-estimation uncertainty are reported, and sensitivity tests pass.
