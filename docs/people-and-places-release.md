# v0.1 · People and Places of the Game

The published v0.0 Alpha stays online while this branch is prepared.

## Ready in the local branch

- Stadium Explorer and verified venue links, with unknown locations left unknown.
- Current home-field evidence table and a point-in-time dynamic Elo replay.
- A frozen shadow forecast tracker records flat and candidate probabilities
  before kickoff, then scores them after a final without revising the forecast.
- Games finder, hourly checks for newly finished games during the regular season,
  retry of late per-game efficiency, and season-scoped player box-score archives.
- Historical game pages show player lines where the archive has them. Older
  seasons need a year-by-year CFBD backfill; no missing statistics are invented.

## HFA release gate

`python3 src/build_elo.py` creates the flat reference. Run
`python3 src/compare_dynamic_hfa.py --from-year 2018 --in-season` to compare
team bonuses recalculated before each game date against the current model.
The provider reads neutral-field expectations from the **flat** reference,
weights past actual and expected wins by recency, shrinks small samples, and
never sees a game on the prediction date or later. Neutral-site games receive
no bonus from `run_elo`.

On the local database (6,239 games, 2018–2026), the current untuned parameters
score 0.187292 Brier and 0.552001 log loss versus 0.186831 and 0.551177 for
flat Elo. This is insufficient to switch production predictions. Tune using
earlier seasons and track the fixed choice prospectively; keep a
separate flat reference so team bonuses never feed their own estimator. Update
the upcoming-game prediction and display pipeline together when activation is
validated. A centered, capped candidate modestly improved exploratory scores;
see [the tuning report](dynamic-hfa-tuning.md). Until then the table and replay
remain experimental.

## Before publishing

1. Fetch the current season player archive with a CFBD key and backfill
   **2004–2025** with `python3 src/fetch_cfbd_players.py 2004 2025` where the
   source supplies records. CFBD documents 2004 as the first box-score season;
   1980–2003 pages say player lines are unavailable. Rebuild and inspect
   `data/processed/game_coverage.json` by season.
2. Run the full data workflow and confirm final-score handoff, advanced stats,
   player lines, and stadium IDs against a few recent games.
3. Confirm the dashboard UI on phone and desktop, then publish v0.1 only after
   the model and data checks above pass.

The frozen candidate lives in `config/hfa_shadow.json`. The scheduled full
build runs `src/track_hfa_shadow.py` after Elo and commits its forecast ledger
in `data/processed/dynamic_hfa_shadow.json`. Only games within seven days of
kickoff enter the ledger. This does not change live Elo or displayed win chance.
