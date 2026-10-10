# Fixing College Football

**Predictive rating models, playoff selection, and Monte Carlo simulation for 46 seasons of FBS college football.**

🔗 **Live dashboard:** https://ak038819-sudo.github.io/College-Football-Coefficient/

This project rebuilds college football's postseason around objective ratings instead of polls. It ingests game data from the College Football Data (CFBD) API, rates every team with Elo and an opponent-strength model, selects and seeds the real 12-team College Football Playoff field, simulates the bracket, and publishes everything as an interactive static dashboard. A test suite gates every deploy.

## Highlights

- **Calibrated Elo engine.** K-factor (35), home-field advantage (50), and season-to-season retention (0.8) are tuned with a dedicated calibration script rather than set by hand.
- **Opponent-strength rating (CoE).** An iterative Bradley-Terry-style model weights wins by opponent strength and game phase (regular season, bowl, CFP), with a recency-decayed 5-year rolling window.
- **Hybrid model.** Z-score standardized Elo and CoE are blended, and the blend weight (0.85 Elo) is calibrated. The adopted blend outperforms pure Elo.
- **Small-sample correction.** Early-season ratings are shrunk toward a regressed prior-season rating, with confidence growing with games played. Full seasons are verified byte-identical to the unshrunk output, while partial seasons (2020, the in-progress year) are correctly damped.
- **Leakage-aware validation.** Team-specific home-field estimates are replayed using only each team's *entering-season* data and scored against the flat model with Brier score and log loss.
- **Simulation.** Rating gaps become win probabilities through a logistic model. A Python Monte Carlo engine and an in-browser JavaScript simulator produce matching bracket odds.
- **Engineering.** SQLite schema built for conference realignment, a one-command pipeline, a pytest suite that has caught multiple real logic bugs, and GitHub Actions CI/CD that deploys to GitHub Pages only when tests pass.

**Stack:** Python, SQLite, pandas, NumPy, JavaScript/HTML/CSS, pytest, GitHub Actions, GitHub Pages, CFBD API

## Contents

- [The Rating Models](#the-rating-models)
- [Validation](#validation)
- [The Reform Ruleset](#the-reform-ruleset)
- [Playoff Format](#playoff-format)
- [Data Coverage](#data-coverage)
- [Running It](#running-it)
- [Testing](#testing)
- [Dashboard](#dashboard)
- [Live Features](#live-features)
- [Continuous Integration / Auto-Deploy](#continuous-integration--auto-deploy)
- [Keeping the Current Season Reproducible](#keeping-the-current-season-reproducible)
- [Project Status](#project-status)
- [Site Releases](#site-releases)
- [Database Design Notes](#database-design-notes)
- [Legacy / Archived](#legacy--archived)

## The Rating Models

### CoE v1: opponent-strength rating (source of truth for playoff selection)

`src/build_coefficients.py`:

- Every team starts a season at rating 1.0.
- Over 15 iterations, a winner's rating increases by `opponent_rating × phase_weight` (regular = 1.0, bowl = 2.0, CFP = 3.0). The loser still gains a small `opponent_rating × phase_weight × 0.15`.
- Each iteration renormalizes to keep the scale stable.
- **Early-season damping** is applied at every iteration: `rating = confidence × computed + (1 − confidence) × prior`, where `confidence = games_played / 8` and the prior is the previous season's rating regressed toward the mean (`--confidence-games`, `--prior-regression`). Applying it once at the start or as a one-time post-hoc blend was tried and did not work.
- **Team CoE** is that season's rating. **Conference CoE** is the sum of member teams' ratings, using `team_membership_by_season`.
- **5-year rolling CoE** (team and conference) is a decay-weighted sum over the trailing five seasons (`0.92^age`).

Team seeding and home field use the 5-year rolling team CoE. Conference bid counts use the 5-year rolling conference CoE.

### Elo

`src/build_elo.py` writes the `elo_game_history` table. Parameters are calibrated by `src/calibrate_elo.py` and stored in `config/model_config.json`. Ties use a margin multiplier of 1.0. Production Elo uses a single flat home-field value; team-specific home-field advantage is under evaluation (see [Validation](#validation)).

### CoE 2.0: hybrid Elo + CoE

An additive layer built alongside v1, which is left unmodified (tagged `coe-v1`).

- `src/build_hybrid_coefficients.py` combines Z-score standardized Elo with a frozen entering-season 5-year CoE, and computes a per-game **Game CoE 2.0** (win floor 2, ceiling below 4, OT loss = 1, loss = 0).
- `src/calibrate_hybrid_weight.py` selected `elo_weight = 0.85`, which outperforms pure Elo.
- `src/compare_models.py` compares v1 against v2, excluding team-seasons with fewer than 8 games.
- `src/build_conference_coe2.py` rolls up conference ratings from external games only. It is inspection-only and not yet wired into bid allocation.
- `src/compare_alpha.py` provides qualitative views of the blend, defaulting to the latest *complete* season to avoid early-season noise.

### Win probabilities and simulation

Bracket simulation converts the CoE gap into a win probability with a logistic function, at a temperature fitted against the record (about 4.5), and plays out first round → quarterfinals → semifinals → final. The host's edge, a measured 1.25 CoE points, applies in the first round and nowhere after it. The dashboard's "Simulate the Bracket!" button runs the same logic client-side; `tests/cfp_bracket.test.cjs` drives that copy out of the shipped page so the two cannot drift.

## Validation

- **Calibration scripts** for Elo parameters and the hybrid blend weight, rather than hand-picked values.
- **Model comparison** of v1 against v2 on full team-seasons.
- **Dynamic home-field replay.** `src/compare_dynamic_hfa.py` replays Elo using each team's entering-season HFA estimate (built only from earlier games), then reports Brier score and log loss against the flat replay by season. It refuses a stale or non-flat reference history and writes nothing. A favorable result alone is not treated as a release gate, because the HFA half-life and shrinkage are not yet tuned.
- **Stability tests** confirm that completed seasons are unaffected by the early-season damping settings.
- **Simulation cross-check** between the Python Monte Carlo engine and the browser simulator.

## The Reform Ruleset

The project's founding idea, which still holds: **no Top 25 poll and no
subjective rankings.** A Coefficient (CoE) system replaces the poll for
playoff selection, seeding and home field.

The invented 24-team bracket that idea was first built into is no longer
what the site runs — see the next section. The rest of the original
ruleset (every FBS team in a conference, an 11-game season of 8 conference
and 3 non-conference games, all games within FBS) describes a hypothetical
league's schedule; this project rates games that were actually played.

The full original rules and the coefficient system's early design draft are in `docs/`. The [dashboard roadmap](docs/roadmap.md) covers the scorebug redesign, a live game-day experience, and an installable app.

## Playoff Format

The site runs the **real 12-team College Football Playoff format**, as the
CFP has run it since the 2025 season. It was the invented 24-team bracket
until 2026-10-10. [`docs/cfp-12-team-format.md`](docs/cfp-12-team-format.md)
is the full write-up, including what dropped out and what did not.

- The **five highest-ranked conference champions** take automatic bids;
  **seven at-large bids** go to the strongest teams left.
- **Seeds run straight down the ranking**, 1 to 12 — the 2025 rule change.
- **Seeds 1–4 sit out the first round.**
- **First round:** 5v12, 6v11, 7v10, 8v9, at the higher seed's home.
  Everything after that is at a neutral site.
- **Quarterfinals:** 1 plays the 8/9 winner, 4 the 5/12 winner, 2 the 7/10
  winner, 3 the 6/11 winner, so the top two seeds can only meet in the
  final.

### What stands in for the selection committee

The format needs a national ranking and this project has no committee, so
every ordering it needs — the top five champions, the best teams available,
and the seeds — is the **5-year rolling team CoE**, which this project
already seeds and assigns home field by. That is a strength rating rather
than a resume ranking, and five years moves more slowly than a season, so
a field reads as "the twelve strongest programs with a champion floor"
rather than a guess at the committee. A **champion** is the top team in the
derived conference standings, which is not always the title-game winner.
**Independents** cannot win a conference, so they compete for at-large bids
like everyone else.

### Title odds

Because the bracket is fixed by the seeds, the published odds are the odds
of the bracket on the page. The 24-team bracket had a random pot draw, so
its odds had to be averaged over draws that had not happened — in 2026,
changing only the draw moved Notre Dame's title chance from 12.0% to 27.7%.

### The retained 24-team model

`src/coefficients/select_playoff_field_v2.py` (field),
`draw_playoff_bracket_v2.py` (pot draw), `simulate_bracket.py` (24-team
simulation) and `select_nit_field.py` (the 16-team NIT) are all still in
the repository and still run, with their tests. Nothing live reads them.
Their ruleset — the conference bid table, the pots, the independent
threshold — is in [`docs/coe_spec.md`](docs/coe_spec.md), which now says
so at the top. The NIT has never been exported or shown on the site.

## Data Coverage

- **Games:** 1980–2026, fetched from CFBD with `home_conference`/`away_conference` captured, loaded from `data/raw/games_YYYY.csv`.
- **Conference membership:** 1980–2026. Pre-2014 membership is derived from each team's most common conference in its games that season (`src/derive_membership_from_games.py`, which flags ties). The current season comes from a committed snapshot (see [Keeping the Current Season Reproducible](#keeping-the-current-season-reproducible)).
- **Frozen 5-year CoE** starts in 1985 (1980 + 5).
- **Playoff field, bracket, and simulation** are currently generated for 2014–2026. Extending them back to 1980 is not yet done.
- **Conference standings** are derived locally from the loaded games, using the same tiebreak order as CFBD's records endpoint: conference win% → conference wins → conference losses → overall win% → overall wins → team name. This is a stable proxy ordering, not each conference's official tiebreaker rules (head-to-head procedures, divisions, etc.). See `src/coefficients/derive_conference_standings_local.py`.

## Running It

**One command, full pipeline:**

```bash
python run_pipeline.py                    # builds everything, shows the 2025 playoff field + bracket
python run_pipeline.py --year 2014        # same, for 2014 instead
python run_pipeline.py --force            # wipe db/league.db and rebuild from scratch
python run_pipeline.py --draw-seed 7      # use a specific bracket draw seed
```

This replaces about 30 manual commands. See `run_pipeline.py`'s docstring for exactly what it does, in order.

**Elo and hybrid models** (run after `run_pipeline.py`, in this order, as CI does):

```bash
python src/build_elo.py
python src/build_hybrid_coefficients.py
python src/build_conference_coe2.py
```

**Individual pieces**, to run or inspect one step at a time:

```bash
# Bootstrap the database (teams, aliases, membership) from the backup DB
python src/bootstrap_league_db.py --backup db/league_backup_before_playoff_migration.db --out db/league.db

# Load one season of games
python src/load_games.py data/raw/games_2025.csv

# Patch known conference-membership gaps (Miami (FL), FAU, etc. -- see the script for sources)
python src/patch_known_membership_gaps.py --db db/league.db

# Run the rating model (all seasons at once)
python src/build_coefficients.py

# Conference standings for one season
python src/coefficients/compute_conference_team_records.py --year 2025 --no-validation
python src/coefficients/derive_conference_standings_local.py --year 2025

# Playoff field + bracket for one season
python src/coefficients/select_playoff_field_v2.py --year 2025
python src/coefficients/draw_playoff_bracket_v2.py --year 2025 --draw-seed 1
```

### Outputs

`data/processed/`:

- `team_ratings_by_season.csv`: every team's rating, every season
- `team_coeff_5yr.csv`: 5-year rolling team CoE
- `conference_ratings_by_season.csv`: every conference's rating (sum of member teams), every season
- `conference_coeff_5yr.csv`: 5-year rolling conference CoE

## Testing

```bash
pip install -r requirements-dev.txt
pytest
```

Most tests run against your real `db/league.db` and skip cleanly if it doesn't exist yet (build it with `run_pipeline.py` first). The things worth checking here are properties of the actual data and algorithms, not synthetic toy cases. Coverage includes:

- **Schema** (`test_schema.py`): the full SQL schema chain builds with no conflicts.
- **Bootstrap** (`test_bootstrap.py`): a fresh bootstrap never resurrects known duplicate team entries and includes real late additions. The dead-team list is imported from `bootstrap_league_db.py` rather than duplicated, so the two cannot drift apart.
- **Rating stability** (`test_ratings_stability.py`): a completed season's ratings do not change with the confidence-blending settings (`--confidence-games`, `--prior-regression`).
- **Playoff field** (`test_playoff_field.py`): every season has exactly 8 byes, equal-sized pots, and 24 qualifiers. On its first run, this caught the Pac-12 collapsing to 2 teams by 2024 while still ranking high enough for 3 bids. The independent threshold never displaces a conference champion, even in a synthetic case built to provoke it.
- **Bracket draw** (`test_bracket_draw.py`): no same-conference matchups across 20 seeds per season, the same seed always gives the same pairing, and home field always goes to the higher-CoE team.
- **NIT** (`test_nit.py`): exactly 16 teams per season with no same-conference first-round matchups. This caught a regression where dissolved conferences in the rolling window produced a 17-team field after membership was extended to 1980.
- **Data fetch** (`test_fetch_went_ot.py`): overtime detection from CFBD line-score data.

## Dashboard

A self-contained, publishable HTML dashboard with embedded data (no server and no API calls needed to view it): year selector, team and conference ratings, Elo ratings, team pages with historical Elo charts, playoff field by conference, the bracket, and bracket simulation.

```bash
python build_dashboard.py --draw-seed 1
```

This writes `ui/dashboard.html`. Open it directly in a browser, or upload it anywhere that serves static files. To change the page's design or layout, edit `ui/dashboard_shell.html` (the template) and rerun the build. The `__DATA_JSON__` placeholder gets replaced with a fresh export from `src/export_dashboard_data.py`.

Team and conference logos are date-accurate (a team's logo matches the season shown), built by `src/build_dated_logo_assets.py`.

## Live Features

### Live scores (beta)

The **Live Scores** tab reads `ui/data/live_scores.json`, a display-only snapshot. It refreshes in an open browser tab every minute and marks a feed older than 15 minutes as delayed. It never modifies historical scores, Elo, CoE, or predictions.

The live view combines the scoreboard with team logos, pregame Elo and win probabilities where a matching upcoming game exists, and an Elo sidebar. Cards open a live game detail view using the CFBD game ID, and team logos and names still link to team pages. The game detail view also refreshes while open. Games outside the historical export (such as FCS opponents) still have a live detail view.

The Home tab shows the live scoreboard with the Elo, conference, and poll rankings beside it; `#section=live` remains available for older links. Team Elo and win chances appear directly on game cards. If a stored pregame prediction is unavailable, both are estimated from the latest published Elo and the configured home-field value. Completed games show the official Elo change once the archive has processed them; until then, a triangle marked `≈` is a result-only estimate, not the full postgame model.

During live or recently completed games, **If scores hold · Elo** is an illustrative result-only scenario. For each game with a matching pregame prediction, it applies `K × (current result − pregame expectation)` to both teams, then reorders the rating board. Tied scores count as ties. It does **not** use score margin or the postgame success-rate performance multiplier, so it is not an official Elo calculation. Games without a matching prediction do not affect it. Published rankings and historical Elo update only through the normal data build.

Home and Live Scores list featured matchups first, using Elo ranks, both teams' rating strength, and how close the pregame win probability is. Ties fall back to kickoff time, then game ID. This ordering is an editorial heuristic, not a claim that the game will be close or consequential, and results do not change it. Stadium names come from CFBD's schedule and live scoreboard when available. Older committed schedule CSVs have no venue column, so those names fill in on the next CFBD data refresh.

The Home scoreboard moves to the next scheduled week when the live feed still contains an older slate. It uses published upcoming-game predictions in that gap and switches back to live cards as soon as the feed reaches that week. Ongoing games remain visible.

The server-side refresh also requests CFBD's weekly `/games/players` box scores for games underway or final in the current scoreboard. Player lines are published in the same snapshot and appear on game details when available. The API key stays in the Actions secret, and a player-data failure leaves the score update working. Box-score availability and timing vary by game.

### Home-field advantage (analysis only)

**Standings → Home-Field Advantage** lists current team-specific HFA estimates, including each team's neutral-field expected-win ratio, an illustrative Elo-point equivalent, and its home-game sample. These are **analysis only**: production Elo and win probabilities still use the single flat `elo.home_field` setting. The automated build refreshes only the current estimates after Elo; `python src/build_hfa.py` remains available for the full historical research tables. An export made without the HFA table shows an explicit unavailable state rather than invented values.

To validate team-specific HFA in Elo without changing the live model, run these on a populated local database:

```bash
python src/build_elo.py
python src/build_hfa.py                          # full historical estimates, not --current-only
python src/compare_dynamic_hfa.py --from-year 2018
```

Neutral-site games get no bonus, and a bounded HFA-to-Elo conversion falls back to the flat value. See [Validation](#validation) for how results are judged.

### Refresh schedule

The full dashboard refresh runs every Sunday at **6:00 p.m. America/Chicago** (CDT or CST as applicable). It fetches fresh game data, rebuilds Elo and upcoming predictions, tests the new data, and publishes the result. The existing Thursday-night refresh remains in place. GitHub Actions schedules can start late, and the Home fallback can use already published next-week games while the rebuild runs.

`.github/workflows/live-scores.yml` fetches CFBD's `/scoreboard` on a game-window schedule (UTC Friday evening, Saturday, and early Sunday, August–December) and can also be run manually from Actions. CFBD's live scoreboard requires a subscribed API key: set `CFBD_API_KEY` in the repository's Actions secrets. The key is used only by the workflow and is never sent to visitors. Until it is activated, the page says the feed is inactive. To expand the schedule for weekday or January games, edit the workflow's cron entries. Scheduled jobs may be delayed, so treat the timestamp as authoritative. A failed fetch keeps the last good snapshot, and the site labels stale data.

## Continuous Integration / Auto-Deploy

`.github/workflows/ci-and-deploy.yml` runs the full test suite on every push and pull request. On a push to `main`, a rebuild-and-deploy job runs only after tests pass. It runs the pipeline, builds Elo and the hybrid models, rebuilds the dashboard from committed source, and pushes the result, which GitHub Pages then deploys within a minute or two.

Because deploy is gated on tests, a failing test blocks every later change from reaching the live site, even though the code still lands in the repo. If the site looks stale, check the Actions tab first.

The deploy job does not need the CFBD API (only committed source is used), which is why the membership snapshot step below matters. The rebuild is deterministic given the same input data, so an unrelated change (such as a docs edit) produces byte-identical output, git has nothing new to commit, and no extra push or loop occurs.

## Keeping the Current Season Reproducible

Historical membership is reproducible from git alone. The **current season is different**: its membership comes from a live CFBD fetch with your own key. Without a committed copy, a fresh `--force` rebuild (or CI) would have that season's games but no membership, and its playoff field wouldn't build.

After fetching fresh membership for the current season, export a snapshot and commit it with your games CSV:

```bash
python src/fetch_cfbd_team_memberships.py 2026 2026   # your existing manual step
python src/export_membership_snapshot.py --year 2026   # snapshot it
git add data/raw/membership_2026.csv data/raw/games_2026.csv
git commit -m "Update 2026 season data"
git push
```

`run_pipeline.py` automatically loads any `data/raw/membership_*.csv` it finds (via `load_membership_snapshot.py`), so both a local `--force` rebuild and CI pick it up with no further steps.

## Project Status

✅ Historical game and membership data, 1980–2026
✅ CoE v1 opponent-strength rating with early-season damping
✅ Calibrated Elo engine and CoE 2.0 hybrid rating
✅ The real 12-team CFP field: 5 champion bids, 7 at-large, straight seeding
✅ Full bracket simulation through the final (Python Monte Carlo + in-browser simulator)
➖ Retained but not live: the 24-team field, its pot draw and the 16-team NIT
✅ Team pages with historical Elo charts, scheduled-game predictions, and a home dashboard
✅ Live scores, weekly automated refresh, and test-gated CI/CD

### Not yet built

- Playoff, bracket, and simulation generation for 1980–2013 (ratings already cover these years)
- Lower-division (FCS) opponent ratings and new-program initialization for Elo
- Wiring CoE 2.0 conference ratings into bid allocation
- Team-specific home-field advantage in production Elo (currently analysis only)
- Real-vs-model historical champion comparison view
- NIT winner's bonus bid the following year
- Official conference standings via a live CFBD pull (current standings are a locally computed substitute; see [Data Coverage](#data-coverage))

## Site Releases

The published site is **v0.0 · Alpha**. The current local update is **v0.1 · People and Places of the Game**, which adds the game finder, stadium pages, home-field standings, a pregame dynamic-HFA replay, and archived player box scores where available. Live Elo still uses a flat home-field value while the in-season replay is validated. v0.1 has not been published. The in-progress **v0.1.1 · Players and Coaches** update adds player and head-coach identity and CFBD roster and coaching ingestion; see [player and coach pages](docs/people-pages.md) for its coverage limits. See the [release history](docs/CHANGELOG.md).

## Database Design Notes

`team_membership_by_season` is keyed by `(team_id, season_year)`, which supports realignment (a team's conference can change from year to year). Conference standings, bid allocation, and the conference-level rating rollup all depend on it.

The pipeline is deterministic given a fixed `--draw-seed` and unchanged database state. The same seed produces an identical bracket, and a different seed produces a different but still valid one.

## Legacy / Archived

The project originally used a discrete win/OT-loss/loss point system, drafted in `docs/coe_spec.md`. That system and everything built on it (bounty multiplier, conference champion derivation, Year 1/Year 2 qualifier and draw scripts) is archived under `archive/discrete_coe_system_2026-09/` for reference. It is fully superseded by the models above. Its SQL tables (`team_coefficient_by_year`, `conference_coefficient_by_year`, `playoff_field_by_year`, etc., still defined in `sql/`) are no longer written to by anything in `src/`.ull data refresh remains separate.
