# Basketball team Elo: fitted on basketball, scored on a season it never saw

Release gate 4's first half. The plan's rule is that no football Elo setting may
be inherited without basketball validation, so nothing here is carried over on
the grounds that it works for football. Every parameter was either measured on
basketball games or chosen by a search over basketball games, and the
configuration football ships was scored alongside the fit so the cost of
inheriting is a number rather than an opinion.

Nothing here publishes. `src/ncaambb/build_ratings.py` writes `db/ncaambb.db`
and makes no commit; the B04 export gate does not exist yet, so no basketball
rating reaches `ui/`.

## What the model is

`src/ncaambb/elo.py`. A team starts at 1500, every rateable game moves both
teams by the same amount in opposite directions, and the move is
`k * multiplier * (result - expected)`. The home team's expectation is the
logistic on the rating gap plus a home advantage, and the advantage is dropped
on a neutral-site game rather than halved.

Three margin layers were available to the search:

| layer | what scales k |
| --- | --- |
| `result_only` | nothing; a one-point win and a thirty-point win move the same |
| `mov` | the winning margin, normalised by the mean winning margin |
| `mov_adjusted` | the margin's surplus over what the rating gap implied |

Every multiplier is normalised so that an average winning margin gives 1.0,
which is what lets k keep one meaning across the three layers and makes the
search's k comparable to football's.

Two quantities are measured, not chosen, and both are measured on the fitting
seasons alone — measuring either on the holdout would leak it:

- **mean winning margin 11.9462 points**, the normaliser above.
- **0.064470 points of margin per point of pregame rating gap**, which is what
  `mov_adjusted` needs to know what a margin was supposed to be.

A game is only learned from if it was played, has both scores, and is between
two Division I teams. A level game and an unplayed game are recorded in the
chain with `counted = 0` and a change of exactly 0, never as a half-win.

## How it was fitted

`src/ncaambb/fit_elo.py`, run in `.github/workflows/cbb-ratings.yml`
(run 39379853150, 2026-10-20). Seasons 2015-2026 were ingested as schedules
only, because a rating reads the final score and the schedule endpoint already
carries it; box scores are not fetched for a fit.

- **Fitted on 2015-2025**, 59,578 rateable games. The search sees only these.
- **Scored on 2026**, 5,752 rateable games, once, at the end, with the chosen
  configuration. The search never saw this season.

The grid is deliberately coarse — k in nine steps from 8 to 45, home advantage
in seven from 0 to 120, offseason retention in six from 0.5 to 1.0, margin
exponent in three — because a fine grid over eleven seasons would be fitting
noise. The search is staged, but k is refitted across the whole grid for each
margin layer, since a layer that scales k changes which k is right.

## What it chose

| parameter | value | football ships |
| --- | --- | --- |
| k | 45 | 45 |
| home_advantage | 100 | 50 |
| offseason_retention | 0.8 | 0.8 |
| margin_model | `mov` | `result_only` |
| margin_exponent | 1.0 | — |
| reference_margin | 11.94622175970996 | measured per sport |
| points_per_elo | 0.06446980254679208 | measured per sport |

In `config/ncaambb_model.json`. `build_ratings.py` reads that file and refuses a
missing parameter rather than defaulting one, because a silent default is how a
parameter nobody fitted ends up inside a published rating.

k and retention landing on football's values is a coincidence worth stating
plainly: it is the search's answer on basketball games, not an inheritance. Home
advantage is twice football's and the margin layer is different, so the
configurations are not the same model.

## What it scored on the held-out season

2026, 5,752 games, none of which the search saw.

| model | Brier | log loss | accuracy | calibration error |
| --- | --- | --- | --- | --- |
| this fit | **0.18656** | 0.54848 | 0.7121 | 0.02908 |
| football's parameters, carried over unchanged | 0.19053 | 0.55936 | 0.7011 | 0.03849 |
| home team at its base rate 0.6370 | 0.23123 | 0.65513 | 0.6370 | 0.00000 |
| coin flip | 0.25000 | 0.69315 | 0.5000 | 0.13700 |

So the fit beats both naive baselines comfortably, and beats football's settings
by 0.004 Brier and about a point of accuracy on a season neither had seen. That
margin is the whole answer to "could we not just use the football numbers?": you
could, and it would be measurably worse, which is a different statement from the
guess either way.

The home team won 63.70% of held-out games, and 100 rating points is roughly
what that base rate asks for at this scale. Football's 50 was fitted against
football's own home-win rate, which is why carrying it over costs accuracy here:
it under-predicts the home side in every game.

### Calibration on the holdout

| predicted bucket | games | mean predicted | observed |
| --- | --- | --- | --- |
| 0.0 | 55 | 0.0699 | 0.0909 |
| 0.1 | 200 | 0.1570 | 0.1650 |
| 0.2 | 298 | 0.2506 | 0.2987 |
| 0.3 | 474 | 0.3533 | 0.3882 |
| 0.4 | 565 | 0.4513 | 0.4761 |
| 0.5 | 638 | 0.5501 | 0.5627 |
| 0.6 | 761 | 0.6516 | 0.6137 |
| 0.7 | 887 | 0.7513 | 0.7001 |
| 0.8 | 1001 | 0.8506 | 0.8182 |
| 0.9 | 873 | 0.9427 | 0.9370 |

The pattern is consistent and worth recording rather than smoothing away: the
model is slightly under-confident below a coin flip and slightly over-confident
above 0.6, which is to say it spreads ratings a little too wide. The largest
single error is 5.1 points in the 0.7 bucket. A probability shown to a reader
should be read with that in mind; shrinking the ratings to fix it was not
attempted, because it trades calibration against Brier and the fit was chosen on
Brier.

## The stored result

`build_ratings.py` replays 2015-2026 in `start_date_utc` order — never anything
week-shaped, after the football archive taught this project that a provider can
number a postseason game week 1 — and stores three things:

- `rating_model`: the configuration as JSON, which seasons it was fitted on,
  which was held out, and what it scored there. A rating cannot be read without
  knowing which model made it or how that model did on unseen games.
- `team_rating`: a team's rating at the end of a season, keyed by model as well
  as season, so two models can sit side by side and never be averaged.
- `rating_game`: the per-game chain. Each game's postgame rating is that team's
  next pregame rating, which is what makes a replay auditable instead of a
  number to take on trust.

65,330 games counted, 0 declined, 4,269 team-seasons rated. In-sample Brier
0.18476, log loss 0.54565, accuracy 0.7165. A second replay in the same run
produced byte-identical row counts and ratings.

Top of 2026 as stored:

| team | rating | games |
| --- | --- | --- |
| Michigan | 2273.7 | 40 |
| Duke | 2211.3 | 38 |
| Arizona | 2169.0 | 39 |
| Florida | 2101.8 | 35 |
| Houston | 2090.1 | 37 |
| UConn | 2068.5 | 40 |
| St. John's | 2056.3 | 37 |
| Illinois | 2054.3 | 37 |
| Purdue | 2046.1 | 39 |
| Gonzaga | 2033.5 | 35 |

## Provider Elo is still never read

`/games` returns `homeTeamEloStart` and `homeTeamEloEnd`. They are not stored,
not in the schema, and a test asserts their absence. Every number above is this
model's.

## Things to know before changing it

- **The fit must not be re-run with the holdout inside it.** The holdout's only
  value is that the search never saw it; folding it in buys a better-looking
  number and no better model. Widen the archive instead.
- **The configuration lives in `config/ncaambb_model.json`, not in code.** The
  ratings workflow writes what the fit chose and now reports a drift from the
  committed file in the run summary, so a fit that prefers something else is
  visible rather than silently replayed.
- **2021 is not a normal season.** 718 games cancelled, 279 never played, 521
  teams appearing instead of the usual 700-odd. It is in the fitting range and
  the retention parameter is what absorbs it; any claim about a 2021 rating
  should say so.
- **`mov_adjusted` can hand a negative base to a fractional exponent** when a
  heavy favourite wins by less than its rating gap implied, and in Python that
  is a complex number rather than an error. The clamp before the power is load
  bearing; a test drives it.
