# Live win probability, and the drive archive it will be fitted on

The scoreboard shows a **pregame** win chance: the Elo expectation that existed
before kickoff. It is right once and then never moves. A viewer watching a
21-point favourite trail by ten in the fourth quarter is being shown the number
that was true three hours earlier.

`src/win_probability.py` answers the live question instead — given the pregame
ratings, the score now, and how much of regulation is left, how often does the
home team win? — and `src/fetch_cfbd_drives.py` fetches the data that will
finish calibrating it.

## The model

With `f` the fraction of regulation left, `m` the current margin (home minus
away) and `d` the venue-adjusted pregame Elo gap:

```
expected final margin  E = m + points_per_elo * d * f
spread of what is left S = sigma * f ** time_exponent

P(home) = P(final margin > tie band) + P(final margin inside it) * overtime_home_win
```

The tie band is what keeps the last minute honest. A continuous normal gives a
level game zero probability, so without it a team ahead by a point with no time
left and a team ahead by three read the same.

## The parameters, and which one is a guess

Fitted by `src/calibrate_win_probability.py` against every completed game from
1980 to 2026 that has a pregame Elo gap — 30,408 games. Values live in
`config/model_config.json` under `live_win_probability`; the full run is in
`data/processed/win_probability_calibration.csv`.

| Parameter | Value | Where it comes from |
| --- | --- | --- |
| `points_per_elo` | 0.052627 | Least squares of **regulation** margin on `elo_game_history.elo_diff_adjusted`. About 19 Elo points per point of margin. |
| `sigma` | 16.651 | That fit's residual spread, in points. |
| `tie_half_width` | 1.142 | Solved so the model's tie mass equals how often games really end regulation level (4.39% in the overtime era). |
| `overtime_home_win` | 0.5099 | Counted: the home team's share of the 812 games tied after regulation. |
| `time_exponent` | 0.5 | **Assumed.** A plain random walk. Nothing in this database can fit it yet. |

Three things are worth saying plainly about that table.

**Regulation margin, not final margin.** A game that reached overtime had a
regulation margin of exactly zero, whatever the final score says. Fitting on
final margins would teach the model that tie games end 31-28.

**The fit is on margins, not on outcomes.** Fitting win/loss outcomes
identifies only the *ratio* of `points_per_elo` to `sigma`, and the live number
needs both: it divides a margin by a spread that shrinks as the clock runs down.

**The model is not trying to beat the ratings.** Fitted on 1980-2017 and scored
on 2018-2026, which the fit never saw, its **pregame** probabilities give Brier
0.18737 against the rating engine's own expectation at 0.18708. With no score on
the board it should agree with the pregame chance printed beside it, and it
does; a test pins the mean gap below 0.02.

One measured aside: the margin fit's intercept is **+1.25 points**, so the flat
50-Elo home-field term the rating engine applies understates the real home edge
by about a point and a quarter. That is a note about `elo.home_field`, not a
change made here.

## What the model does not know

Possession, field position, down, distance and timeouts. The published
scoreboard feed carries the period, the clock and the score, so those are the
inputs. A tie game with two minutes left reads the same whether the home team
has the ball on the opponent's 20 or has just punted.

Overtime, while it is being played, gets **no live number at all**. Regulation
seconds remaining is zero throughout it, which is the right answer for a
finished game and the wrong one for a game still going: a team that has just
scored in the first overtime period would read as a certainty while its
opponent still has a possession coming. The scorebug keeps showing the pregame
chance until the game is final. A game that is already final is decided by its
score however many overtimes it took.

`time_exponent = 0.5` is the sharper limitation, and it bites exactly where
viewers are watching. At 0.5 the spread of the remaining margin with a minute
left is about two points, so a favourite trailing by 3 reads around 13% — too
low, because late-game football is more volatile per second than early-game
football (clock stoppages, hurry-up, onside kicks). A smaller exponent widens
the late spread. Fitting it needs the score at known moments *during* games.

`games.home_line_scores` exists as a column but every committed season CSV
predates it, so the archive holds no per-quarter scores. Hence the drive
archive.

## The drive archive

`src/fetch_cfbd_drives.py` → `data/raw/drives/<year>.csv.gz` →
`src/load_drives.py` → `game_drives`.

Every drive carries the period, the clock and the running score at **both** ends,
so each drive boundary is an in-game state with a known outcome — thousands of
them per season, at every point on the clock. Two readers:

* a game page's drive list and scoring summary (the story of the game, which
  the site has never shown);
* the `time_exponent` fit.

Scores are stored from the **home** team's point of view. CFBD reports running
totals as offense/defense, which means the same number changes meaning from
drive to drive, so the loader resolves it once with `is_home_offense` and leaves
no reader to get it wrong. A drive whose source never said which side was home
keeps NULL scores rather than a guess. `seconds_remaining` is regulation
seconds left at the end of the drive: a drive that ended in overtime is 0, and a
drive with an unreadable clock is NULL — never 0, which is a real and different
state.

Nothing here is a rating input. The Elo and CoE builders never read
`game_drives`, so a gap in drive coverage can change what a game page shows and
what the live fit is measured on, but can never change a rating.

### Backfilling it

CFBD's drive data begins in **2001** (the play-by-play era); earlier seasons are
skipped without calling the API. Run `.github/workflows/backfill-drives.yml`
(Actions → Backfill drives → Run workflow) with a season range. It commits one
season at a time with `[skip ci]` and stops at the first season it cannot
write, naming the season to resume from.

Two things to know, both learned the hard way on earlier backfills:

* a pending run is **cancelled** when another run enters the
  `rebuild-and-deploy-main` concurrency group, so dispatch it when nothing else
  is queued and check it reaches `in_progress`;
* the commits say `[skip ci]`, so dispatch `ci-and-deploy.yml` afterwards or the
  new data never reaches the site.

Once a range is committed, `python3 src/calibrate_win_probability.py` can fit
the time curve, and `config/model_config.json` gets a measured
`time_exponent` in place of the assumption.
