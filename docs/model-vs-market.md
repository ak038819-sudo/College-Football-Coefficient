# The ratings against the closing line

The Track Record page could already say how often the ratings pick the winner,
and that picking the home team every time gets about 60%. Beating that is table
stakes. The question a reader actually has is whether the ratings know anything
the betting market does not, and until now the site had no way to answer it.

A closing spread is a point estimate of the margin produced by people with
money at stake. It is the hardest consensus forecast in sport to beat, and it
is the only benchmark worth putting beside a rating.

## The data

`src/fetch_cfbd_lines.py` → `data/raw/lines/<year>.csv.gz` →
`src/load_lines.py` → `game_lines`, one row per game **per provider**. The
providers disagree, and averaging them at load would hide by how much;
`src/measure_vs_market.py` picks one per game, preferring CFBD's own
`consensus`, then named books, then anything else in name order so the choice
is deterministic. A measurement that silently changed which book it read would
move its own numbers between runs.

**Nothing reads `game_lines` as a rating input, and nothing will.** A benchmark
the model can see is not a benchmark. The Elo and CoE builders never touch the
table, so a gap in line coverage changes what this page says and nothing else.

### The sign

CFBD's `spread` is the home team's handicap and is **negative when the home
team is favoured**: `-7` means home by 7. Every margin in this database is home
minus away. The loader therefore stores `market_home_margin` — the market's
expected margin in this database's direction, `+7` for that same line — and
keeps CFBD's own number beside it so the conversion can be checked.

Getting this backwards is the one error that would make the whole page lie
confidently: the model would appear to beat the market on exactly the games it
lost to it. Five mutation checks fail if the flip is removed or reversed.

A blank spread stays NULL rather than 0, because 0 is a real line. A pick'em is
a game the market found hard to call, not a game nobody priced.

## The four comparisons

`src/measure_vs_market.py` writes `data/processed/market_comparison.json` and
the same report is published in the static manifest for the page.

| Comparison | What it answers |
| --- | --- |
| **Margin error** (MAE, RMSE, bias) | Both sides state an expected margin, so both can be scored against the result. The cleanest comparison, and the one the ratings are least likely to win. |
| **Against the spread** | The model's disagreement with the line *is* a pick: back the home team when the ratings expect more than the number. |
| **Straight up, where they disagree** | Who called the winner on the games where the two favourites differ. |
| **Win probability** (Brier, log loss) | The spread turned into a probability, scored against the rating engine's own pregame number. |

Two details inside those:

**A push is excluded, not counted as a half-win.** Counting it either way would
invent a result nobody got.

**Agreement is excluded from the straight-up comparison.** Over every game the
two agree far too often for the overall figures to separate them; the subset
where they disagree is the only place a difference costs anybody anything.

**The market's probabilities are read through the market's own curve.** The
live win-probability model's `sigma` (16.65 points) is the spread of results
around the *ratings'* expected margin. The market forecasts margins better than
that, so reading its spreads through the wider curve would hand it
probabilities it never implied and then score it on them. The report measures
the root mean square error of the market's own margins on the same games and
uses that width, publishing it as `market_sigma`.

## What it found

Measured on 2026-10-16 over the whole archive, 10,151 priced games, 2013-2026:

| | This model | Closing line |
| --- | --- | --- |
| Average margin miss (MAE) | 13.32 | **12.35** |
| Big misses (RMSE) | 16.86 | **15.68** |
| Lean (signed) | +0.63 | −0.07 |
| Brier | 0.1846 | **0.1712** |

Against the spread: **4925–5015–211**, a 49.5% hit rate on the games that were
not a push, against the 52.38% a bettor needs at −110. On the 1,408 games where
the two disagreed about who would win outright, the ratings called **41.4%**.

The market wins every comparison, which is the expected result and the reason
the comparison is worth publishing. Two things in it are worth more than the
headline:

* **The gap is small and stable.** Just under a point of margin a game, in
  every season of the archive (the model's MAE runs 12.71 to 13.85, the
  market's 11.47 to 13.05). A rating built from scores alone, with no injury
  report, no line movement and no money at stake, lands within a point a game
  of the sharpest consensus in sport.
* **The disagreements are where it loses.** 41.4% is well below a coin flip,
  so the model's disagreements with the line are not an edge — they are mostly
  the model being wrong. That is the single most useful thing on the page, and
  the reason the straight-up figure is reported on the disagreements alone
  rather than over every game, where the two agree far too often to separate.

## What this cannot claim

* **A closing line is not available when a bet would be placed.** Every figure
  here is measured against the sharpest price of the week, which is the right
  test of a forecast and the wrong test of a betting strategy.
* **Only priced games count**, which skews modern and towards games people
  cared about. The season table publishes the share of each season the market
  priced, because a hit rate over 40 games is a curiosity.
* **No vigorish, no bet sizing, no line shopping.** Break-even on these figures
  is 50%; a bettor at the usual -110 needs **52.38%**, and the page says so
  rather than letting 51% read as a profit.

## Filling the archive

Run `.github/workflows/backfill-lines.yml` (Actions → Backfill betting lines →
Run workflow) with a season range. It commits one season at a time with
`[skip ci]` and stops at the first season it cannot fetch, naming the season to
resume from.

**CFBD's line archive starts in 2013**, which is why the range defaults to
2013. Asked for an earlier season it answers with an empty list rather than an
error, so an earlier start is harmless: the fetcher prints "no lines" and the
loop moves on. It writes no file for such a season, and the publish step is
guarded on the file existing — staging a path that is not there is a fatal git
error, not a no-op, which is how the first run of this workflow published
nothing at all starting from 1980.

The same two cautions as every other backfill here:

* a pending run is **cancelled** when another run enters the
  `rebuild-and-deploy-main` concurrency group, so dispatch it when nothing else
  is queued and check it reaches `in_progress`;
* the commits say `[skip ci]`, so dispatch `ci-and-deploy.yml` afterwards or
  the new data never reaches the site.

A season CFBD has no lines for prints "no lines" and leaves any existing file
alone; only a failed request stops the run.
