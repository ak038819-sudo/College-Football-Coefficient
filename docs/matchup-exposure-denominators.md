# Metric-specific exposure and denominators — descriptive-statistics gate

## Source verification

The existing authenticated CFBD `/stats/game/advanced` files provide
`off_plays` as a **total offensive play count** and nested rushing/passing
success rate, PPA, and explosiveness values. According to CFBD's current
[game-advanced endpoint schema](https://api.collegefootballdata.com/api/stats),
the **nested rushing/passing objects do not include eligible play counts**.
Do not equate general box-score rushes/pass attempts to the play-by-play
classification filtered for garbage time.

**The number of team-games is not the number of independent plays.**
The current split-rate and split-PPA descriptive fit therefore explicitly
uses one observation per eligible game, with no false split-play exposure
claim. These are provisional *game-equal* adjusted statistics.

`src/matchup_metric_exposure.py` records the denominator policy for every
metric and `src/build_matchup_intelligence.py` enforces it:

| Metric | Verified present denominator | Current weights | Important interpretation |
|---|---|---|---|
| Overall success rate | `off_plays` | total offensive plays | Weighted mean of observed per-game success rates |
| Overall PPA | `off_plays` | total offensive plays | Weighted mean of observed per-game PPA |
| Overall explosiveness | **none suitable** | equal game weight | Conditional on successful plays; total plays must **not** be substituted |
| Rush/pass success rate | **none** | equal game weight | Rush/pass eligible play counts not in this API response |
| Rush/pass PPA | **none** | equal game weight | Same constraint |
| Rush/pass explosiveness | **none** | equal game weight | Ideally successful plays *of that type*, not all rush/pass plays |

## Why explosiveness is distinct

Success rate is a probability over relevant plays; PPA is a mean value over
relevant plays. CFBD explosiveness describes average PPA on **successful**
plays, so its effective denominator is the successful subset. Weighting it
by all plays overweights low-success games that had many opportunities.
Our research models previously treated *overall* explosiveness like other
overall metrics and used `off_plays`; that was corrected to equal game
weight pending a valid successful-play count. No old results are silently
reclassified as exposure-weighted evidence.

## How verified denominators can be added later

Acquire underlying per-play events or source-supported split eligible counts
with the **same garbage-time filter, play classifications and game IDs** as
the advanced metrics. Validate against team totals and game pairings, then
build metric-specific counts; use successful play counts for explosiveness.
Keep raw observed game values and provenance. Only then compare equal-game
versus opportunity-weighted *descriptive* ratings and confirm how ridge
penalty scales with weights. Do not assume existing lambda=2 (games)
remains equivalent to lambda=200 (plays).

## Release and reliability

Current output includes an explicit `exposure` / `denominator_provenance`
description of each metric's weight source and missingness. The source
coverage check still measures FBS paired team-games, not split-play
denominator coverage. We must not advertise split-rate percentiles as
'weighted by 800 rushing snaps' without verifying their actual counts.

This update does not replace archived CSVs, touch production Elo/Merits or
export UI pages. PR #161 remains draft.
