# Elo conference-flow audit

2026-10-05. Read-only starting point for the next Elo iteration after v0.1.1.
Run `python analysis/elo_conference_flow/audit.py` from the repository root.
Inputs are the committed game and team-page exports, including season-specific
conference memberships. Independents are separate pools. No ratings or model
configuration are changed.

The committed CSV and JSON reports are a dated analysis snapshot, not fixtures
for the changing dashboard exports. Tests verify report reproducibility with
fixed synthetic inputs and check invariants against live exports. Rerun the
audit explicitly when updating this snapshot and its written interpretation.

## What the current implementation does

`src/build_elo.py` applies `delta = K × (result − expected) × M` to one team and
exactly its negative to the opponent. Current settings: K=40, 400-point logistic
scale, flat home advantage=50, and offseason retention=0.8 around 1500.
The default performance layer is strength-adjusted Success Rate (`xsrdiff`):
`M = clamp(1 + 12 × winner_SR_plus, 0.25, 5)`. Missing supported SR inputs use
MOV; the MOV multiplier has its own behavior and is not subject to those SR bounds.

Every one of the 6,024 rated games in the included 2018–2025 exports has a
zero sum of its two independently exported Elo changes. The incomplete 2026
season is excluded because eight rated teams lack conference memberships
(IDs 9, 17, 51, 58, 59, 60, 102, 127). Any season with a missing, null, or blank
membership for a participant in a completed, paired-rated game is excluded in
full, with the reason and missing IDs in `season_summary.json`; no transfers or
internal/cross Brier splits are published for it. Explicit independents remain
separate pools. Rerunning the audit includes a season once its mappings are complete.

Within a season after offseason regression, an intraconference game can
redistribute ratings, but cannot add to that conference's total.
A team's ranking can improve while the conference mean is
unchanged. There is no mathematical guarantee the strongest team absorbs all
of the points: losses transfer points away again.

## 2025 evidence

There are 808 rated FBS-vs-FBS games: 563 internal and 245 cross-conference.
Of the cross-conference games, 166 occur through regular-season week 5,
37 occur later in the regular season, and 42 are postseason. Week 5 is a
reporting cutoff, not a universal conference-schedule start date.

| Conference | Cross-conf games through week 5 | Later regular-season games | Postseason games | Early net Elo | Later regular net Elo | Postseason net Elo |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| ACC | 36 | 14 | 14 | +75.08 | +17.52 | +208.08 |
| Big 12 | 31 | 0 | 8 | +167.30 | 0.00 | +26.51 |
| Big Ten | 38 | 2 | 14 | +204.99 | −17.72 | +137.38 |
| SEC | 39 | 10 | 10 | +790.42 | +34.87 | −258.51 |
| Mountain West | 32 | 4 | 7 | −18.95 | −47.66 | −201.39 |

These are total conference transfers, not per-team averages or a strength
ranking. Conference size and schedule differ. Every listed conference's net
transfer from internal games is zero. The Big 12 therefore has the exact
closed-pool stretch raised by the owner; other conferences have varying late
connections. The postseason can materially revise conference totals.

The CSV includes every pool in each included season. Summary Brier scores are descriptive
checks of published forecasts, not a new out-of-sample comparison. Different
internal/cross-conference matchup difficulty prevents interpreting their raw
Brier difference as evidence for changing the model. Exported numbers are rounded.
These totals exclude FCS games because the current ratings archive excludes them.

## Separate concerns before changing the formula

1. **Conference strength evidence.** In-season zero-sum game updates preserve
   each fixed-membership conference pool's total (and mean) unless games connect
   pools. This is not preservation across seasons: `run_elo()` regresses all
   existing ratings once at the season boundary toward 1500 with retention 0.8.
   For an unchanged pool of N teams with mean m and total T, the new mean is
   `1500 + 0.8 × (m − 1500)` and the new total is
   `1500 × N + 0.8 × (T − 1500 × N)`. Thus two unchanged conference means'
   gap contracts by 20% without a connecting game. Membership changes can also
   alter conference totals and means. These CSVs measure game deltas only;
   they do not account for boundary adjustments or establish year-to-year
   pool conservation. Removing conservation alone does not supply the
   missing evidence. Adding winner bonuses can instead reward schedule volume
   or inflate everybody's scale.
2. **How much winning matters.** At a 20% pregame win probability, K=40 yields a
   gain of 8 with SR multiplier 0.25, 32 with multiplier 1, and 160 with multiplier
   5. The quarter-strength floor is a separate explanation for unexpectedly small
   gains after a marquee upset. Changing this deserves its own test, rather than
   conflating it with conference point retention.
3. **FCS losses.** Those results are currently missing model inputs. A severe
   penalty requires verified opponent identities/classification and historical
   results first. Applying arbitrary deductions only to remembered upsets would
   be inconsistent. Preserving FBS–FCS game archives must be settled before pruning.

## Next experiment

Keep this released model as the baseline. Compare a small, prespecified set of
SR floors (current 0.25 versus 0.5 and 1.0) while holding K and ceiling fixed to
identify the result-weight effect. Then retune K on training seasons only. Report
changes separately for underdog wins, conference and nonconference games, and
postseason. Use season-block uncertainty and a genuinely unused forward window;
already inspected seasons cannot be called untouched validation.

For the conference question, compare the baseline with a conference-level prior
learned only from earlier cross-conference results. Any shared conference update
must be time-ordered, shrink small samples and avoid double-counting the same
result in both team and conference terms. Evaluate cross-conference calibration,
Brier/log loss, conference mean drift and top-team ranking stability. No candidate
should become production merely because it creates larger swings.
