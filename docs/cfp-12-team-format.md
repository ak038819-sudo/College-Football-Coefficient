# The 12-team College Football Playoff, as this project runs it

The site's playoff is the real CFP format. It was the invented 24-team
bracket until 2026-10-10, when the owner asked to "convert our current
bracket to run the irl format for the time being": the project is heading
toward the actual format and away from the invented one.

The 24-team ruleset is still written up in [`coe_spec.md`](coe_spec.md) and
still runs from `src/coefficients/select_playoff_field_v2.py`. Nothing live
reads it. Read that document as the retained alternative, and this one as
what the site shows.

## The format

Twelve teams.

- The **five highest-ranked conference champions** take automatic bids.
- **Seven at-large bids** go to the strongest teams left.
- **Seeds run straight down the ranking**, 1 to 12. This is the 2025 rule
  change: the 2024 bracket gave the top four seeds to the top four
  champions wherever they ranked, which could put the best team in the
  country at seed 5.
- **Seeds 1–4 sit out the first round.**
- **First round:** 5v12, 6v11, 7v10, 8v9, at the higher seed's home.
- **Quarterfinals:** 1 plays the 8/9 winner, 4 the 5/12 winner, 2 the 7/10
  winner, 3 the 6/11 winner. Neutral sites from here on.
- **Semifinals** pair the 1/4 half against the 2/3 half, so the top two
  seeds can only meet in the final.

`src/coefficients/select_cfp_field.py` is the whole of it. The constants at
the top of that file are what the methodology page and the tests read, so
the format is stated once.

## What stands in for the selection committee

The real format needs a national ranking, and this project has no
committee. Every ordering it needs — which champions are the top five,
which teams are the best available at-large, and the seeds themselves — is
the **5-year rolling team coefficient** in
`data/processed/team_coeff_5yr.csv`. That is already what this project
seeds and assigns home field by, so the playoff introduces no second notion
of team strength. Ties break on team name, so a bracket is reproducible
from its inputs.

Two honest limits follow, and the methodology page states both:

- It is a **strength rating, not a resume ranking**. The committee's
  question is who earned it this year; this is who is strong.
- A **five-year window moves more slowly than a season**. A team that
  collapsed this year carries four prior seasons with it.

So a field here is "the twelve strongest programs with a champion floor",
not a guess at what the committee would do.

A **champion** is the top team in this project's derived conference
standings (`conference_standings_by_year`, regular-season conference record
with its tiebreak heuristic). That is not always the team that won the
conference title game — Georgia went 8-0 in SEC play in 2021 and leads
these standings; Alabama won the title game.

**Independents** cannot win a conference, so they cannot hold an automatic
bid. They compete for at-large bids like everyone else, which is both the
real rule and simpler than what it replaced: under the 24-team model
independents belonged to no bid-eligible conference and needed a special
strength threshold to enter the field at all.

## What dropped out

- **The pots and the draw.** The 24-team bracket paired a random Pot 1 team
  against a random Pot 2 team under a no-same-conference constraint. A
  12-team bracket is fixed by the seeds, so there is nothing to draw.
- **The conference bid table.** Bids were allocated per conference by
  conference CoE rank (ranks 1–4 got four bids, rank 5 three, ranks 6–10
  one). Conference CoE now allocates nothing; it is a strength standing,
  and the site labels it as one. The conference page's "Playoff bid rank"
  tile is gone for the same reason.
- **The independent threshold**, as above.
- **Draw-averaged title odds.** This is the improvement. Because the old
  bracket depended on a draw that had not happened, a team's title chance
  had to be averaged over draws to mean anything — in 2026, holding the
  field and the model fixed and changing only the draw, Notre Dame's odds
  ran from 12.0% to 27.7%, and the published figure described no bracket
  anyone could look at. The odds now are the odds of the bracket on the
  page.

## What did not change

- **The win-probability model.** `simulate_cfp_bracket.py` borrows
  `simulate_bracket.py`'s logistic unchanged. Turning a CoE gap into a win
  chance has nothing to do with how many teams are in the field.
- **The fitted temperature (about 4.5) and host edge (1.25 CoE points)**,
  both in `config/model_config.json` and both measured in
  `src/calibrate_sim_temperature.py` against every game since 1980 between
  two playoff-calibre teams. Neither was refitted: they describe how a CoE
  gap and a home ground behave in a football game.
- **Home field in exactly one round.** It was the Round of 24; it is now
  the first round. Everything after is at a neutral site and is simulated
  with no venue term at all. Seeds 1–4 never play a hosted game — their
  reward is not playing one.
- **The CoE 2.0 `cfp_appearance` and `cfp_win` bonuses.** These count
  *real* CFP games (`phase == 'cfp'`), not model ones, so the format change
  does not reach them.
- **The 16-team NIT.** `select_nit_field.py` still runs on the 24-team
  selector's bid structure. It has never been exported or shown on the
  site, so it is untouched rather than converted.

## The invariants under test

`tests/test_cfp_field.py` drives the real selector and simulation over
every season with standings (2014–2026): twelve teams seeded 1 to 12, five
automatic bids and seven at-large, an automatic bid only ever to a
champion, the five automatic bids being exactly the five strongest
champions, no at-large passing over a stronger eligible team, seeds in
ranking order, no independent holding an automatic bid, the top four seeds
sitting out the first round, the higher seed hosting it, one team advancing
per round, and a bye seed reaching the quarterfinals in 100% of runs.

`tests/cfp_bracket.test.cjs` drives the page's own copy — the Simulate
button — out of the shipped shell, because two implementations of one
bracket is the shape that drifts. It checks that the page simulates the
bracket the export describes, that the halves keep seeds 1 and 2 apart,
that the host edge applies in the first round and nowhere after it, and
that a payload withholding the temperature or the host edge degenerates
visibly rather than quietly falling back to a constant nobody fitted.

Both files replaced guards that asserted on the *text* of the page's call
sites. Those passed while the code was broken, and one of them broke on
this change by going looking for a Round of 24 that no longer exists.

## Fitting the panel

The bracket is a CSS grid of eight rows — the eight quarterfinal slots, four
bye seeds alternating with four first-round winners — and every later round is
drawn by spanning those rows, which is what makes the connectors land without
per-round special cases.

That only holds while a slot actually fits the row it is placed in. It did not:
the row height was a 40px literal while the tallest slot, the two-team
first-round card, rendered 153px, because a bracket cell was inheriting the
site-wide 63px team chip. Every slot spilled 113px out of its cell, the round
titles were overdrawn, the last row was clipped, and the tree grew a scrollbar
inside the panel.

The fix is a dependency rather than a number. `--bracket-team-row` sizes a team
line, `--bracket-row` is computed from it plus the card's own gaps and padding,
and the chip is sized by a bracket-scoped rule so the rest of the page's logos
keep theirs. Columns are `flex: 1 1 0` between a 184px and a 320px width, so
the bracket fills the panel instead of huddling on the left, and the tree
scrolls sideways on a narrow screen and never down.

`tests/test_bracket_layout.py` pins those dependencies, not the pixel values:
a bare px row height, an unscoped chip, a chip taller than a team row, a plain
`overflow`, or a column that cannot grow each fail it. Measured in Chromium at
768–1600px before it was written — the tree's `scrollHeight` equals its
`clientHeight` and no slot spills its cell.

## Going back, or going further

Reverting means pointing `export_dashboard_data.py` at
`select_playoff_field_v2.py` and `simulate_bracket.py` again and restoring
the page's pot, draw and Round-of-24 rendering. The selectors, the draw,
the NIT and their tests were all kept for that reason.

Not done here, and not needed by the format: the 2024 seeding variant
(champions take the top four seeds), a conference cap on at-large bids
(the real format has none), and any use of a season-only rating rather
than the five-year window as the ranking. The last of those is the one
worth considering if a field ever looks too slow to react.
