# Basketball provider coverage, measured 2026-10-20

What `/games` and `/teams` actually hold for seasons 2024 through 2027.
Measured by `src/ncaambb/probe_coverage.py` in run
[39368219755](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/39368219755),
52 requests, 0 retries, 0 duplicate rows.

| Season | Label | Games | Finals | Rateable | Unplayed | Postponed/cancelled | D-I teams | Non-D-I teams seen |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2024 | 2023-24 | 6,249 | 6,243 | 5,726 | 4 | 2 | 362 | 355 |
| 2025 | 2024-25 | 6,299 | 6,292 | 5,764 | 2 | 5 | 364 | 338 |
| 2026 | 2025-26 | 6,318 | 6,300 | 5,752 | 0 | 18 | 365 | 363 |
| 2027 | 2026-27 | 6,015 | 0 | 0 | 6,015 | 0 | 365 | 383 |

## The 2026-27 schedule is published

Season 2027 returns **6,015 games, every one `scheduled`**, across 365 Division I
teams. The live season is therefore available, and Release A's schedule and
results coverage is not blocked on waiting for it.

Two things about it differ from a completed season and the UI has to handle both:

- **2,669 of the 6,015 games (44%) carry `startTimeTbd`.** A tip-off time is not
  yet known for nearly half the season, so a time must be able to render as
  undecided rather than as midnight.
- **No postseason games exist yet.** Completed seasons carry 105 to 123
  postseason games; 2027 has none scheduled, because the fields are not set.

## Partitioning is not optional

Every season holds more than twice the 3,000-record cap on `/games`. A single
request per season would have returned 3,000 rows and looked like a complete
answer — roughly half the season, silently. Twelve calendar-month windows cover
a season, and in all four seasons no window reached the cap, so no window needed
splitting. The split path is still exercised by tests, because a busy week in a
future season is not bound by this measurement.

## What is excluded from Division I rating, and why

| Season | Finals not rateable | Games vs a non-D-I opponent |
| --- | --- | --- |
| 2024 | 517 | 518 |
| 2025 | 528 | 530 |
| 2026 | 548 | 551 |

Nearly every unrateable final is a game against a team the provider does not
list for that season — between 8.3% and 8.7% of all finals. These games are **retained**
for schedules and records and excluded from rating until a separately validated
opponent model exists, which is the plan's rule. The small gap between the two
columns is the handful of non-D-I games that were cancelled or never played, so
they were never finals to begin with.

No game in any of the four seasons was excluded for an unknown status, an
unknown season type, a missing team id, a missing start date, a self-match or a
tied final. Those checks are in place and currently find nothing, which is the
useful thing to know about them.

## Status and season type really do vary

- 2026 carries **14 postponed and 4 cancelled** games; 2025 carries 5 cancelled
  and 2 still `scheduled`; 2024 carries 2 cancelled and 4 still `scheduled`.
  A season does not resolve cleanly to finals, so a loader that assumes it will
  would drop or mis-handle these.
- Postseason games: 120, 123 and 105 in the completed seasons. Preseason games
  do not appear in any of the four.
- Neutral-site games: 746, 747, 687 and 295.

## Teams

Division I membership is read from `/teams?season=`, which returns 362, 364, 365
and 365 teams. Games bring in 717, 702, 728 and 748 distinct teams, so between
338 and 383 non-D-I opponents appear per season. Membership is recorded per
season rather than once, because the count moves every year.

## Not measured here

Rosters, player identities and box scores at national scale; possession
definitions; and whether a conference partition is needed if a future window
ever reaches the cap. Provider Elo is returned by `/games` and is deliberately
not read.
