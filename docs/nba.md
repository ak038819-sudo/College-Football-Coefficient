# The NBA archive

Stage N1 of the NBA tab: the source, the database and the loaders. Nothing
here is published yet — no export, no page, no entry in the league selector.
The scope this follows is the NBA tab scope doc, and the stages after this one
are the rating and the tab (N2), game pages (N3), people (N4) and the
postseason bracket (N5).

## The source

ESPN NBA data, 2002 to the season in progress, published by the
[sportsdataverse](https://github.com/sportsdataverse) project as static
Parquet files. No key, no request allowance, no paging: each dataset and
season is one public file. That is the same shape as the NFL tab's source and
a deliberate choice over a metered API — the rolling cap that stalled the
basketball track for eleven days cannot happen to a file anyone may fetch.

**Read the release assets, not the repository tree.** The same data appears in
two places and only one is complete:

| Mirror | Datasets published as Parquet |
| --- | --- |
| `sportsdataverse-data` release assets | all of them |
| `hoopR-nba-data` repository tree | schedules, play-by-play, team box, player box |

Measured on 2026-11-03. Rosters, standings, player core, season statistics and
the draft all 404 in the repository tree while being published as release
assets, so several plausible guesses against the tree returned 404 for data
that was in fact there. The URL shapes in `src/nba/source.py` were pinned from
`R/load_nba.R` in the hoopR package rather than guessed, and the stem is not
derivable from the tag: `espn_nba_schedules` holds `nba_schedule_2027.parquet`
while `espn_nba_standings` holds `standings_2026.parquet`.

CSV is not published, so Parquet is the only format and `pyarrow` is a real
dependency of the ingest. It is build-time only: the published site fetches
JSON.

**Licensing.** `sportsdataverse-data` carries the MIT licence, which permits
redistribution with attribution. The data underneath is ESPN-derived rather
than NBA official, so the site should credit both. The NBA's own tracking and
advanced statistics are not available at all — `stats.nba.com` answers a cloud
IP with a silent timeout, measured three times from a GitHub Actions runner.

**Seasons are named for the year they end.** `2027` is 2026-27, which is what
the source does and what `identity.season_label` renders as `2026-27`. The
turnover is September: the Finals are over and no game of the new season has
been played, so a run in October and a run the following May agree about which
season is current.

## Three traps, each of which produces data that looks right

### An unplayed game is written 0-0, not NULL

The source fills both score columns with zero for every scheduled fixture —
1,108 of them in the season in progress on the day this was written. Read
naively, next April's schedule is 1,108 draws, and a rating replay would take
every one of them as evidence that two teams are equal.

`status_type_completed` is the only field that tells the truth. So the score
is read **only when that flag is set**, and the habit the other three leagues
have of checking `is not None` is not enough here. `games.parse_row` is where
this is caught, and `tests/test_nba_data.py` drives it directly.

Two consequences worth stating: a completed game with no score is refused
rather than stored, and a completed game with a level score is refused too —
the NBA plays overtime until somebody wins, so a draw is a fault in the data,
not a result. There are none in the archive.

### Two different franchises are both "Charlotte Hornets"

| ESPN id | Seasons | What it is |
| --- | --- | --- |
| 3 | 2002 | Charlotte Hornets |
| 3 | 2003-2013 | New Orleans Hornets (playing in Oklahoma City, 2006-07) |
| 3 | 2014- | New Orleans Pelicans |
| 30 | 2005-2014 | Charlotte Bobcats |
| 30 | 2015- | Charlotte Hornets |

Join those on the name and the Pelicans' first eleven seasons are filed under
the Hornets of today, and nothing afterwards looks wrong. The id is the only
safe key, which is why `src/nba/identity.py` offers no way to resolve a team
by name.

There is no relocation table here, unlike the NFL's. ESPN keeps a franchise's
id across a move — Seattle and Oklahoma City are both team 25, New Jersey and
Brooklyn both team 17 — so the franchise needs no reassembling. What changes
is the name, and `team_season` keeps it so a 2007 box score still says
SuperSonics while the standings say Thunder.

### An All-Star game looks like a regular-season game

The All-Star game and its modern variants (Team LeBron, Team Giannis, USA v
World, Stars v Stripes) carry `season_type = 2`, the same value as a game in
Cleveland in January. Filtering the exhibitions by season type does not work.

What marks them is that their sides are not franchises: ids 31 and 32 and a
dozen six-figure ids, listed in `identity.EXHIBITION_IDS`. Measured across
2002-2027 there are 39 such games, and no franchise ever plays one. A row with
one real team and one unknown id is **not** an exhibition — it is a fault, and
it is refused rather than skipped.

## What is stored

`db/nba.db`, 13 MB, its own file and joinable to nothing in football, college
basketball or the NFL. The whole archive ingests in 22 seconds.

```
python -m src.nba.ingest                 # every season
python -m src.nba.ingest --seasons 2027  # one of them
```

| Season | Games | Played | Teams |
| --- | --- | --- | --- |
| 2001-02 | 1,260 | 1,260 | 29 |
| 2004-05 | 1,314 | 1,314 | 30 |
| 2011-12 | 1,074 | 1,074 | 30 |
| 2019-20 | 1,155 | 1,143 | 30 |
| 2025-26 | 1,326 | 1,322 | 30 |
| 2026-27 | 1,200 | 92 | 30 |

33,457 games across 26 seasons, 39 exhibitions skipped. The short seasons are
real: 2011-12 was the lockout (66 games a team) and 2019-20 lost games to the
pandemic. 29 teams until the Charlotte expansion team arrives in 2004-05.

**Conference comes from the standings feed, division does not.** The feed
groups teams by conference only, so `team_season.division` is left NULL rather
than filled from a table written down in code. The standings file for the
season in progress does not exist until the source writes one, and a missing
one leaves the conference NULL rather than failing an ingest that has the
games in hand — every other fetch failure fails the run, because a database
quietly short of a season makes every count taken from it wrong without
looking wrong.

**Zero attendance is an absence, not an empty arena.** It appears in 174 games
of 2011, which had no empty arenas, as well as in the seasons that did. There
is no way to tell a missing reading from a real zero, so all 2,300 of them are
stored as NULL rather than asserting a crowd of nobody.

## Ordering

By date, never by round. The play-in is played between the regular season and
the playoffs, so any ordering by the name or number of a round puts it in the
wrong place. Ordering a season by round once put a bowl game first in the
football archive and zeroed 1,353 season rating changes; the same shape of
mistake is available here.
