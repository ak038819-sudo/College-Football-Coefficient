# Reference data

Hand-maintained facts the models don't compute. Kept separate from game
ingestion: nothing here is fetched, and no rating engine reads it.

## national_champions.csv

National championships **are not inferred from game results.** Before 1998
they were awarded by polls, and even afterward the poll and game champions
have occasionally disagreed. This file records them explicitly.

| column        | meaning |
|---------------|---------|
| `season_year` | season the title belongs to (the 2025 season's title game is played in January 2026) |
| `team_name`   | canonical `teams.team_name` (aliases also accepted) |
| `system`      | `cfp`, `bcs`, `ap`, or `coaches` |
| `title_type`  | `championship_game` (cfp, bcs) or `final_poll` (ap, coaches) |
| `status`      | `awarded` or `vacated` |
| `notes`       | free text |

### Counting convention (what the team page shows)

A team is credited with a national title for a season if it has **at least
one `awarded` row** for that season. The systems recorded, by era:

- **2014 onward:** CFP National Championship winner.
- **1998–2013:** BCS title game winner. An AP poll champion is also recorded
  only when it differs from the BCS winner or survives a BCS vacating
  (2003 USC; 2004 USC).
- **1980–1997:** final AP poll and final Coaches poll. When they disagree,
  both teams are credited and the page marks the season *shared*.

`vacated` rows are shown on the team page but never counted. Seasons before
1980 (the dataset's first season) are not included, and the page says so.
Schools sometimes claim additional titles from other selectors; this file
deliberately doesn't.

### Adding a new season

Add one row after the title game, e.g.

    2026,<Champion>,cfp,championship_game,awarded,

`tests/test_national_champions.py` validates every row (known team, valid
system for the era, exactly one awarded game champion per BCS/CFP season,
and that each CFP champion actually won that season's final CFP game in the
loaded game data), so a typo fails the test suite instead of the website.
