# Dashboard roadmap

## Immediate: scorebugs and scoreboards

- Give every game a recognizable away/home identity: darkened team color bands, prominent logos, subtle background marks, readable scores, Elo, rank, and pregame win chance.
- Keep live period and clock, final state, TV network, estimated versus published Elo changes, and a clear path to game details visible at a glance.
- Check long names, missing logos, small phones, and contrast across the full FBS palette. Keep the Home/Live Scores rankings alongside the scoreboard on desktop.

## Mid term: a game day destination (roughly the next week)

Goal: friends can open this site as their first stop for college football scores.

1. Reduce score and status lag. Show feed freshness clearly; measure actual publish-to-display delay before claiming real-time coverage.
2. Add a genuinely useful live game view: scoring summary and drives or play-by-play when CFBD provides them, team statistics, player leaders, and quarter scoring. Handle games with missing data honestly.
3. Make the mobile scoreboard fast to scan: useful date/week navigation, favorites or a personalized top section, and one-tap access to game details and rankings.
4. Move fast-changing game data behind a lightweight service or managed store if static GitHub Pages publishing is the source of delay. Keep the model and historical exports in the existing pipeline.
5. Validate on live Saturdays with friends: compare scores, clocks, missing games, load time, and data freshness against the source feed; fix the largest gaps first.

The paid CFBD Tier 2 subscription supports the current work. Evaluate Tier 3 only with a concrete feature and traffic plan that uses its additional data and limits; do not upgrade solely because the site has moved hosting.

## Standings and rankings

Top 25 is the Standings landing page as of 2026-10-12: Elo, AP and CFP as three
boards in one row. The full Elo view behind "All ratings" is still only a team,
a rating and a change arrow for all 138 teams, which is thin for the page the
model is named after.

Week by Week shows every team's rank after every week of a season at once as of
2026-10-26: one row per team, one column per week, sorted by any of them and cut
by conference, with the one-week snapshot still behind "One week" and behind
every `stage=` link ever published. The AP and CFP ballots are a second and
third track on the same grid, drawn on the poll's OWN release columns rather
than the Elo weeks, because a poll released in week N is voted on games through
week N-1.

- Add detail to the Elo page: record, conference, movement over a window rather
  than one game, strength of schedule, and the best and worst results that
  built the rating. Decide what belongs there versus on a team page, since a
  team page already carries the rating ledger.
- Widen the poll archive. `data/raw/rankings_<year>.csv` is fetched for the
  current season only, so the AP and CFP tracks are empty for every earlier one.
  The loader, the schema and the grid already take any season; what is missing
  is a backfill run of `fetch_cfbd_games.py` across the archive (it writes
  `rankings_<year>.csv` beside the games), which has to happen in a workflow
  because this container cannot reach the CFBD API.

## Distant future: an installable app

After the mobile site and live data experience are dependable, start with a progressive web app for home-screen install and caching. Consider native iOS/Android apps when notifications, widgets, or platform features justify maintaining separate clients. The app should share the same scores, identity system, and backend rather than duplicating game logic.
