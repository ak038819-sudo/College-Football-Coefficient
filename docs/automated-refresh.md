# Automated data refresh

The workflow `.github/workflows/ci-and-deploy.yml` refreshes the site on its own:
it fetches the latest games, schedule, AP/CFP polls and advanced stats from
CollegeFootballData.com, rebuilds everything, runs the full test suite against
the fresh data, and publishes. You no longer need to run the weekly routine by
hand (it still works if you want to).

## One-time setup: store your CFBD key as a secret

1. On GitHub, open the repository and go to **Settings**.
2. In the left sidebar: **Secrets and variables** -> **Actions**.
3. Under **Repository secrets**, click **New repository secret**.
4. Name: `CFBD_API_KEY`. Secret: your key from collegefootballdata.com/key.
5. Click **Add secret**.

The key is stored encrypted, only the fetch step can read it, GitHub masks it
in logs, and nothing in the project prints it.

## When it runs

- **Sunday night, 11:15 PM CDT / 10:15 PM CST** (Monday 04:15 UTC):
  rebuilds team pages and Elo rankings after the weekend slate.
- **Thursday night, 11:15 PM CDT / 10:15 PM CST** (Friday 04:15 UTC):
  picks up midweek and Thursday results before the Saturday slate.
- **On demand:** **Actions** tab -> **Test, Rebuild Dashboard, and Deploy** ->
  **Run workflow** -> **Run workflow**.
- **Regular-season game windows:** an hourly check on Thursday through Sunday
  (Central time, with UTC spillover). It rebuilds only if a final score is new
  or changed, or if a recent final is still missing available game efficiency
  or player lines. The latter are retried for two days; unavailable coverage
  remains explicitly absent.
- **On a code push with newer live finals:** the workflow checks the live feed
  against the committed raw game scores. If an FBS final is missing or changed,
  it fetches the season and retests it before publishing. A routine UI push
  with no new finals does not spend API requests on a full fetch.

The UTC schedule stays fixed, so it shifts by an hour locally when daylight saving ends.
GitHub can start scheduled runs a few minutes late during busy periods.

To change the schedule, edit the two `cron:` lines near the top of the workflow
(`minute hour day-of-month month day-of-week`, in UTC; day-of-week 0 = Sunday).
To pause only the automatic runs, put a `#` in front of both `- cron:` lines.

## Which season it fetches

January and February count as the previous season (the CFP title game in
January 2027 belongs to the 2026 season); from March on, the new season.
See `src/current_season.py`.

## What each run does

1. Tests the committed code and data (the normal `test` job).
2. Fetches the current season from CFBD.
3. Rebuilds the database, Elo, CoE and the dashboard.
   Finished games move from the live feed into the season archive at this step.
   Current-season player box scores are saved in `data/raw/player_boxscores/`
   and exported into lazy-loaded historical game pages. To backfill the
   documented box-score range with an authorized CFBD key, run the
   **Backfill player box scores** workflow (`.github/workflows/backfill-players.yml`)
   from the Actions tab, giving it a season range; it uses the repository's
   `CFBD_API_KEY` secret, so no key is needed locally. It commits and pushes one
   season at a time and stops at the first season that fails, naming the season
   to resume from, so a range can be re-run safely. Running
   `python3 src/fetch_cfbd_players.py 2004 2025` locally does the same fetch if
   you have a key. Either way, follow a backfill with a dashboard build.
   A full 2004-2025 backfill adds roughly 290 MB to the repository, since the
   archive is committed like the rest of `data/raw/`.
   Player coverage depends on the source feed. `src/audit_game_coverage.py`
   records coverage by season for efficiency, verified venues, and player lines.
4. Runs the full test suite against the freshly fetched data.
5. Commits the new data and rebuilt site in **one** commit, named
   `Scheduled data refresh: season YYYY`, only if something changed.

The fetched data is committed on purpose: `data/raw/*.csv` is what every
rebuild starts from, so a refresh that didn't commit it would be undone by
your next push.

## If a run fails

Nothing is published and the live site stays on its last good version. Open
the failed run in the **Actions** tab and expand the red step:

- **"Repository secret CFBD_API_KEY is not set"**: do the one-time setup above.
- **Fetch step failed**: usually CFBD being briefly unavailable or a rejected
  key. The next scheduled run tries again; a key problem needs a new secret.
- **Test step failed**: the fresh data broke an expectation. That's exactly
  what the gate is for; the details are in the test output.

## Optional: skip advanced stats

Settings -> Secrets and variables -> Actions -> **Variables** tab ->
**New repository variable**: name `FETCH_ADVANCED_STATS`, value `0`.

## Good to know

- GitHub pauses scheduled workflows in a repository with no activity for 60
  days (mostly relevant in the offseason). The Actions tab shows a banner with
  an **Enable workflow** button if that happens.
- After each season's title game, add the champion to
  `data/reference/national_champions.csv` (see `data/reference/README.md`);
  that part is deliberately never automated.

### Source workflow completions

`Backfill player box scores` and `Sync rosters and coaches` now queue the deployment workflow through `workflow_run` on main. Their `[skip ci]`/GITHUB_TOKEN commits no longer require a manually dispatched deploy. Completion, including a failed run that committed earlier seasons, rebuilds committed data without fetching CFBD again. The source repository and branch are checked; the build acquires the existing deploy lock, then resets to current main. A source update that arrives during a build queues a later build from the new tip. Confirm both completion and overlapping-update behavior in Actions after merging this workflow change.

## When the pipeline goes dark

Nothing publishes while the `test` job is red: `rebuild-and-deploy` runs
behind it, so the live site keeps serving its last good build and fresh
ratings never reach it. That state used to be invisible. Every scheduled run
failed between 2026-10-09 06:31 and 2026-10-10 00:13 UTC -- six runs, four
days with no ratings refresh -- and the only place it showed was the Actions
tab.

So the workflow keeps one issue as the pipeline's health light, labelled
`pipeline-alert`:

- The first failure on `main` opens it, naming the run.
- Later failures comment on the same issue rather than opening more. The
  failure that matters here repeats on a schedule, and the useful fact is
  "still broken, since when", not six copies of the same notification.
- The next successful run comments and closes it.

So an **open `pipeline-alert` issue means the site cannot publish right
now**, and no open issue means it can. Pull requests are excluded, because a
red PR already shows its own check to whoever pushed it.

`scripts/alert_pipeline_state.py` holds the logic and talks to the issues API
with the run's own `GITHUB_TOKEN`. It can never fail a run: a missing token
or a GitHub outage prints a warning and exits 0, because losing an alert is
not a broken build.

This covers `ci-and-deploy.yml` only -- the workflow that publishes. A
failure in `live-scores.yml`, `sync-people.yml` or `backfill-players.yml` is
still only visible in Actions.
