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

- **Sunday 20:00 UTC** (3 PM Central in season): after Saturday's games and the
  AP poll's Sunday release.
- **Wednesday 12:00 UTC** (7 AM Central): picks up Tuesday-night CFP rankings
  and midweek games.
- **On demand:** **Actions** tab -> **Test, Rebuild Dashboard, and Deploy** ->
  **Run workflow** -> **Run workflow**.

Times are UTC, so they shift by an hour in local time when daylight saving ends.
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
