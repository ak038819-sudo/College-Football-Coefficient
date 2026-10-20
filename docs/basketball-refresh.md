# Keeping basketball current without putting football at risk

Release gate 6. Three things: a scheduled refresh, a decision about what may
replace what is published, and a health light that says when the refresh has
stopped working.

The plan's rule is that a basketball provider failure must not stop the
football refresh or replace valid data with empty files. Everything below is
built around that one sentence.

## The refresh is its own workflow

`.github/workflows/cbb-refresh.yml`, scheduled through the season and
dispatchable. It shares nothing with football:

- **Its own concurrency group** (`cbb-refresh`). A refresh that shared the
  deploy's group would queue behind a deploy and then be cancelled by the next
  one, which is exactly what happened to `live-scores.yml` before it got a
  group of its own.
- **Its own health light** (below), so neither sport can put out the other's
  alert.
- **Its own credentials.** `CBBD_API_KEY` from the repository secret, never
  echoed, never written anywhere. Football's key is not referenced and a test
  fails if it appears in the file.

It **never refits the model.** `src/ncaambb/fit_elo.py` chooses the parameters
and a person commits them to `config/ncaambb_model.json`; a refresh that
quietly refitted would change what the ratings mean on a schedule nobody is
watching. A test asserts `fit_elo` is not in the workflow.

It pushes back to **the branch it ran on**, not to `main` by name, which is how
the whole thing was rehearsed on a branch before it was ever allowed to write
to the published site.

The season it refreshes comes from `identity.seasons_through()`, the same
helper the rest of the code uses. A copy of the rule written into a workflow
is a copy no test can reach, and this project has been bitten by a season rule
that drifted.

## What may replace what is published

`src/ncaambb/publish.py`. The export gate asks whether an export is fit to
publish at all; this asks whether it may replace the one already live, which
goes wrong in different ways:

| situation | what happens |
| --- | --- |
| the new export is refused by the gate | the live export stays exactly as it is |
| the new export is older than the live one | refused: valid, national, and a step backwards |
| the new export has no usable as-of time | refused |
| the live export is missing or unreadable | published: there is nothing to protect |
| the build directory is empty or truncated | refused |

The swap stages the new export beside the live one and renames, rather than
emptying the live directory and refilling it. A publish that dies halfway then
leaves the previous export intact instead of a directory holding half of each.
It is a swap and not a merge, so a season the new export dropped does not
survive on the site pointed at by nothing.

A refusal **fails the run**. It is not a quiet skip, because the health light
is the only thing that would otherwise notice.

## The health light

`scripts/alert_pipeline_state.py` keeps one open issue per pipeline, labelled
`pipeline-alert`, and closes it when a run succeeds. It now takes a `--scope`,
because one light for the repository was wrong in both directions: a basketball
failure that closed football's alert would hide a site that is not publishing,
and the reverse would hide a basketball refresh that has been dead for days.
Football's default scope keeps the exact marker its existing issues carry, so
nothing already open is orphaned.

Recovery now closes **every** open alert for its scope rather than the first.
Two runs failing seconds apart opened #95 and #96 on 2026-10-18 and the next
green run closed one, leaving a red light over a working pipeline.

## Rolling back

The published export is a directory of files in the repository, so the last
good snapshot is the previous commit of `ui/data/ncaambb` and a revert of the
refresh commit is the rollback. The refresh commits nothing but that
directory and `data/ncaambb/raw` — a test asserts it never runs `git add -A` —
so a revert can never take a code change with it.

While a refresh is failing, the site keeps serving the last good export and the
page badges it stale with its age. That is the intended behaviour, not a
degraded one: refusing to draw an old-but-valid export would break "never
replace valid data with empty files" from the other direction.

## The selector

The league selector sits under the wordmark in the football header, as the
interface contract asks. It is **a link to another page, not a new route**, so
every football deep link published to date opens exactly what it always did,
and football's hash grammar gains nothing.

It does not redirect. A reader whose last visit was basketball still gets
football from a football link, because a redirect would open somebody else's
sport from a shared link and would move the installed app's start URL on its
own. The choice is remembered under `elocast.league.v1`, which is the key the
basketball page reads; a test asserts both files agree on it.

## Provider quota and the committed raw cache

The first rehearsal of this workflow failed, and it is worth writing down why,
because the cause was a design mistake rather than bad luck. It asked the
provider for every season from 2015 to the one being played, on the grounds
that the database is not committed and a fresh checkout has to rebuild it. That
is 131 requests in three minutes; CBBD answered HTTP 429 and the run lost the
last five seasons.

Two things changed.

**The gzipped provider responses are committed**, under `data/ncaambb/raw`,
one file per endpoint per season. The ingestion already preferred that cache
over a request — the only thing missing was for the cache to survive the run.
A settled season cannot change, so the refresh now reloads eleven of the twelve
from disk and asks the provider only about the season being played.

The commit step therefore runs **even when the run failed**. A season fetched
before the rate limit struck is quota already spent, and banking it is what
makes a cut-off run make progress instead of repeating itself; the job still
fails, so the health light still lights. That makes the refresh resumable by
construction, the same property `backfill-players.yml` has for football.

**A rate limit is honoured rather than guessed at.** `src/ncaambb/http.py`
backed off 2, 4, 8 and 16 seconds and gave up — thirty seconds against a
window that was longer. It now reads `Retry-After` and waits as long as the
server asked, capped at two minutes so a wrong or hostile value cannot park a
run. Only the integer-seconds form is read; the HTTP-date form is legal but
CBBD does not send it, and parsing it would mean trusting a clock difference
to decide how long to sleep.

## The season being played is not judged as a finished one

The publish gate's floors describe a season that is over: two thousand games,
and ninety per cent of the Division I list carrying a rating. Neither question
can be asked of a season in progress. In mid-November a correct export has a
few hundred results and most of the country unrated, and the gate as first
written would have refused the entire twelve-season archive because of the one
season a reader actually came to look at.

So each coverage row now says whether its season is `complete`, and the
in-progress branch asks a different question: every Division I team that has
**finished a rateable game** must carry a rating. That is still a real check —
a replay that silently dropped teams fails it exactly as it would in a finished
season — and it is the check that catches the failure mode the relaxation could
otherwise hide. The sample floor is untouched: a season in progress must still
cover three hundred Division I teams, because the team list is published before
tip-off, so a one-team pilot is caught whatever month it is.

A row that does not say is read as finished. A missing flag must not be the way
a thin export gets published. `ui/ncaambb_loader.js` carries the same rule
independently, because a relaxation made on the build side and forgotten in the
browser would refuse in the reader's page what the gate had just accepted.

## What a refresh costs

The export is about 21 MB across twelve seasons, and a season's games file is
about 1.8 MB. Past seasons serialise byte-identically from one run to the next,
so a refresh adds only the current season's file to the repository — roughly
1.8 MB a day while the season is on, plus the current season's raw cache. If that proves too heavy, the next step is
to intern the team keys inside each payload, which are 23 bytes each and appear
twice per game; it is a shape change to the export, the loader and the page,
which is why it is written down here rather than done pre-emptively.
