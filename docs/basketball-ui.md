# One Elocast, two leagues: the interface contract

Austin's direction, 2026-10-20: the league selector is the main control near
the top, the existing bottom navigation stays, and the current look carries
across sports without a redesign. This file is the contract B05 is built
against, with the places it meets measured data called out, because three of
the elements below have no data behind them yet.

BUILT 2026-10-20, unlinked: `ui/basketball.html` and `ui/ncaambb_app.js`,
behind the B04 publisher gate. Where this file says a thing is not built, read
the note at the end of the section; three of the elements below still have no
data behind them and the page says so rather than inventing it.
The data behind it is in [`basketball-ingestion.md`](basketball-ingestion.md)
and [`basketball-coverage.md`](basketball-coverage.md); the release gates are
in [`basketball-integration.md`](basketball-integration.md).

## 1. The league selector

Directly beneath the Elocast wordmark, above the search field. A compact
segmented control while there are two choices — `College Football` ·
`College Basketball` — replaced by a dropdown or a bottom sheet when NFL and
NBA arrive. In that picker, college basketball is labelled **Men's
Basketball**, because the women's game is a different competition and an
unqualified label would be a claim we are not making.

### The URL rule football parity depends on

A league belongs in the route, so a link carries it. But **every football deep
link published to date must keep opening the same page**, so football is the
default and omits the parameter entirely:

| Route | League |
| --- | --- |
| `#section=rankings&view=top25` | football, unchanged |
| `#league=ncaambb&section=rankings&view=top25` | basketball |

This is the same shape `ui/navigation.js` already uses for a section's first
subview, which is left out of the URL. The slug is `ncaambb`, matching the
identity namespace in `src/ncaambb/identity.py`, so one word means one thing
across the stack.

**Switching leagues keeps the destination** where the destination exists in
both: football Standings → basketball Standings. Where it does not, the switch
lands on that league's Home rather than on an empty panel, and says why in one
line. The last league visited is remembered in `localStorage`; a URL carrying
a league always wins over the remembered one, or a shared link would open
somebody else's sport.

## 2. My teams is per league

Football keeps the BYU football card. Basketball gets its own BYU card with its
own record, rating, ranking and next game. Each card carries a small
`Football` or `Men's Basketball` label.

**Favorites must not be reinterpreted.** `ui/favorites.js` stores football team
slugs under `elocast.favorites.v1`. Basketball gets its own key
(`elocast.favorites.ncaambb.v1`); the existing key stays football's and is
never re-read as anything else. Following BYU football **offers** "Follow BYU
basketball too?" and never acts on its own — one tap must not quietly create a
second subscription.

## 3. Scoreboard controls per league

| Element | College football | College basketball |
| --- | --- | --- |
| Main time selector | `Week 8` | date, with previous/next day |
| Season label | `2026` | `2026-27` |
| Game progress | quarter · clock | half/OT · clock |
| Filters | All · Live · Upcoming · Final | the same |
| Sorting | Featured · Kickoff | Featured · Tipoff |

A compact date strip with **Today** centred, not a weekly selector: a
basketball season is about 6,300 games over five months, so a week is both too
coarse to navigate and too fine to summarise.

The season label comes from `season_label()`, so `2026-27` is ending year
**2027** everywhere — in the label, the route and the database. Nothing infers
a season from the calendar year.

### Two measured facts this table has to absorb

**44% of the 2026-27 schedule has no tip-off time.** 2,669 of 6,015 games
carry `startTimeTbd`. A date strip can place them on a day; a `Tipoff` sort
cannot order them against each other, and rendering an unknown time as
midnight would put them first and read as a real time. They need their own
bucket on the day, after the timed games, labelled as undecided.

**There is no in-game clock for basketball yet.** The B02 archive holds final
scores and period points, not a live half or clock, so `half/OT · clock` has
no source. Until a basketball live feed exists, a basketball game card can
honestly show scheduled, final, or a score with no clock — and must not show a
zeroed clock, which reads as the period expiring.

## 4. The bottom tabs keep their names and change their contents

`Home · Stats · Standings · Teams · People` already works for both, and is the
bar `#app-tabs` renders today. Search defaults to the selected league, with the
placeholder naming it ("Search college basketball…").

**A feature appears only in the leagues where it is ready.** Basketball starts
far thinner than football, and that is the rule that makes it honest rather
than broken:

| Tab | Basketball at first release | Football |
| --- | --- | --- |
| Home | schedule, results, My teams | unchanged |
| Standings | team ratings once B03 lands | everything it has now |
| Teams | team pages | plus stadiums, home-field, history |
| People | player pages | players, coaches, awards, recruiting |
| Stats | nothing at first | the full analytical set |

Basketball's Stats eventually carries Player Impact (B08) and Roster Scenarios
(B09). It should carry nothing until one of them is ready; an empty tab is
worse than an absent one.

Basketball has **no coaches, no awards and no recruiting data** — B02 loaded
players only. So basketball's People tab is player pages and nothing else, and
the football tools must not be wired to a basketball league that has no rows
for them.

## 5. The game panel design carries over

Team colours, logos, ratings, probabilities and the venue footer translate as
they are. Basketball needs different game-status details and can take denser
cards, because a basketball night holds many more games than a football
Saturday. It does not need a different visual identity.

## 6. No "All sports" feed in the first release

A clear league selector keeps rankings, seasons, favorites and scores
understandable. A combined Following feed can come later, with BYU football,
BYU basketball and the Jazz each carrying an explicit league label.

## Freshness, per league

Each league shows its own freshness, prominently when stale. `feedAgeShort()`
already produces the short form and is silent under ten minutes; the stale
treatment needs to be a badge that cannot be mistaken for a timestamp.

One thing the badge does **not** do: make the data fresher. A football
scoreboard reading "Updated 40h ago" is not a display defect. Measured between
2026-09-27 and 2026-10-10, GitHub created **22 of the 699 cron firings**
`live-scores.yml` asks for — 3.1%. The page has always polled every 60
seconds; publishing is the limit. Making the staleness visible is right and
worth doing, and the fix is still the open `LIVE_FEED_URL` publisher decision
in [`live-score-delivery.md`](live-score-delivery.md). Basketball will inherit
the same ceiling the moment it has a live feed at all.

## What has to exist first

1. **B03** — validated team Elo. Ratings, rankings and win probabilities on
   every screen above depend on it. Basketball has no rating today.
2. **B04** — the export contract and publisher gate. Nothing basketball may be
   published before it, and a production loader must refuse a team sample or an
   unsupported schema rather than render it as national rankings.
3. Then B05 builds this, unlinked, and B06 turns the selector on.

## What was built, and what it does instead

- **The league selector** is the segmented control beneath the wordmark. It is
  built on both pages, but on the football page it is **STAGED OFF**: see the
  section below. The basketball header links back to football either way. It is a link rather than a route, because every
  football deep link published to date must keep opening the same page. It does
  not redirect; the switch logic that keeps a destination across leagues is in
  `switchLeague()` and is tested, and comes into its own when the two leagues
  share one page.
- **My teams** is not in the preview. The favorites layer under it is:
  namespaced storage, no re-reading of football's key, and an offer that never
  acts. The card itself waits for the per-league home screen.
- **The scoreboard** opens on today, clamped into the loaded season: before
  the season starts that is opening night, not the final. It used to open on
  the last date in the payload, which — since the export carries the whole
  published schedule — was the national championship, months away, and the
  strip labelled it "Today" because it marked its own centre rather than the
  real date. Both are fixed, and `dayStrip` now takes today as its own
  argument. It has the date strip, the four filters,
  and the undecided bucket after the timed games. The `Live` filter returns
  nothing, honestly, because there is no basketball live feed: it is kept so
  the control matches football's.
- **No card shows a clock**, and a test asserts no status line anywhere
  contains one. A missing score renders as nothing, never as a zero.
- **Stats is shown disabled** rather than hidden. A tab that vanishes between
  leagues is more confusing than one that says it is not ready yet. **Teams
  and People are disabled on the same grounds**, and for a reason worth
  separating: both are real basketball sections — the league holds the data
  and `switchLeague` is right to carry a reader from football's Teams to
  basketball's — but this page draws only the board and the scoreboard. A tab
  enabled with no panel behind it hid both and left an empty page, which is
  the exact thing disabling Stats was meant to avoid. The page now gates a tab
  on having a panel as well as on the league having the section.
- **Freshness** is a badge that reads `Stale · updated 26h ago` in red, or the
  plain age in green. A stale export is still drawn, because it is the last
  good snapshot.

## The selector is staged off in the first release

Decided 2026-10-21, with Austin ("let's roll"): the basketball side is
finished and tested, but its first export cannot be built, because the
provider's rolling seven-day allowance is spent and does not recover until
2026-10-27 (measured; see
[`basketball-refresh.md`](basketball-refresh.md)). A visible league tab with
no data behind it is worse than no tab, so the code lands with the tab absent
and the tab goes on once the archive is exported.

Nothing is deleted. Three things are parked, each with a comment saying so:

1. `ui/dashboard_shell.html`, in the `.title-block` header — the
   `<nav class="leagues" id="league-selector">` markup, inside an HTML
   comment.
2. `ui/dashboard_shell.html`, at the end of the inline script — the
   `rememberLeagueChoice()` listener, commented out line by line. It must not
   redirect when it returns: a football link has to open football even for a
   reader whose last visit was basketball, or a shared link opens somebody
   else's sport and the installed app's start URL moves on its own.
3. The `#league-selector a` CSS stays **live**, styling nothing. It is
   addressed by id deliberately, to beat the shell's global
   `:root a:any-link:not(.active):not(.action-button)` rule at (0,3,1); that
   specificity trap was the part that was hard to get right, and deleting the
   rule would mean rediscovering it.

`ui/basketball.html` is unchanged and still complete. It is simply not linked
from anywhere, exactly as it was at the end of B05.

**Turning it on** is: uncomment (1) and (2), then invert the tests under
`--- what the page must not do ---` in `tests/basketball_preview.test.cjs`,
which name the parked blocks. The tests fail loudly if a parked block is
deleted rather than re-added, so there is no silent way to lose this.

### Turning the nav on is not all-or-nothing

Revised 2026-10-21, once the NFL landed its own export and Austin agreed to
turn the selector on with it. The nav is shared, so the rule that matters is
per league rather than per nav: **re-add the nav with the leagues that have an
export, and leave the others parked.** A league listed there with nothing
behind it is the single thing this staging exists to prevent.

The tests were narrowed to match, and they now pass whether the nav is off or
on, failing only if the *basketball* entry goes live before `ui/data/ncaambb`
is published, if it is deleted rather than parked, or if the remembered-league
key drifts from the one `ui/ncaambb_app.js` uses. That is deliberate: a guard
that blocks another league's release gets deleted by whoever is trying to
ship, and then protects nothing.
