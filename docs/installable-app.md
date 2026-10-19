# Elocast as an installed app

Phase 0 of the app roadmap: the site can be added to a phone home screen, it
keeps working without a connection, and it knows which teams you follow. No
server, no account, no new data.

## What makes it installable

Three files, and nothing else:

| File | What it does |
| --- | --- |
| `manifest.webmanifest` | the name, the icons, `display: standalone`, and the start URL |
| `service-worker.js` | the offline shell, and the caching rules below |
| `ui/brand/icon-{192,512}.png`, `icon-maskable-512.png` | rendered from the mark by `scripts/build_brand_assets.py` |

Both live at the repository root, not in `ui/`, for one reason: **a service
worker can never control a path above its own directory.** From the root it
covers `index.html` — the page people actually paste a link to — as well as
everything under `ui/`. Moved into `ui/`, every other check would still pass
and the root redirect would quietly stop working offline.

The manifest is linked from both entry points, because either can be the first
page a browser sees. iOS reads almost nothing from it, so the
`apple-mobile-web-app-*` tags beside it are what give an installed iPhone icon
its name and a chromeless window.

## What is cached, and why each rule is what it is

A stale service worker is the classic way to serve a week-old site forever, so
the only strategy that can do that — cache-first, which never asks the network
again — is allowed for exactly one kind of URL.

| Request | Strategy |
| --- | --- |
| anything carrying `?v=<hash>` | **cache-first** |
| documents and navigations | network-first, falling back to the last cached copy |
| `ui/data/live_scores.json` | network-first, keeping the last good scoreboard |
| other same-origin files | stale-while-revalidate |
| Google Fonts | stale-while-revalidate |
| non-GET, range requests, any other host | not touched at all |

Cache-first is safe for the first row and only the first row, because every
generated asset this site serves carries `?v=sha256(file)[:10]`, stamped in by
`build_dashboard.py` and `src/export_static_data.py`. **The URL changes when
the bytes change**, so a cached copy can never be stale: a deploy asks for a
URL the cache has never seen. `isVersioned()` is the guard, and
`tests/service_worker.test.cjs` drives the real worker to prove that the same
file without a `?v=` still reaches the network every time.

`ui/dashboard.html` is deliberately not in that row. It is the file the hashes
are stamped *into*, so it has no version of its own and is fetched fresh
whenever there is a network. Its cached copy is the offline screen: the
dashboard carries its ratings payload inline, so an offline launch shows the
last ratings this device saw rather than a browser error.

Install does one thing beyond fetching that table's files: it reads the page
it just cached and precaches the `<script src="...?v=hash">` files named in it.
Those cannot be listed by hand, because their URLs change on every deploy, and
the runtime rules never see them on a first visit — the worker registers on
`load`, by which time the page has already fetched them uncontrolled. Without
that step, a first visit followed by an offline launch draws an empty page:
`cacheFirst` has no copy, the fetch fails, and the inline application reaches
an undefined `CfbNavigation`.

Documents and assets go to separate caches, and the precache list is split the
same way. `caches.match()` returns the first hit across every cache in creation
order, so an unversioned asset precached into the document cache would keep
winning over the fresh copy stale-while-revalidate writes to the asset cache,
and the deploy that changed it would never reach anybody.

The stale-while-revalidate refetch is handed to `event.waitUntil()`. Once
`respondWith()` settles the browser may terminate the worker, so without it the
revalidate half of the strategy is not guaranteed to happen at all.

The asset cache is capped at `MAX_ASSET_ENTRIES`, oldest insertion out first.
A season of browsing touches thousands of per-game and per-player files and an
installed app must not grow without limit on someone's phone.

`CACHE_VERSION` names the worker's own rules, not the site's data. Bump it when
a rule changes; `activate` then deletes every `elocast-` cache that is not
current and leaves anything else alone.

## What the app shows that the site does not

An installed window and a browser tab are the same document. The difference is
one class, `is-app` on `<body>`, set from `display-mode: standalone` (or
`navigator.standalone`, which is all iOS had before 16.4) and re-read whenever
the display mode changes. Every app-only rule in the stylesheet hangs off it,
and `tests/test_installable.py` fails if a rule that reveals the bar forgets
the gate — a fixed bar across the bottom of the website is the one failure here
that no test driving the page in jsdom would notice, because jsdom does not lay
out.

In the app:

- The five sections move to a bar along the bottom, as icons with labels, where
  a thumb reaches them. The links carry the same `data-section` attributes as
  the masthead's tabs, so `renderNavigation()` marks the current section in
  both with one pass and the two can never disagree about where you are. The
  icons are inline SVG, so they are part of the document the service worker
  already caches and cost no request that could fail offline.
- The masthead's own tabs are hidden, and the masthead keeps the title and the
  search.
- **Data Coverage, How It Works and Track Record come off the chrome
  entirely.** They are reference reading rather than places to go, and three
  more targets in a five-target bar would make every one of them smaller. Their
  routes still work; nothing links to them.

Two window details go with it. `viewport-fit=cover` is what makes
`env(safe-area-inset-*)` resolve to the real insets, which the bar needs to sit
above the home-screen indicator rather than under it. And the iOS status bar is
`default`, not `black-translucent`: translucent draws the page under the clock,
and iOS writes that clock in light ink whatever is behind it, which was white
on the light theme's cream. An opaque bar takes its colour from `theme-color`,
now declared once per colour scheme.

One trap worth recording: the site colours every link amber through a
`:root a:any-link:not(.active):not(.action-button)` rule that outweighs a plain
`.app-tabs a`, so the bar's resting ink had to be stated at a higher weight.
Measured in Chromium rather than assumed — before that rule, the active icon
and the four inactive ones were all `rgb(181, 116, 26)`.

## Favorite teams

`ui/favorites.js` stores a list of team slugs in `localStorage` under
`elocast.favorites.v1`. That is the whole feature. There is no account and no
server, deliberately: nothing personal is collected, there is no privacy policy
to maintain, and it is the same shape a server would later store behind an
anonymous device token.

Slugs, not ids or names, so a saved favorite survives a rebuild and resolves
through the page's existing `TEAM_BY_SLUG` lookup. Anything that is not a slug
is dropped on read, so a hand-edited or stale value degrades to "no favorites"
rather than throwing on a page that has nothing to do with favorites. Storage
that is absent or throws outright — private browsing, a blocked context, a page
opened from `file://` — reads as an empty list.

The star is on a team page; the followed teams appear as **My teams** at the
top of the home route.

## Since last week

A team page tab (`#team=<slug>&tab=week`). It answers the question the roadmap
calls the highest-value thing on its list: what the week just played did to
this team.

Every movement number comes from `data/elo_timeline/<season>.js`, which
`src/elo_timeline.py` builds from the engine's own audit trail. `d_elo` and
`d_rank` are a team against **its own** previous stage, and nothing is
recomputed in the browser.

Whether the team played is read off the games, not off the timeline's
`stage_games`. That column counts rated games, and a game against an opponent
this project rates nothing for is in no `elo_game_history` row at all — so a
team whose only game that week was an FCS opponent has a `stage_games` of 0 and
would read as a bye. It played, its rating is unchanged because the opponent is
unrated, and the panel says that and lists the game.

Two Elo figures appear on one screen and they are measured differently: the
week's change is computed from the rounded ratings the tables display (so a
column of movements always reconciles with the ratings beside it), while a
game's change is the engine's raw number for that game. On a one-game week
they can differ by a tenth, and the panel says so.

### What is not there yet

**How the seed and the title odds moved.** The panel shows where both stand
today and says plainly that it is doing so. Both are rebuilt from scratch on
every refresh and no week-ago copy of either is kept, so there is nothing
honest to subtract.

Closing that needs a weekly snapshot, and the shape of it is known: the field
is selected from Elo entering the stage plus the conference champions derived
from the games played up to it, and the odds are the same simulation
`build_playoff_data` already runs, per stage rather than once. Both are
point-in-time questions the database can answer; neither is a browser-side
calculation. That is a measurable piece of work on the model side rather than a
display change, which is why phase 0 ships the rating and the rank and says
what it cannot yet show.
