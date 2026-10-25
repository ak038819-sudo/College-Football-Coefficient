# Awards and headshots: what I found, and what the decision is

Written 2026-10-25 as the research half of phase 4 of NFL parity. Everything
below was measured from this container today, and where I could not measure
something I say so rather than guessing. Neither of these is blocked on
engineering; both are decisions about other people's terms.

## Awards

**There is no awards file in nflverse.** The release tags carry players,
rosters, statistics, the draft, play-by-play, snap counts, injuries, depth
charts, officials and the combine. No MVP, no All-Pro, no Pro Bowl, no
Offensive Rookie of the Year.

Two things are close but are not it:

- `draft_picks.csv` carries career `all_pro` and `pro_bowls` **counts** per
  drafted player. The NFL side already stores and shows them on the draft
  board. They say how many, never which season or which trophy, and they
  exist only for players who were drafted from 1999 on.
- Pro Football Reference's award pages are the usual source, and nflverse's
  own `pfr_*` tables are scraped from PFR.

**Every source that has the real thing is unreachable from this container.**
Measured today, all three refused at the egress proxy:

| host | result |
| --- | --- |
| `www.pro-football-reference.com` | connection refused by the proxy |
| `www.nfl.com` | connection refused by the proxy |
| `site.api.espn.com` | connection refused by the proxy |

That is not the obstacle it looks like: the college side's whole awards
archive is fetched in a GitHub Actions workflow, where the network is open,
and the NFL refresh is already a workflow. **The engineering is a day.** The
question is whether we should.

### What the question actually is

The college awards came from ESPN's undocumented public API. Pro Football
Reference is a different proposition: Sports Reference's terms of use state
that automated scraping is not permitted, and their robots.txt and rate
limiting are enforced in practice. I could not read either document from here
to quote it, so treat that as my recollection rather than a verified
quotation — but it is a well known position and I would not want to build on
it without you deciding to.

So the three honest options are:

1. **Leave awards out.** The NFL side keeps the career All-Pro and Pro Bowl
   counts it already has on the draft board, and says awards are not here.
   Costs nothing, loses the Stats → Awards page the college side has.
2. **ESPN's API, as the college side does.** Same provenance, same unstated
   terms, consistent with what is already shipped. I would want to check
   first that it carries NFL awards at all, which I cannot do from here — it
   would be a CI probe before any build.
3. **Ask nflverse.** They take data contributions and an awards table is a
   natural fit for them. Slowest, cleanest, and not in our hands.

My recommendation is **2 with a probe first**, and 1 if the probe comes back
thin. It is the same decision we already made on the college side, so a
different answer here would be inconsistent rather than safer.

## Headshots

This one got clearer, and in a useful direction.

**The images are the NFL's own, not a third party's.** The player file
carries a `headshot` URL for 24,704 of 24,856 players — 99.4% — and every
single one of them is on `static.www.nfl.com`, the league's own Cloudinary
delivery:

```
https://static.www.nfl.com/image/private/f_auto,q_auto/league/hwncbbaztu3pc5unqgnj
```

That is a different situation from the college side, where the blocker was a
commercial licensor: Sportradar's NCAA headshots are current-season only, the
ids do not join, and the pricing was, in your words, a seismic obstacle. None
of that applies here. There is no vendor to pay and no id to reconcile: the
URL is already beside the player.

**What is still a decision.** Using them means hot-linking the league's CDN
from a public site, which is a use of NFL imagery we have not been granted
and a dependency on a host that can block us or change its paths. The
alternative — copying 24,704 images into the repository — is both a larger
licensing claim and about a gigabyte of git history, and I would not do it.

The URL is **deliberately not stored** in the database today, for the reason
the schema says: storing it would put publishing one commit away.

### The three options

1. **Leave it.** Status quo. The player pages carry a jersey plate and a
   career strip, as the college pages do.
2. **Hot-link the NFL's CDN.** One column in the export and an `<img>` on the
   player page. A day's work, no cost, and it is the league's imagery used
   without permission on a public page.
3. **Ask the NFL.** There is a media-rights contact; a hobby site asking to
   display league headshots with attribution is a small ask and may simply be
   answered.

I would not ship 2 without you saying so, and I would not describe it as a
technical blocker, because it is not one. It is yours to call.

## One correction worth recording

`nflverse/nflverse-data` — the repository every people, play-by-play, snap
and injury file in this project comes from — **is licensed CC BY 4.0**
(`LICENSE.md`, Attribution 4.0 International, read today). Earlier notes in
this project said the nflverse repositories carried no licence. That was
right about `nflverse/nfldata`, which serves `games.csv` and still has no
`LICENSE` of any kind, and wrong about `nflverse-data`, which is the source
of almost everything we now hold.

CC BY asks for attribution, which `docs/nfl.md` already gives. The one
remaining licence question on the data is `nfldata`'s silence, and that is
the file the game archive comes from.
