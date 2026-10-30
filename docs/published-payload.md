# The published payload, and the 1 GB ceiling

GitHub Pages refuses to publish a site over **1 GB**. On 2026-10-30 `ui/` was
**637 MB**, up from 466 MB eleven days earlier. Nothing was wrong with the
site; the archive simply grows with every season played and every refresh. This
is the one open item that can stop the site publishing at all, so it is worth
writing down what the space goes on and which cuts are honest ones.

## What the space goes on

Measured on `main` at `ffb4cea`:

| Directory | Size | What it is |
| --- | --- | --- |
| `ui/data/players` | 245 MB | per-game box-score lines, 2004-2026 |
| `ui/data/game_logs` | 102 MB | the same lines turned the other way up, by player |
| `ui/data/people` | 91 MB | player and coach pages |
| `ui/data/nfl` | 40 MB | the NFL export |
| `ui/data/drives` | 20 MB | drive-by-drive records |
| `ui/data/team_pages` | 16 MB | team pages, sharded in #157 |

## The box scores: spelling, not content

The box scores were two thirds repetition. A line was
`{"name":"Marlin Dean","stat":"0","id":"5220652"}` — nineteen bytes of key
spelling on every line — and within a single shard a player's name and athlete
id recurred once per statistic he recorded, because CFBD publishes a row per
rostered player per statistic. Measured on 2025: **306,966 lines drawing on
8,056 distinct names**, and 47% of those lines carry a zero.

So a shard now carries its own string tables and positional rows:

```js
(window.__CFB_BOX_QUEUE__=window.__CFB_BOX_QUEUE__||[]).push(
  {"v":2,"s":2025,"n":["Marlin Dean",...],"i":["5220652",...],
   "c":["defensive",...],"t":["TD",...],
   "g":{"401752672":[["Georgia State","away",[[0,0,[[0,"0",0],...]]]]]}})
```

`encode_box_score_shard` in `src/export_static_data.py` writes it,
`decode_box_score_shard` beside it and `decodeBoxScores` in
`ui/dashboard_shell.html` read it, and tests hold all three to a round trip.
**No value is dropped.** Re-encoding the whole shipped archive: **255.3 MB →
83.4 MB, 67.3% off**, which takes `ui/` to about 465 MB.

Three rules worth keeping:

- **The tables are per shard, never per season.** A game page fetches one
  shard, so a table spanning the season would be bytes the reader must fetch
  and cannot use. Names still recur enough *within* a shard — a player appears
  once per statistic — that interning pays: 66% on 2025, 70% on 2005.
- **A shard queues itself rather than merging into `window.__CFB_PLAYERS__`.**
  An encoded payload and a decoded season cannot share one object, so
  `drainBoxScoreQueue()` is what merges them. It `shift()`s, so a shard that
  arrives mid-drain is still there for the next one.
- **The format is self-describing.** A v2 shard pushes onto the queue; the old
  object form assigned a season. The page reads whichever it is served, which
  matters because pull requests here carry source only — between a merge and
  the next deploy the repository holds old data with a new reader. The
  manifest's `format` is for a human reading the manifest, not for the page.

## What is left, in the order the bytes are

1. **`game_logs`, about 17 MB.** The same dictionary trick on the category and
   statistic keys measures 16-18%, not 67%, because the index costs nearly as
   much as the key it replaces. Worth doing, not worth doing first. Note the
   plain `date` field looks duplicated by `kickoff_utc` and is **not**: one is
   the game's local date and the other UTC, and deriving either from the other
   is a trap this project has already been caught by.
2. ~~The 47% of box-score lines whose statistic is zero.~~ **DONE** — Austin
   took this call on 2026-10-30 ("I see no point in pulling a bunch of
   inferred, irrelivant data, so I'm good tripping that out of the
   scoreboard"). `drop_zero_lines` in `src/export_static_data.py` leaves them
   out of the shards the game page reads, and the panel now says outright that
   it lists only what a player recorded, so an absent line is not read as a gap
   in the data. Measured on the shipped archive: **38.9% of lines go, the
   encoded archive falls 83.4 MB → 59.1 MB (29.1% further off), and no game
   loses its box score**. One game page went from 411 rows to 166.

   Note the saving is 29%, not the "roughly half" an earlier version of this
   document predicted. Halving was right of the *old* format, where each zero
   line also carried a spelled-out name and athlete id; the encoding in #158
   had already deduplicated exactly those bytes, so what was left to remove was
   the smaller part. Two savings on the same bytes do not add.

   The career game log in `ui/data/game_logs` deliberately keeps its zeros: it
   is read a game at a time, and a game where a player's only lines were zeros
   would otherwise vanish from his career entirely. `data/raw/player_boxscores`
   keeps every line either way — this is a statement about the page, not the
   archive.
3. **Non-FBS box scores.** Named in earlier reviews as the obvious cut. In fact
   `export_static_data.export` already keeps only games the database rates and
   has played, so this one is mostly already paid for; measure before
   believing there is a win here.
4. **`.git` is 255 MB**, which is not the Pages limit but is the clone. Most of
   the growth is the live-scoreboard refresh committing `ui/data/live_scores.json`
   every few minutes and the per-season data commits.
