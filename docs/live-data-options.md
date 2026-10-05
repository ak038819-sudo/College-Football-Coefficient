# Live data options

Reviewed 2026-10-04 against provider documentation. No credentials purchased, contract assumed or production feed replaced.

| Option | Documented capabilities | Next evaluation |
| --- | --- | --- |
| Current CFBD pipeline | Existing scores, archive and scoped team mapping in this repository | Measure end-to-end scoreboard age and errors before changing providers. |
| Sportradar NCAA Football | Game coverage flags distinguish full play-by-play and extended box scores; expected_latency reports feed lag. Realtime customers can use push. | Trial exact FBS/FCS coverage, ID mapping, corrections, game completion transitions and latency; confirm redistribution terms and quote. |
| SportsDataIO College Football | Live game state/scores, live/final box score endpoint; realtime guidance suggests polling scores/boxscores every 3–5 seconds under an appropriate subscription. | Trial the same slate and measure discrepancies; confirm plan, coverage and redistribution rights. |

Sources:
- https://developer.sportradar.com/football/docs/ncaafb-ig-live-game-retrieval
- https://developer.sportradar.com/football/reference/ncaafb-game-statistics
- https://sportsdata.io/developers/coverages/ncaa-football
- https://sportsdata.io/developers/api-documentation/ncaa-football
- https://sportsdata.io/help/refresh-rates-feeds-and-timing

A provider adapter should emit the existing normalized snapshot, retain the original provider IDs, require exact reviewed identity mappings, distinguish provider update time from fetch time, and preserve the last good snapshot on failure. Test postponed/canceled games, neutral venues, overtime, clock corrections and late final-score changes. Never substitute two providers' numeric IDs as if they shared a namespace. Keep API keys server-side.

Recommended experiment: compare at least two complete game windows with the current feed. Record median/p95 score-age, missing fixtures, identity conflicts, HTTP errors and cost at the intended polling rate. A static deployment per play is a separate latency bottleneck; choose hosting/polling architecture only after measuring both fetch and publish lag. An undocumented public scoreboard endpoint is not evidence of permission to redistribute data or a stability guarantee.
