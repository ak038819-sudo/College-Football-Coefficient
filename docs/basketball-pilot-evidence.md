# Basketball pilot: measured evidence, 2026-10-20

What the real pilot runs showed. Numbers here are measurements of a
**three-game, single-team sample**, not a claim about the archive. Nothing here
establishes national coverage, and nothing here is a validated rating.

## The run this records

| | |
| --- | --- |
| Workflow | `College basketball player-impact pilot`, run [39365539760](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/39365539760) |
| Source revision | `de5f9bb` on `claude/project-thread-3zppra` (baseline main `8abe5e4`) |
| Season | 2026 ending year, labelled 2025-26 |
| Team | BYU (provider team id 18) |
| Retrieved | 2026-10-20T03:41:06.880463+00:00 |
| Upstream requests | 11: one `/games`, three each of `/lineups/game/{id}`, `/substitutions/game/{id}`, `/plays/game/{id}`, and one `/games/players` |
| Artifact | `cbb-pilot-2026-39365539760-1`, 130,091 bytes, sha256 `62d797b072109bfa1eb2788b31746265d2079a2ca1496c0ee42227b7af2be206`, expires 2026-11-19 |

Earlier runs, same key and workflow, both green: [39363370056](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/39363370056)
(season 2026, before this change) and [39364743733](https://github.com/ak038819-sudo/College-Football-Coefficient/actions/runs/39364743733)
(season 2025). Per-file sha256 hashes are in each run's `manifest.json`.

## Access is real

`CBBD_API_KEY` is present and entitled for `/games`, `/lineups/game/{id}`,
`/substitutions/game/{id}`, `/plays/game/{id}` and `/games/players` for seasons
2025 and 2026. No authorization, quota or schema failure occurred in three runs.
Three green runs prove access; they do not prove coverage.

The 2025-26 schedule returned **35 games, all 35 final**, which is right: ending
year 2026 is a *completed* season. The live season is ending year **2027**, which
no run here has touched and which the pilot workflow cannot currently select.

## Finding 1: the lineup feed under-covers floor time

The box score and lineup feeds are two independent measurements of the same
floor time. One team's regulation game is 200 player-minutes (40 minutes, five
players), and the lineup feed's summed `totalSeconds` × 5 ÷ 60 should equal the
box score's summed player `minutes`. In two of the three sampled games it does
not.

| Game | Plays | Subs | Box score minutes | From lineup feed | Missing | Points vs final |
| --- | --- | --- | --- | --- | --- | --- |
| 212782 | 495 | 74 | 200.0 | 162.92 | 37.08 (18.5%) | 71 = 71 |
| 209835 | 442 | 71 | 200.0 | 147.67 | 52.33 (26.2%) | 98 = 98 |
| 213067 | 391 | 60 | 200.0 | 200.0 | 0 | 85 = 85 |

The defect is in the **lineup feed**, not the box scores. Each box score is
internally consistent: five starters, 200 player-minutes against a two-period
game, and points equal to the final score. The lineup feed independently fails
its own checks in the same two games, for **both** teams (BYU 18 with opponents
338 and 111): its seconds do not cover the game duration and its points do not
reconcile to the final score.

Consequence for B07: aggregate five-player lineup rows cannot be treated as a
complete record of a game. Stints have to be rebuilt from substitutions and
plays, with the box score as the reconciliation anchor, and per-game coverage
has to be measured rather than assumed. The plan already said a successful
collection is not validated data; this is the measurement behind that.

Three games of one team cannot give a rate. What is established is that the gap
occurs, is large, and is not detectable from the lineup feed alone.

## Finding 2: substitution timing is nested, and the stints are valid

The first run of this change reported every sampled substitution as missing a
period or clock. That was this checker's wrong assumption, not a provider gap.
A substitution row carries one athlete's stint with the timing nested under
`subIn` and `subOut`, each with its own `period` and `secondsRemaining`; there
is no top-level clock to read. Pinned against the provider's published
`PlayerSubsititution` and `PlayerSubsititutionSubIn` schemas.

With the right fields read, **all 205 substitution stints across the three games
are valid** — none ends before it starts. Plays do carry a top-level `period`
and `secondsRemaining`, and all 1,328 plays are in non-increasing clock order
within their periods.

## Finding 3: two separate provider record caps

`/games` caps at 3,000 records; `/games/players` caps at **1,000 games**, and the
two are different limits on different routes. Both are now named constants and
both are refused rather than silently accepted. `/games/players` takes no game
id — it filters by season, team, date range, conference or tournament — so one
request covered all 35 games, and the team id is read back from the filtered
rows instead of guessing the name-to-id mapping.

## What this does not cover

- **One side of each game.** A team-filtered box score request returns only that
  team's rows, so each game is reconciled for BYU alone. Widening it needs a
  date-range request, which returns every team playing that day.
- **One team, three games, one season.** No national or multi-season coverage
  measurement exists yet. That is B02's work.
- **Possessions.** Lineup rows carry possession counts, but no possession
  definition has been validated and no possession total has been reconciled.
- **The live season.** Nothing here touches ending year 2027.
- **Raw payload retention.** The snapshots live only in run artifacts, which
  expire after 30 days. The measured numbers and every file hash survive here
  and in the run logs, but the raw provider responses do not. Committing a
  bounded raw sample is a deliberate choice that has not been made.

## Reproducing

```
python analysis/cbb_player_impact/collect.py --season 2026 --team BYU --limit 3 \
  --out analysis/cbb_player_impact/snapshots/<new-dir>
python analysis/cbb_player_impact/summarize.py \
  --snapshot analysis/cbb_player_impact/snapshots/<new-dir>
python -m pytest tests/test_cbb_player_impact.py \
  tests/test_cbb_integration_contract.py tests/test_cbb_reconciliation.py -q
```

The collector refuses an existing output directory, so each snapshot is its own
run. Supply the key through the environment or the repository secret, never as
committed content.
