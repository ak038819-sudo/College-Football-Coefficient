# Elocast NBA team stats and Player Elo

## Current implementation decision

Use imported ESPN Analytics Net Points and the possession-weighted replay below for the website pilot. The team-stat work in section 1 is a separate track. Sections 2–4 preserve the earlier lineup-based research and rotation concept. Their ridge fit, 2017–18 starting scope and 500-possession eligibility describe that experiment. The website pilot uses the 500-minute rule in the current specification.

## Player Elo implementation for the current NBA site

Reviewed against [main at 0e45499](https://github.com/ak038819-sudo/College-Football-Coefficient/tree/0e45499a029ab17db140a54e24b7c117de61fd9b) on November 12, 2026. PR #200 was open. Player Elo can ship independently of the team-stat work.

### Data and identities

- Add an ESPN Analytics Net Points source adapter alongside `src/nba/source.py`. The tested historical source is the [ESPN-derived player_box Parquet archive](https://basketball-data.billmill.org/nba/espn/player_box/season=2026/data.parquet), with the season end year in the URL. Its [data dictionary](https://github.com/llimllib/nba_data/blob/main/docs/DATA_DICTIONARY.md) lists fields and revisions. The site's existing hoopR ESPN box-score adapter and this archive are separate feeds. The 2026–27 file was reachable at review and contained 3,804 usable player-game grades across 169 regular-season games dated October 20 through November 11. Production crosswalk coverage still needs measurement.
- Keep `season, game_id, player_id, team_id, played, seconds_played, o_net_pts, d_net_pts, o_team_poss, d_team_poss`. The denominator is the team's offensive or defensive possessions while the player was on court. The archive's `t_team_poss` sums both sides. Use separate denominators for the two grades.
- The archive uses NBA IDs. Elocast uses ESPN-prefixed game, team and person keys. Seed the player crosswalk from ESPN Net Points season records carrying ESPN IDs and the archive’s documented overrides, then validate with player name, season and appearance evidence. Seed game matches from the archive’s documented `espn_event_id` where present, and franchise mappings from its `espn_team_id` rows. Resolve players through verified NBA-to-ESPN mappings, then canonicalize any aliases from the people layer to a single rating identity and attach the same rating to linked profiles. Resolve games through an explicit NBA-to-ESPN event mapping. A fallback match can use the mapped home and away franchises, game type and date or tipoff with time-zone handling, but must be unique and verified. Validate the final and the player's team against Elocast's canonical game and player appearance when available. Store source IDs alongside mapped IDs. Quarantine ambiguous or unmatched rows and report the counts. Preserve NBA IDs as strings, including the leading zeroes in game IDs.
- Ingest completed regular-season games only, using Elocast's canonical schedule classification. Include Cup games that count toward the regular season and exclude the Cup final, playoffs, play-in, preseason and exhibitions. Sort by canonical date, tipoff and game key, then update once per player per game. An identical duplicate is harmless. Conflicting duplicates require reconciliation before using the grade.
- Backfill from 2023–24 for the validated three-season replay, then extend to earlier verified seasons if desired. Keep the replay start date fixed in configuration, since changing it changes opening ratings. Refresh the current-season file with the existing NBA cadence and capture its source URL, retrieval time and content hash. Older snapshots can be pinned for reproducibility. Review source availability and reuse terms before relying on it for the public feature.
- Skip DNPs and grades with missing or nonfinite values or nonpositive offensive or defensive possessions. Preserve zero Net Points as a valid grade. Count eligibility from canonical regular-season minutes in the season shown, independently of Net Points coverage. Track graded games and graded possessions alongside those minutes. A missed grade gives no update. Corrections and late arrivals trigger a deterministic replay from the earliest affected game or a full rebuild.

### Rating formula

Implement the replay in a dedicated NBA Python module such as `src/nba/player_elo.py`. Maintain a weighted total for offense and defense and a shared weight per canonical player. The first valid game initializes the rating to that game's grade through the equations below. It is provisional until the display eligibility threshold is reached.

```text
off_grade = 100 * o_net_pts / o_team_poss
def_grade = 100 * d_net_pts / d_team_poss
game_grade = off_grade + def_grade
exposure = (o_team_poss + d_team_poss) / 2
decay = 2 ** (-exposure / 2000)
new_weight = decay * old_weight + (1 - decay)
new_off_total = decay * old_off_total + (1 - decay) * off_grade
new_def_total = decay * old_def_total + (1 - decay) * def_grade
off_impact = new_off_total / new_weight
def_impact = new_def_total / new_weight
impact = off_impact + def_impact
display_elo = 1500 + scale * impact
```

Start all three accumulators at zero. Persist or replay all three across season boundaries, inactivity and trades. New possessions alone change them. For a 70-possession game, the decay gives the new grade about 2.4% weight once the model has a long history. The normalized update has more responsiveness during warmup. The trial used this normalization. Keep full precision until rendering. Put the half-life and display scale in versioned model config. **25 Elo points per +1 impact** is a proposed display default for review, with 1500 corresponding to zero net impact. The replays validated update speed in Net Points units. The display scale is a presentation choice.

### Wire it into the existing website

| Integration point | Change |
|---|---|
| `.github/workflows/nba-refresh.yml` | Run Net Points import and replay after schedule and people ingest and before `python -m src.nba.export`. Use the already installed PyArrow dependency. Exercise the first run with publication disabled. |
| `src/nba/schema.sql`, `src/nba/people.py`, `src/nba/identity.py` | Store normalized grades or derived snapshots separately from counting stats. Reuse canonical person/game keys and aliases. Key rated observations by canonical person and game. |
| `src/nba/export.py` and `src/nba/people_export.py` | Put a compact, season-scoped leaderboard in the existing season asset and optional rating summaries and histories in the existing per-player assets. Keep named fields additive to avoid shifting the compact box-score arrays. Include coverage, model version, data timestamp and leaderboard cutoff. Use the existing cache tokens and publication gate. |
| `ui/nba.html`, `ui/nba_app.js`, `ui/nba_loader.js` | Add Player Elo to the existing NBA People panel and show the rating on its player detail route. Link leaderboard names through the existing player route. Parse optional rating fields in the app code and reuse the existing player loader and cached URLs. |

Proposed public payload fields: `player_id, season, as_of, elo, impact_per_100, offense_per_100, defense_per_100, season_minutes, graded_games, graded_possessions, status`. Keep replay detail with `game_id, pregame_impact, postgame_impact, change, game_grade, exposure`. A pregame rating with no previous evidence is null. A player with no valid grade in their history is unavailable. On player pages, show a carried rating when present, its last graded game, and provisional status below **500 current-season minutes**. The leaderboard uses the selected season's last available snapshot, with 500 minutes as its rank threshold and separate data coverage. Sort by full-precision rating and use a stable ID for ties. Older assets without rating fields should still render normally.

On refresh, report valid and rejected rows, unresolved IDs, final games without grades, and last rated date. If a feed outage or incomplete rebuild would remove already published coverage, preserve the last valid output and surface the stale timestamp. Test the existing publisher with the added fields before enabling production. Keep this rollout in the player pages and leaderboard while the separate Team Elo rotation experiment is evaluated.

### Acceptance checks

1. Reconcile imported regular-season games and minutes against the canonical schedule and people data. The 2025–26 pilot archive had 1,228 of 1,230 games. Its reconstructed season rates matched the ESPN season feed within 0.01 for SGA, Luka and LeBron. Test identity joins for all three and for a traded player and a player with aliased IDs.
2. Verify the 2,000-possession replay against the 2025–26 fresh-start snapshot. At the 500-minute cutoff, SGA was first, Luka fourth and LeBron 41st. These are fresh-start test fixtures, so the multi-season leaderboard can differ. In the three-season carryover run, LeBron entered 2025–26 at +3.31 and was +2.72 after five appearances.
3. Reproduce next-500-possession error after five appearances for carryover vs reset. Expect **2.26 vs 4.28** for 2024–25 and **2.69 vs 4.22** for 2025–26, using the pilot snapshots and the 500-prior-season-minute evaluation filter. The same 2,000-possession half-life applies to both models.
4. Test no change across an offseason or DNP, a trade with one continuous rating, the first valid grade, eligibility crossing with no formula change, corrections and idempotent reruns. Include differing offensive and defensive denominators. Verify earlier snapshots do not change when adding later games.
5. Run the existing NBA export, people, refresh workflow and page tests. Preview the People leaderboard and a direct player link on desktop and mobile, including provisional, unavailable and older export states. Check that the existing team, game and player navigation renders with and without the optional fields.

## Original research goals

Put the team statistics from [PR #200](https://github.com/ak038819-sudo/College-Football-Coefficient/pull/200) into Elocast’s NBA analysis and product views. Build a reproducible Player Elo pilot using the full 2017–18 NBA regular season, with Utah as the first team to inspect.

Team Elo ranks demonstrated current-season strength. Player Elo should estimate how much a player adds to the lineups they actually play in. Team and player forecasts can help validate those ratings.

| Milestone | Deliverable |
|---|---|
| 1. Team stats | Reconciled team and game data, with PxP+ and the opportunity grades in the NBA export |
| 2. Lineup audit | One verified Jazz game, then a reconciled league-wide 2017–18 stint file |
| 3. Player Elo pilot | Game-by-game player ratings, possession counts, and a Mitchell–Stockton comparison |
| 4. Rotation adjustment | A team-strength estimate for a specific change in the players and minutes expected on the court |

## 1. Apply the team statistics

### Input and integration

Use the NBA `team_box` dataset exposed in `src/nba/source.py`. Join the two team rows to the canonical games in `src/nba/games.py` using the archive’s game and team identities. Verify that the feed fields for field-goal attempts, free-throw attempts, offensive rebounds, and **total turnovers including team turnovers** match the definitions used by the pilot. Reconcile team points with the final. Keep a coverage count for games with both team boxes and expose missing metrics as unavailable.

For the initial rankings, use completed regular-season games. The canonical schedule determines whether a game counts. NBA Cup games that count in the regular season belong in the aggregate. The Cup final has its own classification.

Put calculations behind one NBA module and reuse them from the season and game exports in `src/nba/export.py` and `src/nba/game_export.py`. The NBA page lives in `ui/nba.html`, with its data loading and rendering in `ui/nba_loader.js` and `ui/nba_app.js`. Check export consumers and coverage gates before adding payload fields. Give derived fields named keys or follow the existing versioned payload convention when extending an array.

### Formulas and display

For each game, estimate each side’s possessions as `FGA + 0.44 × FTA − ORB + TOV`. Use the average of those two estimates as the shared game denominator, P. Use the same denominator in all team-stat calculations.

| Metric | Formula | Meaning |
|---|---|---|
| **PxP+** | `100 × (team points / team possessions) / (league points / league possessions)` | Scoring efficiency relative to the league. 100 is average. |
| **Estimated shooting chances** | `C = P + ORB − TOV` | Shooting opportunities generated or preserved by the team. |
| **Chance creation index** | `100 × (C/P for team) / (C/P for league)` | Chances generated per possession relative to the league. |
| **Points per chance index** | `100 × (points/C for team) / (points/C for league)` | Scoring per estimated chance relative to the league. |
| **Net Opportunities per 100** | `100 × [(team ORB − opp ORB) − (team TOV − opp TOV)] / P` | The team’s estimated chance edge. Zero means even. |

Compute team and league season rates from the summed counts and exposures for the same period. The two PxP+ components multiply back to `PxP+ = chance creation index × points per chance index / 100`. Compute at full precision and round only for display. Label chances as estimates and handle zero denominators as unavailable.

On the team page, show **PxP+ next to both components**, with a short explanation of 100. On game pages, show each side’s actual and expected opportunity rates and the net grade. This gives readers the context for an individual performance.

### Matchup expectation

Use only games played before the matchup. For the date-only replay, freeze every game’s expectation before ingesting any game from that date.

    Opportunity rate = 100 + 100 × (ORB − TOV) / P
    Expected rate = team's prior offensive rate + opponent's prior allowed rate − league rate
    Net Opportunities Above Expected =
        (team actual − team expected) − (opponent actual − opponent expected)

Reuse the trial’s exposure-weighted priors for the first reproducible run. A team profile starts with **1,500 possessions** at the current league rate. The league starts with **6,000 possessions** at the prior season’s rate. These are pilot settings. Game grades are equal and opposite. The opportunity calculations use attempts, rebounds, and turnovers. Points enter PxP+.

### Team Elo trial

The NBA rankings can expose the statistics independently of any change to the active Elo model. In an analysis replay, compare win/loss, opportunity, and PxP+ modifiers at the same temperature and offseason regression.

    Winner Elo gain = F × (1 − E) × M
    Loser Elo change = −winner Elo gain
    g = winner's actual PxP+ edge − winner's expected PxP+ edge
    M = 1 + 0.25 × tanh(g / σ)

E is the winner’s pregame Elo expectation, with venue accounted for. The actual edge is winner PxP+ minus opponent PxP+. For the expected edge, estimate both sides’ scoring rates from prior offense, opposing defense, and the pregame league rate. Use that same league rate for the actual and expected PxP+ values. Freeze σ from a prior warmup sample. The modifier approaches 0.75 and 1.25, giving extreme performances diminishing returns and every winner a positive gain. The tested strength is 25%. The prior trial used F = 20 and 50% offseason regression.

A game’s PxP+ differential uses points scored and is the scoring margin scaled by possessions and the league environment. Its residual grades the difference against matchup expectations. Net Opportunities is a separate candidate. Test the grades individually before combining them, because extra opportunities already contribute to PxP+.

The 11-season trial improved early top-five overlap from 3.00 to 3.11 out of five and raised unsupported five-place jumps from 348 to 430. Treat the shape of the modifier and the strength of the modifier as separate design choices.

### Team acceptance checks

- Reproduce the 2025–26 pilot’s 1,230 games and 30 teams against the pilot input. Detroit led raw Net Opportunities at **+3.47** and Denver led PxP+ at **106.1**. If the production feed gives a different total, report the source and field differences.
- Verify that a game’s two net grades cancel, changing points leaves opportunity grades unchanged, and the unrounded PxP+ components multiply back to PxP+.
- Verify that truncating the replay at a date leaves all earlier expectations intact. Confirm partial-season support and useful coverage for missing boxes.
- For any Elo trial, verify positive winner gains, equal and opposite rating updates, and repeatable output.

## 2. Research benchmark using 2017–18 lineups

Start under an analysis directory with the [NBA lineup-stint archive](https://github.com/ramirobentes/nba_pbp_data) used in the earlier replay. The local 2017–18 sample had **1,230 games and 37,009 unique stints**, with **2,655 stints in 82 Jazz games**. Each row has ten player IDs, elapsed time, points, and possessions. Keep one perspective per stint. Fit on the full league and use Utah for inspection.

Audit Utah’s opener against Denver first (NBA game ID `0021700010`, final 106–96). Verify five distinct players per team, stint ordering including overtime, cumulative team points, and individual minutes against the box score. NBA lineup IDs and ESPN IDs come from different providers. Preserve provider IDs during the pilot, then build a verified crosswalk to the identities in `src/nba/people.py` before attaching results to player pages.

The initial file has two Jazz stints with points and zero recorded possessions. Keep those in the reconciliation. Investigate the events and attribute them only if the possession and lineup can be verified. Otherwise flag those rows and report how they were handled in the rate fit. Keep points, minutes, and rated-exposure coverage separately auditable.

Suggested stint record:

    game_id, date, period, stint_id, home_team, away_team
    home_player_ids[5], away_player_ids[5], start_clock, end_clock
    home_points, away_points, home_possessions, away_possessions
    venue, quality_status

For the first Player Elo build, points and possessions are enough. To add the PxP+ breakdown, join offensive rebounds and turnovers from play-by-play using game ID, period, event order, and substitution boundaries. Reconcile counts to the team boxes. Events at the same clock can belong to different lineups, so clock alone is insufficient. The current NBA game-page reducer in `src/nba/plays.py` publishes summaries from an event feed without explicit possessions. The lineup archive and the possession audit supply the additional evidence needed here.

## 3. Earlier adjusted plus-minus experiment

Fit a possession-weighted adjusted plus-minus model first.

    p_s = (home possessions + away possessions) / 2
    y_s = 100 × (home points − away points) / p_s
    y_s ≈ home-court effect + sum(home player impacts) − sum(away player impacts)

Each stint has a sparse design row with +1 for its five home players and −1 for its five away players. Fit ridge regression with sample weight p_s. Apply the penalty to player coefficients and use a separate venue term, zero at neutral sites. Report the penalty and exposure convention with every run. Ridge supplies shrinkage for small samples and players whose minutes are hard to distinguish from their teammates’. Use the earlier pilot’s 350 penalty as a comparison setting if the weight convention matches. Select any new penalty chronologically.

Start at a zero impact prior and fit only earlier games for the pregame snapshot. Refit after ingesting the completed game. Where only dates are reliable, use one pregame and one postgame cutoff per league date. An initial season-to-date fit is enough to establish the benchmark. Test a sequential update against it once the pilot output is reproducible.

    Display Player Elo = 1500 + c × adjusted impact per 100 possessions

Keep `c` explicit and provisional until we choose a display spread. Persist the underlying impact in points per 100. Eligibility is based on **500 validated current-season on-court possessions**, with overtime included. Use a consistent convention for counting on-court exposure. The pilot used the average of both sides’ possessions, so half-possessions can occur. A player with no observed exposure has no current-season estimate. Smaller samples with exposure are provisional.

First outputs:

- **Stint audit** with source coverage, missing lineups, and unresolved zero-exposure scoring.
- **Player-game CSV** with date, game ID, player ID, game and season possessions, pregame impact, postgame impact, change, display Elo, and eligibility.
- **Mitchell and Stockton comparison** with a game-by-game chart and an explanation of exposure and shrinkage. The earlier dataset had Mitchell near **5,302** on-court possessions and Stockton near **21.5**. Reconcile any differences with the new audit convention.
- **On-court team PxP+** for a player’s lineup possessions. Once rebound and turnover joins pass, show the chance creation and points-per-chance split. These are descriptive lineup stats. The adjusted model estimates individual impact.

The Player Scoring Index in PR #200 remains a separate research note. Team PxP+ is the product metric in this handoff.

## 4. Follow-up research on team rotations

Define a reference rotation and a scenario rotation. Each team’s expected minute allocations sum to **240 in regulation**. Weight each player’s impact by their share of the game and compare the two rotations.

    Team impact change per 100 =
        sum[(scenario minutes − reference minutes) / 48 × player impact]

An unchanged rotation yields zero. If a starter is absent, assign the minutes to the players expected to absorb them and show the contribution of each change. Estimate the conversion to Team Elo separately. The reference rotation is already reflected in Team Elo, so only apply the change in strength from that reference.

Use pregame availability and projected minutes when testing forecasts. Compare the same Team Elo baseline with and without the rotation adjustment, both league-wide and in games where a regular starter is absent. Start with a reviewable scenario output and use the replay to decide when it belongs on a published matchup.

## References

- [PR #200 team-stat definitions and trials](https://github.com/ak038819-sudo/College-Football-Coefficient/blob/analysis/nba-team-metrics/docs/nba-team-metrics.md)
- [Elocast NBA archive and export conventions](https://github.com/ak038819-sudo/College-Football-Coefficient/blob/main/docs/nba.md)
- [NBA lineup-stint archive used in the pilot](https://github.com/ramirobentes/nba_pbp_data)
- [BoxScore Lab team game results used for the team-stat replay](https://boxscorelab.com/downloads/)
