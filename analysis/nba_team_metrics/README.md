# Elocast NBA team metrics

## PxP+

Points per possession indexed to the NBA scoring environment. A value of 100
is league average and 110 means 10% more points per possession than average.

    PxP+ = 100 × (team points / team possessions) / (league points / league possessions)

The league average is possession-weighted. The season table uses all games in
the input through its cutoff. A live game grade would need a league average
available before tipoff.

## Net Opportunities

An offensive rebound can extend a possession and a turnover can end one
without a shot. Net Opportunities measures the two teams' advantage in
creating or denying those chances, with zero as the break-even point.

    Offensive opportunity rate = 100 + 100 × (ORB − TOV) / P
    Net Opportunities per 100 = 100 × [(team ORB − opponent ORB) − (team TOV − opponent TOV)] / P

P is a shared game denominator. For each team, estimate possessions as
FGA + 0.44 × FTA − ORB + TOV, then average the two estimates.
Season values are possession-weighted. Use total turnovers, including team
turnovers. This box-score estimate does not identify individual shooting trips
or distinguish live-ball turnovers.

## Net Opportunities Above Expected

For each team, estimate its opportunity rate from games on dates before tipoff.

    Expected opportunity rate = team offensive rate + opponent allowed rate − league rate
    Net Opportunities Above Expected = (team actual − team expected)
                                      − (opponent actual − opponent expected)

Each team profile starts with 1,500 possessions at the current league rate.
The league profile starts with 6,000 possessions at the prior season's rate.
The game grades for the two opponents are equal and opposite. Points enter
PxP+ only. Opportunity grades use attempts, offensive rebounds and turnovers.

The proposed Elo structure is F × (1 − E) × M for the winner, with the
opposite update for the loser. F is temperature, E is the winner's pregame
Elo expectation, and M is a positive performance modifier. Net Opportunities
Above Expected is a candidate input for M. No modifier weight has been selected.
These metrics run independently of the published NBA ratings.

## Reproduce

Download the two adjacent season files from
[BoxScore Lab team game results](https://boxscorelab.com/downloads/).
Supply the earlier season and the target season. The target can be in progress.

```sh
python analysis/nba_team_metrics/metrics.py \
  2024-25-team-game-results.csv 2025-26-team-game-results.csv \
  --out /tmp/elocast-nba-metrics
```

This writes `team_metrics.csv` for all three rankings and
`game_metrics.csv` for actual and expected opportunity rates and matchup grades.
The 2025–26 replay covered 1,230 games. Detroit led raw Net Opportunities
at +3.47 per 100. Denver led PxP+ at 106.1.
