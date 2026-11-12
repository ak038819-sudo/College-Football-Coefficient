# NBA team statistics

## PxP+

`PxP+ = 100 × (team points / team possessions) / (league points / league possessions)`

PxP+ tells us how efficiently a team scored relative to its league's scoring
environment. A value of 100 is league average and 110 means the team scored
10% more points per possession than average. Compute the league rate from
total points and total possessions across the same period.

## Net Opportunities

`Net Opportunities per 100 = 100 × [(team ORB − opponent ORB) − (team TOV − opponent TOV)] / P`

ORB is offensive rebounds. TOV is total turnovers, including team turnovers.
P is a shared estimate of game possessions. For each side, estimate
possessions as `FGA + 0.44 × FTA − ORB + TOV`, then average the two estimates.

Net Opportunities tells us how many more shooting opportunities a team
created or preserved than its opponent through offensive rebounding and
turnovers. An offensive rebound extends a possession and a turnover can end
one before a shot. Zero means the teams broke even. A value of +3 means an
estimated edge of three opportunities per 100 possessions. For season rates,
add the game differentials and divide by total estimated possessions.

## Net Opportunities Above Expected

Compute each side's offensive opportunity rate on the same possession basis.

`Opportunity rate = 100 + 100 × (ORB − TOV) / P`

`Expected opportunity rate = team offensive rate + opponent allowed rate − league rate`

`Net Opportunities Above Expected = (team actual − team expected) − (opponent actual − opponent expected)`

This tells us whether a team created a larger opportunity edge than its
matchup suggested. A positive grade means it exceeded its expected edge,
and the opponent receives the opposite grade. Use games played before the
matchup to build the expectations. The pilot used 1,500 possessions of
league-average prior exposure per team profile and 6,000 possessions of
prior-season exposure for the league baseline.
