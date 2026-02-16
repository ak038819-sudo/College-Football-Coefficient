# College-Football-Coefficient

Coefficient-based College Football Rebuild
A full-scale rebuild of the college football landscape driven by history, tradition, and fan passion rather than greed, money, and power. This project uses a coefficient-based system to treat all teams and conferences fairly in an era of clearly and obviously biased committees.

## Goals
- Remove subjective rankings with unclear criteria and replace them with an objective, criteria-driven model.
- Preserve and/or rekindle regional rivalries that have been destroyed by conference realignment.
- Replace the super conference dynamic with 8 regional leagues, which all have the opportunity to compete on equal footing.
- Implement an objective, criteria-driven 24-team playoff structure.

## Methodology
- Historical analysis beginning with the College Football Playoff's (CFP) implementation in 2014 and concluding with the most recent iteration in the 20-26 season.
- Historical analysis will exclude the 2020-2021 season due to scheduling irregularities and teams canceling their season altogether.
- Structural rebuild beginning in the 2026-27 season.
- Future seasons will be simulated using EA Sports NCAA 26.

## Tech Stack
- SQLite
- Python
- GitHub

## Data Flow
- Built a pipeline from scratch that took raw game results and added them to a CSV file which was then able to be interpreted.
- Compiled a large data set from scratch comprising every FBS game played from 2014-2016 with the exception of the 2020-2021 season.