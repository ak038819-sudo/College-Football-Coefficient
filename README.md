Fixing College Football

A structural college football simulation engine that replaces the current postseason model with a tier-based, coefficient-driven, standings-qualified playoff system.

Built with:

Python

SQLite (db/league.db)

CFBD-style CSV ingestion

A custom 5-year rolling CoEfficient (CoE) model

Core Philosophy

Qualification is earned on the field.

Conference standings determine playoff qualification

CoE does NOT determine qualification

CoE is used for:

Seeding order

Home-field assignment

Ranking logic within constraints

This separates merit (standings) from strength (performance quality).

Playoff Model (Year2+ Ruleset)
Field Size: 24 Teams
Automatic Bids (Tier-Based)

Conference tiers determine bid allocation:

Tier	Bids
1–4	4 bids
5	3 bids
6–10	1 bid (champion only)

All conference champions qualify.

Byes (8 Total)

Tier 1–2

1st and 2nd place receive byes

Tier 3–6

Champion receives bye

Pot System (World Cup Style Draw)

After qualification:

Pot 0 = Bye teams

Pot 1 = Higher non-bye qualifiers

Pot 2 = Remaining qualifiers

Draw is seeded using a deterministic --seed value.

Round of 24 Constraints

No same-conference matchups

Homefield determined strictly by 5-year rolling CoE

Placement in pot does NOT determine host

CoEfficient (CoE) System
5-Year Rolling Window

For season N:

CoE(N) = sum of seasons N-4 through N
Points Model

Win = 2 pts

OT Loss = 1 pt

Loss = 0 pts

Additional:

Bounty multiplier based on opponent 5-year PPG

Playoff bonuses

Top 8 bonuses

Historical Bootstrapping (Recommended)

To avoid a cold start in 2014, load 2010–2013 seasons so 2014 uses a true 5-year window.

Step 1 — Fetch Historical Seasons
python src/fetch_cfbd_games.py 2010
python src/fetch_cfbd_games.py 2011
python src/fetch_cfbd_games.py 2012
python src/fetch_cfbd_games.py 2013
Step 2 — Load Into Database
python src/load_games.py data/raw/games_2010.csv
python src/load_games.py data/raw/games_2011.csv
python src/load_games.py data/raw/games_2012.csv
python src/load_games.py data/raw/games_2013.csv
Step 3 — Backfill Membership (Pre-2014)
sqlite3 db/league.db <<'SQL'
INSERT OR IGNORE INTO team_membership_by_season (season_year, team_id, conference_real, is_fbs)
SELECT DISTINCT g.season_year, g.home_team_id, 'HISTORICAL', 1
FROM games g
WHERE g.season_year BETWEEN 2010 AND 2013;

INSERT OR IGNORE INTO team_membership_by_season (season_year, team_id, conference_real, is_fbs)
SELECT DISTINCT g.season_year, g.away_team_id, 'HISTORICAL', 1
FROM games g
WHERE g.season_year BETWEEN 2010 AND 2013;
SQL
Step 4 — Compute Team CoE (2010–2014)
for y in 2010 2011 2012 2013 2014; do
  python src/coefficients/compute_team_coe_v0.py --year $y
done
Step 5 — Compute Rolling CoE (2014)
python src/coefficients/compute_team_coe_rolling_5yr.py \
  --year 2014 \
  --formula-version v0
Full Year2 Playoff Pipeline
1️⃣ Conference Rolling CoE
python src/coefficients/compute_conference_coe_rolling_5yr.py \
  --year 2014 \
  --formula-version v0
2️⃣ Select 24-Team Field (Standings-Based)
python src/coefficients/select_playoff_qualifiers.py \
  --year 2014 \
  --formula-version v0 \
  --ruleset year2
3️⃣ Draw the Bracket
python src/coefficients/draw_playoff_year_2.py \
  --year 2014 \
  --formula-version v0 \
  --ruleset year2 \
  --seed 20250101
4️⃣ Build Round of 24

Enforces:

No same-conference matchups

Homefield by rolling CoE

python src/coefficients/build_round_of_24_year2.py \
  --year 2014 \
  --formula-version v0 \
  --ruleset year2
Database Design Notes
team_membership_by_season

Must be keyed by:

PRIMARY KEY (season_year, team_id)

This allows:

Historical seasons

Realignment support

Accurate rolling window computation

Determinism

The system is deterministic when:

--seed is provided to draw scripts

The database state is unchanged

This allows exact replayability of postseason outcomes.

Project Status

✅ 24-team Year2 model implemented
✅ Tier-based bid allocation
✅ Standings-qualified field
✅ 5-year rolling CoE system
✅ World Cup-style pot draw
✅ R24 same-conference constraint
✅ Homefield by CoE
✅ Historical bootstrap supported

Next Planned Enhancements

NIT auto-bid integration

Multi-season simulation runner

Bracket auto-advancement engine

Conference tier evolution logic

Automated validation testing suite