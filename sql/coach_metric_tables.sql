-- Head-coach metrics measured against the Elo expectation (v0.1.1 phase 5).
--
-- One row per coach-season. A row exists for EVERY tenure, including the ones
-- that get no metric, because "this season could not be attributed" is a fact a
-- page has to be able to say. attributed = 0 leaves every measure NULL and
-- names the reason.
CREATE TABLE IF NOT EXISTS coach_season_metrics (
    coach_id            INTEGER NOT NULL,
    team_id             INTEGER NOT NULL,
    season_year         INTEGER NOT NULL,
    attributed          INTEGER NOT NULL,     -- 1 when every game that season is this coach's
    reason              TEXT,                 -- why not, when attributed = 0
    games               INTEGER,              -- games this database holds for that team-season
    wins                INTEGER,
    losses              INTEGER,
    ties                INTEGER,
    expected_wins       REAL,                 -- sum of the pregame Elo win probabilities
    wins_above_expected REAL,                 -- wins + ties/2 - expected_wins
    pregame_elo         REAL,                 -- before the season's first game
    postgame_elo        REAL,                 -- after its last
    elo_change          REAL,
    season_coe2         REAL,
    PRIMARY KEY (coach_id, team_id, season_year)
);

CREATE INDEX IF NOT EXISTS idx_coach_season_metrics_coach
    ON coach_season_metrics (coach_id);
