-- Season-level advanced team stats from CFBD (Milestone 7). DISPLAY DATA ONLY:
-- no rating engine (Elo, CoE, hybrid, standings, predictions, playoff selection)
-- reads this table. NULL means CFBD didn't report the metric -- never 0.
-- Coverage begins in 2001 (play-by-play era).
CREATE TABLE IF NOT EXISTS team_season_advanced (
    team_id               INTEGER NOT NULL,
    season_year           INTEGER NOT NULL,
    source                TEXT    NOT NULL DEFAULT 'cfbd',
    garbage_time_excluded INTEGER NOT NULL DEFAULT 1 CHECK (garbage_time_excluded IN (0,1)),
    off_plays          INTEGER,
    off_ppa            REAL,
    off_success_rate   REAL,
    off_explosiveness  REAL,
    off_pts_per_opp    REAL,
    off_line_yards     REAL,
    off_stuff_rate     REAL,
    off_havoc          REAL,
    def_plays          INTEGER,
    def_ppa            REAL,
    def_success_rate   REAL,
    def_explosiveness  REAL,
    def_pts_per_opp    REAL,
    def_line_yards     REAL,
    def_stuff_rate     REAL,
    def_havoc          REAL,
    PRIMARY KEY (team_id, season_year),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);
