-- PER-GAME advanced team stats from CFBD (EXP-03, xSRDiff performance layer).
--
-- Distinct from team_season_advanced, which is season totals and display-only.
-- This table IS model input: build_elo.py reads off_success_rate through it
-- when the configured performance layer is xsrdiff or raw_srdiff. A season
-- figure could not be used for that -- it includes the game being rated and
-- every game after it, so it would leak the future into a pregame expectation.
--
-- NULL means CFBD did not report the metric, never 0. Coverage begins in 2001
-- (the play-by-play era); earlier games take the Elo fallback path.
--
-- game_id is CFBD's, the same identifier the games table uses, so a stat can
-- only ever attach to the game it came from.
CREATE TABLE IF NOT EXISTS game_team_advanced (
    game_id               INTEGER NOT NULL,
    team_id               INTEGER NOT NULL,
    season_year           INTEGER NOT NULL,
    source                TEXT    NOT NULL DEFAULT 'cfbd',
    garbage_time_excluded INTEGER NOT NULL DEFAULT 1 CHECK (garbage_time_excluded IN (0,1)),

    off_plays          INTEGER,
    off_ppa            REAL,
    off_success_rate   REAL,
    off_explosiveness  REAL,
    def_plays          INTEGER,
    def_ppa            REAL,
    def_success_rate   REAL,
    def_explosiveness  REAL,

    -- Per-game rushing/passing splits. NULL means not reported, not zero.
    off_rush_success_rate REAL,
    off_rush_ppa REAL,
    off_rush_explosiveness REAL,
    off_pass_success_rate REAL,
    off_pass_ppa REAL,
    off_pass_explosiveness REAL,
    def_rush_success_rate REAL,
    def_rush_ppa REAL,
    def_rush_explosiveness REAL,
    def_pass_success_rate REAL,
    def_pass_ppa REAL,
    def_pass_explosiveness REAL,

    PRIMARY KEY (game_id, team_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_game_team_advanced_season ON game_team_advanced(season_year);
