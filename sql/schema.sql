-- ==========================================
-- CORE ENTITIES
-- ==========================================

DROP TABLE IF EXISTS games;
DROP TABLE IF EXISTS new_alignment;
DROP TABLE IF EXISTS team_membership_by_season;
DROP TABLE IF EXISTS conferences_new;
DROP TABLE IF EXISTS seasons;
DROP TABLE IF EXISTS teams;

CREATE TABLE teams (
    team_id INTEGER PRIMARY KEY,
    team_name TEXT NOT NULL,
    short_name TEXT,
    state TEXT
);

CREATE TABLE seasons (
    season_id INTEGER PRIMARY KEY,
    year INTEGER NOT NULL,
    league_mode TEXT NOT NULL   -- 'historical' or 'coe_rebuild'
);

CREATE TABLE conferences_new (
    conference_id INTEGER PRIMARY KEY,
    conference_name TEXT NOT NULL UNIQUE
);

-- ==========================================
-- MEMBERSHIP (BY SEASON)
-- ==========================================

CREATE TABLE team_membership_by_season (
    team_id INTEGER NOT NULL,
    season_year INTEGER NOT NULL,
    conference_real TEXT,
    is_fbs INTEGER NOT NULL CHECK (is_fbs IN (0,1)),
    PRIMARY KEY (team_id, season_year),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE TABLE new_alignment (
    team_id INTEGER NOT NULL,
    conference_name TEXT NOT NULL,
    effective_year_start INTEGER NOT NULL,
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

-- ==========================================
-- GAME RESULTS (RAW EVENTS)
-- ==========================================

CREATE TABLE games (
    game_id INTEGER PRIMARY KEY,
    season_year INTEGER NOT NULL,
    week INTEGER,
    game_date TEXT,
    home_team_id INTEGER NOT NULL,
    away_team_id INTEGER NOT NULL,
    home_score INTEGER,
    away_score INTEGER,
    went_ot INTEGER NOT NULL CHECK (went_ot IN (0,1)),
    neutral_site INTEGER NOT NULL DEFAULT 0 CHECK (neutral_site IN (0,1)),
    is_playoff INTEGER NOT NULL CHECK (is_playoff IN (0,1)),
    is_nit INTEGER NOT NULL CHECK (is_nit IN (0,1)),
    game_phase TEXT NOT NULL CHECK (game_phase IN ('regular','bowl','cfp')),
    game_phase_check TEXT,
    FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
    FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
);

-- NOTE: team_coefficient_by_year, conference_coefficient_by_year, and
-- playoff_field_by_year are intentionally NOT defined here anymore.
-- They live in team_coe_tables.sql, coe_tables.sql, and
-- playoff_field_by_year.sql. Run those files (see docs/COMMANDS.md)
-- after this one.

-- Helpful index
CREATE UNIQUE INDEX IF NOT EXISTS idx_teams_team_name ON teams(team_name);
