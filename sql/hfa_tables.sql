-- Team-specific home-field advantage (src/hfa.py, src/build_hfa.py).
-- ANALYSIS ONLY: production Elo still uses the flat config elo.home_field.
-- Both tables are fully rebuilt on every run and record the parameters used,
-- so a backtest can compare runs made with different L / K / N_eff methods.
--
-- team_hfa_by_season: "entering season" -- uses only games dated before
--   as_of = that season's first game date. No later game can influence it.
-- team_hfa_current:   as_of = the day after the last completed game.
--
-- raw_hfa is NULL when a team has no qualifying home games (never a fake 0);
-- adjusted_hfa is then exactly prior_hfa.
CREATE TABLE IF NOT EXISTS team_hfa_by_season (
    team_id                INTEGER NOT NULL,
    season_year            INTEGER NOT NULL,
    as_of                  TEXT    NOT NULL,
    raw_hfa                REAL,
    adjusted_hfa           REAL    NOT NULL,
    prior_hfa              REAL    NOT NULL,
    fbs_baseline           REAL    NOT NULL,
    fcs_prior_used         INTEGER NOT NULL,
    effective_n            REAL    NOT NULL,
    lambda                 REAL    NOT NULL,
    games_used             INTEGER NOT NULL,
    weighted_actual_wins   REAL    NOT NULL,
    weighted_expected_wins REAL    NOT NULL,
    oldest_game_used       TEXT,
    newest_game_used       TEXT,
    elo_hfa_points         REAL,
    elo_points_at_bound    INTEGER NOT NULL,
    raw_elo_hfa_points     REAL,
    raw_points_at_bound    INTEGER NOT NULL,
    half_life_years        REAL    NOT NULL,
    shrinkage_k            REAL    NOT NULL,
    effective_n_method     TEXT    NOT NULL,
    PRIMARY KEY (team_id, season_year),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE TABLE IF NOT EXISTS team_hfa_current (
    team_id                INTEGER PRIMARY KEY,
    season_year            INTEGER NOT NULL,
    as_of                  TEXT    NOT NULL,
    raw_hfa                REAL,
    adjusted_hfa           REAL    NOT NULL,
    prior_hfa              REAL    NOT NULL,
    fbs_baseline           REAL    NOT NULL,
    fcs_prior_used         INTEGER NOT NULL,
    effective_n            REAL    NOT NULL,
    lambda                 REAL    NOT NULL,
    games_used             INTEGER NOT NULL,
    weighted_actual_wins   REAL    NOT NULL,
    weighted_expected_wins REAL    NOT NULL,
    oldest_game_used       TEXT,
    newest_game_used       TEXT,
    elo_hfa_points         REAL,
    elo_points_at_bound    INTEGER NOT NULL,
    raw_elo_hfa_points     REAL,
    raw_points_at_bound    INTEGER NOT NULL,
    half_life_years        REAL    NOT NULL,
    shrinkage_k            REAL    NOT NULL,
    effective_n_method     TEXT    NOT NULL,
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);
