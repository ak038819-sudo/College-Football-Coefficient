-- Hybrid Rating + Game CoE 2.0 tables (CoE 2.0, Phase 4).
--
-- Completely additive: nothing here is read by build_coefficients.py
-- (CoE v1), and v1's tables/outputs are untouched by this. Both models
-- run side by side until an explicit, later decision to switch.

-- Frozen entering-season 5-year CoE. season_year=2025 means "the
-- coefficient available ENTERING 2025" -- i.e. computed from completed
-- seasons 2020-2024 only. Deliberately named differently from the
-- existing team_coeff_5yr.csv (CoE v1's rolling window, which INCLUDES
-- the current season and is used for current-season playoff selection)
-- to make the distinction impossible to miss in code.
CREATE TABLE IF NOT EXISTS team_coe_5yr_by_season (
    team_id INTEGER NOT NULL,
    season_year INTEGER NOT NULL,

    coe_5yr REAL NOT NULL,

    PRIMARY KEY (team_id, season_year)
);

-- One row per (game, team): the complete Hybrid Rating + Game CoE 2.0
-- audit trail for that team's participation in that game.
CREATE TABLE IF NOT EXISTS hybrid_game_ratings (
    game_id INTEGER NOT NULL,
    team_id INTEGER NOT NULL,

    elo_z REAL,
    coe_z REAL,

    hybrid_strength REAL,
    hybrid_rating REAL,
    hybrid_expectation REAL,

    result_type TEXT,
    game_coe REAL,

    PRIMARY KEY (game_id, team_id)
);
