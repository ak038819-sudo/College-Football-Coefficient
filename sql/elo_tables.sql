-- Elo rating engine audit trail (CoE 2.0, Phase 1).
--
-- Fully isolated from the existing CoE tables -- nothing here is read by
-- build_coefficients.py, and nothing in build_coefficients.py's tables is
-- read by build_elo.py. One row per (game, team): a complete record of
-- what that team's rating was entering the game, what was expected, what
-- actually happened, and what the rating became. Answers "why was this
-- team rated X entering this game?" by walking backward through the table.

CREATE TABLE IF NOT EXISTS elo_game_history (
    game_id INTEGER NOT NULL,
    team_id INTEGER NOT NULL,

    pregame_elo REAL NOT NULL,
    opponent_pregame_elo REAL NOT NULL,

    elo_expectation REAL NOT NULL,
    -- The performance multiplier M actually applied. Still called mov_multiplier
    -- because that is what it was when the column was created and every existing
    -- reader expects the name; performance_model says which layer produced it
    -- (see src/srdiff.py). It is identical for both rows of a game, because it
    -- scales a zero-sum update.
    mov_multiplier REAL NOT NULL,
    elo_change REAL NOT NULL,
    postgame_elo REAL NOT NULL,

    -- xSRDiff performance layer (EXP-03). NULL when the game has no per-game
    -- Success Rate on both sides, or no point-in-time expectation curve covers
    -- its season -- never 0, which would be a real and different claim.
    elo_diff_adjusted REAL,      -- venue-adjusted pregame Elo, this team minus opponent
    success_rate_team REAL,      -- this team's Success Rate in THIS game
    success_rate_opp REAL,
    sr_diff REAL,                -- success_rate_team - success_rate_opp
    xsr_diff REAL,               -- what elo_diff_adjusted implied before kickoff
    sr_plus REAL,                -- sr_diff - xsr_diff
    performance_model TEXT,      -- mov | result_only | raw_srdiff | xsrdiff

    PRIMARY KEY (game_id, team_id)
);
