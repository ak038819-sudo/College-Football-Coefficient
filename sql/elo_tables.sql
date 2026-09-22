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
    mov_multiplier REAL NOT NULL,
    elo_change REAL NOT NULL,
    postgame_elo REAL NOT NULL,

    PRIMARY KEY (game_id, team_id)
);
