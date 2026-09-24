-- Not-yet-final games (Milestone 3). Deliberately SEPARATE from `games` and
-- deliberately WITHOUT score columns: every model engine (Elo, CoE, hybrid,
-- standings, playoff selection) reads only `games`, so nothing in this table
-- can ever reach a rating. A game moves to `games` only once CFBD reports it
-- completed and it's re-fetched.
CREATE TABLE IF NOT EXISTS scheduled_games (
    game_id        INTEGER PRIMARY KEY,
    season_year    INTEGER NOT NULL,
    week           INTEGER,
    season_type    TEXT,
    kickoff_utc    TEXT,                          -- ISO timestamp from CFBD; NULL if unknown
    start_time_tbd INTEGER NOT NULL DEFAULT 0 CHECK (start_time_tbd IN (0,1)),
    home_team_id   INTEGER NOT NULL,
    away_team_id   INTEGER NOT NULL,
    neutral_site   INTEGER NOT NULL DEFAULT 0 CHECK (neutral_site IN (0,1)),
    game_phase     TEXT NOT NULL DEFAULT 'regular' CHECK (game_phase IN ('regular','bowl','cfp')),
    notes          TEXT,
    FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
    FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
);
