-- Completed games an FBS team played against a non-FBS opponent.
--
-- These are DELIBERATELY NOT in `games`. Every rating this project computes --
-- Elo, CoE v1 and 2.0, the hybrid, standings, the playoff field -- reads the
-- games table, across roughly twenty-five files. Filtering them out in each of
-- those would make "not counted in Elo or CoE" a promise somebody has to keep
-- twenty-five times; keeping them in their own table makes it a property of
-- the schema. A rating cannot accidentally read a table it never queries.
--
-- The opponent is a NAME, not a team_id, and that is also deliberate. This
-- project holds FBS programs as entities: they get pages, logos, ratings,
-- directory and search entries. Creating `teams` rows for several hundred FCS
-- programs would put unrated schools into all of that. A name is exactly what
-- a schedule row and a win-loss record need, and nothing more is claimed.
CREATE TABLE IF NOT EXISTS non_fbs_games (
    game_id                 INTEGER PRIMARY KEY,
    season_year             INTEGER NOT NULL,
    week                    INTEGER,
    game_date               TEXT,
    team_id                 INTEGER NOT NULL,   -- the FBS side; the only side we hold
    opponent_name           TEXT    NOT NULL,   -- as the source spells it
    opponent_conference     TEXT,
    opponent_classification TEXT,               -- CFBD's own division string, '' when it gave none
    team_score              INTEGER NOT NULL,
    opponent_score          INTEGER NOT NULL,
    is_home                 INTEGER NOT NULL CHECK (is_home IN (0,1)),
    neutral_site            INTEGER NOT NULL DEFAULT 0 CHECK (neutral_site IN (0,1)),
    went_ot                 INTEGER NOT NULL CHECK (went_ot IN (0,1)),
    game_phase              TEXT    NOT NULL CHECK (game_phase IN ('regular','bowl','cfp')),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_non_fbs_games_team_season
    ON non_fbs_games (team_id, season_year);
