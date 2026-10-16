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

-- Games an FBS team has still to play against a non-FBS opponent.
--
-- Separate from `scheduled_games` for the same reason non_fbs_games is
-- separate from `games`: predict_upcoming.py reads scheduled_games and would
-- be asked to price a game whose opponent has no rating. It is separate from
-- non_fbs_games because that table's scores are NOT NULL, and a game not yet
-- played has no score -- storing one as 0-0 would put a tie in every record.
--
-- What it is for: counting. A team with two FCS games left has two more games
-- left than scheduled_games knows about, and the Schedule Strength page said
-- so wrongly until this table existed. The opponent is a name here too, and
-- for the same reason.
CREATE TABLE IF NOT EXISTS scheduled_non_fbs_games (
    game_id                 INTEGER PRIMARY KEY,
    season_year             INTEGER NOT NULL,
    week                    INTEGER,
    season_type             TEXT,
    kickoff_utc             TEXT,               -- ISO timestamp from CFBD; NULL if unknown
    start_time_tbd          INTEGER NOT NULL DEFAULT 0 CHECK (start_time_tbd IN (0,1)),
    team_id                 INTEGER NOT NULL,   -- the FBS side; the only side we hold
    opponent_name           TEXT    NOT NULL,   -- as the source spells it
    opponent_conference     TEXT,
    opponent_classification TEXT,               -- CFBD's own division string, '' when it gave none
    is_home                 INTEGER NOT NULL CHECK (is_home IN (0,1)),
    neutral_site            INTEGER NOT NULL DEFAULT 0 CHECK (neutral_site IN (0,1)),
    game_phase              TEXT    NOT NULL CHECK (game_phase IN ('regular','bowl','cfp')),
    notes                   TEXT,
    venue                   TEXT,               -- CFBD stadium name, display only
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_scheduled_non_fbs_games_team_season
    ON scheduled_non_fbs_games (team_id, season_year);
