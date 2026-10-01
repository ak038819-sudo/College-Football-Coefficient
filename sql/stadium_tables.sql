-- Physical venues are independent from a game's designated home team and
-- neutral_site flag. A stable seed key survives sponsorship/name changes.
CREATE TABLE IF NOT EXISTS stadiums (
    stadium_id INTEGER PRIMARY KEY,
    stadium_key TEXT NOT NULL UNIQUE,
    stadium_name TEXT NOT NULL,
    city TEXT,
    state TEXT,
    latitude REAL,
    longitude REAL,
    capacity INTEGER,
    surface TEXT,
    indoor INTEGER CHECK (indoor IN (0,1)),
    opened_year INTEGER
);
CREATE TABLE IF NOT EXISTS stadium_aliases (
    alias_key TEXT NOT NULL,
    stadium_id INTEGER NOT NULL REFERENCES stadiums(stadium_id),
    PRIMARY KEY (alias_key, stadium_id)
);
CREATE TABLE IF NOT EXISTS team_stadiums (
    team_id INTEGER NOT NULL REFERENCES teams(team_id),
    stadium_id INTEGER NOT NULL REFERENCES stadiums(stadium_id),
    start_season INTEGER NOT NULL,
    end_season INTEGER,
    is_primary INTEGER NOT NULL CHECK (is_primary IN (0,1)),
    notes TEXT,
    PRIMARY KEY (team_id, stadium_id, start_season),
    CHECK (end_season IS NULL OR end_season >= start_season)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_team_primary_stadium_start
    ON team_stadiums(team_id, start_season) WHERE is_primary = 1;
CREATE TABLE IF NOT EXISTS game_venue_overrides (
    game_id INTEGER PRIMARY KEY,
    stadium_id INTEGER NOT NULL REFERENCES stadiums(stadium_id),
    notes TEXT NOT NULL
);
