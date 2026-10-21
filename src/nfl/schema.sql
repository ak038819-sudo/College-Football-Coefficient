-- NFL schema. Independent of db/league.db and db/ncaambb.db in every respect:
-- its own file, its own tables, its own identity scheme. No football or
-- basketball id is joinable to anything here.
--
-- The one structure worth reading twice is the split between `team` (a
-- FRANCHISE, which is what carries a rating) and `team_code` (what the source
-- called that franchise in a given season). Three franchises changed code
-- mid-archive; without this split they would be six teams and three of them
-- would restart at 1500 on the day they moved.

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshot (
    snapshot_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    kind            TEXT    NOT NULL,
    season          INTEGER,
    retrieved_at    TEXT    NOT NULL,
    source          TEXT,
    source_revision TEXT,
    rows_seen       INTEGER NOT NULL DEFAULT 0,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS team (
    team_key      TEXT PRIMARY KEY,
    code          TEXT NOT NULL UNIQUE,
    name          TEXT NOT NULL,
    location      TEXT,
    nickname      TEXT,
    conference    TEXT,
    division      TEXT,
    first_season  INTEGER,
    last_season   INTEGER,
    snapshot_id   INTEGER REFERENCES snapshot(snapshot_id)
);

-- What the source called a franchise in one season, and the name it used.
-- A row here for 2015 says `STL`; the franchise it points at is `LA`.
CREATE TABLE IF NOT EXISTS team_code (
    code        TEXT    NOT NULL,
    season      INTEGER NOT NULL,
    team_key    TEXT    NOT NULL REFERENCES team(team_key),
    name        TEXT,
    location    TEXT,
    nickname    TEXT,
    conference  TEXT,
    division    TEXT,
    snapshot_id INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (code, season)
);

CREATE INDEX IF NOT EXISTS team_code_by_team ON team_code(team_key, season);

CREATE TABLE IF NOT EXISTS game (
    game_key      TEXT    PRIMARY KEY,
    provider_id   TEXT    NOT NULL UNIQUE,
    season        INTEGER NOT NULL,
    season_label  TEXT    NOT NULL,
    season_type   TEXT    NOT NULL,
    postseason    INTEGER NOT NULL,
    week          INTEGER,
    game_date     TEXT    NOT NULL,
    -- NULL when the source gives no kickoff time. Never midnight.
    kickoff_utc   TEXT,
    kickoff_tbd   INTEGER NOT NULL DEFAULT 0,
    neutral_site  INTEGER NOT NULL DEFAULT 0,
    -- The code the source used, kept so a 2015 game still reads STL.
    home_code     TEXT    NOT NULL,
    away_code     TEXT    NOT NULL,
    -- The franchise, which is what a rating follows.
    home_team_key TEXT    NOT NULL REFERENCES team(team_key),
    away_team_key TEXT    NOT NULL REFERENCES team(team_key),
    -- NULL means unplayed. Never 0.
    home_points   INTEGER,
    away_points   INTEGER,
    overtime      INTEGER,
    divisional_game INTEGER,
    roof          TEXT,
    surface       TEXT,
    stadium_id    TEXT,
    stadium       TEXT,
    rateable      INTEGER NOT NULL,
    exclusions    TEXT    NOT NULL,
    snapshot_id   INTEGER REFERENCES snapshot(snapshot_id)
);

CREATE INDEX IF NOT EXISTS game_by_season ON game(season, game_date);
CREATE INDEX IF NOT EXISTS game_by_home ON game(home_team_key, season);
CREATE INDEX IF NOT EXISTS game_by_away ON game(away_team_key, season);
CREATE INDEX IF NOT EXISTS game_rateable ON game(season, rateable);

-- OUR ratings, and only ours, stamped with the exact configuration that made
-- them and what that configuration scored on a season it had never seen.
CREATE TABLE IF NOT EXISTS rating_model (
    model_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT    NOT NULL UNIQUE,
    config        TEXT    NOT NULL,
    fitted_on     TEXT,
    held_out      TEXT,
    holdout_brier REAL,
    holdout_games INTEGER,
    created_at    TEXT    NOT NULL,
    snapshot_id   INTEGER REFERENCES snapshot(snapshot_id)
);

CREATE TABLE IF NOT EXISTS team_rating (
    team_key      TEXT    NOT NULL REFERENCES team(team_key),
    season        INTEGER NOT NULL,
    model_id      INTEGER NOT NULL REFERENCES rating_model(model_id),
    rating        REAL    NOT NULL,
    games_counted INTEGER NOT NULL,
    updated_at    TEXT    NOT NULL,
    PRIMARY KEY (team_key, season, model_id)
);

CREATE INDEX IF NOT EXISTS rating_by_season ON team_rating(season, model_id);

-- The per-game chain: each game's postgame rating is that team's pregame
-- rating in its next game, which is what makes a replay auditable.
-- `counted = 0` is a game the model declined to learn from, and its change is
-- 0 rather than a half-win.
CREATE TABLE IF NOT EXISTS rating_game (
    game_key      TEXT    NOT NULL REFERENCES game(game_key),
    model_id      INTEGER NOT NULL REFERENCES rating_model(model_id),
    home_pregame  REAL    NOT NULL,
    away_pregame  REAL    NOT NULL,
    home_expected REAL    NOT NULL,
    home_postgame REAL    NOT NULL,
    away_postgame REAL    NOT NULL,
    rating_change REAL    NOT NULL,
    multiplier    REAL,
    counted       INTEGER NOT NULL,
    PRIMARY KEY (game_key, model_id)
);
