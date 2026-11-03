-- NBA schema. Independent of db/league.db, db/ncaambb.db and db/nfl.db in
-- every respect: its own file, its own tables, its own identity scheme. No
-- football, college basketball or NFL id is joinable to anything here.
--
-- Two differences from the NFL schema are worth reading twice.
--
-- First, there is no `team_code` table keyed by a short code. ESPN keeps a
-- franchise's integer id across a relocation, so the franchise needs no
-- reassembling; what changes is its NAME, and `team_season` keeps that so a
-- 2007 box score still says Seattle SuperSonics.
--
-- Second, `team_season.conference` comes from the standings feed rather than
-- from a table written down in code. Division does NOT: the feed groups by
-- conference only, so division is left NULL instead of being invented.

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

-- A franchise. `team_id` is ESPN's, kept as the integer it is so that a
-- reader comparing against the source does not have to undo a decoration.
CREATE TABLE IF NOT EXISTS team (
    team_key      TEXT    PRIMARY KEY,
    team_id       INTEGER NOT NULL UNIQUE,
    abbreviation  TEXT,
    location      TEXT,
    nickname      TEXT,
    name          TEXT    NOT NULL,
    conference    TEXT,
    logo          TEXT,
    logo_dark     TEXT,
    first_season  INTEGER,
    last_season   INTEGER,
    snapshot_id   INTEGER REFERENCES snapshot(snapshot_id)
);

-- What the source called a franchise in one season. A row here for 2007 says
-- Seattle SuperSonics; the franchise it points at is Oklahoma City.
CREATE TABLE IF NOT EXISTS team_season (
    team_key     TEXT    NOT NULL REFERENCES team(team_key),
    season       INTEGER NOT NULL,
    abbreviation TEXT,
    location     TEXT,
    nickname     TEXT,
    name         TEXT,
    conference   TEXT,
    division     TEXT,
    logo         TEXT,
    logo_dark    TEXT,
    snapshot_id  INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (team_key, season)
);

CREATE INDEX IF NOT EXISTS team_season_by_season
    ON team_season(season, team_key);

CREATE TABLE IF NOT EXISTS game (
    game_key      TEXT    PRIMARY KEY,
    provider_id   TEXT    NOT NULL UNIQUE,
    season        INTEGER NOT NULL,
    season_label  TEXT    NOT NULL,
    season_type   TEXT    NOT NULL,
    postseason    INTEGER NOT NULL DEFAULT 0,
    game_date     TEXT    NOT NULL,
    tipoff_utc    TEXT,
    tipoff_tbd    INTEGER NOT NULL DEFAULT 0,
    neutral_site  INTEGER NOT NULL DEFAULT 0,
    completed     INTEGER NOT NULL DEFAULT 0,
    home_team_key TEXT    NOT NULL REFERENCES team(team_key),
    away_team_key TEXT    NOT NULL REFERENCES team(team_key),
    home_team_id  INTEGER NOT NULL,
    away_team_id  INTEGER NOT NULL,
    home_points   INTEGER,
    away_points   INTEGER,
    venue_id      TEXT,
    venue         TEXT,
    venue_city    TEXT,
    venue_state   TEXT,
    attendance    INTEGER,
    notes         TEXT,
    -- Whether this game may move a rating, and every reason it may not.
    rateable      INTEGER NOT NULL DEFAULT 0,
    exclusions    TEXT    NOT NULL DEFAULT '[]',
    snapshot_id   INTEGER REFERENCES snapshot(snapshot_id)
);

CREATE INDEX IF NOT EXISTS game_by_season ON game(season, game_date);
CREATE INDEX IF NOT EXISTS game_by_home ON game(home_team_key, season);
CREATE INDEX IF NOT EXISTS game_by_away ON game(away_team_key, season);
CREATE INDEX IF NOT EXISTS game_rateable ON game(rateable, game_date);

-- A model configuration, registered once and pointed at by every rating it
-- produced, so a rating always says which model made it.
CREATE TABLE IF NOT EXISTS rating_model (
    model_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    name           TEXT    NOT NULL UNIQUE,
    config         TEXT    NOT NULL,
    fitted_on      TEXT,
    held_out       TEXT,
    holdout_brier  REAL,
    holdout_games  INTEGER,
    created_at     TEXT    NOT NULL,
    snapshot_id    INTEGER REFERENCES snapshot(snapshot_id)
);

CREATE TABLE IF NOT EXISTS team_rating (
    team_key      TEXT    NOT NULL REFERENCES team(team_key),
    season        INTEGER NOT NULL,
    model_id      INTEGER NOT NULL REFERENCES rating_model(model_id),
    rating        REAL    NOT NULL,
    games_counted INTEGER NOT NULL DEFAULT 0,
    updated_at    TEXT    NOT NULL,
    PRIMARY KEY (team_key, season, model_id)
);

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
    counted       INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (game_key, model_id)
);
