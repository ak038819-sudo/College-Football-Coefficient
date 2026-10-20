-- NCAA men's basketball schema. Independent of db/league.db in every respect:
-- its own file, its own tables, its own identity scheme. No football table is
-- referenced and no football id is joinable to anything here.
--
-- Provider Elo is deliberately absent. /games returns homeTeamEloStart and
-- homeTeamEloEnd; storing them would let a provider rating leak into ours.

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

-- One row per ingestion run, so every stored row can be traced to the request
-- that produced it and the revision of the code that ran.
CREATE TABLE IF NOT EXISTS snapshot (
    snapshot_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    kind            TEXT    NOT NULL,
    season          INTEGER,
    retrieved_at    TEXT    NOT NULL,
    source_revision TEXT,
    requests        INTEGER NOT NULL DEFAULT 0,
    retries         INTEGER NOT NULL DEFAULT 0,
    rows_seen       INTEGER NOT NULL DEFAULT 0,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS team (
    team_key        TEXT    PRIMARY KEY,
    provider_id     INTEGER NOT NULL UNIQUE,
    school          TEXT    NOT NULL,
    mascot          TEXT,
    abbreviation    TEXT,
    display_name    TEXT,
    short_name      TEXT,
    primary_color   TEXT,
    secondary_color TEXT,
    current_venue   TEXT,
    current_city    TEXT,
    current_state   TEXT,
    first_seen_snapshot INTEGER REFERENCES snapshot(snapshot_id),
    last_seen_snapshot  INTEGER REFERENCES snapshot(snapshot_id)
);

-- Membership is per season because it moves every year: the Division I count
-- measured 362, 364, 365 and 365 across 2024-2027.
CREATE TABLE IF NOT EXISTS team_season (
    team_key      TEXT    NOT NULL REFERENCES team(team_key),
    season        INTEGER NOT NULL,
    season_label  TEXT    NOT NULL,
    conference_id INTEGER,
    conference    TEXT,
    division_one  INTEGER NOT NULL,
    snapshot_id   INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (team_key, season)
);

CREATE TABLE IF NOT EXISTS game (
    game_key       TEXT    PRIMARY KEY,
    provider_id    INTEGER NOT NULL UNIQUE,
    season         INTEGER NOT NULL,
    season_label   TEXT    NOT NULL,
    season_type    TEXT    NOT NULL,
    tournament     TEXT,
    status         TEXT    NOT NULL,
    start_date     TEXT    NOT NULL,
    start_date_utc TEXT    NOT NULL,
    -- 44% of the 2026-27 schedule has no tip-off time yet. A missing time is
    -- recorded as such; it is never rendered or stored as midnight.
    start_time_tbd INTEGER NOT NULL DEFAULT 0,
    neutral_site   INTEGER NOT NULL DEFAULT 0,
    conference_game INTEGER,
    home_team_key  TEXT    NOT NULL REFERENCES team(team_key),
    away_team_key  TEXT    NOT NULL REFERENCES team(team_key),
    -- NULL means the provider gave no score. Never 0.
    home_points    INTEGER,
    away_points    INTEGER,
    home_period_points TEXT,
    away_period_points TEXT,
    venue_id       INTEGER,
    venue          TEXT,
    city           TEXT,
    state          TEXT,
    attendance     INTEGER,
    -- Whether this game may update a Division I rating, and if not, why.
    rateable       INTEGER NOT NULL,
    division_one   INTEGER NOT NULL,
    exclusions     TEXT    NOT NULL,
    snapshot_id    INTEGER REFERENCES snapshot(snapshot_id)
);

CREATE INDEX IF NOT EXISTS game_by_season ON game(season, start_date_utc);
CREATE INDEX IF NOT EXISTS game_by_home ON game(home_team_key, season);
CREATE INDEX IF NOT EXISTS game_by_away ON game(away_team_key, season);
CREATE INDEX IF NOT EXISTS game_rateable ON game(season, rateable);

-- One identity per person, for their whole career.
CREATE TABLE IF NOT EXISTS player (
    player_key    TEXT    PRIMARY KEY,
    provider_id   INTEGER NOT NULL UNIQUE,
    name          TEXT,
    first_name    TEXT,
    last_name     TEXT,
    date_of_birth TEXT,
    hometown      TEXT,
    hometown_state TEXT,
    hometown_country TEXT,
    snapshot_id   INTEGER REFERENCES snapshot(snapshot_id)
);

-- A transfer is two rows here, never two players. Players are never equated by
-- name: the provider id is the identity and this table is the membership.
CREATE TABLE IF NOT EXISTS player_team_season (
    player_key   TEXT    NOT NULL REFERENCES player(player_key),
    team_key     TEXT    NOT NULL REFERENCES team(team_key),
    season       INTEGER NOT NULL,
    season_label TEXT    NOT NULL,
    jersey       TEXT,
    position     TEXT,
    height       REAL,
    weight       REAL,
    snapshot_id  INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (player_key, team_key, season)
);

CREATE INDEX IF NOT EXISTS roster_by_team_season
    ON player_team_season(team_key, season);

CREATE TABLE IF NOT EXISTS team_game_stat (
    game_key    TEXT    NOT NULL REFERENCES game(game_key),
    team_key    TEXT    NOT NULL REFERENCES team(team_key),
    is_home     INTEGER NOT NULL,
    points      INTEGER,
    game_minutes REAL,
    pace        REAL,
    possessions REAL,
    snapshot_id INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (game_key, team_key)
);

CREATE TABLE IF NOT EXISTS player_game_stat (
    game_key    TEXT    NOT NULL REFERENCES game(game_key),
    player_key  TEXT    NOT NULL REFERENCES player(player_key),
    team_key    TEXT    NOT NULL REFERENCES team(team_key),
    starter     INTEGER,
    -- NULL minutes means the provider recorded none, not zero played.
    minutes     REAL,
    points      REAL,
    snapshot_id INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (game_key, player_key)
);

CREATE INDEX IF NOT EXISTS player_game_by_player
    ON player_game_stat(player_key);
