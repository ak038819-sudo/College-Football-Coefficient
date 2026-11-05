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

-- ----------------------------------------------------------------------
-- The game layer (schema 2): what a game page shows, and nothing else.
-- ----------------------------------------------------------------------
-- The plays themselves are NEVER stored. A season of ESPN play-by-play is
-- 642,472 rows and 22 MB of Parquet for 1,325 games — 485 plays a game, of
-- which 120 score. `src/nba/plays.py` reduces each season to the four things
-- below and deletes the file before the next season is fetched, exactly as
-- `src/nfl/plays.py` does with the nflverse play-by-play.
--
-- There is no NBA drive. Basketball's unit is the possession and the feed
-- does not mark one, so the reduction is not "drives and scores": it is the
-- period line score, the shape of the game, a thinned margin curve and the
-- handful of plays a game story would actually print.

-- One row per period: the line score, including overtime periods.
CREATE TABLE IF NOT EXISTS game_period (
    game_key    TEXT    NOT NULL REFERENCES game(game_key),
    period      INTEGER NOT NULL,
    home_points INTEGER NOT NULL,
    away_points INTEGER NOT NULL,
    snapshot_id INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (game_key, period)
);

-- One row per game: how the game went, in the quantities a box score prints.
-- `home_time_leading` and its companions are seconds of elapsed game time,
-- so the three of them add up to the length of the game.
CREATE TABLE IF NOT EXISTS game_shape (
    game_key          TEXT    PRIMARY KEY REFERENCES game(game_key),
    plays             INTEGER NOT NULL DEFAULT 0,
    scoring_plays     INTEGER NOT NULL DEFAULT 0,
    periods           INTEGER NOT NULL DEFAULT 0,
    overtime          INTEGER NOT NULL DEFAULT 0,
    lead_changes      INTEGER NOT NULL DEFAULT 0,
    ties              INTEGER NOT NULL DEFAULT 0,
    home_biggest_lead INTEGER NOT NULL DEFAULT 0,
    away_biggest_lead INTEGER NOT NULL DEFAULT 0,
    home_biggest_run  INTEGER NOT NULL DEFAULT 0,
    away_biggest_run  INTEGER NOT NULL DEFAULT 0,
    home_time_leading INTEGER,
    away_time_leading INTEGER,
    time_tied         INTEGER,
    -- Whether the period line score and the end of the margin curve agree
    -- with the final score the box score carries. NULL for a game with no
    -- final score yet. MEASURED, not assumed: 1 of 2,628 games across 2010
    -- and 2026 does not reconcile — 401810102, whose published plays stop
    -- six points short of the final. Three others did until the line score
    -- was changed from a sum of per-play deltas to the step between
    -- cumulative totals. A game page shows the period detail only
    -- where this is 1, because a line score that contradicts the final is
    -- worse than no line score.
    reconciled        INTEGER,
    snapshot_id       INTEGER REFERENCES snapshot(snapshot_id)
);

-- The thinned margin curve. Each row is a state the archive states — the
-- score at a moment the clock really showed — sampled at most once per
-- `plays.CURVE_SECONDS` of game clock so that one game is a few dozen rows
-- rather than a few hundred. `seconds_left` is REGULATION seconds remaining
-- and is 0 throughout overtime, which is what src/win_probability.py means
-- by it; the period says which overtime it was.
CREATE TABLE IF NOT EXISTS game_state (
    game_key     TEXT    NOT NULL REFERENCES game(game_key),
    sequence     INTEGER NOT NULL,
    period       INTEGER NOT NULL,
    clock        TEXT,
    seconds_left INTEGER NOT NULL,
    elapsed      INTEGER NOT NULL,
    home_score   INTEGER NOT NULL,
    away_score   INTEGER NOT NULL,
    snapshot_id  INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (game_key, sequence)
);

-- The plays worth printing. The rule is in `plays.key_plays` and is
-- model-free on purpose: a play is kept when it changed the lead or tied the
-- game late, or when it was scored in the closing minutes of a close game.
-- Nothing here is chosen by a win-probability model, because the model is
-- fitted on `game_state` and a selection made by it would be circular.
CREATE TABLE IF NOT EXISTS game_play (
    game_key     TEXT    NOT NULL REFERENCES game(game_key),
    sequence     INTEGER NOT NULL,
    period       INTEGER NOT NULL,
    clock        TEXT,
    seconds_left INTEGER NOT NULL,
    team_key     TEXT    REFERENCES team(team_key),
    points       INTEGER NOT NULL DEFAULT 0,
    kind         TEXT,
    scorer       TEXT,
    scorer_id    INTEGER,
    description  TEXT,
    home_score   INTEGER NOT NULL,
    away_score   INTEGER NOT NULL,
    reason       TEXT    NOT NULL,
    snapshot_id  INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (game_key, sequence)
);

CREATE INDEX IF NOT EXISTS game_state_by_game ON game_state(game_key, sequence);
CREATE INDEX IF NOT EXISTS game_play_by_game ON game_play(game_key, sequence);
