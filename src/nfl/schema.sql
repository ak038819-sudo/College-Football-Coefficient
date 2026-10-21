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
    -- Everything below was already in the archive and already being fetched;
    -- schema 1 simply threw it away on the floor of the loader. A blank in
    -- any of these is a real absence and is stored NULL: the betting lines
    -- begin in 1999 but the moneylines only become near-complete in 2007, a
    -- temperature is recorded for outdoor games only, and the source leaves
    -- the referee blank for the games it has not been told about yet.
    home_coach      TEXT,
    away_coach      TEXT,
    -- The home team's expected margin, positive when the home team is
    -- favoured. Measured, not assumed: over the 7,369 games carrying both a
    -- line and a final, mean(result - spread_line) is 0.07 and the flipped
    -- sign gives 4.56, so this is the convention the archive uses.
    spread_line     REAL,
    total_line      REAL,
    home_moneyline  INTEGER,
    away_moneyline  INTEGER,
    home_qb_id      TEXT,
    away_qb_id      TEXT,
    home_qb_name    TEXT,
    away_qb_name    TEXT,
    referee         TEXT,
    -- Days since each team last played. 7 for most games; 4 for a Thursday
    -- after a Sunday, 14 after a bye.
    home_rest       INTEGER,
    away_rest       INTEGER,
    -- Fahrenheit, and miles per hour. Recorded for outdoor games; NULL under
    -- a roof is the absence of a measurement, not a mild day.
    temp            INTEGER,
    wind            INTEGER,
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

-- Head coaches, and what their teams did against what the model expected.
--
-- A coach is keyed by the source's NAME, because the archive numbers nothing
-- but games: `nfl:nflverse:coach:Andy Reid`. Two head coaches who shared a
-- name would therefore share a row, and nothing in this data could tell them
-- apart; 177 distinct names appear across 1999-2026 and none of them repeat
-- in a way this data can see.
--
-- One row per coach PER SEASON PER TEAM. That shape is what carries a
-- midseason change (two coaches, one team, one season) and a coach who moves
-- (one coach, one season, two teams) without either being averaged away.
--
-- The college side could not do this: CFBD never says whose games were whose,
-- so 361 of its 5,657 tenures are unmeasured. The NFL archive names the home
-- and away coach ON EVERY GAME, all 7,548 of them, so every game here is
-- attributed to the person who actually coached it.
CREATE TABLE IF NOT EXISTS coach_season (
    coach_key        TEXT    NOT NULL,
    coach_name       TEXT    NOT NULL,
    season           INTEGER NOT NULL,
    team_key         TEXT    NOT NULL REFERENCES team(team_key),
    model_id         INTEGER NOT NULL REFERENCES rating_model(model_id),
    games            INTEGER NOT NULL,
    wins             INTEGER NOT NULL,
    losses           INTEGER NOT NULL,
    ties             INTEGER NOT NULL,
    postseason_games INTEGER NOT NULL,
    postseason_wins  INTEGER NOT NULL,
    -- The sum of the model's pregame probability that this team won each of
    -- those games. A tie counts half a win on both sides of the comparison.
    expected_wins    REAL    NOT NULL,
    wins_above       REAL    NOT NULL,
    -- Rating gained over the games this coach coached, from the stored chain.
    elo_change       REAL    NOT NULL,
    PRIMARY KEY (coach_key, season, team_key, model_id)
);

CREATE INDEX IF NOT EXISTS coach_by_season ON coach_season(season, model_id);

-- The model against the betting market, season by season.
--
-- The market's probability here is the DE-VIGGED MONEYLINE, not a spread put
-- through a curve. A spread only becomes a probability by way of a model, and
-- comparing our model with our own model of the market would measure nothing.
-- Moneylines cover 72% of the archive overall and 90%+ from 2007, so a season
-- states how many games its comparison rests on.
CREATE TABLE IF NOT EXISTS market_season (
    season             INTEGER NOT NULL,
    model_id           INTEGER NOT NULL REFERENCES rating_model(model_id),
    -- Games with a final score and a stored pregame expectation.
    rated_games        INTEGER NOT NULL,
    model_brier        REAL,
    model_accuracy     REAL,
    -- Games that ALSO carry both moneylines. Smaller, and the only games the
    -- two Brier scores below may be compared on.
    priced_games       INTEGER NOT NULL,
    model_brier_priced REAL,
    market_brier       REAL,
    market_accuracy    REAL,
    -- Games carrying a spread, and how the model's predicted margin did
    -- against it. A pick landing exactly on the line is no pick.
    spread_games       INTEGER NOT NULL,
    ats_wins           INTEGER NOT NULL,
    ats_losses         INTEGER NOT NULL,
    ats_pushes         INTEGER NOT NULL,
    -- Mean absolute error in points, the model's and the market's, on the
    -- same games.
    model_margin_mae   REAL,
    market_margin_mae  REAL,
    PRIMARY KEY (season, model_id)
);
