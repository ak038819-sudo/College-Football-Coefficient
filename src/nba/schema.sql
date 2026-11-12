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

-- ---------------------------------------------------------------------------
-- THE PEOPLE LAYER (N4)
--
-- 2,635 players hold a line in this archive, 2002 to the season in progress,
-- against college football's 99,813: an NBA roster is fifteen names and the
-- league is thirty clubs, so the whole of it is smaller than one football
-- conference's rosters. That is why a person here carries every game of his
-- career rather than a sample of it.
--
-- FOUR THINGS THE FEED DOES THAT THESE TABLES UNDO, each measured before it
-- was written down:
--
-- 0. THE DRAFT FEED IS A DIFFERENT ID SPACE and is not stored at all: 0 of
--    60 picks join to a player id in each of 2010, 2018 and 2019, and its
--    names drop the suffixes the player feed keeps (Robert Williams for
--    Robert Williams III), so a name join would attribute picks to the wrong
--    men. A player's draft round and pick come from his own `player_core`
--    row below, which joins by construction.
-- 1. THE ALL-STAR GAME IS FILED AS A REGULAR-SEASON GAME. LeBron James has 83
--    rows for 2017-18 with `season_type` 2, and the 83rd is for "Team
--    LeBron". Every row whose team is not one of the thirty franchises is
--    dropped on the way in, the same rule `identity.is_franchise` already
--    applies to the schedule.
-- 2. MINUTES, NOT `did_not_play`, SAY WHETHER A MAN PLAYED. The flag is true
--    for 4,800-6,800 rows in most seasons and for 299 in 2009-10, while
--    11,000 rows that season carry no minutes at all. `played` here is
--    whether the line carries minutes, which is consistent across all 26
--    seasons.
-- 3. THE FEED'S OWN SEASON STATISTICS ARE REGULAR SEASON ONLY. Summing every
--    season type against them makes a mess of every comparison, so
--    `person_season` keeps the season type and the rollups stay separate.
-- 4. SIX PLAYERS HOLD TWO PROVIDER IDS EACH. Same name, same date of birth,
--    careers that split across the two records -- Corey Brewer's 2019 is in
--    both. They are linked, never merged, and nothing is summed across the
--    link: see `person_alias` and the football archive's split careers, which
--    were settled the same way.
-- ---------------------------------------------------------------------------

-- One human, as the provider knows him. `person_key` is
-- `nba:espn:player:<athlete id>`; the id is the provider's and is stable
-- across seasons, which was measured rather than assumed: every one of the
-- 851,463 box-score lines joins to a player_core row for its own season.
CREATE TABLE IF NOT EXISTS person (
    person_key       TEXT PRIMARY KEY,
    athlete_id       INTEGER NOT NULL UNIQUE,
    full_name        TEXT    NOT NULL,
    display_name     TEXT,
    short_name       TEXT,
    first_name       TEXT,
    last_name        TEXT,
    position         TEXT,
    position_short   TEXT,
    jersey           TEXT,
    height_inches    INTEGER,
    weight_pounds    INTEGER,
    date_of_birth    TEXT,
    birth_city       TEXT,
    birth_state      TEXT,
    birth_country    TEXT,
    college_id       INTEGER,
    draft_year       INTEGER,
    draft_round      INTEGER,
    draft_selection  INTEGER,
    experience_years INTEGER,
    -- The span the ARCHIVE carries, derived from the lines stored below, not
    -- a biography: a career that began in 1996 begins in 2002 here, and the
    -- page says so rather than implying the man was a rookie.
    first_season     INTEGER,
    last_season      INTEGER,
    snapshot_id      INTEGER REFERENCES snapshot(snapshot_id)
);

-- Two provider ids that are one man. Stored BOTH WAYS round, so neither id is
-- the primary one and a page reached by either shows the other. Nothing is
-- summed across it: the two records' statistics stay apart, because the
-- archive cannot say which of the two holds a given game without reading the
-- game, and a doubled season is worse than a split one.
CREATE TABLE IF NOT EXISTS person_alias (
    person_key  TEXT NOT NULL REFERENCES person(person_key),
    other_key   TEXT NOT NULL REFERENCES person(person_key),
    basis       TEXT NOT NULL,
    snapshot_id INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (person_key, other_key)
);

-- One line per man per game. `played` is whether the line carries minutes
-- (rule 2 above); a row with no minutes is kept because the archive carries
-- it, and the published career log shows only played games.
CREATE TABLE IF NOT EXISTS person_game (
    person_key   TEXT    NOT NULL REFERENCES person(person_key),
    game_key     TEXT    NOT NULL REFERENCES game(game_key),
    season       INTEGER NOT NULL,
    season_type  TEXT    NOT NULL,
    game_date    TEXT,
    team_key     TEXT    REFERENCES team(team_key),
    opponent_key TEXT    REFERENCES team(team_key),
    home_away    TEXT,
    played       INTEGER NOT NULL,
    started      INTEGER,
    minutes      REAL,
    points       INTEGER,
    rebounds     INTEGER,
    offensive_rebounds INTEGER,
    defensive_rebounds INTEGER,
    assists      INTEGER,
    steals       INTEGER,
    blocks       INTEGER,
    turnovers    INTEGER,
    fouls        INTEGER,
    field_goals_made      INTEGER,
    field_goals_attempted INTEGER,
    threes_made           INTEGER,
    threes_attempted      INTEGER,
    free_throws_made      INTEGER,
    free_throws_attempted INTEGER,
    plus_minus   INTEGER,
    snapshot_id  INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (person_key, game_key)
);

-- A man's season with one club, DERIVED from the lines above rather than
-- taken from the feed's own season statistics: one source of truth, and the
-- two agree on the countable stats for 99.5% of players in 2002 and 2010.
-- Where they part company is the COUNT OF GAMES in 2009-2012, where the feed
-- says a man played more games than it carries lines for, so `feed_games`
-- keeps its number and `matches_feed` states whether the two agree. Nothing
-- is quietly reconciled.
CREATE TABLE IF NOT EXISTS person_season (
    person_key   TEXT    NOT NULL REFERENCES person(person_key),
    season       INTEGER NOT NULL,
    season_type  TEXT    NOT NULL,
    team_key     TEXT    NOT NULL REFERENCES team(team_key),
    games        INTEGER NOT NULL,
    starts       INTEGER NOT NULL,
    minutes      REAL,
    points       INTEGER,
    rebounds     INTEGER,
    offensive_rebounds INTEGER,
    defensive_rebounds INTEGER,
    assists      INTEGER,
    steals       INTEGER,
    blocks       INTEGER,
    turnovers    INTEGER,
    fouls        INTEGER,
    field_goals_made      INTEGER,
    field_goals_attempted INTEGER,
    threes_made           INTEGER,
    threes_attempted      INTEGER,
    free_throws_made      INTEGER,
    free_throws_attempted INTEGER,
    plus_minus   INTEGER,
    feed_games   INTEGER,
    matches_feed INTEGER,
    snapshot_id  INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (person_key, season, season_type, team_key)
);

CREATE INDEX IF NOT EXISTS person_game_by_person
    ON person_game(person_key, season, game_date);
CREATE INDEX IF NOT EXISTS person_game_by_game ON person_game(game_key);
CREATE INDEX IF NOT EXISTS person_season_by_season
    ON person_season(season, season_type);

-- ----------------------------------------------------------------------
-- The team box score (schema 4): the counts PxP+ and the opportunity
-- grades are computed from, and nothing derived.
-- ----------------------------------------------------------------------
-- `src/nba/team_box.py` loads this from the `team_box` feed, two rows a
-- game. What is stored here is COUNTS ONLY: not one rate, index or grade,
-- because every one of them is a quotient of numbers that are all here and
-- `src/nba/team_metrics.py` is the single place that divides them. A stored
-- PxP+ would be a second answer to a question the counts already answer,
-- and the two would drift the first time the league denominator changed.
--
-- A MISSING COUNT IS NULL, AND IT COSTS THE WHOLE GAME. Measured on
-- 2025-26: game 401810169 (Chicago at Orlando, 2025-12-01) carries no
-- `total_turnovers` for Chicago, so its possession estimate cannot be made
-- and the game is unmeasurable for BOTH sides -- the denominator is shared.
-- `team_metrics.measurable` is where that is decided; nothing here guesses
-- a zero.
--
-- THE SEASON TYPE IS THE SCHEDULE'S, not the feed's, for the reason
-- `src/nba/people.py` takes the schedule's: this feed calls the NBA Cup
-- final a regular-season game, and the archive already knows better.
CREATE TABLE IF NOT EXISTS team_game_box (
    game_key     TEXT    NOT NULL REFERENCES game(game_key),
    team_key     TEXT    NOT NULL REFERENCES team(team_key),
    opponent_key TEXT    REFERENCES team(team_key),
    season       INTEGER NOT NULL,
    season_type  TEXT    NOT NULL,
    game_date    TEXT    NOT NULL,
    home_away    TEXT,
    points       INTEGER,
    -- The four the possession estimate needs. FGA + 0.44 x FTA - ORB + TOV.
    field_goals_attempted INTEGER,
    free_throws_attempted INTEGER,
    offensive_rebounds    INTEGER,
    -- `total_turnovers` is the one the estimate uses: it includes team
    -- turnovers, which `turnovers` does not. The two disagree on 1,272 of
    -- 2,460 team rows in 2025-26, so both are stored and the arithmetic
    -- names which one it reads.
    total_turnovers       INTEGER,
    turnovers             INTEGER,
    team_turnovers        INTEGER,
    -- WHICH TURNOVER COUNT THE POSSESSION ESTIMATE READS, and on what
    -- basis. Derived per SEASON in `team_box.turnover_basis`, because the
    -- feed's meaning changes between seasons and no single row shows it:
    -- 2001-02 to 2011-12 set `team_turnovers` to a COPY of `turnovers`, so
    -- their `total_turnovers` is the team's turnovers counted twice -- 28.9
    -- a game against a true 14.4 -- and 2017-18 carries no total at all.
    -- The basis is 'total' where the sum is trustworthy and 'team-only'
    -- where the narrower count is all there is, and it is published so a
    -- reader is never shown a 2004 rate as if it were measured the same way
    -- as a 2026 one.
    possession_turnovers  INTEGER,
    turnover_basis        TEXT,
    -- Kept because a page that shows an opportunity edge is asked what made
    -- it, and these are the counts behind the answer.
    field_goals_made      INTEGER,
    free_throws_made      INTEGER,
    threes_made           INTEGER,
    threes_attempted      INTEGER,
    defensive_rebounds    INTEGER,
    total_rebounds        INTEGER,
    assists               INTEGER,
    steals                INTEGER,
    blocks                INTEGER,
    fouls                 INTEGER,
    -- Whether `points` equals the final the schedule carries. Stated, never
    -- corrected: a box score that disagrees with the final is a fact about
    -- the source, and a page may say so rather than publish a quiet fix.
    reconciled   INTEGER,
    snapshot_id  INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (game_key, team_key)
);

CREATE INDEX IF NOT EXISTS team_game_box_by_season
    ON team_game_box(season, season_type, game_date);
CREATE INDEX IF NOT EXISTS team_game_box_by_team
    ON team_game_box(team_key, season);
