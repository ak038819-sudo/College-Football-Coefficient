-- People of the game: players and head coaches as durable entities.
--
-- The point of these tables is that a PERSON is not a name. Display names change
-- (nicknames, suffixes, married names, a source correcting its own spelling), so
-- every link the website generates keys on the immutable player_id / coach_id here
-- and never on the text shown. Source-specific identifiers live in their own
-- *_external_ids table, which is what lets one person carry several sources'
-- numbering without the name ever becoming the key.
--
-- Nothing in the rating pipeline reads any of this. Elo, CoE and the playoff
-- builders are untouched by roster or coaching data.
--
-- CREATE TABLE IF NOT EXISTS throughout (like kickoff_tables.sql) so applying this
-- to an existing db/league.db adds the tables without dropping anything.

CREATE TABLE IF NOT EXISTS players (
    player_id        INTEGER PRIMARY KEY,
    display_name     TEXT NOT NULL,
    first_name       TEXT,
    last_name        TEXT,
    primary_position TEXT,          -- latest known position; season detail lives in player_team_seasons
    hometown         TEXT,
    home_state       TEXT,
    height           INTEGER,       -- inches, as the source gives it; NULL when unknown
    weight           INTEGER,       -- pounds; NULL when unknown
    headshot_path    TEXT,          -- optional; a page must render without one
    headshot_url     TEXT,
    latest_season    INTEGER,       -- most recent season with a roster row, derived on load
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);

-- A person's identifier at one source. The uniqueness constraint is what makes
-- re-running an ingestion idempotent: the same CFBD athlete id always lands on
-- the same player_id instead of minting a second person.
CREATE TABLE IF NOT EXISTS player_external_ids (
    player_id   INTEGER NOT NULL,
    source      TEXT NOT NULL,      -- 'cfbd', 'espn', 'manual', ...
    external_id TEXT NOT NULL,
    confidence  TEXT,               -- 'id' (source id matched), 'context' (name+team+season+position), 'manual'
    is_primary  INTEGER NOT NULL DEFAULT 0 CHECK (is_primary IN (0, 1)),
    PRIMARY KEY (source, external_id),
    FOREIGN KEY (player_id) REFERENCES players(player_id)
);

CREATE INDEX IF NOT EXISTS idx_player_external_ids_player ON player_external_ids(player_id);

-- Name variants that mean the same person: suffix and punctuation forms, and the
-- normalized key used for contextual matching. Aliases exist so a lookup can be
-- forgiving WITHOUT a forgiving lookup ever creating or merging a person.
CREATE TABLE IF NOT EXISTS player_name_aliases (
    player_id  INTEGER NOT NULL,
    alias      TEXT NOT NULL,       -- normalized form (see person_identity.normalize_name)
    raw_name   TEXT,                -- a name as some source actually spelled it
    PRIMARY KEY (player_id, alias),
    FOREIGN KEY (player_id) REFERENCES players(player_id)
);

CREATE INDEX IF NOT EXISTS idx_player_name_aliases_alias ON player_name_aliases(alias);

-- One row per player per season per team. A transfer is two rows for ONE
-- player_id, never two players; a new jersey or position is season metadata on
-- the row, not a new person.
CREATE TABLE IF NOT EXISTS player_team_seasons (
    player_id         INTEGER NOT NULL,
    season_year       INTEGER NOT NULL,
    team_id           INTEGER NOT NULL,
    jersey            INTEGER,
    position          TEXT,
    class_year        TEXT,         -- FR/SO/JR/SR/GR, or the raw source value
    height            INTEGER,
    weight            INTEGER,
    source            TEXT NOT NULL,
    source_updated_at TEXT,
    PRIMARY KEY (player_id, season_year, team_id),
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_player_team_seasons_team ON player_team_seasons(team_id, season_year);
CREATE INDEX IF NOT EXISTS idx_player_team_seasons_season ON player_team_seasons(season_year);

-- No hire_date here on purpose. CFBD's hire date belongs to the JOB, not the
-- person: one coach carries a different one per school (Al Golden has
-- 2005-12-08 for Temple and 2010-12-12 for Miami), so a single column on the
-- person would have to pick one and be wrong about the rest. It lives on
-- coach_tenures instead, which is where it is actually true.
CREATE TABLE IF NOT EXISTS coaches (
    coach_id      INTEGER PRIMARY KEY,
    display_name  TEXT NOT NULL,
    first_name    TEXT,
    last_name     TEXT,
    headshot_path TEXT,
    headshot_url  TEXT,
    latest_season INTEGER,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);

-- CFBD's /coaches feed carries no coach identifier, and its hire date belongs to
-- the job rather than the person, so the NAME is the only person-level signal it
-- offers. The external_id stored here is therefore the normalized name, and
-- confidence records that honestly as 'name only': two coaches who genuinely
-- share a name cannot be separated by this feed, and load_coaches.py flags the
-- ambiguous careers in person_unresolved instead of asserting them. Keeping the
-- key here rather than in code means a later source with real coach ids can be
-- added beside it without a migration.
CREATE TABLE IF NOT EXISTS coach_external_ids (
    coach_id    INTEGER NOT NULL,
    source      TEXT NOT NULL,
    external_id TEXT NOT NULL,
    confidence  TEXT,
    is_primary  INTEGER NOT NULL DEFAULT 0 CHECK (is_primary IN (0, 1)),
    PRIMARY KEY (source, external_id),
    FOREIGN KEY (coach_id) REFERENCES coaches(coach_id)
);

CREATE INDEX IF NOT EXISTS idx_coach_external_ids_coach ON coach_external_ids(coach_id);

-- One row per coach per team per season. CFBD's coaching feed is HEAD COACH only,
-- so role is 'head coach' for every row it produces; the column exists so a source
-- covering coordinators can be added without reshaping the table, and nothing here
-- should ever be read as assistant history.
-- source_team is CFBD's own school string, kept beside the canonical team_id so a
-- bad alias resolution can be found later instead of being silently baked in.
CREATE TABLE IF NOT EXISTS coach_tenures (
    coach_id        INTEGER NOT NULL,
    team_id         INTEGER NOT NULL,
    season_year     INTEGER NOT NULL,
    role            TEXT NOT NULL DEFAULT 'head coach',
    hire_date       TEXT,
    games           INTEGER,
    wins            INTEGER,
    losses          INTEGER,
    ties            INTEGER,
    preseason_rank  INTEGER,
    postseason_rank INTEGER,
    source          TEXT NOT NULL,
    source_team     TEXT,
    PRIMARY KEY (coach_id, team_id, season_year),
    FOREIGN KEY (coach_id) REFERENCES coaches(coach_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_coach_tenures_team ON coach_tenures(team_id, season_year);
CREATE INDEX IF NOT EXISTS idx_coach_tenures_season ON coach_tenures(season_year);

-- Every row an ingestion could NOT confidently attach to a person or a team, kept
-- for review rather than guessed at. A same-name collision, an unresolvable school
-- and a box-score line with no athlete id all land here; none of them is allowed
-- to invent a person record.
CREATE TABLE IF NOT EXISTS person_unresolved (
    entity       TEXT NOT NULL,     -- 'player' or 'coach'
    source       TEXT NOT NULL,
    context      TEXT NOT NULL,     -- which ingestion produced it, e.g. 'roster'
    display_name TEXT NOT NULL,
    season_year  INTEGER,
    source_team  TEXT,
    reason       TEXT NOT NULL,
    seen_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_person_unresolved_reason ON person_unresolved(entity, reason);
