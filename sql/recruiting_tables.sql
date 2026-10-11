-- Recruiting classes and individual recruits, loaded from CFBD by
-- src/load_recruiting.py.
--
-- `team_id` is NULLABLE on purpose, and so is `committed_team_id`. A quarter
-- of CFBD's recruiting classes belong to FCS and lower-division schools this
-- database does not carry, and recruits commit to schools well outside it.
-- Those rows are kept with their provider name in `source_team` /
-- `committed_to` rather than dropped: this repository's recurring fault is a
-- silent skip, and a dropped class is indistinguishable afterwards from a
-- school that never signed anyone.

CREATE TABLE IF NOT EXISTS recruiting_classes (
    source_team  TEXT    NOT NULL,          -- CFBD's own school string, always kept
    season_year  INTEGER NOT NULL,          -- the season the class signs INTO
    team_id      INTEGER,                   -- NULL when this database has no such team
    class_rank   INTEGER,
    points       REAL,
    PRIMARY KEY (source_team, season_year),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_recruiting_classes_team
    ON recruiting_classes (team_id, season_year);

-- CFBD's recruit id is globally unique across seasons (verified over all
-- 81,262 rows of the 2000-2026 archive), so it alone is the key.
CREATE TABLE IF NOT EXISTS recruits (
    recruit_id           TEXT    NOT NULL PRIMARY KEY,
    season_year          INTEGER NOT NULL,
    athlete_id           INTEGER,           -- CFBD athlete id; absent before 2010 and sparse after
    name                 TEXT    NOT NULL,
    position             TEXT,
    recruit_type         TEXT,
    height               INTEGER,
    weight               INTEGER,
    stars                INTEGER,
    rating               REAL,
    national_rank        INTEGER,
    committed_team_id    INTEGER,           -- NULL when uncommitted or not a team here
    committed_to         TEXT,              -- CFBD's own string, always kept
    high_school          TEXT,
    city                 TEXT,
    state_province       TEXT,
    country              TEXT,
    hometown_latitude    REAL,
    hometown_longitude   REAL,
    hometown_county_fips TEXT,
    FOREIGN KEY (committed_team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_recruits_committed
    ON recruits (committed_team_id, season_year);
CREATE INDEX IF NOT EXISTS idx_recruits_season
    ON recruits (season_year);
CREATE INDEX IF NOT EXISTS idx_recruits_athlete
    ON recruits (athlete_id);
