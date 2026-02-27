-- sql/standings_tables.sql
-- Standings foundation: computed records + imported final order

-- 1) Computed conference records (derived from games)
-- This table stores what we can compute objectively: conf/overall W-L.
CREATE TABLE IF NOT EXISTS conference_team_records_by_year (
  season_year     INTEGER NOT NULL,
  conference      TEXT NOT NULL,
  team_id         INTEGER NOT NULL,

  conf_wins       INTEGER NOT NULL DEFAULT 0,
  conf_losses     INTEGER NOT NULL DEFAULT 0,
  conf_ties       INTEGER NOT NULL DEFAULT 0,
  conf_games      INTEGER NOT NULL DEFAULT 0,

  overall_wins    INTEGER NOT NULL DEFAULT 0,
  overall_losses  INTEGER NOT NULL DEFAULT 0,
  overall_ties    INTEGER NOT NULL DEFAULT 0,
  overall_games   INTEGER NOT NULL DEFAULT 0,

  -- Convenience fields (can be computed on the fly, but nice to store)
  conf_win_pct    REAL NOT NULL DEFAULT 0.0,
  overall_win_pct REAL NOT NULL DEFAULT 0.0,

  -- Provenance
  computed_at     TEXT DEFAULT (datetime('now')),

  PRIMARY KEY (season_year, conference, team_id),
  FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_records_year_conf
  ON conference_team_records_by_year (season_year, conference);

CREATE INDEX IF NOT EXISTS idx_records_team
  ON conference_team_records_by_year (team_id);


-- 2) Official conference standings order (imported)
-- This preserves each conference's real tiebreak rules by storing final rank order.
CREATE TABLE IF NOT EXISTS conference_standings_by_year (
  season_year   INTEGER NOT NULL,
  conference    TEXT NOT NULL,
  team_id       INTEGER NOT NULL,

  conf_rank     INTEGER NOT NULL,   -- 1 = champion, 2 = runner-up, etc.

  -- Optional metadata for traceability
  source        TEXT NOT NULL DEFAULT 'unknown',  -- e.g., 'cfbd', 'manual'
  source_detail TEXT,                             -- optional: endpoint/version/notes
  imported_at   TEXT DEFAULT (datetime('now')),

  PRIMARY KEY (season_year, conference, conf_rank),
  UNIQUE (season_year, conference, team_id),
  FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_standings_year_conf
  ON conference_standings_by_year (season_year, conference);

CREATE INDEX IF NOT EXISTS idx_standings_team
  ON conference_standings_by_year (team_id);


-- 3) Optional: Standing validation view table (bridge)
-- Not required, but helpful for comparing imported order vs computed records later.
CREATE TABLE IF NOT EXISTS conference_standings_validation (
  season_year   INTEGER NOT NULL,
  conference    TEXT NOT NULL,
  team_id       INTEGER NOT NULL,

  conf_rank     INTEGER,  -- from imported standings (nullable until imported)
  conf_wins     INTEGER,
  conf_losses   INTEGER,
  conf_games    INTEGER,
  overall_wins  INTEGER,
  overall_losses INTEGER,

  last_updated  TEXT DEFAULT (datetime('now')),

  PRIMARY KEY (season_year, conference, team_id),
  FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_standings_validation_year_conf
  ON conference_standings_validation (season_year, conference);