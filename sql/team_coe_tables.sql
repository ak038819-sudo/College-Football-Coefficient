-- sql/team_coe_tables.sql

CREATE TABLE IF NOT EXISTS team_coefficient_by_year (
  season_year      INTEGER NOT NULL,
  team_id          INTEGER NOT NULL,
  total_points     REAL NOT NULL,
  games_counted    INTEGER NOT NULL,
  points_per_game  REAL NOT NULL,
  formula_version  TEXT NOT NULL,
  PRIMARY KEY (season_year, team_id, formula_version),
  FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_team_coe_year
  ON team_coefficient_by_year (season_year, formula_version);

CREATE INDEX IF NOT EXISTS idx_team_coe_team
  ON team_coefficient_by_year (team_id);


CREATE TABLE IF NOT EXISTS team_coe_components (
  season_year     INTEGER NOT NULL,
  team_id         INTEGER NOT NULL,
  component       TEXT NOT NULL,
  points          REAL NOT NULL,
  games_counted   INTEGER NOT NULL DEFAULT 0,
  formula_version TEXT NOT NULL,
  notes           TEXT,
  PRIMARY KEY (season_year, team_id, component, formula_version),
  FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_team_components_year
  ON team_coe_components (season_year, component, formula_version);


CREATE TABLE IF NOT EXISTS team_coefficient_rolling_5yr (
  season_year         INTEGER NOT NULL,
  team_id             INTEGER NOT NULL,
  window_start_year   INTEGER NOT NULL,
  window_end_year     INTEGER NOT NULL,
  total_points_5yr    REAL NOT NULL,
  games_counted_5yr   INTEGER NOT NULL,
  points_per_game_5yr REAL NOT NULL,
  formula_version     TEXT NOT NULL,
  PRIMARY KEY (season_year, team_id, formula_version),
  FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_team_rolling_year
  ON team_coefficient_rolling_5yr (season_year, formula_version);