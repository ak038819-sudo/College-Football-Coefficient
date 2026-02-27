-- sql/coe_tables.sql

CREATE TABLE IF NOT EXISTS conference_coefficient_by_year (
  season_year      INTEGER NOT NULL,
  conference       TEXT NOT NULL,
  total_points     REAL NOT NULL,
  games_counted    INTEGER NOT NULL,
  points_per_game  REAL NOT NULL,
  formula_version  TEXT NOT NULL,
  PRIMARY KEY (season_year, conference, formula_version)
);

CREATE TABLE IF NOT EXISTS conference_coe_components (
  season_year      INTEGER NOT NULL,
  conference       TEXT NOT NULL,
  component        TEXT NOT NULL,   -- e.g., 'nonconf_base', 'playoff_participation', 'playoff_games'
  points           REAL NOT NULL,
  games_counted    INTEGER NOT NULL DEFAULT 0,
  formula_version  TEXT NOT NULL,
  notes            TEXT,
  PRIMARY KEY (season_year, conference, component, formula_version)
);