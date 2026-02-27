CREATE TABLE IF NOT EXISTS conference_coefficient_rolling_5yr (
  season_year        INTEGER NOT NULL,
  conference         TEXT NOT NULL,
  window_start_year  INTEGER NOT NULL,
  window_end_year    INTEGER NOT NULL,
  total_points_5yr   REAL NOT NULL,
  games_counted_5yr  INTEGER NOT NULL,
  points_per_game_5yr REAL NOT NULL,
  formula_version    TEXT NOT NULL,
  PRIMARY KEY (season_year, conference, formula_version)
);