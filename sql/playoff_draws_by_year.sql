CREATE TABLE IF NOT EXISTS playoff_draws_by_year (
  season_year     INTEGER NOT NULL,
  formula_version TEXT    NOT NULL,
  ruleset         TEXT    NOT NULL,
  draw_seed       INTEGER NOT NULL,
  created_at      TEXT    DEFAULT (datetime('now')),
  PRIMARY KEY (season_year, formula_version, ruleset)
);