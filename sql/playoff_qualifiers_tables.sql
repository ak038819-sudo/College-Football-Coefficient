-- sql/playoff_qualifiers_tables.sql

CREATE TABLE IF NOT EXISTS playoff_qualifiers_by_year (
  season_year     INTEGER NOT NULL,
  conference      TEXT NOT NULL,
  team_id         INTEGER NOT NULL,

  conf_rank       INTEGER NOT NULL,
  bid_type        TEXT NOT NULL,     -- 'champion' or 'at_large'

  formula_version TEXT NOT NULL,
  ruleset         TEXT NOT NULL,     -- 'year1' or 'year2'
  created_at      TEXT DEFAULT (datetime('now')),

  PRIMARY KEY (season_year, conference, team_id, formula_version, ruleset),
  FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_playoff_qualifiers_year
  ON playoff_qualifiers_by_year (season_year);

CREATE INDEX IF NOT EXISTS idx_playoff_qualifiers_year_conf
  ON playoff_qualifiers_by_year (season_year, conference);