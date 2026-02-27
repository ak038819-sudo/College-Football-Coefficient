CREATE TABLE IF NOT EXISTS playoff_field_by_year (
  season_year     INTEGER NOT NULL,
  team_id         INTEGER NOT NULL,
  conference      TEXT    NOT NULL,
  conf_rank       INTEGER NOT NULL,
  conf_coe_rank   INTEGER NOT NULL,
  bid_type        TEXT    NOT NULL,
  pot             INTEGER NOT NULL,   -- 0=bye,1=pot1,2=pot2
  formula_version TEXT    NOT NULL,
  ruleset         TEXT    NOT NULL,
  created_at      TEXT    DEFAULT (datetime('now')),
  pot INTEGER NOT NULL CHECK (pot IN (0,1,2))
  CREATE UNIQUE INDEX IF NOT EXISTS uq_bracket_team_once
  ON playoff_bracket_by_year(season_year, team_id, formula_version, ruleset);
  PRIMARY KEY (season_year, team_id, formula_version, ruleset)
);