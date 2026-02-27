CREATE TABLE IF NOT EXISTS playoff_bracket_by_year (
  season_year     INTEGER NOT NULL,
  slot            INTEGER NOT NULL,   -- 1..24 draw slots
  team_id         INTEGER NOT NULL,
  pot             INTEGER NOT NULL,   -- 0/1/2
  formula_version TEXT    NOT NULL,
  ruleset         TEXT    NOT NULL,
  draw_seed       INTEGER NOT NULL,
  created_at      TEXT    DEFAULT (datetime('now')),
  pot INTEGER NOT NULL CHECK (pot IN (0,1,2))
  PRIMARY KEY (season_year, slot, formula_version, ruleset)
  CREATE UNIQUE INDEX IF NOT EXISTS uq_bracket_team_once
  ON playoff_bracket_by_year(season_year, team_id, formula_version, ruleset);
);

CREATE INDEX IF NOT EXISTS idx_pby_team
ON playoff_bracket_by_year(season_year, team_id, formula_version, ruleset);