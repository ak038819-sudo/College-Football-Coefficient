-- sql/playoff_year1_tables.sql

-- Pot assignments for the Year 1 draw
CREATE TABLE IF NOT EXISTS playoff_pots_by_year (
  season_year     INTEGER NOT NULL,
  team_id         INTEGER NOT NULL,
  conference      TEXT NOT NULL,

  conf_rank       INTEGER NOT NULL,   -- 1 = champion, 2 = runner-up, etc (within conference)
  conf_coe_rank   INTEGER NOT NULL,   -- 1..N conference strength rank (by CoE)
  pot             INTEGER NOT NULL,   -- 0 = bye, 1 = Pot 1, 2 = Pot 2

  bid_type        TEXT NOT NULL,      -- 'champion' or 'at_large' (from qualifiers)
  formula_version TEXT NOT NULL,
  ruleset         TEXT NOT NULL,      -- 'year1'

  PRIMARY KEY (season_year, team_id, formula_version, ruleset),
  FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_playoff_pots_year
  ON playoff_pots_by_year (season_year, formula_version, ruleset);

CREATE INDEX IF NOT EXISTS idx_playoff_pots_pot
  ON playoff_pots_by_year (season_year, pot);


-- The actual draw result (ordered bracket slots)
CREATE TABLE IF NOT EXISTS playoff_bracket_year1 (
  season_year     INTEGER NOT NULL,
  slot            INTEGER NOT NULL,    -- 1..24
  team_id         INTEGER NOT NULL,
  pot             INTEGER NOT NULL,    -- 0/1/2
  formula_version TEXT NOT NULL,
  ruleset         TEXT NOT NULL,       -- 'year1'
  draw_seed       INTEGER NOT NULL,    -- the RNG seed used
  created_at      TEXT DEFAULT (datetime('now')),

  PRIMARY KEY (season_year, slot, formula_version, ruleset),
  FOREIGN KEY (team_id) REFERENCES teams(team_id)
);