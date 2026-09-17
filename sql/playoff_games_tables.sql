CREATE TABLE IF NOT EXISTS playoff_games_by_year (
  season_year      INTEGER NOT NULL,
  round            TEXT    NOT NULL,   -- 'R24', 'R16', etc.
  game_no          INTEGER NOT NULL,
  home_team_id     INTEGER NOT NULL,
  away_team_id     INTEGER NOT NULL,
  home_slot        INTEGER,
  away_slot        INTEGER,
  home_pot         INTEGER,
  away_pot         INTEGER,
  home_is_host_by  TEXT,               -- e.g. 'COE'
  formula_version  TEXT    NOT NULL,
  ruleset          TEXT    NOT NULL,
  created_at       TEXT    DEFAULT (datetime('now')),
  PRIMARY KEY (season_year, round, game_no, formula_version, ruleset),
  FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
  FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
);