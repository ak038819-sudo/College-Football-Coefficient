-- The conventional team box score, one row per (game, team, category),
-- loaded from CFBD by src/load_game_team_stats.py.
--
-- STORED LONG AND AS TEXT ON PURPOSE. CFBD reports every value as a string
-- whose meaning depends on its category: "417" is a number, "31:45" is a
-- clock, "6-14" is made-of-attempted and "5-45" is a count and a yardage.
-- A wide table would need a column and a parse rule invented for every
-- category the provider adds, and the invented rule would be the first thing
-- to go wrong. So the provider's own string is kept verbatim here and parsed
-- once, where it can be tested (src/team_box_score.py).
--
-- A category absent for a team-game is absent, not zero: plenty of older
-- games report only a handful of categories, and a zero there would be a
-- measurement nobody made.

CREATE TABLE IF NOT EXISTS game_team_stats (
    game_id     INTEGER NOT NULL,
    team_id     INTEGER NOT NULL,
    season_year INTEGER NOT NULL,
    category    TEXT    NOT NULL,   -- CFBD's own category name, verbatim
    stat        TEXT    NOT NULL,   -- CFBD's own value string, verbatim
    PRIMARY KEY (game_id, team_id, category),
    FOREIGN KEY (game_id) REFERENCES games(game_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_game_team_stats_season
    ON game_team_stats (season_year);
CREATE INDEX IF NOT EXISTS idx_game_team_stats_team
    ON game_team_stats (team_id, season_year);
