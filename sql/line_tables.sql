-- Closing betting lines from CFBD's /lines (src/fetch_cfbd_lines.py).
--
-- One row per game per provider. The providers disagree, and a single averaged
-- number here would hide by how much, so the choice of provider belongs to
-- whoever reads the table.
--
-- NOTHING HERE IS A RATING INPUT. The Elo and CoE builders never read this
-- table. That is the whole point: a benchmark the model cannot see is the only
-- one worth measuring against, and a market number that leaked into a rating
-- would make every comparison with it meaningless.
--
-- THE SIGN. CFBD's `spread` is the home team's handicap: NEGATIVE when the
-- home team is favoured (-7 is "home by 7"). Every margin in this database
-- runs the other way (home score minus away score), so `market_home_margin`
-- stores the market's expected margin in THIS database's direction (+7 for the
-- same line) and is the column a reader should use. `spread` is kept exactly
-- as served so the conversion can always be checked.
--
-- A line with no spread is stored with NULL spreads rather than 0: a pick'em
-- is a real line at 0, and a game nobody priced is not a game the market
-- called even. The same holds for the total and the moneylines.
CREATE TABLE IF NOT EXISTS game_lines (
    game_id            INTEGER NOT NULL,
    provider           TEXT    NOT NULL,
    season_year        INTEGER NOT NULL,
    season_type        TEXT,
    week               INTEGER,

    spread             REAL,   -- CFBD's own sign: negative = home favoured
    market_home_margin REAL,   -- home minus away, as the rest of this database counts
    spread_open        REAL,   -- CFBD's sign again, the opening number
    market_home_open   REAL,
    over_under         REAL,
    over_under_open    REAL,
    home_moneyline     INTEGER,
    away_moneyline     INTEGER,
    formatted_spread   TEXT,

    PRIMARY KEY (game_id, provider),
    FOREIGN KEY (game_id) REFERENCES games(game_id)
);

CREATE INDEX IF NOT EXISTS idx_game_lines_season ON game_lines(season_year);
CREATE INDEX IF NOT EXISTS idx_game_lines_provider ON game_lines(provider, season_year);
