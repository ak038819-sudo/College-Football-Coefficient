-- Returning production, loaded from the committed archive by
-- src/load_returning_production.py.
--
-- WHAT THIS IS. CFBD's /player/returning answers one question about a team's
-- roster before a snap is played: of everything it produced last season, how
-- much is still on the roster. `percent_*` is the share returning (0-1) and
-- `*_usage` is the share of plays those returners took.
--
-- NOT A RATING INPUT, and deliberately so. This feed was measured as an Elo
-- preseason adjustment and REJECTED: it did not predict better than the flat
-- 0.8 offseason regression already in config/model_config.json, and the
-- decision was to keep it as its own measurement rather than fold a weak
-- signal into the ratings. So the table exists to be LOOKED AT. No rating,
-- selector or Merits figure reads it, and the row below says so where a
-- reader of the schema will see it.
--
-- team_id IS NOT NULLABLE here, unlike recruiting_classes. That endpoint
-- covers all of college football and a school this database does not carry is
-- a normal answer there; this one covers FBS only, so a name that will not
-- resolve is a defect in the identity map and the loader refuses the file
-- rather than dropping the row. The provider's own string is kept anyway, so
-- a rename can always be audited against what was served.

CREATE TABLE IF NOT EXISTS team_returning_production (
    season_year            INTEGER NOT NULL,   -- the season the production returns INTO
    team_id                INTEGER NOT NULL,
    source_team            TEXT    NOT NULL,   -- CFBD's own school string, always kept
    conference             TEXT,

    -- Predicted points added by returning players, in absolute terms.
    total_ppa              REAL,
    total_passing_ppa      REAL,
    total_receiving_ppa    REAL,
    total_rushing_ppa      REAL,

    -- The same, as a SHARE of last season's production (0-1). NULL means the
    -- provider reported none, which is not the same as a team returning
    -- nothing -- that is a real 0.
    percent_ppa            REAL,
    percent_passing_ppa    REAL,
    percent_receiving_ppa  REAL,
    percent_rushing_ppa    REAL,

    -- The share of plays the returning players took (0-1).
    usage                  REAL,
    passing_usage          REAL,
    receiving_usage        REAL,
    rushing_usage          REAL,

    PRIMARY KEY (season_year, team_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_returning_season
    ON team_returning_production (season_year);
CREATE INDEX IF NOT EXISTS idx_returning_team
    ON team_returning_production (team_id, season_year);
