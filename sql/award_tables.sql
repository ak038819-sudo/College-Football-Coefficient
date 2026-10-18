-- National award winners (the Heisman, the position awards, the player- and
-- coach-of-the-year honours), from ESPN's awards endpoint.
--
-- The first feed in this project that is not CollegeFootballData: CFBD has no
-- awards endpoint at all. Stored in its own tables for the same reason the
-- advanced stats are: this is DISPLAY DATA. Nothing in Elo, CoE, the hybrid
-- layer, the playoff model or any prediction reads it, and an award is a vote
-- taken after the fact, so feeding it back into a rating would be circular.

-- One row per award, not per award-season: ESPN numbers an award the same way
-- every year, but the key here is derived from the NAME so a renumbering on
-- their side cannot split one trophy into two.
CREATE TABLE IF NOT EXISTS awards (
    award_key      TEXT PRIMARY KEY,   -- normalized name, e.g. 'heisman-trophy'
    name           TEXT NOT NULL,
    description    TEXT,
    espn_award_id  TEXT,               -- provenance; not the key, see above
    -- What kind of thing this trophy is: 'player' (an FBS player's award, the
    -- only kind a player page shows), 'coach', 'non_fbs' (the FCS, Division II
    -- and Division III player awards), 'weekly' (a week's honour, a different
    -- grain from everything else here) or 'other'. ESPN serves all five from
    -- one endpoint, so without this a coach-of-the-year winner reads as a
    -- player this project failed to identify.
    category       TEXT NOT NULL DEFAULT 'player',
    first_season   INTEGER,            -- first season this project HOLDS, not the
    last_season    INTEGER             -- award's own history
);

-- One row per winner. The grain is (season, award, winner) because a handful of
-- awards are shared in a year, and a co-winner is a winner.
--
-- player_id is NULL when no roster row in this project identifies the person.
-- That is a normal outcome, not a failure: the roster archive begins in 2009,
-- so every winner before then is a name with no page to link to, and the page
-- says the name. `resolution` records WHICH of those happened, so an unlinked
-- winner can never be mistaken for a resolved one.
CREATE TABLE IF NOT EXISTS player_awards (
    season_year      INTEGER NOT NULL,
    award_key        TEXT    NOT NULL,
    winner_name      TEXT    NOT NULL,   -- as ESPN spelled it
    source_team      TEXT,               -- the school as ESPN spelled it
    position         TEXT,
    team_id          INTEGER,            -- resolved through the project's aliases
    player_id        INTEGER,            -- NULL when nothing identifies them
    espn_athlete_id  TEXT,
    espn_team_id     TEXT,
    resolution       TEXT    NOT NULL,   -- 'espn id', 'context', or why not
    PRIMARY KEY (season_year, award_key, winner_name),
    FOREIGN KEY (award_key) REFERENCES awards(award_key),
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    FOREIGN KEY (player_id) REFERENCES players(player_id)
);

CREATE INDEX IF NOT EXISTS idx_player_awards_player ON player_awards(player_id);
CREATE INDEX IF NOT EXISTS idx_player_awards_season ON player_awards(season_year);
CREATE INDEX IF NOT EXISTS idx_player_awards_team ON player_awards(team_id, season_year);
