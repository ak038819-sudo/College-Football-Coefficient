-- Drive-by-drive records from CFBD's /drives (src/fetch_cfbd_drives.py).
--
-- One row per drive. Two readers:
--
--   * a game page's drive list and scoring summary -- display;
--   * src/calibrate_win_probability.py -- which needs the score at a known
--     moment inside a game, and a drive's end is exactly that.
--
-- Nothing here is a rating input. The Elo and CoE builders never read this
-- table, so a gap in drive coverage can change what a game page shows and can
-- change the live win-probability fit, but can never change a rating.
--
-- Scores are stored from the HOME team's perspective, not the offense's. CFBD
-- reports running totals as offense/defense, which means the same number
-- changes meaning from drive to drive; resolving it once at load leaves no
-- reader to get it wrong. A drive whose source never said which side was home
-- keeps NULL home/away scores rather than a guess.
--
-- seconds_remaining is REGULATION seconds left at the end of the drive, so a
-- drive that ended in overtime is 0 and a drive with an unreadable clock is
-- NULL -- never 0, which is a real and different state (the end of the game).
CREATE TABLE IF NOT EXISTS game_drives (
    game_id             INTEGER NOT NULL,
    drive_number        INTEGER NOT NULL,
    season_year         INTEGER NOT NULL,
    season_type         TEXT,
    week                INTEGER,
    drive_id            INTEGER,

    offense_team_id     INTEGER,
    defense_team_id     INTEGER,
    offense_name        TEXT    NOT NULL,
    defense_name        TEXT    NOT NULL,
    is_home_offense     INTEGER CHECK (is_home_offense IN (0,1)),
    scoring             INTEGER CHECK (scoring IN (0,1)),
    drive_result        TEXT,

    start_period        INTEGER,
    start_clock         INTEGER,   -- seconds left in that period
    end_period          INTEGER,
    end_clock           INTEGER,
    seconds_remaining   INTEGER,   -- regulation seconds left at the drive's end

    start_yards_to_goal INTEGER,
    end_yards_to_goal   INTEGER,
    plays               INTEGER,
    yards               INTEGER,

    home_score_before   INTEGER,
    away_score_before   INTEGER,
    home_score_after    INTEGER,
    away_score_after    INTEGER,

    PRIMARY KEY (game_id, drive_number),
    FOREIGN KEY (offense_team_id) REFERENCES teams(team_id),
    FOREIGN KEY (defense_team_id) REFERENCES teams(team_id)
);

CREATE INDEX IF NOT EXISTS idx_game_drives_season ON game_drives(season_year);
CREATE INDEX IF NOT EXISTS idx_game_drives_scoring ON game_drives(game_id, scoring);
