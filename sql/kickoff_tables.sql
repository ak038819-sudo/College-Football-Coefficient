-- Kickoff times (display only). The Elo engine orders games by games.game_date,
-- which is intentionally left unchanged; nothing in the rating pipeline reads this.
-- kickoff_utc is CFBD's start time as given; time_tbd = 1 means CFBD marked the
-- clock time unknown (its placeholder is midnight Eastern, so the date still holds).
-- date_only = 1 marks a season where CFBD has DATES but no times: every game is
-- stamped exactly 00:00:00Z without a TBD flag (1980-2000 at the time of writing).
-- Converting those to Eastern would move every game back a day, so they're never used
-- as times; the stored game_date is shown instead.
CREATE TABLE IF NOT EXISTS game_kickoffs (
    game_id      INTEGER PRIMARY KEY,
    season_year  INTEGER NOT NULL,
    kickoff_utc  TEXT    NOT NULL,
    time_tbd     INTEGER NOT NULL CHECK (time_tbd IN (0, 1)),
    date_only    INTEGER NOT NULL DEFAULT 0 CHECK (date_only IN (0, 1))
);
