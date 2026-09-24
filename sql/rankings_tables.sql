-- AP / CFP poll releases (Milestone 5). DISPLAY DATA ONLY: no rating engine
-- (Elo, CoE, hybrid, standings, playoff selection) reads this table.
-- team_id is NULL when CFBD's school name couldn't be matched to a canonical
-- team; the row is kept (with the raw name) so a poll never shows a gap.
CREATE TABLE IF NOT EXISTS poll_rankings (
    season_year        INTEGER NOT NULL,
    season_type        TEXT    NOT NULL CHECK (season_type IN ('regular','postseason')),
    week               INTEGER NOT NULL,
    poll               TEXT    NOT NULL CHECK (poll IN ('ap','cfp')),
    rank               INTEGER NOT NULL,
    team_id            INTEGER,
    school             TEXT    NOT NULL,
    first_place_votes  INTEGER,             -- NULL = not reported (e.g. CFP), not zero
    points             INTEGER,             -- NULL = not reported
    PRIMARY KEY (season_year, season_type, week, poll, school),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);
