-- CoE 2.0 season rollups (ENG-13) and conference five-season aggregate (ENG-14).
--
-- Additive, like the rest of the CoE 2.0 layer: nothing here is read by
-- build_coefficients.py (CoE v1) or by the live playoff selection, which still
-- runs entirely on v1. formula_version tags every row so a v2 output can be
-- compared against, and superseded by, a later one without ambiguity.

-- A team's CoE 2.0 season total, decomposed. The invariant this table exists to
-- make checkable is:
--     season_coe2 = game_coe_total + bonus_total
-- Both parts are stored rather than just the total, so a ledger can show where
-- every point came from and no bonus can hide inside an opponent-strength term.
CREATE TABLE IF NOT EXISTS team_coe2_by_season (
    team_id         INTEGER NOT NULL,
    season_year     INTEGER NOT NULL,
    game_coe_total  REAL    NOT NULL,   -- sum of that season's Game CoE 2.0 awards
    bonus_total     REAL    NOT NULL,   -- sum of team_coe2_bonuses.points for the season
    season_coe2     REAL    NOT NULL,   -- game_coe_total + bonus_total
    games_counted   INTEGER NOT NULL,
    formula_version TEXT    NOT NULL,
    PRIMARY KEY (team_id, season_year, formula_version)
);

-- One row per bonus actually awarded: the provenance behind bonus_total.
-- `category` names the rule, `count` how many times it applied, `points` the
-- resulting contribution, and `detail` a human-readable reason. A season with no
-- postseason has no rows here at all, which is why the season total must still
-- reconcile when bonus_total is 0.
--
-- Bonuses are derived from REAL results only -- bowl/CFP participation from the
-- games table, conference titles from the derived standings, national titles
-- from the hand-maintained reference list. None of them are inferred from
-- ratings, and none feed back into opponent strength.
CREATE TABLE IF NOT EXISTS team_coe2_bonuses (
    team_id         INTEGER NOT NULL,
    season_year     INTEGER NOT NULL,
    category        TEXT    NOT NULL,
    count           INTEGER NOT NULL,
    points          REAL    NOT NULL,
    detail          TEXT,
    formula_version TEXT    NOT NULL,
    PRIMARY KEY (team_id, season_year, category, formula_version)
);

-- Conference CoE 2.0 over the five seasons BEFORE season_year -- the entering
-- season value, frozen, never including season_year itself. Deliberately a
-- different window from CoE v1's conference_coefficient_rolling_5yr, which
-- INCLUDES the current season because it feeds current-season bid allocation.
-- Keeping the two windows in separate tables with different names is what stops
-- them being mistaken for each other.
CREATE TABLE IF NOT EXISTS conference_coe2_5yr_by_season (
    season_year       INTEGER NOT NULL,
    conference        TEXT    NOT NULL,
    coe2_5yr          REAL    NOT NULL,
    window_start_year INTEGER NOT NULL,
    window_end_year   INTEGER NOT NULL,   -- always season_year - 1
    seasons_counted   INTEGER NOT NULL,
    external_games    INTEGER NOT NULL,   -- contributing games across the window
    formula_version   TEXT    NOT NULL,
    PRIMARY KEY (season_year, conference, formula_version)
);
