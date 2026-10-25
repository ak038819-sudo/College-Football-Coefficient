-- NFL schema. Independent of db/league.db and db/ncaambb.db in every respect:
-- its own file, its own tables, its own identity scheme. No football or
-- basketball id is joinable to anything here.
--
-- The one structure worth reading twice is the split between `team` (a
-- FRANCHISE, which is what carries a rating) and `team_code` (what the source
-- called that franchise in a given season). Three franchises changed code
-- mid-archive; without this split they would be six teams and three of them
-- would restart at 1500 on the day they moved.

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshot (
    snapshot_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    kind            TEXT    NOT NULL,
    season          INTEGER,
    retrieved_at    TEXT    NOT NULL,
    source          TEXT,
    source_revision TEXT,
    rows_seen       INTEGER NOT NULL DEFAULT 0,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS team (
    team_key      TEXT PRIMARY KEY,
    code          TEXT NOT NULL UNIQUE,
    name          TEXT NOT NULL,
    location      TEXT,
    nickname      TEXT,
    conference    TEXT,
    division      TEXT,
    first_season  INTEGER,
    last_season   INTEGER,
    snapshot_id   INTEGER REFERENCES snapshot(snapshot_id)
);

-- What the source called a franchise in one season, and the name it used.
-- A row here for 2015 says `STL`; the franchise it points at is `LA`.
CREATE TABLE IF NOT EXISTS team_code (
    code        TEXT    NOT NULL,
    season      INTEGER NOT NULL,
    team_key    TEXT    NOT NULL REFERENCES team(team_key),
    name        TEXT,
    location    TEXT,
    nickname    TEXT,
    conference  TEXT,
    division    TEXT,
    snapshot_id INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (code, season)
);

CREATE INDEX IF NOT EXISTS team_code_by_team ON team_code(team_key, season);

CREATE TABLE IF NOT EXISTS game (
    game_key      TEXT    PRIMARY KEY,
    provider_id   TEXT    NOT NULL UNIQUE,
    season        INTEGER NOT NULL,
    season_label  TEXT    NOT NULL,
    season_type   TEXT    NOT NULL,
    postseason    INTEGER NOT NULL,
    week          INTEGER,
    game_date     TEXT    NOT NULL,
    -- NULL when the source gives no kickoff time. Never midnight.
    kickoff_utc   TEXT,
    kickoff_tbd   INTEGER NOT NULL DEFAULT 0,
    neutral_site  INTEGER NOT NULL DEFAULT 0,
    -- The code the source used, kept so a 2015 game still reads STL.
    home_code     TEXT    NOT NULL,
    away_code     TEXT    NOT NULL,
    -- The franchise, which is what a rating follows.
    home_team_key TEXT    NOT NULL REFERENCES team(team_key),
    away_team_key TEXT    NOT NULL REFERENCES team(team_key),
    -- NULL means unplayed. Never 0.
    home_points   INTEGER,
    away_points   INTEGER,
    overtime      INTEGER,
    divisional_game INTEGER,
    roof          TEXT,
    surface       TEXT,
    stadium_id    TEXT,
    stadium       TEXT,
    -- Everything below was already in the archive and already being fetched;
    -- schema 1 simply threw it away on the floor of the loader. A blank in
    -- any of these is a real absence and is stored NULL: the betting lines
    -- begin in 1999 but the moneylines only become near-complete in 2007, a
    -- temperature is recorded for outdoor games only, and the source leaves
    -- the referee blank for the games it has not been told about yet.
    home_coach      TEXT,
    away_coach      TEXT,
    -- The home team's expected margin, positive when the home team is
    -- favoured. Measured, not assumed: over the 7,369 games carrying both a
    -- line and a final, mean(result - spread_line) is 0.07 and the flipped
    -- sign gives 4.56, so this is the convention the archive uses.
    spread_line     REAL,
    total_line      REAL,
    home_moneyline  INTEGER,
    away_moneyline  INTEGER,
    home_qb_id      TEXT,
    away_qb_id      TEXT,
    home_qb_name    TEXT,
    away_qb_name    TEXT,
    referee         TEXT,
    -- Days since each team last played. 7 for most games; 4 for a Thursday
    -- after a Sunday, 14 after a bye.
    home_rest       INTEGER,
    away_rest       INTEGER,
    -- Fahrenheit, and miles per hour. Recorded for outdoor games; NULL under
    -- a roof is the absence of a measurement, not a mild day.
    temp            INTEGER,
    wind            INTEGER,
    rateable      INTEGER NOT NULL,
    exclusions    TEXT    NOT NULL,
    snapshot_id   INTEGER REFERENCES snapshot(snapshot_id)
);

CREATE INDEX IF NOT EXISTS game_by_season ON game(season, game_date);
CREATE INDEX IF NOT EXISTS game_by_home ON game(home_team_key, season);
CREATE INDEX IF NOT EXISTS game_by_away ON game(away_team_key, season);
CREATE INDEX IF NOT EXISTS game_rateable ON game(season, rateable);

-- OUR ratings, and only ours, stamped with the exact configuration that made
-- them and what that configuration scored on a season it had never seen.
CREATE TABLE IF NOT EXISTS rating_model (
    model_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT    NOT NULL UNIQUE,
    config        TEXT    NOT NULL,
    fitted_on     TEXT,
    held_out      TEXT,
    holdout_brier REAL,
    holdout_games INTEGER,
    created_at    TEXT    NOT NULL,
    snapshot_id   INTEGER REFERENCES snapshot(snapshot_id)
);

CREATE TABLE IF NOT EXISTS team_rating (
    team_key      TEXT    NOT NULL REFERENCES team(team_key),
    season        INTEGER NOT NULL,
    model_id      INTEGER NOT NULL REFERENCES rating_model(model_id),
    rating        REAL    NOT NULL,
    games_counted INTEGER NOT NULL,
    updated_at    TEXT    NOT NULL,
    PRIMARY KEY (team_key, season, model_id)
);

CREATE INDEX IF NOT EXISTS rating_by_season ON team_rating(season, model_id);

-- The per-game chain: each game's postgame rating is that team's pregame
-- rating in its next game, which is what makes a replay auditable.
-- `counted = 0` is a game the model declined to learn from, and its change is
-- 0 rather than a half-win.
CREATE TABLE IF NOT EXISTS rating_game (
    game_key      TEXT    NOT NULL REFERENCES game(game_key),
    model_id      INTEGER NOT NULL REFERENCES rating_model(model_id),
    home_pregame  REAL    NOT NULL,
    away_pregame  REAL    NOT NULL,
    home_expected REAL    NOT NULL,
    home_postgame REAL    NOT NULL,
    away_postgame REAL    NOT NULL,
    rating_change REAL    NOT NULL,
    multiplier    REAL,
    counted       INTEGER NOT NULL,
    PRIMARY KEY (game_key, model_id)
);

-- Head coaches, and what their teams did against what the model expected.
--
-- A coach is keyed by the source's NAME, because the archive numbers nothing
-- but games: `nfl:nflverse:coach:Andy Reid`. Two head coaches who shared a
-- name would therefore share a row, and nothing in this data could tell them
-- apart; 177 distinct names appear across 1999-2026 and none of them repeat
-- in a way this data can see.
--
-- One row per coach PER SEASON PER TEAM. That shape is what carries a
-- midseason change (two coaches, one team, one season) and a coach who moves
-- (one coach, one season, two teams) without either being averaged away.
--
-- The college side could not do this: CFBD never says whose games were whose,
-- so 361 of its 5,657 tenures are unmeasured. The NFL archive names the home
-- and away coach ON EVERY GAME, all 7,548 of them, so every game here is
-- attributed to the person who actually coached it.
CREATE TABLE IF NOT EXISTS coach_season (
    coach_key        TEXT    NOT NULL,
    coach_name       TEXT    NOT NULL,
    season           INTEGER NOT NULL,
    team_key         TEXT    NOT NULL REFERENCES team(team_key),
    model_id         INTEGER NOT NULL REFERENCES rating_model(model_id),
    games            INTEGER NOT NULL,
    wins             INTEGER NOT NULL,
    losses           INTEGER NOT NULL,
    ties             INTEGER NOT NULL,
    postseason_games INTEGER NOT NULL,
    postseason_wins  INTEGER NOT NULL,
    -- The sum of the model's pregame probability that this team won each of
    -- those games. A tie counts half a win on both sides of the comparison.
    expected_wins    REAL    NOT NULL,
    wins_above       REAL    NOT NULL,
    -- Rating gained over the games this coach coached, from the stored chain.
    elo_change       REAL    NOT NULL,
    PRIMARY KEY (coach_key, season, team_key, model_id)
);

CREATE INDEX IF NOT EXISTS coach_by_season ON coach_season(season, model_id);

-- The model against the betting market, season by season.
--
-- The market's probability here is the DE-VIGGED MONEYLINE, not a spread put
-- through a curve. A spread only becomes a probability by way of a model, and
-- comparing our model with our own model of the market would measure nothing.
-- Moneylines cover 72% of the archive overall and 90%+ from 2007, so a season
-- states how many games its comparison rests on.
CREATE TABLE IF NOT EXISTS market_season (
    season             INTEGER NOT NULL,
    model_id           INTEGER NOT NULL REFERENCES rating_model(model_id),
    -- Games with a final score and a stored pregame expectation.
    rated_games        INTEGER NOT NULL,
    model_brier        REAL,
    model_accuracy     REAL,
    -- Games that ALSO carry both moneylines. Smaller, and the only games the
    -- two Brier scores below may be compared on.
    priced_games       INTEGER NOT NULL,
    model_brier_priced REAL,
    market_brier       REAL,
    market_accuracy    REAL,
    -- Games carrying a spread, and how the model's predicted margin did
    -- against it. A pick landing exactly on the line is no pick.
    spread_games       INTEGER NOT NULL,
    ats_wins           INTEGER NOT NULL,
    ats_losses         INTEGER NOT NULL,
    ats_pushes         INTEGER NOT NULL,
    -- Mean absolute error in points, the model's and the market's, on the
    -- same games.
    model_margin_mae   REAL,
    market_margin_mae  REAL,
    PRIMARY KEY (season, model_id)
);

-- ---------------------------------------------------------------------------
-- The people layer (schema 3).
--
-- A player is keyed by GSIS id: `nfl:nflverse:player:00-0019596`. That is the
-- one id in this data that is both stable and nearly complete — all 24,856
-- rows of the source's player file carry one and none of them repeats — and
-- unlike the college side's athlete ids it is not split across two numbers for
-- one career.
--
-- The archive is DEEPER than the college people archive: rosters and season
-- statistics go back to 1999 here, against 2009 there.
--
-- NOT STORED: the `headshot` URL the player file carries. A portrait is a
-- licensing question rather than a data one, and the college side's portraits
-- are blocked for exactly that reason. Storing the URL would make it one
-- commit away from being published.
CREATE TABLE IF NOT EXISTS player (
    player_key        TEXT PRIMARY KEY,
    gsis_id           TEXT NOT NULL UNIQUE,
    -- Pro-Football-Reference's id, and the ONLY way a snap-count row reaches
    -- a player: that file carries no GSIS id. Present for 91.3% of players.
    pfr_id            TEXT,
    display_name      TEXT NOT NULL,
    first_name        TEXT,
    last_name         TEXT,
    position          TEXT,
    position_group    TEXT,
    -- Inches and pounds, as the source gives them.
    height            INTEGER,
    weight            INTEGER,
    birth_date        TEXT,
    college           TEXT,
    college_conference TEXT,
    jersey_number     INTEGER,
    rookie_season     INTEGER,
    last_season       INTEGER,
    latest_team       TEXT REFERENCES team(team_key),
    -- The source's roster status AS OF THE SNAPSHOT (ACT, RES, CUT, DEV...).
    -- It is not history: a player cut last week reads CUT for every season.
    status            TEXT,
    years_experience  INTEGER,
    draft_season      INTEGER,
    draft_round       INTEGER,
    draft_pick        INTEGER,
    draft_team        TEXT REFERENCES team(team_key),
    -- 1 where this row was built from a roster or statistics row rather than
    -- from the player file, because that file does not carry the id. 1,789 of
    -- the 69,533 roster rows and 9 statistics rows are in that position;
    -- dropping them would lose real seasons of real players, and pretending
    -- the player file carried them would be a lie about where the name came
    -- from.
    stub              INTEGER NOT NULL DEFAULT 0,
    snapshot_id       INTEGER REFERENCES snapshot(snapshot_id)
);

CREATE INDEX IF NOT EXISTS player_by_name ON player(display_name);
CREATE INDEX IF NOT EXISTS player_by_span ON player(last_season, rookie_season);

-- One row per player per season per team. A player who was traded in October
-- has two rows for that season, which is what the source's two rows say and
-- what a career table has to show; averaging them into one would put him on
-- neither team.
CREATE TABLE IF NOT EXISTS player_season (
    player_key       TEXT    NOT NULL REFERENCES player(player_key),
    season           INTEGER NOT NULL,
    team_key         TEXT    NOT NULL REFERENCES team(team_key),
    -- What the source called the team, before the alias and the relocation
    -- were resolved: a 2015 row really does say SL.
    code             TEXT    NOT NULL,
    position         TEXT,
    depth_position   TEXT,
    jersey_number    INTEGER,
    status           TEXT,
    status_label     TEXT,
    height           INTEGER,
    weight           INTEGER,
    years_experience INTEGER,
    -- The last week this player was on the roster, and in which round. The
    -- source's roster file is a snapshot of the end of the season, not a
    -- week-by-week history, so this is the only time information in it.
    last_week        INTEGER,
    last_game_type   TEXT,
    entry_season     INTEGER,
    rookie_season    INTEGER,
    snapshot_id      INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (player_key, season, team_key)
);

CREATE INDEX IF NOT EXISTS player_season_by_team ON player_season(team_key, season);
CREATE INDEX IF NOT EXISTS player_season_by_season ON player_season(season);

-- The context of one statistics row: who, when, which half of the season, and
-- the team the source attributes it to.
--
-- `season_type` is 'reg' or 'post' and they are NEVER summed together. A
-- postseason line is four games against the best teams in the league; adding
-- it to a sixteen-game regular season produces a number that is neither.
CREATE TABLE IF NOT EXISTS player_stat_season (
    player_key  TEXT    NOT NULL REFERENCES player(player_key),
    season      INTEGER NOT NULL,
    season_type TEXT    NOT NULL,
    team_key    TEXT REFERENCES team(team_key),
    code        TEXT,
    position    TEXT,
    games       INTEGER,
    snapshot_id INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (player_key, season, season_type)
);

CREATE INDEX IF NOT EXISTS player_stat_season_by_season
    ON player_stat_season(season, season_type);

-- Statistics in LONG form: one row per player per season per half per stat.
--
-- Long rather than a 148-column table because the source's column set GREW
-- between the release this was first built against and the one it is built
-- against now — explosive-play counts, punting and blocked-kick columns all
-- appeared — and a wide table turns every such addition into a migration. A
-- long table also lets a zero stay absent: 775,102 of the 9.3 million cells
-- in the archive are non-zero, so storing the zeros would be twelve times the
-- rows to say nothing.
--
-- A zero is therefore NOT STORED, and a missing row means "none", which for a
-- counting stat is the same statement. It is not the same statement for a
-- RATE (fg_pct, target_share, passing_cpoe), where a genuine zero is a
-- measurement — but a rate of exactly zero arises only from components that
-- are themselves zero, so the row it belongs to carries no components either
-- and the page shows nothing rather than 0.0%.
CREATE TABLE IF NOT EXISTS player_stat (
    player_key  TEXT    NOT NULL REFERENCES player(player_key),
    season      INTEGER NOT NULL,
    season_type TEXT    NOT NULL,
    stat        TEXT    NOT NULL,
    value       REAL    NOT NULL,
    PRIMARY KEY (player_key, season, season_type, stat)
);

CREATE INDEX IF NOT EXISTS player_stat_by_stat
    ON player_stat(stat, season, season_type, value);

-- The draft, which is this league's recruiting analogue: the college side
-- stores 81,262 recruits with hometowns, and the NFL's equivalent question is
-- who a team spent a pick on and what became of him.
--
-- 1999 onwards only, to match the rest of the archive. The source carries
-- 1980-2026; a pick from 1983 cannot be joined to a game, a rating or a
-- roster here, so storing it would add a table nothing else can reach.
--
-- `player_key` is NULL for the 313 of 7,154 modern picks the source gives no
-- GSIS id for — almost all of them players who never appeared in a game.
-- The pick is still real and still stored; it simply has no page.
CREATE TABLE IF NOT EXISTS draft_pick (
    season          INTEGER NOT NULL,
    round           INTEGER NOT NULL,
    pick            INTEGER NOT NULL,
    team_key        TEXT    NOT NULL REFERENCES team(team_key),
    code            TEXT    NOT NULL,
    player_key      TEXT REFERENCES player(player_key),
    name            TEXT    NOT NULL,
    position        TEXT,
    category        TEXT,
    side            TEXT,
    college         TEXT,
    age             REAL,
    -- The source's `to`: the last season this player played, from Pro
    -- Football Reference rather than from this archive.
    final_season    INTEGER,
    hall_of_fame    INTEGER,
    all_pro         INTEGER,
    pro_bowls       INTEGER,
    seasons_started INTEGER,
    games           INTEGER,
    -- Pro Football Reference's Approximate Value: weighted career, career
    -- total, and the value earned for the team that drafted him.
    weighted_av     INTEGER,
    career_av       INTEGER,
    draft_av        INTEGER,
    snapshot_id     INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (season, round, pick)
);

CREATE INDEX IF NOT EXISTS draft_by_season ON draft_pick(season, round, pick);
CREATE INDEX IF NOT EXISTS draft_by_team ON draft_pick(team_key, season);
CREATE INDEX IF NOT EXISTS draft_by_player ON draft_pick(player_key);

-- ---------------------------------------------------------------------------
-- The game layer (schema 4): what happened inside a game.
--
-- Reduced from the play-by-play and never the play-by-play itself. 2024's
-- 49,492 plays become 6,148 drives and 3,767 scoring plays, which is the same
-- bargain the college side struck in storing 441,307 drives rather than the
-- plays behind them. `src/nfl/pbp_source.py` says why at length.
--
-- A drive is keyed by the source's `fixed_drive`, which is its corrected
-- drive number, not the raw `drive` column.
CREATE TABLE IF NOT EXISTS game_drive (
    game_key        TEXT    NOT NULL REFERENCES game(game_key),
    drive           INTEGER NOT NULL,
    -- The franchise with the ball. NULL for the handful of drives the source
    -- records with no possessing team (2 of 6,148 in 2024, both artefacts at
    -- the end of a half); the drive is kept because dropping it would leave a
    -- gap in a numbered sequence the page reads in order.
    team_key        TEXT REFERENCES team(team_key),
    code            TEXT,
    quarter_start   INTEGER,
    quarter_end     INTEGER,
    -- Game clock as the source writes it, "07:47". Kept as text because it is
    -- what the page prints; `seconds_left` is the number anything computes on.
    clock_start     TEXT,
    clock_end       TEXT,
    -- Regulation seconds left when the drive began. Overtime is 0.
    seconds_left    INTEGER,
    start_yard_line TEXT,
    end_yard_line   TEXT,
    plays           INTEGER,
    yards           INTEGER,
    time_of_possession TEXT,
    first_downs     INTEGER,
    inside_twenty   INTEGER,
    yards_penalized INTEGER,
    -- 'Touchdown', 'Punt', 'Field goal', 'Turnover', 'Turnover on downs',
    -- 'End of half', 'Missed field goal', 'Opp touchdown', 'Safety'. The
    -- source's own words, not ours.
    result          TEXT,
    start_transition TEXT,
    end_transition  TEXT,
    ended_with_score INTEGER,
    -- The score BEFORE the drive, which is what a win-probability curve needs
    -- at each point and what a drive chart shows beside it.
    home_score      INTEGER,
    away_score      INTEGER,
    snapshot_id     INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (game_key, drive)
);

CREATE INDEX IF NOT EXISTS drive_by_game ON game_drive(game_key, drive);
CREATE INDEX IF NOT EXISTS drive_by_team ON game_drive(team_key);

-- Every play that put points on the board, which is what a scoring summary
-- is. The source marks them with `sp = 1`, and all 3,767 of 2024's classify
-- into exactly six kinds with none left over.
CREATE TABLE IF NOT EXISTS scoring_play (
    game_key      TEXT    NOT NULL REFERENCES game(game_key),
    play_id       INTEGER NOT NULL,
    quarter       INTEGER,
    clock         TEXT,
    seconds_left  INTEGER,
    -- The team that SCORED, which is not always the team with the ball: a
    -- pick six is scored by the defence, and `Opp touchdown` is the drive
    -- result that goes with it.
    team_key      TEXT REFERENCES team(team_key),
    code          TEXT,
    -- 'TD', 'FG', 'XP', '2PT', 'SAFETY', 'D2PT'.
    kind          TEXT    NOT NULL,
    points        INTEGER NOT NULL,
    scorer        TEXT,
    scorer_id     TEXT,
    kick_distance INTEGER,
    -- The source's own description of the play, with its leading clock
    -- duplicate stripped. Not rewritten: a summary that paraphrases a play is
    -- a summary that can be wrong about it.
    description   TEXT,
    home_score    INTEGER,
    away_score    INTEGER,
    snapshot_id   INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (game_key, play_id)
);

CREATE INDEX IF NOT EXISTS scoring_by_game ON scoring_play(game_key, play_id);

-- Schema 5: how much a player was on the field, and whether he was fit to be.
--
-- One row per player per game, from the `snap_counts` release.
--
-- **THE SNAP FILE CARRIES NO GSIS ID.** Its only person id is
-- `pfr_player_id`, Pro-Football-Reference's (`BrowSp00`), so a snap row
-- reaches a player page only through `player.pfr_id`, which the player file
-- gives for 91.3% of players and which resolves 99.93% of snap rows. The 246
-- rows it does not resolve, across 29 people, are not stored: a snap count
-- with no player is a number with nobody to show it to.
--
-- 2013 ONWARDS. The release has a `snap_counts_2012.csv` and it holds a
-- header and nothing else — published and empty, which is not the same as
-- absent and must not read as a fetch failure.
CREATE TABLE IF NOT EXISTS player_snap (
    player_key     TEXT    NOT NULL REFERENCES player(player_key),
    game_key       TEXT    NOT NULL REFERENCES game(game_key),
    team_key       TEXT REFERENCES team(team_key),
    -- The position he played THAT WEEK, which is not always the one on his
    -- player row: a fullback asked to play tight end is listed as he played.
    position       TEXT,
    offense_snaps  INTEGER,
    -- The source's share of the team's snaps, 0-1. Stored as it is given
    -- rather than recomputed: the denominator is the team's snap total for
    -- that game, which this archive does not hold.
    offense_pct    REAL,
    defense_snaps  INTEGER,
    defense_pct    REAL,
    st_snaps       INTEGER,
    st_pct         REAL,
    snapshot_id    INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (player_key, game_key)
);

CREATE INDEX IF NOT EXISTS snap_by_game ON player_snap(game_key);
CREATE INDEX IF NOT EXISTS snap_by_player ON player_snap(player_key);

-- The weekly injury report, 2009 onwards, from the `injuries` release.
--
-- Keyed on the WEEK and not on the game, and `game_key` is nullable, because
-- a report is filed before a game and a game can fail to happen: both Buffalo
-- and Cincinnati filed for week 17 of 2022, the game was abandoned after
-- Damar Hamlin's cardiac arrest and never played, and those 17 rows are the
-- only 17 of 92,890 that reach no game. They are real reports and they are
-- kept.
--
-- **THE SOURCE REPEATS A PLAYER WHEN THE REPORT IS UPDATED.** Two players in
-- week 15 of 2024 appear twice, Questionable on the Saturday and Out on the
-- Sunday morning. The LATEST `date_modified` wins, because the last thing the
-- club said before kickoff is what the report said.
--
-- `report_status` is BLANK for a third of the archive and the blank means
-- different things either side of 2016: the league abolished "Probable" after
-- the 2015 season, so a player who would have been listed Probable now
-- carries a practice line and no game status at all. Blanks run about 150 a
-- season through 2015 and about 3,000 a season after. Counting "listed on the
-- report" across that boundary compares two different reports.
CREATE TABLE IF NOT EXISTS injury_report (
    season          INTEGER NOT NULL,
    season_type     TEXT    NOT NULL,
    week            INTEGER NOT NULL,
    team_key        TEXT    NOT NULL REFERENCES team(team_key),
    player_key      TEXT    NOT NULL REFERENCES player(player_key),
    game_key        TEXT REFERENCES game(game_key),
    position        TEXT,
    report_primary   TEXT,
    report_secondary TEXT,
    -- Out, Doubtful, Questionable, Probable (to 2015) or NULL.
    report_status    TEXT,
    practice_primary   TEXT,
    practice_secondary TEXT,
    practice_status    TEXT,
    reported_at     TEXT,
    snapshot_id     INTEGER REFERENCES snapshot(snapshot_id),
    PRIMARY KEY (season, season_type, week, team_key, player_key)
);

CREATE INDEX IF NOT EXISTS injury_by_game ON injury_report(game_key);
CREATE INDEX IF NOT EXISTS injury_by_player ON injury_report(player_key, season);
