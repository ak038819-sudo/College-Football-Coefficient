-- sql/views/v_games_enriched.sql
CREATE VIEW IF NOT EXISTS v_games_enriched AS
SELECT
  g.game_id,
  g.season_year,
  g.week,
  g.game_date,
  g.game_phase,
  g.is_playoff,
  g.is_nit,
  g.neutral_site,
  g.went_ot,

  g.home_team_id,
  g.away_team_id,
  g.home_score,
  g.away_score,

  hm.conference_real AS home_conference,
  am.conference_real AS away_conference,

  CASE
    WHEN hm.conference_real IS NULL OR am.conference_real IS NULL THEN NULL
    WHEN hm.conference_real != am.conference_real THEN 1 ELSE 0
  END AS is_nonconference,

  CASE
    WHEN g.home_score IS NULL OR g.away_score IS NULL THEN NULL
    WHEN g.home_score > g.away_score THEN 'home'
    WHEN g.away_score > g.home_score THEN 'away'
    ELSE 'tie'
  END AS winner,

  CASE
    WHEN g.home_score IS NULL OR g.away_score IS NULL THEN NULL
    WHEN g.went_ot = 1 AND g.home_score < g.away_score THEN 1 ELSE 0
  END AS home_ot_loss,

  CASE
    WHEN g.home_score IS NULL OR g.away_score IS NULL THEN NULL
    WHEN g.went_ot = 1 AND g.away_score < g.home_score THEN 1 ELSE 0
  END AS away_ot_loss

FROM games g
LEFT JOIN team_membership_by_season hm
  ON hm.team_id = g.home_team_id AND hm.season_year = g.season_year
LEFT JOIN team_membership_by_season am
  ON am.team_id = g.away_team_id AND am.season_year = g.season_year;