import datetime as dt
import sqlite3

from track_hfa_shadow import settle, summary, update

UTC = dt.timezone.utc
FROZEN = {'baseline_elo_points': 50}


def forecast(kickoff='2026-10-03T18:00:00Z', recorded='2026-10-02T18:00:00Z'):
    return {'season': 2026, 'kickoff_utc': kickoff, 'recorded_at': recorded,
            'home_id': 1, 'away_id': 2, 'p_flat': .6, 'p_candidate': .65}


def final(home=1, away=2, hs=24, aws=17):
    return {'home_team_id': home, 'away_team_id': away, 'season_year': 2026,
            'home_score': hs, 'away_score': aws}


def test_shadow_scores_only_pregame_forecasts_of_same_game():
    now = dt.datetime(2026, 10, 4, tzinfo=UTC)
    rows = {'10': forecast(), '11': forecast(recorded='2026-10-03T19:00:00Z'),
            '12': forecast(), '13': forecast()}
    finals = {10: final(), 11: final(), 12: final(home=3), 13: final(hs=10, aws=20)}
    assert settle(rows, finals, now) == 2
    assert rows['10']['flat_brier'] == .16
    assert rows['10']['candidate_brier'] == .1225
    assert rows['10']['candidate_log_loss'] < rows['10']['flat_log_loss']
    assert rows['11']['invalid_reason'] == 'Forecast was not recorded before kickoff'
    assert rows['12']['invalid_reason'] == 'Game identity changed; forecast not scored'
    assert summary(rows)['games'] == 2
    assert settle(rows, finals, now) == 0


def test_finished_game_without_prior_forecast_is_never_backfilled():
    conn = sqlite3.connect(':memory:')
    conn.executescript('''
        CREATE TABLE games (game_id INTEGER, home_team_id INTEGER, away_team_id INTEGER,
                            season_year INTEGER, home_score INTEGER, away_score INTEGER);
        CREATE TABLE scheduled_games (game_id INTEGER, season_year INTEGER, kickoff_utc TEXT,
            start_time_tbd INTEGER, home_team_id INTEGER, away_team_id INTEGER, neutral_site INTEGER);
        INSERT INTO games VALUES (10,1,2,2026,24,17);
    ''')
    records = {}
    added, settled = update(conn, records, dt.datetime(2026, 10, 4, tzinfo=UTC),
                            {'elo': {'home_field': 50}}, FROZEN)
    assert (added, settled, records) == (0, 0, {})
