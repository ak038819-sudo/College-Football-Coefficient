import json
import sqlite3

from audit_game_coverage import audit


def test_coverage_distinguishes_missing_source_from_zero_stat(tmp_path):
    conn = sqlite3.connect(':memory:')
    conn.executescript('''
        CREATE TABLE games (game_id INTEGER, season_year INTEGER, home_team_id INTEGER,
                            away_team_id INTEGER, stadium_id INTEGER);
        CREATE TABLE game_team_advanced (game_id INTEGER, team_id INTEGER, off_success_rate REAL);
        INSERT INTO games VALUES (1,2003,1,2,NULL), (2,2004,1,2,100), (3,2004,3,4,NULL);
        INSERT INTO game_team_advanced VALUES (2,1,0.0), (2,2,0.4), (3,3,0.5);
    ''')
    (tmp_path / '2004.json').write_text(json.dumps({'2': [{'name': 'Alpha'}]}))
    result = audit(conn, tmp_path)['seasons']
    assert result['2003']['player_source_supported'] is False
    assert result['2004']['both_team_success_rates'] == 1  # 0.0 is a real reported value
    assert result['2004']['verified_venue_id'] == 1
    assert result['2004']['player_boxscores'] == 1
