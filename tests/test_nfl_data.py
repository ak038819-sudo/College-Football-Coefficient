"""NFL identity, archive parsing, and the database loaders.

These drive the real modules against real archive rows. The one thing worth
saying about the fixtures: every CSV here is written with the same header the
loader requires, because a test that invents a narrower header would pass
while the real file's columns moved underneath it.
"""

from datetime import datetime

import pytest

from src.nfl import games as games_module
from src.nfl import identity, store

# The real header, in the real order, including every column the loader
# requires. A narrower header here would pass while the real file's columns
# moved underneath it, which is the whole reason REQUIRED_COLUMNS exists.
COLUMNS = ('game_id', 'season', 'game_type', 'week', 'gameday', 'weekday',
           'gametime', 'away_team', 'away_score', 'home_team', 'home_score',
           'location', 'result', 'total', 'overtime', 'away_rest',
           'home_rest', 'away_moneyline', 'home_moneyline', 'spread_line',
           'total_line', 'div_game', 'roof', 'surface', 'temp', 'wind',
           'away_qb_id', 'home_qb_id', 'away_qb_name', 'home_qb_name',
           'away_coach', 'home_coach', 'referee', 'stadium_id', 'stadium')
HEADER = ','.join(COLUMNS)

DEFAULTS = dict(weekday='Sunday', gametime='13:00', location='Home',
                total='', overtime=0, away_rest=7, home_rest=7,
                away_moneyline='', home_moneyline='', spread_line='',
                total_line='', div_game=0, roof='outdoors', surface='grass',
                temp='', wind='', away_qb_id='', home_qb_id='',
                away_qb_name='', home_qb_name='', away_coach='',
                home_coach='', referee='', stadium_id='ST01',
                stadium='Some Field')


def row(game_id, season, kind, week, day, away, away_score, home, home_score,
        *, time='13:00', location='Home', result=None, overtime=0, **extra):
    if result is None and away_score != '' and home_score != '':
        result = int(home_score) - int(away_score)
    values = dict(DEFAULTS, game_id=game_id, season=season, game_type=kind,
                  week=week, gameday=day, gametime=time, away_team=away,
                  away_score=away_score, home_team=home,
                  home_score=home_score, location=location,
                  result='' if result is None else result,
                  overtime=overtime, **extra)
    return ','.join(str(values[name]) for name in COLUMNS)


def archive(*rows):
    return '\n'.join([HEADER, *rows]) + '\n'


class TestIdentity:
    def test_a_relocated_code_resolves_to_one_franchise(self):
        assert identity.franchise_key('STL') == identity.franchise_key('LA')
        assert identity.franchise_key('SD') == identity.franchise_key('LAC')
        assert identity.franchise_key('OAK') == identity.franchise_key('LV')

    def test_every_other_code_is_its_own_franchise(self):
        for code in identity.TEAM_CODES - set(identity.RELOCATIONS):
            assert identity.franchise_code(code) == code

    def test_an_unknown_code_is_refused_rather_than_invented(self):
        with pytest.raises(ValueError):
            identity.franchise_key('XYZ')

    def test_a_retired_code_cannot_be_a_franchise_key(self):
        with pytest.raises(ValueError):
            identity.entity_key('franchise', 'STL')

    def test_a_january_game_belongs_to_the_season_that_started_in_autumn(self):
        assert identity.current_season(datetime(2027, 1, 15)) == 2026
        assert identity.current_season(datetime(2027, 2, 8)) == 2026
        assert identity.current_season(datetime(2027, 9, 10)) == 2027

    def test_the_alignment_is_not_applied_before_the_2002_realignment(self):
        assert identity.alignment('SEA', 2001) == (None, None)
        assert identity.alignment('SEA', 2002) == ('NFC', 'West')

    def test_every_franchise_has_exactly_one_division(self):
        assert set(identity.ALIGNMENT) == identity.FRANCHISE_CODES
        assert len(identity.ALIGNMENT) == 32


class TestParsing:
    def test_a_tie_is_kept_and_is_rateable(self):
        parsed = games_module.parse(archive(
            row('2026_05_NYG_WAS', 2026, 'REG', 5, '2026-10-04',
                'NYG', 20, 'WAS', 20)))
        assert parsed[0]['home_points'] == parsed[0]['away_points'] == 20
        assert parsed[0]['rateable'] == 1

    def test_an_unplayed_game_is_stored_but_not_rateable(self):
        parsed = games_module.parse(archive(
            row('2026_12_KC_BUF', 2026, 'REG', 12, '2026-11-29',
                'KC', '', 'BUF', '')))
        assert parsed[0]['home_points'] is None
        assert parsed[0]['rateable'] == 0
        assert parsed[0]['exclusions'] == ['no_final_score']

    def test_a_score_contradicting_the_stated_margin_is_refused(self):
        with pytest.raises(games_module.ArchiveError, match='contradicts'):
            games_module.parse(archive(
                row('2026_01_KC_BUF', 2026, 'REG', 1, '2026-09-13',
                    'KC', 17, 'BUF', 20, result=7)))

    def test_one_score_without_the_other_is_refused(self):
        with pytest.raises(games_module.ArchiveError, match='one score'):
            games_module.parse(archive(
                row('2026_01_KC_BUF', 2026, 'REG', 1, '2026-09-13',
                    'KC', 17, 'BUF', '', result=None)))

    def test_an_unknown_team_code_is_refused(self):
        with pytest.raises(games_module.ArchiveError, match='unknown team'):
            games_module.parse(archive(
                row('2026_01_XXX_BUF', 2026, 'REG', 1, '2026-09-13',
                    'XXX', 17, 'BUF', 20)))

    def test_a_missing_column_fails_the_whole_file(self):
        text = archive(row('2026_01_KC_BUF', 2026, 'REG', 1, '2026-09-13',
                           'KC', 17, 'BUF', 20))
        without_overtime = text.replace(',overtime', ',ot_flag', 1)
        with pytest.raises(games_module.ArchiveError, match='missing columns'):
            games_module.parse(without_overtime)

    def test_a_repeated_game_id_is_refused(self):
        line = row('2026_01_KC_BUF', 2026, 'REG', 1, '2026-09-13',
                   'KC', 17, 'BUF', 20)
        with pytest.raises(games_module.ArchiveError, match='appears twice'):
            games_module.parse(archive(line, line))

    def test_a_missing_kickoff_time_stays_missing(self):
        parsed = games_module.parse(archive(
            row('2026_01_KC_BUF', 2026, 'REG', 1, '2026-09-13',
                'KC', 17, 'BUF', 20, time='')))
        assert parsed[0]['kickoff_utc'] is None
        assert parsed[0]['kickoff_tbd'] == 1

    def test_a_kickoff_time_is_read_as_eastern_and_stored_as_utc(self):
        parsed = games_module.parse(archive(
            row('2026_01_KC_BUF', 2026, 'REG', 1, '2026-09-13',
                'KC', 17, 'BUF', 20, time='13:00')))
        # 13:00 EDT is 17:00 UTC in September.
        assert parsed[0]['kickoff_utc'] == '2026-09-13T17:00:00Z'

    def test_a_game_played_in_january_keeps_its_autumn_season(self):
        parsed = games_module.parse(archive(
            row('2026_19_KC_BUF', 2026, 'WC', 1, '2027-01-10',
                'KC', 17, 'BUF', 20)))
        assert parsed[0]['season'] == 2026
        assert parsed[0]['postseason'] == 1

    def test_games_sort_by_kickoff_not_by_week(self):
        # The postseason restarts its week numbering at 1, so a week sort puts
        # the playoff game first.
        unordered = games_module.parse(archive(
            row('2026_19_KC_BUF', 2026, 'WC', 1, '2027-01-10',
                'KC', 17, 'BUF', 20),
            row('2026_01_KC_BUF', 2026, 'REG', 1, '2026-09-13',
                'KC', 17, 'BUF', 20)))
        ordered = games_module.in_order(unordered)
        assert [game['provider_id'] for game in ordered] == [
            '2026_01_KC_BUF', '2026_19_KC_BUF']


class TestStore:
    def _connection(self, tmp_path):
        return store.connect(tmp_path / 'nfl.db')

    def test_a_relocation_is_one_franchise_with_both_codes_of_games(
            self, tmp_path):
        connection = self._connection(tmp_path)
        parsed = games_module.parse(archive(
            row('2015_01_STL_SEA', 2015, 'REG', 1, '2015-09-13',
                'STL', 17, 'SEA', 20),
            row('2016_01_LA_SEA', 2016, 'REG', 1, '2016-09-11',
                'LA', 17, 'SEA', 20)))
        store.upsert_team_stubs(connection, parsed)
        store.upsert_games(connection, parsed)
        rams = identity.franchise_key('LA')
        played = connection.execute(
            'SELECT COUNT(*) FROM game WHERE away_team_key = ?',
            (rams,)).fetchone()[0]
        assert played == 2
        codes = {r[0] for r in connection.execute(
            'SELECT away_code FROM game WHERE away_team_key = ?', (rams,))}
        assert codes == {'STL', 'LA'}

    def test_loading_twice_changes_no_row(self, tmp_path):
        connection = self._connection(tmp_path)
        parsed = games_module.parse(archive(
            row('2026_01_KC_BUF', 2026, 'REG', 1, '2026-09-13',
                'KC', 17, 'BUF', 20)))
        store.upsert_team_stubs(connection, parsed)
        store.upsert_games(connection, parsed)
        before = connection.execute('SELECT COUNT(*) FROM game').fetchone()[0]
        store.upsert_games(connection, parsed)
        after = connection.execute('SELECT COUNT(*) FROM game').fetchone()[0]
        assert before == after == 1

    def test_a_later_run_updates_a_score_that_arrived(self, tmp_path):
        connection = self._connection(tmp_path)
        scheduled = games_module.parse(archive(
            row('2026_01_KC_BUF', 2026, 'REG', 1, '2026-09-13',
                'KC', '', 'BUF', '')))
        store.upsert_team_stubs(connection, scheduled)
        store.upsert_games(connection, scheduled)
        assert connection.execute(
            'SELECT rateable FROM game').fetchone()[0] == 0
        played = games_module.parse(archive(
            row('2026_01_KC_BUF', 2026, 'REG', 1, '2026-09-13',
                'KC', 17, 'BUF', 20)))
        store.upsert_games(connection, played)
        stored = connection.execute(
            'SELECT home_points, rateable FROM game').fetchone()
        assert stored['home_points'] == 20 and stored['rateable'] == 1

    def test_the_rams_recent_location_row_is_repaired_not_shown(self,
                                                                tmp_path):
        # The source's 2026 Rams row gives `location` as "Los Angeles Rams".
        connection = self._connection(tmp_path)
        store.upsert_teams(connection, [dict(
            season='2026', team='LA', full='Los Angeles Rams',
            location='Los Angeles Rams', nickname='Rams')])
        assert connection.execute(
            'SELECT location FROM team').fetchone()[0] == 'Los Angeles'

    def test_a_correct_location_is_left_alone(self, tmp_path):
        connection = self._connection(tmp_path)
        store.upsert_teams(connection, [dict(
            season='2026', team='KC', full='Kansas City Chiefs',
            location='Kansas City', nickname='Chiefs')])
        assert connection.execute(
            'SELECT location FROM team').fetchone()[0] == 'Kansas City'

    def test_team_spans_come_from_the_games_not_the_team_file(self, tmp_path):
        connection = self._connection(tmp_path)
        parsed = games_module.parse(archive(
            row('1999_01_KC_BUF', 1999, 'REG', 1, '1999-09-12',
                'KC', 17, 'BUF', 20)))
        store.upsert_teams(connection, [dict(
            season='2002', team='KC', full='Kansas City Chiefs',
            location='Kansas City', nickname='Chiefs')])
        store.upsert_team_stubs(connection, parsed)
        store.upsert_games(connection, parsed)
        store.refresh_team_spans(connection)
        assert connection.execute(
            "SELECT first_season FROM team WHERE code = 'KC'"
        ).fetchone()[0] == 1999

    def test_a_future_schema_version_is_refused(self, tmp_path):
        path = tmp_path / 'nfl.db'
        connection = store.connect(path)
        connection.execute('UPDATE schema_version SET version = ?',
                           (store.SCHEMA_VERSION + 1,))
        connection.commit()
        connection.close()
        with pytest.raises(RuntimeError, match='schema version'):
            store.connect(path)
