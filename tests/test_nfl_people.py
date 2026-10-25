"""The NFL people layer: identity, parsing, storing, exporting, gating.

Every number in a fixture here is small and made up; every RULE is one that was
measured against the real archive on 2026-10-21 and is described where the
code applies it.
"""

import json
import urllib.error
from datetime import datetime, timezone

import pytest

from src.nfl import (identity, people, people_export, people_source, publish,
                     publish_gate, store)

KC, LA, SF = ('nfl:nflverse:franchise:KC', 'nfl:nflverse:franchise:LA',
              'nfl:nflverse:franchise:SF')


def team_rows():
    return [dict(season=2015, team=code, full=code, location=None,
                 nickname=None)
            for code in ('KC', 'STL', 'SF', 'NE', 'TB', 'GB', 'ARI', 'LA')]


@pytest.fixture
def db():
    connection = store.connect(':memory:')
    store.upsert_teams(connection, team_rows())
    yield connection
    connection.close()


def player_row(gsis, name, **extra):
    row = dict(gsis_id=gsis, display_name=name, first_name='', last_name='',
               position='QB', position_group='QB', height='75', weight='225',
               birth_date='1980-01-01', college_name='Somewhere',
               college_conference='', jersey_number='12', rookie_season='2005',
               last_season='2020', latest_team='KC', status='RET',
               years_of_experience='15', draft_year='2005', draft_round='1',
               draft_pick='3', draft_team='KAN')
    row.update(extra)
    return row


def roster_row(gsis, team, name='Someone', **extra):
    row = dict(gsis_id=gsis, team=team, full_name=name, position='WR',
               depth_chart_position='WR', jersey_number='81', status='ACT',
               status_description_abbr='A01', height='72', weight='200',
               birth_date='1990-01-01', college='College', years_exp='3',
               week='18', game_type='REG', entry_year='2012',
               rookie_year='2012')
    row.update(extra)
    return row


def stat_row(gsis, team='KC', **stats):
    row = dict(player_id=gsis, player_name='S.Omeone',
               player_display_name='Some One', position='WR',
               position_group='WR', headshot_url='http://x', season='2015',
               season_type='REG', recent_team=team, games='16',
               fg_made_list='', fg_made_distance='')
    row.update({name: str(value) for name, value in stats.items()})
    return row


class TestIdentity:
    def test_the_people_files_aliases_resolve_to_the_archive_codes(self):
        # GSIS roster spellings and Pro-Football-Reference draft spellings.
        assert identity.resolve_code('SL') == 'STL'
        assert identity.people_franchise_key('SL') == LA
        assert identity.people_franchise_key('LAR') == LA
        assert identity.people_franchise_key('KAN') == KC
        assert identity.people_franchise_key('sfo') == SF

    def test_an_unknown_code_is_refused_not_guessed(self):
        with pytest.raises(ValueError):
            identity.resolve_code('XYZ')
        with pytest.raises(ValueError):
            identity.resolve_code('')

    def test_the_game_loader_stays_strict(self):
        # The aliases are for the people files only. The game archive has
        # never used them, so one appearing there is something to look at.
        with pytest.raises(ValueError):
            identity.franchise_code('SL')

    def test_a_player_key_is_namespaced_and_the_id_untouched(self):
        assert identity.player_key(' 00-0019596 ') == \
            'nfl:nflverse:player:00-0019596'
        with pytest.raises(ValueError):
            identity.player_key('')


class TestParsing:
    def test_players_before_the_archive_and_without_an_id_are_left_out(self):
        rows, skipped = people.parse_players([
            player_row('00-1', 'Kept'),
            player_row('00-2', 'Retired 1991', last_season='1991'),
            player_row('', 'No id'),
        ])
        assert [row['display_name'] for row in rows] == ['Kept']
        assert skipped == dict(no_id=1, before_archive=1)
        kept = rows[0]
        assert kept['draft_team'] == KC and kept['height'] == 75
        assert kept['stub'] == 0

    def test_a_traded_player_keeps_both_rows_and_a_duplicate_keeps_one(self):
        rows, stubs, skipped = people.parse_roster(2015, [
            roster_row('00-1', 'KC', jersey_number='10'),
            roster_row('00-1', 'SL'),
            roster_row('00-1', 'KC', jersey_number='11'),
            roster_row('', 'KC'),
        ])
        teams = sorted(row['team_key'] for row in rows)
        assert teams == [KC, LA]
        kc = [row for row in rows if row['team_key'] == KC][0]
        assert kc['jersey_number'] == 11, 'the later duplicate wins'
        assert skipped == dict(no_id=1, duplicate=1)
        # The raw code is kept for what the source really said.
        assert [row['code'] for row in rows if row['team_key'] == LA] == ['SL']
        assert len(stubs) == 1 and stubs[0]['stub'] == 1

    def test_statistics_drop_zeros_lists_and_context_columns(self):
        contexts, values, stubs, skipped = people.parse_stats(2015, 'reg', [
            stat_row('00-1', receptions=80, receiving_yards=1001.0,
                     receiving_tds=0, target_share=0.25,
                     fg_made_list='23;41'),
            stat_row('', receptions=3),
        ])
        stats = {row['stat']: row['value'] for row in values}
        assert stats == dict(receptions=80.0, receiving_yards=1001.0,
                             target_share=0.25)
        assert contexts[0]['games'] == 16 and contexts[0]['team_key'] == KC
        assert skipped['no_id'] == 1

    def test_a_draft_pick_without_an_id_keeps_its_row(self):
        rows, skipped = people.parse_draft([
            dict(season='2010', round='1', pick='5', team='TAM', gsis_id='',
                 pfr_player_name='Nobody'),
            dict(season='2010', round='1', pick='6', team='GNB',
                 gsis_id='00-9', pfr_player_name='Somebody', hof='TRUE'),
            dict(season='1985', round='1', pick='1', team='NWE',
                 gsis_id='00-8', pfr_player_name='Too early'),
        ])
        assert [(row['pick'], row['player_key']) for row in rows] == [
            (5, None), (6, 'nfl:nflverse:player:00-9')]
        assert rows[0]['team_key'] == 'nfl:nflverse:franchise:TB'
        assert rows[1]['hall_of_fame'] == 1
        assert skipped['before_archive'] == 1


def bundle(**extra):
    base = dict(
        players=[player_row('00-1', 'Real Player')],
        rosters={2015: [roster_row('00-1', 'KC', name='Roster Name'),
                        roster_row('00-2', 'SL', name='Only On A Roster')]},
        stats={(2015, 'reg'): [stat_row('00-1', receptions=50),
                               stat_row('00-3', receptions=2)]},
        draft=[dict(season='2005', round='1', pick='3', team='KAN',
                    gsis_id='00-1', pfr_player_name='Real Player'),
               dict(season='2005', round='7', pick='250', team='KAN',
                    gsis_id='00-7', pfr_player_name='Never stored')])
    base.update(extra)
    return base


class TestStoring:
    def test_ids_the_player_file_lacks_become_flagged_stubs(self, db):
        report = people.ingest(db, bundle())
        assert report['players'] == 1 and report['stubs'] == 2
        rows = {row['gsis_id']: dict(row) for row in
                db.execute('SELECT * FROM player')}
        assert rows['00-1']['stub'] == 0
        assert rows['00-1']['display_name'] == 'Real Player', \
            'a stub never renames a real biography'
        assert rows['00-2']['stub'] == 1
        assert rows['00-2']['display_name'] == 'Only On A Roster'
        assert rows['00-3']['stub'] == 1

    def test_a_stub_fills_gaps_and_overwrites_nothing(self, db):
        full = people.parse_players([player_row('00-1', 'Real',
                                                college_name='')])[0]
        people.upsert_players(db, full)
        _, stubs, _ = people.parse_roster(2015, [
            roster_row('00-1', 'KC', name='Stub Name', college='Filled In',
                       height='60')])
        people.upsert_players(db, stubs)
        row = db.execute('SELECT * FROM player').fetchone()
        assert row['college'] == 'Filled In', 'a gap is filled'
        assert row['height'] == 75, 'a value is not overwritten'

    def test_the_player_file_clears_a_stub_flag_whenever_it_arrives(self, db):
        _, stubs, _ = people.parse_roster(2015, [roster_row('00-1', 'KC')])
        people.upsert_players(db, stubs)
        full = people.parse_players([player_row('00-1', 'Real')])[0]
        people.upsert_players(db, full)
        row = db.execute('SELECT stub, display_name FROM player').fetchone()
        assert (row['stub'], row['display_name']) == (0, 'Real')

    def test_a_draft_link_to_an_unstored_player_is_dropped(self, db):
        report = people.ingest(db, bundle())
        assert report['draft_unlinked'] == 1
        links = dict(db.execute('SELECT pick, player_key FROM draft_pick'))
        assert links == {3: 'nfl:nflverse:player:00-1', 250: None}

    def test_ingest_twice_changes_nothing(self, db):
        people.ingest(db, bundle())
        before = [db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
                  for table in ('player', 'player_season', 'player_stat',
                                'player_stat_season', 'draft_pick')]
        people.ingest(db, bundle())
        after = [db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
                 for table in ('player', 'player_season', 'player_stat',
                               'player_stat_season', 'draft_pick')]
        assert before == after

    def test_a_stat_that_stops_being_non_zero_stops_existing(self, db):
        people.ingest(db, bundle())
        revised = bundle(stats={(2015, 'reg'): [
            stat_row('00-1', receptions=0, receiving_yards=12)]})
        people.ingest(db, revised)
        stats = dict(db.execute(
            "SELECT stat, value FROM player_stat WHERE player_key = ?",
            ('nfl:nflverse:player:00-1',)))
        assert stats == dict(receiving_yards=12.0)

    def test_opening_an_old_database_creates_every_table_added_since(
            self, tmp_path):
        path = tmp_path / 'old.db'
        connection = store.connect(path)
        for table in ('player_stat', 'player_stat_season', 'player_season',
                      'draft_pick', 'player', 'game_drive', 'scoring_play',
                      'player_snap', 'injury_report'):
            connection.execute(f'DROP TABLE {table}')
        connection.execute('UPDATE schema_version SET version = 2')
        connection.commit()
        connection.close()
        connection = store.connect(path)
        tables = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'")}
        assert {'player', 'player_season', 'player_stat',
                'player_stat_season', 'draft_pick', 'game_drive',
                'scoring_play', 'player_snap', 'injury_report'} <= tables
        assert connection.execute(
            'SELECT version FROM schema_version').fetchone()[0] \
            == store.SCHEMA_VERSION
        connection.close()

    def test_an_old_player_table_gains_the_column_the_snap_join_needs(
            self, tmp_path):
        """`CREATE TABLE IF NOT EXISTS` leaves an existing table alone.

        A database built at schema 4 has a `player` table with no `pfr_id`,
        and the schema file cannot add it. Without the migration every snap
        row would fail to resolve and the page would simply be empty.
        """
        path = tmp_path / 'four.db'
        connection = store.connect(path)
        connection.execute('ALTER TABLE player DROP COLUMN pfr_id')
        connection.execute('UPDATE schema_version SET version = 4')
        connection.commit()
        connection.close()
        connection = store.connect(path)
        columns = {row[1] for row in
                   connection.execute('PRAGMA table_info(player)')}
        assert 'pfr_id' in columns
        connection.close()


class FakeResponse:
    def __init__(self, body):
        self.body, self.status = body, 200

    def read(self, limit):
        return self.body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def opener_for(missing):
    def opener(request, timeout=None):
        url = request.full_url
        if any(url.endswith(name) for name in missing):
            raise urllib.error.HTTPError(url, 404, 'missing', {}, None)
        return FakeResponse(b'gsis_id,team\n00-1,KC\n')
    return opener


class TestFetching:
    NOW = datetime(2026, 10, 21, tzinfo=timezone.utc)

    def test_the_live_release_tag_is_used(self):
        # The older `player_stats` tag stops at 2024 and 404s from 2025.
        assert '/stats_player/stats_player_reg_2025.csv' in \
            people_source.stats_url(2025, 'reg')

    def test_a_missing_file_for_the_season_in_progress_is_tolerated(
            self, tmp_path):
        got, origins = people_source.load(
            seasons=[2025, 2026], root=tmp_path, now=self.NOW,
            opener=opener_for(['stats_player_post_2026.csv']),
            sleep=lambda seconds: None)
        assert origins['stats_post_2026'] == 'absent'
        assert (2026, 'reg') in got['stats']
        assert (2026, 'post') not in got['stats']

    def test_the_archive_not_having_caught_up_is_tolerated(self, tmp_path):
        # September: the season has started and the source has not yet
        # published its statistics file.
        got, origins = people_source.load(
            seasons=[2026], root=tmp_path, now=self.NOW,
            opener=opener_for(['stats_player_reg_2026.csv',
                               'stats_player_post_2026.csv']),
            sleep=lambda seconds: None)
        assert origins['stats_reg_2026'] == 'absent'
        assert 2026 in got['rosters']

    def test_a_missing_file_for_a_settled_season_fails_the_run(
            self, tmp_path):
        with pytest.raises(people_source.NflSourceError):
            people_source.load(
                seasons=[2025, 2026], root=tmp_path, now=self.NOW,
                opener=opener_for(['stats_player_post_2025.csv']),
                sleep=lambda seconds: None)


def loaded(db, **extra):
    people.ingest(db, bundle(**extra))
    return db


class TestExport:
    def test_a_shard_is_stable_and_in_range(self):
        assert people_export.shard_of('00-0019596') == \
            people_export.shard_of('00-0019596')
        assert all(0 <= people_export.shard_of(f'00-{n:07d}') < 64
                   for n in range(500))

    def test_the_season_in_progress_lives_outside_the_shards(self, db):
        loaded(db, rosters={
            2015: [roster_row('00-1', 'KC')],
            2016: [roster_row('00-1', 'SF')]})
        files = people_export.build(db, settled_before=2016)
        shard = files[f'people/careers/{people_export.shard_of("00-1")}.js']
        settled = shard[2]['players']['00-1']['roster']
        assert [row[0] for row in settled] == [2015]
        current = files['people/current.js'][2]
        assert current['season'] == 2016
        assert [row[0] for row in current['players']['00-1']['roster']] == \
            [2016]

    def test_packed_statistics_decode_to_what_was_stored(self, db):
        loaded(db, stats={(2015, 'reg'): [stat_row(
            '00-1', receptions=50, receiving_yards=612, target_share=0.231,
            penalties=3)]})
        files = people_export.build(db, settled_before=2026)
        body = files[f'people/careers/{people_export.shard_of("00-1")}.js'][2]
        line = body['players']['00-1']['seasons'][0]
        assert line[:4] == [2015, 0, 'KC', 16]
        decoded = {body['stats'][line[at]]: line[at + 1]
                   for at in range(4, len(line), 2)}
        # `penalties` is stored but not on the export's allowlist.
        assert decoded == dict(receptions=50, receiving_yards=612,
                               target_share=0.231)
        assert db.execute("SELECT COUNT(*) FROM player_stat"
                          " WHERE stat = 'penalties'").fetchone()[0] == 1

    def test_every_leader_category_is_exported(self):
        assert set(people_export.LEADER_STATS) <= people_export.EXPORTED_STATS

    def test_a_tie_at_the_cut_keeps_every_tied_player(self, db):
        rows = [stat_row(f'00-{n}', receptions=100 - n) for n in range(3)]
        rows += [stat_row('00-8', receptions=98), stat_row('00-9',
                                                           receptions=97)]
        loaded(db, stats={(2015, 'reg'): rows})
        board = people_export.leaders(db, 2015, deep=3)['receptions']
        assert [row['value'] for row in board] == [100, 99, 98, 98]

    def test_the_index_names_only_players_with_a_page(self, db):
        loaded(db, players=[player_row('00-1', 'Real Player'),
                            player_row('00-5', 'Nothing Here')])
        files = people_export.build(db, settled_before=2026)
        ids = [row[0] for row in files['people/index.js'][2]['players']]
        assert '00-5' not in ids and '00-1' in ids


def manifest_with_people(**people_section):
    section = dict(index='people/index.js?v=1', current='people/current.js?v=1',
                   draft='people/draft.js?v=1', season=2026, shards=64,
                   careers=[f'people/careers/{n}.js?v=1' for n in range(64)])
    section.update(people_section)
    return dict(files=dict(people=section))


class TestGate:
    def test_an_export_without_people_still_passes(self):
        assert publish_gate.check_people(dict(files=dict(teams='t.js'))) == []

    def test_a_complete_people_layer_passes(self):
        assert publish_gate.check_people(manifest_with_people()) == []

    def test_a_missing_shard_is_refused(self):
        reasons = publish_gate.check_people(manifest_with_people(
            careers=[f'people/careers/{n}.js?v=1' for n in range(63)]))
        assert reasons and 'exactly 64' in reasons[0]

    def test_a_missing_index_is_refused(self):
        assert publish_gate.check_people(manifest_with_people(index=None))

    def test_the_shard_count_is_a_literal_on_both_sides(self):
        assert publish_gate.PEOPLE_SHARDS == 64
        assert people_export.PLAYER_SHARDS == 64

    def test_publishing_cannot_remove_a_layer_the_site_has(self, tmp_path,
                                                            monkeypatch):
        live, new = tmp_path / 'live', tmp_path / 'new'
        for directory, files in ((live, dict(teams='t', people={})),
                                 (new, dict(teams='t'))):
            directory.mkdir()
            (directory / 'manifest.json').write_text(json.dumps(dict(
                generated_at='2026-10-21T00:00:00Z', files=files)))
        decision = publish.plan(new, live, gate=lambda *a, **k: [])
        assert decision['publish'] is False
        assert 'people' in decision['reason']


@pytest.mark.skipif(__import__('shutil').which('node') is None,
                    reason='node is not installed')
def test_the_page_decodes_exactly_what_the_exporter_packed(db, tmp_path):
    """Both halves of the packed format, driven together.

    The exporter writes positional arrays and the page's `unpackSeasons`
    reads them. Each has its own unit test, and two unit tests of two halves
    of one format can both pass while the halves disagree; this one runs the
    real exporter's output through the real browser function.
    """
    import subprocess
    from pathlib import Path
    loaded(db, stats={(2015, 'reg'): [stat_row(
        '00-1', receptions=50, receiving_yards=612, target_share=0.231)],
        (2015, 'post'): [stat_row('00-1', receptions=4)]})
    files = people_export.build(db, settled_before=2026)
    path = f'people/careers/{people_export.shard_of("00-1")}.js'
    target = tmp_path / 'shard.js'
    target.write_text(people_export.render(*files[path]), encoding='utf-8')
    app = Path(__file__).resolve().parent.parent / 'ui' / 'nfl_app.js'
    script = (
        "const vm=require('vm'),fs=require('fs');"
        "const ctx={window:{}};vm.createContext(ctx);"
        f"vm.runInContext(fs.readFileSync({json.dumps(str(target))},'utf8'),ctx);"
        f"const app=require({json.dumps(str(app))});"
        "const shard=Object.values(ctx.window.NFL_PEOPLE_SHARDS)[0];"
        "console.log(JSON.stringify(app.unpackSeasons(shard,'00-1')));")
    out = subprocess.run(['node', '-e', script], capture_output=True,
                         text=True, check=True).stdout
    assert json.loads(out) == [
        dict(season=2015, type='reg', team='KC', games=16,
             stats=dict(receptions=50, receiving_yards=612,
                        target_share=0.231)),
        dict(season=2015, type='post', team='KC', games=16,
             stats=dict(receptions=4)),
    ]
