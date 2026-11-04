"""The NBA export contract, the gate that refuses a bad one, and publishing.

The fixture is a whole synthetic league — thirty franchises playing each
other three times, so a season carries 1,305 games — because every floor in
the gate is a league-sized number. Lowering a floor to let a small fixture
through would make the gate's own test the reason the gate stops working.
"""

import json
from datetime import datetime, timedelta, timezone

import pytest

from src.nba import export as export_module
from src.nba import publish, publish_gate, store
from src.nba.build_ratings import build as build_ratings
from src.nba.identity import season_label, team_key

TEAM_IDS = list(range(1, 31))
CONFIG = dict(elo=dict(k=20, home_advantage=80, offseason_retention=0.6,
                       margin_model='mov', margin_exponent=0.75,
                       margin_min=0.25, margin_max=4.0,
                       reference_margin=11.04, points_per_elo=0.0328,
                       initial_rating=1500.0, scale=400.0))
REPORT = dict(fit_seasons=[2025], holdout_seasons=[2026],
              holdout=dict(brier=0.218, games=6605))


def season_games(season, *, played=True):
    """Every pair three times: 1,305 games, past the gate's season floor."""
    rows, moment = [], datetime(season - 1, 10, 20, tzinfo=timezone.utc)
    for meeting in range(3):
        for index, home_id in enumerate(TEAM_IDS):
            for away_id in TEAM_IDS[index + 1:]:
                home, away = ((home_id, away_id) if meeting != 1
                              else (away_id, home_id))
                moment += timedelta(hours=2)
                points = (112, 104) if played else (None, None)
                provider = f'{season}{meeting}{home}x{away}'
                rows.append(dict(
                    game_key=f'nba:espn:game:{provider}',
                    provider_id=provider, season=season,
                    season_label=season_label(season),
                    season_type='regular', postseason=0,
                    game_date=moment.date().isoformat(),
                    tipoff_utc=moment.isoformat().replace('+00:00', 'Z'),
                    tipoff_tbd=0, neutral_site=0,
                    completed=1 if played else 0,
                    home_team_key=team_key(home), away_team_key=team_key(away),
                    home_team_id=home, away_team_id=away,
                    home_points=points[0], away_points=points[1],
                    venue_id=str(home), venue=f'Arena {home}',
                    venue_city=f'City {home}', venue_state='ST',
                    attendance=18000, notes=None,
                    rateable=1 if played else 0,
                    exclusions=[] if played else ['not_played']))
    return rows


def team_rows(season):
    return [dict(team_key=team_key(team_id), team_id=team_id, season=season,
                 abbreviation=f'T{team_id:02d}', location=f'City {team_id}',
                 nickname=f'Club {team_id}', name=f'City {team_id} Club',
                 logo=None, logo_dark=None) for team_id in TEAM_IDS]


def conferences():
    return {team_id: ('Eastern Conference' if team_id % 2 else
                      'Western Conference') for team_id in TEAM_IDS}


@pytest.fixture
def league(tmp_path):
    connection = store.connect(tmp_path / 'nba.db')
    games = season_games(2025) + season_games(2026)
    for season in (2025, 2026):
        store.upsert_team_seasons(connection, team_rows(season),
                                  conferences=conferences())
    store.upsert_games(connection, games)
    store.refresh_team_identity(connection)
    build_ratings(connection, CONFIG, report=REPORT)
    return connection


# Early in 2026-27, so 2026 is a complete season behind us. In the PAST,
# because `write()` runs the gate against the real clock and an export
# stamped tomorrow is refused — as it should be.
NOW = datetime(2026, 10, 1, tzinfo=timezone.utc)


def build(connection, **kwargs):
    kwargs.setdefault('now', NOW)
    return export_module.build(connection, **kwargs)


class TestExport:
    def test_a_full_league_export_passes_the_gate(self, league, tmp_path):
        manifest, payloads, teams, history = build(league)
        written = export_module.write(manifest, payloads, teams, history,
                                      tmp_path / 'out')
        assert publish_gate.check(written, root=tmp_path / 'out',
                                  now=NOW) == []
        assert (tmp_path / 'out' / 'manifest.json').exists()

    def test_the_manifest_states_the_model_and_its_held_out_score(self,
                                                                  league):
        manifest, _, _, _ = build(league)
        model = manifest['model']
        assert model['name'] == 'nba_elo_v1'
        assert model['holdout_brier'] == 0.218
        assert model['held_out'] == [2026]
        assert len(model['config_sha256']) == 64

    def test_every_file_carries_a_hash_of_its_own_bytes(self, league,
                                                        tmp_path):
        manifest, payloads, teams, history = build(league)
        written = export_module.write(manifest, payloads, teams, history,
                                      tmp_path / 'out')
        name, token = written['files']['teams'].split('?v=')
        body = (tmp_path / 'out' / name).read_text(encoding='utf-8')
        import hashlib
        assert token == hashlib.sha256(
            body.encode('utf-8')).hexdigest()[:10]

    def test_a_season_file_names_the_global_the_page_reads(self, league,
                                                           tmp_path):
        manifest, payloads, teams, history = build(league)
        export_module.write(manifest, payloads, teams, history,
                            tmp_path / 'out')
        body = (tmp_path / 'out' / 'seasons' / '2026.js').read_text(
            encoding='utf-8')
        assert body.startswith('window.NBA_SEASONS=')
        assert '"season":2026' in body

    def test_the_board_is_ranked_and_carries_the_seasons_own_name(self,
                                                                  league):
        _, payloads, _, _ = build(league)
        board = payloads[2026]['board']
        assert [row['rank'] for row in board] == list(range(1, 31))
        assert board[0]['rating'] >= board[-1]['rating']
        assert board[0]['name'].startswith('City ')
        assert board[0]['conference'].endswith('Conference')

    def test_each_franchise_is_rated_and_carries_its_record(self, league):
        _, payloads, _, _ = build(league)
        board = payloads[2026]['board']
        assert len(board) == 30
        # Three meetings against each of 29 others.
        assert all(row['wins'] + row['losses'] == 87 for row in board)

    def test_the_games_carry_the_stored_chain_not_a_recomputed_one(self,
                                                                   league):
        _, payloads, _, _ = build(league)
        rated = [game for game in payloads[2026]['games']
                 if 'home_wp' in game]
        assert len(rated) == len(payloads[2026]['games'])
        assert all('wp_estimated' not in game for game in rated)
        assert all(0 < game['home_wp'] < 1 for game in rated)

    def test_an_unplayed_game_keeps_no_score_at_all(self, tmp_path):
        connection = store.connect(tmp_path / 'nba.db')
        games = season_games(2025) + season_games(2026)
        for game in games[:4]:
            game.update(completed=0, home_points=None, away_points=None,
                        rateable=0, exclusions=['not_played'])
        for season in (2025, 2026):
            store.upsert_team_seasons(connection, team_rows(season),
                                      conferences=conferences())
        store.upsert_games(connection, games)
        store.refresh_team_identity(connection)
        build_ratings(connection, CONFIG, report=REPORT)
        _, payloads, _, _ = build(connection)
        blanks = [game for game in payloads[2025]['games']
                  if game['home_points'] is None]
        assert len(blanks) == 4
        assert all(game['away_points'] is None for game in blanks)
        # No score, but still a number a reader can act on — marked as the
        # estimate it is, never as a stored pregame fact.
        assert all(game.get('wp_estimated') == 1 for game in blanks)
        assert all(0 < game['home_wp'] < 1 for game in blanks)

    def test_the_cup_final_is_rated_but_kept_out_of_the_record(self,
                                                               tmp_path):
        connection = store.connect(tmp_path / 'nba.db')
        games = season_games(2025) + season_games(2026)
        # One extra game between two franchises, of the type the NBA keeps
        # out of the standings.
        final = dict(games[-1])
        final.update(game_key='nba:espn:game:cupfinal',
                     provider_id='cupfinal', season=2026,
                     season_type='cup_final', game_date='2026-12-16',
                     tipoff_utc='2026-12-17T01:30:00Z',
                     notes='NBA Cup Championship', neutral_site=1)
        games.append(final)
        for season in (2025, 2026):
            store.upsert_team_seasons(connection, team_rows(season),
                                      conferences=conferences())
        store.upsert_games(connection, games)
        store.refresh_team_identity(connection)
        build_ratings(connection, CONFIG, report=REPORT)
        _, payloads, _, _ = build(connection)
        board = {row['team']: row for row in payloads[2026]['board']}
        # Every franchise still shows 87, including the two who played 88.
        assert {row['wins'] + row['losses'] for row in board.values()} == {87}
        cup = [game for game in payloads[2026]['games']
               if game['type'] == 'cup_final']
        assert len(cup) == 1
        # Rated all the same: a real game between two teams trying to win.
        assert cup[0]['rateable'] == 1
        assert 'home_wp' in cup[0]

    def test_the_season_in_progress_is_not_marked_complete(self, league):
        manifest, _, _, _ = build(league, now=datetime(
            2026, 3, 1, tzinfo=timezone.utc))
        rows = {row['season']: row for row in manifest['coverage']}
        assert rows[2025]['complete'] is True
        assert rows[2026]['complete'] is False

    def test_the_history_file_carries_a_conference_row_a_season(self,
                                                                league):
        _, _, _, history = build(league)
        seasons = {row['season'] for row in history['conferences']}
        assert seasons == {2025, 2026}
        row = history['conferences'][0]
        assert {entry['conference'] for entry in row['conferences']} == {
            'Eastern Conference', 'Western Conference'}
        assert all(entry['teams'] == 15 for entry in row['conferences'])

    def test_the_team_index_is_the_franchises_current_name(self, league):
        _, _, teams, _ = build(league)
        assert len(teams) == 30
        assert all(entry['name'] for entry in teams.values())


class TestGate:
    def test_a_season_missing_a_franchise_is_refused(self, league):
        manifest, _, _, _ = build(league)
        manifest['coverage'][0]['teams'] = 29
        reasons = publish_gate.check(manifest, now=NOW)
        assert any('franchises' in reason for reason in reasons)

    def test_a_season_before_the_expansion_wants_twenty_nine(self):
        assert publish_gate.expected_teams(2004) == 29
        assert publish_gate.expected_teams(2005) == 30

    def test_another_leagues_export_can_never_pass_this_gate(self, league):
        manifest, _, _, _ = build(league)
        manifest['sport'] = 'nfl'
        reasons = publish_gate.check(manifest, now=NOW)
        assert any('nba' in reason for reason in reasons)

    def test_a_model_no_better_than_the_home_team_is_refused(self, league):
        manifest, _, _, _ = build(league)
        manifest['model']['holdout_brier'] = 0.2461
        reasons = publish_gate.check(manifest, now=NOW)
        assert any('home team' in reason for reason in reasons)

    def test_a_holdout_of_one_season_is_too_narrow(self, league):
        manifest, _, _, _ = build(league)
        manifest['model']['holdout_games'] = 1300
        reasons = publish_gate.check(manifest, now=NOW)
        assert any('too narrow' in reason for reason in reasons)

    def test_a_leaked_holdout_is_refused(self, league):
        manifest, _, _, _ = build(league)
        manifest['model']['fitted_on'] = [2025, 2026]
        reasons = publish_gate.check(manifest, now=NOW)
        assert any('leaked' in reason for reason in reasons)

    def test_an_as_of_time_in_the_future_is_refused(self, league):
        manifest, _, _, _ = build(league)
        manifest['generated_at'] = '2030-01-01T00:00:00Z'
        reasons = publish_gate.check(manifest, now=NOW)
        assert any('future' in reason for reason in reasons)

    def test_an_export_hours_old_is_still_publishable(self, league):
        manifest, _, _, _ = build(league, now=NOW - timedelta(hours=30))
        assert publish_gate.check(manifest, now=NOW) == []

    def test_a_named_file_that_is_not_on_disk_is_refused(self, league,
                                                         tmp_path):
        manifest, payloads, teams, history = build(league)
        written = export_module.write(manifest, payloads, teams, history,
                                      tmp_path / 'out')
        (tmp_path / 'out' / 'teams.js').unlink()
        reasons = publish_gate.check(written, root=tmp_path / 'out', now=NOW)
        assert any('not on disk' in reason for reason in reasons)

    def test_an_empty_file_is_refused(self, league, tmp_path):
        manifest, payloads, teams, history = build(league)
        written = export_module.write(manifest, payloads, teams, history,
                                      tmp_path / 'out')
        (tmp_path / 'out' / 'teams.js').write_text('', encoding='utf-8')
        reasons = publish_gate.check(written, root=tmp_path / 'out', now=NOW)
        assert any('empty' in reason for reason in reasons)

    def test_nothing_is_written_when_the_gate_refuses(self, league,
                                                      tmp_path):
        manifest, payloads, teams, history = build(league)
        manifest['model']['holdout_brier'] = 0.9
        out = tmp_path / 'out'
        with pytest.raises(export_module.PublishRefused):
            export_module.write(manifest, payloads, teams, history, out)
        assert not out.exists()

    def test_the_gate_runs_as_a_command_against_a_directory(self, league,
                                                            tmp_path,
                                                            capsys):
        manifest, payloads, teams, history = build(league)
        out = tmp_path / 'out'
        export_module.write(manifest, payloads, teams, history, out)
        assert publish_gate.main([str(out)]) == 0
        assert 'passes the gate' in capsys.readouterr().out


class TestPublish:
    def _export(self, league, directory, **kwargs):
        manifest, payloads, teams, history = build(league, **kwargs)
        return export_module.write(manifest, payloads, teams, history,
                                   directory)

    def test_the_first_export_publishes(self, league, tmp_path):
        self._export(league, tmp_path / 'new')
        decision = publish.plan(tmp_path / 'new', tmp_path / 'live', now=NOW)
        assert decision['publish'] and decision['first']
        assert publish.apply(decision, tmp_path / 'new', tmp_path / 'live')
        assert (tmp_path / 'live' / 'manifest.json').exists()

    def test_an_older_export_never_rolls_the_site_back(self, league,
                                                       tmp_path):
        self._export(league, tmp_path / 'live', now=NOW)
        self._export(league, tmp_path / 'new', now=NOW - timedelta(hours=2))
        decision = publish.plan(tmp_path / 'new', tmp_path / 'live', now=NOW)
        assert not decision['publish']
        assert 'roll the site back' in decision['reason']

    def test_a_refused_export_leaves_the_live_one_alone(self, league,
                                                        tmp_path):
        self._export(league, tmp_path / 'live', now=NOW)
        before = (tmp_path / 'live' / 'manifest.json').read_text()
        (tmp_path / 'new').mkdir()
        (tmp_path / 'new' / 'manifest.json').write_text('{}')
        decision = publish.plan(tmp_path / 'new', tmp_path / 'live', now=NOW)
        assert not decision['publish']
        assert publish.apply(decision, tmp_path / 'new',
                             tmp_path / 'live') is False
        assert (tmp_path / 'live' / 'manifest.json').read_text() == before

    def test_an_export_that_drops_a_live_layer_is_refused(self, league,
                                                          tmp_path):
        self._export(league, tmp_path / 'live', now=NOW)
        live = json.loads(
            (tmp_path / 'live' / 'manifest.json').read_text())
        live['files']['people'] = 'people/index.js'
        (tmp_path / 'live' / 'manifest.json').write_text(json.dumps(live))
        self._export(league, tmp_path / 'new', now=NOW)
        decision = publish.plan(tmp_path / 'new', tmp_path / 'live', now=NOW)
        assert not decision['publish']
        assert 'would remove that part of the site' in decision['reason']

    def test_the_module_entry_point_runs(self, league, tmp_path, monkeypatch,
                                         capsys):
        league.close()
        monkeypatch.setattr(export_module, 'OUT_DIR', tmp_path / 'out')
        code = export_module.main(['--database', str(tmp_path / 'nba.db'),
                                   '--out', str(tmp_path / 'out')])
        assert code == 0
        assert 'Publish gate PASSED' in capsys.readouterr().out
