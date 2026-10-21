"""The NFL export contract and the gate that refuses a bad one.

The fixture is a whole synthetic league — 32 teams, a full season of games —
because every floor in the gate is a league-sized number. Lowering a floor to
let a small fixture through would make the gate's own test the reason the gate
stops working.
"""

import json
from datetime import datetime, timedelta, timezone

import pytest

from src.nfl import export as export_module
from src.nfl import publish_gate, store
from src.nfl.build_ratings import build as build_ratings
from src.nfl.identity import FRANCHISE_CODES, franchise_key

CODES = sorted(FRANCHISE_CODES)
CONFIG = dict(elo=dict(k=48, home_advantage=50, offseason_retention=0.5,
                       margin_model='mov', margin_exponent=0.75,
                       margin_min=0.25, margin_max=4.0,
                       reference_margin=11.67, points_per_elo=0.0527,
                       initial_rating=1500.0, scale=400.0))
REPORT = dict(fit_seasons=[2024], holdout_seasons=[2025],
              holdout=dict(brier=0.2237, games=1424))


def season_games(season, *, finals=True, start_day=10):
    """A full round-robin-ish season: every team plays every other once."""
    rows, day = [], datetime(season, 9, start_day, tzinfo=timezone.utc)
    for i, home in enumerate(CODES):
        for away in CODES[i + 1:]:
            day = day + timedelta(hours=3)
            points = (24, 17) if finals else (None, None)
            rows.append(dict(
                game_key=f'nfl:nflverse:game:{season}_{home}_{away}',
                provider_id=f'{season}_{home}_{away}',
                season=season, season_label=str(season),
                season_type='regular', postseason=0, week=1,
                game_date=day.date().isoformat(),
                kickoff_utc=day.isoformat().replace('+00:00', 'Z'),
                kickoff_tbd=0, neutral_site=0,
                home_code=home, away_code=away,
                home_team_key=franchise_key(home),
                away_team_key=franchise_key(away),
                home_points=points[0], away_points=points[1],
                overtime=0, divisional_game=0, roof=None, surface=None,
                stadium_id=None, stadium=None,
                rateable=1 if finals else 0, exclusions=[]))
    return rows


@pytest.fixture
def league(tmp_path):
    connection = store.connect(tmp_path / 'nfl.db')
    games = season_games(2025) + season_games(2026)
    store.upsert_teams(connection, [
        dict(season='2026', team=code, full=f'{code} Team',
             location=code, nickname='Team') for code in CODES])
    store.upsert_team_stubs(connection, games)
    store.upsert_games(connection, games)
    store.refresh_team_spans(connection)
    build_ratings(connection, CONFIG, report=REPORT)
    return connection


def build(connection, **kwargs):
    return export_module.build(connection, **kwargs)


class TestExport:
    def test_a_full_league_export_passes_the_gate(self, league, tmp_path):
        manifest, payloads, teams = build(league)
        written = export_module.write(manifest, payloads, teams,
                                      tmp_path / 'out')
        assert publish_gate.check(written, root=tmp_path / 'out') == []
        assert (tmp_path / 'out' / 'manifest.json').exists()

    def test_the_manifest_states_the_model_and_its_held_out_score(
            self, league, tmp_path):
        manifest, _, _ = build(league)
        model = manifest['model']
        assert model['name'] == 'nfl_elo_v1'
        assert model['holdout_brier'] == 0.2237
        assert model['held_out'] == [2025]
        assert len(model['config_sha256']) == 64

    def test_every_file_carries_a_hash_of_its_own_bytes(self, league,
                                                        tmp_path):
        manifest, payloads, teams = build(league)
        written = export_module.write(manifest, payloads, teams,
                                      tmp_path / 'out')
        for path, version in publish_gate.manifest_files(written):
            assert version == publish_gate.file_version(
                tmp_path / 'out' / path)

    def test_a_refused_export_writes_nothing(self, league, tmp_path):
        out = tmp_path / 'out'
        manifest, payloads, teams = build(league)
        manifest['model']['holdout_brier'] = 0.3
        with pytest.raises(export_module.PublishRefused):
            export_module.write(manifest, payloads, teams, out)
        assert not out.exists()

    def test_a_season_in_progress_is_marked_and_not_judged_as_finished(
            self, league, tmp_path):
        # Half of 2026's games unplayed, as a live season looks in October.
        league.execute(
            'UPDATE game SET home_points = NULL, away_points = NULL,'
            ' rateable = 0 WHERE season = 2026 AND provider_id LIKE ?',
            ('%\\_W%', ))
        league.commit()
        manifest, payloads, teams = build(
            league, now=datetime(2026, 10, 21, tzinfo=timezone.utc))
        rows = {row['season']: row for row in manifest['coverage']}
        assert rows[2026]['complete'] is False
        assert rows[2025]['complete'] is True
        assert publish_gate.check_coverage(manifest) == []

    def test_the_historical_code_rides_on_the_game_not_the_team(self,
                                                               league):
        """A 2015 game says STL; the franchise index says Rams."""
        games = season_games(2015)
        for game in games:
            if game['home_code'] == 'LA':
                game['home_code'] = 'STL'
        store.upsert_games(league, games)
        exported = export_module.season_games(league, 2015)
        rams = [game for game in exported
                if game.get('home_code') == 'STL']
        assert rams
        assert all(game['home'] == franchise_key('LA') for game in rams)

    def test_a_tie_is_its_own_column_in_the_record(self):
        tally = export_module.records([
            dict(home='a', away='b', home_points=20, away_points=20),
            dict(home='a', away='c', home_points=24, away_points=10),
        ])
        assert tally['a'] == dict(w=1, l=0, t=1)
        assert tally['b'] == dict(w=0, l=0, t=1)


class TestGate:
    def _manifest(self, **overrides):
        base = dict(
            schema_version=1, sport='nfl',
            generated_at='2026-10-21T00:00:00Z',
            model=dict(name='nfl_elo_v1', config_sha256='x' * 64,
                       fitted_on=[2020], held_out=[2025],
                       holdout_brier=0.2237, holdout_games=1424),
            coverage=[dict(season=2025, teams=32, rated_teams=32, games=285,
                           finals=285, complete=True, played_teams=32)],
            seasons={'2025': 'seasons/2025.js?v=abc1234567'},
            files={})
        base.update(overrides)
        return base

    def test_a_clean_manifest_passes(self):
        assert publish_gate.check(self._manifest()) == []

    def test_a_missing_franchise_fails_a_fixed_size_league(self):
        manifest = self._manifest(coverage=[dict(
            season=2025, teams=31, rated_teams=31, games=285, finals=285,
            complete=True, played_teams=31)])
        reasons = publish_gate.check(manifest)
        assert any('exactly 32' in reason for reason in reasons)

    def test_the_pre_expansion_league_is_allowed_31(self):
        manifest = self._manifest(coverage=[dict(
            season=2001, teams=31, rated_teams=31, games=259, finals=259,
            complete=True, played_teams=31)])
        assert publish_gate.check(manifest) == []

    def test_another_sport_is_refused_outright(self):
        assert any('is not nfl' in reason for reason in
                   publish_gate.check(self._manifest(sport='ncaambb')))

    def test_a_future_as_of_time_is_refused(self):
        future = (datetime.now(timezone.utc)
                  + timedelta(hours=2)).isoformat().replace('+00:00', 'Z')
        assert any('future' in reason for reason in
                   publish_gate.check(self._manifest(generated_at=future)))

    def test_an_old_as_of_time_is_not_refused(self):
        assert publish_gate.check(
            self._manifest(generated_at='2020-01-01T00:00:00Z')) == []

    def test_a_leaked_holdout_is_refused(self):
        manifest = self._manifest()
        manifest['model']['fitted_on'] = [2025]
        assert any('leaked' in reason
                   for reason in publish_gate.check(manifest))

    def test_a_one_season_holdout_is_too_small(self):
        manifest = self._manifest()
        manifest['model']['holdout_games'] = 285
        assert any('too small a holdout' in reason
                   for reason in publish_gate.check(manifest))

    def test_a_model_no_better_than_the_home_team_is_refused(self):
        manifest = self._manifest()
        manifest['model']['holdout_brier'] = 0.2472
        assert any('picking the home team' in reason
                   for reason in publish_gate.check(manifest))

    def test_a_missing_complete_flag_is_judged_strictly(self):
        manifest = self._manifest(coverage=[dict(
            season=2026, teams=32, rated_teams=12, games=40, finals=40,
            played_teams=12)])
        reasons = publish_gate.check(manifest)
        assert any('below 240' in reason for reason in reasons)

    def test_a_file_that_does_not_hash_to_its_stamp_is_refused(self,
                                                               tmp_path):
        (tmp_path / 'seasons').mkdir()
        (tmp_path / 'seasons' / '2025.js').write_text('window.NFL_SEASONS={};')
        reasons = publish_gate.check(self._manifest(), root=tmp_path)
        assert any('hashes to' in reason for reason in reasons)

    def test_an_empty_file_never_replaces_a_valid_one(self, tmp_path):
        (tmp_path / 'seasons').mkdir()
        (tmp_path / 'seasons' / '2025.js').write_text('')
        reasons = publish_gate.check(self._manifest(), root=tmp_path)
        assert any('empty' in reason for reason in reasons)
