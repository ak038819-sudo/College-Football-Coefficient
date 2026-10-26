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
                overtime=0, divisional_game=0, roof='outdoors',
                surface='grass', stadium_id=None, stadium=None,
                home_coach=f'{home} Coach', away_coach=f'{away} Coach',
                # A posted line on every game, so the market comparison and
                # the against-the-spread record have something to measure.
                spread_line=3.0, total_line=44.5,
                home_moneyline=-160, away_moneyline=140,
                home_qb_id=None, away_qb_id=None,
                home_qb_name=f'{home} QB', away_qb_name=f'{away} QB',
                referee=None, home_rest=7, away_rest=7, temp=62, wind=5,
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
        manifest, payloads, teams, history = build(league)
        written = export_module.write(manifest, payloads, teams, history,
                                      tmp_path / 'out')
        assert publish_gate.check(written, root=tmp_path / 'out') == []
        assert (tmp_path / 'out' / 'manifest.json').exists()

    def test_the_manifest_states_the_model_and_its_held_out_score(
            self, league, tmp_path):
        manifest, _, _, _ = build(league)
        model = manifest['model']
        assert model['name'] == 'nfl_elo_v1'
        assert model['holdout_brier'] == 0.2237
        assert model['held_out'] == [2025]
        assert len(model['config_sha256']) == 64

    def test_every_file_carries_a_hash_of_its_own_bytes(self, league,
                                                        tmp_path):
        manifest, payloads, teams, history = build(league)
        written = export_module.write(manifest, payloads, teams, history,
                                      tmp_path / 'out')
        for path, version in publish_gate.manifest_files(written):
            assert version == publish_gate.file_version(
                tmp_path / 'out' / path)

    def test_a_refused_export_writes_nothing(self, league, tmp_path):
        out = tmp_path / 'out'
        manifest, payloads, teams, history = build(league)
        manifest['model']['holdout_brier'] = 0.3
        with pytest.raises(export_module.PublishRefused):
            export_module.write(manifest, payloads, teams, history, out)
        assert not out.exists()

    def test_a_season_in_progress_is_marked_and_not_judged_as_finished(
            self, league, tmp_path):
        # Half of 2026's games unplayed, as a live season looks in October.
        league.execute(
            'UPDATE game SET home_points = NULL, away_points = NULL,'
            ' rateable = 0 WHERE season = 2026 AND provider_id LIKE ?',
            ('%\\_W%', ))
        league.commit()
        manifest, payloads, teams, history = build(
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


class TestPregameNumbersOnAGame:
    """The pregame rating and win probability the card draws.

    These numbers already existed in `rating_game`; nothing but the export
    carried them, so every card on the site was a pair of names and a score.
    """

    def test_a_played_game_carries_the_stored_chain_not_a_fresh_sum(
            self, league):
        model = export_module.model_row(league)
        games = export_module.season_games(league, 2025, model['model_id'])
        rated = [game for game in games if 'home_wp' in game]
        assert len(rated) == len(games)
        stored = {row['game_key']: row for row in league.execute(
            'SELECT game_key, home_pregame, away_pregame, home_expected'
            '  FROM rating_game WHERE model_id = ?', (model['model_id'],))}
        for game in rated:
            row = stored[game['id']]
            assert game['home_elo'] == round(row['home_pregame'], 1)
            assert game['away_elo'] == round(row['away_pregame'], 1)
            assert game['home_wp'] == round(row['home_expected'], 4)

    def test_without_a_model_a_game_carries_no_pregame_numbers(self, league):
        games = export_module.season_games(league, 2025)
        assert games
        assert not any('home_wp' in game for game in games)

    def test_an_unplayed_game_is_priced_off_the_board(self, league):
        """The one card a reader most wants a number on.

        The replay only rates games that have been played, so without this
        an upcoming game reaches the page with no probability at all.
        """
        model = export_module.model_row(league)
        rows = [dict(team=f'nfl:nflverse:franchise:{CODES[0]}', rating=1700.0),
                dict(team=f'nfl:nflverse:franchise:{CODES[1]}', rating=1500.0)]
        games = [dict(home=rows[0]['team'], away=rows[1]['team'],
                      home_points=None, away_points=None)]
        export_module.expect_upcoming(games, rows, json.loads(model['config']))
        game = games[0]
        assert game['wp_estimated'] == 1
        assert game['home_elo'] == 1700.0
        assert game['away_elo'] == 1500.0
        # 1700 + 50 against 1500 is a 250-point edge on a 400-point scale.
        assert game['home_wp'] == round(
            1.0 / (1.0 + 10 ** (-250.0 / 400.0)), 4)

    def test_the_home_advantage_is_actually_applied(self, league):
        """`rating_model.config` IS the elo block.

        Reaching for a nested 'elo' key here finds nothing, and the silent
        result is every upcoming game played at a home advantage of zero —
        two equal teams at 50% instead of the league's measured 57%.
        """
        model = export_module.model_row(league)
        rows = [dict(team='a', rating=1500.0), dict(team='b', rating=1500.0)]
        games = [dict(home='a', away='b', home_points=None, away_points=None)]
        export_module.expect_upcoming(games, rows, json.loads(model['config']))
        assert games[0]['home_wp'] > 0.5

    def test_a_neutral_game_gets_no_home_edge(self, league):
        model = export_module.model_row(league)
        rows = [dict(team='a', rating=1500.0), dict(team='b', rating=1500.0)]
        games = [dict(home='a', away='b', home_points=None, away_points=None,
                      neutral=1)]
        export_module.expect_upcoming(games, rows, json.loads(model['config']))
        assert games[0]['home_wp'] == 0.5

    def test_a_stored_probability_is_never_overwritten(self, league):
        model = export_module.model_row(league)
        rows = [dict(team='a', rating=1900.0), dict(team='b', rating=1100.0)]
        games = [dict(home='a', away='b', home_points=None, away_points=None,
                      home_wp=0.25, home_elo=1.0, away_elo=2.0)]
        export_module.expect_upcoming(games, rows, json.loads(model['config']))
        assert games[0]['home_wp'] == 0.25
        assert 'wp_estimated' not in games[0]

    def test_a_played_game_the_model_declined_is_not_priced_after_the_fact(
            self, league):
        """A final score is a result, not a forecast.

        Pricing a game that has already been played off today's ratings would
        put a confident number on a card next to the score that refutes it.
        """
        model = export_module.model_row(league)
        rows = [dict(team='a', rating=1900.0), dict(team='b', rating=1100.0)]
        games = [dict(home='a', away='b', home_points=3, away_points=31)]
        export_module.expect_upcoming(games, rows, json.loads(model['config']))
        assert 'home_wp' not in games[0]
