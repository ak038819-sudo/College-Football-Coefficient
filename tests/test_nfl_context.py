"""Everything phase 1 added to the NFL track.

The widened game row, head coaches and wins above expectation, the model
against the betting market, and the real playoff field.

These drive the real modules. Where a number is asserted it was measured
against the committed archive first and the measurement is stated, because a
test that asserts whatever the code happens to return is a record of a bug as
readily as of a fix.
"""

import json
import sqlite3
import subprocess
from datetime import datetime, timedelta, timezone

import pytest

from src.nfl import coaches as coaches_module
from src.nfl import export as export_module
from src.nfl import games as games_module
from src.nfl import identity, market, playoff, publish_gate, store
from src.nfl.build_ratings import build as build_ratings
from src.nfl.identity import FRANCHISE_CODES, franchise_key

from test_nfl_data import COLUMNS, HEADER, archive, row  # noqa: F401


CODES = sorted(FRANCHISE_CODES)
CONFIG = dict(elo=dict(k=48, home_advantage=50, offseason_retention=0.5,
                       margin_model='mov', margin_exponent=0.75,
                       margin_min=0.25, margin_max=4.0,
                       reference_margin=11.67, points_per_elo=0.0527,
                       initial_rating=1500.0, scale=400.0))
REPORT = dict(fit_seasons=[2024], holdout_seasons=[2025],
              holdout=dict(brier=0.2237, games=1424))


class TestTheWidenedGameRow:
    def test_the_columns_match_the_schema_file_exactly_and_in_order(self):
        """Three places used to list these and two of them drifted.

        The INSERT, the ON CONFLICT clause and the value tuple are now
        generated from GAME_COLUMNS, so this is the one place the list can go
        wrong: a column added to the schema file and not here is stored by
        nothing.
        """
        body = store.SCHEMA_SQL.read_text(encoding='utf-8')
        body = body.split('CREATE TABLE IF NOT EXISTS game (')[1]
        body = body.split(');')[0]
        declared = [line.strip().split()[0] for line in body.splitlines()
                    if line.strip() and not line.strip().startswith('--')]
        assert declared == list(store.GAME_COLUMNS)

    def test_the_context_columns_are_parsed_rather_than_dropped(self):
        parsed = games_module.parse(archive(row(
            '2024_01_BAL_KC', 2024, 'REG', 1, '2024-09-05', 'BAL', 20,
            'KC', 27, total=47, home_coach='Andy Reid',
            away_coach='John Harbaugh', spread_line=3.0, total_line=46.0,
            home_moneyline=-148, away_moneyline=124, temp=67, wind=8,
            home_rest=7, away_rest=7, referee='Shawn Hochuli',
            home_qb_name='Patrick Mahomes', away_qb_name='Lamar Jackson',
            home_qb_id='00-0033873')))[0]
        assert parsed['home_coach'] == 'Andy Reid'
        assert parsed['away_coach'] == 'John Harbaugh'
        assert parsed['spread_line'] == 3.0
        assert parsed['total_line'] == 46.0
        assert parsed['home_moneyline'] == -148
        assert parsed['away_moneyline'] == 124
        assert parsed['temp'] == 67 and parsed['wind'] == 8
        assert parsed['home_rest'] == 7 and parsed['away_rest'] == 7
        assert parsed['referee'] == 'Shawn Hochuli'
        assert parsed['home_qb_name'] == 'Patrick Mahomes'
        assert parsed['home_qb_id'] == '00-0033873'

    def test_a_blank_is_absent_and_never_a_zero(self):
        """A dome game has no temperature. Reading a blank as 0 would put it
        at freezing, which is the `Number(null)` trap in another language."""
        parsed = games_module.parse(archive(row(
            '2024_01_LA_LV', 2024, 'REG', 1, '2024-09-08', 'LA', 10,
            'LV', 20, roof='dome')))[0]
        for field in ('temp', 'wind', 'spread_line', 'total_line',
                      'home_moneyline', 'away_moneyline', 'referee',
                      'home_coach', 'home_qb_name'):
            assert parsed[field] is None, field

    def test_a_total_contradicting_the_scores_is_refused(self):
        """Measured first: the stated total agrees with the scores on all
        7,369 archive games that carry both, so a disagreement is a fault."""
        with pytest.raises(games_module.ArchiveError, match='total'):
            games_module.parse(archive(row(
                '2024_01_BAL_KC', 2024, 'REG', 1, '2024-09-05', 'BAL', 20,
                'KC', 27, total=99)))

    def test_a_price_that_cannot_be_a_price_is_refused(self):
        """American odds are never 0 and never inside (-100, 100). The
        smallest magnitude in the 10,832 moneylines of the archive is exactly
        100, so a value in that band is a corrupted field."""
        with pytest.raises(games_module.ArchiveError, match='moneyline'):
            games_module.parse(archive(row(
                '2024_01_BAL_KC', 2024, 'REG', 1, '2024-09-05', 'BAL', 20,
                'KC', 27, home_moneyline=40, away_moneyline=-140)))
        # The boundary itself is a real price and must still load.
        parsed = games_module.parse(archive(row(
            '2024_01_BAL_KC', 2024, 'REG', 1, '2024-09-05', 'BAL', 20,
            'KC', 27, home_moneyline=-100, away_moneyline=100)))[0]
        assert parsed['home_moneyline'] == -100

    def test_a_total_line_of_zero_is_refused(self):
        with pytest.raises(games_module.ArchiveError, match='total line'):
            games_module.parse(archive(row(
                '2024_01_BAL_KC', 2024, 'REG', 1, '2024-09-05', 'BAL', 20,
                'KC', 27, total_line=0)))

    def test_a_source_that_drops_a_context_column_fails_loudly(self):
        """A missing column would otherwise read as a column full of blanks,
        which is a silent half-import rather than an error."""
        narrow = HEADER.replace(',home_coach', '')
        body = archive(row('2024_01_BAL_KC', 2024, 'REG', 1, '2024-09-05',
                           'BAL', 20, 'KC', 27)).splitlines()[1]
        index = list(COLUMNS).index('home_coach')
        parts = body.split(',')
        del parts[index]
        with pytest.raises(games_module.ArchiveError, match='home_coach'):
            games_module.parse(narrow + '\n' + ','.join(parts) + '\n')

    def test_a_row_short_of_a_column_is_refused_rather_than_nulled(self,
                                                                   tmp_path):
        connection = store.connect(tmp_path / 'nfl.db')
        parsed = games_module.parse(archive(row(
            '2024_01_BAL_KC', 2024, 'REG', 1, '2024-09-05', 'BAL', 20,
            'KC', 27)))
        store.upsert_team_stubs(connection, parsed)
        short = dict(parsed[0])
        del short['temp']
        with pytest.raises(KeyError, match='temp'):
            store.upsert_games(connection, [short])

    def test_the_values_survive_a_round_trip_through_the_database(
            self, tmp_path):
        connection = store.connect(tmp_path / 'nfl.db')
        parsed = games_module.parse(archive(row(
            '2024_01_BAL_KC', 2024, 'REG', 1, '2024-09-05', 'BAL', 20,
            'KC', 27, home_coach='Andy Reid', spread_line=3.0, temp=67,
            wind=8, home_moneyline=-148, away_moneyline=124)))
        store.upsert_team_stubs(connection, parsed)
        store.upsert_games(connection, parsed)
        stored = connection.execute(
            'SELECT home_coach, spread_line, temp, wind, home_moneyline'
            '  FROM game').fetchone()
        assert stored['home_coach'] == 'Andy Reid'
        assert stored['spread_line'] == 3.0
        assert stored['temp'] == 67 and stored['wind'] == 8
        assert stored['home_moneyline'] == -148

    def test_a_schema_1_database_gains_the_columns_rather_than_breaking(
            self, tmp_path):
        """`CREATE TABLE IF NOT EXISTS` does nothing to a table that exists,
        so without an explicit ALTER an old database keeps its 26-column
        `game` table and every INSERT fails on a column the schema declares.
        This drives the real previous schema out of git."""
        old = subprocess.run(
            ['git', 'show', 'HEAD:src/nfl/schema.sql'],
            capture_output=True, text=True, check=True).stdout
        assert 'home_coach' not in old, (
            'HEAD already carries the widened schema, so this test is no '
            'longer exercising a migration')
        path = tmp_path / 'nfl.db'
        legacy = sqlite3.connect(str(path))
        legacy.executescript(old)
        legacy.execute('INSERT INTO schema_version (version) VALUES (1)')
        legacy.commit()
        legacy.close()

        connection = store.connect(path)
        present = {r['name'] for r in
                   connection.execute('PRAGMA table_info(game)')}
        assert present == set(store.GAME_COLUMNS)
        assert connection.execute(
            'SELECT version FROM schema_version').fetchone()[0] == 2
        # And the widened INSERT works against the migrated table.
        parsed = games_module.parse(archive(row(
            '2024_01_BAL_KC', 2024, 'REG', 1, '2024-09-05', 'BAL', 20,
            'KC', 27, home_coach='Andy Reid')))
        store.upsert_team_stubs(connection, parsed)
        store.upsert_games(connection, parsed)
        assert connection.execute(
            'SELECT home_coach FROM game').fetchone()[0] == 'Andy Reid'


def coached_game(game_key, season, home, away, home_points, away_points,
                 *, home_coach, away_coach, postseason=0,
                 home_expected=0.5, rating_change=0.0):
    return dict(game_key=game_key, season=season, postseason=postseason,
                home_team_key=franchise_key(home),
                away_team_key=franchise_key(away),
                home_points=home_points, away_points=away_points,
                home_coach=home_coach, away_coach=away_coach,
                home_expected=home_expected, rating_change=rating_change)


class TestCoaches:
    def test_a_name_with_stray_whitespace_is_one_coach(self):
        assert identity.coach_key('Andy  Reid')[0] \
            == identity.coach_key('Andy Reid')[0]
        assert identity.coach_key(' Andy Reid ')[1] == 'Andy Reid'

    def test_an_empty_name_is_refused_rather_than_keyed(self):
        with pytest.raises(ValueError):
            identity.coach_key('   ')

    def test_both_coaches_are_credited_from_one_game(self):
        counted, names = coaches_module.tally([coached_game(
            'g1', 2024, 'KC', 'BAL', 27, 20, home_coach='Andy Reid',
            away_coach='John Harbaugh', home_expected=0.6)])
        reid = counted[(identity.coach_key('Andy Reid')[0], 2024,
                        franchise_key('KC'))]
        harbaugh = counted[(identity.coach_key('John Harbaugh')[0], 2024,
                            franchise_key('BAL'))]
        assert reid['wins'] == 1 and reid['losses'] == 0
        assert harbaugh['losses'] == 1 and harbaugh['wins'] == 0
        # The two expectations are the two sides of one probability.
        assert reid['expected_wins'] + harbaugh['expected_wins'] \
            == pytest.approx(1.0)

    def test_a_tie_is_half_a_win_on_both_sides_of_the_subtraction(self):
        counted, _ = coaches_module.tally([coached_game(
            'g1', 2024, 'NYG', 'WAS', 20, 20, home_coach='Home Coach',
            away_coach='Away Coach', home_expected=0.5)])
        for bucket in counted.values():
            assert bucket['ties'] == 1
            assert bucket['wins'] == 0 and bucket['losses'] == 0
        rows = [dict(coach_key=key, coach_name='x', season=season,
                     team_key=team, **bucket)
                for (key, season, team), bucket in counted.items()]
        # A tie against a 50% call is exactly expectation, not a loss.
        for row_ in rows:
            actual = row_['wins'] + row_['ties'] / 2.0
            assert actual - row_['expected_wins'] == pytest.approx(0.0)

    def test_a_midseason_change_splits_the_season_rather_than_averaging(self):
        counted, _ = coaches_module.tally([
            coached_game('g1', 2024, 'LV', 'KC', 10, 30,
                         home_coach='First Coach', away_coach='Andy Reid'),
            coached_game('g2', 2024, 'LV', 'DEN', 24, 20,
                         home_coach='Second Coach', away_coach='Sean Payton'),
        ])
        las_vegas = [key for key in counted
                     if key[2] == franchise_key('LV')]
        assert len(las_vegas) == 2
        assert {counted[key]['games'] for key in las_vegas} == {1}

    def test_a_game_with_no_coach_on_one_side_still_credits_the_other(self):
        counted, _ = coaches_module.tally([coached_game(
            'g1', 2001, 'KC', 'BAL', 27, 20, home_coach='Named Coach',
            away_coach='')])
        assert len(counted) == 1
        assert next(iter(counted))[2] == franchise_key('KC')

    def test_a_career_counts_distinct_seasons_not_rows(self):
        """A coach who moved midseason has two rows for one season. Counting
        rows would give every such career an extra year."""
        rows = [
            dict(coach_key='c', coach_name='Mover', season=2024,
                 team_key=franchise_key('KC'), games=5, wins=3, losses=2,
                 ties=0, postseason_games=0, postseason_wins=0,
                 expected_wins=2.5, wins_above=0.5, elo_change=1.0),
            dict(coach_key='c', coach_name='Mover', season=2024,
                 team_key=franchise_key('LV'), games=5, wins=1, losses=4,
                 ties=0, postseason_games=0, postseason_wins=0,
                 expected_wins=2.5, wins_above=-1.5, elo_change=-1.0),
        ]
        career = coaches_module.careers(rows)[0]
        assert career['seasons'] == 1
        assert career['games'] == 10
        assert career['wins_above'] == pytest.approx(-1.0)
        assert career['teams'] == sorted(
            {franchise_key('KC'), franchise_key('LV')})

    def test_wins_above_expectation_is_actual_minus_the_pregame_sum(self,
                                                                   tmp_path):
        """Driven through the database, not computed twice here: three games
        the model gave a 60% chance of winning, two of them won, is
        2 - 1.8 = +0.2."""
        connection = store.connect(tmp_path / 'nfl.db')
        store.upsert_team_stubs(connection, [
            dict(season=2024, home_code='KC', away_code='BAL'),
            dict(season=2024, home_code='KC', away_code='DEN'),
            dict(season=2024, home_code='KC', away_code='LV')])
        model_id = store.record_rating_model(
            connection, 'test', CONFIG['elo'], held_out='[2025]',
            holdout_brier=0.22, holdout_games=600)
        parsed = games_module.parse(archive(
            row('2024_01_BAL_KC', 2024, 'REG', 1, '2024-09-05', 'BAL', 20,
                'KC', 27, home_coach='Andy Reid', away_coach='A'),
            row('2024_02_DEN_KC', 2024, 'REG', 2, '2024-09-12', 'DEN', 30,
                'KC', 10, home_coach='Andy Reid', away_coach='B'),
            row('2024_03_LV_KC', 2024, 'REG', 3, '2024-09-19', 'LV', 3,
                'KC', 24, home_coach='Andy Reid', away_coach='C')))
        store.upsert_team_stubs(connection, parsed)
        store.upsert_games(connection, parsed)
        store.upsert_rating_games(connection, model_id, [
            dict(game_key=game['game_key'], home_pregame=1500.0,
                 away_pregame=1500.0, home_expected=0.6, home_postgame=1500.0,
                 away_postgame=1500.0, rating_change=1.0, multiplier=1.0,
                 counted=1) for game in parsed])
        rows = coaches_module.seasons(connection, model_id)
        reid = [r for r in rows if r['coach_name'] == 'Andy Reid'][0]
        assert reid['games'] == 3 and reid['wins'] == 2
        assert reid['expected_wins'] == pytest.approx(1.8)
        assert reid['wins_above'] == pytest.approx(0.2)
        # The chain's change is the home team's; the away side gets its
        # negative, so the three away coaches are -1.0 each.
        assert reid['elo_change'] == pytest.approx(3.0)
        others = [r for r in rows if r['coach_name'] != 'Andy Reid']
        assert all(r['elo_change'] == pytest.approx(-1.0) for r in others)


class TestMarket:
    def test_american_odds_become_probabilities(self):
        # -150 pays 100 on a 150 stake: 150/250 = 0.6.
        assert market.implied_probability(-150) == pytest.approx(0.6)
        assert market.implied_probability(150) == pytest.approx(0.4)
        assert market.implied_probability(100) == pytest.approx(0.5)
        assert market.implied_probability(None) is None

    def test_the_overround_is_removed_and_the_pair_sums_to_one(self):
        home = market.devig(-148, 124)
        # Raw: 0.5968 + 0.4464 = 1.0432, a 4.3% book.
        assert home == pytest.approx(0.5721, abs=5e-5)
        away = market.devig(124, -148)
        assert home + away == pytest.approx(1.0)

    def test_one_price_alone_is_not_a_probability(self):
        """A single price carries the vig with it and there is nothing to
        divide it out by."""
        assert market.devig(-148, None) is None
        assert market.devig(None, 124) is None

    def test_a_pick_landing_exactly_on_the_line_is_no_pick(self):
        assert market.ats_result(3.0, 3.0, 7) == market.PUSH
        # And a result landing exactly on the line is a push however the
        # model leaned.
        assert market.ats_result(6.0, 3.0, 3) == market.PUSH

    def test_the_spread_sign_is_the_home_margin(self):
        """Measured on the archive: mean(result - spread_line) is 0.07 over
        7,369 games, and 4.56 with the sign flipped. Positive means the home
        team is favoured, so backing the home team at -3 wins on a 7-point
        home win and loses on a 1-point one."""
        assert market.ats_result(6.0, 3.0, 7) == 'win'
        assert market.ats_result(6.0, 3.0, 1) == 'loss'
        assert market.ats_result(-6.0, 3.0, 1) == 'win'

    def test_the_two_brier_scores_are_measured_on_the_same_games(self,
                                                                 tmp_path):
        """The model's all-games Brier against the market's priced-games
        Brier would be a comparison between two different sets of games.
        `model_brier_priced` exists for exactly that reason."""
        connection = store.connect(tmp_path / 'nfl.db')
        parsed = games_module.parse(archive(
            row('2024_01_BAL_KC', 2024, 'REG', 1, '2024-09-05', 'BAL', 20,
                'KC', 27, spread_line=3.0, home_moneyline=-148,
                away_moneyline=124),
            # No prices on this one: rated, but not priced.
            row('2024_02_DEN_KC', 2024, 'REG', 2, '2024-09-12', 'DEN', 30,
                'KC', 10, spread_line=3.0)))
        store.upsert_team_stubs(connection, parsed)
        store.upsert_games(connection, parsed)
        model_id = store.record_rating_model(
            connection, 'test', CONFIG['elo'], held_out='[2025]',
            holdout_brier=0.22, holdout_games=600)
        store.upsert_rating_games(connection, model_id, [
            dict(game_key=game['game_key'], home_pregame=1500.0,
                 away_pregame=1500.0, home_expected=0.6, home_postgame=1500.0,
                 away_postgame=1500.0, rating_change=1.0, multiplier=1.0,
                 counted=1) for game in parsed])
        rows = market.seasons(connection, model_id, CONFIG)
        assert len(rows) == 1
        season = rows[0]
        assert season['rated_games'] == 2
        assert season['priced_games'] == 1
        # One win at 0.6 and one loss at 0.6: (0.4^2 + 0.6^2) / 2 = 0.26.
        assert season['model_brier'] == pytest.approx(0.26)
        # The priced game alone is the home win: 0.4^2.
        assert season['model_brier_priced'] == pytest.approx(0.16)
        assert season['spread_games'] == 2

    def test_the_totals_weight_by_games_and_not_by_season(self):
        """Averaging 28 season Brier scores would weight 1999's 267 games the
        same as a 93-game season in progress."""
        rows = [
            dict(season=2024, rated_games=300, model_brier=0.20,
                 model_accuracy=0.7, priced_games=300,
                 model_brier_priced=0.20, market_brier=0.19,
                 market_accuracy=0.7, spread_games=300, ats_wins=150,
                 ats_losses=150, ats_pushes=0, model_margin_mae=10.0,
                 market_margin_mae=10.0),
            dict(season=2025, rated_games=100, model_brier=0.24,
                 model_accuracy=0.6, priced_games=100,
                 model_brier_priced=0.24, market_brier=0.23,
                 market_accuracy=0.6, spread_games=100, ats_wins=60,
                 ats_losses=40, ats_pushes=0, model_margin_mae=11.0,
                 market_margin_mae=10.0),
        ]
        whole = market.totals(rows)
        # (0.20*300 + 0.24*100) / 400 = 0.21, not the unweighted 0.22.
        assert whole['model_brier'] == pytest.approx(0.21)
        assert whole['rated_games'] == 400
        assert whole['ats_wins'] == 210


def standings_board(records):
    """A rating board from {code: (wins, losses)}, aligned and rated."""
    rows = []
    for code, (wins, losses) in records.items():
        conference, division = identity.alignment(code, 2024)
        rows.append(dict(team=franchise_key(code), code=code,
                         conference=conference, division=division,
                         rating=1500.0 + wins - losses, wins=wins,
                         losses=losses, ties=0))
    return rows


def round_robin(records, season=2024):
    """Games that produce exactly those records, every team playing the same
    number. Each team plays `wins` home wins and `losses` home losses against
    a rotating opponent, which keeps head-to-head out of it unless a test
    puts it in."""
    codes = list(records)
    games, index = [], 0
    for code, (wins, losses) in records.items():
        for outcome_index in range(wins + losses):
            other = codes[(codes.index(code) + 1 + outcome_index)
                          % len(codes)]
            if other == code:
                continue
            won = outcome_index < wins
            index += 1
            games.append(dict(
                id=f'g{index}', season=season, postseason=0,
                home=franchise_key(code), away=franchise_key(other),
                home_points=24 if won else 10,
                away_points=10 if won else 24))
    return games


class TestPlayoffField:
    def test_the_modern_format_is_seven_seeds_and_one_bye(self):
        era = playoff.format_for(2024)
        assert era['seeds'] == 7
        assert era['byes'] == 1
        assert era['division_winners'] == 4

    def test_the_previous_format_is_not_overwritten_by_the_current_one(self):
        era = playoff.format_for(2019)
        assert era['seeds'] == 6
        assert era['byes'] == 2

    def test_the_three_seasons_before_the_realignment_get_no_field(self):
        """The league had six divisions and the stored alignment does not
        cover them. A guessed field would quietly rewrite three seasons."""
        assert playoff.format_for(2001) is None
        assert playoff.field([], [], 2001) is None

    def test_a_conference_sends_exactly_the_format_s_seeds(self):
        records = {}
        for index, code in enumerate(sorted(identity.ALIGNMENT)):
            records[code] = (12 - index % 10, 5 + index % 10)
        board = standings_board(records)
        field = playoff.field(board, round_robin(records), 2024)
        assert len(field['conferences']) == 2
        for group in field['conferences']:
            assert len(group['seeds']) == 7
            assert sum(1 for seed in group['seeds'] if seed['bye']) == 1
            assert sum(1 for seed in group['seeds']
                       if seed['division_winner']) == 4
            # Every division winner is seeded ahead of every wild card, even
            # where a wild card has the better record.
            assert [seed['seed'] for seed in group['seeds']
                    if seed['division_winner']] == [1, 2, 3, 4]

    def test_a_division_winner_outranks_a_better_wild_card(self):
        """The 2024 NFC is the real case: Tampa Bay at 10-7 was seeded 3 and
        Minnesota at 14-3 was seeded 5."""
        records = {code: (4, 13) for code in identity.ALIGNMENT}
        records['TB'] = (10, 7)     # NFC South, weak division
        records['MIN'] = (14, 3)    # NFC North, behind Detroit
        records['DET'] = (15, 2)
        board = standings_board(records)
        field = playoff.field(board, round_robin(records), 2024)
        nfc = [g for g in field['conferences'] if g['conference'] == 'NFC'][0]
        seeds = {seed['team']: seed['seed'] for seed in nfc['seeds']}
        assert seeds[franchise_key('TB')] < seeds[franchise_key('MIN')]

    def test_a_head_to_head_sweep_breaks_a_tie_nothing_else_can(self):
        """Two teams identical on every term the ladder checks before head to
        head, and identical on every term after it.

        Pittsburgh and Miami each finish 1-1 overall, 1-1 in conference and
        0-0 in division, on equal ratings, and Pittsburgh beat Miami. Every
        other tiebreak is level, and the final fallback is the team key,
        which sorts Miami FIRST — so this fails unless head to head is what
        decides it. (A tie inside one division cannot test this: the head to
        head game is also a division game, so division record separates the
        same two teams the same way and the test passes with head to head
        removed entirely.)
        """
        align = {franchise_key(code): identity.alignment(code, 2024)
                 for code in identity.ALIGNMENT}
        games = [
            dict(id='h2h', season=2024, postseason=0,
                 home=franchise_key('PIT'), away=franchise_key('MIA'),
                 home_points=30, away_points=10),
            # Pittsburgh's other game is against an AFC East team, Miami's
            # against an AFC North team, so neither is a division game and
            # both carry one conference win and one conference loss.
            dict(id='pit-loss', season=2024, postseason=0,
                 home=franchise_key('BUF'), away=franchise_key('PIT'),
                 home_points=24, away_points=10),
            dict(id='mia-win', season=2024, postseason=0,
                 home=franchise_key('MIA'), away=franchise_key('CLE'),
                 home_points=24, away_points=10),
        ]
        tally = playoff.records(games, align)
        pit, mia = franchise_key('PIT'), franchise_key('MIA')
        assert playoff.win_pct(tally[pit]) == playoff.win_pct(tally[mia])
        for field_ in ('conference_wins', 'conference_games',
                       'division_wins', 'division_games'):
            assert tally[pit][field_] == tally[mia][field_], field_
        assert mia < pit, 'the key fallback must point the other way'
        ratings = {pit: 1500.0, mia: 1500.0}
        assert playoff._order([mia, pit], tally, ratings) == [pit, mia]

    def test_a_matchup_nobody_played_does_not_decide_a_tie(self):
        """The league calls an unplayed head to head "not applicable". Scoring
        it 0.000 would decide a tie on a game that never happened."""
        align = {franchise_key(code): identity.alignment(code, 2024)
                 for code in identity.ALIGNMENT}
        tally = playoff.records([
            dict(id='a', season=2024, postseason=0,
                 home=franchise_key('MIA'), away=franchise_key('CLE'),
                 home_points=24, away_points=10),
            dict(id='b', season=2024, postseason=0,
                 home=franchise_key('PIT'), away=franchise_key('BUF'),
                 home_points=24, away_points=10),
        ], align)
        group = {franchise_key('MIA'), franchise_key('PIT')}
        assert playoff._head_to_head(franchise_key('MIA'), group,
                                     tally) is None

    def test_the_postseason_does_not_seed_the_postseason(self):
        """A seed is earned in the regular season. Counting January would
        seed a team on the strength of the bracket it is being seeded into."""
        records = {code: (8, 9) for code in identity.ALIGNMENT}
        games = round_robin(records)
        tally = playoff.records(games, {
            franchise_key(code): identity.alignment(code, 2024)
            for code in identity.ALIGNMENT})
        before = dict(tally[franchise_key('KC')])
        games.append(dict(id='sb', season=2024, postseason=1,
                          home=franchise_key('KC'),
                          away=franchise_key('PHI'),
                          home_points=40, away_points=0))
        after = playoff.records(games, {
            franchise_key(code): identity.alignment(code, 2024)
            for code in identity.ALIGNMENT})[franchise_key('KC')]
        assert after['games'] == before['games']
        assert after['wins'] == before['wins']

    def test_a_season_in_progress_is_marked_as_unfinished(self):
        records = {code: (3, 2) for code in identity.ALIGNMENT}
        games = round_robin(records)
        games.append(dict(id='later', season=2024, postseason=0,
                          home=franchise_key('KC'), away=franchise_key('LV'),
                          home_points=None, away_points=None))
        field = playoff.field(standings_board(records), games, 2024)
        assert field['complete'] is False
        assert field['games_played'] < field['games_scheduled']


def season_games(season, *, finals=True, start_day=10):
    """A full season for the export fixture, with context on every game."""
    rows, day = [], datetime(season, 9, start_day, tzinfo=timezone.utc)
    for i, home in enumerate(CODES):
        for away in CODES[i + 1:]:
            day = day + timedelta(hours=3)
            points = (24, 17) if finals else (None, None)
            # Every other game indoors with no reading, so a test can tell
            # "absent" from "zero" in the exported payload.
            indoors = (i % 2 == 0)
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
                overtime=0, divisional_game=0,
                roof='dome' if indoors else 'outdoors',
                surface='turf', stadium_id=None, stadium=None,
                home_coach=f'{home} Coach', away_coach=f'{away} Coach',
                spread_line=3.0, total_line=44.5,
                home_moneyline=-160, away_moneyline=140,
                home_qb_id=None, away_qb_id=None,
                home_qb_name=None, away_qb_name=None, referee=None,
                home_rest=7, away_rest=7,
                temp=None if indoors else 62,
                wind=None if indoors else 5,
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
    summary = build_ratings(connection, CONFIG, report=REPORT)
    coaches_module.build(connection, summary['model_id'])
    market.build(connection, summary['model_id'], config=CONFIG)
    return connection


class TestTheExport:
    def test_the_export_carries_a_history_file_the_manifest_names(
            self, league, tmp_path):
        manifest, payloads, teams, history = export_module.build(league)
        out = tmp_path / 'out'
        written = export_module.write(manifest, payloads, teams, history, out)
        assert 'history' in written['files']
        body = (out / 'history.js').read_text(encoding='utf-8')
        assert body.startswith('window.NFL_HISTORY=')
        payload = json.loads(body.split('=', 1)[1].rstrip(';\n'))
        assert set(payload) == {'conferences', 'track_record', 'coaches',
                                'coach_seasons'}
        assert payload['coaches'], 'no coach careers were exported'
        assert payload['track_record']['seasons'], 'no track record'
        # And the gate checks it like any other named file.
        assert publish_gate.check_files(written, out) == []

    def test_the_schema_version_is_stated_and_both_are_accepted(self):
        """The published export on main is a version 1 export and this gate
        runs against it in CI before the refresh that rewrites it. Refusing 1
        would fail the publish check on data that is perfectly good."""
        assert publish_gate.SCHEMA_VERSION == 2
        assert publish_gate.SUPPORTED_SCHEMA_VERSIONS == (1, 2)
        for version in (1, 2):
            assert publish_gate.check_schema(
                dict(schema_version=version, sport='nfl')) == []
        assert publish_gate.check_schema(
            dict(schema_version=3, sport='nfl')) != []

    def test_the_browser_gate_accepts_exactly_the_versions_python_does(self):
        """Two gates, one answer. A reader that refused what the build writes
        would blank the page for everyone."""
        body = (store.SCHEMA_SQL.parent.parent.parent / 'ui'
                / 'nfl_loader.js').read_text(encoding='utf-8')
        line = [part for part in body.splitlines()
                if 'var SUPPORTED_SCHEMA_VERSIONS' in part][0]
        stated = json.loads(line.split('=', 1)[1].strip().rstrip(';'))
        assert stated == list(publish_gate.SUPPORTED_SCHEMA_VERSIONS)

    def test_a_game_with_no_reading_carries_no_field_rather_than_a_zero(
            self, league):
        _, payloads, _, _ = export_module.build(league)
        games = payloads[2026]['games']
        indoors = [g for g in games if g.get('roof') == 'dome']
        outdoors = [g for g in games if g.get('roof') == 'outdoors']
        assert indoors and outdoors
        assert all('temp' not in g and 'wind' not in g for g in indoors)
        assert all(g['temp'] == 62 and g['wind'] == 5 for g in outdoors)

    def test_the_context_the_archive_carried_reaches_the_payload(self,
                                                                 league):
        _, payloads, _, _ = export_module.build(league)
        game = payloads[2026]['games'][0]
        assert game['home_coach'].endswith(' Coach')
        assert game['spread'] == 3.0 and game['total_line'] == 44.5
        assert game['home_ml'] == -160 and game['away_ml'] == 140
        assert game['home_rest'] == 7 and game['away_rest'] == 7

    def test_a_season_payload_carries_its_own_playoff_field_and_coaches(
            self, league):
        _, payloads, _, _ = export_module.build(league)
        payload = payloads[2026]
        assert payload['playoff']['seeds'] == 7
        assert payload['coaches'], 'no coach rows for the season'
        assert all(row['season'] == 2026 for row in payload['coaches'])

    def test_the_conference_history_begins_at_the_realignment(self, league):
        _, _, _, history = export_module.build(league)
        seasons = [row['season'] for row in history['conferences']]
        assert seasons and min(seasons) >= identity.ALIGNMENT_FROM
        first = history['conferences'][0]
        assert {entry['conference'] for entry in first['conferences']} \
            == set(identity.CONFERENCES)
