"""The basketball export contract and the gate that refuses to publish a sample.

The plan's rule is that a BYU sample must never be published as national
coverage. So the tests that matter here are not "does it write a file" but the
ones a wrong export would otherwise sneak past: a one-team pilot, a model with
no held-out score, a season whose holdout was also fitted on, a manifest
pointing at a file that is missing, empty, or no longer the file it describes.

A national-sized fixture is built rather than the gate's floors lowered. A test
that relaxes the threshold it is testing proves nothing.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ncaambb import export as exporter
from src.ncaambb import publish_gate as gate

# Deliberately a past moment: write() checks the manifest against the real
# clock, and a fixture stamped in the future would be refused for that reason
# rather than the one each test is about.
NOW = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)


# --- fixtures ---------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    from src.ncaambb.store import connect
    connection = connect(tmp_path / 'ncaambb.db')
    yield connection
    connection.close()


def national(connection, season=2026, teams=320, games_each=14, *,
             division_one=None, model=True):
    """A season the size of a real one: 320 Division I teams, 2,240 games.

    The gate's floors are 300 teams and 2,000 games, so this fixture clears
    them the way an actual season does rather than by the floors being moved.
    """
    names = [f'T{index:03d}' for index in range(teams)]
    d1 = teams if division_one is None else division_one
    for index, name in enumerate(names):
        connection.execute(
            'INSERT OR IGNORE INTO team (team_key, provider_id, school,'
            ' display_name, abbreviation, primary_color)'
            ' VALUES (?, ?, ?, ?, ?, ?)',
            (name, 1000 + index, f'School {index}', f'School {index}',
             name, '#101010'))
        connection.execute(
            'INSERT OR IGNORE INTO team_season (team_key, season,'
            ' season_label, conference, conference_id, division_one)'
            ' VALUES (?, ?, ?, ?, ?, ?)',
            (name, season, f'{season - 1}-{season % 100:02d}',
             f'Conf {index % 10}', index % 10, 1 if index < d1 else 0))
    count = 0
    for step in range(1, games_each + 1):
        for index, home in enumerate(names):
            away = names[(index + step) % teams]
            if home == away:
                continue
            count += 1
            connection.execute(
                'INSERT INTO game (game_key, provider_id, season,'
                ' season_label, season_type, status, start_date,'
                ' start_date_utc, start_time_tbd, neutral_site,'
                ' home_team_key, away_team_key, home_points, away_points,'
                ' rateable, division_one, exclusions)'
                " VALUES (?, ?, ?, ?, 'regular', 'final', ?, ?, 0, 0, ?, ?,"
                " ?, ?, 1, 1, '[]')",
                (f'{season}-{count}', count, season,
                 f'{season - 1}-{season % 100:02d}',
                 f'{season - 1}-11-{(step % 28) + 1:02d}',
                 f'{season - 1}-11-{(step % 28) + 1:02d}',
                 home, away, 70 + (index % 7), 60 + (step % 5)))
    connection.commit()
    if model:
        store_model(connection, season)
    return names


def store_model(connection, season, *, fitted=None, held=None, brier=0.1866,
                holdout_games=5752, name='ncaambb_elo_v1'):
    from src.ncaambb.store import record_rating_model, upsert_team_ratings
    config = dict(k=45, home_advantage=100, offseason_retention=0.8,
                  margin_model='mov', reference_margin=11.95)
    model_id = record_rating_model(
        connection, name=name, config=config,
        fitted_on=fitted if fitted is not None else [season - 1],
        held_out=held if held is not None else [season],
        holdout_brier=brier, holdout_games=holdout_games)
    rows = [(row[0], season, 1500.0 + index, 14) for index, row
            in enumerate(connection.execute(
                'SELECT team_key FROM team_season WHERE season = ?'
                ' AND division_one = 1', (season,)).fetchall())]
    upsert_team_ratings(connection, model_id, rows)
    connection.commit()
    return model_id


def built(connection, **kwargs):
    return exporter.build(connection, now=NOW, **kwargs)


# --- the manifest the export describes --------------------------------------

def test_a_national_export_passes_the_gate_and_is_written(db, tmp_path):
    national(db)
    manifest, payloads, teams = built(db)
    written = exporter.write(manifest, payloads, teams, tmp_path / 'out')
    assert gate.check(written, root=tmp_path / 'out') == []
    assert (tmp_path / 'out' / 'manifest.json').exists()
    assert (tmp_path / 'out' / 'seasons' / '2026.js').exists()


def test_the_manifest_names_the_model_its_holdout_and_a_config_hash(db):
    national(db)
    manifest, _, _ = built(db)
    model = manifest['model']
    assert model['name'] == 'ncaambb_elo_v1'
    assert model['held_out'] == [2026] and model['fitted_on'] == [2025]
    assert len(model['config_sha256']) == 64
    assert model['holdout_brier'] == 0.1866


def test_two_exports_of_one_model_agree_on_the_config_hash(db):
    national(db)
    first, _, _ = built(db)
    second, _, _ = built(db)
    assert (first['model']['config_sha256']
            == second['model']['config_sha256'])


def test_a_different_configuration_hashes_differently(db):
    """The hash is what lets a reader tell two exports claiming one model
    apart. A hash of the name would pass every other test here."""
    national(db)
    baseline, _, _ = built(db)
    db.execute("UPDATE rating_model SET config = ?",
               (json.dumps(dict(k=20, home_advantage=100,
                                offseason_retention=0.8, margin_model='mov',
                                reference_margin=11.95)),))
    db.commit()
    changed, _, _ = built(db)
    assert (changed['model']['config_sha256']
            != baseline['model']['config_sha256'])


def test_the_board_is_ranked_and_unrated_teams_are_absent(db):
    names = national(db)
    db.execute('DELETE FROM team_rating WHERE team_key = ?', (names[0],))
    db.commit()
    _, payloads, _ = built(db)
    board = payloads[2026]['board']
    assert [row['rank'] for row in board[:3]] == [1, 2, 3]
    assert board[0]['rating'] >= board[1]['rating']
    assert names[0] not in {row['team'] for row in board}


def test_a_missing_score_stays_missing_in_the_export(db):
    """Football taught this project what a 0 standing in for "no score"
    costs downstream."""
    national(db)
    db.execute("UPDATE game SET status = 'scheduled', home_points = NULL,"
               " away_points = NULL WHERE game_key = '2026-1'")
    db.commit()
    _, payloads, _ = built(db)
    game = next(row for row in payloads[2026]['games']
                if row['id'] == '2026-1')
    assert game['home_points'] is None and game['away_points'] is None


def test_the_record_counts_finals_only(db):
    national(db)
    _, payloads, _ = built(db)
    before = sum(row['wins'] + row['losses'] for row in payloads[2026]['board'])
    db.execute("UPDATE game SET status = 'cancelled' WHERE game_key = '2026-1'")
    db.commit()
    _, payloads, _ = built(db)
    after = sum(row['wins'] + row['losses'] for row in payloads[2026]['board'])
    assert after == before - 2


def test_the_record_separates_division_one_from_everything_else(db):
    national(db)
    db.execute("UPDATE game SET division_one = 0 WHERE game_key = '2026-1'")
    db.commit()
    _, payloads, _ = built(db)
    rows = {row['team']: row for row in payloads[2026]['board']}
    game = db.execute('SELECT home_team_key, home_points, away_points'
                      " FROM game WHERE game_key = '2026-1'").fetchone()
    home = rows[game['home_team_key']]
    assert home['wins'] > home['d1_wins'] or home['losses'] > home['d1_losses']


# --- what the gate refuses --------------------------------------------------

def test_a_one_team_sample_is_refused(db, tmp_path):
    """THE test. A BYU pilot must never be published as national coverage."""
    national(db, teams=4, games_each=2)
    manifest, payloads, teams = built(db)
    with pytest.raises(exporter.PublishRefused) as refused:
        exporter.write(manifest, payloads, teams, tmp_path / 'out')
    assert any('sample' in reason for reason in refused.value.reasons)


def test_a_refused_export_writes_nothing_at_all(db, tmp_path):
    """A half-written directory is itself a way to publish a sample."""
    national(db, teams=4, games_each=2)
    manifest, payloads, teams = built(db)
    out = tmp_path / 'out'
    with pytest.raises(exporter.PublishRefused):
        exporter.write(manifest, payloads, teams, out)
    assert not out.exists()


def test_a_model_with_no_holdout_is_refused(db):
    national(db, model=False)
    store_model(db, 2026, held=[], brier=None, holdout_games=None)
    manifest, _, _ = built(db)
    reasons = gate.check(manifest)
    assert any('held-out' in reason for reason in reasons)


def test_a_holdout_the_fit_could_see_is_refused(db):
    """The leak, stated in the manifest rather than hidden in a log."""
    national(db, model=False)
    store_model(db, 2026, fitted=[2025, 2026], held=[2026])
    manifest, _, _ = built(db)
    assert any('leaked' in reason for reason in gate.check(manifest))


def test_a_holdout_score_no_better_than_a_coin_flip_is_refused(db):
    national(db, model=False)
    store_model(db, 2026, brier=0.25)
    assert any('coin flip' in reason
               for reason in gate.check(built(db)[0]))


def test_a_sample_sized_holdout_is_refused(db):
    national(db, model=False)
    store_model(db, 2026, holdout_games=12)
    assert any('sample-sized' in reason
               for reason in gate.check(built(db)[0]))


def test_an_unsupported_schema_is_refused_and_nothing_else_is_reported(db):
    """A manifest of an unknown shape cannot be checked field by field: every
    later check would be reading fields whose meaning is not agreed."""
    national(db)
    manifest, _, _ = built(db)
    manifest['schema_version'] = 99
    manifest['coverage'] = []
    reasons = gate.check(manifest)
    assert len(reasons) == 1 and 'schema_version 99' in reasons[0]


def test_a_football_manifest_is_refused(db):
    national(db)
    manifest, _, _ = built(db)
    manifest['sport'] = 'cfb'
    assert any('never be read as football' in reason
               for reason in gate.check(manifest))


def test_an_as_of_time_in_the_future_is_refused(db):
    national(db)
    manifest, _, _ = built(db)
    manifest['generated_at'] = (NOW + timedelta(hours=2)).isoformat()
    assert any('in the future' in reason
               for reason in gate.check(manifest, now=NOW))


def test_an_old_as_of_time_is_not_a_refusal(db):
    """Staleness is the reader's job to say, not a reason to publish nothing:
    the last good snapshot must stay publishable, marked stale."""
    national(db)
    manifest, _, _ = built(db)
    manifest['generated_at'] = (NOW - timedelta(days=3)).isoformat()
    assert gate.check(manifest, now=NOW) == []


def test_a_missing_as_of_time_is_refused(db):
    national(db)
    manifest, _, _ = built(db)
    manifest['generated_at'] = ''
    assert any('generated_at' in reason
               for reason in gate.check(manifest, now=NOW))


def test_a_season_most_of_whose_teams_are_unrated_is_refused(db):
    names = national(db)
    db.execute('DELETE FROM team_rating WHERE team_key IN ('
               + ','.join('?' * 60) + ')', names[:60])
    db.commit()
    manifest, _, _ = built(db)
    assert any('are rated, below' in reason for reason in gate.check(manifest))


# --- the files the manifest points at ---------------------------------------

def test_a_manifest_naming_a_missing_file_is_refused(db, tmp_path):
    national(db)
    out = tmp_path / 'out'
    manifest = exporter.write(*built(db), out)
    (out / 'teams.js').unlink()
    assert any('not on disk' in reason
               for reason in gate.check(manifest, root=out))


def test_a_manifest_naming_an_empty_file_is_refused(db, tmp_path):
    """An empty file must never replace a valid one. That is the plan's rule
    about a provider failure, applied to the export directory."""
    national(db)
    out = tmp_path / 'out'
    manifest = exporter.write(*built(db), out)
    (out / 'seasons' / '2026.js').write_text('', encoding='utf-8')
    assert any('is empty' in reason
               for reason in gate.check(manifest, root=out))


def test_a_file_edited_after_the_export_is_refused(db, tmp_path):
    """The version stamp is what stops a deploy serving last week's ratings
    under this week's as-of time."""
    national(db)
    out = tmp_path / 'out'
    manifest = exporter.write(*built(db), out)
    path = out / 'seasons' / '2026.js'
    path.write_text(path.read_text(encoding='utf-8').replace('1500', '9999'),
                    encoding='utf-8')
    reasons = gate.check(manifest, root=out)
    assert any('manifest says' in reason for reason in reasons)


def test_every_file_the_manifest_names_is_checked_not_just_the_first(db,
                                                                    tmp_path):
    national(db)
    out = tmp_path / 'out'
    manifest = exporter.write(*built(db), out)
    (out / 'teams.js').unlink()
    (out / 'seasons' / '2026.js').unlink()
    assert len([reason for reason in gate.check(manifest, root=out)
                if 'not on disk' in reason]) == 2


def test_a_file_with_no_version_stamp_is_refused(db, tmp_path):
    national(db)
    out = tmp_path / 'out'
    manifest = exporter.write(*built(db), out)
    manifest['files']['teams'] = 'teams.js'
    assert any('no ?v= version stamp' in reason
               for reason in gate.check(manifest, root=out))


def test_the_gate_finds_files_in_both_the_files_and_seasons_sections(db,
                                                                    tmp_path):
    national(db)
    out = tmp_path / 'out'
    manifest = exporter.write(*built(db), out)
    named = {path for path, _ in gate.manifest_files(manifest)}
    assert named == {'teams.js', 'seasons/2026.js'}


def test_the_command_line_gate_fails_on_a_refused_export(db, tmp_path,
                                                         capsys):
    national(db)
    out = tmp_path / 'out'
    exporter.write(*built(db), out)
    assert gate.main([str(out)]) == 0
    (out / 'teams.js').unlink()
    assert gate.main([str(out)]) == 1
    assert 'REFUSED' in capsys.readouterr().out


def test_the_command_line_gate_fails_when_there_is_no_manifest(tmp_path,
                                                               capsys):
    assert gate.main([str(tmp_path)]) == 1
    assert 'does not exist' in capsys.readouterr().out


# --- what the export must never touch --------------------------------------

def test_the_export_writes_only_inside_its_own_directory(db, tmp_path):
    national(db)
    out = tmp_path / 'out'
    exporter.write(*built(db), out)
    written = {path.relative_to(out).as_posix()
               for path in out.rglob('*') if path.is_file()}
    assert written == {'manifest.json', 'teams.js', 'seasons/2026.js'}


def test_no_payload_carries_a_provider_rating(db):
    """Provider Elo is never read. /games returns it; it is not in the schema,
    and it must not reappear in an export by another name."""
    national(db)
    manifest, payloads, teams = built(db)
    blob = json.dumps([manifest, payloads, teams])
    for banned in ('homeTeamElo', 'EloStart', 'EloEnd', 'provider_elo'):
        assert banned not in blob


# --- a season still being played --------------------------------------------
#
# The gate's floors describe a finished season: 2,000 games and 90% of the
# Division I list rated. On opening night a correct export has a handful of
# games and almost nobody rated, so applying those floors to the season in
# progress would refuse the whole archive for the only season a reader came
# to look at. These are the tests that pin the distinction, and the ones that
# prove the relaxation did not simply disable the check.

def in_progress(connection, season=2027, teams=320, played=40):
    """A season just underway: every team listed, only `played` with a result.

    Built from the same national fixture so the team list is real; the games
    are then cut back to what mid-November actually looks like.
    """
    names = national(connection, season=season, teams=teams, games_each=14,
                     model=False)
    # Unplay everything, then restore results for the first `played` teams.
    connection.execute("UPDATE game SET status = 'scheduled',"
                       ' home_points = NULL, away_points = NULL'
                       ' WHERE season = ?', (season,))
    keep = set(names[:played])
    rows = connection.execute(
        'SELECT game_key, home_team_key, away_team_key FROM game'
        ' WHERE season = ?', (season,)).fetchall()
    for row in rows:
        if row['home_team_key'] in keep and row['away_team_key'] in keep:
            connection.execute(
                "UPDATE game SET status = 'final', home_points = 70,"
                ' away_points = 61 WHERE game_key = ?', (row['game_key'],))
    connection.commit()
    from src.ncaambb.store import record_rating_model, upsert_team_ratings
    model_id = record_rating_model(
        connection, name='ncaambb_elo_v1',
        config=dict(k=45, home_advantage=100, offseason_retention=0.8,
                    margin_model='mov', reference_margin=11.95),
        fitted_on=[season - 2], held_out=[season - 1],
        holdout_brier=0.1866, holdout_games=5752)
    # Only the teams that have played carry a rating, which is what the
    # replay produces: a rating comes from a counted game.
    upsert_team_ratings(connection, model_id,
                        [(name, season, 1500.0 + index, 4)
                         for index, name in enumerate(sorted(keep))])
    connection.commit()
    return model_id, keep


def test_a_season_still_being_played_is_marked_incomplete(db):
    in_progress(db)
    manifest, _, _ = built(db)
    row = manifest['coverage'][0]
    assert row['season'] == 2027 and row['complete'] is False


def test_a_finished_season_is_marked_complete(db):
    national(db, season=2026)
    manifest, _, _ = built(db)
    assert manifest['coverage'][0]['complete'] is True


def test_a_season_in_progress_passes_the_gate_with_most_teams_unrated(db,
                                                                    tmp_path):
    # Only 40 of 320 teams have finished a game, so the rated fraction is far
    # under 90% of the Division I list. That is what mid-November looks like,
    # not a defect. (The game count is not the thin number here: a season's
    # whole schedule is published before it starts.)
    in_progress(db)
    manifest, payloads, teams = built(db)
    row = manifest['coverage'][0]
    assert row['rated_teams'] < row['division_one_teams'] * gate.MIN_RATED_FRACTION
    written = exporter.write(manifest, payloads, teams, tmp_path / 'out')
    assert gate.check(written, root=tmp_path / 'out') == []


def test_the_same_numbers_are_refused_in_a_finished_season(db):
    """The flag is what changes the verdict, not the numbers. Without this,
    the test above would pass just as well if the floor had been removed."""
    in_progress(db)
    manifest, _, _ = built(db)
    thin = dict(manifest['coverage'][0], complete=True)
    reasons = gate.check_coverage(dict(coverage=[thin]))
    assert any('Division I teams are rated' in reason for reason in reasons)


def test_played_teams_counts_only_teams_with_a_finished_rateable_game(db):
    _, keep = in_progress(db)
    manifest, _, _ = built(db)
    row = manifest['coverage'][0]
    assert row['played_teams'] == len(keep)
    assert row['division_one_teams'] > row['played_teams']


def test_a_team_that_has_played_but_is_unrated_still_fails_the_gate(db):
    """The relaxed check is still a check: every team that has finished a
    game must carry a rating, so a replay that dropped teams is caught in a
    season in progress exactly as it is in a finished one."""
    model_id, keep = in_progress(db)
    db.execute('DELETE FROM team_rating WHERE model_id = ? AND team_key IN'
               ' (SELECT team_key FROM team_rating WHERE model_id = ?'
               '  ORDER BY team_key LIMIT ?)',
               (model_id, model_id, len(keep) // 4))
    db.commit()
    manifest, _, _ = built(db)
    reasons = gate.check_coverage(manifest)
    assert any('that have played are rated' in reason for reason in reasons)


def test_a_season_in_progress_with_no_game_at_all_is_refused():
    reasons = gate.check_coverage(dict(coverage=[dict(
        season=2027, division_one_teams=364, rated_teams=0, games=0,
        played_teams=0, complete=False)]))
    assert any('nothing to' in reason for reason in reasons)


def test_a_season_in_progress_that_hides_how_many_played_is_refused():
    # Without played_teams there is no denominator, so the rated fraction
    # cannot be judged at all. Omitting it must not be a way through.
    reasons = gate.check_coverage(dict(coverage=[dict(
        season=2027, division_one_teams=364, rated_teams=1, games=500,
        complete=False)]))
    assert any('how many teams have played' in reason for reason in reasons)


def test_a_coverage_row_that_does_not_say_is_judged_as_a_finished_season():
    # A missing flag must not be the way a thin export gets published.
    reasons = gate.check_coverage(dict(coverage=[dict(
        season=2027, division_one_teams=364, rated_teams=40, games=500)]))
    assert any('games is below' in reason for reason in reasons)
