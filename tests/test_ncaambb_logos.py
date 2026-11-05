"""Banking each Division I school's mark beside the basketball export.

The CDN is blocked from the development container, so every test here drives
the fetch through an injected opener and nothing in this file touches the
network.
"""

from pathlib import Path
import sys
import urllib.error

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ncaambb import fetch_logos
from src.ncaambb.store import connect


@pytest.fixture
def db(tmp_path):
    connection = connect(tmp_path / 'ncaambb.db')
    yield connection
    connection.close()


def school(connection, provider_id, name, logo):
    connection.execute(
        'INSERT INTO team (team_key, provider_id, school, logo_id)'
        ' VALUES (?, ?, ?, ?)',
        (f'ncaambb:cbbd:team:{provider_id}', provider_id, name, logo))
    connection.commit()


class Opener:
    """Stands in for urlopen. Records what was asked for."""

    def __init__(self, body=b'PNG', fail=()):
        self.body = body
        self.fail = set(fail)
        self.asked = []

    def __call__(self, url):
        self.asked.append(url)
        if url in self.fail:
            raise urllib.error.URLError('refused')
        opener = self

        class Response:
            def __enter__(self_inner):
                return self_inner

            def __exit__(self_inner, *_):
                return False

            def read(self_inner):
                return opener.body
        return Response()


def test_both_cuts_of_every_school_are_fetched(db, tmp_path):
    school(db, 264, 'Siena', 2561)
    opener = Opener()
    summary = fetch_logos.run(db, root=tmp_path, opener=opener)
    assert summary == {'schools': 1, 'written': 2, 'skipped': 0, 'failed': []}
    assert (tmp_path / '2561.png').read_bytes() == b'PNG'
    assert (tmp_path / '2561-dark.png').read_bytes() == b'PNG'
    assert opener.asked == [
        'https://cdn.collegefootballdata.com/cbb-logos/96/2561.png',
        'https://cdn.collegefootballdata.com/cbb-logos-dark/96/2561.png']


def test_a_second_run_asks_for_nothing(db, tmp_path):
    # A mark changes when a school rebrands. Re-fetching 730 files on every
    # run would spend the deploy's time and the repository's history on files
    # that did not change.
    school(db, 264, 'Siena', 2561)
    fetch_logos.run(db, root=tmp_path, opener=Opener())
    opener = Opener()
    summary = fetch_logos.run(db, root=tmp_path, opener=opener)
    assert opener.asked == []
    assert summary['written'] == 0 and summary['skipped'] == 2


def test_refetch_asks_again(db, tmp_path):
    school(db, 264, 'Siena', 2561)
    fetch_logos.run(db, root=tmp_path, opener=Opener(body=b'old'))
    opener = Opener(body=b'new')
    summary = fetch_logos.run(db, root=tmp_path, opener=opener, refetch=True)
    assert summary['written'] == 2
    assert (tmp_path / '2561.png').read_bytes() == b'new'


def test_one_school_failing_does_not_cost_the_others_their_marks(db, tmp_path):
    # The page draws a monogram where a file is missing, so a 404 on one
    # school is a gap in the page and not a reason to abandon 364 others.
    school(db, 264, 'Siena', 2561)
    school(db, 265, 'Marist', 2600)
    bad = 'https://cdn.collegefootballdata.com/cbb-logos/96/2561.png'
    summary = fetch_logos.run(db, root=tmp_path, opener=Opener(fail=[bad]))
    assert summary['written'] == 3
    assert len(summary['failed']) == 1
    assert 'Siena' in summary['failed'][0]
    assert (tmp_path / '2600.png').exists()
    assert not (tmp_path / '2561.png').exists()
    assert (tmp_path / '2561-dark.png').exists()


def test_an_empty_body_is_a_failure_rather_than_an_empty_file(db, tmp_path):
    # A zero-byte PNG written to disk would be skipped by every later run,
    # and drawn as a broken image forever.
    school(db, 264, 'Siena', 2561)
    summary = fetch_logos.run(db, root=tmp_path, opener=Opener(body=b''))
    assert summary['written'] == 0
    assert len(summary['failed']) == 2
    assert not (tmp_path / '2561.png').exists()


def test_a_school_with_no_mark_id_is_not_asked_about(db, tmp_path):
    school(db, 900, 'Some Junior College', None)
    opener = Opener()
    summary = fetch_logos.run(db, root=tmp_path, opener=opener)
    assert summary['schools'] == 0
    assert opener.asked == []


def test_two_schools_sharing_a_mark_are_fetched_once(db, tmp_path):
    # The id is the SCHOOL's, and two provider rows for one school would
    # otherwise each fetch the same two files.
    school(db, 264, 'Siena', 2561)
    school(db, 2264, 'Siena (again)', 2561)
    opener = Opener()
    summary = fetch_logos.run(db, root=tmp_path, opener=opener)
    assert summary['schools'] == 1
    assert len(opener.asked) == 2


def test_a_dry_run_makes_no_requests(db, tmp_path):
    school(db, 264, 'Siena', 2561)
    opener = Opener()
    summary = fetch_logos.run(db, root=tmp_path, opener=opener, dry_run=True)
    assert opener.asked == []
    assert summary['written'] == 2
    assert not (tmp_path / '2561.png').exists()


def test_the_size_is_a_choice_of_this_fetch_and_not_of_the_database(db,
                                                                   tmp_path):
    school(db, 264, 'Siena', 2561)
    opener = Opener()
    fetch_logos.run(db, root=tmp_path, opener=opener, size=256)
    assert opener.asked[0].endswith('/cbb-logos/256/2561.png')


def test_a_database_with_no_marks_at_all_is_a_failed_run(db, tmp_path,
                                                         monkeypatch, capsys):
    # Loading the database is a step of its own in the workflow, and a run
    # that quietly reported "0 schools, 0 files" would look like a success.
    monkeypatch.setattr(fetch_logos.store, 'connect', lambda path: db)
    assert fetch_logos.main(['--dry-run']) == 1
    assert 'has the database been loaded' in capsys.readouterr().out


def test_every_school_failing_is_a_failed_run(db, tmp_path, monkeypatch,
                                              capsys):
    school(db, 264, 'Siena', 2561)
    monkeypatch.setattr(fetch_logos.store, 'connect', lambda path: db)
    monkeypatch.setattr(fetch_logos, 'ASSETS', tmp_path)
    monkeypatch.setattr(fetch_logos, 'fetch', lambda url, opener=None:
                        (_ for _ in ()).throw(urllib.error.URLError('down')))
    assert fetch_logos.main([]) == 1


def test_a_run_that_banked_something_succeeds(db, tmp_path, monkeypatch):
    school(db, 264, 'Siena', 2561)
    monkeypatch.setattr(fetch_logos.store, 'connect', lambda path: db)
    monkeypatch.setattr(fetch_logos, 'ASSETS', tmp_path)
    monkeypatch.setattr(fetch_logos, 'fetch', lambda url, opener=None: b'PNG')
    assert fetch_logos.main([]) == 0
    assert (tmp_path / '2561.png').exists()
