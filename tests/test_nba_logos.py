"""Resolving the 30 club marks, with no network anywhere in the test.

The images themselves cannot be fetched from the build container — the asset
host is refused by the egress proxy, which is why this runs in CI — so these
drive the part that decides WHICH mark each club gets, which is the part that
can be wrong quietly.

The NBA join is the ESPN id inside the team key, not the club name, so what
is tested here that the NFL's counterpart cannot be is the id: a key of
another shape, an id the archive has given to another club, and a club whose
id is simply absent.
"""

import base64
import io

import pytest

from src.nba import logos

HEADER = ('level,league,entity_id,entity_name,variant,mark_type,ext,'
          'last_seen,archive_url\n')

ATL = 'nba:espn:team:1'
BOS = 'nba:espn:team:2'


def manifest(rows):
    body = ''.join(
        f"{r.get('level', 'team')},{r.get('league', 'nba')},{r.get('id', '1')},"
        f"{r.get('name', 'Atlanta Hawks')},{r['variant']},"
        f"{r.get('mark_type', 'logo')},{r.get('ext', 'png')},"
        f"{r.get('last_seen', '2026-10-01')},"
        f"{r.get('url', 'https://example.invalid/' + r['variant'] + '.png')}\n"
        for r in rows)
    return HEADER + body


def both(name='Atlanta Hawks', identifier='1'):
    return [dict(name=name, id=identifier, variant='default'),
            dict(name=name, id=identifier, variant='dark')]


class TestReadingTheKey:
    def test_the_espn_id_is_the_last_field_of_the_team_key(self):
        assert logos.espn_id('nba:espn:team:1') == '1'
        assert logos.espn_id('nba:espn:team:1610612737') == '1610612737'

    @pytest.mark.parametrize('key', [
        'nba:espn:team:atl',      # a code, not an id
        'nba:espn:person:1',      # a person, not a club
        'nba:nbacom:team:1',      # another provider's numbering
        'nfl:espn:team:1',        # another league
        'nba:espn:team:1:extra',  # a longer key
        '1',
    ])
    def test_a_key_of_another_shape_has_no_id(self, key):
        """Returning None rather than a numeric-looking fragment: an id taken
        off the wrong key joins to whichever club happens to hold it."""
        assert logos.espn_id(key) is None


class TestResolving:
    def test_each_club_takes_the_colour_mark_for_light_and_the_ink_for_dark(
            self):
        rows = logos.manifest_rows(manifest(both()))
        placed = logos.resolve(rows, {ATL: 'Atlanta Hawks'})
        assert placed[ATL]['light'].endswith('default.png')
        assert placed[ATL]['dark'].endswith('dark.png')

    def test_the_join_is_the_id_and_not_the_position_in_the_file(self):
        """Two clubs, listed in the opposite order to the keys asked for."""
        rows = logos.manifest_rows(manifest(
            [dict(name='Boston Celtics', id='2', variant='default',
                  url='https://example.invalid/bos.png'),
             dict(name='Boston Celtics', id='2', variant='dark',
                  url='https://example.invalid/bos-dark.png')]
            + both()))
        placed = logos.resolve(rows, {ATL: 'Atlanta Hawks',
                                      BOS: 'Boston Celtics'})
        assert placed[BOS]['light'].endswith('bos.png')
        assert placed[ATL]['light'].endswith('default.png')

    def test_an_id_the_archive_gives_to_another_club_fails_the_run(self):
        """The name is not what the join is made on; it is what the join is
        CHECKED against. An id that has been reassigned would otherwise
        publish another club's mark in silence."""
        rows = logos.manifest_rows(manifest(both('Saint Louis Hawks')))
        with pytest.raises(LookupError) as raised:
            logos.resolve(rows, {ATL: 'Atlanta Hawks'})
        assert 'Saint Louis Hawks' in str(raised.value)
        assert 'disagrees' in str(raised.value)

    def test_a_club_whose_key_carries_no_id_is_named_as_missing(self):
        rows = logos.manifest_rows(manifest(both()))
        with pytest.raises(LookupError) as raised:
            logos.resolve(rows, {'nba:espn:team:atl': 'Atlanta Hawks'})
        assert 'Atlanta Hawks' in str(raised.value)

    def test_a_club_with_no_light_ink_cut_takes_its_colour_mark_on_both(self):
        """Dim is a judgement; absent is a hole."""
        rows = logos.manifest_rows(manifest([dict(variant='default')]))
        placed = logos.resolve(rows, {ATL: 'Atlanta Hawks'})
        assert placed[ATL]['dark'].endswith('default.png')
        assert placed[ATL]['light'].endswith('default.png')

    def test_a_redrawn_mark_takes_the_newest_capture_not_the_last_row(self):
        """The archive keeps a row per set of bytes, so a club that redraws
        its logo appears twice under one variant. No NBA pair is doubled
        today, which is exactly why a rule nothing exercises has to be
        tested rather than watched."""
        rows = logos.manifest_rows(manifest([
            dict(variant='dark', last_seen='2026-10-01',
                 url='https://example.invalid/new.png'),
            dict(variant='dark', last_seen='2026-09-30',
                 url='https://example.invalid/old.png'),
            dict(variant='default'),
        ]))
        placed = logos.resolve(rows, {ATL: 'Atlanta Hawks'})
        assert placed[ATL]['dark'].endswith('new.png')

    def test_the_newest_capture_wins_whichever_order_the_file_lists_them(self):
        """The same two rows the other way round. A rule that only works on
        one ordering is the bug, said differently."""
        rows = logos.manifest_rows(manifest([
            dict(variant='dark', last_seen='2026-09-30',
                 url='https://example.invalid/old.png'),
            dict(variant='dark', last_seen='2026-10-01',
                 url='https://example.invalid/new.png'),
            dict(variant='default'),
        ]))
        placed = logos.resolve(rows, {ATL: 'Atlanta Hawks'})
        assert placed[ATL]['dark'].endswith('new.png')

    def test_an_svg_row_is_not_offered_to_pillow(self):
        """Every NBA logo row is a PNG today. A club whose only dark row were
        an SVG must fall back to its colour mark, not hand Pillow a file it
        cannot open."""
        rows = logos.manifest_rows(manifest([
            dict(variant='default'),
            dict(variant='dark', ext='svg',
                 url='https://example.invalid/vector.svg'),
        ]))
        assert all(row['ext'] == 'png' for row in rows)
        placed = logos.resolve(rows, {ATL: 'Atlanta Hawks'})
        assert placed[ATL]['dark'].endswith('default.png')

    def test_a_club_the_archive_does_not_carry_fails_the_run(self):
        """A bare lookup with `continue` on a miss is how six conference gaps
        hid for twelve seasons."""
        rows = logos.manifest_rows(manifest(both()))
        with pytest.raises(LookupError) as raised:
            logos.resolve(rows, {ATL: 'Atlanta Hawks',
                                 BOS: 'Boston Celtics'})
        assert 'Boston Celtics' in str(raised.value)
        assert 'Atlanta Hawks' not in str(raised.value)

    def test_other_leagues_and_other_mark_types_are_not_read(self):
        """`nbagl` — the G League — is a league of its own in this archive
        and numbers its clubs from 1 as well."""
        rows = logos.manifest_rows(manifest(
            both()
            + [dict(variant='default', league='nbagl',
                    url='https://example.invalid/wrong-league.png'),
               dict(variant='default', mark_type='wordmark',
                    url='https://example.invalid/wordmark.png'),
               dict(variant='default', level='league',
                    url='https://example.invalid/wrong-level.png')]))
        placed = logos.resolve(rows, {ATL: 'Atlanta Hawks'})
        assert placed[ATL]['light'].endswith('default.png')

    def test_a_row_with_no_archive_url_is_not_a_mark(self):
        rows = logos.manifest_rows(manifest(
            [dict(variant='default', url=''), dict(variant='dark')]))
        placed = logos.resolve(rows, {ATL: 'Atlanta Hawks'})
        assert placed[ATL]['light'].endswith('dark.png')


class TestBuilding:
    def opener(self, text, image):
        calls = []

        class Response:
            def __init__(self, body):
                self.body = body

            def read(self):
                return self.body

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        def open_url(url, timeout=None):
            calls.append(url)
            if url == logos.MANIFEST_URL:
                return Response(text.encode('utf-8'))
            return Response(image)
        open_url.calls = calls
        return open_url

    def png(self):
        from PIL import Image
        buffer = io.BytesIO()
        Image.new('RGBA', (512, 512), (10, 20, 30, 255)).save(
            buffer, format='PNG')
        return buffer.getvalue()

    def test_a_mark_is_written_as_a_resized_data_uri(self):
        pytest.importorskip('PIL')
        opener = self.opener(manifest(both()), self.png())
        out = logos.build({ATL: 'Atlanta Hawks'}, opener=opener)
        mark = out[ATL]['light']
        assert mark.startswith('data:image/png;base64,')
        from PIL import Image
        decoded = Image.open(io.BytesIO(base64.b64decode(mark.split(',', 1)[1])))
        assert max(decoded.size) == logos.MARK_SIZE

    def test_one_url_is_fetched_once_however_many_themes_use_it(self):
        pytest.importorskip('PIL')
        opener = self.opener(
            manifest([dict(variant='default',
                           url='https://example.invalid/one.png')]),
            self.png())
        logos.build({ATL: 'Atlanta Hawks'}, opener=opener)
        images = [url for url in opener.calls if url != logos.MANIFEST_URL]
        assert images == ['https://example.invalid/one.png']

    def test_a_dry_run_fetches_no_image_at_all(self):
        opener = self.opener(manifest(both()), b'never read')
        out = logos.build({ATL: 'Atlanta Hawks'}, opener=opener, dry_run=True)
        assert opener.calls == [logos.MANIFEST_URL]
        assert out[ATL]['light'].startswith('http')


# --- the workflow that fetches them ----------------------------------------

from pathlib import Path  # noqa: E402

WORKFLOW = Path(__file__).resolve().parents[1] / (
    '.github/workflows/fetch-nba-logos.yml')


def indented_block(text, header):
    """The lines under `header`, by indentation.

    A hand-rolled reader rather than PyYAML, for the reason
    tests/test_nfl_game_layer.py gives: a test needing a parser a job does
    not have fails the job rather than the code.
    """
    lines = text.split('\n')
    start = lines.index(header)
    depth = len(header) - len(header.lstrip())
    out = []
    for line in lines[start + 1:]:
        if line.strip() and len(line) - len(line.lstrip()) <= depth:
            break
        out.append(line)
    return '\n'.join(out)


class TestRecordingWhichMarksNeedAPlate:
    """A mark close to the page's own colour is plated rather than left to
    disappear. Measured at fetch time, on the shrunk bytes the page draws.
    """

    def fetcher(self, images):
        """An opener serving the manifest, then a PNG per URL."""
        def opener(url, timeout=None):
            if url == logos.MANIFEST_URL:
                body = manifest(both()).encode()
            else:
                body = images[url.rsplit('/', 1)[1]]
            return io.BytesIO(body)
        return opener

    def solid(self, rgb):
        from PIL import Image
        buffer = io.BytesIO()
        Image.new('RGBA', (8, 8), rgb + (255,)).save(buffer, format='PNG')
        return buffer.getvalue()

    def test_a_club_dim_on_the_dark_page_is_recorded_as_such(self):
        pytest.importorskip('PIL')
        black = self.solid((0, 0, 0))
        built = logos.build({ATL: 'Atlanta Hawks'},
                            opener=self.fetcher({'default.png': black,
                                                 'dark.png': black}))
        assert built[ATL]['dim'] == ['dark']

    def test_a_club_dim_on_the_light_page_is_recorded_as_such(self):
        pytest.importorskip('PIL')
        white = self.solid((255, 255, 255))
        built = logos.build({ATL: 'Atlanta Hawks'},
                            opener=self.fetcher({'default.png': white,
                                                 'dark.png': white}))
        assert built[ATL]['dim'] == ['light']

    def test_a_club_that_reads_on_both_carries_no_dim_key_at_all(self):
        """An empty list on every club that reads fine is thirty keys saying
        nothing."""
        pytest.importorskip('PIL')
        built = logos.build({ATL: 'Atlanta Hawks'},
                            opener=self.fetcher({
                                'default.png': self.solid((0, 0, 0)),
                                'dark.png': self.solid((255, 255, 255))}))
        assert 'dim' not in built[ATL]
        assert set(built[ATL]) == {'light', 'dark'}

    def test_a_dry_run_fetches_no_images_and_measures_nothing(self):
        """It reports the join; there are no bytes to measure."""
        built = logos.build({ATL: 'Atlanta Hawks'},
                            opener=self.fetcher({}), dry_run=True)
        assert 'dim' not in built[ATL]


class TestTheWorkflow:
    def text(self):
        return WORKFLOW.read_text()

    def test_it_ingests_before_it_resolves(self):
        """The marks are keyed by franchise and the keys come from the
        database, so resolving before ingesting resolves nothing."""
        text = self.text()
        assert text.index('src.nba.ingest') < text.index('src.nba.logos')

    def test_it_refuses_to_publish_from_anything_but_main(self):
        """publish_to_main.sh ends in `git push origin HEAD:main`, so a
        dispatch from a feature branch would carry its unmerged commits onto
        main, past the pull request."""
        block = indented_block(self.text(), '      - name: Publish')
        assert 'PUBLISH_BRANCH' in block
        assert '!= "main"' in block
        # The bare name also appears in the comment above the guard, so
        # match the command as it is actually invoked.
        assert block.index('!= "main"') < block.index(
            'scripts/publish_to_main.sh'), (
            'the guard must come before the push, not after it')

    def test_it_does_not_share_the_deploy_or_refresh_lock(self):
        """A run queued in someone else's concurrency group is cancelled
        outright by the next run to enter it."""
        block = indented_block(self.text(), '    concurrency:')
        assert 'fetch-nba-logos-' in block
        assert 'rebuild-and-deploy' not in block
        assert 'nba-refresh' not in block

    def test_a_dry_run_fetches_and_commits_nothing(self):
        text = self.text()
        for header in ('      - name: Fetch the marks',
                       '      - name: Publish'):
            assert 'if: ${{ !inputs.dry_run }}' in indented_block(text, header)

    def test_it_installs_an_image_library(self):
        """The encode step needs Pillow, which the NBA refresh deliberately
        does not install."""
        assert 'requirements-dev.txt' in self.text()

    def test_it_publishes_only_the_mark_file(self):
        block = indented_block(self.text(), '      - name: Publish')
        assert 'ui/nba_logo_assets.json' in block
        # NOT under ui/data/nba: the refresh replaces that directory whole
        # and would delete the marks with it.
        assert 'ui/data/nba' not in block
