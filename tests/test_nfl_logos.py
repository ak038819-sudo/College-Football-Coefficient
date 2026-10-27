"""Resolving the 32 club marks, with no network anywhere in the test.

The images themselves cannot be fetched from the build container — the asset
host is refused by the egress proxy, which is why this runs in CI — so these
drive the part that decides WHICH mark each club gets, which is the part that
can be wrong quietly.
"""

import base64
import io
import json

import pytest

from src.nfl import logos

HEADER = ('level,league,entity_id,entity_name,variant,mark_type,ext,'
          'last_seen,archive_url\n')


def manifest(rows):
    body = ''.join(
        f"{r.get('level', 'team')},{r.get('league', 'nfl')},{r.get('id', '1')},"
        f"{r['name']},{r['variant']},{r.get('mark_type', 'logo')},"
        f"{r.get('ext', 'png')},{r.get('last_seen', '2026-10-01')},"
        f"{r.get('url', 'https://example.invalid/' + r['variant'] + '.png')}\n"
        for r in rows)
    return HEADER + body


def both(name):
    return [dict(name=name, variant='default'), dict(name=name, variant='dark')]


class TestResolving:
    def test_each_club_takes_the_colour_mark_for_light_and_the_ink_for_dark(
            self):
        rows = logos.manifest_rows(manifest(both('Buffalo Bills')))
        placed = logos.resolve(rows, {'Buffalo Bills'})
        assert placed['Buffalo Bills']['light'].endswith('default.png')
        assert placed['Buffalo Bills']['dark'].endswith('dark.png')

    def test_a_club_with_no_light_ink_cut_takes_its_colour_mark_on_both(self):
        """Dim is a judgement; absent is a hole."""
        rows = logos.manifest_rows(
            manifest([dict(name='Buffalo Bills', variant='default')]))
        placed = logos.resolve(rows, {'Buffalo Bills'})
        assert placed['Buffalo Bills']['dark'].endswith('default.png')
        assert placed['Buffalo Bills']['light'].endswith('default.png')

    def test_a_redrawn_mark_takes_the_newest_capture_not_the_last_row(self):
        """The archive keeps a row per set of bytes, so a club that has
        redrawn its logo appears twice under one variant. Three NFL
        name/variant pairs are doubled today. Last-row-wins would publish
        whichever the file happens to list second."""
        rows = logos.manifest_rows(manifest([
            dict(name='Green Bay Packers', variant='dark',
                 last_seen='2026-10-01', url='https://example.invalid/new.png'),
            dict(name='Green Bay Packers', variant='dark',
                 last_seen='2026-09-30', url='https://example.invalid/old.png'),
            dict(name='Green Bay Packers', variant='default'),
        ]))
        placed = logos.resolve(rows, {'Green Bay Packers'})
        assert placed['Green Bay Packers']['dark'].endswith('new.png')

    def test_the_newest_capture_wins_whichever_order_the_file_lists_them(self):
        """The same two rows the other way round. A rule that only works on
        one ordering is the bug, said differently."""
        rows = logos.manifest_rows(manifest([
            dict(name='Green Bay Packers', variant='dark',
                 last_seen='2026-09-30', url='https://example.invalid/old.png'),
            dict(name='Green Bay Packers', variant='dark',
                 last_seen='2026-10-01', url='https://example.invalid/new.png'),
            dict(name='Green Bay Packers', variant='default'),
        ]))
        placed = logos.resolve(rows, {'Green Bay Packers'})
        assert placed['Green Bay Packers']['dark'].endswith('new.png')

    def test_an_svg_row_is_not_offered_to_pillow(self):
        """Every NFL logo row is a PNG today. A club whose only dark row were
        an SVG must fall back to its colour mark, not hand Pillow a file it
        cannot open."""
        rows = logos.manifest_rows(manifest([
            dict(name='Buffalo Bills', variant='default'),
            dict(name='Buffalo Bills', variant='dark', ext='svg',
                 url='https://example.invalid/vector.svg'),
        ]))
        assert all(row['ext'] == 'png' for row in rows)
        placed = logos.resolve(rows, {'Buffalo Bills'})
        assert placed['Buffalo Bills']['dark'].endswith('default.png')

    def test_a_club_the_archive_does_not_carry_fails_the_run(self):
        """A bare lookup with `continue` on a miss is how six conference gaps
        hid for twelve seasons."""
        rows = logos.manifest_rows(manifest(both('Buffalo Bills')))
        with pytest.raises(LookupError) as raised:
            logos.resolve(rows, {'Buffalo Bills', 'Kansas City Chiefs'})
        assert 'Kansas City Chiefs' in str(raised.value)
        assert 'Buffalo Bills' not in str(raised.value)

    def test_the_relocation_predecessors_are_not_asked_for(self):
        """The archive lists OAK, SD and STL by CODE. A franchise's mark
        follows the franchise, so those rows go unused rather than becoming a
        second entry for the same club."""
        rows = logos.manifest_rows(
            manifest(both('Las Vegas Raiders') + both('OAK')))
        placed = logos.resolve(rows, {'Las Vegas Raiders'})
        assert set(placed) == {'Las Vegas Raiders'}

    def test_other_leagues_and_other_mark_types_are_not_read(self):
        rows = logos.manifest_rows(manifest(
            both('Buffalo Bills')
            + [dict(name='Buffalo Bills', variant='default', league='nhl',
                    url='https://example.invalid/wrong-league.png'),
               dict(name='Buffalo Bills', variant='default',
                    mark_type='wordmark',
                    url='https://example.invalid/wordmark.png'),
               dict(name='Buffalo Bills', variant='default', level='league',
                    url='https://example.invalid/wrong-level.png')]))
        placed = logos.resolve(rows, {'Buffalo Bills'})
        assert placed['Buffalo Bills']['light'].endswith('default.png')

    def test_a_row_with_no_archive_url_is_not_a_mark(self):
        rows = logos.manifest_rows(manifest(
            [dict(name='Buffalo Bills', variant='default', url=''),
             dict(name='Buffalo Bills', variant='dark')]))
        placed = logos.resolve(rows, {'Buffalo Bills'})
        assert placed['Buffalo Bills']['light'].endswith('dark.png')


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
        opener = self.opener(manifest(both('Buffalo Bills')), self.png())
        out = logos.build({'nfl:nflverse:franchise:BUF': 'Buffalo Bills'},
                          opener=opener)
        mark = out['nfl:nflverse:franchise:BUF']['light']
        assert mark.startswith('data:image/png;base64,')
        from PIL import Image
        decoded = Image.open(io.BytesIO(base64.b64decode(mark.split(',', 1)[1])))
        assert max(decoded.size) == logos.MARK_SIZE

    def test_one_url_is_fetched_once_however_many_themes_use_it(self):
        pytest.importorskip('PIL')
        rows = [dict(name='Buffalo Bills', variant='default',
                     url='https://example.invalid/one.png')]
        opener = self.opener(manifest(rows), self.png())
        logos.build({'nfl:nflverse:franchise:BUF': 'Buffalo Bills'},
                    opener=opener)
        images = [url for url in opener.calls if url != logos.MANIFEST_URL]
        assert images == ['https://example.invalid/one.png']

    def test_a_dry_run_fetches_no_image_at_all(self):
        opener = self.opener(manifest(both('Buffalo Bills')), b'never read')
        out = logos.build({'nfl:nflverse:franchise:BUF': 'Buffalo Bills'},
                          opener=opener, dry_run=True)
        assert opener.calls == [logos.MANIFEST_URL]
        assert out['nfl:nflverse:franchise:BUF']['light'].startswith('http')


# --- the workflow that fetches them ----------------------------------------

from pathlib import Path  # noqa: E402

WORKFLOW = Path(__file__).resolve().parents[1] / (
    '.github/workflows/fetch-nfl-logos.yml')


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


class TestTheWorkflow:
    def text(self):
        return WORKFLOW.read_text()

    def test_it_ingests_before_it_resolves(self):
        """The marks are keyed by franchise and the names come from the
        database, so resolving before ingesting resolves nothing."""
        text = self.text()
        assert text.index('src.nfl.ingest') < text.index('src.nfl.logos')

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
        assert 'fetch-nfl-logos-' in block
        assert 'rebuild-and-deploy' not in block
        assert 'nfl-refresh' not in block

    def test_a_dry_run_fetches_and_commits_nothing(self):
        text = self.text()
        for header in ('      - name: Fetch the marks',
                       '      - name: Publish'):
            assert 'if: ${{ !inputs.dry_run }}' in indented_block(text, header)

    def test_it_installs_an_image_library(self):
        """The encode step needs Pillow, which the NFL refresh deliberately
        does not install."""
        assert 'requirements-dev.txt' in self.text()

    def test_it_publishes_only_the_mark_file(self):
        block = indented_block(self.text(), '      - name: Publish')
        assert 'ui/nfl_logo_assets.json' in block
        # NOT under ui/data/nfl: the refresh replaces that directory whole
        # and would delete the marks with it.
        assert 'ui/data/nfl' not in block
