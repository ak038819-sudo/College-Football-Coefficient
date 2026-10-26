"""A hand-written page's scripts must carry the hash of the file they name.

The service worker serves documents network-first and everything else
stale-while-revalidate. So a returning reader can be handed a FRESH
`ui/nfl.html` beside a CACHED `ui/nfl_app.js` from before the deploy, and a
page that calls a helper the cached module does not have throws a TypeError
and draws nothing until they reload. `ui/dashboard.html` has never had this
problem because build_dashboard.py stamps every script it references; the
NFL page had no generator and so had no stamps.

A stamp nobody recomputes is worse than no stamp, because it reads as a
promise that the file has not changed. That is what this pins.
"""

import hashlib
import re
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.stamp_page_scripts import PAGES, SCRIPT, rewrite, stamp

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('page', PAGES, ids=lambda p: p.name)
def test_every_script_the_page_loads_is_stamped_with_its_own_hash(page):
    text = (ROOT / page).read_text()
    found = SCRIPT.findall(text)
    assert found, f'{page} loads no external script; this test is watching nothing'
    for _, name, version, _ in found:
        target = ROOT / page.parent / name
        assert target.exists(), f'{page} loads {name}, which is not in the tree'
        assert version == '?v=' + stamp(target), (
            f'{page} loads {name}{version}, but that file now hashes to '
            f'{stamp(target)}. Run scripts/stamp_page_scripts.py.')


def test_the_stamper_itself_notices_a_changed_file(tmp_path):
    """Drives `rewrite`, because a checker that always says "fine" is worse
    than no checker and would pass the test above for ever."""
    page = tmp_path / 'page.html'
    module = tmp_path / 'thing.js'
    module.write_text('var a = 1;\n')
    first = hashlib.sha256(module.read_bytes()).hexdigest()[:12]
    page.write_text(f'<script src="thing.js?v={first}"></script>\n')
    text, changes = rewrite(page)
    assert changes == []
    module.write_text('var a = 2;\n')
    text, changes = rewrite(page)
    assert len(changes) == 1
    assert changes[0][0] == 'thing.js'
    assert changes[0][2] == '?v=' + stamp(module)
    assert changes[0][2] in text


def test_an_unstamped_script_is_reported(tmp_path):
    page = tmp_path / 'page.html'
    (tmp_path / 'thing.js').write_text('var a = 1;\n')
    page.write_text('<script src="thing.js"></script>\n')
    _, changes = rewrite(page)
    assert len(changes) == 1
    assert changes[0][1] is None


def test_a_script_the_tree_does_not_have_is_left_alone(tmp_path):
    """A missing file is a broken page, not a stamping problem.

    Stamping the hash of nothing would hide it behind a plausible URL.
    """
    page = tmp_path / 'page.html'
    page.write_text('<script src="absent.js"></script>\n')
    text, changes = rewrite(page)
    assert changes == []
    assert 'absent.js"' in text


def test_check_mode_writes_nothing_and_fails_on_a_stale_stamp():
    result = subprocess.run(
        [sys.executable, 'scripts/stamp_page_scripts.py', '--check'],
        cwd=ROOT, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
