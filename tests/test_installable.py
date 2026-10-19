"""The parts of "installable on a phone" that break silently.

Nothing in this suite installs an app, so every failure mode here is invisible
until someone taps Add to Home Screen and gets a browser icon, a blank splash
screen, or a start URL that 404s. These pin the handful of facts a browser
checks before it offers to install at all: a manifest it can parse, icons at
the sizes it asks for that are really those sizes and really exist, a start URL
inside the scope, and a service worker whose scope reaches the pages it serves.

The scope rule is the one worth stating: a service worker can never control a
path above its own directory, so service-worker.js has to sit at the repository
root to cover index.html as well as ui/. A move into ui/ would still pass every
other test in this file and would silently stop the root redirect from working
offline.
"""
from __future__ import annotations

import json
import re
import struct
from pathlib import Path
from urllib.parse import urlparse

import pytest

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / 'manifest.webmanifest'
WORKER = ROOT / 'service-worker.js'
INDEX = ROOT / 'index.html'
SHELL = ROOT / 'ui' / 'dashboard_shell.html'


@pytest.fixture(scope='module')
def manifest():
    return json.loads(MANIFEST.read_text(encoding='utf-8'))


def head(path: Path) -> str:
    return path.read_text(encoding='utf-8').split('</head>', 1)[0]


def png_size(path: Path):
    data = path.read_bytes()
    assert data[:8] == b'\x89PNG\r\n\x1a\n', f'{path} is not a PNG'
    return struct.unpack('>II', data[16:24])


# ---- the manifest itself -----------------------------------------------------

def test_the_manifest_declares_what_an_install_needs(manifest):
    assert manifest['name'] and manifest['short_name']
    # Without standalone the install is a bookmark that opens a browser tab
    # with its address bar, which is the thing this phase exists to replace.
    assert manifest['display'] == 'standalone'
    assert manifest['theme_color'] == manifest['background_color'] == '#0e1512'


def test_the_start_url_is_a_page_this_repository_publishes(manifest):
    target = ROOT / urlparse(manifest['start_url']).path
    assert target.is_file(), f"start_url points at a missing {manifest['start_url']}"


def test_every_url_in_the_manifest_is_relative_and_inside_the_scope(manifest):
    """The site is published under a project path today and may move to a bare
    domain later. An absolute path would break on one of the two."""
    urls = [manifest['start_url']] + [i['src'] for i in manifest['icons']] + \
           [s['url'] for s in manifest.get('shortcuts', [])]
    for url in urls:
        assert not url.startswith(('/', 'http://', 'https://')), f'{url} is not relative'
        assert not url.startswith('../'), f'{url} escapes the manifest\'s own directory'


def test_shortcut_targets_exist(manifest):
    for shortcut in manifest.get('shortcuts', []):
        assert (ROOT / urlparse(shortcut['url']).path).is_file(), shortcut['url']


def test_the_declared_icon_sizes_are_the_real_ones(manifest):
    """A declared 512 that is really a 180 is accepted by the manifest parser
    and then rendered as a blurred upscale on the home screen."""
    for icon in manifest['icons']:
        path = ROOT / icon['src']
        assert path.is_file(), f"{icon['src']} is missing; run scripts/build_brand_assets.py"
        width, height = png_size(path)
        assert f'{width}x{height}' == icon['sizes'], f"{icon['src']} is {width}x{height}"
        assert icon['type'] == 'image/png'


def test_the_icon_sizes_browsers_ask_for_by_name_are_all_there(manifest):
    declared = {icon['sizes'] for icon in manifest['icons']}
    assert {'192x192', '512x512'} <= declared
    # Without a maskable icon an Android launcher puts the square mark inside
    # its own silhouette, usually with a white border around it.
    assert any(icon.get('purpose') == 'maskable' for icon in manifest['icons'])


def test_the_icons_are_derived_from_the_mark_like_every_other_brand_asset(manifest):
    assert (ROOT / 'ui' / 'brand' / 'mark.svg').is_file()
    source = (ROOT / 'scripts' / 'build_brand_assets.py').read_text(encoding='utf-8')
    for icon in manifest['icons']:
        name = Path(icon['src']).name
        assert name in source, f'{name} is committed but nothing regenerates it'


# ---- the pages that point at it ----------------------------------------------

@pytest.mark.parametrize('page,expected', [(INDEX, 'manifest.webmanifest'),
                                           (SHELL, '../manifest.webmanifest')])
def test_both_entry_points_link_the_manifest(page, expected):
    """Whichever URL someone opens, the browser must find the manifest: the
    root is what people paste, the dashboard is what the root redirects to."""
    found = re.findall(r'<link\s+rel="manifest"\s+href="([^"]+)"', head(page))
    assert found == [expected], f'{page.name} links {found}'


@pytest.mark.parametrize('page', [INDEX, SHELL])
def test_ios_gets_the_tags_it_reads_instead_of_the_manifest(page):
    """iOS reads almost nothing from the manifest. Standalone display and the
    home-screen name are these tags or they do not happen."""
    text = head(page)
    assert 'name="apple-mobile-web-app-capable" content="yes"' in text
    assert 'name="apple-mobile-web-app-title" content="Elocast"' in text
    assert re.search(r'<link\s+rel="apple-touch-icon"', text)


# ---- the worker --------------------------------------------------------------

def test_the_worker_sits_where_its_scope_can_cover_the_whole_site():
    assert WORKER.is_file(), 'service-worker.js must be at the repository root'
    # Its scope is its own directory. Everything the site serves lives under
    # that directory; a worker inside ui/ could not control the root redirect.
    for served in (INDEX, SHELL, ROOT / 'ui' / 'brand' / 'mark.svg'):
        assert WORKER.parent in served.parents


def test_the_dashboard_registers_the_worker_from_the_root():
    """Registering './service-worker.js' from ui/ would ask for a file that is
    not there, and the install prompt never appears."""
    text = SHELL.read_text(encoding='utf-8')
    assert "register('../service-worker.js'" in text
    assert "scope: '../'" in text


@pytest.mark.parametrize('name', ['PRECACHE_DOCUMENTS', 'PRECACHE_ASSETS'])
def test_everything_the_worker_precaches_exists(name):
    """A precache list is fetched on install; an entry that 404s is tolerated
    by the worker but means the shell is not actually available offline."""
    listed = re.search(rf'const {name} = \[(.*?)\];', WORKER.read_text(encoding='utf-8'), re.S)
    assert listed, f'{name} is no longer a literal list'
    paths = re.findall(r"'([^']+)'", listed.group(1))
    assert paths
    for path in paths:
        assert (ROOT / path).is_file(), f'{path} is precached but not published'


def test_the_dashboard_scripts_the_worker_precaches_are_all_versioned():
    """Install reads the built page and caches the scripts it names. Those are
    served cache-first, which is only safe because each URL carries the hash of
    its own bytes -- a script without one would be frozen at install forever."""
    built = ROOT / 'ui' / 'dashboard.html'
    if not built.is_file():
        pytest.skip('ui/dashboard.html has not been built in this checkout')
    srcs = re.findall(r'<script[^>]+src="([^"]+)"', built.read_text(encoding='utf-8'))
    assert srcs, 'the dashboard loads no external scripts at all'
    for src in srcs:
        if src.startswith(('http://', 'https://', '//')):
            continue
        assert '?v=' in src, f'{src} is loaded by the page but carries no version'
        assert (ROOT / 'ui' / src.split('?')[0]).is_file(), src


# ---- the window an installed app actually gets ------------------------------

def test_the_viewport_lets_the_page_reach_the_safe_area():
    """Without viewport-fit=cover every env(safe-area-inset-*) resolves to 0,
    and the bottom bar sits under the home-screen indicator."""
    # Read off the tag itself. The stylesheet below it explains the same flag
    # in a comment, so a search of the whole head passes with the tag gone.
    content = re.search(r'<meta name="viewport" content="([^"]+)"', head(SHELL))
    assert content, 'the dashboard has no viewport meta at all'
    assert 'viewport-fit=cover' in content.group(1), content.group(1)


@pytest.mark.parametrize('page', [INDEX, SHELL])
def test_the_status_bar_is_opaque_in_both_entry_points(page):
    """black-translucent draws the page under the clock, and iOS writes that
    clock in light ink whatever the page behind it is -- white on the light
    theme's cream. The two pages have to say the same thing, because either
    can be the one added to a home screen."""
    text = head(page)
    assert 'content="black-translucent"' not in text
    assert 'name="apple-mobile-web-app-status-bar-style" content="default"' in text


@pytest.mark.parametrize('page', [INDEX, SHELL])
def test_theme_color_is_declared_for_each_theme(page):
    """One unconditional theme-color paints the status bar and the Android
    address bar the dark theme's colour even when the light theme is on."""
    found = dict(re.findall(
        r'<meta name="theme-color" content="(#[0-9a-f]{6})" media="\(prefers-color-scheme: (\w+)\)">',
        head(page)))
    assert set(found.values()) == {'dark', 'light'}, f'{page.name} declares {found}'
    assert found['#0e1512'] == 'dark' and found['#f2f0e8'] == 'light'


def test_the_app_bar_is_hidden_until_the_page_is_running_as_an_app():
    """Every rule that reveals the bar hangs off body.is-app. A rule that
    forgot the gate would put a fixed bar across the bottom of the site in an
    ordinary browser tab, which is the one failure mode of this whole change
    that no test driving the page would notice -- jsdom does not lay out."""
    style = SHELL.read_text(encoding='utf-8').split('</style>', 1)[0]
    # Rules on the BAR, not on what is inside it: a link styled display:flex
    # reveals nothing while its container is display:none.
    reveals = [(selector.strip(), body) for selector, body in
               re.findall(r'([^{}]*)\{([^{}]*)\}', style)
               if re.search(r'\.app-tabs\s*$', selector)
               and re.search(r'display:\s*(?!none)', body)]
    assert reveals, 'nothing in the stylesheet ever shows the bar'
    for selector, body in reveals:
        assert 'body.is-app' in selector, f'{selector} shows the bar unconditionally'
