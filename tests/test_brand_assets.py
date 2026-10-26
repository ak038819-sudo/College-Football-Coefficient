"""The favicon and link-preview tags: the parts that break silently.

Nothing in the test suite renders a browser tab or a Slack unfurl, so every
failure mode here is invisible until someone pastes a link: a relative
og:image that crawlers will not resolve, an icon pointing at a path that is
not published, declared image dimensions that stopped matching the PNG, or the
root redirect and the dashboard drifting apart on what the site is called.

The root index.html matters as much as the dashboard: a link to the site root
is what people paste, and a crawler reading it never follows the refresh.
"""
from __future__ import annotations

import re
import struct
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
INDEX = ROOT / 'index.html'
SHELL = ROOT / 'ui' / 'dashboard_shell.html'
SITE = 'https://ak038819-sudo.github.io/College-Football-Coefficient/'

# Tags that must say the same thing on both pages, or a preview depends on
# which URL was shared.
SHARED = ('og:title', 'og:description', 'og:site_name', 'og:url', 'og:image',
          'og:image:width', 'og:image:height')


def head(path):
    text = path.read_text(encoding='utf-8')
    return text.split('</head>', 1)[0]


def meta(path, key):
    attr = 'name' if key.startswith('twitter:') else 'property'
    found = re.findall(
        rf'<meta\s+{attr}="{re.escape(key)}"\s+content="([^"]*)"', head(path))
    assert found, f'{path.name} has no {key}'
    assert len(found) == 1, f'{path.name} declares {key} {len(found)} times'
    return found[0]


def icon_hrefs(path):
    return re.findall(r'<link\s+rel="(?:icon|apple-touch-icon)"[^>]*href="([^"]+)"',
                      head(path))


def png_size(path):
    """Width and height from the IHDR, without needing Pillow."""
    data = path.read_bytes()
    assert data[:8] == b'\x89PNG\r\n\x1a\n', f'{path} is not a PNG'
    return struct.unpack('>II', data[16:24])


def local_for(url):
    """The file in this repository that a published absolute URL serves."""
    assert url.startswith(SITE), f'{url} is not on this site'
    return ROOT / url[len(SITE):]


# ---- the assets exist --------------------------------------------------------

@pytest.mark.parametrize('page,base', [(INDEX, ROOT), (SHELL, ROOT / 'ui')])
def test_every_icon_resolves_to_a_file_that_exists(page, base):
    hrefs = icon_hrefs(page)
    assert hrefs, f'{page.name} declares no icon at all'
    for href in hrefs:
        assert not href.startswith(('http://', 'https://', '/')), \
            f'{href} should be relative to the page that declares it'
        assert (base / href).is_file(), f'{page.name} points at a missing {href}'


def test_the_mark_is_the_source_and_the_pngs_are_derived_from_it():
    brand = ROOT / 'ui' / 'brand'
    assert (brand / 'mark.svg').is_file()
    for name in ('icon-32.png', 'apple-touch-icon.png', 'og-image.png'):
        assert (brand / name).is_file(), f'{name} is missing; run scripts/build_brand_assets.py'


# ---- the preview tags --------------------------------------------------------

@pytest.mark.parametrize('page', [INDEX, SHELL])
def test_the_preview_image_is_absolute(page):
    """A crawler does not resolve a relative og:image against the page it
    fetched, so a relative one yields a card with no image and no error."""
    for key in ('og:image', 'twitter:image'):
        assert meta(page, key).startswith('https://'), f'{page.name} {key} is relative'


@pytest.mark.parametrize('page', [INDEX, SHELL])
def test_the_preview_image_is_a_file_this_repository_publishes(page):
    assert local_for(meta(page, 'og:image')).is_file()


@pytest.mark.parametrize('page', [INDEX, SHELL])
def test_the_declared_dimensions_match_the_image(page):
    """Regenerating the asset at another size silently makes these a lie, and
    a preview reserves the wrong box."""
    width, height = png_size(local_for(meta(page, 'og:image')))
    assert (str(width), str(height)) == (meta(page, 'og:image:width'),
                                         meta(page, 'og:image:height'))


@pytest.mark.parametrize('page', [INDEX, SHELL])
def test_a_large_card_is_requested(page):
    """With the default `summary`, the image shows as a small square thumbnail
    rather than the 1200x630 card it was drawn as."""
    assert meta(page, 'twitter:card') == 'summary_large_image'


def test_the_root_and_the_dashboard_agree():
    """Whichever URL gets pasted, the preview says the same thing."""
    for key in SHARED:
        assert meta(INDEX, key) == meta(SHELL, key), f'{key} differs'


@pytest.mark.parametrize('name', ['apple-touch-icon.png', 'icon-32.png',
                                  'icon-192.png', 'icon-512.png',
                                  'icon-maskable-512.png'])
def test_all_three_logo_bars_are_complete(name):
    """Dimensions alone missed PNGs whose lower bars were cut off."""
    from PIL import Image
    image = Image.open(ROOT / 'ui' / 'brand' / name).convert('RGB')
    size = image.width
    inner = round(size * 0.6) if 'maskable' in name else size
    offset = (size - inner) / 2
    for y, color in [(19.5, (232, 163, 61)), (32, (238, 241, 236)),
                     (44.5, (107, 122, 112))]:
        # Probe each bar's center and lower interior, catching half-clipping.
        for dy in (0, 1):
            point = (round(offset + 23 * inner / 64),
                     int(offset + (y + dy) * inner / 64))
            assert image.getpixel(point) == color, (name, point)
