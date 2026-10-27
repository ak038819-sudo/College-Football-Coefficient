"""The rule that decides whether a logo needs something behind it.

src/export_logo_files.py has carried a comment since the chip system was
removed saying "tests/test_logo_contrast.py holds these to the stylesheet,
which is the only thing that makes them true". That file did not exist. The
backgrounds were therefore free to drift away from the shells, silently, and
a drift there does not break anything loudly -- it just starts answering the
question about the wrong page.

Now two shells share the rule, so this pins both.
"""

import io
import re
from pathlib import Path

import pytest

from src import logo_contrast

REPO = Path(__file__).resolve().parent.parent
SHELLS = {
    'ui/dashboard_shell.html': REPO / 'ui' / 'dashboard_shell.html',
    'ui/nfl.html': REPO / 'ui' / 'nfl.html',
}


def backgrounds(text):
    """Every `--bg: #rrggbb` the stylesheet declares, in order."""
    return [tuple(int(hit[i:i + 2], 16) for i in (0, 2, 4))
            for hit in re.findall(r'--bg:\s*#([0-9a-fA-F]{6})', text)]


class TestTheBackgroundsAreTheStylesheets:
    @pytest.mark.parametrize('name', sorted(SHELLS))
    def test_the_dark_background_is_the_first_one_the_shell_declares(self, name):
        """The dark palette is the default in both shells: the first --bg."""
        found = backgrounds(SHELLS[name].read_text(encoding='utf-8'))
        assert found, f'{name} declares no --bg at all'
        assert found[0] == logo_contrast.DARK_BG

    @pytest.mark.parametrize('name', sorted(SHELLS))
    def test_the_light_background_is_the_one_the_override_declares(self, name):
        found = backgrounds(SHELLS[name].read_text(encoding='utf-8'))
        rest = [bg for bg in found[1:] if bg != logo_contrast.DARK_BG]
        assert rest, f'{name} declares no light --bg'
        assert set(rest) == {logo_contrast.LIGHT_BG}

    def test_both_shells_agree(self):
        """The rule is shared, so a palette split between the two pages would
        make one of the two answers wrong wherever they differ."""
        seen = {name: set(backgrounds(path.read_text(encoding='utf-8')))
                for name, path in SHELLS.items()}
        assert len(set(map(frozenset, seen.values()))) == 1, seen


def square(rgb, size=16, alpha=255):
    from PIL import Image
    return Image.new('RGBA', (size, size), rgb + (alpha,))


def png(image):
    buffer = io.BytesIO()
    image.save(buffer, format='PNG')
    return buffer.getvalue()


class TestWhatCountsAsDim:
    def test_a_black_mark_vanishes_on_the_dark_page_and_not_the_light_one(self):
        black = png(square((0, 0, 0)))
        assert logo_contrast.is_dim_on(black, logo_contrast.DARK_BG)
        assert not logo_contrast.is_dim_on(black, logo_contrast.LIGHT_BG)

    def test_a_white_mark_vanishes_on_the_light_page_and_not_the_dark_one(self):
        white = png(square((255, 255, 255)))
        assert logo_contrast.is_dim_on(white, logo_contrast.LIGHT_BG)
        assert not logo_contrast.is_dim_on(white, logo_contrast.DARK_BG)

    def test_a_fully_transparent_mark_is_not_dim(self):
        """No ink is not the same as invisible ink: there is nothing to
        plate, and dividing by zero ink would be the alternative."""
        clear = png(square((0, 0, 0), alpha=0))
        assert not logo_contrast.is_dim_on(clear, logo_contrast.DARK_BG)
        assert not logo_contrast.is_dim_on(clear, logo_contrast.LIGHT_BG)

def two_tone(white_cells, total=256):
    """A 32x32 mark of white and black, `white_cells` of 256 cells white.

    Each logical cell is drawn as a 2x2 block, because is_dim_on samples
    every other pixel: without that the sampled grid could hold a different
    ratio from the image and the fixture would not be the thing measured.
    """
    from PIL import Image
    image = Image.new('RGBA', (32, 32), (0, 0, 0, 255))
    for cell in range(total):
        if cell >= white_cells:
            continue
        cx, cy = cell % 16, cell // 16
        for dx in (0, 1):
            for dy in (0, 1):
                image.putpixel((cx * 2 + dx, cy * 2 + dy), (255, 255, 255, 255))
    return image


class TestTheShareRuleNotTheMean:
    """A white mark with heavy black in it averages to a dark grey that
    clears 3:1 against nothing, while most of its ink is white and perfectly
    readable. Mean luminance calls that mark dim and plates it for no
    reason. This is the exact mistake the college comment warns about, and
    the one I made measuring these marks by hand before writing this.
    """

    # 94 of 256 cells white: the mean fails 3:1 on the dark page (2.84)
    # while 37% of the ink clears it, over the 35% the rule asks for.
    FIXTURE = 94

    def mean_rgb(self, image):
        pixels = [image.getpixel((x, y))[:3]
                  for x in range(image.width) for y in range(image.height)]
        return tuple(sum(c) / len(pixels) for c in zip(*pixels))

    def test_the_fixture_really_does_split_the_two_rules(self):
        """Guarding the fixture, not the code: if this stops holding, the
        test below proves nothing and must be rebuilt rather than deleted."""
        image = two_tone(self.FIXTURE)
        mean = logo_contrast.relative_luminance(
            [round(v) for v in self.mean_rgb(image)])
        dark = logo_contrast.relative_luminance(logo_contrast.DARK_BG)
        assert logo_contrast.contrast(mean, dark) < 3.0, (
            'the MEAN of this fixture should fail 3:1 on the dark page')
        assert self.FIXTURE / 256 >= logo_contrast.READABLE_SHARE, (
            'while its readable SHARE should clear the bar')

    def test_the_mark_is_not_dim_because_most_of_its_ink_is_readable(self):
        assert not logo_contrast.is_dim_on(
            png(two_tone(self.FIXTURE)), logo_contrast.DARK_BG)

    def test_the_same_mark_with_less_white_is_dim(self):
        """Below the share, the plate is earned. 64 of 256 is 25%."""
        assert logo_contrast.is_dim_on(
            png(two_tone(64)), logo_contrast.DARK_BG)

    def test_nearly_transparent_pixels_are_not_counted_as_ink(self):
        """Antialiased edges are not the mark."""
        ghost = png(square((0, 0, 0), alpha=logo_contrast.INK_ALPHA - 1))
        assert not logo_contrast.is_dim_on(ghost, logo_contrast.DARK_BG)


class TestNamingTheThemes:
    def test_each_cut_is_judged_against_the_page_it_is_shown_on(self):
        """A light-ink mark on dark and a colour mark on light is the normal
        case, and neither is dim."""
        assert logo_contrast.dim_themes(
            png(square((255, 255, 255))), png(square((0, 0, 0)))) == []

    def test_one_piece_of_artwork_can_be_dim_on_one_side_only(self):
        black = png(square((0, 0, 0)))
        assert logo_contrast.dim_themes(black, black) == ['dark']
        white = png(square((255, 255, 255)))
        assert logo_contrast.dim_themes(white, white) == ['light']

    def test_no_flat_colour_is_dim_on_both_pages(self):
        """A property of this palette, worth stating because it bounds what
        the page has to handle: the two backgrounds are far enough apart
        that clearing 3:1 against neither is impossible for a solid colour.
        To be dim on the dark page a colour needs luminance below 0.120; to
        be dim on the light one, above 0.257. So `dim` is never both, and a
        club always has at least one theme where its mark stands alone.
        """
        both = [v for v in range(0, 256, 5)
                if logo_contrast.dim_themes(*([png(square((v, v, v)))] * 2))
                == ['dark', 'light']]
        assert both == [], f'these greys were dim on both: {both}'
