"""Would a logo disappear against the page it lands on?

One rule, two leagues. The college exporter (src/export_logo_files.py) has
measured this since the chip system was removed; the NFL fetcher
(src/nfl/logos.py) needs the same answer about the same two backgrounds,
because ui/nfl.html and ui/dashboard_shell.html share a palette. Copying it
would have been two rules drifting apart, so it lives here and both import
it.

tests/test_logo_contrast.py holds the two backgrounds to BOTH stylesheets,
which is the only thing that makes them true.
"""

from __future__ import annotations

import io

# The two page backgrounds a mark can land on, as --bg in the two shells.
DARK_BG = (0x0e, 0x15, 0x12)
LIGHT_BG = (0xf2, 0xf0, 0xe8)
# A mark is "dim" on a background when less than this share of its ink clears
# 3:1 against it. Mean luminance was tried first and reads a white mark with a
# navy outline as mid-grey, which is neither; the share of ink you can actually
# see is what decides whether the logo needs something behind it.
READABLE_CONTRAST = 3.0
READABLE_SHARE = 0.35
INK_ALPHA = 160


def relative_luminance(rgb) -> float:
    def channel(value: int) -> float:
        v = value / 255
        return v / 12.92 if v <= 0.04045 else ((v + 0.055) / 1.055) ** 2.4
    r, g, b = (channel(v) for v in rgb)
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def contrast(a: float, b: float) -> float:
    hi, lo = max(a, b), min(a, b)
    return (hi + 0.05) / (lo + 0.05)


def is_dim_on(raw: bytes, background) -> bool:
    """Would this mark disappear against that background?

    Sampled every other pixel in each direction, which is plenty on a 256px
    image and keeps a whole-set export to a second or so.
    """
    from PIL import Image
    image = Image.open(io.BytesIO(raw)).convert("RGBA")
    pixels = image.load()
    bg = relative_luminance(background)
    ink = readable = 0
    for y in range(0, image.height, 2):
        for x in range(0, image.width, 2):
            r, g, b, a = pixels[x, y]
            if a < INK_ALPHA:
                continue
            ink += 1
            if contrast(relative_luminance((r, g, b)), bg) >= READABLE_CONTRAST:
                readable += 1
    if not ink:
        return False
    return readable / ink < READABLE_SHARE


def dim_themes(dark_bytes: bytes, light_bytes: bytes) -> list:
    """The themes a club's artwork disappears on, as the page names them.

    Each cut is judged against the background it is actually shown on, which
    is the whole point: a light-ink mark is dim on the light page and fine on
    the dark one. Where a club has only one piece of artwork the caller
    passes the same bytes twice, which is the case for 24 of the 32 NFL
    clubs.
    """
    out = []
    if is_dim_on(dark_bytes, DARK_BG):
        out.append('dark')
    if is_dim_on(light_bytes, LIGHT_BG):
        out.append('light')
    return out
