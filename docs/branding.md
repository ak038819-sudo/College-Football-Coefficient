# The mark, the favicon and link previews

## One source

`ui/brand/mark.svg` is the logo. Everything else is derived from it:

| File | What it is for |
| --- | --- |
| `ui/brand/mark.svg` | the masthead, and the favicon in every modern browser |
| `ui/brand/icon-32.png` | the favicon fallback for browsers that will not take an SVG |
| `ui/brand/apple-touch-icon.png` | the icon when the site is added to an iOS home screen |
| `ui/brand/og-image.png` | the 1200x630 card shown when a link is pasted anywhere |
| `ui/brand/icon-192.png` | the home-screen icon Android asks for by name |
| `ui/brand/icon-512.png` | the splash-screen icon of an installed app |
| `ui/brand/icon-maskable-512.png` | the same mark inset to 60%, so a launcher can crop it to its own silhouette without cutting the bars |

Regenerate the PNGs after any edit to the mark, and commit what it writes:

```
python3 scripts/build_brand_assets.py
```

Install `requirements-dev.txt` first. Icons are rasterized directly from SVG with
CairoSVG so small browser windows cannot clip the bars. Run
`python3 scripts/build_brand_assets.py --icons-only` to rebuild only the icons.
The preview card still requires Chromium; a full build checks for it before writing.

After deployment, existing iPhone shortcuts may retain the old icon. Remove the
shortcut and add the site to the Home Screen again to refresh it.

**To change the logo**, replace `ui/brand/mark.svg` and re-run that script.
Nothing else needs touching: the masthead, both favicons, the touch icon, the
three install icons and the preview card all come from it. The two marks that were not chosen are kept
outside the repository, in the project's `branding/` folder, beside the
contact sheet they were picked from.

## Why the tags are duplicated

The preview tags appear twice, in `index.html` and in
`ui/dashboard_shell.html`. That is deliberate. GitHub Pages serves this
repository's root, so a link to the site root lands on `index.html`, which
refreshes to the dashboard — and a crawler building a preview reads the page
it fetched and never follows that refresh. Tags only on the dashboard would
mean a bare link to the site previews as nothing.

`tests/test_brand_assets.py` holds the two sets to saying the same thing, and
checks the rest of what cannot be seen from a green build:

- `og:image` is absolute. A crawler does not resolve a relative one against
  the page it fetched; it just shows a card with no image and no error.
- Every icon and image path resolves to a file that actually exists.
- `og:image:width` and `og:image:height` match the real PNG, so regenerating
  the card at another size cannot quietly make them a lie.
- `twitter:card` is `summary_large_image`. On the default `summary` the card
  is drawn as a small square thumbnail instead.

## Checking a preview for real

None of this is visible from the test suite. After a deploy, paste the site
URL into whatever you are sharing to, or use any Open Graph debugger. Crawlers
cache aggressively, so a preview that was wrong before the fix can stay wrong
for a while and generally needs the debugger's re-scrape to clear.
