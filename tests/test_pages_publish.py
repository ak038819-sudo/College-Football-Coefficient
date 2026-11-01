"""What GitHub Pages publishes, and what it must never stop publishing.

Pages serves the branch's whole tree, so `_config.yml`'s `exclude` is the only
thing keeping 179 MB of CFBD build inputs off the public site. Two claims are
worth pinning, and the second is the dangerous one: a typo in that list does
not fail a build, it silently stops serving a file and takes the site down.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]

# The site's entry points: everything else is reached from these.
ROOT_FILES = ("index.html", "service-worker.js", "manifest.webmanifest")

# Jekyll's own default exclusions, restated in _config.yml because naming
# `exclude` replaces the defaults rather than adding to them.
JEKYLL_DEFAULTS = {"node_modules", "vendor", "Gemfile", "Gemfile.lock"}


@pytest.fixture(scope="module")
def excluded() -> set[str]:
    config = yaml.safe_load((ROOT / "_config.yml").read_text(encoding="utf-8"))
    names = config.get("exclude") or []
    assert names, "_config.yml excludes nothing, so the whole repo is published"
    return set(names)


def published(path: str, excluded: set[str]) -> bool:
    """Would Pages serve this repo-relative path?"""
    parts = Path(path).parts
    if not parts:
        return False
    # Jekyll skips anything whose name starts with a dot or an underscore.
    if any(part.startswith((".", "_")) for part in parts):
        return False
    for i in range(1, len(parts) + 1):
        if str(Path(*parts[:i])) in excluded:
            return False
    return True


def test_the_site_s_own_entry_points_are_published(excluded):
    for name in (*ROOT_FILES, "ui"):
        assert published(name, excluded), f"{name} is excluded, which breaks the site"


def test_every_excluded_path_exists_or_is_a_jekyll_default(excluded):
    """An exclusion naming nothing is a rename nobody followed through: the
    directory it meant to keep private is published again, silently."""
    for name in sorted(excluded - JEKYLL_DEFAULTS):
        assert (ROOT / name).exists(), (
            f"_config.yml excludes {name}, which is not in the repo -- either "
            "a typo or a path that moved, and in both cases it protects nothing")


# There is deliberately no text scan for "does a published file mention an
# excluded directory". service-worker.js contains
# `url.pathname.endsWith('/data/live_scores.json')`, which is a suffix test
# matching ui/data/live_scores.json, not a reference to the root data/. A
# regex cannot tell that apart from a fetch without guessing, and a guess here
# either cries wolf or passes everything. The claim that matters is the next
# test, which resolves every path the site actually names.


def _referenced_assets() -> set[str]:
    """Every file the site names, as a repo-relative path."""
    wanted: set[str] = set()
    sw = (ROOT / "service-worker.js").read_text(encoding="utf-8")
    for block in re.findall(r"const PRECACHE_\w+ = \[(.*?)\];", sw, re.S):
        wanted.update(re.findall(r"'([^']+)'", block))
    manifest = json.loads((ROOT / "manifest.webmanifest").read_text(encoding="utf-8"))
    for icon in manifest.get("icons", []):
        wanted.add(icon["src"].lstrip("/"))
    index = (ROOT / "index.html").read_text(encoding="utf-8")
    for href in re.findall(r'(?:href|src)="([^"#:]+)"', index):
        wanted.add(href)
    # The lazily-loaded data files, which the dashboard resolves against ui/.
    static = json.loads((ROOT / "ui/data/static_manifest.json").read_text(encoding="utf-8"))
    urls: list[str] = []

    def walk(node):
        if isinstance(node, str):
            if node.endswith(".js") or ".js?" in node:
                urls.append(node)
        elif isinstance(node, dict):
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(static)
    assert len(urls) > 100, f"only {len(urls)} data URLs found; the walk is not reading the manifest"
    wanted.update("ui/" + url.split("?")[0] for url in urls)
    return wanted


def test_every_file_the_site_names_is_still_published(excluded):
    """The check that matters. Each of these is fetched by the live page, so
    one of them excluded is a 404 on a working build."""
    assets = _referenced_assets()
    assert len(assets) > 100, "the reference scan found almost nothing, so it proves nothing"
    missing = [a for a in sorted(assets) if not published(a, excluded)]
    assert not missing, f"the site fetches these and _config.yml excludes them: {missing[:10]}"
    absent = [a for a in sorted(assets) if not (ROOT / a).exists()]
    assert not absent, f"the site fetches these and they are not in the repo: {absent[:10]}"
