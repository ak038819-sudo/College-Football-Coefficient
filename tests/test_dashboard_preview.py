"""UI preview builds must preserve every authoritative data export."""
import hashlib
import importlib.util
import re
from pathlib import Path


def test_preview_uses_existing_exports_without_recomputing(tmp_path, monkeypatch):
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("dashboard_builder", root / "build_dashboard.py")
    builder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(builder)
    # Isolate output while reading real committed inputs; no database is needed.
    monkeypatch.chdir(root)
    monkeypatch.setattr(builder, "OUT_PATH", tmp_path / "dashboard.html")
    monkeypatch.setattr(builder.subprocess, "run", lambda *a, **kw: (_ for _ in ()).throw(AssertionError("unexpected model rebuild")))
    inputs = [builder.DATA_PATH, builder.TEAM_PAGES_PATH, builder.CONFERENCE_PAGES_PATH,
              builder.LOGO_MANIFEST_PATH, builder.STATIC_MANIFEST_PATH]
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs}
    builder.render_from_exports()
    assert before == {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs}
    html = builder.OUT_PATH.read_text(encoding="utf-8")
    assert "__NAVIGATION_VERSION__" not in html
    assert "__DATA_JSON__" not in html
    version = hashlib.sha256(builder.NAVIGATION_PATH.read_bytes()).hexdigest()[:12]
    assert f'navigation.js?v={version}' in html
    assert "__SEARCH_VERSION__" not in html
    search_version = hashlib.sha256(builder.SEARCH_PATH.read_bytes()).hexdigest()[:12]
    assert f'search.js?v={search_version}' in html
    # Every lazily-loaded data file is content-versioned, so a cached page can never
    # be paired with a stale data file (Milestone E adds the conference one).
    for path, placeholder in ((builder.TEAM_PAGES_PATH, "__TEAM_PAGES_VERSION__"),
                              (builder.CONFERENCE_PAGES_PATH, "__CONFERENCE_PAGES_VERSION__")):
        assert placeholder not in html
        assert hashlib.sha256(path.read_bytes()).hexdigest()[:12] in html


def test_every_routable_rankings_view_can_actually_render(repo_root):
    """
    navigation.js decides which rankings views are reachable; the shell decides
    which ones can be drawn. A view added to one and not the other is a URL that
    resolves to a blank page, and nothing else would catch it.
    """
    nav = (repo_root / "ui" / "navigation.js").read_text(encoding="utf-8")
    shell = (repo_root / "ui" / "dashboard_shell.html").read_text(encoding="utf-8")
    listed = re.search(r"rankings:\s*\[([^\]]*)\]", nav)
    assert listed, "navigation.js no longer declares its rankings views the expected way"
    views = re.findall(r"'([a-z0-9-]+)'", listed.group(1))
    assert "elo-weekly" in views, "the week-by-week view must stay reachable"

    renderers = re.search(r"const views = \{ elo: renderElo.*?\};", shell, re.S)
    assert renderers, "the rankings render dispatch moved; this guard needs updating"
    tabs = re.search(r"rankings: \[\[(.*?)\]\],\n", shell, re.S)
    assert tabs, "the rankings tab labels moved; this guard needs updating"
    for view in views:
        assert f"'{view}'" in renderers.group(0) or f"{view}:" in renderers.group(0), \
            f"{view} is routable but has no renderer"
        assert f"'{view}'" in tabs.group(0), f"{view} is routable but has no tab label"
