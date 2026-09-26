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


def _shell(root: Path) -> str:
    return (root / "ui" / "dashboard_shell.html").read_text(encoding="utf-8")


def test_every_routable_view_has_a_renderer_and_a_link(repo_root):
    """
    ui/navigation.js decides which URLs exist; ui/dashboard_shell.html decides
    what they draw and how they are reached. A view added to one and not the other
    is a route the router will happily send a reader to, showing nothing.
    """
    nav = (repo_root / "ui" / "navigation.js").read_text(encoding="utf-8")
    shell = _shell(repo_root)
    views_block = re.search(r"const views = \{(.*?)\};", nav, re.S).group(1)

    rankings = re.findall(r"'([a-z0-9-]+)'", re.search(r"rankings: \[([^\]]*)\]", nav).group(1))
    assert "conference-coe" in rankings and "conference-coe2" in rankings, \
        "CoE v1 and CoE 2.0 conference rankings are separate views and must stay separate"
    labels = dict(re.findall(r"\['([a-z0-9-]+)', '([^']+)'\]",
                             re.search(r"rankings: \[(.*?)\],\n\s*playoff:", shell, re.S).group(1)))
    for view in rankings:
        assert view in labels, f"rankings view {view} has no tab label, so nothing links to it"
    assert labels["conference-coe2"] != labels["conference-coe"], \
        "the two conference rankings must be distinguishable in the tab bar"
    view_map = re.search(r"const views = \{ elo: renderElo,(.*?)\};", shell, re.S).group(1)
    assert "'conference-coe2': renderConferenceCoe2" in view_map
    # Every rankings view, not just the two named above, has to be drawable: a
    # routable view with no entry here renders undefined and throws (P1-05).
    for view in rankings:
        assert f"'{view}'" in view_map or f"{view}:" in view_map or view == "elo", \
            f"rankings view {view} is routable but has no renderer"
    assert "elo-weekly" in rankings, "the week-by-week Elo view must stay reachable"

    sections = re.findall(r"([a-z]+): \[\]", views_block)
    assert "coverage" in sections
    for section in sections:
        assert f"state.section === '{section}'" in shell, f"section {section} is never rendered"
        assert f'data-section="{section}"' in shell, f"section {section} has no navigation link"


def test_the_coverage_page_is_the_one_place_the_season_table_lives(repo_root):
    """
    Promoting coverage out of Methodology is only worth it if it did not leave a
    second copy behind: two tables drift, and a reader cannot tell which is current.
    """
    shell = _shell(repo_root)
    assert shell.count("coverageTableHtml()") == 2, \
        "coverageTableHtml is defined once and called once, from the coverage page"
    assert "function renderCoverage()" in shell
    # Methodology still explains coverage, but sends the reader to the page for it.
    methodology = shell.split("function renderMethodology()")[1].split("function ")[0]
    assert "coverageTableHtml()" not in methodology
    assert "section: 'coverage'" in methodology, "Methodology must link to the coverage page"
