"""The Modern must style genuine team routes without changing their behavior."""
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]


def test_team_style_is_scoped_and_built_with_cache_version():
    css = (ROOT / "ui/team-modern.css").read_text()
    build = (ROOT / "build_dashboard.py").read_text()
    assert ".team-header.panel" in css
    assert ".team-header ~ .page-tabs" in css
    assert "team-modern.css?v=" in build
    assert "TEAM_MODERN_CSS_PATH" in build
    assert 'final.replace("</head>"' in build
    assert "prefers-reduced-motion" in css
    assert "@media(max-width: 520px)" in css


def test_existing_team_page_uses_same_data_and_all_eight_tabs():
    shell = (ROOT / "ui/dashboard_shell.html").read_text()
    assert "'<div class=\"panel team-header\">'" in shell
    assert "teamStat('Elo'" in shell
    assert "teamStat('Merits (season)'" in shell
    assert "teamStat('Legacy Merits (5-yr)'" in shell
    assert "teamStat('Record'" in shell
    assert "id=\"team-stadiums\"" in shell
    assert "id=\"team-coach\"" in shell
    tabs = re.search(r"const TEAM_TAB_LABELS = \{([^}]+)\}", shell)
    assert tabs
    for name in ("overview", "week", "schedule", "roster", "stats",
                 "leaders", "history", "analytics"):
        assert re.search(r"\b" + name + r"\s*:", tabs.group(1))
    assert "pageTabNav('team', TEAM_TAB_LABELS)" in shell


def test_normal_production_builder_always_generates_and_renders_modern_dashboard():
    """Protect against a literal backslash-n turning the final build into a comment."""
    import ast
    root = ast.parse((ROOT / "build_dashboard.py").read_text())
    main = next(n for n in root.body if isinstance(n, ast.FunctionDef) and n.name == "main")
    calls = [n for n in ast.walk(main) if isinstance(n, ast.Call)]
    render_calls = [n for n in calls
                    if isinstance(n.func, ast.Name) and n.func.id == "render_from_exports"]
    # One is the preview path; the other MUST be the normal production path.
    assert len(render_calls) == 2
    assert any(
        isinstance(n, ast.Constant) and n.value == "src/export_descriptive_site_assets.py"
        for n in ast.walk(main)
    ), "Normal production build must create the source-backed scouting assets"
    assert "\\n    render_from_exports()" not in (ROOT / "build_dashboard.py").read_text()
