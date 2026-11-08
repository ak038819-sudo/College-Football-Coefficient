"""The Modern must style genuine team routes without changing their behavior."""
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]


def test_team_style_is_scoped_and_built_with_cache_version():
    css = (ROOT / "ui/team-modern.css").read_text()
    assert ".team-header.panel" in css
    assert ".team-header ~ .page-tabs" in css
    assert "prefers-reduced-motion" in css
    assert "@media(max-width: 520px)" in css


def test_every_stylesheet_is_linked_with_the_hash_of_its_own_bytes():
    """Drive the builder, rather than reading its source for a literal.

    This used to assert that the string `team-modern.css?v=` appeared in
    build_dashboard.py. That passed while the builder wrote the link with a
    hard-coded name, and it FAILED the moment the same link came from a loop
    over two stylesheets -- a change that altered nothing about the page. A
    test that reads source text is testing how the code is spelled.
    """
    import hashlib
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "build_dashboard", ROOT / "build_dashboard.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Every stylesheet in ui/ must be linked: one added and not listed is a
    # file the page never loads.
    linked = module.stylesheet_links()
    on_disk = sorted(path.name for path in (ROOT / "ui").glob("*.css"))
    assert sorted(path.name for path in module.STYLESHEETS) == on_disk

    for path in module.STYLESHEETS:
        want = hashlib.sha256(path.read_bytes()).hexdigest()[:12]
        assert f'<link rel="stylesheet" href="{path.name}?v={want}">' in linked
    # The hash is of THAT file, not of some other one: changing a byte of one
    # stylesheet must change its own stamp and no other.
    first = module.STYLESHEETS[0]
    before = module.stylesheet_links()
    original = first.read_bytes()
    try:
        first.write_bytes(original + b"\n/* */\n")
        after = module.stylesheet_links()
    finally:
        first.write_bytes(original)
    assert after != before
    assert after.splitlines()[1:] == before.splitlines()[1:]

    # The shell links none of them itself; they are all added at build time,
    # so a stylesheet left out of STYLESHEETS reaches no reader.
    assert '<link rel="stylesheet"' not in module.SHELL_PATH.read_text(
        encoding="utf-8")


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
