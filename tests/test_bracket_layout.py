"""
The bracket fits the panel it is drawn in (UI).

This is a CSS test, which is unusual here, and it exists because the bug it
covers cannot be reached from Python or from a vm sandbox: every slot in the
12-team bracket overflowed its grid cell on the live site, overdrawing the
round titles, clipping the last row and growing a scrollbar inside the panel.
Nothing was wrong with the markup or the JS -- the row height was a 40px
literal while the tallest slot rendered 153px, because a bracket cell was
inheriting the site-wide 63px team chip.

So the properties worth pinning are the DEPENDENCIES between the numbers, not
the numbers: the row height has to be computed from the same variable the
slots are, and the chip inside the bracket has to be sized by the bracket
rather than inherited. A test that asserted "the row is 76px" would pass a
redesign and fail a rename, which is backwards.

Verified in Chromium before this was written: with these rules the tree's
scrollHeight equals its clientHeight at 768-1600px wide, and no slot spills
its cell. That measurement is what the relationships below stand in for; see
docs/cfp-12-team-format.md.
"""
from __future__ import annotations

import re

import pytest


@pytest.fixture(scope="module")
def shell(repo_root):
    return (repo_root / "ui" / "dashboard_shell.html").read_text(encoding="utf-8")


def rule(shell: str, selector: str) -> str:
    """The declarations of the first rule whose selector list contains
    `selector` as a whole selector. Raises rather than returning '' so a
    renamed selector fails loudly instead of vacuously."""
    for match in re.finditer(r"([^{}]+)\{([^{}]*)\}", shell):
        heading = re.sub(r"/\*.*?\*/", " ", match.group(1), flags=re.S)
        selectors = [s.strip() for s in heading.split(",")]
        if selector in selectors:
            return match.group(2)
    raise AssertionError(f"no CSS rule for {selector!r}")


def px(declarations: str, prop: str) -> float:
    m = re.search(rf"(?<![-\w]){re.escape(prop)}\s*:\s*(-?[\d.]+)px", declarations)
    assert m, f"{prop} is not a plain px value in: {declarations.strip()}"
    return float(m.group(1))


def test_the_row_height_is_computed_from_the_slot_height(shell):
    """The two cannot be set independently again. A bare px row height here is
    the exact shape of the bug: it stays put while the slots grow."""
    tree = rule(shell, ".bracket-tree")
    row = re.search(r"--bracket-row\s*:\s*([^;]+);", tree)
    assert row, "the bracket has no --bracket-row"
    assert "var(--bracket-team-row)" in row.group(1), (
        f"--bracket-row does not derive from the team row: {row.group(1).strip()}")


def test_the_grid_rows_and_the_team_rows_read_those_variables(shell):
    assert "var(--bracket-row)" in rule(shell, ".bracket-list")
    assert "var(--bracket-team-row)" in rule(shell, ".fr-team")


def test_a_first_round_card_fits_the_row_it_is_placed_in(shell):
    """The tallest slot in the bracket is the two-team first-round card, and
    the row has to be at least as tall as that card's own declared parts --
    two team rows, the gaps between its three lines, and its padding. This is
    the arithmetic that was wrong: 40px of row against 153px of card."""
    tree = rule(shell, ".bracket-tree")
    card = rule(shell, ".bslot.fr-matchup")
    team_row = px(tree, "--bracket-team-row")

    row_parts = re.search(r"--bracket-row\s*:\s*([^;]+);", tree).group(1)
    row = 2 * team_row + sum(float(n) for n in re.findall(r"\+\s*(-?[\d.]+)px", row_parts))

    needed = 2 * team_row + 2 * px(card, "padding") + 2 * px(card, "gap")
    assert row >= needed, (
        f"the row is {row}px but a first-round card needs at least {needed}px")


def test_the_bracket_sizes_its_own_team_chip(shell):
    """The site-wide chip is far taller than a bracket row, and a bracket that
    inherits it overflows the panel. The override has to be scoped to the
    bracket, so the rest of the page's logos keep their own size."""
    scoped = rule(shell, ".bracket-tree .logo-chip")
    bracket_chip = px(rule(shell, ".bracket-tree"), "--bracket-logo")
    site_chip = px(rule(shell, ".logo-chip.sm"), "height")

    assert "var(--bracket-logo" in scoped, "the scoped chip ignores --bracket-logo"
    assert bracket_chip < site_chip, (
        f"the bracket's chip ({bracket_chip}px) is not smaller than the site's "
        f"({site_chip}px), so scoping it buys nothing")
    assert bracket_chip <= px(rule(shell, ".bracket-tree"), "--bracket-team-row"), (
        "a chip taller than a team row pushes the row open again")


def test_the_tree_scrolls_sideways_and_not_down(shell):
    """Narrow screens are allowed to scroll the bracket horizontally -- that is
    what a bracket does. A vertical scrollbar inside the panel is the bug."""
    tree = rule(shell, ".bracket-tree")
    assert "overflow-x: auto" in tree
    assert "overflow-y" not in tree
    assert "overflow:" not in tree, "a plain overflow would re-enable vertical scrolling"


def test_a_column_grows_with_the_panel(shell):
    """Fitting the panel means filling it, not just staying inside it."""
    col = rule(shell, ".bracket-round-col")
    assert re.search(r"flex\s*:\s*1\s+1\s+0", col), f"columns do not grow: {col.strip()}"
