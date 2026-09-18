"""
The full SQL schema chain must build cleanly with no conflicts, in the
exact order the project's docs/README specify. This is the check that
would have caught the original team_aliases-missing-from-schema.sql bug
and the duplicate-table-definition conflicts we found early in this
project, before they ever reached a real database build.
"""
import sqlite3

SCHEMA_FILES_IN_ORDER = [
    "sql/schema.sql",
    "sql/standings_tables.sql",
    "sql/coe_tables.sql",
    "sql/team_coe_tables.sql",
    "sql/coe_rolling_tables.sql",
    "sql/playoff_qualifiers_tables.sql",
    "sql/playoff_field_by_year.sql",
    "sql/playoff_bracket_by_year.sql",
    "sql/playoff_draws_by_year.sql",
    "sql/playoff_year1_tables.sql",
    "sql/playoff_games_tables.sql",
    "sql/views/v_games_enriched.sql",
]


def test_full_schema_chain_builds_clean(repo_root):
    conn = sqlite3.connect(":memory:")
    for rel_path in SCHEMA_FILES_IN_ORDER:
        path = repo_root / rel_path
        assert path.exists(), f"Expected schema file missing: {rel_path}"
        sql = path.read_text()
        try:
            conn.executescript(sql)
        except sqlite3.OperationalError as e:
            raise AssertionError(f"{rel_path} failed to execute: {e}") from e
    conn.close()


def test_team_aliases_table_is_defined(repo_root):
    """
    Regression test for a real bug found in this project: team_aliases
    was used by load_games.py but never actually defined in any .sql
    file (it only existed by accident in an old backup database).
    """
    conn = sqlite3.connect(":memory:")
    conn.executescript((repo_root / "sql" / "schema.sql").read_text())
    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    assert "team_aliases" in tables
    conn.close()


def test_no_duplicate_table_definitions_across_schema_files(repo_root):
    """
    Regression test for a real bug: team_coefficient_by_year and
    conference_coefficient_by_year were each defined twice (once in the
    old schema.sql, once in their dedicated files), silently conflicting
    depending on which ran last.
    """
    import re

    seen: dict[str, str] = {}
    for rel_path in SCHEMA_FILES_IN_ORDER:
        sql = (repo_root / rel_path).read_text()
        for match in re.finditer(r"CREATE TABLE(?:\s+IF NOT EXISTS)?\s+(\w+)", sql, re.IGNORECASE):
            table = match.group(1)
            if table in seen and seen[table] != rel_path:
                raise AssertionError(
                    f"Table '{table}' is defined in both {seen[table]} and {rel_path}"
                )
            seen[table] = rel_path
