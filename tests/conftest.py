"""
Shared fixtures for the test suite.

Most of these tests run against the REAL db/league.db (the one you build
with run_pipeline.py), because the things worth testing here are properties
of the actual data and actual algorithms, not synthetic toy cases. Tests
that need db/league.db skip cleanly (rather than failing) if it doesn't
exist yet, since a fresh checkout of the repo won't have it built.

sys.path is set up here so tests can import from src/ and src/coefficients/
exactly the way the scripts themselves do (see export_dashboard_data.py's
own sys.path.insert for the same pattern).
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "src" / "coefficients"))

DB_PATH = REPO_ROOT / "db" / "league.db"
BACKUP_DB_PATH = REPO_ROOT / "db" / "league_backup_before_playoff_migration.db"

# Seasons with real conference-membership data, so playoff-field/bracket
# tests have something meaningful to check. Kept as a plain list (not
# dynamically computed from the DB) so a test failure due to missing data
# is obvious, rather than a test that silently checks zero years.
PLAYOFF_YEARS = [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]

# Teams that must NEVER reappear after a bootstrap -- see
# bootstrap_league_db.py's DEAD_TEAMS for why each one is dead.
DEAD_TEAMS = ["UMass", "North Dakota State", "Sacramento State"]


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture(scope="session")
def db_path() -> Path:
    if not DB_PATH.exists():
        pytest.skip(f"{DB_PATH} does not exist -- run run_pipeline.py first to build it")
    return DB_PATH


@pytest.fixture(scope="session")
def db_conn(db_path: Path):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def backup_db_path() -> Path:
    if not BACKUP_DB_PATH.exists():
        pytest.skip(f"{BACKUP_DB_PATH} does not exist -- can't test bootstrap without it")
    return BACKUP_DB_PATH
