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

from bootstrap_league_db import DEAD_TEAMS  # noqa: E402 -- the real, authoritative list; a
# separate hardcoded copy here previously drifted out of sync with it (still listed
# North Dakota State and Sacramento State as dead well after bootstrap_league_db.py's
# real list was fixed to treat them as real 2026 FBS debutants, not dead data -- this
# silently failed CI's "test" job on every commit since, which meant the dependent
# "rebuild-and-deploy" job never ran either, leaving the live site stuck on stale data
# through dozens of otherwise-successful-looking pushes). Importing directly instead of
# duplicating means this can't drift out of sync again.

DB_PATH = REPO_ROOT / "db" / "league.db"
BACKUP_DB_PATH = REPO_ROOT / "db" / "league_backup_before_playoff_migration.db"

# Seasons with real conference-membership data, so playoff-field/bracket
# tests have something meaningful to check. Kept as a plain list (not
# dynamically computed from the DB) so a test failure due to missing data
# is obvious, rather than a test that silently checks zero years.
PLAYOFF_YEARS = [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]


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
