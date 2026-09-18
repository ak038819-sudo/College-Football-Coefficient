"""
A fresh bootstrap must never resurrect the three known-dead duplicate
teams. This is a regression test for a real bug: this cleanup was
originally a one-off manual SQL command that didn't survive a fresh
--force rebuild, and North Dakota State actually reappeared as a 2026
Mountain West "champion" in the real playoff field before being caught.
"""
import sqlite3
import subprocess
import sys

from conftest import DEAD_TEAMS


def test_fresh_bootstrap_excludes_dead_teams(repo_root, backup_db_path, tmp_path):
    out_db = tmp_path / "test_league.db"
    result = subprocess.run(
        [
            sys.executable,
            str(repo_root / "src" / "bootstrap_league_db.py"),
            "--backup", str(backup_db_path),
            "--out", str(out_db),
            "--schema", str(repo_root / "sql" / "schema.sql"),
        ],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
    )
    assert result.returncode == 0, f"bootstrap_league_db.py failed:\n{result.stderr}"

    conn = sqlite3.connect(str(out_db))
    present = {
        row[0]
        for row in conn.execute("SELECT team_name FROM teams WHERE team_name IN ({})".format(
            ",".join("?" for _ in DEAD_TEAMS)
        ), DEAD_TEAMS)
    }
    conn.close()

    assert not present, f"Dead teams resurrected by a fresh bootstrap: {present}"


def test_fresh_bootstrap_includes_real_late_additions(repo_root, backup_db_path, tmp_path):
    """
    Idaho and Massachusetts are genuinely real FBS programs missing from
    the old backup's team list -- the bootstrap script should add them,
    not just remove the dead ones.
    """
    out_db = tmp_path / "test_league2.db"
    subprocess.run(
        [
            sys.executable,
            str(repo_root / "src" / "bootstrap_league_db.py"),
            "--backup", str(backup_db_path),
            "--out", str(out_db),
            "--schema", str(repo_root / "sql" / "schema.sql"),
        ],
        check=True,
        cwd=str(repo_root),
    )
    conn = sqlite3.connect(str(out_db))
    present = {row[0] for row in conn.execute("SELECT team_name FROM teams WHERE team_name IN ('Idaho', 'Massachusetts')")}
    conn.close()
    assert present == {"Idaho", "Massachusetts"}
