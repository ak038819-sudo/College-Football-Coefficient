#!/usr/bin/env python3
"""Phase 1: read-only raw-vs-loaded audit and frozen model-output fingerprints.

Never changes the SQLite source. CSVs are read as source evidence, not imported.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
YEARS = range(1980, 2027)
MODELS = ("elo_game_history", "hybrid_game_ratings", "team_coe2_by_season",
          "team_coe_5yr_by_season", "team_coefficient_by_year")
SOURCE_FIELDS = ("season", "raw_file", "raw_rows", "missing_game_ids",
                 "unresolved_team_games", "source_ids_not_loaded")
ALIAS_FIELDS = ("season", "raw_name", "games")


def exists(db, table):
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def fingerprint(db, table):
    """Stable hash of table contents, including schema and all values, regardless of row order."""
    schema = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    if schema is None:
        return None
    cols = [row[1] for row in db.execute(f'PRAGMA table_info("{table}")')]
    h = hashlib.sha256()
    h.update((schema[0] or "").encode())
    h.update(json.dumps(cols).encode())
    # Hash each row independently and sort row hashes; avoids non-deterministic query order.
    hashes = []
    for row in db.execute(f'SELECT * FROM "{table}"'):
        encoded = json.dumps(list(row), separators=(",", ":"), ensure_ascii=False, default=str).encode()
        hashes.append(hashlib.sha256(encoded).digest())
    for item in sorted(hashes):
        h.update(item)
    return {"rows": len(hashes), "sha256": h.hexdigest()}


def audit_sources(db, raw_dir):
    # Mirrors the existing loader's identity resolution without importing the loader
    # (which can have side effects); ambiguous matches remain unresolved.
    from sys import path as sys_path
    sys_path.insert(0, str(ROOT / "src"))
    from team_identity import resolve_database_name

    loaded = defaultdict(set)
    for season, gid in db.execute("SELECT season_year, game_id FROM games"):
        if gid is not None:
            loaded[int(season)].add(str(gid))
    results = []
    unresolved = Counter()
    for year in YEARS:
        path = raw_dir / f"games_{year}.csv"
        if not path.exists():
            results.append({"season": year, "raw_file": False, "raw_rows": None,
                            "missing_game_ids": None, "unresolved_team_games": None,
                            "source_ids_not_loaded": None})
            continue
        total = missing_ids = unresolved_games = 0
        not_loaded = []
        with path.open(newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                total += 1
                gid = str(row.get("game_id") or "").strip()
                if not gid:
                    missing_ids += 1
                if gid and gid not in loaded[year]:
                    not_loaded.append(gid)
                bad = []
                for side in ("home_team", "away_team"):
                    raw = (row.get(side) or "").strip()
                    try:
                        resolved = resolve_database_name(db.cursor(), raw) if raw else None
                    except ValueError:
                        resolved = None
                    if not resolved:
                        bad.append(raw or "<blank>")
                if bad:
                    unresolved_games += 1
                    for name in bad:
                        unresolved[(year, name)] += 1
        results.append({"season": year, "raw_file": True, "raw_rows": total,
                        "missing_game_ids": missing_ids,
                        "unresolved_team_games": unresolved_games,
                        "source_ids_not_loaded": len(not_loaded)})
    aliases = [{"season": year, "raw_name": name, "games": count}
               for (year, name), count in sorted(unresolved.items())]
    return results, aliases


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", type=Path, default=ROOT / "db/league.db")
    p.add_argument("--raw-dir", type=Path, default=ROOT / "data/raw")
    p.add_argument("--out", type=Path, default=ROOT / "data/processed/historical_audit")
    a = p.parse_args()
    if not a.db.is_file():
        p.error(f"Missing database: {a.db}")
    db = sqlite3.connect(a.db.resolve().as_uri() + "?mode=ro", uri=True)
    db.execute("PRAGMA query_only=ON")
    try:
        if not exists(db, "games"):
            raise RuntimeError("Missing games table")
        source_rows, aliases = audit_sources(db, a.raw_dir)
        tables = {table: fingerprint(db, table) for table in ("games", *MODELS)}
        snapshot = {"database_sha256": hashlib.sha256(a.db.read_bytes()).hexdigest(),
                    "model_tables": tables,
                    "config_sha256": hashlib.sha256((ROOT / "config/model_config.json").read_bytes()).hexdigest(),
                    "source_comparison": source_rows,
                    "unresolved_aliases": aliases,
                    "warnings": [
                        "Missing raw CSV does not mean missing historical games.",
                        "Unresolved names are observations, not necessarily FBS programs.",
                        "Table fingerprints are baselines, not proof of model correctness.",
                        "Source IDs absent from games can reflect deliberate non-FBS filtering.",
                        "2020 is audited as an ordinary season; COVID-related missingness needs interpretation."
                    ]}
    finally:
        db.close()
    a.out.mkdir(parents=True, exist_ok=True)
    (a.out / "baseline_fingerprints.json").write_text(json.dumps(snapshot, indent=2) + "\n")
    csv_files = (("raw_vs_loaded.csv", source_rows, SOURCE_FIELDS),
                 ("unresolved_aliases.csv", aliases, ALIAS_FIELDS))
    # Always rewrite, header included, even with no rows: a stale CSV left beside a
    # fresh JSON reads as unresolved identities that the audit no longer found.
    for filename, rows, fields in csv_files:
        with (a.out / filename).open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
    print(f"Audit complete: {a.out} (no source DB writes)")


if __name__ == "__main__":
    main()
