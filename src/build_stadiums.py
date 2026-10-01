#!/usr/bin/env python3
"""Seed physical venues and resolve game stadiums conservatively (display only).

Only the supplied 2026 primary stadium relationships are seeded. Earlier
seasons are never inferred from a current venue; explicit source venues and
curated game overrides may still resolve historical games. No rating or
neutral-site flag is changed.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import sys
import unicodedata
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from load_games import resolve_team_name, team_id  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
SCHEMA = ROOT / "sql/stadium_tables.sql"
SEED = ROOT / "data/stadiums_2026.tsv"
CATALOG = ROOT / "data/raw/venues.json"


def key(value: str) -> str:
    plain = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "", plain.casefold())


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA.read_text(encoding="utf-8"))
    for table, fields in (("games", {"stadium_id": "INTEGER REFERENCES stadiums(stadium_id)",
                                    "venue_text": "TEXT", "source_venue_id": "INTEGER"}),
                          ("scheduled_games", {"stadium_id": "INTEGER REFERENCES stadiums(stadium_id)",
                                               "source_venue_id": "INTEGER"})):
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
            continue
        have = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
        for col, decl in fields.items():
            if col not in have:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
    if "cfbd_venue_id" not in {r[1] for r in conn.execute("PRAGMA table_info(stadiums)")}:
        # SQLite cannot add a UNIQUE column to an existing table. Older local
        # databases already have the stadiums table without this column.
        conn.execute("ALTER TABLE stadiums ADD COLUMN cfbd_venue_id INTEGER")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_stadiums_cfbd_venue_id ON stadiums(cfbd_venue_id)")
    conn.commit()


def seed_stadiums(conn: sqlite3.Connection, source: Path = SEED) -> dict:
    """Upsert by stable physical-venue key, resolving teams through canonical names."""
    seen = set()
    created = 0
    with source.open(newline="", encoding="utf-8") as f:
        records = list(csv.DictReader(f, delimiter="\t"))
    cur = conn.cursor()
    for rec in records:
        canonical = resolve_team_name(cur, rec["team_name"])
        tid = team_id(cur, canonical)
        if tid in seen:
            raise ValueError(f"Duplicate stadium seed for {canonical}")
        seen.add(tid)
        stadium_key = "home-" + key(canonical)
        name = rec["stadium_name"].strip()
        if not name:
            raise ValueError(f"Missing stadium for {canonical}")
        old = cur.execute("SELECT stadium_id, stadium_name FROM stadiums WHERE stadium_key=?",
                          (stadium_key,)).fetchone()
        if old is None:
            created += 1
        if old and old[1] != name:
            cur.execute("INSERT OR IGNORE INTO stadium_aliases VALUES (?,?)", (key(old[1]), old[0]))
        cur.execute("INSERT INTO stadiums (stadium_key, stadium_name) VALUES (?,?) "
                    "ON CONFLICT(stadium_key) DO UPDATE SET stadium_name=excluded.stadium_name",
                    (stadium_key, name))
        sid = cur.execute("SELECT stadium_id FROM stadiums WHERE stadium_key=?", (stadium_key,)).fetchone()[0]
        cur.execute("INSERT OR IGNORE INTO team_stadiums "
                    "(team_id, stadium_id, start_season, end_season, is_primary) VALUES (?,?,2026,NULL,1)",
                    (tid, sid))
    conn.commit()
    return {"teams_seeded": len(seen), "stadiums_created": created,
            "stadiums_total": conn.execute("SELECT COUNT(*) FROM stadiums").fetchone()[0]}


def stadium_candidates(conn: sqlite3.Connection) -> dict[str, set[int]]:
    by_name: dict[str, set[int]] = {}
    for sid, name in conn.execute("SELECT stadium_id, stadium_name FROM stadiums"):
        by_name.setdefault(key(name), set()).add(sid)
    for alias, sid in conn.execute("SELECT alias_key, stadium_id FROM stadium_aliases"):
        by_name.setdefault(alias, set()).add(sid)
    return by_name


def import_venue_catalog(conn: sqlite3.Connection, source: Path = CATALOG) -> dict:
    """Enrich seeded stadiums and game venues by CFBD ID; never merge ambiguous names."""
    if not source.exists():
        return {"catalog_linked": 0, "catalog_created": 0, "catalog_ambiguous": 0}
    venues = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(venues, list):
        raise ValueError("CFBD venue snapshot must be an array")
    used = set()
    for table in ("games", "scheduled_games"):
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
            used.update(r[0] for r in conn.execute(
                f"SELECT DISTINCT source_venue_id FROM {table} WHERE source_venue_id IS NOT NULL"))
    candidates = stadium_candidates(conn)
    counts = {"catalog_linked": 0, "catalog_created": 0, "catalog_ambiguous": 0}
    for v in venues:
        if not isinstance(v, dict) or not isinstance(v.get("id"), int) or not v.get("name"):
            continue
        cfbd_id, name = v["id"], str(v["name"]).strip()
        existing = conn.execute("SELECT stadium_id FROM stadiums WHERE cfbd_venue_id=?", (cfbd_id,)).fetchone()
        matches = set()
        for sid in candidates.get(key(name), set()):
            linked_id, city, state = conn.execute(
                "SELECT cfbd_venue_id,city,state FROM stadiums WHERE stadium_id=?", (sid,)).fetchone()
            if linked_id not in (None, cfbd_id):
                continue
            # A common name alone cannot establish that two physical venues
            # are the same when both records have conflicting locations.
            if city and v.get("city") and key(city) != key(str(v["city"])):
                continue
            if state and v.get("state") and key(state) != key(str(v["state"])):
                continue
            matches.add(sid)
        if existing:
            sid = existing[0]
        elif len(matches) == 1:
            sid = next(iter(matches))
            counts["catalog_linked"] += 1
        elif cfbd_id in used:
            if len(matches) > 1:
                counts["catalog_ambiguous"] += 1
            conn.execute("INSERT OR IGNORE INTO stadiums (stadium_key, stadium_name) VALUES (?,?)",
                         (f"cfbd-{cfbd_id}", name))
            sid = conn.execute("SELECT stadium_id FROM stadiums WHERE stadium_key=?", (f"cfbd-{cfbd_id}",)).fetchone()[0]
            counts["catalog_created"] += 1
        else:
            continue
        conn.execute("""UPDATE stadiums SET cfbd_venue_id=?, city=?, state=?, latitude=?, longitude=?,
            capacity=?, indoor=?, opened_year=? WHERE stadium_id=?""",
            (cfbd_id, v.get("city"), v.get("state"), v.get("latitude"), v.get("longitude"),
             v.get("capacity"), int(v["dome"]) if v.get("dome") is not None else None,
             v.get("construction_year", v.get("constructionYear")), sid))
    conn.commit()
    return counts


def primary_for(conn: sqlite3.Connection, team: int, season: int) -> int | None:
    rows = conn.execute("""SELECT stadium_id FROM team_stadiums
        WHERE team_id=? AND is_primary=1 AND start_season<=?
          AND (end_season IS NULL OR end_season>=?)""", (team, season, season)).fetchall()
    return rows[0][0] if len(rows) == 1 else None


def resolve_games(conn: sqlite3.Connection) -> dict:
    candidates = stadium_candidates(conn)
    by_source_id = dict(conn.execute("SELECT cfbd_venue_id, stadium_id FROM stadiums WHERE cfbd_venue_id IS NOT NULL"))
    overrides = dict(conn.execute("SELECT game_id, stadium_id FROM game_venue_overrides"))
    report = {"games_backfilled": 0, "scheduled_backfilled": 0, "games_unresolved": 0,
              "scheduled_unresolved": 0, "explicitly_neutral": 0, "ambiguous": [],
              "unresolved_examples": []}
    for table in ("games", "scheduled_games"):
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
            continue
        col = "venue_text" if table == "games" else "venue"
        rows = conn.execute(f"SELECT game_id, season_year, home_team_id, neutral_site, game_phase, "
                            f"{('game_phase_check' if table == 'games' else 'notes')}, {col}, source_venue_id, stadium_id "
                            f"FROM {table}").fetchall()
        for gid, season, home, neutral, phase, notes, source, source_id, before in rows:
            if neutral:
                report["explicitly_neutral"] += 1
            sid = by_source_id.get(source_id)
            if sid is None and source and source.strip():
                matches = candidates.get(key(source), set())
                if len(matches) == 1:
                    sid = next(iter(matches))
                elif len(matches) > 1:
                    report["ambiguous"].append({"table": table, "game_id": gid, "venue": source,
                                                 "candidate_ids": sorted(matches)})
            if sid is None:
                sid = overrides.get(gid)
            # Only the 2026+ relationship is known; never project it into history.
            if sid is None and not source and source_id is None and not neutral and phase == "regular" and not re.search(
                    r"bowl|championship|neutral|classic|kickoff", notes or "", re.I):
                sid = primary_for(conn, home, season)
            if before != sid:
                conn.execute(f"UPDATE {table} SET stadium_id=? WHERE game_id=?", (sid, gid))
            if sid is None:
                report["games_unresolved" if table == "games" else "scheduled_unresolved"] += 1
                if len(report["unresolved_examples"]) < 30:
                    report["unresolved_examples"].append({"table": table, "game_id": gid,
                                                            "source_venue": source, "source_venue_id": source_id})
            elif before is None:
                report["games_backfilled" if table == "games" else "scheduled_backfilled"] += 1
    conn.commit()
    return report


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default=str(ROOT / "db/league.db"))
    p.add_argument("--seed", type=Path, default=SEED)
    p.add_argument("--catalog", type=Path, default=CATALOG)
    p.add_argument("--report", type=Path, default=ROOT / "data/processed/stadium_resolution_report.json")
    args = p.parse_args()
    conn = sqlite3.connect(args.db)
    try:
        ensure_schema(conn)
        seeded = seed_stadiums(conn, args.seed)
        result = {**seeded, **import_venue_catalog(conn, args.catalog), **resolve_games(conn)}
    finally:
        conn.close()
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k not in ("ambiguous", "unresolved_examples")}, indent=2))
    print(f"Manual review: {len(result['ambiguous'])} ambiguous matches, report at {args.report}")


if __name__ == "__main__":
    main()
