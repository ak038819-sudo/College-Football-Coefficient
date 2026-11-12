#!/usr/bin/env python3
"""Historical game archive importer, isolated from production Elo/Merits tables.

Requires CFBD_API_KEY. Defaults to a separate staging database. Does not
infer FBS eligibility, canonicalize historical schools, or build ratings.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
API = "https://api.collegefootballdata.com/games"


def setup(db):
    db.executescript("""
    CREATE TABLE IF NOT EXISTS archive_games (
        source TEXT NOT NULL,
        source_game_id TEXT NOT NULL,
        season INTEGER NOT NULL,
        season_type TEXT,
        start_date TEXT,
        home_name_raw TEXT,
        away_name_raw TEXT,
        home_score INTEGER,
        away_score INTEGER,
        neutral_site INTEGER,
        home_classification TEXT,
        away_classification TEXT,
        home_conference TEXT,
        away_conference TEXT,
        venue TEXT,
        source_payload_json TEXT NOT NULL,
        payload_sha256 TEXT NOT NULL,
        PRIMARY KEY (source, source_game_id)
    );
    CREATE TABLE IF NOT EXISTS archive_fetches (
        season INTEGER NOT NULL,
        season_type TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('complete', 'failed')),
        fetched_at TEXT NOT NULL,
        record_count INTEGER NOT NULL,
        error TEXT,
        PRIMARY KEY (season, season_type)
    );
    """)


def value(row, *keys):
    for key in keys:
        if key in row:
            return row[key]
    return None


def fetch(year, season_type, key, attempts=4):
    params = urllib.parse.urlencode({"year": year, "seasonType": season_type})
    request = urllib.request.Request(
        f"{API}?{params}",
        headers={"Authorization": f"Bearer {key}", "Accept": "application/json",
                 "User-Agent": "Elocast-Historical-Archive/1.0"},
    )
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                result = json.load(response)
            if not isinstance(result, list):
                raise ValueError("CFBD response must be a list of games")
            return result
        except urllib.error.HTTPError as exc:
            if exc.code not in (429, 500, 502, 503, 504) or attempt == attempts - 1:
                raise
            delay = min(30, 2 ** attempt)
            if exc.headers.get("Retry-After", "").isdigit():
                delay = min(60, int(exc.headers["Retry-After"]))
            time.sleep(delay)
        except (urllib.error.URLError, TimeoutError):
            if attempt == attempts - 1:
                raise
            time.sleep(min(30, 2 ** attempt))
    raise RuntimeError("Unreachable retry state")


def normalized(row, year):
    gid = value(row, "id", "game_id")
    if gid is None or str(gid).strip() == "":
        raise ValueError(f"Game in {year} missing source game ID")
    raw = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return (
        "cfbd", str(gid), year, value(row, "seasonType", "season_type"),
        value(row, "startDate", "start_date"), value(row, "homeTeam", "home_team"),
        value(row, "awayTeam", "away_team"), value(row, "homePoints", "home_points"),
        value(row, "awayPoints", "away_points"),
        None if value(row, "neutralSite", "neutral_site") is None
        else int(bool(value(row, "neutralSite", "neutral_site"))),
        value(row, "homeClassification", "home_classification"),
        value(row, "awayClassification", "away_classification"),
        value(row, "homeConference", "home_conference"),
        value(row, "awayConference", "away_conference"),
        value(row, "venue"), raw, hashlib.sha256(raw.encode()).hexdigest(),
    )


def store(db, year, season_type, payload):
    for game in payload:
        source_year = value(game, "season", "year")
        source_kind = value(game, "seasonType", "season_type")
        if source_year is not None and int(source_year) != year:
            raise ValueError(f"Unexpected source season {source_year} for requested {year}")
        if source_kind is not None and source_kind != season_type:
            raise ValueError(f"Unexpected season type {source_kind} for {year} {season_type}")
    rows = [normalized(game, year) for game in payload]
    rows = [(*row[:3], season_type, *row[4:]) for row in rows]
    ids = [r[1] for r in rows]
    if len(ids) != len(set(ids)):
        raise ValueError(f"Duplicate source game IDs in {year} {season_type}")
    # One atomic transaction for games + fetch checkpoint.
    with db:
        db.executemany("""
            INSERT INTO archive_games VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(source, source_game_id) DO UPDATE SET
                season=excluded.season, season_type=excluded.season_type,
                start_date=excluded.start_date, home_name_raw=excluded.home_name_raw,
                away_name_raw=excluded.away_name_raw, home_score=excluded.home_score,
                away_score=excluded.away_score, neutral_site=excluded.neutral_site,
                home_classification=excluded.home_classification,
                away_classification=excluded.away_classification,
                home_conference=excluded.home_conference,
                away_conference=excluded.away_conference, venue=excluded.venue,
                source_payload_json=excluded.source_payload_json,
                payload_sha256=excluded.payload_sha256
        """, rows)
        db.execute("""INSERT INTO archive_fetches VALUES (?,?,'complete',?,?,NULL)
            ON CONFLICT(season, season_type) DO UPDATE SET
            status='complete', fetched_at=excluded.fetched_at,
            record_count=excluded.record_count, error=NULL""",
            (year, season_type, datetime.now(timezone.utc).isoformat(), len(rows)))


def run(db, start, end, key, force=False, pause=0.5, fetcher=fetch):
    if start < 1869 or end < start:
        raise ValueError("Expected 1869 <= start <= end")
    if pause < 0:
        raise ValueError("Pause must be nonnegative")
    setup(db)
    for year in range(start, end + 1):
        for kind in ("regular", "postseason"):
            previous = db.execute(
                "SELECT status FROM archive_fetches WHERE season=? AND season_type=?",
                (year, kind)).fetchone()
            if previous and previous[0] == "complete" and not force:
                print(f"SKIP {year} {kind}: already completed")
                continue
            try:
                payload = fetcher(year, kind, key)
                store(db, year, kind, payload)
                print(f"OK {year} {kind}: {len(payload)} records")
            except Exception as exc:
                with db:
                    db.execute("""INSERT INTO archive_fetches VALUES (?,?,'failed',?,?,?)
                        ON CONFLICT(season, season_type) DO UPDATE SET
                        status='failed', fetched_at=excluded.fetched_at,
                        record_count=0, error=excluded.error""",
                        (year, kind, datetime.now(timezone.utc).isoformat(), 0,
                         f"{type(exc).__name__}: {exc}"[:500]))
                print(f"FAILED {year} {kind}: {exc}")
                raise
            if pause:
                time.sleep(pause)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", type=Path, default=ROOT / "db/historical_staging.db")
    p.add_argument("--start", type=int, required=True)
    p.add_argument("--end", type=int, required=True)
    p.add_argument("--force", action="store_true", help="Re-fetch completed years")
    p.add_argument("--pause", type=float, default=0.5)
    args = p.parse_args()
    key = os.environ.get("CFBD_API_KEY")
    if not key:
        p.error("CFBD_API_KEY environment variable is required")
    if args.db.resolve() == (ROOT / "db/league.db").resolve():
        p.error("Refusing to use the production league.db")
    args.db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(args.db) as db:
        run(db, args.start, args.end, key, force=args.force, pause=args.pause)


if __name__ == "__main__":
    main()
