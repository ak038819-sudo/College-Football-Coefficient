#!/usr/bin/env python3
"""Reconcile the staged 1946-1979 archive against the provider's own records.

The games feed is a list of fixtures; the records feed is a list of season
won-lost-tied lines, derived by the provider separately. Comparing them
catches the failure a count of rows cannot: a season that LOOKS complete
because every partition answered, but is missing games that the provider
itself believes were played.

This is a CROSS-FEED check, not an independent one. Both feeds come from
collegefootballdata.com, so agreement is evidence that the import is
faithful, NOT evidence that the history is right. The independent
references the promotion gate asks for -- record books, conference
archives -- are a separate step and are not in here.

Nothing is promoted, nothing is inferred and nothing is discarded. The
staging database is opened READ-ONLY; the records live in a database of
their own, so the staging artifact chain keeps a single writer.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
API = "https://api.collegefootballdata.com/records"
FIRST, LAST = 1946, 1979
SEASON_TYPES = ("regular", "postseason")
# Columns of archive_team_records, which normalized() fills positionally.
COLUMNS = 21
# The nested won-lost-tied blocks of a records row, by the season_type they
# correspond to in the games feed.
REFERENCE_SPLITS = {"regular": ("regularSeason", "regular_season"),
                    "postseason": ("postseason",)}


def setup(db):
    db.executescript("""
    CREATE TABLE IF NOT EXISTS archive_team_records (
        source TEXT NOT NULL,
        season INTEGER NOT NULL,
        team_raw TEXT NOT NULL,
        team_source_id TEXT,
        classification TEXT,
        conference TEXT,
        division TEXT,
        regular_games INTEGER, regular_wins INTEGER,
        regular_losses INTEGER, regular_ties INTEGER,
        postseason_games INTEGER, postseason_wins INTEGER,
        postseason_losses INTEGER, postseason_ties INTEGER,
        total_games INTEGER, total_wins INTEGER,
        total_losses INTEGER, total_ties INTEGER,
        source_payload_json TEXT NOT NULL,
        payload_sha256 TEXT NOT NULL,
        PRIMARY KEY (source, season, team_raw)
    );
    CREATE TABLE IF NOT EXISTS archive_record_fetches (
        season INTEGER NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('complete', 'failed')),
        fetched_at TEXT NOT NULL,
        record_count INTEGER NOT NULL,
        error TEXT,
        PRIMARY KEY (season)
    );
    """)


def value(row, *keys):
    for key in keys:
        if isinstance(row, dict) and key in row:
            return row[key]
    return None


def split(row, *names):
    """One won-lost-tied block of a records row, by any of its spellings."""
    block = value(row, *names)
    if not isinstance(block, dict):
        return (None, None, None, None)
    return tuple(value(block, field) for field in
                 ("games", "wins", "losses", "ties"))


def fetch(year, key, attempts=4):
    params = urllib.parse.urlencode({"year": year})
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
                raise ValueError("CFBD records response must be a list")
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
    team = value(row, "team")
    if team is None or str(team).strip() == "":
        raise ValueError(f"Records row in {year} has no team name")
    source_year = value(row, "year", "season")
    if source_year is not None and int(source_year) != year:
        raise ValueError(f"Unexpected records season {source_year} for {year}")
    raw = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    team_id = value(row, "teamId", "team_id")
    return (
        "cfbd", year, str(team),
        None if team_id is None else str(team_id),
        value(row, "classification"), value(row, "conference"),
        value(row, "division"),
        *split(row, *REFERENCE_SPLITS["regular"]),
        *split(row, *REFERENCE_SPLITS["postseason"]),
        *split(row, "total"),
        raw, hashlib.sha256(raw.encode()).hexdigest(),
    )


def store(db, year, payload):
    rows = [normalized(row, year) for row in payload]
    names = [row[2] for row in rows]
    if len(names) != len(set(names)):
        raise ValueError(f"Duplicate team names in the {year} records feed")
    with db:
        db.executemany(
            "INSERT OR REPLACE INTO archive_team_records VALUES "
            "(" + ",".join("?" * COLUMNS) + ")", rows)
        db.execute("""INSERT INTO archive_record_fetches VALUES (?,'complete',?,?,NULL)
            ON CONFLICT(season) DO UPDATE SET status='complete',
            fetched_at=excluded.fetched_at,
            record_count=excluded.record_count, error=NULL""",
            (year, datetime.now(timezone.utc).isoformat(), len(rows)))


def collect(db, start, end, key, force=False, pause=0.5, fetcher=fetch):
    """Fetch and checkpoint the records feed, one season a request."""
    setup(db)
    for year in range(start, end + 1):
        previous = db.execute(
            "SELECT status FROM archive_record_fetches WHERE season=?",
            (year,)).fetchone()
        if previous and previous[0] == "complete" and not force:
            print(f"SKIP {year}: records already collected")
            continue
        try:
            payload = fetcher(year, key)
            store(db, year, payload)
            print(f"OK {year}: {len(payload)} record lines")
        except Exception as exc:
            with db:
                db.execute("""INSERT INTO archive_record_fetches VALUES (?,'failed',?,0,?)
                    ON CONFLICT(season) DO UPDATE SET status='failed',
                    fetched_at=excluded.fetched_at, record_count=0,
                    error=excluded.error""",
                    (year, datetime.now(timezone.utc).isoformat(),
                     f"{type(exc).__name__}: {exc}"[:500]))
            print(f"FAILED {year}: {exc}")
            raise
        if pause:
            time.sleep(pause)


def staged(archive, year):
    """Won-lost-tied per team per season type, from the staged fixtures.

    A game with either score missing is counted as played and undecided --
    never as a tie, which is a result the archive does not claim.
    """
    tally = defaultdict(lambda: defaultdict(int))
    rows = archive.execute(
        """SELECT season_type, home_name_raw, away_name_raw,
                  home_score, away_score
           FROM archive_games WHERE season=?""", (year,)).fetchall()
    for kind, home, away, home_score, away_score in rows:
        if kind not in SEASON_TYPES:
            kind = "other"
        for team, own, other in ((home, home_score, away_score),
                                 (away, away_score, home_score)):
            if not team:
                continue
            counts = tally[str(team)]
            counts[f"{kind}_games"] += 1
            if own is None or other is None:
                counts[f"{kind}_undecided"] += 1
            elif own > other:
                counts[f"{kind}_wins"] += 1
            elif own < other:
                counts[f"{kind}_losses"] += 1
            else:
                counts[f"{kind}_ties"] += 1
    return tally


def reference(db, year):
    rows = db.execute(
        """SELECT team_raw, classification, conference,
                  regular_games, regular_wins, regular_losses, regular_ties,
                  postseason_games, postseason_wins, postseason_losses,
                  postseason_ties
           FROM archive_team_records WHERE season=?""", (year,)).fetchall()
    fields = ('classification', 'conference',
              'regular_games', 'regular_wins', 'regular_losses',
              'regular_ties', 'postseason_games', 'postseason_wins',
              'postseason_losses', 'postseason_ties')
    return {row[0]: dict(zip(fields, row[1:])) for row in rows}


def difference(staged_count, reference_count):
    """None where the reference is silent -- an absent line is not a zero."""
    if reference_count is None:
        return None
    return staged_count - int(reference_count)


def reconcile(archive, records, first=FIRST, last=LAST):
    if first < FIRST or last > LAST or last < first:
        raise ValueError("Phase 3 range must be within 1946-1979")
    results = []
    for year in range(first, last + 1):
        checkpoint = records.execute(
            "SELECT status, record_count FROM archive_record_fetches WHERE season=?",
            (year,)).fetchone()
        status = checkpoint[0] if checkpoint else "missing"
        tally = staged(archive, year)
        lines = reference(records, year)
        for team in sorted(set(tally) | set(lines)):
            counts = tally.get(team, {})
            line = lines.get(team)
            row = dict(season=year, team_raw=team,
                       records_checkpoint=status,
                       classification=(line or {}).get('classification'),
                       conference=(line or {}).get('conference'))
            findings = []
            for kind in SEASON_TYPES:
                for field in ('games', 'wins', 'losses', 'ties'):
                    name = f"{kind}_{field}"
                    mine = counts.get(name, 0)
                    theirs = None if line is None else line[name]
                    row[f"staged_{name}"] = mine
                    row[f"reference_{name}"] = theirs
                    row[f"delta_{name}"] = difference(mine, theirs)
                row[f"staged_{kind}_undecided"] = counts.get(
                    f"{kind}_undecided", 0)
            row["empty_reference_line"] = False
            if line is None:
                findings.append("no_reference_line")
            elif not counts:
                if any(line[f"{kind}_games"] or 0 for kind in SEASON_TYPES):
                    findings.append("reference_has_games_none_staged")
                else:
                    # A record line of all zeroes beside nothing staged is
                    # the two feeds AGREEING that the team did not play --
                    # the provider carries such lines for programs it knows
                    # of but has no fixtures for. Counting it as a finding
                    # cried wolf 2,111 times on the first real run and
                    # buried the 754 that matter.
                    row["empty_reference_line"] = True
            else:
                for kind in SEASON_TYPES:
                    if row[f"delta_{kind}_games"]:
                        findings.append(f"{kind}_game_count_mismatch")
                    elif any(row[f"delta_{kind}_{field}"]
                             for field in ('wins', 'losses', 'ties')):
                        findings.append(f"{kind}_result_mismatch")
            if status != "complete":
                findings.append("records_not_collected")
            row["findings"] = ";".join(findings)
            row["reconciled"] = not findings
            results.append(row)
    return results


UNMATCHED_SHOWN = 40


def summarize(rows, first, last):
    findings = defaultdict(int)
    seasons = defaultdict(int)
    unmatched = defaultdict(int)
    for row in rows:
        if "no_reference_line" in (row["findings"] or ""):
            unmatched[row["team_raw"]] += 1
        if not row["findings"]:
            continue
        seasons[row["season"]] += 1
        for finding in row["findings"].split(";"):
            findings[finding] += 1
    # Most seasons first: a name the records feed never carries in thirty
    # years is a different problem from one missing in a single season.
    ranked = sorted(unmatched.items(), key=lambda pair: (-pair[1], pair[0]))
    return {
        "years": [first, last],
        "team_seasons": len(rows),
        "reconciled": sum(1 for row in rows if row["reconciled"]),
        "with_findings": sum(1 for row in rows if row["findings"]),
        "findings": dict(sorted(findings.items())),
        "seasons_with_findings": {str(year): count
                                  for year, count in sorted(seasons.items())},
        "empty_reference_lines": sum(1 for row in rows
                                     if row.get("empty_reference_line")),
        "unmatched_team_names": len(unmatched),
        "unmatched_team_seasons_by_name": dict(ranked[:UNMATCHED_SHOWN]),
        "production_promotion_allowed": False,
        "note": "Both feeds are collegefootballdata.com, so agreement means "
                "the import is faithful to the provider, not that the "
                "history is complete or correct.",
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--staging", type=Path, required=True,
                   help="The staged archive. Opened read-only.")
    p.add_argument("--db", type=Path, required=True,
                   help="Where the records feed is kept. Its own database.")
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--start", type=int, default=FIRST)
    p.add_argument("--end", type=int, default=LAST)
    p.add_argument("--force", action="store_true",
                   help="Re-fetch seasons whose records are already collected")
    p.add_argument("--pause", type=float, default=0.5)
    p.add_argument("--offline", action="store_true",
                   help="Reconcile against the records already stored, "
                        "asking the provider for nothing")
    args = p.parse_args()
    for path in (args.staging, args.db):
        if path.resolve().name == "league.db":
            p.error("Refusing production database")
    if args.db.resolve() == args.staging.resolve():
        p.error("The records belong in their own database, not in the staging "
                "archive, which has one writer")
    if not args.staging.exists():
        p.error(f"No staged archive at {args.staging}")

    args.db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(args.db) as records:
        if args.offline:
            setup(records)
            print("Offline: reconciling against the records already stored.")
        else:
            key = os.environ.get("CFBD_API_KEY")
            if not key:
                p.error("CFBD_API_KEY environment variable is required")
            collect(records, args.start, args.end, key, force=args.force,
                    pause=args.pause)
        with sqlite3.connect(
                f"file:{args.staging.resolve()}?mode=ro", uri=True) as archive:
            rows = reconcile(archive, records, args.start, args.end)

    if not rows:
        raise SystemExit("Nothing to reconcile: no staged games and no "
                         f"records for {args.start}-{args.end}")
    args.out.mkdir(parents=True, exist_ok=True)
    with (args.out / "source_reconciliation.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summary = summarize(rows, args.start, args.end)
    (args.out / "source_reconciliation_summary.json").write_text(
        json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
