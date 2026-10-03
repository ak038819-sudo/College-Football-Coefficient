#!/usr/bin/env python3
"""Load the committed coaching snapshot into coaches / coach_external_ids /
coach_tenures.

CFBD's /coaches feed carries no coach identifier, and its hire date belongs to
the JOB rather than the person -- one coach has a different one per school. The
NAME is therefore the only person-level signal the feed offers, so the key is:

    cfbd:<normalized name>

That is weak evidence by this project's own standard, so it is not dressed up as
anything stronger: confidence is recorded as 'name only', and every career the
feed cannot vouch for is written to person_unresolved for review --

  - a name whose seasons carry more than one hire date (a coach who changed
    jobs, or two people sharing a name),
  - a name with a gap in its seasons (a coach who returned years later, or two
    people sharing a name), and
  - two coaches whose names differ only by a generational suffix (a father and
    son, or one person a source spelled both ways) -- the inverse risk of
    keeping the suffix in the key.

Both readings are genuinely possible from this feed, and neither is asserted.
Two coaches who truly share a name cannot be separated by it at all; saying so
is better than inventing a distinction. A source with real coach ids can be
added beside this key later without a migration.

Head coaches only. Nothing here describes coordinators or assistants.

Idempotent: the key is deterministic, and coach_tenures is keyed on
(coach_id, team_id, season_year), so re-running the same snapshot updates the
same rows.

Usage:
    python src/load_coaches.py data/raw/coaches/coaches.json --db db/league.db
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
from pathlib import Path

import person_identity as identity

SOURCE = "cfbd"
ROLE = "head coach"


def coach_key(record: dict) -> str:
    """The name, with its generational suffix KEPT -- see identity_name.

    Folding the suffix merged Mike Sanford Sr. (UNLV, 2005-2009) with his son
    Mike Sanford Jr. (Western Kentucky and Colorado, 2017-2022) into one coach.
    """
    return identity.identity_name(record.get("name", ""))


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _upsert_coach(conn: sqlite3.Connection, record: dict, external_id: str) -> int:
    row = conn.execute("SELECT coach_id FROM coach_external_ids WHERE source = ? AND external_id = ?",
                       (SOURCE, external_id)).fetchone()
    now = _now()
    first, last = identity.split_name(record.get("first_name"), record.get("last_name"),
                                      record.get("name", ""))
    if row:
        coach_id = int(row[0])
        conn.execute("UPDATE coaches SET display_name = ?, first_name = ?, last_name = ?, "
                     "updated_at = ? WHERE coach_id = ?",
                     (record["name"], first, last, now, coach_id))
        return coach_id
    cur = conn.execute("INSERT INTO coaches (display_name, first_name, last_name, "
                       "created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                       (record["name"], first, last, now, now))
    coach_id = int(cur.lastrowid)
    conn.execute("INSERT INTO coach_external_ids (coach_id, source, external_id, confidence, is_primary) "
                 "VALUES (?, ?, ?, ?, 1)", (coach_id, SOURCE, external_id, "name only"))
    return coach_id


def _migrate_legacy_keys(conn: sqlite3.Connection) -> int:
    """Point coaches stored under an older key form at the current one.

    The first version of this loader keyed on `cfbd:<name>|<hire date>`, and a
    database loaded by it would otherwise gain a SECOND coach for every name on
    the next run: the lookup misses the legacy id, inserts a fresh coach, moves
    the tenures to it and leaves the old row orphaned with a different -- and
    supposedly immutable -- coach_id.

    So the legacy ids are rewritten to the current key, keeping the lowest
    coach_id for each so an id survives wherever one can. Where several legacy
    rows collapse onto one key (a coach with two hire dates had one row per
    job), the extras are removed along with the coach rows they held, because
    those rows were the split this release exists to undo.

    Rebuilding a coach_id is acceptable only because no page has published one
    yet. Once coach URLs are live this must migrate ids, never reissue them.
    """
    rows = conn.execute("SELECT coach_id, external_id FROM coach_external_ids "
                        "WHERE source = ?", (SOURCE,)).fetchall()
    legacy: dict[str, list[int]] = {}
    for coach_id, external_id in rows:
        name = conn.execute("SELECT display_name FROM coaches WHERE coach_id = ?",
                            (coach_id,)).fetchone()
        if not name:
            continue
        current = f"{SOURCE}:{identity.identity_name(name[0])}"
        if external_id != current:
            legacy.setdefault(current, []).append(int(coach_id))
    if not legacy:
        return 0

    migrated = 0
    for current, coach_ids in legacy.items():
        coach_ids.sort()
        keep, superseded = coach_ids[0], coach_ids[1:]
        taken = conn.execute("SELECT coach_id FROM coach_external_ids WHERE source = ? "
                             "AND external_id = ?", (SOURCE, current)).fetchone()
        if taken:
            keep, superseded = int(taken[0]), coach_ids
        else:
            conn.execute("DELETE FROM coach_external_ids WHERE coach_id = ? AND source = ?",
                         (keep, SOURCE))
            conn.execute("INSERT INTO coach_external_ids (coach_id, source, external_id, "
                         "confidence, is_primary) VALUES (?, ?, ?, 'name only', 1)",
                         (keep, SOURCE, current))
        for coach_id in superseded:
            conn.execute("DELETE FROM coach_tenures WHERE coach_id = ?", (coach_id,))
            conn.execute("DELETE FROM coach_external_ids WHERE coach_id = ?", (coach_id,))
            conn.execute("DELETE FROM coaches WHERE coach_id = ?", (coach_id,))
        migrated += 1
    conn.commit()
    return migrated


def load_coaches(conn: sqlite3.Connection, path: str | Path) -> dict:
    identity.apply_schema(conn)
    # Upgrade a coach_tenures created before hire_date moved here from coaches
    # (CREATE TABLE IF NOT EXISTS will not add a column to an existing table).
    if "hire_date" not in {r[1] for r in conn.execute("PRAGMA table_info(coach_tenures)")}:
        conn.execute("ALTER TABLE coach_tenures ADD COLUMN hire_date TEXT")
    _migrate_legacy_keys(conn)
    records = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(records, list):
        raise ValueError(f"{path}: coaching snapshot must be a list")

    # The snapshot is the whole coaching history, so its own rows are replaced
    # wholesale: a season CFBD has corrected must change, not accumulate.
    conn.execute("DELETE FROM coach_tenures WHERE source = ?", (SOURCE,))
    conn.execute("DELETE FROM person_unresolved WHERE entity = 'coach' AND context = 'coaches'")

    stats = {"coaches": 0, "tenures": 0, "unknown_team": 0, "ambiguous": 0, "skipped": 0}
    # Counted as a set, not incremented per record: the feed sends one record per
    # SEASON, so several records are routinely the same person. Incrementing would
    # have reported 5,714 coaches for the 826 the table actually holds.
    coaches_seen: set[int] = set()
    flagged: set[int] = set()
    for record in records:
        name = (record.get("name") or "").strip()
        seasons = [s for s in (record.get("seasons") or [])
                   if s.get("year") is not None and s.get("school")]
        # A snapshot written before the hire date moved onto the season carries it
        # at record level. Reading both keeps those snapshots loadable.
        record_hire = record.get("hire_date")
        seasons = [{**s, "hire_date": identity.date_only(s.get("hire_date") or record_hire)}
                   for s in seasons]
        key = coach_key(record)
        if not name or not seasons or not key:
            stats["skipped"] += 1
            continue
        coach_id = _upsert_coach(conn, {**record, "name": name}, f"{SOURCE}:{key}")
        coaches_seen.add(coach_id)

        for season in seasons:
            year, school = int(season["year"]), season["school"]
            team_id = identity.resolve_team_id(conn, school)
            if team_id is None:
                stats["unknown_team"] += 1
                identity.record_unresolved(conn, "coach", SOURCE, "coaches", name,
                                           "unknown team", year, school)
                continue
            conn.execute(
                "INSERT OR REPLACE INTO coach_tenures (coach_id, team_id, season_year, role, "
                "hire_date, games, wins, losses, ties, preseason_rank, postseason_rank, source, "
                "source_team) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (coach_id, team_id, year, ROLE, season.get("hire_date"), season.get("games"),
                 season.get("wins"), season.get("losses"), season.get("ties"),
                 season.get("preseason_rank"), season.get("postseason_rank"), SOURCE, school))
            stats["tenures"] += 1

    # The ambiguity check runs here, not per record: with one record per season a
    # career is only whole once every record naming that coach has been read, so
    # checking earlier would flag a coach's second job as a gap before the
    # seasons between them had arrived.
    stats["coaches"] = len(coaches_seen)
    for coach_id in sorted(coaches_seen):
        rows = conn.execute("SELECT season_year, hire_date FROM coach_tenures "
                            "WHERE coach_id = ? AND source = ? ORDER BY season_year",
                            (coach_id, SOURCE)).fetchall()
        if not rows:
            continue
        years = sorted({int(r[0]) for r in rows})
        hire_dates = {r[1] for r in rows if r[1]}
        gaps = [(a, b) for a, b in zip(years, years[1:]) if b - a > 1]
        if len(hire_dates) <= 1 and not gaps:
            continue
        # EITHER one coach with two jobs or a break, OR two people sharing a name.
        # Nothing in this feed says which, so it is recorded, never resolved.
        name = conn.execute("SELECT display_name FROM coaches WHERE coach_id = ?",
                            (coach_id,)).fetchone()[0]
        reason = ("name-only identity spanning %d hire dates" % len(hire_dates)
                  if len(hire_dates) > 1 else
                  "name-only identity with a gap after %d" % gaps[0][0])
        identity.record_unresolved(conn, "coach", SOURCE, "coaches", name, reason, years[0])
        stats["ambiguous"] += 1

    # The inverse risk of keeping the suffix in the key: a source that omits it
    # on some rows splits one person in two. Visible here, where a silent merge
    # would not be.
    folded: dict[str, list[str]] = {}
    for (display_name,) in conn.execute(
            "SELECT c.display_name FROM coaches c JOIN coach_external_ids e USING (coach_id) "
            "WHERE e.source = ? ORDER BY c.display_name", (SOURCE,)):
        folded.setdefault(identity.normalize_name(display_name), []).append(display_name)
    for variants in folded.values():
        if len(variants) > 1:
            stats["ambiguous"] += 1
            identity.record_unresolved(
                conn, "coach", SOURCE, "coaches", " / ".join(sorted(variants)),
                "names differing only by a generational suffix", None)

    identity.refresh_latest_seasons(conn)
    conn.commit()
    return stats


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("snapshot")
    p.add_argument("--db", default="db/league.db")
    a = p.parse_args()
    conn = sqlite3.connect(a.db)
    conn.execute("PRAGMA foreign_keys = ON")
    stats = load_coaches(conn, a.snapshot)
    conn.close()
    print(f"{a.snapshot}: {stats['coaches']} head coach"
          f"{'' if stats['coaches'] == 1 else 'es'}, {stats['tenures']} coach-seasons"
          + (f", {stats['unknown_team']} seasons at schools outside this database"
             if stats["unknown_team"] else "")
          + (f", {stats['ambiguous']} name-only identities flagged for review"
             if stats["ambiguous"] else ""))


if __name__ == "__main__":
    main()
