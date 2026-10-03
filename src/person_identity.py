#!/usr/bin/env python3
"""Identity resolution for players and coaches.

The single rule this module exists to enforce: a person is never created or merged
because two names look alike. Resolution is tried in a fixed order, and anything
that does not clear a bar is recorded for review instead of guessed at.

  1. Source identifier. A CFBD athlete id already seen maps to its existing
     player_id. This is the only path that can match across teams and seasons,
     which is what makes transfers one person rather than two.
  2. Strong contextual match. With no source id, a row may attach to an existing
     person only on normalized name AND team AND season AND position, and only if
     exactly one person matches. Two candidates is a collision, not a tie-break.
  3. Nothing. The row is written to person_unresolved and no person is invented.

normalize_name exists for step 2 and for alias storage. It is deliberately NOT
an identity: 'Mike Williams' at two schools normalizes identically, which is why
step 2 also requires team, season and position, and refuses a double match.
"""
from __future__ import annotations

import datetime as dt
import re
import sqlite3
from pathlib import Path
from typing import Optional

SCHEMA = Path(__file__).resolve().parent.parent / "sql" / "person_tables.sql"

# Generational suffixes are decoration on a name, not part of the person: CFBD,
# box scores and school rosters disagree about whether to print them, and about
# whether to punctuate them ("Jr." / "Jr" / "JR").
SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v", "vi"}

CLASS_YEARS = {1: "FR", 2: "SO", 3: "JR", 4: "SR", 5: "GR"}


def apply_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA.read_text(encoding="utf-8"))


def normalize_name(name: str) -> str:
    """A comparison key: case, punctuation and generational suffixes removed.

    'D.J. Uiagalelei', 'DJ Uiagalelei' and 'D J Uiagalelei' share a key, as do
    'Jerome Gaillard Jr.' and 'Jerome Gaillard'. The key is for LOOKUP only --
    sharing one is never by itself a reason to treat two rows as one person.
    """
    cleaned = re.sub(r"[^a-z0-9 ]+", " ", (name or "").lower())
    parts = [p for p in cleaned.split() if p]
    while len(parts) > 1 and parts[-1] in SUFFIXES:
        parts.pop()
    return " ".join(_join_initials(parts))


def _join_initials(parts: list[str]) -> list[str]:
    """Collapse a RUN of single characters into one token: ['d', 'j'] -> ['dj'].

    Stripping the periods from 'D.J.' leaves two tokens where 'DJ' leaves one,
    so without this the two commonest spellings of the same name produce
    different keys -- and a box-score line spelled one way would never match the
    roster row spelled the other.

    Only runs of two or more are joined. A lone middle initial ('John F
    Kennedy') is left alone, because gluing it to a real name would invent a
    token no source ever wrote.
    """
    out: list[str] = []
    run: list[str] = []
    for part in parts + [None]:
        if part is not None and len(part) == 1:
            run.append(part)
            continue
        if len(run) > 1:
            out.append("".join(run))
        else:
            out.extend(run)
        run = []
        if part is not None:
            out.append(part)
    return out


def identity_name(name: str) -> str:
    """A comparison key that KEEPS generational suffixes.

    normalize_name folds 'Jr.' away, which is right where a source id carries the
    identity and the name is only a lookup hint: a box score printing 'Jerome
    Gaillard Jr.' and a roster printing 'Jerome Gaillard' are one player, and the
    athlete id proves it.

    It is wrong where the NAME is the identity. CFBD's coaching feed has no coach
    id, and it contains Mike Sanford Sr. (UNLV, 2005-2009) and Mike Sanford Jr.
    (Western Kentucky and Colorado, 2017-2022) -- a father and son who both held
    FBS head-coaching jobs. Folding the suffix put both careers under one
    coach_id, which is a false statement on a page and one no later correction
    could detect.

    The cost of keeping the suffix is the opposite error: a source that omits it
    on one row splits one person in two. That is the safer failure -- both halves
    stay truthful, and two coaches sharing a display name are visible to a reader
    and to load_coaches.py's own check, where a silent merge is visible to
    nobody.
    """
    cleaned = re.sub(r"[^a-z0-9 ]+", " ", (name or "").lower())
    return " ".join(_join_initials([p for p in cleaned.split() if p]))


def split_name(first: Optional[str], last: Optional[str], display: str) -> tuple[Optional[str], Optional[str]]:
    """Source-provided name parts, falling back to a split of the display name."""
    if first or last:
        return (first or None), (last or None)
    parts = (display or "").split()
    if len(parts) < 2:
        return None, None
    return parts[0], " ".join(parts[1:])


def class_year_label(value) -> Optional[str]:
    """CFBD sends class as 1-4, sometimes 5. A NUMBER outside that range is not a
    class and becomes None.

    CFBD overloads the roster's `year` field: on the stub rows it returns for
    players with no listed position or jersey, it holds the SEASON (2026), not a
    class. Passing that through displayed a class year of "2026" on 1,625 of the
    2026 rows. A season is not a class, and an unknown class must read as
    unknown rather than as a confident wrong answer.

    A non-numeric value is kept verbatim: 'Freshman' or 'RS-FR' is a real class
    some sources give, and this is not the place to start renaming them.
    """
    if value is None or value == "" or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return CLASS_YEARS.get(value)
    text = str(value).strip()
    if text.isdigit():
        return CLASS_YEARS.get(int(text))
    return text or None


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def record_unresolved(conn: sqlite3.Connection, entity: str, source: str, context: str,
                      display_name: str, reason: str, season_year: Optional[int] = None,
                      source_team: Optional[str] = None) -> None:
    conn.execute(
        "INSERT INTO person_unresolved (entity, source, context, display_name, season_year, "
        "source_team, reason, seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (entity, source, context, display_name, season_year, source_team, reason, _now()))


def add_alias(conn: sqlite3.Connection, player_id: int, raw_name: str) -> None:
    alias = normalize_name(raw_name)
    if not alias:
        return
    conn.execute("INSERT OR IGNORE INTO player_name_aliases (player_id, alias, raw_name) "
                 "VALUES (?, ?, ?)", (player_id, alias, raw_name))


def find_by_external_id(conn: sqlite3.Connection, source: str, external_id: str) -> Optional[int]:
    row = conn.execute("SELECT player_id FROM player_external_ids WHERE source = ? AND external_id = ?",
                       (source, str(external_id))).fetchone()
    return row[0] if row else None


def find_by_context(conn: sqlite3.Connection, display_name: str, season_year: int,
                    team_id: int, position: Optional[str]) -> tuple[Optional[int], str]:
    """Name + team + season + position, requiring exactly one candidate.

    Returns (player_id, reason). A second candidate returns (None, 'name collision'):
    two same-name players on one roster must stay two people until something
    stronger than a name separates them.
    """
    alias = normalize_name(display_name)
    if not alias:
        return None, "empty name"
    candidates = [r[0] for r in conn.execute(
        "SELECT DISTINCT s.player_id FROM player_team_seasons s "
        "JOIN player_name_aliases a ON a.player_id = s.player_id "
        "WHERE a.alias = ? AND s.season_year = ? AND s.team_id = ? "
        "AND (? IS NULL OR s.position IS NULL OR s.position = ?)",
        (alias, season_year, team_id, position, position))]
    if len(candidates) == 1:
        return candidates[0], "context"
    if len(candidates) > 1:
        return None, "name collision"
    return None, "no match"


def create_player(conn: sqlite3.Connection, display_name: str, first_name: Optional[str] = None,
                  last_name: Optional[str] = None, position: Optional[str] = None,
                  hometown: Optional[str] = None, home_state: Optional[str] = None,
                  height=None, weight=None) -> int:
    now = _now()
    cur = conn.execute(
        "INSERT INTO players (display_name, first_name, last_name, primary_position, hometown, "
        "home_state, height, weight, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (display_name, first_name, last_name, position, hometown, home_state, height, weight, now, now))
    player_id = int(cur.lastrowid)
    add_alias(conn, player_id, display_name)
    return player_id


def link_external_id(conn: sqlite3.Connection, player_id: int, source: str, external_id: str,
                     confidence: str, is_primary: bool = True) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO player_external_ids (player_id, source, external_id, confidence, is_primary) "
        "VALUES (?, ?, ?, ?, ?)", (player_id, source, str(external_id), confidence, int(is_primary)))


def resolve_player(conn: sqlite3.Connection, source: str, display_name: str,
                   external_id=None, season_year: Optional[int] = None,
                   team_id: Optional[int] = None, position: Optional[str] = None,
                   context: str = "roster", create: bool = True,
                   **profile) -> tuple[Optional[int], str]:
    """One person for a source row, or (None, reason) with the row recorded for review.

    `create=False` is the box-score case: an archived line may only LINK to a person
    who already exists, never bring one into being from a name.
    """
    if external_id not in (None, ""):
        existing = find_by_external_id(conn, source, str(external_id))
        if existing is not None:
            add_alias(conn, existing, display_name)
            return existing, "external id"
        if not create:
            record_unresolved(conn, "player", source, context, display_name,
                              "unknown external id", season_year)
            return None, "unknown external id"
        first, last = split_name(profile.get("first_name"), profile.get("last_name"), display_name)
        player_id = create_player(conn, display_name, first, last, position,
                                  profile.get("hometown"), profile.get("home_state"),
                                  profile.get("height"), profile.get("weight"))
        link_external_id(conn, player_id, source, str(external_id), "id")
        return player_id, "created"

    if season_year is not None and team_id is not None:
        matched, reason = find_by_context(conn, display_name, season_year, team_id, position)
        if matched is not None:
            add_alias(conn, matched, display_name)
            return matched, reason
        if reason == "name collision" or not create:
            record_unresolved(conn, "player", source, context, display_name, reason, season_year)
            return None, reason

    if not create:
        record_unresolved(conn, "player", source, context, display_name, "no source id", season_year)
        return None, "no source id"

    # No source id and no existing contextual match: a new person with no external
    # identity. It is recorded as such so a later ID-bearing feed can be checked
    # against it rather than quietly creating a duplicate.
    first, last = split_name(profile.get("first_name"), profile.get("last_name"), display_name)
    player_id = create_player(conn, display_name, first, last, position,
                              profile.get("hometown"), profile.get("home_state"),
                              profile.get("height"), profile.get("weight"))
    record_unresolved(conn, "player", source, context, display_name, "created without source id",
                      season_year)
    return player_id, "created without source id"


def resolve_team_id(conn: sqlite3.Connection, raw_name: str) -> Optional[int]:
    """Canonical team_id for a source school string, through the project's aliases.

    None rather than an exception: roster and coaching feeds legitimately name
    schools this FBS-only database does not carry, and an opponent outside the
    league is not a data error.
    """
    name = (raw_name or "").strip()
    if not name:
        return None
    row = conn.execute("SELECT team_id FROM teams WHERE team_name = ?", (name,)).fetchone()
    if row:
        return int(row[0])
    row = conn.execute(
        "SELECT t.team_id FROM team_aliases a JOIN teams t ON t.team_name = a.team_name "
        "WHERE TRIM(a.alias) = TRIM(?) LIMIT 1", (name,)).fetchone()
    return int(row[0]) if row else None


def refresh_latest_seasons(conn: sqlite3.Connection) -> None:
    """Derived fields, recomputed from the season rows rather than tracked on write."""
    conn.execute("UPDATE players SET latest_season = ("
                 "SELECT MAX(season_year) FROM player_team_seasons s WHERE s.player_id = players.player_id)")
    conn.execute("UPDATE players SET primary_position = COALESCE(("
                 "SELECT s.position FROM player_team_seasons s WHERE s.player_id = players.player_id "
                 "AND s.position IS NOT NULL ORDER BY s.season_year DESC LIMIT 1), primary_position)")
    conn.execute("UPDATE coaches SET latest_season = ("
                 "SELECT MAX(season_year) FROM coach_tenures t WHERE t.coach_id = coaches.coach_id)")
