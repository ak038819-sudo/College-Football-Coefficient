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

import csv
import datetime as dt
import hashlib
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


def date_only(value) -> Optional[str]:
    """The date part of a timestamp CFBD sends as one.

    Every hire date in the coaching feed arrives as `2010-12-12T00:00:00.000Z`.
    The time is not information -- it is midnight UTC on all 566 of them -- and
    carrying it through meant a coach page printed the whole timestamp where a
    date belongs. Anything that is not a leading ISO date is returned unchanged
    rather than discarded, so a source that sends a different shape is visible
    instead of silently emptied.
    """
    if value is None:
        return None
    text = str(value).strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-" and text[:4].isdigit():
        return text[:10]
    return text or None


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


# The seasons a career normally holds: four years of eligibility, a redshirt
# year, and the free year the NCAA granted for 2020. More than this is unusual
# rather than impossible -- Cam McCormick really did play nine, from 2016 to
# 2024, on injury waivers -- so exceeding it is a reason to check, never a
# reason to discard a row.
MAX_CAREER_SEASONS = 6


def flag_implausible_careers(conn: sqlite3.Connection, source: str = "cfbd") -> int:
    """Record players whose source id spans more seasons than a career usually does.

    The 2009-2026 backfill turned up 446 of these out of 99,813 people. Every
    one carries a CFBD athlete id, so this is the FEED asserting it rather than
    a contextual match guessing, and the two readings are genuinely different:

      - Athlete 4571882 is on thirteen roster rows from 2015 to 2024, at West
        Virginia, Kansas State and Baylor, every one reading "LB, #2, SR". That
        is CFBD reusing an id or repeating a stale row.
      - Cam McCormick's nine seasons, Oregon 2016-2022 then Miami, are real. The
        NCAA granted the waivers.

    Nothing in the feed separates them, so the rows are kept exactly as given
    and the person is flagged, the same way a name-only coach identity is. A
    page can then say the career is unusually long and worth checking, instead
    of either presenting thirteen seasons as fact or quietly dropping a real
    one.

    Recomputed from scratch on every call, because a career is only whole once
    every season has been loaded -- the same reason the coach loader checks
    after reading all its records rather than during.
    """
    conn.execute("DELETE FROM person_unresolved WHERE entity = 'player' "
                 "AND reason LIKE 'source id spanning%'")
    flagged = implausible_careers(conn, source)
    for player_id, (name, reason, first) in flagged.items():
        record_unresolved(conn, "player", source, "rosters", name, reason, first)
    return len(flagged)


def implausible_careers(conn: sqlite3.Connection, source: str = "cfbd") -> dict:
    """{player_id: (display_name, reason, first season)} for the careers above.

    Keyed on player_id, not on the name, because the point of this project's
    identity work is that a name is not a person: two players called John Smith
    would otherwise both carry a caveat only one of them earned. The exporter
    reads this rather than person_unresolved for the same reason -- that table
    records names, having been written for people who have no player_id at all.
    """
    rows = conn.execute(
        "SELECT s.player_id, p.display_name, COUNT(DISTINCT s.season_year) seasons, "
        "COUNT(DISTINCT s.team_id) teams, MIN(s.season_year) first "
        "FROM player_team_seasons s JOIN players p USING (player_id) "
        "WHERE s.source = ? GROUP BY s.player_id "
        "HAVING COUNT(DISTINCT s.season_year) > ?", (source, MAX_CAREER_SEASONS)).fetchall()
    return {int(player_id): (name, "source id spanning %d seasons at %d school%s"
                             % (seasons, teams, "" if teams == 1 else "s"), first)
            for player_id, name, seasons, teams, first in rows}


SPLIT_CAREERS = Path(__file__).resolve().parent.parent / "data" / "identity" / "split_careers.csv"

# The verdicts in that file that are strong enough to link two pages together.
# "needs a human" and "TWO PEOPLE" are deliberately absent: the first is an
# unresolved contradiction and the second is two people who share a name, which
# is the one error this whole module exists to prevent.
LINKED_VERDICTS = frozenset({"one person", "likely one person"})


def load_split_careers(path: Optional[Path] = None) -> list[dict]:
    """The hand-reviewed file of people CFBD recorded under two athlete ids.

    Reviewed by hand and kept in the repository on purpose. These 74 judgements
    are about 74 named people -- whether a hometown of "Ewa Beach, HI" on one
    record and "Cincinnati, OH" on the other is one person with a bad field or
    two players who share a name -- and no rule derived from the feed can settle
    them. A file a reader can open, with the verdict written next to the name,
    is the only honest form for that. See docs/identity-problems.md 1.
    """
    # Resolved at call time, not bound as a default, so a test can point this at
    # a fixture file -- and so the path stays one name to change.
    path = Path(path) if path is not None else SPLIT_CAREERS
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as handle:
        return [{"season": int(row["season"]), "name": row["name"],
                 "ids": (int(row["id_a"]), int(row["id_b"])),
                 "verdict": row["verdict"]}
                for row in csv.DictReader(handle)]


def linked_careers(conn: sqlite3.Connection, path: Optional[Path] = None) -> dict:
    """{player_id: {"id": counterpart, "name": ..., "season": ...}} for the links.

    The relation is SYMMETRIC and nothing is rewritten: each id keeps its own
    page and its own statistics, and each page names the other. Choosing a
    primary would mean choosing which half of a career is the real one, and a
    published id that moved would break every URL already in the wild -- ids are
    a function of the source (docs/identity-problems.md 8).

    A pair is dropped unless both ids are people in THIS database. A link to a
    page that does not exist would send a reader to "player not found", which is
    the same defect the box-score exporter was fixed for.
    """
    present = {int(row[0]): row[1] for row in conn.execute(
        "SELECT player_id, display_name FROM players")}
    links: dict[int, dict] = {}
    for case in load_split_careers(path):
        if case["verdict"] not in LINKED_VERDICTS:
            continue
        first, second = case["ids"]
        if first not in present or second not in present or first == second:
            continue
        for me, other in ((first, second), (second, first)):
            links[me] = {"id": other, "name": present[other], "season": case["season"]}
    return links


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



# --- stable ids -------------------------------------------------------------
# A person's id is published in a URL (#player=<id>, #coach=<id>), so it has to
# be a FUNCTION of the source data and not of the order a rebuild happened to
# read it in. db/league.db is not committed: every deploy builds it from
# scratch. An autoincrement id therefore depended on load order, and did
# diverge -- a database built 2026-first put player 16 on one person while the
# pipeline's sorted 2009-first load put a different person there, so a link
# shared today would have pointed at someone else after the next deploy.
#
# So the id IS the source's identity wherever the source has one. CFBD gives an
# athlete id on all 350,670 roster rows in the 2009-2026 archive, 29,162 of them
# negative, which is CFBD's own placeholder form and still one id per person, so
# the sign is kept rather than folded into something that could collide. The
# coaching feed gives no id at all, so a coach's id is derived from the key the
# loader already builds from the name -- the same key, so the same id, on every
# rebuild.

SURROGATE_BASE = 10 ** 12   # clear of every CFBD athlete id (max 5,454,594)
_HASH_BYTES = 5             # ids stay inside 13 digits, which the routes accept


def derived_id(key: str, base: int = 0) -> int:
    """A deterministic id for a person the source does not number."""
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return base + int.from_bytes(digest[:_HASH_BYTES], "big")


def source_player_id(external_id) -> int:
    """The id a CFBD athlete id gives a person, kept exactly as the source wrote
    it so the same snapshot always produces the same URL."""
    text = str(external_id).strip()
    try:
        return int(text)
    except ValueError:
        # No source this project reads does this; deriving rather than raising
        # means a future one cannot silently fall back to a load-ordered id.
        return derived_id("external:" + text, SURROGATE_BASE)


def free_id(conn: sqlite3.Connection, table: str, column: str, candidate: int) -> int:
    """`candidate`, or the next id after it that nothing holds.

    Two different keys hashing together is a one-in-fifty-thousand event across
    an archive this size, and this is the only path where an id depends on what
    was loaded before it. Probing beats raising: a rebuild that dies on a hash
    collision is worse than one person's URL moving.
    """
    while conn.execute(f"SELECT 1 FROM {table} WHERE {column} = ?", (candidate,)).fetchone():
        candidate += 1
    return candidate


def remap_ids(conn: sqlite3.Connection, table: str, column: str,
              children: list[tuple[str, str]], mapping: dict) -> int:
    """Move people in `mapping` (old id -> new id) without breaking their rows.

    A database written before ids were derived holds people at load-ordered ids,
    and the loaders would never notice: they look a person up by source id, find
    the existing row and reuse its id forever. So the ids are moved rather than
    left to rot, in two passes through a parking range, because an old id and a
    new one can want the same number.

    The parent row is inserted at the new id before the children move and the
    old one is deleted after, so every foreign key holds throughout.
    """
    moves = {old: new for old, new in mapping.items() if old != new}
    if not moves:
        return 0
    parking = {old: 10 ** 15 + i for i, old in enumerate(sorted(moves))}
    for stage in (parking, {parking[old]: new for old, new in moves.items()}):
        for old, new in sorted(stage.items()):
            cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
            others = [c for c in cols if c != column]
            conn.execute(f"INSERT INTO {table} ({column}, {', '.join(others)}) "
                         f"SELECT ?, {', '.join(others)} FROM {table} WHERE {column} = ?",
                         (new, old))
            for child, child_col in children:
                conn.execute(f"UPDATE {child} SET {child_col} = ? WHERE {child_col} = ?",
                             (new, old))
            conn.execute(f"DELETE FROM {table} WHERE {column} = ?", (old,))
    return len(moves)


def migrate_player_ids(conn: sqlite3.Connection, source: str = "cfbd") -> int:
    """Move players the old autoincrement scheme put at a load-ordered id.

    Runs before a snapshot is read, so the ids a loader then reuses are already
    the derived ones.
    """
    mapping = {}
    for player_id, external_id in conn.execute(
            "SELECT player_id, external_id FROM player_external_ids WHERE source = ?", (source,)):
        mapping[int(player_id)] = source_player_id(external_id)
    return remap_ids(conn, "players", "player_id",
                     [("player_external_ids", "player_id"),
                      ("player_team_seasons", "player_id"),
                      ("player_name_aliases", "player_id")], mapping)

def create_player(conn: sqlite3.Connection, display_name: str, first_name: Optional[str] = None,
                  last_name: Optional[str] = None, position: Optional[str] = None,
                  hometown: Optional[str] = None, home_state: Optional[str] = None,
                  height=None, weight=None, player_id: Optional[int] = None) -> int:
    now = _now()
    if player_id is None:
        cur = conn.execute(
            "INSERT INTO players (display_name, first_name, last_name, primary_position, hometown, "
            "home_state, height, weight, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (display_name, first_name, last_name, position, hometown, home_state, height, weight,
             now, now))
        player_id = int(cur.lastrowid)
    else:
        player_id = int(player_id)
        conn.execute(
            "INSERT INTO players (player_id, display_name, first_name, last_name, primary_position, "
            "hometown, home_state, height, weight, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (player_id, display_name, first_name, last_name, position, hometown, home_state,
             height, weight, now, now))
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
                                  profile.get("height"), profile.get("weight"),
                                  player_id=source_player_id(external_id))
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
    # Derived from the context that identified them, because there is no source id
    # to derive from and an autoincrement id would move on the next rebuild.
    derived = free_id(conn, "players", "player_id",
                      derived_id("context:%s|%s|%s" % (normalize_name(display_name), team_id,
                                                       season_year), SURROGATE_BASE))
    player_id = create_player(conn, display_name, first, last, position,
                              profile.get("hometown"), profile.get("home_state"),
                              profile.get("height"), profile.get("weight"), player_id=derived)
    record_unresolved(conn, "player", source, context, display_name, "created without source id",
                      season_year)
    return player_id, "created without source id"


def resolve_team_id(conn: sqlite3.Connection, raw_name: str) -> Optional[int]:
    """Canonical team_id for a source school string, through the project's aliases.

    None rather than an exception: roster and coaching feeds legitimately name
    schools this FBS-only database does not carry, and an opponent outside the
    league is not a data error.
    """
    from team_identity import resolve_database_name
    name = resolve_database_name(conn.cursor(), raw_name)
    row = conn.execute("SELECT team_id FROM teams WHERE team_name = ?", (name,)).fetchone()
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
