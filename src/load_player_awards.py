#!/usr/bin/env python3
"""
Load data/raw/awards/<year>.json.gz into awards and player_awards.

Separate from the fetch on purpose. The fetch is a network call that should not
have to be repeated; what goes wrong here is IDENTITY -- turning "Travis Hunter,
Colorado" into one of this project's 99,813 people -- and that must be fixable
against the committed archive rather than against ESPN.

How a winner is resolved, in order:

  1. an ESPN athlete id this project has already linked to a person. The first
     successful contextual match LINKS the id, so every later run of every
     ingestion resolves that person by id instead of by name.
  2. name + team + season, requiring exactly ONE candidate. Two players of one
     name on one roster stay two people; the award is recorded unresolved
     rather than attached to a coin flip.
  3. surname + team + season, again requiring exactly one candidate, because
     ESPN prints the name a player is announced by and the roster prints the
     name he enrolled under -- "Olu Oluwatimi" won the 2022 Rimington, and the
     roster that season says Olusegun.

Position is a TIE-BREAKER here, never a filter. The two feeds draw on the same
vocabulary but disagree per player: ESPN called Manti Te'o a LB where the
roster says ILB, Aaron Donald a DT where the roster says DL, Jahdae Barron a CB
where the roster says DB. Filtering on an exact string therefore threw away 86
winners who were in the archive all along, every one of them a defender or a
lineman -- the offensive skill positions are the ones whose names the two feeds
happen to spell the same way. Compatibility is by family, and an unknown
position on either side is compatible with anything.

Nothing is created. An award winner with no roster row in this project is kept
as a name, with `resolution` saying why, and the page shows the name without a
link. That is the usual case before 2009, where the roster archive begins, and
for every coach-of-the-year award, whose winner is not a player at all.

Usage:
    python src/load_player_awards.py data/raw/awards/2024.json.gz --db db/league.db
"""
from __future__ import annotations

import argparse
import gzip
import json
import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import person_identity as pid

SCHEMA = Path(__file__).resolve().parent.parent / "sql" / "award_tables.sql"

# What each trophy is, keyed on award_key. An explicit table rather than a
# pattern on the name: ESPN's index mixes four different things under one
# endpoint, and not one of the four coach awards has the word "coach" in its
# name, so the regex that used to decide this matched nothing at all and every
# coach was reported as a failed player match.
#
#   player  -- an FBS player's trophy. These are the ones a player page shows.
#   coach   -- won by a coach, who is in the coaching tables and not the roster.
#   non_fbs -- the FCS, Division II and Division III player awards. Real
#              trophies, but their winners are not in an FBS roster archive, so
#              a failure to resolve them is the archive's scope, not a defect.
#   weekly  -- a week's honour, not a season's. A different grain from every
#              other row here, so it is kept and not shown.
#   other   -- given to a contributor rather than to a player or a coach.
#
# A trophy ESPN adds later is treated as a player award and named in the run's
# summary, so a new one shows up in the log rather than silently vanishing.
AWARD_CATEGORY = {
    # FBS player trophies, the only kind a player page shows.
    "bronko-nagurski-award": "player",
    "burlsworth-trophy": "player",
    "campbell-trophy": "player",
    "chuck-bednarik-award": "player",
    "davey-o-brien-award": "player",
    "dick-butkus-award": "player",
    "disney-spirit-award": "player",
    "doak-walker-award": "player",
    "fred-biletnikoff-award": "player",
    "heisman-memorial-trophy": "player",
    "jim-thorpe-award": "player",
    "john-mackey-award": "player",
    "johnny-unitas-golden-arm-award": "player",
    "lombardi-award": "player",
    "lott-impact-trophy": "player",
    "lou-groza-award": "player",
    "maxwell-award": "player",
    "outland-trophy": "player",
    "paul-hornung-award": "player",
    "ray-guy-award": "player",
    "rimington-trophy": "player",
    "ted-hendricks-award": "player",
    "walter-camp-award": "player",
    "wuerffel-trophy": "player",
    # Won by a coach, who is in the coaching tables and not the roster. Not one
    # of these four has the word "coach" in its name.
    "buddy-teevens-award": "coach",
    "eddie-robinson-award": "coach",
    "frank-broyles-award": "coach",
    "the-home-depot-award": "coach",
    # FCS, Division II and Division III player awards.
    "buck-buchanan-award": "non_fbs",
    "gagliardi-trophy": "non_fbs",
    "harlon-hill-trophy": "non_fbs",
    "melberger-award": "non_fbs",
    "walter-payton-award": "non_fbs",
    # A week's honour, not a season's.
    "defensive-player-of-the-week": "weekly",
    "offensive-player-of-the-week": "weekly",
    "special-teams-player-of-the-week": "weekly",
    # Given to a contributor rather than to a player or a coach.
    "ncfaa-contributions-award": "other",
}
DEFAULT_CATEGORY = "player"

# Positions that mean the same part of a roster. ESPN and CFBD both draw from
# this vocabulary and still disagree on which word they use for one player, so
# a position narrows a field of candidates and never excludes the only one.
POSITION_FAMILY = {
    "QB": "qb",
    "RB": "back", "FB": "back",
    "WR": "receiver", "TE": "receiver",
    "C": "ol", "G": "ol", "OT": "ol", "OL": "ol",
    "DL": "dl", "DT": "dl", "DE": "dl", "NT": "dl",
    "LB": "lb", "ILB": "lb", "OLB": "lb",
    "DB": "db", "CB": "db", "S": "db",
    "PK": "kick", "K": "kick", "P": "kick", "LS": "kick", "PR": "kick",
}
# EDGE is played off the line and off the ball, and both feeds use it for both,
# so it is compatible with either rather than belonging to one.
POSITION_FAMILY_ALSO = {"EDGE": ("dl", "lb")}
# ESPN writes a position it does not know as a dash.
UNKNOWN_POSITIONS = {"", "-", "?", "ATH", "N/A"}


def position_families(position: str | None) -> frozenset[str] | None:
    """The families a position could belong to, or None when it says nothing."""
    code = (position or "").strip().upper()
    if code in UNKNOWN_POSITIONS:
        return None
    if code in POSITION_FAMILY_ALSO:
        return frozenset(POSITION_FAMILY_ALSO[code])
    family = POSITION_FAMILY.get(code)
    return frozenset({family}) if family else None


def positions_compatible(left: str | None, right: str | None) -> bool:
    """Whether two position strings could describe one player."""
    a, b = position_families(left), position_families(right)
    if a is None or b is None:
        return True            # an unknown position rules nobody out
    return bool(a & b)


def apply_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA.read_text(encoding="utf-8"))


def award_key(name: str) -> str:
    """A stable key from an award's name.

    Derived from the name rather than ESPN's id so that a renumbering on their
    side cannot split one trophy into two rows, and so the key is readable in a
    URL and a test.
    """
    slug = re.sub(r"[^a-z0-9]+", "-", (name or "").strip().lower()).strip("-")
    return slug


def read_season(path: Path) -> dict:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fh:
        return json.load(fh)


def _by_alias(conn: sqlite3.Connection, alias: str, season: int,
              team_id: int) -> list[tuple[int, str | None]]:
    """(player_id, position) for everyone of this name on that team that season."""
    return [(r[0], r[1]) for r in conn.execute(
        "SELECT DISTINCT s.player_id, s.position FROM player_team_seasons s "
        "JOIN player_name_aliases a ON a.player_id = s.player_id "
        "WHERE a.alias = ? AND s.season_year = ? AND s.team_id = ?",
        (alias, season, team_id))]


def _by_surname(conn: sqlite3.Connection, surname: str, season: int,
                team_id: int) -> list[tuple[int, str | None]]:
    """The same, matching on the last word of a name rather than the whole name.

    The surname is taken from the names the project already holds rather than
    from players.last_name, which the roster feed leaves empty on some rows,
    and it is normalized the same way an alias is so that punctuation and a
    generational suffix cannot separate two spellings of one name.
    """
    rows = conn.execute(
        "SELECT DISTINCT s.player_id, s.position, a.alias FROM player_team_seasons s "
        "JOIN player_name_aliases a ON a.player_id = s.player_id "
        "WHERE s.season_year = ? AND s.team_id = ?", (season, team_id))
    found: dict[int, str | None] = {}
    for player_id, position, alias in rows:
        if (alias or "").split()[-1:] == [surname]:
            found.setdefault(player_id, position)
    return list(found.items())


def _one_candidate(candidates: list[tuple[int, str | None]],
                   position: str | None) -> tuple[int | None, str]:
    """The single person these candidates name, narrowing by position if need be.

    One candidate is taken whatever its position says, because the two feeds
    disagree about positions far more often than one roster holds two players
    of one name. Several candidates are narrowed to the compatible ones, and
    only a narrowing down to exactly one resolves.
    """
    if len(candidates) == 1:
        return candidates[0][0], "ok"
    if not candidates:
        return None, "no match"
    compatible = [c for c in candidates if positions_compatible(position, c[1])]
    if len(compatible) == 1:
        return compatible[0][0], "ok"
    return None, "name collision"


def resolve_winner(conn: sqlite3.Connection, winner: dict, season: int,
                   category: str) -> tuple[int | None, int | None, str]:
    """(player_id, team_id, resolution) for one winner. Never creates a person."""
    name = (winner.get("display_name") or "").strip()
    espn_id = winner.get("espn_athlete_id")
    school = (winner.get("school") or "").strip()
    team_id = pid.resolve_team_id(conn, school) if school else None
    position = winner.get("position")

    if not name:
        return None, team_id, "no name from the source"

    if espn_id:
        existing = pid.find_by_external_id(conn, "espn", str(espn_id))
        if existing is not None:
            pid.add_alias(conn, existing, name)
            return existing, team_id, "espn id"

    if category == "coach":
        # Not a failed match: a coach is simply not in the player tables.
        return None, team_id, "coach award"
    if category == "non_fbs":
        # Nor is an FCS, Division II or Division III winner. Saying so keeps
        # him out of the count of winners this project failed to find.
        return None, team_id, "outside this archive"
    if team_id is None:
        return None, None, "unknown team"

    alias = pid.normalize_name(name)
    matched, reason = _one_candidate(_by_alias(conn, alias, season, team_id), position)
    resolution = "context"
    if matched is None and reason == "no match":
        # ESPN prints the name he is announced by; the roster prints the name he
        # enrolled under. A surname is what survives that, and it is held to the
        # same single-candidate rule.
        surname = (alias.split() or [""])[-1]
        if surname:
            matched, reason = _one_candidate(
                _by_surname(conn, surname, season, team_id), position)
            resolution = "surname"
    if matched is None:
        pid.record_unresolved(conn, "player", "espn", "awards", name, reason, season)
        return None, team_id, reason
    pid.add_alias(conn, matched, name)
    if espn_id:
        # The contextual match is what earns the id its link, marked as such:
        # 'context' says this pairing came from a name, a team and a season,
        # not from a source that numbered the same person twice.
        pid.link_external_id(conn, matched, "espn", str(espn_id), resolution)
    return matched, team_id, resolution


def load_awards(conn: sqlite3.Connection, path: str | Path) -> dict:
    """One season's file into the tables, replacing that season wholesale."""
    path = Path(path)
    payload = read_season(path)
    season = int(payload["season"])
    apply_schema(conn)
    cur = conn.cursor()
    # Replaced wholesale so a re-run after a correction cannot leave a stale
    # winner beside the right one.
    cur.execute("DELETE FROM player_awards WHERE season_year = ?", (season,))

    stats = {"awards": 0, "winners": 0, "linked": 0, "by_espn_id": 0,
             "by_surname": 0, "coach_awards": 0, "outside": 0, "weekly": 0,
             "unresolved": 0, "no_winner": 0, "uncategorised": []}
    for award in payload.get("awards") or []:
        name = award.get("name")
        key = award_key(name)
        if not key:
            continue
        stats["awards"] += 1
        category = AWARD_CATEGORY.get(key, DEFAULT_CATEGORY)
        if key not in AWARD_CATEGORY:
            stats["uncategorised"].append(name)
        cur.execute(
            """INSERT INTO awards (award_key, name, description, espn_award_id,
                                   category, first_season, last_season)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(award_key) DO UPDATE SET
                    name = excluded.name,
                    description = COALESCE(excluded.description, awards.description),
                    espn_award_id = COALESCE(excluded.espn_award_id, awards.espn_award_id),
                    category = excluded.category,
                    first_season = MIN(awards.first_season, excluded.first_season),
                    last_season = MAX(awards.last_season, excluded.last_season)""",
            (key, name, award.get("description"), award.get("espn_award_id"),
             category, season, season))

        winners = award.get("winners") or []
        if not winners:
            stats["no_winner"] += 1
            continue
        for winner in winners:
            player_id, team_id, resolution = resolve_winner(conn, winner, season, category)
            stats["winners"] += 1
            if player_id is not None:
                stats["linked"] += 1
                if resolution == "espn id":
                    stats["by_espn_id"] += 1
                elif resolution == "surname":
                    stats["by_surname"] += 1
            elif resolution == "coach award":
                stats["coach_awards"] += 1
            elif resolution == "outside this archive":
                stats["outside"] += 1
            elif category == "weekly":
                stats["weekly"] += 1
            else:
                # The only count that means this project failed to find someone
                # it should have: an FBS player's trophy with no person behind it.
                stats["unresolved"] += 1
            cur.execute(
                """INSERT OR REPLACE INTO player_awards
                       (season_year, award_key, winner_name, source_team, position,
                        team_id, player_id, espn_athlete_id, espn_team_id, resolution)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (season, key, (winner.get("display_name") or "").strip() or "(unnamed)",
                 winner.get("school"), winner.get("position"), team_id, player_id,
                 winner.get("espn_athlete_id"), winner.get("espn_team_id"), resolution))
    conn.commit()
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("path", help="data/raw/awards/<year>.json.gz")
    ap.add_argument("--db", default="db/league.db")
    args = ap.parse_args()
    conn = sqlite3.connect(args.db)
    stats = load_awards(conn, args.path)
    print(f"{Path(args.path).name}: {stats['awards']} awards, {stats['winners']} winners, "
          f"{stats['linked']} linked to a person "
          f"({stats['by_espn_id']} by ESPN id, {stats['by_surname']} by surname), "
          f"{stats['coach_awards']} coach, {stats['outside']} outside this archive, "
          f"{stats['weekly']} weekly, {stats['unresolved']} unresolved, "
          f"{stats['no_winner']} awards with no winner recorded")
    for name in stats["uncategorised"]:
        print(f"  NOTE: {name!r} is not in AWARD_CATEGORY; treated as a player award")


if __name__ == "__main__":
    main()
