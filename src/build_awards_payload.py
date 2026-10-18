#!/usr/bin/env python3
"""
The awards payload the site reads: every trophy, every winner, one file.

Two pages read the same payload, so there is one file rather than two. A player
page needs the handful of trophies that one person won; the awards view needs
every winner of every trophy, season by season. Keyed both ways here so neither
page has to scan the other's index.

What is left out, and why:

  * The FCS, Division II and Division III player awards, and the weekly
    honours. They are kept in the database -- they are real trophies and the
    archive should not pretend otherwise -- but this project rates FBS teams
    and holds FBS rosters, so their winners can never have a page, and a list
    of twenty unlinked names is not a feature.
  * Nothing is inferred. A winner with no person behind him is published as the
    name ESPN printed, with the school it printed, and the page says the name
    without a link. Before 2009, where the roster archive begins, that is every
    winner; after it, twenty-two of three hundred and ninety-four.

Display data only. No rating reads an award: a vote taken after the season is
already an opinion about the season, so feeding it back into a rating would be
circular.
"""
from __future__ import annotations

import sqlite3

# The categories a reader is looking for on a page about trophies. 'coach' is
# included because a reader scanning for the national coach of the year expects
# to find it; those winners carry no link, since they are in the coaching
# tables rather than the roster and nothing has matched them yet.
SHOWN = ("player", "coach")


def build_payload(conn: sqlite3.Connection) -> dict:
    """{awards: [...], winners: [...], by_player: {id: [winner index, ...]}}."""
    if not _has_tables(conn):
        return {"awards": [], "winners": [], "by_player": {}}

    # Read positionally rather than by name: this runs inside the static export,
    # whose connection has no row factory.
    awards = [
        {"key": key, "name": name, "description": description,
         "category": category, "first": first, "last": last}
        for key, name, description, category, first, last in conn.execute(
            "SELECT award_key, name, description, category, first_season, last_season "
            "FROM awards WHERE category IN (%s) ORDER BY name" % ",".join("?" * len(SHOWN)),
            SHOWN)
    ]
    shown = {a["key"] for a in awards}

    winners: list[dict] = []
    by_player: dict[str, list[int]] = {}
    rows = conn.execute(
        """SELECT pa.season_year, pa.award_key, pa.winner_name, pa.source_team,
                  pa.position, pa.player_id, pa.team_id, t.team_name
             FROM player_awards pa
             LEFT JOIN teams t ON t.team_id = pa.team_id
            ORDER BY pa.season_year DESC, pa.award_key, pa.winner_name""")
    for season, key, winner_name, source_team, position, player_id, team_id, team_name in rows:
        if key not in shown:
            continue
        # The school is the project's own team name where the winner resolved to
        # one, so a link and its label agree; otherwise it is the string ESPN
        # printed, which is all that is known.
        row = {"season": season, "key": key, "name": winner_name,
               "school": team_name or source_team or None,
               "position": _position(position),
               "player": player_id, "team": team_id}
        winners.append({k: v for k, v in row.items() if v is not None})
        if player_id is not None:
            by_player.setdefault(str(player_id), []).append(len(winners) - 1)
    return {"awards": awards, "winners": winners, "by_player": by_player}


def _position(position: str | None) -> str | None:
    """ESPN writes a position it does not know as a dash; that is not a position."""
    code = (position or "").strip()
    return None if code in ("", "-", "?") else code


def _has_tables(conn: sqlite3.Connection) -> bool:
    found = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' "
        "AND name IN ('awards', 'player_awards')")}
    return found == {"awards", "player_awards"}


def main() -> None:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default="db/league.db")
    args = ap.parse_args()
    conn = sqlite3.connect(args.db)
    payload = build_payload(conn)
    linked = sum(1 for w in payload["winners"] if "player" in w)
    print(f"{len(payload['awards'])} trophies, {len(payload['winners'])} winners, "
          f"{linked} with a page, {len(payload['by_player'])} people")
    for a in payload["awards"]:
        n = sum(1 for w in payload["winners"] if w["key"] == a["key"])
        print(f"  {a['category']:6} {a['name']}: {n} winners "
              f"({a['first']}-{a['last']})")


if __name__ == "__main__":
    main()
