#!/usr/bin/env python3
"""Player and coach page data (v0.1.1 phase 2).

Writes, under ui/data/people/:

  index_<key>.js     name -> person, sharded on the first character of each WORD
                     of the normalized name, so search loads one small shard per
                     keystroke instead of a directory of every person who ever
                     played, and a surname finds its owner. Key is 'a'-'z', '0'
                     for a leading digit, '_' for anything else.
  player_<n>.js      player detail payloads, sharded on player_id % PLAYER_SHARDS.
                     A player page loads exactly one.
  roster_<season>.js one season's rosters grouped by team, carrying the rows a
                     roster table displays, for the team page's roster module.
  coaches.js         every coach with their whole tenure history. Head coaches
                     number in the hundreds, not the hundred-thousands, so this
                     stays one file.
  team_coaches.js    season -> team -> the head coach's id and name, so a team
                     page can name its coach without loading every career.

Sharded from the start on purpose. Only 2026 rosters are synced today (15,909
people), but the backfill reaches 2009, and a design that only works at one
season's scale would have to be rebuilt -- along with every URL it had already
published -- the moment that ran.

Files are `window.__CFB_PEOPLE__...=` scripts rather than bare JSON for the same
reason as team_pages.js: browsers block fetch() of local JSON when
dashboard.html is opened from disk, while a <script src> works there and on
GitHub Pages.

Every value is copied from the person tables; nothing is recomputed here. A
missing bio field stays absent rather than becoming a zero or an "N/A" -- the
page omits what it does not know.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import person_identity as identity  # noqa: E402
from person_identity import normalize_name  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "ui" / "data" / "people"
# Chosen against the real archive, not guessed: 99,813 people across 2009-2026
# make a 16-shard file 3.0 MB, which is what a reader would download to open one
# player page. 64 shards puts it near 750 KB. The page reads this number from
# the manifest it emits, so it can be raised again as seasons accumulate without
# touching the UI -- which is why the layout was sharded in the first place.
PLAYER_SHARDS = 64
# Where a coach's signees came from, for the map on his page. All 573 coaches'
# points in one file is 710 KB, which is not a thing to load to draw one map;
# 32 shards puts a coach page at about 20 KB. Sharded on coach_id the same way
# players are, with the same floored modulo on both sides.
RECRUIT_SHARDS = 32


def shard_key(name: str) -> str:
    """The first character of the normalized name, folded to a safe filename."""
    return _fold_initial(normalize_name(name)[:1])


def _fold_initial(first: str) -> str:
    if not first:
        return "_"
    if "a" <= first <= "z":
        return first
    return "0" if first.isdigit() else "_"


def shard_keys(name: str) -> list[str]:
    """Every shard a name belongs in: the first letter of each of its words.

    Sharding on the full name alone put Cade Klubnik in `c` and nothing else, so
    searching "Klubnik" -- which is how anyone looks for a player -- loaded shard
    `k` and found nobody. A person is listed under each word of their name, which
    costs roughly twice the index and makes a surname searchable.
    """
    words = [w for w in normalize_name(name).split(" ") if w]
    keys = {_fold_initial(w[0]) for w in words}
    return sorted(keys) or ["_"]


def _row_dict(row: sqlite3.Row) -> dict:
    """Only the keys that actually have a value. A page omits what it lacks."""
    return {k: row[k] for k in row.keys() if row[k] is not None}


def build_players(conn: sqlite3.Connection) -> dict:
    conn.row_factory = sqlite3.Row
    players: dict[int, dict] = {}
    for row in conn.execute(
            "SELECT player_id, display_name, first_name, last_name, primary_position, "
            "hometown, home_state, height, weight, headshot_path, headshot_url, latest_season "
            "FROM players ORDER BY player_id"):
        player = _row_dict(row)
        player["seasons"] = []
        players[int(row["player_id"])] = player

    for row in conn.execute(
            "SELECT s.player_id, s.season_year, s.team_id, s.jersey, s.position, s.class_year, "
            "s.height, s.weight FROM player_team_seasons s ORDER BY s.player_id, s.season_year, s.team_id"):
        player = players.get(int(row["player_id"]))
        if player is None:
            continue
        season = _row_dict(row)
        season.pop("player_id", None)
        player["seasons"].append(season)

    # A player's own season statistics, grouped by season and category so the
    # page can render one table per category without re-sorting 85,768 rows in
    # the browser. The stat types stay the feed's own labels: renaming YDS to
    # "Yards" here would be this project asserting a reading of a statistic it
    # did not compute.
    for row in conn.execute(
            "SELECT player_id, season_year, team_id, category, stat_type, stat "
            "FROM player_season_stats ORDER BY season_year DESC, category, stat_type"):
        player = players.get(int(row["player_id"]))
        if player is None:
            continue
        seasons = player.setdefault("stats", {})
        season = seasons.setdefault(str(row["season_year"]), {})
        category = season.setdefault(row["category"], {"team_id": int(row["team_id"])})
        category[row["stat_type"]] = row["stat"]

    # A season the source itself holds more of than this page can show. Kept
    # beside the statistics rather than on the player, because it is true of one
    # season and not of the player's career.
    for row in conn.execute(
            "SELECT player_id, season_year, reason FROM player_season_stat_caveats"):
        player = players.get(int(row["player_id"]))
        if player is None:
            continue
        season = player.setdefault("stats", {}).get(str(row["season_year"]))
        if season is not None:
            season["_partial"] = row["reason"]

    # The identity the feed could not vouch for, carried through to the page so
    # the weakness is visible to a reader rather than only to the database --
    # the same treatment a name-only coach identity gets.
    for player_id, (_, reason, _first) in identity.implausible_careers(conn).items():
        player = players.get(player_id)
        if player is not None:
            player["identity_note"] = reason

    # A person with no season row is not displayable: the roster that justified
    # them is gone, so the page would have nothing true to show.
    displayable = {pid: p for pid, p in players.items() if p["seasons"]}

    # The counterpart page, for a person the source recorded under two athlete
    # ids where a human has read both records and judged them one person. The
    # statistics are NOT fused: each id keeps the totals the source attached to
    # it, and each page says where the rest of the career is. Summing them would
    # invent numbers -- YPC and PCT do not add -- and merging the ids would
    # break both pages' URLs.
    #
    # Attached after the displayable filter, and only where the counterpart
    # survived it. A link to a page that was dropped would land a reader on
    # "player not found" -- the same defect the box-score exporter was fixed for.
    for player_id, link in identity.linked_careers(conn).items():
        player = displayable.get(player_id)
        if player is not None and link["id"] in displayable:
            player["same_person"] = link

    return displayable


def build_coaches(conn: sqlite3.Connection) -> dict:
    conn.row_factory = sqlite3.Row
    coaches: dict[int, dict] = {}
    for row in conn.execute(
            "SELECT coach_id, display_name, first_name, last_name, headshot_path, headshot_url, "
            "latest_season FROM coaches ORDER BY coach_id"):
        coach = _row_dict(row)
        coach["tenures"] = []
        coaches[int(row["coach_id"])] = coach

    for row in conn.execute(
            "SELECT coach_id, team_id, season_year, role, hire_date, games, wins, losses, ties, "
            "preseason_rank, postseason_rank FROM coach_tenures ORDER BY coach_id, season_year, team_id"):
        coach = coaches.get(int(row["coach_id"]))
        if coach is None:
            continue
        tenure = _row_dict(row)
        tenure.pop("coach_id", None)
        coach["tenures"].append(tenure)

    # What each season was worth against the Elo expectation
    # (src/build_coach_metrics.py). Attached to the tenure it measures, and the
    # unmeasured seasons carry their reason instead of a number, so the page can
    # say a season is not attributable rather than leaving a blank cell.
    if conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' "
                    "AND name = 'coach_season_metrics'").fetchone():
        measured: dict = {}
        for row in conn.execute(
                "SELECT coach_id, team_id, season_year, attributed, reason, "
                "expected_wins, wins_above_expected, elo_change, season_coe2 "
                "FROM coach_season_metrics"):
            measured[(int(row["coach_id"]), int(row["team_id"]),
                      int(row["season_year"]))] = row
        for coach_id, coach in coaches.items():
            for tenure in coach["tenures"]:
                row = measured.get((coach_id, tenure.get("team_id"),
                                    tenure.get("season_year")))
                if row is None:
                    continue
                if not row["attributed"]:
                    tenure["unmeasured"] = row["reason"]
                    continue
                for key in ("expected_wins", "wins_above_expected", "elo_change",
                            "season_coe2"):
                    if row[key] is not None:
                        tenure[key] = round(row[key], 3)

    # The recruiting class the program signed that season
    # (src/load_recruiting.py). The join CFBD's feed supports is team and
    # season, which attributes a class to whoever held the job that year
    # rather than to whoever did the recruiting -- and in a coach's first
    # season at a school those are usually different people. Saban's 2000 LSU
    # class, signed three weeks after he was hired, ranked 21st; his next four
    # ranked 2nd, 21st, 3rd and 4th. So a first season is MARKED rather than
    # quietly averaged in, and the page says what the mark means.
    if conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' "
                    "AND name = 'recruiting_classes'").fetchone():
        classes = {}
        for row in conn.execute(
                "SELECT team_id, season_year, class_rank, points FROM recruiting_classes "
                "WHERE team_id IS NOT NULL"):
            classes[(int(row["team_id"]), int(row["season_year"]))] = row
        # Who held each job each season, so "this coach's first season here"
        # can be told from "this coach was already here". A coach who returns
        # years later (Mack Brown at North Carolina) inherits again, which is
        # why this asks about the season before rather than about a minimum.
        held = {}
        for coach_id, coach in coaches.items():
            for tenure in coach["tenures"]:
                held[(tenure.get("team_id"), tenure.get("season_year"))] = coach_id
        for coach_id, coach in coaches.items():
            for tenure in coach["tenures"]:
                team_id = tenure.get("team_id")
                season = tenure.get("season_year")
                row = classes.get((team_id, season))
                if row is None:
                    continue
                if row["class_rank"] is not None:
                    tenure["class_rank"] = int(row["class_rank"])
                if row["points"] is not None:
                    tenure["class_points"] = round(float(row["points"]), 2)
                if held.get((team_id, season - 1)) != coach_id:
                    tenure["class_inherited"] = True

    # The identity this feed could not vouch for, carried through to the page so
    # the weakness is visible to a reader rather than only to the database.
    for row in conn.execute(
            "SELECT display_name, reason FROM person_unresolved WHERE entity = 'coach' "
            "AND reason LIKE 'name-only%'"):
        for coach in coaches.values():
            if coach["display_name"] == row["display_name"]:
                coach["identity_note"] = row["reason"]
    return {cid: c for cid, c in coaches.items() if c["tenures"]}


ROSTER_FIELDS = ["player_id", "name", "jersey", "position", "class_year", "height", "weight"]


def build_rosters(players: dict) -> dict:
    """season -> {fields, teams: {team_id: [row, ...]}} for the team page module.

    The rows carry what a roster table displays rather than ids alone. Ids alone
    would make a 115-name roster depend on all PLAYER_SHARDS detail files --
    every player in the country, 6 MB of them, to show one team. One season file
    is a few hundred KB and answers the question on its own.

    A row is a list, not an object, because the key names would otherwise be
    repeated 15,909 times in the file.
    """
    rosters: dict[int, dict[int, list]] = defaultdict(lambda: defaultdict(list))
    for pid, player in players.items():
        for season in player["seasons"]:
            rosters[season["season_year"]][season["team_id"]].append([
                pid, player["display_name"], season.get("jersey"),
                season.get("position") or player.get("primary_position"),
                season.get("class_year"),  # already a label; load_rosters folds it
                season.get("height") or player.get("height"),
                season.get("weight") or player.get("weight")])
    # Sorted by jersey where there is one, then by name, which is the order a
    # roster is published in. A missing jersey sorts last rather than first.
    def order(row):
        jersey = str(row[2] or "")
        digits = int(jersey) if jersey.isdigit() else 10 ** 6
        return (digits, jersey, normalize_name(row[1]))
    return {season: {"fields": ROSTER_FIELDS,
                     "teams": {str(team): sorted(rows, key=order)
                               for team, rows in teams.items()}}
            for season, teams in rosters.items()}


def build_team_coaches(coaches: dict) -> dict:
    """season -> team_id -> [coach_id, display_name], for a team page's header.

    Its own small file. A team header asking coaches.js for one name would pull
    every coach's whole career -- 800 KB for a line of text -- and it would do
    it on every team page.
    """
    by_season: dict[int, dict[str, list]] = defaultdict(dict)
    for cid, coach in coaches.items():
        for tenure in coach["tenures"]:
            by_season[tenure["season_year"]][str(tenure["team_id"])] = [cid, coach["display_name"]]
    return {season: teams for season, teams in sorted(by_season.items())}


def build_name_index(players: dict, coaches: dict) -> dict:
    """shard -> [[kind, id, display_name, latest_team_id, position_or_role], ...]

    kind is 'p' or 'c' so a result can be badged by entity type without a second
    lookup, which is what the guide asks of search. A person appears in one shard
    per word of their name, so a search for a surname reaches them -- see
    shard_keys.
    """
    shards: dict[str, list] = defaultdict(list)

    def add(row, name):
        for key in shard_keys(name):
            shards[key].append(row)

    for pid, player in players.items():
        last = player["seasons"][-1]
        add(["p", pid, player["display_name"], last.get("team_id"),
             last.get("position") or player.get("primary_position")],
            player["display_name"])
    for cid, coach in coaches.items():
        last = coach["tenures"][-1]
        add(["c", cid, coach["display_name"], last.get("team_id"), "head coach"],
            coach["display_name"])
    for rows in shards.values():
        rows.sort(key=lambda r: (normalize_name(r[2]), r[1]))
    return shards


def _write(path: Path, variable: str, payload) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(variable + "=" + json.dumps(payload, separators=(",", ":")) + ";\n",
                    encoding="utf-8")
    return hashlib.sha256(path.read_bytes()).hexdigest()[:10]


def build_coach_recruits(conn: sqlite3.Connection, coaches: dict) -> dict:
    """coach_id -> {"points": [[lat, lon, count, label], ...], "unlocated": n}.

    Only the classes he SIGNED. A first season at a school was recruited by his
    predecessor, and a map is a claim about where a coach goes for players --
    putting someone else's class on it would be the wrong claim in the most
    visible possible place. Ownership is worked out here from who held the job
    the season before, NOT from the class_inherited flag build_coaches sets:
    that flag only exists where the archive also has a class RANK, and FIU's
    2022 class has 17 located recruits and no ranking row, which would have put
    Butch Davis's last class on Mike MacIntyre's map.

    Recruits are grouped by hometown rather than listed one per point, because
    a town that sent a coach eleven players is the thing the map is for, and
    eleven circles drawn on top of each other does not show it. A signee the
    feed gave no coordinates for (466 of them, across 195 coaches) is counted
    in "unlocated" rather than dropped, so the page's total is his whole class.
    """
    if not conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' "
                        "AND name = 'recruits'").fetchone():
        return {}
    held: dict[tuple, int] = {}
    for coach_id, coach in coaches.items():
        for tenure in coach["tenures"]:
            held[(tenure.get("team_id"), tenure.get("season_year"))] = coach_id
    # (team, season) -> the coach who signed that class, which is the coach who
    # already held the job the season before. A coach who returns years later
    # (Mack Brown at North Carolina) inherits again, which is why this asks
    # about the season before rather than about a minimum.
    signed = {(team, season): coach_id for (team, season), coach_id in held.items()
              if held.get((team, season - 1)) == coach_id}

    grouped: dict = defaultdict(lambda: defaultdict(int))
    unlocated: dict = defaultdict(int)
    # Labels are kept per coach, not per point: two towns can round to one
    # point (the feed has a "Hampton, SC" at Hampton, Virginia's coordinates),
    # and a shared cache would then label one coach's town with another
    # coach's recruit's spelling of somewhere else.
    labels: dict = defaultdict(dict)
    for row in conn.execute(
            "SELECT committed_team_id, season_year, hometown_latitude, "
            "hometown_longitude, city, state_province FROM recruits "
            "WHERE committed_team_id IS NOT NULL"):
        coach_id = signed.get((int(row["committed_team_id"]), int(row["season_year"])))
        if coach_id is None:
            continue
        if row["hometown_latitude"] is None or row["hometown_longitude"] is None:
            unlocated[coach_id] += 1
            continue
        # Two decimal places is about a kilometre, which is the same town and
        # not the same street -- enough to merge a city without merging two.
        point = (round(float(row["hometown_latitude"]), 2),
                 round(float(row["hometown_longitude"]), 2))
        grouped[coach_id][point] += 1
        labels[coach_id].setdefault(
            point, ", ".join(p for p in (row["city"], row["state_province"]) if p))
    return {coach_id: {"points": sorted(([lat, lon, count, labels[coach_id].get((lat, lon), "")]
                                         for (lat, lon), count in grouped[coach_id].items()),
                                        key=lambda p: (-p[2], p[3])),
                       "unlocated": unlocated.get(coach_id, 0)}
            for coach_id in set(grouped) | set(unlocated)}


def export(conn: sqlite3.Connection, out_dir: Path = OUT_DIR) -> dict:
    players = build_players(conn)
    coaches = build_coaches(conn)
    rosters = build_rosters(players)
    name_index = build_name_index(players, coaches)

    recruits = build_coach_recruits(conn, coaches)

    manifest: dict = {"player_shards": PLAYER_SHARDS, "recruit_shards": RECRUIT_SHARDS,
                      "index": {}, "players": {}, "coach_recruits": {},
                      "rosters": {}, "counts": {}}

    sharded: dict[int, dict] = defaultdict(dict)
    for pid, player in players.items():
        sharded[pid % PLAYER_SHARDS][str(pid)] = player
    for shard in range(PLAYER_SHARDS):
        version = _write(out_dir / f"player_{shard}.js",
                         f"(window.__CFB_PEOPLE_PLAYERS__=window.__CFB_PEOPLE_PLAYERS__||{{}})[{shard}]",
                         sharded.get(shard, {}))
        manifest["players"][str(shard)] = f"data/people/player_{shard}.js?v={version}"

    for key, rows in sorted(name_index.items()):
        version = _write(out_dir / f"index_{key}.js",
                         f"(window.__CFB_PEOPLE_INDEX__=window.__CFB_PEOPLE_INDEX__||{{}})"
                         f"[{json.dumps(key)}]", rows)
        manifest["index"][key] = f"data/people/index_{key}.js?v={version}"

    for season, teams in sorted(rosters.items()):
        version = _write(out_dir / f"roster_{season}.js",
                         f"(window.__CFB_PEOPLE_ROSTERS__=window.__CFB_PEOPLE_ROSTERS__||{{}})[{season}]",
                         teams)
        manifest["rosters"][str(season)] = f"data/people/roster_{season}.js?v={version}"

    by_shard: dict[int, dict] = defaultdict(dict)
    for coach_id, points in recruits.items():
        by_shard[coach_id % RECRUIT_SHARDS][str(coach_id)] = points
    for shard in range(RECRUIT_SHARDS):
        version = _write(
            out_dir / f"coach_recruits_{shard}.js",
            f"(window.__CFB_PEOPLE_COACH_RECRUITS__=window.__CFB_PEOPLE_COACH_RECRUITS__||{{}})[{shard}]",
            by_shard.get(shard, {}))
        manifest["coach_recruits"][str(shard)] = \
            f"data/people/coach_recruits_{shard}.js?v={version}"

    version = _write(out_dir / "coaches.js", "window.__CFB_PEOPLE_COACHES__", coaches)
    manifest["coaches"] = f"data/people/coaches.js?v={version}"

    version = _write(out_dir / "team_coaches.js", "window.__CFB_PEOPLE_TEAM_COACHES__",
                     build_team_coaches(coaches))
    manifest["team_coaches"] = f"data/people/team_coaches.js?v={version}"
    manifest["counts"] = {"players": len(players), "coaches": len(coaches),
                          "player_seasons": sum(len(p["seasons"]) for p in players.values()),
                          "coach_seasons": sum(len(c["tenures"]) for c in coaches.values()),
                          "coach_recruit_points": sum(len(v["points"]) for v in recruits.values()),
                          "coach_recruits_unlocated": sum(v["unlocated"] for v in recruits.values()),
                          "seasons": sorted(rosters),
                          "stat_seasons": sorted(
                              {int(y) for p in players.values() for y in p.get("stats", {})},
                              reverse=True)}
    return manifest


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--out", default=str(OUT_DIR))
    a = p.parse_args()
    conn = sqlite3.connect(a.db)
    manifest = export(conn, Path(a.out))
    conn.close()
    counts = manifest["counts"]
    print(f"Wrote {a.out}: {counts['players']} players ({counts['player_seasons']} player-seasons), "
          f"{counts['coaches']} coaches ({counts['coach_seasons']} coach-seasons), "
          f"{len(manifest['index'])} name shards, seasons {counts['seasons'] or 'none'}")


if __name__ == "__main__":
    main()
