#!/usr/bin/env python3
"""
Point-in-time Elo standings and week-to-week movement (P1-05, P1-06).

The site could show a season's FINAL Elo and a team's own rating history, but
not "where did the table stand in week 8?" or "who moved, and how far?". This
module answers both from the engine's own audit trail. Nothing is recalculated:
every rating here is a `postgame_elo` that `build_elo.py` wrote, or a
`pregame_elo` for the preseason snapshot.

## Stages

A season is cut into stages, oldest first:

    pre       Preseason -- each team's rating ENTERING the season, which is the
              pregame_elo of its first game. Not 1500 for most teams: Elo carries
              over with the configured offseason retention, so this snapshot is
              the carry-in, and week 1's movement is measured against it.
    w<N>      one stage per regular-season week the season actually has
    post      every bowl and CFP game, as one stage

Postseason is one stage on purpose. `games.week` restarts at 1 for the bowl and
cfp phases, and the two phases INTERLEAVE in wall-clock time -- in 2024 bowls run
2024-12-15 to 2025-01-04 while the CFP runs 2024-12-21 to 2025-01-21 -- so
"after the bowls" is not a moment in time at all. One postseason stage is the
honest cut, and its snapshot is the season's final table.

## What a snapshot means

The rating shown for a team at stage k is the postgame Elo of the LAST game that
team played among stages 1..k, taken straight from `elo_game_history`. Because
each of those numbers was computed by walking every game in strict chronological
order, a snapshot is a real historical state, not a re-simulation of a subset.

One wrinkle worth knowing rather than hiding: stage membership follows the week
and phase labels, not the calendar, and in a few seasons a late regular-season
week is played after the first bowls (1998: week 15 on 12-05, bowls from 12-04).
So "as of week 15" can include a game played a day after a bowl game that lands
in the postseason stage. The alternative -- cutting by date and calling the
result "week 15" -- would be less honest, not more.

## Movement

`d_elo` and `d_rank` compare a team against its own previous stage. They are
computed from the ROUNDED ratings the site displays, so a column of movements
always reconciles with the ratings beside it rather than being off by a tenth.
Both are None when there is no previous stage, or when the team has no rating in
it -- never 0, which would claim the team stood still.

`d_rank` is reported the way a reader sees movement: positive means the team
climbed (rank 12 -> rank 8 is +4), even though the rank number went down.

`stage_games` is how many games the team played IN that stage, which is what
separates a real 0.0 from a bye week. A team on a bye keeps its rating, so its
movement is genuinely 0.0 -- but a reader should be told it did not play rather
than left to infer that a game came out exactly even.

Ranks are shared on a tie (two teams at the same rating are both 4th, and the
next team is 6th), the same convention build_coe2_rollups.conference_coe2_rank
uses.

## Poll tracks

`payload["polls"]` carries the same shape for the AP and CFP polls, so a reader
can follow a team through the human ballots beside the model. Their columns are
the poll's OWN releases, never the Elo stages above: a poll released in week N
is voted on games through week N-1, and the two are not the same moment. Lining
them up in one row of columns would assert an alignment the data does not have,
so each track keeps its own column set and says what it is.

A poll ranks 25 teams, so most cells are empty by design. `d_rank` compares a
team against the previous RELEASE of that same poll and is None when the team
was unranked in it -- a team entering the poll at 23 did not climb from 26.
Poll rows carry both the team index (null when CFBD's school name never matched
a canonical team) and the raw school name, so a track never shows a gap.
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict

# Positional rows with the field list shipped alongside, appended-last on change,
# matching every other payload the site loads (see src/export_static_data.py).
STAGE_FIELDS = ["key", "label", "phase", "week", "first_date", "last_date", "games"]
ROW_FIELDS = ["team", "elo", "rank", "d_elo", "d_rank", "played", "stage_games"]
POLLS = ("ap", "cfp")
POLL_RELEASE_FIELDS = ["key", "label", "season_type", "week"]
POLL_ROW_FIELDS = ["team", "school", "rank", "d_rank"]

PRESEASON_KEY = "pre"
POSTSEASON_KEY = "post"
POSTSEASON_PHASES = ("bowl", "cfp")


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return bool(conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


def stage_key(phase: str, week) -> str:
    """Which stage a game belongs to. The only place that mapping is defined."""
    return POSTSEASON_KEY if phase in POSTSEASON_PHASES else f"w{int(week)}"


def stage_label(key: str) -> str:
    if key == PRESEASON_KEY:
        return "Preseason"
    if key == POSTSEASON_KEY:
        return "Postseason"
    return f"Week {key[1:]}"


def poll_release_key(season_type: str, week) -> str:
    """Which release a poll row belongs to. The only place that mapping lives."""
    return ("p" if season_type == "postseason" else "r") + str(int(week))


def poll_release_label(season_type: str, week) -> str:
    # The postseason release IS the final poll, whatever week number CFBD gave
    # it, so it is named for what it is rather than for its number.
    return "Final" if season_type == "postseason" else f"Week {int(week)}"


def poll_timeline(conn: sqlite3.Connection, season: int, index_of: dict[int, int]) -> dict:
    """
    {poll: {"releases": [...], "rows": [[...], ...]}} for the polls that have a
    release in `season`. A poll with none is absent rather than present and
    empty, so the page can tell "never released" from "released and empty".
    """
    if not _table_exists(conn, "poll_rankings"):
        return {}
    tracks: dict[str, dict] = {}
    for poll in POLLS:
        # Postseason sorts after every regular-season week, the ordering
        # export_dashboard_data.build_polls already uses to find "the latest".
        releases = conn.execute(
            """SELECT season_type, week FROM poll_rankings
               WHERE season_year = ? AND poll = ?
               GROUP BY season_type, week
               ORDER BY (season_type = 'postseason'), week""",
            (season, poll),
        ).fetchall()
        if not releases:
            continue
        meta, snapshots = [], []
        previous: dict = {}
        for season_type, week in releases:
            rows = conn.execute(
                """SELECT rank, team_id, school FROM poll_rankings
                   WHERE season_year = ? AND poll = ? AND season_type = ? AND week = ?
                   ORDER BY rank, school""",
                (season, poll, season_type, week),
            ).fetchall()
            rows_out, now = [], {}
            for rank, team_id, school in rows:
                # An unmatched school is still a row, keyed by its raw name so
                # its own movement is still measured across releases.
                identity = team_id if team_id is not None else f"school:{school}"
                before = previous.get(identity)
                rows_out.append([index_of.get(team_id), school, rank,
                                 None if before is None else before - rank])
                now[identity] = rank
            previous = now
            meta.append([poll_release_key(season_type, week),
                         poll_release_label(season_type, week), season_type, int(week)])
            snapshots.append(rows_out)
        tracks[poll] = {"releases": meta, "rows": snapshots}
    return tracks


def ranks_for(values: list[float]) -> list[int]:
    """
    Competition ranks for values already sorted strongest first: ties share the
    better rank and the next distinct value skips the ranks they used up.
    """
    out: list[int] = []
    for i, value in enumerate(values):
        out.append(out[-1] if i and value == values[i - 1] else i + 1)
    return out


def _fetch_games(conn: sqlite3.Connection, season: int | None) -> list[sqlite3.Row]:
    """
    Completed games with their Elo rows, in the engine's own order: the same
    (season, game_date, game_id) ordering build_elo.py walks, so "the last game
    so far" means the same thing here as it does there.
    """
    where = "g.home_score IS NOT NULL AND g.away_score IS NOT NULL"
    params: tuple = ()
    if season is not None:
        where += " AND g.season_year = ?"
        params = (season,)
    # Its own cursor with its own row_factory: callers pass connections configured
    # either way (export_static_data.py does not set one), and this module reads
    # its rows by name.
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row
    return cur.execute(
        f"""
        SELECT g.season_year, g.game_id, g.game_date, g.week, g.game_phase,
               e.team_id, e.pregame_elo, e.postgame_elo
        FROM games g
        JOIN elo_game_history e ON e.game_id = g.game_id
        WHERE {where}
        ORDER BY g.season_year, g.game_date, g.game_id
        """,
        params,
    ).fetchall()


def build_timeline(conn: sqlite3.Connection, season: int | None = None) -> dict:
    """
    {season: payload}. A season with no Elo rows is absent rather than present
    and empty, so a caller can tell "not built yet" from "built and empty".
    """
    if not (_table_exists(conn, "elo_game_history") and _table_exists(conn, "games")):
        return {}

    names = {tid: name for tid, name in conn.execute("SELECT team_id, team_name FROM teams")}
    conferences = _memberships(conn)

    by_season: dict[int, list[sqlite3.Row]] = defaultdict(list)
    for row in _fetch_games(conn, season):
        by_season[row["season_year"]].append(row)

    return {year: _season_payload(year, rows, names, conn, conferences.get(year, {}))
            for year, rows in sorted(by_season.items())}


def _memberships(conn: sqlite3.Connection) -> dict[int, dict[int, str]]:
    """
    {season: {team_id: conference}} from THAT season's membership, never today's,
    the same rule build_conference_elo_by_year follows. Empty when the table is
    not built: a conference filter is worth having and not worth failing over.
    """
    if not _table_exists(conn, "team_membership_by_season"):
        return {}
    out: dict[int, dict[int, str]] = defaultdict(dict)
    for year, tid, conference in conn.execute(
            """SELECT season_year, team_id, conference_real FROM team_membership_by_season
               WHERE conference_real IS NOT NULL"""):
        out[int(year)][tid] = conference
    return out


def _season_payload(season: int, rows: list[sqlite3.Row], names: dict,
                    conn: sqlite3.Connection | None = None,
                    conferences: dict[int, str] | None = None) -> dict:
    # Stage order: the regular-season weeks the season actually has, by week
    # number, then the postseason if it has one. Never a fixed 1..15 list -- a
    # season that played 14 weeks should not show an empty 15th.
    weeks = sorted({int(r["week"]) for r in rows if r["game_phase"] not in POSTSEASON_PHASES})
    keys = [PRESEASON_KEY] + [f"w{w}" for w in weeks]
    if any(r["game_phase"] in POSTSEASON_PHASES for r in rows):
        keys.append(POSTSEASON_KEY)

    entering: dict[int, float] = {}     # team -> rating carried into the season
    per_stage: dict[str, list[sqlite3.Row]] = defaultdict(list)
    meta: dict[str, dict] = {}
    for row in rows:                                  # already in chronological order
        team = row["team_id"]
        if team not in entering:
            entering[team] = row["pregame_elo"]
        key = stage_key(row["game_phase"], row["week"])
        per_stage[key].append(row)
        info = meta.setdefault(key, {"first": row["game_date"], "last": row["game_date"],
                                     "games": set(), "phase": row["game_phase"],
                                     "week": None if key == POSTSEASON_KEY else int(row["week"])})
        info["last"] = max(info["last"], row["game_date"])
        info["first"] = min(info["first"], row["game_date"])
        info["games"].add(row["game_id"])

    teams = sorted(entering, key=lambda tid: names.get(tid, str(tid)))
    index_of = {tid: i for i, tid in enumerate(teams)}

    current = dict(entering)
    played: dict[int, int] = defaultdict(int)
    in_stage: dict[int, int] = defaultdict(int)
    previous: dict[int, tuple[float, int]] = {}        # team -> (rating, rank) last stage
    stages, snapshots = [], []

    for key in keys:
        in_stage.clear()
        if key != PRESEASON_KEY:
            for row in per_stage[key]:                # chronological within the stage
                current[row["team_id"]] = row["postgame_elo"]
                played[row["team_id"]] += 1
                in_stage[row["team_id"]] += 1

        # Rounded first, then everything else derives from what the site shows.
        ordered = sorted(((round(current[tid], 1), tid) for tid in current),
                         key=lambda pair: (-pair[0], names.get(pair[1], "")))
        rank_of = ranks_for([elo for elo, _ in ordered])

        rows_out, now = [], {}
        for (elo, tid), rank in zip(ordered, rank_of):
            before = previous.get(tid)
            rows_out.append([index_of[tid], elo, rank,
                             None if before is None else round(elo - before[0], 1),
                             None if before is None else before[1] - rank,
                             played[tid], in_stage[tid]])
            now[tid] = (elo, rank)
        previous = now

        info = meta.get(key, {})
        stages.append([key, stage_label(key),
                       "preseason" if key == PRESEASON_KEY
                       else "postseason" if key == POSTSEASON_KEY else info.get("phase", ""),
                       info.get("week"),
                       info.get("first"), info.get("last"),
                       len(info.get("games", ()))])
        snapshots.append(rows_out)

    payload = {
        "season": season,
        # Third element appended last, and None where the season has no
        # membership row for the team: a reader that destructures [id, name]
        # ignores it (see loadEloTimeline in ui/dashboard_shell.html).
        "teams": [[tid, names.get(tid, str(tid)), (conferences or {}).get(tid)] for tid in teams],
        "stage_fields": STAGE_FIELDS,
        "stages": stages,
        "row_fields": ROW_FIELDS,
        "rows": snapshots,
    }
    # Appended last, and absent when no poll ever released in this season: the
    # payload is positional rows plus a field list, so a reader built before the
    # poll tracks existed keeps working (see src/export_static_data.py).
    tracks = poll_timeline(conn, season, index_of) if conn is not None else {}
    if tracks:
        payload["poll_release_fields"] = POLL_RELEASE_FIELDS
        payload["poll_row_fields"] = POLL_ROW_FIELDS
        payload["polls"] = tracks
    return payload
