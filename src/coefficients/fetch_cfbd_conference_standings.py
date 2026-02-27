#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple


CFBD_BASE_URL = "https://api.collegefootballdata.com"
DEFAULT_SOURCE = "cfbd_records_heuristic"
DEFAULT_SOURCE_DETAIL = "GET /records (ranked by conf_wpct, conf_wins, overall_wpct, overall_wins)"


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def resolve_team_id(conn: sqlite3.Connection, raw_team: str) -> Optional[int]:
    """
    Resolve CFBD team name to teams.team_id using:
      1) exact match on teams.team_name
      2) team_aliases(alias -> team_name canonical) then lookup teams.team_name
    """
    raw_team = (raw_team or "").strip()
    if not raw_team:
        return None

    row = conn.execute(
        "SELECT team_id FROM teams WHERE team_name = ?",
        (raw_team,),
    ).fetchone()
    if row:
        return int(row["team_id"])

    alias_row = conn.execute(
        "SELECT team_name FROM team_aliases WHERE alias = ?",
        (raw_team,),
    ).fetchone()
    if alias_row:
        canonical = alias_row["team_name"]
        row2 = conn.execute(
            "SELECT team_id FROM teams WHERE team_name = ?",
            (canonical,),
        ).fetchone()
        if row2:
            return int(row2["team_id"])

    return None


def cfbd_get_json(path: str, params: Dict[str, Any], api_key: str) -> Any:
    """
    GET JSON from CFBD REST API using Bearer token.
    """
    qs = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    url = f"{CFBD_BASE_URL}{path}?{qs}" if qs else f"{CFBD_BASE_URL}{path}"

    req = urllib.request.Request(url)
    req.add_header("Accept", "application/json")
    req.add_header("Authorization", f"Bearer {api_key}")

    with urllib.request.urlopen(req) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body)


def get_valid_conferences(conn: sqlite3.Connection, season_year: int) -> set[str]:
    """
    Return the set of conferences that exist in OUR DB for this season among FBS teams.
    This lets us filter CFBD records down to just the conference universe we care about.
    """
    rows = conn.execute(
        """
        SELECT DISTINCT conference_real AS conference
        FROM team_membership_by_season
        WHERE season_year = ?
          AND is_fbs = 1
          AND conference_real IS NOT NULL
        """,
        (season_year,),
    ).fetchall()
    return {r["conference"] for r in rows if r["conference"]}


def _safe_div(n: float, d: float) -> float:
    return (n / d) if d else 0.0


def record_fields(r: Dict[str, Any]) -> Tuple[int, int, int, float, int, int, int, float]:
    """
    Extract:
      conf_w, conf_l, conf_t, conf_wpct,
      overall_w, overall_l, overall_t, overall_wpct
    """
    cg = r.get("conferenceGames") or {}
    tg = r.get("total") or {}

    conf_w = int(cg.get("wins") or 0)
    conf_l = int(cg.get("losses") or 0)
    conf_t = int(cg.get("ties") or 0)
    conf_g = conf_w + conf_l + conf_t
    conf_wpct = _safe_div(conf_w + 0.5 * conf_t, conf_g)

    overall_w = int(tg.get("wins") or 0)
    overall_l = int(tg.get("losses") or 0)
    overall_t = int(tg.get("ties") or 0)
    overall_g = overall_w + overall_l + overall_t
    overall_wpct = _safe_div(overall_w + 0.5 * overall_t, overall_g)

    return conf_w, conf_l, conf_t, conf_wpct, overall_w, overall_l, overall_t, overall_wpct


def build_conference_rankings(records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Group records by conference and sort within each conference.
    Sorting heuristic (deterministic):
      1) conference win% desc
      2) conference wins desc
      3) conference losses asc
      4) overall win% desc
      5) overall wins desc
      6) team name asc

    NOTE: This is NOT a perfect recreation of each league's tiebreak rules;
    it is a stable proxy ordering derived from CFBD records.
    """
    by_conf: Dict[str, List[Dict[str, Any]]] = {}

    for r in records:
        conf = (r.get("conference") or "").strip()
        team = (r.get("team") or "").strip()
        if not conf or not team:
            continue

        conf_w, conf_l, conf_t, conf_wpct, ow, ol, ot, owpct = record_fields(r)

        enriched = dict(r)
        enriched["_conf_w"] = conf_w
        enriched["_conf_l"] = conf_l
        enriched["_conf_t"] = conf_t
        enriched["_conf_wpct"] = conf_wpct
        enriched["_overall_w"] = ow
        enriched["_overall_l"] = ol
        enriched["_overall_t"] = ot
        enriched["_overall_wpct"] = owpct

        by_conf.setdefault(conf, []).append(enriched)

    for conf, lst in by_conf.items():
        lst.sort(
            key=lambda x: (
                -float(x["_conf_wpct"]),
                -int(x["_conf_w"]),
                int(x["_conf_l"]),
                -float(x["_overall_wpct"]),
                -int(x["_overall_w"]),
                str(x.get("team") or ""),
            )
        )

    return by_conf


def write_standings(
    conn: sqlite3.Connection,
    season_year: int,
    standings_by_conf: Dict[str, List[Dict[str, Any]]],
    source: str,
    source_detail: str,
    delete_existing: bool,
    print_skips: bool = False,
) -> Tuple[int, int]:
    """
    Insert into conference_standings_by_year.
    Returns (inserted_rows, skipped_unresolved_teams).
    """
    if delete_existing:
        conn.execute("DELETE FROM conference_standings_by_year WHERE season_year=?", (season_year,))

    inserted = 0
    skipped = 0

    insert_sql = """
    INSERT OR REPLACE INTO conference_standings_by_year
      (season_year, conference, team_id, conf_rank, source, source_detail)
    VALUES (?, ?, ?, ?, ?, ?)
    """

    for conf, teams in standings_by_conf.items():
        rank = 1
        for r in teams:
            team_name = (r.get("team") or "").strip()
            team_id = resolve_team_id(conn, team_name)
            if team_id is None:
                skipped += 1
                if print_skips:
                    print(f"[SKIP] Unresolved team: {team_name} (conference={conf}, year={season_year})", file=sys.stderr)
                continue

            conn.execute(
                insert_sql,
                (season_year, conf, team_id, rank, source, source_detail),
            )
            inserted += 1
            rank += 1

    return inserted, skipped


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    p.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help="Provenance label stored in conference_standings_by_year.source",
    )
    p.add_argument(
        "--source-detail",
        default=DEFAULT_SOURCE_DETAIL,
        help="Longer provenance string stored in conference_standings_by_year.source_detail",
    )
    p.add_argument(
        "--keep-existing",
        action="store_true",
        help="Do not delete existing standings rows for this year before inserting.",
    )
    p.add_argument(
        "--print-skips",
        action="store_true",
        help="Print unresolved team names (useful for generating team_aliases fixes).",
    )
    args = p.parse_args()

    api_key = os.getenv("CFBD_API_KEY")
    if not api_key:
        print("ERROR: CFBD_API_KEY env var not set.", file=sys.stderr)
        print('Set it like: export CFBD_API_KEY="...token..."', file=sys.stderr)
        raise SystemExit(2)

    conn = connect(args.db)
    try:
        valid_confs = get_valid_conferences(conn, args.year)
        if not valid_confs:
            print(
                f"ERROR: No valid FBS conferences found in team_membership_by_season for year={args.year}. "
                "Did you load memberships?",
                file=sys.stderr,
            )
            raise SystemExit(2)

        # Pull CFBD team records for the year.
        # We request division=fbs, but we *also* hard-filter using our DB conference universe
        # to guard against endpoints returning extra divisions.
        try:
            records = cfbd_get_json(
                "/records",
                {"year": args.year, "division": "fbs"},
                api_key,
            )
        except Exception:
            # Fallback if the API rejects/ignores the filter for some reason
            records = cfbd_get_json(
                "/records",
                {"year": args.year},
                api_key,
            )

        if not isinstance(records, list) or not records:
            print(f"No records returned for year={args.year}.", file=sys.stderr)
            raise SystemExit(1)

        # Hard-filter to conferences we know about for this season (FBS only, per our DB).
        filtered = []
        for r in records:
            conf = (r.get("conference") or "").strip()
            if conf in valid_confs:
                filtered.append(r)

        standings_by_conf = build_conference_rankings(filtered)

        inserted, skipped = write_standings(
            conn,
            season_year=args.year,
            standings_by_conf=standings_by_conf,
            source=args.source,
            source_detail=args.source_detail,
            delete_existing=(not args.keep_existing),
            print_skips=args.print_skips,
        )
        conn.commit()
    finally:
        conn.close()

    print(
        f"Imported conference standings for {args.year}: "
        f"{inserted} rows inserted, {skipped} teams skipped (unresolved names). "
        f"source={args.source}"
    )


if __name__ == "__main__":
    main()