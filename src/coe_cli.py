#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_DB = Path("db/league.db")


def connect(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def fetchall(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> List[sqlite3.Row]:
    return conn.execute(sql, params).fetchall()


def fetchone(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> Optional[sqlite3.Row]:
    return conn.execute(sql, params).fetchone()


def list_tables(conn: sqlite3.Connection) -> List[str]:
    rows = fetchall(
        conn,
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    return [r["name"] for r in rows]


def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = fetchall(conn, f"PRAGMA table_info({table})")
    return [r["name"] for r in rows]


def norm(s: str) -> str:
    return (s or "").strip()


def pick_col(cols: List[str], preferred: List[str], contains: List[str] | None = None) -> Optional[str]:
    """
    Pick a column from cols using:
      1) exact (case-insensitive) match from preferred list
      2) fallback: first col whose lowercase contains any token in `contains`
    """
    lower_map = {c.lower(): c for c in cols}
    for p in preferred:
        if p.lower() in lower_map:
            return lower_map[p.lower()]

    if contains:
        for c in cols:
            lc = c.lower()
            if any(tok.lower() in lc for tok in contains):
                return c

    return None


def resolve_team_id(conn: sqlite3.Connection, raw_team_name: str) -> Tuple[int, str]:
    """
    Resolve raw team name to (team_id, canonical_team_name) using:
      - teams table direct match
      - team_aliases mapping (supports alias->team_id OR alias->team_name schemas)
    """
    name = norm(raw_team_name)

    # teams direct hit
    row = fetchone(conn, "SELECT team_id, team_name FROM teams WHERE team_name = ?", (name,))
    if row:
        return int(row["team_id"]), str(row["team_name"])

    # team_aliases schema detection
    if "team_aliases" not in list_tables(conn):
        raise ValueError(f"Unknown team: {raw_team_name!r} (and no team_aliases table found)")

    a_cols = table_columns(conn, "team_aliases")

    alias_col = pick_col(a_cols, ["alias"], contains=["alias"])
    if not alias_col:
        raise ValueError(f"team_aliases table exists but no alias-like column found. Columns: {a_cols}")

    # Case A: alias -> team_id
    if "team_id" in a_cols:
        row = fetchone(
            conn,
            f"""
            SELECT t.team_id, t.team_name
            FROM team_aliases a
            JOIN teams t ON t.team_id = a.team_id
            WHERE a.{alias_col} = ?
            """,
            (name,),
        )
        if row:
            return int(row["team_id"]), str(row["team_name"])

    # Case B: alias -> team_name
    team_name_col = pick_col(a_cols, ["team_name"], contains=["team_name", "team"])
    if team_name_col:
        row = fetchone(
            conn,
            f"""
            SELECT t.team_id, t.team_name
            FROM team_aliases a
            JOIN teams t ON t.team_name = a.{team_name_col}
            WHERE a.{alias_col} = ?
            """,
            (name,),
        )
        if row:
            return int(row["team_id"]), str(row["team_name"])

    # soft suggestions
    sug = fetchall(conn, "SELECT team_name FROM teams WHERE team_name LIKE ? ORDER BY team_name LIMIT 8", (f"%{name}%",))
    suggestions = [r["team_name"] for r in sug]
    msg = f"Unknown team: {raw_team_name!r}"
    if suggestions:
        msg += "\nDid you mean:\n  - " + "\n  - ".join(suggestions)
    raise ValueError(msg)


def print_table(headers, rows, formats=None):
    """
    Pretty-print a table with column-aware float formatting.

    formats = {
        "column_name": ".3f",   # format spec
    }
    """

    formats = formats or {}

    def fmt(col_name, value):
        if value is None:
            return ""

        # Format floats by column
        if isinstance(value, float):
            spec = formats.get(col_name, ".3f")  # default = 3 decimals
            formatted = format(value, spec)

            # Remove trailing zeros & trailing decimal point
            if "." in formatted:
                formatted = formatted.rstrip("0").rstrip(".")

            return formatted

        return str(value)

    # Build formatted data matrix
    data = [headers]
    for row in rows:
        formatted_row = [
            fmt(headers[i], row[i]) for i in range(len(headers))
        ]
        data.append(formatted_row)

    # Compute column widths
    widths = [
        max(len(str(data[r][c])) for r in range(len(data)))
        for c in range(len(headers))
    ]

    fmt_string = "  ".join("{:<" + str(w) + "}" for w in widths)

    # Print header
    print(fmt_string.format(*data[0]))
    print(fmt_string.format(*["-" * w for w in widths]))

    # Print rows
    for row in data[1:]:
        print(fmt_string.format(*row))


def guess_team_coe_table(conn: sqlite3.Connection) -> str:
    if "team_coefficient_by_year" in list_tables(conn):
        return "team_coefficient_by_year"

    candidates = [t for t in list_tables(conn) if "team_coefficient" in t]
    if candidates:
        return candidates[0]

    raise ValueError("Could not find a team coefficient table (expected team_coefficient_by_year or similar).")


def guess_conf_coe_table(conn: sqlite3.Connection) -> Optional[str]:
    tables = list_tables(conn)
    preferred = [
        "conference_coefficient_by_year",
        "conference_coefficient_rolling_5yr",
        "conference_coefficient_by_year_rolling_5yr",
        "conference_coefficient",
        "conference_ratings_by_year",
    ]
    for t in preferred:
        if t in tables:
            return t

    heur = [
        t for t in tables
        if ("conference" in t or "league" in t)
        and ("coefficient" in t or "coe" in t or "rating" in t)
    ]
    return heur[0] if heur else None


def cmd_team(args: argparse.Namespace) -> int:
    conn = connect(Path(args.db))

    table = args.table or guess_team_coe_table(conn)
    cols = table_columns(conn, table)

    # crucial fix: prefer season/year-ish columns (NOT default "year" blindly)
    year_col = args.year_col or pick_col(cols, ["season", "year"], contains=["season", "year"])
    if not year_col:
        raise ValueError(f"Could not find a season/year column in {table}. Columns: {cols}. Use --year-col.")

    team_id_col = args.team_id_col or pick_col(cols, ["team_id"], contains=["team_id"])
    if not team_id_col:
        raise ValueError(f"Could not find team_id column in {table}. Columns: {cols}. Use --team-id-col.")

    # Try to find points/ppg columns. You can override with flags.
    ppg_col = args.ppg_col or pick_col(cols, ["points_per_game_5yr", "ppg_5yr", "ppg"], contains=["ppg", "per_game"])
    points_col = args.points_col or pick_col(cols, ["total_points_5yr", "total_points", "points"], contains=["total", "points"])

    if not ppg_col and not points_col:
        raise ValueError(
            f"Could not identify a PPG or points column in {table}. Columns: {cols}\n"
            f"Use --ppg-col and/or --points-col."
        )

    team_id, canonical = resolve_team_id(conn, args.team)

    where = [f"{team_id_col} = ?"]
    params: List[Any] = [team_id]

    if args.start_year is not None:
        where.append(f"{year_col} >= ?")
        params.append(args.start_year)
    if args.end_year is not None:
        where.append(f"{year_col} <= ?")
        params.append(args.end_year)
    if args.year is not None:
        where.append(f"{year_col} = ?")
        params.append(args.year)

    select_cols = [year_col]
    headers = [year_col]
    if points_col:
        select_cols.append(points_col)
        headers.append(points_col)
    if ppg_col:
        select_cols.append(ppg_col)
        headers.append(ppg_col)

    sql = f"""
    SELECT {", ".join(select_cols)}
    FROM {table}
    WHERE {" AND ".join(where)}
    ORDER BY {year_col}
    """
    rows = fetchall(conn, sql, tuple(params))
    if not rows:
        print(f"No rows found for {canonical} in {table} (filters applied).")
        return 0

    out_rows: List[Tuple[Any, ...]] = [tuple(r[c] for c in select_cols) for r in rows]

    print(f"\nTeam CoE: {canonical}  (team_id={team_id})")
    print(f"Source: {table}\n")
    formats = {
        "total_points": ".1f",
        "points_per_game": ".3f",
    }
    print_table(headers, out_rows, formats=formats)
    return 0


def cmd_conf(args: argparse.Namespace) -> int:
    conn = connect(Path(args.db))

    table = args.table or guess_conf_coe_table(conn)
    if not table:
        raise ValueError(
            "Could not auto-find a conference/league coefficient table.\n"
            "Pass --table <your_table_name> (and optionally --name-col/--ppg-col/--points-col)."
        )

    cols = table_columns(conn, table)

    year_col = args.year_col or pick_col(cols, ["season", "year"], contains=["season", "year"])
    if not year_col:
        raise ValueError(f"Could not find a season/year column in {table}. Columns: {cols}. Use --year-col.")

    name_col = args.name_col or pick_col(cols, ["conference", "league", "conf"], contains=["conference", "league", "conf"])
    if not name_col:
        raise ValueError(f"Could not find a conference name column in {table}. Columns: {cols}. Use --name-col.")

    ppg_col = args.ppg_col or pick_col(cols, ["points_per_game_5yr", "ppg_5yr", "ppg"], contains=["ppg", "per_game"])
    points_col = args.points_col or pick_col(cols, ["total_points_5yr", "total_points", "points"], contains=["total", "points"])

    if not ppg_col and not points_col:
        raise ValueError(
            f"Could not identify a PPG or points column in {table}. Columns: {cols}\n"
            f"Use --ppg-col and/or --points-col."
        )

    where = [f"{name_col} = ?"]
    params: List[Any] = [args.conference]

    if args.start_year is not None:
        where.append(f"{year_col} >= ?")
        params.append(args.start_year)
    if args.end_year is not None:
        where.append(f"{year_col} <= ?")
        params.append(args.end_year)
    if args.year is not None:
        where.append(f"{year_col} = ?")
        params.append(args.year)

    select_cols = [year_col]
    headers = [year_col]
    if points_col:
        select_cols.append(points_col)
        headers.append(points_col)
    if ppg_col:
        select_cols.append(ppg_col)
        headers.append(ppg_col)

    sql = f"""
    SELECT {", ".join(select_cols)}
    FROM {table}
    WHERE {" AND ".join(where)}
    ORDER BY {year_col}
    """
    rows = fetchall(conn, sql, tuple(params))
    if not rows:
        print(f"No rows found for {args.conference} in {table} (filters applied).")
        return 0

    out_rows: List[Tuple[Any, ...]] = [tuple(r[c] for c in select_cols) for r in rows]

    print(f"\nConference/League CoE: {args.conference}")
    print(f"Source: {table}\n")
    formats = {
        "total_points": ".1f",
        "points_per_game": ".3f",
    }
    print_table(headers, out_rows, formats=formats)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="coe",
        description="Evergreen utilities to query CoE by year (team + conference).",
    )
    p.add_argument("--db", default=str(DEFAULT_DB), help=f"Path to SQLite db (default: {DEFAULT_DB})")

    sub = p.add_subparsers(dest="cmd", required=True)

    pt = sub.add_parser("team", help="Query a team's CoE by year")
    pt.add_argument("team", help="Team name (canonical) or alias")
    pt.add_argument("--year", type=int, default=None, help="Exact season")
    pt.add_argument("--start-year", type=int, default=None, help="Start season (inclusive)")
    pt.add_argument("--end-year", type=int, default=None, help="End season (inclusive)")
    pt.add_argument("--table", default=None, help="Override coefficient table")
    pt.add_argument("--year-col", default=None, help="Override season/year column")
    pt.add_argument("--team-id-col", default=None, help="Override team_id column")
    pt.add_argument("--ppg-col", default=None, help="Override PPG column")
    pt.add_argument("--points-col", default=None, help="Override total points column")
    pt.set_defaults(func=cmd_team)

    pc = sub.add_parser("conf", help="Query a conference/league CoE by year")
    pc.add_argument("conference", help="Conference/league name (e.g., SEC)")
    pc.add_argument("--year", type=int, default=None, help="Exact season")
    pc.add_argument("--start-year", type=int, default=None, help="Start season (inclusive)")
    pc.add_argument("--end-year", type=int, default=None, help="End season (inclusive)")
    pc.add_argument("--table", default=None, help="Override coefficient table")
    pc.add_argument("--year-col", default=None, help="Override season/year column")
    pc.add_argument("--name-col", default=None, help="Override conference name column")
    pc.add_argument("--ppg-col", default=None, help="Override PPG column")
    pc.add_argument("--points-col", default=None, help="Override total points column")
    pc.set_defaults(func=cmd_conf)

    return p


def main() -> int:
    try:
        args = build_parser().parse_args()
        return int(args.func(args))
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())