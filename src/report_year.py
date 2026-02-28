#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

DEFAULT_DB = Path("db/league.db")


def connect(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def col_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == col for r in rows)


def fmt_float(v: Any, decimals: int = 3) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        s = f"{v:.{decimals}f}"
        return s.rstrip("0").rstrip(".") if "." in s else s
    return str(v)


def print_section(title: str) -> None:
    bar = "=" * len(title)
    print(f"\n{title}\n{bar}")


def print_table(headers: List[str], rows: List[Tuple[Any, ...]], formats: Optional[Dict[str, str]] = None) -> None:
    formats = formats or {}

    def fmt(col: str, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, float):
            spec = formats.get(col, ".3f")
            s = format(value, spec)
            return s.rstrip("0").rstrip(".") if "." in s else s
        return str(value)

    data = [headers] + [[fmt(headers[i], r[i]) for i in range(len(headers))] for r in rows]
    widths = [max(len(str(data[r][c])) for r in range(len(data))) for c in range(len(headers))]
    line = "  ".join("{:<" + str(w) + "}" for w in widths)

    print(line.format(*data[0]))
    print(line.format(*["-" * w for w in widths]))
    for r in data[1:]:
        print(line.format(*r))


def main() -> int:
    p = argparse.ArgumentParser(description="Season snapshot report (standings, CoE, conference CoE, playoff field).")
    p.add_argument("season_year", type=int, help="Season year to report (e.g., 2024)")
    p.add_argument("--db", default=str(DEFAULT_DB), help=f"Path to SQLite db (default: {DEFAULT_DB})")
    p.add_argument("--formula-version", default=None, help="Optional formula_version filter for CoE tables")
    p.add_argument("--top", type=int, default=25, help="How many teams to show in top team CoE list (default 25)")
    args = p.parse_args()

    conn = connect(Path(args.db))
    y = args.season_year

        # --- Standings ---
    if table_exists(conn, "conference_standings_by_year") and col_exists(conn, "conference_standings_by_year", "season_year"):
        print_section(f"{y} Conference Standings (by conference)")

        cols = [r["name"] for r in conn.execute("PRAGMA table_info(conference_standings_by_year)").fetchall()]  # type: ignore

        # Build SELECT list safely (no trailing commas)
        select_cols = [
            "s.conference AS conference",
            "s.team_id AS team_id",
            "t.team_name AS team_name",
        ]

        optional = [
            "place",
            "wins", "losses", "ot_losses",
            "conf_wins", "conf_losses", "conf_ot_losses",
        ]
        for c in optional:
            if c in cols:
                select_cols.append(f"s.{c} AS {c}")

        # Adaptive ORDER BY
        order_parts = ["s.conference"]

        if "place" in cols:
            order_parts.append("s.place")
        else:
            # Fall back to sensible ordering if explicit place is not stored
            if "conf_wins" in cols:
                order_parts.append("s.conf_wins DESC")
            if "conf_losses" in cols:
                order_parts.append("s.conf_losses ASC")
            if "conf_ot_losses" in cols:
                order_parts.append("s.conf_ot_losses ASC")

            if "wins" in cols:
                order_parts.append("s.wins DESC")
            if "losses" in cols:
                order_parts.append("s.losses ASC")
            if "ot_losses" in cols:
                order_parts.append("s.ot_losses ASC")

            order_parts.append("t.team_name")

        sql = f"""
        SELECT {", ".join(select_cols)}
        FROM conference_standings_by_year s
        JOIN teams t ON t.team_id = s.team_id
        WHERE s.season_year = ?
        ORDER BY {", ".join(order_parts)}
        """

        rows = conn.execute(sql, (y,)).fetchall()
        if not rows:
            print("No standings rows found.")
        else:
            headers = ["team_name"] + [c for c in optional if c in cols]

            current = None
            buf: List[Tuple[Any, ...]] = []

            for r in rows:
                conf = r["conference"]
                if current is None:
                    current = conf

                if conf != current:
                    print(f"\n[{current}]")
                    print_table(headers, buf)
                    buf = []
                    current = conf

                buf.append(tuple([r["team_name"]] + [r[c] for c in headers[1:]]))

            if current is not None:
                print(f"\n[{current}]")
                print_table(headers, buf)

    else:
        print_section("Standings")
        print("conference_standings_by_year not found (or missing season_year). Skipping.")

    # --- Team CoE ---
    if table_exists(conn, "team_coefficient_by_year") and col_exists(conn, "team_coefficient_by_year", "season_year"):
        print_section(f"{y} Team CoE (Top {args.top} by PPG)")
        where = ["c.season_year = ?"]
        params: List[Any] = [y]
        if args.formula_version and col_exists(conn, "team_coefficient_by_year", "formula_version"):
            where.append("c.formula_version = ?")
            params.append(args.formula_version)

        sql = f"""
        SELECT t.team_name,
               c.total_points,
               c.games_counted,
               c.points_per_game
        FROM team_coefficient_by_year c
        JOIN teams t ON t.team_id = c.team_id
        WHERE {" AND ".join(where)}
        ORDER BY c.points_per_game DESC, c.total_points DESC, t.team_name
        LIMIT ?
        """
        params2 = params + [args.top]
        rows = conn.execute(sql, tuple(params2)).fetchall()
        out = [(r["team_name"], r["total_points"], r["games_counted"], r["points_per_game"]) for r in rows]
        print_table(
            ["team_name", "total_points", "games_counted", "points_per_game"],
            out,
            formats={"total_points": ".1f", "points_per_game": ".3f"},
        )
    else:
        print_section("Team CoE")
        print("team_coefficient_by_year not found (or missing season_year). Skipping.")

    # --- Conference CoE ---
    if table_exists(conn, "conference_coefficient_by_year") and col_exists(conn, "conference_coefficient_by_year", "season_year"):
        print_section(f"{y} Conference CoE (by PPG)")
        sql = """
        SELECT conference, total_points, points_per_game
        FROM conference_coefficient_by_year
        WHERE season_year = ?
        ORDER BY points_per_game DESC, total_points DESC, conference
        """
        rows = conn.execute(sql, (y,)).fetchall()
        out = [(r["conference"], r["total_points"], r["points_per_game"]) for r in rows]
        print_table(
            ["conference", "total_points", "points_per_game"],
            out,
            formats={"total_points": ".1f", "points_per_game": ".3f"},
        )
    else:
        print_section("Conference CoE")
        print("conference_coefficient_by_year not found (or missing season_year). Skipping.")

        # --- Playoff field ---
    if table_exists(conn, "playoff_field_by_year") and col_exists(conn, "playoff_field_by_year", "season_year"):
        print_section(f"{y} Playoff Field")

        p_cols = [r["name"] for r in conn.execute("PRAGMA table_info(playoff_field_by_year)").fetchall()]  # type: ignore

        # columns we might have
        optional = [c for c in ["conference", "pot", "seed", "slot", "rank", "is_champion"] if c in p_cols]

        # SELECT list
        select_cols = ["t.team_name AS team_name"]
        if "conference" in optional:
            select_cols.append("f.conference AS conference")
        else:
            select_cols.append("NULL AS conference")

        for c in optional:
            if c == "conference":
                continue
            select_cols.append(f"f.{c} AS {c}")

        # ORDER BY: prefer seed if present; else pot; else team_name
        order_parts = []
        if "seed" in p_cols:
            order_parts.append("f.seed ASC")
        if "pot" in p_cols:
            order_parts.append("f.pot ASC")
        if "conference" in p_cols:
            order_parts.append("f.conference ASC")
        order_parts.append("t.team_name ASC")

        sql = f"""
        SELECT {", ".join(select_cols)}
        FROM playoff_field_by_year f
        JOIN teams t ON t.team_id = f.team_id
        WHERE f.season_year = ?
        ORDER BY {", ".join(order_parts)}
        """

        rows = conn.execute(sql, (y,)).fetchall()
        if not rows:
            print("No playoff_field_by_year rows found.")
        else:
            headers = ["team_name"]
            if "conference" in p_cols:
                headers.append("conference")
            for c in optional:
                if c != "conference":
                    headers.append(c)

            out = []
            for r in rows:
                row_vals = [r["team_name"]]
                if "conference" in p_cols:
                    row_vals.append(r["conference"])
                for c in optional:
                    if c != "conference":
                        row_vals.append(r[c])
                out.append(tuple(row_vals))

            print_table(headers, out)

    else:
        print_section("Playoff Field")
        print("playoff_field_by_year not found (or missing season_year). Skipping.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())