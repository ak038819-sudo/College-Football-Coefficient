from __future__ import annotations

from dataclasses import dataclass
import sqlite3
from typing import Dict, List, Optional


@dataclass(frozen=True)
class SelectedTeam:
    team_id: int
    conference: Optional[str]
    conf_tier: Optional[int]
    coe_rank: int
    bid_type: str   # "bye" | "auto" | "nit" | "at_large"
    reason: str


def load_conference_tiers(conn: sqlite3.Connection) -> Dict[str, int]:
    """
    Expects: fbs_conferences(conference TEXT PRIMARY KEY, conf_tier INTEGER)
    """
    rows = conn.execute("SELECT conference, conf_tier FROM fbs_conferences").fetchall()
    return {r["conference"]: int(r["conf_tier"]) for r in rows}


def load_conference_champions(conn: sqlite3.Connection, year: int) -> Dict[str, int]:
    """
    Expects: conference_champions_by_year(season_year, conference, team_id, ...)
    """
    rows = conn.execute(
        """
        SELECT conference, team_id
        FROM conference_champions_by_year
        WHERE season_year = ?
        """,
        (year,),
    ).fetchall()
    return {r["conference"]: int(r["team_id"]) for r in rows}


def assert_standings_present(conn: sqlite3.Connection, year: int) -> None:
    row = conn.execute(
        "SELECT 1 FROM conference_standings_by_year WHERE season_year = ? LIMIT 1",
        (year,),
    ).fetchone()
    if not row:
        raise RuntimeError(
            f"No conference standings found for year={year}. "
            "Run fetch_cfbd_conference_standings.py first."
        )


def load_conf_top2(conn: sqlite3.Connection, year: int, conf: str) -> tuple[int | None, int | None]:
    """
    Tier 1 & 2 bye logic: take conf_rank 1 and 2 from conference_standings_by_year.
    Requires that fetch_cfbd_conference_standings.py has been run for the year.
    """
    rows = conn.execute(
        """
        SELECT team_id
        FROM conference_standings_by_year
        WHERE season_year = ?
          AND conference = ?
          AND conf_rank IN (1, 2)
        ORDER BY conf_rank ASC
        """,
        (year, conf),
    ).fetchall()

    top1 = int(rows[0]["team_id"]) if len(rows) >= 1 else None
    top2 = int(rows[1]["team_id"]) if len(rows) >= 2 else None
    return top1, top2


def load_all_fbs_team_ids(conn: sqlite3.Connection, year: int) -> List[int]:
    rows = conn.execute(
        """
        SELECT DISTINCT team_id
        FROM team_membership_by_season
        WHERE season_year = ?
          AND is_fbs = 1
        """,
        (year,),
    ).fetchall()
    return [int(r["team_id"]) for r in rows]


def load_coe_ranks(conn: sqlite3.Connection, year: int) -> Dict[int, int]:
    """
    CoE source table:
      team_coefficient_by_year

    Uses points_per_game as the CoE score.
    Higher points_per_game = better rank (1 = best).
    """
    table = "team_coefficient_by_year"

    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table,),
    ).fetchone()
    if not exists:
        raise RuntimeError(f"Expected CoE table '{table}' not found in db.")

    cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    required = {"season_year", "team_id", "points_per_game"}
    missing = required - cols
    if missing:
        raise RuntimeError(
            f"'{table}' missing columns: {sorted(missing)}. Found: {sorted(cols)}"
        )

    rows = conn.execute(
        f"""
        SELECT team_id, coe_rank FROM (
          SELECT
            team_id,
            ROW_NUMBER() OVER (
              ORDER BY points_per_game DESC, team_id ASC
            ) AS coe_rank
          FROM {table}
          WHERE season_year = ?
            AND points_per_game IS NOT NULL
        )
        """,
        (year,),
    ).fetchall()

    if not rows:
        raise RuntimeError(
            f"No CoE rows found in '{table}' for season_year={year}. "
            "Run build_coefficients.py first (or verify the year exists)."
        )

    return {int(r["team_id"]): int(r["coe_rank"]) for r in rows}


def get_team_conf(conn: sqlite3.Connection, year: int, team_id: int) -> Optional[str]:
    """
    Your membership table uses conference_real.
    """
    row = conn.execute(
        """
        SELECT conference_real
        FROM team_membership_by_season
        WHERE season_year = ?
          AND team_id = ?
          AND is_fbs = 1
        LIMIT 1
        """,
        (year, team_id),
    ).fetchone()
    return row["conference_real"] if row else None


def select_year2_field(conn: sqlite3.Connection, year: int, field_size: int = 16) -> List[SelectedTeam]:
    # standings are required for tier 1/2 top2 byes
    assert_standings_present(conn, year)

    tiers = load_conference_tiers(conn)
    champs = load_conference_champions(conn, year)
    all_fbs = set(load_all_fbs_team_ids(conn, year))
    coe_rank = load_coe_ranks(conn, year)

    # -----------------------
    # 1) Determine byes
    # -----------------------
    bye_ids: set[int] = set()
    for conf, tier in tiers.items():
        if tier in (1, 2):
            top1, top2 = load_conf_top2(conn, year, conf)
            if top1:
                bye_ids.add(top1)
            if top2:
                bye_ids.add(top2)
        elif tier in (3, 4, 5, 6):
            champ_id = champs.get(conf)
            if champ_id:
                bye_ids.add(champ_id)

    # Strict FBS-only
    bye_ids = {t for t in bye_ids if t in all_fbs}

    # -----------------------
    # 2) Auto bids (baseline): include all champs
    # -----------------------
    auto_ids: set[int] = {
        champs[conf]
        for conf in champs
        if conf in tiers and champs[conf] in all_fbs
    }

    # -----------------------
    # 3) NIT placeholder
    # -----------------------
    nit_ids: set[int] = set()

    # -----------------------
    # 4) Assemble field
    # -----------------------
    selected: Dict[int, SelectedTeam] = {}

    def add(team_id: int, bid_type: str, reason: str) -> None:
        if team_id in selected:
            return
        conf = get_team_conf(conn, year, team_id)
        tier = tiers.get(conf) if conf else None
        rank = int(coe_rank.get(team_id, 999999))
        selected[team_id] = SelectedTeam(
            team_id=team_id,
            conference=conf,
            conf_tier=tier,
            coe_rank=rank,
            bid_type=bid_type,
            reason=reason,
        )

    # Byes first (these will become top seeds later)
    for tid in sorted(bye_ids):
        add(tid, "bye", "Year2 bye rule")

    # Champs as auto bids
    for tid in sorted(auto_ids):
        add(tid, "auto", "Conference champion")

    # NIT teams (later)
    for tid in sorted(nit_ids):
        add(tid, "nit", "NIT selection")

    # Fill remaining by CoE rank (at-large ordering)
    remaining = [tid for tid in all_fbs if tid not in selected]
    remaining.sort(key=lambda tid: (coe_rank.get(tid, 999999), tid))

    for tid in remaining:
        if len(selected) >= field_size:
            break
        add(tid, "at_large", "At-large by CoE rank")

    # Hard sanity checks
    if len(selected) != field_size:
        raise RuntimeError(f"Selected field size {len(selected)} != expected {field_size}")

    return list(selected.values())


def main() -> None:
    import argparse
    from pathlib import Path

    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--field-size", type=int, default=16)
    ap.add_argument("--db", type=str, default="db/league.db")
    args = ap.parse_args()

    conn = sqlite3.connect(Path(args.db))
    conn.row_factory = sqlite3.Row

    field = select_year2_field(conn, args.year, args.field_size)

    # Sort for readable output
    order = {"bye": 0, "auto": 1, "nit": 2, "at_large": 3}
    field_sorted = sorted(field, key=lambda t: (order.get(t.bid_type, 9), t.coe_rank, t.team_id))

    print(f"Year {args.year} selected field ({len(field_sorted)} teams):")
    for t in field_sorted:
        print(
            f"{t.bid_type:8} coe_rank={t.coe_rank:4} team_id={t.team_id:5} "
            f"conf={t.conference} tier={t.conf_tier}  // {t.reason}"
        )

    conn.close()


if __name__ == "__main__":
    main()