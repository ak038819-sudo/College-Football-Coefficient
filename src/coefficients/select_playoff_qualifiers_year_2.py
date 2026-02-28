from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import sqlite3
from typing import Dict, List, Optional, Tuple


@dataclass(frozen=True)
class SelectedTeam:
    team_id: int
    conference: Optional[str]
    conf_tier: Optional[int]
    conf_rank: Optional[int]        # rank within conference standings table (for display only)
    conf_coe_rank: Optional[int]    # rank of conference by conference CoE
    coe_rank: int                   # rank of team by team CoE
    bid_type: str                   # "bye" | "champion" | "at_large" | "nit"
    pot: int                        # 0=bye, 1,2 for R24
    reason: str


# ----------------------------
# Loaders / helpers
# ----------------------------

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


def get_team_conf(conn: sqlite3.Connection, year: int, team_id: int) -> Optional[str]:
    """
    membership uses conference_real
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


def load_conf_rank_map(conn: sqlite3.Connection, year: int) -> Dict[Tuple[str, int], int]:
    """
    Returns mapping: (conference, team_id) -> conf_rank
    Expects conference_standings_by_year has: season_year, conference, team_id, conf_rank
    """
    rows = conn.execute(
        """
        SELECT conference, team_id, conf_rank
        FROM conference_standings_by_year
        WHERE season_year = ?
          AND conf_rank IS NOT NULL
        """,
        (year,),
    ).fetchall()
    out: Dict[Tuple[str, int], int] = {}
    for r in rows:
        out[(r["conference"], int(r["team_id"]))] = int(r["conf_rank"])
    return out


def load_conf_topk_by_rank(conn: sqlite3.Connection, year: int, conf: str, k: int) -> List[int]:
    """
    Pull the top-k teams by conf_rank from conference_standings_by_year.
    """
    rows = conn.execute(
        """
        SELECT team_id
        FROM conference_standings_by_year
        WHERE season_year = ?
          AND conference = ?
          AND conf_rank IS NOT NULL
        ORDER BY conf_rank ASC
        LIMIT ?
        """,
        (year, conf, k),
    ).fetchall()
    return [int(r["team_id"]) for r in rows]


def load_coe_ranks(conn: sqlite3.Connection, year: int, formula_version: Optional[str] = None) -> Dict[int, int]:
    """
    CoE source: team_coefficient_by_year (points_per_game DESC)
    Optional formula_version filter.
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
        raise RuntimeError(f"'{table}' missing columns: {sorted(missing)}. Found: {sorted(cols)}")

    where = ["season_year = ?", "points_per_game IS NOT NULL"]
    params: List[object] = [year]

    if formula_version is not None and "formula_version" in cols:
        where.append("formula_version = ?")
        params.append(formula_version)

    rows = conn.execute(
        f"""
        SELECT team_id, coe_rank FROM (
          SELECT
            team_id,
            ROW_NUMBER() OVER (
              ORDER BY points_per_game DESC, team_id ASC
            ) AS coe_rank
          FROM {table}
          WHERE {" AND ".join(where)}
        )
        """,
        tuple(params),
    ).fetchall()

    if not rows:
        if formula_version is not None and "formula_version" in cols:
            vers = conn.execute(
                f"SELECT DISTINCT formula_version FROM {table} ORDER BY formula_version"
            ).fetchall()
            vers_list = [v[0] for v in vers]
            raise RuntimeError(
                f"No CoE rows found for season_year={year} and formula_version={formula_version!r}. "
                f"Available formula_version values: {vers_list}"
            )
        raise RuntimeError(f"No CoE rows found in '{table}' for season_year={year}. Run build_coefficients.py first.")

    return {int(r["team_id"]): int(r["coe_rank"]) for r in rows}


def load_conference_coe_ranks(conn: sqlite3.Connection, year: int) -> Dict[str, int]:
    """
    Conference CoE rank (1 = best), from conference_coefficient_by_year.points_per_game DESC.
    If the table doesn't exist, we fall back to empty {}.
    """
    table = "conference_coefficient_by_year"
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()

    if not exists:
        return {}

    cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if not {"season_year", "conference", "points_per_game"} <= cols:
        return {}

    rows = conn.execute(
        """
        SELECT conference,
               ROW_NUMBER() OVER (ORDER BY points_per_game DESC, conference ASC) AS conf_coe_rank
        FROM conference_coefficient_by_year
        WHERE season_year = ?
          AND points_per_game IS NOT NULL
        """,
        (year,),
    ).fetchall()

    return {r["conference"]: int(r["conf_coe_rank"]) for r in rows}


def load_conference_tiers(conn: sqlite3.Connection, year: int) -> Dict[str, int]:
    """
    Deterministic Year-specific tiers for REAL conferences only (exclude FBS Independents).

    Tier = conference CoE rank among conferences (1..10), assigned after excluding Independents.

    This guarantees we always have 10 tiered conferences and therefore a 24-team field
    per the tier bid rules (22 from conferences + 2 fixed Independent bids below).
    """
    conf_ranks_all = load_conference_coe_ranks(conn, year)  # {conf: conf_coe_rank}
    excluded = {"FBS Independents"}

    items = [(conf, int(rank)) for conf, rank in conf_ranks_all.items() if conf not in excluded]
    items.sort(key=lambda x: (x[1], x[0]))

    top10 = items[:10]
    if len(top10) < 10:
        raise RuntimeError(
            f"Tier inference failed for year={year}: only {len(top10)} eligible conferences after excluding {excluded}. "
            "Check conference_coefficient_by_year contents."
        )

    tiers: Dict[str, int] = {}
    for i, (conf, _rank) in enumerate(top10, start=1):
        tiers[conf] = i

    return tiers


def bids_for_tier(tier: int) -> int:
    # Tier 1–4 → 4 bids
    # Tier 5 → 3 bids
    # Tier 6–10 → Champion only
    if 1 <= tier <= 4:
        return 4
    if tier == 5:
        return 3
    if 6 <= tier <= 10:
        return 1
    return 0


def slot_bye_and_pot(tier: int, slot: int) -> Tuple[bool, int]:
    """
    slot is 1-based position among that conference's allocated bids
    (after ensuring champion is included and trimmed to k).

    Returns (is_bye, pot)
      pot: 0=bye, 1=pot1, 2=pot2

    Bye rules (8 teams):
      Tier 1–2: slot 1 & 2 are byes
      Tier 3–6: slot 1 (champ) is a bye

    Pot placement rules (your spec):
      Tier 1–2: slot3 -> Pot1, slot4 -> Pot2
      Tier 3–4: slot2 & slot3 -> Pot1, slot4 -> Pot2
      Tier 5:   slot2 -> Pot1, slot3 -> Pot2
      Tier 6:   champ only (slot1 bye)
      Tier 7–10: champion (slot1) -> Pot2
    """
    # Byes
    if tier in (1, 2) and slot in (1, 2):
        return True, 0
    if 3 <= tier <= 6 and slot == 1:
        return True, 0

    # Non-byes -> pot mapping
    if tier in (1, 2):
        if slot == 3:
            return False, 1
        if slot == 4:
            return False, 2
        return False, 2

    if tier in (3, 4):
        if slot in (2, 3):
            return False, 1
        if slot == 4:
            return False, 2
        return False, 2

    if tier == 5:
        if slot == 2:
            return False, 1
        if slot == 3:
            return False, 2
        return False, 2

    if tier == 6:
        # slot1 handled above as bye
        return False, 2

    if 7 <= tier <= 10:
        # champ only (slot1) -> Pot2
        return False, 2

    return False, 2


def fill_conference_to_k_by_coe(
    conn: sqlite3.Connection,
    year: int,
    conf: str,
    k: int,
    current: List[int],
    all_fbs: set[int],
    coe_rank: Dict[int, int],
) -> List[int]:
    """
    If conference_standings_by_year doesn't provide enough teams (rare edge cases),
    fill remaining conference bids using best (lowest) CoE rank within that conference.
    """
    if len(current) >= k:
        return current[:k]

    rows = conn.execute(
        """
        SELECT team_id
        FROM team_membership_by_season
        WHERE season_year = ?
          AND conference_real = ?
          AND is_fbs = 1
        """,
        (year, conf),
    ).fetchall()
    candidates = [int(r["team_id"]) for r in rows]
    candidates = [tid for tid in candidates if tid in all_fbs and tid in coe_rank and tid not in current]
    candidates.sort(key=lambda tid: (coe_rank.get(tid, 999999), tid))

    needed = k - len(current)
    return current + candidates[:needed]


# ----------------------------
# Main selection (Year 2)
# ----------------------------

def select_year2_field(
    conn: sqlite3.Connection,
    year: int,
    field_size: int = 24,
    formula_version: str = "v0",
) -> List[SelectedTeam]:
    """
    Year 2+ selection:
      - All conference champions qualify (via tier bid slots; champion forced into slot 1)
      - Conference bid allocation by tier:
          Tier 1–4 -> 4 bids
          Tier 5    -> 3 bids
          Tier 6–10 -> champion only
      - Byes:
          Tier 1–2 -> slot 1 & 2 byes (8 total from top 2 confs)
          Tier 3–6 -> champion/slot1 bye
      - Pots: determined by conference slot positions (not conf_rank)
      - Independents: fixed 2 bids by CoE (no champs/standings semantics)
    """
    assert_standings_present(conn, year)

    tiers = load_conference_tiers(conn, year)  # excludes independents
    champs = load_conference_champions(conn, year)
    all_fbs = set(load_all_fbs_team_ids(conn, year))  # strict FBS-only
    coe_rank = load_coe_ranks(conn, year, formula_version=formula_version)
    conf_rank_map = load_conf_rank_map(conn, year)
    conf_coe_rank = load_conference_coe_ranks(conn, year)

    selected: Dict[int, SelectedTeam] = {}

    def add(team_id: int, bid_type: str, reason: str, pot: int) -> None:
        if team_id in selected:
            return

        conf = get_team_conf(conn, year, team_id)
        tier = tiers.get(conf) if conf else None
        rank = int(coe_rank.get(team_id, 999999))
        c_rank = conf_rank_map.get((conf, team_id)) if conf else None
        c_coe = conf_coe_rank.get(conf) if conf else None

        selected[team_id] = SelectedTeam(
            team_id=team_id,
            conference=conf,
            conf_tier=tier,
            conf_rank=c_rank,
            conf_coe_rank=c_coe,
            coe_rank=rank,
            bid_type=bid_type,
            pot=pot,
            reason=reason,
        )

    # Build per-conference selection according to tier bids (10 real conferences)
    for conf, tier in sorted(tiers.items(), key=lambda x: x[1]):
        if conf == "FBS Independents":
            continue
        k = bids_for_tier(tier)
        if k <= 0:
            continue

        print(f"[DEBUG] {conf=} {tier=} {k=}")

        # Candidate list by standings rank (top-k)
        topk = load_conf_topk_by_rank(conn, year, conf, k)

        # Must include champion for that conference (if present)
        champ_id = champs.get(conf)
        if champ_id is not None and champ_id in all_fbs and champ_id not in topk:
            topk.append(champ_id)

        # Filter FBS-only
        topk = [tid for tid in topk if tid in all_fbs]

        # If standings ever under-provide (shouldn't in normal years), fill to k by CoE in that conference
        topk = fill_conference_to_k_by_coe(
            conn=conn,
            year=year,
            conf=conf,
            k=k,
            current=topk,
            all_fbs=all_fbs,
            coe_rank=coe_rank,
        )

        # If we overflow due to champ inclusion, drop worst non-champ by conf_rank then coe_rank
        if champ_id is not None and len(topk) > k:
            def sort_key_drop(tid: int) -> Tuple[int, int, int]:
                cr = conf_rank_map.get((conf, tid), 999)
                rk = coe_rank.get(tid, 999999)
                return (cr, rk, tid)

            non_champs = [tid for tid in topk if tid != champ_id]
            non_champs.sort(key=sort_key_drop, reverse=True)  # worst first
            while len(topk) > k and non_champs:
                drop = non_champs.pop(0)
                if drop in topk:
                    topk.remove(drop)

        # Stable slot order: conf_rank (if exists) then team coe_rank then team_id
        def slot_sort_key(tid: int) -> Tuple[int, int, int]:
            cr = conf_rank_map.get((conf, tid), 999)
            rk = coe_rank.get(tid, 999999)
            return (cr, rk, tid)

        topk_sorted = sorted(topk, key=slot_sort_key)

        # Force champion into slot 1 if present
        if champ_id is not None and champ_id in topk_sorted:
            topk_sorted.remove(champ_id)
            topk_sorted.insert(0, champ_id)

        # HARD CLAMP (after champion adjustment)
        if len(topk_sorted) > k:
            topk_sorted = topk_sorted[:k]
        # Assign slot positions 1..k
        for slot, tid in enumerate(topk_sorted, start=1):
            is_champ = (champ_id is not None and tid == champ_id)
            is_bye, pot = slot_bye_and_pot(tier=tier, slot=slot)

            if is_bye:
                bid_type = "bye"
                reason = "Year2 bye rule"
            elif is_champ:
                bid_type = "champion"
                reason = "Conference champion"
            else:
                bid_type = "at_large"
                reason = "Conference bid allocation"

            add(tid, bid_type, reason, pot)

    # Independents: fixed 2 bids by CoE (no champ/standings semantics)
    indep_conf = "FBS Independents"
    indep_ids = [
        tid for tid in all_fbs
        if get_team_conf(conn, year, tid) == indep_conf and tid in coe_rank
    ]
    indep_ids.sort(key=lambda tid: (coe_rank.get(tid, 999999), tid))

    for idx, tid in enumerate(indep_ids[:2]):
        # Keep Independents out of bye logic; place best in Pot 1, next in Pot 2.
        pot = 1 if idx == 0 else 2
        add(tid, "at_large", "Independent bid (by CoE)", pot)

    # Hard size check — tier math should land exactly on 24
    if len(selected) != field_size:
        by_conf: Dict[str, int] = {}
        for t in selected.values():
            by_conf[t.conference or "None"] = by_conf.get(t.conference or "None", 0) + 1

        raise RuntimeError(
            f"Selected field size {len(selected)} != expected {field_size}. "
            f"By conference counts: {by_conf}"
        )

    return list(selected.values())


# ----------------------------
# Persist to playoff_field_by_year
# ----------------------------

def upsert_playoff_field(
    conn: sqlite3.Connection,
    year: int,
    field: List[SelectedTeam],
    ruleset: str,
    formula_version: str,
) -> None:
    """
    Writes to playoff_field_by_year with columns observed in your DB:
      season_year, team_id, conference, conf_rank, conf_coe_rank,
      bid_type, pot, formula_version, ruleset, created_at
    Strategy: delete then insert for (season_year, ruleset, formula_version).
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='playoff_field_by_year'"
    ).fetchone()
    if not exists:
        raise RuntimeError("Expected table playoff_field_by_year not found in db.")

    conn.execute(
        """
        DELETE FROM playoff_field_by_year
        WHERE season_year = ?
          AND ruleset = ?
          AND formula_version = ?
        """,
        (year, ruleset, formula_version),
    )

    conn.executemany(
        """
        INSERT INTO playoff_field_by_year (
            season_year, team_id, conference, conf_rank, conf_coe_rank,
            bid_type, pot, formula_version, ruleset, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                year,
                t.team_id,
                t.conference,
                t.conf_rank,
                t.conf_coe_rank,
                t.bid_type,
                t.pot,
                formula_version,
                ruleset,
                now,
            )
            for t in field
        ],
    )
    conn.commit()


def main() -> None:
    import argparse
    from pathlib import Path
    from collections import Counter

    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--field-size", type=int, default=24)
    ap.add_argument("--formula-version", type=str, default="v0")
    ap.add_argument("--ruleset", type=str, default="year2")
    ap.add_argument("--db", type=str, default="db/league.db")
    ap.add_argument("--write-db", action="store_true", help="Persist results to playoff_field_by_year")
    args = ap.parse_args()

    conn = sqlite3.connect(Path(args.db))
    conn.row_factory = sqlite3.Row

    field = select_year2_field(conn, args.year, args.field_size, formula_version=args.formula_version)

    # Sort for readable output (but DO NOT change membership)
    order = {"bye": 0, "champion": 1, "nit": 2, "at_large": 3}
    field_sorted = sorted(
        field,
        key=lambda t: (
            order.get(t.bid_type, 9),
            (t.conf_coe_rank or 999),
            t.pot,
            t.coe_rank,
            t.team_id,
        ),
    )

    pot_counts = Counter(t.pot for t in field_sorted)
    bid_counts = Counter(t.bid_type for t in field_sorted)

    print(f"Year {args.year} selected field ({len(field_sorted)} teams):")
    print(f"Pot counts: {dict(sorted(pot_counts.items()))}   Bid counts: {dict(sorted(bid_counts.items()))}\n")

    for t in field_sorted:
        print(
            f"{t.bid_type:9} conf_coe_rank={str(t.conf_coe_rank):>3} "
            f"conf_rank={str(t.conf_rank):>3} pot={t.pot} "
            f"coe_rank={t.coe_rank:4} team_id={t.team_id:5} "
            f"conf={t.conference} tier={t.conf_tier}  // {t.reason}"
        )

    if args.write_db:
        upsert_playoff_field(conn, args.year, field_sorted, ruleset=args.ruleset, formula_version=args.formula_version)
        print(
            f"\nWrote {len(field_sorted)} rows to playoff_field_by_year for "
            f"season_year={args.year}, ruleset={args.ruleset}, formula_version={args.formula_version}"
        )

    conn.close()


if __name__ == "__main__":
    main()