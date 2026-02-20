#!/usr/bin/env python3
"""
suggest_aliases.py

Given:
  - a SQLite db with a `teams` table (team_name column)
  - a SQLite db with a `team_aliases` table (alias, team_name)
  - a text file of missing aliases (one per line)

Produce:
  - SQL INSERT statements mapping alias -> canonical team_name

Upgrades over the simple fuzzy loop:
  ✅ Manual overrides (always win)
  ✅ Stronger normalization (casefold, strip accents, punctuation collapse)
  ✅ Safer matching (multiple scoring signals + guards)
  ✅ Confidence buckets (HIGH/MED/LOW)
  ✅ Emits comments + summary to stderr (keeps SQL clean in stdout)

Usage:
  python src/suggest_aliases.py db/league.db /tmp/missing_2025.txt 0.86 > /tmp/alias_suggestions.sql

Then apply:
  sqlite3 db/league.db < /tmp/alias_suggestions.sql
"""

from __future__ import annotations

import re
import sys
import sqlite3
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Dict, Iterable, List, Optional, Set, Tuple


# -----------------------------
# Manual overrides (edit freely)
# -----------------------------
MANUAL_ALIASES: Dict[str, str] = {
    # Common CFBD-ish / scoreboard variants
    "App State": "Appalachian State",
    "UL Monroe": "Louisiana-Monroe",
    "Massachusetts": "UMass",
    "Florida Atlantic": "FAU",
    "Florida International": "FIU",

    # Accent variants (example)
    "San José State": "San Jose State",
}


# -------------------------------------------------------
# Normalization helpers (make strings comparable reliably)
# -------------------------------------------------------
_PUNCT_RE = re.compile(r"[^a-z0-9\s]+")
_WS_RE = re.compile(r"\s+")

def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def norm(s: str) -> str:
    """
    Aggressive normalize:
      - None-safe
      - strip accents
      - casefold
      - remove punctuation
      - collapse whitespace
    """
    s = (s or "").strip()
    s = strip_accents(s)
    s = s.casefold()
    s = _PUNCT_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s

def tokens(s: str) -> List[str]:
    s = norm(s)
    return s.split() if s else []

def token_sort_key(s: str) -> str:
    return " ".join(sorted(tokens(s)))

def seq_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


# -----------------------
# Scoring / safety checks
# -----------------------
@dataclass
class Match:
    alias_raw: str
    alias_norm: str
    team_raw: str
    team_norm: str
    score: float
    confidence: str
    reason: str

def score_pair(alias_norm: str, team_norm: str) -> float:
    """
    Blended score:
      - raw normalized sequence ratio
      - token-sorted ratio (handles swapped word order)
      - token overlap bonus
    """
    if not alias_norm or not team_norm:
        return 0.0

    r1 = seq_ratio(alias_norm, team_norm)
    r2 = seq_ratio(token_sort_key(alias_norm), token_sort_key(team_norm))

    a_toks = set(alias_norm.split())
    t_toks = set(team_norm.split())
    if not a_toks or not t_toks:
        overlap = 0.0
    else:
        overlap = len(a_toks & t_toks) / max(len(a_toks), len(t_toks))

    # Weighting tuned for team names
    blended = (0.55 * r1) + (0.35 * r2) + (0.10 * overlap)
    return blended

def confidence_bucket(score: float) -> str:
    if score >= 0.92:
        return "HIGH"
    if score >= 0.86:
        return "MED"
    return "LOW"

def guard_reject(alias_norm: str, team_norm: str, score: float) -> Optional[str]:
    """
    Extra safety: reject matches that are suspicious even if score is okay.
    Returns a reason string if rejected, otherwise None.
    """
    # If very short alias, require very high score
    if len(alias_norm) <= 4 and score < 0.95:
        return "alias too short for non-exact match"

    # If alias has a strong token not present in team, be cautious
    a = set(alias_norm.split())
    t = set(team_norm.split())
    missing_tokens = [tok for tok in a if tok not in t and len(tok) >= 4]
    if missing_tokens and score < 0.92:
        return f"missing token(s) {missing_tokens} and score < 0.92"

    return None


# -------------------------
# DB + file IO
# -------------------------
def load_teams(db_path: str) -> List[str]:
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT team_name FROM teams")
        return [r[0] for r in cur.fetchall()]

def load_existing_aliases(db_path: str) -> Set[str]:
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT alias FROM team_aliases")
        return {r[0] for r in cur.fetchall()}

def read_missing(path: str) -> List[str]:
    out: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                out.append(s)
    return out


# -------------------------
# Main suggestion engine
# -------------------------
def suggest_aliases(
    missing: Iterable[str],
    teams: List[str],
    existing_aliases: Set[str],
    min_score: float,
) -> Tuple[List[Match], List[str]]:
    # Precompute normalized team names
    team_norm_map: List[Tuple[str, str]] = [(t, norm(t)) for t in teams]

    matches: List[Match] = []
    skipped: List[str] = []

    # Also build normalized manual map to catch accent/case mismatches
    manual_norm = {norm(k): v for k, v in MANUAL_ALIASES.items()}

    for m in missing:
        if m in existing_aliases:
            skipped.append(f"{m} (already in team_aliases)")
            continue

        m_norm = norm(m)

        # 1) Manual override wins
        if m in MANUAL_ALIASES:
            target = MANUAL_ALIASES[m]
            matches.append(
                Match(
                    alias_raw=m,
                    alias_norm=m_norm,
                    team_raw=target,
                    team_norm=norm(target),
                    score=1.0,
                    confidence="HIGH",
                    reason="manual override (exact key)",
                )
            )
            continue
        if m_norm in manual_norm:
            target = manual_norm[m_norm]
            matches.append(
                Match(
                    alias_raw=m,
                    alias_norm=m_norm,
                    team_raw=target,
                    team_norm=norm(target),
                    score=1.0,
                    confidence="HIGH",
                    reason="manual override (normalized key)",
                )
            )
            continue

        # 2) Fuzzy search best match
        best_team = None
        best_team_norm = None
        best_score = -1.0

        for t_raw, t_norm in team_norm_map:
            s = score_pair(m_norm, t_norm)
            if s > best_score:
                best_score = s
                best_team = t_raw
                best_team_norm = t_norm

        if not best_team or best_team_norm is None:
            skipped.append(f"{m} (no teams to compare)")
            continue

        # 3) Safety guard
        reject_reason = guard_reject(m_norm, best_team_norm, best_score)
        if reject_reason:
            skipped.append(f"{m} (rejected: {reject_reason}; best={best_team} score={best_score:.3f})")
            continue

        # 4) Threshold
        if best_score < min_score:
            skipped.append(f"{m} (score {best_score:.3f} below min {min_score:.2f}; best={best_team})")
            continue

        conf = confidence_bucket(best_score)
        matches.append(
            Match(
                alias_raw=m,
                alias_norm=m_norm,
                team_raw=best_team,
                team_norm=best_team_norm,
                score=best_score,
                confidence=conf,
                reason="fuzzy match",
            )
        )

    # Order: manual/high first, then by score desc
    matches.sort(key=lambda x: (x.confidence != "HIGH", -x.score, x.alias_raw))
    return matches, skipped


def emit_sql(matches: List[Match]) -> None:
    """
    Emit SQL to STDOUT only (so you can pipe to a .sql file).
    """
    print("-- Suggested aliases (review before running)")
    print("-- Format: alias -> team_name")
    for m in matches:
        # Escape single quotes for SQL
        a = m.alias_raw.replace("'", "''")
        t = m.team_raw.replace("'", "''")
        print(f"-- {m.confidence} {m.score:.3f} {m.reason}: '{m.alias_raw}' -> '{m.team_raw}'")
        print(
            "INSERT OR IGNORE INTO team_aliases(alias, team_name) "
            f"VALUES ('{a}', '{t}');"
        )


def main() -> int:
    if len(sys.argv) not in (3, 4):
        print(
            "Usage: python src/suggest_aliases.py <db_path> <missing_txt> [min_score]\n"
            "Example: python src/suggest_aliases.py db/league.db /tmp/missing_2025.txt 0.86 > /tmp/alias_suggestions.sql",
            file=sys.stderr,
        )
        return 2

    db_path = sys.argv[1]
    missing_txt = sys.argv[2]
    min_score = float(sys.argv[3]) if len(sys.argv) == 4 else 0.86

    teams = load_teams(db_path)
    existing = load_existing_aliases(db_path)
    missing = read_missing(missing_txt)

    matches, skipped = suggest_aliases(missing, teams, existing, min_score)

    # SQL to stdout
    emit_sql(matches)

    # Summary to stderr
    print("", file=sys.stderr)
    print(f"[alias suggester] teams: {len(teams)}", file=sys.stderr)
    print(f"[alias suggester] missing inputs: {len(missing)}", file=sys.stderr)
    print(f"[alias suggester] suggestions: {len(matches)}", file=sys.stderr)
    print(f"[alias suggester] skipped: {len(skipped)}", file=sys.stderr)
    if skipped:
        print("[alias suggester] first few skips:", file=sys.stderr)
        for line in skipped[:12]:
            print(f"  - {line}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())