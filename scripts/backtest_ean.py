"""Research-only EAN v0.3 candidate evaluation from validated JSONL.

Run: python scripts/backtest_ean.py candidates.jsonl --out report.json
The input must contain externally validated rarity/impact/consistency scores.
No historical recipients are assumed or manufactured.
"""
import argparse
import json
from pathlib import Path
from src.nba.ean import Candidate, evaluate, career_recipients


def run(input_path):
    rows = []
    for number, line in enumerate(Path(input_path).read_text().splitlines(), 1):
        if not line.strip():
            continue
        record = json.loads(line)
        candidate = Candidate(**record)
        result = evaluate(candidate)
        rows.append((candidate, result))
    winners = career_recipients(rows)
    return {
        "model_version": "ean-v0.3-research",
        "candidate_count": len(rows),
        "qualified_candidate_count": sum(r.decision.value == "QUALIFIED" for _, r in rows),
        "unique_recipient_count": len(winners),
        "target_recipient_count": 10,
        "certification_enabled": False,
        "candidates": [
            {"player_id": c.player_id, "season": c.season, "scope": c.scope,
             "achievement_profile": c.achievement_profile, "rarity": c.rarity,
             "impact": c.impact, "sustained": c.sustained, "participation": c.participation,
             "coverage_tier": c.coverage_tier, "eas": r.eas,
             "decision": r.decision.value, "reason": r.reason}
            for c, r in rows
        ],
        "recipients": [
            {"player_id": pid, "season": c.season, "achievement_profile": c.achievement_profile,
             "eas": r.eas}
            for pid, (c, r) in winners.items()
        ],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="JSONL of independently validated candidate scores")
    parser.add_argument("--out", default="ean-report.json")
    args = parser.parse_args()
    report = run(args.input)
    Path(args.out).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(f"Evaluated {report['candidate_count']} candidates; "
          f"{report['unique_recipient_count']} research qualifiers (not certified).")
