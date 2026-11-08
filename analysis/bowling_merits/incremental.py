"""Incremental Bowling Merits recalculation, with full-rebuild parity.

Only awards whose values or maturity changed are emitted as upserts.
The underlying engine remains the sole authority for scoring.
"""
from engine import Rules,calculate

APPROVED_RULES=Rules(1.5,.5,3)

def award_key(row):
    return (row["season"],row["id"],row["team"])

def recalculate(games,previous_ledger,as_of=None):
    """Return a deterministic change set and the complete canonical snapshot.

    The first implementation deliberately recomputes in memory before diffing.
    This guarantees correctness with opponent lookback/lookahead dependencies
    and avoids unsafe local-window assumptions. No database writes occur.
    """
    updated=calculate(games,APPROVED_RULES,as_of=as_of)
    old={award_key(r):r for r in previous_ledger}
    new={award_key(r):r for r in updated}
    upserts=[r for r in updated if old.get(award_key(r))!=r]
    deletes=sorted(set(old)-set(new))
    return {"upserts":upserts,"deletes":deletes,"ledger":updated,
            "changed_awards":len(upserts)+len(deletes)}
