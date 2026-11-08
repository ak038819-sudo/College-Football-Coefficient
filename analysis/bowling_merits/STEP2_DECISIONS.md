# Bowling Merits — Step 2 research decision (2026-11-08)

## Locked choice: three future opponent performances

The research model uses the next **three** opponent performances, weighted 1, 0.75, 0.5, together with the previous three performances (same weights). The fourth future performance is **not** part of the candidate production formula. Preserve the four-game comparison only as historical sensitivity evidence.

Eleven-season study (2014–2019, 2021–2025): challenger k3 vs k4 mean absolute ranking movement **0.289** positions, with identical top-10 and top-25 membership in all 11 seasons. This is a model design decision, not proof of optimality.

## Next decision: scoring philosophy

Compare **control** (win 2 + 2Q; regulation loss 0) against **challenger** (win 1.5 + 2.5Q; regulation loss -0.5(1-Q)). Both award +0.5 for overtime losses. Both use k=3.

Investigate teams with the largest rank changes, and explain those movements using wins, regulation losses, overtime losses, and opponent-quality evidence. Do not choose a winner from ranking stability alone.

## Caveat

Season-specific FBS eligibility reconciliation remains deferred by explicit project decision. Research results are provisional pending that check. No production scoring change is authorized.

## Locked choice: Challenger A scoring

Approved scoring formula for Bowling Merits:
- Win: **1.5 + 2.5Q**
- Regulation loss: **-0.5(1-Q)**
- Overtime loss: **+0.5**, irrespective of opponent quality
- Future opponent performances: **three**, with weights 1, 0.75, 0.5

This supersedes the control as the selected Bowling Merits model. Control remains in historical research outputs as a benchmark, not a production candidate. The approved scoring choice is a project decision; the research evidence supports its intended emphasis but does not establish universal superiority.

## Transition to Step 3: integration

Integrate the selected formula into the Elocast data pipeline behind a feature flag. Preserve current production Merits, existing database schema guarantees, and live scoreboard behavior. Produce deterministic incremental and full-rebuild parity checks. The deferred season-specific FBS eligibility reconciliation must remain visible and must be completed before production promotion. Do not merge or deploy without explicit approval.
