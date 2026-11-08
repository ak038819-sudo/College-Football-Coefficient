# Bowling Merits — research specification v0.1

Status: overtime policy APPROVED 2026-11-08. Other parameters experimental. No production changes.

## Principle
Merits rewards season accomplishments, not historical reputation or Elo expectations. Awards mature as evidence arrives from the opponent's subsequent games.

## Scoring
Let Q be opponent's local quality (0–1), excluding the matchup itself.

| Result | Control | Challenger |
|---|---|---|
| Regulation win | 2+2Q | 1.5+2.5Q |
| Overtime win | 2+2Q | 1.5+2.5Q |
| Regulation loss | 0 | -0.5(1-Q) |
| **Overtime loss** | **+0.5** | **+0.5** |

**Approved rule:** OT loss earns +0.5 Merits independent of opponent strength. OT victory earns the normal win award. No bonus for regulation victory. OT credit replaces, rather than supplements, the ordinary loss score.

## Quality signal
For each team performance against opponent j, compute q_j=(W_j+2)/(W_j+L_j+4), with W/L counted strictly before that game in the same season. An OT loss remains a loss for this record.

- Win: S=0.5+0.5q_j
- Regulation loss: S=0.5q_j
- **OT loss: S=0.25+0.5q_j** (approved)

For Q, take the weighted average of the evaluated opponent's up to three preceding games (weights 1, .75, .5) and next k games (weights 1, .75, .5, .25). Exclude the evaluated matchup. If no other games, Q=.5. k=3 leads; k=4 is a comparison. Fewer than k future games means provisional, not mature.

## Correctness gates
1. Single canonical engine for chronological replay, standings, and game ledger.
2. Verify overtime flag, final scores, date order, and eligibility; no silent regulation default on missing OT status.
3. Enforce point-in-time q_j, no future-data leakage.
4. Test control and challenger, regulation and OT, maturity, postseason, and incomplete schedules.
5. Match incremental recalculation to full historical rebuild; reconcile prior experimental inconsistencies.
6. Preserve current production Merits and configuration until explicit release approval.

## Reference fixture
2026 CSV source blob SHA: 00e011a7f34088c87616b599dd1ff813b13e1f08.
BYU through 2026-11-08: challenger without OT credit 17.4281; with OT credit 18.2236; difference +0.7955. Oct 24 OT loss to UCF changes from -0.2955 to +0.5000. Other awards unchanged in that snapshot.

## Still undecided
Control versus challenger promotion, 3 versus 4 maturation games, historical eligibility implementation, postseason policy, and production release.
