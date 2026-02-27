# College Football Coefficient (CoE) Specification

Version: v0.1 (Draft)
Status: Authoritative Rules Document for Coefficient Calculation

---

# 1. Purpose

The Coefficient (CoE) system replaces subjective rankings with a transparent, multi-year, performance-based qualification structure.

The CoE system determines:

1. Conference playoff bid allocation
2. Team playoff qualification and seeding
3. Home field advantage in the playoff
4. NIT bid allocation

The system is designed to:
- Reward sustained performance
- Reward competitive non-conference scheduling
- Eliminate "cupcake inflation"
- Encourage competitive balance across conferences
- Remove subjective committee bias

---

# 2. Structural Principles

## 2.1 Conference Requirement
All FBS teams must belong to a conference.
Independent status is not permitted.

## 2.2 Regular Season Structure
- 11-game regular season
- 8 conference games (required)
- 3 non-conference games
- All games must be played within FBS

## 2.3 Playoff Structure
- 24-team playoff
- All conference champions qualify
- Remaining bids allocated by conference CoE
- Top seeds receive byes to Round of 16
- Home field determined by team CoE

---

# 3. Coefficient Framework

There are two distinct but related coefficients:

1. Conference Coefficient (Conference CoE)
2. Team Coefficient (Team CoE)

---

# 4. Conference Coefficient (Conference CoE)

## 4.1 Scope

Conference CoE is determined by:

- Non-conference games
- Playoff games

Conference games DO NOT affect Conference CoE.

---

## 4.2 Non-Conference Game Scoring

For each non-conference game:

- Win = 2 points
- Overtime Loss = 1 point
- Loss = 0 points

Definition:
- Overtime Loss is a loss in a game that went to overtime.
- Regulation losses receive 0 points.

---

## 4.3 Bounty Multiplier

Base points earned are multiplied by opponent strength.

Formula: TBD

Adjusted Points = Base Points × Opponent CoE Points Per Game (5-year rolling)


Opponent CoE Points Per Game is defined as: Opponent Total Conference CoE Points (last 5 years) ÷ Opponent Total Eligible CoE Games (last 5 years)


Purpose:
- Rewards beating strong programs
- Prevents padding against weak teams

---

## 4.4 Playoff Bonus Points

Additional Conference CoE points are awarded:

- Playoff Participation: +6 points (max 12)
- Every Playoff Game Played: +1.5 points

These bonuses apply to the conference of the participating team.

---

## 4.5 Rolling Window

Conference CoE is calculated on a rolling 5-year window.

Example:
- 2026 CoE = 2022–2026 cumulative total

Bid allocation for a season is determined after non-conference play concludes (typically late September).

---

# 5. Team Coefficient (Team CoE)

Team CoE is used for:
- Playoff qualification within conference
- Home field advantage
- Pot placement

## 5.1 Scope

Team CoE includes:
- Conference games
- Non-conference games
- Playoff games

Conference games affect Team CoE but NOT Conference CoE.

---

## 5.2 Regular Season Bonuses

- Top 8 overall regular season finish: +0.25
- Rank 9–24 overall: +0.25

(Note: Final implementation of "overall finish" must be formally defined in future revision.)

---

## 5.3 Playoff Bonuses

- Playoff Participation: +6 points (max 12)
- Every Playoff Game Played: +1.5 points

---

# 6. Conference Bid Allocation

## Year 1 (Initial System)

Conference Ranking → Automatic Bids:

- 1–4: Top 4 teams qualify
- 5: Top 3 qualify
- 6: Top 2 qualify
- 7–10: Champion qualifies

---

## Year 2 and Beyond

Conference Ranking → Automatic Bids:

- 1–4: Top 4 teams qualify
- 5: Top 3 qualify
- 6–10: Champion qualifies

NIT winner grants an additional bid to their conference.

---

# 7. Seeding Structure

## Year 1 Seeding

1 & 2:
- 1st and 2nd receive byes
- 3rd → Pot 1
- 4th → Pot 2

3 & 4:
- Champion receives bye
- Runner-up + 3rd → Pot 1
- 4th → Pot 2

5:
- Champion receives bye
- Runner-up → Pot 1
- 3rd → Pot 2

6:
- Champion receives bye
- Runner-up → Pot 1

7–10:
- Champions → Pot 2

---

## Year 2+ Seeding

Same as Year 1 except:

- Conference 6 Champion no longer guaranteed Pot 1.
- If NIT winner is in Conference 7–10:
  - That conference champion moves to Pot 1.

NIT seeding:
- If Conference rank 1–6 → Pot 1
- If 7–10 → Pot 2

---

# 8. Deterministic Implementation Requirements

The system must be:

- Fully reproducible from database state
- Deterministic (no randomness except pot draw phase)
- Transparent (all component scores stored in database)
- Versioned (each formula iteration tagged)

---

# 9. Open Questions (To Be Resolved)

- Precise definition of "Top 8 finish"
- Scaling normalization for bounty multiplier
- Maximum cap on multiplier?
- Handling of conference realignment within 5-year window
- Handling vacated wins

---

# 10. Guiding Philosophy

The CoE system exists to:

- Replace subjective rankings
- Reward competitive scheduling
- Encourage cross-conference strength testing
- Restore legitimacy to championship qualification
- Remove centralized selection bias

The system must always favor transparency over complexity.
