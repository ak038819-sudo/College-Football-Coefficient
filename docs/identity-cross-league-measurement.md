# Joining a school's football team to its basketball team

Measured 2026-11-08 on the committed 2026 snapshots. No provider calls.

Phase 1 item 3: *does CBBD number teams the same way CFBD does?* The audit
flagged this as unmeasured and warned against assuming it, because both are
CFBD-family providers and a coincidence would be plausible. It is not a
coincidence, and the answer is worse than "no".

## The result

| | |
| --- | ---: |
| FBS football teams in `data/reference/team_identities.json` | 139 |
| CBBD teams in the 2026 snapshot | 365 |
| **Schools whose CBBD id equals their CFBD id** | **0 of 139** |
| **CBBD ids that are also a valid CFBD football id — for a different school** | **81** |
| Schools paired on the ESPN id | **139 of 139** |
| Football teams left unpaired | **0** |
| Basketball-only schools (no FBS football) | 226 |
| CBBD teams with no usable ESPN id | **0 of 365** |

## Why the provider id is refused outright

The two feeds number teams independently, and both number them from small
integers. So the id spaces do not merely disagree — they **overlap on the wrong
rows**. A naive join does not fail, raise, or return nothing. It returns a
team, confidently, and that team is some other school:

| CBBD team | its id | the football team that id means |
| --- | ---: | --- |
| Florida Gators | 87 | Notre Dame |
| Colorado State Rams | 57 | Florida |
| Nevada Wolf Pack | 189 | Bowling Green |
| Siena Saints | 264 | Washington |
| San Diego Toreros | 258 | Virginia |
| Cal State Northridge Matadors | 38 | Colorado |

81 of the 365 basketball teams would silently resolve to a football team this
way. This is exactly the failure the roadmap's risk register names — *"Identity
mismatches → wrong team or game association → fail closed and require reviewed
mapping"* — and it is the reason each league's own `identity.py` already refuses
a bare provider id as an entity key. The same rule now applies across leagues.

## The key that does work

**The ESPN team id**, which both sides already carry:

- The football registry stores it per team (`provider_ids.espn`), for all 139.
- CBBD embeds it in its logo URLs — `.../cbb-logos/500/<espn_id>.png` — for
  **365 of 365** teams, so no team needs a hand-written mapping.

On the 2026 data this pairs **139 of 139** FBS football schools with a
basketball team and leaves no football team unmatched. **Institutional grouping
is therefore a join, not a curated mapping table**, which is the question item 3
existed to settle.

### One honest correction

A first pass at this measurement reported 25 schools where the two feeds
"named the same school differently". That was wrong: those 25 were artifacts of
the measurement's own name-matching fallback, which trimmed mascots and so
matched *Purdue Fort Wayne Mastodons* to *Purdue* and *Idaho State Bengals* to
*Idaho*. Joined on the ESPN id instead, **display names agree for all 139 real
pairs**.

So a name join would in fact have worked on today's data. It is still not what
`src/institution_identity.py` uses, for two reasons: a display name is a label
the provider can restyle at any time, and the near-misses that broke the first
measurement — *Idaho* against *Idaho State*, *Illinois* against *Illinois
State*, *Northwestern* against *Northwestern State* — are precisely the pairs a
name join gets wrong while looking right. The id costs nothing, so there is no
reason to spend a guess on it.

This correction is recorded rather than quietly fixed because the first number
was reported inside this project before it was checked, and because it is a
clean instance of a recurring lesson here: a measurement that uses a fuzzy
matcher is measuring the matcher too.

## What was built

`src/institution_identity.py`, with `tests/test_institution_identity.py` (15
tests, 5 mutants verified caught).

- `resolve(espn_id, season)` → `{espn_id, football, basketball}` or `None`.
- `institutions(season)` → every school in either feed, with the missing side
  `None`. A basketball-only school is a valid entry, not an error: 226 of them
  exist and a reader may well favourite one.
- `unresolved_basketball_teams(season)` → teams the module cannot key, with the
  reason, following `identity-problems.md`'s practice of counting coverage gaps
  rather than dropping them. Currently empty.

It **fails closed**: no name fallback, no nearest match, and a non-integer id
returns `None`. Two basketball teams claiming one ESPN id are **both** dropped
and reported rather than one winning, and a duplicate ESPN id in the football
registry raises, because that registry is reviewed by hand and a duplicate there
is a defect rather than a feed quirk.

```
$ python3 src/institution_identity.py --season 2026
season 2026: 365 institutions
  both leagues:    139
  football only:   0
  basketball only: 226
  unkeyable basketball teams: 0
```

## What this does not settle

- **Non-FBS football.** The registry holds 139 FBS teams. Schools with FCS or no
  football are basketball-only here, which is correct for this registry but is
  not the same as a complete picture of college athletics.
- **NFL and NBA.** Those are professional leagues with no shared institution, so
  they need no join — but My Teams still has to present all four leagues' teams
  in one list, which is a presentation problem this module does not touch.
- **People.** A player who appears in both sports is not addressed. `src/person_identity.py`
  and `identity-problems.md` own that, and nothing here changes it.
- **Seasons.** Measured on 2026 only. The join key is stable by construction, but
  the 139/365 counts are a snapshot, and the roadmap's own lesson applies: a
  measurement on one season is not a claim about the archive.
