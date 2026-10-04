# Identity problems in the people data

Every person on this site comes from CFBD, and CFBD's own idea of who somebody
is has limits. This note records each limit that is known, what it costs a
reader, what the code does about it, and what is still an open decision. The
numbers are measured against the live database on 2026-10-04 — rosters 2009
to 2026, season statistics 2009 to 2025, coaches 1980 to 2026, box scores 2004
to 2026 — not estimated.

The governing rule, from the v0.1.1 guide: *a correct identity graph with no
headshots is shippable; beautiful pages built on misidentified people are not.*
So everything below is either fixed, or flagged in words on the page that shows
it. Nothing here is silently guessed.

## 1. CFBD holds two athlete ids for one person

**The defect.** A statistic attaches to a person by athlete id. CFBD sometimes
carries two ids under one name at one school in one season, and each id then
holds a fraction of that season. A page printing one id's totals asserts the
fraction as the whole.

**Measured.** 172 player-seasons, 170 people, across 11 seasons. Of the 98
name-and-season groups that resolved to a person here, **74 have both ids in
this database** — two pages, each holding part of one career, and neither looks
short. (The load-time measurement in the changelog counts the raw feed instead:
104 name-and-team groups, 85 with both ids on a roster. This one counts what a
reader can actually reach from a page.) The example that found it: Sherod
White's 2022 at New Mexico read 17 carries for 41 yards, while the other id
held 23 carries, 101 yards and 3 touchdowns.

**What the code does.** `player_season_stat_caveats` records the season, and
the page daggers that season in every category table and words the reason once
underneath. The caveat hangs on the *season*, not the person, so a 2018 total
is not caveated because 2022 was split.

**Why they are not merged.** The feed gives no way to tell one person recorded
twice from two players who share a name on one roster — and both exist. Merging
would be a guess that silently fuses two people, which is worse than a flagged
fraction.

**Open decision.** Whether to hand-merge the 74. That is a judgement about 74
specific people, not a rule, so it is the project owner's call rather than the
code's.

## 2. A coach has no identifier at all

CFBD's `/coaches` feed carries **no coach id**, and each record is one season
rather than a career. The name is the only person-level signal, so `coach_id`
is derived from the normalized name and stored in `coach_external_ids`, ready
for a real-id source to replace it.

`hire_date` belongs to the **job**, not the person: Al Golden carries
2005-12-08 for five Temple seasons and 2010-12-12 for five Miami seasons. Two
designs were tried and abandoned against the real feed — grouping on
`(name, hire_date)` splits one person per job, grouping on overlapping seasons
splits one person per season.

**Measured.** 827 coaches, 5,657 tenures. 160 name-only identities span more
than one hire date (106 span two, 39 three, 10 four, 5 five) and are recorded
in `person_unresolved` rather than asserted either way.

**What this costs a reader.** Two head coaches who share a name are one page
here. There is no fix available from this feed; it needs a source that numbers
coaches.

## 3. An athlete id can span an implausible career

**Measured.** 446 of 99,813 athlete ids cover more than six seasons.

This is **not** an error to correct. Athlete 4571882 has 13 rows at 3 schools
all reading "LB, #2, SR", which is plainly a reused id — but Cam McCormick's
nine seasons are real, on NCAA injury waivers. The code keeps every row, flags
the person (`identity.flag_implausible_careers`), and the page says so in
words. The flag is keyed on `player_id`, never on the display name, so two John
Smiths cannot share one caveat.

## 4. A name is a lookup key, never an identity

Resolution order is: the source's athlete id; then name **and** team **and**
season **and** position with exactly **one** candidate; then nothing, recorded
in `person_unresolved`. A second candidate is a collision, not a tie-break.

This is why a box-score line for two players sharing a name on one roster is
left as plain text rather than linked to whichever matched first, and why
`load_player_season_stats` has no name fallback at all: a roster says who was
*present*, a statistic says what somebody *did*.

## 5. A numbered line whose person has no page here

The box-score archive now carries CFBD's athlete ids for all 23 seasons, so a
line the source numbered links to that person because the source says whose
line it is. Two cases still do not link, deliberately:

- **The id has no page here.** Rosters begin in 2009, so a 2004 line can carry
  a perfectly good id for somebody this database never saw. Linking it would
  send a reader to "player not found".
- **And in that case the name is not used either.** The only name such a line
  could match is a *different* athlete id on the same roster — case 1 above, or
  two players sharing a name. A fallback there would put one player's work on
  another player's page while the source had already named somebody else.

## 6. The feed's team names are not this database's

Fixed 2026-10-04, and it had been costing readers in every season. The archive
carries CFBD's team names; a page looks a team up by the canonical name used
here. Six FBS teams differ:

| CFBD | here |
|---|---|
| UL Monroe | ULM |
| San José State | San Jose State |
| App State | Appalachian State |
| Florida Atlantic | FAU |
| Florida International | FIU |
| Miami | Miami (FL) |

Until the fix, those teams' rosters were never found, so **not one of their
player lines linked to anybody, in any season** — on Georgia–UL Monroe 2015, 79
of 159 rows. The six were already `team_aliases` rows; nothing was consulting
them when exporting box scores. Note that the feed's bare "Miami" means Miami
(FL) while its "Miami (OH)" matches a real team, so a canonical name is never
captured by an alias.

## 7. What `person_unresolved` is, and is not

119,054 rows, and the large majority are **not** identity failures:

| rows | reason |
|---|---|
| 87,431 | unknown team — a school outside this FBS-only database |
| 30,891 | an athlete id with no roster row here |
| 446 | a source id spanning an implausible career (§3) |
| 160 | a coach's name-only identity spanning several hire dates (§2) |
| 67 | a coach's name-only identity with a gap in its seasons |
| 59 | a coach at a school outside this database |

Reporting the first two as misidentified people would be wrong. They are
coverage boundaries: this is an FBS database, and a player at an FCS school is
absent by design rather than by failure. Of the 30,891, only **14** name a
school this database does carry.

## 8. Ids are a function of the source, never of load order

`db/league.db` is gitignored and rebuilt from scratch on every deploy, so a
published id that depended on insert order would point at a different person
after each build. `player_id` **is** the CFBD athlete id, verbatim: 29,162 of
the 99,813 are negative, which is CFBD's own placeholder form, and the sign is
kept. Routes accept `-?\d{1,13}`, and both the exporter and the page shard on a
*floored* modulo, because JavaScript's `-1044360 % 64` is -56 where Python's is
56.

A coach id is derived from the feed's name key. Where no source id exists at
all, an id is derived from `(name, team, season)` above `SURROGATE_BASE`
(10¹²); there are currently none.

This was a real defect, caught in review: coach pages shipped to main with
load-ordered ids before `identity.migrate_player_ids` and the coach remap moved
an existing database onto source ids.

## Coverage boundaries that look like bugs and are not

- Rosters, and therefore people, begin in **2009**.
- `/coaches` is **head coaches only** — no coordinators or assistants. Never
  present it as a coaching staff.
- `defensive` and `fumbles` statistics begin in **2016**; before that CFBD
  sends eight categories, so a defensive player's page for 2009–2015 is
  genuinely empty.
- 2026 has no season-statistics snapshot yet: `sync-people.yml`'s
  `season_stats` input is off by default, so the current season must be asked
  for explicitly.
- 1,625 of the 31,382 rows in the 2026 roster snapshot are stubs: no position,
  jersey, height, weight, hometown or class year. They are people the feed knows
  by name only. `class_year_label` accepts only 1–5 because CFBD has also sent
  a *season* in that field, which would otherwise print as a class — in the
  current snapshot every stub row's `year` is simply null.
- Two athlete ids appear twice in one roster season — mid-year transfers, on
  both schools' rosters. The `(player_id, season_year, team_id)` key carries
  both correctly.

## How to check any of this again

```sh
python3 - <<'PY'
import sqlite3
db = sqlite3.connect('db/league.db')
for entity, reason, n in db.execute(
        "SELECT entity, reason, COUNT(*) FROM person_unresolved "
        "GROUP BY entity, reason ORDER BY 3 DESC"):
    print(f"{n:>7}  {entity:<7} {reason}")
PY
```

A measurement on one season is not a claim about the archive: two statements in
these docs were true of 2025 alone and false across seventeen seasons. When the
data widens, re-measure.

See also [people-pages.md](people-pages.md) for how the pages are built, and
[CHANGELOG.md](CHANGELOG.md) for what shipped when.
