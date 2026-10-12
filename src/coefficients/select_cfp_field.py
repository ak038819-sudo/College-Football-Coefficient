#!/usr/bin/env python3
"""The real College Football Playoff: 12 teams, 5 automatic bids, straight seeding.

This replaces the invented 24-team field as the live model (owner's
decision, 2026-10-10: "convert our current bracket to run the irl format
for the time being"). The 24-team selector is still in the repository at
select_playoff_field_v2.py and still runs, so the old model is a config
change away rather than a rewrite away -- but nothing live reads it now.

The format, as the CFP actually runs it since the 2025 season:

  * The five highest-ranked conference champions get automatic bids.
  * Seven at-large bids fill the field.
  * Seeds 1-12 are assigned straight off the ranking, which is what
    changed for 2025: the 2024 bracket gave the top four seeds to the
    top four champions regardless of where they ranked.
  * Seeds 1-4 sit out the first round.
  * First round: 5v12, 6v11, 7v10, 8v9, at the higher seed's home.
  * Quarterfinals, semifinals and the final are at neutral sites.

WHAT STANDS IN FOR THE SELECTION COMMITTEE

The real format needs a national ranking, and this project has no
committee. It uses **Elo as it stood entering the postseason** of the
season being selected -- the live rating engine's own number, the one on
every game card and the one the Track Record page scores. Every ordering
below is that one number, with the team name as a deterministic tiebreak:
which champions are the top five, which seven teams are the best
available at-large, and the seeds themselves.

Elo replaced the 5-year rolling coefficient here on the owner's call
(2026-10-12: "I'd rather use Elo as the committee standin as it is the
more refined metric"). It is the better stand-in for a measurable reason
rather than a stylistic one: for 2024, the only completed season this
project covers that the real committee seeded as a twelve-team field,
Elo's twelve contain ten of the committee's twelve where the rolling
coefficient's contain seven, and the coefficient adds Alabama,
Washington, Oklahoma and LSU, none of whom were selected. A five-year
window cannot do better, because four of the five seasons it averages
have already happened: a team that collapses this autumn carries four
prior seasons with it, and across 2014-2025 switching metrics moves a
median of four teams in and out of the field.

It is still a strength rating rather than a resume ranking, so it is
explicitly not the committee's question -- Elo rates how good a team is,
not what its season deserves, and it carries 80% of last season's rating
into this one by design (offseason_retention). Read a field from this
model as "the twelve teams the engine rates highest, with a champion
floor".

WHAT CHANGED BY DROPPING THE 24-TEAM MODEL

  * No pots and no draw. The 24-team bracket paired a random Pot 1 team
    against a random Pot 2 team, so its title odds had to be averaged
    over draws to mean anything. A 12-team bracket is fully determined by
    the seeds, so the odds now describe the one real bracket.
  * No conference bid table. Bids are no longer allocated per conference
    by conference CoE rank; conference CoE now only decides nothing here
    at all. Every bid past the five champions is won on team strength.
  * No independent threshold. Independents could not belong to a
    bid-eligible conference under the old model, so they needed a special
    rule to get in at all. Here they are simply at-large candidates like
    everyone else, which is also the real rule -- an independent cannot
    win a conference, so it cannot hold an automatic bid.

Usage:
    python src/coefficients/select_cfp_field.py --year 2025
"""
from __future__ import annotations

import argparse
import sqlite3

FIELD_SIZE = 12
AUTO_BIDS = 5        # the five highest-ranked conference champions
BYE_SEEDS = 4        # seeds 1-4 sit out the first round
AT_LARGE_BIDS = FIELD_SIZE - AUTO_BIDS

# Not a conference. It has no championship and therefore no automatic bid,
# which is the real rule as well as this project's long-standing treatment.
INDEPENDENTS = "FBS Independents"

# First round, by seed. The higher seed hosts, which under straight seeding
# is always the first of each pair.
FIRST_ROUND_PAIRS = [(5, 12), (6, 11), (7, 10), (8, 9)]

# Quarterfinals: (bye seed, the first-round pair whose winner it meets).
# This is the standard 1/8-9 and 4/5-12 half against the 2/7-10 and 3/6-11
# half, so the overall seeds 1 and 2 can only meet in the final.
QUARTERFINALS = [(1, (8, 9)), (4, (5, 12)), (2, (7, 10)), (3, (6, 11))]


# What the ranking is, named once. The pages, the docs and the exports read
# this rather than each carrying the word "Elo" or "CoE" of their own.
RANKING_LABEL = "Elo"
RANKING_NOTE = "Elo entering the postseason"

# A team the ranking has no number for sorts last. It cannot be ranked above a
# team that has played, and the engine's own 1500 prior would do exactly that
# to a first-year FBS team with no games -- so unranked means last here, not
# average.
UNRANKED = float("-inf")

# The last phase of a season before the postseason. The committee ranks teams
# on the season as played up to this point, so the playoff must not be seeded
# by ratings that already contain playoff results.
PRE_POSTSEASON_PHASE = "regular"

ELO_ENTERING_POSTSEASON = f"""
WITH last_game AS (
  SELECT e.team_id, e.postgame_elo,
         ROW_NUMBER() OVER (PARTITION BY e.team_id
                            ORDER BY g.game_date DESC, g.game_id DESC) AS rn
  FROM elo_game_history e
  JOIN games g ON g.game_id = e.game_id
  WHERE g.season_year = ? AND g.game_phase = '{PRE_POSTSEASON_PHASE}'
)
SELECT t.team_id, t.team_name, last_game.postgame_elo
FROM last_game JOIN teams t ON t.team_id = last_game.team_id
WHERE last_game.rn = 1
"""


def load_team_elo(conn: sqlite3.Connection, season_year: int,
                  by_id: bool = False) -> dict:
    """team -> Elo entering that season's postseason. The ranking.

    Keyed by team name, or by team_id when `by_id` -- the projection plays a
    schedule of ids and has to start from the same numbers this selects by.

    For the season being played, "entering the postseason" is simply the
    latest rating there is, so this is also where a live projection starts.

    Read off the stored chain rather than recomputed: each game's postgame_elo
    is the next game's pregame, so a team's last regular-season game carries
    the rating it took into the postseason.

    Ordered by game_date, not by week. CFBD numbers some postseason games
    week 1, and a season ordered by week puts a bowl game first -- the mistake
    that made 1,353 season Elo changes exactly zero when the coach metrics
    were first built.
    """
    rows = conn.execute(ELO_ENTERING_POSTSEASON, (season_year,)).fetchall()
    return {(tid if by_id else name): elo for tid, name, elo in rows}


def rank_key(ranking: dict[str, float]):
    """Strongest first, with the team name breaking ties deterministically.

    Two teams on an identical rating must not seed differently from one run to
    the next, or a bracket stops being reproducible from its inputs.
    """
    return lambda team: (-ranking.get(team, UNRANKED), team)


def conference_champions(conn: sqlite3.Connection, season_year: int) -> dict[str, str]:
    """conference -> its champion, for every real conference with standings.

    The champion is conf_rank 1 of the derived conference standings, the same
    order the 24-team model used. Independents are skipped: no conference, no
    championship, no automatic bid.
    """
    rows = conn.execute(
        """
        SELECT s.conference, t.team_name
        FROM conference_standings_by_year s
        JOIN teams t ON t.team_id = s.team_id
        WHERE s.season_year = ? AND s.conf_rank = 1 AND s.conference != ?
        """,
        (season_year, INDEPENDENTS),
    ).fetchall()
    return {conf: team for conf, team in rows}


def fbs_conference_of(conn: sqlite3.Connection, season_year: int) -> dict[str, str]:
    """Every FBS team that season -> its conference. The at-large pool.

    Membership rather than standings, so a team whose conference has no
    derived standings that year is still eligible for an at-large bid, and so
    independents are in the pool.
    """
    rows = conn.execute(
        """
        SELECT t.team_name, m.conference_real
        FROM team_membership_by_season m
        JOIN teams t ON t.team_id = m.team_id
        WHERE m.season_year = ? AND m.is_fbs = 1
        """,
        (season_year,),
    ).fetchall()
    return {team: conf for team, conf in rows}


def select_field(conn: sqlite3.Connection, season_year: int,
                 ranking: dict[str, float]) -> list[dict]:
    """The seeded 12-team field, seed 1 first.

    Each entry: team, conference, bid_type ('auto' or 'at_large'), seed,
    rating, and champion_of (the conference it won, or None).

    `ranking` is the committee stand-in, injected rather than loaded, so this
    function knows the FORMAT and nothing about which metric ranks it -- which
    is what let the metric change in one place, and what lets the
    rest-of-season projection hand in a simulated season's own ratings.

    Raises if the season cannot fill a 12-team field, rather than returning a
    short one that every caller downstream would then have to check.
    """
    champions = conference_champions(conn, season_year)
    conf_of = fbs_conference_of(conn, season_year)
    by_rank = rank_key(ranking)

    champion_of = {team: conf for conf, team in champions.items()}
    auto = sorted(champion_of, key=by_rank)[:AUTO_BIDS]

    pool = [t for t in sorted(conf_of, key=by_rank) if t not in set(auto)]
    at_large = pool[:AT_LARGE_BIDS]

    field = auto + at_large
    if len(field) != FIELD_SIZE:
        raise RuntimeError(
            f"{season_year}: built a {len(field)}-team field, expected {FIELD_SIZE} "
            f"({len(auto)} champions, {len(at_large)} at-large from a pool of {len(pool)})"
        )

    auto_set = set(auto)
    return [
        {
            "team": team,
            "conference": conf_of.get(team, champion_of.get(team, "")),
            "bid_type": "auto" if team in auto_set else "at_large",
            "champion_of": champion_of.get(team),
            "seed": seed,
            "rating": ranking.get(team, 0.0),
        }
        for seed, team in enumerate(sorted(field, key=by_rank), start=1)
    ]


def first_round_games(field: list[dict]) -> list[dict]:
    """The four first-round games, higher seed hosting."""
    by_seed = {q["seed"]: q for q in field}
    games = []
    for high, low in FIRST_ROUND_PAIRS:
        host, visitor = by_seed[high], by_seed[low]
        games.append({
            "home_seed": high, "away_seed": low,
            "home": host["team"], "away": visitor["team"],
            "home_conf": host["conference"], "away_conf": visitor["conference"],
            "home_rating": host["rating"], "away_rating": visitor["rating"],
        })
    return games


def byes(field: list[dict]) -> list[dict]:
    """Seeds 1-4, in seed order."""
    return [q for q in field if q["seed"] <= BYE_SEEDS]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/league.db")
    p.add_argument("--year", type=int, required=True)
    args = p.parse_args()

    conn = sqlite3.connect(args.db)
    ranking = load_team_elo(conn, args.year)
    champions = conference_champions(conn, args.year)
    field = select_field(conn, args.year, ranking)
    conn.close()

    print(f"=== {args.year} College Football Playoff field (12 teams, {AUTO_BIDS} automatic) ===")
    print(f"    ranked by {RANKING_NOTE}\n")
    print(f"Conference champions ({len(champions)}), strongest first:")
    for team in sorted(champions.values(), key=rank_key(ranking)):
        conf = next(c for c, t in champions.items() if t == team)
        got = any(q["team"] == team and q["bid_type"] == "auto" for q in field)
        print(f"  {team:<22} {conf:<20} {ranking.get(team, 0.0):>8.0f}  "
              f"{'automatic bid' if got else '-'}")

    print(f"\n{'Seed':>4}  {'Team':<22} {'Conference':<20} {'Bid':<10} {RANKING_LABEL:>8}")
    for q in field:
        print(f"{q['seed']:>4}  {q['team']:<22} {q['conference']:<20} "
              f"{q['bid_type']:<10} {q['rating']:>8.0f}")

    print(f"\nByes to the quarterfinals: " +
          ", ".join(f"({q['seed']}) {q['team']}" for q in byes(field)))
    print("\nFirst round (higher seed hosts):")
    for g in first_round_games(field):
        print(f"  ({g['away_seed']}) {g['away']:<22} @ ({g['home_seed']}) {g['home']}")
    print("\nQuarterfinals:")
    by_seed = {q["seed"]: q["team"] for q in field}
    for bye_seed, (hi, lo) in QUARTERFINALS:
        print(f"  ({bye_seed}) {by_seed[bye_seed]:<22} vs winner of {hi}/{lo}")


if __name__ == "__main__":
    main()
