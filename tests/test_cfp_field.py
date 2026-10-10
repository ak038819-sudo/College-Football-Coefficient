"""Invariants of the real 12-team College Football Playoff field and bracket.

These drive the live selector and the live simulation over every season the
project has standings for, not a season someone happened to eyeball. The
properties worth holding are the ones a reader of the site would notice if
they broke: twelve teams, five automatic bids going to the five strongest
champions and nobody else, seeds that are the ranking, and a bracket whose
halves keep seeds 1 and 2 apart until the final.

The invented 24-team model's own invariants (8 byes, two pots of 8) live in
test_playoff_field.py and still hold for that selector, which is still in
the repository. They are not properties of what the site shows.
"""
import random

import pytest

from conftest import PLAYOFF_YEARS
from select_cfp_field import (
    AUTO_BIDS, BYE_SEEDS, FIELD_SIZE, FIRST_ROUND_PAIRS, INDEPENDENTS,
    QUARTERFINALS, byes, conference_champions, fbs_conference_of,
    first_round_games, load_team_coe_5yr, rank_key, select_field,
)
from simulate_cfp_bracket import ROUND_ORDER, run_simulation, simulate_one_bracket


def _field(db_conn, year):
    return select_field(db_conn, year, load_team_coe_5yr(year))


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_the_field_is_twelve_teams_seeded_one_through_twelve(db_conn, year):
    field = _field(db_conn, year)
    assert len(field) == FIELD_SIZE == 12
    assert [q["seed"] for q in field] == list(range(1, 13))
    assert len({q["team"] for q in field}) == FIELD_SIZE, "a team was selected twice"


def test_the_format_is_the_real_one():
    """The literals, not the constants.

    Every other test below reads these names, so a test that only compares a
    count to AUTO_BIDS passes whatever AUTO_BIDS happens to be -- which it did,
    until this test existed. This is the one place the numbers are spelled out,
    so changing the format has to come here and say so.
    """
    assert (FIELD_SIZE, AUTO_BIDS, BYE_SEEDS) == (12, 5, 4)
    assert FIRST_ROUND_PAIRS == [(5, 12), (6, 11), (7, 10), (8, 9)]


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_exactly_five_automatic_bids_and_seven_at_large(db_conn, year):
    field = _field(db_conn, year)
    auto = [q for q in field if q["bid_type"] == "auto"]
    at_large = [q for q in field if q["bid_type"] == "at_large"]
    assert len(auto) == AUTO_BIDS == 5
    assert len(at_large) == FIELD_SIZE - AUTO_BIDS == 7
    assert len(auto) + len(at_large) == FIELD_SIZE, "a bid_type outside the two kinds"


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_an_automatic_bid_goes_only_to_a_conference_champion(db_conn, year):
    champions = conference_champions(db_conn, year)
    champion_teams = set(champions.values())
    for q in _field(db_conn, year):
        if q["bid_type"] == "auto":
            assert q["team"] in champion_teams, f"{year}: {q['team']} holds an automatic bid"
            assert champions[q["champion_of"]] == q["team"]
        else:
            assert q["champion_of"] is None


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_the_five_automatic_bids_are_the_five_strongest_champions(db_conn, year):
    """The cut has to be the ranking, or the format's one rule means nothing."""
    team_coe = load_team_coe_5yr(year)
    champions = sorted(conference_champions(db_conn, year).values(), key=rank_key(team_coe))
    auto = {q["team"] for q in _field(db_conn, year) if q["bid_type"] == "auto"}
    assert auto == set(champions[:AUTO_BIDS])


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_no_at_large_bid_passes_over_a_stronger_eligible_team(db_conn, year):
    """Every at-large is the best team left, which is the only rule they follow."""
    team_coe = load_team_coe_5yr(year)
    field = _field(db_conn, year)
    chosen = {q["team"] for q in field}
    auto = {q["team"] for q in field if q["bid_type"] == "auto"}
    weakest_at_large = min(
        (q["team_coe_5yr"] for q in field if q["bid_type"] == "at_large"))
    for team in fbs_conference_of(db_conn, year):
        if team in chosen or team in auto:
            continue
        assert team_coe.get(team, 0.0) <= weakest_at_large, (
            f"{year}: {team} ({team_coe.get(team, 0.0):.3f}) was left out while a weaker "
            f"team took an at-large bid ({weakest_at_large:.3f})"
        )


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_seeds_run_straight_down_the_ranking(db_conn, year):
    """The 2025 rule change. Under the 2024 rule the top four seeds were the top
    four champions regardless of rank, so this is the assertion that says which
    format the site is running."""
    field = _field(db_conn, year)
    coe = [q["team_coe_5yr"] for q in field]
    assert coe == sorted(coe, reverse=True), f"{year}: seeds are not in ranking order"


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_an_independent_can_be_selected_but_never_automatically(db_conn, year):
    """No conference, no championship, no automatic bid -- the real rule."""
    for q in _field(db_conn, year):
        if q["conference"] == INDEPENDENTS:
            assert q["bid_type"] == "at_large", f"{year}: {q['team']} took an automatic bid"


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_the_top_four_seeds_sit_out_the_first_round(db_conn, year):
    field = _field(db_conn, year)
    assert [q["seed"] for q in byes(field)] == [1, 2, 3, 4]
    playing = {g["home_seed"] for g in first_round_games(field)} | \
              {g["away_seed"] for g in first_round_games(field)}
    assert playing == set(range(BYE_SEEDS + 1, FIELD_SIZE + 1))


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_the_higher_seed_hosts_the_first_round(db_conn, year):
    for g in first_round_games(_field(db_conn, year)):
        assert g["home_seed"] < g["away_seed"]
        # And the host is actually the stronger team, since seeding IS the
        # ranking -- so the fitted home-field edge is never handed to the
        # weaker side.
        assert g["home_coe"] >= g["away_coe"]


def test_the_bracket_shape_keeps_the_top_two_seeds_apart_until_the_final():
    """Structural, no database: the halves are what makes a bracket a bracket.

    Seeds 1 and 2 meeting in a semifinal would make the whole seeding
    pointless, and nothing else in the code would complain.
    """
    first_half = {QUARTERFINALS[0][0], QUARTERFINALS[1][0]}
    second_half = {QUARTERFINALS[2][0], QUARTERFINALS[3][0]}
    assert first_half == {1, 4} and second_half == {2, 3}
    # Every seed appears exactly once, as a bye or in exactly one first-round game.
    bye_seeds = [q[0] for q in QUARTERFINALS]
    paired = [s for pair in FIRST_ROUND_PAIRS for s in pair]
    assert sorted(bye_seeds + paired) == list(range(1, FIELD_SIZE + 1))
    # And each quarterfinal names a first-round game that exists.
    assert sorted(pair for _, pair in QUARTERFINALS) == sorted(FIRST_ROUND_PAIRS)


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_one_simulated_bracket_advances_exactly_one_team_per_round(db_conn, year):
    """Drives the real simulation: the counts a bracket must produce by shape."""
    team_coe = load_team_coe_5yr(year)
    field = _field(db_conn, year)
    result = simulate_one_bracket(field, team_coe, 4.5, random.Random(7), 1.25)

    assert set(result) == {q["team"] for q in field}, "a team vanished from the bracket"
    reached = lambda r: [t for t, v in result.items() if v == r]
    # 4 first-round losers, 4 quarterfinal losers, 2 semifinal losers,
    # 1 runner-up, 1 champion.
    assert len(reached("first_round")) == 4
    assert len(reached("qf")) == 4
    assert len(reached("sf")) == 2
    assert len(reached("final")) == 1
    assert len(reached("champion")) == 1
    # A bye seed can never be eliminated in a round it does not play.
    for q in byes(field):
        assert result[q["team"]] != "first_round"


def _synthetic_field(coe_by_seed):
    """A 12-team field with CoE chosen per seed, for the structural cases below.

    Real data cannot be made to produce them: no season obliges the top two
    seeds to win every game, and none has twelve equally strong teams.
    """
    field = [{"seed": seed, "team": f"Seed{seed:02d}", "conference": f"Conf{seed}",
              "bid_type": "at_large", "champion_of": None,
              "team_coe_5yr": coe_by_seed(seed)}
             for seed in range(1, FIELD_SIZE + 1)]
    return field, {q["team"]: q["team_coe_5yr"] for q in field}


def test_the_top_two_seeds_can_only_meet_in_the_final():
    """The halves, through the real simulation rather than through the table.

    QUARTERFINALS being right on paper is not the same as the simulation
    pairing semifinalists by it. Give seeds 1 and 2 a CoE no one can beat and
    they must both reach the final; if they share a half they meet a round
    early and only one of them gets there.
    """
    field, team_coe = _synthetic_field(lambda seed: 1000.0 if seed <= 2 else 1.0)
    for run in range(25):
        result = simulate_one_bracket(field, team_coe, 4.5, random.Random(run), 1.25)
        finalists = {t for t, v in result.items() if v in ("final", "champion")}
        assert finalists == {"Seed01", "Seed02"}, (
            f"run {run}: the final was {sorted(finalists)} -- the top two seeds "
            "met before it, so the bracket halves are wrong"
        )


def test_only_the_first_round_is_played_at_a_host():
    """Every round after the first is neutral, and nothing else says so.

    With twelve equally strong teams and a decisive venue edge, the first round
    is settled entirely by who hosts. If that edge leaked into a later round the
    bye seeds -- who are nominally 'home' in the quarterfinal pairing order --
    would win far more than half of them.
    """
    field, team_coe = _synthetic_field(lambda seed: 5.0)
    decisive = 500.0

    hosts_won = 0
    bye_qf_wins = 0
    runs = 300
    for run in range(runs):
        result = simulate_one_bracket(field, team_coe, 4.5, random.Random(run), decisive)
        for high, low in FIRST_ROUND_PAIRS:
            if result[f"Seed{low:02d}"] == "first_round":
                hosts_won += 1
        for bye_seed, _ in QUARTERFINALS:
            if ROUND_ORDER.index(result[f"Seed{bye_seed:02d}"]) > ROUND_ORDER.index("qf"):
                bye_qf_wins += 1

    assert hosts_won == runs * len(FIRST_ROUND_PAIRS), (
        "the host did not win every first-round game, so the venue edge is not "
        "being applied in the round that has a host"
    )
    share = bye_qf_wins / (runs * len(QUARTERFINALS))
    assert 0.40 < share < 0.60, (
        f"bye seeds won {share:.0%} of quarterfinals between equal teams; a "
        "neutral site means about half, so a venue edge is leaking past the "
        "first round"
    )


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_the_published_odds_are_a_probability_distribution(db_conn, year):
    counts, n_sims, field, _ = run_simulation(
        str(_db_path(db_conn)), year, 400, 4.5, sim_seed=3, home_field=1.25)
    assert set(counts) == {q["team"] for q in field}
    # Exactly one champion per run, so the champion counts sum to the run count.
    assert sum(c["champion"] for c in counts.values()) == n_sims
    assert sum(c["final"] for c in counts.values()) == 2 * n_sims
    assert sum(c["sf"] for c in counts.values()) == 4 * n_sims
    assert sum(c["qf"] for c in counts.values()) == 8 * n_sims
    # Reaching a later round implies reaching every earlier one.
    for team, c in counts.items():
        for earlier, later in zip(ROUND_ORDER, ROUND_ORDER[1:]):
            assert c[earlier] >= c[later], f"{year}: {team} {later} > {earlier}"


@pytest.mark.parametrize("year", PLAYOFF_YEARS)
def test_a_bye_seed_is_in_the_quarterfinals_in_every_run(db_conn, year):
    """The whole value of a top-four seed, and it must be exactly 100%."""
    counts, n_sims, field, _ = run_simulation(
        str(_db_path(db_conn)), year, 200, 4.5, sim_seed=11, home_field=1.25)
    for q in byes(field):
        assert counts[q["team"]]["qf"] == n_sims, f"{year}: seed {q['seed']} missed a quarterfinal"


def _db_path(db_conn):
    """The file behind the fixture's connection, so run_simulation can open its own."""
    for _, name, file in db_conn.execute("PRAGMA database_list"):
        if name == "main":
            return file
    raise AssertionError("no main database on the connection")
