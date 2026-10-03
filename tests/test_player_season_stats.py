"""Season player statistics: what the fetcher keeps, and what it refuses to keep.

The rule these exist to hold: a statistic is attached to a person because the
source numbered them, never because two names looked alike.
"""
import pytest

from fetch_cfbd_player_season_stats import FIRST_SEASON, stat_rows


def _row(**over):
    row = {"playerId": 4361182, "player": "Bryce Young", "team": "Alabama",
           "conference": "SEC", "category": "passing", "statType": "YDS", "stat": 4872}
    row.update(over)
    return row


def test_a_stat_row_keeps_the_athlete_id_the_feed_gave():
    [row] = stat_rows([_row()], 2021)
    assert row == {"athlete_id": "4361182", "name": "Bryce Young", "team": "Alabama",
                   "conference": "SEC", "season_year": 2021, "category": "passing",
                   "stat_type": "YDS", "stat": 4872}


def test_a_row_with_no_athlete_id_is_dropped_rather_than_matched_by_name():
    """The whole reason this endpoint is used instead of the box-score archive.
    A row the source will not number cannot be attached to a person, and a page
    showing one player's yards under another player's name is worse than a page
    showing none."""
    assert stat_rows([_row(playerId=None)], 2021) == []
    assert stat_rows([{"player": "Bryce Young", "category": "passing",
                       "statType": "YDS", "stat": 1}], 2021) == []


@pytest.mark.parametrize("missing", ["player", "category", "statType"])
def test_a_row_missing_what_identifies_the_statistic_is_dropped(missing):
    """A stat with no category or type cannot be labelled on a page, and an
    unlabelled number is not a fact about anybody."""
    assert stat_rows([_row(**{missing: None})], 2021) == []


@pytest.mark.parametrize("value,expected", [
    (4872, 4872), ("4872", 4872), (12.5, 12.5), ("12.5", 12.5),
    # The feed really sends these: completions as a pair, a long as "T80" when
    # the longest play was tied. Coercing them would invent a value.
    ("19/30", "19/30"), ("T80", "T80"),
    (None, None), ("", None),
])
def test_a_stat_is_a_number_only_where_the_source_gave_one(value, expected):
    [row] = stat_rows([_row(stat=value)], 2021)
    assert row["stat"] == expected


def test_the_feeds_own_key_spellings_are_both_read():
    """CFBD has shipped both camelCase and snake_case for these fields. Reading
    one spelling only would archive an empty season and look like real coverage."""
    [row] = stat_rows([{"athlete_id": 99, "name": "Snake Case", "school": "Oregon",
                        "category": "rushing", "stat_type": "YDS", "value": 100}], 2015)
    assert (row["athlete_id"], row["name"], row["team"], row["category"],
            row["stat_type"], row["stat"]) == ("99", "Snake Case", "Oregon", "rushing",
                                               "YDS", 100)


def test_a_payload_that_is_not_a_list_is_an_error_not_an_empty_season():
    """An error object written as a snapshot would look like a season in which
    nobody recorded a statistic."""
    with pytest.raises(ValueError):
        stat_rows({"error": "unauthorized"}, 2021)


def test_the_coverage_boundary_is_stated_rather_than_assumed():
    assert FIRST_SEASON == 2004
