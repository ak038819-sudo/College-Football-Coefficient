"""The box-score limit probe: that it can tell a truncation from a gap.

The whole point is the comparison. A probe that reported only the whole-window
number would have agreed with the broken code it exists to check.
"""

from datetime import date
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ncaambb.probe_box_cap import compare, days, render


def rows_for(game_ids):
    return [dict(gameId=identifier, teamId=team,
                 startDate=f'2025-11-{(identifier % 15) + 1:02d}T19:00:00Z')
            for identifier in game_ids for team in (10, 20)]


def provider(limit):
    """A provider that silently truncates a response at `limit` games."""
    def getter(path, params, **kwargs):
        start = int(params['startDateRange'][8:10])
        end = int(params['endDateRange'][8:10])
        # Four games a day, ids derived from the day so days and the whole
        # window agree about which games exist.
        ids = [day * 100 + n for day in range(start, end + 1) for n in range(4)]
        return rows_for(ids[:limit])
    return getter


def test_a_truncated_window_is_detected_by_the_days_it_omits():
    result = compare(2026, date(2025, 11, 1), date(2025, 11, 15),
                     getter=provider(10))
    assert result['whole_games'] == 10
    assert result['day_games'] == 60
    assert result['missing_from_whole'] == 50
    assert 'TRUNCATED at 10 games' in '\n'.join(render(result))


def test_an_untruncated_window_is_reported_as_complete():
    result = compare(2026, date(2025, 11, 1), date(2025, 11, 15),
                     getter=provider(10_000))
    assert result['whole_games'] == result['day_games'] == 60
    assert result['missing_from_whole'] == 0
    assert 'not truncation' in '\n'.join(render(result))


def test_the_day_total_counts_distinct_games_not_rows():
    """Each game is two rows, one per team. Counting rows would make a complete
    window look like twice as many games as exist and hide a real shortfall."""
    result = compare(2026, date(2025, 11, 1), date(2025, 11, 1),
                     getter=provider(10_000))
    assert result['whole_rows'] == 8 and result['whole_games'] == 4


def test_every_day_of_the_window_is_requested_inclusively():
    assert days(date(2025, 11, 1), date(2025, 11, 3)) == [
        date(2025, 11, 1), date(2025, 11, 2), date(2025, 11, 3)]
