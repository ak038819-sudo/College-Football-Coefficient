"""The live NBA scoreboard: the four traps, and the publish that stays whole.

Every test here drives `src/nba/live.py` itself against a fake scoreboard
rather than asserting on its text, because a test that reads the source passes
while the code is broken. The fixtures are shaped from a real response
measured from an Actions runner on 2026-11-13.
"""

import datetime as dt
import io
import json
import urllib.error

import pytest

from src.nba import live


# ------------------------------------------------------------------ fixtures

def event(*, event_id='401909966', state='post', completed=True,
          home=('11', '114'), away=('27', '101'), period=4,
          clock='0.0', detail='Final', date='2026-11-12T00:00Z',
          broadcasts=None, venue='Gainbridge Fieldhouse'):
    competitors = []
    for where, (team_id, score) in (('home', home), ('away', away)):
        side = {'id': team_id, 'homeAway': where,
                'team': {'abbreviation': 'XX', 'displayName': f'Club {team_id}'}}
        if score is not None:
            side['score'] = score
        competitors.append(side)
    status = {'period': period, 'displayClock': clock,
              'type': {'state': state, 'completed': completed,
                       'shortDetail': detail}}
    return {
        'id': event_id, 'date': date, 'status': status,
        'competitions': [{
            'id': event_id, 'competitors': competitors, 'status': status,
            'neutralSite': False, 'venue': {'fullName': venue},
            'broadcasts': broadcasts if broadcasts is not None else [],
        }],
    }


class FakeResponse(io.BytesIO):
    def __init__(self, body, status=200):
        super().__init__(body)
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def opener_for(days):
    """A fake urlopen serving one payload per `dates=` value it is asked for."""
    asked = []

    def open_url(request, timeout=None):
        url = request.full_url if hasattr(request, 'full_url') else str(request)
        stamp = url.split('dates=')[1].split('&')[0]
        asked.append(stamp)
        return FakeResponse(json.dumps({'events': days.get(stamp, [])})
                            .encode('utf-8'))

    open_url.asked = asked
    return open_url


# --------------------------------------------------------------- trap 1: 0-0

def test_an_unplayed_game_publishes_no_score_rather_than_a_nil_nil_draw():
    # THE TRAP. ESPN sends "0" for both sides of a game that has not tipped
    # off, exactly as the Parquet archive writes 0-0. Published as read, a
    # nine-game evening slate is nine scoreless ties.
    row = live.read_event(event(state='pre', completed=False,
                                home=('11', '0'), away=('27', '0'),
                                period=0, clock='0.0', detail='7:00 PM'))
    assert row['home']['points'] is None
    assert row['away']['points'] is None
    assert row['state'] == 'pre'
    assert row['completed'] is False


def test_a_game_in_progress_publishes_the_score_it_has_reached():
    row = live.read_event(event(state='in', completed=False,
                                home=('11', '58'), away=('27', '61'),
                                period=3, clock='5:21', detail='5:21 - 3rd'))
    assert row['home']['points'] == 58
    assert row['away']['points'] == 61
    assert row['clock'] == '5:21'
    assert row['period'] == 3


def test_a_finished_game_publishes_its_final_score():
    row = live.read_event(event())
    assert (row['away']['points'], row['home']['points']) == (101, 114)
    assert row['completed'] is True


# ------------------------------------------------- trap 2: absent, not zero

def test_a_missing_clock_is_absent_rather_than_zero():
    # `Number(undefined)` is 0, which reads as 0:00 -- a game that has not
    # started shown as one about to end. This has been found three times in
    # this repository, so it is tested on the only path that can produce it.
    row = live.read_event(event(state='pre', completed=False,
                                home=('11', '0'), away=('27', '0'),
                                period=None, clock=None, detail='7:00 PM'))
    assert row['clock'] is None
    assert row['period'] is None


def test_a_game_before_the_tip_publishes_no_clock_even_though_one_is_sent():
    # The real feed sends `displayClock: "0.0"` for a game that has not
    # started. Passed through, the card reads 0:00 -- a game about to end.
    row = live.read_event(event(state='pre', completed=False,
                                home=('11', '0'), away=('27', '0'),
                                period=0, clock='0.0', detail='7:00 PM'))
    assert row['clock'] is None


def test_an_absent_score_on_a_started_game_is_absent_rather_than_zero():
    row = live.read_event(event(state='in', completed=False,
                                home=('11', None), away=('27', None),
                                period=1, clock='9:12', detail='9:12 - 1st'))
    assert row['home']['points'] is None
    assert row['away']['points'] is None


# ------------------------------------------------------- trap 3: exhibitions

def test_an_all_star_game_is_skipped_rather_than_published_as_a_league_game():
    # Team LeBron is on this scoreboard like any other side, and its ids are
    # not franchises. Published, it would appear on the slate as a game.
    assert live.read_event(event(home=('31', '184'), away=('32', '175'))) is None


def test_a_game_with_one_unknown_side_is_skipped_rather_than_half_published():
    assert live.read_event(event(home=('11', '101'), away=('999', '98'))) is None


def test_every_franchise_id_resolves_to_the_archive_s_own_team_key():
    row = live.read_event(event())
    assert row['home']['key'] == 'nba:espn:team:11'
    assert row['away']['key'] == 'nba:espn:team:27'


def test_the_game_key_is_the_one_the_export_already_publishes():
    # The whole reason this is cheap: the ids join with no crosswalk. The
    # season payload's `id` is exactly this string.
    row = live.read_event(event(event_id='401909976'))
    assert row['game_key'] == 'nba:espn:game:401909976'
    assert row['id'] == '401909976'


# ------------------------------------------------------- trap 4: which days

def test_the_days_asked_for_are_eastern_days_not_utc_days():
    # 03:30 UTC on the 14th is still the evening of the 13th in the east,
    # when half the slate is being played. Asking for the UTC day would skip
    # every game of that evening.
    now = dt.datetime(2026, 11, 14, 3, 30, tzinfo=dt.timezone.utc)
    assert live.eastern_today(now) == dt.date(2026, 11, 13)
    assert live.days_to_ask(now, back=1, ahead=1) == [
        dt.date(2026, 11, 12), dt.date(2026, 11, 13), dt.date(2026, 11, 14)]


def test_a_window_of_days_is_fetched_and_each_one_is_asked_for_once():
    days = {'20261112': [event(event_id='1')],
            '20261113': [event(event_id='2', home=('2', '99'),
                               away=('3', '90'))]}
    open_url = opener_for(days)
    payload = live.snapshot([dt.date(2026, 11, 12), dt.date(2026, 11, 13)],
                            opener=open_url, sleep=lambda _s: None,
                            now=dt.datetime(2026, 11, 13, 12, 0,
                                            tzinfo=dt.timezone.utc))
    assert open_url.asked == ['20261112', '20261113']
    assert [row['id'] for row in payload['games']] == ['1', '2']
    assert [row['day'] for row in payload['games']] == ['2026-11-12',
                                                        '2026-11-13']
    assert payload['updated_at'] == '2026-11-13T12:00:00Z'


def test_a_game_listed_on_two_days_is_published_once():
    # ESPN lists a late game under both days around it often enough that a
    # naive concatenation duplicates cards on the slate.
    days = {'20261112': [event(event_id='7')],
            '20261113': [event(event_id='7')]}
    payload = live.snapshot([dt.date(2026, 11, 12), dt.date(2026, 11, 13)],
                            opener=opener_for(days), sleep=lambda _s: None)
    assert [row['id'] for row in payload['games']] == ['7']


# ------------------------------------------------------------ fetch and fail

def test_a_server_error_is_retried_and_a_not_found_is_not():
    attempts = []

    def flaky(request, timeout=None):
        attempts.append(1)
        if len(attempts) < 3:
            raise urllib.error.HTTPError(request.full_url, 503, 'busy',
                                         None, None)
        return FakeResponse(json.dumps({'events': []}).encode('utf-8'))

    assert live.fetch_day(dt.date(2026, 11, 13), opener=flaky,
                          sleep=lambda _s: None) == {'events': []}
    assert len(attempts) == 3

    def gone(request, timeout=None):
        attempts.append(1)
        raise urllib.error.HTTPError(request.full_url, 404, 'gone', None, None)

    attempts.clear()
    with pytest.raises(live.NbaLiveError):
        live.fetch_day(dt.date(2026, 11, 13), opener=gone,
                       sleep=lambda _s: None)
    assert len(attempts) == 1


def test_the_sleep_between_attempts_is_resolved_inside_the_call():
    # A default argument binds at DEF time, so `def f(sleep=time.sleep)`
    # cannot be patched and the test passes vacuously while the real run
    # waits. This repository has shipped that bug twice.
    waits = []

    def flaky(request, timeout=None):
        if len(waits) < 2:
            raise urllib.error.HTTPError(request.full_url, 500, 'x', None, None)
        return FakeResponse(b'{"events": []}')

    live.fetch_day(dt.date(2026, 11, 13), opener=flaky, sleep=waits.append)
    assert waits == [live.BACKOFF_SECONDS, live.BACKOFF_SECONDS * 2]


def test_a_body_that_is_not_json_is_refused_rather_than_published():
    def html(request, timeout=None):
        return FakeResponse(b'<html>error</html>')

    with pytest.raises(live.NbaLiveError, match='did not return JSON'):
        live.fetch_day(dt.date(2026, 11, 13), opener=html,
                       sleep=lambda _s: None)


def test_an_oversized_body_is_refused():
    def flood(request, timeout=None):
        return FakeResponse(b'x' * (live.MAX_BYTES + 1))

    with pytest.raises(live.NbaLiveError, match='more than'):
        live.fetch_day(dt.date(2026, 11, 13), opener=flood,
                       sleep=lambda _s: None)


# ------------------------------------------------------------- the published file

def test_the_snapshot_is_renamed_into_place_so_a_reader_never_sees_half(tmp_path):
    out = tmp_path / 'nba_live.json'
    payload = live.snapshot([dt.date(2026, 11, 12)],
                            opener=opener_for({'20261112': [event()]}),
                            sleep=lambda _s: None)
    live.write(payload, out)
    assert json.loads(out.read_text())['games'][0]['id'] == '401909966'
    # Nothing is left staged beside it.
    assert [p.name for p in tmp_path.iterdir()] == ['nba_live.json']


def test_it_is_not_written_inside_the_directory_the_refresh_replaces():
    # src/nba/publish.py swaps the WHOLE of ui/data/nba for a freshly built
    # directory, so a snapshot written in there is deleted by the next
    # refresh -- which is why ui/nba_logo_assets.json sits outside it too.
    assert 'ui/data/nba/' not in str(live.DEFAULT_OUT).replace('\\', '/')
    assert str(live.DEFAULT_OUT).replace('\\', '/') == 'ui/data/nba_live.json'


def test_main_fetches_writes_and_reports(tmp_path, monkeypatch):
    # The module the workflow invokes needs a test that INVOKES it: a broken
    # main() has reached main twice in this repository because every test
    # drove the functions under it.
    out = tmp_path / 'nba_live.json'
    monkeypatch.setattr(live, 'snapshot',
                        lambda days, **kw: {'source': 'espn-scoreboard',
                                            'sport': 'nba',
                                            'updated_at': '2026-11-13T12:00:00Z',
                                            'days': ['2026-11-13'],
                                            'games': [{'state': 'in'}]})
    assert live.main(['--out', str(out)]) == 0
    assert json.loads(out.read_text())['games'] == [{'state': 'in'}]
