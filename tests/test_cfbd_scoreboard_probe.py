"""The scoreboard shape probe reports what it claims to report.

A diagnostic that quietly reports nothing is worse than no diagnostic, because
it is read as reassurance. These tests drive the real `probe()` and `main()`
over responses in the shape CFBD's generated client declares, plus the shapes
that would make a lazier probe look fine: an empty slate, a slate with nothing
in progress, a field nobody has seen before.
"""
from __future__ import annotations

import io
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))

from probe_cfbd_scoreboard import ADAPTER_READS, DECLARED, main, probe  # noqa: E402


def team(points=21, **over):
    out = {'winProbability': 0.61, 'lineScores': [7, 14], 'points': points,
           'classification': 'fbs', 'conference': 'Conference USA',
           'name': 'Western Kentucky', 'id': 98}
    out.update(over)
    return out


def game(gid=1, status='in_progress', **over):
    live = status == 'in_progress'
    out = {
        'id': gid, 'startDate': '2026-11-08T19:00:00.000Z', 'startTimeTBD': False,
        'tv': 'CBSSN', 'neutralSite': False, 'conferenceGame': True, 'status': status,
        'period': 2 if live else None, 'clock': '07:21' if live else None,
        'situation': '3rd & 4 at WKU 38' if live else None,
        'possession': 'home' if live else None,
        'lastPlay': 'Smith pass complete for 12 yards' if live else None,
        'venue': {'state': 'KY', 'city': 'Bowling Green', 'name': 'Houchens-Smith Stadium'},
        'homeTeam': team(), 'awayTeam': team(points=35, name='Delaware', id=48),
        'weather': {'windDirection': 180.0, 'windSpeed': 7.0,
                    'description': 'Clear', 'temperature': 54.0},
        'betting': {'awayMoneyline': -150, 'homeMoneyline': 130,
                    'overUnder': 52.5, 'spread': -3.5},
    }
    out.update(over)
    return out


def report(payload, **kwargs):
    return '\n'.join(probe(payload, **kwargs))


def table(text):
    """The TOP-LEVEL FIELDS rows, keyed by field name.

    Scanning the whole report by first word collided with the summary's own
    "venue is object on the wire" line, which is how the first version of
    these tests read the wrong line and still passed.
    """
    section = text.split('TOP-LEVEL FIELDS')[1].split('NESTED PATHS')[0]
    rows = {}
    for line in section.splitlines():
        if not line.strip() or line.lstrip().startswith('field '):
            continue
        rows[line.split()[0]] = line
    return rows


# --- the finding this probe exists for --------------------------------------

def test_it_names_the_venue_as_an_object():
    # The whole reason the probe was written. If it ever stops saying this, it
    # has stopped answering the question it was built to answer.
    text = report([game()])
    assert 'venue           3' not in text  # not treated as a flat scalar count
    assert 'venue' in text
    lines = [line for line in text.splitlines() if line.startswith('venue ')]
    assert lines and 'object' in lines[0], lines
    assert 'venue.name' in text and 'venue.city' in text and 'venue.state' in text
    assert 'null venue' in text


def test_it_names_the_fields_nothing_reads():
    text = report([game()])
    assert 'Sent but read by nothing: betting, weather' in text
    # And it marks them in the table rather than only in the summary.
    assert any(line.startswith('weather') and 'NOTHING' in line for line in text.splitlines())
    assert any(line.startswith('betting') and 'NOTHING' in line for line in text.splitlines())


def test_it_distinguishes_what_the_publisher_reads_from_what_the_adapter_reads():
    rows = table(report([game()]))
    assert 'publisher+adapter' in rows['venue']
    # possession is read by the new adapter only; the Python publisher ignores it.
    assert 'adapter only' in rows['possession']
    assert 'adapter only' in rows['lastPlay']
    assert 'adapter only' in rows['situation']


def test_an_unknown_field_is_called_out_in_the_table_and_the_summary():
    text = report([game(drivesUrl='/x'), game(gid=2, redZone=True)])
    assert 'UNKNOWN FIELDS' in text
    assert 'drivesUrl' in text and 'redZone' in text
    assert 'NOT IN THE DECLARED LIST' in text


def test_nothing_unknown_is_said_plainly_rather_than_left_blank():
    text = report([game()])
    assert 'No field outside the declared list.' in text
    assert 'UNKNOWN FIELDS' not in text


# --- the cases that would make a lazier probe look fine ---------------------

def test_an_empty_slate_refuses_to_claim_anything():
    text = report([])
    assert '0 games' in text
    assert 'EMPTY slate says nothing about the shape' in text
    # And it must NOT print a shape table built from nothing.
    assert 'TOP-LEVEL FIELDS' not in text


def test_a_slate_with_nothing_in_progress_says_what_it_cannot_settle():
    text = report([game(status='completed'), game(gid=2, status='scheduled')])
    assert 'NO GAME IS IN PROGRESS' in text
    assert 're-run while a game is live' in text
    # It still reports the shape of everything else.
    assert 'TOP-LEVEL FIELDS' in text
    assert 'venue.name' in text


def test_a_live_game_present_means_no_such_warning():
    assert 'NO GAME IS IN PROGRESS' not in report([game(), game(gid=2, status='completed')])


def test_a_non_list_response_is_refused_rather_than_summarised():
    for payload in [{'games': []}, 'nonsense', None, 42]:
        text = report(payload)
        assert text.startswith('REFUSED:'), payload
        assert 'TOP-LEVEL FIELDS' not in text


def test_a_declared_field_that_is_absent_is_reported_as_absent():
    # The inverse of an unknown field, and just as interesting: a field CFBD
    # declares and stopped sending would otherwise vanish from the report.
    missing = game()
    del missing['possession']
    del missing['weather']
    text = report([missing])
    rows = [line for line in text.splitlines() if line.startswith(('possession', 'weather'))]
    assert len(rows) == 2, rows
    for row in rows:
        assert '(absent)' in row and 'declared only' in row, row


def test_a_malformed_row_does_not_take_the_probe_down():
    # One bad row in a slate must not lose the report for the other fifty.
    text = report([game(), None, 'nonsense', 42, game(gid=2)])
    assert 'TOP-LEVEL FIELDS' in text
    assert 'venue.name' in text
    # Presence is counted against the rows that ARE objects, not against five.
    assert any('2/2' in line for line in text.splitlines() if line.startswith('venue'))


def test_nulls_are_reported_as_a_type_rather_than_hidden():
    # A field that is null on every row still has to appear, or "we never saw
    # it" and "it is always empty" become indistinguishable.
    rows = table(report([game(status='completed')]))
    assert 'null' in rows['possession'], rows['possession']
    assert '1/1' in rows['possession']


def test_deeply_nested_values_do_not_recurse_without_a_bound():
    deep = game()
    node = deep['venue']
    for _ in range(50):
        node['nested'] = {}
        node = node['nested']
    text = report([deep])
    assert 'TOP-LEVEL FIELDS' in text
    assert max(line.count('.') for line in text.splitlines()) <= 6


# --- values ----------------------------------------------------------------

def test_sample_values_cover_the_fields_whose_shape_is_the_question():
    text = report([game(), game(gid=2, status='completed')])
    assert 'SAMPLE VALUES' in text
    assert 'in_progress' in text and 'completed' in text
    assert '07:21' in text
    assert 'home' in text  # possession


def test_sample_values_never_include_a_long_or_identifying_field():
    text = report([game(lastPlay='Smith pass complete to Jones for 12 yards')])
    values = text.split('SAMPLE VALUES')[1].split('WHAT THIS SETTLES')[0]
    # lastPlay is free text about named people; it is shape-checked, not sampled.
    assert 'Smith pass' not in values
    assert 'Houchens-Smith' not in values, 'a venue name was printed as a sample value'
    # But its TYPE is still reported above.
    assert 'lastPlay' in text


def test_values_can_be_turned_off_entirely():
    text = report([game()], show_values=False)
    assert 'SAMPLE VALUES' not in text
    assert 'TOP-LEVEL FIELDS' in text


# --- the reference lists ----------------------------------------------------

def test_the_declared_list_matches_cfbds_generated_client():
    # ScoreboardGame.__properties from CFBD/cfbd-python, OpenAPI 5.33.2, read
    # 2026-11-12. Pinned as literals so a drifting copy is a failure, not a
    # silently different list.
    assert DECLARED == ['id', 'startDate', 'startTimeTBD', 'tv', 'neutralSite',
                        'conferenceGame', 'status', 'period', 'clock', 'situation',
                        'possession', 'lastPlay', 'venue', 'homeTeam', 'awayTeam',
                        'weather', 'betting']
    assert len(DECLARED) == 17


def test_the_adapter_reads_everything_but_weather_and_betting():
    assert sorted(set(DECLARED) - set(ADAPTER_READS)) == ['betting', 'weather']


# --- main() -----------------------------------------------------------------

def test_main_reads_a_saved_response_without_touching_the_network(tmp_path, capsys, monkeypatch):
    # A module whose main() is broken while every function under it is tested
    # has reached main twice in this repository.
    def no_network(*args, **kwargs):
        raise AssertionError('main() called the network with --from-file')

    monkeypatch.setattr('probe_cfbd_scoreboard.urlopen', no_network)
    path = tmp_path / 'response.json'
    path.write_text(json.dumps([game()]), encoding='utf-8')
    assert main(['--from-file', str(path)]) == 0
    out = capsys.readouterr().out
    assert 'TOP-LEVEL FIELDS' in out
    assert 'venue.name' in out


def test_main_calls_the_endpoint_with_the_key_in_the_header(monkeypatch, capsys):
    seen = {}

    class Response(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self.close()
            return False

    def fake_urlopen(request, timeout=None):
        seen['url'] = request.full_url
        seen['headers'] = dict(request.headers)
        return Response(json.dumps([game()]).encode())

    monkeypatch.setattr('probe_cfbd_scoreboard.urlopen', fake_urlopen)
    monkeypatch.setenv('CFBD_API_KEY', 'secret-key')
    assert main([]) == 0
    assert 'classification=fbs' in seen['url']
    # The key goes in the header and never into the URL, where it would reach
    # logs and referrers.
    assert 'secret-key' not in seen['url']
    assert seen['headers']['Authorization'] == 'Bearer secret-key'
    # And it is never printed.
    assert 'secret-key' not in capsys.readouterr().out


def test_main_refuses_without_a_key(monkeypatch):
    monkeypatch.delenv('CFBD_API_KEY', raising=False)
    with pytest.raises(SystemExit):
        main([])


def test_no_values_is_wired_through_main(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr('probe_cfbd_scoreboard.urlopen',
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError('network')))
    path = tmp_path / 'response.json'
    path.write_text(json.dumps([game()]), encoding='utf-8')
    main(['--from-file', str(path), '--no-values'])
    assert 'SAMPLE VALUES' not in capsys.readouterr().out
