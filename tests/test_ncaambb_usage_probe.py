"""The usage probe's own output, driven rather than read.

What this probe prints is the only thing anyone acts on, and twice it has
printed something untrue and sent a session off to wait for a recovery that
was never coming. So these tests call the real reporting functions with the
provider stubbed out and read what comes back.
"""

from pathlib import Path
import io
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))

import probe_cbbd_limits as probe  # noqa: E402


USAGE = {
    'window': {'start': '2026-10-27T16:48:04Z', 'end': '2026-11-03T16:48:04Z'},
    'totals': {'requests': 1017, 'cfbRequests': 7, 'cbbRequests': 1010},
    'topEndpoints': [
        {'api': 'cbb', 'endpoint': '/games/players', 'requests': 616,
         'lastUsedAt': '2026-11-03T16:37:51Z'},
    ],
}


@pytest.fixture
def spoken(monkeypatch):
    """Run a reporting function and hand back everything it printed."""
    import json as _json

    def answer(base, path, params=None, *, key):
        return 200, {}, _json.dumps(USAGE), 0.01

    monkeypatch.setattr(probe, 'call', answer)

    def run(function, *args):
        buffer = io.StringIO()
        held, sys.stdout = sys.stdout, buffer
        try:
            function(*args)
        finally:
            sys.stdout = held
        return buffer.getvalue()

    return run


def test_the_usage_report_does_not_promise_a_recovery(spoken):
    """It used to say in so many words that capacity returns as the oldest
    requests age out of the window. The cap is monthly, the window is a
    report, and that sentence is why two sessions planned around a date that
    meant nothing. MEASURED 2026-11-03: the refusal arrived with 1,010
    requests in the window, and 2026-10-27's arrived with 202."""
    said = spoken(probe.report_usage, 'not-a-real-key')
    assert 'age out' not in said
    assert 'monthly' in said.lower(), 'it has to name the real window'
    assert 'REPORT' in said, 'and say what the figure above it is'


def test_the_usage_report_still_reports_the_spend(spoken):
    said = spoken(probe.report_usage, 'not-a-real-key')
    assert '1010 basketball' in said
    assert '/games/players' in said


def test_a_paced_run_of_nothing_claims_nothing(spoken):
    """`ok == count` is 0 == 0, so the probe reported that two seconds between
    requests "was enough" having never made one. A diagnostic that invents a
    measurement is worse than one that declines to."""
    said = spoken(probe.report_paced, 'not-a-real-key', 2.0, 0)
    assert 'was enough' not in said
    assert 'measures nothing' in said


def test_a_paced_run_that_happened_is_still_reported(spoken):
    said = spoken(probe.report_paced, 'not-a-real-key', 2.0, 2)
    assert '2 of 2 succeeded' in said
    assert 'was enough' in said
