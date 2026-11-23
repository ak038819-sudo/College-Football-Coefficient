"""One range, named in nine places: the gates and their workflows agree.

The historical archive is 1869-1979. That range is written down in each
gate script and again in each gate's workflow, because a workflow cannot
import a Python constant and a script must refuse an out-of-range request
on its own. Nine copies of one number is how a widening goes half-done:
the archive reaches 1869 and a gate quietly keeps answering only for 1946,
which looks like a clean report rather than a narrowed one.

So this file holds the range ONCE and asserts every copy matches it. It is
deliberately a test about numbers rather than behaviour -- the behaviour is
tested in each gate's own file -- and it is the thing that fails first when
the next phase widens the archive again.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent

FIRST, LAST = 1869, 1979

# Every script that reads the staged archive, and the workflow that runs it.
GATES = {
    'scripts/import_historical_archive.py': None,
    'scripts/audit_historical_backfill.py':
        '.github/workflows/historical-backfill.yml',
    'scripts/reconcile_historical_sources.py':
        '.github/workflows/historical-reconcile.yml',
    'scripts/build_historical_identity_map.py':
        '.github/workflows/historical-identity.yml',
    'scripts/build_historical_eligibility.py':
        '.github/workflows/historical-identity.yml',
    'scripts/audit_historical_preservation.py':
        '.github/workflows/historical-preservation.yml',
}

SCRIPTS = [name for name in GATES if 'import_historical' not in name]
WORKFLOWS = sorted({path for path in GATES.values() if path})


def text(relative):
    return (ROOT / relative).read_text()


@pytest.mark.parametrize('script', SCRIPTS)
def test_each_gate_names_the_whole_archive(script):
    found = re.search(r'^FIRST, LAST = (\d{4}), (\d{4})$', text(script),
                      re.MULTILINE)
    assert found, f'{script} has no FIRST, LAST line to check'
    assert (int(found.group(1)), int(found.group(2))) == (FIRST, LAST)


@pytest.mark.parametrize('script', SCRIPTS)
def test_each_gate_refuses_outside_it_in_its_own_words(script):
    # The guard has to name the range it enforces, or an operator who
    # mistypes a year is told only that something is wrong. Either dash is
    # accepted: these are sentences for a person, not a format.
    assert re.search(f'{FIRST}[-\u2013]{LAST}', text(script)), script


@pytest.mark.parametrize('workflow', WORKFLOWS)
def test_each_workflow_guards_the_same_range(workflow):
    body = text(workflow)
    assert f'if not {FIRST} <= first <= last <= {LAST}:' in body, workflow
    assert re.search(f'{FIRST}[-\u2013]{LAST}', body), workflow


@pytest.mark.parametrize('workflow', WORKFLOWS)
def test_no_workflow_still_offers_a_phase_three_default(workflow):
    # A default of 1946 would make the narrowed range the easy one to run.
    assert 'default: "1946"' not in text(workflow)
    assert f'default: "{FIRST}"' in text(workflow)


def test_the_probe_keeps_its_own_narrower_range():
    # The probe asks the provider about the period Phase 3 had NOT staged,
    # so 1869-1945 is correct there and must not be widened to match the
    # gates: widening it would re-ask for seasons already staged.
    found = re.search(r'^FIRST, LAST = (\d{4}), (\d{4})$',
                      text('scripts/probe_early_football_coverage.py'),
                      re.MULTILINE)
    assert found
    assert (int(found.group(1)), int(found.group(2))) == (1869, 1945)
