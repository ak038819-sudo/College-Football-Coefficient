"""Whether a freshly built export may replace the published one.

The plan's rule is that a basketball provider failure must not replace valid
data with empty files. These tests drive the cases where that would happen
quietly: a refused export arriving over a good one, an export older than the
one already live, a publish that dies halfway through, and a first publish
with nothing to protect.

The gate's own refusals are tested in tests/test_ncaambb_export.py. What is
tested here is what a publisher does with them.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ncaambb import publish

NOW = datetime(2026, 11, 7, 12, 0, tzinfo=timezone.utc)


def export(root, *, at=None, marker='new', files=True):
    """An export directory whose manifest passes the gate."""
    root = Path(root)
    (root / 'seasons').mkdir(parents=True, exist_ok=True)
    moment = (at or NOW).isoformat().replace('+00:00', 'Z')
    season_text = f'window.NCAAMBB_SEASONS={{"marker":"{marker}"}};\n'
    teams_text = f'window.NCAAMBB_TEAMS={{"marker":"{marker}"}};\n'
    manifest = dict(
        schema_version=1, sport='ncaambb', generated_at=moment,
        latest_season=2027, marker=marker,
        model=dict(name='ncaambb_elo_v1', config_sha256='a' * 64,
                   fitted_on=[2025], held_out=[2026],
                   holdout_brier=0.18656, holdout_games=5752),
        coverage=[dict(season=2027, season_label='2026-27',
                       division_one_teams=365, rated_teams=364, games=6318)],
        seasons={}, files={})
    if files:
        (root / 'seasons' / '2027.js').write_text(season_text,
                                                  encoding='utf-8')
        (root / 'teams.js').write_text(teams_text, encoding='utf-8')
        manifest['seasons'] = {
            '2027': 'seasons/2027.js?v=' + publish_stamp(season_text)}
        manifest['files'] = {'teams': 'teams.js?v=' + publish_stamp(teams_text)}
    (root / 'manifest.json').write_text(json.dumps(manifest),
                                        encoding='utf-8')
    return root


def publish_stamp(text):
    from src.ncaambb.publish_gate import file_version
    import hashlib
    return hashlib.sha256(text.encode('utf-8')).hexdigest()[:10]


def live_marker(live):
    return json.loads((Path(live) / 'manifest.json')
                      .read_text(encoding='utf-8'))['marker']


# --- the decision -----------------------------------------------------------

def test_a_good_export_replaces_an_older_published_one(tmp_path):
    new = export(tmp_path / 'new', marker='new')
    live = export(tmp_path / 'live', at=NOW - timedelta(hours=6),
                  marker='old')
    decision = publish.plan(new, live, now=NOW)
    assert decision['publish'] is True
    assert publish.apply(decision, new, live) is True
    assert live_marker(live) == 'new'


def test_the_first_publish_has_nothing_to_protect(tmp_path):
    new = export(tmp_path / 'new')
    live = tmp_path / 'live'
    decision = publish.plan(new, live, now=NOW)
    assert decision['publish'] is True and decision['first'] is True
    assert publish.apply(decision, new, live) is True
    assert (live / 'manifest.json').exists()


def test_a_refused_export_leaves_the_published_one_untouched(tmp_path):
    """The rule: a provider failure must never replace valid data with empty
    files, and half a directory is empty enough."""
    new = export(tmp_path / 'new', marker='broken')
    manifest = json.loads((new / 'manifest.json').read_text())
    manifest['coverage'][0]['division_one_teams'] = 1
    (new / 'manifest.json').write_text(json.dumps(manifest))
    live = export(tmp_path / 'live', at=NOW - timedelta(hours=6),
                  marker='good')
    decision = publish.plan(new, live, now=NOW)
    assert decision['publish'] is False
    assert 'publish gate' in decision['reason']
    assert publish.apply(decision, new, live) is False
    assert live_marker(live) == 'good'


def test_an_export_older_than_the_published_one_is_refused(tmp_path):
    """Valid, national, and a step backwards. Publishing it would roll the
    site back to yesterday's ratings without anybody noticing."""
    new = export(tmp_path / 'new', at=NOW - timedelta(days=1), marker='stale')
    live = export(tmp_path / 'live', at=NOW, marker='current')
    decision = publish.plan(new, live, now=NOW)
    assert decision['publish'] is False
    assert 'roll the site back' in decision['reason']
    assert live_marker(live) == 'current'


def test_an_export_of_the_same_moment_still_publishes(tmp_path):
    # Equal is not older: a rerun of the same build must be able to repair a
    # half-published directory.
    new = export(tmp_path / 'new', at=NOW, marker='rerun')
    live = export(tmp_path / 'live', at=NOW, marker='first')
    decision = publish.plan(new, live, now=NOW)
    assert decision['publish'] is True
    publish.apply(decision, new, live)
    assert live_marker(live) == 'rerun'


def test_an_empty_build_directory_is_refused(tmp_path):
    new = tmp_path / 'new'
    new.mkdir()
    live = export(tmp_path / 'live', marker='good')
    decision = publish.plan(new, live, now=NOW)
    assert decision['publish'] is False
    assert 'no readable manifest' in decision['reason']
    assert live_marker(live) == 'good'


def test_a_truncated_manifest_is_refused_rather_than_crashing(tmp_path):
    new = export(tmp_path / 'new')
    (new / 'manifest.json').write_text('{"schema_versi')
    live = export(tmp_path / 'live', marker='good')
    assert publish.plan(new, live, now=NOW)['publish'] is False
    assert live_marker(live) == 'good'


def test_a_published_export_nobody_can_read_is_not_a_reason_to_stop(tmp_path):
    """A corrupt live manifest means there is nothing to protect, not that
    the site must stay corrupt."""
    new = export(tmp_path / 'new', marker='new')
    live = export(tmp_path / 'live', marker='corrupt')
    (live / 'manifest.json').write_text('not json')
    decision = publish.plan(new, live, now=NOW)
    assert decision['publish'] is True and decision['first'] is True
    publish.apply(decision, new, live)
    assert live_marker(live) == 'new'


def test_a_new_export_with_no_as_of_time_never_publishes(tmp_path):
    new = export(tmp_path / 'new')
    manifest = json.loads((new / 'manifest.json').read_text())
    manifest['generated_at'] = 'whenever'
    (new / 'manifest.json').write_text(json.dumps(manifest))
    live = export(tmp_path / 'live', marker='good')
    # The gate refuses it first; either way nothing is replaced.
    assert publish.plan(new, live, now=NOW)['publish'] is False
    assert live_marker(live) == 'good'


# --- the swap ---------------------------------------------------------------

def test_the_live_directory_is_never_emptied_before_the_new_one_is_ready(
        tmp_path, monkeypatch):
    """A publish that dies halfway leaves the previous export intact, not a
    directory holding half of each."""
    new = export(tmp_path / 'new', marker='new')
    live = export(tmp_path / 'live', at=NOW - timedelta(hours=1),
                  marker='good')
    real_rename = Path.rename

    def explode(self, target):
        if self.name.endswith('.incoming'):
            raise OSError('the disk filled up mid-swap')
        return real_rename(self, target)

    monkeypatch.setattr(Path, 'rename', explode)
    decision = publish.plan(new, live, now=NOW)
    with pytest.raises(OSError):
        publish.apply(decision, new, live)
    assert live.exists(), 'the live export must still be there'
    assert live_marker(live) == 'good'


def test_no_staging_directory_survives_a_publish(tmp_path):
    # The live export gets its own parent, so anything left beside it came
    # from the swap rather than from the fixture.
    new = export(tmp_path / 'new')
    live = export(tmp_path / 'site' / 'ncaambb', at=NOW - timedelta(hours=1))
    publish.apply(publish.plan(new, live, now=NOW), new, live)
    leftovers = [path.name for path in live.parent.iterdir()
                 if path.name != live.name]
    assert leftovers == []


def test_a_stale_file_from_the_old_export_does_not_survive(tmp_path):
    """A swap, not a merge: a season the new export dropped must not stay on
    the site, pointed at by nothing and served forever."""
    new = export(tmp_path / 'new')
    live = export(tmp_path / 'live', at=NOW - timedelta(hours=1))
    (live / 'seasons' / '1999.js').write_text('window.OLD=1;\n')
    publish.apply(publish.plan(new, live, now=NOW), new, live)
    assert not (live / 'seasons' / '1999.js').exists()


def test_leftovers_from_a_previous_crash_are_cleared(tmp_path):
    new = export(tmp_path / 'new', marker='new')
    live = export(tmp_path / 'live', at=NOW - timedelta(hours=1))
    stale = live.with_name(live.name + '.incoming')
    stale.mkdir()
    (stale / 'junk.js').write_text('x')
    publish.apply(publish.plan(new, live, now=NOW), new, live)
    assert live_marker(live) == 'new'
    assert not stale.exists()


# --- what a workflow prints -------------------------------------------------

def test_the_decision_reads_as_a_sentence_about_somebody_s_site(tmp_path):
    new = export(tmp_path / 'new')
    live = export(tmp_path / 'live', at=NOW - timedelta(hours=6))
    report = publish.render(publish.plan(new, live, now=NOW))
    assert 'PUBLISHING' in report and 'NOT PUBLISHING' not in report
    assert '2026-27' in report and '365' in report


def test_a_refusal_says_which_check_refused_it(tmp_path):
    new = export(tmp_path / 'new')
    manifest = json.loads((new / 'manifest.json').read_text())
    manifest['model']['held_out'] = []
    (new / 'manifest.json').write_text(json.dumps(manifest))
    report = publish.render(publish.plan(new, tmp_path / 'live', now=NOW))
    assert 'NOT PUBLISHING' in report
    assert 'held-out' in report
