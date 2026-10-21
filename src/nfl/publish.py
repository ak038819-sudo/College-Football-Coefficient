"""Deciding whether a freshly built NFL export may replace the published one.

The export gate in `publish_gate.py` asks
whether an export is fit to publish at all; this asks whether it may replace
the one already live, which is a different question with different ways to go
wrong:

- The new export is a valid national export, but OLDER than the live one. That
  is a clock or an ordering fault, and publishing it would quietly roll the
  site back.
- The new export is fine and the live one is missing or unreadable. That is the
  first publish, not an error.
- The new export is refused. The live one stays exactly as it is: a provider
  failure must never replace valid data with empty files, and "empty"
  includes "half a directory".

Nothing here fetches or builds. `plan()` decides and explains; `apply()` moves
files and is the only function that writes. They are separate so the decision
can be read, logged and tested without a filesystem full of exports.
"""

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from .publish_gate import check

MANIFEST = 'manifest.json'


def read_manifest(directory):
    """The manifest of an export directory, or None.

    None covers every way a live export can be absent or unreadable — not
    there at all, a directory with no manifest, a truncated file. All of them
    mean the same thing to a publisher: there is nothing here to protect.
    """
    path = Path(directory) / MANIFEST
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return None


def generated_at(manifest):
    value = (manifest or {}).get('generated_at')
    if not isinstance(value, str) or not value:
        return None
    text = value[:-1] + '+00:00' if value.endswith('Z') else value
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def plan(new_dir, live_dir, *, now=None, gate=check):
    """Whether the new export may replace the live one, and why.

    Returns a dict with `publish`, a `reason` a human can act on, and the two
    manifests, so a workflow can print exactly what it decided to do with
    somebody's live site.
    """
    incoming = read_manifest(new_dir)
    live = read_manifest(live_dir)
    if incoming is None:
        return dict(publish=False, first=False, incoming=None, live=live,
                    reason=f'{new_dir} holds no readable manifest; the '
                           'published export is left as it is')
    refusals = gate(incoming, root=Path(new_dir), now=now)
    if refusals:
        return dict(publish=False, first=False, incoming=incoming, live=live,
                    reason='the new export does not pass the publish gate: '
                           + '; '.join(refusals))
    if live is None:
        return dict(publish=True, first=True, incoming=incoming, live=None,
                    reason='nothing is published yet; this is the first '
                           'NFL export')
    new_moment, live_moment = generated_at(incoming), generated_at(live)
    if new_moment is None:
        return dict(publish=False, first=False, incoming=incoming, live=live,
                    reason='the new export has no usable as-of time')
    if live_moment is not None and new_moment < live_moment:
        return dict(publish=False, first=False, incoming=incoming, live=live,
                    reason=f'the new export is from {incoming["generated_at"]}'
                           f' and the published one from '
                           f'{live["generated_at"]}; publishing it would roll '
                           'the site back')
    return dict(publish=True, first=False, incoming=incoming, live=live,
                reason=f'replacing the export of {live.get("generated_at")} '
                       f'with {incoming["generated_at"]}')


def apply(decision, new_dir, live_dir):
    """Put the new export in place, or leave the live one alone.

    The new directory is staged beside the live one and swapped, rather than
    the live one being emptied and refilled. A publish that dies halfway
    through then leaves the previous export intact instead of a directory
    holding half of each.
    """
    if not decision.get('publish'):
        return False
    live = Path(live_dir)
    staged = live.with_name(live.name + '.incoming')
    previous = live.with_name(live.name + '.previous')
    for path in (staged, previous):
        if path.exists():
            shutil.rmtree(path)
    shutil.copytree(new_dir, staged)
    live.parent.mkdir(parents=True, exist_ok=True)
    if live.exists():
        live.rename(previous)
    try:
        staged.rename(live)
    except OSError:
        # The swap failed with the live directory already moved aside. Put it
        # back rather than leaving the site with nothing at all.
        if previous.exists() and not live.exists():
            previous.rename(live)
        raise
    if previous.exists():
        shutil.rmtree(previous)
    return True


def render(decision):
    manifest = decision.get('incoming') or {}
    lines = ['# NFL publish decision', '',
             ('PUBLISHING' if decision.get('publish') else 'NOT PUBLISHING')
             + ': ' + decision.get('reason', ''), '']
    if manifest.get('coverage'):
        lines += ['| season | teams | rated | games |',
                  '| --- | --- | --- | --- |']
        for row in manifest['coverage']:
            lines.append(f'| {row.get("season_label", row.get("season"))} | '
                         f'{row.get("teams")} | '
                         f'{row.get("rated_teams")} | {row.get("games")} |')
    return '\n'.join(lines)
