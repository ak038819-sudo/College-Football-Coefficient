#!/usr/bin/env python3
"""
The light-ink artwork for our own teams' current logo.

The dated set (src/build_dated_logo_assets.py) carries one image per era, and
one image only: the colour mark, which is drawn for a white page. On this
site's dark theme a third of those marks have under 35% of their ink readable
against the background, which is why every logo has sat on a coloured chip.

ESPN publishes a second artwork for exactly this -- the same mark in light ink
-- and the sportsdataverse archive carries it for 672 of its 687 teams. This
fetches it for the 139 teams this project rates and writes it as an OVERLAY
file rather than into the dated set itself, for two reasons: the dated set is
generated from a folder of era PNGs that lives outside this repository, so a
rebuild of it would wipe anything written in, and the artwork is only honest
for the CURRENT era. ESPN serves today's mark; a 1985 page still shows the
1985 colour mark, which is the right answer for it.

The join is the ESPN team id that data/reference/team_identities.json already
records for all 139 teams, so nothing here matches on a name.

Usage:
    python3 src/fetch_team_dark_logos.py [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fetch_non_fbs_logos import (  # noqa: E402
    DARK_VARIANT, FETCH_FAILURE_LIMIT, FETCH_FAILURE_TOLERATED, MARK_SIZE,
    MANIFEST_URL, encode_bytes, fetch, sdv_marks,
)

REPO = Path(__file__).resolve().parent.parent
DATED = REPO / "ui/dated_logo_assets.json"
IDENTITIES = REPO / "data/reference/team_identities.json"
OUT = REPO / "ui/dated_logo_dark_assets.json"
SOURCES = REPO / "data/reference/team_dark_logo_sources.csv"


def team_espn_ids(identities_path: Path = IDENTITIES) -> dict[str, str]:
    """canonical team name -> its ESPN id, as the identity file already records it."""
    teams = json.loads(identities_path.read_text(encoding="utf-8"))["teams"]
    ids = {}
    for team in teams:
        espn = (team.get("provider_ids") or {}).get("espn")
        if espn:
            ids[team["canonical_name"]] = str(espn)
    return ids


def current_era(entries: list[dict]) -> dict | None:
    """The era a page shows for a team today.

    Most eras in the set are closed and end at the current season rather than
    at None, so "the one still open" is not a test that works here: the current
    era is simply the latest one. An open end sorts above any year.
    """
    if not entries:
        return None
    return max(entries, key=lambda e: (e["end"] is None, e["end"] if e["end"] is not None else 0))


def build(*, dry_run: bool = False, size: int = MARK_SIZE, manifest_url: str = MANIFEST_URL,
          out: Path = OUT, sources_out: Path = SOURCES, dated_path: Path = DATED,
          identities_path: Path = IDENTITIES, fetcher=fetch) -> dict:
    dated = json.loads(dated_path.read_text(encoding="utf-8"))
    ids = team_espn_ids(identities_path)
    missing_id = sorted(name for name in dated if name not in ids)
    report = {"teams": len(dated), "with_id": len(dated) - len(missing_id), "missing_id": missing_id,
              "fetched": 0, "missing_dark": [], "fetch_failed": []}
    if dry_run:
        return report

    marks = sdv_marks(fetcher(manifest_url).decode("utf-8"))
    result: dict[str, dict] = {}
    rows = []
    for name in sorted(dated):
        espn = ids.get(name)
        era = current_era(dated[name])
        row = {"team_name": name, "espn_id": espn or "", "era_start": "", "era_end": "",
               "sha256": "", "archive_url": ""}
        if espn is None or era is None:
            rows.append(row)
            continue
        row["era_start"] = era["start"]
        row["era_end"] = "" if era["end"] is None else era["end"]
        mark = marks.get(espn, {}).get(DARK_VARIANT)
        if mark is None:
            report["missing_dark"].append(name)
            rows.append(row)
            continue
        row["archive_url"] = mark["archive_url"]
        try:
            raw = fetcher(mark["archive_url"])
        except Exception as error:  # noqa: BLE001 -- reported, and counted below
            print(f"{name}: {mark['archive_url']} failed ({type(error).__name__}: {error})",
                  flush=True)
            report["fetch_failed"].append(name)
            rows.append(row)
            continue
        digest = hashlib.sha256(raw).hexdigest()
        if digest != mark["sha256"]:
            raise ValueError(f"{name}: archive served {digest} where the manifest says {mark['sha256']}")
        # White is this artwork's ink, not its background, so it is not filled out.
        data, _ = encode_bytes(raw, size, remove_white=False)
        result[name] = {"espn": espn, "start": era["start"], "end": era["end"], "data": data}
        report["fetched"] += 1
        row["sha256"] = mark["sha256"]
        rows.append(row)

    failed = len(report["fetch_failed"])
    attempted = report["fetched"] + failed
    if failed and (not report["fetched"] or
                   (failed > FETCH_FAILURE_TOLERATED and failed > attempted * FETCH_FAILURE_LIMIT)):
        raise RuntimeError(
            f"{failed} of {attempted} images failed to download, which reads as an outage rather "
            f"than stale manifest rows. Nothing was written; try again later. "
            f"Failed: {', '.join(report['fetch_failed'])}")

    Path(out).write_text(json.dumps(result), encoding="utf-8")
    with Path(sources_out).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["team_name", "espn_id", "era_start", "era_end",
                                                    "sha256", "archive_url"])
        writer.writeheader()
        writer.writerows(rows)
    report["written"] = len(result)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="report what would be fetched, without touching the network")
    parser.add_argument("--size", type=int, default=MARK_SIZE)
    parser.add_argument("--manifest-url", default=MANIFEST_URL)
    args = parser.parse_args()
    report = build(dry_run=args.dry_run, size=args.size, manifest_url=args.manifest_url)
    print(f"{report['teams']} teams, {report['with_id']} with an ESPN id, "
          f"{report['fetched']} dark artworks fetched")
    for key in ("missing_id", "missing_dark", "fetch_failed"):
        if report[key]:
            print(f"  {key.replace('_', ' ')}: {', '.join(report[key])}")


if __name__ == "__main__":
    main()
