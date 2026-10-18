#!/usr/bin/env python3
"""
National award winners, from ESPN's public college-football awards endpoint.

CollegeFootballData has no awards endpoint -- it carries rosters, statistics,
recruiting and play-by-play, but nothing about honours -- so this is the first
feed in the project that does not come from CFBD. ESPN publishes one index per
season listing roughly thirty awards (the Heisman, the position awards, the
player- and coach-of-the-year honours), each dereferenced to a record naming
its winner.

    index   /seasons/<year>/awards?limit=100
            -> {"pageCount": n, "items": [{"$ref": ...}, ...]}
    award   each $ref
            -> {"id", "name", "description", "history",
                "winners": [{"athlete": {"$ref": .../athletes/<id>},
                             "team":    {"$ref": .../teams/<id>}}]}
    athlete each winner's athlete $ref, because the award record carries only
            the id -- the NAME is what the rest of this project can resolve.

The archive written here is normalised JSON, one file per season, committed so
that a rebuild of an old season does not depend on what ESPN serves today. The
loader (src/load_player_awards.py) is what turns a name into one of this
project's people, and it is deliberately a separate step: resolving an identity
is the part that goes wrong, and it must be fixable without re-fetching.

Nothing here invents a winner. An award ESPN lists with no winner for a season
is written with `winners: []`, and a season ESPN has no index for writes no
file at all, so "not awarded", "not fetched" and "nobody won it" stay three
different states.

Usage:
    python src/fetch_espn_awards.py 2024            # one season
    python src/fetch_espn_awards.py 2005 2026       # a range, one file each
"""
from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import cfbd_http

BASE = ("https://sports.core.api.espn.com/v2/sports/football/leagues/"
        "college-football/seasons")
OUT_DIR = Path("data/raw/awards")
TIMEOUT = 30.0

# ESPN's core API answers an unadorned client with a 403, so the request looks
# like a browser on espn.com. This is the same shape the public R and Python
# clients for this endpoint send.
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.espn.com",
    "Referer": "https://www.espn.com/",
}


def get(url: str, params: dict | None = None, describe: str = "ESPN request"):
    """A retried GET. Reuses cfbd_http, which is a general retrying transport:
    it retries a request that got no answer and never one the API answered."""
    return cfbd_http.get_json(url, headers=HEADERS, timeout=TIMEOUT,
                              params=params, describe=describe)


def ref_id(ref) -> str | None:
    """The numeric id at the end of an ESPN `$ref` path, e.g. .../athletes/4426339."""
    if not isinstance(ref, str):
        return None
    path = ref.split("?", 1)[0].rstrip("/")
    tail = path.rsplit("/", 1)[-1]
    return tail if tail.isdigit() else None


def ref_of(value) -> str | None:
    """A `$ref` out of a nested object, or None when ESPN gave none."""
    if isinstance(value, dict):
        ref = value.get("$ref")
        return ref if isinstance(ref, str) else None
    return None


def index_refs(year: int) -> list[str]:
    """Every award `$ref` for a season, following ESPN's paging."""
    url = f"{BASE}/{year}/awards"
    first = get(url, {"limit": 100, "lang": "en", "region": "us"},
                describe=f"ESPN award index {year}")
    items = list(first.get("items") or [])
    pages = first.get("pageCount") or 1
    for page in range(2, int(pages) + 1):
        more = get(url, {"limit": 100, "lang": "en", "region": "us", "page": page},
                   describe=f"ESPN award index {year} page {page}")
        items.extend(more.get("items") or [])
    return [ref for ref in (ref_of(item) for item in items) if ref]


def athlete_name(ref: str, cache: dict[str, dict]) -> dict:
    """An athlete's name and position from its own record, cached per run.

    One athlete can win several awards in a season (a Heisman winner usually
    wins three or four), and each award record names only the id.
    """
    if ref in cache:
        return cache[ref]
    try:
        record = get(ref, describe=f"ESPN athlete {ref_id(ref)}")
    except Exception as error:  # noqa: BLE001
        # A winner whose athlete record will not load is still a winner, and the
        # award still happened. The name is simply unknown, and the loader will
        # record it as unresolved rather than the run failing over one person.
        print(f"  athlete {ref_id(ref)} did not load ({type(error).__name__}: {error})",
              flush=True)
        cache[ref] = {}
        return cache[ref]
    position = record.get("position")
    cache[ref] = {
        "display_name": record.get("displayName") or record.get("fullName"),
        "first_name": record.get("firstName"),
        "last_name": record.get("lastName"),
        "position": (position or {}).get("abbreviation") if isinstance(position, dict) else None,
        "jersey": record.get("jersey"),
    }
    return cache[ref]


def team_name(ref: str, cache: dict[str, dict]) -> dict:
    """A team's school name from its own record, cached per run."""
    if ref in cache:
        return cache[ref]
    try:
        record = get(ref, describe=f"ESPN team {ref_id(ref)}")
    except Exception as error:  # noqa: BLE001
        print(f"  team {ref_id(ref)} did not load ({type(error).__name__}: {error})", flush=True)
        cache[ref] = {}
        return cache[ref]
    cache[ref] = {
        # `location` is the school ("Michigan"); displayName includes the
        # nickname. The school is what this project's team aliases resolve.
        "school": record.get("location"),
        "display_name": record.get("displayName"),
        "abbreviation": record.get("abbreviation"),
    }
    return cache[ref]


def winner_record(winner: dict, athletes: dict, teams: dict) -> dict:
    """One winner, flattened, with whatever ESPN actually gave about them."""
    athlete_ref = ref_of(winner.get("athlete"))
    team_ref = ref_of(winner.get("team"))
    row = {
        "espn_athlete_id": ref_id(athlete_ref),
        "espn_team_id": ref_id(team_ref),
        "display_name": None,
        "position": None,
        "school": None,
    }
    if athlete_ref:
        row.update({k: v for k, v in athlete_name(athlete_ref, athletes).items()
                    if k in ("display_name", "first_name", "last_name", "position", "jersey")})
    if team_ref:
        team = team_name(team_ref, teams)
        row["school"] = team.get("school")
        row["team_display_name"] = team.get("display_name")
    return row


def fetch_season(year: int) -> dict | None:
    """One season's awards, or None when ESPN has no index for it."""
    refs = index_refs(year)
    if not refs:
        return None
    athletes: dict[str, dict] = {}
    teams: dict[str, dict] = {}
    awards = []
    for ref in refs:
        award = get(ref, describe=f"ESPN award {ref_id(ref)}")
        awards.append({
            "espn_award_id": str(award.get("id")) if award.get("id") is not None else ref_id(ref),
            "name": award.get("name"),
            "description": award.get("description"),
            "winners": [winner_record(w, athletes, teams)
                        for w in (award.get("winners") or [])],
        })
    awards.sort(key=lambda a: (a["name"] or "", a["espn_award_id"] or ""))
    return {"season": year, "source": "espn", "awards": awards}


def write_season(payload: dict, out_dir: Path = OUT_DIR) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{payload['season']}.json.gz"
    body = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write(body)
    return path


def summarise(payload: dict) -> str:
    awards = payload["awards"]
    winners = [w for a in awards for w in a["winners"]]
    named = sum(1 for w in winners if w.get("display_name"))
    with_school = sum(1 for w in winners if w.get("school"))
    return (f"{payload['season']}: {len(awards)} awards, {len(winners)} winners, "
            f"{named} named, {with_school} with a school")


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 2
    first = int(argv[0])
    last = int(argv[1]) if len(argv) > 1 else first
    for year in range(first, last + 1):
        payload = fetch_season(year)
        if payload is None:
            # No index at all: ESPN does not carry this season. Writing an empty
            # file would say "no awards were given", which is a different claim.
            print(f"{year}: ESPN has no award index for this season; no file written.",
                  flush=True)
            continue
        path = write_season(payload)
        print(f"{summarise(payload)} -> {path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
