"""Where a player is from, as a point a map can draw.

A player page carries a hometown as text (97.5% of this archive's 99,813
players have one), and the recruiting archive carries coordinates for the towns
recruits were signed from. Joining the two puts a pin on 89% of player pages
without fetching anything new.

Two ways a player gets a point, and the page says which:

  'own'  -- his OWN recruiting record names the coordinate. 36,824 players.
  'town' -- his hometown text matches, exactly, a town name and state that the
            recruiting archive holds a coordinate for. 52,018 players.

The second is this project's own inference rather than a fact a source stated
about him, which is the same distinction the box-score exporter draws between a
line attributed by athlete id and a line matched by name. Both are town-level
either way: the archive's coordinate is the town's, not the player's address.

PLAUSIBILITY. The feed's coordinates are mostly right and occasionally resolve
the wrong place entirely -- Death Valley, California at 65.2N in Alaska;
Woodberry Forest, Virginia in Charlotte, North Carolina; the DeKalb, Illinois
rows that half land in Crawford County, Ohio. A coordinate is therefore kept
only when it falls within BOX_MARGIN_DEGREES of the bounding box of the state
it claims, measured against the state geometry this site already ships for its
maps. 198 of 80,462 coordinates fail that, and spot-checking the biggest
offenders found every one of them genuinely misplaced.

The test is a bounding box and not the state polygon on purpose. That geometry
is simplified for drawing a national map, so a strict point-in-polygon test
rejects 1,851 coordinates that are perfectly correct -- Cincinnati sits on the
Ohio River boundary, Honolulu loses its island, and every coastal town from
Galveston to Staten Island falls outside the drawn coast. The box catches a
coordinate in the wrong REGION, which is the only error that matters for a dot
on a national map, and it does so without inventing a rejection.

The county FIPS column the same feed sends is NOT used. It disagrees with its
own row's state on 1,219 records whose coordinates are correct (West Bloomfield,
Michigan carries a New York county; Harvey, Louisiana an Illinois one), and one
row sends three digits instead of five.
"""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# The geometry the site's maps are drawn from, so a pin and the state it sits in
# cannot come from two different sources.
STATES_GEOJSON = ROOT / "ui" / "data" / "us_states.geojson"

# Degrees of slack around a state's bounding box. 0.6 rather than a tighter
# figure because 0.35 rejected Key West; the rejected set is identical from 0.6
# through 1.0, so the remaining failures are far from their claimed state rather
# than near its edge, and the exact value is not load-bearing.
BOX_MARGIN_DEGREES = 0.6

# The geojson names states; the feed abbreviates them. Public fact, written out
# rather than derived, so a rename in either source fails loudly here instead of
# quietly dropping a state's worth of pins.
STATE_ABBREVIATIONS = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "District of Columbia": "DC", "Florida": "FL", "Georgia": "GA",
    "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN",
    "Iowa": "IA", "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA",
    "Maine": "ME", "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI",
    "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO", "Montana": "MT",
    "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH",
    "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA",
    "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD",
    "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT",
    "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY",
}


def state_boxes(path: Path | None = None) -> dict[str, tuple[float, float, float, float]]:
    """abbreviation -> (min_lat, max_lat, min_lon, max_lon).

    The path is resolved inside the function, not bound as a default argument,
    so a test can point the module at a different geometry file.
    """
    geojson = json.loads(Path(path or STATES_GEOJSON).read_text())
    boxes: dict[str, tuple[float, float, float, float]] = {}
    for feature in geojson.get("features", []):
        abbreviation = STATE_ABBREVIATIONS.get((feature.get("properties") or {}).get("name"))
        if abbreviation is None:
            continue
        geometry = feature.get("geometry") or {}
        coordinates = geometry.get("coordinates") or []
        polygons = [coordinates] if geometry.get("type") == "Polygon" else coordinates
        # Outer ring only. A hole cannot widen a bounding box, so reading the
        # rings a state is cut out by would cost time and change nothing.
        points = [point for polygon in polygons for point in (polygon[0] if polygon else [])]
        if not points:
            continue
        lats = [point[1] for point in points]
        lons = [point[0] for point in points]
        boxes[abbreviation] = (min(lats), max(lats), min(lons), max(lons))
    return boxes


def plausible(boxes: dict, state: str | None, lat, lon) -> bool:
    """Whether a coordinate is near enough the state it claims to be drawn.

    A state this geometry does not carry (a territory, or a blank) is not
    plausible-by-default: there is nothing to check it against, so it is left
    off the map rather than drawn on trust.
    """
    box = boxes.get((state or "").strip().upper())
    if box is None:
        return False
    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        return False
    margin = BOX_MARGIN_DEGREES
    return (box[0] - margin <= lat <= box[1] + margin
            and box[2] - margin <= lon <= box[3] + margin)


def _key(city: str | None, state: str | None) -> tuple[str, str] | None:
    city = (city or "").strip().lower()
    state = (state or "").strip().upper()
    return (city, state) if city and state else None


def town_points(conn: sqlite3.Connection, boxes: dict) -> dict[tuple[str, str], tuple[float, float]]:
    """(town, state) -> coordinate, for towns the archive agrees about.

    An off-continent hometown needs no filter of its own: its state is not one
    of the fifty, so the plausibility check has nothing to measure it against
    and drops it. (The archive carries no coordinates at all for a recruit
    outside the United States today, so an explicit country filter here would
    exclude nothing and could never be tested.)

    A town whose plausible coordinates still disagree is dropped rather than
    averaged. Three town names in 5,469 have two coordinates at all, and a
    midpoint between two real places is a third place that is neither.
    """
    seen: dict[tuple[str, str], set[tuple[float, float]]] = defaultdict(set)
    for city, state, lat, lon in conn.execute(
            "SELECT city, state_province, hometown_latitude, hometown_longitude "
            "FROM recruits WHERE hometown_latitude IS NOT NULL "
            "AND hometown_longitude IS NOT NULL"):
        key = _key(city, state)
        if key is None or not plausible(boxes, state, lat, lon):
            continue
        seen[key].add((float(lat), float(lon)))
    return {key: next(iter(points)) for key, points in seen.items() if len(points) == 1}


def player_points(conn: sqlite3.Connection, boxes: dict | None = None) -> tuple[dict, dict]:
    """player_id -> [lat, lon, source], plus the counts worth reporting.

    A player's own recruiting record wins over his town's name every time, the
    same way an athlete id wins over a name match in the box-score archive: one
    is a source saying where he is from, the other is this project matching a
    string.

    A database with no recruiting archive loaded locates nobody, rather than
    failing: the pin is display data, and the people exports already run
    against databases that carry only some of what the site shows.
    """
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' "
                        "AND name = 'recruits'").fetchone():
        return {}, {"towns": 0, "own": 0, "town": 0, "located": 0}
    if boxes is None:
        boxes = state_boxes()
    towns = town_points(conn, boxes)

    points: dict[int, list] = {}
    for player_id, state, lat, lon in conn.execute(
            "SELECT e.player_id, r.state_province, r.hometown_latitude, r.hometown_longitude "
            "FROM recruits r JOIN player_external_ids e "
            "  ON e.source = 'cfbd' AND e.external_id = CAST(r.athlete_id AS TEXT) "
            "WHERE r.hometown_latitude IS NOT NULL AND r.hometown_longitude IS NOT NULL "
            "ORDER BY r.season_year"):
        if plausible(boxes, state, lat, lon):
            points[int(player_id)] = [float(lat), float(lon), "own"]

    matched = 0
    for player_id, hometown, home_state in conn.execute(
            "SELECT player_id, hometown, home_state FROM players"):
        player_id = int(player_id)
        if player_id in points:
            continue
        key = _key(hometown, home_state)
        point = towns.get(key) if key else None
        if point is not None:
            points[player_id] = [point[0], point[1], "town"]
            matched += 1

    counts = {
        "towns": len(towns),
        "own": len(points) - matched,
        "town": matched,
        "located": len(points),
    }
    return points, counts
