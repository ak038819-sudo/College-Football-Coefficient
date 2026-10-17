"""Where a player page's hometown pin comes from (src/hometown_points.py).

Three things can go wrong here without anything failing: a coordinate the feed
resolved to the wrong state drawn as if it were right, a town whose two
coordinates get averaged into a third place, and a name match quietly
overwriting what a player's own recruiting record says.
"""
import json
import sqlite3

import pytest

from hometown_points import (BOX_MARGIN_DEGREES, STATE_ABBREVIATIONS, plausible,
                             player_points, state_boxes, town_points)


@pytest.fixture(scope="module")
def boxes():
    return state_boxes()


def _db(recruits=(), players=(), external=(), with_recruits_table=True):
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE players (player_id INTEGER PRIMARY KEY, display_name TEXT,
            hometown TEXT, home_state TEXT);
        CREATE TABLE player_external_ids (player_id INTEGER, source TEXT, external_id TEXT);
    """)
    if with_recruits_table:
        conn.executescript("""
            CREATE TABLE recruits (recruit_id INTEGER PRIMARY KEY, athlete_id INTEGER,
                season_year INTEGER, city TEXT, state_province TEXT, country TEXT,
                hometown_latitude REAL, hometown_longitude REAL);
        """)
        conn.executemany(
            "INSERT INTO recruits (recruit_id, athlete_id, season_year, city, state_province, "
            "country, hometown_latitude, hometown_longitude) VALUES (?,?,?,?,?,?,?,?)", recruits)
    conn.executemany(
        "INSERT INTO players (player_id, display_name, hometown, home_state) VALUES (?,?,?,?)",
        players)
    conn.executemany(
        "INSERT INTO player_external_ids (player_id, source, external_id) VALUES (?,?,?)",
        external)
    conn.commit()
    return conn


def test_every_state_in_the_site_geometry_is_named(boxes):
    # The geojson is the site's own map data. A state it names that this module
    # does not should fail here rather than silently drop that state's pins.
    geojson = json.loads((__import__("hometown_points").STATES_GEOJSON).read_text())
    names = {(f.get("properties") or {}).get("name") for f in geojson["features"]}
    assert names <= set(STATE_ABBREVIATIONS), names - set(STATE_ABBREVIATIONS)
    assert len(boxes) == 51, "fifty states and the District of Columbia"
    low, high, west, east = boxes["TX"]
    assert low < 30.27 < high and west < -97.74 < east, "Austin is in Texas"


def test_a_coordinate_in_the_wrong_state_is_not_drawn(boxes):
    # The real case: five DeKalb, Illinois recruiting rows carry a coordinate in
    # Crawford County, Ohio. Four others carry the real one.
    assert not plausible(boxes, "IL", 40.8925566, -82.7726802)
    assert plausible(boxes, "IL", 41.9294736, -88.7503647)
    # Woodberry Forest, Virginia resolved to Charlotte, North Carolina.
    assert not plausible(boxes, "VA", 35.1668118, -80.7514585)
    # Death Valley, California resolved to 65N, in Alaska.
    assert not plausible(boxes, "CA", 65.189, -162.357)


def test_a_correct_coordinate_on_a_border_or_a_coast_is_kept(boxes):
    # These are why the test is a bounding box and not the drawn polygon: all
    # four are correct, and a strict point-in-polygon test against simplified
    # map geometry rejects every one of them.
    assert plausible(boxes, "OH", 39.1014537, -84.5124602)     # Cincinnati, on the river
    assert plausible(boxes, "HI", 21.304547, -157.8556764)     # Honolulu
    assert plausible(boxes, "TX", 29.299328, -94.7945882)      # Galveston
    assert plausible(boxes, "FL", 24.5629, -81.7722)           # Key West
    assert plausible(boxes, "NY", 40.564209, -74.1253046)      # Staten Island


def test_a_state_the_geometry_does_not_carry_is_not_trusted(boxes):
    # A territory or a blank has nothing to check a coordinate against, so it is
    # left off the map rather than drawn on trust.
    assert not plausible(boxes, "PR", 18.4655, -66.1057)
    assert not plausible(boxes, None, 40.0, -80.0)
    assert not plausible(boxes, "", 40.0, -80.0)
    assert not plausible(boxes, "OH", None, -84.0)
    assert not plausible(boxes, "OH", "forty", -84.0)


def test_the_margin_is_slack_and_not_a_second_state(boxes):
    low, high, west, east = boxes["OH"]
    assert plausible(boxes, "OH", low - BOX_MARGIN_DEGREES / 2, (west + east) / 2)
    assert not plausible(boxes, "OH", low - BOX_MARGIN_DEGREES - 0.5, (west + east) / 2)


def test_a_town_with_two_coordinates_is_dropped_not_averaged(boxes):
    conn = _db(recruits=[
        # Dekalb, Illinois: both spellings, both plausible, different places.
        (1, None, 2015, "DeKalb", "IL", "USA", 41.9294736, -88.7503647),
        (2, None, 2016, "DeKalb", "IL", "USA", 41.5, -88.0),
        # Nashville: every row agrees.
        (3, None, 2015, "Nashville", "TN", "USA", 36.1627, -86.7816),
        (4, None, 2016, "nashville", "TN", "USA", 36.1627, -86.7816),
    ])
    towns = town_points(conn, boxes)
    assert ("dekalb", "IL") not in towns, "a midpoint between two towns is a third town"
    assert towns[("nashville", "TN")] == (36.1627, -86.7816)


def test_an_implausible_row_does_not_poison_its_town(boxes):
    # The Illinois case as the archive actually holds it: the wrong coordinate is
    # the more common one, so a mode or a majority vote picks Ohio. Dropping it
    # on plausibility leaves one coordinate, and the town keeps its pin.
    conn = _db(recruits=[
        (1, None, 2015, "Dekalb", "IL", "USA", 40.8925566, -82.7726802),
        (2, None, 2015, "Dekalb", "IL", "USA", 40.8925566, -82.7726802),
        (3, None, 2015, "Dekalb", "IL", "USA", 40.8925566, -82.7726802),
        (4, None, 2016, "DeKalb", "IL", "USA", 41.9294736, -88.7503647),
    ])
    assert town_points(conn, boxes)[("dekalb", "IL")] == (41.9294736, -88.7503647)


def test_an_off_continent_hometown_is_not_a_town_point(boxes):
    conn = _db(recruits=[(1, None, 2015, "Melbourne", "VIC", "Australia", -37.81, 144.96)])
    assert town_points(conn, boxes) == {}


def test_his_own_record_beats_a_match_on_his_town_name(boxes):
    conn = _db(
        recruits=[
            # His own row, naming a different point from the town's others.
            (1, 555, 2015, "Portland", "OR", "USA", 45.52, -122.68),
            (2, None, 2016, "Austin", "TX", "USA", 30.2672, -97.7431),
            (3, None, 2016, "Austin", "TX", "USA", 30.2672, -97.7431),
        ],
        players=[(555, "Own Record", "Austin", "TX"), (777, "Name Match", "Austin", "TX"),
                 (888, "Nowhere", "Lagos", None)],
        external=[(555, "cfbd", "555"), (777, "cfbd", "777"), (888, "cfbd", "888")])
    points, counts = player_points(conn, boxes)
    assert points[555] == [45.52, -122.68, "own"]
    assert points[777] == [30.2672, -97.7431, "town"]
    assert 888 not in points, "no coordinate means no pin"
    # Two towns: Austin, and the Portland his own row names.
    assert counts == {"towns": 2, "own": 1, "town": 1, "located": 2}


def test_an_implausible_own_record_falls_back_to_his_town(boxes):
    conn = _db(
        recruits=[
            (1, 555, 2015, "Woodberry Forest", "VA", "USA", 35.1668118, -80.7514585),
            (2, None, 2016, "Richmond", "VA", "USA", 37.5407, -77.4360),
        ],
        players=[(555, "Bad Coordinate", "Richmond", "VA")],
        external=[(555, "cfbd", "555")])
    points, counts = player_points(conn, boxes)
    assert points[555] == [37.5407, -77.4360, "town"]
    assert counts["own"] == 0


def test_a_database_with_no_recruiting_archive_locates_nobody():
    conn = _db(players=[(555, "Nobody", "Austin", "TX")], with_recruits_table=False)
    points, counts = player_points(conn)
    assert points == {}
    assert counts["located"] == 0
