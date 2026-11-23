#!/usr/bin/env python3
"""Turn the stored team box score into the rows a game page draws.

CFBD reports every value as a STRING whose meaning depends on its category:
"417" is a number, "31:45" is a clock, "6-14" is made-of-attempted and
"5-45" is a count and a yardage. src/load_game_team_stats.py keeps those
strings verbatim; this is the one place that interprets them, so the
guesswork is testable instead of scattered.

TWO RULES WORTH STATING.

A category this file does not know is still SHOWN, below the ones it does,
under a label derived from its own name. CFBD adds categories season by
season, and a reader who can see "qbHurries" spelled oddly is better served
than one who cannot see it at all. Dropping the unfamiliar is this
repository's recurring fault.

A percentage is derived only from a value that actually parses as made-of-
attempted, and a conversion out of zero attempts has no percentage rather
than a 0%. Nobody failed on third down if nobody faced a third down.
"""
from __future__ import annotations

import re

# Conventional box-score order, which is not the order the provider returns.
# A category absent from this list is not unknown data, it is just data with
# no agreed place; it is appended after these.
DISPLAY_ORDER = [
    ("firstDowns", "First downs"),
    ("thirdDownEff", "Third down"),
    ("fourthDownEff", "Fourth down"),
    ("totalYards", "Total yards"),
    ("netPassingYards", "Passing yards"),
    ("completionAttempts", "Comp-Att"),
    ("yardsPerPass", "Yards per pass"),
    ("rushingYards", "Rushing yards"),
    ("rushingAttempts", "Rushing attempts"),
    ("yardsPerRushAttempt", "Yards per rush"),
    ("turnovers", "Turnovers"),
    ("fumblesLost", "Fumbles lost"),
    ("interceptions", "Interceptions thrown"),
    ("totalPenaltiesYards", "Penalties"),
    ("possessionTime", "Time of possession"),
    ("sacks", "Sacks"),
    ("tacklesForLoss", "Tackles for loss"),
    ("qbHurries", "QB hurries"),
    ("passesDeflected", "Passes deflected"),
    ("interceptionYards", "Interception return yards"),
    ("kickReturnYards", "Kick return yards"),
    ("puntReturnYards", "Punt return yards"),
]
KNOWN = {category for category, _label in DISPLAY_ORDER}
# Categories a reader already sees on the scoreboard or in the drive chart,
# and which say nothing new in a totals table.
SUPPRESSED = {"passingTDs", "rushingTDs", "kickingPoints"}

_FRACTION = re.compile(r"^\s*(\d+)\s*-\s*(\d+)\s*$")
# A category whose "a-b" is two different quantities, not made-of-attempted.
# Reading 5-45 as "5 of 45 penalties" would be a number nobody measured.
NOT_A_FRACTION = {"totalPenaltiesYards"}


def _abbreviation(word):
    """`TD` and `TDs` both are; `Yards` and `yards` are not. A trailing plural
    `s` is dropped before the test, so a pluralised abbreviation keeps its
    capitals instead of being flattened to lowercase."""
    stem = word[:-1] if len(word) > 1 and word.endswith("s") else word
    return len(stem) > 1 and stem.isupper()


def label_for(category):
    """A human label: the curated one, or the provider's own name spaced out.

    `label_for` never returns an empty string, because an unlabelled row in a
    table is worse than an awkwardly labelled one.
    """
    for known, label in DISPLAY_ORDER:
        if known == category:
            return label
    spaced = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", str(category or "").strip())
    words = spaced.replace("_", " ").split()
    if not words:
        return "Unnamed"
    # Sentence case, to match the curated labels: "First downs", not "First
    # Downs". An abbreviation is left alone, including a plural one: CFBD
    # writes `defensiveTDs`, and lowercasing that gave "Defensive tds" on a
    # live panel.
    rest = [w if _abbreviation(w) else w.lower() for w in words[1:]]
    first = (words[0] if _abbreviation(words[0])
             else words[0][:1].upper() + words[0][1:].lower())
    return " ".join([first] + rest)


def fraction(category, value):
    """(made, attempted) for a genuine made-of-attempted value, else None."""
    if category in NOT_A_FRACTION:
        return None
    match = _FRACTION.match(str(value or ""))
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def percent(category, value):
    """The conversion rate, 0-100 and rounded to one place, or None.

    None for a value that is not a fraction AND for a fraction out of zero:
    0 of 0 is not a 0% conversion rate, it is a team that never faced the
    down.
    """
    parsed = fraction(category, value)
    if not parsed or parsed[1] == 0:
        return None
    return round(100.0 * parsed[0] / parsed[1], 1)


def _order_key(category):
    for index, (known, _label) in enumerate(DISPLAY_ORDER):
        if known == category:
            return (0, index, "")
    return (1, 0, str(category))


def panel_rows(away_stats, home_stats):
    """[{category, label, away, home, away_pct, home_pct}] in reading order.

    `away_stats` and `home_stats` are {category: value string}. A category
    either side reported appears, with "" for the side that did not report
    it -- the two teams of one game can disagree on which categories exist,
    and blanking the row for both would lose a real measurement.
    """
    away_stats = dict(away_stats or {})
    home_stats = dict(home_stats or {})
    categories = (set(away_stats) | set(home_stats)) - SUPPRESSED
    rows = []
    for category in sorted(categories, key=_order_key):
        away = away_stats.get(category, "")
        home = home_stats.get(category, "")
        if str(away).strip() == "" and str(home).strip() == "":
            continue
        rows.append({"category": category, "label": label_for(category),
                     "away": str(away), "home": str(home),
                     "away_pct": percent(category, away),
                     "home_pct": percent(category, home)})
    return rows
