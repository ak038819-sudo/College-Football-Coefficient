#!/usr/bin/env python3
"""
Which college football season is "current" on a given date (Milestone 8).

A season spans late August through the January title game, so January and
February belong to the PREVIOUS calendar year's season (the CFP final in
January 2027 is part of the 2026 season). From March on, the upcoming
season's schedule is what CFBD publishes, so the calendar year is used.

The scheduled refresh uses this to pick which season to fetch; a naive
"current calendar year" would fetch an empty season every January and miss
the bowls and the title game.

Usage:
    python src/current_season.py            # prints e.g. 2026
    python src/current_season.py 2027-01-20 # prints 2026
"""
from __future__ import annotations

import datetime as dt
import sys

LAST_MONTH_OF_PRIOR_SEASON = 2   # January + February -> previous season


def season_for(day: dt.date) -> int:
    return day.year - 1 if day.month <= LAST_MONTH_OF_PRIOR_SEASON else day.year


if __name__ == "__main__":
    day = dt.date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else dt.date.today()
    print(season_for(day))
