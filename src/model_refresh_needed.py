#!/usr/bin/env python3
"""Tell CI whether a live FBS final is absent or differs in the model's raw CSV.

The live scoreboard is independent of the historical/model pipeline. A push
normally reuses committed raw data; this check prevents it from republishing
week-old ratings when a newer final is already in the subscriber feed.
Prints `true` or `false` for a GitHub Actions step output.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

from current_season import season_for
from datetime import date


def needs_refresh(snapshot: dict, rows: list[dict], season: int) -> bool:
    by_id = {str(row["game_id"]): row for row in rows}
    for game in snapshot.get("games", []):
        if game.get("status") != "completed" or str(game.get("season")) != str(season):
            # The feed may omit season; its kickoff still locates the game.
            if game.get("status") != "completed" or game.get("season") is not None:
                continue
            if not str(game.get("start_date", "")).startswith(str(season)):
                continue
        home, away = game.get("home") or {}, game.get("away") or {}
        if home.get("classification", "").lower() != "fbs" or away.get("classification", "").lower() != "fbs":
            continue
        if not isinstance(home.get("points"), int) or not isinstance(away.get("points"), int):
            continue
        row = by_id.get(str(game.get("id")))
        if row is None or row.get("home_score") != str(home["points"]) or row.get("away_score") != str(away["points"]):
            return True
    return False


def main() -> None:
    season = season_for(date.today())
    live_path = Path("ui/data/live_scores.json")
    raw_path = Path(f"data/raw/games_{season}.csv")
    if not live_path.exists():
        print("false")
        return
    snapshot = json.loads(live_path.read_text(encoding="utf-8"))
    rows = list(csv.DictReader(raw_path.open(newline="", encoding="utf-8"))) if raw_path.exists() else []
    print("true" if needs_refresh(snapshot, rows, season) else "false")


if __name__ == "__main__":
    main()
