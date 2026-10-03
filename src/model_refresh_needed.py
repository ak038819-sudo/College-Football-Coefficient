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

import player_archive
from current_season import season_for
from datetime import date, datetime, timezone, timedelta


def needs_refresh(snapshot: dict, rows: list[dict], season: int,
                  advanced_ids: set[str] | None = None, player_ids: set[str] | None = None,
                  now: datetime | None = None) -> bool:
    by_id = {str(row["game_id"]): row for row in rows}
    for game in snapshot.get("games", []):
        if game.get("status") != "completed" or str(game.get("season")) != str(season):
            # The feed may omit season; its kickoff still locates the game.
            if game.get("status") != "completed" or game.get("season") is not None:
                continue
            try:
                kickoff_date = date.fromisoformat(str(game.get('start_date'))[:10])
            except ValueError:
                continue
            if season_for(kickoff_date) != season:
                continue
        home, away = game.get("home") or {}, game.get("away") or {}
        if home.get("classification", "").lower() != "fbs" or away.get("classification", "").lower() != "fbs":
            continue
        if not isinstance(home.get("points"), int) or not isinstance(away.get("points"), int):
            continue
        row = by_id.get(str(game.get("id")))
        if row is None or row.get("home_score") != str(home["points"]) or row.get("away_score") != str(away["points"]):
            return True
        # CFBD can mark a game final before its advanced and player feeds arrive.
        # Retry for two days, then leave unavailable stats explicitly absent.
        if advanced_ids is not None or player_ids is not None:
            try:
                kickoff = datetime.fromisoformat(str(game['start_date']).replace('Z', '+00:00'))
                recent = timedelta(0) <= (now or datetime.now(timezone.utc)) - kickoff <= timedelta(days=2)
            except (KeyError, TypeError, ValueError):
                recent = False
            if recent and ((advanced_ids is not None and str(game['id']) not in advanced_ids) or
                           (player_ids is not None and str(game['id']) not in player_ids)):
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
    advanced = Path(f'data/raw/game_advanced_{season}.csv')
    advanced_counts: dict[str, int] = {}
    if advanced.exists():
        with advanced.open(newline='', encoding='utf-8') as source:
            for row in csv.DictReader(source):
                if row.get('off_success_rate') not in (None, ''):
                    gid = row['game_id']
                    advanced_counts[gid] = advanced_counts.get(gid, 0) + 1
    advanced_ids = {gid for gid, count in advanced_counts.items() if count >= 2}
    player_ids = set(player_archive.read_season(player_archive.DEFAULT_DIR, season))
    print("true" if needs_refresh(snapshot, rows, season, advanced_ids, player_ids) else "false")


if __name__ == "__main__":
    main()
