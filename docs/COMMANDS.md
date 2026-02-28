# Evergreen Commands (Fixing College Football)

These are “evergreen” utilities for quickly inspecting CoEfficient (CoE) outputs from `db/league.db`.

> All commands assume you are running from the repository root in Codespaces.

---

## CoE CLI

### Team CoE by Year

Pull a team’s CoE values across seasons (team name or alias supported via `teams` + `team_aliases`).

```bash
python src/coe_cli.py team "Alabama"

Single season:
    python src/coe_cli.py team "Alabama" --year 2022

Range:
    python src/coe_cli.py team "Alabama" --start-year 2014 --end-year 2024

Alias example (only works if alias exists in team_aliases):
    python src/coe_cli.py team "Bama" --start-year 2014 --end-year 2024

Conference / League CoE by Year

Pull a conference/league CoE across seasons.

python src/coe_cli.py conf "SEC"

Single season:

python src/coe_cli.py conf "SEC" --year 2022

Range:

python src/coe_cli.py conf "SEC" --start-year 2014 --end-year 2024
Overrides (if schema/table names change)

The CLI tries to auto-detect table/column names, but you can override anything.

Override coefficient table name
python src/coe_cli.py conf "SEC" --table conference_coefficient_by_year
Override year/season and name columns
python src/coe_cli.py conf "SEC" --table conference_coefficient_by_year --year-col season --name-col conference
Override PPG / points columns
python src/coe_cli.py team "Alabama" --ppg-col points_per_game_5yr --points-col total_points_5yr
Helpful SQLite introspection

List tables:
    sqlite3 db/league.db ".tables"

Check a table schema:
    sqlite3 db/league.db ".schema team_coefficient_by_year"
    sqlite3 db/league.db ".schema conference_coefficient_by_year"
    sqlite3 db/league.db ".schema team_aliases"

Quick look at a few rows:

sqlite3 -header -column db/league.db "SELECT * FROM team_coefficient_by_year LIMIT 10;"

### Add a pointer in your README
In `README.md`, add a short “Quick Commands” section:

```md
## Quick Commands

See `docs/COMMANDS.md` for evergreen CLI utilities (team + conference CoE queries).