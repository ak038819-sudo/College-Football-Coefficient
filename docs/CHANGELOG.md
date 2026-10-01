# Site release history

## v0.1 — People and Places of the Game (in progress, local branch)

- Cross-season Find a Game page.
- Team home-field advantage table under Standings.
- Opt-in dynamic home-field Elo replay and comparison; production Elo remains unchanged.
- Canonical stadiums and season-specific team relationships, with conservative game venue resolution and stadium names on game cards and details.
- Stadium Explorer and verified venue links on game pages.
- Pregame, in-season team HFA comparison against flat Elo, using earlier games only. Production activation awaits held-out validation.
- Hourly finalization checks during regular-season game windows, with retries for late efficiency and player feeds.
- Player box-score archive and historical game-page display where CFBD supplies lines; older years require a backfill.

## v0.0 — Alpha (currently published)

- The published baseline before the People and Places of the Game update. This label describes the current live site; the new label appears on the site when v0.1 is published.

## Historical development notes

The earlier `v0.1-foundation` tag and the notes below refer to a development milestone, not the site release named v0.1 above.

### Data & Playoff Foundation
## Added:
- FBS-only filtering in fetch_cfbd_games.py
- game_phase classification (regular / bowl / cfp)
- Alias resolution system:
- Team membership ingestion by season
- 12-team playoff builder
- Playoff detection fallback logic for 2015/2016

## Fixed:
- CFP misclassification for early seasons
- SQLite heredoc command usage issues
- Schema mismatch for playoff_field_by_year

## Known Constraints:
- load_games.py requires game_phase column (intentional strict mode)
- Alias system maps alias → canonical team_name
