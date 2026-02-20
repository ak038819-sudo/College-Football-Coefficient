#v0.1 – Data & Playoff Foundation
##Added:
- FBS-only filtering in fetch_cfbd_games.py
- game_phase classification (regular / bowl / cfp)
- Alias resolution system:
- Team membership ingestion by season
- 12-team playoff builder
- Playoff detection fallback logic for 2015/2016

##Fixed:
- CFP misclassification for early seasons
- SQLite heredoc command usage issues
- Schema mismatch for playoff_field_by_year

##Known Constraints:
- load_games.py requires game_phase column (intentional strict mode)
- Alias system maps alias → canonical team_name