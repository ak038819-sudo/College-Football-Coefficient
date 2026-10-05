# Team identity audit — issue #33

Reviewed 2026-10-03; cross-check rerun against the October 4 exports. Canonical source: `data/reference/team_identities.json`; provider evidence: `data/reference/espn_team_directory.json`; reproducible report: `data/reference/team_identity_audit.json`.

139 supported program identities were matched to ESPN's 762-team directory. The review includes canonical names, exact aliases, provider display names and separately scoped IDs. Charlotte and Troy have lower-division homonyms: the selected identities are Charlotte 49ers (2429) and Troy Trojans (2653). No prefix matches are accepted. Texas Southern Tigers therefore cannot become Texas, and Florida Atlantic Owls resolves only to FAU.

CFBD scoreboard IDs are verified for the 115 supported programs present in the saved scoreboard evidence. The other 24 remain unverified for that provider namespace; the resolver can accept their exact reviewed name, but does not invent an ID or reuse an ESPN ID. A known conflicting ID/name pair is rejected. Internal database IDs are never re-keyed from this registry.

`python src/audit_team_identities.py --out data/reference/team_identity_audit.json` checks names and IDs through archived/scheduled games, predictions, team pages, Elo rows, standings, player seasons, rosters, coaches, stadium hosts, logos and colors. October 4 result: zero errors across 30,840 games, 271,131 roster rows, 5,657 coach tenures and 346,540 internal ID references. Current logo assignments for all 139 teams were inspected in contact sheets; 499 dated logo asset paths have correct program ownership and exist. The audit does not establish the historical first-use date of every logo or verify every hex color against a brand guide.

268 names outside the supported catalog are inventoried in the report, including FCS, lower-division and former FBS programs. They remain unresolved rather than being forced onto a similar supported name. Expanding historical/FCS coverage is a separate data-model task. A stadium host link is optional when the seed has no supported program; all 138 exported host links were verified.

Ingestion and person imports now use the same exact-name registry, including UMass -> Massachusetts. Tests reject ambiguous aliases, duplicate scoped provider IDs, database alias conflicts and provider homonyms. Browser tests cover every provider directory display name, including 623 negative matches. National player statistics keep source athlete IDs as identity keys instead of merging namesakes by name.

This audit verifies team attribution, not the correctness of every score/statistic, coach person identity, athlete ID or historical logo date. Those are tracked separately in `identity-problems.md` and `release-v0.1.9.md`.
