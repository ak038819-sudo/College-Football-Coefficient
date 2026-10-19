# Team panels

The BYU mountain background is the first approved panel. Foreground logos and all
scores, names, ratings and links remain live HTML. The decorative image has no
foreground team logo or embedded game information.

Add future approved art to this folder and register its canonical team name in
TEAM_PANELS in ui/dashboard_shell.html. The shared helpers apply it to scoreboard
rows and home/Standings Top-25 boards. Teams without artwork keep the existing
brand color and watermark treatment. Panel-enabled rows remove the logo-chip
background, while preserving dated official logo selection and image fallbacks.

Keep the skyline and identifying detail near the vertical center: background-size
cover crops the artwork for narrow rows without stretching. Check both compact
rankings and scoreboard rows on mobile before adding a team. Keep school fonts
separate; use licensed web fonts when available, not lettering baked into art.

Build with python3 build_dashboard.py --reuse-exports. The source template and
its generated dashboard must both be committed. This first stage does not change
chips in dense tables, playoff brackets or other team pages.
