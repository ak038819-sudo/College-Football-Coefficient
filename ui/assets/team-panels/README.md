# Team panels

Eight teams have approved panels: Auburn, Ball State, BYU, Charlotte, Florida,
Kentucky, Ohio State and Tennessee. Foreground logos and all scores, names,
ratings and links remain live HTML. The decorative image has no foreground team
logo or embedded game information.

Add future approved art to this folder and register its canonical team name in
TEAM_PANELS in ui/dashboard_shell.html. The key must be the team's name exactly
as the ratings data spells it, or the row quietly keeps its chip and nothing
reports an error; tests/live_teams.test.cjs holds every key to a real team and
every image to a file that exists. Art is committed as opaque RGB PNG, which is
what these were delivered as once their unused alpha channel was dropped. The shared helpers apply it to scoreboard
rows and home/Standings Top-25 boards. Teams without artwork keep the existing
brand color and watermark treatment. Panel-enabled rows remove the logo-chip
background, while preserving dated official logo selection and image fallbacks.

Keep the skyline and identifying detail near the vertical center: background-size
cover crops the artwork for narrow rows without stretching. A compact rankings
row shows only about a third of the picture's height, so a subject that sits
high in the frame -- a mascot standing above a skyline -- falls outside that
band at the default position. Where it does, give the registry entry a focus,
which is the vertical background-position for that panel alone and defaults to
55%: smaller moves the band up. Tennessee uses 19% and Kentucky 24% so their
mascots' heads are in frame. Check both compact rankings and scoreboard rows on
mobile before adding a team. Keep school fonts
separate; use licensed web fonts when available, not lettering baked into art.

Build with python3 build_dashboard.py --reuse-exports. The source template and
its generated dashboard must both be committed. This first stage does not change
chips in dense tables, playoff brackets or other team pages.
