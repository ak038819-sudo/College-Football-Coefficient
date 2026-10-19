# Marks for the schools this project does not rate

An FBS team's logo comes from the dated set: one image per era, so a 1985 page
shows the 1985 mark (`src/build_dated_logo_assets.py`). The schools in
`non_fbs_games` never had one. They are not teams this project holds -- no page,
no rating, no row in `data/reference/team_identities.json` -- so a team page
named them in plain text and the live scoreboard drew a monogram from their
initials. 207 schools across 3,813 completed games, with nothing to look at.

`src/fetch_non_fbs_logos.py` fills that in.

## Where the marks come from

[sportsdataverse/sdv-assets](https://github.com/sportsdataverse/sdv-assets), an
archive of the marks the official providers serve, which keeps every version it
has seen. Its college-football rows come from ESPN's CDN and, for a handful of
schools ESPN lists without a logo, NCAA.com. Images are content-addressed by
sha256 on a DigitalOcean CDN; only the manifest (`manifest/marks.csv`) is in the
repository.

Two things that archive is not:

**It is not dated.** Every college-football row in its manifest has an empty
`valid_from`/`valid_to`, because ESPN's college CDN serves only today's mark.
(Its NHL, XFL and UFL sources do give season ranges; college does not.) So each
school here gets ONE undated entry, stored as starting at year 0 -- "every
season", since nothing reads that number as a date -- and the dated set remains
the only source of era-accurate logos. A school in both sets keeps its dated
eras; see `DATED_LOGOS` in `ui/dashboard_shell.html`.

**It is not a licence.** The archive grants nothing and claims nothing: its
README says the marks belong to their owners and are archived for nominative
use, which is exactly the footing the dated set is already on. These are used
here to identify the school that played the game. The repository's standing rule
for images still applies (`docs/people-pages.md`, "Portraits"): a reachable URL
is not permission, and nothing is taken from a third-party logo site. The
archive captures official sources only and never sportslogos.net, Sports
Reference or non-free Wikipedia files.

## How the join works

By ESPN team id, with nothing guessed.

`data/reference/espn_team_directory.json` is already in this repository (it is
the provider evidence behind `docs/team-identity-audit.md`) and files each
school under `location`, the school name without its mascot. Our opponent name
-- CFBD's own spelling, as `non_fbs_games.opponent_name` -- matches that exactly
for 185 of the 207 schools. No prefix matching, no fuzzy matching, no mascot
stripping.

The rest:

- **One rename.** `RENAMES` in the script maps Mankato State to Minnesota State.
  Mankato State became Minnesota State University, Mankato in 1998, and the
  games we hold under the old name were played before that.
- **Four FBS programs under an older name.** 'App State', 'Florida Atlantic',
  'Florida International' and 'UL Monroe' are teams this project holds, spelled
  the way the source spelled them in the seasons they were still FCS. They take
  their own dated eras, era-accurate, and nothing is fetched for them.
- **A name ESPN files twice** identifies nobody and gets no mark. ESPN lists two
  Charlottes, two Troys and two Roosevelts; none is a school we play, but the
  guard is there rather than taking whichever row came first.
- **21 schools ESPN does not list at all** keep the monogram. Almost all are
  defunct or long out of football: Bacone, Lambuth, Wesley, St. Paul's,
  Mankato-era opponents like Menlo, and programs that dropped the sport
  (Boston University, Wichita State, Hofstra, Long Beach State, Santa Clara,
  Pacific). The script prints the list on every run.
- **Eight schools ESPN lists but the archive has no mark for** keep the monogram
  too, and this is the group that can change on its own: a later capture may
  carry them. They are Eastern Oregon, Midwestern State, Oklahoma Panhandle,
  Point University, Reinhardt, Southwestern (KS), St. Francis (IL) and Virginia
  Lynchburg. The provenance file records their ESPN ids with the source columns
  empty, which is how you tell them from the 21 -- and from the four dated
  aliases, whose id column is blank because the row is not an ESPN lookup.

Measured on the first fetch, 2026-10-19 (run 39193931559): 207 opponents, 186
of them resolved to an ESPN id (185 exact plus the rename), 174 marks fetched, 4
taking their own dated eras, so 178 schools carry a mark and 29 keep the
monogram. Resolving to an id is not the same as the archive having a logo,
which is the difference between those last two bullets.

## The live scoreboard

A team page knows its opponent by CFBD's schedule spelling ('Charleston
Southern'), which is what the set is keyed by. The live feed does not: it names
a school ESPN's way, mascot and all ('Charleston Southern Buccaneers'), and
`ui/live_teams.js` resolves only the 139 teams in the registry, so a school
this project does not hold has no canonical name to look a mark up under.

So the ESPN id travels with the mark, in each entry's `espn` field and through
to the manifest, and `liveLogoName()` finds the mark by the feed's own team id.
That works because CFBD's scoreboard ids ARE ESPN's: all 124 teams in the feed
on 2026-10-18 matched the ESPN directory by id, the three FCS sides among them.
A school with no mark falls back to the feed's name, finds nothing and draws the
monogram, exactly as before.

Found by Codex on the first review of this work: merging the marks into the
name lookup alone fixed the team page and did nothing for the scoreboard.

## Running it

```sh
python3 src/fetch_non_fbs_logos.py --dry-run   # resolve and report, no network
python3 src/fetch_non_fbs_logos.py             # fetch, then write the set
python3 src/export_logo_files.py               # manifest + the files the page fetches
```

In CI: dispatch **Fetch non-FBS logos**
(`.github/workflows/fetch-non-fbs-logos.yml`), which does all three and commits
`ui/non_fbs_logo_assets.json`, `ui/logo_manifest.json`, `ui/assets/logos/` and
the provenance file. It commits with `[skip ci]`, so dispatch
`ci-and-deploy.yml` afterwards or the marks never reach the site.

Run it when a new opponent appears on a schedule, or when a school has redrawn
its mark. The archive re-captures monthly, so a redrawn mark shows up there
within a month of the provider serving it; the script takes the newest capture
of each mark.

Each image is flood-filled to remove a white background, resized to 96 px and
given a chip colour drawn from its own pixels -- the same treatment, through the
same functions, as the dated set.

## What is written down

| File | What it holds |
| --- | --- |
| `ui/non_fbs_logo_assets.json` | `{opponent_name: [{start: 0, end: null, data, bg}]}`, the committed source set |
| `data/reference/non_fbs_logo_sources.csv` | one row per school: its ESPN id and display name, the archive's source and sha256, and the URL the image came from -- or empty columns where it has no mark |
| `ui/assets/logos/non_fbs/<slug>/0-now.png` | what the browser actually fetches, derived by `src/export_logo_files.py` |

The provenance file is the record of which school got which mark, so a wrong
logo can be traced to the id it was taken from rather than re-derived.
