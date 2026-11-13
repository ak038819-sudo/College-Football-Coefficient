"""Export the NBA archive as static files, with a manifest and a gate.

Writes `ui/data/nba/` and nothing else: no college football file, no
basketball file, no NFL file, no shared global. A reader of these files can
always answer three questions before it draws a number — which model made it,
what that model scored on games it had never seen, and how much of the league
is in here — because the manifest is refused unless it can.

The gate in `src/nba/publish_gate.py` runs BEFORE the first byte is written.
An export that would fail it is not written at all, because a half-written
export directory is itself a way to publish a sample.

Two NBA facts shape what comes out of here.

**A GAME'S RECORD IS NOT ITS STANDINGS LINE.** The NBA Cup final is a real
game and the league keeps it out of the standings, so the won-lost tally
counts regular-season games only and the game itself still appears, rated,
flagged `cup_final`. A page that summed every completed game would put two
franchises a game ahead of the record the NBA credits them with.

**AN UNPLAYED GAME HAS NO SCORE, NOT A ZERO.** The loader has already turned
the source's 0-0 fixtures into nulls; nothing here may put a number back.
"""

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from . import elo, game_export, people_export, series, team_box, team_metrics
from .identity import current_season, season_label
from . import win_probability
from .publish_gate import SCHEMA_VERSION, check, render
from .store import connect

OUT_DIR = Path('ui/data/nba')
MANIFEST = 'manifest.json'

# The game types that count toward a won-lost record. The Cup final is a
# completed game between two franchises and is NOT one of them: see the
# module note.
RECORD_TYPES = frozenset({'regular'})


class PublishRefused(Exception):
    """Raised instead of writing, or instead of keeping, a refused export."""

    def __init__(self, reasons):
        super().__init__(render(reasons))
        self.reasons = list(reasons)


def model_row(connection, name=None):
    """The newest stored model, or the one named. Never two, never a guess."""
    sql = ('SELECT model_id, name, config, fitted_on, held_out, holdout_brier,'
           '       holdout_games, created_at FROM rating_model')
    if name:
        row = connection.execute(sql + ' WHERE name = ?', (name,)).fetchone()
    else:
        row = connection.execute(
            sql + ' ORDER BY model_id DESC LIMIT 1').fetchone()
    if row is None:
        raise ValueError('No rating model is stored; run build_ratings first')
    return dict(row)


def _seasons_json(value):
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return []
    return parsed if isinstance(parsed, list) else []


def model_section(row):
    config = json.loads(row['config'])
    canonical = json.dumps(config, sort_keys=True,
                           separators=(',', ':')).encode('utf-8')
    return dict(
        name=row['name'],
        config=config,
        config_sha256=hashlib.sha256(canonical).hexdigest(),
        fitted_on=_seasons_json(row['fitted_on']),
        held_out=_seasons_json(row['held_out']),
        holdout_brier=row['holdout_brier'],
        holdout_games=row['holdout_games'],
        built_at=row['created_at'],
    )


def teams(connection):
    """Every franchise, keyed by franchise key.

    The name here is the franchise's CURRENT name. A season's own name lives
    in that season's payload, because team 25 is the SuperSonics in 2007 and
    the Thunder now, and one index cannot be both.
    """
    rows = connection.execute(
        'SELECT team_key, team_id, abbreviation, location, nickname, name,'
        '       conference, logo, logo_dark, first_season, last_season'
        '  FROM team ORDER BY team_id').fetchall()
    return {row['team_key']: dict(
        id=row['team_id'], abbr=row['abbreviation'],
        location=row['location'], nickname=row['nickname'],
        name=row['name'], conference=row['conference'],
        logo=row['logo'], logo_dark=row['logo_dark'],
        first_season=row['first_season'], last_season=row['last_season'],
    ) for row in rows}


def season_names(connection, season):
    """What the source called each franchise in one season."""
    rows = connection.execute(
        'SELECT team_key, abbreviation, location, nickname, name, conference'
        '  FROM team_season WHERE season = ?', (season,)).fetchall()
    return {row['team_key']: dict(
        abbr=row['abbreviation'], location=row['location'],
        nickname=row['nickname'], name=row['name'],
        conference=row['conference']) for row in rows}


def season_games(connection, season, model_id=None):
    """One season's games, in tip-off order, with the model's own chain."""
    rows = connection.execute(
        'SELECT game_key, provider_id, season_type, postseason, game_date,'
        '       tipoff_utc, tipoff_tbd, neutral_site, completed,'
        '       home_team_key, away_team_key, home_points, away_points,'
        '       venue, venue_city, venue_state, attendance, notes, rateable'
        '  FROM game WHERE season = ?'
        "  ORDER BY game_date, COALESCE(tipoff_utc, ''), provider_id",
        (season,)).fetchall()
    chain = {}
    if model_id is not None:
        chain = {row['game_key']: row for row in connection.execute(
            'SELECT r.game_key, r.home_pregame, r.away_pregame,'
            '       r.home_expected FROM rating_game r JOIN game g'
            '    ON g.game_key = r.game_key'
            ' WHERE g.season = ? AND r.model_id = ?', (season, model_id))}
    out = []
    for row in rows:
        game = dict(
            id=row['game_key'],
            date=row['game_date'],
            at=row['tipoff_utc'],
            type=row['season_type'],
            home=row['home_team_key'],
            away=row['away_team_key'],
            # A missing score stays missing. A zero is a measurement.
            home_points=row['home_points'],
            away_points=row['away_points'],
            rateable=row['rateable'],
        )
        if row['neutral_site']:
            game['neutral'] = 1
        if row['tipoff_tbd']:
            game['time_tbd'] = 1
        if row['postseason']:
            game['postseason'] = 1
        for field, column in (('venue', 'venue'), ('city', 'venue_city'),
                              ('state', 'venue_state'),
                              ('attendance', 'attendance'),
                              ('notes', 'notes')):
            if row[column] is not None and row[column] != '':
                game[field] = row[column]
        # What the model thought before tip-off: each side's rating and the
        # home side's win probability. This is the stored chain, not a number
        # recomputed from today's ratings, so a 2007 card shows what the model
        # believed in 2007 rather than what it believes now.
        stored = chain.get(row['game_key'])
        if stored is not None:
            game['home_elo'] = round(stored['home_pregame'], 1)
            game['away_elo'] = round(stored['away_pregame'], 1)
            game['home_wp'] = round(stored['home_expected'], 4)
        out.append(game)
    return out


def records(games):
    """Each franchise's won-lost, from finished REGULAR-SEASON games only.

    The Cup final is deliberately absent: the NBA does not count it in the
    standings, and a page that did would show two franchises with 83 games.
    """
    tally = defaultdict(lambda: dict(w=0, l=0))
    for game in games:
        if game['type'] not in RECORD_TYPES:
            continue
        home, away = game['home_points'], game['away_points']
        if home is None or away is None or home == away:
            continue
        winner = game['home'] if home > away else game['away']
        loser = game['away'] if home > away else game['home']
        tally[winner]['w'] += 1
        tally[loser]['l'] += 1
    return tally


def expect_upcoming(games, rows, config):
    """Fill in the pregame numbers for games the replay has not rated.

    The chain in `rating_game` only covers games that have been played, so an
    UPCOMING game reaches the page with no probability at all — which is the
    one card a reader most wants a number on. Each side is played at its
    current rating, which for a team's very next game IS its pregame rating.

    For a game further out it is an estimate rather than a stored fact,
    because both sides play in between, which is why it is marked and why it
    never overwrites a stored number.
    """
    ratings = {row['team']: row['rating'] for row in rows or []}
    advantage = float(config.get('home_advantage') or 0.0)
    scale = float(config.get('scale') or elo.SCALE)
    for game in games:
        if 'home_wp' in game:
            continue
        if game['home_points'] is not None or game['away_points'] is not None:
            continue
        home, away = ratings.get(game['home']), ratings.get(game['away'])
        if home is None or away is None:
            continue
        game['home_elo'] = round(home, 1)
        game['away_elo'] = round(away, 1)
        game['home_wp'] = round(elo.expected_home(
            home, away, neutral=bool(game.get('neutral')),
            home_advantage=advantage, scale=scale), 4)
        game['wp_estimated'] = 1


def board(connection, season, model_id, games, names=None, index=None):
    """The season's rating board: one row a franchise, ranked.

    Ranked here rather than in the page, so every view of this season agrees
    on what #7 is.

    THE CONFERENCE FALLS BACK TO THE FRANCHISE'S CURRENT ONE. The standings
    feed for a season IN PROGRESS has not been published yet — measured
    2026-11-04: all thirty 2026-27 rows carry a NULL conference while every
    earlier season carries one — so a board built from the per-season rows
    alone put the whole league in one unnamed group on the only season a
    reader came to look at. The franchise's conference now is a fact, and
    `conference_from` says which of the two a row used so that nothing
    claims the feed recorded something it did not. `conference_history` does
    NOT fall back: a trend may not be drawn from today's alignment applied
    backwards.
    """
    names = names or {}
    index = index or {}
    ratings = connection.execute(
        'SELECT team_key, rating, games_counted FROM team_rating'
        ' WHERE season = ? AND model_id = ?', (season, model_id)).fetchall()
    tally = records(games)
    rows = []
    for rating in ratings:
        key = rating['team_key']
        record = tally.get(key, dict(w=0, l=0))
        named = names.get(key, {})
        current = index.get(key, {})
        conference = named.get('conference')
        rows.append(dict(
            team=key,
            name=named.get('name'),
            abbr=named.get('abbr'),
            conference=conference or current.get('conference'),
            conference_from=('season' if conference
                             else ('franchise' if current.get('conference')
                                   else None)),
            # Stored to one decimal: the display rounds to a whole number, and
            # a rounded value must never be what the next calculation reads.
            rating=round(rating['rating'], 1),
            rated_games=rating['games_counted'],
            wins=record['w'], losses=record['l'],
        ))
    rows.sort(key=lambda row: (-row['rating'], row['team']))
    for index, row in enumerate(rows, start=1):
        row['rank'] = index
    return rows


def _round(value, digits=3):
    """A rate rounded for the wire, or None. NEVER read back by arithmetic.

    Three decimals, which is one more than any page draws, for the reason
    the Elo board stores a decimal it does not show: the page rounds for
    display and a rounded number must not be what the next calculation
    reads. The identity `cci x ppc / 100 = pxp` holds at full precision
    inside `team_metrics` and to within this rounding here.
    """
    if value is None:
        return None
    return round(float(value), digits)


def team_statistics(connection, season):
    """One season's team statistics, or None where the layer is absent.

    The league prior for the matchup expectation is the PREVIOUS season's
    league opportunity rate, read from the archive rather than assumed. A
    season whose predecessor is not in the database — 2002, or a partial
    ingest — replays from a break-even league prior instead, which is the
    neutral statement rather than a borrowed number, and `priors.league_rate`
    then says so by being null.
    """
    rows = team_box.season_rows(connection, season)
    if not rows:
        return None
    previous = team_box.season_rows(connection, season - 1,
                                    season_types=('regular',))
    prior = (team_metrics.league_opportunity_rate(previous) if previous
             else None)
    return team_metrics.build_season(rows, league_prior_rate=prior)


def attach_team_statistics(rows, built):
    """Put each club's PxP+ and opportunity numbers on its board row.

    On the board rather than in a section of their own, because every view
    that ranks clubs already reads these rows, and a parallel table keyed by
    the same club would be a second place for the two to disagree.

    A club with no measured game gets no keys at all — not zeros. A reader
    can tell "no estimate" from "exactly average" only if the absence is an
    absence.
    """
    if not built:
        return
    for row in rows:
        stats = built['teams'].get(row['team'])
        if not stats:
            continue
        row['pxp'] = _round(stats['pxp'])
        row['cci'] = _round(stats['cci'])
        row['ppc'] = _round(stats['ppc'])
        row['net_opportunities'] = _round(stats['net_opportunities'])
        row['opportunity_rate'] = _round(stats['opportunity_rate'])
        row['allowed_rate'] = _round(stats['allowed_rate'])
        row['measured_games'] = stats['games']


def team_statistics_section(built):
    """The league environment the indexes are measured against.

    Published beside the board so a reader can see what 100 means here: the
    league's own points per possession over the same games, the exposure it
    was measured on, and the pilot priors the expectation used. The counts
    say how many games could not be measured at all, which is the one number
    that makes "unavailable" on a club row legible.
    """
    if not built:
        return None
    league = built['league']
    counts = built['counts']
    return dict(
        record_types=built['record_types'],
        turnover_basis=built['turnover_basis'],
        games=counts['games'],
        measured_games=counts['measured'],
        unmeasured_games=counts['unmeasured'],
        # A game with a possession estimate and no final score. It still
        # carries its opportunity grades; only the scoring rate leaves it
        # out, because adding its possessions with no points against them
        # is a zero, not an absence.
        unscored_games=counts['unscored'],
        unpaired_rows=counts['unpaired'],
        league=dict(
            possessions=_round(league['possessions'], 1),
            points=int(league['points']),
            points_per_possession=_round(league['points_per_possession'], 5),
            chances_per_possession=_round(league['chances_per_possession'], 5),
            points_per_chance=_round(league['points_per_chance'], 5),
            opportunity_rate=_round(league['opportunity_rate']),
        ),
        priors=dict(built['priors'],
                    league_rate=_round(built['priors']['league_rate'])),
        estimated=True,
    )


def played_teams(games):
    """Franchises with at least one finished, rateable game.

    The denominator for a season in progress. In September every franchise
    has none, so measuring against the whole league would call a correct
    export empty.
    """
    seen = set()
    for game in games:
        if not game['rateable'] or game['home_points'] is None:
            continue
        seen.update((game['home'], game['away']))
    return len(seen)


def season_teams(connection, season):
    """How many franchises played in a season, from the games themselves."""
    row = connection.execute(
        'SELECT COUNT(*) FROM (SELECT home_team_key AS k FROM game'
        '  WHERE season = ? UNION SELECT away_team_key FROM game'
        '  WHERE season = ?)', (season, season)).fetchone()
    return row[0]


def coverage_row(connection, season, games, rows, *, complete=True,
                team_stats=None):
    played = sum(1 for game in games if game['home_points'] is not None)
    counts = (team_stats or {}).get('counts')
    return dict(
        # How much of the season the team statistics could be measured on,
        # or absent where the loader has never run. A gate and a page can
        # then tell "no team box scores in this database" from "every box
        # score is here and one game of them is short a turnover count".
        team_stats=(None if counts is None else dict(
            games=counts['games'], measured=counts['measured'],
            unmeasured=counts['unmeasured'],
            teams=len([stats for stats in (team_stats['teams'] or {}).values()
                       if stats]))),
        season=season,
        season_label=season_label(season),
        teams=season_teams(connection, season),
        rated_teams=len(rows),
        games=len(games),
        finals=played,
        rateable=sum(1 for game in games if game['rateable']),
        postseason=sum(1 for game in games
                       if game['type'] == 'postseason'),
        playin=sum(1 for game in games if game['type'] == 'playin'),
        cup_final=sum(1 for game in games if game['type'] == 'cup_final'),
        # Stated by the code that knows when the export was generated, rather
        # than guessed by the gate from the numbers.
        complete=bool(complete),
        played_teams=played_teams(games),
    )


def champions(connection):
    """Who won each Finals, taken from the games rather than a typed list.

    The champion is the winner of the LAST postseason game of a season, which
    is what a Finals game 4-7 is. A season still being played simply has
    none, and so does a season whose last postseason game is unplayed.
    """
    rows = connection.execute(
        'SELECT season, home_team_key, away_team_key, home_points,'
        '       away_points FROM game g'
        "  WHERE season_type = 'postseason' AND completed = 1"
        '    AND home_points IS NOT NULL AND away_points IS NOT NULL'
        '    AND game_date = (SELECT max(game_date) FROM game x'
        "                      WHERE x.season = g.season"
        "                        AND x.season_type = 'postseason')"
        '  ORDER BY season').fetchall()
    out = {}
    for row in rows:
        if row['home_points'] == row['away_points']:
            continue
        home_won = row['home_points'] > row['away_points']
        out[str(row['season'])] = dict(
            champion=row['home_team_key'] if home_won else row['away_team_key'],
            runner_up=row['away_team_key'] if home_won else row['home_team_key'])
    return out


def conference_history(connection, model_id):
    """Each conference's rating, season by season.

    No new data: the same `team_rating` rows the one-season board reads,
    grouped by the conference already stored per season. A season whose
    standings file never arrived has no stored conference, and is left out
    rather than having today's alignment applied backwards.
    """
    rows = connection.execute(
        'SELECT r.season, r.rating, r.team_key, s.conference'
        '  FROM team_rating r JOIN team_season s'
        '    ON s.team_key = r.team_key AND s.season = r.season'
        ' WHERE r.model_id = ? AND s.conference IS NOT NULL'
        ' ORDER BY r.season', (model_id,)).fetchall()
    grouped = defaultdict(lambda: defaultdict(list))
    for row in rows:
        grouped[row['season']][row['conference']].append(
            dict(team=row['team_key'], rating=row['rating']))
    out = []
    for season in sorted(grouped):
        conferences = []
        for conference, members in sorted(grouped[season].items()):
            values = sorted(member['rating'] for member in members)
            best = max(members, key=lambda member: member['rating'])
            conferences.append(dict(
                conference=conference,
                teams=len(members),
                mean=round(sum(values) / len(values), 1),
                median=round(_median(values), 1),
                best=best['team'],
                best_rating=round(best['rating'], 1),
            ))
        out.append(dict(season=season, season_label=season_label(season),
                        conferences=conferences))
    return out


def _median(values):
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def build(connection, *, model_name=None, seasons=None, now=None):
    """Everything an export writes, and the manifest that describes it.

    Built whole before anything is written, so the gate can refuse it while
    the output directory is still untouched.
    """
    model = model_row(connection, model_name)
    available = [row[0] for row in connection.execute(
        'SELECT DISTINCT season FROM team_rating WHERE model_id = ?'
        ' ORDER BY season', (model['model_id'],)).fetchall()]
    wanted = sorted(set(seasons) & set(available)) if seasons else available
    if not wanted:
        raise ValueError('No season has a stored rating for this model')
    moment = (now or datetime.now(timezone.utc)).replace(microsecond=0)
    in_progress = current_season(moment)
    # Read once: every season's board falls back to it for a franchise whose
    # season row has no conference.
    index = teams(connection)
    payloads, coverage, statistics = {}, [], {}
    for season in wanted:
        games = season_games(connection, season, model['model_id'])
        names = season_names(connection, season)
        rows = board(connection, season, model['model_id'], games, names,
                     index)
        # The team statistics, or nothing at all where `src/nba/team_box.py`
        # has never run. Computed once a season: the board reads the season
        # half and `game_export` the per-game half, so there is one replay
        # behind both and they cannot disagree.
        built = team_statistics(connection, season)
        statistics[season] = built
        attach_team_statistics(rows, built)
        # `rating_model.config` IS the elo block, not the whole file:
        # build_ratings stores `config['elo']`. Reaching for an 'elo' key here
        # would find nothing and quietly play every game at a home advantage
        # of zero.
        expect_upcoming(games, rows, json.loads(model['config']))
        payloads[season] = dict(season=season,
                                season_label=season_label(season),
                                board=rows, games=games, names=names)
        section = team_statistics_section(built)
        if section:
            payloads[season]['team_stats'] = section
        # THE BRACKET AND THE PLAY-IN ride in the season's own payload
        # rather than a file of their own, and derive from the games just
        # read and the board just built -- no query, no fetch, about 2 KB in
        # a file a reader who opens a season has already got. Absent where
        # the season has no postseason, which is every season in progress:
        # see src/nba/series.py.
        bracket = series.build(games, rows)
        if bracket:
            payloads[season]['playoffs'] = bracket
        rounds = series.playin(games)
        if rounds:
            payloads[season]['playin'] = rounds
        coverage.append(coverage_row(connection, season, games, rows,
                                     complete=season < in_progress,
                                     team_stats=built))
    history = dict(conferences=conference_history(connection,
                                                  model['model_id']),
                   champions=champions(connection))
    # The game layer, one payload per month, or nothing at all where the
    # reduction has never run. Absent is said out loud rather than published
    # as a directory of empty files: see game_export.has_plays.
    detail = {}
    if game_export.has_plays(connection) or team_box.has_team_box(connection):
        params = win_probability.load_params()
        for season in wanted:
            months = game_export.build(connection, season,
                                       model_id=model['model_id'],
                                       params=params,
                                       team_stats=statistics.get(season))
            if months:
                detail[season] = months
    manifest = dict(
        schema_version=SCHEMA_VERSION,
        sport='nba',
        sport_label='NBA',
        generated_at=moment.isoformat().replace('+00:00', 'Z'),
        model=model_section(model),
        coverage=coverage,
        latest_season=wanted[-1],
        seasons={},
        files={},
    )
    # The people layer, an index and a file per player, or nothing where the
    # loader has never run. Absent is said out loud for the same reason the
    # game layer's absence is: see people_export.has_people.
    people = people_export.build(connection)
    return manifest, payloads, index, history, detail or None, people


def write(manifest, payloads, team_index, history=None, out_dir=OUT_DIR, *,
          gate=check, games=None, people=None):
    """Write the export, but only if the gate passes, then check the disk."""
    out_dir = Path(out_dir)
    staged = {}
    for season, payload in payloads.items():
        body = json.dumps(payload, separators=(',', ':'), sort_keys=True)
        staged[f'seasons/{season}.js'] = (
            f'window.NBA_SEASONS=(window.NBA_SEASONS||{{}});'
            f'window.NBA_SEASONS[{season}]={body};\n')
    staged['teams.js'] = ('window.NBA_TEAMS=' + json.dumps(
        team_index, separators=(',', ':'), sort_keys=True) + ';\n')
    staged['history.js'] = ('window.NBA_HISTORY=' + json.dumps(
        history or {}, separators=(',', ':'), sort_keys=True) + ';\n')
    for season, months in (games or {}).items():
        for month, payload in months.items():
            body = json.dumps(payload, separators=(',', ':'), sort_keys=True)
            staged[f'games/{season}/{month}.js'] = (
                'window.NBA_GAMEMONTHS=(window.NBA_GAMEMONTHS||{});'
                f'window.NBA_GAMEMONTHS["{season}/{month}"]={body};\n')

    if people:
        index, players = people
        staged['people/index.js'] = (
            'window.NBA_PEOPLE=' + people_export.body(index) + ';\n')
        for athlete_id, payload in players.items():
            staged[f'people/{athlete_id}.js'] = (
                'window.NBA_PLAYER=(window.NBA_PLAYER||{});'
                f'window.NBA_PLAYER[{athlete_id}]='
                f'{people_export.body(payload)};\n')

    def stamp(text):
        return hashlib.sha256(text.encode('utf-8')).hexdigest()[:10]

    manifest = dict(manifest)
    manifest['seasons'] = {
        str(season):
            f'seasons/{season}.js?v={stamp(staged[f"seasons/{season}.js"])}'
        for season in payloads}
    manifest['files'] = dict(
        teams=f'teams.js?v={stamp(staged["teams.js"])}',
        history=f'history.js?v={stamp(staged["history.js"])}')
    if games:
        # ONE cache token per season, not one per file. A month file's path
        # is derived by the page from the game's own date, so the manifest
        # does not have to name 175 of them, and naming them all would make
        # the manifest — which every reader fetches — heavier than the one
        # month file a reader who opens a game fetches. The token still has
        # to be there: a URL with no version stamp is one a reader can be
        # served from a cache a previous export wrote.
        manifest['files']['games'] = dict(seasons={
            str(season): dict(
                v=stamp(''.join(staged[f'games/{season}/{month}.js']
                                for month in game_export.month_order(
                                    list(months)))),
                months=game_export.month_order(list(months)))
            for season, months in games.items()})
    if people:
        # ONE token for 2,635 player files, and a second for the index. The
        # path of a player's file is derived by the page from his own id, so
        # naming every file here would make the manifest every reader fetches
        # heavier than the one file a reader who opens a player fetches. The
        # token is still required: an unversioned URL is one a reader can be
        # served from the cache a previous export wrote.
        manifest['files']['people'] = dict(
            index=f'people/index.js?v={stamp(staged["people/index.js"])}',
            count=len(people[1]),
            v=stamp(''.join(staged[f'people/{athlete_id}.js']
                            for athlete_id in sorted(people[1]))))
    reasons = gate(manifest, root=None)
    if reasons:
        raise PublishRefused(reasons)
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, text in staged.items():
        target = out_dir / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding='utf-8')
    (out_dir / MANIFEST).write_text(
        json.dumps(manifest, separators=(',', ':'), sort_keys=True) + '\n',
        encoding='utf-8')
    written = gate(manifest, root=out_dir)
    if written:
        raise PublishRefused(written)
    return manifest


def summarise(manifest, out_dir):
    model = manifest['model']
    lines = ['# NBA export', '',
             f'Schema {manifest["schema_version"]}, as of '
             f'{manifest["generated_at"]}, written to `{out_dir}`.', '',
             f'Model `{model["name"]}` fitted on {model["fitted_on"]}, held '
             f'out {model["held_out"]}, holdout Brier {model["holdout_brier"]}'
             f' on {model["holdout_games"]} games.', '',
             '| season | teams | rated | games | played |',
             '| --- | --- | --- | --- | --- |']
    for row in manifest['coverage']:
        lines.append(f'| {row["season_label"]} | {row["teams"]} | '
                     f'{row["rated_teams"]} | {row["games"]} | '
                     f'{row["finals"]} |')
    folk = manifest.get('files', {}).get('people')
    games = manifest.get('files', {}).get('games')
    if games:
        seasons = games.get('seasons', {})
        months = sum(len(entry.get('months', [])) for entry in seasons.values())
        lines += ['', f'Game layer: {len(seasons)} seasons in {months} month '
                      f'files.']
    else:
        # Said out loud. An export with no game layer is a complete export of
        # everything else, and a page learns it from the absent key — but a
        # run that was meant to build one and did not should not look
        # identical to a run that was not.
        lines += ['', 'Game layer: ABSENT. This database carries no reduced '
                      'play-by-play; run `python -m src.nba.plays`.']
    measured = [row for row in manifest['coverage']
                if (row.get('team_stats') or {}).get('measured')]
    if measured:
        total = sum(row['team_stats']['measured'] for row in measured)
        short = sum(row['team_stats']['unmeasured'] for row in measured)
        lines += ['', f'Team statistics: {total} games measured across '
                      f'{len(measured)} seasons'
                      + (f', {short} unmeasurable from the feed.' if short
                         else '.')]
    else:
        # Said out loud, for the reason the absent game layer is: a run that
        # was meant to measure these and did not must not look like a run
        # that was never asked to.
        lines += ['', 'Team statistics: ABSENT. This database carries no team '
                      'box scores; run `python -m src.nba.team_box`.']
    if folk:
        lines += ['', f'People layer: {folk["count"]} players.']
    else:
        lines += ['', 'People layer: ABSENT. This database carries no '
                      'player box scores; run `python -m src.nba.people`.']
    lines += ['', 'Publish gate PASSED.']
    return '\n'.join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Export the NBA archive, if the gate allows it')
    parser.add_argument('--out', default=str(OUT_DIR))
    parser.add_argument('--database', default=None)
    parser.add_argument('--model', default=None,
                        help='model name; the newest stored model by default')
    parser.add_argument('--seasons', default=None,
                        help='comma-separated seasons; every rated season by '
                             'default')
    args = parser.parse_args(argv)
    seasons = ([int(part) for part in args.seasons.split(',') if part.strip()]
               if args.seasons else None)
    connection = connect(args.database)
    try:
        manifest, payloads, team_index, history, detail, folk = build(
            connection, seasons=seasons, model_name=args.model)
    finally:
        connection.close()
    try:
        manifest = write(manifest, payloads, team_index, history,
                         Path(args.out), games=detail, people=folk)
    except PublishRefused as refused:
        print(str(refused))
        return 1
    print(summarise(manifest, args.out))
    return 0


if __name__ == '__main__':
    sys.exit(main())
