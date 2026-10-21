// Following a team, and the one screen that says what the last week did to it.
//
// Driven through the real dashboard in jsdom rather than by lifting functions
// out of the shell, because the things that break here are the wiring: a star
// that saves nothing, a My teams panel that never appears, a tab whose panel
// stays on "Loading…". The numbers are checked against
// data/elo_timeline/<season>.js, which src/elo_timeline.py builds, so the test
// fails if the page starts showing a figure it computed itself.
const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {JSDOM,ResourceLoader,VirtualConsole}=require('jsdom');
const {execFileSync}=require('node:child_process');
const root=path.resolve(__dirname,'..');
execFileSync('python3',['build_dashboard.py','--reuse-exports'],{cwd:root});

class LocalAssets extends ResourceLoader {
  fetch(url){
    const filename=path.resolve(root,'.'+new URL(url).pathname);
    if(!filename.startsWith(root+path.sep)||!filename.endsWith('.js')) return null;
    return Promise.resolve(fs.readFileSync(filename));
  }
}
async function until(w,predicate){
  for(let i=0;i<300;i++){if(predicate()) return;await new Promise(r=>setTimeout(r,10));}
  throw new Error('never reached: '+w.document.querySelector('#content')?.textContent.slice(0,300));
}

/** The timeline payload the page will read, parsed straight from the export. */
function timeline(season){
  const file=path.join(root,'ui/data/elo_timeline',season+'.js');
  const sandbox={window:{}};
  sandbox.window.window=sandbox.window;
  require('node:vm').runInNewContext(fs.readFileSync(file,'utf8'),sandbox);
  const raw=sandbox.window.__CFB_ELO_TIMELINE__[season];
  const unpack=(fields,row)=>Object.fromEntries(fields.map((f,i)=>[f,row[i]]));
  return {teams:raw.teams.map(([id,name])=>({id,name})),
    stages:raw.stages.map(r=>unpack(raw.stage_fields,r)),
    rows:raw.rows.map(stage=>stage.map(r=>unpack(raw.row_fields,r)))};
}

/** The team-page export, parsed the same way the page's loader parses it. */
function teamPages(){
  const file=path.join(root,'ui/data/team_pages.js');
  const sandbox={window:{}};
  sandbox.window.window=sandbox.window;
  require('node:vm').runInNewContext(fs.readFileSync(file,'utf8'),sandbox);
  return sandbox.window.__CFB_TEAM_PAGES__||sandbox.window.TEAM_PAGES||sandbox.window.__TEAM_PAGES__;
}

function open(){
  const vc=new VirtualConsole();const errors=[];vc.on('jsdomError',e=>errors.push(e.message));
  const dom=new JSDOM(fs.readFileSync(path.join(root,'ui/dashboard.html'),'utf8'),{
    url:'http://dashboard.test/ui/dashboard.html',runScripts:'dangerously',
    resources:new LocalAssets(),pretendToBeVisual:true,virtualConsole:vc,
    beforeParse(w){
      w.matchMedia=()=>({matches:false,addListener(){},removeListener(){}});
      w.scrollTo=()=>{};w.HTMLElement.prototype.scrollIntoView=()=>{};
      w.fetch=async url=>({ok:true,json:async()=>JSON.parse(
        fs.readFileSync(path.join(root,'ui',String(url).split('?')[0]),'utf8'))});
    }});
  return {dom,w:dom.window,d:dom.window.document,errors};
}

test('following a team, and what the last week did to it',async t=>{
  const {dom,w,d,errors}=open();
  const route=async(hash,predicate)=>{
    const old=d.querySelector('#content').firstElementChild;
    w.location.hash=hash;
    await until(w,()=>d.querySelector('#content').firstElementChild!==old&&predicate());
  };
  try {
    await until(w,()=>d.querySelector('#fav-teams'));

    // The season and the team are chosen from the data, never pinned: a
    // scheduled refresh changes who played last week, and an assertion keyed
    // to one row would fail on the data rather than on the code.
    // Read from the export rather than the page: the shell's top-level `const
    // DATA` belongs to the script, not to window, so a test cannot see it.
    const DATA=JSON.parse(fs.readFileSync(path.join(root,'ui/dashboard_data.json'),'utf8'));
    const season=DATA.years_all[DATA.years_all.length-1];
    const tl=timeline(String(season));
    const last=tl.rows[tl.rows.length-1];
    const moved=last.filter(r=>r.stage_games>0&&r.d_elo!==null&&r.d_elo!==0);
    assert.ok(moved.length,'no team moved in the latest stage of '+season);
    // The "Where it stands" panel exists only for a team the playoff field or
    // the rest-of-season projection knows about, and removing it otherwise is
    // correct: an empty box is worse than no box. So the mover is chosen to
    // satisfy that precondition rather than taken from row zero -- which is
    // what broke here, when a refresh made the biggest mover of the week a
    // team in neither (Marshall, 2026). Keying an assertion to row zero has
    // now cost this suite twice; search the data for a row that satisfies the
    // precondition instead.
    const standingNames=new Set([
      ...(((DATA.playoff_by_year||{})[String(season)]||{}).field||[]).map(q=>q.team),
      ...((DATA.projection&&DATA.projection.season===season
        ? DATA.projection.teams||[] : [])).map(t=>t.team)]);
    const named=r=>tl.teams[r.team].name;
    // Falling back to row zero keeps the rest of the suite running when no
    // mover has a standing at all; the standing assertions below then check
    // that the panel is correctly ABSENT, so neither case passes vacuously.
    const pick=moved.find(r=>standingNames.has(named(r)))||moved[0];
    const hasStanding=standingNames.has(named(pick));
    const name=tl.teams[pick.team].name;
    const team=DATA.teams.find(x=>x.name===name);
    assert.ok(team,name+' is not in the team index');

    await t.test('the home page offers favorites before any team is followed',()=>{
      assert.equal(w.CfbFavorites.read().length,0);
      assert.match(d.querySelector('#fav-teams').textContent,/Follow a team/);
    });

    await t.test('the star on a team page saves that team on the device',async()=>{
      await route('#team='+team.slug,()=>d.querySelector('[data-fav-slug]'));
      const star=d.querySelector('[data-fav-slug]');
      assert.equal(star.dataset.favSlug,team.slug);
      assert.equal(star.getAttribute('aria-pressed'),'false');
      star.click();
      assert.equal(star.getAttribute('aria-pressed'),'true');
      assert.deepEqual(Array.from(w.CfbFavorites.read()),[team.slug]);
      assert.deepEqual(Array.from(JSON.parse(w.localStorage.getItem(w.CfbFavorites.KEY))),[team.slug],
        'the favorite has to be in storage, not only in a variable');
    });

    await t.test('a followed team appears on the home page with its rating',async()=>{
      await route('#section=home',()=>d.querySelector('#fav-teams .fav-card'));
      const card=d.querySelector('#fav-teams .fav-card');
      assert.match(card.textContent,new RegExp(name.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')));
      assert.match(card.textContent,/Elo/);
      assert.ok(card.querySelector('a[href*="tab=week"]'),'the card links to the week view');
    });

    await t.test('the week view shows the movement the engine recorded',async()=>{
      await route('#team='+team.slug+'&tab=week',
        ()=>d.querySelector('#tp-week')&&!d.querySelector('#tp-week .empty-note'));
      const panel=d.querySelector('#tp-week');
      const text=panel.textContent;
      // The rating and the rank as the timeline holds them, formatted the way
      // the rest of the site formats them (Elo as a whole integer).
      assert.ok(text.includes(String(Math.round(pick.elo))),
        'Elo '+Math.round(pick.elo)+' is not on the panel: '+text.slice(0,300));
      assert.ok(text.includes('#'+pick.rank),'rank #'+pick.rank+' is not on the panel');
      // Read the Elo-change tile by its own label: the same figure also
      // appears beside the game that caused it, so a panel-wide text search
      // would pass with the tile empty.
      const tile=label=>[...panel.querySelectorAll('.team-stat')]
        .find(el=>el.querySelector('.lbl').textContent.trim()===label);
      const change=tile('Elo change');
      assert.ok(change,'the panel has no Elo change tile');
      assert.ok(change.textContent.includes(Math.abs(pick.d_elo).toFixed(1)),
        'the Elo change tile says "'+change.textContent.trim()+'", not '+pick.d_elo);
      assert.ok(change.querySelector('.'+(pick.d_elo>0?'pos':'neg')),
        'the change is not coloured by direction');
      assert.ok(tile('Elo').textContent.includes(String(Math.round(pick.elo))));
      assert.ok(tile('Rank').textContent.includes('#'+pick.rank));
      // The games that produced it, with a link into the game story.
      assert.ok(panel.querySelector('.week-game'),'no game is listed for a team that played');
      assert.ok(panel.querySelector('.week-game-link[href*="game="]'));
      // Seed and odds are shown as they stand, in their own panel, and said
      // to be exactly that rather than dressed up as a week-on-week move.
      const standing=d.querySelector('#tp-week-standing');
      if (hasStanding) {
        assert.ok(standing,'the playoff standing panel is missing for '+name
          +', which the field or the projection does carry');
        assert.match(standing.textContent,/[Nn]o week-ago copy/);
        assert.match(standing.textContent,/Playoff field/i);
      } else {
        assert.equal(standing,null,'the standing panel is drawn for '+name
          +', which neither the playoff field nor the projection carries, so '
          +'it would be an empty box');
      }
    });

    await t.test('unfollowing removes the team from the home page again',async()=>{
      d.querySelector('[data-fav-slug]').click();
      assert.deepEqual(Array.from(w.CfbFavorites.read()),[]);
      await route('#section=home',()=>d.querySelector('#fav-teams'));
      assert.equal(d.querySelector('#fav-teams .fav-card'),null);
    });

    await t.test('a stored favorite that no longer resolves is skipped, not drawn',async()=>{
      w.localStorage.setItem(w.CfbFavorites.KEY,JSON.stringify(['not-a-real-team',team.slug]));
      await route('#team='+team.slug,()=>d.querySelector('[data-fav-slug]'));
      await route('#section=home',()=>d.querySelector('#fav-teams .fav-card'));
      assert.equal(d.querySelectorAll('#fav-teams .fav-card').length,1);
    });

    await t.test('a week whose only game was unrated is not called a bye',async()=>{
      // The timeline counts RATED games, so a team whose single game that week
      // was against an FCS opponent has stage_games 0 and would read as a bye.
      // Found from the data rather than named, so it holds in any week.
      const tp=teamPages();
      const nonFbs=(tp.non_fbs||[]);            // [id, season, date, week, tid, opp, ...]
      const stage=tl.stages[tl.stages.length-1];
      const idleIds=new Set(last.filter(r=>r.stage_games===0)
        .map(r=>DATA.teams.find(x=>x.name===tl.teams[r.team].name)?.id));
      const hit=nonFbs.find(r=>r[1]===season&&r[10]===0&&r[3]===stage.week&&idleIds.has(r[4]));
      if(!hit){ t.diagnostic('no unrated-only week in '+season+' '+stage.key); return; }
      const only=DATA.teams.find(x=>x.id===hit[4]);
      await route('#team='+only.slug+'&tab=week',
        ()=>d.querySelector('#tp-week')&&!d.querySelector('#tp-week .empty-note'));
      const text=d.querySelector('#tp-week').textContent;
      assert.ok(!/did not play/.test(text),only.name+' played '+hit[5]+' and was called a bye');
      assert.ok(d.querySelector('#tp-week .week-game'),'the unrated game is not listed');
      assert.match(text,new RegExp(hit[5].replace(/[.*+?^${}()|[\]\\]/g,'\\$&')));
      assert.match(text,/rates nothing for/,'the unchanged rating is not explained');
    });

    assert.deepEqual(Array.from(errors),[]);
  } finally {
    dom.window.close();
  }
});
