/* Pure statistics and table operations shared by every Stats view. */
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.CfbStats=api;})(globalThis,function(){
  'use strict';
  const categories = {
    passing: {label:'Passing',fields:['yards','td','completions','attempts','completionPct','interceptions','ypa','ypc','ypg'],sort:'yards',minimum:'attempts'},
    rushing: {label:'Rushing',fields:['yards','td','attempts','ypa','ypg','tdRate'],sort:'yards',minimum:'attempts'},
    receiving: {label:'Receiving',fields:['yards','td','receptions','ypr','ypg','recPg'],sort:'yards',minimum:'receptions'},
    defense: {label:'Defense',fields:['tackles','solo','assisted','tacklesPg','sacks','tfl','interceptions','passesDefended'],sort:'tackles',minimum:'games'},
    kicking: {label:'Kicking',fields:['fgMade','fgAttempts','fgPct','xpMade','xpAttempts','xpPct','points'],sort:'points',minimum:'fgAttempts'}
  };
  const labels={yards:'YDS',td:'TD',completions:'CMP',attempts:'ATT',completionPct:'CMP %',interceptions:'INT',ypg:'YDS/G',ypa:'YDS/ATT',receptions:'REC',ypr:'YDS/REC',tackles:'TKL',solo:'SOLO',sacks:'SACK',tfl:'TFL',passesDefended:'PD',fgMade:'FGM',fgAttempts:'FGA',fgPct:'FG %',xpMade:'XPM',xpAttempts:'XPA',points:'PTS',games:'Games',
    // Derived from the sums above, never from a second source: YDS/CMP, carries
    // per TD, catches and tackles per game, extra-point rate, assisted tackles
    // (TKL - SOLO, the standard decomposition), and a team's own W-L for context.
    ypc:'YDS/CMP',tdRate:'CAR/TD',recPg:'REC/G',tacklesPg:'TKL/G',xpPct:'XP %',assisted:'AST',record:'Team W-L',wins:'Wins',losses:'Losses',pointsFor:'Points',pointsAgainst:'Opp points',ppg:'PTS/G',oppPpg:'Opp PTS/G',passYards:'Pass YDS',rushYards:'Rush YDS',passYpg:'Pass YDS/recorded G',rushYpg:'Rush YDS/recorded G',boxGames:'Box score G',elo:'Elo',coefficient:'Season CoE',rolling:'5-year CoE',change:'Last-game Δ Elo'};
  const num=value=>{if(value==null||String(value).trim()==='')return null;const n=Number(String(value).replaceAll(',','').replace(/%$/,''));return Number.isFinite(n)?n:null;};
  function sortRows(rows,key,dir='desc',type='number') {
    return rows.map((r,i)=>({r,i})).sort((a,b)=>{
      const x=type==='string'?(a.r[key]==null?null:String(a.r[key])):num(a.r[key]);
      const y=type==='string'?(b.r[key]==null?null:String(b.r[key])):num(b.r[key]);
      if(x==null||y==null)return x==null&&y==null?a.i-b.i:x==null?1:-1;
      const cmp=type==='string'?x.localeCompare(y):x-y;
      return (dir==='asc'?cmp:-cmp)||a.i-b.i;
    }).map(x=>x.r);
  }
  function filterRows(rows,f={}) {
    return rows.filter(r=>(!f.team||r.team===f.team)&&(!f.conference||r.conference===f.conference)&&
      (!f.query||String(r.name||r.team).toLowerCase().includes(f.query.toLowerCase()))&&
      (!f.minimum||(num(r[f.minimumKey||'games'])??-1)>=f.minimum));
  }
  function aggregatePlayers(archive,validGames,conferences={},teamById=new Map()) {
    const records=new Map(), covered=new Set();
    const sums={YDS:'yards',TD:'td',CAR:'attempts',REC:'receptions',INT:'interceptions',TOT:'tackles',SOLO:'solo',SACKS:'sacks',TFL:'tfl',PD:'passesDefended',PTS:'points'};
    for(const [gameId,teams] of Object.entries(archive||{})) {
      const game=validGames.get(Number(gameId));if(!game||!game.completed)continue;
      for(const team of teams||[]) {
        const teamId=team.home_away==='home'?game.home_id:team.home_away==='away'?game.away_id:null;
        const known=teamById.get(String(teamId));if(!known||team.name!==known.name)continue;
        covered.add(gameId+'|'+teamId);
        for(const cat of team.categories||[]) {
          const category=cat.name==='defensive'||cat.name==='interceptions'?'defense':cat.name;
          if(!categories[category])continue;
          for(const line of cat.lines||[]) {
            if(!line.name||/^team$/i.test(line.name))continue;
            // Source athlete IDs keep namesakes separate; unnumbered lines stay scoped.
            const athleteId=line.id==null||String(line.id).trim()===''?null:String(line.id).trim();
            const id=teamId+'|'+(athleteId==null?'name:'+line.name:'id:'+athleteId)+'|'+category;
            if(!records.has(id))records.set(id,{key:id,id:athleteId,name:line.name,team:known.name,teamId,conference:conferences[String(teamId)]||null,category,_games:new Set()});
            const row=records.get(id);row._games.add(gameId);
            const add=(key,n)=>{if(n!=null)row[key]=(row[key]??0)+n;};
            if(cat.type==='C/ATT'||cat.type==='FG'||cat.type==='XP') {
              const parts=String(line.stat).split('/').map(num);if(parts.length!==2||parts.some(n=>n==null))continue;
              if(cat.type==='C/ATT'){add('completions',parts[0]);add('attempts',parts[1]);}
              if(cat.type==='FG'){add('fgMade',parts[0]);add('fgAttempts',parts[1]);}
              if(cat.type==='XP'){add('xpMade',parts[0]);add('xpAttempts',parts[1]);}
            } else if(sums[cat.type] && !(cat.name==='interceptions'&&cat.type!=='INT')) add(sums[cat.type],num(line.stat));
          }
        }
      }
    }
    const divide=(r,a,b,k,scale=1)=>{r[k]=r[a]!=null&&r[b]>0?r[a]/r[b]*scale:null;};
    const rows=[...records.values()].map(r=>{r.games=r._games.size;delete r._games;
      divide(r,'completions','attempts','completionPct',100);divide(r,'fgMade','fgAttempts','fgPct',100);
      divide(r,'xpMade','xpAttempts','xpPct',100);
      divide(r,'yards','attempts','ypa');divide(r,'yards','receptions','ypr');divide(r,'yards','games','ypg');
      divide(r,'yards','completions','ypc');divide(r,'attempts','td','tdRate');
      divide(r,'receptions','games','recPg');divide(r,'tackles','games','tacklesPg');
      // TKL - SOLO. Only where both are present: a feed that reported one and
      // not the other would otherwise read as "no assisted tackles".
      r.assisted=r.tackles!=null&&r.solo!=null?r.tackles-r.solo:null;
      return r;});
    return {rows,covered};
  }
  function aggregateTeams(games,players,conferences,teamById,ratings={}) {
    const rows=new Map();
    const get=id=>{if(!rows.has(id)){const team=teamById.get(String(id));if(!team)return null;rows.set(id,{name:team.name,team:team.name,teamId:id,conference:conferences[String(id)]||null,games:0,wins:0,losses:0,pointsFor:0,pointsAgainst:0,boxGames:0});}return rows.get(id);};
    const covered=new Map();
    for(const key of players.covered){const [game,id]=key.split('|');if(!covered.has(Number(id)))covered.set(Number(id),new Set());covered.get(Number(id)).add(game);}
    for(const g of games){if(!g.completed||g.home_score==null||g.away_score==null)continue;for(const [id,pts,opp] of [[g.home_id,g.home_score,g.away_score],[g.away_id,g.away_score,g.home_score]]){const r=get(id);if(!r)continue;r.games++;r.pointsFor+=pts;r.pointsAgainst+=opp;r.wins+=pts>opp?1:0;r.losses+=pts<opp?1:0;}}
    for(const p of players.rows){const r=get(p.teamId);if(!r)continue;const add=(key,value)=>{if(value!=null)r[key]=(r[key]??0)+value;};if(p.category==='passing')add('passYards',p.yards);if(p.category==='rushing')add('rushYards',p.yards);if(p.category==='defense')for(const k of ['tackles','solo','sacks','tfl','interceptions','passesDefended'])add(k,p[k]);if(p.category==='kicking')for(const k of ['fgMade','fgAttempts','xpMade','xpAttempts'])add(k,p[k]);}
    for(const r of rows.values()){r.boxGames=covered.get(r.teamId)?.size||0;r.ppg=r.games?r.pointsFor/r.games:null;r.oppPpg=r.games?r.pointsAgainst/r.games:null;r.passYpg=r.passYards!=null&&r.boxGames?r.passYards/r.boxGames:null;r.rushYpg=r.rushYards!=null&&r.boxGames?r.rushYards/r.boxGames:null;r.fgPct=r.fgAttempts?r.fgMade/r.fgAttempts*100:null;Object.assign(r,ratings[r.name]||{});}
    return [...rows.values()];
  }
  // ---- One sort key per cell, shared by every sortable table -------------
  // A column's numbers are written for a reader, not for a sort: a rank reads
  // '#12', a signed number uses U+2212, a probability can read '<0.1%', a record
  // is '10-2' and a blank is an em dash. Reading such a column as text sorts
  // 1, 10, 11; reading it as a leading number only held while EVERY cell in it
  // started with a digit, so one '± 0', '<0.1%' or 'bye' sent the whole column
  // back to a text sort. Each cell is read on its own instead.
  const BLANK=/^(?:[-‒–—―−]|…|\.\.\.|n\/?a)$/i;
  const MONTH_DATE=/^[A-Za-z]{3,9}\.?\s+\d{1,2},?\s+\d{4}$/;
  const ISO_DATE=/^(\d{4})-(\d{2})-(\d{2})(?:[T ].*)?$/;
  const NUMBERS=/^[-+]?\d+(?:\.\d+)?(?:\s+[-+]?\d+(?:\.\d+)?)*$/;
  // null means "no value": a blank sorts last in BOTH directions, so it is never
  // negated with the comparison. {nums} is a tuple, {text} a word.
  function cellKey(value) {
    const raw=String(value==null?'':value).trim();
    if(!raw||BLANK.test(raw))return null;
    // A calendar date is neither a number nor a word: 'Sep 3, 2022' sorts before
    // 'Sep 10, 2022' by day, and after it in any text order.
    if(MONTH_DATE.test(raw)){const t=Date.parse(raw);if(Number.isFinite(t))return {nums:[t]};}
    // A stored date is dash-separated like a record, and must not be read as one:
    // its month is not a loss to be negated below.
    const iso=ISO_DATE.exec(raw);
    if(iso)return {nums:[Number(iso[1]),Number(iso[2]),Number(iso[3])]};
    // U+2212 is a minus sign; a dash or slash BETWEEN digits is a separator
    // ('10-2', '31/48'), so it becomes a space before the numbers are read.
    let s=raw.replace(/−/g,'-').replace(/(\d),(?=\d)/g,'$1').replace(/[#%±]/g,' ').trim();
    let nudge=0;
    if(/^[<>]/.test(s)){nudge=s[0]==='<'?-1e-9:1e-9;s=s.slice(1).trim();}
    const paired=/(\d)\s*[–—/-]\s*(?=\d)/.test(s);
    s=s.replace(/(\d)\s*[–—/-]\s*(?=\d)/g,'$1 ').trim();
    // A unit spelled out after the numbers is a label, not an order ('6-3 9 bids'
    // is a record and a bid count), so one trailing word is dropped.
    s=s.replace(/^((?:[-+]?\d+(?:\.\d+)?\s+)+)[A-Za-z]+$/,'$1').trim();
    if(!NUMBERS.test(s))return {text:raw.toLocaleLowerCase()};
    const nums=s.split(/\s+/).map(Number);
    if(!nums.every(Number.isFinite))return {text:raw.toLocaleLowerCase()};
    // A record or a made/attempted pair reads best first on both halves: more
    // wins, then fewer losses. Negating the losses keeps one direction -- and it
    // holds however the cell continues, since a record is often followed by what
    // it works out to ('30-11 73.2%', '6-3 9 bids') or by ties ('131-121-4').
    if(paired)nums[1]=-nums[1];
    nums[0]+=nudge;
    return {nums};
  }
  function compareCells(a,b,dir='asc') {
    if(a==null||b==null)return a==null&&b==null?0:a==null?1:-1;
    let cmp;
    if(a.nums&&b.nums){cmp=0;
      for(let i=0;i<Math.max(a.nums.length,b.nums.length)&&!cmp;i++)cmp=(a.nums[i]??0)-(b.nums[i]??0);
      cmp=cmp<0?-1:cmp>0?1:0;}
    else if(a.nums||b.nums)cmp=a.nums?-1:1;
    else cmp=a.text.localeCompare(b.text,undefined,{numeric:true,sensitivity:'base'});
    return dir==='desc'?-cmp:cmp;
  }
  // Keys for one whole column. Where the column is mostly numbers, a word in it
  // ('bye') is treated as no value and sorts last, rather than deciding the
  // order for every number around it.
  function columnKeys(values) {
    const keys=values.map(cellKey),live=keys.filter(key=>key!=null);
    const numeric=live.filter(key=>key.nums).length;
    return numeric&&numeric>=live.length*0.6?keys.map(key=>key&&key.nums?key:null):keys;
  }
  function sortCells(entries,dir) {
    const keys=columnKeys(entries.map(entry=>entry.value));
    return entries.map((entry,i)=>({entry,key:keys[i],i}))
      .sort((a,b)=>compareCells(a.key,b.key,dir)||a.i-b.i).map(x=>x.entry);
  }
  // Adapter for existing statistical tables: reuse the same comparator, retain
  // row links and hidden/filter state, and leave their initial order untouched.
  function enhanceTable(table) {
    if(table.dataset.sortable || table.classList.contains('stats-table') || !table.tHead || !table.tBodies.length || table.tHead.rows.length!==1)return;
    table.dataset.sortable='true';
    const headers=[...table.tHead.rows[0].cells],body=table.tBodies[0];
    headers.forEach((header,index)=>{
      const label=header.textContent.trim();if(!label)return;
      // A cell may carry its own sort key in data-sort. Needed where the text is
      // not the value (a rating with its rank beside it) and where the column is
      // filled in after this runs, so the key is read on the click, never now.
      const value=row=>{const cell=row.cells[index];if(!cell)return '';
        const keyed=cell.getAttribute('data-sort');
        return keyed!=null&&keyed.trim()!==''?keyed:cell.textContent;};
      const button=table.ownerDocument.createElement('button');button.type='button';button.className='sort-button';button.textContent=label;
      header.replaceChildren(button);header.setAttribute('scope','col');
      let direction='asc';
      button.addEventListener('click',()=>{
        direction=direction==='asc'?'desc':'asc';
        for(const h of headers){h.setAttribute('aria-sort','none');const b=h.querySelector('.sort-button');if(b)b.textContent=b.dataset.label||b.textContent.replace(/ [▲▼]$/,'');}
        header.setAttribute('aria-sort',direction==='asc'?'ascending':'descending');button.dataset.label=label;button.textContent=label+(direction==='asc'?' ▲':' ▼');
        const entries=[...body.rows].map(element=>({element,value:value(element)}));
        for(const entry of sortCells(entries,direction))body.appendChild(entry.element);
      });
    });
  }
  return {categories,labels,num,sortRows,filterRows,aggregatePlayers,aggregateTeams,enhanceTable,cellKey,compareCells,columnKeys,sortCells};
});
