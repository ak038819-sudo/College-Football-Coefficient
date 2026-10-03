/* Pure statistics and table operations shared by every Stats view. */
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.CfbStats=api;})(globalThis,function(){
  'use strict';
  const categories = {
    passing: {label:'Passing',fields:['yards','td','completions','attempts','completionPct','interceptions','ypg'],sort:'yards',minimum:'attempts'},
    rushing: {label:'Rushing',fields:['yards','td','attempts','ypa','ypg'],sort:'yards',minimum:'attempts'},
    receiving: {label:'Receiving',fields:['yards','td','receptions','ypr','ypg'],sort:'yards',minimum:'receptions'},
    defense: {label:'Defense',fields:['tackles','solo','sacks','tfl','interceptions','passesDefended'],sort:'tackles',minimum:'games'},
    kicking: {label:'Kicking',fields:['fgMade','fgAttempts','fgPct','xpMade','xpAttempts','points'],sort:'points',minimum:'fgAttempts'}
  };
  const labels={yards:'YDS',td:'TD',completions:'CMP',attempts:'ATT',completionPct:'CMP %',interceptions:'INT',ypg:'YDS/G',ypa:'YDS/ATT',receptions:'REC',ypr:'YDS/REC',tackles:'TKL',solo:'SOLO',sacks:'SACK',tfl:'TFL',passesDefended:'PD',fgMade:'FGM',fgAttempts:'FGA',fgPct:'FG %',xpMade:'XPM',xpAttempts:'XPA',points:'PTS',games:'Games',wins:'Wins',losses:'Losses',pointsFor:'Points',pointsAgainst:'Opp points',ppg:'PTS/G',oppPpg:'Opp PTS/G',passYards:'Pass YDS',rushYards:'Rush YDS',passYpg:'Pass YDS/recorded G',rushYpg:'Rush YDS/recorded G',boxGames:'Box score G',elo:'Elo',coefficient:'Season CoE',rolling:'5-year CoE',change:'Last-game Δ Elo'};
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
        const known=teamById.get(String(teamId));if(!known)continue;
        covered.add(gameId+'|'+teamId);
        for(const cat of team.categories||[]) {
          const category=cat.name==='defensive'||cat.name==='interceptions'?'defense':cat.name;
          if(!categories[category])continue;
          for(const line of cat.lines||[]) {
            if(!line.name||/^team$/i.test(line.name))continue;
            // Provider archive has no player ID: retain a scoped key for future ID resolution.
            const id=teamId+'|'+line.name+'|'+category;
            if(!records.has(id))records.set(id,{key:id,name:line.name,team:known.name,teamId,conference:conferences[String(teamId)]||null,category,_games:new Set()});
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
    const rows=[...records.values()].map(r=>{r.games=r._games.size;delete r._games;divide(r,'completions','attempts','completionPct',100);divide(r,'fgMade','fgAttempts','fgPct',100);divide(r,'yards','attempts','ypa');divide(r,'yards','receptions','ypr');divide(r,'yards','games','ypg');return r;});
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
  // Adapter for existing statistical tables: reuse the same comparator, retain
  // row links and hidden/filter state, and leave their initial order untouched.
  function enhanceTable(table) {
    if(table.dataset.sortable || table.classList.contains('stats-table') || !table.tHead || !table.tBodies.length || table.tHead.rows.length!==1)return;
    table.dataset.sortable='true';
    const headers=[...table.tHead.rows[0].cells],body=table.tBodies[0];
    headers.forEach((header,index)=>{
      const label=header.textContent.trim();if(!label)return;
      const values=[...body.rows].map(row=>row.cells[index]?.textContent.trim()||'');
      const numeric=values.filter(v=>v&&!/^[-—–]$/.test(v)).every(v=>/^[-+]?\d/.test(v));
      const value=row=>{const text=row.cells[index]?.textContent.trim()||'';if(!text||/^[-—–]$/.test(text))return null;if(!numeric)return text;const match=text.replaceAll(',','').match(/^[-+]?\d+(?:\.\d+)?/);return match?Number(match[0]):null;};
      const button=table.ownerDocument.createElement('button');button.type='button';button.className='sort-button';button.textContent=label;
      header.replaceChildren(button);header.setAttribute('scope','col');
      let direction='asc';
      button.addEventListener('click',()=>{
        direction=direction==='asc'?'desc':'asc';
        for(const h of headers){h.setAttribute('aria-sort','none');const b=h.querySelector('.sort-button');if(b)b.textContent=b.dataset.label||b.textContent.replace(/ [▲▼]$/,'');}
        header.setAttribute('aria-sort',direction==='asc'?'ascending':'descending');button.dataset.label=label;button.textContent=label+(direction==='asc'?' ▲':' ▼');
        const rows=[...body.rows].map(element=>({element,value:value(element)}));
        for(const row of sortRows(rows,'value',direction,numeric?'number':'string'))body.appendChild(row.element);
      });
    });
  }
  return {categories,labels,num,sortRows,filterRows,aggregatePlayers,aggregateTeams,enhanceTable};
});
