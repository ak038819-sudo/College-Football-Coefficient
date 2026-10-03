// Exact scoreboard display names. Never infer identity from a school prefix or
// compare a provider ID with an internal database ID. Unlisted names stay raw.
(function (root) {
  const displayNames = {
    'Air Force': 'Air Force Falcons', Akron: 'Akron Zips', Alabama: 'Alabama Crimson Tide',
    'Appalachian State': 'App State Mountaineers', Arizona: 'Arizona Wildcats',
    'Arizona State': 'Arizona State Sun Devils', Arkansas: 'Arkansas Razorbacks',
    'Arkansas State': 'Arkansas State Red Wolves', Army: 'Army Black Knights', Auburn: 'Auburn Tigers',
    BYU: 'BYU Cougars', 'Ball State': 'Ball State Cardinals', Baylor: 'Baylor Bears',
    'Boise State': 'Boise State Broncos', 'Boston College': 'Boston College Eagles',
    'Bowling Green': 'Bowling Green Falcons', Buffalo: 'Buffalo Bulls', California: 'California Golden Bears',
    'Central Michigan': 'Central Michigan Chippewas', Charlotte: 'Charlotte 49ers',
    Cincinnati: 'Cincinnati Bearcats', Clemson: 'Clemson Tigers', 'Coastal Carolina': 'Coastal Carolina Chanticleers',
    Colorado: 'Colorado Buffaloes', 'Colorado State': 'Colorado State Rams', Delaware: 'Delaware Blue Hens',
    Duke: 'Duke Blue Devils', 'East Carolina': 'East Carolina Pirates', 'Eastern Michigan': 'Eastern Michigan Eagles',
    FAU: 'Florida Atlantic Owls', FIU: 'Florida International Panthers', Florida: 'Florida Gators',
    'Florida State': 'Florida State Seminoles', 'Fresno State': 'Fresno State Bulldogs', Georgia: 'Georgia Bulldogs',
    'Georgia Southern': 'Georgia Southern Eagles', 'Georgia State': 'Georgia State Panthers',
    'Georgia Tech': 'Georgia Tech Yellow Jackets', "Hawai'i": "Hawai'i Rainbow Warriors", Houston: 'Houston Cougars',
    Idaho: 'Idaho Vandals', Illinois: 'Illinois Fighting Illini', Indiana: 'Indiana Hoosiers', Iowa: 'Iowa Hawkeyes',
    'Iowa State': 'Iowa State Cyclones', 'Jacksonville State': 'Jacksonville State Gamecocks',
    'James Madison': 'James Madison Dukes', Kansas: 'Kansas Jayhawks', 'Kansas State': 'Kansas State Wildcats',
    'Kennesaw State': 'Kennesaw State Owls', 'Kent State': 'Kent State Golden Flashes', Kentucky: 'Kentucky Wildcats',
    LSU: 'LSU Tigers', Liberty: 'Liberty Flames', Louisiana: 'Louisiana Ragin Cajuns',
    'Louisiana Tech': 'Louisiana Tech Bulldogs', Louisville: 'Louisville Cardinals', Marshall: 'Marshall Thundering Herd',
    Maryland: 'Maryland Terrapins', Massachusetts: 'Massachusetts Minutemen', Memphis: 'Memphis Tigers',
    'Miami (FL)': 'Miami Hurricanes', 'Miami (OH)': 'Miami (OH) RedHawks', Michigan: 'Michigan Wolverines',
    'Michigan State': 'Michigan State Spartans', 'Middle Tennessee': 'Middle Tennessee Blue Raiders',
    Minnesota: 'Minnesota Golden Gophers', 'Mississippi State': 'Mississippi State Bulldogs', Missouri: 'Missouri Tigers',
    'Missouri State': 'Missouri State Bears', 'NC State': 'NC State Wolfpack', Navy: 'Navy Midshipmen',
    Nebraska: 'Nebraska Cornhuskers', Nevada: 'Nevada Wolf Pack', 'New Mexico': 'New Mexico Lobos',
    'New Mexico State': 'New Mexico State Aggies', 'North Carolina': 'North Carolina Tar Heels',
    'North Dakota State': 'North Dakota State Bison', 'North Texas': 'North Texas Mean Green',
    'Northern Illinois': 'Northern Illinois Huskies', Northwestern: 'Northwestern Wildcats',
    'Notre Dame': 'Notre Dame Fighting Irish', Ohio: 'Ohio Bobcats', 'Ohio State': 'Ohio State Buckeyes',
    Oklahoma: 'Oklahoma Sooners', 'Oklahoma State': 'Oklahoma State Cowboys', 'Old Dominion': 'Old Dominion Monarchs',
    'Ole Miss': 'Ole Miss Rebels', Oregon: 'Oregon Ducks', 'Oregon State': 'Oregon State Beavers',
    'Penn State': 'Penn State Nittany Lions', Pittsburgh: 'Pittsburgh Panthers', Purdue: 'Purdue Boilermakers',
    Rice: 'Rice Owls', Rutgers: 'Rutgers Scarlet Knights', SMU: 'SMU Mustangs', 'Sacramento State': 'Sacramento State Hornets',
    'Sam Houston': 'Sam Houston Bearkats', 'San Diego State': 'San Diego State Aztecs',
    'San Jose State': 'San Jose State Spartans', 'South Alabama': 'South Alabama Jaguars',
    'South Carolina': 'South Carolina Gamecocks', 'South Florida': 'South Florida Bulls',
    'Southern Miss': 'Southern Miss Golden Eagles', Stanford: 'Stanford Cardinal', Syracuse: 'Syracuse Orange',
    TCU: 'TCU Horned Frogs', Temple: 'Temple Owls', Tennessee: 'Tennessee Volunteers', Texas: 'Texas Longhorns',
    'Texas A&M': 'Texas A&M Aggies', 'Texas State': 'Texas State Bobcats', 'Texas Tech': 'Texas Tech Red Raiders',
    Toledo: 'Toledo Rockets', Troy: 'Troy Trojans', Tulane: 'Tulane Green Wave', Tulsa: 'Tulsa Golden Hurricane',
    UAB: 'UAB Blazers', UCF: 'UCF Knights', UCLA: 'UCLA Bruins', UConn: 'UConn Huskies', ULM: 'UL Monroe Warhawks',
    UNLV: 'UNLV Rebels', USC: 'USC Trojans', UTEP: 'UTEP Miners', UTSA: 'UTSA Roadrunners', Utah: 'Utah Utes',
    'Utah State': 'Utah State Aggies', Vanderbilt: 'Vanderbilt Commodores', Virginia: 'Virginia Cavaliers',
    'Virginia Tech': 'Virginia Tech Hokies', 'Wake Forest': 'Wake Forest Demon Deacons', Washington: 'Washington Huskies',
    'Washington State': 'Washington State Cougars', 'West Virginia': 'West Virginia Mountaineers',
    'Western Kentucky': 'Western Kentucky Hilltoppers', 'Western Michigan': 'Western Michigan Broncos',
    Wisconsin: 'Wisconsin Badgers', Wyoming: 'Wyoming Cowboys'
  };
  const aliases = {
    'Florida Atlantic': 'FAU', 'FAU Owls': 'FAU', 'Florida International': 'FIU', 'FIU Panthers': 'FIU',
    'App State': 'Appalachian State', 'Appalachian State Mountaineers': 'Appalachian State',
    'Miami': 'Miami (FL)', 'Miami (FL) Hurricanes': 'Miami (FL)',
    'Miami (Ohio) RedHawks': 'Miami (OH)', 'UMass': 'Massachusetts', 'UMass Minutemen': 'Massachusetts',
    'UL Monroe': 'ULM', 'ULM Warhawks': 'ULM', 'Hawaii': "Hawai'i", 'Hawaii Rainbow Warriors': "Hawai'i",
    "Louisiana Ragin' Cajuns": 'Louisiana'
  };
  const normalize = name => String(name || '').normalize('NFKD').toLowerCase()
    .replace(/[\u0300-\u036f]/g, '').replace(/[’']/g, '').replace(/\s+/g, ' ').trim();

  function createResolver(teams) {
    const byName = new Map(teams.map(team => [team.name, team]));
    const index = new Map();
    function add(name, team) {
      if (!team) return;
      const key = normalize(name);
      // An ambiguous alias must never silently select the last school.
      if (index.has(key) && index.get(key) !== team) index.set(key, null);
      else if (!index.has(key)) index.set(key, team);
    }
    teams.forEach(team => add(team.name, team));
    Object.entries(displayNames).forEach(([school, name]) => add(name, byName.get(school)));
    Object.entries(aliases).forEach(([name, school]) => add(name, byName.get(school)));
    return side => index.get(normalize(side?.name)) || null;
  }
  const api = {createResolver};
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.CfbLiveTeams = api;
})(typeof globalThis !== 'undefined' ? globalThis : this);
