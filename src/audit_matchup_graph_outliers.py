"""Research-only schedule graph and outlier diagnostics for descriptive FBS metrics.

Connectedness is measured on the *observed FBS game graph*, not conference
membership. Removing a bridge reveals fragile between-group comparisons.
Outlier sensitivity re-fits the SAME descriptive solver after symmetrically
winsorizing team-game outcomes; no predictive or causal claims are made.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
from math import isfinite

try:
    from src.describe_matchup_metrics import describe_units
    from src.matchup_intelligence import Observation
except ModuleNotFoundError:
    from describe_matchup_metrics import describe_units
    from matchup_intelligence import Observation


def schedule_graph(observations: list[Observation]) -> dict:
    """Return components, bridge edges and team-specific connectivity facts.

    Each undirected matchup is one edge regardless of repeated games or
    whether both team-game observations exist. This diagnoses connectivity;
    it does *not* assert a team's offensive/defensive effect is identifiable.
    """
    adjacency: dict[str, set[str]] = defaultdict(set)
    edge_rows: dict[tuple[str, str], int] = defaultdict(int)
    for r in observations:
        if not r.team or not r.opponent or r.team == r.opponent:
            raise ValueError("Invalid schedule edge")
        a,b=sorted((r.team,r.opponent))
        adjacency[a].add(b)
        adjacency[b].add(a)
        edge_rows[a,b] += 1
    seen=set()
    components=[]
    for start in sorted(adjacency):
        if start in seen:
            continue
        stack=[start]
        found=[]
        seen.add(start)
        while stack:
            node=stack.pop()
            found.append(node)
            for neighbor in sorted(adjacency[node]):
                if neighbor not in seen:
                    seen.add(neighbor)
                    stack.append(neighbor)
        components.append(sorted(found))
    components.sort(key=lambda c:(-len(c),c[0]))
    # Tarjan bridge search: one *unique matchup* edge, not one row.
    discovery={}
    low={}
    bridges=set()
    tick=0
    def dfs(node, parent):
        nonlocal tick
        discovery[node]=low[node]=tick
        tick+=1
        for nxt in sorted(adjacency[node]):
            if nxt==parent:
                continue
            if nxt not in discovery:
                dfs(nxt,node)
                low[node]=min(low[node],low[nxt])
                if low[nxt]>discovery[node]:
                    bridges.add(tuple(sorted((node,nxt))))
            else:
                low[node]=min(low[node],discovery[nxt])
    for node in sorted(adjacency):
        if node not in discovery:
            dfs(node,None)
    giant=len(components[0]) if components else 0
    membership={t:i for i,comp in enumerate(components) for t in comp}
    return {
        "teams":len(adjacency),
        "unique_matchups":len(edge_rows),
        "connected_components":len(components),
        "largest_component_teams":giant,
        "component_sizes":[len(c) for c in components],
        "bridge_matchups":len(bridges),
        "team_graph":{
            t:{
                "distinct_opponents":len(adjacency[t]),
                "component_id":membership[t],
                "component_size":len(components[membership[t]]),
                "in_largest_component":membership[t]==0,
                "incident_bridges":sum(tuple(sorted((t,n))) in bridges for n in adjacency[t]),
            } for t in sorted(adjacency)
        },
    }


def _quantile(values: list[float], q:float)->float:
    if not values or not 0<=q<=1:
        raise ValueError("Invalid values or quantile")
    ordered=sorted(values)
    pos=(len(ordered)-1)*q
    k=int(pos)
    return ordered[k]+(pos-k)*(ordered[min(k+1,len(ordered)-1)]-ordered[k])


def winsor_sensitivity(observations:list[Observation], *,
                      shrinkage:float=2., lower_q:float=.01,
                      upper_q:float=.99, min_games:int=3)->dict:
    """Refit after tail-clipping game-level metric values.

    Percentiles are compared for matched rated teams only, because a change
    of reference population would exaggerate or conceal the sensitivity.
    Clipping is a diagnostic, never a recommendation to replace actual values.
    """
    if not observations:
        return {"status":"no_data"}
    if not (0<=lower_q<upper_q<=1):
        raise ValueError("Invalid winsorization bounds")
    values=[r.value for r in observations]
    if not all(isfinite(v) for v in values):
        raise ValueError("Nonfinite metric input")
    lo,hi=_quantile(values,lower_q),_quantile(values,upper_q)
    original=describe_units(observations,shrinkage=shrinkage,min_games=min_games)
    clipped_rows=[replace(r,value=max(lo,min(hi,r.value))) for r in observations]
    clipped=describe_units(clipped_rows,shrinkage=shrinkage,min_games=min_games)
    changed=sum(r.value!=new.value for r,new in zip(observations,clipped_rows))
    common=sorted(t for t in original["teams"] if
                  original["teams"][t]["rating_eligible"] and
                  clipped["teams"].get(t,{}).get("rating_eligible"))
    diffs={}
    for key in ("offensive_percentile","defensive_percentile",
                "offensive_schedule_percentile","defensive_schedule_percentile"):
        shifts=sorted(abs(original["teams"][t][key]-clipped["teams"][t][key])
                      for t in common)
        diffs[key]={
            "median":_quantile(shifts,.5) if shifts else None,
            "p90":_quantile(shifts,.9) if shifts else None,
            "max":max(shifts) if shifts else None,
            "at_least_10_points":sum(x>=10 for x in shifts),
            "at_least_20_points":sum(x>=20 for x in shifts),
        }
    return {
        "status":"evaluated" if common else "insufficient",
        "observations":len(values),"lower_quantile":lower_q,
        "upper_quantile":upper_q,"lower_clip":lo,"upper_clip":hi,
        "clipped_observation_count":changed,
        "clipped_observation_fraction":changed/len(values),
        "matched_rated_teams":len(common),
        "percentile_shift":diffs,
        "note":"Outlier sensitivity only; not a new metric, predictive skill or confidence interval",
    }
