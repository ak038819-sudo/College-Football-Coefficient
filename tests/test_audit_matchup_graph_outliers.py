"""Schedule graph and outlier robustness don't infer confidence from thin data."""
import pytest
from src.audit_matchup_graph_outliers import schedule_graph, winsor_sensitivity
from src.matchup_intelligence import Observation


def obs(edges):
    return [Observation(a,b,v) for a,b,v in edges]


def test_disconnected_components_and_bridge_account_for_repeated_games():
    r=schedule_graph(obs([
        ("A","B",.4),("B","A",.3),("A","B",.5),
        ("B","C",.6),("D","E",.4),
    ]))
    assert r["teams"]==5
    assert r["connected_components"]==2
    assert r["component_sizes"]==[3,2]
    assert r["bridge_matchups"]==3
    assert r["team_graph"]["B"]["distinct_opponents"]==2
    assert r["team_graph"]["B"]["incident_bridges"]==2
    assert not r["team_graph"]["D"]["in_largest_component"]


def test_cycle_has_no_bridge():
    r=schedule_graph(obs([("A","B",.4),("B","C",.5),("C","A",.6)]))
    assert r["connected_components"]==1
    assert r["bridge_matchups"]==0


def test_outlier_clip_preserves_raw_data_and_matches_comparison_population():
    names=("A","B","C","D","E")
    rows=[Observation(a,b,(.4+.02*i-.01*j) if (i,j)!=(0,1) else 3.0)
          for i,a in enumerate(names) for j,b in enumerate(names) if i!=j]
    first=rows[0].value
    outcome=winsor_sensitivity(rows, lower_q=.1,upper_q=.9,min_games=3)
    assert outcome["status"]=="evaluated"
    assert outcome["matched_rated_teams"]==5
    assert outcome["clipped_observation_count"]>=1
    assert rows[0].value==first
    assert outcome["percentile_shift"]["offensive_percentile"]["max"]>=0


def test_empty_outlier_and_invalid_edges():
    assert winsor_sensitivity([])["status"]=="no_data"
    assert schedule_graph([])["connected_components"]==0
    with pytest.raises(ValueError):
        schedule_graph([Observation("A","A",1)])
    with pytest.raises(ValueError):
        winsor_sensitivity([Observation("A","B",.5)],lower_q=.9,upper_q=.1)
