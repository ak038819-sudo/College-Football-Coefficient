"""Leave-one-opponent-out sensitivity never invents certainty for thin samples."""
from src.audit_matchup_stability import opponent_sensitivity
from src.matchup_intelligence import Observation


def test_sparse_schedule_not_called_stable():
    rows=[Observation("A","B",.5),Observation("B","A",.4)]
    output=opponent_sensitivity(rows)
    assert output["A"]["status"]=="insufficient"
    assert output["A"]["checks"]==0


def test_connected_schedule_produces_finite_percentile_sensitivity():
    names=("A","B","C","D","E","F")
    rows=[]
    for i,a in enumerate(names):
        for j,b in enumerate(names):
            if a!=b:
                rows.append(Observation(a,b,.45+.01*(i-j)))
    output=opponent_sensitivity(rows, min_games=3)
    assert output["A"]["status"]=="evaluated"
    assert output["A"]["checks"]==5
    assert 0<=output["A"]["max_offense_percentile_change"]<=100
    assert 0<=output["A"]["mean_defense_percentile_change"]<=100
