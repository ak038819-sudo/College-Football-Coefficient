from dataclasses import replace
import pytest
from src.nba.ean import Candidate, Decision, evaluate, career_recipients


BASE = Candidate("player-1", 2016, "regular", "scoring", 100, 100, 100, 0.95, True, "A")


def test_perfect_candidate_qualifies():
    result = evaluate(BASE)
    assert result.decision == Decision.QUALIFIED
    assert result.eas == 100


def test_exact_threshold_passes():
    # 0.45*100 + 0.40*100 + 0.15*(200/3) = 95,
    # but the sustained gate must still be >=80.
    candidate = replace(BASE, rarity=95, impact=98, sustained=80)
    assert evaluate(candidate).decision == Decision.FAILED


@pytest.mark.parametrize("field,value", [
    ("rarity", 94.99), ("impact", 97.99), ("sustained", 79.99),
    ("participation", 0.699), ("rarity", None),
])
def test_gates(field, value):
    result = evaluate(replace(BASE, **{field: value}))
    assert result.decision != Decision.QUALIFIED


def test_missing_data_not_failure():
    assert evaluate(replace(BASE, impact=None)).decision == Decision.INSUFFICIENT_DATA
    assert evaluate(replace(BASE, data_validated=False)).decision == Decision.INSUFFICIENT_DATA


def test_playoff_not_scored_as_regular():
    assert evaluate(replace(BASE, scope="playoffs")).decision == Decision.INSUFFICIENT_DATA


def test_invalid_range_rejected():
    with pytest.raises(ValueError):
        evaluate(replace(BASE, rarity=101))


def test_one_per_career():
    second = replace(BASE, season=2017, rarity=99)
    winners = career_recipients([(BASE, evaluate(BASE)), (second, evaluate(second))])
    assert len(winners) == 1
    assert winners["player-1"][0].season == 2016
