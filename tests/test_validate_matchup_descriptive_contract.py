"""Research UI handoff validity guarantees."""
from src.validate_matchup_descriptive_contract import verify_contract


def valid():
    return {
        "schema_version":"elocast-descriptive-v1",
        "data_kind":"historical_descriptive_fbs_units",
        "as_of_exclusive":"2026-10-31",
        "metric_definitions":{
            "rush_success_rate":{
                "observation_weighting":"game_equal",
                "denominator_field":None,
                "eligible_population":100,
                "minimum_population":30,
                "status":"available",
            }
        },
        "teams":{
            "1":{
                "team_id":1,"name":"A","metrics":{
                    "rush_success_rate":{
                        "status":"insufficient_sample",
                        "sample_games":1,"distinct_opponents":1,
                        "adjusted_offense":None,
                        "adjusted_defense_allowed":None,
                        "offense_percentile":None,
                        "defense_percentile":None,
                        "offensive_schedule_percentile":None,
                        "defensive_schedule_percentile":None,
                    }
                }
            }
        }
    }


def test_valid_sparse_data():
    assert verify_contract(valid())["passed"]


def test_reject_denominator_fabrication_and_leaked_ranking():
    data=valid()
    m=data["metric_definitions"]["rush_success_rate"]
    m["denominator_field"]="off_plays"
    row=data["teams"]["1"]["metrics"]["rush_success_rate"]
    row["offense_percentile"]=99
    result=verify_contract(data)
    assert not result["passed"]
    assert len(result["errors"])>=2


def test_reject_bogus_probability_field():
    data=valid()
    data["win_probability"]=.9
    assert not verify_contract(data)["passed"]
