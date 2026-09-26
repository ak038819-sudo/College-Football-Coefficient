"""
Model validation harness (MODEL-07).

Two kinds of question, deliberately kept apart:

  metrics / predictive  How good are the model's FORECASTS? Brier score, log
                        loss and calibration over walk-forward folds. These
                        apply to Elo and to the hybrid expectation -- anything
                        that states a probability before kickoff.

  achievement           Is CoE a good ACHIEVEMENT system? Not the same question,
                        and the spec is explicit that CoE must not be tuned to
                        maximise prediction. A rating that perfectly predicted
                        results would just be Elo; CoE is supposed to reward what
                        a team earned against the schedule it actually played.

Mixing the two is the failure mode this package exists to prevent, so the report
keeps them in separate sections and never computes a single "score".
"""
from validation.folds import Fold, assert_chronological, season_folds  # noqa: F401
from validation.metrics import (brier_score, calibration_error, log_loss,  # noqa: F401
                                reliability_bins, score_predictions, sharpness)
