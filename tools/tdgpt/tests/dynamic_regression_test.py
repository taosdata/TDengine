"""Tests for the dynamic regressors, including XGBoost and PLS.

The XGBoost case needs the package, so it is skipped where it is absent; PLS only
needs scikit-learn and runs wherever the service can.
"""

import json
import sys
from unittest.mock import Mock, patch

import numpy as np
import pytest

from lib.dynamic.algo.regressioner import PLSRegressioner, XGBoostRegressioner
from lib.dynamic.wrapper import regression
from lib.service_registry import ServiceRegistry


def _config(tmp_path, algo, **extra):
    path = tmp_path / "model.json"
    path.write_text(json.dumps({"algo": algo, **extra}), encoding="utf-8")
    return str(path)


class TestRegistration:
    def test_the_wrapper_routes_both_new_models(self):
        assert regression._DETECTOR_CLASSES["xgboost_regression"] is XGBoostRegressioner
        assert regression._DETECTOR_CLASSES["pls"] is PLSRegressioner

    def test_the_registry_accepts_them_as_regression_models(self):
        assert "xgboost_regression" in ServiceRegistry._regression_models
        assert "pls" in ServiceRegistry._regression_models

    def test_the_classification_name_is_not_taken_over(self):
        """A config is routed to the first matching service, and regression is
        checked before classification -- so the bare name must stay out of this
        list, or `algo: xgboost` classifiers would run as regressors."""
        assert "xgboost" not in ServiceRegistry._regression_models
        assert "xgboost" in ServiceRegistry._classification_models


class TestConfigContract:
    @pytest.mark.parametrize(
        "cls, algo",
        [
            (XGBoostRegressioner, "XGBoost_Regression"),
            (PLSRegressioner, "pls"),
        ],
    )
    def test_a_matching_algo_builds_the_model(self, tmp_path, cls, algo):
        """Case and `-`/`_` are normalised, so the config spelling is free."""
        reg = cls(_config(tmp_path, algo), [[1.0, 2.0]])

        with patch.object(cls, "_build_model", return_value=Mock()):
            assert reg.build() is not None

    def test_another_algo_is_refused(self, tmp_path):
        reg = PLSRegressioner(_config(tmp_path, "linear"), [[1.0, 2.0]])

        assert reg.build() is None

    def test_a_missing_model_path_is_reported(self, tmp_path):
        reg = PLSRegressioner(_config(tmp_path, "pls"), [[1.0, 2.0]])

        with patch("lib.dynamic.algo.regressioner.AppLogger") as logger:
            assert reg.build() is None

        assert any("model_path" in str(call) for call in logger.error.call_args_list)

    def test_a_missing_xgboost_is_reported_then_predict_raises(self, tmp_path):
        reg = XGBoostRegressioner(
            _config(tmp_path, "xgboost_regression", model_path=str(tmp_path / "model.pkl")),
            [[1.0, 2.0]],
        )

        with patch.dict(sys.modules, {"xgboost": None}):
            assert reg.build() is None
            with pytest.raises(RuntimeError):
                reg.predict()


class TestExecute:
    def test_the_service_runs_the_regressor(self, tmp_path):
        path = _config(tmp_path, "pls", model_path=str(tmp_path / "model.pkl"))
        service = regression.DynamicRegressionService(
            name="pls_test", desc="test", algo="pls", path=path
        )
        service.set_input_data([[1.0, 2.0], [3.0, 4.0]])

        model = Mock()
        model.predict.return_value = np.array([0.5, 1.5])

        with patch.object(PLSRegressioner, "_build_model", return_value=model):
            assert service.execute() == [0.5, 1.5]

    def test_a_prediction_count_mismatch_is_reported(self, tmp_path):
        path = _config(tmp_path, "pls", model_path=str(tmp_path / "model.pkl"))
        service = regression.DynamicRegressionService(
            name="pls_test", desc="test", algo="pls", path=path
        )
        service.set_input_data([[1.0, 2.0], [3.0, 4.0]])

        model = Mock()
        model.predict.return_value = np.array([0.5])

        with patch.object(PLSRegressioner, "_build_model", return_value=model):
            with pytest.raises(ValueError):
                service.execute()


class TestRealPLS:
    def test_a_trained_model_is_loaded_and_flattens_its_output(self, tmp_path):
        import joblib
        from sklearn.cross_decomposition import PLSRegression

        rng = np.random.default_rng(0)
        X = rng.normal(size=(40, 3))
        y = X @ np.array([1.5, -2.0, 0.5]) + 0.1

        model = PLSRegression(n_components=2)
        model.fit(X, y)

        model_path = tmp_path / "pls.pkl"
        joblib.dump(model, model_path)

        reg = PLSRegressioner(
            _config(tmp_path, "pls", model_path=str(model_path)), X[:4].tolist()
        )

        predictions = reg.predict()

        assert len(predictions) == 4
        # PLSRegression.predict returns (n, 1); the client must get floats.
        assert all(isinstance(value, float) for value in predictions)
        assert np.allclose(predictions, model.predict(X[:4]).ravel(), atol=1e-6)


class TestRealXGBoost:
    def test_a_trained_model_is_loaded_and_predicts(self, tmp_path):
        xgboost = pytest.importorskip("xgboost")
        import joblib

        rng = np.random.default_rng(0)
        X = rng.normal(size=(60, 4))
        y = X @ np.array([1.0, -0.5, 2.0, 0.25])

        model = xgboost.XGBRegressor(n_estimators=10, max_depth=2, random_state=0)
        model.fit(X, y)

        model_path = tmp_path / "xgboost.pkl"
        # Fill-only pipeline state: what a pickle without a scaler carries.
        joblib.dump({"model": model, "pipeline": {"fill_values": {}}}, model_path)

        reg = XGBoostRegressioner(
            _config(tmp_path, "xgboost_regression", model_path=str(model_path)),
            X[:5].tolist(),
        )

        predictions = reg.predict()

        assert len(predictions) == 5
        assert all(isinstance(value, float) for value in predictions)
