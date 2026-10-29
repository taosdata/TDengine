"""Tests for the dynamic classifiers, including the XGBoost one.

`XGBoostClassifier` is exercised twice: with a stubbed model so that routing and
the config contract hold without the dependency installed, and end to end against
the real package when it is available (the importorskip keeps the file usable on a
host that does not carry xgboost).
"""

import json
import sys
from unittest.mock import Mock, patch

import numpy as np
import pytest

from lib.dynamic.algo.classifier import XGBoostClassifier
from lib.dynamic.wrapper import classification
from lib.service_registry import ServiceRegistry


def _config(tmp_path, algo, **extra):
    path = tmp_path / "model.json"
    path.write_text(json.dumps({"algo": algo, **extra}), encoding="utf-8")
    return str(path)


class TestRegistration:
    """`algo: xgboost` must reach the new class from both entry points."""

    def test_the_wrapper_routes_xgboost(self):
        assert classification._CLASSIFIER_CLASSES["xgboost"] is XGBoostClassifier

    def test_the_registry_accepts_it_as_a_classification_model(self):
        assert "xgboost" in ServiceRegistry._classification_models


class TestConfigContract:
    def test_a_matching_algo_builds_the_model(self, tmp_path):
        clf = XGBoostClassifier(_config(tmp_path, "XGBoost"), [[1.0, 2.0]])

        with patch.object(XGBoostClassifier, "_build_model", return_value=Mock()):
            assert clf.build() is not None

    def test_another_algo_is_refused(self, tmp_path):
        """The config names the algorithm it holds; a mismatch must not load it."""
        clf = XGBoostClassifier(_config(tmp_path, "decision_tree"), [[1.0, 2.0]])

        assert clf.build() is None

    def test_a_missing_model_path_is_reported(self, tmp_path):
        clf = XGBoostClassifier(_config(tmp_path, "xgboost"), [[1.0, 2.0]])

        with patch("lib.dynamic.algo.classifier.AppLogger") as logger:
            assert clf.build() is None

        assert any("model_path" in str(call) for call in logger.error.call_args_list)

    def test_a_missing_dependency_is_reported_then_predict_raises(self, tmp_path):
        """No xgboost installed: a clear error at load, not an ImportError from joblib."""
        clf = XGBoostClassifier(
            _config(tmp_path, "xgboost", model_path=str(tmp_path / "model.pkl")),
            [[1.0, 2.0]],
        )

        with patch.dict(sys.modules, {"xgboost": None}):
            assert clf.build() is None
            with pytest.raises(RuntimeError):
                clf.predict()


class TestExecute:
    def test_the_service_runs_the_classifier(self, tmp_path):
        path = _config(tmp_path, "xgboost", model_path=str(tmp_path / "model.pkl"))
        service = classification.DynamicClassificationService(
            name="xgboost_test", desc="test", algo="xgboost", path=path
        )
        service.set_input_data([[1.0, 2.0], [3.0, 4.0]])

        model = Mock()
        model.predict.return_value = np.array([0, 1])

        with patch.object(XGBoostClassifier, "_build_model", return_value=model):
            assert service.execute() == [0, 1]

    def test_a_prediction_count_mismatch_is_reported(self, tmp_path):
        path = _config(tmp_path, "xgboost", model_path=str(tmp_path / "model.pkl"))
        service = classification.DynamicClassificationService(
            name="xgboost_test", desc="test", algo="xgboost", path=path
        )
        service.set_input_data([[1.0, 2.0], [3.0, 4.0]])

        model = Mock()
        model.predict.return_value = np.array([0])

        with patch.object(XGBoostClassifier, "_build_model", return_value=model):
            with pytest.raises(ValueError):
                service.execute()


class TestPipelineState:
    """The shared preprocessing must tolerate the shapes real pickles carry."""

    def test_fill_values_without_center_or_scale(self):
        """Regression: `np.array(None)` is not None, so the guard let a
        fill-only state through and prediction died on `None + 1e-8`."""
        from lib.dynamic.model_loader import ModelLoader

        X = np.array([[1.0, np.nan], [3.0, 4.0]])

        out = ModelLoader.apply_pipeline_preprocessing(X, {"fill_values": {1: 0.0}})

        assert out.tolist() == [[1.0, 0.0], [3.0, 4.0]]

    def test_center_and_scale_are_applied_together(self):
        from lib.dynamic.model_loader import ModelLoader

        X = np.array([[2.0, 4.0]])

        out = ModelLoader.apply_pipeline_preprocessing(
            X, {"center": [1.0, 1.0], "scale": [1.0, 1.0]}
        )

        assert np.allclose(out, [[1.0, 3.0]])


class TestRealXGBoost:
    """Runs where xgboost is installed; skipped otherwise."""

    def test_a_trained_model_is_loaded_and_classifies(self, tmp_path):
        xgboost = pytest.importorskip("xgboost")
        import joblib
        from sklearn.datasets import make_classification

        X, y = make_classification(
            n_samples=60,
            n_features=4,
            n_informative=3,
            n_redundant=0,
            random_state=0,
        )
        model = xgboost.XGBClassifier(n_estimators=5, max_depth=2, random_state=0)
        model.fit(X, y)

        model_path = tmp_path / "xgboost.pkl"
        # Fill-only pipeline state: what an XGBoost pickled without a scaler has.
        joblib.dump({"model": model, "pipeline": {"fill_values": {}}}, model_path)

        clf = XGBoostClassifier(
            _config(tmp_path, "xgboost", model_path=str(model_path)),
            X[:5].tolist(),
        )

        predictions = clf.predict()

        assert len(predictions) == 5
        # The sklearn wrapper carries classes_, so labels come back as trained --
        # not as encoded indices from a bare Booster.
        assert set(predictions).issubset(set(y.tolist()))
