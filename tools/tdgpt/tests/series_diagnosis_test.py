# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""Unit tests for lib/dynamic/algo/series_diagnosis.py and
lib/taosanalytics/handlers/series_diagnosis.py."""

import json
import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

from lib.dynamic.algo.series_diagnosis import (
    _MIN_SERIES_LENGTH,
    IsolationForestSeriesDiagnosisScorer,
    OCSVMSeriesDiagnosisScorer,
)
from lib.taosanalytics.handlers.series_diagnosis import (
    _MAX_SERIES_LENGTH,
    handle_series_diagnosis,
)

# ── shared fixtures ──────────────────────────────────────────────────────────

# A fixed "inlier" feature vector — the IsolationForest is trained near this.
_INLIER_FEATURES = [float(i) * 0.01 for i in range(22)]
# An "outlier" vector far from the training distribution.
_OUTLIER_FEATURES = [100.0 + float(i) for i in range(22)]

# A minimal valid series (no pycatch22 needed; features are mocked in inference tests).
_VALID_SERIES = [float(i % 10) for i in range(_MIN_SERIES_LENGTH + 10)]


def _make_iforest_pkl(tmp_dir, name="iforest.pkl", with_pipeline=False):
    """Train a small IsolationForest on _INLIER_FEATURES + noise and save as pkl."""
    rng = np.random.default_rng(42)
    X_train = np.array(_INLIER_FEATURES) + rng.normal(0, 0.01, (100, 22))
    model = IsolationForest(n_estimators=10, random_state=42)
    model.fit(X_train)
    if with_pipeline:
        center = X_train.mean(axis=0).tolist()
        scale = (X_train.std(axis=0) + 1e-8).tolist()
        data = {"model": model, "pipeline": {"center": center, "scale": scale}}
    else:
        data = model
    path = os.path.join(tmp_dir, name)
    joblib.dump(data, path)
    return path


def _make_ocsvm_pkl(tmp_dir, name="ocsvm.pkl"):
    rng = np.random.default_rng(0)
    X_train = np.array(_INLIER_FEATURES) + rng.normal(0, 0.01, (100, 22))
    model = OneClassSVM(kernel="rbf", nu=0.1)
    model.fit(X_train)
    path = os.path.join(tmp_dir, name)
    joblib.dump(model, path)
    return path


def _write_cfg(tmp_dir, algo, model_path, name="model.json"):
    path = os.path.join(tmp_dir, name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"algo": algo, "model_path": model_path}, f)
    return path


def _mock_catch22(features):
    """Return a sys.modules patch dict that stubs pycatch22 to return *features*."""
    return patch.dict(
        "sys.modules",
        {"pycatch22": MagicMock(catch22_all=MagicMock(return_value={"values": features}))},
    )


# ── IsolationForest scorer: validation ───────────────────────────────────────

class IForestValidationTest(unittest.TestCase):
    """Input-validation paths — never reach feature extraction."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        cls.pkl = _make_iforest_pkl(cls.tmp)
        cls.cfg = _write_cfg(cls.tmp, "iforest_diagnosis", cls.pkl)

    def _scorer(self, series):
        return IsolationForestSeriesDiagnosisScorer(path=self.cfg, series=series)

    def test_short_series_raises_value_error(self):
        with self.assertRaises(ValueError):
            self._scorer([1.0] * (_MIN_SERIES_LENGTH - 1)).predict()

    def test_nan_in_series_raises_value_error(self):
        series = list(_VALID_SERIES)
        series[5] = float("nan")
        with self.assertRaises(ValueError):
            self._scorer(series).predict()

    def test_none_in_series_raises_value_error(self):
        series = list(_VALID_SERIES)
        series[5] = None
        with self.assertRaises(ValueError):
            self._scorer(series).predict()

    def test_inf_in_series_raises_value_error(self):
        series = list(_VALID_SERIES)
        series[5] = float("inf")
        with self.assertRaises(ValueError):
            self._scorer(series).predict()

    def test_neg_inf_in_series_raises_value_error(self):
        series = list(_VALID_SERIES)
        series[5] = float("-inf")
        with self.assertRaises(ValueError):
            self._scorer(series).predict()

    def test_algo_mismatch_returns_none_and_predict_raises(self):
        cfg = _write_cfg(self.tmp, "svm_diagnosis", self.pkl, "wrong_algo.json")
        scorer = IsolationForestSeriesDiagnosisScorer(path=cfg, series=_VALID_SERIES)
        with self.assertRaises(RuntimeError):
            scorer.predict()

    def test_missing_model_path_raises_runtime_error(self):
        path = os.path.join(self.tmp, "no_path.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"algo": "iforest_diagnosis"}, f)
        scorer = IsolationForestSeriesDiagnosisScorer(path=path, series=_VALID_SERIES)
        with self.assertRaises(RuntimeError):
            scorer.predict()

    def test_pkl_type_mismatch_raises_runtime_error(self):
        wrong = os.path.join(self.tmp, "wrong_type.pkl")
        joblib.dump(OneClassSVM(), wrong)
        cfg = _write_cfg(self.tmp, "iforest_diagnosis", wrong, "wrong_type.json")
        scorer = IsolationForestSeriesDiagnosisScorer(path=cfg, series=_VALID_SERIES)
        with self.assertRaises(RuntimeError):
            scorer.predict()


# ── IsolationForest scorer: inference ────────────────────────────────────────

class IForestInferenceTest(unittest.TestCase):
    """Score-correctness and pipeline-preprocessing paths."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        cls.pkl = _make_iforest_pkl(cls.tmp)
        cls.cfg = _write_cfg(cls.tmp, "iforest_diagnosis", cls.pkl)
        cls.pkl_pipe = _make_iforest_pkl(cls.tmp, name="iforest_pipe.pkl", with_pipeline=True)
        cls.cfg_pipe = _write_cfg(cls.tmp, "iforest_diagnosis", cls.pkl_pipe, "pipeline.json")

    def _score(self, cfg, features):
        scorer = IsolationForestSeriesDiagnosisScorer(path=cfg, series=_VALID_SERIES)
        with _mock_catch22(features):
            return scorer.predict()

    def test_score_returns_float(self):
        self.assertIsInstance(self._score(self.cfg, _INLIER_FEATURES), float)

    def test_outlier_scores_higher_than_inlier(self):
        # The model was trained near _INLIER_FEATURES; _OUTLIER_FEATURES is far outside.
        inlier_score = self._score(self.cfg, _INLIER_FEATURES)
        outlier_score = self._score(self.cfg, _OUTLIER_FEATURES)
        self.assertGreater(
            outlier_score,
            inlier_score,
            "feature vector outside the training distribution must score higher",
        )

    def test_pipeline_preprocessing_is_applied(self):
        from lib.dynamic.model_loader import ModelLoader

        with patch.object(
            ModelLoader,
            "apply_pipeline_preprocessing",
            wraps=ModelLoader.apply_pipeline_preprocessing,
        ) as mock_prep:
            self._score(self.cfg_pipe, _INLIER_FEATURES)
            mock_prep.assert_called_once()

    def test_no_preprocessing_without_pipeline(self):
        from lib.dynamic.model_loader import ModelLoader

        with patch.object(ModelLoader, "apply_pipeline_preprocessing") as mock_prep:
            self._score(self.cfg, _INLIER_FEATURES)
            mock_prep.assert_not_called()


# ── OCSVM scorer: smoke tests ─────────────────────────────────────────────────

class OCSVMScorerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        cls.pkl = _make_ocsvm_pkl(cls.tmp)
        cls.cfg = _write_cfg(cls.tmp, "svm_diagnosis", cls.pkl)

    def _score(self, features):
        scorer = OCSVMSeriesDiagnosisScorer(path=self.cfg, series=_VALID_SERIES)
        with _mock_catch22(features):
            return scorer.predict()

    def test_score_returns_float(self):
        self.assertIsInstance(self._score(_INLIER_FEATURES), float)

    def test_outlier_scores_higher_than_inlier(self):
        self.assertGreater(self._score(_OUTLIER_FEATURES), self._score(_INLIER_FEATURES))


# ── handler: input validation ─────────────────────────────────────────────────

class SeriesDiagnosisHandlerTest(unittest.TestCase):
    """Tests for handle_series_diagnosis input-validation and routing."""

    def _call(self, payload):
        from flask import Flask
        app_ctx = Flask(__name__)
        with app_ctx.test_request_context(
            "/series-diagnosis",
            method="POST",
            json=payload,
            content_type="application/json",
        ):
            from flask import request as flask_req
            return handle_series_diagnosis(flask_req)

    def test_missing_algo_returns_error(self):
        result = self._call({"data": [[1.0] * 60]})
        self.assertEqual(result["rows"], -1)
        self.assertIn("algo", result["msg"])

    def test_missing_data_returns_error(self):
        result = self._call({"algo": "iforest_diagnosis"})
        self.assertEqual(result["rows"], -1)
        self.assertIn("data", result["msg"])

    def test_multi_column_data_rejected(self):
        result = self._call({
            "algo": "iforest_diagnosis",
            "data": [[1.0] * 60, [2.0] * 60],
        })
        self.assertEqual(result["rows"], -1)
        self.assertIn("1 column", result["msg"])

    def test_series_too_long_rejected(self):
        result = self._call({
            "algo": "iforest_diagnosis",
            "data": [[1.0] * (_MAX_SERIES_LENGTH + 1)],
        })
        self.assertEqual(result["rows"], -1)
        self.assertIn("too long", result["msg"])

    def test_valid_request_routes_to_scorer(self):
        with patch(
            "lib.taosanalytics.handlers.series_diagnosis.do_series_diagnosis",
            return_value=0.42,
        ):
            result = self._call({
                "algo": "iforest_diagnosis",
                "data": [[float(i) for i in range(60)]],
            })
        self.assertEqual(result["rows"], 1)
        self.assertAlmostEqual(result["score"], 0.42)
        self.assertEqual(result["algo"], "iforest_diagnosis")


if __name__ == "__main__":
    unittest.main()
