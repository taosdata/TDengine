# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

"""Validate Lite dependencies with small CPU training and forecasting workloads."""

import importlib.metadata as metadata
import json
import sys
import tempfile
from pathlib import Path

import numpy as np
import pyarrow
import pycatch22
from Cython.Build import cythonize
from pyod.models.iforest import IForest
from sklearn.cross_decomposition import PLSRegression
from statsforecast.models import AutoCES, AutoETS, AutoTheta
from xgboost import XGBClassifier, XGBRegressor

x = np.arange(64, dtype=float).reshape(32, 2)
y = np.arange(32, dtype=float)
clf = XGBClassifier(n_estimators=3, max_depth=2, n_jobs=1)
clf.fit(x, (y >= 16).astype(int))
assert len(clf.predict(x)) == 32
reg = XGBRegressor(n_estimators=3, max_depth=2, n_jobs=1)
reg.fit(x, y)
assert np.isfinite(reg.predict(x)).all()
assert np.isfinite(PLSRegression(n_components=1).fit(x, y).predict(x)).all()
assert len(IForest(n_estimators=5, n_jobs=1, random_state=42).fit(x).predict(x)) == 32
series = np.arange(1, 49, dtype=float) + np.sin(np.arange(48))
features = pycatch22.catch22_all((np.arange(128) + np.sin(np.arange(128))).tolist())
assert len(features["values"]) == 22
assert np.isfinite(features["values"]).all()
for cls in (AutoETS, AutoCES, AutoTheta):
    result = cls(season_length=1).fit(series).predict(h=3)
    assert len(result["mean"]) == 3 and np.isfinite(result["mean"]).all(), cls
with tempfile.TemporaryDirectory() as directory:
    module = Path(directory) / "user_algorithm.pyx"
    module.write_text("def square(double x): return x * x\n", encoding="utf-8")
    cythonize([str(module)], quiet=True, compiler_directives={"language_level": 3})
    assert module.with_suffix(".c").is_file()
print(
    json.dumps(
        {
            "python": sys.version.split()[0],
            "pyarrow": pyarrow.__version__,
            "xgboost_cpu": metadata.version("xgboost-cpu"),
            "checks": [
                "xgboost-classification",
                "xgboost-regression",
                "pls",
                "pyod-iforest",
                "catch22",
                "ets",
                "ces",
                "theta",
                "cython-code-generation",
            ],
        },
        indent=2,
    )
)
