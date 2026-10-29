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

"""DynamicRegressionService: a regression service driven by a parameter config file."""

from lib.base import AbstractRegressionService
from lib.dynamic.algo.regressioner import (
    ElasticNetRegressioner,
    LassoRegressioner,
    LinearRegressioner,
    PLSRegressioner,
    PolynomialRegressioner,
    RidgeRegressioner,
    SVRRegressioner,
    XGBoostRegressioner,
)
from lib.log import AppLogger

_DETECTOR_CLASSES = {
    "linear": LinearRegressioner,
    "lasso": LassoRegressioner,
    "ridge": RidgeRegressioner,
    "elasticnet": ElasticNetRegressioner,
    "polynomial": PolynomialRegressioner,
    "svr": SVRRegressioner,
    # `xgboost` alone is the classification model; the registry maps one name to
    # one service, so the regressor needs its own.
    "xgboost_regression": XGBoostRegressioner,
    "pls": PLSRegressioner,
}


class DynamicRegressionService(AbstractRegressionService):
    """
    A simple dynamic regression service driven by a JSON config file.
    The detector is constructed and executed when execute() is called.

    Currently supported algorithms: linear, lasso, ridge, elasticnet, polynomial, svr,
    xgboost_regression, pls.
    """

    def __init__(self, name: str, desc: str, algo: str, path: str):
        super().__init__()

        self.name = name
        self.desc = desc

        self.config_file_path = path
        self.algo = algo

    def execute(self):
        """Construct the detector from the config file and run regression."""
        algo_name = self.algo.lower()
        AppLogger.info(
            "execute dynamic regression service:%s, algo:%s", self.name, algo_name
        )

        detector_class = _DETECTOR_CLASSES.get(algo_name)
        if detector_class is None:
            raise ValueError(
                f"unsupported algorithm '{algo_name}' in dynamic regression service"
            )

        detector = detector_class(
            path=self.config_file_path,
            input_data=self.input_data,
            schema=self.schema,
        )

        result = detector.predict()
        expected_size = len(self.input_data) if self.input_data is not None else None

        if result is None:
            raise ValueError("dynamic regression detector returned no result")

        if expected_size is not None and len(result) != expected_size:
            raise ValueError(
                "dynamic regression detector returned %d predictions for %d input samples"
                % (len(result), expected_size)
            )

        return result
