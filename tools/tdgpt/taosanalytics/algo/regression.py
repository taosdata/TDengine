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

"""regression handler functions"""

from taosanalytics.error import failed_load_model_except
from taosanalytics.log import AppLogger
from taosanalytics.service_registry import loader


def do_regression(input_data, schema, algo_name, params):
    """Execute regression using the specified algorithm.

    Args:
        input_data: Feature matrix (list of sample rows)
        schema: Column schema metadata
        algo_name: Name of the registered regression service
        params: Additional parameters

    Returns:
        list[float]: Predicted values
    """
    s = loader.get_service(algo_name)

    if s is None:
        AppLogger.error("specified regression model not found: %s", algo_name)
        failed_load_model_except(algo_name)

    s.set_input_data(input_data, schema)
    s.set_params(params)

    result = s.execute()

    AppLogger.debug(
        "regression result: %d predictions from %d input samples",
        len(result),
        len(input_data),
    )

    return result
