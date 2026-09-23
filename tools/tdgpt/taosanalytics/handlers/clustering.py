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

"""Clustering handlers: encapsulates clustering business logic"""


from taosanalytics.log import AppLogger
from taosanalytics.service_registry import loader
from taosanalytics.util import get_algo


def handle_clustering(request):
    """
    Execute clustering business logic.

    Parses the incoming request, validates parameters, runs the clustering
    algorithm, and returns the result dict.

    :param request: Flask request object
    :return: dict with clustering result or error information
    """

    try:
        req_json = request.get_json(force=True, silent=False)
        if not req_json:
            raise ValueError("request body is empty or not JSON")
    except Exception as e:
        AppLogger.error("failed to parse clustering request: %s", str(e))
        return {"msg": str(e), "rows": -1}

    # Extract required fields
    if "algo" not in req_json:
        msg = "missing required field: algo"
        AppLogger.error(msg)
        return {"msg": msg, "rows": -1}

    if "data" not in req_json:
        msg = "missing required field: data"
        AppLogger.error(msg)
        return {"msg": msg, "rows": -1}

    try:
        algo = get_algo(req_json, None)
        data = req_json["data"]
        ts_list = req_json.get("ts")
        options = req_json.get("params", {})

        if not isinstance(data, list) or len(data) == 0:
            raise ValueError("data must be a non-empty list")

        if isinstance(data[0], list):
            pass
        elif isinstance(data[0], (int, float)):
            data = [[x] for x in data]
        else:
            raise ValueError("data must be a list of numbers or list of lists")

        if ts_list is not None:
            if not isinstance(ts_list, list) or len(ts_list) != len(data):
                raise ValueError("ts_list length must match data length")

        # Get the clustering service
        service = loader.get_service(algo)
        if service is None:
            msg = f"clustering algorithm '{algo}' not found"
            AppLogger.error(msg)
            return {"msg": msg, "rows": -1}

        # Set input data and parameters
        service.set_input_data(data, ts_list)
        service.set_params(options)

        # Execute clustering
        AppLogger.info("executing clustering with algo=%s", algo)
        result_data = service.execute()

        result = {
            "algo": algo,
            "params": options,
            "labels": result_data["labels"],
            "centers": result_data["centers"],
            "n_clusters": result_data["n_clusters"],
            "metrics": result_data["metrics"],
            "rows": len(result_data["labels"]),
        }

        # Include optional fields
        if "n_noise" in result_data:
            result["n_noise"] = result_data["n_noise"]

        if "probabilities" in result_data:
            result["probabilities"] = result_data["probabilities"]

        if "ts_list" in result_data:
            result["ts"] = result_data["ts_list"]

        AppLogger.debug("clustering result: %s", str(result))
        return result

    except Exception as e:
        result = {"rows": -1, "msg": str(e)}
        AppLogger.error("failed to execute clustering: %s", str(e))
        return result
