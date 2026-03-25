# encoding:utf-8
"""misc service: encapsulates miscellaneous tool business logic"""

import numpy as np

from taosanalytics.algo.tool.batch import do_batch_process, update_config
from taosanalytics.log import AppLogger
from taosanalytics.util import do_initial_check


def handle_batch(request):
    """
    Execute batch processing business logic.

    :param request: Flask request object
    :return: dict with batch result or error information
    """
    try:
        payload_obj = do_initial_check(request)
    except Exception as e:
        return {"msg": str(e), "rows": -1}

    data = payload_obj.get("data", None)
    ts = payload_obj.get("ts", None)
    windows = payload_obj.get("window", None)

    if data is None or ts is None or windows is None:
        msg = "'data', 'ts', and 'window' are required fields in the payload."
        AppLogger.get_instance().error(msg)
        return {"msg": msg, "rows": -1}

    conf = update_config(payload_obj.get("config", None))

    try:
        # median, lower bounding, upper bounding, processed_batches
        center, lower, upper, processed_batches = do_batch_process(np.array(ts), np.array(data), windows, conf)

        res = {"rows": lower.size, "center": center.tolist(), "lower": lower.tolist(), "upper": upper.tolist()}
        AppLogger.get_instance().debug("batch processed result: %s", res)

        return res
    except Exception as e:
        AppLogger.get_instance().error('golden batch process failed, %s', str(e))
        return {"msg": str(e), "rows": -1}
