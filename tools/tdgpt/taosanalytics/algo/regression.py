# pylint: disable=c0103
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
