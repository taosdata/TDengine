---
title: Time-Series Forecasting
sidebar_label: Time-Series Forecasting
---

### Input Limitations

`execute` is the core method of forecasting algorithms. Before calling this method, the framework configures the historical time-series data used for forecasting in the `self.list` object attribute.

### Output Limitations and Parent Class Attributes

Running the `execute` method generates the following dictionary objects:

```python
return {
    "mse": mse, # Mean squared error of the fit data
    "res": res  # Result groups [timestamp, forecast results, lower boundary of confidence interval, upper boundary of confidence interval]
}
```

The parent class `AbstractForecastService` of forecasting algorithms includes the following object attributes.

| Attribute   | Description                                                  | Default |
| ----------- | ------------------------------------------------------------ | ------- |
| period      | Specify the periodicity of the data, i.e. the number of data points included in each period. If the data is not periodic, enter 0. | 0       |
| start_ts    | Specify the start time of forecasting results.               | 0       |
| time_step   | Specify the interval between consecutive data points in the forecast results. | 0       |
| fc_rows     | Specify the number of forecast rows to return.               | 0       |
| return_conf | Specify 1 to include a confidence interval in the forecast results or 0 to not include a confidence interval in the results. If you specify 0, the mean is returned as the upper and lower boundaries. | 1       |
| conf        | Specify a confidence interval quantile.                      | 95      |

### Sample Code

The following code is an sample algorithm that always returns 1 as the forecast results.

```python
import numpy as np
from taosanalytics.service import AbstractForecastService


# Algorithm files must start with an underscore ("_") and end with "Service".
class _MyForecastService(AbstractForecastService):
    """ Define a class inheriting from AbstractAnomalyDetectionService and implementing the `execute` method.  """

    # Name the algorithm using only lowercase ASCII characters.
    name = 'myfc'

    # Include a description of the algorithm (recommended)
    desc = """return the forecast time series data"""

    def __init__(self):
        """Method to initialize the class"""
        super().__init__()

    def execute(self):
        """ Implementation of algorithm logic"""
        res = []

        """This algorithm always returns 1 as the forecast result. The number of results returned is determined by the self.fc_rows value input by the user."""
        ts_list = [self.start_ts + i * self.time_step for i in range(self.fc_rows)]
        res.append(ts_list)  # set timestamp column for forecast results

        """Generate forecast results whose value is 1. """
        res_list = [1] * self.fc_rows
        res.append(res_list)

        """Check whether user has requested the upper and lower boundaries of the confidence interval."""
        if self.return_conf:
            """If the algorithm does not calculate these values, return the forecast results."""
            bound_list = [1] * self.fc_rows
            res.append(bound_list)  # lower confidence limit
            res.append(bound_list)  # upper confidence limit

        """Return results"""
        return {"res": res, "mse": 0}

    def set_params(self, params):
        """This algorithm does not take any parameters, only calling a parent function, so this logic is not included."""
        return super().set_params(params)

```

Save this file to the `./lib/taosanalytics/algo/fc/` directory and restart the `taosanode` service. In the TDengine CLI, run `SHOW ANODES FULL` to see your new algorithm. Your applications can now use this algorithm via SQL.

```SQL
--- Detect anomalies in the `col` column using the newly added `myfc` algorithm
SELECT  _flow, _fhigh, _frowts, FORECAST(col_name, "algo=myfc")
FROM foo;
```

If you have never started the anode, see [Operations & Maintenance](../../../management/) to add the anode to your TDengine cluster.

### Unit Testing

You can add unit test cases to the `forecase_test.py` file in the `taosanalytics/test` directory or create a file for unit tests. Unit tests have a dependency on the Python unittest module.

```python
def test_myfc(self):
    """ Test the myfc class """
    s = loader.get_service("myfc")

    # Configure data for forecasting
    s.set_input_list(self.get_input_list(), None)
    # Check whether all results are 1
    r = s.set_params(
        {"fc_rows": 10, "start_ts": 171000000, "time_step": 86400 * 30, "start_p": 0}
    )
    r = s.execute()

    expected_list = [1] * 10
    self.assertEqlist(r["res"][0], expected_list)
```
