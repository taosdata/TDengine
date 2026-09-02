# pylint: disable=c0103
"""main service module"""

import datetime
from abc import ABC, abstractmethod

import numpy as np


class AnalyticsService(ABC):
    """Analytics root class with only one method"""

    READY = 0x01
    UNAVAILABLE = 0x02

    _toStatusName = {READY: "READY", UNAVAILABLE: "UNAVAIL"}

    @abstractmethod
    def execute(self):
        """the main execute method to perform forecast or anomaly detection"""

    def get_desc(self) -> str:
        """algorithm description"""
        return ""

    def get_params(self) -> dict:
        """return exist params"""
        return {}

    def get_status(self) -> str:
        """return model status"""
        return AnalyticsService._toStatusName[AnalyticsService.READY]


class AbstractAnalyticsService(AnalyticsService, ABC):
    """abstract base analytics service class definition"""

    name = ""
    desc = ""
    status = ""
    _builtins = False

    def __init__(self):
        self.list = None
        self.ts_list = None

    def set_input_list(self, input_list: list, input_ts_list: list = None):
        """set the input list"""
        self.list = input_list
        self.ts_list = input_ts_list

    def set_params(self, params: dict) -> None:
        """set the parameters for current algo"""
        if params is None:
            return

        if not isinstance(params, dict):
            raise ValueError("invalid parameter type, only dict allowed")

    def get_desc(self) -> str:
        return self.desc

    @property
    def is_builtins(self) -> bool:
        return self._builtins


class AbstractAnomalyDetectionService(AbstractAnalyticsService, ABC):
    """abstract anomaly detection service, all anomaly detection algorithm class should
    inherit from this class"""

    def __init__(self):
        self.valid_code = 1
        super().__init__()
        self.type = "anomaly-detection"
        self.input_data_lists = []

    def input_is_empty(self):
        """check if the input list is empty or None"""
        return (self.list is None) or (len(self.list) == 0)

    def set_params(self, params: dict) -> None:
        super().set_params(params)

        if "valid_code" in params:
            self.valid_code = int(params["valid_code"])

    def set_input_list(self, input_list: list, input_ts_list: list = None):
        """set the input list"""
        self.ts_list = input_ts_list

        # let's check if the input list is 1-dimensional or 2-dimensional
        if input_list is not None and len(input_list) > 0:
            if isinstance(input_list[0], list):

                # check for the length of all items in the list
                list_len = len(input_list[0])
                if not all(len(x) == list_len for x in input_list):
                    raise ValueError(
                        "multiple dimensions of data for anomaly detection are not equal"
                    )

                self.input_data_lists = input_list
                self.list = input_list[
                    0
                ]  # keep the first element of the self.input_data_lists
            else:
                self.list = input_list
                self.input_data_lists = [input_list]


class AbstractForecastService(AbstractAnalyticsService, ABC):
    """
    abstract forecast service, all forecast algorithms class should be inherent from
    this base class
    """

    def __init__(self):
        super().__init__()
        self.type = "forecast"

        self.period = 0
        self.start_ts = 0
        self.time_step = 0
        self.rows = 0

        self.return_conf = 1
        self.conf = 0.95
        self.precision = "ms"

        # get the local timezone info, which will be used for forecast result ts list generation
        self.tz = datetime.datetime.now().astimezone().tzinfo

        self.past_dynamic_real = []
        self.dynamic_real = []

    def set_input_data(
        self,
        input_list: list,
        input_ts_list: list = None,
        past_dynamic_real_list: list = None,
        dynamic_real_list: list = None,
    ):
        """set the input data"""
        if past_dynamic_real_list is not None:
            self.past_dynamic_real = past_dynamic_real_list

        if dynamic_real_list is not None:
            self.dynamic_real = dynamic_real_list

        self.set_input_list(input_list, input_ts_list)

    def set_params(self, params: dict) -> None:
        if not {"start_ts", "time_step", "rows"}.issubset(params.keys()):
            raise ValueError(
                "params are missing, start_ts, time_step, rows are all required"
            )

        self.start_ts = int(params["start_ts"])

        self.time_step = int(params["time_step"])

        if self.time_step <= 0:
            raise ValueError("time_step should be greater than 0")

        self.rows = int(params["rows"])

        if self.rows <= 0:
            raise ValueError("forecast rows is not specified yet")

        self.period = int(params["period"]) if "period" in params else 0
        if self.period < 0:
            raise ValueError("periods should be greater than 0")

        self.conf = float(params["conf"]) if "conf" in params else 0.95

        if self.conf < 0 or self.conf >= 1.0:
            raise ValueError("invalid value of conf, should between 0 and 1.0")

        self.return_conf = int(params["return_conf"]) if "return_conf" in params else 1
        self.precision = params.get("precision", "ms")

        if "tz" in params:
            self.tz = params["tz"]

    def get_params(self):
        return {
            "period": self.period,
            "start": self.start_ts,
            "every": self.time_step,
            "forecast_rows": self.rows,
            "return_conf": self.return_conf,
            "conf": self.conf,
            "tz": str(self.tz),
            "precision": self.precision,
        }


class AbstractStatsForecastService(AbstractForecastService, ABC):
    """Base service for forecasting models provided by StatsForecast."""

    model_info = ""

    def _validate_input_values(self, values: np.ndarray) -> None:
        """Validate model-specific input constraints."""

    @abstractmethod
    def _fit_model(self, values: np.ndarray):
        """Fit and return a StatsForecast model."""

    def _format_forecast_result(self, fitted_model, values: np.ndarray) -> tuple:
        """Build forecasts, confidence intervals, and fitted MSE."""
        level = self.conf * 100 if self.return_conf else None
        if level is not None and float(level).is_integer():
            level = int(level)

        forecast_res = fitted_model.predict(
            h=self.rows, level=[level] if level is not None else None
        )

        forecast = forecast_res["mean"].tolist()
        if self.return_conf:
            result = [
                forecast,
                forecast_res[f"lo-{level}"].tolist(),
                forecast_res[f"hi-{level}"].tolist(),
            ]
        else:
            result = [forecast]

        fitted = fitted_model.predict_in_sample()["fitted"]
        mse = float(np.nanmean((values - fitted) ** 2))
        return result, mse

    def execute(self) -> dict:
        """Forecast the configured number of time-series values."""
        min_rows = max(2, self.period * 2)
        if self.list is None or len(self.list) < min_rows:
            raise ValueError("number of input data is less than the required periods")

        if self.rows <= 0:
            raise ValueError("fc rows is not specified yet")

        values = np.asarray(self.list, dtype=float).copy()
        if not np.isfinite(values).all():
            raise ValueError("input data contains NaN or infinite values")

        self._validate_input_values(values)
        fitted_model = self._fit_model(values)
        result, mse = self._format_forecast_result(fitted_model, values)

        timestamps = [
            self.start_ts + index * self.time_step for index in range(self.rows)
        ]
        result.insert(0, timestamps)

        return {"mse": mse, "model_info": self.model_info, "res": result}


class AbstractImputationService(AbstractAnalyticsService, ABC):
    """
    abstract imputation service, all imputation algorithms class should be inherent from
    this base class
    """

    def __init__(self):
        super().__init__()
        self.type = "imputation"

    def set_input_data(self, input_list: list, input_ts_list: list = None):
        """set the input data"""
        self.set_input_list(input_list, input_ts_list)

    def set_params(self, params: dict) -> None:
        pass

    def get_params(self):
        return {"dummy": "dummy"}


class AbstractCorrelationService(AbstractAnalyticsService, ABC):
    """abstract correlation analysis service"""

    def __init__(self):
        super().__init__()
        self.list1 = None
        self.type = "correlation"

    def set_second_input_data(self, input_list1):
        self.list1 = input_list1

    def set_params(self, params: dict) -> None:
        pass

    def get_params(self):
        return {"dummy": "dummy"}


class AbstractRegressionService(AbstractAnalyticsService, ABC):
    """
    Abstract regression service, all regression algorithms should inherit from this base class.

    Mirrors the structure of AbstractForecastService:
      - set_input_data()  ←→  set_input_data()
      - set_params()      ←→  set_params()
      - execute()         ←→  execute()

    Responsibilities:
      - Load and manage input feature data and labels
      - Execute regression analysis
      - Return predicted values as list[float]
    """

    def __init__(self):
        super().__init__()
        self.type = "regression"
        self.input_data = None  # Feature matrix: list of sample rows
        self.schema = None  # Column schema metadata

    def set_input_data(self, input_data: list, schema: list = None):
        """Set the input feature data for regression.

        Args:
            input_data: Feature matrix (list of sample rows, each row is a list of feature values)
            schema: Optional schema describing the columns (same format as forecast schema)
        """
        self.input_data = input_data
        self.schema = schema

    def set_params(self, params: dict) -> None:
        """Set regression parameters. Override in subclass if needed."""

    def get_params(self):
        return {"dummy": "dummy"}

    @abstractmethod
    def execute(self):
        """Execute regression and return predicted values.

        Returns:
            list[float]: Predicted values, one per input sample
        """


class AbstractClassificationService(AbstractAnalyticsService, ABC):
    """
    Abstract classification service, all classification algorithms should inherit from this base class.

    Mirrors the structure of AbstractRegressionService:
      - set_input_data()  ←→  set_input_data()
      - set_params()      ←→  set_params()
      - execute()         ←→  execute()

    Responsibilities:
      - Load and manage input feature data
      - Execute classification analysis
      - Return predicted class labels as list[int] or list[str]
    """

    def __init__(self):
        super().__init__()
        self.type = "classification"
        self.input_data = None  # Feature matrix: list of sample rows
        self.schema = None  # Column schema metadata

    def set_input_data(self, input_data: list, schema: list = None):
        """Set the input feature data for classification.

        Args:
            input_data: Feature matrix (list of sample rows, each row is a list of feature values)
            schema: Optional schema describing the columns
        """
        self.input_data = input_data
        self.schema = schema

    def set_params(self, params: dict) -> None:
        """Set classification parameters. Override in subclass if needed."""

    def get_params(self):
        return {"dummy": "dummy"}

    @abstractmethod
    def execute(self):
        """Execute classification and return predicted class labels.

        Returns:
            list[int] or list[str]: Predicted class labels, one per input sample
        """
