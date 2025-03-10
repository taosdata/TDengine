# encoding:utf-8
# pylint: disable=c0103
"""main service module"""
from abc import abstractmethod, ABC


class AnalyticsService:
    """ Analytics root class with only one method"""

    @abstractmethod
    def execute(self):
        """ the main execute method to perform fc or anomaly detection """

    def get_desc(self) -> str:
        """algorithm description"""
        return ""

    def get_params(self) -> dict:
        """return exist params """
        return {}


class AbstractAnalyticsService(AnalyticsService, ABC):
    """ abstract base analytics service class definition"""
    name = ''
    desc = ''

    def __init__(self):
        self.list = None
        self.ts_list = None

    def set_input_list(self, input_list: list, input_ts_list: list = None):
        """ set the input list """
        self.list = input_list
        self.ts_list = input_ts_list

    def set_params(self, params: dict) -> None:
        """set the parameters for current algo """
        if params is None:
            return

        if not isinstance(params, dict):
            raise ValueError('invalid parameter type, only dict allowed')

    def get_desc(self) -> str:
        return self.desc


class AbstractAnomalyDetectionService(AbstractAnalyticsService, ABC):
    """ abstract anomaly detection service, all anomaly detection algorithm class should be
     inherent from this class"""

    def __init__(self):
        super().__init__()
        self.type = "anomaly-detection"

    def input_is_empty(self):
        """ check if the input list is empty or None """
        return (self.list is None) or (len(self.list) == 0)


class AbstractForecastService(AbstractAnalyticsService, ABC):
    """abstract forecast service, all forecast algorithms class should be inherent from
    this base class"""

    def __init__(self):
        super().__init__()
        self.type = "forecast"

        self.period = 0
        self.start_ts = 0
        self.time_step = 0
        self.fc_rows = 0

        self.return_conf = 1
        self.conf = 0.05

    def set_params(self, params: dict) -> None:
        if not {'start_ts', 'time_step', 'fc_rows'}.issubset(params.keys()):
            raise ValueError('params are missing, start_ts, time_step, fc_rows are all required')

        self.start_ts = int(params['start_ts'])

        self.time_step = int(params['time_step'])

        if self.time_step <= 0:
            raise ValueError('time_step should be greater than 0')

        self.fc_rows = int(params['fc_rows'])

        if self.fc_rows <= 0:
            raise ValueError('fc rows is not specified yet')

        self.period = int(params['period']) if 'period' in params else 0
        if self.period < 0:
            raise ValueError("periods should be greater than 0")

        self.conf = float(params['conf']) if 'conf' in params else 95

        self.conf = 1.0 - self.conf / 100.0
        if self.conf < 0 or self.conf >= 1.0:
            raise ValueError("invalid value of conf, should between 0 and 100")

        self.return_conf = int(params['return_conf']) if 'return_conf' in params else 1

    def get_params(self):
        return {
            "period": self.period, "start": self.start_ts, "every": self.time_step,
            "forecast_rows": self.fc_rows, "return_conf": self.return_conf, "conf": self.conf
        }
