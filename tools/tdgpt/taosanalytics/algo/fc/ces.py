# encoding:utf-8
# pylint: disable=c0103
"""complex exponential smoothing definition"""

from statsforecast.models import AutoCES

from taosanalytics.base import AbstractStatsForecastService


class _CesService(AbstractStatsForecastService):
    """Forecast time-series data using CES."""
    name = "ces"
    desc = "forecast algorithm by using complex exponential smoothing"
    _builtins = True
    model_info = "CES"

    def __init__(self):
        super().__init__()

        self.model = "Z"

    def set_params(self, params: dict) -> None:
        super().set_params(params)

        self.model = str(params.get("model", "Z")).upper()
        if self.model not in ("N", "S", "P", "F", "Z"):
            raise ValueError("model parameter can only be 'N', 'S', 'P', 'F' or 'Z'")

    def get_params(self) -> dict:
        p = super().get_params()
        p.update({"model": self.model})
        return p

    def _fit_model(self, values):
        """Fit and return the configured CES model."""
        season_length = self.period if self.period > 0 else 1
        model = AutoCES(season_length=season_length, model=self.model)
        return model.fit(values)
