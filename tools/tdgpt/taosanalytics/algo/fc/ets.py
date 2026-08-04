# pylint: disable=c0103
"""error, trend, seasonal definition"""

import numpy as np
from statsforecast.models import AutoETS

from taosanalytics.base import AbstractStatsForecastService


class _EtsService(AbstractStatsForecastService):
    """Forecast time-series data using ETS."""

    name = "ets"
    desc = "forecast algorithm by using error, trend and seasonal models"
    _builtins = True
    model_info = "ETS"

    def __init__(self):
        super().__init__()

        self.model = "ZZZ"
        self.damped = None

    def set_params(self, params: dict) -> None:
        super().set_params(params)

        self.model = str(params.get("model", "ZZZ")).upper()
        if (
            len(self.model) != 3
            or self.model[0] not in ("A", "M", "Z")
            or self.model[1] not in ("N", "A", "M", "Z")
            or self.model[2] not in ("N", "A", "M", "Z")
        ):
            raise ValueError("model parameter is not a valid ETS model")

        self.damped = None
        if "damped" in params:
            damped = str(params["damped"]).lower()
            if damped not in ("0", "1", "false", "true"):
                raise ValueError("damped parameter can only be 0 or 1")
            self.damped = damped in ("1", "true")

    def get_params(self) -> dict:
        p = super().get_params()
        p.update({"model": self.model, "damped": self.damped})
        return p

    def _validate_input_values(self, values: np.ndarray) -> None:
        """Validate ETS-specific input constraints."""
        if "M" in self.model and np.any(values <= 0):
            raise ValueError(
                "multiplicative ETS components require strictly positive input data"
            )

    def _fit_model(self, values: np.ndarray):
        """Fit and return the configured ETS model."""
        season_length = self.period if self.period > 0 else 1
        model = AutoETS(
            season_length=season_length, model=self.model, damped=self.damped
        )
        return model.fit(values)
