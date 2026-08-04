# pylint: disable=c0103
"""theta definition"""

import numpy as np
from statsforecast.models import Theta

from taosanalytics.base import AbstractStatsForecastService


class _ThetaService(AbstractStatsForecastService):
    """Forecast time-series data using the Theta method."""

    name = "theta"
    desc = "forecast algorithm by using the standard Theta method"
    _builtins = True
    model_info = "Theta"

    def __init__(self):
        super().__init__()

        self.decomposition_type = "multiplicative"

    def set_params(self, params: dict) -> None:
        super().set_params(params)

        self.decomposition_type = str(
            params.get("decomposition_type", "multiplicative")
        ).lower()
        if self.decomposition_type not in ("additive", "multiplicative"):
            raise ValueError(
                "decomposition_type parameter can only be 'additive' or 'multiplicative'"
            )

    def get_params(self) -> dict:
        p = super().get_params()
        p.update({"decomposition_type": self.decomposition_type})
        return p

    def _validate_input_values(self, values: np.ndarray) -> None:
        """Validate Theta-specific input constraints."""
        if self.decomposition_type == "multiplicative" and np.any(values <= 0):
            raise ValueError(
                "multiplicative decomposition requires strictly positive input data"
            )

    def _fit_model(self, values: np.ndarray):
        """Fit and return the configured Theta model."""
        season_length = self.period if self.period > 0 else 1
        model = Theta(
            season_length=season_length, decomposition_type=self.decomposition_type
        )
        return model.fit(values)
