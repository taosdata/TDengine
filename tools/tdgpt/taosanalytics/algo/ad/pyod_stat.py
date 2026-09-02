"""PyOD statistical anomaly detection services."""

try:
    from pyod.models.copod import COPOD
    from pyod.models.ecod import ECOD
    from pyod.models.hbos import HBOS
    from pyod.models.iforest import IForest
    from pyod.models.pca import PCA
except ImportError:
    COPOD = None
    ECOD = None
    HBOS = None
    IForest = None
    PCA = None

from taosanalytics.algo.tool.pyod_util import (
    fit_predict,
    validate_bool_int,
    validate_contamination,
    validate_int_min,
    validate_nonneg_int,
    validate_positive_float,
    validate_positive_int,
)
from taosanalytics.base import AbstractAnomalyDetectionService

if ECOD is not None:

    class _EcodService(AbstractAnomalyDetectionService):
        """ECOD algorithm is to check anomaly data in the input list."""

        name = "ecod"
        desc = """Empirical-Cumulative-distribution-based Outlier Detection from PyOD."""
        _builtins = True

        def __init__(self, contamination=0.1):
            super().__init__()
            self.contamination = contamination

        def execute(self):
            """Perform ECOD anomaly detection."""
            if self.input_is_empty():
                return []

            detector = ECOD(contamination=self.contamination)
            return fit_predict(detector, self.input_data_lists, self.valid_code)

        def set_params(self, params):
            super().set_params(params)

            if "contamination" in params:
                self.contamination = validate_contamination(params["contamination"])

        def get_params(self):
            return {"contamination": self.contamination}


if HBOS is not None:

    class _HbosService(AbstractAnomalyDetectionService):
        """HBOS algorithm is to check anomaly data in the input list."""

        name = "hbos"
        desc = """Histogram-based Outlier Score from PyOD."""
        _builtins = True

        def __init__(self, contamination=0.1, n_bins=10):
            super().__init__()
            self.contamination = contamination
            self.n_bins = n_bins

        def execute(self):
            """Perform HBOS anomaly detection."""
            if self.input_is_empty():
                return []

            detector = HBOS(contamination=self.contamination, n_bins=self.n_bins)
            return fit_predict(detector, self.input_data_lists, self.valid_code)

        def set_params(self, params):
            super().set_params(params)

            if "contamination" in params:
                self.contamination = validate_contamination(params["contamination"])

            if "n_bins" in params:
                self.n_bins = validate_int_min(params["n_bins"], "n_bins", 2)

        def get_params(self):
            return {"contamination": self.contamination, "n_bins": self.n_bins}


if COPOD is not None:

    class _CopodService(AbstractAnomalyDetectionService):
        """COPOD algorithm is to check anomaly data in the input list."""

        name = "copod"
        desc = """Copula-Based Outlier Detection from PyOD."""
        _builtins = True

        def __init__(self, contamination=0.1):
            super().__init__()
            self.contamination = contamination

        def execute(self):
            """Perform COPOD anomaly detection."""
            if self.input_is_empty():
                return []

            detector = COPOD(contamination=self.contamination)
            return fit_predict(detector, self.input_data_lists, self.valid_code)

        def set_params(self, params):
            super().set_params(params)

            if "contamination" in params:
                self.contamination = validate_contamination(params["contamination"])

        def get_params(self):
            return {"contamination": self.contamination}


if IForest is not None:

    class _IForestService(AbstractAnomalyDetectionService):
        """IForest algorithm is to check anomaly data in the input list."""

        name = "iforest"
        desc = """Isolation Forest anomaly detection from PyOD."""
        _builtins = True

        def __init__(self, contamination=0.1, n_estimators=100, random_state=42):
            super().__init__()
            self.contamination = contamination
            self.n_estimators = n_estimators
            self.random_state = random_state

        def execute(self):
            """Perform IForest anomaly detection."""
            if self.input_is_empty():
                return []

            detector = IForest(
                contamination=self.contamination,
                n_estimators=self.n_estimators,
                random_state=self.random_state,
            )
            return fit_predict(detector, self.input_data_lists, self.valid_code)

        def set_params(self, params):
            super().set_params(params)

            if "contamination" in params:
                self.contamination = validate_contamination(params["contamination"])

            if "n_estimators" in params:
                self.n_estimators = validate_positive_int(params["n_estimators"], "n_estimators")

            if "random_state" in params:
                self.random_state = validate_nonneg_int(params["random_state"], "random_state")

        def get_params(self):
            return {
                "contamination": self.contamination,
                "n_estimators": self.n_estimators,
                "random_state": self.random_state,
            }


if PCA is not None:

    class _PcaService(AbstractAnomalyDetectionService):
        """PCA algorithm is to check anomaly data in the input list."""

        name = "pca"
        desc = """Principal Component Analysis anomaly detection from PyOD."""
        _builtins = True

        def __init__(
            self, contamination=0.1, n_components=None, standardization=True
        ):
            super().__init__()
            self.contamination = contamination
            self.n_components = n_components
            self.standardization = standardization

        def execute(self):
            """Perform PCA anomaly detection."""
            if self.input_is_empty():
                return []

            detector = PCA(
                contamination=self.contamination,
                n_components=self.n_components,
                standardization=self.standardization,
            )
            return fit_predict(detector, self.input_data_lists, self.valid_code)

        def set_params(self, params):
            super().set_params(params)

            if "contamination" in params:
                self.contamination = validate_contamination(params["contamination"])

            if "n_components" in params:
                # Accepts a positive integer (exact component count) or a float in (0, 1)
                # (variance ratio to retain, e.g. 0.95 keeps 95% of variance — PyOD convention).
                n_components = validate_positive_float(params["n_components"], "n_components")
                if n_components >= 1 and not float(n_components).is_integer():
                    raise ValueError(
                        "n_components must be a positive integer or a float in (0, 1) for variance ratio"
                    )
                if float(n_components).is_integer():
                    n_components = int(n_components)
                self.n_components = n_components

            if "standardization" in params:
                self.standardization = validate_bool_int(params["standardization"], "standardization")

        def get_params(self):
            return {
                "contamination": self.contamination,
                "n_components": self.n_components,
                "standardization": int(self.standardization),
            }
