"""DynamicClassificationService: a classification service driven by a parameter config file."""

from taosanalytics.algo.dynamic.classifier import (
    DecisionTreeClassifier,
    LogisticRegressionClassifier,
)
from taosanalytics.base import AbstractClassificationService
from taosanalytics.log import AppLogger

_CLASSIFIER_CLASSES = {
    "logistic_regression": LogisticRegressionClassifier,
    "decision_tree": DecisionTreeClassifier,
}


class DynamicClassificationService(AbstractClassificationService):
    """
    A simple dynamic classification service driven by a JSON config file.
    The classifier is constructed and executed when execute() is called.

    Currently supported algorithms: logisticregression, decisiontree.
    """

    def __init__(self, name: str, desc: str, algo: str, path: str):
        super().__init__()

        self.name = name
        self.desc = desc

        self.config_file_path = path
        self.algo = algo

    def execute(self):
        """Construct the classifier from the config file and run classification."""
        if not self.input_data:
            raise ValueError("input_data is required for classification")

        algo_name = self.algo.lower()
        AppLogger.info(
            "execute dynamic classification service:%s, algo:%s", self.name, algo_name
        )

        classifier_class = _CLASSIFIER_CLASSES.get(algo_name)
        if classifier_class is None:
            raise ValueError(
                f"unsupported algorithm '{algo_name}' in dynamic classification service"
            )

        classifier = classifier_class(
            path=self.config_file_path,
            input_data=self.input_data,
            schema=self.schema,
        )

        result = classifier.predict()
        expected_size = len(self.input_data)

        if result is None:
            raise ValueError("dynamic classification classifier returned no result")

        if len(result) != expected_size:
            raise ValueError(
                "dynamic classification classifier returned %d predictions for %d input samples"
                % (len(result), expected_size)
            )

        return result
