# encoding:utf-8
"""load and return the available services"""
import copy
import importlib
import inspect
import os
from collections import defaultdict
from taosanalytics.conf import app_logger
from taosanalytics.service import AbstractAnomalyDetectionService, AbstractForecastService, AbstractImputationService

os.environ['KERAS_BACKEND'] = 'torch'


class AnalyticsServiceLoader:
    """ Singleton register for multiple anomaly detection algorithms and fc algorithms"""

    def __init__(self):
        self.services = defaultdict(list)

    def get_service(self, name):
        """ get the required service """
        serv = self.services.get(name, [])[0] if self.services.get(name) else None
        return copy.copy(serv)

    def get_typed_services(self, type_str: str) -> list:
        """ get specified type service """
        all_items = []
        for key, val in self.services.items():
            if val[0].type == type_str:
                try:
                    one = {"name": key, "desc": val[0].get_desc(), "params": val[0].get_params()}
                    all_items.append(one)
                except AttributeError as e:
                    app_logger.log_inst.error("failed to get service: %s info, reason: %s", key, e);

        return all_items

    def get_service_list(self):
        """ return all available service info """
        info = {
            "protocol": 1.0,
            "version": 0.1,
            "details": [
                self.get_forecast_algo_list(),
                self.get_anomaly_detection_algo_list(),
                self.get_imputation_algo_list(),
                self.get_corr_algo_list()
            ]
        }

        return info

    def get_anomaly_detection_algo_list(self):
        """ get all available service list """
        return {
            "type": "anomaly-detection",
            "algo": self.get_typed_services("anomaly-detection")
        }

    def get_imputation_algo_list(self):
        """ get all available service list """
        return {
            "type": "imputation",
            "algo": self.get_typed_services("imputation")
        }

    def get_forecast_algo_list(self):
        """ get all available service list """
        return {
            "type": "forecast",
            "algo": self.get_typed_services("forecast")
        }

    def get_corr_algo_list(self):
        return {
            "type": "correlation",
            "algo": self.get_typed_services("correlation")
        }

    def load_all_service(self) -> None:
        """ load all algorithms in the specified directory"""

        def register_service(container, name: str, service):
            """ register service for both anomaly detection and fc """
            app_logger.log_inst.info("register service: %s", name)
            container[name].append(service)

        def do_load_service(cur_directory, lib_prefix, sub_directory):
            """ the implementation of load services """
            service_directory = cur_directory + sub_directory

            if not os.path.exists(service_directory):
                app_logger.log_inst.fatal(
                    "service directory:%s not lib exists, failed to load service",
                    service_directory)
                raise FileNotFoundError(f"service directory:{service_directory} not found")

            all_files = os.listdir(service_directory)

            for item in all_files:
                if item in ('__init__.py', '__pycache__') or not item.endswith('py'):
                    continue

                full_path = os.path.join(service_directory, item)
                if os.path.isdir(full_path):
                    continue

                # do load algorithm
                name = lib_prefix + item.split('.')[0]
                module = importlib.import_module(name)

                app_logger.log_inst.info("load algorithm:%s", name)

                for (class_name, _) in inspect.getmembers(module, inspect.isclass):

                    if class_name in (
                            AbstractAnomalyDetectionService.__name__,
                            AbstractForecastService.__name__,
                            AbstractImputationService.__name__
                    ) or (not class_name.startswith('_')):
                        continue

                    algo_cls = getattr(module, class_name)

                    if algo_cls is not None:
                        obj = algo_cls()
                        register_service(self.services, algo_cls.name, obj)

        # start to load all services
        current_directory = os.path.dirname(os.path.abspath(__file__))

        do_load_service(current_directory, 'taosanalytics.algo.ad.', '/algo/ad/')
        do_load_service(current_directory, 'taosanalytics.algo.fc.', '/algo/fc/')
        do_load_service(current_directory, 'taosanalytics.algo.imputat.', '/algo/imputat/')
        do_load_service(current_directory, 'taosanalytics.algo.correl.', '/algo/correl/')


loader: AnalyticsServiceLoader = AnalyticsServiceLoader()
