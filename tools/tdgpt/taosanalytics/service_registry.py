# encoding:utf-8
"""load and return the available services"""
import copy
import importlib
import inspect
import os
import sys
from collections import defaultdict
from taosanalytics.log import AppLogger
from taosanalytics.analytics_base import (
    AbstractAnomalyDetectionService,
    AbstractForecastService,
    AbstractImputationService,
    AbstractCorrelationService
)


class ServiceRegistry:
    """ Singleton register for multiple anomaly detection algorithms and fc algorithms"""

    def __init__(self):
        self.services = defaultdict(list)
        self._loaded = False

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
                    one = {"name": key, "desc": val[0].get_desc(), "params": val[0].get_params(),
                           "status": val[0].get_status()}
                    all_items.append(one)
                except AttributeError as e:
                    AppLogger.error("failed to get service: %s info, reason: %s", key, e)

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

    def register_all_services(self) -> None:
        """ register all algorithms/models in the specified directory"""

        if self._loaded:
            AppLogger.warning("already register all service abort from the register all procedure")
            return

        self._loaded = True

        def register_service(container, name: str, service):
            """ register service for both anomaly detection and fc """
            AppLogger.info("register service: %s", name)
            container[name].append(service)

        def _do_register(cur_directory, lib_prefix, sub_directory, required: bool = True):
            """ the implementation of load services """
            service_directory = os.path.join(cur_directory, sub_directory)

            if not os.path.exists(service_directory):
                AppLogger.fatal(
                    "service directory:%s does not exist, failed to load service",
                    service_directory)

                if required:
                    # fail fast if try to register the built-in service to diagnose the bug.
                    raise FileNotFoundError(f"service directory:{service_directory} does not exist")
                else:
                    # ignore the failure and continue in case of registering custom models
                    return

            all_files = os.listdir(service_directory)

            for item in all_files:
                if item in ('__init__.py', '__pycache__') or not item.endswith('py'):
                    continue

                full_path = os.path.join(service_directory, item)
                if os.path.isdir(full_path):
                    continue

                # do load algorithm
                name = lib_prefix + item.split('.')[0]

                try:
                    module = importlib.import_module(name)
                except Exception as e:
                    AppLogger.error("failed to import module %s: %s, skipping", name, str(e))
                    continue

                AppLogger.info("load algorithm:%s", name)

                for (class_name, _) in inspect.getmembers(module, inspect.isclass):

                    if class_name in (
                            AbstractAnomalyDetectionService.__name__,
                            AbstractForecastService.__name__,
                            AbstractImputationService.__name__,
                            AbstractCorrelationService.__name__
                    ) or (not class_name.startswith('_')):
                        continue

                    algo_cls = getattr(module, class_name)

                    if algo_cls is not None:
                        version = sys.version_info

                        # ignore the shesd for python 3.12 version due to pandas compatibility
                        if (version.major, version.minor) == (3, 12) and class_name == '_SHESDService':
                            AppLogger.info(
                                "%s not loaded due to Pandas compatibility problem on Python 3.12",
                                class_name)
                            continue

                        try:
                            obj = algo_cls()
                            register_service(self.services, algo_cls.name, obj)
                        except Exception as e:
                            AppLogger.error("failed to instantiate %s from %s: %s, skipping",
                                          class_name, name, str(e))
                            continue

        # start to load all services
        current_directory = os.path.dirname(os.path.abspath(__file__))

        _do_register(current_directory, 'taosanalytics.algo.ad.', 'algo/ad/', True)
        _do_register(current_directory, 'taosanalytics.algo.fc.', 'algo/fc/', True)
        _do_register(current_directory, 'taosanalytics.algo.imputat.', 'algo/imputat/', True)
        _do_register(current_directory, 'taosanalytics.algo.correl.', 'algo/correl/', True)

        # load user defined ML model-driven script.
        AppLogger.info("start to load custom defined models")

        _do_register(current_directory, 'taosanalytics.algo.custom.ad.', 'algo/custom/ad/', False)
        _do_register(current_directory, 'taosanalytics.algo.custom.fc.', 'algo/custom/fc/', False)


loader: ServiceRegistry = ServiceRegistry()
