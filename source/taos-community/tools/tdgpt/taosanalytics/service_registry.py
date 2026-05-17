# encoding:utf-8
"""load and return the available services"""
import copy
import importlib
import inspect
import json
import os
import sys
import threading
from collections import defaultdict
from pathlib import Path

from taosanalytics.conf import Configure
from taosanalytics.handlers.dynamic_forecast import DynamicForecastService
from taosanalytics.handlers.dynamic_anomaly import DynamicAnomalyService
from taosanalytics.exception import NotFoundDynamicModelError
from taosanalytics.log import AppLogger
from taosanalytics.base import (
    AbstractAnomalyDetectionService,
    AbstractForecastService,
    AbstractImputationService,
    AbstractCorrelationService
)


class ServiceRegistry:
    """ Singleton register for multiple anomaly detection algorithms and forecast algorithms"""

    _only_params_models = ['arima', 'prophet', 'theta']
    _only_params_anomaly_models = ['iforest']

    _base_class_name = [
        AbstractAnomalyDetectionService.__name__,
        AbstractForecastService.__name__,
        AbstractImputationService.__name__,
        AbstractCorrelationService.__name__
    ]

    def __init__(self):
        self.services = defaultdict()
        self._loaded = False
        self._services_lock = threading.RLock()

    def get_service(self, name):
        """ get the required service """
        with self._services_lock:
            self.sync_dynamic_services()
            return copy.copy(self.services.get(name, None))

    def sync_dynamic_services(self):
        """Synchronize dynamic services with the shared config directory."""
        with self._services_lock:
            dynamic_service_names = [
                name for name, service in self.services.items()
                if isinstance(service, (DynamicForecastService, DynamicAnomalyService))
            ]

            for name in dynamic_service_names:
                service = self.services.get(name)
                if service is None:
                    continue

                config_file_path = getattr(service, 'config_file_path', None)
                if config_file_path and not Path(config_file_path).exists():
                    del self.services[name]
                    AppLogger.info(
                        "remove dynamic model '%s' from memory because config file is gone",
                        name)

            dyn_dir = Path(Configure.get_instance().get_dynamic_model_directory())
            if not dyn_dir.exists() or not dyn_dir.is_dir():
                return

            for config_path in dyn_dir.iterdir():
                if not config_path.is_file() or config_path.suffix != '.json':
                    continue

                model_name = config_path.stem
                if model_name in self.services:
                    continue

                try:
                    self.register_service_from_file(str(config_path))
                except Exception as e:
                    AppLogger.error(
                        "failed to sync dynamic model from file %s: %s, skipping",
                        str(config_path),
                        str(e))

    def get_typed_services(self, type_str: str) -> list:
        """ get specified type service """
        all_items = []
        with self._services_lock:
            AppLogger.info("Fetching services of type: %s, total: %d", type_str, len(self.services))
            services_snapshot = list(self.services.items())

        for key, val in services_snapshot:
            if val.type == type_str:
                try:
                    one = {
                        "name": key,
                        "desc": val.get_desc(),
                        "params": val.get_params(),
                        "status": val.get_status(),
                        'builtins': val.is_builtins
                    }
                    all_items.append(one)
                except AttributeError as e:
                    AppLogger.error("failed to get service: %s info, reason: %s", key, e)

        return all_items

    def get_service_list(self):
        """ return all available service info """
        self.sync_dynamic_services()

        info = {
            "protocol": 1.0,
            "version": 0.1,
            "details": [
                self.get_forecast_algo_list(),
                self.get_anomaly_algo_list(),
                self.get_imputation_algo_list(),
                self.get_corr_algo_list()
            ]
        }

        return info

    def get_anomaly_algo_list(self):
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

    def register_service_from_file(self, config_file: str):
        """Load and register algorithms from a single json file.
        which is a algorithm parameter file that defines the algorithm name, description, parameters, and other metadata.

        config_file: a json format file that contains the algorithm definition,
        including the file path of the algorithm implementation and its metadata.

        """

        with self._services_lock:
            if not config_file.endswith('.json'):
                msg = f"not a json file: {config_file}"
                raise ValueError(msg)

            config_file_name = Path(config_file).name
            model_name = config_file_name.replace(os.sep, '.')[:-5]  # strip .json

            AppLogger.info("loading algorithm/model from file: %s (module: %s)", config_file, model_name)

            with open(config_file, 'r', encoding="utf-8") as f:
                file_content = f.read()
                try:
                    config = json.loads(file_content)
                except Exception as e:
                    msg = f"failed to load dynamic model configuration, file: {config_file}: {e}"
                    raise ValueError(msg) from e

            if not isinstance(config, dict):
                raise ValueError(f"invalid config format in file: {config_file}, expected a JSON object")

            if 'algo' in config:
                algo_name = config['algo']

                if algo_name.lower() in ServiceRegistry._only_params_models:
                    # only parameters model
                    # create a simple service object with the metadata, the actual implementation can be loaded later when execute.
                    serv = DynamicForecastService(
                        name=model_name,
                        desc=f"dynamic generated model training from {algo_name}",
                        algo=algo_name,
                        path=config_file,
                    )
                elif algo_name.lower() in ServiceRegistry._only_params_anomaly_models:
                    serv = DynamicAnomalyService(
                        name=model_name,
                        desc=f"dynamic generated anomaly detection from {algo_name}",
                        algo=algo_name,
                        path=config_file,
                    )
                else:
                    msg = f"unsupported algorithm '{algo_name}' in dynamic model configuration file: {config_file}"
                    raise ValueError(msg)

                if model_name in self.services:
                    raise RuntimeError(f"model with name '{model_name}' already exists, register failed")

                ServiceRegistry._register_service(self.services, model_name, serv)
                AppLogger.info("register dynamic model:'%s' from %s, total:%d", model_name, config_file,
                                len(self.services))
            else:
                msg = f"failed to register dynamic service, missing 'algo' field in dynamic model configuration file: {config_file}"
                raise ValueError(msg)

    def unregister_dynamic_service(self, name: str):
        """ unregister the dynamic service, e.g. when the model is deleted or updated."""
        with self._services_lock:
            if name in self.services:
                if self.services[name].is_builtins:
                    raise RuntimeError(f"try to unregister built-in model:'{name}', operation not allowed")

                if not isinstance(self.services[name], (DynamicForecastService, DynamicAnomalyService)):
                    raise RuntimeError(f"try to unregister non-dynamic model:'{name}', operation not allowed")

                del self.services[name]
                AppLogger.info("unregister dynamic model:'%s'", name)
            else:
                raise NotFoundDynamicModelError(f"try to unregister non-existing model:'{name}', operation failed")

    @staticmethod
    def _register_service(container, name: str, service):
        """ register service for all kinds """
        if name in container:
            AppLogger.error("service with name '%s' already exists", name)
            raise RuntimeError(f"service with name '{name}' already exists, failed to register service")

        container[name] = service

    def _register_services_in_dir(self, cur_directory, lib_prefix, sub_directory, required: bool = True):
        """ the implementation of load services """
        service_directory = str(os.path.join(cur_directory, sub_directory))

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
            if item in ('__init__.py', '__pycache__') or not item.endswith('.py'):
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

                if class_name in ServiceRegistry._base_class_name or (not class_name.startswith('_')):
                    continue

                algo_cls = getattr(module, class_name)

                if algo_cls.__module__ != module.__name__:
                    AppLogger.warning("class %s in module %s is not defined in the module, skipping",
                                      class_name, name)
                    continue

                if algo_cls.__module__  != name:
                    AppLogger.warning("class %s in module %s is imported from another module %s, skipping",
                                      class_name, name, algo_cls.__module__)
                    continue

                if algo_cls is not None:
                    version = sys.version_info

                    # ignore the shesd for python 3.12 version due to the pandas version compatibility issue
                    if (version.major, version.minor) == (3, 12) and class_name == '_SHESDService':
                        AppLogger.info(
                            "%s not loaded due to Pandas compatibility problem on Python 3.12",
                            class_name)
                        continue

                    try:
                        obj = algo_cls()
                        ServiceRegistry._register_service(self.services, algo_cls.name, obj)
                    except Exception as e:
                        AppLogger.error("failed to register service:%s from %s: %s, skipping",
                                        class_name, name, str(e))
                        continue

    def register_all_services(self) -> None:
        """ register all algorithms/models in the specified directory"""
        if self._loaded:
            AppLogger.warning("already register all service abort from the register all procedure")
            return

        # start to load all services
        current_directory = os.path.dirname(os.path.abspath(__file__))

        self._register_services_in_dir(current_directory, 'taosanalytics.algo.ad.', 'algo/ad/', True)
        self._register_services_in_dir(current_directory, 'taosanalytics.algo.fc.', 'algo/fc/', True)
        self._register_services_in_dir(current_directory, 'taosanalytics.algo.imputat.', 'algo/imputat/', True)
        self._register_services_in_dir(current_directory, 'taosanalytics.algo.correl.', 'algo/correl/', True)

        # load user defined ML model-driven script.
        AppLogger.info("start to load custom defined models")

        self._register_services_in_dir(current_directory, 'taosanalytics.algo.custom.ad.', 'algo/custom/ad/', False)
        self._register_services_in_dir(current_directory, 'taosanalytics.algo.custom.fc.', 'algo/custom/fc/', False)

        AppLogger.info("start to load custom defined models from config files")

        dyn_dir = Configure.get_instance().get_dynamic_model_directory()

        if Path(dyn_dir).exists() and os.path.isdir(dyn_dir):
            for item in os.listdir(dyn_dir):
                if item.endswith('.json'):
                    try:
                        self.register_service_from_file(os.path.join(dyn_dir, item))
                    except Exception as e:
                        AppLogger.error("failed to register dynamic model from file %s: %s, skipping", item, str(e))
                        continue
        else:
            AppLogger.debug("dynamic model directory '%s' does not exist or is not a directory, "
                            "skipping loading dynamic models from config files", dyn_dir)

        # mark the register procedure.
        self._loaded = True


loader: ServiceRegistry = ServiceRegistry()
