# encoding:utf-8
# pylint: disable=c0103
"""configuration model definition"""
import importlib.util
import logging
import platform
import os.path
from pathlib import Path
from typing import Optional

os.environ.setdefault('KERAS_BACKEND', 'torch')
try:
    import torch  # noqa: F401 - Optional runtime dependency
except Exception:  # pragma: no cover
    torch = None  # noqa: F841

try:
    import keras  # noqa: F401 - Optional runtime dependency
except Exception:  # pragma: no cover
    keras = None  # noqa: F841

_ANODE_SECTION_NAME = "taosanode"


class Configure:
    """ configuration class (singleton) """

    _instance = None
    _lock = __import__('threading').Lock()

    def __new__(cls, conf_path: Optional[str] = None):
        with cls._lock:
            if cls._instance is None:
                instance = super().__new__(cls)
                instance._conf = instance._get_default_conf()
                instance.path = None

                if conf_path is not None and os.path.exists(conf_path):
                    instance.path = conf_path
                else:
                    instance.path = instance._conf['conf_path']
                    if conf_path is not None:
                        print(f"Input configuration file not available. Use default config file: {instance.path}")

                if os.path.exists(instance.path):
                    instance.reload()
                else:
                    print(
                        f"Configuration file {instance.path} is not available, start by using minimum config variables")

                cls._instance = instance
        return cls._instance

    @classmethod
    def init(cls, conf_path: Optional[str] = None) -> 'Configure':
        """Initialize the singleton with an explicit config path. Must be called before get_instance()."""
        with cls._lock:
            cls._instance = None
        return cls(conf_path)

    @classmethod
    def get_instance(cls) -> 'Configure':
        """Return the singleton instance, creating it with defaults if not yet initialized."""
        if cls._instance is None:
            cls()
        return cls._instance

    # def _get_default_conf_windows(self):
    #     # raw_path = r"%PROGRAMDATA%"
    #     # base_path = os.path.join(os.path.expandvars(raw_path), "tdgpt")
    #     # keep inline with the TDengine installation configuration
    #
    #     base_path = "c:/TDengine/taosanode/"

    def _get_default_conf_impl(self, base_path: str):
        return {
            "log_dir": os.path.join(base_path, "log"),
            "log_file": "taosanode.app.log",
            "model_dir": os.path.join(base_path, "model"),
            "dynamic_model_dir": os.path.join(base_path, "model", "dynamic"),
            "img_dir": os.path.join(base_path, "img"),
            "conf_path": os.path.join(base_path, "cfg", "taosanode.config.py"),
            "log_level": logging.DEBUG,
            "draw_result": False,
            "host": "0.0.0.0",
            "port": 6035,
        }

    def _get_default_conf_linux(self):
        return {
            "log_dir": "/var/log/taos/taosanode/",
            "log_file": "taosanode.app.log",
            "model_dir": '/usr/local/taos/taosanode/model/',
            'dynamic_model_dir': '/usr/local/taos/taosanode/model/dynamic/',
            "img_dir":'/usr/local/taos/taosanode/img/',
            "conf_path": "/etc/taos/taosanode.config.py",
            "log_level": logging.DEBUG,
            "draw_result": False,
            "host": "0.0.0.0",
            "port": 6035,
        }

    def _get_default_conf(self) -> dict:
        package_dir = Path(__file__).resolve().parent

        if platform.system().lower() == "windows":
            if package_dir.name == "taosanalytics" and package_dir.parent.name == "lib":
                base_path = str(package_dir.parent.parent).replace("\\", "/")
            else:
                # default installation directory in windows
                base_path = "c:/TDengine/taosanode"

            default = self._get_default_conf_impl(base_path)
        else:
            if os.environ.get('GITHUB_ACTIONS') and package_dir.name == "taosanalytics":
                base_path = str(package_dir.parent).replace("\\", "/")
                default = self._get_default_conf_impl(base_path)
            else:
                default = self._get_default_conf_linux()

        return default

    def get_log_path(self) -> str:
        """ return log file full path """
        return os.path.join(str(self._conf['log_dir']), str(self._conf['log_file']))

    def get_log_dir(self) -> str:
        return self._conf["log_dir"]

    def get_log_level(self):
        """ return the log level specified by configuration file """
        return self._conf['log_level']

    def get_model_directory(self):
        """ return model directory """
        return self._conf['model_dir']

    def get_dynamic_model_directory(self):
        """ return dynamic model directory, which is a subdirectory under model directory """
        return os.path.join(self._conf['dynamic_model_dir'])

    def get_tsfm_service(self, service_name):
        return self._conf.get(service_name, None)

    def get_draw_result_option(self):
        """ get the option for draw results or not"""
        return self._conf['draw_result']

    def get_server_bind(self) -> tuple:
        """return (host, port) for the HTTP server"""
        return self._conf['host'], self._conf['port']

    def get_img_dir(self) -> str:
        return self._conf["img_dir"]

    def reload(self):
        """ load the info from config file """
        spec = importlib.util.spec_from_file_location("gunicorn_config", self.path)
        config_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config_module)

        conf_vars = {}
        for key in dir(config_module):
            if not key.startswith('__'):
                value = getattr(config_module, key)
                if not callable(value):  # exclude the Python functions
                    conf_vars[key] = value

        if 'app_log' in conf_vars:
            self._conf['log_dir'] = os.path.dirname(conf_vars['app_log'])
            self._conf['log_file'] = os.path.basename(conf_vars['app_log'])
            conf_vars.pop('app_log')

        if 'log_level' in conf_vars:
            log_flag = {
                'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'CRITICAL': logging.CRITICAL,
                'ERROR': logging.ERROR, 'WARN': logging.WARN
            }

            log_level = conf_vars['log_level'].upper()
            if log_level in log_flag:
                self._conf['log_level'] = log_flag[log_level]

            conf_vars.pop('log_level')

        if 'model_dir' in conf_vars:
            self._conf['model_dir'] = conf_vars['model_dir']
            conf_vars.pop('model_dir')

        if 'draw_result' in conf_vars:
            self._conf['draw_result'] = conf_vars['draw_result']
            conf_vars.pop('draw_result')

        if 'bind' in conf_vars:
            host, _, port = conf_vars['bind'].partition(':')
            if host:
                self._conf['host'] = host
            if port.isdigit():
                self._conf['port'] = int(port)
            conf_vars.pop('bind')

        self._conf.update(conf_vars)

        # Auto-derive model service URLs from models configuration.
        # This makes models[x]["port"] + models[x]["endpoint"] the single
        # source of truth; no need to maintain separate URL constants.
        models = self._conf.get('models')
        if models:
            for model_name, model_cfg in models.items():
                port = model_cfg.get('port')
                endpoint = model_cfg.get('endpoint', '/ds_predict')
                if port:
                    # Use algo_name as config key if specified (for SQL-visible names
                    # like tdtsfm_1, timemoe-fc), otherwise use the model key.
                    conf_key = model_cfg.get('algo_name', model_name)
                    self._conf[conf_key] = f'http://127.0.0.1:{port}{endpoint}'
