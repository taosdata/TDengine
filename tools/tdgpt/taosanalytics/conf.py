# encoding:utf-8
# pylint: disable=c0103
"""configuration model definition"""
import importlib.util
import logging
import platform
import os.path
import torch   # do not remove it
import keras
from pathlib import Path
from typing import Optional

_ANODE_SECTION_NAME = "taosanode"


class Configure:
    """ configuration class """
    def __init__(self, conf_path: Optional[str] = None):
        self._conf = self._get_default_conf()
        self.path = None

        if conf_path is not None and os.path.exists(conf_path):
            self.path = conf_path
        else:
            self.path = self._conf['conf_path']
            print(
                f"Input configuration file not available. Use default config file: {self.path}")

        if os.path.exists(self.path):
            self.reload()
        else:
            print(f"Configuration file {self.path} is not available, start by using minimum config variables")

    def _get_default_conf(self):
        if platform.system().lower() == "windows":
            # raw_path = r"%PROGRAMDATA%"
            # base_path = os.path.join(os.path.expandvars(raw_path), "tdgpt")
            # keep inline with the TDengine installation configuration
            base_path = "c:/TDengine/tdgpt/"

            default = {
                "log_dir": os.path.join(base_path, "log"),
                "log_file": "taosanode.app.log",
                "model_dir": os.path.join(base_path, "model"),
                "conf_path": os.path.join(base_path, "conf/taosanode.config.py"),
                "log_level": logging.DEBUG,
                "draw_result": False,
            }
        else:
            default = {
                "log_dir": "/var/log/taos/taosanode/",
                "log_file": "taosanode.app.log",
                "model_dir": '/usr/local/taos/taosanode/model/',
                "conf_path": "/etc/taos/taosanode.config.py",
                "log_level": logging.DEBUG,
                "draw_result": False,
            }

            if os.environ.get('GITHUB_ACTIONS'):
               default['log_dir'] = '/home/runner/work/TDengine/TDengine/tools/tdgpt/log/'

        return default

    def get_log_path(self) -> str:
        """ return log file full path """
        return os.path.join(self._conf['log_dir'], 'taosanode.app.log')

    def get_log_dir(self) -> str:
        return self._conf["log_dir"]

    def get_log_level(self):
        """ return the log level specified by configuration file """
        return self._conf['log_level']

    def get_model_directory(self):
        """ return model directory """
        return self._conf['model_dir']

    def get_tsfm_service(self, service_name):
        return self._conf.get(service_name, None)

    def get_draw_result_option(self):
        """ get the option for draw results or not"""
        return self._conf['draw_result']

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

        self._conf.update(conf_vars)


class AppLogger():
    """ system log_inst class (singleton) """
    LOG_STR_FORMAT = '%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'

    _instance = None
    _lock = __import__('threading').Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance.log_inst = logging.getLogger(__name__)
                cls._instance.log_inst.setLevel(logging.INFO)
        return cls._instance

    @classmethod
    def get_instance(cls) -> 'AppLogger':
        """return the singleton instance"""
        if cls._instance is None:
            cls()
        return cls._instance

    def set_handler(self, file_path: str):
        """ set the log_inst handler """
        path = Path(file_path)

        # create directory if not exists
        if not os.path.exists(path.parent):
            os.mkdir(path.parent)

        handler = logging.FileHandler(file_path)
        handler.setFormatter(logging.Formatter(self.LOG_STR_FORMAT))

        self.log_inst.addHandler(handler)

    def set_log_level(self, log_level):
        """adjust log level"""
        try:
            self.log_inst.setLevel(log_level)
            self.log_inst.info(f"set log level:{log_level}")
        except ValueError as e:
            self.log_inst.error(f"failed to set log level: {log_level}, {e}")


conf = Configure()


def setup_log_info(name: str):
    """ prepare the log info for unit test """
    base_dir = "/home/runner/work/TDengine/TDengine/tools/tdgpt/log/" if os.environ.get('GITHUB_ACTIONS') else conf.get_log_dir()
    log_file = os.path.join(base_dir, name)

    AppLogger.get_instance().set_handler(log_file)

    try:
        AppLogger.get_instance().set_log_level(logging.DEBUG)
    except ValueError as e:
        print(f"set log level failed:{e}")
