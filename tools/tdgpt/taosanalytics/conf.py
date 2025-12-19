# encoding:utf-8
# pylint: disable=c0103
"""configuration model definition"""
import configparser
import importlib.util
import logging
import os.path
from pathlib import Path

_ANODE_SECTION_NAME = "taosanode"


class Configure:
    """ configuration class """

    def __init__(self, conf_path="/etc/taos/taosanode.config.py"):
        self.path = None

        self._log_path = 'taosanode.app.log'
        self._log_level = logging.INFO
        self._model_directory = '/var/lib/taos/taosanode/model/'
        self._draw_result = False
        self._all = {}

        self.reload(conf_path)

    def get_log_path(self) -> str:
        """ return log file full path """
        return self._log_path

    def get_log_level(self):
        """ return the log level specified by configuration file """
        return self._log_level

    def get_model_directory(self):
        """ return model directory """
        return self._model_directory

    def get_tsfm_service(self, service_name):
        return self._all.get(service_name, None)

    def get_draw_result_option(self):
        """ get the option for draw results or not"""
        return self._draw_result

    def reload(self, new_path: str):
        """ load the info from config file """
        self.path = new_path

        # 动态加载配置文件
        spec = importlib.util.spec_from_file_location("gunicorn_config", self.path)
        config_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config_module)

        # config_vars = {}
        for key in dir(config_module):
            if not key.startswith('__'):
                value = getattr(config_module, key)
                if not callable(value):  # 排除函数
                    self._all[key] = value

        self._log_path = self._all.get('app_log', '/var/log/taos/taosanode/taosanode.app.log')

        log_level = self._all.get('log_level', 'DEBUG')

        log_flag = {
            'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'CRITICAL': logging.CRITICAL,
            'ERROR': logging.ERROR, 'WARN': logging.WARN
        }

        if log_level.upper() in log_flag:
            self._log_level = log_flag[log_level.upper()]
        else:
            self._log_level = logging.INFO

        self._model_directory = self._all.get('model_dir', '/usr/local/taos/taosanode/model/')
        self._draw_result = self._all.get('draw_result', False)



class AppLogger():
    """ system log_inst class """
    LOG_STR_FORMAT = '%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'

    def __init__(self):
        self.log_inst = logging.getLogger(__name__)
        self.log_inst.setLevel(logging.INFO)

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
            self.log_inst.info("set log level:%d", log_level)
        except ValueError as e:
            self.log_inst.error("failed to set log level: %d, %s", log_level, str(e))


conf = Configure()
app_logger = AppLogger()


def setup_log_info(name: str):
    """ prepare the log info for unit test """
    app_logger.set_handler(name)

    try:
        app_logger.set_log_level(logging.DEBUG)
    except ValueError as e:
        print("set log level failed:%s", e)
