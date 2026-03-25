import logging
import os
from pathlib import Path

from taosanalytics.conf import Configure


class AppLogger():
    """ system log_inst class (singleton) """
    _LOG_STR_FORMAT = '%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'

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
    def get_instance(cls) -> logging.Logger:
        """return the singleton instance"""
        if cls._instance is None:
            cls()

        return cls._instance.log_inst

    @classmethod
    def set_handler(cls, file_path: str):
        """ set the log_inst handler """
        path = Path(file_path)

        # create directory if not exists
        if not os.path.exists(path.parent):
            os.mkdir(path.parent)

        handler = logging.FileHandler(file_path)
        handler.setFormatter(logging.Formatter(cls._LOG_STR_FORMAT))

        cls.get_instance().addHandler(handler)

    @classmethod
    def set_log_level(cls, log_level):
        """adjust log level"""
        try:
            logger = cls.get_instance()
            logger.setLevel(log_level)
            logger.info(f"set log level:{log_level}")
        except ValueError as e:
            cls.get_instance().error(f"failed to set log level: {log_level}, {e}")


def setup_log_info(name: str):
    """ prepare the log info for unit test """
    _GIT_HUB_HOST = "/home/runner/work/TDengine/TDengine/tools/tdgpt/log/"

    base_dir = _GIT_HUB_HOST if os.environ.get('GITHUB_ACTIONS') else Configure.get_instance().get_log_dir()

    log_file = os.path.join(base_dir, name)
    AppLogger.set_handler(log_file)

    try:
        AppLogger.set_log_level(logging.DEBUG)
    except ValueError as e:
        print(f"set log level failed:{e}")
