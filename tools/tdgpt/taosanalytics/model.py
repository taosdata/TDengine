# encoding:utf-8
# pylint: disable=c0103
import threading
import time
from datetime import datetime
from typing import Any

from taosanalytics.conf import app_logger

class ModelInfo:
    def __init__(self):
        self.name = ""
        self.path = ""
        self.load_time = None
        self.note = ""
        self.size = 0
        self.invoke_cls_name = None
        self.model = None
        self.elapsed_time = 0

    @classmethod
    def create_model_info(cls, model_name: str, path: str, time: datetime, note: str, el:int, service: str, model: Any):
        if model is None:
            app_logger.log_inst.error(f"empty model, create {model_name} model info failed")
            return None

        info = ModelInfo()
        info.name = model_name
        info.path = path
        info.load_time = time
        info.note = note
        info.invoke_cls_name = service
        info.elapsed_time = el
        info.model = model

        return info

    def __json__(self):
        return {
            "model_name": self.name,
            "path": self.path,
            "load_time": self.load_time.strftime('%Y-%m-%d %H:%M:%S'),
            "invoke_cls_name": self.invoke_cls_name,
            'note':self.note
        }

class ModelManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._models = {}
                cls._instance._model_locks = {}
            return cls._instance

    def load_model(self, model_name: str, model_path: str, model_loader, class_name:str):
        """load the model on demand"""
        if model_name not in self._models:
            with self._lock:
                if model_name not in self._models:
                    # prepare lock for model_name
                    self._model_locks[model_name] = threading.Lock()

            with self._model_locks[model_name]:
                # protect the load procedure
                if model_name not in self._models:
                    app_logger.log_inst.info("try to load module:%s", model_path)

                    model, model_desc = None, ''
                    elapsed = 0

                    try:
                        st = time.time()
                        model, model_desc = model_loader(model_path)
                        elapsed = (int) ((time.time() - st) * 1000)
                    except Exception as e:
                        app_logger.log_inst.error(
                            "failed to load model from disk: %s for %s model, code:%s, continue...",
                            model_path, model_name, str(e))

                    if model is not None:
                        app_logger.log_inst.info("%s load model %s in file: %s completed, elapsed time:%.2fs, total loaded models:%d",
                                                 class_name, model_name, model_path,
                                                 elapsed/1000.0, len(self._models) + 1)

                    # only lock the set procedure
                    with self._lock:
                        info = ModelInfo.create_model_info(model_name, model_path, datetime.now(), model_desc, elapsed, class_name, model)
                        if info is not None:
                            self._models[model_name] = info

        return self._models.get(model_name, None)

    def get_model(self, model_name: str) -> Any:
        """get already loaded model"""
        info = self._models.get(model_name)
        return None if info is None else info.model

    def unload_model(self, model_name: str) -> None:
        """unload model"""
        with self._lock:
            if model_name in self._models:
                del self._models[model_name]
                del self._model_locks[model_name]

    def get_model_list(self) -> dict:
        msg = {}

        for key in self._models.keys():
            info = self._models.get(key)

            try:
                msg[key] = info.__json__()
            except Exception as e:
                app_logger.log_inst.error(
                    "failed to serialize loaded model: %s, code:%s, continue...",
                    key, str(e))

        return msg


model_manager = ModelManager()
