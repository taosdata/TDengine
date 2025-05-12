from __future__ import annotations
import argparse

from typing import Optional

from .misc import CrashGenError

# from crash_gen.misc import CrashGenError

# gConfig:    Optional[argparse.Namespace]

class Config:
    _config = None # type Optional[argparse.Namespace]

    @classmethod    
    def init(cls, parser: argparse.ArgumentParser):
        if cls._config is not None:
            raise CrashGenError("Config can only be initialized once")
        cls._config = parser.parse_args()
        # print(cls._config)

    @classmethod
    def setConfig(cls, config: argparse.Namespace):
        cls._config = config

    @classmethod
    # TODO: check items instead of exposing everything
    def getConfig(cls) -> argparse.Namespace:
        if cls._config is None:
            raise CrashGenError("invalid state")
        return cls._config

    @classmethod
    def clearConfig(cls):
        cls._config = None

    @classmethod
    def isSet(cls, cfgKey):
        cfg = cls.getConfig()
        if cfgKey not in cfg:
            return False
        return cfg.__getattribute__(cfgKey)