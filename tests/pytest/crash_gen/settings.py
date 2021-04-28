from __future__ import annotations
import argparse
from typing import Optional

from crash_gen.misc import CrashGenError

# gConfig:    Optional[argparse.Namespace]

class Settings:
    _config = None # type Optional[argparse.Namespace]

    @classmethod    
    def init(cls):
        cls._config = None

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