from functools import wraps
import os


class NoEnvError(Exception):
    pass


def check_env(func):
    @wraps(func)
    def _func():
        if "TDENGINE_URL" not in os.environ:
            raise NoEnvError("Please set environment variable TDENGINE_URL")
        func()

    return _func
