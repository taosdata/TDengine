from taos.cinterface import *
from taos.error import *
from taos.result import *


class TaosStream(object):
    """TDengine Stream interface"""

    def __init__(self, stream):
        self._raw = stream

    def as_ptr(self):
        return self._raw

    def close(self):
        """Close stmt."""
        if self._raw is not None:
            taos_close_stream(self._raw)
            self._raw = None

    def __del__(self):
        self.close()
