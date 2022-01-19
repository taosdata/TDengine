from taos.result import TaosResult
from .cinterface import *
from .error import *


class TaosSubscription(object):
    """TDengine subscription object"""

    def __init__(self, sub, with_callback = False):
        self._sub = sub
        self._with_callback = with_callback

    def consume(self):
        """Consume rows of a subscription"""
        if self._sub is None:
            raise OperationalError("Invalid use of consume")
        if self._with_callback:
            raise OperationalError("DONOT use consume method in an subscription with callback")
        result = taos_consume(self._sub)
        return TaosResult(result)

    def close(self, keepProgress=True):
        """Close the Subscription."""
        if self._sub is None:
            return False

        taos_unsubscribe(self._sub, keepProgress)
        self._sub = None
        return True
    
    def __del__(self):
        self.close()


if __name__ == "__main__":
    from .connection import TaosConnection

    conn = TaosConnection(host="127.0.0.1", user="root", password="taosdata", database="test")

    # Generate a cursor object to run SQL commands
    sub = conn.subscribe(True, "test", "select * from meters;", 1000)

    for i in range(0, 10):
        data = sub.consume()
        for d in data:
            print(d)

    sub.close()
    conn.close()
