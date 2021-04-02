from .cinterface import CTaosInterface
from .error import *


class TDengineSubscription(object):
    """TDengine subscription object
    """

    def __init__(self, sub):
        self._sub = sub

    def consume(self):
        """Consume rows of a subscription
        """
        if self._sub is None:
            raise OperationalError("Invalid use of consume")

        result, fields = CTaosInterface.consume(self._sub)
        buffer = [[] for i in range(len(fields))]
        while True:
            block, num_of_fields = CTaosInterface.fetchBlock(result, fields)
            if num_of_fields == 0:
                break
            for i in range(len(fields)):
                buffer[i].extend(block[i])

        self.fields = fields
        return list(map(tuple, zip(*buffer)))

    def close(self, keepProgress=True):
        """Close the Subscription.
        """
        if self._sub is None:
            return False

        CTaosInterface.unsubscribe(self._sub, keepProgress)
        return True


if __name__ == '__main__':
    from .connection import TDengineConnection
    conn = TDengineConnection(
        host="127.0.0.1",
        user="root",
        password="taosdata",
        database="test")

    # Generate a cursor object to run SQL commands
    sub = conn.subscribe(True, "test", "select * from meters;", 1000)

    for i in range(0, 10):
        data = sub.consume()
        for d in data:
            print(d)

    sub.close()
    conn.close()
