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
        print(buffer)
        while True:
            block, num_of_fields = CTaosInterface.fetchBlock(result, fields)
            if num_of_fields == 0: break
            for i in range(len(fields)):
                buffer[i].extend(block[i])

        return list(map(tuple, zip(*buffer)))


    def close(self, keepProgress = True):
        """Close the Subscription.
        """
        if self._sub is None:
            return False
        
        CTaosInterface.unsubscribe(self._sub, keepProgress)
        return True
