# from .cursor import TDengineCursor
from .cursor import TDengineCursor
from .cinterface import CTaosInterface

class TDengineConnection(object):
    """ TDengine connection object
    """
    def __init__(self, *args, **kwargs):
        self._conn = None
        self._host = None
        self._user = "root"
        self._password = "taosdata"
        self._database = None
        self._port = 0
        self._config = None
        self._chandle = None

        self.config(**kwargs)

    def config(self, **kwargs):
        # host
        if 'host' in kwargs:
            self._host = kwargs['host']

        # user
        if 'user' in kwargs:
            self._user = kwargs['user']

        # password
        if 'password' in kwargs:
            self._password = kwargs['password']
        
        # database
        if 'database' in kwargs:
            self._database = kwargs['database']

        # port
        if 'port' in kwargs:
            self._port = kwargs['port']

        # config
        if 'config' in kwargs:
            self._config = kwargs['config']

        self._chandle = CTaosInterface(self._config)
        self._conn = self._chandle.connect(self._host, self._user, self._password, self._database, self._port)

    def close(self):
        """Close current connection.
        """
        return CTaosInterface.close(self._conn)

    def cursor(self):
        """Return a new Cursor object using the connection.
        """
        return TDengineCursor(self)

    def commit(self):
        """Commit any pending transaction to the database.

        Since TDengine do not support transactions, the implement is void functionality.
        """
        pass

    def rollback(self):
        """Void functionality
        """
        pass

    def clear_result_set(self):
        """Clear unused result set on this connection.
        """
        result = self._chandle.useResult(self._conn)[0]
        if result:
            self._chandle.freeResult(result)

if __name__ == "__main__":
    conn = TDengineConnection(host='192.168.1.107')
    conn.close()
    print("Hello world")