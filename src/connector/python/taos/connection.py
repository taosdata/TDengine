# encoding:UTF-8
from types import FunctionType
from .cinterface import *
from .cursor import TaosCursor
from .subscription import TaosSubscription
from .statement import TaosStmt
from .stream import TaosStream
from .result import *


class TaosConnection(object):
    """TDengine connection object"""

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
        if "host" in kwargs:
            self._host = kwargs["host"]

        # user
        if "user" in kwargs:
            self._user = kwargs["user"]

        # password
        if "password" in kwargs:
            self._password = kwargs["password"]

        # database
        if "database" in kwargs:
            self._database = kwargs["database"]

        # port
        if "port" in kwargs:
            self._port = kwargs["port"]

        # config
        if "config" in kwargs:
            self._config = kwargs["config"]

        self._chandle = CTaosInterface(self._config)
        self._conn = self._chandle.connect(self._host, self._user, self._password, self._database, self._port)

    def close(self):
        """Close current connection."""
        if self._conn:
            taos_close(self._conn)
            self._conn = None

    @property
    def client_info(self):
        # type: () -> str
        return taos_get_client_info()

    @property
    def server_info(self):
        # type: () -> str
        return taos_get_server_info(self._conn)

    def select_db(self, database):
        # type: (str) -> None
        taos_select_db(self._conn, database)

    def execute(self, sql):
        # type: (str) -> None
        """Simplely execute sql ignoring the results"""
        res = taos_query(self._conn, sql)
        taos_free_result(res)

    def query(self, sql):
        # type: (str) -> TaosResult
        result = taos_query(self._conn, sql)
        return TaosResult(result, True, self)

    def query_a(self, sql, callback, param):
        # type: (str, async_query_callback_type, c_void_p) -> None
        """Asynchronously query a sql with callback function"""
        taos_query_a(self._conn, sql, callback, param)

    def subscribe(self, restart, topic, sql, interval, callback=None, param=None):
        # type: (bool, str, str, int, subscribe_callback_type, c_void_p) -> TaosSubscription
        """Create a subscription."""
        if self._conn is None:
            return None
        sub = taos_subscribe(self._conn, restart, topic, sql, interval, callback, param)
        return TaosSubscription(sub, callback != None)

    def statement(self, sql=None):
        # type: (str | None) -> TaosStmt
        if self._conn is None:
            return None
        stmt = taos_stmt_init(self._conn)
        if sql != None:
            taos_stmt_prepare(stmt, sql)

        return TaosStmt(stmt)

    def load_table_info(self, tables):
        # type: (str) -> None
        taos_load_table_info(self._conn, tables)

    def stream(self, sql, callback, stime=0, param=None, callback2=None):
        # type: (str, Callable[[Any, TaosResult, TaosRows], None], int, Any, c_void_p) -> TaosStream
        # cb = cast(callback, stream_callback_type)
        # ref = byref(cb)

        stream = taos_open_stream(self._conn, sql, callback, stime, param, callback2)
        return TaosStream(stream)

    def insert_lines(self, lines):
        # type: (list[str]) -> None
        """Line protocol and schemaless support

        ## Example

        ```python
        import taos
        conn = taos.connect()
        conn.exec("drop database if exists test")
        conn.select_db("test")
        lines = [
            'ste,t2=5,t3=L"ste" c1=true,c2=4,c3="string" 1626056811855516532',
        ]
        conn.insert_lines(lines)
        ```

        ## Exception

        ```python
        try:
            conn.insert_lines(lines)
        except SchemalessError as err:
            print(err)
        ```
        """
        return taos_insert_lines(self._conn, lines)

    def cursor(self):
        # type: () -> TaosCursor
        """Return a new Cursor object using the connection."""
        return TaosCursor(self)

    def commit(self):
        """Commit any pending transaction to the database.

        Since TDengine do not support transactions, the implement is void functionality.
        """
        pass

    def rollback(self):
        """Void functionality"""
        pass

    def clear_result_set(self):
        """Clear unused result set on this connection."""
        pass

    def __del__(self):
        self.close()


if __name__ == "__main__":
    conn = TaosConnection()
    conn.close()
    print("Hello world")
