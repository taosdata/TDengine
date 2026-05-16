import sys
from sqlalchemy import types as sqltypes
from sqlalchemy.engine import default, reflection
from sqlalchemy import text

# taos.sqlalchemy.BaseDialect
import taos

#
# ---------------- taos rest impl -------------
#
import taos.sqlalchemy
import taosrest


# rest connect
class AlchemyRestConnection:
    threadsafety = 0
    paramstyle = "pyformat"
    Error = taosrest.Error

    # connect
    def connect(self, **kwargs):
        host = kwargs["host"] if "host" in kwargs else None
        port = kwargs["port"] if "port" in kwargs else None
        user = kwargs["username"] if "username" in kwargs else "root"
        password = kwargs["password"] if "password" in kwargs else "taosdata"
        database = kwargs["database"] if "database" in kwargs else None
        token = kwargs["token"] if "token" in kwargs else None

        if not host:
            host = "localhost"
        if host == "localhost" and not port:
            port = 6041

        url = f"http://{host}"
        if port:
            url += f":{port}"
        return taosrest.connect(url=url, user=user, password=password, database=database, token=token)


# rest dialect
class TaosRestDialect(taos.sqlalchemy.BaseDialect):
    name = "taosrest"
    driver = "taosrest"

    @classmethod
    def dbapi(cls):
        return AlchemyRestConnection()

    @classmethod
    def import_dbapi(cls):
        return AlchemyRestConnection()
