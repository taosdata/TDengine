# encoding:UTF-8
from taos.bind import *
from taos.bind2 import *
from taos.connection import TaosConnection
from taos.cursor import *

# For some reason, the following is needed for VS Code (through PyLance) to
# recognize that "error" is a valid module of the "taos" package.
from taos.error import *
from taos.field import *
from taos.result import *
from taos.schemaless import *
from taos.statement import *
from taos.statement2 import *
from taos.subscription import *

try:
    from taos.sqlalchemy import *
except:
    pass

from taos._version import __version__

# Globals
threadsafety = 2
"""sqlalchemy will read this attribute"""
paramstyle = "qmark"
"""sqlalchemy will read this attribute"""

__all__ = [
    "__version__",
    "IS_V3",
    "IGNORE",
    "TAOS_FIELD_COL",
    "TAOS_FIELD_TAG",
    "TAOS_FIELD_QUERY",
    "TAOS_FIELD_TBNAME",
    # functions
    "connect",
    "connect_test",
    "new_bind_param",
    "new_bind_params",
    "new_multi_binds",
    "new_multi_bind",
    # objects
    "TaosBind",
    "TaosConnection",
    "TaosCursor",
    "TaosResult",
    "TaosRows",
    "TaosRow",
    "TaosStmt",
    "TaosStmt2",
    "TaosStmt2Option",
    "BindTable",
    "PrecisionEnum",
    "SmlPrecision",
    "SmlProtocol",
    "utils",
]


def connect(*args, **kwargs):
    # type: (..., ...) -> TaosConnection
    """Function to return a TDengine connector object

    Current supporting keyword parameters:
    @dsn: Data source name as string
    @host: Hostname(optional)
    @port: Port number(optional)
    @user: Username as string(optional)
    @password: Password as string(optional)
    @totp_code: TOTP code for TOTP authentication(optional)
    @bearer_token: Token for token authentication(optional)
    @database: Database name(optional)

    @rtype: TDengineConnector
    """
    return TaosConnection(*args, **kwargs)


def connect_test(**kwargs):
    # type: (...) -> None
    """Function to test connection to TDengine with TOTP authentication

    Current supporting keyword parameters:
    @host: Hostname(optional)
    @port: Port number(optional)
    @user: Username as string(optional)
    @password: Password as string(optional)
    @totp_code: TOTP code for TOTP authentication(required)
    @database: Database name(optional)
    """
    from taos.cinterface import taos_connect_test

    host = kwargs.get("host")
    port = kwargs.get("port", 0)
    user = kwargs.get("user", "root")
    password = kwargs.get("password", "taosdata")
    totp = kwargs.get("totp_code")
    db = kwargs.get("database")

    if totp is None:
        raise TypeError("connect_test() missing 1 required keyword-only argument: 'totp_code'")

    taos_connect_test(host, user, password, totp, db, port)
