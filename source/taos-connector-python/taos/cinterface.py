# encoding:UTF-8

import ctypes
import platform
import inspect
from ctypes import *
import pytz

try:
    from typing import Any
except BaseException:
    pass

from taos.error import *
from taos.schemaless import *
from typing import List, Tuple, Union

_UNSUPPORTED = {}


# C interface class


class TaosOption:
    Locale = 0
    Charset = 1
    Timezone = 2
    ConfigDir = 3
    ShellActivityTimer = 4
    MaxOptions = 5


def _load_taos_linux():
    try:
        return ctypes.CDLL("libtaos.so")
    except Exception as e:
        raise InterfaceError("unable to load taos client library: %s" % e)


def _load_taos_darwin():
    try:
        from ctypes.macholib.dyld import dyld_find as _dyld_find

        return ctypes.CDLL(_dyld_find("libtaos.dylib"))
    except Exception as e:
        raise InterfaceError("unable to load taos client library: %s" % e)


def _load_taos_windows():
    try:
        return ctypes.windll.LoadLibrary("taos")
    except Exception as e:
        print("unable to load taos client library: %s" % e)
        try:
            from ctypes.util import find_library

            ctypes.windll.LoadLibrary(find_library("taos"))
        except Exception as final_err:
            raise InterfaceError("unable to load taos client library: %s" % final_err)


def _load_taos():
    load_func = {
        "Linux": _load_taos_linux,
        "Darwin": _load_taos_darwin,
        "Windows": _load_taos_windows,
    }
    pf = platform.system()
    if pf not in load_func:
        raise InterfaceError("unsupported platform: %s" % pf)
    try:
        return load_func[pf]()
    except Exception as err:
        raise InterfaceError("unable to load taos client library: %s" % err)


_libtaos = _load_taos()

_libtaos.taos_get_client_info.restype = c_char_p


def taos_get_client_info():
    # type: () -> str
    """Get client version info."""
    return _libtaos.taos_get_client_info().decode("utf-8")


IS_V3 = False

if taos_get_client_info().split(".")[0] < "3":
    from taos.field import (
        CONVERT_FUNC,
        CONVERT_FUNC_BLOCK,
        TaosFields,
        TaosField,
        set_tz,
        convert_func,
        convert_block_func,
    )
else:
    from taos.field import (
        CONVERT_FUNC,
        CONVERT_FUNC_BLOCK,
        TaosFields,
        TaosField,
        set_tz,
        convert_func,
        convert_block_func,
    )

    # use _v3s TaosField overwrite _v2s here, dont change import order
    from taos.field_v3 import (
        CONVERT_FUNC_BLOCK_v3,
        TaosFields,
        TaosFieldEs,
        TaosField,
        TaosFieldE,
        TAOS_FIELD_T,
        TaosFieldAll,
        TaosFieldAllCls,
        convert_block_func_v3,
    )
    from taos.constants import FieldType

    IS_V3 = True

_libtaos.taos_fetch_fields.restype = ctypes.POINTER(TaosField)

try:
    _libtaos.taos_init.restype = None
except Exception as err:
    _UNSUPPORTED["taos_init"] = err

_libtaos.taos_connect.restype = ctypes.c_void_p
_libtaos.taos_fetch_row.restype = ctypes.POINTER(ctypes.c_void_p)
_libtaos.taos_errstr.restype = ctypes.c_char_p

try:
    _libtaos.taos_subscribe.restype = ctypes.c_void_p
except Exception as err:
    _UNSUPPORTED["taos_subscribe"] = err

try:
    _libtaos.taos_consume.restype = ctypes.c_void_p
    _libtaos.taos_consume.argstype = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["taos_consume"] = err

_libtaos.taos_fetch_lengths.restype = ctypes.POINTER(ctypes.c_int)
_libtaos.taos_free_result.restype = None
_libtaos.taos_query.restype = ctypes.POINTER(ctypes.c_void_p)

try:
    _libtaos.taos_stmt_errstr.restype = c_char_p
except AttributeError:
    None
finally:
    None

_libtaos.taos_options.restype = None
_libtaos.taos_options.argtypes = (c_int, c_void_p)


def taos_options(option, value):
    # type: (TaosOption, str) -> None
    _value = c_char_p(value.encode("utf-8"))
    _libtaos.taos_options(option, _value)


def taos_options_connection(connection, option, value):
    _value = c_char_p(value.encode("utf-8"))
    _libtaos.taos_options_connection(connection, option, _value)


def taos_set_conn_mode(connection, mode, value):
    _libtaos.taos_set_conn_mode(connection, mode, value)


def taos_init():
    # type: () -> None
    """
    C: taos_init
    """
    _libtaos.taos_init()


_libtaos.taos_cleanup.restype = None


def taos_cleanup():
    # type: () -> None
    """Cleanup workspace."""
    _libtaos.taos_cleanup()


_libtaos.taos_get_server_info.restype = c_char_p
_libtaos.taos_get_server_info.argtypes = (c_void_p,)


def taos_get_server_info(connection):
    # type: (c_void_p) -> str
    """Get server version as string."""
    return _libtaos.taos_get_server_info(connection).decode("utf-8")


_libtaos.taos_close.restype = None
_libtaos.taos_close.argtypes = (c_void_p,)


def taos_close(connection):
    # type: (c_void_p) -> None
    """Close the TAOS* connection"""
    if _libtaos.taos_close is not None:
        _libtaos.taos_close(connection)


_libtaos.taos_connect.restype = c_void_p
_libtaos.taos_connect.argtypes = c_char_p, c_char_p, c_char_p, c_char_p, c_uint16


def taos_connect(host=None, user="root", password="taosdata", db=None, port=0):
    # type: (None|str, str, str, None|str, int) -> c_void_p
    """Create TDengine database connection.

    - host: server hostname/FQDN
    - user: user name
    - password: user password
    - db: database name (optional)
    - port: server port

    @rtype: c_void_p, TDengine handle
    """
    # host
    try:
        _host = c_char_p(host.encode("utf-8")) if host is not None else None
    except AttributeError:
        raise AttributeError("host is expected as a str")

    # user
    try:
        _user = c_char_p(user.encode("utf-8"))
    except AttributeError:
        raise AttributeError("user is expected as a str")

    # password
    try:
        _password = c_char_p(password.encode("utf-8"))
    except AttributeError:
        raise AttributeError("password is expected as a str")

    # db
    try:
        _db = c_char_p(db.encode("utf-8")) if db is not None else None
    except AttributeError:
        raise AttributeError("db is expected as a str")

    # port
    try:
        _port = c_uint16(port)
    except TypeError:
        raise TypeError("port is expected as an uint16")

    connection = cast(_libtaos.taos_connect(_host, _user, _password, _db, _port), c_void_p)

    if connection.value is None:
        null_ptr = c_void_p(None)
        errno = taos_errno(null_ptr)
        errstr = taos_errstr(null_ptr)
        raise ConnectionError(errstr, errno)
    return connection


_libtaos.taos_connect_auth.restype = c_void_p
_libtaos.taos_connect_auth.argtypes = c_char_p, c_char_p, c_char_p, c_char_p, c_uint16


def taos_connect_auth(host=None, user="root", auth="", db=None, port=0):
    # type: (None|str, str, str, None|str, int) -> c_void_p
    """Connect server with auth token.

    - host: server hostname/FQDN
    - user: user name
    - auth: base64 encoded auth token
    - db: database name (optional)
    - port: server port

    @rtype: c_void_p, TDengine handle
    """
    # host
    try:
        _host = c_char_p(host.encode("utf-8")) if host is not None else None
    except AttributeError:
        raise AttributeError("host is expected as a str")

    # user
    try:
        _user = c_char_p(user.encode("utf-8"))
    except AttributeError:
        raise AttributeError("user is expected as a str")

    # auth
    try:
        _auth = c_char_p(auth.encode("utf-8"))
    except AttributeError:
        raise AttributeError("password is expected as a str")

    # db
    try:
        _db = c_char_p(db.encode("utf-8")) if db is not None else None
    except AttributeError:
        raise AttributeError("db is expected as a str")

    # port
    try:
        _port = c_uint16(port)
    except TypeError:
        raise TypeError("port is expected as an int")

    connection = cast(_libtaos.taos_connect_auth(_host, _user, _auth, _db, _port), c_void_p)

    if connection.value is None:
        null_ptr = c_void_p(None)
        errno = taos_errno(null_ptr)
        errstr = taos_errstr(null_ptr)
        raise ConnectionError(errstr, errno)
    return connection


try:
    _libtaos.taos_connect_totp.restype = c_void_p
    _libtaos.taos_connect_totp.argtypes = c_char_p, c_char_p, c_char_p, c_char_p, c_char_p, c_uint16
except Exception as err:
    _UNSUPPORTED["taos_connect_totp"] = err

try:
    _libtaos.taos_connect_test.restype = c_int
    _libtaos.taos_connect_test.argtypes = c_char_p, c_char_p, c_char_p, c_char_p, c_char_p, c_uint16
except Exception as err:
    _UNSUPPORTED["taos_connect_test"] = err

try:
    _libtaos.taos_connect_token.restype = c_void_p
    _libtaos.taos_connect_token.argtypes = c_char_p, c_char_p, c_char_p, c_uint16
except Exception as err:
    _UNSUPPORTED["taos_connect_token"] = err


def taos_connect_totp(host=None, user="root", password="taosdata", totp=None, db=None, port=0):
    # type: (None|str, str, str, None|str, None|str, int) -> c_void_p
    """Connect TDengine with TOTP authentication.

    - host: server hostname/FQDN
    - user: user name
    - password: user password
    - totp: time-based one-time password
    - db: database name (optional)
    - port: server port

    @rtype: c_void_p, TDengine handle
    """
    _check_if_supported()

    _host = _str_to_c_char_p(host, "host")
    _user = _str_to_c_char_p(user, "user")
    _password = _str_to_c_char_p(password, "password")
    _totp = _str_to_c_char_p(totp, "totp")
    _db = _str_to_c_char_p(db, "db")

    try:
        _port = c_uint16(port)
    except TypeError:
        raise TypeError("port is expected as an uint16")

    connection = cast(_libtaos.taos_connect_totp(_host, _user, _password, _totp, _db, _port), c_void_p)

    if connection.value is None:
        null_ptr = c_void_p(None)
        errno = taos_errno(null_ptr)
        errstr = taos_errstr(null_ptr)
        raise ConnectionError(errstr, errno)
    return connection


def taos_connect_test(host=None, user="root", password="taosdata", totp=None, db=None, port=0):
    # type: (None|str, str, str, None|str, None|str, int) -> None
    """Test TDengine connection with TOTP authentication.

    - host: server hostname/FQDN
    - user: user name
    - password: user password
    - totp: time-based one-time password for testing
    - db: database name (optional)
    - port: server port
    """
    _check_if_supported()

    _host = _str_to_c_char_p(host, "host")
    _user = _str_to_c_char_p(user, "user")
    _password = _str_to_c_char_p(password, "password")
    _totp = _str_to_c_char_p(totp, "totp")
    _db = _str_to_c_char_p(db, "db")

    try:
        _port = c_uint16(port)
    except TypeError:
        raise TypeError("port is expected as an uint16")

    code = _libtaos.taos_connect_test(_host, _user, _password, _totp, _db, _port)
    if code != 0:
        null_ptr = c_void_p(None)
        errno = taos_errno(null_ptr)
        errstr = taos_errstr(null_ptr)
        raise ConnectionError(errstr, errno)


def taos_connect_token(host=None, token=None, db=None, port=0):
    # type: (None|str, None|str, None|str, int) -> c_void_p
    """Connect TDengine with token authentication.

    - host: server hostname/FQDN
    - token: authentication token
    - db: database name (optional)
    - port: server port

    @rtype: c_void_p, TDengine handle
    """
    _check_if_supported()

    _host = _str_to_c_char_p(host, "host")
    _token = _str_to_c_char_p(token, "token")
    _db = _str_to_c_char_p(db, "db")

    try:
        _port = c_uint16(port)
    except TypeError:
        raise TypeError("port is expected as an uint16")

    connection = cast(_libtaos.taos_connect_token(_host, _token, _db, _port), c_void_p)

    if connection.value is None:
        null_ptr = c_void_p(None)
        errno = taos_errno(null_ptr)
        errstr = taos_errstr(null_ptr)
        raise ConnectionError(errstr, errno)
    return connection


def _str_to_c_char_p(value, name):
    if value is None:
        return None
    if not isinstance(value, str):
        raise AttributeError(f"{name} is expected as a str")
    return c_char_p(value.encode("utf-8"))


_libtaos.taos_query.restype = c_void_p
_libtaos.taos_query.argtypes = c_void_p, c_char_p


def taos_query(connection, sql):
    # type: (c_void_p, str) -> c_void_p
    """Run SQL

    - sql: str, sql string to run

    @return: TAOS_RES*, result pointer

    """
    try:
        ptr = c_char_p(sql.encode("utf-8"))
        res = c_void_p(_libtaos.taos_query(connection, ptr))
        errno = taos_errno(res)
        if errno != 0:
            errstr = taos_errstr(res)
            taos_free_result(res)
            raise ProgrammingError(errstr, errno)
        return res
    except AttributeError:
        raise AttributeError("sql is expected as a string")


try:
    _libtaos.taos_query_with_reqid.restype = c_void_p
    _libtaos.taos_query_with_reqid.argtypes = c_void_p, c_char_p, c_int
except Exception as err:
    _UNSUPPORTED["taos_query_with_reqid"] = err


def taos_query_with_reqid(connection, sql, req_id):
    # type: (c_void_p, str, int) -> c_void_p
    """Run SQL with request id

    - sql: str, sql string to run
    - reqid: int, request id

    @return: TAOS_RES*, result pointer

    """
    _check_if_supported()
    try:
        ptr = c_char_p(sql.encode("utf-8"))
        res = c_void_p(_libtaos.taos_query_with_reqid(connection, ptr, req_id))
        errno = taos_errno(res)
        if errno != 0:
            errstr = taos_errstr(res)
            taos_free_result(res)
            raise ProgrammingError(errstr, errno)
        return res
    except AttributeError:
        raise AttributeError("sql is expected as a string")


async_query_callback_type = CFUNCTYPE(None, c_void_p, c_void_p, c_int)
_libtaos.taos_query_a.restype = None
_libtaos.taos_query_a.argtypes = c_void_p, c_char_p, async_query_callback_type, c_void_p


def taos_query_a(connection, sql, callback, param):
    # type: (c_void_p, str, async_query_callback_type, c_void_p) -> None
    _libtaos.taos_query_a(connection, c_char_p(sql.encode("utf-8")), async_query_callback_type(callback), param)


# add req_id for async query
try:
    async_query_with_reqid_callback_type = CFUNCTYPE(None, c_void_p, c_void_p, c_int)
    _libtaos.taos_query_a_with_reqid.restype = None
    _libtaos.taos_query_a_with_reqid.argtypes = (
        c_void_p,
        c_char_p,
        async_query_with_reqid_callback_type,
        c_void_p,
        c_int,
    )
except Exception as err:
    _UNSUPPORTED["taos_query_a_with_reqid"] = err


def taos_query_a_with_reqid(connection, sql, callback, param, req_id):
    # type: (c_void_p, str, async_query_with_reqid_callback_type, c_void_p, int) -> None
    """
    Run SQL with request id

    - sql: str, sql string to run
    - req_id: int, request id

    @return: None

    """
    _check_if_supported()
    _libtaos.taos_query_a_with_reqid(
        connection, c_char_p(sql.encode("utf-8")), async_query_with_reqid_callback_type(callback), param, req_id
    )


async_fetch_rows_callback_type = CFUNCTYPE(None, c_void_p, c_void_p, c_int)
_libtaos.taos_fetch_rows_a.restype = None
_libtaos.taos_fetch_rows_a.argtypes = c_void_p, async_fetch_rows_callback_type, c_void_p


def taos_fetch_rows_a(result, callback, param):
    # type: (c_void_p, async_fetch_rows_callback_type, c_void_p) -> None
    _libtaos.taos_fetch_rows_a(result, async_fetch_rows_callback_type(callback), param)


def taos_affected_rows(result):
    # type: (c_void_p) -> int
    """The affected rows after running query"""
    return _libtaos.taos_affected_rows(result)


subscribe_callback_type = CFUNCTYPE(None, c_void_p, c_void_p, c_void_p, c_int)


def taos_subscribe(connection, restart, topic, sql, interval, callback=None, param=None):
    # type: (c_void_p, bool, str, str, c_int, subscribe_callback_type, c_void_p | None) -> c_void_p
    """Create a subscription
    @restart boolean,
    @sql string, sql statement for data query, must be a 'select' statement.
    @topic string, name of this subscription
    """
    if callback is not None:
        callback = subscribe_callback_type(callback)
    return c_void_p(
        _libtaos.taos_subscribe(
            connection,
            1 if restart else 0,
            c_char_p(topic.encode("utf-8")),
            c_char_p(sql.encode("utf-8")),
            callback,
            c_void_p(param),
            interval,
        )
    )


def taos_consume(sub):
    """Consume data of a subscription"""
    return c_void_p(_libtaos.taos_consume(sub))


try:
    _libtaos.taos_unsubscribe.restype = None
    _libtaos.taos_unsubscribe.argstype = c_void_p, c_int
except Exception as err:
    _UNSUPPORTED["taos_unsubscribe"] = err


def taos_unsubscribe(sub, keep_progress):
    """Cancel a subscription"""
    _libtaos.taos_unsubscribe(sub, 1 if keep_progress else 0)


def taos_use_result(result):
    """Use result after calling self.query, it's just for 1.6."""
    fields = []
    pfields = taos_fetch_fields_raw(result)
    pfields = cast(pfields, POINTER(TaosField))
    for i in range(taos_field_count(result)):
        fields.append(
            {
                "name": pfields[i].name,
                "bytes": pfields[i].bytes,
                "type": pfields[i].type,
            }
        )

    return fields


_libtaos.taos_is_null.restype = c_bool
_libtaos.taos_is_null.argtypes = ctypes.c_void_p, c_int, c_int


def taos_is_null(result, row, col):
    return _libtaos.taos_is_null(result, row, col)


_can_get_is_null_by_column = True
try:
    _libtaos.taos_is_null_by_column.restype = c_int
    _libtaos.taos_is_null_by_column.argtypes = (c_void_p, c_int, ctypes.POINTER(c_bool), ctypes.POINTER(c_int))
except Exception as err:
    _can_get_is_null_by_column = False
    _UNSUPPORTED["taos_is_null_by_column"] = err


def taos_is_null_by_column(result, row, col):
    pRow = pointer(c_int(row))
    is_nulls = (c_bool * row)()
    errno = _libtaos.taos_is_null_by_column(result, col, is_nulls, pRow)
    if errno != 0:
        raise InternalError(errno=errno, msg=taos_errstr(result))
    return is_nulls


_libtaos.taos_fetch_block.restype = c_int
_libtaos.taos_fetch_block.argtypes = c_void_p, c_void_p


def taos_fetch_block_raw(result):
    pblock = ctypes.c_void_p(0)
    num_of_rows = _libtaos.taos_fetch_block(result, ctypes.byref(pblock))
    if num_of_rows == 0:
        return None, 0
    return pblock, abs(num_of_rows)


if not IS_V3:
    pass
else:
    _libtaos.taos_get_column_data_offset.restype = ctypes.POINTER(ctypes.c_int)
    _libtaos.taos_get_column_data_offset.argtypes = ctypes.c_void_p, c_int


def taos_get_column_data_offset(result, field, rows):
    # Make sure to call taos_get_column_data_offset after taos_fetch_block()
    offsets = _libtaos.taos_get_column_data_offset(result, field)
    if not offsets:
        raise OperationalError("offsets empty, use taos_fetch_block before it")
    return offsets[:rows]


def taos_fetch_block_v3(result, fields=None, field_count=None, decode_binary=True):
    if fields is None:
        fields = taos_fetch_fields(result)
    if field_count is None:
        field_count = taos_field_count(result)
    pblock = ctypes.c_void_p(0)
    num_of_rows = _libtaos.taos_fetch_block(result, ctypes.byref(pblock))
    if num_of_rows == 0:
        return None, 0
    precision = taos_result_precision(result)
    blocks = [None] * field_count
    for i in range(len(fields)):
        data = ctypes.cast(pblock, ctypes.POINTER(ctypes.c_void_p))[i]
        if fields[i]["type"] not in CONVERT_FUNC_BLOCK_v3 and fields[i]["type"] not in CONVERT_FUNC_BLOCK:
            raise DatabaseError("Invalid data type returned from database")
        offsets = []
        is_null = []
        if fields[i]["type"] in (
            FieldType.C_VARCHAR,
            FieldType.C_NCHAR,
            FieldType.C_JSON,
            FieldType.C_VARBINARY,
            FieldType.C_GEOMETRY,
            FieldType.C_BLOB,
        ):
            offsets = taos_get_column_data_offset(result, i, num_of_rows)
            f = convert_block_func_v3(fields[i]["type"], decode_binary=decode_binary)
            blocks[i] = f(data, is_null, num_of_rows, offsets, precision)
        else:
            if _can_get_is_null_by_column:
                is_null = taos_is_null_by_column(result, num_of_rows, i)
            else:
                is_null = [taos_is_null(result, j, i) for j in range(num_of_rows)]
            f = convert_block_func(fields[i]["type"], decode_binary=decode_binary)
            blocks[i] = f(data, is_null, num_of_rows, offsets, precision)

    return blocks, abs(num_of_rows)


def taos_fetch_block_v2(result, fields=None, field_count=None, decode_binary=True):
    pblock = ctypes.c_void_p(0)
    num_of_rows = _libtaos.taos_fetch_block(result, ctypes.byref(pblock))
    if num_of_rows == 0:
        return None, 0
    precision = taos_result_precision(result)
    if fields is None:
        fields = taos_fetch_fields(result)
    if field_count is None:
        field_count = taos_field_count(result)
    blocks = [None] * field_count
    fieldLen = taos_fetch_lengths(result, field_count)
    for i in range(len(fields)):
        data = ctypes.cast(pblock, ctypes.POINTER(ctypes.c_void_p))[i]
        if fields[i]["type"] not in CONVERT_FUNC:
            raise DatabaseError("Invalid data type returned from database")
        is_null = [taos_is_null(result, j, i) for j in range(num_of_rows)]
        f = convert_block_func(fields[i]["type"], decode_binary=decode_binary)
        blocks[i] = f(data, is_null, num_of_rows, fieldLen[i], precision)

    return blocks, abs(num_of_rows)


if not IS_V3:
    taos_fetch_block = taos_fetch_block_v2
else:
    taos_fetch_block = taos_fetch_block_v3

_libtaos.taos_fetch_row.restype = c_void_p
_libtaos.taos_fetch_row.argtypes = (c_void_p,)


def taos_fetch_row_raw(result):
    # type: (c_void_p) -> c_void_p
    row = c_void_p(_libtaos.taos_fetch_row(result))
    if row:
        return row
    return None


def taos_fetch_row(result, fields, decode_binary=True):
    # type: (c_void_p, Array[TaosField], bool) -> tuple(c_void_p, int)
    pblock = taos_fetch_row_raw(result)
    if pblock:
        num_of_rows = 1
        precision = taos_result_precision(result)
        field_count = taos_field_count(result)
        blocks = [None] * field_count
        field_lens = taos_fetch_lengths(result, field_count)
        for i in range(field_count):
            data = ctypes.cast(pblock, ctypes.POINTER(ctypes.c_void_p))[i]
            if fields[i].type not in CONVERT_FUNC:
                raise DatabaseError("Invalid data type returned from database")
            if data is None:
                blocks[i] = [None]
            else:
                f = convert_func(fields[i].type, decode_binary=decode_binary)
                blocks[i] = f(data, [False], num_of_rows, field_lens[i], precision)
    else:
        return None, 0
    return blocks, abs(num_of_rows)


_libtaos.taos_free_result.argtypes = (c_void_p,)


def taos_free_result(result):
    # type: (c_void_p) -> None
    if result is not None:
        _libtaos.taos_free_result(result)


_libtaos.taos_field_count.restype = c_int
_libtaos.taos_field_count.argstype = (c_void_p,)


def taos_field_count(result):
    # type: (c_void_p) -> int
    return _libtaos.taos_field_count(result)


def taos_num_fields(result):
    # type: (c_void_p) -> int
    return _libtaos.taos_num_fields(result)


_libtaos.taos_fetch_fields.restype = c_void_p
_libtaos.taos_fetch_fields.argstype = (c_void_p,)


def taos_fetch_fields_raw(result):
    # type: (c_void_p) -> c_void_p
    return c_void_p(_libtaos.taos_fetch_fields(result))


try:
    _libtaos.taos_fetch_fields_e.restype = c_void_p
    _libtaos.taos_fetch_fields_e.argstype = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["taos_fetch_fields_e"] = err


def taos_fetch_fields_e_raw(result):
    # type: (c_void_p) -> c_void_p
    return c_void_p(_libtaos.taos_fetch_fields_e(result))


def taos_fetch_fields(result):
    # type: (c_void_p) -> TaosFields
    fields = taos_fetch_fields_raw(result)
    count = taos_field_count(result)
    return TaosFields(fields, count)


def taos_fetch_fields_e(result):
    # type: (c_void_p) -> TaosFieldEs
    fields = taos_fetch_fields_e_raw(result)
    count = taos_field_count(result)
    return TaosFieldEs(fields, count)


def taos_fetch_lengths(result, field_count=None):
    # type: (c_void_p, int) -> Array[int]
    """Make sure to call taos_fetch_row or taos_fetch_block before fetch_lengths"""
    lens = _libtaos.taos_fetch_lengths(result)
    if field_count is None:
        field_count = taos_field_count(result)
    if not lens:
        raise OperationalError("field length empty, use taos_fetch_row/block before it")
    return lens[:field_count]


def taos_result_precision(result):
    # type: (c_void_p) -> c_int
    return _libtaos.taos_result_precision(result)


_libtaos.taos_errno.restype = c_int
_libtaos.taos_errno.argstype = (c_void_p,)


def taos_errno(result):
    # type: (ctypes.c_void_p) -> c_int
    """Return the error number."""
    return _libtaos.taos_errno(result)


_libtaos.taos_errstr.restype = c_char_p
_libtaos.taos_errstr.argstype = (c_void_p,)


def taos_errstr(result=c_void_p(None)):
    # type: (ctypes.c_void_p) -> str
    """Return the error styring"""
    return _libtaos.taos_errstr(result).decode("utf-8")


_libtaos.taos_stop_query.restype = None
_libtaos.taos_stop_query.argstype = (c_void_p,)


def taos_stop_query(result):
    # type: (ctypes.c_void_p) -> None
    """Stop current query"""
    return _libtaos.taos_stop_query(result)


try:
    _libtaos.taos_load_table_info.restype = c_int
    _libtaos.taos_load_table_info.argstype = (c_void_p, c_char_p)
except Exception as err:
    _UNSUPPORTED["taos_load_table_info"] = err


def taos_load_table_info(connection, tables):
    # type: (ctypes.c_void_p, str) -> None
    """Stop current query"""
    _check_if_supported()
    errno = _libtaos.taos_load_table_info(connection, c_char_p(tables.encode("utf-8")))
    if errno != 0:
        msg = taos_errstr()
        raise OperationalError(msg, errno)


_libtaos.taos_validate_sql.restype = c_int
_libtaos.taos_validate_sql.argstype = (c_void_p, c_char_p)


def taos_validate_sql(connection, sql):
    # type: (ctypes.c_void_p, str) -> None | str
    """Get taosd server info"""
    errno = _libtaos.taos_validate_sql(connection, ctypes.c_char_p(sql.encode("utf-8")))
    if errno != 0:
        msg = taos_errstr()
        return msg
    return None


_libtaos.taos_print_row.restype = c_int
_libtaos.taos_print_row.argstype = (c_char_p, c_void_p, c_void_p, c_int)


def taos_print_row(row, fields, num_fields, buffer_size=4096):
    # type: (ctypes.c_void_p, ctypes.c_void_p | TaosFields | TaosFieldEs, int, int) -> str
    """Print an row to string"""
    p = ctypes.create_string_buffer(buffer_size)
    if isinstance(fields, TaosFields) or (IS_V3 and isinstance(fields, TaosFieldEs)):
        _libtaos.taos_print_row(p, row, fields.as_ptr(), num_fields)
    else:
        _libtaos.taos_print_row(p, row, fields, num_fields)
    if p:
        return p.value.decode("utf-8")
    raise OperationalError("taos_print_row failed")


_libtaos.taos_select_db.restype = c_int
_libtaos.taos_select_db.argstype = (c_void_p, c_char_p)


def taos_select_db(connection, db):
    # type: (ctypes.c_void_p, str) -> None
    """Select database, eq to sql: use <db>"""
    res = _libtaos.taos_select_db(connection, ctypes.c_char_p(db.encode("utf-8")))
    if res != 0:
        raise DatabaseError("select database error", res)


_libtaos.taos_stmt_init.restype = c_void_p
_libtaos.taos_stmt_init.argstype = (c_void_p,)


def taos_stmt_init(connection):
    # type: (c_void_p) -> (c_void_p)
    """Create a statement query
    @param(connection): c_void_p TAOS*
    @rtype: c_void_p, *TAOS_STMT
    """
    return c_void_p(_libtaos.taos_stmt_init(connection))


# taos_stmt_init_with_reqid
try:
    _libtaos.taos_stmt_init_with_reqid.restype = c_void_p
    _libtaos.taos_stmt_init_with_reqid.argstype = (c_void_p, c_int)
except Exception as err:
    _UNSUPPORTED["taos_stmt_init_with_reqid"] = err


def taos_stmt_init_with_reqid(connection, req_id):
    # type: (c_void_p, int) -> c_void_p
    """Create a statement query

    connection: c_void_p TAOS*
    req_id: c_int

    @return: c_void_p, *TAOS_STMT
    """
    _check_if_supported()
    return c_void_p(_libtaos.taos_stmt_init_with_reqid(connection, req_id))


_libtaos.taos_stmt_prepare.restype = c_int
_libtaos.taos_stmt_prepare.argstype = (c_void_p, c_char_p, c_int)


def taos_stmt_prepare(stmt, sql):
    # type: (ctypes.c_void_p, str) -> None
    """Prepare a statement query
    @stmt: c_void_p TAOS_STMT*
    """
    buffer = sql.encode("utf-8")
    res = _libtaos.taos_stmt_prepare(stmt, ctypes.c_char_p(buffer), len(buffer))
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


_libtaos.taos_stmt_close.restype = c_int
_libtaos.taos_stmt_close.argstype = (c_void_p,)


def taos_stmt_close(stmt):
    # type: (ctypes.c_void_p) -> None
    """Close a statement query
    @stmt: c_void_p TAOS_STMT*
    """
    res = _libtaos.taos_stmt_close(stmt)
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


try:
    _libtaos.taos_stmt_errstr.restype = c_char_p
    _libtaos.taos_stmt_errstr.argstype = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["taos_stmt_errstr"] = err


def taos_stmt_errstr(stmt):
    # type: (ctypes.c_void_p) -> str
    """Get error message from stetement query
    @stmt: c_void_p TAOS_STMT*
    """
    _check_if_supported()
    err = c_char_p(_libtaos.taos_stmt_errstr(stmt))
    if err:
        return err.value.decode("utf-8")


try:
    _libtaos.taos_stmt_set_tbname.restype = c_int
    _libtaos.taos_stmt_set_tbname.argstype = (c_void_p, c_char_p)
except Exception as err:
    _UNSUPPORTED["taos_stmt_set_tbname"] = err


def taos_stmt_set_tbname(stmt, name):
    # type: (ctypes.c_void_p, str) -> None
    """Set table name of a statement query if exists.
    @stmt: c_void_p TAOS_STMT*
    """
    _check_if_supported()
    res = _libtaos.taos_stmt_set_tbname(stmt, c_char_p(name.encode("utf-8")))
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


try:
    _libtaos.taos_stmt_set_tbname_tags.restype = c_int
    _libtaos.taos_stmt_set_tbname_tags.argstype = (c_void_p, c_char_p, c_void_p)
except Exception as err:
    _UNSUPPORTED["taos_stmt_set_tbname_tags"] = err


def taos_stmt_set_tbname_tags(stmt, name, tags):
    # type: (c_void_p, str, c_void_p) -> None
    """Set table name with tags bind params.
    @stmt: c_void_p TAOS_STMT*
    """
    _check_if_supported()
    res = _libtaos.taos_stmt_set_tbname_tags(stmt, ctypes.c_char_p(name.encode("utf-8")), tags)

    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


try:
    _libtaos.taos_stmt_is_insert.restype = c_int
    _libtaos.taos_stmt_is_insert.argstype = (c_void_p, POINTER(c_int))
except Exception as err:
    _UNSUPPORTED["taos_stmt_is_insert"] = err


def taos_stmt_is_insert(stmt):
    # type: (ctypes.c_void_p) -> bool
    """Set table name with tags bind params.
    @stmt: c_void_p TAOS_STMT*
    """
    is_insert = ctypes.c_int()
    res = _libtaos.taos_stmt_is_insert(stmt, ctypes.byref(is_insert))
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)
    return is_insert == 0


try:
    _libtaos.taos_stmt_num_params.restype = c_int
    _libtaos.taos_stmt_num_params.argstype = (c_void_p, POINTER(c_int))
except Exception as err:
    _UNSUPPORTED["taos_stmt_num_params"] = err


def taos_stmt_num_params(stmt):
    # type: (ctypes.c_void_p) -> int
    """Params number of the current statement query.
    @stmt: TAOS_STMT*
    """
    num_params = ctypes.c_int()
    res = _libtaos.taos_stmt_num_params(stmt, ctypes.byref(num_params))
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)
    return num_params.value


try:
    _libtaos.taos_stmt_bind_param.restype = c_int
    _libtaos.taos_stmt_bind_param.argstype = (c_void_p, c_void_p)
except Exception as err:
    _UNSUPPORTED["taos_stmt_bind_param"] = err


def taos_stmt_bind_param(stmt, bind):
    # type: (ctypes.c_void_p, Array[TaosBind]) -> None
    """Bind params in the statement query.
    @stmt: TAOS_STMT*
    @bind: TAOS_BIND*
    """
    # ptr = ctypes.cast(bind, POINTER(TaosBind))
    # ptr = pointer(bind)
    res = _libtaos.taos_stmt_bind_param(stmt, bind)
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


try:
    _libtaos.taos_stmt_bind_param_batch.restype = c_int
    _libtaos.taos_stmt_bind_param_batch.argstype = (c_void_p, c_void_p)
except Exception as err:
    _UNSUPPORTED["taos_stmt_bind_param_batch"] = err


def taos_stmt_bind_param_batch(stmt, bind):
    # type: (ctypes.c_void_p, Array[TaosMultiBind]) -> None
    """Bind params in the statement query.
    @stmt: TAOS_STMT*
    @bind: TAOS_BIND*
    """
    # ptr = ctypes.cast(bind, POINTER(TaosMultiBind))
    # ptr = pointer(bind)
    _check_if_supported()
    res = _libtaos.taos_stmt_bind_param_batch(stmt, bind)
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


try:
    _libtaos.taos_stmt_bind_single_param_batch.restype = c_int
    _libtaos.taos_stmt_bind_single_param_batch.argstype = (c_void_p, c_void_p, c_int)
except Exception as err:
    _UNSUPPORTED["taos_stmt_bind_single_param_batch"] = err


def taos_stmt_bind_single_param_batch(stmt, bind, col):
    # type: (ctypes.c_void_p, Array[TaosMultiBind], c_int) -> None
    """Bind params in the statement query.
    @stmt: TAOS_STMT*
    @bind: TAOS_MULTI_BIND*
    @col: column index
    """
    _check_if_supported()
    res = _libtaos.taos_stmt_bind_single_param_batch(stmt, bind, col)
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


try:
    _libtaos.taos_stmt_add_batch.restype = c_int
    _libtaos.taos_stmt_add_batch.argstype = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["taos_stmt_add_batch"] = err


def taos_stmt_add_batch(stmt):
    # type: (ctypes.c_void_p) -> None
    """Add current params into batch
    @stmt: TAOS_STMT*
    """
    res = _libtaos.taos_stmt_add_batch(stmt)
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


try:
    _libtaos.taos_stmt_execute.restype = c_int
    _libtaos.taos_stmt_execute.argstype = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["taos_stmt_execute"] = err


def taos_stmt_execute(stmt):
    # type: (ctypes.c_void_p) -> None
    """Execute a statement query
    @stmt: TAOS_STMT*
    """
    res = _libtaos.taos_stmt_execute(stmt)
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


try:
    _libtaos.taos_stmt_use_result.restype = c_void_p
    _libtaos.taos_stmt_use_result.argstype = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["taos_stmt_use_result"] = err


def taos_stmt_use_result(stmt):
    # type: (ctypes.c_void_p) -> c_void_p
    """Get result of the statement.
    @stmt: TAOS_STMT*
    """
    _check_if_supported()
    result = c_void_p(_libtaos.taos_stmt_use_result(stmt))
    if result is None:
        raise StatementError(taos_stmt_errstr(stmt))
    return result


try:
    _libtaos.taos_stmt_affected_rows.restype = c_int
    _libtaos.taos_stmt_affected_rows.argstype = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["taos_stmt_affected_rows"] = err


def taos_stmt_affected_rows(stmt):
    # type: (ctypes.c_void_p) -> int
    """Get stmt affected rows.
    @stmt: TAOS_STMT*
    """
    _check_if_supported()
    return _libtaos.taos_stmt_affected_rows(stmt)


# taos_schemaless_insert

try:
    _libtaos.taos_schemaless_insert.restype = c_void_p
    _libtaos.taos_schemaless_insert.argstype = (
        c_void_p,
        c_void_p,
        c_int,
        c_int,
        c_int,
    )
except Exception as err:
    _UNSUPPORTED["taos_schemaless_insert"] = err

############################################ stmt2 begin ############################################

# char *taos_stmt2_error(TAOS_STMT2 *stmt);
try:
    _libtaos.taos_stmt2_error.restype = c_char_p
    _libtaos.taos_stmt2_error.argstype = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["taos_stmt2_error"] = err


def taos_stmt2_error(stmt):
    # type: (ctypes.c_void_p) -> str | None
    """
    Get error message from statement
    @stmt: c_void_p TAOS_STMT2*
    """
    _check_if_supported()
    err = c_char_p(_libtaos.taos_stmt2_error(stmt))
    if err:
        return err.value.decode("utf-8")
    else:
        return None


# TAOS_STMT2 *taos_stmt2_init(TAOS *taos, TAOS_STMT2_OPTION *option);
try:
    _libtaos.taos_stmt2_init.restype = c_void_p
    _libtaos.taos_stmt2_init.argtypes = (c_void_p, c_void_p)
except Exception as err:
    _UNSUPPORTED["taos_stmt2_init"] = err


def taos_stmt2_init(taos, option):
    # type: (ctypes.c_void_p, TaosStmt2OptionImpl|None) -> ctypes.c_void_p
    """
    Initialize a statement with options.
    @param taos: TAOS*, pointer to TAOS connection
    @param option: TAOS_STMT2_OPTION*, initialization options
    @return: c_void_p, pointer to initialized statement
    """
    _check_if_supported()
    option_ptr = ctypes.byref(option) if option is not None else c_void_p(None)
    stmt = c_void_p(_libtaos.taos_stmt2_init(taos, option_ptr))
    if not stmt:
        raise StatementError(msg="stmt2 init failed.")
    #
    return stmt


# int taos_stmt2_prepare(TAOS_STMT2 *stmt, const char *sql, unsigned long length);
try:
    _libtaos.taos_stmt2_prepare.restype = c_int
    _libtaos.taos_stmt2_prepare.argstype = (c_void_p, c_char_p, c_ulong)
except Exception as err:
    _UNSUPPORTED["taos_stmt2_prepare"] = err


def taos_stmt2_prepare(stmt, sql):
    # type: (ctypes.c_void_p, str) -> None
    """
    Prepare a statement query
    @stmt: c_void_p TAOS_STMT2*
    @sql: str SQL query
    """
    _check_if_supported()
    buffer = sql.encode("utf-8")
    res = _libtaos.taos_stmt2_prepare(stmt, ctypes.c_char_p(buffer), len(buffer))
    if res != 0:
        error_msg = taos_stmt2_error(stmt)
        raise StatementError(msg=error_msg, errno=res)
    #


# int taos_stmt2_bind_param(TAOS_STMT2 *stmt, TAOS_STMT2_BINDV *bindv, int32_t col_idx);
try:
    _libtaos.taos_stmt2_bind_param.restype = ctypes.c_int
    _libtaos.taos_stmt2_bind_param.argtypes = (ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int32)
except Exception as err:
    _UNSUPPORTED["taos_stmt2_bind_param"] = err


def taos_stmt2_bind_param(stmt, bindv, col_idx):
    # type: (ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int32) -> None
    """
    Bind params in the statement.
    @stmt: TAOS_STMT2*
    @bindv: TAOS_STMT2_BINDV*
    @col_idx: c_int32 column index
    """
    _check_if_supported()
    res = _libtaos.taos_stmt2_bind_param(stmt, bindv, col_idx)
    if res != 0:
        error_msg = taos_stmt2_error(stmt)
        raise StatementError(msg=error_msg, errno=res)


# int taos_stmt2_exec(TAOS_STMT2 *stmt, int *affected_rows);
try:
    _libtaos.taos_stmt2_exec.restype = c_int
    _libtaos.taos_stmt2_exec.argstype = (c_void_p, ctypes.POINTER(ctypes.c_int))
except Exception as err:
    _UNSUPPORTED["taos_stmt2_exec"] = err


def taos_stmt2_exec(stmt):
    # type: (ctypes.c_void_p) -> int
    """
    Execute a statement
    @stmt: c_void_p *TAOS_STMT2
    @return: int number of affected rows
    """
    _check_if_supported()
    affected_rows = ctypes.c_int(0)
    res = _libtaos.taos_stmt2_exec(stmt, ctypes.byref(affected_rows))
    if res != 0:
        error_msg = taos_stmt2_error(stmt)
        raise StatementError(msg=error_msg, errno=res)
    #
    return affected_rows.value


# int taos_stmt2_close(TAOS_STMT2 *stmt);
try:
    _libtaos.taos_stmt2_close.restype = c_int
    _libtaos.taos_stmt2_close.argstype = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["taos_stmt2_close"] = err


def taos_stmt2_close(stmt):
    # type: (ctypes.c_void_p) -> None
    """
    Close a statement
    @stmt: c_void_p TAOS_STMT2*
    """
    _check_if_supported()
    res = _libtaos.taos_stmt2_close(stmt)
    if res != 0:
        error_msg = taos_stmt2_error(stmt)
        raise StatementError(msg=error_msg, errno=res)
    #


# int taos_stmt2_is_insert(TAOS_STMT2 *stmt, int *insert);
try:
    _libtaos.taos_stmt2_is_insert.restype = c_int
    _libtaos.taos_stmt2_is_insert.argstype = (c_void_p, ctypes.POINTER(ctypes.c_int))
except Exception as err:
    _UNSUPPORTED["taos_stmt2_is_insert"] = err


def taos_stmt2_is_insert(stmt):
    # type: (ctypes.c_void_p) -> bool
    """
    Check if the statement is an insert statement.
    @stmt: c_void_p TAOS_STMT2*
    @return: bool True if it's an insert statement, False otherwise
    """
    _check_if_supported()
    is_insert = ctypes.c_int(0)
    res = _libtaos.taos_stmt2_is_insert(stmt, ctypes.byref(is_insert))
    if res != 0:
        error_msg = taos_stmt2_error(stmt)
        raise StatementError(msg=error_msg, errno=res)
    #
    return is_insert.value != 0


# int taos_stmt2_get_fields(TAOS_STMT2 *stmt, int *count, TAOS_FIELD_E **fields);
try:
    _libtaos.taos_stmt2_get_fields.restype = c_int
    _libtaos.taos_stmt2_get_fields.argstype = (c_void_p, c_int, ctypes.POINTER(ctypes.c_int), ctypes.c_void_p)
except Exception as err:
    _UNSUPPORTED["taos_stmt2_get_fields"] = err


# void taos_stmt2_free_fields(TAOS_STMT2 *stmt, TAOS_FIELD_E *fields);
try:
    _libtaos.taos_stmt2_free_fields.argstype = (c_void_p, c_int, ctypes.c_void_p)
except Exception as err:
    _UNSUPPORTED["taos_stmt2_free_fields"] = err

# define field_type
TAOS_FIELD_COL = 1
TAOS_FIELD_TAG = 2
TAOS_FIELD_QUERY = 3
TAOS_FIELD_TBNAME = 4


# get fields
def taos_stmt2_get_fields(stmt):
    # type: (ctypes.c_void_p) -> Tuple[int, List[TaosFieldAll]]
    """
    Get fields information for a given statement and field type.
    @stmt: c_void_p TAOS_STMT2*
    @return: Tuple(int, List[TAOS_FIELD_E]) count, list of fields
    """
    _check_if_supported()
    _check_if_supported("taos_stmt2_free_fields")
    count = ctypes.c_int(0)
    # fields_ptr = ctypes.c_void_p(0)
    # TODO: FIXME
    fields_ptr = ctypes.POINTER(TaosFieldAll)()

    res = _libtaos.taos_stmt2_get_fields(stmt, ctypes.byref(count), ctypes.byref(fields_ptr))
    if res != 0:
        error_msg = taos_stmt2_error(stmt)
        raise StatementError(msg=error_msg, errno=res)
    #

    # TODO: FIXME
    fields = []
    for i in range(count.value):
        field_c: TaosFieldAll = fields_ptr[i]
        field_py = TaosFieldAllCls(
            name=field_c.name,
            type=field_c.type,
            precision=field_c.precision,
            scale=field_c.scale,
            bytes_=field_c.bytes,
            field_type=field_c.field_type,
        )
        fields.append(field_py)

    _libtaos.taos_stmt2_free_fields(stmt, fields_ptr)
    return count.value, fields


# TAOS_RES *taos_stmt2_result(TAOS_STMT2 *stmt);
try:
    _libtaos.taos_stmt2_result.restype = c_void_p
    _libtaos.taos_stmt2_result.argstype = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["taos_stmt2_result"] = err


def taos_stmt2_result(stmt):
    # type: (ctypes.c_void_p) -> ctypes.c_void_p
    """
    Get the result set from a statement execution.
    @stmt: c_void_p TAOS_STMT2*
    @return: ctypes.POINTER(TAOS_RES) result set pointer
    """
    _check_if_supported()
    res = _libtaos.taos_stmt2_result(stmt)
    if not res:
        error_msg = _libtaos.taos_stmt2_error(stmt)
        raise StatementError(msg=error_msg, errno=-1)
    #
    return res


############################################ stmt2 end ############################################


def taos_schemaless_insert(
    connection: c_void_p,
    lines: Union[List[str], Tuple[str]],
    protocol: SmlProtocol,
    precision: SmlPrecision,
):
    _check_if_supported()
    num_of_lines = len(lines)
    lines = (c_char_p(line.encode("utf-8")) for line in lines)
    lines_type = ctypes.c_char_p * num_of_lines
    p_lines = lines_type(*lines)

    res = c_void_p(
        _libtaos.taos_schemaless_insert(
            connection,
            p_lines,
            num_of_lines,
            protocol,
            precision,
        )
    )
    errno = taos_errno(res)
    affected_rows = taos_affected_rows(res)
    if errno != 0:
        errstr = taos_errstr(res)
        taos_free_result(res)
        raise SchemalessError(errstr, errno, affected_rows)

    taos_free_result(res)
    return affected_rows


# taos_schemaless_insert_ttl

try:
    _libtaos.taos_schemaless_insert_ttl.restype = c_void_p
    _libtaos.taos_schemaless_insert_ttl.argstype = (
        c_void_p,
        c_void_p,
        c_int,
        c_int,
        c_int,
        c_int,
    )
except Exception as err:
    _UNSUPPORTED["taos_schemaless_insert_ttl"] = err


def taos_schemaless_insert_ttl(
    connection: c_void_p,
    lines: Union[List[str], Tuple[str]],
    protocol: SmlProtocol,
    precision: SmlPrecision,
    ttl: int,
):
    _check_if_supported()
    num_of_lines = len(lines)
    lines = (c_char_p(line.encode("utf-8")) for line in lines)
    lines_type = ctypes.c_char_p * num_of_lines
    p_lines = lines_type(*lines)

    res = c_void_p(
        _libtaos.taos_schemaless_insert_ttl(
            connection,
            p_lines,
            num_of_lines,
            protocol,
            precision,
            ttl,
        )
    )
    errno = taos_errno(res)
    affected_rows = taos_affected_rows(res)
    if errno != 0:
        errstr = taos_errstr(res)
        taos_free_result(res)
        raise SchemalessError(errstr, errno, affected_rows)

    taos_free_result(res)
    return affected_rows


# taos_schemaless_insert_ttl_with_reqid

try:
    _libtaos.taos_schemaless_insert_ttl_with_reqid.restype = c_void_p
    _libtaos.taos_schemaless_insert_ttl_with_reqid.argstype = (
        c_void_p,
        c_void_p,
        c_int,
        c_int,
        c_int,
        c_int,
        c_int,
    )
except Exception as err:
    _UNSUPPORTED["taos_schemaless_insert_ttl_with_reqid"] = err


def taos_schemaless_insert_ttl_with_reqid(
    connection: c_void_p,
    lines: Union[List[str], Tuple[str]],
    protocol: SmlProtocol,
    precision: SmlPrecision,
    ttl: int,
    req_id: int,
):
    _check_if_supported()
    num_of_lines = len(lines)
    lines = (c_char_p(line.encode("utf-8")) for line in lines)
    lines_type = ctypes.c_char_p * num_of_lines
    p_lines = lines_type(*lines)

    res = c_void_p(
        _libtaos.taos_schemaless_insert_ttl_with_reqid(
            connection,
            p_lines,
            num_of_lines,
            protocol,
            precision,
            ttl,
            req_id,
        )
    )
    errno = taos_errno(res)
    affected_rows = taos_affected_rows(res)
    if errno != 0:
        errstr = taos_errstr(res)
        taos_free_result(res)
        raise SchemalessError(errstr, errno, affected_rows)

    taos_free_result(res)
    return affected_rows


# taos_schemaless_insert_with_reqid

try:
    _libtaos.taos_schemaless_insert_with_reqid.restype = c_void_p
    _libtaos.taos_schemaless_insert_with_reqid.argstype = c_void_p, c_void_p, c_int, c_int, c_int, c_int
except Exception as err:
    _UNSUPPORTED["taos_schemaless_insert_with_reqid"] = err


def taos_schemaless_insert_with_reqid(connection, lines, protocol, precision, req_id):
    # type: (c_void_p, list[str] | tuple(str), SmlProtocol, SmlPrecision, int) -> int
    _check_if_supported()
    num_of_lines = len(lines)
    lines = (c_char_p(line.encode("utf-8")) for line in lines)
    lines_type = ctypes.c_char_p * num_of_lines
    p_lines = lines_type(*lines)
    res = c_void_p(
        _libtaos.taos_schemaless_insert_with_reqid(connection, p_lines, num_of_lines, protocol, precision, req_id)
    )
    errno = taos_errno(res)
    affected_rows = taos_affected_rows(res)
    if errno != 0:
        errstr = taos_errstr(res)
        taos_free_result(res)
        raise SchemalessError(errstr, errno, affected_rows)

    taos_free_result(res)
    return affected_rows


# taos_schemaless_insert_raw

try:
    _libtaos.taos_schemaless_insert_raw.restype = c_void_p
    _libtaos.taos_schemaless_insert_raw.argstype = (
        c_void_p,
        c_char_p,
        c_int,
        c_int,
        c_int,
        c_int,
    )
except Exception as err:
    _UNSUPPORTED["taos_schemaless_insert_raw"] = err


def taos_schemaless_insert_raw(
    connection: c_void_p,
    lines_raw: str,
    protocol: SmlProtocol,
    precision: SmlPrecision,
) -> int:
    _check_if_supported()
    length = len(lines_raw)
    lines_raw = c_char_p(lines_raw.encode("utf-8"))
    type_p_int = POINTER(c_int)
    total_rows = type_p_int(c_int(0))

    res = c_void_p(_libtaos.taos_schemaless_insert_raw(connection, lines_raw, length, total_rows, protocol, precision))

    errno = taos_errno(res)
    affected_rows = taos_affected_rows(res)

    # print(f"affected_rows: <{affected_rows}> "
    #       f"type: {type(affected_rows)} "
    #       f"total_rows: <{total_rows.contents.value}>")

    if errno != 0:
        errstr = taos_errstr(res)
        taos_free_result(res)
        raise SchemalessError(errstr, errno, affected_rows)

    taos_free_result(res)
    return affected_rows


# taos_schemaless_insert_raw_with_reqid

try:
    _libtaos.taos_schemaless_insert_raw_with_reqid.restype = c_void_p
    _libtaos.taos_schemaless_insert_raw_with_reqid.argstype = (
        c_void_p,
        c_char_p,
        c_int,
        c_int,
        c_int,
        c_int,
        c_int,
    )
except Exception as err:
    _UNSUPPORTED["taos_schemaless_insert_raw_with_reqid"] = err


def taos_schemaless_insert_raw_with_reqid(
    connection: c_void_p,
    lines_raw: str,
    protocol: SmlProtocol,
    precision: SmlPrecision,
    req_id: int,
) -> int:
    _check_if_supported()
    length = len(lines_raw)
    lines_raw = c_char_p(lines_raw.encode("utf-8"))
    type_p_int = POINTER(c_int)
    total_rows = type_p_int(c_int(0))

    res = c_void_p(
        _libtaos.taos_schemaless_insert_raw_with_reqid(
            connection,
            lines_raw,
            length,
            total_rows,
            protocol,
            precision,
            req_id,
        )
    )

    errno = taos_errno(res)
    affected_rows = taos_affected_rows(res)

    # print(f"affected_rows: <{affected_rows}> "
    #       f"type: {type(affected_rows)} "
    #       f"total_rows: <{total_rows.contents.value}>")

    if errno != 0:
        errstr = taos_errstr(res)
        taos_free_result(res)
        raise SchemalessError(errstr, errno, affected_rows)

    taos_free_result(res)
    return affected_rows


# taos_schemaless_insert_raw_ttl

try:
    _libtaos.taos_schemaless_insert_raw_ttl.restype = c_void_p
    _libtaos.taos_schemaless_insert_raw_ttl.argstype = (
        c_void_p,
        c_char_p,
        c_int,
        c_int,
        c_int,
        c_int,
        c_int,
    )
except Exception as err:
    _UNSUPPORTED["taos_schemaless_insert_raw_ttl"] = err


def taos_schemaless_insert_raw_ttl(
    connection: c_void_p,
    lines_raw: str,
    protocol: SmlProtocol,
    precision: SmlPrecision,
    ttl: int,
) -> int:
    _check_if_supported()
    length = len(lines_raw)
    lines_raw = c_char_p(lines_raw.encode("utf-8"))
    type_p_int = POINTER(c_int)
    total_rows = type_p_int(c_int(0))

    res = c_void_p(
        _libtaos.taos_schemaless_insert_raw_ttl(
            connection,
            lines_raw,
            length,
            total_rows,
            protocol,
            precision,
            ttl,
        )
    )

    errno = taos_errno(res)
    affected_rows = taos_affected_rows(res)

    # print(f"affected_rows: <{affected_rows}> "
    #       f"type: {type(affected_rows)} "
    #       f"total_rows: <{total_rows.contents.value}>")

    if errno != 0:
        errstr = taos_errstr(res)
        taos_free_result(res)
        raise SchemalessError(errstr, errno, affected_rows)

    taos_free_result(res)
    return affected_rows


# taos_schemaless_insert_raw_ttl_with_reqid

try:
    _libtaos.taos_schemaless_insert_raw_ttl_with_reqid.restype = c_void_p
    _libtaos.taos_schemaless_insert_raw_ttl_with_reqid.argstype = (
        c_void_p,
        c_char_p,
        c_int,
        c_int,
        c_int,
        c_int,
        c_int,
        c_int,
    )
except Exception as err:
    _UNSUPPORTED["taos_schemaless_insert_raw_ttl_with_reqid"] = err


def taos_schemaless_insert_raw_ttl_with_reqid(
    connection: c_void_p,
    lines_raw: str,
    protocol: SmlProtocol,
    precision: SmlPrecision,
    ttl: int,
    req_id: int,
) -> int:
    _check_if_supported()
    length = len(lines_raw)
    lines_raw = c_char_p(lines_raw.encode("utf-8"))
    type_p_int = POINTER(c_int)
    total_rows = type_p_int(c_int(0))

    res = c_void_p(
        _libtaos.taos_schemaless_insert_raw_ttl_with_reqid(
            connection,
            lines_raw,
            length,
            total_rows,
            protocol,
            precision,
            ttl,
            req_id,
        )
    )

    errno = taos_errno(res)
    affected_rows = taos_affected_rows(res)

    # print(f"affected_rows: <{affected_rows}> "
    #       f"type: {type(affected_rows)} "
    #       f"total_rows: <{total_rows.contents.value}>")

    if errno != 0:
        errstr = taos_errstr(res)
        taos_free_result(res)
        raise SchemalessError(errstr, errno, affected_rows)

    taos_free_result(res)
    return affected_rows


try:
    _libtaos.tmq_conf_new.restype = c_void_p
except Exception as err:
    _UNSUPPORTED["tmq_conf_new"] = err


def tmq_conf_new():
    # type: () -> c_void_p
    _check_if_supported()
    return c_void_p(_libtaos.tmq_conf_new())


try:
    _libtaos.tmq_conf_set.restype = c_int
    _libtaos.tmq_conf_set.argtypes = (c_void_p, c_char_p, c_char_p)
except Exception as err:
    _UNSUPPORTED["tmq_conf_set"] = err


def tmq_conf_set(conf, key, value):
    # type: (c_void_p, str, str) -> None
    _check_if_supported()

    if not isinstance(value, str):
        raise TmqError(msg=f"fail to execute tmq_conf_set({key},{value}), {value} is not string type")

    tmq_res = _libtaos.tmq_conf_set(conf, ctypes.c_char_p(key.encode("utf-8")), ctypes.c_char_p(value.encode("utf-8")))
    if tmq_res != 0:
        raise TmqError(msg=f"fail to execute tmq_conf_set({key},{value}), code={tmq_res}", errno=tmq_res)


try:
    _libtaos.tmq_conf_destroy.argtypes = (c_void_p,)
    _libtaos.tmq_conf_destroy.restype = None
except Exception as err:
    _UNSUPPORTED["tmq_conf_destroy"] = err


def tmq_conf_destroy(conf):
    # type (c_void_p,) -> None
    _check_if_supported()
    _libtaos.tmq_conf_destroy(conf)


tmq_commit_cb = CFUNCTYPE(None, c_void_p, c_int, c_void_p)

try:
    _libtaos.tmq_conf_set_auto_commit_cb.argtypes = (c_void_p, tmq_commit_cb, c_void_p)
    _libtaos.tmq_conf_set_auto_commit_cb.restype = None
except Exception as err:
    _UNSUPPORTED["tmq_conf_set_auto_commit_cb"] = err


def tmq_conf_set_auto_commit_cb(conf, cb, param):
    # type (c_void_p, tmq_commit_cb, c_void_p) -> None
    _check_if_supported()
    _libtaos.tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb(cb), param)


try:
    _libtaos.tmq_consumer_new.restype = c_void_p
    _libtaos.tmq_consumer_new.argtypes = (c_void_p, c_char_p, c_int)
except Exception as err:
    _UNSUPPORTED["tmq_consumer_new"] = err


def tmq_consumer_new(conf):
    # type: (c_void_p) -> c_void_p
    _check_if_supported()
    errstr = ctypes.create_string_buffer(256)
    tmq = cast(_libtaos.tmq_consumer_new(conf, errstr, 255), c_void_p)
    if tmq.value is None:
        err = errstr.value.decode("utf-8", errors="replace")
        raise TmqError(f"failed on tmq_consumer_new(), err: {err}")
    return tmq


try:
    _libtaos.tmq_err2str.restype = c_char_p
    _libtaos.tmq_err2str.argtypes = (c_int,)
except Exception as err:
    _UNSUPPORTED["tmq_err2str"] = err


def tmq_err2str(errno):
    # type (c_int) -> c_char_p
    _check_if_supported()
    return c_char_p(_libtaos.tmq_err2str(errno)).value.decode("utf-8")


try:
    _libtaos.tmq_list_new.restype = c_void_p
except Exception as err:
    _UNSUPPORTED["tmq_list_new"] = err


def tmq_list_new():
    # type () -> c_void_p
    _check_if_supported()
    return c_void_p(_libtaos.tmq_list_new())


try:
    _libtaos.tmq_list_append.restype = c_int
    _libtaos.tmq_list_append.argtypes = (c_void_p, c_char_p)
except Exception as err:
    _UNSUPPORTED["tmq_list_append"] = err


def tmq_list_append(list, topic):
    # type (c_void_p, c_char_p) -> None
    _check_if_supported()

    if not isinstance(topic, str):
        raise TmqError(f"topic value: {topic} is not string type")

    res = _libtaos.tmq_list_append(list, ctypes.c_char_p(topic.encode("utf-8")))
    return res


try:
    _libtaos.tmq_list_destroy.restype = None
    _libtaos.tmq_list_destroy.argtypes = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["tmq_list_destroy"] = err


def tmq_list_destroy(list):
    _check_if_supported()
    # type (c_void_p,) -> None
    _libtaos.tmq_list_destroy(list)


try:
    _libtaos.tmq_list_get_size.restype = c_int
    _libtaos.tmq_list_get_size.argtypes = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["tmq_list_get_size"] = err

try:
    _libtaos.tmq_list_to_c_array.restype = ctypes.POINTER(ctypes.c_char_p)
    _libtaos.tmq_list_to_c_array.argtypes = (c_void_p,)
except Exception as err:
    _UNSUPPORTED["tmq_list_to_c_array"] = err


def tmq_list_to_c_array(list):
    # type (c_void_p,) -> [string]
    _check_if_supported()
    _check_if_supported("tmq_list_get_size")
    c_array = _libtaos.tmq_list_to_c_array(list)
    size = _libtaos.tmq_list_get_size(list)
    res = []
    for i in range(size):
        res.append(c_array[i].decode("utf-8"))
    return res


try:
    _libtaos.tmq_subscribe.argtypes = (c_void_p, c_void_p)
    _libtaos.tmq_subscribe.restype = c_int
except Exception as err:
    _UNSUPPORTED["tmq_subscribe"] = err


def tmq_subscribe(tmq, list):
    # type (c_void_p, c_void_p) -> None
    _check_if_supported()
    res = _libtaos.tmq_subscribe(tmq, list)
    if res != 0:
        del list
        raise TmqError(msg=f"failed on tmq_subscribe(), errno={res:X}, errmsg={tmq_err2str(res)}", errno=res)


try:
    _libtaos.tmq_unsubscribe.argtypes = (c_void_p,)
    _libtaos.tmq_unsubscribe.restype = c_int
except Exception as err:
    _UNSUPPORTED["tmq_unsubscribe"] = err


def tmq_unsubscribe(tmq):
    # type (c_void_p,) -> None
    _check_if_supported()
    res = _libtaos.tmq_unsubscribe(tmq)
    if res == -1:
        # -1 means empty subscription topic list..
        # tmq_unsubscribe will subscribe a empty topic list to clear the consumer task.
        return
    if res != 0:
        raise TmqError(msg=f"failed on tmq_unsubscribe(), errno={res:X}, errmsg={tmq_err2str(res)}", errno=res)


try:
    _libtaos.tmq_subscription.argtypes = (c_void_p, c_void_p)
    _libtaos.tmq_subscription.restype = c_int
except Exception as err:
    _UNSUPPORTED["tmq_subscription"] = err


def tmq_subscription(tmq):
    # type (c_void_p, c_void_p) -> [string]
    _check_if_supported()
    topics = tmq_list_new()
    res = _libtaos.tmq_subscription(tmq, byref(topics))
    if res != 0:
        raise TmqError(msg=f"failed on tmq_subscription(), errno={res:X}, errmsg={tmq_err2str(res)}", errno=res)
    res = tmq_list_to_c_array(topics)
    tmq_list_destroy(topics)
    return res


try:
    _libtaos.tmq_consumer_poll.argtypes = (c_void_p, c_int64)
    _libtaos.tmq_consumer_poll.restype = c_void_p
except Exception as err:
    _UNSUPPORTED["tmq_consumer_poll"] = err


def tmq_consumer_poll(tmq, wait_time):
    # type (c_void_p, c_int64) -> c_void_p
    _check_if_supported()
    return c_void_p(_libtaos.tmq_consumer_poll(tmq, wait_time))


try:
    _libtaos.tmq_consumer_close.argtypes = (c_void_p,)
    _libtaos.tmq_consumer_close.restype = c_int
except Exception as err:
    _UNSUPPORTED["tmq_consumer_close"] = err


def tmq_consumer_close(tmq):
    # type (c_void_p,) -> None
    _check_if_supported()
    res = _libtaos.tmq_consumer_close(tmq)
    if res != 0:
        raise TmqError(msg=f"failed on tmq_consumer_close(), errno={res:X}, errmsg={tmq_err2str(res)}", errno=res)


try:
    _libtaos.tmq_commit_sync.argtypes = (c_void_p, c_void_p)
    _libtaos.tmq_commit_sync.restype = c_int
except Exception as err:
    _UNSUPPORTED["tmq_commit"] = err


def tmq_commit_sync(tmq, offset):
    # type: (c_void_p, c_void_p|None) -> None
    _check_if_supported()
    res = _libtaos.tmq_commit_sync(tmq, offset)
    if res != 0:
        raise TmqError(msg=f"failed on tmq_commit_sync(), errno={res:X}, errmsg={tmq_err2str(res)}", errno=res)


try:
    _libtaos.tmq_commit_offset_sync.argtypes = (c_void_p, c_char_p, c_int32, c_int64)
    _libtaos.tmq_commit_offset_sync.restype = c_int32
except Exception as err:
    _UNSUPPORTED["tmq_commit_offset_sync"] = err


def tmq_commit_offset_sync(tmq, topic, vg_id, offset):
    # type: (c_void_p, str, int, int) -> None
    _check_if_supported()
    res = _libtaos.tmq_commit_offset_sync(tmq, c_char_p(topic.encode("utf-8")), c_int32(vg_id), c_int64(offset))
    if res != 0:
        raise TmqError(msg=f"failed on tmq_commit_offset_sync(), errno={res:X}, errmsg={tmq_err2str(res)}", errno=res)


try:
    _libtaos.tmq_get_topic_name.argtypes = (c_void_p,)
    _libtaos.tmq_get_topic_name.restype = c_char_p
except Exception as err:
    _UNSUPPORTED["tmq_get_topic_name"] = err


def tmq_get_topic_name(res):
    # type: (c_void_p) -> str
    _check_if_supported()
    return _libtaos.tmq_get_topic_name(res).decode("utf-8")


try:
    _libtaos.tmq_get_vgroup_id.argtypes = (c_void_p,)
    _libtaos.tmq_get_vgroup_id.restype = c_int
except Exception as err:
    _UNSUPPORTED["tmq_get_vgroup_id"] = err


def tmq_get_vgroup_id(res):
    # type: (c_void_p) -> int
    _check_if_supported()
    return _libtaos.tmq_get_vgroup_id(res)


try:
    _libtaos.tmq_get_table_name.argtypes = (c_void_p,)
    _libtaos.tmq_get_table_name.restype = c_char_p
except Exception as err:
    _UNSUPPORTED["tmq_get_table_name"] = err


def tmq_get_table_name(res):
    # type: (c_void_p) -> str
    _check_if_supported()
    tb_name = _libtaos.tmq_get_table_name(res)
    if tb_name:
        return tb_name.decode("utf-8")
    # else:
    #     print("change msg.with.table.name to true in tmq config")


try:
    _libtaos.tmq_get_db_name.argtypes = (c_void_p,)
    _libtaos.tmq_get_db_name.restype = c_char_p
except Exception as err:
    _UNSUPPORTED["tmq_get_db_name"] = err


def tmq_get_db_name(res):
    # type: (c_void_p) -> str
    _check_if_supported()
    return _libtaos.tmq_get_db_name(res).decode("utf-8")


def _check_if_supported(func=None):
    if not func:
        func = inspect.stack()[1][3]
    if func in _UNSUPPORTED:
        raise InterfaceError(
            "C function %s is not supported in v%s: %s" % (func, taos_get_client_info(), _UNSUPPORTED[func])
        )


def unsupported_methods():
    for m, e in range(_UNSUPPORTED):
        print("unsupported %s: %s", m, e)


try:
    _libtaos.taos_get_table_vgId.argstype = (c_void_p, c_char_p, c_char_p, POINTER(c_int))
    _libtaos.taos_get_table_vgId.restype = c_int
except Exception as err:
    _UNSUPPORTED["taos_get_table_vgId"] = err


def taos_get_table_vgId(conn, db, table):
    # type: (c_void_p, str, str) -> int
    _check_if_supported()
    vg_id = c_int()
    code = _libtaos.taos_get_table_vgId(
        conn, c_char_p(db.encode("utf-8")), c_char_p(table.encode("utf-8")), ctypes.byref(vg_id)
    )
    if code != 0:
        raise InternalError(taos_errstr(c_void_p(None)))
    return vg_id.value


try:
    _libtaos.tmq_get_res_type.argstype = (c_void_p,)
    _libtaos.tmq_get_res_type.restype = c_int
except Exception as err:
    _UNSUPPORTED["tmq_get_res_type"] = err


def tmq_get_res_type(message):
    # type: (c_void_p) -> int
    return _libtaos.tmq_get_res_type(message)


try:
    _libtaos.tmq_get_vgroup_offset.argstype = (c_void_p,)
    _libtaos.tmq_get_vgroup_offset.restype = c_int64
except Exception as err:
    _UNSUPPORTED["tmq_get_vgroup_offset"] = err


def tmq_get_vgroup_offset(message):
    # type: (c_void_p) -> int
    return _libtaos.tmq_get_vgroup_offset(message)


class TmqTopicAssignment(Structure):
    _fields_ = [
        ("_vg_id", c_int32),
        ("_current_offset", c_int64),
        ("_begin", c_int64),
        ("_end", c_int64),
    ]

    @property
    def vg_id(self):
        return self._vg_id

    @property
    def current_offset(self):
        return self._current_offset

    @property
    def begin(self):
        return self._begin

    @property
    def end(self):
        return self._end

    def __str__(self):
        return "vg_id: %s, current_offset: %s, begin: %s, end: %s" % (
            self.vg_id,
            self.current_offset,
            self.begin,
            self.end,
        )


class TmqTopicAssignments(Structure):
    def __init__(self, assignments, count):
        self._assignments = []
        if isinstance(assignments, c_void_p):
            self._assignments = cast(assignments, POINTER(TmqTopicAssignment))
        if isinstance(assignments, POINTER(TmqTopicAssignment)):
            self._assignments = assignments
        self._count = count
        self._iter = 0

    def as_ptr(self):
        return self._assignments

    @property
    def count(self):
        return self._count

    @property
    def assignments(self):
        return self._assignments

    def next(self):
        return self._next()

    def _next(self):
        if self._iter < self.count:
            assignment = self._assignments[self._iter]
            self._iter += 1
            return assignment
        else:
            raise StopIteration

    def __next__(self):
        return self._next()

    def __getitem__(self, item):
        return self._assignments[item]

    def __iter__(self):
        self._iter = 0
        return self

    def __len__(self):
        return self._count


try:
    _libtaos.tmq_get_topic_assignment.argstype = (c_void_p, c_char_p, c_void_p, POINTER(c_int))
    _libtaos.tmq_get_topic_assignment.restype = c_int
except Exception as err:
    _UNSUPPORTED["tmq_get_topic_assignment"] = err


def tmq_get_topic_assignment(tmq, topic_name):
    # type: (c_void_p, str) -> List[Tuple]

    _check_if_supported()
    num_of_assignment = c_int()
    assignment = c_void_p()
    assignments = []
    code = _libtaos.tmq_get_topic_assignment(
        tmq, c_char_p(topic_name.encode("utf-8")), byref(assignment), ctypes.byref(num_of_assignment)
    )
    if code != 0:
        raise TmqError(
            msg=f"failed on tmq_get_topic_assignment(), errno={code:X}, errmsg={tmq_err2str(code)}", errno=code
        )

    tmq_assignments = TmqTopicAssignments(assignment, num_of_assignment.value)

    for tmq_assignment in tmq_assignments:
        assignments.append(
            (tmq_assignment.vg_id, tmq_assignment.current_offset, tmq_assignment.begin, tmq_assignment.end)
        )

    _libtaos.tmq_free_assignment(assignment)
    return assignments


try:
    _libtaos.tmq_offset_seek.argstype = (c_void_p, c_char_p, c_int32, c_int64)
    _libtaos.tmq_offset_seek.restype = c_int
except Exception as err:
    _UNSUPPORTED["tmq_offset_seek"] = err


def tmq_offset_seek(tmq, topic_name, vgroup_id, offset):
    code = _libtaos.tmq_offset_seek(tmq, c_char_p(topic_name.encode("utf-8")), c_int32(vgroup_id), c_int64(offset))
    if code != 0:
        raise TmqError(msg=f"failed on tmq_offset_seek(), errno={code:X}, errmsg={tmq_err2str(code)}", errno=code)


try:
    _libtaos.tmq_committed.argstype = (c_void_p, c_char_p, c_int32)
    _libtaos.tmq_committed.restype = c_int64
except Exception as err:
    _UNSUPPORTED["tmq_committed"] = err


def tmq_committed(tmq, topic, vgroup_id):
    # type: (c_void_p, str, int) -> int
    res = _libtaos.tmq_committed(tmq, c_char_p(topic.encode("utf-8")), c_int32(vgroup_id))
    if res < 0:
        raise TmqError(msg=f"failed on tmq_committed(), errno={res:X}, errmsg={tmq_err2str(res)}", errno=res)
    return res


try:
    _libtaos.tmq_position.argstype = (c_void_p, c_char_p, c_int32)
    _libtaos.tmq_position.restype = c_int64
except Exception as err:
    _UNSUPPORTED["tmq_position"] = err


def tmq_position(tmq, topic, vgroup_id):
    # type: (c_void_p, str, int) -> int
    offset = _libtaos.tmq_position(tmq, c_char_p(topic.encode("utf-8")), c_int32(vgroup_id))
    if offset < 0:
        raise TmqError(msg=f"failed on tmq_position(), errno={offset:X}, errmsg={tmq_err2str(offset)}", errno=offset)
    return offset


class CTaosInterface(object):
    def __init__(self, config=None, tz=None):
        """
        Function to initialize the class
        @host     : str, hostname to connect
        @user     : str, username to connect to server
        @password : str, password to connect to server
        @db       : str, default db to use when log in
        @config   : str, config directory

        @rtype    : None
        """
        if config is not None:
            taos_options(TaosOption.ConfigDir, config)

        if tz is not None:
            set_tz(pytz.timezone(tz))
            taos_options(TaosOption.Timezone, tz)

    @property
    def config(self):
        """Get current config"""
        return self._config

    def connect(self, host=None, user="root", password="taosdata", db=None, port=0):
        """
        Function to connect to server

        @rtype: c_void_p, TDengine handle
        """

        return taos_connect(host, user, password, db, port)

    def connect_totp(self, host=None, user="root", password="taosdata", totp=None, db=None, port=0):
        """
        Function to connect to server with TOTP authentication

        @rtype: c_void_p, TDengine handle
        """
        return taos_connect_totp(host, user, password, totp, db, port)

    def connect_token(self, host=None, token=None, db=None, port=0):
        """
        Function to connect to server with token authentication

        @rtype: c_void_p, TDengine handle
        """
        return taos_connect_token(host, token, db, port)
