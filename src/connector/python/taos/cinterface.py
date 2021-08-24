# encoding:UTF-8

import ctypes
import platform
import sys
from ctypes import *
try:
    from typing import Any
except:
    pass

from .error import *
from .bind import *
from .field import *


# stream callback
stream_callback_type = CFUNCTYPE(None, c_void_p, c_void_p, c_void_p)
stream_callback2_type = CFUNCTYPE(None, c_void_p)

# C interface class
class TaosOption:
    Locale = (0,)
    Charset = (1,)
    Timezone = (2,)
    ConfigDir = (3,)
    ShellActivityTimer = (4,)
    MaxOptions = (5,)


def _load_taos_linux():
    return ctypes.CDLL("libtaos.so")


def _load_taos_darwin():
    return ctypes.CDLL("libtaos.dylib")


def _load_taos_windows():
    return ctypes.windll.LoadLibrary("taos")


def _load_taos():
    load_func = {
        "Linux": _load_taos_linux,
        "Darwin": _load_taos_darwin,
        "Windows": _load_taos_windows,
    }
    try:
        return load_func[platform.system()]()
    except:
        sys.exit("unsupported platform to TDengine connector")


_libtaos = _load_taos()

_libtaos.taos_fetch_fields.restype = ctypes.POINTER(TaosField)
_libtaos.taos_init.restype = None
_libtaos.taos_connect.restype = ctypes.c_void_p
_libtaos.taos_fetch_row.restype = ctypes.POINTER(ctypes.c_void_p)
_libtaos.taos_errstr.restype = ctypes.c_char_p
_libtaos.taos_subscribe.restype = ctypes.c_void_p
_libtaos.taos_consume.restype = ctypes.c_void_p
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


def taos_options(option, *args):
    # type: (TaosOption, Any) -> None
    _libtaos.taos_options(option, *args)


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


_libtaos.taos_get_client_info.restype = c_char_p


def taos_get_client_info():
    # type: () -> str
    """Get client version info.
    获取客户端版本信息。
    """
    return _libtaos.taos_get_client_info().decode()


_libtaos.taos_get_server_info.restype = c_char_p
_libtaos.taos_get_server_info.argtypes = (c_void_p,)


def taos_get_server_info(connection):
    # type: (c_void_p) -> str
    return _libtaos.taos_get_server_info(connection).decode()


_libtaos.taos_close.restype = None
_libtaos.taos_close.argtypes = (c_void_p,)


def taos_close(connection):
    # type: (c_void_p) -> None
    """Close the TAOS* connection"""
    _libtaos.taos_close(connection)


_libtaos.taos_connect.restype = c_void_p
_libtaos.taos_connect.argtypes = c_char_p, c_char_p, c_char_p, c_char_p, c_uint16


def taos_connect(host=None, user="root", password="taosdata", db=None, port=0):
    # type: (None|str, str, str, None|str, int) -> c_void_p
    """Create TDengine database connection.
    创建数据库连接，初始化连接上下文。其中需要用户提供的参数包含：

    - host: server hostname/FQDN, TDengine管理主节点的FQDN
    - user: user name/用户名
    - password: user password / 用户密码
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
        raise ConnectionError("connect to TDengine failed")
    return connection


_libtaos.taos_connect_auth.restype = c_void_p
_libtaos.taos_connect_auth.argtypes = c_char_p, c_char_p, c_char_p, c_char_p, c_uint16


def taos_connect_auth(host=None, user="root", auth="", db=None, port=0):
    # type: (None|str, str, str, None|str, int) -> c_void_p
    """
    创建数据库连接，初始化连接上下文。其中需要用户提供的参数包含：

    - host: server hostname/FQDN, TDengine管理主节点的FQDN
    - user: user name/用户名
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
        _port = c_int(port)
    except TypeError:
        raise TypeError("port is expected as an int")

    connection = c_void_p(_libtaos.taos_connect_auth(_host, _user, _auth, _db, _port))

    if connection.value is None:
        raise ConnectionError("connect to TDengine failed")
    return connection


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


async_query_callback_type = CFUNCTYPE(None, c_void_p, c_void_p, c_int)
_libtaos.taos_query_a.restype = None
_libtaos.taos_query_a.argtypes = c_void_p, c_char_p, async_query_callback_type, c_void_p


def taos_query_a(connection, sql, callback, param):
    # type: (c_void_p, str, async_query_callback_type, c_void_p) -> c_void_p
    _libtaos.taos_query_a(connection, c_char_p(sql.encode("utf-8")), async_query_callback_type(callback), param)


async_fetch_rows_callback_type = CFUNCTYPE(None, c_void_p, c_void_p, c_int)
_libtaos.taos_fetch_rows_a.restype = None
_libtaos.taos_fetch_rows_a.argtypes = c_void_p, async_fetch_rows_callback_type, c_void_p


def taos_fetch_rows_a(result, callback, param):
    # type: (c_void_p, async_fetch_rows_callback_type, c_void_p) -> c_void_p
    _libtaos.taos_fetch_rows_a(result, async_fetch_rows_callback_type(callback), param)


def taos_affected_rows(result):
    # type: (c_void_p) -> c_int
    """The affected rows after runing query"""
    return _libtaos.taos_affected_rows(result)


subscribe_callback_type = CFUNCTYPE(None, c_void_p, c_void_p, c_void_p, c_int)
_libtaos.taos_subscribe.restype = c_void_p
# _libtaos.taos_subscribe.argtypes = c_void_p, c_int, c_char_p, c_char_p, subscribe_callback_type, c_void_p, c_int


def taos_subscribe(connection, restart, topic, sql, interval, callback=None, param=None):
    # type: (c_void_p, bool, str, str, c_int, subscribe_callback_type, c_void_p | None) -> c_void_p
    """Create a subscription
    @restart boolean,
    @sql string, sql statement for data query, must be a 'select' statement.
    @topic string, name of this subscription
    """
    if callback != None:
        callback = subscribe_callback_type(callback)
    if param != None:
        param = c_void_p(param)
    return c_void_p(
        _libtaos.taos_subscribe(
            connection,
            1 if restart else 0,
            c_char_p(topic.encode("utf-8")),
            c_char_p(sql.encode("utf-8")),
            callback or None,
            param,
            interval,
        )
    )


_libtaos.taos_consume.restype = c_void_p
_libtaos.taos_consume.argstype = c_void_p,


def taos_consume(sub):
    """Consume data of a subscription"""
    return c_void_p(_libtaos.taos_consume(sub))


_libtaos.taos_unsubscribe.restype = None
_libtaos.taos_unsubscribe.argstype = c_void_p, c_int


def taos_unsubscribe(sub, keep_progress):
    """Cancel a subscription"""
    _libtaos.taos_unsubscribe(sub, 1 if keep_progress else 0)


def taos_use_result(result):
    """Use result after calling self.query, it's just for 1.6."""
    fields = []
    pfields = taos_fetch_fields_raw(result)
    for i in range(taos_field_count(result)):
        fields.append(
            {
                "name": pfields[i].name,
                "bytes": pfields[i].bytes,
                "type": pfields[i].type,
            }
        )

    return fields


_libtaos.taos_fetch_block.restype = c_int
_libtaos.taos_fetch_block.argtypes = c_void_p, c_void_p


def taos_fetch_block_raw(result):
    pblock = ctypes.c_void_p(0)
    num_of_rows = _libtaos.taos_fetch_block(result, ctypes.byref(pblock))
    if num_of_rows == 0:
        return None, 0
    return pblock, abs(num_of_rows)


def taos_fetch_block(result, fields=None, field_count=None):
    pblock = ctypes.c_void_p(0)
    num_of_rows = _libtaos.taos_fetch_block(result, ctypes.byref(pblock))
    if num_of_rows == 0:
        return None, 0
    precision = taos_result_precision(result)
    if fields == None:
        fields = taos_fetch_fields(result)
    if field_count == None:
        field_count = taos_field_count(result)
    blocks = [None] * field_count
    fieldLen = taos_fetch_lengths(result, field_count)
    for i in range(len(fields)):
        data = ctypes.cast(pblock, ctypes.POINTER(ctypes.c_void_p))[i]
        if fields[i]["type"] not in CONVERT_FUNC:
            raise DatabaseError("Invalid data type returned from database")
        blocks[i] = CONVERT_FUNC_BLOCK[fields[i]["type"]](data, num_of_rows, fieldLen[i], precision)

    return blocks, abs(num_of_rows)


_libtaos.taos_fetch_row.restype = c_void_p
_libtaos.taos_fetch_row.argtypes = (c_void_p,)


def taos_fetch_row_raw(result):
    # type: (c_void_p) -> c_void_p
    row = c_void_p(_libtaos.taos_fetch_row(result))
    if row:
        return row
    return None


def taos_fetch_row(result, fields):
    # type: (c_void_p, Array[TaosField]) -> tuple(c_void_p, int)
    pblock = ctypes.c_void_p(0)
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
                blocks[i] = CONVERT_FUNC[fields[i].type](data, num_of_rows, field_lens[i], precision)
    else:
        return None, 0
    return blocks, abs(num_of_rows)


_libtaos.taos_free_result.argtypes = (c_void_p,)


def taos_free_result(result):
    # type: (c_void_p) -> None
    if result != None:
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


def taos_fetch_fields(result):
    # type: (c_void_p) -> TaosFields
    fields = taos_fetch_fields_raw(result)
    count = taos_field_count(result)
    return TaosFields(fields, count)


def taos_fetch_lengths(result, field_count=None):
    # type: (c_void_p, int) -> Array[int]
    """Make sure to call taos_fetch_row or taos_fetch_block before fetch_lengths"""
    lens = _libtaos.taos_fetch_lengths(result)
    if field_count == None:
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


_libtaos.taos_load_table_info.restype = c_int
_libtaos.taos_load_table_info.argstype = (c_void_p, c_char_p)


def taos_load_table_info(connection, tables):
    # type: (ctypes.c_void_p, str) -> None
    """Stop current query"""
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
    # type: (ctypes.c_void_p, ctypes.c_void_p | TaosFields, int, int) -> str
    """Print an row to string"""
    p = ctypes.create_string_buffer(buffer_size)
    if isinstance(fields, TaosFields):
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


try:
    _libtaos.taos_open_stream.restype = c_void_p
    _libtaos.taos_open_stream.argstype = c_void_p, c_char_p, stream_callback_type, c_int64, c_void_p, Any
except:
    pass


def taos_open_stream(connection, sql, callback, stime=0, param=None, callback2=None):
    # type: (ctypes.c_void_p, str, stream_callback_type, c_int64, c_void_p, c_void_p) -> ctypes.pointer
    if callback2 != None:
        callback2 = stream_callback2_type(callback2)
    """Open an stream"""
    return c_void_p(
        _libtaos.taos_open_stream(
            connection, ctypes.c_char_p(sql.encode("utf-8")), stream_callback_type(callback), stime, param, callback2
        )
    )


_libtaos.taos_close_stream.restype = None
_libtaos.taos_close_stream.argstype = (c_void_p,)


def taos_close_stream(stream):
    # type: (c_void_p) -> None
    """Open an stream"""
    return _libtaos.taos_close_stream(stream)


_libtaos.taos_stmt_init.restype = c_void_p
_libtaos.taos_stmt_init.argstype = (c_void_p,)


def taos_stmt_init(connection):
    # type: (c_void_p) -> (c_void_p)
    """Create a statement query
    @param(connection): c_void_p TAOS*
    @rtype: c_void_p, *TAOS_STMT
    """
    return c_void_p(_libtaos.taos_stmt_init(connection))


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
except AttributeError:
    print("WARNING: libtaos(%s) does not support taos_stmt_errstr" % taos_get_client_info())


def taos_stmt_errstr(stmt):
    # type: (ctypes.c_void_p) -> str
    """Get error message from stetement query
    @stmt: c_void_p TAOS_STMT*
    """
    err = c_char_p(_libtaos.taos_stmt_errstr(stmt))
    if err:
        return err.value.decode("utf-8")

try:
    _libtaos.taos_stmt_set_tbname.restype = c_int
    _libtaos.taos_stmt_set_tbname.argstype = (c_void_p, c_char_p)
except AttributeError:
    print("WARNING: libtaos(%s) does not support taos_stmt_set_tbname" % taos_get_client_info())



def taos_stmt_set_tbname(stmt, name):
    # type: (ctypes.c_void_p, str) -> None
    """Set table name of a statement query if exists.
    @stmt: c_void_p TAOS_STMT*
    """
    res = _libtaos.taos_stmt_set_tbname(stmt, c_char_p(name.encode("utf-8")))
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)

try:
    _libtaos.taos_stmt_set_tbname_tags.restype = c_int
    _libtaos.taos_stmt_set_tbname_tags.argstype = (c_void_p, c_char_p, c_void_p)
except AttributeError:
    print("WARNING: libtaos(%s) does not support taos_stmt_set_tbname_tags" % taos_get_client_info())



def taos_stmt_set_tbname_tags(stmt, name, tags):
    # type: (c_void_p, str, c_void_p) -> None
    """Set table name with tags bind params.
    @stmt: c_void_p TAOS_STMT*
    """
    res = _libtaos.taos_stmt_set_tbname_tags(stmt, ctypes.c_char_p(name.encode("utf-8")), tags)

    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


_libtaos.taos_stmt_is_insert.restype = c_int
_libtaos.taos_stmt_is_insert.argstype = (c_void_p, POINTER(c_int))


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


_libtaos.taos_stmt_num_params.restype = c_int
_libtaos.taos_stmt_num_params.argstype = (c_void_p, POINTER(c_int))


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


_libtaos.taos_stmt_bind_param.restype = c_int
_libtaos.taos_stmt_bind_param.argstype = (c_void_p, c_void_p)


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
except AttributeError:
    print("WARNING: libtaos(%s) does not support taos_stmt_bind_param_batch" % taos_get_client_info())



def taos_stmt_bind_param_batch(stmt, bind):
    # type: (ctypes.c_void_p, Array[TaosMultiBind]) -> None
    """Bind params in the statement query.
    @stmt: TAOS_STMT*
    @bind: TAOS_BIND*
    """
    # ptr = ctypes.cast(bind, POINTER(TaosMultiBind))
    # ptr = pointer(bind)
    res = _libtaos.taos_stmt_bind_param_batch(stmt, bind)
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)

try:
    _libtaos.taos_stmt_bind_single_param_batch.restype = c_int
    _libtaos.taos_stmt_bind_single_param_batch.argstype = (c_void_p, c_void_p, c_int)
except AttributeError:
    print("WARNING: libtaos(%s) does not support taos_stmt_bind_single_param_batch" % taos_get_client_info())


def taos_stmt_bind_single_param_batch(stmt, bind, col):
    # type: (ctypes.c_void_p, Array[TaosMultiBind], c_int) -> None
    """Bind params in the statement query.
    @stmt: TAOS_STMT*
    @bind: TAOS_MULTI_BIND*
    @col: column index
    """
    res = _libtaos.taos_stmt_bind_single_param_batch(stmt, bind, col)
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


_libtaos.taos_stmt_add_batch.restype = c_int
_libtaos.taos_stmt_add_batch.argstype = (c_void_p,)


def taos_stmt_add_batch(stmt):
    # type: (ctypes.c_void_p) -> None
    """Add current params into batch
    @stmt: TAOS_STMT*
    """
    res = _libtaos.taos_stmt_add_batch(stmt)
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


_libtaos.taos_stmt_execute.restype = c_int
_libtaos.taos_stmt_execute.argstype = (c_void_p,)


def taos_stmt_execute(stmt):
    # type: (ctypes.c_void_p) -> None
    """Execute a statement query
    @stmt: TAOS_STMT*
    """
    res = _libtaos.taos_stmt_execute(stmt)
    if res != 0:
        raise StatementError(msg=taos_stmt_errstr(stmt), errno=res)


_libtaos.taos_stmt_use_result.restype = c_void_p
_libtaos.taos_stmt_use_result.argstype = (c_void_p,)


def taos_stmt_use_result(stmt):
    # type: (ctypes.c_void_p) -> None
    """Get result of the statement.
    @stmt: TAOS_STMT*
    """
    result = c_void_p(_libtaos.taos_stmt_use_result(stmt))
    if result == None:
        raise StatementError(taos_stmt_errstr(stmt))
    return result

try:
    _libtaos.taos_insert_lines.restype = c_int
    _libtaos.taos_insert_lines.argstype = c_void_p, c_void_p, c_int
except AttributeError:
    print("WARNING: libtaos(%s) does not support insert_lines" % taos_get_client_info())




def taos_insert_lines(connection, lines):
    # type: (c_void_p, list[str] | tuple(str)) -> None
    num_of_lines = len(lines)
    lines = (c_char_p(line.encode("utf-8")) for line in lines)
    lines_type = ctypes.c_char_p * num_of_lines
    p_lines = lines_type(*lines)
    errno = _libtaos.taos_insert_lines(connection, p_lines, num_of_lines)
    if errno != 0:
        raise LinesError("insert lines error", errno)


class CTaosInterface(object):
    def __init__(self, config=None):
        """
        Function to initialize the class
        @host     : str, hostname to connect
        @user     : str, username to connect to server
        @password : str, password to connect to server
        @db       : str, default db to use when log in
        @config   : str, config directory

        @rtype    : None
        """
        if config is None:
            self._config = ctypes.c_char_p(None)
        else:
            try:
                self._config = ctypes.c_char_p(config.encode("utf-8"))
            except AttributeError:
                raise AttributeError("config is expected as a str")

        if config is not None:
            taos_options(3, self._config)

        taos_init()

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


if __name__ == "__main__":
    cinter = CTaosInterface()
    conn = cinter.connect()
    result = cinter.query(conn, "show databases")

    print("Query Affected rows: {}".format(cinter.affected_rows(result)))

    fields = taos_fetch_fields_raw(result)

    data, num_of_rows = taos_fetch_block(result, fields)

    print(data)

    cinter.free_result(result)
    cinter.close(conn)
