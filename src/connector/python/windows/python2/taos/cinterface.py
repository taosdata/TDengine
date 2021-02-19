import ctypes
from .constants import FieldType
from .error import *
import math
import datetime


def _convert_millisecond_to_datetime(milli):
    return datetime.datetime.fromtimestamp(milli / 1000.0)


def _convert_microsecond_to_datetime(micro):
    return datetime.datetime.fromtimestamp(micro / 1000000.0)


def _crow_timestamp_to_python(data, num_of_rows, nbytes=None, micro=False):
    """Function to convert C bool row to python row
    """
    _timestamp_converter = _convert_millisecond_to_datetime
    if micro:
        _timestamp_converter = _convert_microsecond_to_datetime

    if num_of_rows > 0:
        return list(map(_timestamp_converter, ctypes.cast(
            data, ctypes.POINTER(ctypes.c_longlong))[:abs(num_of_rows)]))
    else:
        return list(map(_timestamp_converter, ctypes.cast(
            data, ctypes.POINTER(ctypes.c_longlong))[:abs(num_of_rows)]))


def _crow_bool_to_python(data, num_of_rows, nbytes=None, micro=False):
    """Function to convert C bool row to python row
    """
    if num_of_rows > 0:
        return [
            None if ele == FieldType.C_BOOL_NULL else bool(ele) for ele in ctypes.cast(
                data, ctypes.POINTER(
                    ctypes.c_byte))[
                :abs(num_of_rows)]]
    else:
        return [
            None if ele == FieldType.C_BOOL_NULL else bool(ele) for ele in ctypes.cast(
                data, ctypes.POINTER(
                    ctypes.c_bool))[
                :abs(num_of_rows)]]


def _crow_tinyint_to_python(data, num_of_rows, nbytes=None, micro=False):
    """Function to convert C tinyint row to python row
    """
    if num_of_rows > 0:
        return [None if ele == FieldType.C_TINYINT_NULL else ele for ele in ctypes.cast(
            data, ctypes.POINTER(ctypes.c_byte))[:abs(num_of_rows)]]
    else:
        return [None if ele == FieldType.C_TINYINT_NULL else ele for ele in ctypes.cast(
            data, ctypes.POINTER(ctypes.c_byte))[:abs(num_of_rows)]]


def _crow_smallint_to_python(data, num_of_rows, nbytes=None, micro=False):
    """Function to convert C smallint row to python row
    """
    if num_of_rows > 0:
        return [
            None if ele == FieldType.C_SMALLINT_NULL else ele for ele in ctypes.cast(
                data, ctypes.POINTER(
                    ctypes.c_short))[
                :abs(num_of_rows)]]
    else:
        return [
            None if ele == FieldType.C_SMALLINT_NULL else ele for ele in ctypes.cast(
                data, ctypes.POINTER(
                    ctypes.c_short))[
                :abs(num_of_rows)]]


def _crow_int_to_python(data, num_of_rows, nbytes=None, micro=False):
    """Function to convert C int row to python row
    """
    if num_of_rows > 0:
        return [None if ele == FieldType.C_INT_NULL else ele for ele in ctypes.cast(
            data, ctypes.POINTER(ctypes.c_int))[:abs(num_of_rows)]]
    else:
        return [None if ele == FieldType.C_INT_NULL else ele for ele in ctypes.cast(
            data, ctypes.POINTER(ctypes.c_int))[:abs(num_of_rows)]]


def _crow_bigint_to_python(data, num_of_rows, nbytes=None, micro=False):
    """Function to convert C bigint row to python row
    """
    if num_of_rows > 0:
        return [None if ele == FieldType.C_BIGINT_NULL else ele for ele in ctypes.cast(
            data, ctypes.POINTER(ctypes.c_longlong))[:abs(num_of_rows)]]
    else:
        return [None if ele == FieldType.C_BIGINT_NULL else ele for ele in ctypes.cast(
            data, ctypes.POINTER(ctypes.c_longlong))[:abs(num_of_rows)]]


def _crow_float_to_python(data, num_of_rows, nbytes=None, micro=False):
    """Function to convert C float row to python row
    """
    if num_of_rows > 0:
        return [None if math.isnan(ele) else ele for ele in ctypes.cast(
            data, ctypes.POINTER(ctypes.c_float))[:abs(num_of_rows)]]
    else:
        return [None if math.isnan(ele) else ele for ele in ctypes.cast(
            data, ctypes.POINTER(ctypes.c_float))[:abs(num_of_rows)]]


def _crow_double_to_python(data, num_of_rows, nbytes=None, micro=False):
    """Function to convert C double row to python row
    """
    if num_of_rows > 0:
        return [None if math.isnan(ele) else ele for ele in ctypes.cast(
            data, ctypes.POINTER(ctypes.c_double))[:abs(num_of_rows)]]
    else:
        return [None if math.isnan(ele) else ele for ele in ctypes.cast(
            data, ctypes.POINTER(ctypes.c_double))[:abs(num_of_rows)]]


def _crow_binary_to_python(data, num_of_rows, nbytes=None, micro=False):
    """Function to convert C binary row to python row
    """
    assert(nbytes is not None)
    if num_of_rows > 0:
        return [None if ele.value[0:1] == FieldType.C_BINARY_NULL else ele.value.decode(
            'utf-8') for ele in (ctypes.cast(data, ctypes.POINTER(ctypes.c_char * nbytes)))[:abs(num_of_rows)]]
    else:
        return [None if ele.value[0:1] == FieldType.C_BINARY_NULL else ele.value.decode(
            'utf-8') for ele in (ctypes.cast(data, ctypes.POINTER(ctypes.c_char * nbytes)))[:abs(num_of_rows)]]


def _crow_nchar_to_python(data, num_of_rows, nbytes=None, micro=False):
    """Function to convert C nchar row to python row
    """
    assert(nbytes is not None)
    res = []
    for i in range(abs(num_of_rows)):
        try:
            if num_of_rows >= 0:
                tmpstr = ctypes.c_char_p(data)
                res.append(tmpstr.value.decode())
            else:
                res.append((ctypes.cast(data + nbytes * i,
                                        ctypes.POINTER(ctypes.c_wchar * (nbytes // 4))))[0].value)
        except ValueError:
            res.append(None)

    return res


def _crow_binary_to_python_block(data, num_of_rows, nbytes=None, micro=False):
    """Function to convert C binary row to python row
    """
    assert(nbytes is not None)
    res = []
    if num_of_rows > 0:
        for i in range(abs(num_of_rows)):
            try:
                rbyte = ctypes.cast(
                    data + nbytes * i,
                    ctypes.POINTER(
                        ctypes.c_short))[
                    :1].pop()
                tmpstr = ctypes.c_char_p(data + nbytes * i + 2)
                res.append(tmpstr.value.decode()[0:rbyte])
            except ValueError:
                res.append(None)
    else:
        for i in range(abs(num_of_rows)):
            try:
                rbyte = ctypes.cast(
                    data + nbytes * i,
                    ctypes.POINTER(
                        ctypes.c_short))[
                    :1].pop()
                tmpstr = ctypes.c_char_p(data + nbytes * i + 2)
                res.append(tmpstr.value.decode()[0:rbyte])
            except ValueError:
                res.append(None)
    return res


def _crow_nchar_to_python_block(data, num_of_rows, nbytes=None, micro=False):
    """Function to convert C nchar row to python row
    """
    assert(nbytes is not None)
    res = []
    if num_of_rows >= 0:
        for i in range(abs(num_of_rows)):
            try:
                tmpstr = ctypes.c_char_p(data + nbytes * i + 2)
                res.append(tmpstr.value.decode())
            except ValueError:
                res.append(None)
    else:
        for i in range(abs(num_of_rows)):
            try:
                res.append((ctypes.cast(data + nbytes * i + 2,
                                        ctypes.POINTER(ctypes.c_wchar * (nbytes // 4))))[0].value)
            except ValueError:
                res.append(None)
    return res


_CONVERT_FUNC = {
    FieldType.C_BOOL: _crow_bool_to_python,
    FieldType.C_TINYINT: _crow_tinyint_to_python,
    FieldType.C_SMALLINT: _crow_smallint_to_python,
    FieldType.C_INT: _crow_int_to_python,
    FieldType.C_BIGINT: _crow_bigint_to_python,
    FieldType.C_FLOAT: _crow_float_to_python,
    FieldType.C_DOUBLE: _crow_double_to_python,
    FieldType.C_BINARY: _crow_binary_to_python,
    FieldType.C_TIMESTAMP: _crow_timestamp_to_python,
    FieldType.C_NCHAR: _crow_nchar_to_python
}

_CONVERT_FUNC_BLOCK = {
    FieldType.C_BOOL: _crow_bool_to_python,
    FieldType.C_TINYINT: _crow_tinyint_to_python,
    FieldType.C_SMALLINT: _crow_smallint_to_python,
    FieldType.C_INT: _crow_int_to_python,
    FieldType.C_BIGINT: _crow_bigint_to_python,
    FieldType.C_FLOAT: _crow_float_to_python,
    FieldType.C_DOUBLE: _crow_double_to_python,
    FieldType.C_BINARY: _crow_binary_to_python_block,
    FieldType.C_TIMESTAMP: _crow_timestamp_to_python,
    FieldType.C_NCHAR: _crow_nchar_to_python_block
}

# Corresponding TAOS_FIELD structure in C


class TaosField(ctypes.Structure):
    _fields_ = [('name', ctypes.c_char * 65),
                ('type', ctypes.c_char),
                ('bytes', ctypes.c_short)]

# C interface class


class CTaosInterface(object):

    libtaos = ctypes.windll.LoadLibrary('taos')

    libtaos.taos_fetch_fields.restype = ctypes.POINTER(TaosField)
    libtaos.taos_init.restype = None
    libtaos.taos_connect.restype = ctypes.c_void_p
    #libtaos.taos_use_result.restype = ctypes.c_void_p
    libtaos.taos_fetch_row.restype = ctypes.POINTER(ctypes.c_void_p)
    libtaos.taos_errstr.restype = ctypes.c_char_p
    libtaos.taos_subscribe.restype = ctypes.c_void_p
    libtaos.taos_consume.restype = ctypes.c_void_p
    libtaos.taos_fetch_lengths.restype = ctypes.c_void_p
    libtaos.taos_free_result.restype = None
    libtaos.taos_errno.restype = ctypes.c_int
    libtaos.taos_query.restype = ctypes.POINTER(ctypes.c_void_p)

    def __init__(self, config=None):
        '''
        Function to initialize the class
        @host     : str, hostname to connect
        @user     : str, username to connect to server
        @password : str, password to connect to server
        @db       : str, default db to use when log in
        @config   : str, config directory

        @rtype    : None
        '''
        if config is None:
            self._config = ctypes.c_char_p(None)
        else:
            try:
                self._config = ctypes.c_char_p(config.encode('utf-8'))
            except AttributeError:
                raise AttributeError("config is expected as a str")

        if config is not None:
            CTaosInterface.libtaos.taos_options(3, self._config)

        CTaosInterface.libtaos.taos_init()

    @property
    def config(self):
        """ Get current config
        """
        return self._config

    def connect(
            self,
            host=None,
            user="root",
            password="taosdata",
            db=None,
            port=0):
        '''
        Function to connect to server

        @rtype: c_void_p, TDengine handle
        '''
        # host
        try:
            _host = ctypes.c_char_p(host.encode(
                "utf-8")) if host is not None else ctypes.c_char_p(None)
        except AttributeError:
            raise AttributeError("host is expected as a str")

        # user
        try:
            _user = ctypes.c_char_p(user.encode("utf-8"))
        except AttributeError:
            raise AttributeError("user is expected as a str")

        # password
        try:
            _password = ctypes.c_char_p(password.encode("utf-8"))
        except AttributeError:
            raise AttributeError("password is expected as a str")

        # db
        try:
            _db = ctypes.c_char_p(
                db.encode("utf-8")) if db is not None else ctypes.c_char_p(None)
        except AttributeError:
            raise AttributeError("db is expected as a str")

        # port
        try:
            _port = ctypes.c_int(port)
        except TypeError:
            raise TypeError("port is expected as an int")

        connection = ctypes.c_void_p(CTaosInterface.libtaos.taos_connect(
            _host, _user, _password, _db, _port))

        if connection.value is None:
            print('connect to TDengine failed')
            raise ConnectionError("connect to TDengine failed")
            # sys.exit(1)
        # else:
        #    print('connect to TDengine success')

        return connection

    @staticmethod
    def close(connection):
        '''Close the TDengine handle
        '''
        CTaosInterface.libtaos.taos_close(connection)
        #print('connection is closed')

    @staticmethod
    def query(connection, sql):
        '''Run SQL

        @sql: str, sql string to run

        @rtype: 0 on success and -1 on failure
        '''
        try:
            return CTaosInterface.libtaos.taos_query(
                connection, ctypes.c_char_p(sql.encode('utf-8')))
        except AttributeError:
            raise AttributeError("sql is expected as a string")
        # finally:
        #     CTaosInterface.libtaos.close(connection)

    @staticmethod
    def affectedRows(result):
        """The affected rows after runing query
        """
        return CTaosInterface.libtaos.taos_affected_rows(result)

    @staticmethod
    def subscribe(connection, restart, topic, sql, interval):
        """Create a subscription
         @restart boolean,
         @sql string, sql statement for data query, must be a 'select' statement.
         @topic string, name of this subscription
        """
        return ctypes.c_void_p(CTaosInterface.libtaos.taos_subscribe(
            connection,
            1 if restart else 0,
            ctypes.c_char_p(topic.encode('utf-8')),
            ctypes.c_char_p(sql.encode('utf-8')),
            None,
            None,
            interval))

    @staticmethod
    def consume(sub):
        """Consume data of a subscription
        """
        result = ctypes.c_void_p(CTaosInterface.libtaos.taos_consume(sub))
        fields = []
        pfields = CTaosInterface.fetchFields(result)
        for i in range(CTaosInterface.libtaos.taos_num_fields(result)):
            fields.append({'name': pfields[i].name.decode('utf-8'),
                           'bytes': pfields[i].bytes,
                           'type': ord(pfields[i].type)})
        return result, fields

    @staticmethod
    def unsubscribe(sub, keepProgress):
        """Cancel a subscription
        """
        CTaosInterface.libtaos.taos_unsubscribe(sub, 1 if keepProgress else 0)

    @staticmethod
    def useResult(result):
        '''Use result after calling self.query
        '''
        fields = []
        pfields = CTaosInterface.fetchFields(result)
        for i in range(CTaosInterface.fieldsCount(result)):
            fields.append({'name': pfields[i].name.decode('utf-8'),
                           'bytes': pfields[i].bytes,
                           'type': ord(pfields[i].type)})

        return fields

    @staticmethod
    def fetchBlock(result, fields):
        pblock = ctypes.c_void_p(0)
        num_of_rows = CTaosInterface.libtaos.taos_fetch_block(
            result, ctypes.byref(pblock))
        if num_of_rows == 0:
            return None, 0
        isMicro = (CTaosInterface.libtaos.taos_result_precision(
            result) == FieldType.C_TIMESTAMP_MICRO)
        blocks = [None] * len(fields)
        fieldL = CTaosInterface.libtaos.taos_fetch_lengths(result)
        fieldLen = [
            ele for ele in ctypes.cast(
                fieldL, ctypes.POINTER(
                    ctypes.c_int))[
                :len(fields)]]
        for i in range(len(fields)):
            data = ctypes.cast(pblock, ctypes.POINTER(ctypes.c_void_p))[i]
            if fields[i]['type'] not in _CONVERT_FUNC_BLOCK:
                raise DatabaseError("Invalid data type returned from database")
            blocks[i] = _CONVERT_FUNC_BLOCK[fields[i]['type']](
                data, num_of_rows, fieldLen[i], isMicro)

        return blocks, abs(num_of_rows)

    @staticmethod
    def fetchRow(result, fields):
        pblock = ctypes.c_void_p(0)
        pblock = CTaosInterface.libtaos.taos_fetch_row(result)
        if pblock:
            num_of_rows = 1
            isMicro = (CTaosInterface.libtaos.taos_result_precision(
                result) == FieldType.C_TIMESTAMP_MICRO)
            blocks = [None] * len(fields)
            fieldL = CTaosInterface.libtaos.taos_fetch_lengths(result)
            fieldLen = [
                ele for ele in ctypes.cast(
                    fieldL, ctypes.POINTER(
                        ctypes.c_int))[
                    :len(fields)]]
            for i in range(len(fields)):
                data = ctypes.cast(pblock, ctypes.POINTER(ctypes.c_void_p))[i]
                if fields[i]['type'] not in _CONVERT_FUNC:
                    raise DatabaseError(
                        "Invalid data type returned from database")
                if data is None:
                    blocks[i] = [None]
                else:
                    blocks[i] = _CONVERT_FUNC[fields[i]['type']](
                        data, num_of_rows, fieldLen[i], isMicro)
        else:
            return None, 0
        return blocks, abs(num_of_rows)

    @staticmethod
    def freeResult(result):
        CTaosInterface.libtaos.taos_free_result(result)
        result.value = None

    @staticmethod
    def fieldsCount(result):
        return CTaosInterface.libtaos.taos_field_count(result)

    @staticmethod
    def fetchFields(result):
        return CTaosInterface.libtaos.taos_fetch_fields(result)

    # @staticmethod
    # def fetchRow(result, fields):
    #     l = []
    #     row = CTaosInterface.libtaos.taos_fetch_row(result)
    #     if not row:
    #         return None

    #     for i in range(len(fields)):
    #         l.append(CTaosInterface.getDataValue(
    #             row[i], fields[i]['type'], fields[i]['bytes']))

    #     return tuple(l)

    # @staticmethod
    # def getDataValue(data, dtype, byte):
    #     '''
    #     '''
    #     if not data:
    #         return None

    #     if (dtype == CTaosInterface.TSDB_DATA_TYPE_BOOL):
    #         return ctypes.cast(data,  ctypes.POINTER(ctypes.c_bool))[0]
    #     elif (dtype == CTaosInterface.TSDB_DATA_TYPE_TINYINT):
    #         return ctypes.cast(data,  ctypes.POINTER(ctypes.c_byte))[0]
    #     elif (dtype == CTaosInterface.TSDB_DATA_TYPE_SMALLINT):
    #         return ctypes.cast(data,  ctypes.POINTER(ctypes.c_short))[0]
    #     elif (dtype == CTaosInterface.TSDB_DATA_TYPE_INT):
    #         return ctypes.cast(data,  ctypes.POINTER(ctypes.c_int))[0]
    #     elif (dtype == CTaosInterface.TSDB_DATA_TYPE_BIGINT):
    #         return ctypes.cast(data,  ctypes.POINTER(ctypes.c_long))[0]
    #     elif (dtype == CTaosInterface.TSDB_DATA_TYPE_FLOAT):
    #         return ctypes.cast(data,  ctypes.POINTER(ctypes.c_float))[0]
    #     elif (dtype == CTaosInterface.TSDB_DATA_TYPE_DOUBLE):
    #         return ctypes.cast(data,  ctypes.POINTER(ctypes.c_double))[0]
    #     elif (dtype == CTaosInterface.TSDB_DATA_TYPE_BINARY):
    #         return (ctypes.cast(data,  ctypes.POINTER(ctypes.c_char))[0:byte]).rstrip('\x00')
    #     elif (dtype == CTaosInterface.TSDB_DATA_TYPE_TIMESTAMP):
    #         return ctypes.cast(data,  ctypes.POINTER(ctypes.c_long))[0]
    #     elif (dtype == CTaosInterface.TSDB_DATA_TYPE_NCHAR):
    #         return (ctypes.cast(data,  ctypes.c_char_p).value).rstrip('\x00')

    @staticmethod
    def errno(result):
        """Return the error number.
        """
        return CTaosInterface.libtaos.taos_errno(result)

    @staticmethod
    def errStr(result):
        """Return the error styring
        """
        return CTaosInterface.libtaos.taos_errstr(result).decode('utf-8')


if __name__ == '__main__':
    cinter = CTaosInterface()
    conn = cinter.connect()
    result = cinter.query(conn, 'show databases')

    print('Query Affected rows: {}'.format(cinter.affectedRows(result)))

    fields = CTaosInterface.useResult(result)

    data, num_of_rows = CTaosInterface.fetchBlock(result, fields)

    print(data)

    cinter.freeResult(result)
    cinter.close(conn)
