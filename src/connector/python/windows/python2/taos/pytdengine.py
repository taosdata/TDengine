import ctypes
import numpy as np
import pandas as pd

class TaosField(ctypes.Structure):
    _fields_ = [('name'  , ctypes.c_char * 64),
                ('bytes' , ctypes.c_short),
                ('type'  , ctypes.c_char)]

class TaosClass(object):
    '''
    '''
    TSDB_DATA_TYPE_NULL      = 0
    TSDB_DATA_TYPE_BOOL      = 1 
    TSDB_DATA_TYPE_TINYINT   = 2 
    TSDB_DATA_TYPE_SMALLINT  = 3 
    TSDB_DATA_TYPE_INT       = 4 
    TSDB_DATA_TYPE_BIGINT    = 5 
    TSDB_DATA_TYPE_FLOAT     = 6 
    TSDB_DATA_TYPE_DOUBLE    = 7 
    TSDB_DATA_TYPE_BINARY    = 8 
    TSDB_DATA_TYPE_TIMESTAMP = 9 
    TSDB_DATA_TYPE_NCHAR     = 10

    libtaos = ctypes.windll.LoadLibrary('taos')

    libtaos.taos_fetch_fields.restype = ctypes.POINTER(TaosField)
    libtaos.taos_init.restype = None
    libtaos.taos_connect.restype = ctypes.c_void_p
    libtaos.taos_use_result.restype = ctypes.c_void_p
    libtaos.taos_fetch_row.restype = ctypes.POINTER(ctypes.c_void_p)
    libtaos.taos_errstr.restype = ctypes.c_char_p

    def __init__(self, host=None, user='root', password='taosdata', db=None, port=0, config=None):
        '''
        Function to initialize the class
        @host     : str, hostname to connect
        @user     : str, username to connect to server
        @password : str, password to connect to server
        @db       : str, default db to use when log in
        @config   : str, config directory

        @rtype    : None
        '''
        self.host          = ctypes.c_char_p(host)
        self.user          = ctypes.c_char_p(user)
        self.password      = ctypes.c_char_p(password)
        self.db            = ctypes.c_char_p(db)
        self.config        = ctypes.c_char_p(config)
        self.port          = ctypes.c_int(port)

        if config != None:
            TaosClass.libtaos.taos_options(2, self.config)

        TaosClass.libtaos.taos_init()

    def connect(self):
        '''
        Function to connect to server
        
        @rtype: c_void_p, TDengine handle
        '''
        connection = ctypes.c_void_p(TaosClass.libtaos.taos_connect(self.host, self.user, self.password, self.db, self.port))

        if connection.value == None:
            print('connect to TDengine failed')
            # sys.exit(1)
        else:
            print('connect to TDengine success')

        return connection

    @staticmethod
    def close(connection):
        '''
        Close the TDengine handle
        '''
        TaosClass.libtaos.taos_close(connection)
        print('connection is closed')

    @staticmethod
    def fetchBlock(result, fields):
        pblock = ctypes.c_void_p(0)
        num_of_rows = TaosClass.libtaos.taos_fetch_block(result, ctypes.byref(pblock))

        if num_of_rows == 0:
            return None, 0

        blocks = [None] * len(fields)
        for i in range(len(fields)):
            data = ctypes.cast(pblock, ctypes.POINTER(ctypes.c_void_p))[i]

            if (fields[i]['type'] == TaosClass.TSDB_DATA_TYPE_BOOL):
                if num_of_rows > 0:
                    blocks[i] = ctypes.cast(data,  ctypes.POINTER(ctypes.c_bool))[:abs(num_of_rows)][::-1]
                else:
                    blocks[i] = ctypes.cast(data,  ctypes.POINTER(ctypes.c_bool))[:abs(num_of_rows)]
            elif (fields[i]['type'] == TaosClass.TSDB_DATA_TYPE_TINYINT):
                if num_of_rows > 0:
                    blocks[i] = ctypes.cast(data,  ctypes.POINTER(ctypes.c_byte))[:abs(num_of_rows)][::-1]
                else:
                    blocks[i] = ctypes.cast(data,  ctypes.POINTER(ctypes.c_byte))[:abs(num_of_rows)]
            elif (fields[i]['type'] == TaosClass.TSDB_DATA_TYPE_SMALLINT):
                if num_of_rows > 0:
                    blocks[i] = ctypes.cast(data,  ctypes.POINTER(ctypes.c_short))[:abs(num_of_rows)][::-1]
                else:
                    blocks[i] = ctypes.cast(data,  ctypes.POINTER(ctypes.c_short))[:abs(num_of_rows)]
            elif (fields[i]['type'] == TaosClass.TSDB_DATA_TYPE_INT):
                if num_of_rows > 0:
                    blocks[i] = ctypes.cast(data,  ctypes.POINTER(ctypes.c_int))[:abs(num_of_rows)][::-1]
                else:
                    blocks[i] = ctypes.cast(data,  ctypes.POINTER(ctypes.c_int))[:abs(num_of_rows)]
            elif (fields[i]['type'] == TaosClass.TSDB_DATA_TYPE_BIGINT):
                if num_of_rows > 0:
                    blocks[i] = ctypes.cast(data,  ctypes.POINTER(ctypes.c_longlong))[:abs(num_of_rows)][::-1]
                else:
                    blocks[i] = ctypes.cast(data,  ctypes.POINTER(ctypes.c_longlong))[:abs(num_of_rows)]
            elif (fields[i]['type'] == TaosClass.TSDB_DATA_TYPE_FLOAT):
                if num_of_rows > 0:
                    blocks[i] = ctypes.cast(data,  ctypes.POINTER(ctypes.c_float))[:abs(num_of_rows)][::-1]
                else:
                    blocks[i] = ctypes.cast(data,  ctypes.POINTER(ctypes.c_float))[:abs(num_of_rows)]
            elif (fields[i]['type'] == TaosClass.TSDB_DATA_TYPE_DOUBLE):
                if num_of_rows > 0:
                    blocks[i] = ctypes.cast(data,  ctypes.POINTER(ctypes.c_double))[:abs(num_of_rows)][::-1]
                else:
                    blocks[i] = ctypes.cast(data,  ctypes.POINTER(ctypes.c_double))[:abs(num_of_rows)]
            elif (fields[i]['type'] == TaosClass.TSDB_DATA_TYPE_TIMESTAMP):
                if num_of_rows > 0:
                    blocks[i] = ctypes.cast(data,  ctypes.POINTER(ctypes.c_longlong))[:abs(num_of_rows)][::-1]
                else:
                    blocks[i] = ctypes.cast(data,  ctypes.POINTER(ctypes.c_longlong))[:abs(num_of_rows)]
            # TODO : Make it more efficient
            elif (fields[i]['type'] == TaosClass.TSDB_DATA_TYPE_BINARY):
                if num_of_rows > 0:
                    blocks[i] = [ele.value for ele in (ctypes.cast(data,  ctypes.POINTER(ctypes.c_char * fields[i]['bytes'])))[:abs(num_of_rows)][::-1]]
                else:
                    blocks[i] = [ele.value for ele in (ctypes.cast(data,  ctypes.POINTER(ctypes.c_char * fields[i]['bytes'])))[:abs(num_of_rows)]]
            elif (fields[i]['type'] == TaosClass.TSDB_DATA_TYPE_NCHAR):
                if num_of_rows > 0:
                    blocks[i] = [ele.value for ele in (ctypes.cast(data,  ctypes.POINTER(ctypes.c_wchar * (fields[i]['bytes']/4))))[:abs(num_of_rows)][::-1]]
                else:
                    blocks[i] = [ele.value for ele in (ctypes.cast(data,  ctypes.POINTER(ctypes.c_wchar * (fields[i]['bytes']/4))))[:abs(num_of_rows)]]

        return blocks, abs(num_of_rows)

    @staticmethod
    def query(connection, sql):
        '''
        Run SQL

        @sql: str, sql string to run

        @rtype: 0 on success and -1 on failure
        '''
        return TaosClass.libtaos.taos_query(connection, ctypes.c_char_p(sql))

    @staticmethod
    def useResult(connection):
        '''
        Use result after calling self.query
        '''
        result = ctypes.c_void_p(TaosClass.libtaos.taos_use_result(connection))
        fields = []
        pfields = TaosClass.fetchFields(result)
        for i in range(TaosClass.fieldsCount(connection)):
            fields.append({'name': pfields[i].name, 'bytes':pfields[i].bytes, 'type': ord(pfields[i].type)})

        return result, fields
 
    @staticmethod
    def freeResult(result):
        TaosClass.libtaos.taos_free_result(result)
        result.value = None

    @staticmethod
    def fieldsCount(connection):
        return TaosClass.libtaos.taos_field_count(connection)

    @staticmethod
    def fetchFields(result):
        return TaosClass.libtaos.taos_fetch_fields(result)

    @staticmethod
    def fetchRow(result, fields):
        l = []
        row = TaosClass.libtaos.taos_fetch_row(result)
        if not row: return None

        for i in range(len(fields)):
            l.append(TaosClass.getDataValue(row[i], fields[i]['type'], fields[i]['bytes']))

        return tuple(l)

    @staticmethod
    def getDataValue(data, dtype, byte):
        '''
        '''
        if not data: return None

        if (dtype == TaosClass.TSDB_DATA_TYPE_BOOL):
            return ctypes.cast(data,  ctypes.POINTER(ctypes.c_bool))[0]
        elif (dtype == TaosClass.TSDB_DATA_TYPE_TINYINT):
            return ctypes.cast(data,  ctypes.POINTER(ctypes.c_byte))[0]
        elif (dtype == TaosClass.TSDB_DATA_TYPE_SMALLINT):
            return ctypes.cast(data,  ctypes.POINTER(ctypes.c_short))[0]
        elif (dtype == TaosClass.TSDB_DATA_TYPE_INT):
            return ctypes.cast(data,  ctypes.POINTER(ctypes.c_int))[0]
        elif (dtype == TaosClass.TSDB_DATA_TYPE_BIGINT):
            return ctypes.cast(data,  ctypes.POINTER(ctypes.c_long))[0]
        elif (dtype == TaosClass.TSDB_DATA_TYPE_FLOAT):
            return ctypes.cast(data,  ctypes.POINTER(ctypes.c_float))[0]
        elif (dtype == TaosClass.TSDB_DATA_TYPE_DOUBLE):
            return ctypes.cast(data,  ctypes.POINTER(ctypes.c_double))[0]
        elif (dtype == TaosClass.TSDB_DATA_TYPE_BINARY):
            return (ctypes.cast(data,  ctypes.POINTER(ctypes.c_char))[0:byte]).rstrip('\x00')
        elif (dtype == TaosClass.TSDB_DATA_TYPE_TIMESTAMP):
            return ctypes.cast(data,  ctypes.POINTER(ctypes.c_long))[0]
        elif (dtype == TaosClass.TSDB_DATA_TYPE_NCHAR):
            return (ctypes.cast(data,  ctypes.c_char_p).value).rstrip('\x00')

    @staticmethod
    def affectedRows(connection):
        return TaosClass.libtaos.taos_affected_rows(connection)

    @staticmethod
    def errno(connection):
        return TaosClass.libtaos.taos_errno(connection)

    @staticmethod
    def errStr(connection):
        return TaosClass.libtaos.taos_errstr(connection)


class TaosCursor():
    '''
    Object in TDengine python client which the same as a connection to TDengine server.
    '''
    def __init__(self, connection):
        self.connection = connection
        # self.buffered   = buffered;
        self.result     = ctypes.c_void_p(0)
        self.fields     = []

        self.buffer     = None
        self.iter       = 0

    # def __iter__(self):
    #     self.iter = 0
    #     return self
    #
    # def next(self):
    #     if self.buffered:
    #         if self.iter >= len(self.buffer[0]):
    #             raise StopIteration
    #         else:
    #             return tuple(row[self.iter] for row in self.buffer)
    #     else:
    #         if self.iter >= len(self.buffer[0]):
    #             self.buffer, num_of_fields = TaosClass.fetchBlock(self.result, self.fields)
    #             if num_of_fields == 0:
    #                 raise StopIteration
    #             else:
    #                 self.iter = 1
    #                 return tuple(row[self.iter-1] for row in self.buffer)
    #         else:
    #             self.iter += 1
    #             l = tuple(row[self.iter-1] for row in self.buffer)

    def fetchall(self, format=list):
        '''
        Fetch data after run commands like 'show/select/describe' TaosCursor.execute.

        @format: list -> return a list of list, default and the fastest
                 dict -> return a dictionary with the name of each column as the key
                 numpy.array->return an array
                 pandas.DataFrame->return data as the form of pandas.DataFram

        @rtype: depends on the format
        '''
        if TaosClass.fieldsCount(self.connection) != 0:
            # select or show command
            self.result, self.fields = TaosClass.useResult(self.connection)
            self.iter = 0
            # if self.buffered:
            self.buffer = [[] for i in range(len(self.fields))]
            while True:
                block, num_of_fields = TaosClass.fetchBlock(self.result, self.fields)
                if num_of_fields == 0: break;
                for i in range(len(self.fields)):
                    self.buffer[i].extend(block[i])
            self.freeResult()

            if format == list:
                return self.buffer
            elif format == dict:
                return dict(zip(self.columns(), self.buffer))
            elif format == np.array:
                return [np.asarray(self.buffer[i]) for i in range(len(self.columns()))]
            elif format == pd.DataFrame:
                l = [np.asarray(self.buffer[i]) for i in range(len(self.columns()))]
                return pd.DataFrame.from_records(dict(zip(self.columns(), l)))
            else:
                return None
        else:
            return None

    def execute(self, sql):
        '''
        run sql command

        @rtype: int, 0 for succeed and others for failure
        '''
        # release previous result
        self.freeResult()

        res = TaosClass.query(self.connection, sql)
        if res != 0: return res
        else: return 0

    def freeResult(self):
        if self.result.value != None:
            TaosClass.freeResult(self.result)

    def columns(self):
        '''
        return the column names when query using TaosCursor.execute.

        @rtype: list of str
        '''
        return [self.fields[col]['name'] for col in range(len(self.fields))]

    def error(self):
        '''
        return error string of if execute is wrong

        @rtype: str
        '''
        return TaosClass.errStr(self.connection)

    def close(self):
        self.freeResult()
        TaosClass.close(self.connection)
        self.connection.value = None

class TaosConnection:
    '''
    TDengine connection object
    '''
    def __init__(self, host=None, user='root', passwd='taosdata', database=None, port=0, config=None):
        '''
        @host     : IP address of the TDengine server host
        @user     : user name to log in
        @password : password used to log in
        @database : database to use when logging in
        @port     : port number
        @config   : configuration directory
        '''
        self.taos = TaosClass(host, user, passwd, database, port, config)
        self.cursors = []

    def cursor(self):
        '''
        Generate a TaosCursor object, each object is the same as a connection to TDengine

        @rtype: TaosCursor
        '''
        self.cursors.append(TaosCursor(self.taos.connect()))
        return self.cursors[-1]

    def close(self):
        '''
        Close the connection
        '''
        for cur in self.cursors:
            cur.close()
        
def connector(host=None, user='root', passwd='taosdata', database=None, port=0, config=None):
    '''
    Function to create a TaosConnection object

    @host     : str, ipaddr of the TDengine server
    @user     : str, username used to login
    @passwd   : str, password used to login
    @database : str, database to use when connect, if database is not None and not exists on server, it will result in a connection failure
    @port     : port number
    @config   : configuration directory

    @rtype    : TaosConnection object
    '''
    return TaosConnection(host, user, passwd, database, port, config)
