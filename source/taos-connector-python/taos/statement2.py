# encoding:UTF-8
from taos.cinterface import *
from taos.error import StatementError
from taos.result import TaosResult
from taos import bind2
from taos import log
from taos import utils
from taos.precision import PrecisionEnum

_taos_async_fn_t = ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int)


#
# Bind data with table
#
class BindTable(object):
    def __init__(self, name, tags):
        self.name = name
        self.tags = tags
        self.datas = []

    def add_col_data(self, data):
        """Add column data to the table"""
        self.datas.append(data)


class TaosStmt2OptionImpl(ctypes.Structure):
    _fields_ = [
        ("reqid", ctypes.c_int64),
        ("singleStbInsert", ctypes.c_bool),
        ("singleTableBindOnce", ctypes.c_bool),
        ("asyncExecFn", _taos_async_fn_t),
        ("userdata", ctypes.c_void_p),
    ]


def taos_stmt2_async_exec(userdata, result_set, error_code):
    print(f"Executing asynchronously with userdata: {userdata}, result_set: {result_set}, error_code: {error_code}")


class TaosStmt2Option:
    def __init__(
        self, reqid: int = None, single_stb_insert: bool = False, single_table_bind_once: bool = False, **kwargs
    ):
        self._impl = TaosStmt2OptionImpl()
        if reqid is None:
            reqid = utils.gen_req_id()
        else:
            if type(reqid) is not int:
                raise StatementError(f"reqid type error, expected int type but got {type(reqid)}.")

        self.reqid = reqid
        self.single_stb_insert = single_stb_insert
        self.single_table_bind_once = single_table_bind_once
        self.is_async = kwargs.get("is_async", False)
        self.async_exec_fn = taos_stmt2_async_exec if self.is_async else _taos_async_fn_t()
        self.userdata = ctypes.c_void_p(None)

    @property
    def reqid(self) -> int:
        return self._impl.reqid

    @reqid.setter
    def reqid(self, value: int):
        self._impl.reqid = ctypes.c_int64(value)

    @property
    def single_stb_insert(self) -> bool:
        return self._impl.singleStbInsert

    @single_stb_insert.setter
    def single_stb_insert(self, value: bool):
        self._impl.singleStbInsert = ctypes.c_bool(value)

    @property
    def single_table_bind_once(self) -> bool:
        return self._impl.singleTableBindOnce

    @single_table_bind_once.setter
    def single_table_bind_once(self, value: bool):
        self._impl.singleTableBindOnce = ctypes.c_bool(value)

    @property
    def async_exec_fn(self) -> _taos_async_fn_t:
        return self._impl.asyncExecFn

    @async_exec_fn.setter
    def async_exec_fn(self, value: _taos_async_fn_t):
        if value is not None and not isinstance(value, _taos_async_fn_t):
            raise TypeError("async_exec_fn must be an instance of _taos_async_fn_t or None")
        self._impl.asyncExecFn = value

    @property
    def userdata(self) -> ctypes.c_void_p:
        return self._impl.userdata

    @userdata.setter
    def userdata(self, value: ctypes.c_void_p):
        self._impl.userdata = value

    def get_impl(self):
        return self._impl


#
#    ------------- global fun define -------------
#


def checkConsistent(tbnames, tags, datas):
    # todo
    return True


def obtainSchema(statement2):
    """Obtain schema information from statement2 object"""
    if statement2._stmt2 is None:
        raise StatementError("stmt2 object is null.")

    try:
        _, fields = statement2.get_fields()
        statement2.tag_fields = []
        statement2.fields = []
        statement2.all_fields = fields
        statement2.is_tbname = False
        for field in fields:
            if field.field_type == TAOS_FIELD_TAG:
                statement2.tag_fields.append(field)
            elif field.field_type == TAOS_FIELD_COL:
                statement2.fields.append(field)
            elif field.field_type == TAOS_FIELD_TBNAME:
                statement2.is_tbname = True
        log.debug(f"obtain schema tag fields = {statement2.tag_fields}")
        log.debug(f"obtain schema fields     = {statement2.fields}")
    except Exception as err:
        log.debug(f"obtain schema tag/col fields failed, reason: {repr(err)}")
        return False

    return len(statement2.fields) > 0


def getField(statement2, index, isTag):
    """Get field information by index and type"""
    if isTag:
        return statement2.tag_fields[index]
    else:
        return statement2.fields[index]


def createTagsBind(statement2, tags_tables):
    """Create stmt2Bind list from tags"""
    binds = []
    # Process tables tags
    for tags_table in tags_tables:
        # Process single table
        n = len(tags_table)
        binds_table = bind2.new_stmt2_binds(n)
        for i in range(n):
            field = getField(statement2, i, True)
            values = [tags_table[i]]
            log.debug(
                f"tag i = {i} type={field.type} precision={field.precision} length={field.bytes} values = {values} tags_table = {tags_table}\n"
            )
            binds_table[i].set_value(field.type, values, field.precision)
        binds.append(binds_table)

    return binds


def createColsBind(statement2, cols_tables):
    """Create stmt2Bind list from columns"""
    binds = []
    # Process tables columns data
    for cols_table in cols_tables:
        # Process single table
        n = len(cols_table)
        binds_table = bind2.new_stmt2_binds(n)
        for i in range(n):
            field = getField(statement2, i, isTag=False)
            binds_table[i].set_value(field.type, cols_table[i], field.precision)
        binds.append(binds_table)

    return binds


def createSuperBindV(statement2, cols_tables):
    """Create super table bind for automatic table name and tag binding"""
    if cols_tables is None:
        raise StatementError("data bind params is None.")
    bind_names = []
    bind_tags = []
    bind_cols = []
    tag_count = len(statement2.tag_fields)
    cols_count = len(statement2.all_fields)
    for cols_table in cols_tables:
        cols_bind = bind2.new_stmt2_binds(cols_count)
        tags_bind = bind2.new_stmt2_binds(tag_count)
        tag_index = 0
        cols_index = 0
        for i in range(cols_count):
            field = statement2.all_fields[i]

            log.debug(
                f"index i = {i} type={field.type} precision={field.precision} length={field.bytes} cols_table = {cols_table[i]}\n"
            )

            if field.field_type == TAOS_FIELD_TAG:
                values = [cols_table[i]]
                tags_bind[tag_index].set_value(field.type, values, field.precision)
                tag_index += 1
            elif field.field_type == TAOS_FIELD_COL:
                values = [cols_table[i]]
                cols_bind[cols_index].set_value(field.type, values, field.precision)
                cols_index += 1
            elif field.field_type == TAOS_FIELD_TBNAME:
                bind_names.append(cols_table[i])

        bind_tags.append(tags_bind)
        bind_cols.append(cols_bind)

    return bind2.new_bindv(len(bind_names), bind_names, bind_tags, bind_cols)


def createQueryBindV(statement2, datas):
    """Create bind for query operations"""
    if statement2._stmt2 is None:
        raise StatementError("stmt2 object is null.")

    # Create bindv
    ret = utils.detectListNone(datas)
    if ret == utils.ALL_NONE or ret == utils.HAVE_NONE:
        raise StatementError("params datas some is None, some is not None, this is error.")

    types = []
    query_array = []
    # Process tables columns data
    for cols_table in datas:
        n = len(cols_table)
        for i in range(n):
            value = cols_table[i]
            if isinstance(value, (list, tuple, set)):
                value = value[0]
            else:
                query_array.append([value])
            if isinstance(value, bool):
                types.append(FieldType.C_BOOL)
            elif isinstance(value, int):
                if value > 0:
                    types.append(FieldType.C_BIGINT_UNSIGNED)
                else:
                    types.append(FieldType.C_BIGINT)
            elif isinstance(value, float):
                types.append(FieldType.C_DOUBLE)
            elif isinstance(value, str):
                types.append(FieldType.C_BINARY)
            else:
                raise StatementError(
                    f"data type not support, only support int/float/str/bool type, but got {type(value)}"
                )

            log.debug(f"createQueryBindV cols_table={cols_table[i]} {type(cols_table[i])}")

    if len(query_array) > 0:
        datas = [query_array]

    statement2.set_columns_type(types)
    return createBindV(statement2, None, None, datas)


#
# Create bindv from list
#
def createBindV(statement2, tbnames, tags, datas):
    """Create bind vector from table names, tags, and data"""
    if tbnames is None and tags is None and datas is None:
        raise StatementError("all bind params is None.")

    log.debug(f"createBindV datas={datas}")

    # Count validation
    count = -1
    ret = utils.detectListNone(tbnames)
    if ret == utils.ALL_NONE:
        bind_names = None
    elif ret == utils.HAVE_NONE:
        raise StatementError("params tbnames some is None, some is not None, this is error.")
    else:
        # not found none
        if type(tbnames) not in [list, tuple]:
            raise StatementError(f"tbnames type error, expected list or tuple type but got {type(tbnames)}.")

        bind_names = tbnames
        count = len(tbnames)

    # Process tags
    ret = utils.detectListNone(tags)
    if ret == utils.ALL_NONE:
        bind_tags = None
    elif ret == utils.HAVE_NONE:
        raise StatementError("params tags some is None, some is not None, this is error.")
    else:
        # not found none
        bind_tags = createTagsBind(statement2, tags)
        if count == -1:
            count = len(tags)
        else:
            if count != len(tags):
                err = f"tags count is inconsistent. require count={count} real={len(tags)}"
                raise StatementError(err)

    # Process datas
    ret = utils.detectListNone(datas)
    if ret == utils.ALL_NONE:
        bind_datas = None
    elif ret == utils.HAVE_NONE:
        raise StatementError("params datas some is None, some is not None, this is error.")
    else:
        # not found none
        bind_datas = createColsBind(statement2, datas)
        if count == -1:
            count = len(datas)
        else:
            if count != len(datas):
                err = f"datas count is inconsistent. require count={count} real={len(datas)}"
                raise StatementError(err)

    # Create bind vector
    log.debug(f"createBindV count={count} bind_names={bind_names} bind_tags={bind_tags} bind_datas={bind_datas}")
    return bind2.new_bindv(count, bind_names, bind_tags, bind_datas)


#
# STMT2 object implementation
#


class TaosStmt2(object):
    """TDengine STMT2 interface for prepared statements"""

    def __init__(self, stmt2, decode_binary=True):
        self._stmt2 = stmt2
        self._decode_binary = decode_binary
        self.fields = None
        self.tag_fields = None
        self.all_fields = None
        self.is_tbname = False
        self.types = None
        self._is_insert = None
        self._affected_rows = 0

    def prepare(self, sql):
        if self._stmt2 is None:
            raise StatementError(ErrMsg.STMT2_NULL)

        if sql is None:
            raise StatementError("sql is null.")

        if not isinstance(sql, str):
            raise StatementError("sql is not str type.")

        if len(sql) == 0:
            raise StatementError("sql is empty.")

        self.fields = None
        self.tag_fields = None
        self._is_insert = None
        self.is_tbname = False

        taos_stmt2_prepare(self._stmt2, sql)

        # obtain schema if insert
        if self.is_insert():
            if obtainSchema(self) is False:
                raise StatementError(f"obtain schema failed, maybe this sql is not supported, sql: {sql}")

    def bind_param(self, tbnames, tags, datas):
        if self._stmt2 is None:
            raise StatementError(ErrMsg.STMT2_NULL)

        log.debug(f"bind_param tbnames  = {tbnames} \n")
        log.debug(f"bind_param tags     = {tags}    \n")
        log.debug(f"bind_param datasTbs = {datas}   \n")

        # check consistent
        if checkConsistent(tbnames, tags, datas) == False:
            raise StatementError("check consistent failed.")

        # bindV
        bindv = None
        if self._is_insert and self.is_tbname and tbnames is None and tags is None:
            log.debug("insert with super bind mode.\n")
            bindv = createSuperBindV(self, datas)
        elif self._is_insert == False:
            bindv = createQueryBindV(self, datas)
        else:
            bindv = createBindV(self, tbnames, tags, datas)

        if bindv is None:
            raise StatementError("create stmt2 bindV failed.")

        # Call engine
        taos_stmt2_bind_param(self._stmt2, bindv.get_address(), -1)

    def bind_param_with_tables(self, tables):
        """Bind parameters using table objects"""
        tbnames = []
        tags_tables = []
        datas_tables = []

        for table in tables:
            tbnames.append(table.name)
            tags_tables.append(table.tags)
            datas_tables.append(table.datas)

        # Call bind
        self.bind_param(tbnames, tags_tables, datas_tables)

    def execute(self) -> int:
        """Execute the prepared statement"""
        if self._stmt2 is None:
            raise StatementError("stmt2 object is null.")

        self._affected_rows = taos_stmt2_exec(self._stmt2)
        return self._affected_rows

    def result(self):
        """Get result from the executed statement"""
        if self._stmt2 is None:
            raise StatementError("stmt2 object is null.")

        result = taos_stmt2_result(self._stmt2)
        return TaosResult(result, close_after=False, decode_binary=self._decode_binary)

    def close(self):
        """Close the statement and release resources"""
        if self._stmt2 is None:
            return

        taos_stmt2_close(self._stmt2)
        self._stmt2 = None

    def is_insert(self):
        """Check if the statement is an INSERT statement"""
        if self._stmt2 is None:
            raise StatementError("stmt2 object is null.")

        if self._is_insert is None:
            self._is_insert = taos_stmt2_is_insert(self._stmt2)

        return self._is_insert

    def get_fields(self):
        """Get field information from the statement"""
        if self._stmt2 is None:
            raise StatementError("stmt2 object is null.")

        return taos_stmt2_get_fields(self._stmt2)

    def error(self):
        """Get error information from the statement"""
        if self._stmt2 is None:
            raise StatementError("stmt2 object is null.")

        return taos_stmt2_error(self._stmt2)

    def set_columns_type(self, types):
        """Set columns type for query operations

        If query must set columns type (not engine interface),
        type is defined in class FieldType in constants.py
        """
        self.fields = []
        for field_type in types:
            item = TaosFieldAllCls(None, field_type, PrecisionEnum.Milliseconds, None, None, TAOS_FIELD_COL)
            self.fields.append(item)

    def __del__(self):
        self.close()

    @property
    def affected_rows(self):
        # type: () -> int
        return self._affected_rows
