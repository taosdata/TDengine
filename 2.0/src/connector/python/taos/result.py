from .cinterface import *

# from .connection import TaosConnection
from .error import *


class TaosResult(object):
    """TDengine result interface"""

    def __init__(self, result, close_after=False, conn=None):
        # type: (c_void_p, bool, TaosConnection) -> TaosResult
        # to make the __del__ order right
        self._conn = conn
        self._close_after = close_after
        self._result = result
        self._fields = None
        self._field_count = None
        self._precision = None

        self._block = None
        self._block_length = None
        self._row_count = 0

    def __iter__(self):
        return self

    def __next__(self):
        return self._next_row()

    def next(self):
        # fetch next row
        return self._next_row()

    def _next_row(self):
        if self._result is None or self.fields is None:
            raise OperationalError("Invalid use of fetch iterator")

        if self._block == None or self._block_iter >= self._block_length:
            self._block, self._block_length = self.fetch_block()
            self._block_iter = 0
            # self._row_count += self._block_length

        raw = self._block[self._block_iter]
        self._block_iter += 1
        return raw

    @property
    def fields(self):
        """fields definitions of the current result"""
        if self._result is None:
            raise ResultError("no result object setted")
        if self._fields == None:
            self._fields = taos_fetch_fields(self._result)

        return self._fields

    @property
    def field_count(self):
        """Field count of the current result, eq to taos_field_count(result)"""
        return self.fields.count

    @property
    def row_count(self):
        """Return the rowcount of the object"""
        return self._row_count

    @property
    def precision(self):
        if self._precision == None:
            self._precision = taos_result_precision(self._result)
        return self._precision

    @property
    def affected_rows(self):
        return taos_affected_rows(self._result)

    # @property
    def field_lengths(self):
        return taos_fetch_lengths(self._result, self.field_count)

    def rows_iter(self, num_of_rows=None):
        return TaosRows(self, num_of_rows)

    def blocks_iter(self):
        return TaosBlocks(self)

    def fetch_block(self):
        if self._result is None:
            raise OperationalError("Invalid use of fetch iterator")

        block, length = taos_fetch_block_raw(self._result)
        if length == 0:
            raise StopIteration
        precision = self.precision
        field_count = self.field_count
        fields = self.fields
        blocks = [None] * field_count
        lengths = self.field_lengths()
        for i in range(field_count):
            data = ctypes.cast(block, ctypes.POINTER(ctypes.c_void_p))[i]
            if fields[i].type not in CONVERT_FUNC_BLOCK:
                raise DatabaseError("Invalid data type returned from database")
            blocks[i] = CONVERT_FUNC_BLOCK[fields[i].type](data, length, lengths[i], precision)

        return list(map(tuple, zip(*blocks))), length

    def fetch_all(self):
        if self._result is None:
            raise OperationalError("Invalid use of fetchall")

        if self._fields == None:
            self._fields = taos_fetch_fields(self._result)
        buffer = [[] for i in range(len(self._fields))]
        self._row_count = 0
        while True:
            block, num_of_fields = taos_fetch_block(self._result, self._fields)
            errno = taos_errno(self._result)
            if errno != 0:
                raise ProgrammingError(taos_errstr(self._result), errno)
            if num_of_fields == 0:
                break
            self._row_count += num_of_fields
            for i in range(len(self._fields)):
                buffer[i].extend(block[i])
        return list(map(tuple, zip(*buffer)))

    def fetch_rows_a(self, callback, param):
        taos_fetch_rows_a(self._result, callback, param)

    def stop_query(self):
        return taos_stop_query(self._result)

    def errno(self):
        """**DO NOT** use this directly unless you know what you are doing"""
        return taos_errno(self._result)

    def errstr(self):
        return taos_errstr(self._result)

    def check_error(self, errno=None, close=True):
        if errno == None:
            errno = self.errno()
        if errno != 0:
            msg = self.errstr()
            self.close()
            raise OperationalError(msg, errno)

    def close(self):
        """free result object."""
        if self._result != None and self._close_after:
            taos_free_result(self._result)
        self._result = None
        self._fields = None
        self._field_count = None
        self._field_lengths = None

    def __del__(self):
        self.close()


class TaosRows:
    """TDengine result rows iterator"""

    def __init__(self, result, num_of_rows=None):
        self._result = result
        self._num_of_rows = num_of_rows

    def __iter__(self):
        return self

    def __next__(self):
        return self._next_row()

    def next(self):
        return self._next_row()

    def _next_row(self):
        if self._result is None:
            raise OperationalError("Invalid use of fetch iterator")
        if self._num_of_rows != None and self._num_of_rows <= self._result._row_count:
            raise StopIteration

        row = taos_fetch_row_raw(self._result._result)
        if not row:
            raise StopIteration
        self._result._row_count += 1
        return TaosRow(self._result, row)

    @property
    def row_count(self):
        """Return the rowcount of the object"""
        return self._result._row_count


class TaosRow:
    def __init__(self, result, row):
        self._result = result
        self._row = row

    def __str__(self):
        return taos_print_row(self._row, self._result.fields, self._result.field_count)

    def __call__(self):
        return self.as_tuple()

    def _astuple(self):
        return self.as_tuple()

    def __iter__(self):
        return self.as_tuple()

    def as_ptr(self):
        return self._row

    def as_tuple(self):
        precision = self._result.precision
        field_count = self._result.field_count
        blocks = [None] * field_count
        fields = self._result.fields
        field_lens = self._result.field_lengths()
        for i in range(field_count):
            data = ctypes.cast(self._row, ctypes.POINTER(ctypes.c_void_p))[i]
            if fields[i].type not in CONVERT_FUNC:
                raise DatabaseError("Invalid data type returned from database")
            if data is None:
                blocks[i] = None
            else:
                blocks[i] = CONVERT_FUNC[fields[i].type](data, 1, field_lens[i], precision)[0]
        return tuple(blocks)


class TaosBlocks:
    """TDengine result blocks iterator"""

    def __init__(self, result):
        self._result = result

    def __iter__(self):
        return self

    def __next__(self):
        return self._result.fetch_block()

    def next(self):
        return self._result.fetch_block()
