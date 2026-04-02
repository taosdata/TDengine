from taos.cinterface import *

from taos.error import *

from ctypes import c_void_p


class TaosResult(object):
    """TDengine result interface"""

    def __init__(self, result, close_after=False, decode_binary=True):
        # type: (c_void_p, bool, bool) -> None
        # to make the __del__ order right
        self._close_after = close_after
        if isinstance(result, c_void_p):
            self._result = result
        else:
            self._result = c_void_p(result)

        self._fields = None
        self._field_count = None
        self._precision = None

        self._block = None
        self._block_length = None
        self._row_count = 0
        self.decode_binary = decode_binary

    def __iter__(self):
        return self

    def __next__(self):
        return self._next_row()

    def next(self):
        return self._next_row()

    def _next_row(self):
        if self._result is None or self.fields is None:
            raise OperationalError("Invalid use of fetch iterator")

        if self._block is None or self._block_iter >= self._block_length:
            self._block, self._block_length = self.fetch_block()
            self._block_iter = 0

        raw = self._block[self._block_iter]
        self._block_iter += 1
        self._row_count += 1
        return raw

    @property
    def fields(self):
        """fields definitions of the current result"""
        if self._result is None:
            raise ResultError("no result object")
        if self._fields is None:
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
        if self._precision is None:
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

        blocks, length = taos_fetch_block(self._result, decode_binary=self.decode_binary)
        if length == 0:
            raise StopIteration

        return list(map(tuple, zip(*blocks))), length

    def fetch_all(self):
        if self._result is None:
            raise OperationalError("Invalid use of fetchall")

        if self._fields is None:
            self._fields = taos_fetch_fields(self._result)
        buffer = [[] for i in range(len(self._fields))]
        self._row_count = 0
        while True:
            block, num_of_fields = taos_fetch_block(self._result, self._fields, decode_binary=self.decode_binary)
            errno = taos_errno(self._result)
            if errno != 0:
                raise ProgrammingError(taos_errstr(self._result), errno)
            if num_of_fields == 0:
                break
            self._row_count += num_of_fields
            for i in range(len(self._fields)):
                buffer[i].extend(block[i])
        return list(map(tuple, zip(*buffer)))

    def fetch_all_into_dict(self):
        """Fetch all rows and convert it to dict"""
        names = [field.name for field in self.fields]
        rows = self.fetch_all()
        return list(dict(zip(names, row)) for row in rows)

    def fetch_rows_a(self, callback, param):
        taos_fetch_rows_a(self._result, callback, param)

    def stop_query(self):
        return taos_stop_query(self._result)

    def get_topic_name(self):
        return tmq_get_topic_name(self._result)

    def get_vgroup_id(self):
        return tmq_get_vgroup_id(self._result)

    def get_table_name(self):
        return tmq_get_table_name(self._result)

    def get_db_name(self):
        return tmq_get_db_name(self._result)

    def errno(self):
        """**DO NOT** use this directly unless you know what you are doing"""
        return taos_errno(self._result)

    def errstr(self):
        return taos_errstr(self._result)

    def check_error(self, errno=None, close=True):
        if errno is None:
            errno = self.errno()
        if errno != 0:
            msg = self.errstr()
            self.close()
            raise OperationalError(msg, errno)

    def close(self):
        """free result object."""
        if self._result is not None and self._close_after:
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
        if self._num_of_rows is not None and self._num_of_rows <= self._result._row_count:
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
                f = convert_func(fields[i].type, self._result.decode_binary)
                blocks[i] = f(data, [False], 1, field_lens[i], precision)[0]
        return tuple(blocks)

    def as_dict(self):
        values = self.as_tuple()
        names = self._result.fields
        dict(zip(names, values))


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
