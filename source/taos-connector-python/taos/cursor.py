# encoding:UTF-8
from typing import Optional

from taos import log
from taos.cinterface import *
from taos.constants import FieldType
from taos.error import *


class TaosCursor(object):
    """Database cursor which is used to manage the context of a fetch operation.

    Attributes:
        .description: Read-only attribute consists of 7-item sequences:

            > name (mandatory)
            > type_code (mandatory)
            > display_size
            > internal_size
            > precision
            > scale
            > null_ok

            This attribute will be None for operations that do not return rows or
            if the cursor has not had an operation invoked via the .execute*() method yet.

        .rowcount:This read-only attribute specifies the number of rows that the last
            .execute*() produced (for DQL statements like SELECT) or affected
    """

    def __init__(self, connection=None, decode_binary=True):
        self._description = []
        self._rowcount = -1
        self._connection = None
        self._result = None
        self._stmt_result = None
        self._fields = None
        self._block = None
        self._stmt = None
        self._bind_sql = ""
        self._block_rows = -1
        self._block_iter = 0
        self._affected_rows = 0
        self._logfile = ""
        self.decode_binary = decode_binary

        if connection is not None:
            self._connection = connection

    def __iter__(self):
        return self

    def __next__(self):
        return self._taos_next()

    def next(self):
        return self._taos_next()

    def _taos_next(self):
        if self._result is None or self._fields is None:
            raise OperationalError("Invalid use of fetch iterator")

        if self._block_rows <= self._block_iter:
            block, self._block_rows = taos_fetch_row(self._result, self._fields, decode_binary=self.decode_binary)
            if self._block_rows == 0:
                raise StopIteration
            self._block = list(map(tuple, zip(*block)))
            self._block_iter = 0

        data = self._block[self._block_iter]
        self._block_iter += 1

        return data

    @property
    def description(self):
        """Return the description of the object."""
        return self._description

    @property
    def rowcount(self):
        """
        For INSERT statement, rowcount is assigned immediately after execute the statement.
        For SELECT statement, rowcount will not get correct value until fetched all data.
        """
        return self._rowcount

    @property
    def affected_rows(self):
        """Return the rowcount of insertion"""
        return self._affected_rows

    def callproc(self, procname, *args):
        """Call a stored database procedure with the given name.

        Void functionality since no stored procedures.
        """
        pass

    def log(self, logfile):
        self._logfile = logfile

    def close(self):
        """Close the cursor."""
        if self._connection is None:
            return False
        if self._stmt is not None:
            self._stmt.close()
            self._stmt = None
        self._reset_result()
        self._connection = None
        self._bind_sql = ""

        return True

    def keys(self):
        """Return the list of column names"""
        if self._fields:
            return self._fields
        elif self._stmt_result and self._stmt_result.fields:
            return self._stmt_result.fields
        return []

    def execute(self, operation, params=None, req_id: Optional[int] = None):
        if not operation:
            return None
        log.debug(f"execute: {operation} with params: {params}")
        self._reset_result()
        if params is not None and isinstance(params, (dict, list, tuple)) and len(params) > 0:
            return self._execute_stmt(operation, params)
        else:
            return self._execute_sql(operation, req_id=req_id)

    def _execute_stmt(self, operation, params, is_convert=False):
        """Prepare and execute a database operation (query or command)."""

        if not self._connection:
            # TODO : change the exception raised here
            raise ProgrammingError("Cursor is not connected")

        if self._stmt is None or self._bind_sql != operation:
            if self._stmt is not None:
                self._stmt.close()
                self._stmt = None

            self._stmt = self._connection.statement2(operation)
            self._bind_sql = operation
            log.debug(f"bind sql: {operation}, params: {params}")
            if self._stmt is None:
                raise OperationalError("Failed to initialize statement")
            # Here we handle the data format of unbound super tables called by SQLAlchemy
            if is_convert and not self._stmt.is_tbname:
                if isinstance(params[0], (list, tuple, set)):
                    params = [[list(col) for col in zip(*params)]]

        self._stmt.bind_param(None, None, params)
        self._stmt.execute()
        if self._stmt.is_insert():
            self._affected_rows = self._stmt.affected_rows
            self._rowcount = self._affected_rows
            return self._affected_rows
        else:
            self._stmt_result = self._stmt.result()
            self._fields = self._stmt_result.fields
            self._handle_result()
            return self._stmt_result

    def _execute_sql(self, operation, req_id: Optional[int] = None):
        """Prepare and execute a database operation (query or command)."""

        if not self._connection:
            raise ProgrammingError("Cursor is not connected")

        sql = operation

        if req_id is None:
            self._result = taos_query(self._connection._conn, sql)
        else:
            self._result = taos_query_with_reqid(self._connection._conn, sql, req_id)
        # print("   << Query ({}) Exec Done".format(localSeqNum))
        if self._logfile:
            with open(self._logfile, "a", encoding="utf-8") as logfile:
                logfile.write("%s;\n" % operation)

        if taos_field_count(self._result) == 0:
            affected_rows = taos_affected_rows(self._result)
            self._affected_rows = affected_rows
            self._rowcount = affected_rows
            return affected_rows
        else:
            self._fields = taos_fetch_fields(self._result)
            return self._handle_result()

    def executemany(self, operation, data_list):
        """
        Prepare a database operation (query or command) and then execute it against all parameter sequences or mappings
        found in the sequence seq_of_parameters.
        """
        if not operation or not data_list or len(data_list) == 0:
            return None

        self._reset_result()

        return self._execute_stmt(operation, data_list, True)

    def execute_many(self, operation, data_list, req_id: Optional[int] = None):
        """
        Prepare a database operation (query or command) and then execute it against all parameter sequences or mappings
        found in the sequence seq_of_parameters.
        """
        sql = operation
        flag = True
        affected_rows = 0
        for line in data_list:
            if isinstance(line, dict):
                flag = False
                affected_rows += self.execute(sql.format(**line), req_id=req_id)
            elif isinstance(line, list):
                sql += f" {tuple(line)} "
            elif isinstance(line, tuple):
                sql += f" {line} "
        if flag:
            affected_rows += self.execute(sql, req_id=req_id)
        return affected_rows

    def fetchone(self):
        """Fetch the next row of a query result set, returning a single sequence, or None when no more data is available."""
        try:
            if self._stmt_result is not None:
                return self._stmt_result.next()
            else:
                return self._taos_next()
        except StopIteration:
            return None

    def fetchmany(self, size: int = None):
        """Fetch the next set of rows of a query result, returning a sequence of sequences."""
        if self._result is None or self._fields is None:
            raise OperationalError("Invalid use of fetch iterator")
        if size is None:
            return self.fetchall()

        if size <= 0:
            raise ValueError("size must be greater than 0")

        data = []
        count = 0

        try:
            while count < size:
                # If current block is exhausted, fetch new data block
                if self._block_iter >= self._block_rows:
                    block, self._block_rows = taos_fetch_block(
                        self._result, self._fields, decode_binary=self.decode_binary
                    )

                    # If no more data available, break the loop
                    if self._block_rows == 0:
                        break

                    self._block = list(map(tuple, zip(*block)))
                    self._block_iter = 0

                # Calculate how many rows can be taken from current block
                remaining_in_block = self._block_rows - self._block_iter
                needed = size - count
                rows_to_take = min(remaining_in_block, needed)

                # Get data from current block
                end_index = self._block_iter + rows_to_take
                data.extend(self._block[self._block_iter : end_index])

                # Update counters
                self._block_iter += rows_to_take
                count += rows_to_take

        except Exception as e:
            errno = taos_errno(self._result)
            if errno != 0:
                raise ProgrammingError(taos_errstr(self._result), errno)
            raise e

        return data

    def istype(self, col, dataType):
        if dataType.upper() == "BOOL":
            if self._description[col][1] == FieldType.C_BOOL:
                return True
        if dataType.upper() == "TINYINT":
            if self._description[col][1] == FieldType.C_TINYINT:
                return True
        if dataType.upper() == "TINYINT UNSIGNED":
            if self._description[col][1] == FieldType.C_TINYINT_UNSIGNED:
                return True
        if dataType.upper() == "SMALLINT":
            if self._description[col][1] == FieldType.C_SMALLINT:
                return True
        if dataType.upper() == "SMALLINT UNSIGNED":
            if self._description[col][1] == FieldType.C_SMALLINT_UNSIGNED:
                return True
        if dataType.upper() == "INT":
            if self._description[col][1] == FieldType.C_INT:
                return True
        if dataType.upper() == "INT UNSIGNED":
            if self._description[col][1] == FieldType.C_INT_UNSIGNED:
                return True
        if dataType.upper() == "BIGINT":
            if self._description[col][1] == FieldType.C_BIGINT:
                return True
        if dataType.upper() == "BIGINT UNSIGNED":
            if self._description[col][1] == FieldType.C_BIGINT_UNSIGNED:
                return True
        if dataType.upper() == "FLOAT":
            if self._description[col][1] == FieldType.C_FLOAT:
                return True
        if dataType.upper() == "DOUBLE":
            if self._description[col][1] == FieldType.C_DOUBLE:
                return True
        if dataType.upper() == "BINARY" or dataType.upper() == "VARCHAR":
            if self._description[col][1] == FieldType.C_BINARY:
                return True
        if dataType.upper() == "TIMESTAMP":
            if self._description[col][1] == FieldType.C_TIMESTAMP:
                return True
        if dataType.upper() == "NCHAR":
            if self._description[col][1] == FieldType.C_NCHAR:
                return True
        if dataType.upper() == "JSON":
            if self._description[col][1] == FieldType.C_JSON:
                return True
        if dataType.upper() == "VARBINARY":
            if self._description[col][1] == FieldType.C_VARBINARY:
                return True

        return False

    def fetchall_row(self):
        """Fetch all (remaining) rows of a query result, returning them as a sequence of sequences (e.g. a list of tuples). Note that the cursor's arraysize attribute can affect the performance of this operation."""
        if self._result is None or self._fields is None:
            raise OperationalError("Invalid use of fetchall")

        buffer = [[] for i in range(len(self._fields))]
        self._rowcount = 0
        while True:
            block, num_of_rows = taos_fetch_row(self._result, self._fields, decode_binary=self.decode_binary)
            errno = taos_errno(self._result)
            if errno != 0:
                raise ProgrammingError(taos_errstr(self._result), errno)
            if num_of_rows == 0:
                break
            self._rowcount += num_of_rows
            for i in range(len(self._fields)):
                buffer[i].extend(block[i])
        return list(map(tuple, zip(*buffer)))

    def fetchall(self):
        if self._stmt_result is not None:
            return self._stmt_result.fetch_all()
        else:
            return self._fetchall_sql()

    def _fetchall_sql(self):
        if self._result is None:
            raise OperationalError("Invalid use of fetchall")
        fields = self._fields if self._fields is not None else taos_fetch_fields(self._result)
        buffer = [[] for i in range(len(fields))]
        self._rowcount = 0
        while True:
            block, num_of_rows = taos_fetch_block(self._result, self._fields, decode_binary=self.decode_binary)
            errno = taos_errno(self._result)
            if errno != 0:
                raise ProgrammingError(taos_errstr(self._result), errno)
            if num_of_rows == 0:
                break
            self._rowcount += num_of_rows
            for i in range(len(self._fields)):
                buffer[i].extend(block[i])
        return list(map(tuple, zip(*buffer)))

    def stop_query(self):
        if self._result is not None:
            taos_stop_query(self._result)

    def nextset(self):
        """ """
        pass

    def setinputsize(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    def _reset_result(self):
        """Reset the result to unused version."""
        self._description = []
        if self._result is not None:
            taos_free_result(self._result)
        self._result = None
        self._fields = None
        self._block = None
        self._block_rows = -1
        self._block_iter = 0
        self._affected_rows = 0

    def _handle_result(self):
        """Handle the return result from query."""
        self._description = []
        for ele in self._fields:
            self._description.append((ele["name"], ele["type"], None, None, None, None, False))

        return self._result

    def __del__(self):
        self.close()
