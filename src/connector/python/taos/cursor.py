# encoding:UTF-8
from .cinterface import *
from .error import *
from .constants import FieldType
from .result import *


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

    def __init__(self, connection=None):
        self._description = []
        self._rowcount = -1
        self._connection = None
        self._result = None
        self._fields = None
        self._block = None
        self._block_rows = -1
        self._block_iter = 0
        self._affected_rows = 0
        self._logfile = ""

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
            block, self._block_rows = taos_fetch_row(self._result, self._fields)
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
        """Return the rowcount of the object"""
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

        self._reset_result()
        self._connection = None

        return True

    def execute(self, operation, params=None):
        """Prepare and execute a database operation (query or command)."""
        if not operation:
            return None

        if not self._connection:
            # TODO : change the exception raised here
            raise ProgrammingError("Cursor is not connected")

        self._reset_result()

        stmt = operation
        if params is not None:
            pass

        # global querySeqNum
        # querySeqNum += 1
        # localSeqNum = querySeqNum # avoid race condition
        # print("   >> Exec Query ({}): {}".format(localSeqNum, str(stmt)))
        self._result = taos_query(self._connection._conn, stmt)
        # print("   << Query ({}) Exec Done".format(localSeqNum))
        if self._logfile:
            with open(self._logfile, "a") as logfile:
                logfile.write("%s;\n" % operation)

        if taos_field_count(self._result) == 0:
            affected_rows = taos_affected_rows(self._result)
            self._affected_rows += affected_rows
            return affected_rows
        else:
            self._fields = taos_fetch_fields(self._result)
            return self._handle_result()

    def executemany(self, operation, seq_of_parameters):
        """Prepare a database operation (query or command) and then execute it against all parameter sequences or mappings found in the sequence seq_of_parameters."""
        pass

    def fetchone(self):
        """Fetch the next row of a query result set, returning a single sequence, or None when no more data is available."""
        pass

    def fetchmany(self):
        pass

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
        if dataType.upper() == "BINARY":
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

        return False

    def fetchall_row(self):
        """Fetch all (remaining) rows of a query result, returning them as a sequence of sequences (e.g. a list of tuples). Note that the cursor's arraysize attribute can affect the performance of this operation."""
        if self._result is None or self._fields is None:
            raise OperationalError("Invalid use of fetchall")

        buffer = [[] for i in range(len(self._fields))]
        self._rowcount = 0
        while True:
            block, num_of_fields = taos_fetch_row(self._result, self._fields)
            errno = taos_errno(self._result)
            if errno != 0:
                raise ProgrammingError(taos_errstr(self._result), errno)
            if num_of_fields == 0:
                break
            self._rowcount += num_of_fields
            for i in range(len(self._fields)):
                buffer[i].extend(block[i])
        return list(map(tuple, zip(*buffer)))

    def fetchall(self):
        if self._result is None:
            raise OperationalError("Invalid use of fetchall")
        fields = self._fields if self._fields is not None else taos_fetch_fields(self._result)
        buffer = [[] for i in range(len(fields))]
        self._rowcount = 0
        while True:
            block, num_of_fields = taos_fetch_block(self._result, self._fields)
            errno = taos_errno(self._result)
            if errno != 0:
                raise ProgrammingError(taos_errstr(self._result), errno)
            if num_of_fields == 0:
                break
            self._rowcount += num_of_fields
            for i in range(len(self._fields)):
                buffer[i].extend(block[i])
        return list(map(tuple, zip(*buffer)))

    def stop_query(self):
        if self._result != None:
            taos_stop_query(self._result)
            
    def nextset(self):
        """ """
        pass

    def setinputsize(self, sizes):
        pass

    def setutputsize(self, size, column=None):
        pass

    def _reset_result(self):
        """Reset the result to unused version."""
        self._description = []
        self._rowcount = -1
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
