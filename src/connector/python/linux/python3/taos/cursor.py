from .cinterface import CTaosInterface
from .error import *
from .constants import FieldType
import threading

# querySeqNum = 0


class TDengineCursor(object):
    """Database cursor which is used to manage the context of a fetch operation.

    Attributes:
        .description: Read-only attribute consists of 7-item sequences:

            > name (mondatory)
            > type_code (mondatory)
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
        self._description = None
        self._rowcount = -1
        self._connection = None
        self._result = None
        self._fields = None
        self._block = None
        self._block_rows = -1
        self._block_iter = 0
        self._affected_rows = 0
        self._logfile = ""
        self._threadId = threading.get_ident()

        if connection is not None:
            self._connection = connection

    def __iter__(self):
        return self

    def __next__(self):
        if self._result is None or self._fields is None:
            raise OperationalError("Invalid use of fetch iterator")

        if self._block_rows <= self._block_iter:
            block, self._block_rows = CTaosInterface.fetchBlock(
                self._result, self._fields)
            if self._block_rows == 0:
                raise StopIteration
            self._block = list(map(tuple, zip(*block)))
            self._block_iter = 0

        data = self._block[self._block_iter]
        self._block_iter += 1

        return data

    @property
    def description(self):
        """Return the description of the object.
        """
        return self._description

    @property
    def rowcount(self):
        """Return the rowcount of the object
        """
        return self._rowcount

    @property
    def affected_rows(self):
        """Return the rowcount of insertion
        """
        return self._affected_rows

    def callproc(self, procname, *args):
        """Call a stored database procedure with the given name.

        Void functionality since no stored procedures.
        """
        pass

    def log(self, logfile):
        self._logfile = logfile

    def close(self):
        """Close the cursor.
        """
        if self._connection is None:
            return False

        self._reset_result()
        self._connection = None

        return True

    def execute(self, operation, params=None):
        """Prepare and execute a database operation (query or command).
        """
        # if threading.get_ident() != self._threadId:
        #     info ="Cursor execute:Thread ID not match,creater:"+str(self._threadId)+" caller:"+str(threading.get_ident())
        #     raise OperationalError(info)
            # print(info)
            # return None

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
        # localSeqNum = querySeqNum # avoid raice condition
        # print("   >> Exec Query ({}): {}".format(localSeqNum, str(stmt)))
        self._result = CTaosInterface.query(self._connection._conn, stmt)
        # print("   << Query ({}) Exec Done".format(localSeqNum))
        if (self._logfile):
            with open(self._logfile, "a") as logfile:
                logfile.write("%s;\n" % operation)

        errno = CTaosInterface.libtaos.taos_errno(self._result)
        if errno == 0:
            if CTaosInterface.fieldsCount(self._result) == 0:
                self._affected_rows += CTaosInterface.affectedRows(
                    self._result )
                return CTaosInterface.affectedRows(self._result )
            else:
                self._fields = CTaosInterface.useResult(
                    self._result)
                return self._handle_result()
        else:
            raise ProgrammingError(
                CTaosInterface.errStr(
                    self._result), errno)

    def executemany(self, operation, seq_of_parameters):
        """Prepare a database operation (query or command) and then execute it against all parameter sequences or mappings found in the sequence seq_of_parameters.
        """
        pass

    def fetchone(self):
        """Fetch the next row of a query result set, returning a single sequence, or None when no more data is available.
        """
        pass

    def fetchmany(self):
        pass

    def istype(self, col, dataType):
        if (dataType.upper() == "BOOL"):
            if (self._description[col][1] == FieldType.C_BOOL):
                return True
        if (dataType.upper() == "TINYINT"):
            if (self._description[col][1] == FieldType.C_TINYINT):
                return True
        if (dataType.upper() == "INT"):
            if (self._description[col][1] == FieldType.C_INT):
                return True
        if (dataType.upper() == "BIGINT"):
            if (self._description[col][1] == FieldType.C_INT):
                return True
        if (dataType.upper() == "FLOAT"):
            if (self._description[col][1] == FieldType.C_FLOAT):
                return True
        if (dataType.upper() == "DOUBLE"):
            if (self._description[col][1] == FieldType.C_DOUBLE):
                return True
        if (dataType.upper() == "BINARY"):
            if (self._description[col][1] == FieldType.C_BINARY):
                return True
        if (dataType.upper() == "TIMESTAMP"):
            if (self._description[col][1] == FieldType.C_TIMESTAMP):
                return True
        if (dataType.upper() == "NCHAR"):
            if (self._description[col][1] == FieldType.C_NCHAR):
                return True

        return False

    def fetchall(self):
        """Fetch all (remaining) rows of a query result, returning them as a sequence of sequences (e.g. a list of tuples). Note that the cursor's arraysize attribute can affect the performance of this operation.
        """
        # if threading.get_ident() != self._threadId:
        #     info ="[WARNING] Cursor fetchall:Thread ID not match,creater:"+str(self._threadId)+" caller:"+str(threading.get_ident())
        #     raise OperationalError(info)
            # print(info)
            # return None
        if self._result is None or self._fields is None:
            raise OperationalError("Invalid use of fetchall")

        buffer = [[] for i in range(len(self._fields))]
        self._rowcount = 0
        while True:
            block, num_of_fields = CTaosInterface.fetchBlock(self._result, self._fields)
            errno = CTaosInterface.libtaos.taos_errno(self._result)
            if errno != 0:
                raise ProgrammingError(CTaosInterface.errStr(self._result), errno)
            if num_of_fields == 0:
                break
            self._rowcount += num_of_fields
            for i in range(len(self._fields)):
                buffer[i].extend(block[i])
        return list(map(tuple, zip(*buffer)))

    def nextset(self):
        """
        """
        pass

    def setinputsize(self, sizes):
        pass

    def setutputsize(self, size, column=None):
        pass

    def _reset_result(self):
        """Reset the result to unused version.
        """
        self._description = None
        self._rowcount = -1
        if self._result is not None:
            CTaosInterface.freeResult(self._result)
        self._result = None
        self._fields = None
        self._block = None
        self._block_rows = -1
        self._block_iter = 0
        self._affected_rows = 0

    def _handle_result(self):
        """Handle the return result from query.
        """
        # if threading.get_ident() != self._threadId:
        #     info = "Cursor handleresult:Thread ID not match,creater:"+str(self._threadId)+" caller:"+str(threading.get_ident())
        #     raise OperationalError(info)
            # print(info)
            # return None

        self._description = []
        for ele in self._fields:
            self._description.append(
                (ele['name'], ele['type'], None, None, None, None, False))

        return self._result

