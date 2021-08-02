# encoding:UTF-8
"""Python exceptions
"""


class Error(Exception):
    def __init__(self, msg=None, errno=0xffff):
        self.msg = msg
        self.errno = errno
        self._full_msg = "[0x%04x]: %s" % (self.errno & 0xffff, self.msg)

    def __str__(self):
        return self._full_msg


class Warning(Exception):
    """Exception raised for important warnings like data truncations while inserting."""

    pass


class InterfaceError(Error):
    """Exception raised for errors that are related to the database interface rather than the database itself."""

    pass


class DatabaseError(Error):
    """Exception raised for errors that are related to the database."""

    pass

class ConnectionError(Error):
    """Exceptin raised for connection failed"""
    pass

class DataError(DatabaseError):
    """Exception raised for errors that are due to problems with the processed data like division by zero, numeric value out of range."""

    pass


class OperationalError(DatabaseError):
    """Exception raised for errors that are related to the database's operation and not necessarily under the control of the programmer"""

    pass


class IntegrityError(DatabaseError):
    """Exception raised when the relational integrity of the database is affected."""

    pass


class InternalError(DatabaseError):
    """Exception raised when the database encounters an internal error."""

    pass


class ProgrammingError(DatabaseError):
    """Exception raised for programming errors."""

    pass


class NotSupportedError(DatabaseError):
    """Exception raised in case a method or database API was used which is not supported by the database,."""

    pass


class StatementError(DatabaseError):
    """Exception raised in STMT API."""

    pass

class ResultError(DatabaseError):
    """Result related APIs."""

    pass

class LinesError(DatabaseError):
    """taos_insert_lines errors."""

    pass