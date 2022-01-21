from taos.cinterface import *
from taos.error import *
from taos.result import *


class TaosStmt(object):
    """TDengine STMT interface"""

    def __init__(self, stmt, conn = None):
        self._conn = conn
        self._stmt = stmt

    def set_tbname(self, name):
        """Set table name if needed.

        Note that the set_tbname* method should only used in insert statement
        """
        if self._stmt is None:
            raise StatementError("Invalid use of set_tbname")
        taos_stmt_set_tbname(self._stmt, name)

    def prepare(self, sql):
        # type: (str) -> None
        taos_stmt_prepare(self._stmt, sql)

    def set_tbname_tags(self, name, tags):
        # type: (str, Array[TaosBind]) -> None
        """Set table name with tags, tags is array of BindParams"""
        if self._stmt is None:
            raise StatementError("Invalid use of set_tbname")
        taos_stmt_set_tbname_tags(self._stmt, name, tags)

    def bind_param(self, params, add_batch=True):
        # type: (Array[TaosBind], bool) -> None
        if self._stmt is None:
            raise StatementError("Invalid use of stmt")
        taos_stmt_bind_param(self._stmt, params)
        if add_batch:
            taos_stmt_add_batch(self._stmt)

    def bind_param_batch(self, binds, add_batch=True):
        # type: (Array[TaosMultiBind], bool) -> None
        if self._stmt is None:
            raise StatementError("Invalid use of stmt")
        taos_stmt_bind_param_batch(self._stmt, binds)
        if add_batch:
            taos_stmt_add_batch(self._stmt)

    def add_batch(self):
        if self._stmt is None:
            raise StatementError("Invalid use of stmt")
        taos_stmt_add_batch(self._stmt)

    def execute(self):
        if self._stmt is None:
            raise StatementError("Invalid use of execute")
        taos_stmt_execute(self._stmt)

    def use_result(self):
        result = taos_stmt_use_result(self._stmt)
        return TaosResult(result)

    def close(self):
        """Close stmt."""
        if self._stmt is None:
            return
        taos_stmt_close(self._stmt)
        self._stmt = None

    def __del__(self):
        self.close()


if __name__ == "__main__":
    from taos.connection import TaosConnection

    conn = TaosConnection()

    stmt = conn.statement("select * from log.log limit 10")
    stmt.execute()
    result = stmt.use_result()
    for row in result:
        print(row)
    stmt.close()
    conn.close()
