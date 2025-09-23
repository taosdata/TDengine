from new_test_framework.utils import tdLog, tdSql
import time
import os
import platform
import taos
import threading


class TestCreateSameNameDb:
    """This test case is used to veirfy TD-25762
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self.db_name = "db"

    def test_create_same_name_db(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx
        """
        try:
            # create same name database multiple times
            for i in range(100):
                tdLog.debug(f"round {str(i+1)} create database {self.db_name}")
                tdSql.execute(f"create database {self.db_name}")
                tdLog.debug(f"round {str(i+1)} drop database {self.db_name}")
                tdSql.execute(f"drop database {self.db_name}")
        except Exception as ex:
            tdLog.exit(str(ex))

        tdLog.success(f"{__file__} successfully executed")

