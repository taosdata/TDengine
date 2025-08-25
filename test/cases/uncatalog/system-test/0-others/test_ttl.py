from new_test_framework.utils import tdLog, tdSql
import time


class TestTtl:
    updatecfgDict = {'ttlUnit': 1, "ttlPushInterval": 1, "ttlChangeOnWrite": 0}

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.ttl = 5
        cls.dbname = "test"

    def check_ttl_result(self):
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(f'create table {self.dbname}.t1(ts timestamp, c1 int)')
        tdSql.execute(f'create table {self.dbname}.t2(ts timestamp, c1 int) ttl {self.ttl}')
        tdSql.query(f'show {self.dbname}.tables')
        tdSql.checkRows(2)
        tdSql.execute(f'flush database {self.dbname}')

        time.sleep(self.ttl + 2)
        tdSql.query(f'show {self.dbname}.tables')
        tdSql.checkRows(1)

    def test_ttl(self):
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
        self.check_ttl_result()

        tdLog.success(f"{__file__} successfully executed")

