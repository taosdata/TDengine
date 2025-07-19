from new_test_framework.utils import tdLog, tdSql
import time


class TestTtlChangeOnWrite:
    updatecfgDict = {'ttlUnit': 1, "ttlPushInterval": 3, "ttlChangeOnWrite": 1, "trimVDbIntervalSec": 360,
                     "ttlFlushThreshold": 100, "ttlBatchDropNum": 10}

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.ttl = 5
        cls.tables = 100
        cls.dbname = "test"

    def check_batch_drop_num(self):
        tdSql.execute(f'create database {self.dbname} vgroups 1')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'create table stb(ts timestamp, c1 int) tags(t1 int)')
        for i in range(self.tables):
            tdSql.execute(f'create table t{i} using stb tags({i}) ttl {self.ttl}')

        tdSql.execute(f'flush database {self.dbname}')
        time.sleep(self.ttl + self.updatecfgDict['ttlPushInterval'] + 1)
        tdSql.query('show tables')
        tdSql.checkRows(90)

    def check_ttl_result(self):
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(f'create table {self.dbname}.t1(ts timestamp, c1 int)')
        tdSql.execute(f'create table {self.dbname}.t2(ts timestamp, c1 int) ttl {self.ttl}')
        tdSql.query(f'show {self.dbname}.tables')
        tdSql.checkRows(2)

        tdSql.execute(f'flush database {self.dbname}')
        time.sleep(self.ttl - 1)
        tdSql.execute(f'insert into {self.dbname}.t2 values(now, 1)');

        tdSql.execute(f'flush database {self.dbname}')
        time.sleep(self.ttl - 1)
        tdSql.query(f'show {self.dbname}.tables')
        tdSql.checkRows(2)

        tdSql.execute(f'flush database {self.dbname}')
        time.sleep(self.ttl * 2)
        tdSql.query(f'show {self.dbname}.tables')
        tdSql.checkRows(1)

    def test_ttl_change_on_write(self):
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
        self.check_batch_drop_num()
        self.check_ttl_result()

        tdLog.success(f"{__file__} successfully executed")

