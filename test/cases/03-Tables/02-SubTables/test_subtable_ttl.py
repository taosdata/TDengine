from new_test_framework.utils import tdLog, tdSql
import time


class TestTtl:
    updatecfgDict = {'ttlUnit': 1, "ttlPushInterval": 3, "ttlChangeOnWrite": 1, "trimVDbIntervalSec": 360,
                     "ttlFlushThreshold": 100, "ttlBatchDropNum": 10}

    def setup_class(cls):
        cls.ttl = 5
        cls.dbname = "test1"

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

    def do_ttl(self):
        self.check_ttl_result()
        print("\ndo ttl ................................ [passed]")

    #
    # ------------------- test_ttl_change_on_write.py ----------------
    #
    def init_class(self):
        self.ttl = 5
        self.tables = 100
        self.dbname = "test"

    def check_batch_drop_num(self):
        tdSql.execute(f'create database {self.dbname} vgroups 1')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'create table stb(ts timestamp, c1 int) tags(t1 int)')
        childs = []
        for i in range(self.tables):
            childs.append(f't{i} using stb tags({i}) ttl {self.ttl}')
        sql = ' '.join(childs)
        tdSql.execute(f'create table {sql}')

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
        tdSql.execute(f'insert into {self.dbname}.t2 values(now, 1)')

        tdSql.execute(f'flush database {self.dbname}')
        time.sleep(self.ttl - 1)
        tdSql.query(f'show {self.dbname}.tables')
        tdSql.checkRows(2)

        tdSql.execute(f'flush database {self.dbname}')
        time.sleep(self.ttl * 2)
        tdSql.query(f'show {self.dbname}.tables')
        tdSql.checkRows(1)

    def do_ttl_change_on_write(self):
        self.init_class()
        self.check_batch_drop_num()
        self.check_ttl_result()

        print("do ttl change on write ................ [passed]")

    #
    # ------------------- main ----------------
    #
    def test_subtable_ttl(self):
        """Subtable TTL
        
        1. Create 100 subtables with TTL
        2. flush database and wait for TTL to take effect
        3. Verify that the correct number of subtables have been dropped
        4. Create 2 subtables, one with TTL and one without
        5. Insert data into the subtable with TTL before it expires
        6. Verify that the subtable with TTL is not dropped after the TTL period,
        7. while the subtable without TTL remains unaffected.

        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_ttl.py
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_ttl_change_on_write.py
 
        """
        self.do_ttl()
        self.do_ttl_change_on_write()