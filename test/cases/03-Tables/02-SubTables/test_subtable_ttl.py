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
    # ------------------- test_ttl_comment.py ----------------
    #
    def do_ttl_comment(self):
        dbname="db"
        tdSql.prepare()

        tdSql.error(f"create table {dbname}.ttl_table1(ts timestamp, i int) ttl 1.1")
        tdSql.error(f"create table {dbname}.ttl_table2(ts timestamp, i int) ttl 1e1")
        tdSql.error(f"create table {dbname}.ttl_table3(ts timestamp, i int) ttl -1")
        tdSql.error(f"create table {dbname}.ttl_table4(ts timestamp, i int) ttl 2147483648")

        print("============== STEP 1 ===== test normal table")

        tdSql.execute(f"create table {dbname}.normal_table1(ts timestamp, i int)")
        tdSql.execute(f"create table {dbname}.normal_table2(ts timestamp, i int) comment '' ttl 3")

        tdSql.query("select * from information_schema.ins_tables where table_name like 'normal_table1'")
        tdSql.checkData(0, 0, 'normal_table1')
        tdSql.checkData(0, 7, 0)
        tdSql.checkData(0, 8, None)

        tdSql.query("select * from information_schema.ins_tables where table_name like 'normal_table2'")
        tdSql.checkData(0, 0, 'normal_table2')
        tdSql.checkData(0, 7, 3)
        tdSql.checkData(0, 8, '')

        tdSql.execute(f"alter table {dbname}.normal_table1 comment 'nihao'")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'normal_table1'")
        tdSql.checkData(0, 0, 'normal_table1')
        tdSql.checkData(0, 8, 'nihao')

        tdSql.execute(f"alter table {dbname}.normal_table1 comment ''")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'normal_table1'")
        tdSql.checkData(0, 0, 'normal_table1')
        tdSql.checkData(0, 8, '')

        tdSql.execute(f"alter table {dbname}.normal_table2 comment 'fly'")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'normal_table2'")
        tdSql.checkData(0, 0, 'normal_table2')
        tdSql.checkData(0, 8, 'fly')

        tdSql.execute(f"alter table {dbname}.normal_table1 ttl 1")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'normal_table1'")
        tdSql.checkData(0, 0, 'normal_table1')
        tdSql.checkData(0, 7, 1)

        tdSql.execute(f"alter table {dbname}.normal_table2 ttl 0")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'normal_table2'")
        tdSql.checkData(0, 0, 'normal_table2')
        tdSql.checkData(0, 7, 0)

        print("============== STEP 2 ===== test super table")

        tdSql.execute(f"create table {dbname}.super_table1(ts timestamp, i int) tags(t int)")
        tdSql.execute(f"create table {dbname}.super_table2(ts timestamp, i int) tags(t int) comment ''")
        tdSql.execute(f"create table {dbname}.super_table3(ts timestamp, i int) tags(t int) comment 'super'")

        tdSql.query("select * from information_schema.ins_stables where stable_name like 'super_table1'")
        tdSql.checkData(0, 0, 'super_table1')
        tdSql.checkData(0, 6, None)

        tdSql.query("select * from information_schema.ins_stables where stable_name like 'super_table2'")
        tdSql.checkData(0, 0, 'super_table2')
        tdSql.checkData(0, 6, '')

        tdSql.query("select * from information_schema.ins_stables where stable_name like 'super_table3'")
        tdSql.checkData(0, 0, 'super_table3')
        tdSql.checkData(0, 6, 'super')

        tdSql.execute(f"alter table {dbname}.super_table1 comment 'nihao'")
        tdSql.query("select * from information_schema.ins_stables where stable_name like 'super_table1'")
        tdSql.checkData(0, 0, 'super_table1')
        tdSql.checkData(0, 6, 'nihao')

        tdSql.execute(f"alter table {dbname}.super_table1 comment ''")
        tdSql.query("select * from information_schema.ins_stables where stable_name like 'super_table1'")
        tdSql.checkData(0, 0, 'super_table1')
        tdSql.checkData(0, 6, '')

        tdSql.execute(f"alter table {dbname}.super_table2 comment 'fly'")
        tdSql.query("select * from information_schema.ins_stables where stable_name like 'super_table2'")
        tdSql.checkData(0, 0, 'super_table2')
        tdSql.checkData(0, 6, 'fly')

        tdSql.execute(f"alter table {dbname}.super_table3 comment 'tdengine'")
        tdSql.query("select * from information_schema.ins_stables where stable_name like 'super_table3'")
        tdSql.checkData(0, 0, 'super_table3')
        tdSql.checkData(0, 6, 'tdengine')

        print("============== STEP 3 ===== test child table")

        tdSql.execute(f"create table {dbname}.child_table1 using  {dbname}.super_table1 tags(1) ttl 10")
        tdSql.execute(f"create table {dbname}.child_table2 using  {dbname}.super_table1 tags(1) comment ''")
        tdSql.execute(f"create table {dbname}.child_table3 using  {dbname}.super_table1 tags(1) comment 'child'")
        tdSql.execute(f"insert into {dbname}.child_table4 using  {dbname}.super_table1 tags(1) values(now, 1)")
        tdSql.execute(f"insert into {dbname}.child_table5 using  {dbname}.super_table1 tags(1) ttl 23 comment '' values(now, 1)")
        tdSql.error(f"insert into {dbname}.child_table6 using  {dbname}.super_table1 tags(1) ttl -23 comment '' values(now, 1)")

        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table1'")
        tdSql.checkData(0, 0, 'child_table1')
        tdSql.checkData(0, 7, 10)
        tdSql.checkData(0, 8, None)

        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table2'")
        tdSql.checkData(0, 0, 'child_table2')
        tdSql.checkData(0, 7, 0)
        tdSql.checkData(0, 8, '')

        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table3'")
        tdSql.checkData(0, 0, 'child_table3')
        tdSql.checkData(0, 8, 'child')

        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table4'")
        tdSql.checkData(0, 0, 'child_table4')
        tdSql.checkData(0, 7, 0)
        tdSql.checkData(0, 8, None)

        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table5'")
        tdSql.checkData(0, 0, 'child_table5')
        tdSql.checkData(0, 7, 23)
        tdSql.checkData(0, 8, '')

        tdSql.execute(f"alter table {dbname}.child_table1 comment 'nihao'")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table1'")
        tdSql.checkData(0, 0, 'child_table1')
        tdSql.checkData(0, 8, 'nihao')

        tdSql.execute(f"alter table {dbname}.child_table1 comment ''")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table1'")
        tdSql.checkData(0, 0, 'child_table1')
        tdSql.checkData(0, 8, '')

        tdSql.execute(f"alter table {dbname}.child_table2 comment 'fly'")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table2'")
        tdSql.checkData(0, 0, 'child_table2')
        tdSql.checkData(0, 8, 'fly')

        tdSql.execute(f"alter table {dbname}.child_table3 comment 'tdengine'")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table3'")
        tdSql.checkData(0, 0, 'child_table3')
        tdSql.checkData(0, 8, 'tdengine')

        tdSql.execute(f"alter table {dbname}.child_table4 comment 'tdengine'")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table4'")
        tdSql.checkData(0, 0, 'child_table4')
        tdSql.checkData(0, 8, 'tdengine')

        tdSql.execute(f"alter table {dbname}.child_table4 ttl 9")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table4'")
        tdSql.checkData(0, 0, 'child_table4')
        tdSql.checkData(0, 7, 9)

        tdSql.execute(f"alter table {dbname}.child_table3 ttl 9")
        tdSql.query("select * from information_schema.ins_tables where table_name like 'child_table3'")
        tdSql.checkData(0, 0, 'child_table3')
        tdSql.checkData(0, 7, 9)
    
        print("do ttl comment ........................ [passed]")


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
        7. While the subtable without TTL remains unaffected.
        8. Create/Alter ttl/comment on normal/child table
        9. Verify ttl/comment in information_schema

        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_ttl.py
            - 2025-11-03 Alex Duan Migrated from uncatalog/system-test/0-others/test_ttl_change_on_write.py
            - 2025-12-21 Alex Duan Migrated from uncatalog/system-test/2-query/test_ttl_comment.py
 
        """
        self.do_ttl()
        self.do_ttl_change_on_write()
        self.do_ttl_comment()