import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestUserPrivilegeTable:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    
    def check_show_tables(self, dbName = 'test', stbNum = 0, tbNum = 0, sysTbNum = 42, otherDbTbNum = 0):
        tdSql.query(f"show {dbName}.stables;")
        tdSql.checkRows(stbNum)
        tdSql.query(f"select * from information_schema.ins_stables where db_name='{dbName}';")
        tdSql.checkRows(stbNum)
        tdSql.query(f"show {dbName}.tables;")
        tdSql.checkRows(tbNum)
        tdSql.query(f"select * from information_schema.ins_tables where db_name='{dbName}';")
        tdSql.checkRows(tbNum)
        tdSql.query(f"select * from information_schema.ins_tables")
        tdSql.checkRows(tbNum + sysTbNum + otherDbTbNum)

    def test_user_privilege_table(self):
        """Privilege:  table

        Verify user privileges related to table operations, including grant, revoke, and query privileges.

        Catalog:
            - User

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/user/privilege_table.sim
            - 2025-9-29 Cary  Xu   TS-6667: Check privilege of show stables
            - 2026-2-14 Cary  Xu   6745760688: Check privilege of show tables

        """

        tdLog.info(f"=============== init env")
        tdSql.execute(f"drop database if exists test;")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")
        tdSql.execute(
            f"create stable st1(ts timestamp, i int) tags(id int, loc varchar(20));"
        )
        tdSql.execute(f"create table st1s1 using st1 tags(1, 'beijing');")
        tdSql.execute(f"create table st1s2 using st1 tags(2, 'shanghai');")
        tdSql.execute(f"insert into st1s1 values(now, 1) st1s2 values(now, 2);")
        tdSql.execute(
            f"create stable st2(ts timestamp, i int) tags(id int, loc varchar(20));"
        )
        tdSql.execute(f"create table st2s1 using st2 tags(1, 'beijing');")
        tdSql.execute(f"create table st2s2 using st2 tags(2, 'shanghai');")
        tdSql.execute(f"insert into st2s1 values(now, 1) st2s2 values(now, 2);")
        tdSql.execute(f"create user wxy pass 'taosdata';")
        self.check_show_tables("test", 2, 4, 42)

        tdLog.info(
            f"=============== case 1: database unauthorized and table unauthorized"
        )

        tdSql.connect("wxy")

        tdSql.execute(f"reset query cache;")
        tdSql.error(f"select * from test.st1;")
        tdSql.error(f"show test.stables;")
        tdSql.error(
            f"insert into test.st1s1 values(now, 10) test.st1s2 values(now, 20);"
        )
        tdSql.error(f"select * from test.st2;")
        tdSql.error(
            f"insert into test.st2s1 values(now, 10) test.st2s2 values(now, 20);"
        )

        tdLog.info(
            f"=============== case 2: database unauthorized and table read privilege"
        )

        tdSql.connect("root")

        tdSql.execute(f"grant read on test.st1 to wxy;")

        tdSql.connect("wxy")

        tdSql.execute(f"reset query cache;")
        tdSql.query(f"select * from test.st1;")
        tdSql.checkRows(2)
        self.check_show_tables("test", 1, 2, 42)

        tdSql.error(
            f"insert into test.st1s1 values(now, 10) test.st1s2 values(now, 20);"
        )
        tdSql.error(f"select * from test.st2;")
        tdSql.error(
            f"insert into test.st2s1 values(now, 10) test.st2s2 values(now, 20);"
        )

        tdLog.info(
            f"=============== case 3: database unauthorized and table read privilege with condition"
        )

        tdSql.connect("root")

        tdSql.execute(f"revoke read on test.st1 from wxy;")
        tdSql.execute(f"grant read on test.st1 with id = 1 to wxy;")
        tdSql.query(f"show test.stables;")
        tdSql.checkRows(2)

        tdSql.connect("wxy")

        tdSql.execute(f"reset query cache;")
        tdSql.query(f"select * from test.st1;")
        tdSql.checkRows(1)
        self.check_show_tables("test", 1, 2, 42)

        tdSql.error(f"insert into test.st1s1 values(now, 10);")
        tdSql.error(f"insert into test.st1s2 values(now, 20);")
        tdSql.error(f"select * from test.st2;")
        tdSql.error(
            f"insert into test.st2s1 values(now, 10) test.st2s2 values(now, 20);"
        )

        tdLog.info(
            f"=============== case 4: database unauthorized and table write privilege"
        )

        tdSql.connect("root")

        tdSql.execute(f"revoke read on test.st1 with id = 1 from wxy;")
        tdSql.execute(f"grant write on test.st1 to wxy;")
        tdSql.query(f"show test.stables;")
        tdSql.checkRows(2)

        tdSql.connect("wxy")

        tdSql.execute(f"reset query cache;")
        tdSql.error(f"select tbname, * from test.st1;")
        self.check_show_tables("test", 1, 2, 42)
        tdSql.execute(f"insert into test.st1s1 values(now, 10);")
        tdSql.execute(f"insert into test.st1s2 values(now, 20);")
        tdSql.error(f"select * from test.st2;")
        tdSql.error(
            f"insert into test.st2s1 values(now, 10) test.st2s2 values(now, 20);"
        )

        tdLog.info(
            f"=============== case 5: database unauthorized and table write privilege with condition"
        )

        tdSql.connect("root")

        tdSql.execute(f"revoke write on test.st1 from wxy;")
        tdSql.execute(f"grant write on test.st1 with id = 1 to wxy;")
        tdSql.query(f"show test.stables;")
        tdSql.checkRows(2)

        tdSql.connect("wxy")

        tdSql.execute(f"reset query cache;")
        tdSql.error(f"select tbname, * from test.st1;")
        self.check_show_tables("test", 1, 2, 42)
        tdSql.execute(f"insert into test.st1s1 values(now, 10);")
        tdSql.execute(
            f"insert into test.st1s3 using test.st1 tags(1, 'dachang') values(now, 100);"
        )
        tdSql.error(f"insert into test.st1s2 values(now, 20);")
        tdSql.error(
            f"insert into test.st1s4 using test.st1 tags(3, 'dachang') values(now, 300);"
        )
        tdSql.error(f"select * from test.st2;")
        tdSql.error(
            f"insert into test.st2s1 values(now, 10) test.st2s2 values(now, 20);"
        )

        tdLog.info(
            f"=============== case 6: database read privilege and table unauthorized"
        )

        tdSql.connect("root")

        tdSql.execute(f"revoke write on test.st1 with id = 1 from wxy;")
        tdSql.execute(f"grant read on test.* to wxy;")
        tdSql.query(f"show test.stables;")
        tdSql.checkRows(2)

        tdSql.connect("wxy")

        tdSql.execute(f"reset query cache;")
        tdSql.query(f"select * from test.st1;")
        tdSql.checkRows(6)
        self.check_show_tables("test", 2, 5, 42)

        tdSql.error(
            f"insert into test.st1s1 values(now, 10) test.st1s2 values(now, 20);"
        )
        tdSql.query(f"select * from test.st2;")
        tdSql.checkRows(2)

        tdSql.error(
            f"insert into test.st2s1 values(now, 10) test.st2s2 values(now, 20);"
        )

        tdLog.info(
            f"=============== case 7: database read privilege and table read privilege"
        )

        tdSql.connect("root")

        tdSql.execute(f"grant read on test.st1 to wxy;")

        tdSql.connect("wxy")

        tdSql.execute(f"reset query cache;")
        tdSql.query(f"select * from test.st1;")
        tdSql.checkRows(6)
        self.check_show_tables("test", 2, 5, 42)

        tdSql.error(
            f"insert into test.st1s1 values(now, 10) test.st1s2 values(now, 20);"
        )
        tdSql.query(f"select * from test.st2;")
        tdSql.checkRows(2)

        tdSql.error(
            f"insert into test.st2s1 values(now, 10) test.st2s2 values(now, 20);"
        )

        tdLog.info(
            f"=============== case 8: database read privilege and table read privilege with condition"
        )

        tdSql.connect("root")

        tdSql.execute(f"revoke read on test.st1 from wxy;")
        tdSql.execute(f"grant read on test.st1 with id = 1 to wxy;")

        tdSql.connect("wxy")

        tdSql.execute(f"reset query cache;")
        tdSql.query(f"select * from test.st1;")
        tdSql.checkRows(4)
        self.check_show_tables("test", 2, 5, 42)

        tdSql.error(
            f"insert into test.st1s1 values(now, 10) test.st1s2 values(now, 20);"
        )
        tdSql.query(f"select * from test.st2;")
        tdSql.checkRows(2)

        tdSql.error(
            f"insert into test.st2s1 values(now, 10) test.st2s2 values(now, 20);"
        )

        tdLog.info(
            f"=============== case 9: database read privilege and table write privilege"
        )

        tdSql.connect("root")

        tdSql.execute(f"revoke read on test.st1 with id = 1 from wxy;")
        tdSql.execute(f"grant write on test.st1 to wxy;")

        tdSql.connect("wxy")

        tdSql.execute(f"reset query cache;")
        tdSql.query(f"select * from test.st1;")
        tdSql.checkRows(6)
        self.check_show_tables("test", 2, 5, 42)

        tdSql.execute(
            f"insert into test.st1s1 values(now, 10) test.st1s2 values(now, 20);"
        )
        tdSql.query(f"select * from test.st2;")
        tdSql.checkRows(2)

        tdSql.error(
            f"insert into test.st2s1 values(now, 10) test.st2s2 values(now, 20);"
        )

        tdLog.info(
            f"=============== case 10: database read privilege and table write privilege with condition"
        )

        tdSql.connect("root")

        tdSql.execute(f"revoke write on test.st1 from wxy;")
        tdSql.execute(f"grant write on test.st1 with id = 1 to wxy;")

        tdSql.connect("wxy")

        tdSql.execute(f"reset query cache;")
        tdSql.query(f"select * from test.st1;")
        tdSql.checkRows(8)
        self.check_show_tables("test", 2, 5, 42)

        tdSql.execute(f"insert into test.st1s1 values(now, 10);")
        tdSql.error(f"insert into test.st1s2 values(now, 20);")
        tdSql.query(f"select * from test.st2;")
        tdSql.checkRows(2)

        tdSql.error(
            f"insert into test.st2s1 values(now, 10) test.st2s2 values(now, 20);"
        )

        tdLog.info(
            f"=============== case 11: database write privilege and table unauthorized"
        )

        tdSql.connect("root")

        tdSql.execute(f"revoke read on test.* from wxy;")
        tdSql.execute(f"revoke write on test.st1 with id = 1 from wxy;")
        tdSql.execute(f"grant write on test.* to wxy;")

        tdSql.connect("wxy")

        tdSql.execute(f"reset query cache;")
        tdSql.error(f"select * from test.st1;")
        self.check_show_tables("test", 2, 5, 42)
        tdSql.execute(
            f"insert into test.st1s1 values(now, 10) test.st1s2 values(now, 20);"
        )
        tdSql.error(f"select * from test.st2;")
        tdSql.execute(
            f"insert into test.st2s1 values(now, 10) test.st2s2 values(now, 20);"
        )

        tdLog.info(
            f"=============== case 12: database write privilege and table read privilege"
        )

        tdSql.connect("root")

        tdSql.execute(f"grant read on test.st1 to wxy;")

        tdSql.connect("wxy")

        tdSql.execute(f"reset query cache;")
        tdSql.query(f"select * from test.st1;")
        tdSql.checkRows(11)
        self.check_show_tables("test", 2, 5, 42)

        tdSql.execute(
            f"insert into test.st1s1 values(now, 10) test.st1s2 values(now, 20);"
        )
        tdSql.error(f"select * from test.st2;")
        tdSql.execute(
            f"insert into test.st2s1 values(now, 10) test.st2s2 values(now, 20);"
        )

        tdLog.info(
            f"=============== case 13: database write privilege and table read privilege with condition"
        )

        tdSql.connect("root")

        tdSql.execute(f"revoke read on test.st1 from wxy;")
        tdSql.execute(f"grant read on test.st1 with id = 1 to wxy;")

        tdSql.connect("wxy")

        tdSql.execute(f"reset query cache;")
        tdSql.query(f"select * from test.st1;")
        tdSql.checkRows(8)
        self.check_show_tables("test", 2, 5, 42)
    
        tdSql.execute(
            f"insert into test.st1s1 values(now, 10) test.st1s2 values(now, 20);"
        )
        tdSql.error(f"select * from test.st2;")
        tdSql.execute(
            f"insert into test.st2s1 values(now, 10) test.st2s2 values(now, 20);"
        )

        tdLog.info(
            f"=============== case 14: database write privilege and table write privilege"
        )

        tdSql.connect("root")

        tdSql.execute(f"revoke read on test.st1 with id = 1 from wxy;")
        tdSql.execute(f"grant write on test.st1 to wxy;")

        tdSql.connect("wxy")

        tdSql.execute(f"reset query cache;")
        tdSql.error(f"select * from test.st1;")
        self.check_show_tables("test", 2, 5, 42)
        tdSql.execute(
            f"insert into test.st1s1 values(now, 10) test.st1s2 values(now, 20);"
        )
        tdSql.error(f"select * from test.st2;")
        tdSql.execute(
            f"insert into test.st2s1 values(now, 10) test.st2s2 values(now, 20);"
        )

        tdLog.info(
            f"=============== case 15: database write privilege and table write privilege with condition"
        )

        tdSql.connect("root")

        tdSql.execute(f"revoke write on test.st1 from wxy;")
        tdSql.execute(f"grant write on test.st1 with id = 1 to wxy;")

        tdSql.connect("wxy")

        tdSql.execute(f"reset query cache;")
        tdSql.error(f"select * from test.st1;")
        self.check_show_tables("test", 2, 5, 42)
        tdSql.execute(f"insert into test.st1s1 values(now, 10);")
        tdSql.error(f"insert into test.st1s2 values(now, 20);")
        tdSql.error(f"select * from test.st2;")
        tdSql.execute(
            f"insert into test.st2s1 values(now, 10) test.st2s2 values(now, 20);"
        )

        tdLog.info(
            f"=============== case 16: create new database and tables and check privileges"
        )

        tdSql.connect("root")
        tdSql.execute(f"create database test2 vgroups 4;")
        tdSql.execute(f"use test2;")
        for i in range(0, 2):
            tdSql.execute(f"create stable st{i}(ts timestamp, i int) tags(id int, loc varchar(20));")
            for j in range(0, 2):
                tdSql.execute(f"create table st{i}s{j} using st{i} tags({j}, 'loc{j}');")
                tdSql.execute(f"insert into st{i}s{j} values(now, {i * j});")
        for i in range(0, 2):
            tdSql.execute(f"create table ntb{i}(ts timestamp, i int);")
            tdSql.execute(f"insert into ntb{i} values(now, {i});")

        tdSql.execute(f"grant read on test2.st1 to wxy;")

        tdSql.connect("wxy")
        tdSql.execute(f"reset query cache;")
        self.check_show_tables("test", 2, 5, 42)
        self.check_show_tables("test2", 1, 10, 42, 5)



# system sh/exec.sh -n dnode1 -s stop -x SIGINT
