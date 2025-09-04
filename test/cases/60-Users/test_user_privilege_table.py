import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestUserPrivilegeTable:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_user_privilege_table(self):
        """user privilege table

        1. -

        Catalog:
            - User

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/user/privilege_table.sim

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

        tdLog.info(
            f"=============== case 1: database unauthorized and table unauthorized"
        )

        tdSql.connect("wxy")

        tdSql.execute(f"reset query cache;")
        tdSql.error(f"select * from test.st1;")
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

        tdSql.connect("wxy")

        tdSql.execute(f"reset query cache;")
        tdSql.query(f"select * from test.st1;")
        tdSql.checkRows(1)

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

        tdSql.connect("wxy")

        tdSql.execute(f"reset query cache;")
        tdSql.error(f"select tbname, * from test.st1;")
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

        tdSql.connect("wxy")

        tdSql.execute(f"reset query cache;")
        tdSql.error(f"select tbname, * from test.st1;")
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

        tdSql.connect("wxy")

        tdSql.execute(f"reset query cache;")
        tdSql.query(f"select * from test.st1;")
        tdSql.checkRows(6)

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
        tdSql.execute(f"insert into test.st1s1 values(now, 10);")
        tdSql.error(f"insert into test.st1s2 values(now, 20);")
        tdSql.error(f"select * from test.st2;")
        tdSql.execute(
            f"insert into test.st2s1 values(now, 10) test.st2s2 values(now, 20);"
        )


# system sh/exec.sh -n dnode1 -s stop -x SIGINT
