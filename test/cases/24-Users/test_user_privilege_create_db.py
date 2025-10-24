import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestUserPrivilegeCreateDb:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_user_privilege_create_db(self):
        """Privilege: create db

        Verify user privileges for database creation, including grant, revoke, and query operations.

        Catalog:
            - User

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/user/privilege_create_db.sim

        """

        tdLog.info(f"========================root user create user")
        tdSql.execute(f'create user u1 pass "taosdata"')
        tdSql.execute(f'create user u2 pass "taosdata"')
        tdSql.execute(f"alter user u1 createdb 1")
        tdSql.execute(f"alter user u2 createdb 1")
        tdSql.execute(f"create database test")
        tdSql.query(
            f'select * from information_schema.ins_user_privileges where user_name == "root"'
        )
        tdSql.checkRows(1)

        tdLog.info(f"=============connect with u1")
        tdSql.connect("u1")
        tdSql.execute(f"create database u1_d1")
        tdSql.execute(f"use u1_d1")
        tdSql.execute(f"create table u1_d1.t1(ts timestamp, c2 int)")
        tdSql.execute(f"use information_schema")
        tdSql.query(
            f'select * from ins_user_privileges where user_name == "u1" order by privilege'
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, "read")
        tdSql.checkData(1, 1, "write")
        tdSql.checkData(0, 2, "u1_d1")
        tdSql.checkData(1, 2, "u1_d1")

        tdSql.error(f"grant all on *.* to u1")
        tdSql.error(f"grant all on test.* to u1")

        tdLog.info(f"=============connect with u2")
        tdSql.connect("u2")
        tdSql.execute(f"create database u2_d1")
        tdSql.execute(f"use u2_d1")
        tdSql.execute(f"create table u2_d1.t1(ts timestamp, c2 int)")
        tdSql.execute(f"use information_schema")
        tdSql.query(
            f'select * from ins_user_privileges where user_name == "u2" order by privilege'
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, "read")
        tdSql.checkData(1, 1, "write")
        tdSql.checkData(0, 2, "u2_d1")
        tdSql.checkData(1, 2, "u2_d1")

        tdSql.error(f"select * from u1_d1.t1")
        tdSql.error(f"revoke read on u2_d1.* from u2")

        tdLog.info(f"=============connect with root, revoke read from u1, all from u2")
        tdSql.connect("root")
        tdSql.execute(f"revoke read on u1_d1.* from u1")
        tdSql.execute(f"revoke all on u2_d1.* from u2")

        tdLog.info(f"=============connect with u1")
        tdSql.connect("u1")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"insert into u1_d1.t1 values(now, 1)")
        tdSql.error(f"select * from u1_d1.t1;")

        tdLog.info(f"=============connect with u2")
        tdSql.connect("u2")
        tdSql.error(f"select * from u2_d1.t1;")
        tdSql.error(f"insert into u2_d1.t1 values(now, 1)")

        tdLog.info(f"=============connect with root, grant read to u1, all to u2")
        tdSql.connect("root")
        tdSql.execute(f"grant read on u1_d1.* to u1")
        tdSql.execute(f"grant all on u2_d1.* to u2")

        tdLog.info(f"=============connect with u1")
        tdSql.connect("u1")
        tdSql.execute(f"reset query cache")
        tdSql.query(f"select * from u1_d1.t1;")
        tdSql.execute(f"insert into u1_d1.t1 values(now, 2)")

        tdLog.info(f"=============connect with u2")
        tdSql.connect("u2")
        tdSql.query(f"select * from u2_d1.t1;")
        tdSql.execute(f"insert into u2_d1.t1 values(now, 2)")
