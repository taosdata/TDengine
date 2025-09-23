import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestUserPrivilegeDb:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_user_privilege_db(self):
        """Privilege: db

        Verify user privileges related to database operations, including grant, revoke, and query privileges.

        Catalog:
            - User

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/user/privilege_db.sim
            - 2025-7-19 Kaili Xu   Check that privileges of db/stb/view/topic are cleaned after drop db

        """

        tdLog.info(f"=============== create db")
        tdSql.execute(f"create database d1 vgroups 1 wal_retention_period 3600;")
        tdSql.execute(f"use d1")
        tdSql.execute(f"create table d1_stb (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table d1_stb1 (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table d1_stb2 (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create topic d1_topic_1 as select ts, i from d1_stb")
        tdSql.execute(f"create view d1_view1 as select * from d1_stb")
        tdSql.execute(f"create view d1_view2 as select * from d1_stb")
        tdSql.execute(f"create view d1_view3 as select * from d1_stb")

        tdSql.execute(f"create database d2 vgroups 1 wal_retention_period 3600;")
        tdSql.execute(f"create database d3 vgroups 1 wal_retention_period 3600;")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(5)

        tdLog.info(f"=============== create users")
        tdSql.execute(f"create user user1 PASS '123124(*&xx)'")
        tdSql.execute(f"create user user2 PASS '1234(*&xx'")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(3)

        tdLog.info(f"=============== test read")
        tdSql.error(f"GRANT read ON d1.* to a;")
        tdSql.error(f"GRANT read ON d0.* to user1;")

        tdSql.execute(f"GRANT read ON d1.* to user1;")
        tdSql.execute(f"GRANT read ON d2.* to user1;")
        tdSql.execute(f"GRANT read ON *.* to user1;")

        tdSql.execute(f"REVOKE read ON d1.* from user1;")
        tdSql.execute(f"REVOKE read ON d2.* from user1;")
        tdSql.execute(f"REVOKE read ON *.* from user1;")

        tdLog.info(f"=============== test write")
        tdSql.error(f"GRANT write ON d1.* to a;")
        tdSql.error(f"GRANT write ON d0.* to user1;")

        tdSql.execute(f"GRANT write ON d1.* to user1;")
        tdSql.execute(f"GRANT write ON d2.* to user1;")
        tdSql.execute(f"GRANT write ON *.* to user1;")

        tdSql.execute(f"REVOKE write ON d1.* from user1;")
        tdSql.execute(f"REVOKE write ON d2.* from user1;")
        tdSql.execute(f"REVOKE write ON *.* from user1;")

        tdLog.info(f"=============== test all")
        tdSql.error(f"GRANT all ON d1.* to a;")
        tdSql.error(f"GRANT all ON d0.* to user1;")

        tdSql.execute(f"GRANT all ON d1.* to user1;")
        tdSql.execute(f"GRANT all ON d2.* to user1;")
        tdSql.execute(f"GRANT all ON *.* to user1;")

        tdSql.execute(f"REVOKE all ON d1.* from user1;")
        tdSql.execute(f"REVOKE all ON d2.* from user1;")
        tdSql.execute(f"REVOKE all ON *.* from user1;")

        tdLog.info(f"=============== test read write")
        tdSql.error(f"GRANT read,write ON d1.* to a;")
        tdSql.error(f"GRANT read,write ON d0.* to user1;")

        tdSql.execute(f"GRANT read,write ON d1.* to user1;")
        tdSql.execute(f"GRANT read,write ON d2.* to user1;")
        tdSql.execute(f"GRANT read,write ON *.* to user1;")

        tdSql.execute(f"REVOKE read,write ON d1.* from user1;")
        tdSql.execute(f"REVOKE read,write ON d2.* from user1;")
        tdSql.execute(f"REVOKE read,write ON *.* from user1;")

        tdLog.info(f"=============== create users")
        tdSql.execute(f"create user u1 PASS 'taosdata'")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(4)

        tdSql.execute(f"GRANT read ON d1.* to u1;")
        tdSql.execute(f"GRANT write ON d2.* to u1;")
        tdSql.execute(f"GRANT read ON d1.d1_stb to u1")
        tdSql.execute(f"GRANT write ON d1.d1_stb to u1")
        tdSql.execute(f"GRANT write ON d1.d1_stb1 to u1")
        tdSql.execute(f"GRANT alter ON d1.d1_stb to u1")
        tdSql.execute(f"GRANT alter ON d1.d1_stb1 to u1")
        tdSql.execute(f"GRANT alter ON d1.d1_stb2 to u1")
        tdSql.execute(f"GRANT subscribe ON d1_topic_1 to u1")
        tdSql.execute(f"GRANT read ON d1.d1_view1 to u1")
        tdSql.execute(f"GRANT read ON d1.d1_view2 to u1")
        tdSql.execute(f"GRANT read ON d1.d1_view3 to u1")
        tdSql.execute(f"GRANT write ON d1.d1_view1 to u1")
        tdSql.execute(f"GRANT write ON d1.d1_view2 to u1")
        tdSql.execute(f"GRANT alter ON d1.d1_view3 to u1")
        tdSql.execute(f"GRANT all ON d1.d1_view3 to u1")
        tdSql.query(f"select * from information_schema.ins_user_privileges where user_name = 'u1'")
        tdSql.checkRows(16)

        tdLog.info(f"=============== re connect")
        tdLog.info(f"user u1 login")
        tdSql.connect("u1")

        tdSql.error(f"drop database d1;")
        tdSql.error(f"drop database d2;")

        tdSql.error(f"create stable d1.st (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"use d2")
        tdSql.execute(f"create table d2_stb (ts timestamp, i int) tags (j int)")

        # Insufficient privilege for operation
        tdSql.error(f"create topic d2_topic_1 as select ts, i from d2_stb")

        tdSql.execute(f"use d1")
        # Insufficient privilege for operation
        tdSql.error(f"drop topic d1_topic_1")
        tdSql.execute(f"create topic d1_topic_2 as select ts, i from d1_stb")

        tdSql.execute(f"drop topic d1_topic_2")

        tdLog.info(f"=============== drop db and check privileges")
        tdSql.connect("root")
        tdSql.execute(f"drop topic d1_topic_1")
        tdSql.execute(f"drop database d1")
        tdSql.query(f"select * from information_schema.ins_user_privileges where user_name = 'u1'")
        tdSql.checkRows(1)
