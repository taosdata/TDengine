import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestUserPrivilegeTopic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_user_privilege_topic(self):
        """Privilege: topic

        Verify user privileges related to topic operations, including grant, revoke, and query privileges.

        Catalog:
            - User

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/user/privilege_topic.sim

        """

        tdLog.info(f"=============== create db")
        tdSql.execute(f"create database root_d1 vgroups 1 wal_retention_period 3600;")
        tdSql.execute(f"create database root_d2 vgroups 1 wal_retention_period 3600;")
        tdSql.execute(f"create database root_d3 vgroups 1 wal_retention_period 3600;")

        tdSql.query(f"show user privileges")
        tdSql.checkRows(1)
        tdSql.checkKeyData("root", 1, "all")
        tdSql.checkKeyData("root", 2, "all")

        tdLog.info(f"=============== create users")
        tdSql.execute(f"create user user1 PASS 'taosdata'")
        tdSql.execute(f"create user user2 PASS 'taosdata'")
        tdSql.execute(f"create user user3 PASS 'taosdata'")
        tdSql.execute(f"create user user4 PASS 'taosdata'")
        tdSql.execute(f"create user user5 PASS 'taosdata'")
        tdSql.execute(f"create user user6 PASS 'taosdata'")
        tdSql.execute(f"alter user user1 sysinfo 0")

        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(7)

        tdSql.execute(f"GRANT read ON root_d1.* to user1;")
        tdSql.execute(f"GRANT write ON root_d2.* to user2;")
        tdSql.execute(f"GRANT read ON root_d3.* to user3;")
        tdSql.execute(f"GRANT write ON root_d3.* to user3;")

        tdSql.query(f"show user privileges")
        tdSql.checkRows(5)
        tdSql.checkKeyData("user1", 1, "read")
        tdSql.checkKeyData("user1", 2, "root_d1")
        tdSql.checkKeyData("user2", 1, "write")
        tdSql.checkKeyData("user2", 2, "root_d2")

        tdLog.info(f"=============== create topis")
        tdSql.execute(f"use root_d1")
        tdSql.execute(f"create table root_d1_stb (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create topic root_d1_topic1 as select ts, i from root_d1_stb")
        tdSql.execute(f"create topic root_d1_topic2 as select ts, i from root_d1_stb")
        tdSql.execute(f"create topic root_d1_topic3 as select ts, i from root_d1_stb")
        tdSql.execute(f"create topic root_d1_topic4 as select ts, i from root_d1_stb")

        tdSql.query(f"show user privileges")
        tdSql.checkRows(5)

        tdSql.execute(f"GRANT subscribe ON root_d1_topic1 TO user4")
        tdSql.execute(f"GRANT subscribe ON root_d1_topic2 TO user5")
        tdSql.execute(f"GRANT subscribe ON root_d1_topic3 TO user6")
        tdSql.query(f"show user privileges")
        tdSql.checkRows(8)
        tdSql.checkKeyData("user4", 1, "subscribe")
        tdSql.checkKeyData("user4", 2, "root_d1_topic1")
        tdSql.checkKeyData("user5", 1, "subscribe")
        tdSql.checkKeyData("user5", 2, "root_d1_topic2")
        tdSql.checkKeyData("user6", 1, "subscribe")
        tdSql.checkKeyData("user6", 2, "root_d1_topic3")

        tdSql.execute(f"REVOKE subscribe ON root_d1_topic3 from user6")
        tdSql.query(f"show user privileges")
        tdSql.checkRows(7)
        tdSql.checkKeyData("user4", 1, "subscribe")
        tdSql.checkKeyData("user4", 2, "root_d1_topic1")
        tdSql.checkKeyData("user5", 1, "subscribe")
        tdSql.checkKeyData("user5", 2, "root_d1_topic2")

        tdLog.info(f"=============== repeat revoke/grant or invalid revoke/grant")
        tdSql.execute(f"GRANT subscribe ON root_d1_topic1 to user4")
        tdSql.execute(f"GRANT subscribe ON root_d1_topic2 to user4")
        tdSql.execute(f"GRANT subscribe ON root_d1_topic3 to user4")
        tdSql.execute(f"GRANT subscribe ON root_d1_topic1 to user5")
        tdSql.execute(f"GRANT subscribe ON root_d1_topic2 to user5")
        tdSql.execute(f"GRANT subscribe ON root_d1_topic3 to user5")
        tdSql.execute(f"GRANT subscribe ON root_d1_topic1 to user6")
        tdSql.execute(f"GRANT subscribe ON root_d1_topic2 to user6")
        tdSql.execute(f"GRANT subscribe ON root_d1_topic3 to user6")
        tdSql.execute(f"REVOKE subscribe ON root_d1_topic1 from user4")
        tdSql.execute(f"REVOKE subscribe ON root_d1_topic2 from user4")
        tdSql.execute(f"REVOKE subscribe ON root_d1_topic3 from user4")
        tdSql.execute(f"REVOKE subscribe ON root_d1_topic1 from user5")
        tdSql.execute(f"REVOKE subscribe ON root_d1_topic2 from user5")
        tdSql.execute(f"REVOKE subscribe ON root_d1_topic3 from user5")
        tdSql.execute(f"REVOKE subscribe ON root_d1_topic1 from user6")
        tdSql.execute(f"REVOKE subscribe ON root_d1_topic2 from user6")
        tdSql.execute(f"REVOKE subscribe ON root_d1_topic3 from user6")

        tdLog.info(f"=============== invalid revoke/grant")
        tdSql.error(f"GRANT subscribe ON root_d1_topicx from user5")
        tdSql.error(f"REVOKE subscribe ON root_d1_topicx from user5")

        tdLog.info(f"=============== check")
        tdSql.execute(f"GRANT subscribe ON root_d1_topic1 TO user4")
        tdSql.execute(f"GRANT subscribe ON root_d1_topic2 TO user5")
        tdSql.execute(f"GRANT subscribe ON root_d1_topic3 TO user6")
        tdSql.query(f"show user privileges")
        tdSql.checkRows(8)

        tdLog.info(f"=============== re connect")
        tdLog.info(f"user u1 login")
        tdSql.connect("user1")

        tdSql.error(f"show user privileges")
