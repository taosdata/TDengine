import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestUserBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_user_basic(self):
        """user basic

        1. -

        Catalog:
            - User

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/user/basic.sim

        """

        tdLog.info(f"=============== step0")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkKeyData("root", 1, 1)
        tdSql.checkKeyData("root", 2, 1)
        tdSql.checkKeyData("root", 3, 1)
        tdSql.checkKeyData("root", 4, 1)

        tdSql.execute(f"alter user root pass 'taosdata'")

        tdSql.error(f"ALTER USER root SYSINFO 0")
        tdSql.error(f"ALTER USER root SYSINFO 1")
        tdSql.error(f"ALTER USER root enable 0")
        tdSql.error(f"ALTER USER root enable 1")
        tdSql.error(f"ALTER USER root createdb 0")
        tdSql.error(f"ALTER USER root createdb 1")

        # sql_error create database db vgroups 1;
        tdSql.error(f"GRANT read ON db.* to root;")
        tdSql.error(f"GRANT read ON *.* to root;")
        tdSql.error(f"REVOKE read ON db.* from root;")
        tdSql.error(f"REVOKE read ON *.* from root;")
        tdSql.error(f"GRANT write ON db.* to root;")
        tdSql.error(f"GRANT write ON *.* to root;")
        tdSql.error(f"REVOKE write ON db.* from root;")
        tdSql.error(f"REVOKE write ON *.* from root;")
        tdSql.error(f"REVOKE write ON *.* from root;")

        tdSql.error(f"GRANT all ON *.* to root;")
        tdSql.error(f"REVOKE all ON *.* from root;")
        tdSql.error(f"GRANT read,write ON *.* to root;")
        tdSql.error(f"REVOKE read,write ON *.* from root;")

        tdLog.info(f"=============== step1: sysinfo create")
        tdSql.execute(f"CREATE USER u1 PASS 'taosdata' SYSINFO 0;")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(2)

        tdSql.checkKeyData("u1", 1, 0)
        tdSql.checkKeyData("u1", 2, 1)
        tdSql.checkKeyData("u1", 3, 0)
        tdSql.checkKeyData("u1", 4, 0)

        tdSql.execute(f"CREATE USER u2 PASS 'taosdata' SYSINFO 1;")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(3)

        tdSql.checkKeyData("u2", 1, 0)
        tdSql.checkKeyData("u2", 2, 1)
        tdSql.checkKeyData("u2", 3, 1)
        tdSql.checkKeyData("u2", 4, 0)

        tdLog.info(f"=============== step2: sysinfo alter")
        tdSql.execute(f"ALTER USER u1 SYSINFO 1")
        tdSql.query(f"select * from information_schema.ins_users")

        tdSql.checkKeyData("u1", 1, 0)
        tdSql.checkKeyData("u1", 2, 1)
        tdSql.checkKeyData("u1", 3, 1)
        tdSql.checkKeyData("u1", 4, 0)

        tdSql.execute(f"ALTER USER u1 SYSINFO 0")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkKeyData("u1", 1, 0)
        tdSql.checkKeyData("u1", 2, 1)
        tdSql.checkKeyData("u1", 3, 0)
        tdSql.checkKeyData("u1", 4, 0)

        tdSql.execute(f"ALTER USER u1 SYSINFO 0")
        tdSql.execute(f"ALTER USER u1 SYSINFO 0")

        tdSql.execute(f"drop user u1")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step3: enable alter")
        tdSql.execute(f"ALTER USER u2 enable 0")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(2)

        tdSql.checkKeyData("u2", 1, 0)
        tdSql.checkKeyData("u2", 2, 0)
        tdSql.checkKeyData("u2", 3, 1)
        tdSql.checkKeyData("u2", 4, 0)

        tdSql.execute(f"ALTER USER u2 enable 1")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkKeyData("u2", 1, 0)
        tdSql.checkKeyData("u2", 2, 1)
        tdSql.checkKeyData("u2", 3, 1)
        tdSql.checkKeyData("u2", 4, 0)

        tdSql.execute(f"ALTER USER u2 enable 1")
        tdSql.execute(f"ALTER USER u2 enable 1")

        tdLog.info(f"=============== step4: createdb alter")
        tdSql.execute(f"ALTER USER u2 createdb 1")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(2)
        tdSql.checkKeyData("u2", 1, 0)
        tdSql.checkKeyData("u2", 2, 1)
        tdSql.checkKeyData("u2", 3, 1)
        tdSql.checkKeyData("u2", 4, 1)

        tdSql.execute(f"ALTER USER u2 createdb 0")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkKeyData("u2", 1, 0)
        tdSql.checkKeyData("u2", 2, 1)
        tdSql.checkKeyData("u2", 3, 1)
        tdSql.checkKeyData("u2", 4, 0)

        tdLog.info(f"=============== restart taosd")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== step5: enable privilege")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(2)

        tdSql.checkKeyData("u2", 1, 0)
        tdSql.checkKeyData("u2", 2, 1)
        tdSql.checkKeyData("u2", 3, 1)
        tdSql.checkKeyData("u2", 4, 0)

        tdSql.error(f"CREATE USER u100 PASS 'taosdata' SYSINFO -1;")
        tdSql.error(f"CREATE USER u101 PASS 'taosdata' SYSINFO 2;")
        tdSql.error(f"CREATE USER u102 PASS 'taosdata' SYSINFO 20000;")
        tdSql.error(f"CREATE USER u103 PASS 'taosdata' SYSINFO 1000;")
        tdSql.error(f"ALTER USER u1 enable -1")
        tdSql.error(f"ALTER USER u1 enable 2")
        tdSql.error(f"ALTER USER u1 enable 1000")
        tdSql.error(f"ALTER USER u1 enable 10000")
        tdSql.error(f"ALTER USER u1 sysinfo -1")
        tdSql.error(f"ALTER USER u1 sysinfo 2")
        tdSql.error(f"ALTER USER u1 sysinfo 1000")
        tdSql.error(f"ALTER USER u1 sysinfo -20000")
        tdSql.error(f"ALTER USER u1 createdb -1")
        tdSql.error(f"ALTER USER u1 createdb 3")
        tdSql.error(f"ALTER USER u1 createdb 1000")
        tdSql.error(f"ALTER USER u1 createdb 100000")
