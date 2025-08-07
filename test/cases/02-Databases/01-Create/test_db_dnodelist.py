import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseDnodeList:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_dnode_list(self):
        """Options: dnodelist

        1. create database with dnodelist option
        2. test the creation with different numbers of replicas and vgroups
        3. alter database dnodelist option

        Catalog:
            - Database:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/db/dnodelist.sim

        """

        clusterComCheck.checkDnodes(5)

        tdLog.info(f"--- error case")

        tdSql.error(
            f"create database d1 vgroups 1 dnodes '1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890';"
        )
        tdSql.error(f"create database d1 vgroups 1 dnodes '1 ';")
        tdSql.error(f"create database d1 vgroups 1 dnodes ' 1';")
        tdSql.error(f"create database d1 vgroups 1 dnodes '1,';")
        tdSql.error(f"create database d1 vgroups 1 dnodes '1, ';")
        tdSql.error(f"create database d1 vgroups 1 dnodes 'a ';")
        tdSql.error(f"create database d1 vgroups 1 dnodes '- ';")
        tdSql.error(f"create database d1 vgroups 1 dnodes '1,1';")
        tdSql.error(f"create database d1 vgroups 1 dnodes '1, 1';")
        tdSql.error(f"create database d1 vgroups 1 dnodes '1,1234567890';")
        tdSql.error(f"create database d1 vgroups 1 dnodes '1,2,6';")
        tdSql.error(f"create database d1 vgroups 1 dnodes ',1,2';")
        tdSql.error(f"create database d1 vgroups 1 dnodes 'x1,2';")
        tdSql.error(f"create database d1 vgroups 1 dnodes 'c1,ab2';")
        tdSql.error(f"create database d1 vgroups 1 dnodes '1,1,2';")

        tdSql.error(f"create database d1 vgroups 1 replica 2 dnodes '1';")
        tdSql.error(f"create database d1 vgroups 1 replica 2 dnodes '1,8';")
        tdSql.error(f"create database d1 vgroups 1 replica 3 dnodes '1';")
        tdSql.error(f"create database d1 vgroups 1 replica 3 dnodes '1,2';")
        tdSql.error(f"create database d1 vgroups 1 replica 3 dnodes '1,2,4,6';")

        tdLog.info(f"--- replica 1")

        tdLog.info(f"--- case10")
        tdSql.execute(f"create database d10 vgroups 1 dnodes '1';")
        tdSql.query(f"show dnodes;")
        tdSql.checkKeyData(1, 2, 1)

        tdSql.error(f"alter database d10 replica 1 dnodes '1,2,3';")
        tdSql.execute(f"drop database d10;")

        tdLog.info(f"--- case11")
        tdSql.execute(f"create database d11 vgroups 1 dnodes '2';")
        tdSql.query(f"show dnodes;")
        tdSql.checkKeyData(2, 2, 1)

        tdSql.execute(f"drop database d11;")

        tdLog.info(f"--- case12")
        tdSql.execute(f"create database d12 vgroups 2 dnodes '3,4';")
        tdSql.query(f"show dnodes;")
        tdSql.checkKeyData(3, 2, 1)
        tdSql.checkKeyData(4, 2, 1)

        tdSql.execute(f"drop database d12;")

        tdLog.info(f"--- case13")
        tdSql.execute(f"create database d13 vgroups 2 dnodes '5';")
        tdSql.query(f"show dnodes;")
        tdSql.checkKeyData(5, 2, 2)

        tdSql.execute(f"drop database d13;")

        tdLog.info(f"--- case14")
        tdSql.execute(f"create database d14 vgroups 1 dnodes '1,2,5';")
        tdSql.execute(f"drop database d14;")

        tdLog.info(f"--- case15")
        tdSql.execute(f"create database d15 vgroups 2 dnodes '1,4,3';")
        tdSql.execute(f"drop database d15;")

        tdLog.info(f"--- case16")
        tdSql.execute(f"create database d16 vgroups 3 dnodes '1';")
        tdSql.query(f"show dnodes;")
        tdSql.checkKeyData(1, 2, 3)

        tdSql.execute(f"drop database d16;")

        tdLog.info(f"--- case17")
        tdSql.execute(f"create database d17 vgroups 3 dnodes '1,4';")
        tdSql.execute(f"drop database d17;")

        tdLog.info(f"--- case18")
        tdSql.execute(f"create database d18 vgroups 3 dnodes '1,2,4';")
        tdSql.query(f"show dnodes;")
        tdSql.checkKeyData(1, 2, 1)
        tdSql.checkKeyData(2, 2, 1)
        tdSql.checkKeyData(4, 2, 1)

        tdSql.execute(f"drop database d18;")

        tdLog.info(f"--- replica 2")

        tdLog.info(f"--- case20")
        tdSql.execute(f"create database d20 replica 2 vgroups 1 dnodes '1,2';")
        tdSql.query(f"show dnodes;")
        tdSql.checkKeyData(1, 2, 1)
        tdSql.checkKeyData(2, 2, 1)

        tdSql.execute(f"drop database d20;")

        tdLog.info(f"--- case21")
        tdSql.execute(f"create database d21 replica 2 vgroups 3 dnodes '1,2,3';")
        tdSql.query(f"show dnodes;")
        tdSql.checkKeyData(1, 2, 2)
        tdSql.checkKeyData(2, 2, 2)
        tdSql.checkKeyData(3, 2, 2)

        tdSql.execute(f"drop database d21;")

        tdLog.info(f"--- case22")
        tdSql.execute(f"create database d22 replica 2 vgroups 2 dnodes '1,2';")
        tdSql.query(f"show dnodes;")
        tdSql.checkKeyData(1, 2, 2)
        tdSql.checkKeyData(2, 2, 2)

        tdSql.execute(f"drop database d22;")

        tdLog.info(f"--- replica 3")

        tdLog.info(f"--- case30")
        tdSql.execute(f"create database d30 replica 3 vgroups 3 dnodes '1,2,3';")
        tdSql.query(f"show dnodes;")
        tdSql.checkKeyData(1, 2, 3)
        tdSql.checkKeyData(2, 2, 3)
        tdSql.checkKeyData(3, 2, 3)

        tdSql.error(f"alter database d30 replica 1 dnodes '1';")
        tdSql.execute(f"drop database d30;")

        tdLog.info(f"--- case31")
        tdSql.execute(f"create database d31 replica 3 vgroups 2 dnodes '1,2,4';")
        tdSql.query(f"show dnodes;")
        tdSql.checkKeyData(1, 2, 2)
        tdSql.checkKeyData(2, 2, 2)
        tdSql.checkKeyData(4, 2, 2)

        tdSql.execute(f"drop database d31;")

        tdLog.info(f"--- case32")
        tdSql.execute(f"create database d32 replica 3 vgroups 4 dnodes '4,2,3,1';")
        tdSql.query(f"show dnodes;")
        tdSql.checkKeyData(1, 2, 3)
        tdSql.checkKeyData(2, 2, 3)
        tdSql.checkKeyData(3, 2, 3)
        tdSql.checkKeyData(4, 2, 3)

        tdSql.execute(f"drop database d32;")

        tdLog.info(f"--- case33")
        tdSql.execute(f"create database d33 replica 3 vgroups 5 dnodes '4,2,3,1,5';")
        tdSql.query(f"show dnodes;")
        tdSql.checkKeyData(1, 2, 3)
        tdSql.checkKeyData(2, 2, 3)
        tdSql.checkKeyData(3, 2, 3)
        tdSql.checkKeyData(4, 2, 3)
        tdSql.checkKeyData(5, 2, 3)

        tdSql.execute(f"drop database d33;")
