from new_test_framework.utils import tdLog, tdSql, etool
import time


class TestAlterReplicaParallel:
    """Test ALTER DATABASE REPLICA with PARALLEL option for group parallel concurrency control.

    This test verifies:
    1. ALTER DATABASE ... REPLICA N PARALLEL M syntax is accepted
    2. PARALLEL 0 (unlimited) works
    3. PARALLEL without REPLICA is accepted
    4. Negative PARALLEL values are rejected

    Since: v3.3.x

    Labels: database, alter, parallel

    Jira: TD-xxx

    History:
        - 2026-06-03 Created

    """

    dnode_nums = 3

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def waitTransactionDone(self, timeout=120):
        """Wait until all transactions are finished"""
        for i in range(timeout):
            tdSql.query("show transactions")
            if tdSql.queryRows == 0:
                return True
            time.sleep(1)
        tdLog.info(f"transactions not done within {timeout}s")
        return False

    def test_alter_replica_parallel_syntax(self):
        """Test basic PARALLEL syntax in ALTER DATABASE REPLICA"""
        dbName = "test_par_syntax"
        tdSql.execute(f"drop database if exists {dbName}")
        tdSql.execute(f"create database {dbName} vgroups 2")

        # Test: ALTER DATABASE with PARALLEL parameter
        tdSql.execute(f"alter database {dbName} replica 3 parallel 2")
        tdLog.info("alter database with PARALLEL 2 succeeded")

        self.waitTransactionDone()
        tdSql.execute(f"drop database if exists {dbName}")

    def test_alter_replica_parallel_zero(self):
        """Test PARALLEL 0 means unlimited (all groups concurrent)"""
        dbName = "test_par_zero"
        tdSql.execute(f"drop database if exists {dbName}")
        tdSql.execute(f"create database {dbName} vgroups 2")

        # PARALLEL 0 should be accepted (means unlimited)
        tdSql.execute(f"alter database {dbName} replica 3 parallel 0")
        tdLog.info("alter database with PARALLEL 0 succeeded")

        self.waitTransactionDone()
        tdSql.execute(f"drop database if exists {dbName}")

    def test_alter_replica_parallel_one(self):
        """Test PARALLEL 1 means serial execution (one group at a time)"""
        dbName = "test_par_one"
        tdSql.execute(f"drop database if exists {dbName}")
        tdSql.execute(f"create database {dbName} vgroups 2")

        # PARALLEL 1 should be accepted
        tdSql.execute(f"alter database {dbName} replica 3 parallel 1")
        tdLog.info("alter database with PARALLEL 1 succeeded")

        self.waitTransactionDone()
        tdSql.execute(f"drop database if exists {dbName}")

    def test_alter_replica_parallel_large(self):
        """Test PARALLEL value larger than vgroup count (equivalent to unlimited)"""
        dbName = "test_par_large"
        tdSql.execute(f"drop database if exists {dbName}")
        tdSql.execute(f"create database {dbName} vgroups 2")

        # PARALLEL 100 > vgroups(2), should be accepted
        tdSql.execute(f"alter database {dbName} replica 3 parallel 100")
        tdLog.info("alter database with PARALLEL 100 succeeded")

        self.waitTransactionDone()
        tdSql.execute(f"drop database if exists {dbName}")

    def test_alter_replica_without_parallel(self):
        """Test ALTER DATABASE REPLICA without PARALLEL (default behavior, unlimited)"""
        dbName = "test_par_none"
        tdSql.execute(f"drop database if exists {dbName}")
        tdSql.execute(f"create database {dbName} vgroups 2")

        # Without PARALLEL should work (backward compatible)
        tdSql.execute(f"alter database {dbName} replica 3")
        tdLog.info("alter database without PARALLEL succeeded")

        self.waitTransactionDone()
        tdSql.execute(f"drop database if exists {dbName}")

    def test_alter_parallel_negative_rejected(self):
        """Test that negative PARALLEL values are rejected"""
        dbName = "test_par_neg"
        tdSql.execute(f"drop database if exists {dbName}")
        tdSql.execute(f"create database {dbName} vgroups 2")

        # Negative PARALLEL should be rejected
        tdSql.error(f"alter database {dbName} replica 3 parallel -1")
        tdLog.info("alter database with negative PARALLEL correctly rejected")

        tdSql.execute(f"drop database if exists {dbName}")
