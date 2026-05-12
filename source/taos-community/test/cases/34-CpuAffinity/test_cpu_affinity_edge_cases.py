import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
from cpu_affinity_utils import (
    get_system_cpu_count,
    parse_core_ids_string,
)


class TestCpuAffinityEdgeCases:
    """Edge case tests for CPU affinity feature"""

    updatecfgDict = {"enableCpuAffinity": 1, "managementCpuCores": 1}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_sequential_core_assignment(self):
        """T019: Core IDs are assigned sequentially from core 0

        Verify management=[0..M-1], write=[M..M+W-1], read=[M+W..total-1].

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        total_cpus = get_system_cpu_count()
        if total_cpus < 3:
            tdLog.info("T019: SKIP — system has <3 CPUs")
            return

        tdLog.info("T019: Verify sequential core assignment")
        tdSql.query("SHOW CPU_ALLOCATION")
        tdSql.checkRows(3)

        alloc = {}
        all_ids = {}
        for row_idx in range(3):
            dnode_id = tdSql.queryResult[row_idx][0]
            category = tdSql.queryResult[row_idx][1]
            cores = tdSql.queryResult[row_idx][2]
            core_ids_str = tdSql.queryResult[row_idx][3]
            alloc[category] = cores
            all_ids[category] = parse_core_ids_string(core_ids_str)

        m = alloc["management"]
        w = alloc["write"]
        r = alloc["read"]

        # Verify sequential assignment
        expected_mgmt = set(range(0, m))
        expected_write = set(range(m, m + w))
        expected_read = set(range(m + w, m + w + r))

        assert all_ids["management"] == expected_mgmt, \
            f"management: expected {expected_mgmt}, got {all_ids['management']}"
        assert all_ids["write"] == expected_write, \
            f"write: expected {expected_write}, got {all_ids['write']}"
        assert all_ids["read"] == expected_read, \
            f"read: expected {expected_read}, got {all_ids['read']}"

        # Verify coverage
        assert m + w + r == total_cpus, \
            f"Total {m}+{w}+{r}={m+w+r} != system CPUs {total_cpus}"

        tdLog.info("T019: PASS — sequential core assignment verified")

    def test_cores_sum_equals_total(self):
        """T020: Sum of all category cores equals system CPU count

        1. SHOW CPU_ALLOCATION
        2. verify sum of cores equals system CPU count

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        total_cpus = get_system_cpu_count()
        if total_cpus < 3:
            tdLog.info("T020: SKIP — system has <3 CPUs")
            return

        tdLog.info("T020: Verify cores sum equals total CPUs")
        tdSql.query("SHOW CPU_ALLOCATION")
        tdSql.checkRows(3)

        total_cores = 0
        for row_idx in range(3):
            dnode_id = tdSql.queryResult[row_idx][0]
            total_cores += tdSql.queryResult[row_idx][2]

        assert total_cores == total_cpus, \
            f"Sum of cores {total_cores} != system CPUs {total_cpus}"

        tdLog.info("T020: PASS — cores sum matches system total")

    def test_basic_data_operations_with_affinity(self):
        """T021: Normal database operations work with affinity enabled

        With affinity on, create database, create table, insert data, query data.
        Verify all operations succeed.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        tdLog.info("T021: Verify basic data operations with affinity enabled")

        db = "test_cpu_affinity_ops"

        # Create database
        tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
        tdSql.execute(f"CREATE DATABASE {db}")
        tdSql.execute(f"USE {db}")

        # Create table
        tdSql.execute(
            f"CREATE STABLE {db}.stb1 (ts TIMESTAMP, v1 INT, v2 FLOAT) TAGS (t1 INT)"
        )
        tdSql.execute(f"CREATE TABLE {db}.ct1 USING {db}.stb1 TAGS(1)")
        tdSql.execute(f"CREATE TABLE {db}.ct2 USING {db}.stb1 TAGS(2)")

        # Insert data
        now = int(time.time() * 1000)
        for i in range(100):
            tdSql.execute(
                f"INSERT INTO {db}.ct1 VALUES ({now + i}, {i}, {i * 1.1})"
            )
            tdSql.execute(
                f"INSERT INTO {db}.ct2 VALUES ({now + i}, {i * 2}, {i * 2.2})"
            )

        # Query data
        tdSql.query(f"SELECT COUNT(*) FROM {db}.stb1")
        tdSql.checkRows(1)
        count = tdSql.queryResult[0][0]
        assert count == 200, f"Expected 200 rows, got {count}"

        # Aggregate query
        tdSql.query(f"SELECT SUM(v1) FROM {db}.ct1")
        tdSql.checkRows(1)

        # Join-like query (group by tag)
        tdSql.query(f"SELECT t1, COUNT(*) FROM {db}.stb1 GROUP BY t1")
        tdSql.checkRows(2)

        # Cleanup
        tdSql.execute(f"DROP DATABASE IF EXISTS {db}")

        tdLog.info("T021: PASS — all data operations succeed with affinity enabled")

    def test_restart_with_affinity(self):
        """T022: Restart preserves affinity allocation and data

        With affinity enabled, write data, restart taosd, verify the same
        CPU allocation is restored and data is still queryable.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        total_cpus = get_system_cpu_count()
        if total_cpus < 3:
            tdLog.info("T022: SKIP — system has <3 CPUs")
            return

        tdLog.info("T022: Verify restart preserves affinity and data")

        db = "test_cpu_restart"

        # Get allocation before restart
        tdSql.query("SHOW CPU_ALLOCATION")
        before_alloc = {}
        for row_idx in range(3):
            dnode_id = tdSql.queryResult[row_idx][0]
            cat = tdSql.queryResult[row_idx][1]
            before_alloc[cat] = {
                "cores": tdSql.queryResult[row_idx][2],
                "core_ids": tdSql.queryResult[row_idx][3],
                "enabled": tdSql.queryResult[row_idx][4],
            }

        # Write some data
        tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
        tdSql.execute(f"CREATE DATABASE {db}")
        tdSql.execute(f"USE {db}")
        tdSql.execute(f"CREATE TABLE {db}.t1 (ts TIMESTAMP, v1 INT)")
        now = int(time.time() * 1000)
        for i in range(50):
            tdSql.execute(f"INSERT INTO {db}.t1 VALUES ({now + i}, {i})")

        # Restart taosd
        tdLog.info("T022: Restarting taosd...")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        # Verify allocation is the same after restart
        tdSql.query("SHOW CPU_ALLOCATION")
        tdSql.checkRows(3)
        for row_idx in range(3):
            dnode_id = tdSql.queryResult[row_idx][0]
            cat = tdSql.queryResult[row_idx][1]
            assert cat in before_alloc, f"Unexpected category {cat} after restart"
            assert tdSql.queryResult[row_idx][2] == before_alloc[cat]["cores"], \
                f"{cat}: cores changed after restart"
            assert tdSql.queryResult[row_idx][3] == before_alloc[cat]["core_ids"], \
                f"{cat}: core_ids changed after restart"
            assert tdSql.queryResult[row_idx][4] == before_alloc[cat]["enabled"], \
                f"{cat}: enabled changed after restart"

        # Verify data survived restart
        tdSql.query(f"SELECT COUNT(*) FROM {db}.t1")
        tdSql.checkRows(1)
        count = tdSql.queryResult[0][0]
        assert count == 50, f"Expected 50 rows after restart, got {count}"

        # Cleanup
        tdSql.execute(f"DROP DATABASE IF EXISTS {db}")

        tdLog.info("T022: PASS — restart preserves allocation and data")
