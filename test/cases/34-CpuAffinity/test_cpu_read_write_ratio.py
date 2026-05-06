import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
from cpu_affinity_utils import (
    get_system_cpu_count,
    parse_core_ids_string,
)


class TestReadWriteEqualSplit:
    """Tests for US3: Default equal split — readCpuCores and otherCpuCores each get half"""

    updatecfgDict = {"enableCpuAffinity": 1, "managementCpuCores": 1}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_equal_split(self):
        """US3-T010: Default equal split — read and write get approximately equal cores

        Deploy with managementCpuCores=1, rely on dynamic defaults for
        readCpuCores and otherCpuCores. Verify read and write rows have
        approximately equal cores. Verify total = system CPU count.
        Verify core_ids are disjoint across all 3 categories.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
            - 2026-04-22 Rewritten for readCpuCores/otherCpuCores
        """
        total_cpus = get_system_cpu_count()
        if total_cpus < 3:
            tdLog.info("T010: SKIP — system has <3 CPUs")
            return

        tdLog.info("T010: Verify default equal split")
        tdSql.query("SHOW CPU_ALLOCATION")
        tdSql.checkRows(3)

        alloc = {}
        all_core_ids = {}
        for row_idx in range(3):
            dnode_id = tdSql.queryResult[row_idx][0]
            category = tdSql.queryResult[row_idx][1]
            cores = tdSql.queryResult[row_idx][2]
            core_ids_str = tdSql.queryResult[row_idx][3]
            alloc[category] = cores
            all_core_ids[category] = parse_core_ids_string(core_ids_str)

        # Total cores should equal system CPU count
        total_allocated = sum(alloc.values())
        assert total_allocated == total_cpus, \
            f"Total allocated {total_allocated} != system CPUs {total_cpus}"

        # With default 50/50 split, read and write should be approximately equal
        remaining = total_cpus - alloc["management"]
        expected_read = remaining // 2
        expected_write = remaining - expected_read
        assert alloc["read"] == expected_read, \
            f"Expected read={expected_read}, got {alloc['read']}"
        assert alloc["write"] == expected_write, \
            f"Expected write={expected_write}, got {alloc['write']}"

        # Verify core_ids are disjoint
        mgmt_ids = all_core_ids["management"]
        write_ids = all_core_ids["write"]
        read_ids = all_core_ids["read"]

        assert mgmt_ids.isdisjoint(write_ids), \
            f"management and write overlap: {mgmt_ids & write_ids}"
        assert mgmt_ids.isdisjoint(read_ids), \
            f"management and read overlap: {mgmt_ids & read_ids}"
        assert write_ids.isdisjoint(read_ids), \
            f"write and read overlap: {write_ids & read_ids}"

        # Union should cover all cores
        all_ids = mgmt_ids | write_ids | read_ids
        assert len(all_ids) == total_cpus, \
            f"Union of core_ids has {len(all_ids)} cores, expected {total_cpus}"

        tdLog.info("T010: PASS — default equal split correct, disjoint core_ids")


class TestMinReadCores:
    """Tests for US3: readCpuCores=1 — minimum read core allocation"""

    updatecfgDict = {"enableCpuAffinity": 1, "managementCpuCores": 1, "readCpuCores": 1}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_min_read_cores(self):
        """US3-T011: Minimum read cores — read gets 1, write gets the rest

        Deploy with readCpuCores=1, rely on dynamic default for otherCpuCores.
        Verify read row shows cores=1. Verify write gets remaining - 1.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
            - 2026-04-22 Rewritten for readCpuCores/otherCpuCores
        """
        total_cpus = get_system_cpu_count()
        if total_cpus < 3:
            tdLog.info("T011: SKIP — system has <3 CPUs")
            return

        tdLog.info("T011: Verify readCpuCores=1 minimum allocation")
        tdSql.query("SHOW CPU_ALLOCATION")
        tdSql.checkRows(3)

        alloc = {}
        for row_idx in range(3):
            dnode_id = tdSql.queryResult[row_idx][0]
            category = tdSql.queryResult[row_idx][1]
            cores = tdSql.queryResult[row_idx][2]
            alloc[category] = cores

        # Read must have exactly 1 core
        assert alloc["read"] == 1, \
            f"Expected read cores=1, got {alloc['read']}"

        # Write should get the remaining cores
        remaining = total_cpus - alloc["management"]
        assert alloc["write"] == remaining - 1, \
            f"Expected write={remaining - 1}, got {alloc['write']}"

        tdLog.info("T011: PASS — readCpuCores=1 allocation correct")


class TestMinWriteCores:
    """Tests for US3: otherCpuCores=1 — minimum write core allocation"""

    updatecfgDict = {"enableCpuAffinity": 1, "managementCpuCores": 1, "otherCpuCores": 1}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_min_write_cores(self):
        """US3-T012: Minimum write cores — write gets 1, read gets the rest

        Deploy with otherCpuCores=1, rely on dynamic default for readCpuCores.
        Verify write row shows cores=1. Verify read gets remaining - 1.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
            - 2026-04-22 Rewritten for readCpuCores/otherCpuCores
        """
        total_cpus = get_system_cpu_count()
        if total_cpus < 3:
            tdLog.info("T012: SKIP — system has <3 CPUs")
            return

        tdLog.info("T012: Verify otherCpuCores=1 minimum allocation")
        tdSql.query("SHOW CPU_ALLOCATION")
        tdSql.checkRows(3)

        alloc = {}
        for row_idx in range(3):
            dnode_id = tdSql.queryResult[row_idx][0]
            category = tdSql.queryResult[row_idx][1]
            cores = tdSql.queryResult[row_idx][2]
            alloc[category] = cores

        # Write must have exactly 1 core
        assert alloc["write"] == 1, \
            f"Expected write cores=1, got {alloc['write']}"

        # Read should get the remaining cores
        remaining = total_cpus - alloc["management"]
        assert alloc["read"] == remaining - 1, \
            f"Expected read={remaining - 1}, got {alloc['read']}"

        tdLog.info("T012: PASS — otherCpuCores=1 allocation correct")


class TestCustomCoreAllocation:
    """Tests for US3: Custom readCpuCores=1, otherCpuCores=2 — explicit allocation"""

    updatecfgDict = {"enableCpuAffinity": 1, "managementCpuCores": 1, "readCpuCores": 1, "otherCpuCores": 2}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_custom_allocation(self):
        """US3-T013: Custom allocation — verify exact core counts

        Deploy with readCpuCores=1, otherCpuCores=2, managementCpuCores=1.
        Verify exact core counts. Verify core_ids are sequential and disjoint.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
            - 2026-04-22 Rewritten for readCpuCores/otherCpuCores
        """
        total_cpus = get_system_cpu_count()
        if total_cpus < 4:
            tdLog.info("T013: SKIP — system has <4 CPUs (need 1+2+1=4)")
            return

        tdLog.info("T013: Verify custom readCpuCores=1, otherCpuCores=2")
        tdSql.query("SHOW CPU_ALLOCATION")
        tdSql.checkRows(3)

        alloc = {}
        all_core_ids = {}
        for row_idx in range(3):
            dnode_id = tdSql.queryResult[row_idx][0]
            category = tdSql.queryResult[row_idx][1]
            cores = tdSql.queryResult[row_idx][2]
            core_ids_str = tdSql.queryResult[row_idx][3]
            alloc[category] = cores
            all_core_ids[category] = parse_core_ids_string(core_ids_str)

        # Verify exact counts
        assert alloc["management"] == 1, \
            f"Expected management=1, got {alloc['management']}"
        assert alloc["write"] == 2, \
            f"Expected write=2, got {alloc['write']}"
        assert alloc["read"] == 1, \
            f"Expected read=1, got {alloc['read']}"

        # Verify sequential assignment: mgmt=[0], write=[1,2], read=[3]
        assert all_core_ids["management"] == {0}, \
            f"management: expected {{0}}, got {all_core_ids['management']}"
        assert all_core_ids["write"] == {1, 2}, \
            f"write: expected {{1, 2}}, got {all_core_ids['write']}"
        assert all_core_ids["read"] == {3}, \
            f"read: expected {{3}}, got {all_core_ids['read']}"

        # Verify disjoint
        mgmt_ids = all_core_ids["management"]
        write_ids = all_core_ids["write"]
        read_ids = all_core_ids["read"]

        assert mgmt_ids.isdisjoint(write_ids), \
            f"management and write overlap: {mgmt_ids & write_ids}"
        assert mgmt_ids.isdisjoint(read_ids), \
            f"management and read overlap: {mgmt_ids & read_ids}"
        assert write_ids.isdisjoint(read_ids), \
            f"write and read overlap: {write_ids & read_ids}"

        tdLog.info("T013: PASS — custom allocation verified")
