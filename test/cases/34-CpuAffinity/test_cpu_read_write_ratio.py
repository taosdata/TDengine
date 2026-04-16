import math
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
from cpu_affinity_utils import (
    get_system_cpu_count,
    parse_core_ids_string,
)


class TestReadWriteRatio50:
    """Tests for US3: readCpuRatio=50 — equal split between read and write"""

    updatecfgDict = {"enableCpuAffinity": 1, "managementCpuCores": 1, "readCpuRatio": 50}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_ratio_50_50(self):
        """US3-T010: 50/50 ratio — read and write get approximately equal cores

        Deploy with readCpuRatio=50. Verify read and write rows have
        approximately equal cores. Verify total = system CPU count.
        Verify core_ids are disjoint across all 3 categories.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        total_cpus = get_system_cpu_count()
        if total_cpus < 3:
            tdLog.info("T010: SKIP — system has <3 CPUs")
            return

        tdLog.info("T010: Verify readCpuRatio=50 splits equally")
        tdSql.query("SHOW CPU_ALLOCATION")
        tdSql.checkRows(3)

        alloc = {}
        all_core_ids = {}
        for row_idx in range(3):
            category = tdSql.queryResult[row_idx][0]
            cores = tdSql.queryResult[row_idx][1]
            core_ids_str = tdSql.queryResult[row_idx][2]
            alloc[category] = cores
            all_core_ids[category] = parse_core_ids_string(core_ids_str)

        # Total cores should equal system CPU count
        total_allocated = sum(alloc.values())
        assert total_allocated == total_cpus, \
            f"Total allocated {total_allocated} != system CPUs {total_cpus}"

        # With 50% ratio, read and write should be approximately equal
        remaining = total_cpus - alloc["management"]
        expected_read = math.floor(remaining * 50 / 100)
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

        tdLog.info("T010: PASS — 50/50 ratio splits correctly, disjoint core_ids")


class TestReadWriteRatio0:
    """Tests for US3: readCpuRatio=0 — minimum read core guarantee"""

    updatecfgDict = {"enableCpuAffinity": 1, "managementCpuCores": 1, "readCpuRatio": 0}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_ratio_0_min_guarantee(self):
        """US3-T011: Ratio 0% — read still gets minimum 1 core

        Deploy with readCpuRatio=0. Verify read row shows cores >= 1
        (minimum guarantee). Verify write gets the bulk.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        total_cpus = get_system_cpu_count()
        if total_cpus < 3:
            tdLog.info("T011: SKIP — system has <3 CPUs")
            return

        tdLog.info("T011: Verify readCpuRatio=0 with minimum guarantee")
        tdSql.query("SHOW CPU_ALLOCATION")
        tdSql.checkRows(3)

        alloc = {}
        for row_idx in range(3):
            category = tdSql.queryResult[row_idx][0]
            cores = tdSql.queryResult[row_idx][1]
            alloc[category] = cores

        # Read must have at least 1 core (min guarantee)
        assert alloc["read"] >= 1, \
            f"Expected read cores >= 1, got {alloc['read']}"

        # Write should get the bulk of remaining cores
        remaining = total_cpus - alloc["management"]
        assert alloc["write"] == remaining - alloc["read"], \
            f"Expected write={remaining - alloc['read']}, got {alloc['write']}"

        # Write should get most of the remaining (it should be remaining - 1)
        assert alloc["write"] >= alloc["read"], \
            f"Expected write >= read, got write={alloc['write']}, read={alloc['read']}"

        tdLog.info("T011: PASS — ratio 0% with min-1-core guarantee")


class TestReadWriteRatio100:
    """Tests for US3: readCpuRatio=100 — minimum write core guarantee"""

    updatecfgDict = {"enableCpuAffinity": 1, "managementCpuCores": 1, "readCpuRatio": 100}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_ratio_100_min_guarantee(self):
        """US3-T012: Ratio 100% — write still gets minimum 1 core

        Deploy with readCpuRatio=100. Verify write row shows cores >= 1
        (minimum guarantee). Verify read gets the bulk.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        total_cpus = get_system_cpu_count()
        if total_cpus < 3:
            tdLog.info("T012: SKIP — system has <3 CPUs")
            return

        tdLog.info("T012: Verify readCpuRatio=100 with minimum guarantee")
        tdSql.query("SHOW CPU_ALLOCATION")
        tdSql.checkRows(3)

        alloc = {}
        for row_idx in range(3):
            category = tdSql.queryResult[row_idx][0]
            cores = tdSql.queryResult[row_idx][1]
            alloc[category] = cores

        # Write must have at least 1 core (min guarantee)
        assert alloc["write"] >= 1, \
            f"Expected write cores >= 1, got {alloc['write']}"

        # Read should get the bulk
        remaining = total_cpus - alloc["management"]
        assert alloc["read"] == remaining - alloc["write"], \
            f"Expected read={remaining - alloc['write']}, got {alloc['read']}"

        assert alloc["read"] >= alloc["write"], \
            f"Expected read >= write, got read={alloc['read']}, write={alloc['write']}"

        tdLog.info("T012: PASS — ratio 100% with min-1-core guarantee")


class TestReadWriteRatio30:
    """Tests for US3: readCpuRatio=30 — verify floor-based calculation"""

    updatecfgDict = {"enableCpuAffinity": 1, "managementCpuCores": 1, "readCpuRatio": 30}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_ratio_30_70(self):
        """US3-T013: Ratio 30% — read=floor(remaining*30/100), write=remaining-read

        Deploy with readCpuRatio=30, managementCpuCores=1.
        Verify exact floor-based calculation.
        Verify core_ids are sequential and disjoint.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        total_cpus = get_system_cpu_count()
        if total_cpus < 3:
            tdLog.info("T013: SKIP — system has <3 CPUs")
            return

        tdLog.info("T013: Verify readCpuRatio=30 floor-based calculation")
        tdSql.query("SHOW CPU_ALLOCATION")
        tdSql.checkRows(3)

        alloc = {}
        all_core_ids = {}
        for row_idx in range(3):
            category = tdSql.queryResult[row_idx][0]
            cores = tdSql.queryResult[row_idx][1]
            core_ids_str = tdSql.queryResult[row_idx][2]
            alloc[category] = cores
            all_core_ids[category] = parse_core_ids_string(core_ids_str)

        remaining = total_cpus - alloc["management"]
        expected_read = math.floor(remaining * 30 / 100)
        # Ensure min-1-core guarantee
        if expected_read < 1:
            expected_read = 1
        expected_write = remaining - expected_read
        if expected_write < 1:
            expected_write = 1
            expected_read = remaining - expected_write

        assert alloc["read"] == expected_read, \
            f"Expected read={expected_read}, got {alloc['read']} (remaining={remaining})"
        assert alloc["write"] == expected_write, \
            f"Expected write={expected_write}, got {alloc['write']} (remaining={remaining})"

        # Verify core_ids are sequential: mgmt=[0..M-1], write=[M..M+W-1], read=[M+W..total-1]
        mgmt_cores = alloc["management"]
        write_cores = alloc["write"]
        read_cores = alloc["read"]

        expected_mgmt_ids = set(range(0, mgmt_cores))
        expected_write_ids = set(range(mgmt_cores, mgmt_cores + write_cores))
        expected_read_ids = set(range(mgmt_cores + write_cores, total_cpus))

        assert all_core_ids["management"] == expected_mgmt_ids, \
            f"Expected mgmt ids {expected_mgmt_ids}, got {all_core_ids['management']}"
        assert all_core_ids["write"] == expected_write_ids, \
            f"Expected write ids {expected_write_ids}, got {all_core_ids['write']}"
        assert all_core_ids["read"] == expected_read_ids, \
            f"Expected read ids {expected_read_ids}, got {all_core_ids['read']}"

        tdLog.info(f"T013: PASS — ratio 30/70 with floor calculation (remaining={remaining})")
