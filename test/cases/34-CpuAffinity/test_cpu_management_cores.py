import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
from cpu_affinity_utils import (
    get_system_cpu_count,
    parse_core_ids_string,
)


class TestManagementCoresDefault:
    """Tests for US2: Management cores with default value (managementCpuCores=1)"""

    updatecfgDict = {"enableCpuAffinity": 1, "managementCpuCores": 1, "readCpuRatio": 50}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_default_management_cores(self):
        """US2-T007: Default management cores = 1

        Deploy taosd with enableCpuAffinity=1, managementCpuCores=1.
        Verify SHOW CPU_ALLOCATION: management row shows cores=1, core_ids='0'.
        Verify write + read rows consume the remaining cores.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        total_cpus = get_system_cpu_count()
        if total_cpus < 3:
            tdLog.info("T007: SKIP — system has <3 CPUs, affinity auto-disabled")
            return

        tdLog.info("T007: Verify default managementCpuCores=1")
        tdSql.query("SHOW CPU_ALLOCATION")
        tdSql.checkRows(3)

        alloc = {}
        for row_idx in range(3):
            category = tdSql.queryResult[row_idx][0]
            cores = tdSql.queryResult[row_idx][1]
            core_ids = tdSql.queryResult[row_idx][2]
            alloc[category] = {"cores": cores, "core_ids": core_ids}

        # Management should have 1 core, core_ids="0"
        assert alloc["management"]["cores"] == 1, \
            f"Expected management cores=1, got {alloc['management']['cores']}"
        assert alloc["management"]["core_ids"] == "0", \
            f"Expected management core_ids='0', got {alloc['management']['core_ids']}"

        # Write + read should consume remaining cores
        remaining = total_cpus - 1
        actual_remaining = alloc["write"]["cores"] + alloc["read"]["cores"]
        assert actual_remaining == remaining, \
            f"Expected write+read cores={remaining}, got {actual_remaining}"

        tdLog.info("T007: PASS — management cores=1, remaining split correctly")

    def test_management_cores_via_dnode_variables(self):
        """US2-T009: Verify managementCpuCores visible in DNODE VARIABLES

        Query SHOW DNODE 1 VARIABLES LIKE 'managementCpuCores' and verify
        the returned value matches configured value.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        tdLog.info("T009: Verify managementCpuCores in DNODE VARIABLES")

        tdSql.query("SHOW DNODE 1 VARIABLES LIKE 'managementCpuCores'")
        tdSql.checkRows(1)
        value = str(tdSql.queryResult[0][2])
        assert value == "1", f"Expected managementCpuCores=1, got {value}"

        tdLog.info("T009: PASS — managementCpuCores visible in DNODE VARIABLES")


class TestManagementCoresCustom:
    """Tests for US2: Custom management cores (managementCpuCores=2)"""

    updatecfgDict = {"enableCpuAffinity": 1, "managementCpuCores": 2, "readCpuRatio": 50}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_custom_management_cores(self):
        """US2-T008: Custom management cores = 2

        Deploy taosd with enableCpuAffinity=1, managementCpuCores=2.
        Verify SHOW CPU_ALLOCATION: management row shows cores=2, core_ids='0,1'.
        Verify remaining cores = total - 2 split between write and read.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        total_cpus = get_system_cpu_count()
        if total_cpus < 4:
            tdLog.info("T008: SKIP — system has <4 CPUs, cannot allocate 2 mgmt + min write + min read")
            return

        tdLog.info("T008: Verify managementCpuCores=2")
        tdSql.query("SHOW CPU_ALLOCATION")
        tdSql.checkRows(3)

        alloc = {}
        for row_idx in range(3):
            category = tdSql.queryResult[row_idx][0]
            cores = tdSql.queryResult[row_idx][1]
            core_ids = tdSql.queryResult[row_idx][2]
            alloc[category] = {"cores": cores, "core_ids": core_ids}

        # Management should have 2 cores, core_ids="0,1"
        assert alloc["management"]["cores"] == 2, \
            f"Expected management cores=2, got {alloc['management']['cores']}"
        assert alloc["management"]["core_ids"] == "0,1", \
            f"Expected management core_ids='0,1', got {alloc['management']['core_ids']}"

        # Remaining = total - 2, split between write and read
        remaining = total_cpus - 2
        actual_remaining = alloc["write"]["cores"] + alloc["read"]["cores"]
        assert actual_remaining == remaining, \
            f"Expected write+read cores={remaining}, got {actual_remaining}"

        tdLog.info("T008: PASS — custom management cores=2")
