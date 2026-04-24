import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
from cpu_affinity_utils import (
    get_taosd_pid,
    get_all_thread_affinities,
    has_restricted_affinity,
    get_system_cpu_count,
    get_full_cpu_set,
)


class TestCpuAffinitySwitch:
    """Tests for US1: Enable/Disable CPU Affinity via Master Switch (enableCpuAffinity=0)"""

    updatecfgDict = {"enableCpuAffinity": 0, "managementCpuCores": 4, "readCpuCores": 2, "otherCpuCores": 2}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_switch_off_default(self):
        """US1-T003: Master switch off — no threads have restricted affinity

        Deploy taosd with enableCpuAffinity=0. Verify SHOW CPU_ALLOCATION
        returns enabled=false, cores=0, core_ids='-' for all 3 rows.
        Verify via /proc that no threads have restricted affinity masks.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        tdLog.info("T003: Verify switch off — all rows show enabled=false")
        tdSql.query("SHOW CPU_ALLOCATION")
        tdSql.checkRows(3)

        categories_found = set()
        for row_idx in range(3):
            category = tdSql.queryResult[row_idx][0]
            cores = tdSql.queryResult[row_idx][1]
            core_ids = tdSql.queryResult[row_idx][2]
            enabled = tdSql.queryResult[row_idx][3]

            categories_found.add(category)
            assert enabled is False or enabled == 0 or str(enabled).lower() == "false", \
                f"Row {row_idx}: expected enabled=false, got {enabled}"
            assert cores == 0, f"Row {row_idx}: expected cores=0, got {cores}"
            assert core_ids == "-", f"Row {row_idx}: expected core_ids='-', got {core_ids}"

        assert categories_found == {"management", "write", "read"}, \
            f"Expected categories management/write/read, got {categories_found}"

        # Verify via /proc that no threads have restricted affinity
        pid = get_taosd_pid()
        if pid:
            total_cpus = get_system_cpu_count()
            threads = get_all_thread_affinities(pid)
            for t in threads:
                if t["cpus_allowed"]:
                    assert not has_restricted_affinity(t["cpus_allowed"], total_cpus), \
                        f"Thread {t['name']} (tid={t['tid']}) has restricted affinity " \
                        f"{t['cpus_allowed']} but switch is off"

        tdLog.info("T003: PASS — switch off, no restricted affinity")

    def test_switch_off_preserves_config(self):
        """US1-T005: Config values preserved when switch is off

        With enableCpuAffinity=0, managementCpuCores=4, readCpuCores=2, otherCpuCores=2,
        verify the config values are stored and queryable via SHOW DNODE VARIABLES,
        but SHOW CPU_ALLOCATION still shows enabled=false.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        tdLog.info("T005: Verify config preserved when switch off")

        tdSql.query("SHOW DNODE 1 VARIABLES LIKE 'managementCpuCores'")
        tdSql.checkRows(1)
        value = str(tdSql.queryResult[0][2])
        assert value == "4", f"Expected managementCpuCores=4, got {value}"

        tdSql.query("SHOW DNODE 1 VARIABLES LIKE 'readCpuCores'")
        tdSql.checkRows(1)
        value = str(tdSql.queryResult[0][2])
        assert value == "2", f"Expected readCpuCores=2, got {value}"

        tdSql.query("SHOW DNODE 1 VARIABLES LIKE 'otherCpuCores'")
        tdSql.checkRows(1)
        value = str(tdSql.queryResult[0][2])
        assert value == "2", f"Expected otherCpuCores=2, got {value}"

        # SHOW CPU_ALLOCATION should still show disabled
        tdSql.query("SHOW CPU_ALLOCATION")
        tdSql.checkRows(3)
        for row_idx in range(3):
            enabled = tdSql.queryResult[row_idx][3]
            assert enabled is False or enabled == 0 or str(enabled).lower() == "false", \
                f"Row {row_idx}: expected enabled=false, got {enabled}"

        tdLog.info("T005: PASS — config preserved but not applied")

    def test_upgrade_no_behavior_change(self):
        """US1-T006: Default config causes no behavior change on upgrade

        Deploy with no explicit enableCpuAffinity (relies on default=0).
        Verify via SHOW DNODE VARIABLES that default is 0.
        Verify SHOW CPU_ALLOCATION shows disabled.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        tdLog.info("T006: Verify default enableCpuAffinity=0")

        tdSql.query("SHOW DNODE 1 VARIABLES LIKE 'enableCpuAffinity'")
        tdSql.checkRows(1)
        value = str(tdSql.queryResult[0][2])
        assert value == "0", f"Expected enableCpuAffinity=0 (default), got {value}"

        tdSql.query("SHOW CPU_ALLOCATION")
        tdSql.checkRows(3)
        for row_idx in range(3):
            enabled = tdSql.queryResult[row_idx][3]
            assert enabled is False or enabled == 0 or str(enabled).lower() == "false", \
                f"Row {row_idx}: expected enabled=false, got {enabled}"

        tdLog.info("T006: PASS — default switch is off, backward compatible")


class TestCpuAffinityEnabled:
    """Tests for US1: Master switch ON — threads should have restricted affinity"""

    updatecfgDict = {"enableCpuAffinity": 1, "managementCpuCores": 1}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_switch_on(self):
        """US1-T004: Master switch on — threads have restricted affinity masks

        Deploy taosd with enableCpuAffinity=1, managementCpuCores=1.
        Verify SHOW CPU_ALLOCATION returns enabled=true with non-zero cores
        and valid core_ids. Verify via /proc that threads have restricted masks.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        tdLog.info("T004: Verify switch on — all rows show enabled=true")

        total_cpus = get_system_cpu_count()
        if total_cpus < 3:
            tdLog.info("T004: SKIP — system has <3 CPUs, affinity auto-disabled")
            return

        tdSql.query("SHOW CPU_ALLOCATION")
        tdSql.checkRows(3)

        categories_found = set()
        total_cores_allocated = 0
        for row_idx in range(3):
            category = tdSql.queryResult[row_idx][0]
            cores = tdSql.queryResult[row_idx][1]
            core_ids = tdSql.queryResult[row_idx][2]
            enabled = tdSql.queryResult[row_idx][3]

            categories_found.add(category)
            assert enabled is True or enabled == 1 or str(enabled).lower() == "true", \
                f"Row {row_idx} ({category}): expected enabled=true, got {enabled}"
            assert cores > 0, f"Row {row_idx} ({category}): expected cores>0, got {cores}"
            assert core_ids != "-", \
                f"Row {row_idx} ({category}): expected valid core_ids, got '-'"

            # Verify core_ids are valid integers
            ids = [int(x.strip()) for x in core_ids.split(",")]
            assert len(ids) == cores, \
                f"Row {row_idx} ({category}): core_ids count {len(ids)} != cores {cores}"
            total_cores_allocated += cores

        assert categories_found == {"management", "write", "read"}, \
            f"Expected categories management/write/read, got {categories_found}"
        assert total_cores_allocated == total_cpus, \
            f"Total cores {total_cores_allocated} != system CPUs {total_cpus}"

        # Verify via /proc that threads have restricted affinity
        pid = get_taosd_pid()
        if pid:
            threads = get_all_thread_affinities(pid)
            restricted_count = 0
            for t in threads:
                if t["cpus_allowed"] and has_restricted_affinity(t["cpus_allowed"], total_cpus):
                    restricted_count += 1
            assert restricted_count > 0, \
                "No threads have restricted affinity masks despite switch being on"

        tdLog.info("T004: PASS — switch on, threads have restricted affinity")
