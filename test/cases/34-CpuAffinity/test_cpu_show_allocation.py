import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
from cpu_affinity_utils import (
    get_system_cpu_count,
    parse_core_ids_string,
)


class TestShowCpuAllocationEnabled:
    """Tests for US4: SHOW CPU_ALLOCATION and information_schema with affinity enabled"""

    updatecfgDict = {"enableCpuAffinity": 1, "managementCpuCores": 1}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_show_cpu_allocation_schema(self):
        """US4-T014: SHOW CPU_ALLOCATION returns exactly 3 rows with correct schema

        Verify result has exactly 3 rows with columns: thread_category, cores,
        core_ids, enabled. Verify thread_category values are management, write, read.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        total_cpus = get_system_cpu_count()
        if total_cpus < 3:
            tdLog.info("T014: SKIP — system has <3 CPUs")
            return

        tdLog.info("T014: Verify SHOW CPU_ALLOCATION schema")
        tdSql.query("SHOW CPU_ALLOCATION")
        tdSql.checkRows(3)

        categories = set()
        for row_idx in range(3):
            dnode_id = tdSql.queryResult[row_idx][0]
            category = tdSql.queryResult[row_idx][1]
            cores = tdSql.queryResult[row_idx][2]
            core_ids = tdSql.queryResult[row_idx][3]
            enabled = tdSql.queryResult[row_idx][4]

            categories.add(category)

            # Verify dnode_id column
            assert isinstance(dnode_id, int), f"dnode_id should be int, got {type(dnode_id)}"
            assert dnode_id == 1, f"Expected dnode_id=1 in single-node, got {dnode_id}"
            # Verify column types
            assert isinstance(category, str), f"thread_category should be str, got {type(category)}"
            assert isinstance(cores, int), f"cores should be int, got {type(cores)}"
            assert isinstance(core_ids, str), f"core_ids should be str, got {type(core_ids)}"

        assert categories == {"management", "write", "read"}, \
            f"Expected categories {{management, write, read}}, got {categories}"

        tdLog.info("T014: PASS — schema validated")

    def test_information_schema_table(self):
        """US4-T015: information_schema.ins_cpu_allocation returns same data

        Execute SELECT * FROM information_schema.ins_cpu_allocation and verify
        same 3 rows with same schema as SHOW CPU_ALLOCATION.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        total_cpus = get_system_cpu_count()
        if total_cpus < 3:
            tdLog.info("T015: SKIP — system has <3 CPUs")
            return

        tdLog.info("T015: Verify information_schema.ins_cpu_allocation")

        # Get SHOW CPU_ALLOCATION results
        tdSql.query("SHOW CPU_ALLOCATION")
        show_results = {}
        for row_idx in range(3):
            dnode_id = tdSql.queryResult[row_idx][0]
            cat = tdSql.queryResult[row_idx][1]
            show_results[cat] = {
                "dnode_id": tdSql.queryResult[row_idx][0],
                "cores": tdSql.queryResult[row_idx][2],
                "core_ids": tdSql.queryResult[row_idx][3],
                "enabled": tdSql.queryResult[row_idx][4],
            }

        # Get information_schema results
        tdSql.query("SELECT * FROM information_schema.ins_cpu_allocation")
        tdSql.checkRows(3)

        schema_results = {}
        for row_idx in range(3):
            dnode_id = tdSql.queryResult[row_idx][0]
            cat = tdSql.queryResult[row_idx][1]
            schema_results[cat] = {
                "dnode_id": tdSql.queryResult[row_idx][0],
                "cores": tdSql.queryResult[row_idx][2],
                "core_ids": tdSql.queryResult[row_idx][3],
                "enabled": tdSql.queryResult[row_idx][4],
            }

        # Verify identical results
        assert set(show_results.keys()) == set(schema_results.keys()), \
            f"Category mismatch: SHOW={set(show_results.keys())}, schema={set(schema_results.keys())}"

        for cat in show_results:
            for field in ("dnode_id", "cores", "core_ids", "enabled"):
                assert show_results[cat][field] == schema_results[cat][field], \
                    f"{cat}.{field}: SHOW={show_results[cat][field]} != schema={schema_results[cat][field]}"

        tdLog.info("T015: PASS — information_schema matches SHOW")

    def test_show_cpu_allocation_core_ids_valid(self):
        """US4-T016: core_ids are valid integers in [0, cpu_count)

        For each row where enabled=true, parse core_ids as comma-separated ints.
        Verify each is in [0, cpu_count). Verify union of all ids equals full core set.
        Verify no overlap across categories.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        total_cpus = get_system_cpu_count()
        if total_cpus < 3:
            tdLog.info("T016: SKIP — system has <3 CPUs")
            return

        tdLog.info("T016: Verify core_ids validity and completeness")
        tdSql.query("SHOW CPU_ALLOCATION")
        tdSql.checkRows(3)

        all_ids = set()
        category_ids = {}
        for row_idx in range(3):
            dnode_id = tdSql.queryResult[row_idx][0]
            category = tdSql.queryResult[row_idx][1]
            core_ids_str = tdSql.queryResult[row_idx][3]
            enabled = tdSql.queryResult[row_idx][4]

            if enabled is True or enabled == 1 or str(enabled).lower() == "true":
                ids = parse_core_ids_string(core_ids_str)
                category_ids[category] = ids

                # Each core ID should be in valid range
                for cid in ids:
                    assert 0 <= cid < total_cpus, \
                        f"{category}: core_id {cid} out of range [0, {total_cpus})"

                # Check no overlap with previously seen categories
                overlap = all_ids & ids
                assert len(overlap) == 0, \
                    f"{category}: overlapping core_ids {overlap} with another category"
                all_ids |= ids

        # Union should cover all cores
        expected_ids = set(range(total_cpus))
        assert all_ids == expected_ids, \
            f"Core ID union {all_ids} != full set {expected_ids}"

        tdLog.info("T016: PASS — core_ids valid, complete, disjoint")

    def test_dnode_variables_show_switch_enabled(self):
        """US4-T018a: SHOW DNODE VARIABLES shows enableCpuAffinity=1 when enabled

        1. SHOW DNODE 1 VARIABLES LIKE 'enableCpuAffinity'
        2. Verify enableCpuAffinity=1 in DNODE VARIABLES

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        tdLog.info("T018a: Verify enableCpuAffinity=1 in DNODE VARIABLES")

        tdSql.query("SHOW DNODE 1 VARIABLES LIKE 'enableCpuAffinity'")
        tdSql.checkRows(1)
        value = str(tdSql.queryResult[0][2])
        assert value == "1", f"Expected enableCpuAffinity=1, got {value}"

        tdLog.info("T018a: PASS — enableCpuAffinity=1 visible")


class TestShowCpuAllocationDisabled:
    """Tests for US4: SHOW CPU_ALLOCATION with affinity disabled"""

    updatecfgDict = {"enableCpuAffinity": 0}

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_show_cpu_allocation_disabled(self):
        """US4-T017: SHOW CPU_ALLOCATION shows disabled state

        With enableCpuAffinity=0, verify all 3 rows show enabled=false,
        cores=0, core_ids='-'.

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        tdLog.info("T017: Verify SHOW CPU_ALLOCATION when disabled")
        tdSql.query("SHOW CPU_ALLOCATION")
        tdSql.checkRows(3)

        for row_idx in range(3):
            dnode_id = tdSql.queryResult[row_idx][0]
            category = tdSql.queryResult[row_idx][1]
            cores = tdSql.queryResult[row_idx][2]
            core_ids = tdSql.queryResult[row_idx][3]
            enabled = tdSql.queryResult[row_idx][4]

            assert enabled is False or enabled == 0 or str(enabled).lower() == "false", \
                f"{category}: expected enabled=false, got {enabled}"
            assert cores == 0, f"{category}: expected cores=0, got {cores}"
            assert core_ids == "-", f"{category}: expected core_ids='-', got {core_ids}"

        tdLog.info("T017: PASS — disabled state shows correctly")

    def test_dnode_variables_show_switch_disabled(self):
        """US4-T018b: SHOW DNODE VARIABLES shows enableCpuAffinity=0 when disabled

        1. SHOW DNODE 1 VARIABLES LIKE 'enableCpuAffinity'
        2. Verify enableCpuAffinity=0 in DNODE VARIABLES

        Since: v3.3.0.0

        Labels: cpu-affinity, ci

        Jira: None

        History:
            - 2026-04-16 Initial creation
        """
        tdLog.info("T018b: Verify enableCpuAffinity=0 in DNODE VARIABLES")

        tdSql.query("SHOW DNODE 1 VARIABLES LIKE 'enableCpuAffinity'")
        tdSql.checkRows(1)
        value = str(tdSql.queryResult[0][2])
        assert value == "0", f"Expected enableCpuAffinity=0, got {value}"

        tdLog.info("T018b: PASS — enableCpuAffinity=0 visible")
