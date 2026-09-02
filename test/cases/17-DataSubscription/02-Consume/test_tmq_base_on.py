import sys
import time
import os

from new_test_framework.utils import tdLog, tdSql, tdCom, tdDnodes


class TestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def test_tmq_base_on(self):
        """TMQ subscription: VST inheritance (BASE ON) full-chain end-to-end

        Full-chain test of the BASE ON inheritance subscription path, across every
        delivery mode a production consumer hits, on a MULTI-VGROUP source:
          Scenario A — realtime DB topic (snapshot=false): incremental WAL meta path.
          Scenario B — snapshot DB topic (snapshot=true): bootstrap from the persisted
                       SMetaEntry (metaSnapshot.c), a different code path than A.
          Scenario C — snapshot STABLE topic: stable-scoped meta (parents not delivered),
                       replayed against pre-existing parents in the target.

        Each scenario:
        1. Creates parent VSTs, a child stable inheriting via CREATE ... BASE ON,
           and a standalone stable altered with ADD/DROP BASE ON.
        2. Subscribes and consumes the json meta, asserting the frozen contract:
             - create-super: baseOn[], ownColStart, ownTagStart (ABSENT for non-inherited)
             - alter-super: alterType 22 (ADD) / 23 (DROP) with baseOn[]
        3. Replays every raw meta via tmq_write_raw into a fresh target db (idempotent
           under the per-vnode duplicate meta a multi-vgroup db-topic delivers) and
           verifies inheritance was reconstructed (ins_vstable_inherits + SHOW CREATE
           STABLE ... BASE ON), proving cross-cluster sync.

        The C binary `tmq_base_on_test` performs all scenarios and exits non-zero on
        any failure (it uses exit(1), not assert(), since it is built -DNDEBUG).

        Since: v3.4.1.0

        Labels: tmq,vtable,inheritance

        Jira: None

        History:
            - 2026-06-20 Created for VST inheritance (BASE ON) subscription coverage
            - 2026-06-21 Extended to snapshot + stable-topic + multi-vgroup scenarios
        """
        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_base_on_test' % (buildPath)
        tdLog.info(cmdStr)
        ret = os.system(cmdStr)
        if ret != 0:
            tdLog.exit("tmq_base_on_test != 0")
        tdLog.success(f"{__file__} successfully executed")
