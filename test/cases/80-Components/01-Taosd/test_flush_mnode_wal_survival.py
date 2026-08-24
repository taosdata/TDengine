###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import os
import shutil
import time

from new_test_framework.utils import tdLog, tdSql, tdDnodes


class TestFlushMnodeWalSurvival:
    """Coverage for `FLUSH MNODE` (FS "支持手动修改 sdb" §4.1).

    `FLUSH MNODE` writes the full in-memory sdb state to sdb.data and force
    -truncates the WAL. The whole point of the command is: once it returns
    success, the mnode WAL can be discarded (e.g. by an operator preparing to
    hand-edit sdb.data) without losing anything committed up to that point.

    This is verified directly rather than by inspecting file sizes: create a
    marker object before FLUSH MNODE, and another one after it (so the second
    one is never covered by a flush and lives only in the WAL). Force-kill
    taosd (SIGKILL, so nothing is flushed as a side effect of a graceful
    shutdown), delete the mnode WAL outright, and restart. The pre-flush
    marker must survive; the post-flush marker must be gone -- that second
    half is the negative control proving the loss is really caused by
    deleting the WAL (and that FLUSH MNODE's protection is exactly bounded at
    the flush point), not some unrelated side effect of the test.
    """

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.index = 1

    def _get_cfg_dir(self):
        return tdDnodes.dnodes[0].cfgDir

    def _get_data_dir(self):
        # cfgDir and dataDir are sibling directories: .../dnodeN/cfg, .../dnodeN/data
        return os.path.join(os.path.dirname(self._get_cfg_dir()), "data")

    def _mnode_wal_dir(self):
        return os.path.join(self._get_data_dir(), "mnode", "wal")

    def _user_exists(self, name):
        tdSql.query(f"select * from information_schema.ins_users where name='{name}';")
        return tdSql.queryRows > 0

    def test_flush_mnode_wal_survival(self):
        """FLUSH MNODE makes prior mnode state durable enough to survive total WAL loss;
        anything committed after the flush and never re-flushed does not.

        1. Create marker_before, then run FLUSH MNODE.
        2. Create marker_after (deliberately left un-flushed).
        3. Force-kill taosd (SIGKILL -- no graceful-shutdown flush side effect).
        4. Delete the mnode WAL directory entirely.
        5. Restart taosd.
        6. marker_before must still exist; marker_after must be gone.

        Since: v3.4.3.0

        Labels: common,ci,integration,functional
        """
        marker_before = "flush_wal_marker_before"
        marker_after = "flush_wal_marker_after"

        tdSql.execute(f"create user {marker_before} pass 'Test1234!';")
        tdSql.checkEqual(self._user_exists(marker_before), True)

        tdLog.info("running FLUSH MNODE")
        tdSql.execute("flush mnode;")

        tdSql.execute(f"create user {marker_after} pass 'Test1234!';")
        tdSql.checkEqual(self._user_exists(marker_after), True)

        wal_dir = self._mnode_wal_dir()
        tdSql.checkEqual(os.path.isdir(wal_dir), True)

        tdLog.info("force-killing taosd (SIGKILL) to avoid an implicit flush on graceful exit")
        tdDnodes.forcestop(self.index)

        tdLog.info("deleting mnode WAL entirely: %s" % wal_dir)
        shutil.rmtree(wal_dir, ignore_errors=True)
        tdSql.checkEqual(os.path.isdir(wal_dir), False)

        tdLog.info("restarting taosd with the mnode WAL gone")
        tdDnodes.start(self.index)
        time.sleep(2)

        tdSql.checkEqual(self._user_exists(marker_before), True)
        after_survived = self._user_exists(marker_after)
        tdLog.info("marker_after present after WAL loss (expected False): %s" % after_survived)
        tdSql.checkEqual(after_survived, False)

        if self._user_exists(marker_before):
            tdSql.execute(f"drop user {marker_before};")
