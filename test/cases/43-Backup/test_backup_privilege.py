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

from new_test_framework.utils import tdLog, tdSql


class TestBackupPrivilege:
    """Backup privilege (CREATE BACKUP / SHOW BACKUPS)

    Covers the two backup-related privileges:

        GRANT CREATE BACKUP TO <user>    -> allow CREATE FULL BACKUP
        GRANT SHOW BACKUPS  TO <user>    -> allow SHOW BACKUPS

    Both are system-category privileges (no ON <target> clause). A plain
    user without any grant must be rejected; after the grant the statement
    succeeds; after revoke it is rejected again.
    """

    backup_dirs = None
    ROOT_PASS = "taosdata"
    USER_PASS = "abcd@1234"

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        cls.dnode_id = 1
        cls.backup_dirs = []

    def teardown_class(cls):
        # drop the users created by the tests and clean up backup dirs
        tdSql.connect()  # root
        for u in ("priv_u1", "priv_u2"):
            tdSql.execute_ignore_error(f"DROP USER IF EXISTS {u}")
        for d in cls.backup_dirs:
            if d and os.path.exists(d):
                shutil.rmtree(d, ignore_errors=True)

    def _unique_backup_dir(self, tag):
        d = f"/tmp/backup_priv_{tag}_{int(time.time() * 1000)}"
        self.backup_dirs.append(d)
        return d

    def _create_user(self, user):
        tdSql.execute(f"CREATE USER IF NOT EXISTS {user} PASS '{self.USER_PASS}'")

    def _grant(self, privilege, user):
        tdSql.execute(f"GRANT {privilege} TO {user}")

    def _revoke(self, privilege, user):
        tdSql.execute(f"REVOKE {privilege} FROM {user}")

    def _login(self, user):
        tdSql.connect(user, password=self.USER_PASS)

    def _login_root(self):
        tdSql.connect()

    def _wait_backup_done(self, backup_id=None, baseline=None, timeout=60):
        """Wait for a specific newly created backup to appear and finish."""
        if backup_id is None:
            if baseline is None:
                tdLog.exit("backup id or pre-create backup baseline is required")
            backup_id = self._wait_backup_record(baseline, timeout)
        deadline = time.time() + timeout
        while time.time() < deadline:
            tdSql.query("SHOW BACKUPS")
            for r in range(tdSql.queryRows):
                if tdSql.getData(r, 0) == backup_id and tdSql.getData(r, 1) != "running":
                    return
            time.sleep(1)
        tdLog.exit("backup did not finish within %ds" % timeout)

    def test_create_backup_privilege(self):
        """CREATE BACKUP privilege

        1. root creates a plain user priv_u1
        2. priv_u1 login, CREATE FULL BACKUP        -> rejected (no privilege)
        3. root GRANT CREATE BACKUP TO priv_u1
        4. priv_u1 CREATE FULL BACKUP               -> success
        5. root REVOKE CREATE BACKUP FROM priv_u1
        6. priv_u1 CREATE FULL BACKUP               -> rejected (revoked)

        Since: v3.4.3.0

        Labels: common,ci,integration,functional,privilege
        Jira: None

        History:
            - 2026-08-20 Bomin Zhang created

        """
        tdLog.info("=========== CREATE BACKUP privilege ===========")
        self._login_root()
        self._create_user("priv_u1")

        # 2. without CREATE BACKUP, creating a backup is rejected
        self._login("priv_u1")
        tdSql.error(f"CREATE FULL BACKUP ON DNODE {self.dnode_id} TO '{self._unique_backup_dir('nopriv')}'")

        # 3. grant CREATE BACKUP
        self._login_root()
        self._grant("CREATE BACKUP", "priv_u1")

        # 4. now the user can create a backup
        self._login("priv_u1")
        backup_dir = self._unique_backup_dir("granted")
        tdSql.query("SHOW BACKUPS")
        baseline = max((tdSql.getData(r, 0) for r in range(tdSql.queryRows)), default=0)
        tdSql.execute(f"CREATE FULL BACKUP ON DNODE {self.dnode_id} TO '{backup_dir}'")
        self._wait_backup_done(baseline=baseline)

        # 5. revoke CREATE BACKUP
        self._login_root()
        self._revoke("CREATE BACKUP", "priv_u1")

        # 6. creating a backup is rejected again
        self._login("priv_u1")
        tdSql.error(f"CREATE FULL BACKUP ON DNODE {self.dnode_id} TO '{self._unique_backup_dir('revoked')}'")

        self._login_root()
        tdLog.info("create backup privilege passed")

    def test_kill_backup_privilege(self):
        """KILL BACKUP privilege (same as CREATE BACKUP)

        1. root creates a plain user priv_u3
        2. root starts a backup (so there is a running backup id to kill)
        3. priv_u3 KILL BACKUP <id>          -> rejected (no privilege)
        4. root GRANT CREATE BACKUP TO priv_u3  (MND_OPER_BACKUP covers KILL too)
        5. priv_u3 KILL BACKUP <id>          -> success
        6. root REVOKE CREATE BACKUP FROM priv_u3
        7. priv_u3 KILL BACKUP <id>          -> rejected (revoked)

        KILL BACKUP shares the MND_OPER_BACKUP operation with CREATE BACKUP,
        so it is gated by the same CREATE BACKUP privilege.

        Since: v3.4.3.0

        Labels: common,ci,integration,functional,privilege
        Jira: None

        History:
            - 2026-08-20 Bomin Zhang created

        """
        tdLog.info("=========== KILL BACKUP privilege ===========")
        self._login_root()
        self._create_user("priv_u3")

        # 2. root starts a backup so a running backup id exists
        backup_dir = self._unique_backup_dir("kill_base")
        tdSql.query("SHOW BACKUPS")
        baseline = max((tdSql.getData(r, 0) for r in range(tdSql.queryRows)), default=0)
        tdSql.execute(f"CREATE FULL BACKUP ON DNODE {self.dnode_id} TO '{backup_dir}'")
        backup_id = self._wait_backup_record(baseline)
        tdLog.info(f"backup id to kill: {backup_id}")

        # 3. without CREATE BACKUP, killing is rejected
        self._login("priv_u3")
        tdSql.error(f"KILL BACKUP {backup_id}")

        # 4. grant CREATE BACKUP (covers MND_OPER_BACKUP, i.e. KILL as well)
        self._login_root()
        self._grant("CREATE BACKUP", "priv_u3")

        # 5. now the user can kill the backup
        self._login("priv_u3")
        tdSql.execute(f"KILL BACKUP {backup_id}")

        # wait for the killed backup to be marked cancelled and for the dnode
        # task to actually finish (no running record left), so the next test
        # can start a fresh backup
        self._wait_backup_done(backup_id=backup_id)

        # 6. revoke CREATE BACKUP
        self._login_root()
        self._revoke("CREATE BACKUP", "priv_u3")

        # 7. killing again (the same id) is rejected
        self._login("priv_u3")
        tdSql.error(f"KILL BACKUP {backup_id}")

        self._login_root()
        tdLog.info("kill backup privilege passed")

    def test_show_backups_privilege(self):
        """SHOW BACKUPS is visible to all users

        1. root creates a plain user priv_u2
        2. root creates a backup
        3. priv_u2 SHOW BACKUPS -> sees the backup records (no grant needed)

        Like other cluster-level SHOW commands (SHOW NODES / SHOW USERS /
        SHOW MOUNTS), PRIV_SHOW_BACKUPS is granted to every user through the
        default sysinfo_1 role (SYS_ADMIN_INFO1_ROLES), so a plain user can
        always list the backup records.

        Since: v3.4.3.0

        Labels: common,ci,integration,functional,privilege
        Jira: None

        History:
            - 2026-08-20 Bomin Zhang created

        """
        tdLog.info("=========== SHOW BACKUPS visibility ===========")
        self._login_root()
        self._create_user("priv_u2")

        # ensure at least one backup record exists
        tdSql.query("SHOW BACKUPS")
        baseline = max((tdSql.getData(r, 0) for r in range(tdSql.queryRows)), default=0)
        tdSql.execute(f"CREATE FULL BACKUP ON DNODE {self.dnode_id} TO '{self._unique_backup_dir('base')}'")
        self._wait_backup_done(baseline=baseline)

        # a plain user can see the backup records without any grant
        self._login("priv_u2")
        tdSql.query("SHOW BACKUPS")
        if tdSql.queryRows == 0:
            tdLog.exit("SHOW BACKUPS should be visible to a plain user, got 0 rows")

        self._login_root()
        tdLog.info("show backups visibility passed")

    def _wait_backup_record(self, baseline, timeout=60):
        """Wait until a backup newer than baseline appears."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            tdSql.query("SHOW BACKUPS")
            for r in range(tdSql.queryRows):
                backup_id = tdSql.getData(r, 0)
                if backup_id > baseline:
                    return backup_id
            time.sleep(1)
        tdLog.exit(f"backup record did not enter running within {timeout}s")
