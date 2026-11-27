# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""Regression test for the TDB free-page stack corruption.

Defect
------
`tdbPagerFetchPage()` sets `loadPage = 0` on every allocation, so a page that
is recycled from the free-page stack (`pgno <= dbOrigSize`) is zeroed by
`tdbPagerInitPage(init=0)` *before* `tdbPagerWrite()` records it in the undo
journal.  The journal therefore stores the already-initialized page image
instead of the free-list link that is on disk.  If that meta transaction never
reaches `tdbPostCommit`, the journal is replayed and the initialized image is
written back over the link word.  The free stack then points at a page whose
first four bytes are page content, and the next pop reports

    corrupted next pgno:4227858434, self:<pgno>, db file size:<n>
    tAssert at .../libs/tdb/src/db/tdbBtree.c:724

`4227858434` is `FC000002`, the `02 00 00 FC` leaf page header.  The link word
only matters while the free stack root page is full, so the scenario has to push
at least 680 pages into the free stack first.

Expected result
---------------
The free-page stack must survive a rolled-back meta transaction.  This case
FAILS while the defect is present and PASSES once recovery restores the real
free-list link.  It kills taosd on purpose and inspects its log, so it must not
run concurrently with other cases on the same dnode.

Why the seeding uses only SQL
-----------------------------
Dropping a table does not delete its `table.db` entry and silently skips the
schema entry, so ordinary DDL does not release meta pages.  Child table *tag*
index entries are deleted for real, and a large tag makes such an entry span an
overflow chain, so dropping a stable together with its children releases about
one page per child.  That is enough to push the free stack root page past its
capacity and switch it to the linked-list form.
"""

import glob
import os
import re
import shutil
import signal
import subprocess
import tempfile
import time

from new_test_framework.utils import tdLog, tdSql, tdDnodes


class TestTdbFreePageStackCorruption:
    # Tag length that makes one tag index entry span an overflow chain.
    TAG_VALUE_LEN = 1000
    # Dropped children; each releases about one page, 680 fill the root page.
    CHILD_TABLES = 1500
    # Children per CREATE TABLE statement.
    CHILD_BATCH = 200
    # DDL after recovery, to walk a long part of the free page chain.
    DRAIN_TABLES = 800

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.tmp_dir = tempfile.mkdtemp(prefix="tdb-freepage-")
        cls.markers = []

    # --- util ----------------------------------------------------------------
    def dnode(self):
        return tdDnodes.dnodes[0]

    def cfg_dir(self):
        return self.dnode().cfgDir

    def log_dir(self):
        return self.dnode().logDir

    def taosd_pids(self):
        """Pids of the taosd instance that serves this dnode only."""
        cfg = self.cfg_dir()
        listing = subprocess.run(["ps", "-efww"], capture_output=True, text=True).stdout
        pids = []
        for line in listing.splitlines():
            if "taosd" in line and cfg in line and "grep" not in line:
                try:
                    pids.append(int(line.split()[1]))
                except (IndexError, ValueError):
                    continue
        return pids

    def sigkill_taosd(self):
        for pid in self.taosd_pids():
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    def wait_taosd_exit(self, timeout=60):
        self.sigkill_taosd()
        deadline = time.time() + timeout
        while self.taosd_pids() and time.time() < deadline:
            time.sleep(0.2)
        time.sleep(0.5)

    def start_taosd(self):
        # startWithoutSleep() does not wait for the "online" log line, so a taosd
        # that aborts during recovery does not abort the test itself.
        tdDnodes.startWithoutSleep(1)
        time.sleep(10)

    def meta_dirs(self):
        """vnode meta directories, the TDB environments that back each vnode."""
        found = []
        for entry in self.dnode().dataDir:
            data_dir = entry.split(" ")[0]
            if not os.path.isdir(data_dir):
                continue
            found.extend(glob.glob(os.path.join(data_dir, "vnode", "vnode*", "meta")))
        return found

    def pending_journals(self):
        found = []
        for meta_dir in self.meta_dirs():
            found.extend(glob.glob(os.path.join(meta_dir, "main.tdb-journal.*")))
        return found

    def ready_journals(self):
        """Journals that already hold at least one journaled page image.

        A record is a 4-byte page number plus one 4096-byte page; the first two
        records are the free stack root and the recycled page.
        """
        needed = 2 * (4 + 4096)
        ready = []
        for path in self.pending_journals():
            try:
                if os.path.getsize(path) >= needed:
                    ready.append(path)
            except OSError:
                continue
        return ready

    def preserve_logs(self, phase):
        """Copy the taosd logs aside; starting taosd again wipes taosdlog.0."""
        src = self.log_dir()
        if not os.path.isdir(src):
            return
        dst = os.path.join(self.tmp_dir, "logs-%s" % phase)
        os.makedirs(dst, exist_ok=True)
        for name in os.listdir(src):
            path = os.path.join(src, name)
            if os.path.isfile(path):
                try:
                    shutil.copy2(path, os.path.join(dst, name))
                except OSError:
                    pass

    def scan_corruption_markers(self, phase):
        """Collect every corruption marker seen so far, current logs plus copies.

        The log line itself (timestamp, thread, message) is the identity, so the
        same event found in the live log and in a preserved copy is reported once.
        """
        roots = [self.log_dir()] + sorted(glob.glob(os.path.join(self.tmp_dir, "logs-*")))
        seen = []
        for root in roots:
            for path in glob.glob(os.path.join(root, "*")):
                if not os.path.isfile(path):
                    continue
                try:
                    with open(path, errors="ignore") as handle:
                        for line in handle:
                            if re.search(r"corrupted next pgno|tAssert at|Assertion .*failed", line):
                                seen.append(line.strip())
                except OSError:
                    continue

        for entry in seen:
            if entry not in self.markers:
                self.markers.append(entry)
        if seen:
            tdLog.error("corruption markers after %s: %d" % (phase, len(seen)))
        return seen

    def assert_no_corruption(self, phase):
        """Fail as soon as any corruption marker has been logged."""
        self.preserve_logs(phase)
        self.scan_corruption_markers(phase)
        for marker in self.markers:
            tdLog.error("TDB free-page stack corruption: %s" % marker)
        assert not self.markers, (
            "TDB free-page stack was corrupted (%d marker(s)); first: %s"
            % (len(self.markers), self.markers[0])
        )

    def taosd_responsive(self):
        """True when taosd answers; execute_ignore_error() reports a dead server as
        None and, unlike execute(), does not retry."""
        return tdSql.execute_ignore_error("select server_version()") is not None

    # --- impl ----------------------------------------------------------------
    def do_seed_free_page_stack(self):
        """Step 1: push more than 680 pages into the meta free-page stack."""
        tdSql.execute("drop database if exists w")
        tdSql.execute("create database w vgroups 1")
        tdSql.execute("use w")
        tdSql.execute(
            "create stable w.st (ts timestamp, c int) tags (tg binary(%d))"
            % self.TAG_VALUE_LEN
        )

        pad = "v" * (self.TAG_VALUE_LEN - 6)
        for start in range(0, self.CHILD_TABLES, self.CHILD_BATCH):
            children = []
            for i in range(start, min(start + self.CHILD_BATCH, self.CHILD_TABLES)):
                children.append("w.ct%d using w.st tags('%s%05d')" % (i, pad, i))
            tdSql.execute("create table " + " ".join(children))

        # Dropping the stable cascades to the children; deleting each child
        # releases its large tag index entry, which carries an overflow chain.
        tdSql.execute("drop stable w.st")
        tdSql.execute("use w")
        tdSql.query("show stables")
        tdSql.checkRows(0)

        # Parent stable for the DDL in step 3; created here so the commit below
        # persists it too.
        tdSql.execute("create stable w.kt (ts timestamp, c int) tags (tg int)")

        tdLog.info("free-page stack seeded with %d dropped children" % self.CHILD_TABLES)

    def do_commit_vnode(self):
        """Step 2: commit the vnode so the seeded free stack reaches disk.

        The seeding above only reaches the vnode WAL; main.tdb keeps its initial
        17 pages until a commit flushes the filled free-page stack.  `FLUSH
        DATABASE` is rewritten to TDMT_VND_COMMIT and performs that commit.
        """
        tdSql.execute("flush database w")

    def do_kill_taosd_holding_meta_journal(self):
        """Step 3: create child tables, then kill taosd while its journal remains.

        The meta transaction spans the DDL below and its undo journal is only
        removed by tdbPostCommit, so the journal is still on disk once the DDL has
        finished.  By then it holds a page recycled from the free-page stack, whose
        journaled image is the initialized copy rather than the free-list link.
        """
        if not self.meta_dirs():
            tdLog.exit("no vnode meta directory found under %s" % self.dnode().dataDir)

        # The dnode is started through `screen ... bash -c '... taosd -c <cfg>'`, so
        # the process listing can match the wrapper as well as taosd itself; signal
        # every match.
        pids = self.taosd_pids()
        assert pids, "taosd is not running on %s" % self.cfg_dir()

        # 12 statements: a recycled free page enters the undo journal at about the
        # 9th statement, once a page split allocates from the free stack.  The
        # margin above that keeps the scenario short and reliable.
        for t in range(12):
            tdSql.execute("create table w.k%d using w.kt tags(%d)" % (t, t))

        # Wait for the journal to appear, then keep the transaction open a moment
        # longer so the DDL has settled before taosd is killed.
        deadline = time.time() + 180
        while time.time() < deadline and not self.pending_journals():
            time.sleep(0.5)
        time.sleep(1.0)

        for target in pids:
            try:
                os.kill(target, signal.SIGKILL)
            except ProcessLookupError:
                pass
        self.wait_taosd_exit()

        left_behind = self.ready_journals()
        assert left_behind, (
            "no meta undo journal survived the kill; the rollback path under test "
            "was not exercised"
        )
        tdLog.info("uncommitted meta journal left behind: %s" % left_behind)

        # Corruption can already show up in the killed process itself.
        self.assert_no_corruption("after-kill")

    def do_verify_free_page_stack_survives(self):
        """Step 4: recovery must restore the free stack and keep working."""
        self.start_taosd()
        recovered = bool(self.taosd_pids()) and self.taosd_responsive()
        if not recovered:
            tdLog.error(
                "taosd did not survive recovering the meta journal; pids=%s"
                % self.taosd_pids()
            )
        self.assert_no_corruption("after-recovery")
        assert recovered, "taosd did not survive recovery of the meta journal"

        # Allocate a long run of pages so a large part of the free page chain is
        # popped.  execute_ignore_error() reports a failure as None without
        # retrying or raising, so a dead server stops the loop at once and the
        # number of tables actually created shows how far the free stack drained.
        created = 0
        for t in range(self.DRAIN_TABLES):
            if tdSql.execute_ignore_error(
                "create table w.e%d using w.kt tags(%d)" % (t, t)
            ) is None:
                break
            created += 1
        tdLog.info("drain workload: %d/%d tables created" % (created, self.DRAIN_TABLES))
        self.assert_no_corruption("after-drain")
        assert created == self.DRAIN_TABLES, (
            "allocating from the free-page stack failed (%d/%d tables)"
            % (created, self.DRAIN_TABLES)
        )

        # Data written after recovery must survive another clean restart.
        tdDnodes.stop(1)
        self.start_taosd()
        restarted = bool(self.taosd_pids()) and self.taosd_responsive()
        self.assert_no_corruption("after-final-restart")
        assert restarted, "taosd did not come back after the post-recovery restart"

        tables = int(
            tdSql.getFirstValue(
                "select count(*) from information_schema.ins_tables where db_name='w'"
            )
        )
        tdLog.info("tables visible in w after restart: %d" % tables)
        assert tables >= self.DRAIN_TABLES, (
            "metadata did not survive the restart (count=%s)" % tables
        )

        self.assert_no_corruption("final")
        tdLog.info("finished verifying the free page stack after rollback and drain")

    # --- main ----------------------------------------------------------------
    def test_tdb_free_page_stack_corruption(self):
        """Free-page stack must survive a rolled-back meta transaction

        1. Fill the meta free-page stack past its root page capacity using DDL
           only (large tag on a stable, then drop the stable with its children)
        2. Commit the vnode (FLUSH DATABASE) so the seeded free stack is
           persisted from the WAL to disk
        3. Run DDL and SIGKILL taosd while a meta undo journal holds the free
           stack root and the recycled page images
        4. Recovery must restore the real free-list link: taosd must come up,
           allocating from the free stack must keep working, and the taosd log
           must not contain "corrupted next pgno" / "tAssert at"

        The case FAILS when the defect is present, and PASSES once the undo
        journal records the pre-image before the recycled page is zeroed.

        This case kills taosd and inspects its log, so it must not share a dnode
        with other cases.

        Catalog:
            - TDB

        Since: v3.4.3.1

        Labels: common,integration

        Jira: None

        History:
            - 2025-10-15 Bomin Zhang Created

        """
        self.do_seed_free_page_stack()
        self.do_commit_vnode()
        self.do_kill_taosd_holding_meta_journal()
        self.do_verify_free_page_stack_survives()
