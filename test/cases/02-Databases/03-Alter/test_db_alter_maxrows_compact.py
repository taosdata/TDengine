from new_test_framework.utils import tdLog, tdSql
import re
import time


class TestDbAlterMaxrowsCompact:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    #
    # --- util ---
    #

    def wait_compact_finish(self, timeout=60):
        for _ in range(timeout):
            rows = tdSql.query("show compacts")
            if rows == 0:
                return
            time.sleep(1)
        raise Exception("compact did not finish within timeout")

    def get_block_dist(self, tbname):
        """Parse `show table distributed` text output into (total_blocks, block_max_rows)."""
        tdSql.query(f"show table distributed {tbname}")
        total_blocks = None
        block_max_rows = None
        for row in tdSql.queryResult:
            line = row[0]
            m = re.search(r"Total_Blocks=\[(\d+)\]", line)
            if m:
                total_blocks = int(m.group(1))
            m = re.search(r"MaxRows=\[(\d+)\]", line)
            if m:
                block_max_rows = int(m.group(1))
        if total_blocks is None or block_max_rows is None:
            raise Exception(f"failed to parse 'show table distributed' output: {tdSql.queryResult}")
        return total_blocks, block_max_rows

    def get_fixed_dist_total_blocks(self, tbname):
        tdSql.query(
            f"select * from information_schema.ins_table_fixed_distributed "
            f"where db_name='{self.db}' and table_name='{tbname}'"
        )
        tdSql.checkRows(1)
        return tdSql.queryResult[0][2]

    #
    # --- impl ---
    #

    def prepare(self):
        self.db = "test_alter_maxrows_compact"
        self.tb = "nt"
        self.rows = 1200

        tdSql.execute(f"drop database if exists {self.db}")
        # keep both bounds small so a modest amount of data already spans several
        # blocks, avoiding a large data write just to exercise the feature
        tdSql.execute(f"create database {self.db} vgroups 1 minrows 10 maxrows 400")
        tdSql.execute(f"use {self.db}")
        tdSql.execute(f"create table {self.tb} (ts timestamp, v int)")

        sql = f"insert into {self.tb} values "
        for i in range(self.rows):
            sql += f"(now+{i + 1}s, {i}) "
        tdSql.execute(sql)

        # commit the data to disk so it is visible to compact/block-distribution
        tdSql.execute(f"flush database {self.db}")
        time.sleep(2)

        print("prepare ................................ [ passed ]")

    def do_baseline(self):
        self.baseline_blocks, baseline_max_rows = self.get_block_dist(self.tb)
        tdLog.info(f"baseline: total_blocks={self.baseline_blocks}, block_max_rows={baseline_max_rows}")

        assert baseline_max_rows <= 400, \
            f"baseline block max rows {baseline_max_rows} exceeds created maxrows 400"
        assert self.baseline_blocks == self.get_fixed_dist_total_blocks(self.tb), \
            "show table distributed and ins_table_fixed_distributed disagree on total_blocks"

        print("do baseline ............................ [ passed ]")

    def do_shrink_maxrows_and_compact(self):
        tdSql.execute(f"alter database {self.db} minrows 10 maxrows 200")
        tdSql.query(f"select * from information_schema.ins_databases where name='{self.db}'")
        tdSql.checkData(0, 12, '200')

        # no new write happened since the alter, so a plain compact would skip this
        # fileset (tsdbShouldCompact() returns false because lastCompact > lastCommit);
        # FORCE bypasses that skip and rewrites every file in the time range, which is
        # required for the new MAXROWS to actually apply to already-committed data
        tdSql.execute(f"compact database {self.db} force")
        self.wait_compact_finish()

        shrink_blocks, shrink_max_rows = self.get_block_dist(self.tb)
        tdLog.info(f"after shrink: total_blocks={shrink_blocks}, block_max_rows={shrink_max_rows}")

        assert shrink_max_rows <= 200, \
            f"block max rows {shrink_max_rows} exceeds altered maxrows 200"
        assert shrink_blocks > self.baseline_blocks, \
            f"expected more, smaller blocks after shrinking maxrows and force-compacting, " \
            f"got {shrink_blocks} blocks vs baseline {self.baseline_blocks}"
        assert shrink_blocks == self.get_fixed_dist_total_blocks(self.tb), \
            "show table distributed and ins_table_fixed_distributed disagree on total_blocks"

        self.shrink_blocks = shrink_blocks

        print("do shrink maxrows and compact .......... [ passed ]")

    def do_grow_maxrows_and_compact(self):
        tdSql.execute(f"alter database {self.db} minrows 10 maxrows 800")
        tdSql.query(f"select * from information_schema.ins_databases where name='{self.db}'")
        tdSql.checkData(0, 12, '800')

        tdSql.execute(f"compact database {self.db} force")
        self.wait_compact_finish()

        grow_blocks, grow_max_rows = self.get_block_dist(self.tb)
        tdLog.info(f"after grow: total_blocks={grow_blocks}, block_max_rows={grow_max_rows}")

        assert grow_max_rows <= 800, \
            f"block max rows {grow_max_rows} exceeds altered maxrows 800"
        assert grow_blocks < self.shrink_blocks, \
            f"expected fewer, larger blocks after growing maxrows and force-compacting, " \
            f"got {grow_blocks} blocks vs {self.shrink_blocks} after the shrink step"
        assert grow_blocks == self.get_fixed_dist_total_blocks(self.tb), \
            "show table distributed and ins_table_fixed_distributed disagree on total_blocks"

        print("do grow maxrows and compact ............ [ passed ]")

    def cleanup(self):
        tdSql.execute(f"drop database {self.db}")

    #
    # --- main ---
    #

    def test_db_alter_maxrows_compact(self):
        """Verify ALTER DATABASE MAXROWS takes effect on existing data via COMPACT ... FORCE

        1. Create a database with a small MAXROWS/MINROWS and write a modest amount of
           data into a single table, then flush it to disk and record the baseline
           block distribution
        2. Shrink MAXROWS (and MINROWS), run COMPACT DATABASE ... FORCE (required since
           there is no new write after the alter, so a plain compact would skip the
           fileset), poll SHOW COMPACTS until the task finishes, then verify via
           SHOW TABLE DISTRIBUTED / ins_table_fixed_distributed that the block count
           increased and no block exceeds the new MAXROWS
        3. Grow MAXROWS beyond the original value and repeat step 2, verifying the
           block count decreases again and blocks stay within the new bound

        Catalog:
            - Database

        Since: v3.4.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-08-24 Claude Added to verify ALTER DATABASE MAXROWS + COMPACT FORCE

        """
        self.prepare()
        self.do_baseline()
        self.do_shrink_maxrows_and_compact()
        self.do_grow_maxrows_and_compact()
        self.cleanup()
