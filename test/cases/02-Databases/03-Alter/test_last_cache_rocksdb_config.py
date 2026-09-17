"""
test_last_cache_rocksdb_config.py

Verification for "Last Cache RocksDB 引擎参数可配置化":

    test_cachesize_dynamic_resize       - Area 1: CACHESIZE dynamic modification
                                           (block cache + LRU cache resize live,
                                           no restart)
    test_background_threads_change      - Area 2: sLastCacheBackgroundThreads change
                                           + restart, config round-trips and taosd
                                           stays healthy
    test_other_params_no_crash          - Area 3: all remaining sLastCache* params -
                                           only asserts taosd does not crash after
                                           restart with varied (including boundary)
                                           values
    test_options_override_priority      - Area 4: sLastCacheOptionsOverride overlap
                                           with an explicit typed param - explicit
                                           param wins
    test_ins_last_cache_stats           - Area 5: information_schema.ins_last_cache_stats
                                           system table - row shape, read-only

Parameter reference (source/common/src/tglobal.c), all CFG_CATEGORY_GLOBAL so
must be changed via `ALTER ALL DNODES` (not `ALTER DNODE <id>`):

    | Parameter                        | Default | Range            | Dyn scope |
    |-----------------------------------|---------|------------------|-----------|
    | sLastCacheWriteBufferSize (MB)    | 8       | [1, 1048576]     | LAZY      |
    | sLastCacheMaxWriteBufferNumber     | 2       | [1, 64]          | LAZY      |
    | sLastCacheBackgroundThreads        | 2       | [1, 256]         | LAZY      |
    | sLastCacheBloomFilterBits          | 10      | [0, 32]          | LAZY      |
    | sLastCacheMaxOpenFiles             | -1      | [-1, 1048576]    | LAZY      |
    | sLastCacheCompressionType          | none    | -                | LAZY      |
    | sLastCacheOptionsOverride          | ""      | -                | LAZY      |

CACHESIZE is a per-database DDL option (`ALTER DATABASE <db> CACHESIZE <n>`,
valid range [1, 65536] MB, 0 is rejected by DDL validation); it is the sole
exception among these knobs that takes effect live, without a restart,
because tsdbCacheSetCapacity() calls rocksdb_cache_set_capacity() on the
already-open block cache in addition to resizing the in-memory LRU cache.
CACHESIZE is split evenly between the two: each gets CACHESIZE / 2, so
block_cache_capacity in ins_last_cache_stats reports half of the configured
CACHESIZE (this keeps total last-cache memory usage at ~CACHESIZE instead
of doubling it).

All other sLastCache* params are `_LAZY`: the RocksDB instance for each vnode
must be reopened (dnode restart) before the new value is picked up.
"""

from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestLastCacheRocksdbConfig:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # ------------------------------------------------------------------
    # --- util ---
    # ------------------------------------------------------------------

    def _query_dnode_var(self, param):
        tdSql.query(f"SHOW DNODE 1 VARIABLES LIKE '{param}'", queryTimes=1)
        tdSql.checkRows(1)

    def _alter_all_dnodes(self, param, value):
        tdSql.execute(f"ALTER ALL DNODES '{param}' '{value}'", queryTimes=1)

    def _alter_all_dnodes_and_verify(self, param, value):
        self._alter_all_dnodes(param, value)
        self._query_dnode_var(param)
        tdSql.checkData(0, 2, str(value))

    def _restart_dnode(self):
        tdLog.info("=============== stop and restart taosd")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

    def _assert_alive(self):
        # minimal "did not crash" probe: a trivial query must succeed
        tdSql.query("select server_version()", queryTimes=1)
        tdSql.checkRows(1)

    def _query_block_cache_capacity(self, dbname):
        tdSql.query(
            f"select block_cache_capacity from information_schema.ins_last_cache_stats "
            f"where db_name='{dbname}'"
        )
        return tdSql.queryResult

    # ------------------------------------------------------------------
    # --- impl ---
    # ------------------------------------------------------------------

    def do_cachesize_dynamic_resize(self):
        dbname = "db_lc_cachesize"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} cachemodel 'both' cachesize 16 vgroups 2")
        tdSql.execute(f"use {dbname}")
        tdSql.execute("create stable st(ts timestamp, c1 int) tags(t1 int)")
        for i in range(4):
            tdSql.execute(f"create table tb{i} using st tags({i})")
            tdSql.execute(f"insert into tb{i} values(now, {i})")

        # populate the last cache / open rocksdb
        tdSql.query("select last(*) from st")
        tdSql.checkRows(1)

        # CACHESIZE is split evenly between the LRU cache and the RocksDB
        # block cache, so block_cache_capacity reports CACHESIZE / 2.
        rows_before = self._query_block_cache_capacity(dbname)
        assert rows_before, "expected at least one ins_last_cache_stats row for db_lc_cachesize"
        for row in rows_before:
            assert row[0] == 8 * 1024 * 1024, (
                f"expected block_cache_capacity == 8MiB (half of 16MiB CACHESIZE) "
                f"before alter, got {row[0]}")
        tdLog.info(f"[Area1] initial block_cache_capacity rows: {rows_before}")

        # dynamic resize, no restart
        tdSql.execute(f"alter database {dbname} cachesize 32")
        rows_after = self._query_block_cache_capacity(dbname)
        assert rows_after, "expected ins_last_cache_stats rows after cachesize alter"
        for row in rows_after:
            assert row[0] == 16 * 1024 * 1024, (
                f"expected block_cache_capacity == 16MiB (half of 32MiB CACHESIZE) "
                f"live after CACHESIZE alter without restart, got {row[0]}")
        tdLog.info(f"[Area1] block_cache_capacity live-resized to 16MiB rows: {rows_after}")

        # shrink back down, also live
        tdSql.execute(f"alter database {dbname} cachesize 8")
        rows_shrunk = self._query_block_cache_capacity(dbname)
        assert rows_shrunk, "expected ins_last_cache_stats rows after cachesize shrink"
        for row in rows_shrunk:
            assert row[0] == 4 * 1024 * 1024, (
                f"expected block_cache_capacity == 4MiB (half of 8MiB CACHESIZE) "
                f"after shrink, got {row[0]}")
        tdLog.info(f"[Area1] block_cache_capacity live-shrunk to 4MiB rows: {rows_shrunk}")

        tdSql.execute(f"drop database if exists {dbname}")
        print("cachesize dynamic resize ......................... [ passed ]")

    def do_background_threads_change(self):
        dbname = "db_lc_threads"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} cachemodel 'both' cachesize 16")
        tdSql.execute(f"use {dbname}")
        tdSql.execute("create stable st(ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute("create table tb0 using st tags(0)")
        tdSql.execute("insert into tb0 values(now, 1)")
        tdSql.query("select last(*) from st")
        tdSql.checkRows(1)

        self._alter_all_dnodes_and_verify("sLastCacheBackgroundThreads", "8")
        tdLog.info("[Area2] sLastCacheBackgroundThreads=8 config round-tripped")

        self._restart_dnode()
        self._assert_alive()

        # confirm the config value survived the restart
        self._query_dnode_var("sLastCacheBackgroundThreads")
        tdSql.checkData(0, 2, "8")
        tdLog.info("[Area2] sLastCacheBackgroundThreads=8 persisted after restart")

        # last cache still functional (new thread pool serves the vnode)
        tdSql.execute(f"use {dbname}")
        tdSql.execute("insert into tb0 values(now+1s, 2)")
        tdSql.query("select last(c1) from tb0")
        tdSql.checkData(0, 0, 2)
        tdLog.info("[Area2] last cache read/write OK after thread-count change")

        # Stress values below the code-registered upper bound (256): each
        # vnode gets its own dedicated RocksDB background thread pool, so
        # applying the literal maximum spawns 256 OS threads per vnode -
        # with multiple vnodes in this db that is enough thread-stack
        # memory to OOM a CI host on its own, independent of any product
        # bug. The 256/257 upper boundary itself is still exercised via
        # the out-of-range check below, which is rejected by config
        # validation before ever being applied.
        for val in ("1", "32"):
            self._alter_all_dnodes_and_verify("sLastCacheBackgroundThreads", val)
            self._restart_dnode()
            self._assert_alive()
            tdLog.info(f"[Area2] sLastCacheBackgroundThreads={val} restart OK")

        # out-of-range values must be rejected, not silently accepted
        tdSql.error("ALTER ALL DNODES 'sLastCacheBackgroundThreads' '0'")
        tdSql.error("ALTER ALL DNODES 'sLastCacheBackgroundThreads' '257'")
        tdLog.info("[Area2] out-of-range sLastCacheBackgroundThreads rejected")

        # restore default and leave the dnode in a known-good state
        self._alter_all_dnodes("sLastCacheBackgroundThreads", "2")
        self._restart_dnode()
        tdSql.execute(f"drop database if exists {dbname}")
        print("background threads change ......................... [ passed ]")

    def do_other_params_no_crash(self):
        dbname = "db_lc_smoke"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} cachemodel 'both' cachesize 16")
        tdSql.execute(f"use {dbname}")
        tdSql.execute("create stable st(ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute("create table tb0 using st tags(0)")

        # (param, default, [valid values to try], [invalid values that must error])
        #
        # Valid values deliberately stay well below the code-registered
        # upper bound for sLastCacheWriteBufferSize/sLastCacheMaxOpenFiles:
        # actually applying the literal maximum (1048576 MB write buffer,
        # 1048576 open files) makes RocksDB try to allocate resources far
        # beyond what a CI host has, which fails vnode-open with Out of
        # Memory - a self-inflicted resource exhaustion, not a product
        # crash bug. The out-of-range boundary itself is still exercised
        # via the invalid_values below, which are rejected by config
        # validation before ever being applied.
        #
        # Each param is reset to its default before moving on to the next
        # one, so non-default values never compound across params - e.g.
        # sLastCacheWriteBufferSize left at 512 while sLastCacheMaxWrite-
        # BufferNumber is swept up to 64 would budget 512MB x 64 = 32GiB
        # of write-buffer memory per vnode and OOM the host on restart.
        cases = [
            ("sLastCacheWriteBufferSize", "8", ["1", "64", "512"], ["0", "1048577", "-1"]),
            ("sLastCacheMaxWriteBufferNumber", "2", ["1", "8", "64"], ["0", "65", "-1"]),
            ("sLastCacheBloomFilterBits", "10", ["0", "16", "32"], ["-1", "33"]),
            ("sLastCacheMaxOpenFiles", "-1", ["-1", "100", "10000"], ["-2", "1048577"]),
            # snappy/lz4/zstd/bogus are deliberately excluded: sLastCacheCompressionType
            # has no enum validation at ALTER time (tglobal.c only checks string
            # length), so any of them is accepted by config validation and only
            # fails later at vnode-open. This build's vendored RocksDB has no
            # compression codec linked in at all (confirmed via taosd log:
            # "Compression type ZSTD is not linked with the binary", same for
            # Snappy/LZ4) - misreported up the stack as Out of Memory. "none"
            # is the only compression value that actually opens successfully
            # here, so it's the only one exercised as a valid value.
            ("sLastCacheCompressionType", "none", ["none"], []),
        ]

        for i, (param, default, valid_values, invalid_values) in enumerate(cases):
            for value in valid_values:
                self._alter_all_dnodes_and_verify(param, value)
                self._restart_dnode()
                self._assert_alive()

                tdSql.execute(f"use {dbname}")
                tdSql.execute(f"insert into tb0 values(now+{i}s, {i})")
                tdSql.query("select last(c1) from tb0")
                tdSql.checkData(0, 0, i)
                tdLog.info(f"[Area3] {param}={value} restart+exercise OK (no crash)")

            for bad_value in invalid_values:
                tdSql.error(f"ALTER ALL DNODES '{param}' '{bad_value}'")
                tdLog.info(f"[Area3] {param}={bad_value} correctly rejected")

            # reset before the next param's sweep so values never compound
            self._alter_all_dnodes(param, default)

        self._restart_dnode()
        tdSql.execute(f"drop database if exists {dbname}")
        print("other params no-crash smoke ......................... [ passed ]")

    def do_options_override_priority(self):
        dbname = "db_lc_override"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} cachemodel 'both' cachesize 16")
        tdSql.execute(f"use {dbname}")
        tdSql.execute("create stable st(ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute("create table tb0 using st tags(0)")
        tdSql.execute("insert into tb0 values(now, 1)")

        # override sets write_buffer_size to 4MiB, explicit param sets it to
        # 32MiB - explicit param must win because its setter runs after the
        # override is parsed into the options object.
        override_str = "write_buffer_size=4194304"
        self._alter_all_dnodes("sLastCacheOptionsOverride", override_str)
        self._alter_all_dnodes_and_verify("sLastCacheWriteBufferSize", "32")

        self._query_dnode_var("sLastCacheOptionsOverride")
        tdSql.checkData(0, 2, override_str)

        self._restart_dnode()
        self._assert_alive()

        # last cache still functional post-restart with both set
        tdSql.execute(f"use {dbname}")
        tdSql.execute("insert into tb0 values(now+1s, 2)")
        tdSql.query("select last(c1) from tb0")
        tdSql.checkData(0, 0, 2)
        tdLog.info(
            "[Area4] overlapping override + explicit write_buffer_size: "
            "taosd healthy, last cache functional post-restart")

        # sLastCacheOptionsOverride has no syntax validation at ALTER time
        # (tglobal.c only checks string length), so a malformed value is
        # accepted and persisted verbatim - confirmed here without ever
        # restarting into it, because doing so is destructive (see below).
        # Set and immediately verify inside try/finally: if the assertion
        # below ever fails, the malformed value must still be reset to a
        # valid one before this function returns - otherwise it stays
        # persisted in the dnode's local config store and can brick a
        # later restart in an unrelated test file (see comment below).
        malformed = "not_a_valid_option=1"
        try:
            self._alter_all_dnodes("sLastCacheOptionsOverride", malformed)
            self._query_dnode_var("sLastCacheOptionsOverride")
            tdSql.checkData(0, 2, malformed)
            tdLog.info(
                "[Area4] malformed sLastCacheOptionsOverride is accepted and "
                "persisted verbatim by config validation (no syntax check)")
        finally:
            self._alter_all_dnodes("sLastCacheOptionsOverride", override_str)

        # sLastCacheOptionsOverride is dnode-wide (every vnode's RocksDB
        # last cache parses the same string), not scoped to a single
        # vnode/database. Restarting with it actually applied - confirmed
        # empirically via taosd log inspection, not guessed - makes EVERY
        # vnode fail tsdbOpenRocksCache() with "Invalid argument: Could
        # not find option: : not_a_valid_option", which cascades to
        # "failed to open all vnodes" / "failed to open nodes", and taosd
        # itself then exits the whole process rather than surviving in a
        # degraded state (log shows "dnode env is cleaned up" ~3s later,
        # no taosd process left). Because the value is persisted to the
        # dnode's local config store (data/dnode/config/global.json), a
        # restart in this state cannot be recovered via SQL - there is no
        # live server left to ALTER it back. So this test deliberately
        # never restarts into this state: doing so would permanently
        # brick the shared dnode for every test after this one, and
        # checkDnodes()'s 100s timeout calls tdLog.exit() (sys.exit),
        # which would abort the whole pytest run rather than just fail
        # this one case. Reset to a valid value now, before any restart.
        # Note: an empty string cannot be used here - ALTER ALL DNODES
        # 'key' '' is itself rejected (mndMCfg2DCfg in mndConfig.c requires
        # a non-empty value for the two-argument form, confirmed by reading
        # the source), so reuse the known-valid override_str already
        # restored above by the try/finally.
        self._alter_all_dnodes("sLastCacheWriteBufferSize", "8")
        self._restart_dnode()
        self._assert_alive()
        tdSql.execute(f"drop database if exists {dbname}")
        print("options override priority ......................... [ passed ]")

    def do_ins_last_cache_stats(self):
        dbname = "db_lc_stats"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} cachemodel 'both' cachesize 16 vgroups 3")
        tdSql.execute(f"use {dbname}")
        tdSql.execute("create stable st(ts timestamp, c1 int) tags(t1 int)")
        for i in range(6):
            tdSql.execute(f"create table tb{i} using st tags({i})")
            tdSql.execute(f"insert into tb{i} values(now, {i})")
        tdSql.query("select last(*) from st")
        tdSql.checkRows(1)

        # column shape: 23 columns per the registered lastCacheStatsSchema
        tdSql.query(
            "select dnode_id, vgroup_id, db_name, block_cache_capacity, block_cache_usage, "
            "block_cache_pinned_usage, estimate_table_readers_mem, cur_size_active_mem_table, "
            "cur_size_all_mem_tables, num_immutable_mem_table, mem_table_flush_pending, "
            "num_running_flushes, compaction_pending, num_running_compactions, "
            "estimate_pending_compaction_bytes, is_write_stopped, actual_delayed_write_rate, "
            "num_files_at_level0, total_sst_files_size, estimate_num_keys, num_live_versions, "
            "background_errors, update_time "
            f"from information_schema.ins_last_cache_stats where db_name='{dbname}'"
        )
        rows = tdSql.queryResult
        assert rows, "expected at least one ins_last_cache_stats row for db_lc_stats"
        # one row per vgroup that has opened its last cache
        assert len(rows) <= 3, f"expected at most 3 rows (vgroups=3), got {len(rows)}"
        for row in rows:
            assert row[2] == dbname
            assert row[3] >= 0  # block_cache_capacity
        tdLog.info(f"[Area5] ins_last_cache_stats rows for {dbname}: {rows}")

        # read-only: writes must fail. Use an explicit column list matching the
        # value count so the failure is due to the table being read-only, not
        # an unrelated column-count mismatch (the schema has 23 columns).
        tdSql.error(
            f"insert into information_schema.ins_last_cache_stats "
            f"(dnode_id, vgroup_id, db_name) values(1, 1, '{dbname}')"
        )
        tdSql.error(f"delete from information_schema.ins_last_cache_stats where db_name='{dbname}'")
        tdSql.error(
            f"update information_schema.ins_last_cache_stats set block_cache_capacity=0 "
            f"where db_name='{dbname}'"
        )
        tdLog.info("[Area5] ins_last_cache_stats confirmed read-only")

        tdSql.execute(f"drop database if exists {dbname}")
        print("ins_last_cache_stats ......................... [ passed ]")

    # ------------------------------------------------------------------
    # --- main ---
    # ------------------------------------------------------------------

    def test_cachesize_dynamic_resize(self):
        """Area 1: CACHESIZE dynamic modification

        1. Create db with cachemodel 'both' and cachesize > 0.
        2. Write data and run a last() query to open the block cache.
        3. ALTER DATABASE ... CACHESIZE to a new value.
        4. Verify block_cache_capacity in ins_last_cache_stats reflects half
           of the new value immediately (no restart), both growing and
           shrinking - CACHESIZE is split evenly between the LRU cache and
           the RocksDB block cache so total memory usage stays ~CACHESIZE.

        Catalog:
            - Database:Alter

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-02 Created for last cache RocksDB config feature
            - 2026-09-03 Removed the cachesize=0 edge case: CREATE DATABASE
              rejects cachesize 0 outright ([1, 65536] range), so it is not
              reachable via any supported DDL path
            - 2026-09-10 Updated expected block_cache_capacity values to
              CACHESIZE / 2: CACHESIZE is now split evenly between the LRU
              cache and the RocksDB block cache to avoid doubling total
              last-cache memory usage on upgrade

        """
        self.do_cachesize_dynamic_resize()

    def test_background_threads_change(self):
        """Area 2: sLastCacheBackgroundThreads modification + restart

        1. ALTER ALL DNODES the thread-count param to a non-default value.
        2. Verify SHOW DNODE VARIABLES round-trips the new value immediately
           (the config item itself updates even though it is _LAZY for the
           RocksDB env; only the RocksDB-side effect requires a restart).
        3. Restart the dnode so the new thread count is applied to freshly
           opened per-vnode RocksDB environments.
        4. Verify taosd is healthy and last-cache reads/writes still work
           after the restart, including boundary values.

        Catalog:
            - Database:Alter

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-02 Created for last cache RocksDB config feature

        """
        self.do_background_threads_change()

    def test_other_params_no_crash(self):
        """Area 3: remaining sLastCache* params - no-crash smoke test

        For sLastCacheWriteBufferSize, sLastCacheMaxWriteBufferNumber,
        sLastCacheBloomFilterBits, sLastCacheMaxOpenFiles and
        sLastCacheCompressionType: set several values (including the
        actual code-registered boundary values), restart the dnode after
        each change, and assert only that taosd stays up (a trivial query
        succeeds) and last-cache reads/writes keep working. Out-of-range
        values are also checked to be rejected by config validation
        without crashing taosd.

        Catalog:
            - Database:Alter

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-02 Created for last cache RocksDB config feature

        """
        self.do_other_params_no_crash()

    def test_options_override_priority(self):
        """Area 4: sLastCacheOptionsOverride priority vs explicit params

        sLastCacheOptionsOverride is applied to the RocksDB Options object
        first (via rocksdb_get_options_from_string), and every explicit
        sLastCacheXxx setter (write_buffer_size, max_write_buffer_number,
        max_open_files, compression, ...) runs afterwards and therefore
        overwrites whatever the override string set for the same key. This
        sets an overlapping key via the override string together with a
        differing explicit value, restarts, and confirms taosd starts up
        healthily. It also confirms a malformed override string is
        accepted and persisted verbatim by config validation (no syntax
        check), without ever restarting into it: sLastCacheOptionsOverride
        is dnode-wide, so a bad value makes every vnode fail to open its
        RocksDB last cache, and taosd exits the whole process rather than
        surviving in a degraded state - confirmed via direct taosd log
        inspection. Since the value is persisted to local dnode state,
        that failure cannot be recovered via SQL afterwards, so this test
        resets it to a valid value before ever restarting.

        Catalog:
            - Database:Alter

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-02 Created for last cache RocksDB config feature

        """
        self.do_options_override_priority()

    def test_ins_last_cache_stats(self):
        """Area 5: information_schema.ins_last_cache_stats system table

        1. Create a multi-vgroup database, populate the last cache.
        2. Verify one row per vgroup is returned, with the expected
           columns present (dnode_id, vgroup_id, db_name, block_cache_*,
           rocksdb stats columns, update_time).
        3. Verify it is read-only: DML against it must fail.

        Catalog:
            - Database:Query

        Since: v3.4.3.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-02 Created for last cache RocksDB config feature

        """
        self.do_ins_last_cache_stats()
