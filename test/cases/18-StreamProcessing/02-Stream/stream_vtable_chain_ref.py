import time
from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamVtableChainRef:
    """End-to-end tests for vtable chain-ref resolution in stream processing.

    Covers TC01-TC12 from the design spec (vtable chain-ref stream adaptation).

    NOTE: TCs that depend on cluster topology (cross-vnode), 10s throttling,
    or fault injection are gated and may be skipped if unsupported in the
    current CI environment.
    """
    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_chain_ref(self):
        """TC01-TC12 -- vtable chain-ref end-to-end.

        Since: v3.4.1.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-05-12 vtable chain-ref initial e2e cases.
            - 2026-05-19 drop TC13 (chain-too-deep): the upstream parser
              now rejects > 32-hop CREATE/ALTER VTABLE via
              TSDB_CODE_VTABLE_REF_DEPTH_EXCEEDED before the stream
              runtime resolver can be exercised; coverage moved to
              parser-level tests.
            - 2026-05-19 add TC13 cross-db chain, TC14 single-round
              multi-vnode fanout (covers streamBatchFanoutDrain shared
              semaphore), TC15 hop2 tblRefCache reuse for col + tag.
        """
        try:
            tdSql.query("alter all dnodes 'debugflag 135'")
            tdSql.query("alter local 'debugflag 135'")
            tdSql.query("show snodes")
            if tdSql.getRows() == 0:
                tdStream.createSnode()
            self._tc01_one_hop()
            self._tc02_three_hop_same_vg()
            self._tc03_three_hop_tag_chain_same_vg()
            self._tc04_three_hop_cross_vg()
            self._tc05_three_hop_tag_chain_cross_vg()
            self._tc06_event_window_chain_with_tag_cond()
            self._tc07_event_window_chain_with_tag_cond_cross_vg()
            self._tc08_partition_by_tag()
            self._tc09_tag_changed_fatal()
            self._tc10_col_terminal_changed_patch()
            self._tc11_ref_table_not_exist()
            self._tc12_ref_col_not_exist()
            self._tc13_cross_db_chain()
            self._tc14_single_round_fanout_multi_vnode()
            self._tc15_chain_cache_reuse_col_and_tag()
        finally:
            tdStream.dropAllStreamsAndDbs()

    # ----- helpers --------------------------------------------------------

    def _waitRows(self, sql, expected, timeout=60):
        deadline = time.time() + timeout
        last_err = None
        while time.time() < deadline:
            try:
                tdSql.query(sql, queryTimes=1)
                if tdSql.getRows() >= expected:
                    return
            except Exception as e:
                last_err = e
            time.sleep(1)
        tdLog.exit(f"timeout waiting for {expected} rows from: {sql}, last_err={last_err}")

    def _checkRows(self, label, sql, expected_rows):
        """Assert query produces exactly expected_rows in order.

        expected_rows: list[tuple] matching the SELECT projection. Caller is
        responsible for casting timestamp columns to bigint (epoch ms) so the
        comparison is timezone-independent.
        """
        self._waitRows(sql, len(expected_rows))
        tdSql.query(sql)
        actual = []
        for r in range(tdSql.getRows()):
            actual.append(tuple(tdSql.getData(r, c) for c in range(len(expected_rows[0]))))
        if tdSql.getRows() != len(expected_rows):
            tdLog.exit(
                f"{label}: expected {len(expected_rows)} rows, got {tdSql.getRows()} (rows={actual})"
            )
        if actual != expected_rows:
            tdLog.exit(f"{label}: row mismatch\n  expected: {expected_rows}\n  actual:   {actual}")

    # ----- test cases -----------------------------------------------------

    def _tc01_one_hop(self):
        tdLog.info("TC01: one-hop vtable chain ref")
        tdSql.execute("create database tc01 vgroups 1")
        tdSql.execute("use tc01")
        tdSql.execute("create table tc01.ct1 (ts timestamp, v int)")
        tdSql.execute("create vtable tc01.vt (ts timestamp, v int from tc01.ct1.v)")
        tdSql.execute(
            "create stream tc01.s1 sliding(1s) from tc01.vt into tc01.res as "
            "select ts, v from tc01.vt"
        )
        tdStream.checkStreamStatus("s1")
        ts = int(time.time() * 1000) // 1000 * 1000
        tdSql.execute(
            f"insert into tc01.ct1 values "
            f"({ts}, 1) ({ts + 1000}, 2) ({ts + 2000}, 3)"
        )
        # One-hop vtable mirrors source rows verbatim through the stream.
        expected = [(ts, 1), (ts + 1000, 2), (ts + 2000, 3)]
        self._checkRows(
            "TC01", "select cast(ts as bigint), v from tc01.res order by ts", expected
        )
        tdSql.execute("drop database tc01 force")

    def _tc02_three_hop_same_vg(self):
        tdLog.info("TC02: three-hop chain inside one vgroup")
        tdSql.execute("create database tc02 vgroups 1")
        tdSql.execute("use tc02")
        tdSql.execute("create table tc02.ct1 (ts timestamp, v int)")
        tdSql.execute("create vtable tc02.vt1 (ts timestamp, v int from tc02.ct1.v)")
        tdSql.execute("create vtable tc02.vt2 (ts timestamp, v int from tc02.vt1.v)")
        tdSql.execute("create vtable tc02.vt3 (ts timestamp, v int from tc02.vt2.v)")
        tdSql.execute(
            "create stream tc02.s2 sliding(1s) from tc02.vt3 into tc02.res as "
            "select ts, v from tc02.vt3"
        )
        tdStream.checkStreamStatus("s2")
        ts = int(time.time() * 1000) // 1000 * 1000
        tdSql.execute(
            f"insert into tc02.ct1 values "
            f"({ts}, 1) ({ts + 1000}, 2) ({ts + 2000}, 3)"
        )
        expected = [(ts, 1), (ts + 1000, 2), (ts + 2000, 3)]
        self._checkRows(
            "TC02", "select cast(ts as bigint), v from tc02.res order by ts", expected
        )
        tdSql.execute("drop database tc02 force")

    def _tc03_three_hop_tag_chain_same_vg(self):
        tdLog.info("TC03: three-hop tag chain inside one vgroup")
        # Source: physical stable + ct0 carrying a region tag.
        # Chain: vct1.tag <- ct0.tag, vct2.tag <- vct1.tag, vct3.tag <- vct2.tag.
        # Stream over vstb partition by region asserts the 3-hop tag chain
        # surfaces the original tag value on every output row.
        db = "tc03"
        tdSql.execute(f"create database {db} vgroups 1")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create stable {db}.stb (ts timestamp, v int) tags (region int)")
        tdSql.execute(f"create table {db}.ct0 using {db}.stb tags (77)")
        tdSql.execute(f"create stable {db}.vstb (ts timestamp, v int) tags (region int) virtual 1")
        tdSql.execute(
            f"create vtable {db}.vct1 (v from {db}.ct0.v) "
            f"using {db}.vstb tags(region from {db}.ct0.region)"
        )
        tdSql.execute(
            f"create vtable {db}.vct2 (v from {db}.vct1.v) "
            f"using {db}.vstb tags(region from {db}.vct1.region)"
        )
        tdSql.execute(
            f"create vtable {db}.vct3 (v from {db}.vct2.v) "
            f"using {db}.vstb tags(region from {db}.vct2.region)"
        )
        tdSql.execute(
            f"create stream {db}.s2b interval(1s) sliding(1s) from {db}.vstb "
            f"partition by region into {db}.res "
            f"tags (region int as region) as "
            f"select _twstart as ts, last(v) as v from %%trows"
        )
        tdStream.checkStreamStatus("s2b")
        ts = int(time.time() * 1000) // 1000 * 1000
        tdSql.execute(f"insert into {db}.ct0 values ({ts}, 1)")
        tdSql.execute(f"insert into {db}.ct0 values ({ts + 1500}, 2)")
        # Third row in the next 1s window forces the prior window to close.
        tdSql.execute(f"insert into {db}.ct0 values ({ts + 2500}, 3)")
        # interval 1s sliding 1s closes window N when window N+1 receives data.
        # Three rows at +0/+1500/+2500 ms close two windows; last(v) per window.
        expected = [
            (ts, 1, 77),
            (ts + 1000, 2, 77),
        ]
        self._checkRows(
            "TC03",
            f"select cast(ts as bigint), v, region from {db}.res order by ts",
            expected,
        )
        tdSql.execute(f"drop database {db} force")

    def _tc04_three_hop_cross_vg(self):
        tdLog.info("TC04: three-hop chain across vgroups")
        # Hash routes tables by name; with vgroups=3 the four candidate tables
        # may collapse onto one vg by chance. Retry with a name suffix until
        # the chain physically spans at least two vgroups.
        max_attempts = 8
        chosen_suffix = None
        chosen_vgs = None
        for attempt in range(max_attempts):
            suffix = "" if attempt == 0 else f"_a{attempt}"
            db = f"tc04{suffix}"
            tdSql.execute(f"drop database if exists {db}")
            tdSql.execute(f"create database {db} vgroups 3")
            tdSql.execute(f"use {db}")
            tdSql.execute(f"create table {db}.ct0 (ts timestamp, v int)")
            tdSql.execute(f"create vtable {db}.vt1 (ts timestamp, v int from {db}.ct0.v)")
            tdSql.execute(f"create vtable {db}.vt2 (ts timestamp, v int from {db}.vt1.v)")
            tdSql.execute(f"create vtable {db}.vt3 (ts timestamp, v int from {db}.vt2.v)")
            vgs = {}
            for tbl in ("ct0", "vt1", "vt2", "vt3"):
                tdSql.query(
                    f"select vgroup_id from information_schema.ins_tables "
                    f"where db_name='{db}' and table_name='{tbl}'"
                )
                vgs[tbl] = tdSql.getData(0, 0)
            tdLog.info(f"TC04 attempt {attempt} db={db} vgs={vgs}")
            # require that consecutive chain hops cross a vg boundary at least once.
            crosses = sum(
                1 for a, b in (("vt3", "vt2"), ("vt2", "vt1"), ("vt1", "ct0"))
                if vgs[a] != vgs[b]
            )
            if crosses >= 1 and len(set(vgs.values())) >= 2:
                chosen_suffix = suffix
                chosen_vgs = vgs
                break
            tdSql.execute(f"drop database {db}")
        if chosen_suffix is None:
            tdLog.exit("TC04: failed to obtain a cross-vgroup chain layout after retries")
        db = f"tc04{chosen_suffix}"
        tdLog.info(f"TC04 chosen db={db} vgs={chosen_vgs}")
        tdSql.execute(
            f"create stream {db}.s3 sliding(1s) from {db}.vt3 into {db}.res as "
            f"select ts, v from {db}.vt3"
        )
        tdStream.checkStreamStatus("s3")
        ts = int(time.time() * 1000) // 1000 * 1000
        tdSql.execute(
            f"insert into {db}.ct0 values "
            f"({ts}, 1) ({ts + 1000}, 2) ({ts + 2000}, 3)"
        )
        expected = [(ts, 1), (ts + 1000, 2), (ts + 2000, 3)]
        self._checkRows(
            "TC04", f"select cast(ts as bigint), v from {db}.res order by ts", expected
        )
        tdSql.execute(f"drop database {db} force")

    def _tc05_three_hop_tag_chain_cross_vg(self):
        tdLog.info("TC05: three-hop tag chain across vgroups")
        # Same chain shape as TC03 but vgroups=3 with retry-by-suffix until
        # the four chain tables physically span at least two vgroups.
        max_attempts = 8
        chosen_suffix = None
        chosen_vgs = None
        for attempt in range(max_attempts):
            suffix = "" if attempt == 0 else f"_a{attempt}"
            db = f"tc05{suffix}"
            tdSql.execute(f"drop database if exists {db}")
            tdSql.execute(f"create database {db} vgroups 3")
            tdSql.execute(f"use {db}")
            tdSql.execute(f"create stable {db}.stb (ts timestamp, v int) tags (region int)")
            tdSql.execute(f"create table {db}.ct0 using {db}.stb tags (88)")
            tdSql.execute(f"create stable {db}.vstb (ts timestamp, v int) tags (region int) virtual 1")
            tdSql.execute(
                f"create vtable {db}.vct1 (v from {db}.ct0.v) "
                f"using {db}.vstb tags(region from {db}.ct0.region)"
            )
            tdSql.execute(
                f"create vtable {db}.vct2 (v from {db}.vct1.v) "
                f"using {db}.vstb tags(region from {db}.vct1.region)"
            )
            tdSql.execute(
                f"create vtable {db}.vct3 (v from {db}.vct2.v) "
                f"using {db}.vstb tags(region from {db}.vct2.region)"
            )
            vgs = {}
            for tbl in ("ct0", "vct1", "vct2", "vct3"):
                tdSql.query(
                    f"select vgroup_id from information_schema.ins_tables "
                    f"where db_name='{db}' and table_name='{tbl}'"
                )
                vgs[tbl] = tdSql.getData(0, 0)
            tdLog.info(f"TC05 attempt {attempt} db={db} vgs={vgs}")
            crosses = sum(
                1 for a, b in (("vct3", "vct2"), ("vct2", "vct1"), ("vct1", "ct0"))
                if vgs[a] != vgs[b]
            )
            if crosses >= 1 and len(set(vgs.values())) >= 2:
                chosen_suffix = suffix
                chosen_vgs = vgs
                break
            tdSql.execute(f"drop database {db}")
        if chosen_suffix is None:
            tdLog.exit("TC05: failed to obtain a cross-vgroup tag-chain layout after retries")
        db = f"tc05{chosen_suffix}"
        tdLog.info(f"TC05 chosen db={db} vgs={chosen_vgs}")
        tdSql.execute(
            f"create stream {db}.s3b interval(1s) sliding(1s) from {db}.vstb "
            f"partition by region into {db}.res "
            f"tags (region int as region) as "
            f"select _twstart as ts, last(v) as v from %%trows"
        )
        tdStream.checkStreamStatus("s3b")
        ts = int(time.time() * 1000) // 1000 * 1000
        tdSql.execute(f"insert into {db}.ct0 values ({ts}, 1)")
        tdSql.execute(f"insert into {db}.ct0 values ({ts + 1500}, 2)")
        tdSql.execute(f"insert into {db}.ct0 values ({ts + 2500}, 3)")
        expected = [
            (ts, 1, 88),
            (ts + 1000, 2, 88),
        ]
        self._checkRows(
            "TC05",
            f"select cast(ts as bigint), v, region from {db}.res order by ts",
            expected,
        )
        tdSql.execute(f"drop database {db} force")

    def _tc06_event_window_chain_with_tag_cond(self):
        tdLog.info("TC06: event-window stream over chain-ref vchild with col + non-partition tag in event cond")
        # Shape:
        #   - two physical child tables under stb, each carrying (region, area):
        #       ct0  -> region=88, area=1   (event condition matches)
        #       ct1  -> region=99, area=2   (event condition never matches)
        #   - vstb mirrors the schema with virtual=1 + 2 tags;
        #   - per ct, a 3-hop col + tag chain into the vstb:
        #       vct1*  (v <- ct*.v,  region/area <- ct*)
        #       vct2*  (v <- vct1*, region/area <- vct1*)
        #       vct3*  (v <- vct2*, region/area <- vct2*)
        #   - stream uses event_window with both a column predicate (v) and a
        #     non-partition tag predicate (area), partitioning by region.
        # Expectation: only the region=88 partition (whose chain-resolved area
        # equals 1) closes an event window and produces a row in res; the
        # region=99 partition is silenced because the tag predicate fails on
        # its chain-resolved area.
        db = "tc06"
        tdSql.execute(f"create database {db} vgroups 1")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create stable {db}.stb (ts timestamp, v int) tags (region int, area int)"
        )
        tdSql.execute(f"create table {db}.ct0 using {db}.stb tags (88, 1)")
        tdSql.execute(f"create table {db}.ct1 using {db}.stb tags (99, 2)")
        tdSql.execute(
            f"create stable {db}.vstb (ts timestamp, v int) "
            f"tags (region int, area int) virtual 1"
        )
        # Chain rooted at ct0 (matches event predicate).
        tdSql.execute(
            f"create vtable {db}.vct1a (v from {db}.ct0.v) using {db}.vstb "
            f"tags(region from {db}.ct0.region, area from {db}.ct0.area)"
        )
        tdSql.execute(
            f"create vtable {db}.vct2a (v from {db}.vct1a.v) using {db}.vstb "
            f"tags(region from {db}.vct1a.region, area from {db}.vct1a.area)"
        )
        tdSql.execute(
            f"create vtable {db}.vct3a (v from {db}.vct2a.v) using {db}.vstb "
            f"tags(region from {db}.vct2a.region, area from {db}.vct2a.area)"
        )
        # Chain rooted at ct1 (must be silenced by the tag predicate).
        tdSql.execute(
            f"create vtable {db}.vct1b (v from {db}.ct1.v) using {db}.vstb "
            f"tags(region from {db}.ct1.region, area from {db}.ct1.area)"
        )
        tdSql.execute(
            f"create vtable {db}.vct2b (v from {db}.vct1b.v) using {db}.vstb "
            f"tags(region from {db}.vct1b.region, area from {db}.vct1b.area)"
        )
        tdSql.execute(
            f"create vtable {db}.vct3b (v from {db}.vct2b.v) using {db}.vstb "
            f"tags(region from {db}.vct2b.region, area from {db}.vct2b.area)"
        )
        tdSql.execute(
            f"create stream {db}.s3c "
            f"event_window(start with v >= 10 and area = 1 end with v < 5) "
            f"from {db}.vstb partition by tbname, region "
            f"stream_options(max_delay(3s)) into {db}.res "
            f"tags(region int as region) as "
            f"select _twstart as ts, last(v) as v from %%trows"
        )
        tdStream.checkStreamStatus("s3c")
        ts = int(time.time() * 1000) // 1000 * 1000
        # Drive both chains with the same value sequence: 12 (start) -> 4 (end).
        # Only ct0's chain should fire because its chain-resolved area=1.
        tdSql.execute(f"insert into {db}.ct0 values ({ts}, 12)")
        tdSql.execute(f"insert into {db}.ct0 values ({ts + 500}, 4)")
        tdSql.execute(f"insert into {db}.ct0 values ({ts + 1500}, 0)")
        tdSql.execute(f"insert into {db}.ct1 values ({ts}, 12)")
        tdSql.execute(f"insert into {db}.ct1 values ({ts + 500}, 4)")
        tdSql.execute(f"insert into {db}.ct1 values ({ts + 1500}, 0)")
        # 3 partitions (vct1a/vct2a/vct3a) x region=88 each fire 1 window
        # [v=12, v=4]; ct1 chain (region=99, area=2) is silenced by the tag
        # predicate. last(v) within the window is 4.
        expected = [
            (ts, 4, 88),
            (ts, 4, 88),
            (ts, 4, 88),
        ]
        self._checkRows(
            "TC06",
            f"select cast(ts as bigint), v, region from {db}.res order by ts, region",
            expected,
        )
        tdSql.execute(f"drop database {db} force")

    def _tc07_event_window_chain_with_tag_cond_cross_vg(self):
        tdLog.info("TC07: event-window chain-ref vchild with col + non-partition tag, across vgroups")
        # Same shape as TC06 but with vgroups=3 and a retry-by-suffix loop
        # until the eight chain tables (ct0/ct1 + vct1/2/3 a/b) physically
        # span at least two vgroups. Validates that the event_window stream
        # over a chain-ref vstb still surfaces correct results when chain
        # hops cross vnode boundaries (forces cross-vg PSEUDO_COL chain RPCs
        # and cross-vg col chain merge).
        max_attempts = 8
        chosen_suffix = None
        chosen_vgs = None
        for attempt in range(max_attempts):
            suffix = "" if attempt == 0 else f"_a{attempt}"
            db = f"tc07{suffix}"
            tdSql.execute(f"drop database if exists {db}")
            tdSql.execute(f"create database {db} vgroups 3")
            tdSql.execute(f"use {db}")
            tdSql.execute(
                f"create stable {db}.stb (ts timestamp, v int) tags (region int, area int)"
            )
            tdSql.execute(f"create table {db}.ct0 using {db}.stb tags (88, 1)")
            tdSql.execute(f"create table {db}.ct1 using {db}.stb tags (99, 2)")
            tdSql.execute(
                f"create stable {db}.vstb (ts timestamp, v int) "
                f"tags (region int, area int) virtual 1"
            )
            tdSql.execute(
                f"create vtable {db}.vct1a (v from {db}.ct0.v) using {db}.vstb "
                f"tags(region from {db}.ct0.region, area from {db}.ct0.area)"
            )
            tdSql.execute(
                f"create vtable {db}.vct2a (v from {db}.vct1a.v) using {db}.vstb "
                f"tags(region from {db}.vct1a.region, area from {db}.vct1a.area)"
            )
            tdSql.execute(
                f"create vtable {db}.vct3a (v from {db}.vct2a.v) using {db}.vstb "
                f"tags(region from {db}.vct2a.region, area from {db}.vct2a.area)"
            )
            tdSql.execute(
                f"create vtable {db}.vct1b (v from {db}.ct1.v) using {db}.vstb "
                f"tags(region from {db}.ct1.region, area from {db}.ct1.area)"
            )
            tdSql.execute(
                f"create vtable {db}.vct2b (v from {db}.vct1b.v) using {db}.vstb "
                f"tags(region from {db}.vct1b.region, area from {db}.vct1b.area)"
            )
            tdSql.execute(
                f"create vtable {db}.vct3b (v from {db}.vct2b.v) using {db}.vstb "
                f"tags(region from {db}.vct2b.region, area from {db}.vct2b.area)"
            )
            vgs = {}
            for tbl in ("ct0", "ct1", "vct1a", "vct2a", "vct3a", "vct1b", "vct2b", "vct3b"):
                tdSql.query(
                    f"select vgroup_id from information_schema.ins_tables "
                    f"where db_name='{db}' and table_name='{tbl}'"
                )
                vgs[tbl] = tdSql.getData(0, 0)
            tdLog.info(f"TC07 attempt {attempt} db={db} vgs={vgs}")
            # Require at least one chain hop to cross a vg boundary on either
            # the ct0 chain or the ct1 chain, and at least 2 distinct vgs in total.
            crosses = sum(
                1 for a, b in (
                    ("vct3a", "vct2a"), ("vct2a", "vct1a"), ("vct1a", "ct0"),
                    ("vct3b", "vct2b"), ("vct2b", "vct1b"), ("vct1b", "ct1"),
                )
                if vgs[a] != vgs[b]
            )
            if crosses >= 1 and len(set(vgs.values())) >= 2:
                chosen_suffix = suffix
                chosen_vgs = vgs
                break
            tdSql.execute(f"drop database {db}")
        if chosen_suffix is None:
            tdLog.exit("TC07: failed to obtain a cross-vgroup chain layout after retries")
        db = f"tc07{chosen_suffix}"
        tdLog.info(f"TC07 chosen db={db} vgs={chosen_vgs}")
        tdSql.execute(
            f"create stream {db}.s3d "
            f"event_window(start with v >= 10 and area = 1 end with v < 5) "
            f"from {db}.vstb partition by tbname, region "
            f"stream_options(max_delay(3s)) into {db}.res "
            f"tags(region int as region) as "
            f"select _twstart as ts, last(v) as v from %%trows"
        )
        tdStream.checkStreamStatus("s3d")
        ts = int(time.time() * 1000) // 1000 * 1000
        tdSql.execute(f"insert into {db}.ct0 values ({ts}, 12)")
        tdSql.execute(f"insert into {db}.ct0 values ({ts + 500}, 4)")
        tdSql.execute(f"insert into {db}.ct0 values ({ts + 1500}, 0)")
        tdSql.execute(f"insert into {db}.ct1 values ({ts}, 12)")
        tdSql.execute(f"insert into {db}.ct1 values ({ts + 500}, 4)")
        tdSql.execute(f"insert into {db}.ct1 values ({ts + 1500}, 0)")
        # Same expectation as TC06: only the ct0 chain (area=1) fires, and
        # each of vct1a/vct2a/vct3a produces one row with last(v)=4.
        expected = [
            (ts, 4, 88),
            (ts, 4, 88),
            (ts, 4, 88),
        ]
        self._checkRows(
            "TC07",
            f"select cast(ts as bigint), v, region from {db}.res order by ts, region",
            expected,
        )
        tdSql.execute(f"drop database {db} force")

    def _tc08_partition_by_tag(self):
        tdLog.info("TC08: partition by vtable tag")
        # Per spec v0.3, partition-by tag only makes sense on a CHILD vtable
        # (NORMAL vtable has no tag concept). The minimal shape is:
        #   * physical child tables carrying the data column;
        #   * a virtual super-table mirroring the schema;
        #   * two virtual child tables under that vstb whose column refs the
        #     physical ct (one-hop column chain) and whose tag is a literal on
        #     the vchild's own STag (no tag chain RPC needed);
        #   * a stream from the vstb partition-by tag.
        # The test asserts that the chain-ref reader plumbing surfaces the
        # tag values into the stream output groups.
        tdSql.execute("create database tc08 vgroups 1")
        tdSql.execute("use tc08")
        tdSql.execute("create table tc08.ct1 (ts timestamp, v int)")
        tdSql.execute("create table tc08.ct2 (ts timestamp, v int)")
        tdSql.execute(
            "create stable tc08.vstb (ts timestamp, v int) "
            "tags (region int) virtual 1"
        )
        tdSql.execute(
            "create vtable tc08.vct1 (v from tc08.ct1.v) "
            "using tc08.vstb tags(11)"
        )
        tdSql.execute(
            "create vtable tc08.vct2 (v from tc08.ct2.v) "
            "using tc08.vstb tags(22)"
        )
        tdSql.execute(
            "create stream tc08.s4 interval(1s) sliding(1s) from tc08.vstb "
            "partition by region into tc08.res "
            "tags (region int as region) as "
            "select _twstart as ts, last(v) as v from %%trows"
        )
        tdStream.checkStreamStatus("s4")
        ts = int(time.time() * 1000) // 1000 * 1000
        tdSql.execute(f"insert into tc08.ct1 values ({ts}, 1)")
        tdSql.execute(f"insert into tc08.ct1 values ({ts + 1500}, 2)")
        tdSql.execute(f"insert into tc08.ct1 values ({ts + 2500}, 3)")
        tdSql.execute(f"insert into tc08.ct2 values ({ts}, 10)")
        tdSql.execute(f"insert into tc08.ct2 values ({ts + 1500}, 20)")
        tdSql.execute(f"insert into tc08.ct2 values ({ts + 2500}, 30)")
        # 2 partitions (region=11 from ct1, region=22 from ct2);
        # 3 rows per partition close 2 windows each.
        expected = [
            (ts, 1, 11),
            (ts + 1000, 2, 11),
            (ts, 10, 22),
            (ts + 1000, 20, 22),
        ]
        self._checkRows(
            "TC08",
            "select cast(ts as bigint), v, region from tc08.res order by region, ts",
            expected,
        )
        tdSql.execute("drop database tc08 force")

    def _tc09_tag_changed_fatal(self):
        tdLog.info("TC09: chain-ref partition tag mutation -> stream Failed (0x701D)")
        # Spec TC09: when the terminal-link tag value of a partition-by tag
        # chain changes, the >=10s throttled recheck hook must detect the
        # diff and return TSDB_CODE_STREAM_VTB_TAG_CHANGED (0x701D). The
        # reader bails, the trigger task self-fails on the error RSP, and
        # mnode surfaces the failure as status='Failed' with a message
        # containing 'partition tag changed'.
        db = "tc09"
        tdSql.execute(f"create database {db} vgroups 1")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create stable {db}.stb (ts timestamp, v int) tags (region int)")
        tdSql.execute(f"create table {db}.ct1 using {db}.stb tags (11)")
        tdSql.execute(f"create stable {db}.vstb (ts timestamp, v int) tags (region int) virtual 1")
        tdSql.execute(
            f"create vtable {db}.vct1 (v from {db}.ct1.v) "
            f"using {db}.vstb tags(region from {db}.ct1.region)"
        )
        tdSql.execute(
            f"create stream {db}.s5 interval(1s) sliding(1s) from {db}.vstb "
            f"partition by region into {db}.res "
            f"tags (region int as region) as "
            f"select _twstart as ts, last(v) as v from %%trows"
        )
        tdStream.checkStreamStatus("s5")
        ts = int(time.time() * 1000) // 1000 * 1000
        # Initial batch -- builds vtbCache, lastCheckMs is set to ~now.
        tdSql.execute(f"insert into {db}.ct1 values ({ts}, 1) ({ts + 1500}, 2)")
        # Mutate the chain-terminal tag value; cached resolved tag is now stale.
        tdSql.execute(f"alter table {db}.ct1 set tag region = 99")
        # Wait past the 10s recheck throttle window (vnodeStream.c:4105).
        time.sleep(11)
        # Push a new row past the previous batch so the WAL-meta hook fires
        # and triggers streamMaybeRecheckVTableCache -> diff -> 0x701D.
        tdSql.execute(f"insert into {db}.ct1 values ({ts + 12000}, 3)")
        # Poll ins_streams until status flips to Failed.
        deadline = time.time() + 60
        status, message = (None, None)
        while time.time() < deadline:
            tdSql.query(
                "select status, message from information_schema.ins_streams "
                "where stream_name = 's5'"
            )
            if tdSql.getRows() >= 1:
                status, message = tdSql.getData(0, 0), tdSql.getData(0, 1)
                if status == "Failed":
                    break
            time.sleep(1)
        tdLog.info(f"TC09 final stream state: status={status!r} message={message!r}")
        if status != "Failed":
            tdLog.exit(
                f"TC09 expected status 'Failed', got {status!r} message={message!r}"
            )
        if "partition tag changed" not in (message or "").lower():
            tdLog.exit(
                f"TC09 expected message contain 'partition tag changed', "
                f"got {message!r}"
            )
        tdSql.execute(f"drop database {db} force")

    def _tc10_col_terminal_changed_patch(self):
        tdLog.info("TC10: middle vtable column re-points -> stream redeploys, resumes against new ref")
        # Spec TC10 (current product behavior): runtime ALTER on a middle
        # vtable's column ref is reported by the trigger reader as
        # INTERNAL_ERROR; mnode then tears down and redeploys the whole
        # stream. After redeploy completes the stream MUST come back to
        # 'Running' with 'Failed times' >= 1, and rows written to the NEW
        # chain terminal MUST surface via the rebuilt chain.
        db = "tc10"
        tdSql.execute(f"create database {db} vgroups 1")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {db}.ct1 (ts timestamp, v int)")
        tdSql.execute(f"create table {db}.ct2 (ts timestamp, v int)")
        tdSql.execute(f"create vtable {db}.vt1 (ts timestamp, v int from {db}.ct1.v)")
        tdSql.execute(f"create vtable {db}.vt2 (ts timestamp, v int from {db}.vt1.v)")
        tdSql.execute(f"create vtable {db}.vt3 (ts timestamp, v int from {db}.vt2.v)")
        tdSql.execute(
            f"create stream {db}.s6 sliding(1s) from {db}.vt3 into {db}.res as "
            f"select ts, v from {db}.vt3"
        )
        tdStream.checkStreamStatus("s6")
        ts = int(time.time() * 1000) // 1000 * 1000
        # Baseline write against the old chain (vt2 -> vt1 -> ct1).
        tdSql.execute(
            f"insert into {db}.ct1 values ({ts}, 100) ({ts + 1000}, 200)"
        )
        self._checkRows(
            "TC10-baseline",
            f"select cast(ts as bigint), v from {db}.res order by ts",
            [(ts, 100), (ts + 1000, 200)],
        )
        # Capture pre-change message ("Running start from: <ts0>") so we can
        # detect redeploy by message-change rather than by 'Failed times' text
        # (mnode redeploy resets the message back to a clean 'Running start
        # from: <ts1>' with ts1 > ts0).
        tdSql.query(
            "select status, message from information_schema.ins_streams "
            "where stream_name = 's6'"
        )
        initial_status = tdSql.getData(0, 0) if tdSql.getRows() >= 1 else None
        initial_message = tdSql.getData(0, 1) if tdSql.getRows() >= 1 else None
        tdLog.info(
            f"TC10 pre-change: status={initial_status!r} message={initial_message!r}"
        )
        # Re-point the middle node onto a new physical table.
        tdSql.execute(f"drop vtable {db}.vt2")
        tdSql.execute(
            f"create vtable {db}.vt2 (ts timestamp, v int from {db}.ct2.v)"
        )
        # The trigger reader reports INTERNAL_ERROR -> mnode tears down and
        # redeploys the stream. Poll until message changes from the captured
        # initial value (proves the redeploy round-trip happened) and status
        # is back to 'Running'.
        deadline = time.time() + 120
        redeployed = False
        last_status, last_message = None, None
        while time.time() < deadline:
            tdSql.query(
                "select status, message from information_schema.ins_streams "
                "where stream_name = 's6'"
            )
            if tdSql.getRows() >= 1:
                last_status = tdSql.getData(0, 0)
                last_message = tdSql.getData(0, 1)
                if last_status == "Running" and last_message != initial_message:
                    redeployed = True
                    break
            time.sleep(2)
        if not redeployed:
            tdLog.exit(
                f"TC10 expected stream to be redeployed (Running with "
                f"message changed from {initial_message!r}); "
                f"last status={last_status!r} message={last_message!r}"
            )
        tdLog.info(
            f"TC10 redeployed: status={last_status!r} message={last_message!r}"
        )
        # Write to the new chain terminal; the rebuilt chain must surface it.
        new_ts = int(time.time() * 1000) // 1000 * 1000
        tdSql.execute(f"insert into {db}.ct2 values ({new_ts}, 300)")
        self._waitRows(
            f"select cast(ts as bigint), v from {db}.res where v = 300", 1
        )
        tdSql.query(
            f"select cast(ts as bigint), v from {db}.res where v = 300"
        )
        if tdSql.getRows() != 1 or tdSql.getData(0, 1) != 300:
            tdLog.exit(
                f"TC10: post-redeploy write to ct2 missing in res "
                f"(got {tdSql.getRows()} rows)"
            )
        tdSql.execute(f"drop database {db} force")

    def _tc11_ref_table_not_exist(self):
        tdLog.info(
            "TC11: runtime resolver propagates STREAM_VTB_REF_TABLE_NOT_EXIST"
            " when a middle-chain vtable is dropped"
        )
        # Spec TC11: when a middle-link ref-table disappears at runtime, the
        # vnode reader returns TSDB_CODE_STREAM_VTB_REF_TABLE_NOT_EXIST in the
        # per-item response, A function propagates it, the trigger task
        # self-fails on the error RSP, and mnode surfaces the failure as
        # status='Failed' with a message containing the ref-table-not-exist
        # error string.
        db = "tc11"
        tdSql.execute(f"create database {db} vgroups 1")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {db}.ct1 (ts timestamp, v int)")
        tdSql.execute(f"create vtable {db}.vt1 (ts timestamp, v int from {db}.ct1.v)")
        tdSql.execute(f"create vtable {db}.vt2 (ts timestamp, v int from {db}.vt1.v)")
        tdSql.execute(
            f"create stream {db}.s7 sliding(1s) from {db}.vt2 "
            f"into {db}.res as select ts, v from {db}.vt2"
        )
        tdStream.checkStreamStatus("s7")
        ts = int(time.time() * 1000) // 1000 * 1000
        # Initial insert builds vtbCache for the full vt2 -> vt1 -> ct1 chain.
        tdSql.execute(f"insert into {db}.ct1 values ({ts}, 1)")
        self._waitRows(f"select cast(ts as bigint), v from {db}.res", 1)
        # Drop the middle-chain vtable. ct1 and vt2 remain; the next resolver
        # round must fail to look up vt1.
        tdSql.execute(f"drop vtable {db}.vt1")
        # Wait past the 10s recheck throttle so the next WAL-meta hook re-runs
        # the chain resolver instead of reusing the cached entry.
        time.sleep(11)
        # New row triggers WAL_META_NEW -> streamMaybeRecheckVTableCache ->
        # batched RPC to vt1's vnode -> ref-table miss -> REF_TABLE_NOT_EXIST.
        tdSql.execute(f"insert into {db}.ct1 values ({ts + 12000}, 2)")
        deadline = time.time() + 60
        status, message = (None, None)
        while time.time() < deadline:
            tdSql.query(
                "select status, message from information_schema.ins_streams "
                "where stream_name = 's7'"
            )
            if tdSql.getRows() >= 1:
                status, message = tdSql.getData(0, 0), tdSql.getData(0, 1)
                if status == "Failed":
                    break
            time.sleep(1)
        tdLog.info(f"TC11 final stream state: status={status!r} message={message!r}")
        if status != "Failed":
            tdLog.exit(
                f"TC11 expected status 'Failed', got {status!r} message={message!r}"
            )
        msg_lower = (message or "").lower()
        if "ref table not exist" not in msg_lower and "0x701b" not in msg_lower:
            tdLog.exit(
                f"TC11 expected message contain 'ref table not exist', got {message!r}"
            )
        tdSql.execute(f"drop database {db} force")

    def _tc12_ref_col_not_exist(self):
        tdLog.info(
            "TC12: chain-ref resolver propagates STREAM_VTB_REF_COL_NOT_EXIST"
            " when an intermediate vtable column is dropped at runtime"
        )
        # Spec TC12: REF_COL_NOT_EXIST (0x701C) is raised at
        # vnodeStream.c:4843 when a vtable hop fails to locate the
        # referenced column name in the vtable's schema (cidFound=false)
        # or when the colRef array does not carry an entry for that cid.
        #
        # Setup: vt2.v -> vt1.v -> nt.v (a valid 3-hop chain at DDL
        # time). After the initial insert builds the cache we drop
        # vt1.v via `alter vtable vt1 drop column v`. vt2's colRef
        # still points at vt1.v, but vt1's schema no longer carries it,
        # so the next chain resolve hop=1 must miss the cid and report
        # 0x701C; A propagates; trigger self-fails; mnode surfaces
        # status='Failed'.
        db = "tc12"
        tdSql.execute(f"create database {db} vgroups 1")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {db}.nt (ts timestamp, v int, x int)")
        # vt1 carries `v` (the chain) and `pad` (so drop column is legal).
        tdSql.execute(
            f"create vtable {db}.vt1 ("
            f"ts timestamp, v int from {db}.nt.v, pad int from {db}.nt.x)"
        )
        tdSql.execute(f"create vtable {db}.vt2 (ts timestamp, v int from {db}.vt1.v)")
        tdSql.execute(
            f"create stream {db}.s8 sliding(1s) from {db}.vt2 "
            f"into {db}.res as select ts, v from {db}.vt2"
        )
        tdStream.checkStreamStatus("s8")
        ts = int(time.time() * 1000) // 1000 * 1000
        # Initial insert primes vtbCache with the full vt2 -> vt1 -> nt chain.
        tdSql.execute(f"insert into {db}.nt values ({ts}, 1, 100)")
        self._waitRows(f"select cast(ts as bigint), v from {db}.res", 1)
        # Drop the intermediate vtable column. vt1's schema loses `v`,
        # but vt2.v's colRef entry still references vt1.v.
        tdSql.execute(f"alter vtable {db}.vt1 drop column v")
        # Wait past the 10s recheck throttle so the next resolver round
        # re-runs against the new vt1 schema.
        time.sleep(11)
        tdSql.execute(f"insert into {db}.nt values ({ts + 12000}, 2, 200)")
        deadline = time.time() + 60
        status, message = (None, None)
        observed_failed = False
        while time.time() < deadline:
            tdSql.query(
                "select status, message from information_schema.ins_streams "
                "where stream_name = 's8'"
            )
            rows = tdSql.getRows()
            if rows >= 1:
                status, message = tdSql.getData(0, 0), tdSql.getData(0, 1)
                if status == "Failed":
                    observed_failed = True
                    break
            elif observed_failed:
                break
            time.sleep(0.2)
        tdLog.info(
            f"TC12 final stream state: status={status!r} message={message!r}"
            f" observed_failed={observed_failed}"
        )
        if not observed_failed:
            tdLog.exit(
                f"TC12 expected to observe status 'Failed',"
                f" got {status!r} message={message!r}"
            )
        msg_lower = (message or "").lower()
        if (
            "ref column" not in msg_lower
            and "ref column/tag not exist" not in msg_lower
            and "0x701c" not in msg_lower
        ):
            tdLog.exit(
                f"TC12 expected message contain 'ref column not exist', got {message!r}"
            )
        tdSql.execute(f"drop database {db} force")

    # ------------------------------------------------------------------
    # TC13 — chain ref crossing database boundary.
    # ------------------------------------------------------------------
    def _tc13_cross_db_chain(self):
        tdLog.info("TC13: chain ref across databases")
        # Topology:
        #   db_src.ct0 (physical child) holds the source column.
        #   db_view.vt1.v from db_src.ct0.v   (cross-db hop)
        #   db_view.vt2.v from db_view.vt1.v  (same-db hop)
        # Stream lives in db_view on vt2; resolver must walk a chain that
        # crosses a database boundary.
        db_src = "tc13_src"
        db_view = "tc13_view"
        tdSql.execute(f"drop database if exists {db_src}")
        tdSql.execute(f"drop database if exists {db_view}")
        tdSql.execute(f"create database {db_src} vgroups 1")
        tdSql.execute(f"create database {db_view} vgroups 1")
        tdSql.execute(f"create table {db_src}.ct0 (ts timestamp, v int)")
        tdSql.execute(
            f"create vtable {db_view}.vt1 "
            f"(ts timestamp, v int from {db_src}.ct0.v)"
        )
        tdSql.execute(
            f"create vtable {db_view}.vt2 "
            f"(ts timestamp, v int from {db_view}.vt1.v)"
        )
        tdSql.execute(
            f"create stream {db_view}.s13 sliding(1s) from {db_view}.vt2 "
            f"into {db_view}.res as select ts, v from {db_view}.vt2"
        )
        tdStream.checkStreamStatus("s13")
        ts = int(time.time() * 1000) // 1000 * 1000
        tdSql.execute(
            f"insert into {db_src}.ct0 values "
            f"({ts}, 1) ({ts + 1000}, 2) ({ts + 2000}, 3)"
        )
        expected = [(ts, 1), (ts + 1000, 2), (ts + 2000, 3)]
        self._checkRows(
            "TC13",
            f"select cast(ts as bigint), v from {db_view}.res order by ts",
            expected,
        )
        tdSql.execute(f"drop database {db_view} force")
        tdSql.execute(f"drop database {db_src} force")

    # ------------------------------------------------------------------
    # TC14 — single vt fans one resolve round out to multiple vnodes.
    # Forces streamBatchFanoutDrain to fire >= 2 concurrent per-vg RPCs
    # in a single hop, exercising the shared-semaphore fan-out path.
    # ------------------------------------------------------------------
    def _tc14_single_round_fanout_multi_vnode(self):
        tdLog.info("TC14: single-round fanout to multiple vnodes")
        # Vgroup routing is hash-based; with vgroups=4 and four candidate ct
        # names the placement may still collapse onto one vg. Retry the suffix
        # until ct_a/ct_b/ct_c physically land on >= 2 distinct vgroups, then
        # the single-vt resolver round will fan out to >= 2 vnodes.
        max_attempts = 8
        chosen_suffix = None
        chosen_vgs = None
        for attempt in range(max_attempts):
            suffix = "" if attempt == 0 else f"_a{attempt}"
            db = f"tc14{suffix}"
            tdSql.execute(f"drop database if exists {db}")
            tdSql.execute(f"create database {db} vgroups 4")
            tdSql.execute(f"use {db}")
            tdSql.execute(f"create table {db}.ct_a (ts timestamp, v int)")
            tdSql.execute(f"create table {db}.ct_b (ts timestamp, v int)")
            tdSql.execute(f"create table {db}.ct_c (ts timestamp, v int)")
            # Single vt with three columns each referring to a different ct.
            # Hop-1 batch will hold 3 ref items routed to 3 (or 2) vgroups
            # → streamBatchFanoutDrain fires concurrent RPCs.
            tdSql.execute(
                f"create vtable {db}.vt_fan ("
                f"ts timestamp, "
                f"a int from {db}.ct_a.v, "
                f"b int from {db}.ct_b.v, "
                f"c int from {db}.ct_c.v)"
            )
            vgs = {}
            for tbl in ("ct_a", "ct_b", "ct_c", "vt_fan"):
                tdSql.query(
                    f"select vgroup_id from information_schema.ins_tables "
                    f"where db_name='{db}' and table_name='{tbl}'"
                )
                vgs[tbl] = tdSql.getData(0, 0)
            tdLog.info(f"TC14 attempt {attempt} db={db} vgs={vgs}")
            ct_vgs = {vgs["ct_a"], vgs["ct_b"], vgs["ct_c"]}
            if len(ct_vgs) >= 2:
                chosen_suffix = suffix
                chosen_vgs = vgs
                break
            tdSql.execute(f"drop database {db}")
        if chosen_suffix is None:
            tdLog.exit("TC14: failed to obtain a multi-vnode fanout layout after retries")
        db = f"tc14{chosen_suffix}"
        tdLog.info(f"TC14 chosen db={db} vgs={chosen_vgs}")
        tdSql.execute(
            f"create stream {db}.s14 sliding(1s) from {db}.vt_fan "
            f"into {db}.res as select ts, a, b, c from {db}.vt_fan"
        )
        tdStream.checkStreamStatus("s14")
        ts = int(time.time() * 1000) // 1000 * 1000
        # Aligned timestamps across the three cts so the vt has full rows in
        # each 1s window.
        tdSql.execute(f"insert into {db}.ct_a values ({ts}, 10) ({ts + 1000}, 20)")
        tdSql.execute(f"insert into {db}.ct_b values ({ts}, 11) ({ts + 1000}, 21)")
        tdSql.execute(f"insert into {db}.ct_c values ({ts}, 12) ({ts + 1000}, 22)")
        expected = [
            (ts, 10, 11, 12),
            (ts + 1000, 20, 21, 22),
        ]
        self._checkRows(
            "TC14",
            f"select cast(ts as bigint), a, b, c from {db}.res order by ts",
            expected,
        )
        tdSql.execute(f"drop database {db} force")

    # ------------------------------------------------------------------
    # TC15 — second resolve hop hits the tblRefCache populated by hop 1.
    # Also covers both col and tag reuse in the same cache.
    # ------------------------------------------------------------------
    def _tc15_chain_cache_reuse_col_and_tag(self):
        tdLog.info("TC15: hop2 cache reuse from hop1 (col + tag)")
        # Topology:
        #   stb.v_x / stb.region_x are the physical "source" col & tag.
        #   vct_mid mirrors them at depth 1.
        #   vct_top has FOUR refs:
        #     - col  v_x      <- vct_mid.v_x         (depth 2 chain)
        #     - col  v_y      <- ct0.v_x             (depth 1, terminal in hop1)
        #     - tag  region_x <- vct_mid.region_x    (depth 2 chain)
        #     - tag  region_y <- ct0.region_x        (depth 1, terminal in hop1)
        #
        # Resolver behaviour in one streamCallResolveBatched cycle:
        #   hop1 batch (vct_top's direct refs): 4 items
        #     (vct_mid, v_x, col)        non-terminal -> hop2 continuation
        #     (ct0,     v_x, col)        terminal     -> publish (ct0,v_x)
        #     (vct_mid, region_x, tag)   non-terminal -> hop2 continuation
        #     (ct0,     region_x, tag)   terminal     -> publish (ct0,region_x)
        #   hop2 batch (vct_mid continuations): 2 items
        #     (ct0, v_x, col)            -> tblRefCache HIT (from hop1 publish)
        #     (ct0, region_x, tag)       -> tblRefCache HIT (from hop1 publish)
        #   => hop2 fires zero RPCs; result still has to be correct.
        db = "tc15"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create stable {db}.stb (ts timestamp, v_x int) "
            f"tags (region_x int)"
        )
        tdSql.execute(f"create table {db}.ct0 using {db}.stb tags(77)")
        tdSql.execute(
            f"create stable {db}.vstb (ts timestamp, v_x int, v_y int) "
            f"tags (region_x int, region_y int) virtual 1"
        )
        # Depth-1 mirror of ct0 (col + tag).
        tdSql.execute(
            f"create vtable {db}.vct_mid ("
            f"v_x from {db}.ct0.v_x, "
            f"v_y from {db}.ct0.v_x) "
            f"using {db}.vstb "
            f"tags(region_x from {db}.ct0.region_x, "
            f"region_y from {db}.ct0.region_x)"
        )
        # Depth-2 (via vct_mid) for v_x/region_x, depth-1 (direct to ct0.v_x /
        # ct0.region_x) for v_y/region_y. The depth-1 paths publish cache
        # entries keyed (ct0, v_x) and (ct0, region_x); the depth-2 paths
        # produce hop2 continuations that look up those very same keys.
        tdSql.execute(
            f"create vtable {db}.vct_top ("
            f"v_x from {db}.vct_mid.v_x, "
            f"v_y from {db}.ct0.v_x) "
            f"using {db}.vstb "
            f"tags(region_x from {db}.vct_mid.region_x, "
            f"region_y from {db}.ct0.region_x)"
        )
        tdSql.execute(
            f"create stream {db}.s15 state_window(v_x) "
            f"from {db}.vstb partition by tbname,region_x into {db}.res as "
            f"select _twstart as ts, _twend as w_end, last(v_x) as v_x, count(v_y) as v_y "
            f"from %%trows"
        )
        tdStream.checkStreamStatus("s15")
        ts = int(time.time() * 1000) // 1000 * 1000
        # state_window(v_x) closes a window when v_x transitions to a new
        # value. Rows 1+2 share v_x=10 (one open window); row 3 carries
        # v_x=50, which triggers the prior window's close + emit. Row 3
        # itself opens a new window that stays open (no close expected).
        tdSql.execute(
            f"insert into {db}.ct0 (ts, v_x) values "
            f"({ts}, 10) ({ts + 1500}, 10) ({ts + 2500}, 50)"
        )
        # vct_top.v_y references ct0.v_x (not a distinct ct0.v_y), so the
        # emitted v_y count just mirrors ct0.v_x row count within the window.
        # region_y references ct0.region_x (==77), not a distinct tag, so
        # both vct_mid and vct_top see region_x=77.
        expected = [
            (ts, ts + 1500, 10, 2, "vct_mid", 77),
            (ts, ts + 1500, 10, 2, "vct_top", 77),
        ]
        self._checkRows(
            "TC15",
            f"select cast(ts as bigint), cast(w_end as bigint), v_x, v_y, tag_tbname, region_x "
            f"from {db}.res order by tag_tbname",
            expected,
        )
        tdSql.execute(f"drop database {db} force")
