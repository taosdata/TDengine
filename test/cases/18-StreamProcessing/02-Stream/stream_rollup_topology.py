import time

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamRollupTopology:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.ensureSnode()

    def prepare_topology_data(self):
        tdSql.executes(
            [
                "drop database if exists db_rollup_topology force",
                "create database db_rollup_topology vgroups 1",
                "use db_rollup_topology",
                "create stable meters (ts timestamp, current float) tags (location nchar(64))",
                "create table t1 using meters tags ('A.B.C')",
            ]
        )

    def create_topology_stream(self, name="s_topo", out="rs_topo"):
        tdSql.execute(
            f"create stream {name} interval(1h) sliding(1h) from meters "
            f"rollup by location stream_options(max_delay(3s)) into {out} "
            "tags (loc nchar(256) as %%1) "
            "as select _twstart as ts, count(*) as cnt from %%trows"
        )
        self.wait_stream_running(name, "db_rollup_topology")

    def wait_stream_running(self, stream_name, db_name, timeout=60):
        status = None
        for _ in range(timeout):
            tdSql.query(
                "select status from information_schema.ins_streams "
                f"where db_name='{db_name}' and stream_name='{stream_name}'"
            )
            if tdSql.queryRows > 0:
                status = tdSql.getData(0, 0)
                if status == "Running":
                    tdStream.checkStreamStatus(stream_name)
                    return
            time.sleep(1)
        raise AssertionError(f"{stream_name} did not become Running, status={status}")

    def wait_stream_error(self, stream_name, db_name, timeout=30):
        status = None
        message = ""
        for _ in range(timeout):
            tdSql.query(
                "select status, message from information_schema.ins_streams "
                f"where db_name='{db_name}' and stream_name='{stream_name}'"
            )
            if tdSql.queryRows > 0:
                status = tdSql.getData(0, 0)
                message = tdSql.getData(0, 1)
                if str(status).lower() == "failed":
                    return str(message)
            time.sleep(1)
        raise AssertionError(f"{stream_name} did not fail, status={status}, message={message}")

    def distinct_locs(self, stb):
        tdSql.query(f"select distinct loc from {stb}")
        return sorted(tdSql.getData(i, 0) for i in range(tdSql.queryRows))

    def wait_locs(self, stb, expected, timeout=30):
        last = []
        for _ in range(timeout):
            last = self.distinct_locs(stb)
            if last == expected:
                return
            time.sleep(1)
        raise AssertionError(f"{stb} did not reach locs {expected}, last={last}")

    def test_rollup_topology_add(self):
        """Rollup topology adds groups for new child table paths.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_topology_data()
        self.create_topology_stream()
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        self.wait_locs("rs_topo", ["A", "A.B", "A.B.C"])

        tdSql.execute("create table t2 using meters tags ('A.B.D')")
        tdSql.execute("insert into t2 values (1700003600000, 2.0)")
        self.wait_locs("rs_topo", ["A", "A.B", "A.B.C", "A.B.D"])

    def test_rollup_topology_drop_orphan(self):
        """Rollup topology keeps historical orphan output after dropping a child table.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_topology_data()
        self.create_topology_stream()
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        self.wait_locs("rs_topo", ["A", "A.B", "A.B.C"])
        tdSql.execute("drop table t1")

        tdSql.execute("create table t3 using meters tags ('A.B.D')")
        tdSql.execute("insert into t3 values (1700003600000, 1.0)")
        self.wait_locs("rs_topo", ["A", "A.B", "A.B.C", "A.B.D"])
        self.wait_scalar("select count(*) from rs_topo where loc='A.B.C' and ts=1700003600000", 0)

    def test_rollup_topology_drop_shared_ancestor(self):
        """Rollup topology recalculates shared ancestors after dropping a child table.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_topology_data()
        tdSql.execute("create table t2 using meters tags ('A.B.D')")
        self.create_topology_stream()
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        tdSql.execute("insert into t2 values (1700000000000, 2.0)")
        self.wait_locs("rs_topo", ["A", "A.B", "A.B.C", "A.B.D"])

        tdSql.execute("drop table t1")
        tdSql.execute("insert into t2 values (1700003600000, 3.0)")
        self.wait_scalar("select cnt from rs_topo where loc='A' order by ts desc limit 1", 1)
        self.wait_scalar("select cnt from rs_topo where loc='A.B' order by ts desc limit 1", 1)

    def test_rollup_topology_add_illegal_path(self):
        """Rollup topology reports failure when a new child table has an illegal path.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_topology_data()
        self.create_topology_stream()
        tdSql.execute("create table t_bad using meters tags ('A..B')")
        tdSql.execute("insert into t_bad values (1700000000000, 1.0)")
        message = self.wait_stream_error("s_topo", "db_rollup_topology")
        assert "rollup" in message.lower()
        assert "path" in message.lower()


    def prepare_vstb_topology_data(self):
        tdSql.executes(
            [
                "drop database if exists db_rollup_vstb_topology force",
                "create database db_rollup_vstb_topology vgroups 1",
                "use db_rollup_vstb_topology",
                "create table src1 (ts timestamp, c1 int)",
                "create table src2 (ts timestamp, c1 int)",
                "create stable vstb (ts timestamp, c1 int) "
                "tags(path varchar(64), region varchar(20)) virtual 1",
                "create vtable vctb1 (src1.c1) using vstb tags('A.B.C', 'north')",
            ]
        )

    def create_vstb_topology_stream(self):
        tdSql.execute(
            "create stream s_vtopo interval(1h) sliding(1h) from vstb "
            "rollup by path stream_options(max_delay(3s)) into rs_vtopo "
            "tags (gid varchar(256) as %%1) "
            "as select _twstart as ts, count(*) as cnt from %%trows"
        )
        self.wait_stream_ready("s_vtopo", "db_rollup_vstb_topology")

    def wait_stream_ready(self, stream_name, db_name, timeout=90):
        status = None
        message = ""
        for _ in range(timeout):
            tdSql.query(
                "select status, message from information_schema.ins_streams "
                f"where db_name='{db_name}' and stream_name='{stream_name}'"
            )
            if tdSql.queryRows > 0:
                status = tdSql.getData(0, 0)
                message = tdSql.getData(0, 1)
                if status in ("Running", "Ready"):
                    tdStream.checkStreamStatus(stream_name)
                    return
                if str(status).lower() == "failed":
                    raise AssertionError(f"{stream_name} failed: {message}")
            time.sleep(1)
        raise AssertionError(f"{stream_name} did not become ready, status={status}, message={message}")

    def query_gids(self):
        tdSql.query("select distinct gid from rs_vtopo")
        return sorted(tdSql.getData(i, 0) for i in range(tdSql.queryRows))

    def wait_gids(self, expected, timeout=60):
        last = []
        for _ in range(timeout):
            last = self.query_gids()
            if last == expected:
                return
            time.sleep(1)
        raise AssertionError(f"rs_vtopo did not reach gids {expected}, last={last}")

    def wait_latest_count(self, gid, expected, timeout=60):
        last = None
        for _ in range(timeout):
            tdSql.query(f"select cnt from rs_vtopo where gid='{gid}' order by ts desc limit 1")
            if tdSql.queryRows > 0:
                last = tdSql.getData(0, 0)
                if last == expected:
                    return
            time.sleep(1)
        raise AssertionError(f"latest count for gid={gid} did not reach {expected}, last={last}")

    def test_rollup_vstb_initial_expand(self):
        """Initial virtual stable rollup expands into all path groups.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """
        self.prepare_vstb_topology_data()
        self.create_vstb_topology_stream()
        tdSql.execute("insert into src1 values (1700000000000, 1)")
        self.wait_gids(["A", "A.B", "A.B.C"])
        self.wait_latest_count("A", 1)
        self.wait_latest_count("A.B", 1)
        self.wait_latest_count("A.B.C", 1)

    def test_rollup_vstb_add_triggers_restart(self):
        """Adding a vtable redeploys virtual stable rollup and adds groups.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """
        self.prepare_vstb_topology_data()
        self.create_vstb_topology_stream()
        tdSql.execute("insert into src1 values (1700000000000, 1)")
        self.wait_gids(["A", "A.B", "A.B.C"])
        tdSql.execute("create vtable vctb2 (src2.c1) using vstb tags('A.B.D', 'north')")
        self.wait_stream_ready("s_vtopo", "db_rollup_vstb_topology")
        tdSql.execute("insert into src2 values (1700003600000, 2)")
        self.wait_gids(["A", "A.B", "A.B.C", "A.B.D"])
        self.wait_latest_count("A", 1)
        self.wait_latest_count("A.B", 1)
        self.wait_latest_count("A.B.D", 1)

    def test_rollup_vstb_retire_triggers_restart(self):
        """Dropping a vtable redeploys virtual stable rollup and keeps historical output.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """
        self.prepare_vstb_topology_data()
        tdSql.execute("create vtable vctb2 (src2.c1) using vstb tags('A.B.D', 'north')")
        self.create_vstb_topology_stream()
        tdSql.execute("insert into src1 values (1700000000000, 1)")
        tdSql.execute("insert into src2 values (1700000000000, 2)")
        self.wait_gids(["A", "A.B", "A.B.C", "A.B.D"])
        tdSql.execute("drop table vctb2")
        self.wait_stream_ready("s_vtopo", "db_rollup_vstb_topology")
        assert "A.B.D" in self.query_gids()
        tdSql.execute("insert into src1 values (1700007200000, 3)")
        self.wait_latest_count("A", 1)
        self.wait_latest_count("A.B", 1)
        self.wait_latest_count("A.B.C", 1)

    def test_rollup_vstb_tbgids_multi(self):
        """A virtual table row contributes to parent and leaf rollup groups.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """
        self.prepare_vstb_topology_data()
        self.create_vstb_topology_stream()
        for i in range(3):
            tdSql.execute(f"insert into src1 values ({1700000000000 + i * 100}, 1)")
        self.wait_latest_count("A", 3)
        self.wait_latest_count("A.B", 3)
        self.wait_latest_count("A.B.C", 3)


    def prepare_subquery_data(self):
        tdSql.executes(
            [
                "drop database if exists db_rollup_subquery force",
                "create database db_rollup_subquery vgroups 1",
                "use db_rollup_subquery",
                "create stable meters (ts timestamp, current float) tags (location nchar(64))",
                "create table t1 using meters tags ('A.B.C')",
                "create table t2 using meters tags ('A.B.D')",
                "create stable other_stb (ts timestamp, val int) tags (location nchar(64))",
                "create table o1 using other_stb tags ('A.B.C')",
                "create table o2 using other_stb tags ('A.B.D')",
            ]
        )

    def wait_scalar(self, sql, expected, timeout=60):
        last = None
        for _ in range(timeout):
            tdSql.query(sql)
            if tdSql.queryRows > 0:
                last = tdSql.getData(0, 0)
                if last == expected:
                    return
            time.sleep(1)
        raise AssertionError(f"{sql} did not return {expected}, last={last}")

    def wait_distinct_locs(self, table, expected, timeout=60):
        last = []
        for _ in range(timeout):
            tdSql.query(f"select distinct loc from {table}")
            last = sorted(tdSql.getData(i, 0) for i in range(tdSql.queryRows))
            if last == expected:
                return
            time.sleep(1)
        raise AssertionError(f"{table} did not reach locs {expected}, last={last}")

    def create_external_subquery_stream(self):
        tdSql.execute(
            "create stream s_sub interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_sub "
            "tags (loc nchar(256) as %%1) "
            "as select _twstart as ts, sum(val) as total from other_stb "
            "where (location = %%1 or (location >= concat(%%1, '.') and location < concat(%%1, '/'))) "
            "and _c0 >= _twstart and _c0 < _twend"
        )
        self.wait_stream_ready("s_sub", "db_rollup_subquery")

    def write_external_rows(self):
        tdSql.execute("insert into o1 values (1700000000500, 10)")
        tdSql.execute("insert into o2 values (1700000000500, 20)")

    def trigger_both_paths(self):
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        tdSql.execute("insert into t2 values (1700000000000, 2.0)")

    def test_rollup_subquery_external_stb(self):
        """Rollup stream query can aggregate an external stable by rollup path.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """
        self.prepare_subquery_data()
        self.create_external_subquery_stream()
        self.write_external_rows()
        self.trigger_both_paths()

        self.wait_distinct_locs("rs_sub", ["A", "A.B", "A.B.C", "A.B.D"])
        self.wait_scalar("select total from rs_sub where loc='A'", 30)
        self.wait_scalar("select total from rs_sub where loc='A.B'", 30)
        self.wait_scalar("select total from rs_sub where loc='A.B.C'", 10)
        self.wait_scalar("select total from rs_sub where loc='A.B.D'", 20)

    def test_rollup_subquery_like_path_index(self):
        """Rollup path range predicates use tag index filtering.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """
        self.prepare_subquery_data()
        tdSql.query(
            "explain verbose true select sum(val) from other_stb "
            "where location = 'A.B' or (location >= concat('A.B', '.') and location < concat('A.B', '/'))"
        )
        plan_text = "\n".join(str(tdSql.getData(i, 0)) for i in range(tdSql.queryRows))
        assert "Tag Index Filter" in plan_text

    def test_rollup_subquery_no_trows(self):
        """Rollup stream works when query does not read %%trows directly.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """
        self.prepare_subquery_data()
        tdSql.execute(
            "create stream s_sub2 interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_sub2 "
            "tags (loc nchar(256) as %%1) "
            "as select _twstart as ts, %%1 as path, count(*) as cnt from meters "
            "where (location = %%1 or (location >= concat(%%1, '.') and location < concat(%%1, '/'))) "
            "and _c0 >= _twstart and _c0 < _twend"
        )
        self.wait_stream_ready("s_sub2", "db_rollup_subquery")
        self.trigger_both_paths()
        self.wait_distinct_locs("rs_sub2", ["A", "A.B", "A.B.C", "A.B.D"])
        self.wait_scalar("select cnt from rs_sub2 where loc='A'", 2)
        self.wait_scalar("select cnt from rs_sub2 where loc='A.B'", 2)
        self.wait_scalar("select cnt from rs_sub2 where loc='A.B.C'", 1)
        self.wait_scalar("select cnt from rs_sub2 where loc='A.B.D'", 1)
