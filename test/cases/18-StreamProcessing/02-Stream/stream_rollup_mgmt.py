import time

from new_test_framework.utils import clusterComCheck, sc, tdLog, tdSql, tdStream


class TestStreamRollupMgmt:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.ensureSnode()

    def prepare_mgmt_data(self):
        tdSql.executes(
            [
                "drop database if exists db_rollup_mgmt force",
                "create database db_rollup_mgmt vgroups 1",
                "use db_rollup_mgmt",
                "create stable meters (ts timestamp, current float) tags (location nchar(64))",
                "create table t1 using meters tags ('A.B.C')",
            ]
        )

    def wait_stream_status(self, stream_name, db_name, expected, timeout=60):
        status = None
        for _ in range(timeout):
            tdSql.query(
                "select status from information_schema.ins_streams "
                f"where db_name='{db_name}' and stream_name='{stream_name}'"
            )
            if tdSql.queryRows > 0:
                status = tdSql.getData(0, 0)
                if status == expected:
                    if expected == "Running":
                        tdStream.checkStreamStatus(stream_name)
                    return
            time.sleep(1)
        raise AssertionError(f"{stream_name} did not become {expected}, status={status}")

    def create_mgmt_rollup_stream(self, stream, out):
        tdSql.execute(
            f"create stream {stream} interval(1h) sliding(1h) from meters "
            f"rollup by location stream_options(max_delay(3s)) into {out} "
            "tags (loc nchar(256) as %%1) "
            "as select _twstart as ts, count(*) as cnt from %%trows"
        )
        self.wait_stream_status(stream, "db_rollup_mgmt", "Running")

    def test_rollup_stop_start(self):
        """Rollup stream keeps processing after stop and start.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_mgmt_data()
        self.create_mgmt_rollup_stream("s_pr", "rs_pr")
        tdSql.execute("stop stream s_pr")
        self.wait_stream_status("s_pr", "db_rollup_mgmt", "Stopped")
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")

        tdSql.execute("start stream s_pr")
        self.wait_stream_status("s_pr", "db_rollup_mgmt", "Running")
        self.wait_distinct_locs("rs_pr", ["A", "A.B", "A.B.C"])

    def test_rollup_drop_keeps_output(self):
        """Dropping a rollup stream keeps the output stable.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_mgmt_data()
        self.create_mgmt_rollup_stream("s_dk", "rs_dk")
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        self.wait_scalar("select count(*) from rs_dk", 3)

        tdSql.execute("drop stream s_dk")
        tdSql.query("show stables like 'rs_dk'")
        assert tdSql.queryRows == 1
        tdSql.query("select count(*) from rs_dk")
        assert tdSql.getData(0, 0) == 3

    def test_rollup_show_status_single_logical_stream(self):
        """Rollup stream metadata shows one logical stream.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_mgmt_data()
        self.create_mgmt_rollup_stream("s_one", "rs_one")
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        self.wait_distinct_locs("rs_one", ["A", "A.B", "A.B.C"])

        tdSql.query(
            "select count(*) from information_schema.ins_streams "
            "where db_name='db_rollup_mgmt' and stream_name='s_one'"
        )
        assert tdSql.getData(0, 0) == 1
        tdSql.query("show streams")
        assert sum(1 for row in tdSql.queryResult if row[0] == "s_one") == 1


    def prepare_error_data(self):
        tdSql.executes(
            [
                "drop database if exists db_rollup_error force",
                "create database db_rollup_error vgroups 1",
                "use db_rollup_error",
                "create stable meters (ts timestamp, current float) tags(location varchar(64))",
            ]
        )

    def create_error_stream(self, stream="s_error", out="rs_error"):
        tdSql.execute(
            f"create stream {stream} interval(1h) sliding(1h) from meters "
            f"rollup by location stream_options(max_delay(3s)) into {out} "
            "tags (loc varchar(64) as %%1, leaf varchar(64) as cast(%%rollup_tag as varchar(64))) "
            "as select _twstart, count(*) as cnt from %%trows"
        )

    def wait_error_stream_status(self, stream, expected, timeout=60):
        last = None
        for _ in range(timeout):
            tdSql.query(
                "select status, message from information_schema.ins_streams "
                f"where db_name='db_rollup_error' and stream_name='{stream}'"
            )
            if tdSql.queryRows == 1:
                last = (tdSql.getData(0, 0), tdSql.getData(0, 1))
                if str(last[0]).lower() == expected:
                    return last
            time.sleep(1)
        raise AssertionError(f"{stream} did not reach {expected}, last={last}")

    def get_show_stream_row(self, stream):
        tdSql.query("show streams")
        for row in tdSql.queryResult:
            if row[0] == stream:
                return row
        raise AssertionError(f"{stream} not found in SHOW STREAMS: {tdSql.queryResult}")

    def make_illegal_stream(self, stream="s_error", out="rs_error", path="A..B"):
        tdSql.execute(f"create table t_bad using meters tags ('{path}')")
        self.create_error_stream(stream, out)
        tdSql.execute("insert into t_bad values (1700000000000, 1.0)")
        return self.wait_error_stream_status(stream, "failed")

    def restart_stream(self, stream="s_error"):
        tdSql.execute(f"stop stream {stream}")
        self.wait_error_stream_status(stream, "stopped")
        tdSql.execute(f"start stream {stream}")

    def test_rollup_error_terminal(self):
        """Illegal rollup paths put the stream into failed state.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_error_data()
        status, message = self.make_illegal_stream(path="A..B")
        assert str(status).lower() == "failed"
        assert "Stream rollup tag path is illegal" in str(message)
        assert "invalid tag value" in str(message)
        assert "uid=" in str(message)
        assert "tag='A..B'" in str(message)

        row = self.get_show_stream_row("s_error")
        assert str(row[1]).lower() == "failed"
        assert "Stream rollup tag path is illegal" in str(row[2])
        assert "invalid tag value" in str(row[2])

    def test_rollup_error_manual_start_recurs(self):
        """Restarting a stream with illegal rollup paths fails again.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_error_data()
        self.make_illegal_stream(path="A..B")
        self.restart_stream("s_error")
        status, message = self.wait_error_stream_status("s_error", "failed")
        assert str(status).lower() == "failed"
        assert "Stream rollup tag path is illegal" in str(message)
        assert "invalid tag value" in str(message)
        assert "tag='A..B'" in str(message)

    def test_rollup_error_recovers_after_cleanup_and_manual_start(self):
        """Rollup stream recovers after removing illegal paths and restarting.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_error_data()
        self.make_illegal_stream(path="A..B")
        tdSql.execute("drop table t_bad")
        self.restart_stream("s_error")
        self.wait_error_stream_status("s_error", "running")

    def test_show_streams_rollup_visibility(self):
        """SHOW STREAMS exposes rollup stream metadata.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_error_data()
        self.create_error_stream("s_show", "rs_show")
        self.wait_error_stream_status("s_show", "running")

        tdSql.query(
            "select sql from information_schema.ins_streams "
            "where db_name='db_rollup_error' and stream_name='s_show'"
        )
        assert "rollup by location" in str(tdSql.getData(0, 0)).lower()
        row = self.get_show_stream_row("s_show")
        assert row[0] == "s_show"

    def test_show_create_stream_rollup(self):
        """SHOW CREATE STREAM preserves the ROLLUP BY clause.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_error_data()
        self.create_error_stream("s_create", "rs_create")
        tdSql.query("show create stream s_create")
        tdSql.checkRows(1)
        assert "rollup by location" in str(tdSql.queryResult[0][1]).lower()

    def test_rollup_notify_placeholders_render_path(self):
        """Rollup placeholders render path values with notify enabled.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_error_data()
        tdSql.execute("create table t_good using meters tags ('A.B.C')")
        tdSql.execute(
            "create stream s_notify interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) "
            "notify('ws://localhost:12345/rollup_notify') on(window_close) "
            "into rs_notify "
            "tags (loc varchar(64) as %%1, leaf varchar(64) as cast(%%rollup_tag as varchar(64))) "
            "as select _twstart, count(*) as cnt from %%trows"
        )
        self.wait_error_stream_status("s_notify", "running")
        tdSql.execute("insert into t_good values (1700000000000, 1.0)")

        for _ in range(20):
            tdSql.query("select loc, leaf from rs_notify")
            got = {tdSql.getData(i, 0): tdSql.getData(i, 1) for i in range(tdSql.queryRows)}
            if got == {"A": "A", "A.B": "B", "A.B.C": "C"}:
                return
            time.sleep(1)
        raise AssertionError(f"rollup path placeholders did not render as expected: {got}")


    def prepare_recover_data(self):
        tdSql.executes(
            [
                "drop database if exists db_rollup_recover force",
                "create database db_rollup_recover vgroups 1",
                "use db_rollup_recover",
                "create stable meters (ts timestamp, current float) tags (location nchar(64))",
                "create table t1 using meters tags ('A.B.C')",
            ]
        )

    def wait_scalar(self, sql, expected, timeout=30):
        last = None
        for _ in range(timeout):
            tdSql.query(sql)
            if tdSql.queryRows > 0:
                last = tdSql.getData(0, 0)
                if last == expected:
                    return
            time.sleep(1)
        raise AssertionError(f"{sql} did not return {expected}, last={last}")

    def wait_distinct_locs(self, table, expected, timeout=30):
        last = []
        for _ in range(timeout):
            last = sorted(self.query_column(f"select distinct loc from {table}"))
            if last == expected:
                return
            time.sleep(1)
        raise AssertionError(f"{table} did not reach locs {expected}, last={last}")

    def query_column(self, sql):
        tdSql.query(sql)
        return [tdSql.getData(i, 0) for i in range(tdSql.queryRows)]

    def create_recover_rollup_stream(self, stream, out, options="stream_options(max_delay(3s))"):
        tdSql.execute(
            f"create stream {stream} interval(1h) sliding(1h) from meters "
            f"rollup by location {options} into {out} "
            "tags (loc nchar(256) as %%1) "
            "as select _twstart as ts, count(*) as cnt from %%trows"
        )
        self.wait_stream_status(stream, "db_rollup_recover", "Running")

    def test_rollup_restart_rebuild_groups(self):
        """Rollup groups are rebuilt after dnode restart.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_recover_data()
        self.create_recover_rollup_stream("s_rc", "rs_rc")
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        self.wait_distinct_locs("rs_rc", ["A", "A.B", "A.B.C"])
        before = sorted(self.query_column("select distinct loc from rs_rc"))

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)
        tdSql.execute("use db_rollup_recover")
        self.wait_stream_status("s_rc", "db_rollup_recover", "Running")

        tdSql.execute("insert into t1 values (1700003600000, 2.0)")
        self.wait_scalar("select count(*) from rs_rc where loc='A'", 2)
        after = sorted(self.query_column("select distinct loc from rs_rc"))
        assert before == after

    def test_rollup_fill_history(self):
        """Rollup fill history populates ancestor and leaf groups.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_recover_data()
        for i in range(5):
            tdSql.execute(f"insert into t1 values ({1700000000000 + i * 100}, 1.0)")
        self.create_recover_rollup_stream(
            "s_fh", "rs_fh", "stream_options(max_delay(3s)|fill_history('1970-01-01 00:00:00'))"
        )

        self.wait_distinct_locs("rs_fh", ["A", "A.B", "A.B.C"], timeout=60)
        self.wait_scalar("select cnt from rs_fh where loc='A.B.C'", 5, timeout=60)
        self.wait_scalar("select cnt from rs_fh where loc='A'", 5, timeout=60)

    def test_rollup_fill_history_no_trows_fetches_table_data(self):
        """Rollup fill history works when query fetches source table data directly.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_recover_data()
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        tdSql.execute("insert into t1 values (1700000001000, 2.0)")
        tdSql.execute(
            "create stream s_fh_fetch interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)|fill_history('1970-01-01 00:00:00')) into rs_fh_fetch "
            "tags (loc nchar(256) as %%1) "
            "as select _twstart as ts, count(*) as cnt from meters "
            "where (location = %%1 or (location >= concat(%%1, '.') and location < concat(%%1, '/'))) "
            "and _c0 >= _twstart and _c0 < _twend"
        )
        self.wait_stream_status("s_fh_fetch", "db_rollup_recover", "Running")
        self.wait_distinct_locs("rs_fh_fetch", ["A", "A.B", "A.B.C"], timeout=60)
        self.wait_scalar("select cnt from rs_fh_fetch where loc='A'", 2, timeout=60)
        self.wait_scalar("select cnt from rs_fh_fetch where loc='A.B'", 2, timeout=60)
        self.wait_scalar("select cnt from rs_fh_fetch where loc='A.B.C'", 2, timeout=60)

    def test_rollup_delete_recalc(self):
        """Rollup delete recalculation matches partition stream recalculation.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_recover_data()
        tdSql.execute(
            "create stream s_dr interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)|delete_recalc) into rs_dr "
            "tags (loc nchar(256) as %%1) "
            "as select _twstart as ts, count(*) as cnt from %%trows"
        )
        self.wait_stream_status("s_dr", "db_rollup_recover", "Running")
        tdSql.execute(
            "create stream s_part interval(1h) sliding(1h) from meters "
            "partition by location stream_options(max_delay(3s)|delete_recalc) into rs_part "
            "tags (loc nchar(256) as location) "
            "as select _twstart as ts, count(*) as cnt from %%trows"
        )
        self.wait_stream_status("s_part", "db_rollup_recover", "Running")
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        tdSql.execute("insert into t1 values (1700000010000, 2.0)")
        self.wait_scalar("select cnt from rs_dr where loc='A.B.C'", 2)
        self.wait_scalar("select cnt from rs_part where loc='A.B.C'", 2)

        tdSql.execute("delete from t1 where ts=1700000010000")
        time.sleep(5)
        tdSql.query("select cnt from rs_part where loc='A.B.C'")
        expected = tdSql.getData(0, 0)
        self.wait_scalar("select cnt from rs_dr where loc='A.B.C'", expected)
        self.wait_scalar("select cnt from rs_dr where loc='A.B'", expected)
        self.wait_scalar("select cnt from rs_dr where loc='A'", expected)

    def test_rollup_delete_recalc_no_trows_fetches_table_data(self):
        """Rollup delete recalculation works when query fetches source table data directly.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_recover_data()
        tdSql.execute(
            "create stream s_dr_fetch interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)|delete_recalc) into rs_dr_fetch "
            "tags (loc nchar(256) as %%1) "
            "as select _twstart as ts, count(*) as cnt from meters "
            "where (location = %%1 or (location >= concat(%%1, '.') and location < concat(%%1, '/'))) "
            "and _c0 >= _twstart and _c0 < _twend"
        )
        self.wait_stream_status("s_dr_fetch", "db_rollup_recover", "Running")
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        tdSql.execute("insert into t1 values (1700000010000, 2.0)")
        self.wait_distinct_locs("rs_dr_fetch", ["A", "A.B", "A.B.C"])
        self.wait_scalar("select cnt from rs_dr_fetch where loc='A'", 2)
        self.wait_scalar("select cnt from rs_dr_fetch where loc='A.B'", 2)
        self.wait_scalar("select cnt from rs_dr_fetch where loc='A.B.C'", 2)

        tdSql.execute("delete from t1 where ts=1700000010000")
        self.wait_scalar("select cnt from rs_dr_fetch where loc='A'", 1)
        self.wait_scalar("select cnt from rs_dr_fetch where loc='A.B'", 1)
        self.wait_scalar("select cnt from rs_dr_fetch where loc='A.B.C'", 1)
