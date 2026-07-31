import time

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamRollupExpand:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.ensureSnode()

    def prepare_expand_data(self):
        tdSql.executes(
            [
                "drop database if exists db_rollup_expand force",
                "create database db_rollup_expand vgroups 1",
                "use db_rollup_expand",
                "create stable meters (ts timestamp, current float) tags (location nchar(64))",
            ]
        )

    def wait_subtables(self, stb, expected, timeout=20):
        for _ in range(timeout):
            tdSql.query(f"select count(*) from (select distinct tbname from {stb})")
            if tdSql.getData(0, 0) >= expected:
                return
            time.sleep(1)
        raise AssertionError(f"{stb} did not reach {expected} subtables")

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

    def query_column(self, sql):
        tdSql.query(sql)
        return [tdSql.getData(i, 0) for i in range(tdSql.queryRows)]

    def test_rollup_expand_single_level(self):
        """Rollup expands a single-level path.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_expand_data()
        tdSql.execute("create table t1 using meters tags ('A')")
        tdSql.execute(
            "create stream s_e1 interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_e1 "
            "tags (loc nchar(256) as %%1) "
            "as select _twstart, count(*) from %%trows"
        )
        self.wait_stream_running("s_e1", "db_rollup_expand")
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        self.wait_subtables("rs_e1", 1)
        assert self.query_column("select distinct loc from rs_e1") == ["A"]

    def test_rollup_expand_three_levels(self):
        """Rollup expands a three-level path.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_expand_data()
        tdSql.execute("create table t1 using meters tags ('北京.朝阳.望京')")
        tdSql.execute(
            "create stream s_e2 interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_e2 "
            "tags (loc nchar(256) as %%1, leaf nchar(64) as %%rollup_tag) "
            "as select _twstart, avg(current) as avg_current from %%trows"
        )
        self.wait_stream_running("s_e2", "db_rollup_expand")
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        self.wait_subtables("rs_e2", 3)
        got = self.query_column("select distinct loc from rs_e2 order by loc")
        assert sorted(got) == ["北京", "北京.朝阳", "北京.朝阳.望京"]
        tdSql.query("select leaf from rs_e2 where loc='北京.朝阳.望京'")
        assert tdSql.getData(0, 0) == "望京"

    def test_rollup_expand_shared_ancestor(self):
        """Rollup shares ancestor groups for sibling leaf paths.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_expand_data()
        tdSql.execute("create table t1 using meters tags ('A.B.C')")
        tdSql.execute("create table t2 using meters tags ('A.B.D')")
        tdSql.execute(
            "create stream s_e3 interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_e3 "
            "tags (loc nchar(256) as %%1) "
            "as select _twstart, count(*) as cnt from %%trows"
        )
        self.wait_stream_running("s_e3", "db_rollup_expand")
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        tdSql.execute("insert into t2 values (1700000000000, 2.0)")
        self.wait_subtables("rs_e3", 4)
        got = self.query_column("select distinct loc from rs_e3 order by loc")
        assert sorted(got) == ["A", "A.B", "A.B.C", "A.B.D"]
        tdSql.query("select cnt from rs_e3 where loc='A'")
        assert tdSql.getData(0, 0) == 2
        tdSql.query("select cnt from rs_e3 where loc='A.B'")
        assert tdSql.getData(0, 0) == 2
        tdSql.query("select cnt from rs_e3 where loc='A.B.C'")
        assert tdSql.getData(0, 0) == 1

    def test_rollup_expand_middle_node_has_data(self):
        """Rollup counts middle-node and leaf-node source data.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_expand_data()
        tdSql.execute("create table t_mid using meters tags ('A.B')")
        tdSql.execute("create table t_leaf using meters tags ('A.B.C')")
        tdSql.execute(
            "create stream s_e4 interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_e4 "
            "tags (loc nchar(256) as %%1) "
            "as select _twstart, count(*) as cnt from %%trows"
        )
        self.wait_stream_running("s_e4", "db_rollup_expand")
        tdSql.execute("insert into t_mid values (1700000000000, 1.0)")
        tdSql.execute("insert into t_leaf values (1700000000000, 2.0)")
        self.wait_subtables("rs_e4", 3)
        tdSql.query("select cnt from rs_e4 where loc='A'")
        assert tdSql.getData(0, 0) == 2
        tdSql.query("select cnt from rs_e4 where loc='A.B'")
        assert tdSql.getData(0, 0) == 2
        tdSql.query("select cnt from rs_e4 where loc='A.B.C'")
        assert tdSql.getData(0, 0) == 1

    def test_rollup_expand_unique_prefix_count(self):
        """Rollup creates one group per unique path prefix.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_expand_data()
        paths = [
            "A.B.C.D",
            "A.B.C.E",
            "A.B.F",
            "A.B.G",
            "A.H.I.J",
            "A.H.K",
            "A.L",
            "M.N.O",
            "M.N.P",
            "M.Q",
        ]
        expected = set()
        for i, path in enumerate(paths):
            tdSql.execute(f"create table t{i} using meters tags ('{path}')")
            segments = path.split(".")
            for j in range(len(segments)):
                expected.add(".".join(segments[: j + 1]))

        tdSql.execute(
            "create stream s_e5 interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_e5 "
            "tags (loc nchar(256) as %%1) "
            "as select _twstart, count(*) from %%trows"
        )
        self.wait_stream_running("s_e5", "db_rollup_expand")
        for i in range(len(paths)):
            tdSql.execute(f"insert into t{i} values (1700000000000, 1.0)")
        self.wait_subtables("rs_e5", len(expected))
        got = self.query_column("select distinct loc from rs_e5")
        assert sorted(got) == sorted(expected)

    def test_rollup_trows_dataset_per_group(self):
        """Rollup %%trows contains rows for each rollup group.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_expand_data()
        tdSql.execute("create table t1 using meters tags ('A.B.C')")
        tdSql.execute("create table t2 using meters tags ('A.B.D')")
        tdSql.execute(
            "create stream s_e6 interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_e6 "
            "tags (loc nchar(256) as %%1) "
            "as select _twstart, count(*) as cnt from %%trows"
        )
        self.wait_stream_running("s_e6", "db_rollup_expand")
        for i in range(3):
            tdSql.execute(f"insert into t1 values ({1700000000000 + i * 100}, 1.0)")
        for i in range(2):
            tdSql.execute(f"insert into t2 values ({1700000000000 + i * 100}, 2.0)")

        self.wait_subtables("rs_e6", 4)
        tdSql.query("select cnt from rs_e6 where loc='A'")
        assert tdSql.getData(0, 0) == 5
        tdSql.query("select cnt from rs_e6 where loc='A.B'")
        assert tdSql.getData(0, 0) == 5
        tdSql.query("select cnt from rs_e6 where loc='A.B.C'")
        assert tdSql.getData(0, 0) == 3
        tdSql.query("select cnt from rs_e6 where loc='A.B.D'")
        assert tdSql.getData(0, 0) == 2


    def prepare_illegal_path_data(self):
        tdSql.executes(
            [
                "drop database if exists db_rollup_illegal force",
                "create database db_rollup_illegal vgroups 1",
                "use db_rollup_illegal",
                "create stable meters (ts timestamp, current float) tags(location varchar(64))",
            ]
        )

    def create_illegal_path_stream(self, stream="s_illegal", out="rs_illegal"):
        tdSql.execute(
            f"create stream {stream} interval(1h) sliding(1h) from meters "
            f"rollup by location stream_options(max_delay(3s)) into {out} "
            "tags (loc varchar(64) as %%1) "
            "as select _twstart, count(*) as cnt from %%trows"
        )

    def wait_stream_status(self, stream, db_name, expected, timeout=60):
        last = None
        for _ in range(timeout):
            tdSql.query(
                "select status, message from information_schema.ins_streams "
                f"where db_name='{db_name}' and stream_name='{stream}'"
            )
            if tdSql.queryRows == 1:
                last = (tdSql.getData(0, 0), tdSql.getData(0, 1))
                if str(last[0]).lower() == expected:
                    return last
            time.sleep(1)
        raise AssertionError(f"{stream} did not reach {expected}, last={last}")

    def query_scalar(self, sql):
        tdSql.query(sql)
        return tdSql.getData(0, 0)

    def test_empty_and_null_rollup_tags_are_skipped(self):
        """Rollup skips empty and null tag paths.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_illegal_path_data()
        tdSql.execute("create table t_empty using meters tags ('')")
        tdSql.execute("create table t_null using meters tags (NULL)")
        self.create_illegal_path_stream("s_skip", "rs_skip")
        self.wait_stream_status("s_skip", "db_rollup_illegal", "running")

        tdSql.execute("insert into t_empty values (1700000000000, 1.0)")
        tdSql.execute("insert into t_null values (1700000000000, 2.0)")
        time.sleep(5)
        assert self.query_scalar("select count(*) from rs_skip") == 0

    def check_illegal_path(self, path, tag_expr=None):
        table_name = "t_bad"
        tag_expr = tag_expr or f"'{path}'"
        self.prepare_illegal_path_data()
        tdSql.execute(f"create table {table_name} using meters tags ({tag_expr})")
        self.create_illegal_path_stream()
        tdSql.execute(f"insert into {table_name} values (1700000000000, 1.0)")
        status, message = self.wait_stream_status("s_illegal", "db_rollup_illegal", "failed")
        message = str(message)
        assert str(status).lower() == "failed"
        assert "Stream rollup tag path is illegal" in message
        assert "invalid tag value" in message
        assert "uid=" in message
        assert "tag=" in message

    def test_illegal_leading_dot(self):
        """Rollup rejects paths with a leading dot.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.check_illegal_path(".A.B")

    def test_illegal_trailing_dot(self):
        """Rollup rejects paths with a trailing dot.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.check_illegal_path("A.B.")

    def test_illegal_consecutive_dot(self):
        """Rollup rejects paths with consecutive dots.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.check_illegal_path("A..B")

    def test_illegal_empty_segment(self):
        """Rollup rejects paths with empty segments.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.check_illegal_path("..B")

    def test_illegal_control_char(self):
        """Rollup rejects paths containing control characters.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.check_illegal_path("A\x01B")

    def test_illegal_segment_leading_space(self):
        """Rollup rejects path segments with leading spaces.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.check_illegal_path("A. B")

    def test_illegal_segment_trailing_space(self):
        """Rollup rejects path segments with trailing spaces.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.check_illegal_path("A .B")

    def test_normal_and_empty_mix(self):
        """Rollup skips empty paths while processing valid paths.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_illegal_path_data()
        tdSql.execute("create table t_good using meters tags ('A.B')")
        tdSql.execute("create table t_empty using meters tags ('')")
        self.create_illegal_path_stream("s_mix", "rs_mix")
        self.wait_stream_status("s_mix", "db_rollup_illegal", "running")
        tdSql.execute("insert into t_good values (1700000000000, 1.0)")
        tdSql.execute("insert into t_empty values (1700000000000, 1.0)")

        for _ in range(20):
            tdSql.query("select distinct loc from rs_mix")
            got = sorted(tdSql.getData(i, 0) for i in range(tdSql.queryRows))
            if got == ["A", "A.B"]:
                return
            time.sleep(1)
        raise AssertionError(f"rollup mix produced wrong groups: {got}")

    def test_error_retains_existing_data(self):
        """Rollup keeps existing output when a later illegal path fails the stream.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_illegal_path_data()
        tdSql.execute("create table t_good using meters tags ('A.B')")
        self.create_illegal_path_stream("s_retains", "rs_retains")
        self.wait_stream_status("s_retains", "db_rollup_illegal", "running")
        tdSql.execute("insert into t_good values (1700000000000, 1.0)")
        for _ in range(20):
            if self.query_scalar("select count(*) from rs_retains where loc='A.B'") == 1:
                break
            time.sleep(1)
        else:
            raise AssertionError("rollup output was not produced before illegal path")

        tdSql.execute("create table t_bad using meters tags ('A..B')")
        tdSql.execute("insert into t_bad values (1700000001000, 2.0)")
        self.wait_stream_status("s_retains", "db_rollup_illegal", "failed")
        assert self.query_scalar("select count(*) from rs_retains where loc='A.B'") == 1


    def prepare_placeholder_data(self):
        tdSql.executes(
            [
                "drop database if exists db_rollup_placeholder force",
                "create database db_rollup_placeholder vgroups 1",
                "use db_rollup_placeholder",
                "create stable meters (ts timestamp, current float) tags (location nchar(64))",
                "create table t1 using meters tags ('A.B.C')",
                "create table t2 using meters tags ('A.B.D')",
            ]
        )

    def wait_rows(self, table, expected, timeout=20):
        for _ in range(timeout):
            tdSql.query(f"select count(*) from {table}")
            if tdSql.getData(0, 0) >= expected:
                return
            time.sleep(1)
        raise AssertionError(f"{table} did not reach {expected} rows")

    def test_rollup_placeholder_path_in_subtable(self):
        """Rollup path placeholder can build output subtable names.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_placeholder_data()
        tdSql.execute(
            "create stream s_p1 interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_p1 "
            "output_subtable(concat(replace(cast(%%1 as varchar(256)), '.', '_'), '_avg')) "
            "(ts, cnt) tags (full_loc nchar(256) as %%1) "
            "as select _twstart, count(*) from %%trows"
        )
        self.wait_stream_running("s_p1", "db_rollup_placeholder")
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        self.wait_rows("rs_p1", 3)
        names = self.query_column("select distinct tbname from rs_p1 order by tbname")
        assert "A_avg" in names
        assert "A_B_avg" in names
        assert "A_B_C_avg" in names

    def test_rollup_placeholder_path_in_tags(self):
        """Rollup path placeholder can populate output tags.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_placeholder_data()
        tdSql.execute(
            "create stream s_p2 interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_p2 "
            "tags (full_loc nchar(256) as %%1) "
            "as select _twstart, count(*) from %%trows"
        )
        self.wait_stream_running("s_p2", "db_rollup_placeholder")
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        self.wait_rows("rs_p2", 3)
        got = self.query_column("select distinct full_loc from rs_p2 order by full_loc")
        assert sorted(got) == ["A", "A.B", "A.B.C"]

    def test_rollup_placeholder_leaf_tag(self):
        """Rollup leaf placeholder can populate query columns and output tags.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_placeholder_data()
        tdSql.execute(
            "create stream s_p3 interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_p3 "
            "(ts, leaf_val, cnt) "
            "tags (full_loc nchar(256) as %%1, leaf nchar(64) as %%rollup_tag) "
            "as select _twstart, %%rollup_tag as leaf_val, count(*) as cnt from %%trows"
        )
        self.wait_stream_running("s_p3", "db_rollup_placeholder")
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        self.wait_rows("rs_p3", 3)
        tdSql.query("select leaf, leaf_val from rs_p3 where full_loc='A.B.C'")
        assert tdSql.getData(0, 0) == "C"
        assert tdSql.getData(0, 1) == "C"
        tdSql.query("select leaf, leaf_val from rs_p3 where full_loc='A.B'")
        assert tdSql.getData(0, 0) == "B"
        assert tdSql.getData(0, 1) == "B"
        tdSql.query("select leaf, leaf_val from rs_p3 where full_loc='A'")
        assert tdSql.getData(0, 0) == "A"
        assert tdSql.getData(0, 1) == "A"
        tdSql.execute("insert into t1 values (1700003600000, 2.0)")
        self.wait_rows("rs_p3", 6)
        tdSql.query("select leaf, leaf_val from rs_p3 where full_loc='A.B.C' order by ts desc limit 1")
        assert tdSql.getData(0, 0) == "C"
        assert tdSql.getData(0, 1) == "C"

    def test_rollup_placeholder_leaf_nchar_tag(self):
        """Rollup leaf placeholder supports nchar path values.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_placeholder_data()
        tdSql.executes(
            [
                "drop table t1",
                "drop table t2",
                "create table t_unicode using meters tags ('华.东.沪')",
            ]
        )
        tdSql.execute(
            "create stream s_p11 interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_p11 "
            "(ts, leaf_val, cnt) "
            "tags (full_loc nchar(256) as %%1, leaf nchar(64) as %%rollup_tag) "
            "as select _twstart, %%rollup_tag as leaf_val, count(*) as cnt from %%trows"
        )
        self.wait_stream_running("s_p11", "db_rollup_placeholder")
        tdSql.execute("insert into t_unicode values (1700000000000, 1.0)")
        self.wait_rows("rs_p11", 3)
        tdSql.query("select leaf, leaf_val from rs_p11 where full_loc='华.东.沪'")
        assert tdSql.getData(0, 0) == "沪"
        assert tdSql.getData(0, 1) == "沪"
        tdSql.query("select leaf, leaf_val from rs_p11 where full_loc='华.东'")
        assert tdSql.getData(0, 0) == "东"
        assert tdSql.getData(0, 1) == "东"
        tdSql.query("select leaf, leaf_val from rs_p11 where full_loc='华'")
        assert tdSql.getData(0, 0) == "华"
        assert tdSql.getData(0, 1) == "华"

    def test_rollup_placeholder_leaf_varchar_tag(self):
        """Rollup leaf placeholder supports varchar path values.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_placeholder_data()
        tdSql.executes(
            [
                "drop table t1",
                "drop table t2",
                "drop stable meters",
                "create stable meters (ts timestamp, current float) tags (location varchar(64))",
                "create table t1 using meters tags ('X.Y.Z')",
            ]
        )
        tdSql.execute(
            "create stream s_p10 interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_p10 "
            "(ts, leaf_val, cnt) "
            "tags (full_loc varchar(256) as %%1, leaf varchar(64) as %%rollup_tag) "
            "as select _twstart, %%rollup_tag as leaf_val, count(*) as cnt from %%trows"
        )
        self.wait_stream_running("s_p10", "db_rollup_placeholder")
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        self.wait_rows("rs_p10", 3)
        tdSql.query("select leaf, leaf_val from rs_p10 where full_loc='X.Y.Z'")
        assert tdSql.getData(0, 0) == "Z"
        assert tdSql.getData(0, 1) == "Z"
        tdSql.query("select leaf, leaf_val from rs_p10 where full_loc='X.Y'")
        assert tdSql.getData(0, 0) == "Y"
        assert tdSql.getData(0, 1) == "Y"
        tdSql.query("select leaf, leaf_val from rs_p10 where full_loc='X'")
        assert tdSql.getData(0, 0) == "X"
        assert tdSql.getData(0, 1) == "X"

    def test_rollup_placeholder_tbcount_in_query(self):
        """Rollup table-count placeholder is valid in the query projection.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_placeholder_data()
        tdSql.execute(
            "create stream s_p4 interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_p4 "
            "(ts, cnt) "
            "tags (full_loc nchar(256) as %%1) "
            "as select _twstart, _trollup_tbcount as cnt from %%trows"
        )
        self.wait_stream_running("s_p4", "db_rollup_placeholder")
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        tdSql.execute("insert into t2 values (1700000000000, 2.0)")
        self.wait_rows("rs_p4", 4)
        tdSql.query("select cnt from rs_p4 where full_loc='A'")
        assert tdSql.getData(0, 0) == 2
        tdSql.query("select cnt from rs_p4 where full_loc='A.B'")
        assert tdSql.getData(0, 0) == 2
        tdSql.query("select cnt from rs_p4 where full_loc='A.B.C'")
        assert tdSql.getData(0, 0) == 1
        tdSql.query("select cnt from rs_p4 where full_loc='A.B.D'")
        assert tdSql.getData(0, 0) == 1
        tdSql.execute("create table t3 using meters tags ('A.B.E')")
        tdSql.execute("insert into t3 values (1700003600000, 3.0)")
        self.wait_rows("rs_p4", 7)
        tdSql.query("select cnt from rs_p4 where full_loc='A' order by ts desc limit 1")
        assert tdSql.getData(0, 0) == 3
        tdSql.query("select cnt from rs_p4 where full_loc='A.B' order by ts desc limit 1")
        assert tdSql.getData(0, 0) == 3
        tdSql.query("select cnt from rs_p4 where full_loc='A.B.E' order by ts desc limit 1")
        assert tdSql.getData(0, 0) == 1

    def test_rollup_placeholder_tbcount_in_subtable_rejected(self):
        """Rollup table-count placeholder is rejected in OUTPUT_SUBTABLE.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_placeholder_data()
        tdSql.error(
            "create stream s_p5 interval(1h) sliding(1h) from meters "
            "rollup by location into rs_p5 "
            "output_subtable(cast(_trollup_tbcount as varchar(20))) "
            "as select _twstart, count(*) from %%trows",
            expectErrInfo="_trollup_tbcount",
            fullMatched=False,
        )

    def test_rollup_placeholder_tbcount_in_tags_rejected(self):
        """Rollup table-count placeholder is rejected in TAGS.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_placeholder_data()
        tdSql.error(
            "create stream s_p6 interval(1h) sliding(1h) from meters "
            "rollup by location into rs_p6 "
            "tags (cnt int as _trollup_tbcount) "
            "as select _twstart, count(*) from %%trows",
            expectErrInfo="_trollup_tbcount",
            fullMatched=False,
        )

    def test_rollup_placeholder_without_rollup_clause(self):
        """Rollup placeholders require a ROLLUP BY clause.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_placeholder_data()
        tdSql.error(
            "create stream s_p7 interval(1h) sliding(1h) from meters "
            "into rs_p7 as select _twstart, %%rollup_tag from %%trows",
            expectErrInfo="rollup placeholders require ROLLUP BY",
            fullMatched=False,
        )

    def test_rollup_placeholder_no_dot_path(self):
        """Rollup placeholders support single-segment paths.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_placeholder_data()
        tdSql.executes(
            [
                "drop table t1",
                "drop table t2",
                "create table t_single using meters tags ('SINGLE')",
            ]
        )
        tdSql.execute(
            "create stream s_p8 interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_p8 "
            "tags (full_loc nchar(256) as %%1, leaf nchar(64) as %%rollup_tag) "
            "as select _twstart, count(*) from %%trows"
        )
        self.wait_stream_running("s_p8", "db_rollup_placeholder")
        tdSql.execute("insert into t_single values (1700000000000, 1.0)")
        self.wait_rows("rs_p8", 1)
        tdSql.query("select full_loc, leaf from rs_p8")
        assert tdSql.getData(0, 0) == "SINGLE"
        assert tdSql.getData(0, 1) == "SINGLE"

    def test_rollup_placeholder_tbcount_multi_vgroup(self):
        """Rollup table-count placeholder works across multiple vgroups.

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        tdSql.executes(
            [
                "drop database if exists db_rollup_placeholder_mv force",
                "create database db_rollup_placeholder_mv vgroups 3",
                "use db_rollup_placeholder_mv",
                "create stable meters (ts timestamp, current float) tags (location nchar(64))",
            ]
        )
        for i in range(9):
            tdSql.execute(f"create table t_mv_{i} using meters tags ('M.N')")
        tdSql.execute(
            "create stream s_p9 interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_p9 "
            "tags (full_loc nchar(256) as %%1) "
            "as select _twstart, _trollup_tbcount as cnt from %%trows"
        )
        self.wait_stream_running("s_p9", "db_rollup_placeholder_mv")
        for i in range(9):
            tdSql.execute(f"insert into t_mv_{i} values (1700000000000, {i + 1}.0)")
        self.wait_rows("rs_p9", 2)
        tdSql.query("select cnt from rs_p9 where full_loc='M'")
        assert tdSql.getData(0, 0) == 9
        tdSql.query("select cnt from rs_p9 where full_loc='M.N'")
        assert tdSql.getData(0, 0) == 9
