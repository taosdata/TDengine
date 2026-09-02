import time

from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamRollupBasic:
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdStream.ensureSnode()

    def test_stream_rollup_smoke(self):
        """Stream rollup smoke

        1. Create a rollup stream without data
        2. Verify the stream is deployed and not in ERROR state

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
                "drop database if exists db_rollup_smoke force",
                "create database db_rollup_smoke vgroups 1",
                "create stable db_rollup_smoke.meters "
                "(ts timestamp, current float) tags(location varchar(64))",
                "create stream db_rollup_smoke.s_smoke interval(1h) sliding(1h) "
                "from db_rollup_smoke.meters rollup by location "
                "into db_rollup_smoke.rs_smoke "
                "as select _twstart, avg(current) from %%trows",
            ]
        )

        tdSql.query(
            "select status from information_schema.ins_streams "
            "where db_name='db_rollup_smoke' and stream_name='s_smoke'"
        )
        if tdSql.queryRows != 1:
            raise Exception("cannot find rollup smoke stream")

        status = tdSql.getData(0, 0)
        if status is not None and str(status).lower() == "failed":
            raise Exception(f"rollup smoke stream status is FAILED")


    def test_stream_rollup_syntax(self):
        """Stream rollup syntax

        1. Verify ROLLUP BY placeholder syntax and semantic checks

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215

        History:
            - 2026-05-18 Created
        """

        self.prepare_data()
        self.check_errors()
        self.check_valid_placeholders()

    def prepare_data(self):
        tdSql.executes(
            [
                "drop database if exists db_rollup_syntax force",
                "create database db_rollup_syntax vgroups 1",
                "create table db_rollup_syntax.stb (ts timestamp, c1 int) tags(tag1 int, tag2 varchar(20))",
                "create table db_rollup_syntax.ctb1 using db_rollup_syntax.stb tags(1, 'a')",
                "create table db_rollup_syntax.q (ts timestamp, c1 int)",
            ]
        )

    def expect_rollup_error(self, sql, message):
        tdSql.error(sql, expectErrInfo=message, fullMatched=False)

    def check_errors(self):
        self.expect_rollup_error(
            "create stream db_rollup_syntax.s_child interval(1s) sliding(1s) "
            "from db_rollup_syntax.ctb1 rollup by tag2 into db_rollup_syntax.out_child "
            "as select _tlocaltime, avg(c1) from db_rollup_syntax.q",
            "super table",
        )
        self.expect_rollup_error(
            "create stream db_rollup_syntax.s_no_from_interval interval(1s) sliding(1s) "
            "rollup by tag2 into db_rollup_syntax.out_no_from_interval "
            "as select _tlocaltime, avg(c1) from db_rollup_syntax.q",
            "requires explicit FROM",
        )
        self.expect_rollup_error(
            "create stream db_rollup_syntax.s_no_rollup_subtable interval(1s) sliding(1s) "
            "from db_rollup_syntax.stb partition by tbname into db_rollup_syntax.out_no_rollup_subtable "
            "output_subtable(cast(%%rollup_tag as varchar(20))) "
            "as select _tlocaltime, avg(c1) from db_rollup_syntax.q",
            "rollup placeholders require ROLLUP BY",
        )
        self.expect_rollup_error(
            "create stream db_rollup_syntax.s_no_rollup_tags interval(1s) sliding(1s) "
            "from db_rollup_syntax.stb partition by tbname into db_rollup_syntax.out_no_rollup_tags "
            "tags(rt nchar(20) as %%rollup_tag) "
            "as select _tlocaltime, avg(c1) from db_rollup_syntax.q",
            "rollup placeholders require ROLLUP BY",
        )
        self.expect_rollup_error(
            "create stream db_rollup_syntax.s_out_tbcount interval(1s) sliding(1s) "
            "from db_rollup_syntax.stb rollup by tag2 into db_rollup_syntax.out_tbcount "
            "output_subtable(cast(_trollup_tbcount as varchar(20))) "
            "as select _tlocaltime, avg(c1) from db_rollup_syntax.q",
            "_trollup_tbcount",
        )
        self.expect_rollup_error(
            "create stream db_rollup_syntax.s_tags_tbcount interval(1s) sliding(1s) "
            "from db_rollup_syntax.stb rollup by tag2 into db_rollup_syntax.tags_tbcount "
            "tags(tbcount int as _trollup_tbcount) "
            "as select _tlocaltime, avg(c1) from db_rollup_syntax.q",
            "_trollup_tbcount",
        )

    def check_valid_placeholders(self):
        tdSql.execute(
            "create stream db_rollup_syntax.s_query_ok interval(1s) sliding(1s) "
            "from db_rollup_syntax.stb rollup by tag2 into db_rollup_syntax.out_query_ok(ts, rt, tbcount) "
            "as select _tlocaltime, %%rollup_tag, _trollup_tbcount from db_rollup_syntax.q"
        )
        tdSql.execute(
            "create stream db_rollup_syntax.s_out_ok interval(1s) sliding(1s) "
            "from db_rollup_syntax.stb rollup by tag2 into db_rollup_syntax.out_out_ok "
            "output_subtable(cast(%%rollup_tag as varchar(20))) (ts, v) "
            "tags(rt varchar(20) as %%rollup_tag) "
            "as select _tlocaltime, avg(c1) from db_rollup_syntax.q"
        )
        tdSql.execute(
            "create stream db_rollup_syntax.s_default_subtable_ok interval(1s) sliding(1s) "
            "from db_rollup_syntax.stb rollup by tag2 into db_rollup_syntax.out_default_subtable_ok (ts, v) "
            "tags(rt varchar(20) as %%rollup_tag) "
            "as select _tlocaltime, avg(c1) from db_rollup_syntax.q"
        )
        tdSql.execute(
            "create stream db_rollup_syntax.s_rollup_tag_expr_ok interval(1s) sliding(1s) "
            "from db_rollup_syntax.stb rollup by tag2 into db_rollup_syntax.out_rollup_tag_expr_ok "
            "output_subtable(cast(tag2 as varchar(20))) "
            "(ts, v) "
            "tags(rt varchar(20) as tag2) "
            "as select _tlocaltime, avg(c1) from db_rollup_syntax.q"
        )


    def test_stream_rollup_alter(self):
        """Stream rollup tag alter freeze

        1. Verify ALTER TAG operations that change grouping are blocked for a rollup tag referenced by stream
        2. Verify ALTER TAG is allowed for non-rollup tags
        3. Verify DROP STREAM unlocks the rollup tag
        4. Verify the same freeze applies to virtual stable tags

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0

        Labels: common,ci

        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215

        History:
            - 2026-05-19 Created
        """

        self.check_stable_rollup_tag_frozen()
        self.check_virtual_stable_rollup_tag_frozen()

    def expect_frozen(self, sql):
        tdSql.error(sql, expectErrInfo="Col/Tag referenced by stream", fullMatched=False)

    def create_rollup_stream(self, db, stream, stb, out):
        tdSql.execute(
            f"create stream {db}.{stream} interval(1s) sliding(1s) "
            f"from {db}.{stb} rollup by path into {db}.{out}(ts, v) "
            f"as select _tlocaltime, count(*) from {db}.{stb}"
        )

    def check_stable_rollup_tag_frozen(self):
        db = "db_rollup_alter"
        tdSql.executes(
            [
                f"drop database if exists {db} force",
                f"create database {db} vgroups 1",
                f"create stable {db}.stb (ts timestamp, c1 int) "
                "tags(path varchar(64), region varchar(20))",
                f"create table {db}.ctb1 using {db}.stb tags('root.a', 'north')",
            ]
        )

        self.create_rollup_stream(db, "s_drop", "stb", "out_drop")
        self.expect_frozen(f"alter table {db}.stb drop tag path")
        tdSql.execute(f"drop stream {db}.s_drop")
        tdSql.execute(f"alter table {db}.stb drop tag path")

        tdSql.execute(f"alter table {db}.stb add tag path varchar(64)")
        self.create_rollup_stream(db, "s_modify", "stb", "out_modify")
        tdSql.execute(f"alter table {db}.stb modify tag path varchar(128)")
        tdSql.execute(f"drop stream {db}.s_modify")
        tdSql.execute(f"alter table {db}.stb modify tag path varchar(256)")

        self.create_rollup_stream(db, "s_rename", "stb", "out_rename")
        self.expect_frozen(f"alter table {db}.ctb1 set tag path='root.b'")
        tdSql.execute(f"alter table {db}.stb rename tag path path_new")
        tdSql.execute(f"alter table {db}.stb modify tag region varchar(40)")
        tdSql.execute(f"alter table {db}.stb rename tag region area")
        tdSql.execute(f"alter table {db}.stb drop tag area")

        tdSql.execute(f"drop stream {db}.s_rename")
        tdSql.execute(f"alter table {db}.ctb1 set tag path_new='root.b'")

    def check_virtual_stable_rollup_tag_frozen(self):
        db = "db_rollup_vstb_alter"
        tdSql.executes(
            [
                f"drop database if exists {db} force",
                f"create database {db} vgroups 1",
                f"create table {db}.src (ts timestamp, c1 int)",
                f"create stable {db}.vstb (ts timestamp, c1 int) "
                "tags(path varchar(64), region varchar(20)) virtual 1",
                f"create vtable {db}.vctb1 ({db}.src.c1) using {db}.vstb "
                "tags('root.v', 'west')",
            ]
        )

        self.create_rollup_stream(db, "s_vstb", "vstb", "out_vstb")
        self.expect_frozen(f"alter table {db}.vstb drop tag path")
        tdSql.execute(f"alter table {db}.vstb modify tag path varchar(128)")
        tdSql.execute(f"alter table {db}.vstb rename tag path path_new")
        tdSql.execute(f"alter table {db}.vstb modify tag region varchar(40)")

        tdSql.execute(f"drop stream {db}.s_vstb")
        tdSql.execute(f"alter table {db}.vstb rename tag path_new path_newer")


    def prepare_default_tags_data(self):
        tdSql.executes(
            [
                "drop database if exists db_rollup_default force",
                "create database db_rollup_default vgroups 1",
                "use db_rollup_default",
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

    def wait_stream_running(self, stream_name, timeout=60):
        status = None
        for _ in range(timeout):
            tdSql.query(
                "select status from information_schema.ins_streams "
                f"where db_name='db_rollup_default' and stream_name='{stream_name}'"
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

    def test_rollup_default_subtable_name(self):
        """Rollup default subtable names are unique.

        1. Create a rollup stream without OUTPUT_SUBTABLE or TAGS
        2. Verify default output subtable names are unique for each rollup group

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_default_tags_data()
        tdSql.execute(
            "create stream s_def_sub interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_def_sub "
            "as select _twstart, count(*) from %%trows"
        )
        self.wait_stream_running("s_def_sub")
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        tdSql.execute("insert into t2 values (1700000000000, 2.0)")
        self.wait_rows("rs_def_sub", 4)

        names = self.query_column("select distinct tbname from rs_def_sub")
        assert len(names) == 4
        assert len(set(names)) == 4

    def test_rollup_default_tags_inherit_rollup_col(self):
        """Rollup default tags inherit the rollup tag column.

        1. Create a rollup stream without explicit TAGS
        2. Verify the output stable keeps the rollup tag column and values

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_default_tags_data()
        tdSql.execute(
            "create stream s_def_tag interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_def_tag "
            "as select _twstart, count(*) from %%trows"
        )
        self.wait_stream_running("s_def_tag")

        tdSql.query("describe rs_def_tag")
        tag_rows = [row for row in tdSql.queryResult if str(row[3]).upper() == "TAG"]
        assert any(row[0] == "location" and "NCHAR" in str(row[1]).upper() for row in tag_rows)

        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        self.wait_rows("rs_def_tag", 3)
        got = self.query_column("select distinct location from rs_def_tag order by location")
        assert sorted(got) == ["A", "A.B", "A.B.C"]

    def test_rollup_explicit_output_subtable(self):
        """Rollup supports explicit output subtable expressions.

        1. Create a rollup stream with OUTPUT_SUBTABLE using the rollup path placeholder
        2. Verify output subtable names and inherited rollup tag values

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_default_tags_data()
        tdSql.execute(
            "create stream s_def_x interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_def_x "
            "output_subtable(concat(replace(cast(%%1 as varchar(256)), '.', '_'), '_x')) "
            "as select _twstart, count(*) from %%trows"
        )
        self.wait_stream_running("s_def_x")
        tdSql.query("describe rs_def_x")
        tag_rows = [row for row in tdSql.queryResult if str(row[3]).upper() == "TAG"]
        assert any(row[0] == "location" and "NCHAR" in str(row[1]).upper() for row in tag_rows)

        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        self.wait_rows("rs_def_x", 3)

        names = self.query_column("select distinct tbname from rs_def_x order by tbname")
        assert all(name.endswith("_x") for name in names)
        assert "A_x" in names
        assert "A_B_x" in names
        assert "A_B_C_x" in names
        got = self.query_column("select distinct location from rs_def_x order by location")
        assert sorted(got) == ["A", "A.B", "A.B.C"]

    def test_rollup_explicit_tags_expression(self):
        """Rollup supports explicit TAGS expressions.

        1. Create a rollup stream with explicit full-path and leaf tags
        2. Verify %%rollup_tag renders the leaf segment

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_default_tags_data()
        tdSql.execute(
            "create stream s_def_t interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_def_t "
            "(ts, cnt) tags (loc nchar(256) as %%1, leaf nchar(64) as %%rollup_tag) "
            "as select _twstart, count(*) from %%trows"
        )
        self.wait_stream_running("s_def_t")
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        self.wait_rows("rs_def_t", 3)

        tdSql.query("select leaf from rs_def_t where loc='A.B.C'")
        assert tdSql.getData(0, 0) == "C"

    def test_rollup_subtable_collision_user_responsible(self):
        """Rollup keeps user-provided output subtable collision behavior.

        1. Create a rollup stream with a fixed OUTPUT_SUBTABLE expression
        2. Verify all rollup groups target the configured subtable name

        Catalog:
            - Streams:Rollup

        Since: v3.4.2.0
        Labels: common,ci
        Feishu: https://project.feishu.cn/taosdata_td/feature/detail/6979200215
        History:
            - 2026-05-19 Created
        """

        self.prepare_default_tags_data()
        tdSql.execute(
            "create stream s_def_c interval(1h) sliding(1h) from meters "
            "rollup by location stream_options(max_delay(3s)) into rs_def_c "
            "output_subtable('fixed_name') "
            "as select _twstart, count(*) from %%trows"
        )
        self.wait_stream_running("s_def_c")
        tdSql.execute("insert into t1 values (1700000000000, 1.0)")
        tdSql.execute("insert into t2 values (1700000000000, 2.0)")
        self.wait_rows("rs_def_c", 1)

        names = self.query_column("select distinct tbname from rs_def_c")
        assert names == ["fixed_name"]

        tdSql.query("show create stream s_def_c")
        sql = str(tdSql.queryResult[0][1]).lower()
        assert "output_subtable" in sql
        assert "fixed_name" in sql
