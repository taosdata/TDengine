from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster

class TestStreamOutputTable:

    def setup_class(self):
        tdLog.debug(f"start to execute {__file__}")

    def test_output_table_schema_validation(self):
        """
        Verify that calculation result must match output table schema

        Description:
            - Verify error is raised when calculation result and output table column type do not match
            - Verify error is raised when calculation result and output table column name do not match
            - Verify error is raised when calculation result and output table Tag column type do not match
            - Verify error is raised when calculation result and output table Tag column name do not match

        Since: v3.3.7.0

        Catalog:
            - Streams: 02-Stream

        Labels: common,ci

        Jira: TS-7721

        History:
            - 2025-12-01 Created by Kane Kuang

        """

        tdStream.createSnode()

        self.prepareData()
        self.testValidSchema()
        self.testInvalidSchema()

    def prepareData(self):
        tdLog.info(f"prepare data")

        sqls = [
            "create database db vgroups 1;",
            "use db;",
            "create table stb (ts timestamp, `Col_1` int) tags(`Tag_1` int);",
        ]

        tdSql.executes(sqls)
        tdLog.info(f"create successfully.")

    def testValidSchema(self):
        tdLog.info(f"test valid output table schema:")

        tdSql.execute("create stable out_valid(ts timestamp, `Min` int, `Max` int, `Sum` bigint, `Cnt` bigint, `Avg` double) tags (`Tag_1` int);")
        tdSql.execute("create stream s_valid interval(1s) sliding(1s) from stb partition by tbname, `Tag_1` into out_valid tags (`Tag_1` int as `Tag_1`) as select _twstart as ts, min(`Col_1`) as `Min`, max(`Col_1`) as `Max`, sum(`Col_1`) as `Sum`, count(`Col_1`) as `Cnt`, avg(`Col_1`) as `Avg` from %%trows;")

    def testInvalidSchema(self):
        tdLog.info(f"test invalid output table schema:")

        base_stream_sql = "create stream s_invalid_{id} interval(1s) sliding(1s) from stb partition by tbname, `Tag_1` into out_invalid_{id} tags (`Tag_1` int as `Tag_1`) as select _twstart as ts, min(`Col_1`) as `Min`, max(`Col_1`) as `Max`, sum(`Col_1`) as `Sum`, count(`Col_1`) as `Cnt`, avg(`Col_1`) as `Avg` from %%trows;"

        invalid_schemas = [
            # output table column type mismatch
            "create stable out_invalid_{id}(ts timestamp, `Min` int, `Max` int, `Sum` int, `Cnt` bigint, `Avg` double) tags (`Tag_1` int);",
            "create stable out_invalid_{id}(ts timestamp, `Min` int, `Max` int, `Sum` bigint, `Cnt` int, `Avg` double) tags (`Tag_1` int);",
            "create stable out_invalid_{id}(ts timestamp, `Min` int, `Max` int, `Sum` bigint, `Cnt` bigint, `Avg` float) tags (`Tag_1` int);",
            # output table column name mismatch
            "create stable out_invalid_{id}(ts timestamp, `Min` int, `Max` int, `sum` bigint, `Cnt` bigint, `Avg` double) tags (`Tag_1` int);",
            "create stable out_invalid_{id}(ts timestamp, `Min` int, `Max` int, `Sum` bigint, `CNT` bigint, `Avg` double) tags (`Tag_1` int);",
            "create stable out_invalid_{id}(ts timestamp, `Min` int, `Max` int, `Sum` bigint, `Cnt` bigint, `AvG` double) tags (`Tag_1` int);",
            # output table tag column type mismatch
            "create stable out_invalid_{id}(ts timestamp, `Min` int, `Max` int, `Sum` bigint, `Cnt` bigint, `Avg` double) tags (`Tag_1` bigint);",
            # output table tag column name mismatch
            "create stable out_invalid_{id}(ts timestamp, `Min` int, `Max` int, `Sum` bigint, `Cnt` bigint, `Avg` double) tags (`tag_1` int);",
            "create stable out_invalid_{id}(ts timestamp, `Min` int, `Max` int, `Sum` bigint, `Cnt` bigint, `Avg` double) tags (`tAg_1` int);",
            "create stable out_invalid_{id}(ts timestamp, `Min` int, `Max` int, `Sum` bigint, `Cnt` bigint, `Avg` double) tags (`TAG_1` int);",
        ]

        for i, schema_sql in enumerate(invalid_schemas):
            tdSql.execute(schema_sql.format(id=i))
            tdSql.error(base_stream_sql.format(id=i))

        tdLog.info(f"test invalid output table schema successfully.")
