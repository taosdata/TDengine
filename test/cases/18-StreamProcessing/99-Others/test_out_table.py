from new_test_framework.utils import tdSql, tdLog, tdStream, StreamItem
from new_test_framework.utils.eutil import findTaosdLog

class TestStreamTagCache:
    def setup_class(cls):
        tdLog.info(f"start to execute {__file__}")

    def test_out_table(self):
        """summary: test out table schema change

        description:
            - check_all_types_basic:
                test tag cache with all data types in basic scenario
            - check_all_types_alter:
                test tag cache with all data types in alter table tag scenario
            - check_null_tag_value:
                test tag cache with null tag values
            - check_create_drop_ctable:
                test tag cache with create and drop child table operations
            - check fallback_to_normal_cache:
                test stable tag filter cache fallback to normal tag filter cache
            - todo(Tony Zhang): check virtual super table


        Since: v3.3.8.8

        Labels: stream, outTable, schemaChange

        Catalog:
            - StreamProcessing:Others

        History:
            - 2025-12-23 Pengrk: created this test

        """
        tdStream.createSnode()
        self.check_out_table_schema_change()

    def check_out_table_schema_change(self):
        db_name = "test"
        stb_name = "stb"
        out_exist_tbname = "out_table"
        out_not_exist_tbname = "out_table_not_exist"
        out_more_cols = "out_table_more_cols"
        tdSql.execute(f"create database if not exists {db_name} vgroups 1 keep 3650", show=1)
        tdSql.execute(f"use {db_name}")
        tdSql.execute(f"create stable {out_exist_tbname} (ts timestamp, c1 int) tags (gid1 int, gid2 int);", show=1)
        tdSql.execute(f"alter table {out_exist_tbname} add column c2 int;", show=1)
        tdSql.execute(f"create stable {out_more_cols} (ts timestamp, c1 int, c2 int, c3 int) tags (gid1 int, gid2 int);", show=1)
        tdSql.execute(f"alter table {out_more_cols} drop column c3;", show=1)
        tdSql.execute(f"create stable {stb_name} (ts timestamp, c0 int, c1 int, c2 int, c3 int) tags (gid1 int, gid2 int);", show=1)



        sql1 =f"""create stream s1 interval(1s) sliding (1s) from {stb_name} partition by tbname,
            gid1, gid2 into {out_exist_tbname} tags (gid1 int as gid1, gid2 int as gid2)
            as select _twstart ts, last(c1) c1, last(c2) c2 from %%tbname where _c0 >= _twstart and _c0 < _twend group by gid1, gid2;"""
        sql2 =f"""create stream s2 interval(1s) sliding (1s) from {stb_name} partition by tbname,
            gid1, gid2 into {out_not_exist_tbname} tags (gid1 int as gid1, gid2 int as gid2)
            as select _twstart ts, last(c1) c1, last(c2) c2 from %%tbname where _c0 >= _twstart and _c0 < _twend group by gid1, gid2;"""
        sql3 =f"""create stream s3 interval(1s) sliding (1s) from {stb_name} partition by tbname,
            gid1, gid2 into {out_not_exist_tbname} tags (gid1 int as gid1, gid2 int as gid2)
            as select _twstart ts, last(c1) c1, last(c2) c2 from %%tbname where _c0 >= _twstart and _c0 < _twend group by gid1, gid2;"""
        streams = [
            self.StreamItem(sql1, self.checks1),
            self.StreamItem(sql2, self.checks2),
            self.StreamItem(sql3, self.checks3)
        ]
        for stream in streams:
            tdSql.execute(stream.sql)
        tdStream.checkStreamStatus()

        tdSql.execute(f"create table ct1 using {stb_name} tags (1, 2);", show=1)
        tdSql.execute(f"insert into ct1 (ts, c1) values ('2025-01-01 00:00:00', 0), ('2025-01-01 00:00:01', 1);", show=1)

        tdSql.query(f"select gid1, gid2, c1, c2 from {out_exist_tbname}")


    def checks1(self):
        db_name = "test"
        out_exist_tbname = "out_table"
        result_sql = f"select gid1, gid2, c1, c2 from {db_name}.{out_exist_tbname}"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, 1)
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, 1)
            and tdSql.compareData(0, 3, "NULL")
        )

    def checks2(self):
        db_name = "test"
        out_not_exist_tbname = "out_table_not_exist"
        result_sql = f"select gid1, gid2, c1, c2 from {db_name}.{out_not_exist_tbname}"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, 1)
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, 1)
            and tdSql.compareData(0, 3, "NULL")
        )
    def checks3(self):
        db_name = "test"
        out_more_cols = "out_table_more_cols"
        result_sql = f"select gid1, gid2, c0, c1, c2 from {db_name}.{out_more_cols}"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 1
            and tdSql.compareData(0, 0, 1)
            and tdSql.compareData(0, 1, 2)
            and tdSql.compareData(0, 2, "NULL")
            and tdSql.compareData(0, 3, 1)
            and tdSql.compareData(0, 4, "NULL")
        )


    class StreamItem:
        def __init__(self, sql, checkfunc):
            self.sql = sql
            self.checkfunc = checkfunc

        def check(self):
            self.checkfunc()
