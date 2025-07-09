from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStableCreateMt:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_create_mt(self):
        """create mt

        1. -

        Catalog:
            - SuperTables:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-7 Simon Guan Migrated from tsim/parser/create_mt.sim

        """

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "fi_in_db"
        tbPrefix = "fi_in_tb"
        mtPrefix = "fi_in_mt"

        tdLog.info(f"excuting test script create_mt.sim")
        tdLog.info(f"=============== set up")
        i = 0
        db = dbPrefix + "0"
        mt = mtPrefix + "0"
        tb = tbPrefix + "0"

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        # case1: case_insensitivity test
        tdLog.info(f"=========== create_mt.sim case1: case insensitivity test")
        tdSql.execute(
            f"CREATE TABLE {mt} (TS TIMESTAMP, COL1 INT, COL2 BIGINT, COL3 FLOAT, COL4 DOUBLE, COL5 BINARY(10), COL6 BOOL, COL7 SMALLINT, COL8 TINYINT, COL9 NCHAR(10)) TAGS (TAG1 INT, TAG2 BIGINT, TAG3 DOUBLE, TAG4 BINARY(10), TAG5 BOOL, TAG6 NCHAR(10))"
        )
        tdSql.query(f"show stables")

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, mt)

        tdSql.error(f"DROP METRIC {mt}")

        tdSql.execute(f"DROP TABLE {mt}")
        tdSql.query(f"show stables")

        tdSql.checkRows(0)

        tdLog.info(f"case_insensitivity test passed")

        # case2: illegal_metric_name test
        tdLog.info(f"=========== create_mt.sim case2: illegal_mt_name test")
        illegal_tb1 = "1db"
        illegal_tb2 = "d@b"

        tdSql.error(
            f"create table {illegal_tb1} (ts timestamp, tcol int) tags (tag1 int)"
        )

        tdSql.error(
            f"create table {illegal_tb2} (ts timestamp, tcol int) tags (tag1 int)"
        )

        tdLog.info(f"illegal_metric_name test passed")

        ## case3: illegal_data_types_in_data_column test
        tdLog.info(f"========== create_mt.sim case3: metric illegal data types test")

        i_ts = "time"  # "illegal" "ts"
        i_binary = "binary"  # "illegal" "binary"
        i_bigint = "long"  # "illegal" "bigint"
        i_smallint = "short"  # "illegal" "smallint"
        i_tinyint = "byte"  # "illegal" "tinyint"
        i_binary2 = "varchar(20)"  # "illegal" "string"
        i_nchar = "nchar"  # "unspecified" "nchar" "length"

        tdSql.error(f"create table {mt} (ts {i_ts} , col int) tags (tag1 int)")
        tdSql.error(
            f"create table {mt} (ts timestamp, col {i_binary} ) tags (tag1 int)"
        )
        tdSql.error(
            f"create table {mt} (ts timestamp, col {i_bigint} ) tags (tag1 int)"
        )
        tdSql.error(
            f"create table {mt} (ts timestamp, col {i_smallint} ) tags (tag1 int)"
        )
        tdSql.execute(
            f"create table {mt} (ts timestamp, col {i_binary2} ) tags (tag1 int)"
        )
        tdSql.execute(f"drop table {mt}")
        tdSql.error(
            f"create table {mt} (ts timestamp, col {i_tinyint} ) tags (tag1 int)"
        )
        tdSql.error(f"create table {mt} (ts timestamp, col {i_nchar} ) tags (tag1 int)")

        # correct using of nchar
        tdSql.execute(
            f"create table {mt} (ts timestamp, col nchar(10)) tags (tag1 int)"
        )
        tdSql.query(f"show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, mt)

        tdSql.execute(f"drop table {mt}")
        tdLog.info(f"illegal_data_type_in_tags test passed")

        ## case4: illegal_data_type_in_tags test
        i_ts = "time"  # "illegal" "ts"
        i_binary = "binary"  # "illegal" "binary"
        i_bigint = "long"  # "illegal" "bigint"
        i_smallint = "short"  # "illegal" "smallint"
        i_tinyint = "byte"  # "illegal" "tinyint"
        i_binary2 = "varchar(20)"  # "illegal" "string"
        i_bool = "boolean"
        nchar = "nchar"  # "nchar" "with" "unspecified" "length"
        tdLog.info(f"========== create_mt.sim case4: illegal data types in tags test")
        ##sql_error create table $mt (ts timestamp, col int) tags (tag1 timestamp )
        tdSql.error(f"create table {mt} (ts timestamp, col int) tags (tag1 {i_ts} )")
        tdSql.error(
            f"create table {mt} (ts timestamp, col int) tags (tag1 {i_binary} )"
        )
        tdSql.error(
            f"create table {mt} (ts timestamp, col int) tags (tag1 {i_bigint} )"
        )
        tdSql.error(
            f"create table {mt} (ts timestamp, col int) tags (tag1 {i_smallint} )"
        )
        tdSql.error(
            f"create table {mt} (ts timestamp, col int) tags (tag1 {i_tinyint} )"
        )
        tdSql.execute(
            f"create table {mt} (ts timestamp, col int) tags (tag1 {i_binary2} )"
        )
        tdSql.execute(f"drop table {mt}")
        tdSql.error(f"create table {mt} (ts timestamp, col int) tags (tag1 {i_bool} )")
        tdSql.error(f"create table {mt} (ts timestamp, col int) tags (tag1 {nchar} )")
        # correct use of nchar in tags
        tdSql.execute(
            f"create table {mt} (ts timestamp, col int) tags (tag1 nchar(20))"
        )
        tdSql.query(f"show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, mt)

        tdSql.execute(f"drop table {mt}")
        tdLog.info(f"illegal_data_type_in_tags test passed")

        # illegal_tag_name test
        # Only frequently used key words are tested here
        tb_ = "table"
        tbs = "tables"
        db_ = "database"
        dbs = "databases"
        ses = "session"
        int = "int"
        bint = "bigint"
        binary = "binary"
        str = "string"
        tag = "tag"
        tags = "tags"
        sint = "smallint"
        tint = "tinyint"
        nchar = "nchar"
        bool = "bool"
        stable = "stable"
        stables = "stables"

        tdSql.error(f"create table {mt} (ts timestamp, col1 int) tags ( {tb_} int)")
        tdSql.error(f"create table {mt} (ts timestamp, col1 int) tags ( {tbs} int)")
        tdSql.error(f"create table {mt} (ts timestamp, col1 int) tags ( {db_} int)")
        tdSql.error(f"create table {mt} (ts timestamp, col1 int) tags ( {dbs} int)")
        tdSql.error(f"create table {mt} (ts timestamp, col1 int) tags ( {ses} int)")
        tdSql.error(f"create table {mt} (ts timestamp, col1 int) tags ( {int} int)")
        tdSql.error(f"create table {mt} (ts timestamp, col1 int) tags ( {bint} int)")
        tdSql.error(f"create table {mt} (ts timestamp, col1 int) tags ( {binary} int)")
        tdSql.execute(f"create table {mt} (ts timestamp, col1 int) tags ( {str} int)")
        tdSql.execute(f"drop table {mt}")
        tdSql.error(f"create table {mt} (ts timestamp, col1 int) tags ( {tag} int)")
        tdSql.error(f"create table {mt} (ts timestamp, col1 int) tags ( {tags} int)")
        tdSql.error(f"create table {mt} (ts timestamp, col1 int) tags ( {sint} int)")
        tdSql.error(f"create table {mt} (ts timestamp, col1 int) tags ( {tint} int)")
        tdSql.error(f"create table {mt} (ts timestamp, col1 int) tags ( {nchar} int)")
        tdSql.error(f"create table {mt} (ts timestamp, col1 int) tags ( {stable} int)")
        tdSql.error(f"create table {mt} (ts timestamp, col1 int) tags ( {stables} int)")
        tdSql.error(f"create table {mt} (ts timestamp, col1 int) tags ( {bool} int)")

        tdLog.info(f"illegal_column_name test passed")

        # case: negative tag values
        tdSql.execute(f"create table {mt} (ts timestamp, col1 int) tags (tg int)")
        tdSql.execute(f"create table {tb} using {mt} tags (-1)")
        # -x ng_tag_v
        #  return -1
        # ng_tag_v:
        tdSql.query(f"show tags from {tb}")
        tdSql.checkData(0, 5, -1)

        tdSql.execute(f"drop table {tb}")

        # case: unmatched_tag_types
        tdLog.info(f"create_mt.sim unmatched_tag_types")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"create table {tb} using {mt} tags ('123')")
        tdSql.query(f"show tags from {tb}")
        tdLog.info(f"tdSql.getData(0,5) = {tdSql.getData(0,5)}")
        tdSql.checkData(0, 5, 123)

        tdSql.execute(f"drop table {tb}")

        tdSql.error(f"create table {tb} using {mt} tags (abc)")
        # the case below might need more consideration
        tdSql.error(f"create table {tb} using {mt} tags ('abc')")
        tdSql.execute(f"drop table if exists {tb}")
        tdSql.execute(f"reset query cache")
        tdSql.execute(f"create table {tb} using {mt} tags (1e1)")

        tdSql.error(f"create table {tb} using {mt} tags ('1e1')")
        tdSql.error(f"create table {tb} using {mt} tags (2147483649)")

        ## case: chinese_char_in_metric
        tdLog.info(f"========== create_mt.sim chinese_char_in_metrics_support test")
        CN_char = "涛思"
        mt1 = "mt1"
        mt2 = "mt2"
        # no chinese char allowed in db, metric, table, column names
        # sql_error create table $CN_char (ts timestamp, col1 int) tags (tag1 int)
        # sql_error create table $mt1 (ts timestamp, $CN_char int) tags (tag1 int)
        # sql_error create table $mt2 (ts timestamp, col1 int) tags ( $CN_char int)
        # sql show metrics
        # if $rows != 3 then
        #  return -1
        # endi
        ##print expected: $CN_char
        ##print returned: $tdSql.getData(0,0)
        # if $tdSql.getData(0,0) != $CN_char then
        #  return -1
        # endi
        ##print expected: $mt1
        ##print returned: $tdSql.getData(1,0)
        # if $tdSql.getData(1,0) != $mt1 then
        #  return -1
        # endi
        ##print expected: $mt2
        ##print returned: $tdSql.getData(2,0)
        # if $tdSql.getData(2,0) != $mt2 then
        #  return -1
        # endi
        # sql select tg from  $mt1
        ##print expected $CN_char
        ##print returned $tdSql.getData(1,0)
        # if $tdSql.getData(1,0) != $CN_char then
        #  return -1
        # endi
        # sql select tg from  $mt2
        ##print expected: $CN_char
        ##print returned: $tdSql.getData(2,0)
        # if $tdSql.getData(2,0) != $CN_char then
        #  return -1
        # endi
        #
        # sql drop table $CN_char
        # sql drop table $mt1
        # sql drop table $mt2

        tdLog.info(f"chinese_char_in_metrics test passed")

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
