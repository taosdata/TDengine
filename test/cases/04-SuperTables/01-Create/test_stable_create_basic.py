from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestStableCreateMt:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_create_mt(self):
        """Stable name basic

        1. Attempt to create stables with invalid table names, column names, and invalid data types
        2. Create a super table containing multiple tag types
        3. Create child tables and insert data
        4. Query using tags

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-11 Simon Guan Migrated from tsim/parser/create_mt.sim
            - 2025-8-11 Simon Guan Migrated from tsim/tag/create.sim
            - 2025-8-11 Simon Guan Migrated from tsim/column/metrics.sim

        """

        self.CreateMt()
        tdStream.dropAllStreamsAndDbs()
        self.CreateTag()
        tdStream.dropAllStreamsAndDbs()
        self.ColumnMetrics()
        tdStream.dropAllStreamsAndDbs()

    def CreateMt(self):
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

    def CreateTag(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ta_cr_db"
        tbPrefix = "ta_cr_tb"
        mtPrefix = "ta_cr_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdLog.info(f"=============== step2")
        i = 2
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool)")
        tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step3")
        i = 3
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol smallint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step4")
        i = 4
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol tinyint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step5")
        i = 5
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int)")
        tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step6")
        i = 6
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bigint)")
        tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step7")
        i = 7
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol float)")
        tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step8")
        i = 8
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol double)")
        tdSql.execute(f"create table {tb} using {mt} tags( 1 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step9")
        i = 9
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( '1')")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = '1'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol = '0'")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step10")
        i = 10
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 bool)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step11")
        i = 11
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 smallint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step12")
        i = 12
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 tinyint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step13")
        i = 13
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 int)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step14")
        i = 14
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 bigint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step15")
        i = 15
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 float)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step16")
        i = 16
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 double)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step17")
        i = 17
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, '2' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = true")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step18")
        i = 18
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol smallint, tgcol2 tinyint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step19")
        i = 19
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol tinyint, tgcol2 int)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step20")
        i = 20
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol int, tgcol2 bigint)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step21")
        i = 21
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bigint, tgcol2 float)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step22")
        i = 22
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol float, tgcol2 double)"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2 )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = 2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step23")
        i = 23
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol double, tgcol2 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, '2' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol2 = '2'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 0")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step24")
        i = 24
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol1 bool, tgcol2 bool, tgcol3 int, tgcol4 float, tgcol5 double, tgcol6 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, 3, 4, 5, '6' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol1 = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol2 = 1")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol3 = 3")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol4 = 4")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol5 = 5")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol6 = '6'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol6 = '0'")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step25")
        i = 25
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 int, tgcol3 float, tgcol4 double, tgcol5 binary(10), tgcol6 binary(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags( 1, 2, 3, 4, '5', '6' )")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol6 = '6'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol6 = '0'")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step26")
        i = 26
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol binary(10), tgcol2 binary(10), tgcol3 binary(10), tgcol4 binary(10), tgcol5 binary(10), tgcol6 binary(10))"
        )
        tdSql.execute(
            f"create table {tb} using {mt} tags( '1', '2', '3', '4', '5', '6' )"
        )
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol3 = '3'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from {mt} where tgcol3 = '0'")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step27")
        i = 27
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.error(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol bool, tgcol2 bool, tgcol3 int, tgcol4 float, tgcol5 double, tgcol6 binary(10), tgcol7)"
        )

        tdLog.info(f"=============== step28")
        i = 28
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol binary(250), tgcol2 binary(250))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags('1', '1')")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = '1'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdLog.info(f"=============== step29")
        i = 29
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol binary(25), tgcol2 binary(250))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags('1', '1')")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt} where tgcol = '1'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)

        tdLog.info(f"=============== step30")
        i = 30
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol binary(250), tgcol2 binary(250), tgcol3 binary(30))"
        )

        tdLog.info(f"=============== step31")
        i = 31
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int) TAGS(tgcol binary(5))"
        )
        tdSql.error(f"create table {tb} using {mt} tags('1234567')")
        tdSql.execute(f"create table {tb} using {mt} tags('12345')")
        tdSql.execute(f"insert into {tb} values(now, 1)")
        tdSql.query(f"select * from {mt}")
        tdLog.info(f"sql select * from {mt}")
        tdSql.checkRows(1)

        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")
        tdSql.checkData(0, 2, 12345)

    def ColumnMetrics(self):
        tdLog.info(f"=============== step1")
        tdSql.prepare("d2", drop=True)
        tdSql.execute(f"use d2")
        tdSql.execute(
            f"create table d2.mt (ts timestamp, c000 int, c001 int, c002 int, c003 int, c004 int, c005 int, c006 int, c007 int, c008 int, c009 int, c010 int, c011 int, c012 int, c013 int, c014 int, c015 int, c016 int, c017 int, c018 int, c019 int, c020 int, c021 int, c022 int, c023 int, c024 int, c025 int, c026 int, c027 int, c028 int, c029 int, c030 int, c031 int, c032 int, c033 int, c034 int, c035 int, c036 int, c037 int, c038 int, c039 int, c040 int, c041 int, c042 int, c043 int, c044 int, c045 int, c046 int, c047 int, c048 int, c049 int, c050 int, c051 int, c052 int, c053 int, c054 int, c055 int, c056 int, c057 int, c058 int, c059 int, c060 int, c061 int, c062 int, c063 int, c064 int, c065 int, c066 int, c067 int, c068 int, c069 int, c070 int, c071 int, c072 int, c073 int, c074 int, c075 int, c076 int, c077 int, c078 int, c079 int, c080 int, c081 int, c082 int, c083 int, c084 int, c085 int, c086 int, c087 int, c088 int, c089 int, c090 int, c091 int, c092 int, c093 int, c094 int, c095 int, c096 int, c097 int, c098 int, c099 int, c100 int, c101 int, c102 int, c103 int, c104 int, c105 int, c106 int, c107 int, c108 int, c109 int, c110 int, c111 int, c112 int, c113 int, c114 int, c115 int, c116 int, c117 int, c118 int, c119 int, c120 int, c121 int, c122 int, c123 int, c124 int, c125 int, c126 int, c127 int, c128 int, c129 int, c130 int, c131 int, c132 int, c133 int, c134 int, c135 int, c136 int, c137 int, c138 int, c139 int, c140 int, c141 int, c142 int, c143 int, c144 int, c145 int, c146 int, c147 int, c148 int, c149 int, c150 int, c151 int, c152 int, c153 int, c154 int, c155 int, c156 int, c157 int, c158 int, c159 int, c160 int, c161 int, c162 int, c163 int, c164 int, c165 int, c166 int, c167 int, c168 int, c169 int, c170 int, c171 int, c172 int, c173 int, c174 int, c175 int, c176 int, c177 int, c178 int, c179 int, c180 int, c181 int, c182 int, c183 int, c184 int, c185 int, c186 int, c187 int, c188 int, c189 int, c190 int, c191 int, c192 int, c193 int, c194 int, c195 int, c196 int, c197 int, c198 int, c199 int, c200 int, c201 int, c202 int, c203 int, c204 int, c205 int, c206 int, c207 int, c208 int, c209 int, c210 int, c211 int, c212 int, c213 int, c214 int, c215 int, c216 int, c217 int, c218 int, c219 int, c220 int, c221 int, c222 int, c223 int, c224 int, c225 int, c226 int, c227 int, c228 int, c229 int, c230 int, c231 int, c232 int, c233 int, c234 int, c235 int, c236 int, c237 int, c238 int, c239 int, c240 int, c241 int, c242 int, c243 int, c244 int, c245 int, c246 int, c247 int, c248 int) tags(a int, b smallint, c binary(20), d float, e double, f bigint)"
        )
        tdSql.execute(f"create table d2.t1 using d2.mt tags(1, 2, '3', 4, 5, 6)")
        tdSql.execute(f"create table d2.t2 using d2.mt tags(6, 7, '8', 9, 10, 11)")

        tdSql.query(f"show tables")
        tdSql.checkRows(2)

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step2")
        tdSql.execute(
            f"insert into d2.t1 values (now,0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0  )"
        )
        tdSql.execute(
            f"insert into d2.t1 values (now+1m,1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1  )"
        )
        tdSql.execute(
            f"insert into d2.t1 values (now+2m,2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2  )"
        )
        tdSql.execute(
            f"insert into d2.t1 values (now+3m,3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3  )"
        )
        tdSql.execute(
            f"insert into d2.t1 values (now+4m,4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4  )"
        )
        tdSql.execute(
            f"insert into d2.t1 values (now+5m,5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5  )"
        )

        tdSql.execute(
            f"insert into d2.t1 values (now+6m,6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6  )"
        )

        tdSql.execute(
            f"insert into d2.t1 values (now+7m,7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7  )"
        )

        tdSql.execute(
            f"insert into d2.t1 values (now+8m,8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8  )"
        )

        tdSql.execute(
            f"insert into d2.t1 values (now+9m,9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9  )"
        )

        tdSql.execute(
            f"insert into d2.t2 values (now,0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 , 0 )"
        )
        tdSql.execute(
            f"insert into d2.t2 values (now+1m,1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1 , 1  )"
        )
        tdSql.execute(
            f"insert into d2.t2 values (now+2m,2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 , 2 )"
        )
        tdSql.execute(
            f"insert into d2.t2 values (now+3m,3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3 , 3  )"
        )
        tdSql.execute(
            f"insert into d2.t2 values (now+4m,4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4 , 4  )"
        )
        tdSql.execute(
            f"insert into d2.t2 values (now+5m,5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5 , 5  )"
        )

        tdSql.execute(
            f"insert into d2.t2 values (now+6m,6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6 , 6  )"
        )

        tdSql.execute(
            f"insert into d2.t2 values (now+7m,7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7 , 7  )"
        )

        tdSql.execute(
            f"insert into d2.t2 values (now+8m,8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8 , 8  )"
        )

        tdSql.execute(
            f"insert into d2.t2 values (now+9m,9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9 , 9  )"
        )

        tdLog.info(f"=============== step3")
        tdSql.query(f"select * from d2.mt")
        tdSql.checkRows(20)

        tdSql.query(f"select * from d2.mt where ts < now + 4m")
        tdSql.checkRows(10)

        tdSql.query(f"select * from d2.mt where c001 = 1")
        tdSql.checkRows(2)

        tdSql.query(f"select * from d2.mt where c002 = 2 and c003 = 2")
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from d2.mt where c002 = 2 and c003 = 2 and ts < now + 4m"
        )
        tdSql.checkRows(2)

        tdSql.query(f"select count(*) from d2.mt")
        tdSql.checkData(0, 0, 20)

        tdSql.query(
            f"select count(c001), count(c248), avg(c001), avg(c248), sum(c001), max(c001), min(c248), avg(c235), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*) from d2.mt"
        )

        tdSql.checkData(0, 0, 20)
        tdSql.checkData(0, 1, 20)
        tdSql.checkData(0, 2, 4.500000000)
        tdSql.checkData(0, 3, 4.500000000)
        tdSql.checkData(0, 4, 90)
        tdSql.checkData(0, 5, 9)
        tdSql.checkData(0, 6, 0)
        tdSql.checkData(0, 7, 4.500000000)
        tdSql.checkData(0, 8, 20)
        tdSql.checkData(0, 9, 20)

        tdSql.query(
            f"select count(c001), count(c248), avg(c001), avg(c248), sum(c001), max(c001), min(c248), avg(c238), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*) from d2.mt where a = 1"
        )
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(0, 1, 10)
        tdSql.checkData(0, 2, 4.500000000)
        tdSql.checkData(0, 3, 4.500000000)
        tdSql.checkData(0, 4, 45)
        tdSql.checkData(0, 5, 9)
        tdSql.checkData(0, 6, 0)
        tdSql.checkData(0, 7, 4.500000000)
        tdSql.checkData(0, 8, 10)
        tdSql.checkData(0, 9, 10)

        tdLog.info(f"=============== step4")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from d2.mt")
        tdSql.checkRows(20)

        tdSql.query(f"select * from d2.mt where c001 = 1")
        tdSql.checkRows(2)

        tdSql.query(f"select * from d2.mt where c002 = 2 and c003 = 2")
        tdSql.checkRows(2)

        tdSql.query(f"select count(*) from d2.mt")
        tdSql.checkData(0, 0, 20)

        tdSql.query(
            f"select count(c001), count(c248), avg(c001), avg(c248), sum(c001), max(c001), min(c248), avg(c128), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*), count(*) from d2.mt"
        )
        tdSql.checkData(0, 0, 20)
        tdSql.checkData(0, 1, 20)
        tdSql.checkData(0, 2, 4.500000000)
        tdSql.checkData(0, 3, 4.500000000)
        tdSql.checkData(0, 4, 90)
        tdSql.checkData(0, 5, 9)
        tdSql.checkData(0, 6, 0)
        tdSql.checkData(0, 7, 4.500000000)
        tdSql.checkData(0, 8, 20)
        tdSql.checkData(0, 9, 20)
