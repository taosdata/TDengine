from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSubTableCreateTb:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_sub_table_create_tb(self):
        """Name: basic

        1. Attempt to create tables with invalid table names
        2. Attempt to create tables with invalid column names
        3. Attempt to create tables with invalid data types

        Catalog:
            - Table:SubTable:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/create_tb.sim

        """

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "fi_in_db"
        tbPrefix = "fi_in_tb"
        mtPrefix = "fi_in_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"create_tb test")
        tdLog.info(f"=============== set up")
        i = 0
        db = dbPrefix + "0"
        mt = mtPrefix + "0"
        tb = tbPrefix + "0"

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        # case1: case_insensitivity test
        tdLog.info(f"=========== create_tb.sim case1: case_insensitivity test")
        tdSql.execute(
            f"CREATE TABLE {tb} (TS TIMESTAMP, COL1 INT, COL2 BIGINT, COL3 FLOAT, COL4 DOUBLE, COL5 BINARY(10), COL6 BOOL, COL7 SMALLINT, COL8 TINYINT, COL9 NCHAR(10));"
        )
        tdSql.query(f"show tables")

        tdSql.checkRows(1)
        tdSql.checkData(0, 0, tb)

        tdSql.execute(f"DROP TABLE {tb}")
        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"case_insensitivity test passed")

        # case2: illegal_table_name test
        tdLog.info(f"=========== create_tb.sim case2: illegal_table_name test")
        illegal_db1 = "1db"
        illegal_db2 = "d@b"

        tdSql.error(f"create table {illegal_db1} (ts timestamp, tcol int)")
        tdSql.error(f"create table {illegal_db2} (ts timestamp, tcol int)")
        tdLog.info(f"illegal_table_name test passed")

        # case3: illegal_data_types
        tdLog.info(f"========== create_tb.sim case3: illegal_data_types test")
        i_ts = "time"  # "illegal" "ts"
        i_binary = "binary"  # "illegal" "binary"
        i_bigint = "long"  # "illegal" "bigint"
        i_smallint = "short"  # "illegal" "smallint"
        i_tinyint = "byte"  # "illegal" "tinyint"
        i_binary2 = "varchar(20)"  # "illegal" "string"
        nchar = "nchar"  # "unspecified" "nchar" "length"

        tdSql.error(f"create table {tb} (ts {i_ts} , col int)")
        tdSql.error(f"create table {tb} (ts timestamp, col {i_binary} )")
        tdSql.error(f"create table {tb} (ts timestamp, col {i_bigint} )")
        tdSql.error(f"create table {tb} (ts timestamp, col {i_smallint} )")
        tdSql.error(f"create table {tb} (ts timestamp, col {i_tinyint} )")
        tdSql.execute(f"create table {tb} (ts timestamp, col {i_binary2} )")
        tdSql.execute(f"drop table {tb}")
        tdSql.error(f"create table {tb} (ts timestamp, col {nchar} )")
        tdSql.execute(f"create table {tb} (ts timestamp, col nchar(20))")
        tdSql.query(f"show tables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, tb)

        tdSql.execute(f"drop table {tb}")
        tdLog.info(f"illegal data type test passed")

        # case4: illegal_column_names
        # Only frequently used key words are tested here
        tdLog.info(f"========== create_tb.sim case4: illegal_column_names")
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

        tdSql.error(f"create table {tb} (ts timestamp, {tb_} int)")
        tdSql.error(f"create table {tb} (ts timestamp, {tbs} int)")
        tdSql.error(f"create table {tb} (ts timestamp, {db_} int)")
        tdSql.error(f"create table {tb} (ts timestamp, {dbs} int)")
        tdSql.error(f"create table {tb} (ts timestamp, {ses} int)")
        tdSql.error(f"create table {tb} (ts timestamp, {int} int)")
        tdSql.error(f"create table {tb} (ts timestamp, {bint} int)")
        tdSql.error(f"create table {tb} (ts timestamp, {binary} int)")
        tdSql.execute(f"create table {tb} (ts timestamp, {str} int)")
        tdSql.execute(f"drop table {tb}")
        tdSql.error(f"create table {tb} (ts timestamp, {tag} int)")
        tdSql.error(f"create table {tb} (ts timestamp, {tags} int)")
        tdSql.error(f"create table {tb} (ts timestamp, {sint} int)")
        tdSql.error(f"create table {tb} (ts timestamp, {tint} int)")
        tdSql.error(f"create table {tb} (ts timestamp, {nchar} int)")

        # too long column name
        tdSql.error(
            f"create table {tb} (ts timestamp, abcde_123456789_123456789_123456789_123456789_123456789_123456789 int)"
        )
        tdSql.error(
            f"create table tx(ts timestamp, k int) tags(abcd5_123456789_123456789_123456789_123456789_123456789_123456789 int)"
        )
        tdLog.info(f"illegal_column_names test passed")

        # case5: chinese_char_in_table_support
        tdLog.info(
            f"========== create_tb.sim case5: chinese_char_in_table_support test"
        )

        CN_char = "涛思"
        tb1 = "tb1"
        tdSql.error(f"create table {CN_char} (ts timestamp, col1 int)")
        # sql show tables
        # if $rows != 1 then
        #  return -1
        # endi
        # print expected: $CN_char
        # print returned: $tdSql.getData(0,0)
        # if $tdSql.getData(0,0) != $CN_char then
        #  return -1
        # endi
        # sql drop table $CN_char

        tdSql.error(f"create table {tb1} (ts timestamp, {CN_char} int)")
        # print expected: $tb1
        # print returned: $tdSql.getData(1,0)
        # sql show tables
        # if $rows != 1 then
        #  return -1
        # endi
        # if $tdSql.getData(0,0) != $tb1 then
        #  return -1
        # endi
        # sql describe $tb1
        ##print expected $CN_char
        ##print returned $tdSql.getData(1,0)
        # if $tdSql.getData(1,0) != $CN_char then
        #  return -1
        # endi
        # sql drop table $tb1
        tdLog.info(f"chinese_char_in_table_support test passed")

        # case6: table_already_exists
        tdLog.info(f"========== create_tb.sim case6: table_already_exists")
        tdSql.execute(f"create table tbs (ts timestamp, col int)")
        tdSql.execute(f"insert into tbs values (now, 1)")
        tdSql.error(f"create table tbs (ts timestamp, col bool)")
        # sql_error create table tb (ts timestamp, col bool)
        tdLog.info(f"table_already_exists test passed")

        # case7: table_name_length_exceeds_limit
        tdLog.info(f"========== create_tb.sim case7: table_name_length_exceeds_limit")
        tbname32 = "_32_aaaabbbbccccddddaaaabbbbcccc"
        tbname64 = "_64_aaaabbbbccccddddaaaabbbbccccddddaaaabbbbccccddddaaaabbbbcccc"
        tbname63 = "_63_aaaabbbbccccddddaaaabbbbccccddddaaaabbbbccccddddaaaabbbbccc"
        tbname65 = "_65_aaaabbbbccccddddaaaabbbbccccddddaaaabbbbccccddddaaaabbbbcccca1111111111111111111111111111111111aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        tdSql.execute(f"create table {tbname32} (ts timestamp, col int)")
        tdSql.execute(f"insert into {tbname32} values (now, 1)")
        tdSql.execute(f"create table {tbname64} (ts timestamp, col int)")
        tdSql.execute(f"insert into {tbname64} values (now, 1)")
        tdSql.execute(f"create table {tbname63} (ts timestamp, col int)")
        tdSql.execute(f"insert into {tbname63} values (now, 1)")
        tdSql.error(f"create table {tbname65} (ts timestamp, col int)")
        # sql_error create table tb (ts timestamp, col bool)
        tdLog.info(f"table_already_exists test passed")

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)
