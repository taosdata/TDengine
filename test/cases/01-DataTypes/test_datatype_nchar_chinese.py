from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestNChar:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_nchar(self):
        """DataTypes: nchar chinese

        1. Create table
        2. Insert data
        3. Query data

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/parser/nchar.sim

        """

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "nchar_db"
        tbPrefix = "nchar_tb"
        mtPrefix = "nchar_stb"
        tbNum = 10
        rowNum = 20
        totalNum = 200
        col = "NCHAR"

        tdLog.info(f"=============== set up")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.prepare(dbname=db)
        tdSql.execute(f"use {db}")

        # case1: create_metric_with_nchar_data
        tdLog.info(f"========== nchar.sim case1: create_table_with_nchar_data test")

        # unspecified nchar length
        tdSql.error(f"create table {mt} (ts timestamp, col nchar)")
        tdSql.error(f"create table {mt} (ts timestamp, col int) tags (tg nchar)")
        # nchar as column or tag names
        tdSql.error(f"create table {mt} (ts timestamp, nchar int)")
        tdSql.error(f"create table {mt} (ts timestamp, col int) tags (nchar int)")
        tdSql.execute(
            f"create table {mt} (ts timestamp, col nchar(60)) tags (tg nchar(60))"
        )
        tdSql.query(f"show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, mt)

        tdSql.query(f"describe {mt}")
        tdSql.checkRows(3)
        tdSql.checkData(1, 1, "NCHAR")

        tdSql.execute(f"create table {tb} using {mt} tags ('tag1')")
        tdSql.query(f"describe {tb}")
        tdSql.checkData(1, 1, "NCHAR")
        tdSql.checkData(2, 1, "NCHAR")

        tdSql.execute(f"drop table {tb}")
        tdSql.execute(f"drop table {mt}")
        tdLog.info(f"create_metric_with_nchar_data test passed")

        # case2 : insert_nchar_data
        # print ========== nchar.sim case2: insert_nchar_data
        tdSql.execute(
            f"create table {mt} (ts timestamp, col nchar(10)) tags(tg nchar(10))"
        )
        tdSql.execute(f"create table {tb} using {mt} tags ('tag1')")
        tdSql.query(f"describe {mt}")
        # print expected: NCHAR
        # print returned: $tdSql.getData(1,1)
        tdSql.checkData(1, 1, "NCHAR")

        # insert nchar data with a pair of single quotes
        col = '"NCHAR"'
        col_ = "NCHAR"
        tdSql.execute(f"insert into {tb} values (now+1s, {col} )")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdLog.info(f"expected: {col_}")
        tdLog.info(f"returned: {tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, col_)

        # insert nchar data with a pair of double quotes
        col = '"NCHAR"'
        col_ = "NCHAR"
        tdSql.execute(f"insert into {tb} values (now+2s, {col} )")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdLog.info(f"expected: {col_}")
        tdLog.info(f"returned: {tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, col_)

        # insert nchar data without quotes
        col = "NCHAR"
        col_ = "nchar"
        tdSql.error(f"insert into {tb} values (now+3s, {col} )")
        tdSql.query(f"select * from {tb} order by ts desc")
        # print expected: $col_
        # print returned: $tdSql.getData(0,1)
        # if $tdSql.getData(0,1, $col_ then
        #  return -1
        # endi

        # insert nchar data with space and no quotes
        tdSql.error(f"insert into {tb} values (now+4s, n char )")

        # insert nchar data with space and a pair of single quotes
        tdSql.execute(f"insert into {tb} values (now+5s, 'NCHAR' )")
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkData(0, 1, "NCHAR")

        # insert nchar data with space and a pair of double quotes
        tdSql.execute(f'insert into {tb} values (now+6s, "NCHAR" )')
        tdSql.query(f"select * from {tb} order by ts desc")
        tdSql.checkData(0, 1, "NCHAR")

        tdSql.execute(f'insert into {tb} values (now+7s, "涛思" )')
        tdSql.query(f"select * from {tb} order by ts desc")
        tdLog.info(f"expected: 涛思")
        tdLog.info(f"returned: {tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, "涛思")

        # insert nchar data with a single quote and a double quote
        # sql insert into $tb values (now+8s, 'NCHAR")
        tdSql.error(f"insert into {tb} values (now+9s, 'NCHAR\")")
        tdSql.error(f"insert into {tb} values (now+10s, 'NCHAR])")

        tdSql.execute(f"drop table {tb}")
        tdSql.execute(f"drop table {mt}")
        tdSql.execute(f"reset query cache")
        # create multiple metrics and tables and insert nchar type data
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol nchar(10), tbcol1 int) TAGS(tgcol nchar(10))"
        )
        i = 0
        while i < 5:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( '0' )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , '0', {x} )")
                x = x + 1
            i = i + 1

        while i < 10:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( '1' )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                tdSql.execute(f"insert into {tb} values (now + {ms} , '1', {x} )")
                x = x + 1
            i = i + 1

        # case5: query_with_tag_filtering
        tdLog.info(f"========== nchar.sim case5: query test")
        # simple query with nchar tag filtering
        tdSql.query(f"select * from {mt} where tgcol = '1'")

        tdLog.info(f"reset query cache")
        tdSql.execute(f"reset query cache")
        tdSql.query(f"select * from {mt} where tgcol = '1'")
        tdSql.checkRows(100)

        # sql select * from $mt where tgcol > '0'
        ##print rows = $rows
        # if $rows != 100 then
        #  return -1
        # endi
        ##print $tdSql.getData(0,3)
        # if $tdSql.getData(0,3, 1 then
        #  return -1
        # endi

        # cumulative query with nchar tag filtering
        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tgcol = '1'"
        )
        tdSql.checkRows(1)

        tdSql.query(
            f"select count(tbcol1), avg(tbcol1), sum(tbcol1), min(tbcol1), max(tbcol1), first(tbcol1), last(tbcol1) from {mt} where tbcol1 = 1 group by tgcol"
        )
        tdSql.checkRows(2)

        tdSql.query(f"select count(tbcol) from {mt} where tbcol1 = 1 group by tgcol")
        tdSql.checkRows(2)

        tdLog.info(f"tdSql.getData(0,0) = {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)

        tdSql.error(f"select avg(tbcol) from {mt} where tbcol1 = 1 group by tgcol")
        tdSql.error(f"select sum(tbcol) from {mt} where tbcol1 = 1 group by tgcol")

        tdSql.query(
            f"select first(tbcol), tgcol from {mt} where tbcol1 = 1 group by tgcol order by tgcol"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 1)

        tdSql.query(
            f"select last(tbcol), tgcol from {mt} where tbcol1 = 1 group by tgcol order by tgcol"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 0)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 1)

        tdSql.execute(f"create table stbb (ts timestamp, c1 nchar(5)) tags (t1 int)")
        tdSql.execute(f"create table tbb1 using stbb tags(1)")
        tdSql.execute(f"insert into tbb1 values ('2018-09-17 09:00:00', '涛思')")
        tdSql.execute(f"insert into tbb1 values ('2018-09-17 09:00:01', 'insrt')")

        tdSql.query(f"select * from tbb1 order by ts asc")
        tdSql.checkRows(2)

        tdLog.info(f"tdSql.getData(1,1) = {tdSql.getData(1,1)}")
        tdSql.checkData(1, 1, "insrt")

        tdSql.query(f"select * from stbb")
        tdSql.checkRows(2)
        tdSql.checkData(1, 1, "insrt")
