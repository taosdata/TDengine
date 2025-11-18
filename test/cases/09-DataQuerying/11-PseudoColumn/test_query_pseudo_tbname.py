from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTbname:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_tbname(self):
        """Pseudo column tbname
        
        1. Create 1 database 1 stable 2000 child tables
        2. Insert 10 rows for each child table
        3. select tbname from stable with tbname in (...) 
        4. select tbname from child table with tbname in (...) 
        5. select tbname from stable with tbname in (...) and other tag filtering 
        6. select tbname from stable with tbname in (...) and group by tag 
        7. select tbname from stable with duplicated tbnames in (...) 
        8. select tbname from stable with wrong tbnames in (...) 
        9. select tbname from stable with tbname in (...) and column filtering 
        10. select tbname from stable with tbname in (...) with Upper case table name
        11. Restart dnode and check tbname in query again
        

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-20 Simon Guan Migrated from tsim/parser/tbnameIn.sim

        """

        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ti_db"
        tbPrefix = "ti_tb"
        stbPrefix = "ti_stb"
        tbNum = 2000
        rowNum = 10
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tsu = rowNum * delta
        tsu = tsu - delta
        tsu = tsu + ts0

        tdLog.info(f"========== tbnameIn.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 int, t2 nchar(20), t3 binary(20), t4 bigint, t5 smallint, t6 double)"
        )

        i = 0
        ts = ts0
        halfNum = tbNum / 2
        while i < halfNum:
            i1 = i + int(halfNum)
            tb = tbPrefix + str(i)
            tb1 = tbPrefix + str(i1)
            tgstr = "'tb" + str(i) + "'"
            tgstr1 = "'tb" + str(i1) + "'"
            tdSql.execute(
                f"create table {tb} using {stb} tags( {i} , {tgstr} , {tgstr} , {i} , {i} , {i} )"
            )
            tdSql.execute(
                f"create table {tb1} using {stb} tags( {i1} , {tgstr1} , {tgstr1} , {i} , {i} , {i} )"
            )

            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                c = x / 10
                c = c * 10
                c = x - c
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ( {ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} )  {tb1} values ( {ts} , {c} , NULL , {c} , NULL , {c} , {c} , true, {binary} , {nchar} )"
                )
                x = x + 1

            i = i + 1

        tdLog.info(f"====== tables created")

        self.tbnameIn_query()

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        self.tbnameIn_query()

    def tbnameIn_query(self):
        dbPrefix = "ti_db"
        tbPrefix = "ti_tb"
        stbPrefix = "ti_stb"
        tbNum = 2000
        rowNum = 10
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tsu = rowNum * delta
        tsu = tsu - delta
        tsu = tsu + ts0

        tdLog.info(f"========== tbnameIn_query.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)
        tb = tbPrefix + "0"
        tdLog.info(f"====== use db")
        tdSql.execute(f"use {db}")

        #### tbname in + other tag filtering
        ## [TBASE-362]
        # tbname in + tag filtering is allowed now!!!
        tdSql.query(
            f"select count(*) from {stb} where tbname in ('ti_tb1', 'ti_tb300') and t1 > 2"
        )

        # tbname in used on meter
        tdSql.query(f"select count(*) from {tb} where tbname in ('ti_tb1', 'ti_tb300')")

        ## tbname in + group by tag
        tdSql.query(
            f"select count(*), t1 from {stb} where tbname in ('ti_tb1', 'ti_tb300') group by t1 order by t1 asc"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, rowNum)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 0, rowNum)

        tdSql.checkData(1, 1, 300)

        ## duplicated tbnames
        tdSql.query(
            f"select count(*), t1 from {stb} where tbname in ('ti_tb1', 'ti_tb1', 'ti_tb1', 'ti_tb2', 'ti_tb2', 'ti_tb3') group by t1 order by t1 asc"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, rowNum)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 0, rowNum)

        tdSql.checkData(1, 1, 2)

        tdSql.checkData(2, 0, rowNum)

        tdSql.checkData(2, 1, 3)

        ## wrong tbnames
        tdSql.query(
            f"select count(*), t1 from {stb} where tbname in ('tbname in', 'ti_tb1', 'ti_stb0') group by t1 order by t1"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 10)

        tdSql.checkData(0, 1, 1)

        ## tbname in + colummn filtering
        tdSql.query(
            f"select count(*), t1 from {stb} where tbname in ('tbname in', 'ti_tb1', 'ti_stb0', 'ti_tb2') and c8 like 'binary%' group by t1 order by t1 asc"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 10)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 0, 10)

        tdSql.checkData(1, 1, 2)

        ## tbname in can accpet Upper case table name
        tdSql.query(
            f"select count(*), t1 from {stb} where tbname in ('ti_tb0', 'TI_tb1', 'TI_TB2') group by t1 order by t1"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 10)

        tdSql.checkData(0, 1, 0)

        # multiple tbname in is not allowed NOW
        tdSql.query(
            f"select count(*), t1 from {stb} where tbname in ('ti_tb1', 'ti_tb300') and tbname in ('ti_tb5', 'ti_tb1000') group by t1 order by t1 asc"
        )
