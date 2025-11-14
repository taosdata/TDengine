from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestFilterSma:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_filter_sma(self):
        """Filter sma

        1. Create db with STT_TRIGGER option and set value to 1, will flush data to disk easily
        2. Create supper table and sub table
        3. Insert some data into sub table
        4. Flush database, the action will trigger the data to be written to disk
        5. Query the sub table with filter condition on flag column
        6. Check the number of rows returned by the query

        Catalog:
            - Query:Filter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: TS-6650

        History:
            - 2025-6-20 Ethan liu add test for sma filter

        """

        db = "sma_db"
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== start sma_filter")

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} STT_TRIGGER 1")
        tdSql.execute(f"use {db}")

        tdSql.execute(f"create table st1 (ts timestamp, flag SMALLINT, flag_varchar VARCHAR(10),flag_nchar NCHAR(10), flag_binary BINARY(10), flag_varbinary VARBINARY(10), flag_geometry GEOMETRY(50)) tags (t1 VARCHAR(10))")
        tdSql.execute(f"create table tb1 using st1 tags('t1')")

        insertCount = 1000
        while insertCount > 0:
            tdSql.execute(f"insert into tb1 values ( {ts0} , 1,'1','1','1','1','POINT(1 1)')")
            ts0 += delta
            insertCount -= 1

        tdLog.info(f"========== insert data into tb1 successfully, total rows: {1000}")

        insertCount = 500
        while insertCount > 0:
            tdSql.execute(f"insert into tb1 values ( {ts0} , 2,'2','2','2','2','POINT(2 2)')")
            ts0 += delta
            insertCount -= 1

        tdLog.info(f"========== insert data into tb1 successfully, total rows: {1500}")

        insertCount = 500
        while insertCount > 0:
            tdSql.execute(f"insert into tb1 values ( {ts0} , 3,'a0','b0','c0','d0','POINT(3 3)')")
            ts0 += delta
            insertCount -= 1

        tdLog.info(f"========== insert data into tb1 successfully, total rows: {2000}")

        insertCount = 500
        while insertCount > 0:
            tdSql.execute(f"insert into tb1 values ( {ts0} , 0,'0','0','0','0','POINT(0 0)')")
            ts0 += delta
            insertCount -= 1

        tdLog.info(f"========== insert data into tb1 successfully, total rows: {2500}")

        tdSql.execute(f"flush database {db}")
        tdLog.info(f"========== flush database {db} successfully")

        # filter on flag column
        tdSql.query(f"select * from tb1 where flag in ('1')")
        tdSql.checkRows(1000)

        tdSql.query(f"select count(*) from tb1 where flag in ('1')")
        tdSql.checkData(0, 0, 1000)

        tdSql.query(f"select * from tb1 where flag in (1)")
        tdSql.checkRows(1000)

        tdSql.query(f"select count(*) from tb1 where flag in (1)")
        tdSql.checkData(0, 0, 1000)

        tdSql.query(f"select * from tb1 where flag in (2)")
        tdSql.checkRows(500)

        tdSql.query(f"select count(*) from tb1 where flag in (2)")
        tdSql.checkData(0, 0, 500)

        tdSql.query(f"select * from tb1 where flag in ('2')")
        tdSql.checkRows(500)

        tdSql.query(f"select count(*) from tb1 where flag in ('2')")
        tdSql.checkData(0, 0, 500)

        tdSql.query(f"select * from tb1 where flag in (3)")
        tdSql.checkRows(500)

        tdSql.query(f"select count(*) from tb1 where flag in (3)")
        tdSql.checkData(0, 0, 500)

        tdSql.query(f"select * from tb1 where flag in ('3')")
        tdSql.checkRows(500)

        tdSql.query(f"select count(*) from tb1 where flag in ('3')")
        tdSql.checkData(0, 0, 500)

        tdSql.query(f"select * from tb1 where flag in (4)")
        tdSql.checkRows(0)

        tdSql.query(f"select count(*) from tb1 where flag in (4)")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select * from tb1 where flag in ('4')")
        tdSql.checkRows(0)

        tdSql.query(f"select count(*) from tb1 where flag in ('4')")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select * from tb1 where flag in(0)")
        tdSql.checkRows(500)

        tdSql.query(f"select count(*) from tb1 where flag in(0)")
        tdSql.checkData(0,0,500)

        tdSql.query(f"select * from tb1 where flag in('0')")
        tdSql.checkRows(500)

        tdSql.query(f"select count(*) from tb1 where flag in('0')")
        tdSql.checkData(0,0,500)

        # Special case
        tdSql.query(f"select * from tb1 where flag in ('d4')")
        tdSql.checkRows(500)

        # Special case
        tdSql.query(f"select count(*) from tb1 where flag in ('d4')")
        tdSql.checkData(0, 0, 500)

        tdSql.query(f"select * from tb1 where flag == 0")
        tdSql.checkRows(500)

        tdSql.query(f"select count(*) from tb1 where flag == 0")
        tdSql.checkData(0, 0, 500)

        tdSql.query(f"select * from tb1 where flag == '0'")
        tdSql.checkRows(500)

        tdSql.query(f"select count(*) from tb1 where flag == '0'")
        tdSql.checkData(0, 0, 500)

        tdSql.query(f"select * from tb1 where flag == 1")
        tdSql.checkRows(1000)

        tdSql.query(f"select count(*) from tb1 where flag == 1")
        tdSql.checkData(0, 0, 1000)

        tdSql.query(f"select * from tb1 where flag == '1'")
        tdSql.checkRows(1000)

        tdSql.query(f"select count(*) from tb1 where flag == '1'")
        tdSql.checkData(0, 0, 1000)

        tdSql.query(f"select * from tb1 where flag == 4")
        tdSql.checkRows(0)

        tdSql.query(f"select count(*) from tb1 where flag == 4")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select * from tb1 where flag == '4'")
        tdSql.checkRows(0)

        tdSql.query(f"select count(*) from tb1 where flag == '4'")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select * from tb1 where flag > 1")
        tdSql.checkRows(1000)

        tdSql.query(f"select count(*) from tb1 where flag > 1")
        tdSql.checkData(0, 0, 1000)

        tdSql.query(f"select * from tb1 where flag > '1'")
        tdSql.checkRows(1000)

        tdSql.query(f"select count(*) from tb1 where flag > '1'")
        tdSql.checkData(0, 0, 1000)

        tdSql.query(f"select * from tb1 where flag >= 1")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag >= 1")
        tdSql.checkData(0, 0, 2000)

        tdSql.query(f"select * from tb1 where flag >= '1'")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag >= '1'")
        tdSql.checkData(0, 0, 2000)

        tdSql.query(f"select * from tb1 where flag < 3")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag < 3")
        tdSql.checkData(0, 0, 2000)

        tdSql.query(f"select * from tb1 where flag < '3'")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag < '3'")
        tdSql.checkData(0, 0, 2000)

        tdSql.query(f"select * from tb1 where flag <= 2")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag <= 2")
        tdSql.checkData(0, 0, 2000)

        tdSql.query(f"select * from tb1 where flag <= '2'")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag <= '2'")
        tdSql.checkData(0, 0, 2000)

        tdSql.query(f"select * from tb1 where flag != 1")
        tdSql.checkRows(1500)

        tdSql.query(f"select count(*) from tb1 where flag != 1")
        tdSql.checkData(0, 0, 1500)

        tdSql.query(f"select * from tb1 where flag != '1'")
        tdSql.checkRows(1500)

        tdSql.query(f"select count(*) from tb1 where flag != '1'")
        tdSql.checkData(0, 0, 1500)

        tdSql.query(f"select * from tb1 where flag != 4")
        tdSql.checkRows(2500)

        tdSql.query(f"select count(*) from tb1 where flag != 4")
        tdSql.checkData(0, 0, 2500)

        tdSql.query(f"select * from tb1 where flag != '4'")
        tdSql.checkRows(2500)

        tdSql.query(f"select count(*) from tb1 where flag != '4'")
        tdSql.checkData(0, 0, 2500)

        tdSql.query(f"select * from tb1 where flag < 'd4'")
        tdSql.checkRows(0)

        tdSql.query(f"select count(*) from tb1 where flag != 'd4'")
        tdSql.checkData(0, 0, 2000)

        tdSql.query(f"select * from tb1 where flag in ('1', '2', '3')")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag in ('1', '2', '3')")
        tdSql.checkData(0, 0, 2000)

        tdSql.query(f"select * from tb1 where flag in (1, '2', 'd','0')")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag in (1, '2', 'd','0')")
        tdSql.checkData(0, 0, 2000)

        tdSql.query(f"select * from tb1 where flag > 1 and flag < 3")
        tdSql.checkRows(500)

        tdSql.query(f"select count(*) from tb1 where flag > 1 and flag < 3")
        tdSql.checkData(0, 0, 500)

        tdSql.query(f"select * from tb1 where flag > '1' and flag < '3'")
        tdSql.checkRows(500)

        tdSql.query(f"select count(*) from tb1 where flag > '1' and flag < '3'")
        tdSql.checkData(0, 0, 500)

        tdSql.query(f"select * from tb1 where flag >= 1 and flag <= 2")
        tdSql.checkRows(1500)

        tdSql.query(f"select count(*) from tb1 where flag >= 1 and flag <= 2")
        tdSql.checkData(0, 0, 1500)

        tdSql.query(f"select * from tb1 where flag >= '1' and flag <= '2'")
        tdSql.checkRows(1500)

        tdSql.query(f"select count(*) from tb1 where flag >= '1' and flag <= '2'")
        tdSql.checkData(0, 0, 1500)

        tdSql.query(f"select * from tb1 where flag < 2 or flag > 3")
        tdSql.checkRows(1500)

        tdSql.query(f"select count(*) from tb1 where flag < 2 or flag > 3")
        tdSql.checkData(0, 0, 1500)

        tdSql.query(f"select * from tb1 where flag < '2' or flag > 3")
        tdSql.checkRows(1500)

        tdSql.query(f"select count(*) from tb1 where flag < '2' or flag > 3")
        tdSql.checkData(0, 0, 1500)

        # filter on flag_varchar column
        tdSql.query(f"select * from tb1 where flag_varchar in ('1')")
        tdSql.checkRows(1000)

        tdSql.query(f"select count(*) from tb1 where flag_varchar in ('1')")
        tdSql.checkData(0, 0, 1000)

        tdSql.query(f"select * from tb1 where flag_varchar in (1)")
        tdSql.checkRows(1000)

        tdSql.query(f"select count(*) from tb1 where flag_varchar in (1)")
        tdSql.checkData(0, 0, 1000)

        tdSql.query(f"select * from tb1 where flag_varchar in (148)")
        tdSql.checkRows(0)

        tdSql.query(f"select count(*) from tb1 where flag_varchar in (148)")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select * from tb1 where flag_varchar in ('2')")
        tdSql.checkRows(500)

        tdSql.query(f"select count(*) from tb1 where flag_varchar in ('2')")
        tdSql.checkData(0, 0, 500)

        tdSql.query(f"select * from tb1 where flag_varchar in ('d6')")
        tdSql.checkRows(0)

        tdSql.query(f"select count(*) from tb1 where flag_varchar in ('d6')")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select * from tb1 where flag_varchar in ('3')")
        tdSql.checkRows(0)

        tdSql.query(f"select count(*) from tb1 where flag_varchar in ('3')")
        tdSql.checkData(0,0,0)

        tdSql.query(f"select count(*) from tb1 where flag_varchar in ('0','3','g9')")
        tdSql.checkData(0, 0, 500)

        tdSql.query(f"select * from tb1 where flag_varchar < 'g5'")
        tdSql.checkRows(2500)

        tdSql.query(f"select count(*) from tb1 where flag_varchar < 'g5'")
        tdSql.checkData(0, 0, 2500)

        tdSql.query(f"select * from tb1 where flag_varchar > 'g5'")
        tdSql.checkRows(0)

        tdSql.query(f"select count(*) from tb1 where flag_varchar > 'g5'")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select * from tb1 where flag_varchar != 'g5'")
        tdSql.checkRows(2500)

        tdSql.query(f"select count(*) from tb1 where flag_varchar != 'g5'")
        tdSql.checkData(0, 0, 2500)

        tdSql.query(f"select * from tb1 where flag_varchar != '1'")
        tdSql.checkRows(1500)

        tdSql.query(f"select count(*) from tb1 where flag_varchar != '1'")
        tdSql.checkData(0, 0, 1500)

        tdSql.query(f"select * from tb1 where flag_varchar >= '0'")
        tdSql.checkRows(2500)

        tdSql.query(f"select count(*) from tb1 where flag_varchar >= '0'")
        tdSql.checkData(0, 0, 2500)

        tdSql.query(f"select * from tb1 where flag_varchar <= 'd4'")
        tdSql.checkRows(2500)

        tdSql.query(f"select count(*) from tb1 where flag_varchar <= 'd4'")
        tdSql.checkData(0, 0, 2500)

        tdSql.query(f"select * from tb1 where flag_varchar > '1' and flag_varchar < '3'")
        tdSql.checkRows(500)

        tdSql.query(f"select count(*) from tb1 where flag_varchar > '1' and flag_varchar < '3'")
        tdSql.checkData(0, 0, 500)

        tdSql.query(f"select * from tb1 where flag_varchar >= '1' and flag_varchar <= '2'")
        tdSql.checkRows(1500)

        tdSql.query(f"select count(*) from tb1 where flag_varchar >= '1' and flag_varchar <= '2'")
        tdSql.checkData(0, 0, 1500)

        tdSql.query(f"select * from tb1 where flag_varchar < '2' or flag_varchar > '3'")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag_varchar < '2' or flag_varchar > '3'")
        tdSql.checkData(0, 0, 2000)

        # filter on flag_nchar column
        tdSql.query(f"select * from tb1 where flag_nchar in ('1')")
        tdSql.checkRows(1000)

        tdSql.query(f"select count(*) from tb1 where flag_nchar in ('1')")
        tdSql.checkData(0,0,1000)

        tdSql.query(f"select count(*) from tb1 where flag_nchar in (1)")
        tdSql.checkData(0,0,1000)

        tdSql.query(f"select * from tb1 where flag_nchar in (1)")
        tdSql.checkRows(1000)

        tdSql.query(f"select * from tb1 where flag_nchar in ('0','h7','gf','3')")
        tdSql.checkRows(500)

        tdSql.query(f"select count(*) from tb1 where flag_nchar in ('0','h7','gf','3')")
        tdSql.checkData(0,0,500)

        tdSql.query(f"select * from tb1 where flag_nchar not in ('2')")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag_nchar not in ('2')")
        tdSql.checkData(0, 0, 2000)

        tdSql.query(f"select * from tb1 where flag_nchar > '0' and flag_nchar < '4'")
        tdSql.checkRows(1500)

        tdSql.query(f"select count(*) from tb1 where flag_nchar > '0' and flag_nchar < '4'")
        tdSql.checkData(0,0,1500)

        tdSql.query(f"select * from tb1 where flag_nchar >= 'a0'")
        tdSql.checkRows(500)

        tdSql.query(f"select count(*) from tb1 where flag_nchar >= 'a0'")
        tdSql.checkData(0,0,500)

        tdSql.query(f"select * from tb1 where flag_nchar >= 'g8'")
        tdSql.checkRows(0)

        tdSql.query(f"select count(*) from tb1 where flag_nchar >= 'g8'")
        tdSql.checkData(0,0,0)

        tdSql.query(f"select * from tb1 where flag_nchar == 'b0'")
        tdSql.checkRows(500)

        tdSql.query(f"select count(*) from tb1 where flag_nchar == 'b0'")
        tdSql.checkData(0,0,500)

        tdSql.query(f"select * from tb1 where flag_nchar == 't0'")
        tdSql.checkRows(0)

        tdSql.query(f"select count(*) from tb1 where flag_nchar == 't0'")
        tdSql.checkData(0,0,0)

        tdSql.query(f"select * from tb1 where flag_nchar != '0'")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag_nchar != '0'")
        tdSql.checkData(0,0,2000)

        tdSql.query(f"select * from tb1 where flag_nchar != 'hi'")
        tdSql.checkRows(2500)

        tdSql.query(f"select count(*) from tb1 where flag_nchar != 'hi'")
        tdSql.checkData(0,0,2500)

        tdSql.query(f"select * from tb1 where flag_nchar <= 't6'")
        tdSql.checkRows(2500)

        tdSql.query(f"select count(*) from tb1 where flag_nchar != 't6'")
        tdSql.checkData(0,0,2500)

        # filter on binary column
        tdSql.query(f"select * from tb1 where flag_binary in ('2')")
        tdSql.checkRows(500)

        tdSql.query(f"select count(*) from tb1 where flag_binary in ('2')")
        tdSql.checkData(0,0,500)

        tdSql.query(f"select count(*) from tb1 where flag_binary in (1)")
        tdSql.checkData(0,0,1000)

        tdSql.query(f"select * from tb1 where flag_binary in (1)")
        tdSql.checkRows(1000)

        tdSql.query(f"select * from tb1 where flag_binary not in ('2')")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag_binary not in ('2')")
        tdSql.checkData(0,0,2000)

        tdSql.query(f"select * from tb1 where flag_binary > '0' and flag_binary < 'd6'")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag_binary > '0' and flag_binary < 'd6'")
        tdSql.checkData(0,0,2000)

        tdSql.query(f"select * from tb1 where flag_binary >= '0'")
        tdSql.checkRows(2500)

        tdSql.query(f"select count(*) from tb1 where flag_binary >= '0'")
        tdSql.checkData(0, 0, 2500)

        tdSql.query(f"select * from tb1 where flag_binary >= 'd4'")
        tdSql.checkRows(0)

        tdSql.query(f"select count(*) from tb1 where flag_binary >= 'd4'")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select * from tb1 where flag_binary == 'd4'")
        tdSql.checkRows(0)

        tdSql.query(f"select count(*) from tb1 where flag_binary == 'd4'")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select * from tb1 where flag_binary != '0'")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag_binary != '0'")
        tdSql.checkData(0, 0, 2000)

        tdSql.query(f"select * from tb1 where flag_binary != 'd4'")
        tdSql.checkRows(2500)

        tdSql.query(f"select count(*) from tb1 where flag_binary != 'd4'")
        tdSql.checkData(0, 0, 2500)

        tdSql.query(f"select * from tb1 where flag_binary <= 'b2'")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag_binary <= 'b2'")
        tdSql.checkData(0, 0, 2000)

        tdSql.query(f"select * from tb1 where flag_binary < '2' or flag_binary >= 'b2'")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag_binary < '2' or flag_binary >= 'b2'")
        tdSql.checkData(0, 0, 2000)

        # filter on varbinary column
        tdSql.query(f"select * from tb1 where flag_varbinary in ('2')")
        tdSql.checkRows(500)

        tdSql.query(f"select count(*) from tb1 where flag_varbinary in ('2')")
        tdSql.checkData(0, 0, 500)

        tdSql.query(f"select * from tb1 where flag_varbinary in (1)")
        tdSql.checkRows(1000)

        tdSql.query(f"select count(*) from tb1 where flag_varbinary in (1)")
        tdSql.checkData(0, 0, 1000)

        tdSql.query(f"select * from tb1 where flag_varbinary in (148)")
        tdSql.checkRows(0)

        tdSql.query(f"select count(*) from tb1 where flag_varbinary in (148)")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select * from tb1 where flag_varbinary not in ('2')")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag_varbinary not in ('2')")
        tdSql.checkData(0, 0, 2000)

        tdSql.query(f"select * from tb1 where flag_varbinary > '0' and flag_varbinary < 'd6'")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag_varbinary > '0' and flag_varbinary < 'd6'")
        tdSql.checkData(0, 0, 2000)

        tdSql.query(f"select * from tb1 where flag_varbinary >= '0'")
        tdSql.checkRows(2500)

        tdSql.query(f"select count(*) from tb1 where flag_varbinary >= '0'")
        tdSql.checkData(0, 0, 2500)

        tdSql.query(f"select * from tb1 where flag_varbinary >= 'd4'")
        tdSql.checkRows(0)

        tdSql.query(f"select count(*) from tb1 where flag_varbinary >= 'd4'")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select * from tb1 where flag_varbinary == 'd4'")
        tdSql.checkRows(0)

        tdSql.query(f"select count(*) from tb1 where flag_varbinary == 'd4'")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select * from tb1 where flag_varbinary != '0'")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag_varbinary != '0'")
        tdSql.checkData(0, 0, 2000)

        tdSql.query(f"select * from tb1 where flag_varbinary != 'd4'")
        tdSql.checkRows(2500)

        tdSql.query(f"select count(*) from tb1 where flag_varbinary != 'd4'")
        tdSql.checkData(0, 0, 2500)

        tdSql.query(f"select * from tb1 where flag_varbinary <= 'c3'")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag_varbinary <= 'c3'")
        tdSql.checkData(0, 0, 2000)

        tdSql.query(f"select * from tb1 where flag_varbinary < '2' or flag_varbinary >= 'c3'")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag_varbinary < '2' or flag_varbinary >= 'c3'")
        tdSql.checkData(0, 0, 2000)

        # filter on flag_geometry column
        tdSql.query(f"select * from tb1 where flag_geometry in ('POINT(1 1)')")
        tdSql.checkRows(1000)

        tdSql.query(f"select count(*) from tb1 where flag_geometry in ('POINT(1 1)')")
        tdSql.checkData(0, 0, 1000)

        tdSql.query(f"select * from tb1 where flag_geometry in ('POINT(2 2)')")
        tdSql.checkRows(500)

        tdSql.query(f"select count(*) from tb1 where flag_geometry in ('POINT(2 2)')")
        tdSql.checkData(0, 0, 500)

        tdSql.query(f"select * from tb1 where flag_geometry in ('POINT(3 5)')")
        tdSql.checkRows(0)

        tdSql.query(f"select count(*) from tb1 where flag_geometry in ('POINT(3 5)')")
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select * from tb1 where flag_geometry not in ('POINT(3 3)')")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag_geometry not in ('POINT(3 3)')")
        tdSql.checkData(0, 0, 2000)

        tdSql.error(f"select * from tb1 where flag_geometry > 'POINT(0 0)' and flag_geometry < 'POINT(3 3)'")
        tdSql.error(f"select count(*) from tb1 where flag_geometry > 'POINT(0 0)' and flag_geometry < 'POINT(3 3)'")

        tdSql.error(f"select * from tb1 where flag_geometry >= 'POINT(0 0)'")
        tdSql.error(f"select count(*) from tb1 where flag_geometry >= 'POINT(0 0)'")

        tdSql.error(f"select * from tb1 where flag_geometry >= 'POINT(2 2)'")
        tdSql.error(f"select count(*) from tb1 where flag_geometry >= 'POINT(2 2)'")

        tdSql.query(f"select * from tb1 where flag_geometry == 'POINT(2 2)'")
        tdSql.checkRows(500)

        tdSql.query(f"select count(*) from tb1 where flag_geometry == 'POINT(2 2)'")
        tdSql.checkData(0, 0, 500)

        tdSql.query(f"select * from tb1 where flag_geometry != 'POINT(0 0)'")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag_geometry != 'POINT(0 0)'")
        tdSql.checkData(0, 0, 2000)

        tdSql.query(f"select * from tb1 where flag_geometry != 'POINT(3 3)'")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag_geometry != 'POINT(3 3)'")
        tdSql.checkData(0, 0, 2000)

        tdSql.error(f"select * from tb1 where flag_geometry <= 'POINT(3 3)'")
        tdSql.error(f"select count(*) from tb1 where flag_geometry <= 'POINT(3 3)'")

        tdSql.error(f"select * from tb1 where flag_geometry < 'POINT(3 3)' or flag_geometry >= 'POINT(4 4)'")
        tdSql.error(f"select count(*) from tb1 where flag_geometry < 'POINT(3 3)' or flag_geometry >= 'POINT(4 4)'")

        tdSql.query(f"select * from tb1 where flag_geometry in ('POINT(1 1)', 'POINT(2 2)', 'POINT(3 3)')")
        tdSql.checkRows(2000)

        tdSql.query(f"select count(*) from tb1 where flag_geometry in ('POINT(1 1)', 'POINT(2 2)', 'POINT(3 3)')")
        tdSql.checkData(0, 0, 2000)

        tdSql.query(f"select * from tb1 where flag_geometry in ('POINT(1 1)', 'POINT(2 2)', 'POINT(4 4)')")
        tdSql.checkRows(1500)

        tdSql.query(f"select count(*) from tb1 where flag_geometry in ('POINT(1 1)', 'POINT(2 2)', 'POINT(4 4)')")
        tdSql.checkData(0, 0, 1500)

        tdSql.error(f"select * from tb1 where flag_geometry > 'POINT(1 1)' and flag_geometry < 'POINT(3 3)'")
        tdSql.error(f"select count(*) from tb1 where flag_geometry > 'POINT(1 1)' and flag_geometry < 'POINT(3 3)'")

        tdLog.info(f"end sma_filter test successfully")