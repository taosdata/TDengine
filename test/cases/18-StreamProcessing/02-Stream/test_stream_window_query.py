import time

from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamTableType,
    StreamTable,
    StreamCheckItem,
)

class TestStreamWindowQuery:
    dbname = "test"
    subTblNum = 3
    tblRowNum = 10
    tableList = []
    
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
            
    def test_stream_window_query(self):
        """Stream window query

        1. Test the stream using window query for the calculation statement.
        2. The select in the query may not use aggregate functions 
        3. The select in the query only uses constants 
        4. Other combinations of the select list in the query


        Since: v3.3.8.9

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-02 xsRen Created

        """

        tdStream.createSnode()
        
        self.prepareData()

        streams = []
        streams.append(self.Basic0()) # OK

        tdStream.checkAll(streams)

    def prepareData(self):
        tdLog.info(f"prepare data")

        tdStream.dropAllStreamsAndDbs()
        time.sleep(1)
        tdStream.init_database(self.dbname)

        st = StreamTable(self.dbname, "st", StreamTableType.TYPE_SUP_TABLE)
        st.createTable(3)
        st.append_data(0, self.tblRowNum)

        self.tableList.append("st")
        for i in range(0, self.subTblNum + 1):
            self.tableList.append(f"st_{i}")

        ntb = StreamTable(self.dbname, "ntb", StreamTableType.TYPE_NORMAL_TABLE)
        ntb.createTable()
        ntb.append_data(0, self.tblRowNum)
        self.tableList.append(f"ntb")
        
    class Basic0(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb0"
            self.stbName = "stb"

        def create(self):
            print(f"=== Basic0 Create {self.db} ===")
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists {self.stbName} (cts timestamp, cint int) tags (tint int)")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using stb tags(1)")
            tdSql.execute(f"create table ct2 using stb tags(2)")
            tdSql.execute(f"create table ct3 using stb tags(3)")
            tdSql.execute(f"create table ct4 using stb tags(4)")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)
            createStreamSql = f"create stream s0_1 count_window(3,3,cint) from ct1 into res_ct_1 as select _twstart, 1 from %%trows count_window(2)"
            tdSql.execute(createStreamSql)
            createStreamSql = f"create stream s0_2 count_window(3,3,cint) from ct1 into res_ct_2 as select _twstart, 1 from test.st_0 count_window(2)"
            tdSql.execute(createStreamSql)
            createStreamSql = f"create stream s0_3 count_window(3,3,cint) from ct1 into res_ct_3 as select _twstart, 1 from %%trows session(cts, 2)"
            tdSql.execute(createStreamSql)
            createStreamSql = f"create stream s0_4 count_window(3,3,cint) from ct1 into res_ct_4 as select _twstart, 1 from test.st session(cts, 2);"
            tdSql.execute(createStreamSql)
            createStreamSql = f"create stream s0_5 count_window(3,3,cint) from ct1 into res_ct_5 as select _twstart, 1 from %%trows interval(3s)"
            tdSql.execute(createStreamSql)
            createStreamSql = f"create stream s0_6 count_window(3,3,cint) from ct1 into res_ct_6 as select _twstart, 1 from test.st interval(3s)"
            tdSql.execute(createStreamSql)
            createStreamSql = f"create stream s0_7 count_window(3,3,cint) from ct1 into res_ct_7 as select _twstart, 1 from %%trows state_window(cint)"
            tdSql.execute(createStreamSql)
            createStreamSql = f"create stream s0_8 count_window(3,3,cint) from ct1 into res_ct_8 as select _twstart, 1 from test.st_0 state_window(cint)"
            tdSql.execute(createStreamSql)
            createStreamSql = f"create stream s0_9 count_window(3,3,cint) from ct1 into res_ct_9 as select _twstart, 1 from %%trows event_window start with cint > 2 end with cint < 8"
            tdSql.execute(createStreamSql)
            createStreamSql = f"create stream s0_10 count_window(3,3,cint) from ct1 into res_ct_10 as select _twstart, 1 from test.st_0 event_window start with cint > 2 end with cint < 8"
            tdSql.execute(createStreamSql)
            
            
            createStreamSql = f"create stream s0_x count_window(3,3,cint) from ct1 into res_ct_1 as select 1 from %%trows count_window(2)"
            tdSql.error(createStreamSql)

        def insert1(self):
            sqls = [
                "insert into ct1 values ('2025-01-01 00:00:00', 1);",
                "insert into ct1 values ('2025-01-01 00:00:01', 2);",
                "insert into ct1 values ('2025-01-01 00:00:02', 3);",
                "insert into ct1 values ('2025-01-01 00:00:03', 4);",
                "insert into ct1 values ('2025-01-01 00:00:04', 5);",
                "insert into ct1 values ('2025-01-01 00:00:05', 6);",
                "insert into ct1 values ('2025-01-01 00:00:06', 7);",
                "insert into ct1 values ('2025-01-01 00:00:07', 8);",
                "insert into ct1 values ('2025-01-01 00:00:08', 9);",
                "insert into ct1 values ('2025-01-01 00:00:09', 10);",    
                        
                "insert into ct2 values ('2025-01-01 00:00:00', 1);",
                "insert into ct2 values ('2025-01-01 00:00:01', 2);",
                "insert into ct2 values ('2025-01-01 00:00:02', 3);",
                "insert into ct2 values ('2025-01-01 00:00:03', 4);",
                "insert into ct2 values ('2025-01-01 00:00:04', 5);",
                "insert into ct2 values ('2025-01-01 00:00:05', 6);",
                "insert into ct2 values ('2025-01-01 00:00:06', 7);",
                "insert into ct2 values ('2025-01-01 00:00:07', 8);",
                "insert into ct2 values ('2025-01-01 00:00:08', 9);",
                "insert into ct2 values ('2025-01-01 00:00:09', 10);", 

                "insert into ct3 values ('2025-01-01 00:00:00', 1);",
                "insert into ct3 values ('2025-01-01 00:00:01', 2);",
                "insert into ct3 values ('2025-01-01 00:00:02', 3);",
                "insert into ct3 values ('2025-01-01 00:00:03', 4);",
                "insert into ct3 values ('2025-01-01 00:00:04', 5);",
                "insert into ct3 values ('2025-01-01 00:00:05', 6);",
                "insert into ct3 values ('2025-01-01 00:00:06', 7);",
                "insert into ct3 values ('2025-01-01 00:00:07', 8);",
                "insert into ct3 values ('2025-01-01 00:00:08', 9);",
                "insert into ct3 values ('2025-01-01 00:00:09', 10);", 

                "insert into ct4 values ('2025-01-01 00:00:00', 1);",
                "insert into ct4 values ('2025-01-01 00:00:01', 2);",
                "insert into ct4 values ('2025-01-01 00:00:02', 3);",
                "insert into ct4 values ('2025-01-01 00:00:03', 4);",
                "insert into ct4 values ('2025-01-01 00:00:04', 5);",
                "insert into ct4 values ('2025-01-01 00:00:05', 6);",
                "insert into ct4 values ('2025-01-01 00:00:06', 7);",
                "insert into ct4 values ('2025-01-01 00:00:07', 8);",
                "insert into ct4 values ('2025-01-01 00:00:08', 9);",
                "insert into ct4 values ('2025-01-01 00:00:09', 10);",  
            ]
            tdSql.executes(sqls)

        def check1(self):
            for i in range(1, 11):
                result_sql = f"select * from {self.db}.res_ct_{i}"
                tdSql.checkResultsByFunc(
                    sql=result_sql,
                    func=lambda: tdSql.getRows() == 3
                    and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
                    and tdSql.compareData(0, 1, 1)
                    and tdSql.compareData(1, 0, "2025-01-01 00:00:03.000")
                    and tdSql.compareData(1, 1, 1)
                    and tdSql.compareData(2, 0, "2025-01-01 00:00:06.000")
                    and tdSql.compareData(2, 1, 1)
                )
