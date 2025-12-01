import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream


class TestStreamEventTrigger:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_event_trigger(self):
        """Trigger mode stable event

        1. Create snode and database
        2. Create super table and sub tables
        3. Create streams with event_window trigger mode
        4. Insert data into source tables
        5. Check stream results

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-07-22

        """

        tdStream.dropAllStreamsAndDbs()
        tdStream.createSnode()

        tdLog.info(f"=============== create database")
        tdSql.prepare(dbname="sdb", vgroups=1)

        tdLog.info(f"=============== create super table")
        tdSql.execute(f"create stable stb (cts timestamp, cint int, cuint int unsigned) tags(tint int);")

        tdLog.info(f"=============== create sub table")
        tdSql.execute(f"create table ct1 using stb tags(1);")
        tdSql.execute(f"create table ct2 using stb tags(2);")
        tdSql.execute(f"create table ct3 using stb tags(1);")
        tdSql.execute(f"create table ct4 using stb tags(2);")
        
        tdLog.info(f"=============== create stream")
        sql1 = "create stream s1\
                event_window(start with cint > 1  end with cuint < 5)\
                from ct1 into res_ct1 (firstts, lastts, num_v, cnt_v, avg_v)\
                as select first(_c0), last_row(_c0), _twrownum, count(*), avg(cuint) from %%trows;"
        sql2 = "create stream s2\
                event_window(start with cint > 1  end with cuint < 5) true_for(3s) from ct2\
                into res_ct2 (firstts, lastts, num_v, cnt_v, avg_v)\
                as select first(_c0), last_row(_c0), _twrownum, count(*), avg(cuint) from %%trows;"
        sql3 = "create stream s3\
                event_window(start with cint > 1  end with cuint < 5) true_for(3s) from stb\
                partition by tbname into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, num_v, cnt_v, avg_v)\
                as select first(_c0), last_row(_c0), _twrownum, count(*), avg(cuint) from %%trows;"
         
        streams = [
            self.StreamItem(sql1, self.checks1),
            self.StreamItem(sql2, self.checks2),
            self.StreamItem(sql3, self.checks3),
            # self.StreamItem(sql4, self.checks4),
        ]

        for stream in streams:
            tdSql.execute(stream.sql)
        tdStream.checkStreamStatus()
        # time.sleep(3)

        tdLog.info(f"=============== write query data")
        sqls = [
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:00', 0, 9);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:01', 1, 8);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:02', 2, 7);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:03', 3, 6);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:04', 4, 4);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:05', 0, 2);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:06', 1, 2);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:07', 2, 1);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:08', 3, 8);",
            "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:09', 4, 4);",
            
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:00', 2, 9);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:01', 1, 9);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:02', 1, 9);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:03', 1, 4);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:04', 2, 9);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:05', 2, 9);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:06', 2, 4);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:07', 3, 1);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:08', 4, 9);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:09', 4, 9);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:10', 4, 9);",
            "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:11', 4, 4);",
            
            "insert into ct3 using stb tags(2) values ('2025-01-01 00:00:00', 2, 9);",
            "insert into ct3 using stb tags(2) values ('2025-01-01 00:00:01', 1, 9);",
            "insert into ct3 using stb tags(2) values ('2025-01-01 00:00:02', 1, 9);",
            "insert into ct3 using stb tags(2) values ('2025-01-01 00:00:03', 1, 4);",
            "insert into ct3 using stb tags(2) values ('2025-01-01 00:00:04', 2, 9);",
            "insert into ct3 using stb tags(2) values ('2025-01-01 00:00:05', 2, 9);",
            "insert into ct3 using stb tags(2) values ('2025-01-01 00:00:06', 2, 4);",
            "insert into ct3 using stb tags(2) values ('2025-01-01 00:00:07', 3, 1);",
            "insert into ct3 using stb tags(2) values ('2025-01-01 00:00:08', 4, 9);",
            "insert into ct3 using stb tags(2) values ('2025-01-01 00:00:09', 4, 9);",
            "insert into ct3 using stb tags(2) values ('2025-01-01 00:00:10', 4, 9);",
            "insert into ct3 using stb tags(2) values ('2025-01-01 00:00:11', 4, 4);",
            
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:00', 0, 0);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:01', 0, 0);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:02', 1, 1);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:03', 1, 1);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:04', 1, 1);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:05', 2, 2);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:06', 2, 2);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:07', 2, 2);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:08', 2, 2);",
            "insert into ct4 using stb tags(2) values ('2025-01-01 00:00:09', 3, 3);",
        ]
        tdSql.executes(sqls)
        tdSql.query("select _wstart, count(cint), avg(cint) from stb partition by tbname state_window(cint)")
        tdSql.printResult()

        tdLog.info(f"=============== check stream result")
        for stream in streams:
            stream.check()


        ############ true_for 
        # sql5 = "create stream s5 state_window(cint) true_for(5s) from ct1 into res_truefor_ct1 (firstts, num_v, cnt_v, avg_v) as select first(_c0), _twrownum, count(*), avg(cuint) from %%trows;"
         
        # tdSql.execute(sql5)
        # tdStream.checkStreamStatus("s5")
        # # time.sleep(3)
                
        # tdLog.info(f"=============== continue write data into ct1 for true_for(5s)")
        # sqls = [
        #     "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:10', 4, 0);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:11', 4, 0);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:15', 4, 1);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:16', 5, 1);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:17', 5, 1);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:18', 5, 2);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:21', 5, 2);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:22', 6, 2);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:29', 6, 2);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:30', 7, 3);",
        # ]
        # tdSql.executes(sqls)
        # self.checks5(1)
        
        # ############ option: fill history
        # # no set start_time
        # sql6 = "create stream s6 state_window(cint) from ct1 stream_options(fill_history) into res_fill_all_ct1 (firstts, num_v, cnt_v, avg_v) as select first(_c0), _twrownum, count(*), avg(cuint) from %%trows;"
         
        # tdSql.execute(sql6)
        # tdStream.checkStreamStatus("s6")
        # # time.sleep(3)
                
        # tdLog.info(f"=============== continue write data into ct1 for new real data ")
        # sqls = [
        #     "insert into ct1 using stb tags(1) values ('2025-01-02 00:00:10', 4, 0);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-02 00:00:11', 4, 0);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-02 00:00:15', 4, 1);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-02 00:00:16', 5, 1);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-02 00:00:17', 5, 1);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-02 00:00:18', 5, 2);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-02 00:00:21', 5, 2);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-02 00:00:22', 6, 2);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-02 00:00:29', 6, 2);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-02 00:00:30', 7, 3);",
        # ]
        # tdSql.executes(sqls)
        # self.checks6(1)
        
        # # set start_time
        # sql7 = "create stream s7 state_window(cint) true_for(5s) from ct1 stream_options(fill_history('2025-01-02 00:00:10')) into res_fill_part_ct1 (firstts, num_v, cnt_v, avg_v) as select first(_c0), _twrownum, count(*), avg(cuint) from %%trows;"
        # tdSql.execute(sql7)
        # tdStream.checkStreamStatus("s7")
        # # time.sleep(3)
                
        # tdLog.info(f"=============== continue write data into ct1 for new real data ")
        # sqls = [
        #     "insert into ct1 using stb tags(1) values ('2025-01-03 00:00:10', 4, 0);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-03 00:00:11', 4, 0);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-03 00:00:15', 4, 1);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-03 00:00:16', 5, 1);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-03 00:00:17', 5, 1);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-03 00:00:18', 5, 2);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-03 00:00:21', 5, 2);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-03 00:00:22', 6, 2);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-03 00:00:29', 6, 2);",
        #     "insert into ct1 using stb tags(1) values ('2025-01-03 00:00:30', 7, 3);",
        # ]
        # tdSql.executes(sqls)
        # self.checks6(2)       
        
        # ############ option: max_delay
        # # no  true_for
        # tdLog.info(f"=============== create sub table")
        # tdSql.execute(f"create table ct5 using stb tags(1);")
        
        # sql8 = "create stream s8 state_window(cint) from ct5 stream_options(max_delay(3s)) into res_max_delay_ct5 (lastts, firstts, num_v, cnt_v, avg_v) as select last_row(_c0), first(_c0), _twrownum, count(*), avg(cuint) from %%trows;"
         
        # tdSql.execute(sql8)
        # tdStream.checkStreamStatus("s8")
        # time.sleep(10)
                
        # tdLog.info(f"=============== continue write data into ct5 for new real data ")
        # sqls = [
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:10', 1, 0);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:11', 1, 0);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:12', 1, 1);",
        # ]
        # tdLog.info(f"=============== 3 rows ")
        # tdSql.executes(sqls)
        # time.sleep(7)   
        # sqls = [
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:13', 1, 1);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:14', 1, 1);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:15', 2, 2);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:16', 2, 2);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:17', 2, 2);",
        # ]
        # tdLog.info(f"=============== 5 rows: 17 ")
        # tdSql.executes(sqls)     
        # time.sleep(7)     
        # sqls = [
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:18', 2, 2);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:19', 2, 2);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:20', 1, 1);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:21', 1, 1);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:22', 1, 1);",
        # ]
        # tdLog.info(f"=============== 5 rows: 22 ")
        # tdSql.executes(sqls)   
        # time.sleep(7)      
        # sqls = [
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:23', 1, 1);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:24', 1, 1);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:25', 2, 2);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:26', 2, 2);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:27', 2, 2);",
        # ]
        # tdLog.info(f"=============== 5 rows: 27 ")
        # tdSql.executes(sqls)   
        # time.sleep(7)      
        # sqls = [
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:28', 2, 2);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:29', 2, 2);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:30', 3, 3);",
        # ]
        # tdLog.info(f"=============== 5 rows: 30 ")
        # tdSql.executes(sqls)  
        # time.sleep(1)      
        # self.checks6(3)
        
        # max_delay + true_for
        
        ############ option: watermark
        
        return

    def checks1(self):
        result_sql = "select firstts, lastts, num_v, cnt_v, avg_v from res_ct1"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 3
            and tdSql.compareData(0, 0, "2025-01-01 00:00:02.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:04.000")
            and tdSql.compareData(0, 2, 3)
            and tdSql.compareData(0, 3, 3)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:07.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:07.000")
            and tdSql.compareData(1, 2, 1)
            and tdSql.compareData(1, 3, 1)
            and tdSql.compareData(2, 0, "2025-01-01 00:00:08.000")
            and tdSql.compareData(2, 1, "2025-01-01 00:00:09.000"),
        )

        tdSql.query("desc sdb.res_ct1")
        tdSql.printResult()
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "firstts")
        tdSql.checkData(1, 0, "lastts")
        tdSql.checkData(2, 0, "num_v")
        tdSql.checkData(3, 0, "cnt_v")
        tdSql.checkData(4, 0, "avg_v")
        tdSql.checkData(0, 1, "TIMESTAMP")
        tdSql.checkData(1, 1, "TIMESTAMP")
        tdSql.checkData(2, 1, "BIGINT")
        tdSql.checkData(3, 1, "BIGINT")
        tdSql.checkData(4, 1, "DOUBLE")
        tdSql.checkData(0, 2, "8")
        tdSql.checkData(1, 2, "8")
        tdSql.checkData(2, 2, "8")
        tdSql.checkData(3, 2, "8")
        tdSql.checkData(4, 2, "8")
        tdLog.info(f"=============== check s1 result success !!!!!!!! =====================")
        return

    def checks2(self):
        result_sql = "select firstts, lastts, num_v, cnt_v, avg_v from res_ct2"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:03.000")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(0, 3, 4)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:08.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:11.000")
            and tdSql.compareData(1, 2, 4)
            and tdSql.compareData(1, 3, 4),
        )
        tdLog.info(f"=============== check s2 result success !!!!!!!! =====================")
        return

    def checks3(self):
        result_sql = "select firstts, lastts, num_v, cnt_v, avg_v from res_stb_ct2"
        tdSql.checkResultsByFunc(
            sql=result_sql,
            func=lambda: tdSql.getRows() == 2
            and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
            and tdSql.compareData(0, 1, "2025-01-01 00:00:03.000")
            and tdSql.compareData(0, 2, 4)
            and tdSql.compareData(0, 3, 4)
            and tdSql.compareData(1, 0, "2025-01-01 00:00:08.000")
            and tdSql.compareData(1, 1, "2025-01-01 00:00:11.000")
            and tdSql.compareData(1, 2, 4)
            and tdSql.compareData(1, 3, 4),
        )
        tdLog.info(f"=============== check s3 result success !!!!!!!! =====================")
        return

    class StreamItem:
        def __init__(self, sql, checkfunc):
            self.sql = sql
            self.checkfunc = checkfunc

        def check(self):
            self.checkfunc()
