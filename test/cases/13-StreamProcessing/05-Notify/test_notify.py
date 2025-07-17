import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream, StreamCheckItem


class TestStreamNotifyTrigger:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_notify_trigger(self):
        """basic qdb 2

        Verification testing during the development process.

        Catalog:
            - Streams:Notify

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-26 Guoxy Created

        """

        tdStream.dropAllStreamsAndDbs()
        tdStream.createSnode()

        streams = []

        # streams.append(self.Basic1())    # OK
        streams.append(self.Basic2())      # OK
        # streams.append(self.Basic3())    # failed
        # streams.append(self.Basic4())    # OK
        # streams.append(self.Basic5())    # OK

        tdStream.checkAll(streams)

    class Basic1(StreamCheckItem):
        def __init__(self):
            self.db = "sdb1"
            self.stbName = "stb"

        def create(self):
            tdLog.info(f"=============== create database")
            tdSql.execute(f"create database {self.db} vgroups 5")
            tdSql.execute(f"use {self.db}")

            tdLog.info(f"=============== create super table")
            tdSql.execute(f"create stable stb (cts timestamp, cint int, cuint int unsigned) tags(tint int);")

            tdLog.info(f"=============== create sub table")
            tdSql.execute(f"create table ct1 using stb tags(1);")
            tdSql.execute(f"create table ct2 using stb tags(2);")
            tdSql.execute(f"create table ct3 using stb tags(1);")
            tdSql.execute(f"create table ct4 using stb tags(2);")

            tdLog.info(f"=============== create stream")
            sql1 = ("create stream s1 state_window(cint) from ct1                           "
                    "notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history|on_failure_pause) into "
                    "res_ct1 (firstts, lastts, num_v, cnt_v, avg_v) as "
                    "select first(_c0), last_row(_c0), _twrownum, count(*), avg(cuint) from %%trows")

            sql2 = "create stream s2 state_window(cint) from ct2                           notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history|on_failure_pause) into res_ct2 (firstts, lastts, num_v, cnt_v, avg_v) as select first(_c0), last_row(_c0), _twrownum, count(*), avg(cuint) from %%trows;"
            sql3 = "create stream s3 state_window(cint) from stb partition by tbname       notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history|on_failure_pause) into stb_res OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, num_v, cnt_v, avg_v) tags (nameoftbl varchar(128) as tbname, gid bigint as _tgrpid) as select first(_c0), last_row(_c0), _twrownum, count(*), avg(cuint) from %%trows partition by tbname;"
            sql4 = "create stream s4 state_window(cint) from stb partition by tbname, tint notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history|on_failure_pause) into stb_mtag_res OUTPUT_SUBTABLE(CONCAT('res_stb_mtag_', tbname, '_', cast(tint as varchar))) (firstts, lastts, num_v, cnt_v, avg_v) tags (nameoftbl varchar(128) as tbname, gid bigint as _tgrpid) as select first(_c0), last_row(_c0), _twrownum, count(*), avg(cuint) from %%trows partition by %%1, %%2;"

            tdSql.execute(sql1)
            tdSql.execute(sql2)
            tdSql.execute(sql3)
            tdSql.execute(sql4)

        def insert1(self):
            tdLog.info(f"=============== write query data")
            sqls = [
                "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:00', 0, 0);",
                "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:01', 0, 0);",
                "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:02', 1, 1);",
                "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:03', 1, 1);",
                "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:04', 1, 1);",
                "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:05', 2, 2);",
                "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:06', 2, 2);",
                "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:07', 2, 2);",
                "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:08', 2, 2);",
                "insert into ct1 using stb tags(1) values ('2025-01-01 00:00:09', 3, 3);",

                "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:00', 0, 0);",
                "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:01', 0, 0);",
                "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:02', 1, 1);",
                "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:03', 1, 1);",
                "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:04', 1, 1);",
                "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:05', 2, 2);",
                "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:06', 2, 2);",
                "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:07', 2, 2);",
                "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:08', 2, 2);",
                "insert into ct2 using stb tags(2) values ('2025-01-01 00:00:09', 3, 3);",

                "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:00', 0, 0);",
                "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:01', 0, 0);",
                "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:02', 1, 1);",
                "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:03', 1, 1);",
                "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:04', 1, 1);",
                "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:05', 2, 2);",
                "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:06', 2, 2);",
                "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:07', 2, 2);",
                "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:08', 2, 2);",
                "insert into ct3 using stb tags(1) values ('2025-01-01 00:00:09', 3, 3);",

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

        # ############ option: max_delay
        # # no  true_for
        # tdLog.info(f"=============== create sub table")
        # tdSql.execute(f"create table ct5 using stb tags(1);")
        
        # sql8 = "create stream s8 state_window(cint) from ct5 stream_options(max_delay(3s)) notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history|on_failure_pause) into res_max_delay_ct5 (lastts, firstts, num_v, cnt_v, avg_v) as select last_row(_c0), first(_c0), _twrownum, count(*), avg(cuint) from %%trows;"
         
        # tdSql.execute(sql8)
        # # tdStream.checkStreamStatus("s8")
        # time.sleep(3)
                
        # tdLog.info(f"=============== continue write data into ct5 for new real data ")
        # sqls = [
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:10', 1, 0);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:11', 1, 0);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:12', 1, 1);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:13', 1, 1);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:14', 1, 1);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:15', 2, 2);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:16', 2, 2);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:17', 2, 2);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:18', 2, 2);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:19', 2, 3);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:20', 1, 0);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:21', 1, 0);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:22', 1, 1);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:23', 1, 1);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:24', 1, 1);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:25', 2, 2);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:26', 2, 2);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:27', 2, 2);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:28', 2, 2);",
        #     "insert into ct5 using stb tags(1) values ('2025-01-04 00:00:29', 2, 3);",
        # ]
        # tdSql.executes(sqls)
        # self.checks6(3)  #  max_delay 觸發的結果時間戳 ，與 最後窗口關閉 的結果時間戳 是一樣的嗎？ 如果是，需要配合 通知 來測試。
        
        # # max_delay + true_for
        
        # ############ option: watermark
        

        def checks1(self):
            result_sql = "select firstts, lastts, num_v, cnt_v, avg_v from res_ct1"
            tdSql.checkResultsByFunc(
                sql=result_sql,
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
                and tdSql.compareData(0, 1, "2025-01-01 00:00:01.000")
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(0, 3, 2)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:02.000")
                and tdSql.compareData(1, 1, "2025-01-01 00:00:04.000")
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(1, 3, 3)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:05.000")
                and tdSql.compareData(2, 1, "2025-01-01 00:00:08.000")
                and tdSql.compareData(2, 2, 4)
                and tdSql.compareData(2, 3, 4),
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
            result_sql = "select firstts, num_v, cnt_v, avg_v from res_ct2"
            tdSql.checkResultsByFunc(
                sql=result_sql,
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
                and tdSql.compareData(0, 1, 2)
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:02.000")
                and tdSql.compareData(1, 1, 3)
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:05.000")
                and tdSql.compareData(2, 1, 4)
                and tdSql.compareData(2, 2, 4),
            )

            tdSql.query("desc sdb.res_ct2")
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
            tdLog.info(f"=============== check s2 result success !!!!!!!! =====================")
            return

        def checks3(self):
            result_sql = "select firstts, num_v, cnt_v, avg_v from res_stb_ct1"
            tdSql.checkResultsByFunc(
                sql=result_sql,
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
                and tdSql.compareData(0, 1, 2)
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:02.000")
                and tdSql.compareData(1, 1, 3)
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:05.000")
                and tdSql.compareData(2, 1, 4)
                and tdSql.compareData(2, 2, 4),
            )

            tdSql.query("desc sdb.stb_res")
            tdSql.printResult()
            tdSql.checkRows(7)
            tdSql.checkData(0, 0, "firstts")
            tdSql.checkData(1, 0, "lastts")
            tdSql.checkData(2, 0, "num_v")
            tdSql.checkData(3, 0, "cnt_v")
            tdSql.checkData(4, 0, "avg_v")
            tdSql.checkData(5, 0, "nameoftbl")
            tdSql.checkData(6, 0, "gid")
            tdSql.checkData(0, 1, "TIMESTAMP")
            tdSql.checkData(1, 1, "TIMESTAMP")
            tdSql.checkData(2, 1, "BIGINT")
            tdSql.checkData(3, 1, "BIGINT")
            tdSql.checkData(4, 1, "DOUBLE")
            tdSql.checkData(5, 1, "VARCHAR")
            tdSql.checkData(6, 1, "BIGINT")
            tdSql.checkData(0, 2, "8")
            tdSql.checkData(1, 2, "8")
            tdSql.checkData(2, 2, "8")
            tdSql.checkData(3, 2, "8")
            tdSql.checkData(4, 2, "8")
            tdSql.checkData(5, 2, "128")
            tdSql.checkData(6, 2, "8")
            tdSql.checkData(5, 3, "TAG")
            tdSql.checkData(6, 3, "TAG")
            tdLog.info(f"=============== check s3 result success !!!!!!!! =====================")
            return

        def checks4(self):
            result_sql = "select firstts, num_v, cnt_v, avg_v from res_stb_mtag_ct1_1"
            tdSql.checkResultsByFunc(
                sql=result_sql,
                func=lambda: tdSql.getRows() == 3
                and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
                and tdSql.compareData(0, 1, 2)
                and tdSql.compareData(0, 2, 2)
                and tdSql.compareData(1, 0, "2025-01-01 00:00:02.000")
                and tdSql.compareData(1, 1, 3)
                and tdSql.compareData(1, 2, 3)
                and tdSql.compareData(2, 0, "2025-01-01 00:00:05.000")
                and tdSql.compareData(2, 1, 4)
                and tdSql.compareData(2, 2, 4),
            )

            tdSql.query("desc sdb.stb_mtag_res")
            tdSql.printResult()
            tdSql.checkRows(7)
            tdSql.checkData(0, 0, "firstts")
            tdSql.checkData(1, 0, "lastts")
            tdSql.checkData(2, 0, "num_v")
            tdSql.checkData(3, 0, "cnt_v")
            tdSql.checkData(4, 0, "avg_v")
            tdSql.checkData(5, 0, "nameoftbl")
            tdSql.checkData(6, 0, "gid")
            tdSql.checkData(0, 1, "TIMESTAMP")
            tdSql.checkData(1, 1, "TIMESTAMP")
            tdSql.checkData(2, 1, "BIGINT")
            tdSql.checkData(3, 1, "BIGINT")
            tdSql.checkData(4, 1, "DOUBLE")
            tdSql.checkData(5, 1, "VARCHAR")
            tdSql.checkData(6, 1, "BIGINT")
            tdSql.checkData(0, 2, "8")
            tdSql.checkData(1, 2, "8")
            tdSql.checkData(2, 2, "8")
            tdSql.checkData(3, 2, "8")
            tdSql.checkData(4, 2, "8")
            tdSql.checkData(5, 2, "128")
            tdSql.checkData(6, 2, "8")
            tdSql.checkData(5, 3, "TAG")
            tdSql.checkData(6, 3, "TAG")
            tdLog.info(f"=============== check s4 result success !!!!!!!! =====================")
            return

        def checks5(self, check_idx):
            result_sql = "select firstts, num_v, cnt_v, avg_v from res_truefor_ct1"
            if 0 == check_idx:
                tdSql.checkResultsByFunc(
                    sql=result_sql,
                    func=lambda: tdSql.getRows() == 0,
                )
                tdLog.info(f"=============== check s5-0 result success !!!!!!!! =====================")
            elif 1 == check_idx:
                tdSql.checkResultsByFunc(
                    sql=result_sql,
                    func=lambda: tdSql.getRows() == 3
                    and tdSql.compareData(0, 0, "2025-01-01 00:00:10.000")
                    and tdSql.compareData(0, 1, 3)
                    and tdSql.compareData(0, 2, 3)
                    and tdSql.compareData(1, 0, "2025-01-01 00:00:16.000")
                    and tdSql.compareData(1, 1, 4)
                    and tdSql.compareData(1, 2, 4)
                    and tdSql.compareData(2, 0, "2025-01-01 00:00:22.000")
                    and tdSql.compareData(2, 1, 2)
                    and tdSql.compareData(2, 2, 2),
                )
                tdLog.info(f"=============== check s5-1 result success !!!!!!!! =====================")

            return

        def checks6(self, check_idx):

            if 0 == check_idx:
                result_sql = "select firstts, num_v, cnt_v, avg_v from res_fill_all_ct1"
                tdSql.checkResultsByFunc(
                    sql=result_sql,
                    func=lambda: tdSql.getRows() == 0,
                )
                tdLog.info(f"=============== check s6-0 result success !!!!!!!! =====================")
            elif 1 == check_idx:
                result_sql = "select firstts, num_v, cnt_v, avg_v from res_fill_all_ct1"
                tdSql.checkResultsByFunc(
                    sql=result_sql,
                    func=lambda: tdSql.getRows() == 11
                    and tdSql.compareData(0, 0, "2025-01-01 00:00:00.000")
                    and tdSql.compareData(0, 1, 2)
                    and tdSql.compareData(0, 2, 2)
                    and tdSql.compareData(1, 0, "2025-01-01 00:00:02.000")
                    and tdSql.compareData(1, 1, 3)
                    and tdSql.compareData(1, 2, 3)
                    and tdSql.compareData(2, 0, "2025-01-01 00:00:05.000")
                    and tdSql.compareData(2, 1, 4)
                    and tdSql.compareData(2, 2, 4),
                )
                tdLog.info(f"=============== check s6-1 result success !!!!!!!! =====================")
            elif 2 == check_idx:   #  fill history start time from 2025-01-02 00:00:10
                result_sql = "select firstts, num_v, cnt_v, avg_v from res_fill_part_ct1"
                tdSql.checkResultsByFunc(
                    sql=result_sql,
                    func=lambda: tdSql.getRows() == 7
                    and tdSql.compareData(0, 0, "2025-01-02 00:00:10.000")
                    and tdSql.compareData(0, 1, 3)
                    and tdSql.compareData(0, 2, 3)
                    and tdSql.compareData(1, 0, "2025-01-02 00:00:16.000")
                    and tdSql.compareData(1, 1, 4)
                    and tdSql.compareData(1, 2, 4)
                    and tdSql.compareData(2, 0, "2025-01-02 00:00:22.000")
                    and tdSql.compareData(2, 1, 2)
                    and tdSql.compareData(2, 2, 2),
                )
                tdLog.info(f"=============== check s6-2 result success !!!!!!!! =====================")
            elif 3 == check_idx:
                result_sql = "select lastts, firstts, num_v, cnt_v, avg_v from res_max_delay_ct5"
                tdSql.checkResultsByFunc(
                    sql=result_sql,
                    func=lambda: tdSql.getRows() == 6
                    and tdSql.compareData(0, 0, "2025-01-02 00:00:10.000")
                    and tdSql.compareData(0, 1, 3)
                    and tdSql.compareData(0, 2, 3)
                    and tdSql.compareData(1, 0, "2025-01-02 00:00:15.000")
                    and tdSql.compareData(1, 1, 5)
                    and tdSql.compareData(1, 2, 5)
                    and tdSql.compareData(2, 0, "2025-01-02 00:00:22.000")
                    and tdSql.compareData(2, 1, 2)
                    and tdSql.compareData(2, 2, 2),
                )
                tdLog.info(f"=============== check s6-3 result success !!!!!!!! =====================")

            return

    class Basic2(StreamCheckItem):
        def __init__(self):
            self.db = "sdb2"
            self.stb = "stb"

        def create(self):
            tdLog.info(f"=============== create database")
            tdSql.execute(f"create database {self.db} vgroups 4;")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table if not exists {self.stb} (ts timestamp, cint int, cbool bool, cfloat float, cdouble double, cbytes varchar(100), cdecimal decimal(10, 2)) tags (tag1 int, tag2 int);")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdLog.info(f"=============== create sub table")
            tdSql.execute(f"create table ct0 using {self.stb} tags(0, 1);")
            tdSql.execute(f"create table ct1 using {self.stb} tags(1, 2);")
            tdSql.execute(f"create table ct2 using {self.stb} tags(2, 3);")
            tdSql.execute(f"create table ct3 using {self.stb} tags(3, 4);")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            tdLog.info(f"=============== create stream")
            tdSql.execute(
                f"create stream s0 state_window(cint) from ct0 "
                f"stream_options(DELETE_RECALC) "
                f"notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history) "
                f"into "
                f"res_ct0 (twstart, twend, top_cdouble_10) as "
                f"select _twstart wstart, _twend, top(cdouble, 10) from ct0 "
                f"where _twstart <= _c0 and _c0 <= _twend "
            )

        def insert1(self):
            tdLog.info(f"=============== insert data into stb")
            sqls = [
                "insert into ct0 values ('2025-01-01 00:00:00.000', 1, 0, 1.1, 1.1, '1', 3.3333);",
                "insert into ct0 values ('2025-01-01 00:00:01.000', 1, 0, 1.1, 2.1, '1', 4.3333);",
                "insert into ct0 values ('2025-01-01 00:00:02.000', 1, 0, 1.1, 3.1, '1', 5.3333);",
                "insert into ct0 values ('2025-01-01 00:00:03.000', 1, 0, 1.1, 4.1, '1', 6.3333);",
                "insert into ct0 values ('2025-01-01 00:00:04.000', 1, 0, 1.1, 5.1, '1', 7.3333);",
                "insert into ct0 values ('2025-01-01 00:00:05.000', 1, 0, 1.1, 6.1, '1', 8.3333);",
                "insert into ct0 values ('2025-01-01 00:00:06.000', 1, 0, 1.1, 7.1, '1', 9.3333);",
                "insert into ct0 values ('2025-01-01 00:00:07.000', 1, 0, 1.1, 8.1, '1', 10.3333);",
                "insert into ct0 values ('2025-01-01 00:00:08.000', 1, 0, 1.1, 9.1, '1', 11.3333);",
                "insert into ct0 values ('2025-01-01 00:00:09.000', 1, 0, 1.1, 10.1, '1', 12.3333);",
                "insert into ct0 values ('2025-01-01 00:00:10.000', 1, 0, 1.1, 11.1, '1', 13.3333);",
            ]

            tdSql.executes(sqls)
            tdSql.execute(f"delete from ct0 where _c0 = '2025-01-01 00:00:00.000';")

            tdSql.execute(f"insert into ct0 values('2025-01-01 00:00:05.000', 911, 0, 1.1, 1.1, '1', 23.123456);")

        def check1(self):
            tdLog.info("do check the results")

    class Basic3(StreamCheckItem):
        def __init__(self):
            self.db = "sdb3"
            self.stb = "stb"

        def create(self):
            tdLog.info(f"=============== create database")
            tdSql.execute(f"create database {self.db} vgroups 4;")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table if not exists {self.stb} (ts timestamp, cint int, cbool bool, cfloat float, cdouble double, cbytes varchar(100), cdecimal decimal(10, 2)) tags (tag1 int, tag2 int);")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdLog.info(f"=============== create sub table")
            tdSql.execute(f"create table ct0 using {self.stb} tags(0, 1);")
            tdSql.execute(f"create table ct1 using {self.stb} tags(1, 2);")
            tdSql.execute(f"create table ct2 using {self.stb} tags(2, 3);")
            tdSql.execute(f"create table ct3 using {self.stb} tags(3, 4);")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)


            tdLog.info(f"=============== create stream")
            tdSql.execute(
                f"create stream s0 state_window(cint) from ct0 "
                f"stream_options(DELETE_RECALC) "
                f"notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history|on_failure_pause) "
                f"into "
                f"res_ct0_0 (firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from ct0 "
                f"where _twstart - 10s <= _c0 and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s1 state_window(cint) from ct0 "
                f"stream_options(DELETE_RECALC) "
                f"notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history|on_failure_pause) "
                f"into "
                f"res_ct0_1 (firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from ct0 "
                f"where _twstart - 10s <= _c0 and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s2 state_window(cint) from ct0 "
                f"stream_options(DELETE_RECALC) "
                f"notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history|on_failure_pause) "
                f"into "
                f"res_ct0_2 (firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from ct0 "
                f"where _twstart - 10s <= _c0 and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s3 state_window(cint) from ct0 "
                f"stream_options(DELETE_RECALC) "
                f"notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history|on_failure_pause) "
                f"into "
                f"res_ct0_3 (firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from ct0 "
                f"where _twstart - 10s <= _c0 and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s4 state_window(cint) from ct0 "
                f"stream_options(DELETE_RECALC) "
                f"notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history|on_failure_pause) "
                f"into "
                f"res_ct0_4 (firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from ct0 "
                f"where _twstart - 10s <= _c0 and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s5 state_window(cint) from ct0 "
                f"stream_options(DELETE_RECALC) "
                f"notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history|on_failure_pause) "
                f"into "
                f"res_ct0_5 (firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from ct0 "
                f"where _twstart - 10s <= _c0 and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s6 state_window(cint) from ct0 "
                f"stream_options(DELETE_RECALC) "
                f"notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history|on_failure_pause) "
                f"into "
                f"res_ct0_6 (firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from ct0 "
                f"where _twstart - 10s <= _c0 and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s7 state_window(cint) from ct0 "
                f"stream_options(DELETE_RECALC) "
                f"notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history|on_failure_pause) "
                f"into "
                f"res_ct0_7 (firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from ct0 "
                f"where _twstart - 10s <= _c0 and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s8 state_window(cint) from ct0 "
                f"stream_options(DELETE_RECALC) "
                f"notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history|on_failure_pause) "
                f"into "
                f"res_ct0_8 (firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from ct0 "
                f"where _twstart - 10s <= _c0 and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s9 state_window(cint) from ct0 "
                f"stream_options(DELETE_RECALC) "
                f"notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history|on_failure_pause) "
                f"into "
                f"res_ct0_9 (firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from ct0 "
                f"where _twstart - 10s <= _c0 and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s10 state_window(cint) from ct0 "
                f"stream_options(DELETE_RECALC) "
                f"notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history|on_failure_pause) "
                f"into "
                f"res_ct0_10 (firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from ct0 "
                f"where _twstart - 10s <= _c0 and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s11 state_window(cint) from ct0 "
                f"stream_options(DELETE_RECALC) "
                f"notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history|on_failure_pause) "
                f"into "
                f"res_ct0_11 (firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from ct0 "
                f"where _twstart - 10s <= _c0 and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s12 state_window(cint) from ct0 "
                f"stream_options(DELETE_RECALC) "
                f"notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history|on_failure_pause) "
                f"into "
                f"res_ct0_12 (firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from ct0 "
                f"where _twstart - 10s <= _c0 and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s13 state_window(cint) from ct0 "
                f"stream_options(DELETE_RECALC) "
                f"notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history|on_failure_pause) "
                f"into "
                f"res_ct0_13 (firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from ct0 "
                f"where _twstart - 10s <= _c0 and _c0 <= _twend "
            )

            tdSql.execute(
                f"create stream s14 state_window(cint) from ct0 "
                f"stream_options(DELETE_RECALC) "
                f"notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history) "
                f"into "
                f"res_ct0_14 (firstts, lastts, cnt_v, sum_v, avg_v) as "
                f"select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from ct0 "
                f"where _twstart - 10s <= _c0 and _c0 <= _twend "
            )

        def insert1(self):
            tdLog.info(f"=============== insert data into stb")
            sqls = [
                "insert into ct0 values ('2025-01-01 00:00:00.000', 1, 0, 1.1, 1.1, '1', 3.3333);",
                "insert into ct0 values ('2025-01-01 00:00:01.000', 1, 0, 1.1, 1.1, '1', 4.3333);",
                "insert into ct0 values ('2025-01-01 00:00:02.000', 1, 0, 1.1, 1.1, '1', 5.3333);",
                "insert into ct0 values ('2025-01-01 00:00:03.000', 1, 0, 1.1, 1.1, '1', 6.3333);",
                "insert into ct0 values ('2025-01-01 00:00:04.000', 1, 0, 1.1, 1.1, '1', 7.3333);",
                "insert into ct0 values ('2025-01-01 00:00:05.000', 1, 0, 1.1, 1.1, '1', 8.3333);",
                "insert into ct0 values ('2025-01-01 00:00:06.000', 1, 0, 1.1, 1.1, '1', 9.3333);",
                "insert into ct0 values ('2025-01-01 00:00:07.000', 1, 0, 1.1, 1.1, '1', 10.3333);",
                "insert into ct0 values ('2025-01-01 00:00:08.000', 1, 0, 1.1, 1.1, '1', 11.3333);",
                "insert into ct0 values ('2025-01-01 00:00:09.000', 1, 0, 1.1, 1.1, '1', 12.3333);",
                "insert into ct0 values ('2025-01-01 00:00:10.000', 1, 0, 1.1, 1.1, '1', 13.3333);",
            ]

            tdSql.executes(sqls)
            tdSql.execute(f"delete from ct0 where _c0 = '2025-01-01 00:00:00.000';")

            tdSql.execute(f"insert into ct0 values('2025-01-01 00:00:05.000', 911, 0, 1.1, 1.1, '1', 23.123456);")

        def check1(self):
            tdLog.info("do check the results")

    class Basic4(StreamCheckItem):
        def __init__(self):
            self.db = "sdb4"
            self.stb = "stb"

        def create(self):
            tdLog.info(f"=============== create database")
            tdSql.execute(f"create database {self.db} vgroups 4;")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table if not exists {self.stb} (ts timestamp, cint int, cbool bool, cfloat float, cdouble double, cbytes varchar(100), cdecimal decimal(10, 2)) tags (tag1 int, tag2 int);")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdLog.info(f"=============== create sub table")
            tdSql.execute(f"create table ct0 using {self.stb} tags(0, 1);")
            tdSql.execute(f"create table ct1 using {self.stb} tags(1, 2);")
            tdSql.execute(f"create table ct2 using {self.stb} tags(2, 3);")
            tdSql.execute(f"create table ct3 using {self.stb} tags(3, 4);")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            tdLog.info(f"=============== create stream")
            tdSql.execute(
                f"create stream s0 interval(4s) sliding(4s) from ct0 "
                f"notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history) "
                f"into "
                f"res_ct0 (twstart, twend, sum_cdecimal, count_val, min_float, max_double, last_bytes) as "
                f"select _twstart wstart, _twend, sum(cdecimal), count(*), min(cfloat), max(cdouble), last(cbytes) from ct0 "
                f"where _twstart <= _c0 and _c0 <= _twend "
            )

        def insert1(self):
            tdLog.info(f"=============== insert data into stb")
            sqls = [
                "insert into ct0 values ('2025-01-01 00:00:00.000', 1, 0, 1.1, 1.1, '11', 3.3333);",
                "insert into ct0 values ('2025-01-01 00:00:01.000', 1, 0, 1.1, 2.1, '12', 4.3333);",
                "insert into ct0 values ('2025-01-01 00:00:02.000', 1, 0, 1.1, 3.1, '13', 5.3333);",
                "insert into ct0 values ('2025-01-01 00:00:03.000', 1, 0, 1.1, 4.1, '14', 6.3333);",
                "insert into ct0 values ('2025-01-01 00:00:04.000', 1, 0, 1.1, 5.1, '15', 7.3333);",
                "insert into ct0 values ('2025-01-01 00:00:05.000', 1, 0, 1.1, 6.1, '16', 8.3333);",
                "insert into ct0 values ('2025-01-01 00:00:06.000', 1, 0, 1.1, 7.1, '17', 9.3333);",
                "insert into ct0 values ('2025-01-01 00:00:07.000', 1, 0, 1.1, 8.1, '18', 10.3333);",
                "insert into ct0 values ('2025-01-01 00:00:08.000', 1, 0, 1.1, 9.1, '19', 11.3333);",
                "insert into ct0 values ('2025-01-01 00:00:09.000', 1, 0, 1.1, 10.1, '20', 12.3333);",
                "insert into ct0 values ('2025-01-01 00:00:10.000', 1, 0, 1.1, 11.1, '21', 13.3333);",
            ]

            tdSql.executes(sqls)
            # tdSql.execute(f"delete from ct0 where _c0 = '2025-01-01 00:00:00.000';")
            # tdSql.execute(f"insert into ct0 values('2025-01-01 00:00:05.000', 911, 0, 1.1, 1.1, '1', 23.123456);")

        def check1(self):
            tdLog.info("do check the results")

    class Basic5(StreamCheckItem):
        def __init__(self):
            self.db = "sdb5"
            self.stb = "stb"

        def create(self):
            tdLog.info(f"=============== create database")
            tdSql.execute(f"create database {self.db} vgroups 4;")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table if not exists {self.stb} (ts timestamp, cint int, cbool bool, cfloat float, cdouble double, cbytes varchar(100), cdecimal decimal(10, 2)) tags (tag1 int, tag2 int);")
            tdSql.query(f"show stables")
            tdSql.checkRows(1)

            tdLog.info(f"=============== create sub table")
            tdSql.execute(f"create table ct0 using {self.stb} tags(0, 1);")
            tdSql.execute(f"create table ct1 using {self.stb} tags(1, 2);")
            tdSql.execute(f"create table ct2 using {self.stb} tags(2, 3);")
            tdSql.execute(f"create table ct3 using {self.stb} tags(3, 4);")

            tdSql.query(f"show tables")
            tdSql.checkRows(4)

            tdLog.info(f"=============== create stream")
            tdSql.execute(
                f"create stream s0 interval(4s) sliding(4s) from ct0 "
                f"notify('ws://localhost:12345/notify') on(window_open|window_close) notify_options(notify_history) "
                f"into "
                f"res_ct0 (twstart, twend, count_val, min_float, max_double, last_bytes) as "
                f"select _twstart wstart, _twend, count(*), min(cfloat), max(cdouble), last(cbytes) from ct0 "
                f"where _twstart <= _c0 and _c0 <= _twend "
            )

        def insert1(self):
            tdLog.info(f"=============== insert data into stb")
            sqls = [
                "insert into ct0 values ('2025-01-01 00:00:00.000', 1, 0, 1.1, 1.1, '11', 3.3333);",
                "insert into ct0 values ('2025-01-01 00:00:01.000', 1, 0, 1.1, 2.1, '12', 4.3333);",
                "insert into ct0 values ('2025-01-01 00:00:02.000', 1, 0, 1.1, 3.1, '13', 5.3333);",
                "insert into ct0 values ('2025-01-01 00:00:03.000', 1, 0, 1.1, 4.1, '14', 6.3333);",
                "insert into ct0 values ('2025-01-01 00:00:04.000', 1, 0, 1.1, 5.1, '15', 7.3333);",
                "insert into ct0 values ('2025-01-01 00:00:05.000', 1, 0, 1.1, 6.1, '16', 8.3333);",
                "insert into ct0 values ('2025-01-01 00:00:06.000', 1, 0, 1.1, 7.1, '17', 9.3333);",
                "insert into ct0 values ('2025-01-01 00:00:07.000', 1, 0, 1.1, 8.1, '18', 10.3333);",
                "insert into ct0 values ('2025-01-01 00:00:08.000', 1, 0, 1.1, 9.1, '19', 11.3333);",
                "insert into ct0 values ('2025-01-01 00:00:09.000', 1, 0, 1.1, 10.1, '20', 12.3333);",
                "insert into ct0 values ('2025-01-01 00:00:10.000', 1, 0, 1.1, 11.1, '21', 13.3333);",
            ]

            tdSql.executes(sqls)

        def check1(self):
            tdLog.info("do check the results")
