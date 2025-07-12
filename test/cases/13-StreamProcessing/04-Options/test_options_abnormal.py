import time
from new_test_framework.utils import (tdLog,tdSql,tdStream,StreamCheckItem,)


class TestStreamOptionsTrigger:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_options_trigger(self):
        """Stream basic test 1
        """

        tdStream.createSnode()

        streams = []
        streams.append(self.Basic0())  # WATERMARK [ok]
        # streams.append(self.Basic1())  # EXPIRED_TIME   fail 
        # streams.append(self.Basic2())  # IGNORE_DISORDER  [ok]
        
        # TD-36343 [流计算开发阶段] 流计算state窗口+delete_recalc删除数据后重算结果错误
        # streams.append(self.Basic3())  # DELETE_RECALC
        
        # TD-36305 [流计算开发阶段] 流计算state窗口+超级表%%rows+delete_output_table没有删除结果表
        # streams.append(self.Basic4())  # DELETE_OUTPUT_TABLE
        
        # streams.append(self.Basic5())  # FILL_HISTORY        [ok]
        # streams.append(self.Basic6())  # FILL_HISTORY_FIRST  [ok]
        # streams.append(self.Basic7())  # CALC_NOTIFY_ONLY
        # # streams.append(self.Basic8())  # LOW_LATENCY_CALC  temp no test
        # streams.append(self.Basic9())  # PRE_FILTER     [ok]
        # streams.append(self.Basic10()) # FORCE_OUTPUT   [ok] 
        # streams.append(self.Basic11()) # MAX_DELAY        
        # streams.append(self.Basic11_1()) # MAX_DELAY        
        # streams.append(self.Basic12()) # EVENT_TYPE [ok]
        # streams.append(self.Basic13()) # IGNORE_NODATA_TRIGGER
        
        # streams.append(self.Basic14()) # watermark + expired_time + ignore_disorder  fail  对超期的数据仍然进行了计算
        
        tdStream.checkAll(streams)

    class Basic0(StreamCheckItem):
        def __init__(self):
            self.db  = "sdb0"
            self.stbName = "stb"
            self.stbName2 = "stb2"
            
            self.ntbName = "ntb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName}  (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.stbName2} (cts timestamp, cint int, cdouble double, cvarchar varchar(16)) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.ntbName}  (cts timestamp, cint int, cdouble double, cvarchar varchar(16))")
            # tdSql.query(f"show stables")
            # tdSql.checkRows(1)

            tdSql.execute(f"create table ct1 using {self.stbName} tags(1)")
            tdSql.execute(f"create table ct2 using {self.stbName} tags(2)")

            tdSql.execute(f"create table ct101 using {self.stbName2} tags(1)")
            tdSql.execute(f"create table ct102 using {self.stbName2} tags(2)")

            # tdSql.query(f"show tables")
            # tdSql.checkRows(2)

            ##### fail to wait fix
            # tdSql.error(
            #     f"create stream sn0 state_window(cint) from ct1 options(watermark(10s) | expired_time(5s)) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            # )
            # tdSql.error(
            #     f"create stream sn0_g state_window(cint) from {self.stbName} partition by tbname, tint options(watermark(10s) | expired_time(5s)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            # )

            tdSql.error(
                f"create stream sn1 state_window(cint) from ct1 options(watermark(0.5s)) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.error(
                f"create stream sn1x state_window(cint) from ct1 options(watermark(0.1d)) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.error(
                f"create stream sn2 state_window(cint) from ct1 options(fill_history(1733368671))) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.error(
                f"create stream sn3 state_window(cint) from ct1 options(fill_history | fill_history_first) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            ##### fail to wait fix
            # tdSql.error(
            #     f"create stream sn4 period(10s) from ct1 options(fill_history) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            # )

            # tdSql.error(
            #     f"create stream sn5 period(10s) from ct1 options(fill_history_first) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            # )
            
            tdSql.error(
                f"create stream sn6 state_window(cint) from ct1 options(pre_filter(cdouble < 5)) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )            
            
            tdSql.execute(
                f"create stream sok0 state_window(cint) from ntb options(pre_filter(cint < 5 and cvarchar like '%abc%')) into res_ntb (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            
        def insert1(self):
            pass

        def check1(self):
            pass