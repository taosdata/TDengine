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
        streams.append(self.Basic0())  
        
        tdStream.checkAll(streams)

    class Basic0(StreamCheckItem):
        def __init__(self):
            self.db       = "sdb0"
            self.stbName  = "stb"
            self.stbName2 = "stb2"            
            self.ntbName = "ntb"
            
            # vtable
            self.vstbName  = "vstb"
            self.vstbName2 = "vstb2"            
            self.vntbName = "vntb"

        def create(self):
            tdSql.execute(f"create database {self.db} vgroups 1 buffer 8")
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"create table if not exists  {self.stbName}  (cts timestamp, cint int) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.stbName2} (cts timestamp, cint int, cdouble double, cvarchar varchar(16)) tags (tint int)")
            tdSql.execute(f"create table if not exists  {self.ntbName}  (cts timestamp, cint int, cdouble double, cvarchar varchar(16))")            
            
            tdSql.execute(f"create table if not exists  {self.vstbName}  (cts timestamp, cint int) tags (tint int) virtual 1")
            tdSql.execute(f"create table if not exists  {self.vstbName2} (cts timestamp, cint int, cdouble double, cvarchar varchar(16)) tags (tint int) virtual 1")
            tdSql.execute(f"create table if not exists  {self.vntbName}  (cts timestamp, cint int from {self.ntbName}.cint, cdouble double from {self.ntbName}.cdouble, cvarchar varchar(16) from {self.ntbName}.cvarchar)")

            tdSql.execute(f"create table ct1 using {self.stbName} tags(1)")
            tdSql.execute(f"create table ct2 using {self.stbName} tags(2)")
            tdSql.execute(f"create table ct101 using {self.stbName2} tags(1)")
            tdSql.execute(f"create table ct102 using {self.stbName2} tags(2)")

            # vtables
            tdSql.execute(f"create vtable vct1   (cint from {self.db}.ct1.cint) using {self.db}.{self.vstbName} tags(1)")
            tdSql.execute(f"create vtable vct2   (cint from {self.db}.ct2.cint) using {self.db}.{self.vstbName} tags(2)")
            tdSql.execute(f"create vtable vct101 (cint from ct101.cint, cdouble from ct101.cdouble, cvarchar from ct101.cvarchar) using {self.db}.{self.vstbName2} tags(1)")
            tdSql.execute(f"create vtable vct102 (cint from ct102.cint, cdouble from ct102.cdouble, cvarchar from ct102.cvarchar) using {self.db}.{self.vstbName2} tags(2)")  

            tdSql.error(
                f"create stream sn0 state_window(cint) from vct1 stream_options(watermark(10s) | expired_time(5s)) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            tdSql.error(
                f"create stream sn0_g state_window(cint) from {self.vstbName} partition by tbname, tint stream_options(watermark(10s) | expired_time(5s)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.error(
                f"create stream sn1 state_window(cint) from vct1 stream_options(watermark(0.5s)) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.error(
                f"create stream sn1x state_window(cint) from vct1 stream_options(watermark(0.1d)) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.error(
                f"create stream sn2 state_window(cint) from vct1 stream_options(fill_history(1733368671))) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.error(
                f"create stream sn3 state_window(cint) from vct1 stream_options(fill_history | fill_history_first) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.error(
                f"create stream sn4 period(10s) from vct1 stream_options(fill_history) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )

            tdSql.error(
                f"create stream sn5 period(10s) from vct1 stream_options(fill_history_first) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            
            tdSql.error(
                f"create stream sn6 state_window(cint) from vct1 stream_options(pre_filter(cdouble < 5)) into res_ct1 (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )            
            
            tdSql.execute(
                f"create stream sn7 state_window(cint) from vntb stream_options(pre_filter(cint < 5 and cvarchar like '%abc%')) into res_ntb (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            
            tdSql.error(
                f"create stream sn8 state_window(cint) from {self.vntbName} into res_ntb (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows where cbigint > 1;"
            )           
            
            tdSql.error(
                f"create stream sn9 state_window(cint) from vntb options(pre_filter(cint < 5 and cvarchar like '%abc%')) into res_ntb (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )
            
            # %%trows must not use with WINDOW_OPEN in event_type
            # tdSql.error(
            #     f"create stream sn10 state_window(cint) from vct1 stream_options(event_type(WINDOW_OPEN|WINDOW_CLOSE)) into res_ct1 (lastts, firstts, cnt_v, sum_v, avg_v) as select last_row(_c0), first(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            # ) 
            
            tdSql.execute(
                f"create stream sn11_g state_window(cint) from {self.vstbName} partition by tbname, tint stream_options(watermark(10s) | expired_time(500s)) into res_stb OUTPUT_SUBTABLE(CONCAT('res_stb_', tbname)) (firstts, lastts, cnt_v, sum_v, avg_v) as select first(_c0), last_row(_c0), count(cint), sum(cint), avg(cint) from %%trows;"
            )      
            tdSql.error(
                f"alter table vct1 set tag tint = 999;"
            )            
            
        def insert1(self):
            pass

        def check1(self):
            pass