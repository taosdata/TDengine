import sys
import time
import threading
from taos.tmq import Consumer
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *

class TDTestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.tdCom = tdCom
        self.subtable = True
        self.partition_tbname_alias = "ptn_alias" if self.subtable else ""
        self.partition_col_alias = "pcol_alias" if self.subtable else ""
        self.partition_tag_alias = "ptag_alias" if self.subtable else ""
        self.partition_expression_alias = "pexp_alias" if self.subtable else ""
        self.stb_name = str()
        self.ctb_name = str()
        self.tb_name = str()
        self.des_table_suffix = "_output"
        self.stream_suffix = "_stream"
        self.subtable_prefix = "prefix_" if self.subtable else ""
        self.subtable_suffix = "_suffix" if self.subtable else ""
        self.stb_stream_des_table = str()
        self.ctb_stream_des_table = str()
        self.tb_stream_des_table = str()
        self.downsampling_function_list = ["min(c1)", "max(c2)", "sum(c3)", "first(c4)", "last(c5)", "apercentile(c6, 50)", "avg(c7)", "count(c8)", "spread(c1)",
        "stddev(c2)", "hyperloglog(c11)", "timediff(1, 0, 1h)", "timezone()", "to_iso8601(1)", 'to_unixtimestamp("1970-01-01T08:00:00+08:00")', "min(t1)", "max(t2)", "sum(t3)",
        "first(t4)", "last(t5)", "apercentile(t6, 50)", "avg(t7)", "count(t8)", "spread(t1)", "stddev(t2)", "hyperloglog(t11)"]
        self.stb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.downsampling_function_list)))
        self.stb_source_select_str = ','.join(self.downsampling_function_list)
        self.tb_source_select_str = ','.join(self.downsampling_function_list[0:15])

    def at_once_interval(self, interval, partition="tbname", delete=False, fill_value=None, fill_history_value=None, case_when=None):
        tdLog.info(f"testing stream at_once+interval: interval: {interval}, partition: {partition}, fill_history: {fill_history_value}")
        self.delete = delete
        self.tdCom.case_name = sys._getframe().f_code.co_name
        self.tdCom.prepare_data(interval=interval, fill_history_value=fill_history_value)
        self.stb_name = self.tdCom.stb_name.replace(f"{self.tdCom.dbname}.", "")
        self.ctb_name = self.tdCom.ctb_name.replace(f"{self.tdCom.dbname}.", "")
        self.tb_name = self.tdCom.tb_name.replace(f"{self.tdCom.dbname}.", "")
        self.stb_stream_des_table = f'{self.stb_name}{self.des_table_suffix}'
        self.ctb_stream_des_table = f'{self.ctb_name}{self.des_table_suffix}'
        self.tb_stream_des_table = f'{self.tb_name}{self.des_table_suffix}'
        self.tb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', self.downsampling_function_list[0:15])))

        if partition == "tbname":
            if case_when:
                stream_case_when_partition = case_when
            else:
                stream_case_when_partition = self.partition_tbname_alias

            partition_elm_alias = self.partition_tbname_alias
        elif partition == "c1":
            if case_when:
                stream_case_when_partition = case_when
            else:
                stream_case_when_partition = self.partition_col_alias
            partition_elm_alias = self.partition_col_alias
        elif partition == "abs(c1)":
            partition_elm_alias = self.partition_expression_alias
        elif partition is None:
            partition_elm_alias = '"no_partition"'
        else:
            partition_elm_alias = self.partition_tag_alias
        if partition == "tbname" or partition is None:
            if case_when:
                stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", {stream_case_when_partition}), "{self.subtable_suffix}")' if self.subtable else None
                ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", {stream_case_when_partition}), "{self.subtable_suffix}")' if self.subtable else None
                tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", {stream_case_when_partition}), "{self.subtable_suffix}")' if self.subtable else None
            else:
                stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
                ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
                tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", {partition_elm_alias}), "{self.subtable_suffix}")' if self.subtable else None
        else:
            stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", cast(cast(abs(cast({partition_elm_alias} as int)) as bigint) as varchar(100))), "{self.subtable_suffix}")' if self.subtable else None
            ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.subtable_prefix}", cast(cast(abs(cast({partition_elm_alias} as int)) as bigint) as varchar(100))), "{self.subtable_suffix}")' if self.subtable else None
            tb_subtable_value = f'concat(concat("{self.tb_name}_{self.subtable_prefix}", cast(cast(abs(cast({partition_elm_alias} as int)) as bigint) as varchar(100))), "{self.subtable_suffix}")' if self.subtable else None
        if partition:
            partition_elm = f'partition by {partition} {partition_elm_alias}'
        else:
            partition_elm = ""
        if fill_value:
            if "value" in fill_value.lower():
                fill_value='VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.stb_name} {partition_elm} interval({self.tdCom.dataDict["interval"]}s)', trigger_mode="at_once", subtable_value=stb_subtable_value, fill_value=fill_value, fill_history_value=fill_history_value)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.stb_source_select_str}  from {self.ctb_name} {partition_elm} interval({self.tdCom.dataDict["interval"]}s)', trigger_mode="at_once", subtable_value=ctb_subtable_value, fill_value=fill_value, fill_history_value=fill_history_value)
        if fill_value:
            if "value" in fill_value.lower():
                fill_value='VALUE,1,2,3,4,5,6,7,8,9,10,11'
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tb_source_select_str}  from {self.tb_name} {partition_elm} interval({self.tdCom.dataDict["interval"]}s)', trigger_mode="at_once", subtable_value=tb_subtable_value, fill_value=fill_value, fill_history_value=fill_history_value)
        start_time = self.tdCom.date_time
        for i in range(self.tdCom.range_count):
            ts_value = str(self.tdCom.date_time+self.tdCom.dataDict["interval"])+f'+{i*10}s'
            ts_cast_delete_value = self.tdCom.time_cast(ts_value)
            self.tdCom.sinsert_rows(tbname=self.tdCom.ctb_name, ts_value=ts_value)
            if i%2 == 0:
                self.tdCom.sinsert_rows(tbname=self.tdCom.ctb_name, ts_value=ts_value)
            if self.delete and i%2 != 0:
                self.tdCom.sdelete_rows(tbname=self.tdCom.ctb_name, start_ts=ts_cast_delete_value)
            self.tdCom.date_time += 1
            self.tdCom.sinsert_rows(tbname=self.tdCom.tb_name, ts_value=ts_value)
            if i%2 == 0:
                self.tdCom.sinsert_rows(tbname=self.tdCom.tb_name, ts_value=ts_value)
            if self.delete and i%2 != 0:
                self.tdCom.sdelete_rows(tbname=self.tdCom.tb_name, start_ts=ts_cast_delete_value)
            self.tdCom.date_time += 1
            if partition:
                partition_elm = f'partition by {partition}'
            else:
                partition_elm = ""

            if not fill_value:
                for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        self.tdCom.check_query_data(f'select wstart, {self.stb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.stb_source_select_str}  from {tbname} {partition_elm} interval({self.tdCom.dataDict["interval"]}s) order by wstart', sorted=True)
                    else:
                        self.tdCom.check_query_data(f'select wstart, {self.tb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.tb_source_select_str}  from {tbname} {partition_elm} interval({self.tdCom.dataDict["interval"]}s) order by wstart', sorted=True)

        if self.subtable:
            for tname in [self.stb_name, self.ctb_name]:
                tdSql.query(f'select * from {self.ctb_name}')
                ptn_counter = 0
                for c1_value in tdSql.queryResult:
                    if partition == "c1":
                        tdSql.query(f'select count(*) from `{tname}_{self.subtable_prefix}{abs(c1_value[1])}{self.subtable_suffix}`;')
                    elif partition is None:
                        tdSql.query(f'select count(*) from `{tname}_{self.subtable_prefix}no_partition{self.subtable_suffix}`;')
                    elif partition == "abs(c1)":
                        abs_c1_value = abs(c1_value[1])
                        tdSql.query(f'select count(*) from `{tname}_{self.subtable_prefix}{abs_c1_value}{self.subtable_suffix}`;')
                    elif partition == "tbname" and ptn_counter == 0:
                        tdSql.query(f'select count(*) from `{tname}_{self.subtable_prefix}{self.ctb_name}{self.subtable_suffix}`;')
                        ptn_counter += 1
                    tdSql.checkEqual(tdSql.queryResult[0][0] > 0, True)

            tdSql.query(f'select * from {self.tb_name}')
            ptn_counter = 0
            for c1_value in tdSql.queryResult:
                if partition == "c1":
                    tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}{abs(c1_value[1])}{self.subtable_suffix}`;')
                elif partition is None:
                    tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}no_partition{self.subtable_suffix}`;')
                elif partition == "abs(c1)":
                    abs_c1_value = abs(c1_value[1])
                    tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}{abs_c1_value}{self.subtable_suffix}`;')
                elif partition == "tbname" and ptn_counter == 0:
                    tdSql.query(f'select count(*) from `{self.tb_name}_{self.subtable_prefix}{self.tb_name}{self.subtable_suffix}`;')
                    ptn_counter += 1

                tdSql.checkEqual(tdSql.queryResult[0][0] > 0, True)
        if fill_value:
            end_date_time = self.tdCom.date_time
            final_range_count = self.tdCom.range_count
            history_ts = str(start_time)+f'-{self.tdCom.dataDict["interval"]*(final_range_count+2)}s'
            start_ts = self.tdCom.time_cast(history_ts, "-")
            future_ts = str(end_date_time)+f'+{self.tdCom.dataDict["interval"]*(final_range_count+2)}s'
            end_ts = self.tdCom.time_cast(future_ts)
            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=history_ts)
            self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=history_ts)
            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=future_ts)
            self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=future_ts)
            self.tdCom.date_time = start_time
            # update
            history_ts = str(start_time)+f'-{self.tdCom.dataDict["interval"]*(final_range_count+2)}s'
            start_ts = self.tdCom.time_cast(history_ts, "-")
            future_ts = str(end_date_time)+f'+{self.tdCom.dataDict["interval"]*(final_range_count+2)}s'
            end_ts = self.tdCom.time_cast(future_ts)
            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=history_ts)
            self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=history_ts)
            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=future_ts)
            self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=future_ts)
            self.tdCom.date_time = start_time
            for i in range(self.tdCom.range_count):
                ts_value = str(self.tdCom.date_time+self.tdCom.dataDict["interval"])+f'+{i*10}s'
                ts_cast_delete_value = self.tdCom.time_cast(ts_value)
                self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=ts_value)
                self.tdCom.date_time += 1
                self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=ts_value)
                self.tdCom.date_time += 1
            if self.delete:
                self.tdCom.sdelete_rows(tbname=self.ctb_name, start_ts=self.tdCom.time_cast(start_time), end_ts=ts_cast_delete_value)
                self.tdCom.sdelete_rows(tbname=self.tb_name, start_ts=self.tdCom.time_cast(start_time), end_ts=ts_cast_delete_value)
            for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    if "value" in fill_value.lower():
                        fill_value='VALUE,1,2,3,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
                    if partition == "tbname":
                        self.tdCom.check_query_data(f'select wstart, {self.fill_stb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.fill_stb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by wstart', fill_value=fill_value)
                    else:
                        self.tdCom.check_query_data(f'select wstart, {self.fill_stb_output_select_str} from {tbname}{self.des_table_suffix} where `min(c1)` is not Null order by wstart,`min(c1)`', f'select * from (select _wstart AS wstart, {self.fill_stb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by wstart) where `min(c1)` is not Null order by wstart,`min(c1)`', fill_value=fill_value)
                else:
                    if "value" in fill_value.lower():
                        fill_value='VALUE,1,2,3,6,7,8,9,10,11'
                    if partition == "tbname":
                        self.tdCom.check_query_data(f'select wstart, {self.fill_tb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.fill_tb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by wstart', fill_value=fill_value)
                    else:
                        self.tdCom.check_query_data(f'select wstart, {self.fill_tb_output_select_str} from {tbname}{self.des_table_suffix} where `min(c1)` is not Null order by wstart,`min(c1)`', f'select * from (select _wstart AS wstart, {self.fill_tb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by wstart) where `min(c1)` is not Null order by wstart,`min(c1)`', fill_value=fill_value)

            if self.delete:
                self.tdCom.sdelete_rows(tbname=self.ctb_name, start_ts=start_ts, end_ts=ts_cast_delete_value)
                self.tdCom.sdelete_rows(tbname=self.tb_name, start_ts=start_ts, end_ts=ts_cast_delete_value)
                for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        if "value" in fill_value.lower():
                            fill_value='VALUE,1,2,3,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
                        if partition == "tbname":
                            self.tdCom.check_query_data(f'select wstart, {self.fill_stb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.fill_stb_source_select_str}  from {tbname} where ts >= {start_ts.replace("-", "+")} and ts <= {end_ts} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by wstart', fill_value=fill_value)
                        else:
                            self.tdCom.check_query_data(f'select wstart, {self.fill_stb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart,`min(c1)`', f'select * from (select _wstart AS wstart, {self.fill_stb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by wstart) where `min(c1)` is not Null order by wstart,`min(c1)`', fill_value=fill_value)

                    else:
                        if "value" in fill_value.lower():
                            fill_value='VALUE,1,2,3,6,7,8,9,10,11'
                        if partition == "tbname":
                            self.tdCom.check_query_data(f'select wstart, {self.fill_tb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.fill_tb_source_select_str}  from {tbname} where ts >= {start_ts.replace("-", "+")} and ts <= {end_ts} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by wstart', fill_value=fill_value)
                        else:
                            self.tdCom.check_query_data(f'select wstart, {self.fill_tb_output_select_str} from {tbname}{self.des_table_suffix} order by wstart,`min(c1)`', f'select * from (select _wstart AS wstart, {self.fill_tb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by wstart) where `min(c1)` is not Null order by wstart,`min(c1)`', fill_value=fill_value)


    def run(self):
        self.at_once_interval(interval=random.randint(10, 15), partition="tbname", delete=True)
        self.at_once_interval(interval=random.randint(10, 15), partition="c1", delete=True)
        self.at_once_interval(interval=random.randint(10, 15), partition="abs(c1)", delete=True)
        self.at_once_interval(interval=random.randint(10, 15), partition=None, delete=True)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())