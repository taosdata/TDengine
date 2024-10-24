import sys
import threading
from util.log import *
from util.sql import *
from util.cases import *
from util.common import *

class TDTestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.tdCom = tdCom

    def force_window_close(self, interval, partition="tbname", funciton_name="", funciton_name_alias= "",delete=False, fill_value=None, fill_history_value=None, case_when=None, ignore_expired=1, ignore_update=1):
        tdLog.info(f"*** testing stream force_window_close+interp+every: every: {interval}, partition: {partition}, fill_history: {fill_history_value}, fill: {fill_value}, delete: {delete}, case_when: {case_when} ***")
        self.tdCom.subtable=False
        col_value_type = "Incremental" if partition=="c1" else "random"
        custom_col_index = 1 if partition=="c1" else None
        self.tdCom.custom_col_val = 0
        self.delete = delete
        self.tdCom.case_name = sys._getframe().f_code.co_name
        self.tdCom.prepare_data(interval=interval, fill_history_value=fill_history_value, custom_col_index=custom_col_index, col_value_type=col_value_type)
        self.stb_name = self.tdCom.stb_name.replace(f"{self.tdCom.dbname}.", "")
        self.ctb_name = self.tdCom.ctb_name.replace(f"{self.tdCom.dbname}.", "")
        self.tb_name = self.tdCom.tb_name.replace(f"{self.tdCom.dbname}.", "")
        self.stb_stream_des_table = f'{self.stb_name}{self.tdCom.des_table_suffix}'
        self.ctb_stream_des_table = f'{self.ctb_name}{self.tdCom.des_table_suffix}'
        self.tb_stream_des_table = f'{self.tb_name}{self.tdCom.des_table_suffix}'
        if partition == "tbname":
            if case_when:
                stream_case_when_partition = case_when
            else:
                stream_case_when_partition = self.tdCom.partition_tbname_alias

            partition_elm_alias = self.tdCom.partition_tbname_alias

        elif partition == "c1":
            if case_when:
                stream_case_when_partition = case_when
            else:
                stream_case_when_partition = self.tdCom.partition_col_alias
            partition_elm_alias = self.tdCom.partition_col_alias
        elif partition == "abs(c1)":
            partition_elm_alias = self.tdCom.partition_expression_alias
        elif partition is None:
            partition_elm_alias = '"no_partition"'
        else:
            partition_elm_alias = self.tdCom.partition_tag_alias
        if partition == "tbname" or partition is None:
            if case_when:
                stb_subtable_value = f'concat(concat("{self.stb_name}_{self.tdCom.subtable_prefix}", {stream_case_when_partition}), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
                ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.tdCom.subtable_prefix}", {stream_case_when_partition}), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
                tb_subtable_value = f'concat(concat("{self.tb_name}_{self.tdCom.subtable_prefix}", {stream_case_when_partition}), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
            else:
                stb_subtable_value = f'concat(concat("{self.stb_name}_{self.tdCom.subtable_prefix}", {partition_elm_alias}), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
                ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.tdCom.subtable_prefix}", {partition_elm_alias}), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
                tb_subtable_value = f'concat(concat("{self.tb_name}_{self.tdCom.subtable_prefix}", {partition_elm_alias}), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
        else:
            stb_subtable_value = f'concat(concat("{self.stb_name}_{self.tdCom.subtable_prefix}", cast(cast(abs(cast({partition_elm_alias} as int)) as bigint) as varchar(100))), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
            ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.tdCom.subtable_prefix}", cast(cast(abs(cast({partition_elm_alias} as int)) as bigint) as varchar(100))), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
            tb_subtable_value = f'concat(concat("{self.tb_name}_{self.tdCom.subtable_prefix}", cast(cast(abs(cast({partition_elm_alias} as int)) as bigint) as varchar(100))), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
        if partition:
            partition_elm = f'partition by {partition} {partition_elm_alias}'
        else:
            partition_elm = ""
        if fill_value:
            if "value" in fill_value.lower():
                fill_value='VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
        # create error stream
        tdLog.info("create error stream")
        sleep(10)
        tdSql.error(f"create stream itp_force_error_1  trigger force_window_close  IGNORE EXPIRED 1 IGNORE UPDATE 0 into itp_force_error_1 as  select _irowts,tbname,_isfilled,interp(c1,1) from  {self.stb_name}   partition by tbname   every(5s)   fill(prev) ;")
        tdSql.error(f"create stream itp_force_error_1  trigger force_window_close  IGNORE EXPIRED 0 IGNORE UPDATE 1 into itp_force_error_1 as  select _irowts,tbname,_isfilled,interp(c1,1) from  {self.stb_name}   partition by tbname   every(5s)   fill(prev) ;")
        tdSql.error(f"create stream itp_force_error_1  trigger at_once  IGNORE EXPIRED 0 IGNORE UPDATE 1 into itp_force_error_1 as  select _irowts,tbname,_isfilled,interp(c1,1) from  {self.stb_name}   partition by tbname   every(5s)   fill(prev) ;")


        # function name : interp
        trigger_mode = "force_window_close"
        # # subtable is true
        # create stream add :subtable_value=stb_subtable_value or subtable_value=ctb_subtable_value
        # no subtable
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.tdCom.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select  _irowts as irowts,tbname as table_name, _isfilled as isfilled, {funciton_name} as {funciton_name_alias} from {self.stb_name} {partition_elm} every({self.tdCom.dataDict["interval"]}s)', trigger_mode=trigger_mode, fill_value=fill_value, fill_history_value=fill_history_value,ignore_expired=ignore_expired,ignore_update=ignore_update)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.tdCom.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select  _irowts as irowts, tbname as table_name, _isfilled as isfilled, {funciton_name} as {funciton_name_alias} from {self.ctb_name} {partition_elm} every({self.tdCom.dataDict["interval"]}s)', trigger_mode=trigger_mode, fill_value=fill_value, fill_history_value=fill_history_value,ignore_expired=ignore_expired,ignore_update=ignore_update)

        if fill_value:
            if "value" in fill_value.lower():
                fill_value='VALUE,1,2,3,4,5,6,7,8,9,10,11'
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.tdCom.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select  _irowts as irowts,tbname as table_name, _isfilled as isfilled, {funciton_name} as {funciton_name_alias} from {self.tb_name} every({self.tdCom.dataDict["interval"]}s)', trigger_mode=trigger_mode, fill_value=fill_value, fill_history_value=fill_history_value,ignore_expired=ignore_expired,ignore_update=ignore_update)
        start_time = self.tdCom.date_time

        time.sleep(1)
        start_force_ts = str(0) 
        for i in range(self.tdCom.range_count):
            ts_value = str(self.tdCom.date_time+self.tdCom.dataDict["interval"])+f'+{i*10}s'
            if start_force_ts == "0":
                start_force_ts = ts_value 
            ts_cast_delete_value = self.tdCom.time_cast(ts_value)
            self.tdCom.sinsert_rows(tbname=self.tdCom.ctb_name, ts_value=ts_value, custom_col_index=custom_col_index, col_value_type=col_value_type)
            if i%2 == 0:
                self.tdCom.sinsert_rows(tbname=self.tdCom.ctb_name, ts_value=ts_value, custom_col_index=custom_col_index, col_value_type=col_value_type)
            if self.delete and i%2 != 0:
                self.tdCom.sdelete_rows(tbname=self.tdCom.ctb_name, start_ts=ts_cast_delete_value)
            self.tdCom.date_time += 1
            self.tdCom.sinsert_rows(tbname=self.tdCom.tb_name, ts_value=ts_value, custom_col_index=custom_col_index, col_value_type=col_value_type)
            if i%2 == 0:
                self.tdCom.sinsert_rows(tbname=self.tdCom.tb_name, ts_value=ts_value, custom_col_index=custom_col_index, col_value_type=col_value_type)
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
                        self.tdCom.check_query_data(f'select wstart, {self.tdCom.stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {tbname} {partition_elm} interval({self.tdCom.dataDict["interval"]}s) order by wstart', sorted=True)
                    else:
                        self.tdCom.check_query_data(f'select wstart, {self.tdCom.tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.tdCom.tb_source_select_str}  from {tbname} {partition_elm} interval({self.tdCom.dataDict["interval"]}s) order by wstart', sorted=True)

        if self.tdCom.subtable:
            for tname in [self.stb_name, self.ctb_name]:
                group_id = self.tdCom.get_group_id_from_stb(f'{tname}_output')
                tdSql.query(f'select * from {self.ctb_name}')
                ptn_counter = 0
                for c1_value in tdSql.queryResult:
                    if partition == "c1":
                        tbname = self.tdCom.get_subtable_wait(f'{tname}_{self.tdCom.subtable_prefix}{abs(c1_value[1])}{self.tdCom.subtable_suffix}')
                        tdSql.query(f'select count(*) from `{tbname}`')
                    elif partition is None:
                        tbname = self.tdCom.get_subtable_wait(f'{tname}_{self.tdCom.subtable_prefix}no_partition{self.tdCom.subtable_suffix}')
                        tdSql.query(f'select count(*) from `{tbname}`')
                    elif partition == "abs(c1)":
                        abs_c1_value = abs(c1_value[1])
                        tbname = self.tdCom.get_subtable_wait(f'{tname}_{self.tdCom.subtable_prefix}{abs_c1_value}{self.tdCom.subtable_suffix}')
                        tdSql.query(f'select count(*) from `{tbname}`')
                    elif partition == "tbname" and ptn_counter == 0:
                        tbname = self.tdCom.get_subtable_wait(f'{tname}_{self.tdCom.subtable_prefix}{self.ctb_name}{self.tdCom.subtable_suffix}_{tname}_output_{group_id}')
                        tdSql.query(f'select count(*) from `{tbname}`')
                        ptn_counter += 1
                    tdSql.checkEqual(tdSql.queryResult[0][0] > 0, True)
            group_id = self.tdCom.get_group_id_from_stb(f'{self.tb_name}_output')
            tdSql.query(f'select * from {self.tb_name}')
            ptn_counter = 0
            for c1_value in tdSql.queryResult:
                if partition == "c1":
                    tbname = self.tdCom.get_subtable_wait(f'{self.tb_name}_{self.tdCom.subtable_prefix}{abs(c1_value[1])}{self.tdCom.subtable_suffix}')
                    tdSql.query(f'select count(*) from `{tbname}`')
                elif partition is None:
                    tbname = self.tdCom.get_subtable_wait(f'{self.tb_name}_{self.tdCom.subtable_prefix}no_partition{self.tdCom.subtable_suffix}')
                    tdSql.query(f'select count(*) from `{tbname}`')
                elif partition == "abs(c1)":
                    abs_c1_value = abs(c1_value[1])
                    tbname = self.tdCom.get_subtable_wait(f'{self.tb_name}_{self.tdCom.subtable_prefix}{abs_c1_value}{self.tdCom.subtable_suffix}')
                    tdSql.query(f'select count(*) from `{tbname}`')
                elif partition == "tbname" and ptn_counter == 0:
                    tbname = self.tdCom.get_subtable_wait(f'{self.tb_name}_{self.tdCom.subtable_prefix}{self.tb_name}{self.tdCom.subtable_suffix}_{self.tb_name}_output_{group_id}')
                    tdSql.query(f'select count(*) from `{tbname}`')
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
                        self.tdCom.check_query_data(f'select irowts, table_name, isfilled, {funciton_name_alias} from {tbname}{self.tdCom.des_table_suffix} where irowts >= {start_force_ts}   and irowts <= {end_ts}  order by irowts', f'select _irowts as irowts ,tbname as table_name, _isfilled as isfilled , {funciton_name} as  {funciton_name_alias}  from {tbname}   partition by {partition}  range({start_force_ts},{end_ts})  every({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by irowts', fill_value=fill_value)
                    else:
                        self.tdCom.check_query_data(f'select wstart, {self.tdCom.fill_stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} where `min(c1)` is not Null order by wstart,`min(c1)`', f'select * from (select _wstart AS wstart, {self.tdCom.fill_stb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by wstart) where `min(c1)` is not Null order by wstart,`min(c1)`', fill_value=fill_value)
                else:
                    if "value" in fill_value.lower():
                        fill_value='VALUE,1,2,3,6,7,8,9,10,11'
                    if partition == "tbname":
                        self.tdCom.check_query_data(f'select wstart, {self.tdCom.fill_tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.tdCom.fill_tb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by wstart', fill_value=fill_value)
                    else:
                        self.tdCom.check_query_data(f'select wstart, {self.tdCom.fill_tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} where `min(c1)` is not Null order by wstart,`min(c1)`', f'select * from (select _wstart AS wstart, {self.tdCom.fill_tb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by wstart) where `min(c1)` is not Null order by wstart,`min(c1)`', fill_value=fill_value)

            if self.delete:
                self.tdCom.sdelete_rows(tbname=self.ctb_name, start_ts=start_ts, end_ts=ts_cast_delete_value)
                self.tdCom.sdelete_rows(tbname=self.tb_name, start_ts=start_ts, end_ts=ts_cast_delete_value)
                for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        if "value" in fill_value.lower():
                            fill_value='VALUE,1,2,3,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
                        if partition == "tbname":
                            self.tdCom.check_query_data(f'select wstart, {self.tdCom.fill_stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.tdCom.fill_stb_source_select_str}  from {tbname} where ts >= {start_ts.replace("-", "+")} and ts <= {end_ts} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by wstart', fill_value=fill_value)
                        else:
                            self.tdCom.check_query_data(f'select wstart, {self.tdCom.fill_stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} order by wstart,`min(c1)`', f'select * from (select _wstart AS wstart, {self.tdCom.fill_stb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by wstart) where `min(c1)` is not Null order by wstart,`min(c1)`', fill_value=fill_value)

                    else:
                        if "value" in fill_value.lower():
                            fill_value='VALUE,1,2,3,6,7,8,9,10,11'
                        if partition == "tbname":
                            self.tdCom.check_query_data(f'select wstart, {self.tdCom.fill_tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.tdCom.fill_tb_source_select_str}  from {tbname} where ts >= {start_ts.replace("-", "+")} and ts <= {end_ts} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by wstart', fill_value=fill_value)
                        else:
                            self.tdCom.check_query_data(f'select wstart, {self.tdCom.fill_tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} order by wstart,`min(c1)`', f'select * from (select _wstart AS wstart, {self.tdCom.fill_tb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by wstart) where `min(c1)` is not Null order by wstart,`min(c1)`', fill_value=fill_value)


    def run(self):
        self.force_window_close(interval=random.randint(10, 15), partition="tbname", funciton_name="interp(c1)", funciton_name_alias="intp_c1" ,delete=False, ignore_update=1, fill_value="PREV")
        self.force_window_close(interval=random.randint(10, 15), partition="c1", ignore_update=1)
        self.force_window_close(interval=random.randint(10, 15), partition="abs(c1)", ignore_update=1)
        self.force_window_close(interval=random.randint(10, 15), partition=None, delete=True)
        self.force_window_close(interval=random.randint(10, 15), partition=self.tdCom.stream_case_when_tbname, case_when=f'case when {self.tdCom.stream_case_when_tbname} = tbname then {self.tdCom.partition_tbname_alias} else tbname end')
        self.force_window_close(interval=random.randint(10, 15), partition="tbname", fill_history_value=1, fill_value="NULL")
        for fill_value in ["NULL", "PREV", "NEXT", "LINEAR", "VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11"]:
        # for fill_value in ["PREV", "NEXT", "LINEAR", "VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11"]:
            self.at_once_interval(interval=random.randint(10, 15), partition="tbname", fill_value=fill_value)
            self.at_once_interval(interval=random.randint(10, 15), partition="tbname", fill_value=fill_value, delete=True)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())