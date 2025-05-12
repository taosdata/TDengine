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

    def at_once_interval_ext(self, interval, partition="tbname", delete=False, fill_value=None, fill_history_value=None, subtable=None, case_when=None, stb_field_name_value=None, tag_value=None, use_exist_stb=False, use_except=False):
        tdLog.info(f"*** testing stream at_once+interval+exist_stb+custom_tag: interval: {interval}, partition: {partition}, fill_history: {fill_history_value}, delete: {delete}, subtable: {subtable}, stb_field_name_value: {stb_field_name_value}, tag_value: {tag_value} ***")
        if use_except:
            if stb_field_name_value == self.tdCom.partitial_stb_filter_des_select_elm or stb_field_name_value == self.tdCom.exchange_stb_filter_des_select_elm or len(stb_field_name_value.split(",")) == len(self.tdCom.partitial_stb_filter_des_select_elm.split(",")):
                partitial_tb_source_str = self.tdCom.partitial_ext_tb_source_select_str
            else:
                partitial_tb_source_str = self.tdCom.ext_tb_source_select_str
        else:
            if stb_field_name_value == self.tdCom.partitial_stb_filter_des_select_elm or stb_field_name_value == self.tdCom.exchange_stb_filter_des_select_elm:
                partitial_tb_source_str = self.tdCom.partitial_ext_tb_source_select_str
            else:
                partitial_tb_source_str = self.tdCom.ext_tb_source_select_str

        if stb_field_name_value is not None:
            if len(stb_field_name_value) == 0:
                stb_field_name_value = ",".join(self.tdCom.tb_filter_des_select_elm.split(",")[:5])
            # else:
            #     stb_field_name_value = self.tdCom.tb_filter_des_select_elm
        self.delete = delete
        self.tdCom.case_name = sys._getframe().f_code.co_name
        defined_tag_count = len(tag_value.split()) if tag_value is not None else 0
        self.tdCom.prepare_data(interval=interval, fill_history_value=fill_history_value, ext_stb=use_exist_stb)
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
        elif partition == "tbname,t1,c1":
            partition_elm_alias = f'{self.tdCom.partition_tbname_alias},t1,c1'
        else:
            partition_elm_alias = self.tdCom.partition_tag_alias
        if subtable:
            if partition == "tbname":
                if case_when:
                    stb_subtable_value = f'concat(concat("{self.stb_name}_{self.tdCom.subtable_prefix}", {stream_case_when_partition}), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
                else:
                    stb_subtable_value = f'concat(concat("{self.stb_name}_{self.tdCom.subtable_prefix}", {partition_elm_alias}), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
            else:
                if subtable == "constant":
                    # stb_subtable_value = f'"{self.tdCom.ext_ctb_stream_des_table}"'
                    stb_subtable_value = f'"constant_{self.tdCom.ext_ctb_stream_des_table}"'
                else:
                    stb_subtable_value = f'concat(concat("{self.stb_name}_{self.tdCom.subtable_prefix}", cast(cast(cast({subtable} as int unsigned) as bigint) as varchar(100))), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
        else:
            stb_subtable_value = None
        if fill_value:
            if "value" in fill_value.lower():
                fill_value='VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
        # self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.tdCom.stream_suffix}', des_table=self.tdCom.ext_stb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tdCom.ext_tb_source_select_str}  from {self.stb_name} partition by {partition} interval({self.tdCom.dataDict["interval"]}s)', trigger_mode="at_once", fill_value=fill_value, fill_history_value=fill_history_value, stb_field_name_value=stb_field_name_value, tag_value=tag_value, use_exist_stb=use_exist_stb)
        if partition:
            stream_sql = self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.tdCom.stream_suffix}', des_table=self.tdCom.ext_stb_stream_des_table, subtable_value=stb_subtable_value, source_sql=f'select _wstart AS wstart, {partitial_tb_source_str}  from {self.stb_name} partition by {partition} interval({self.tdCom.dataDict["interval"]}s)', trigger_mode="at_once", fill_value=fill_value, fill_history_value=fill_history_value, stb_field_name_value=stb_field_name_value, tag_value=tag_value, use_exist_stb=use_exist_stb, use_except=use_except)
        else:
            stream_sql = self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.tdCom.stream_suffix}', des_table=self.tdCom.ext_stb_stream_des_table, subtable_value=stb_subtable_value, source_sql=f'select _wstart AS wstart, {partitial_tb_source_str}  from {self.stb_name} interval({self.tdCom.dataDict["interval"]}s)', trigger_mode="at_once", fill_value=fill_value, fill_history_value=fill_history_value, stb_field_name_value=stb_field_name_value, tag_value=tag_value, use_exist_stb=use_exist_stb, use_except=use_except)
        if stream_sql:
            tdSql.error(stream_sql)
            return
        start_time = self.tdCom.date_time
        if subtable == "constant":
            range_count = 1
        else:
            range_count = self.tdCom.range_count

        time.sleep(1)

        for i in range(range_count):
            latency = 0
            tag_value_list = list()
            ts_value = str(self.tdCom.date_time+self.tdCom.dataDict["interval"])+f'+{i*10}s'
            ts_cast_delete_value = self.tdCom.time_cast(ts_value)
            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=ts_value)
            if self.tdCom.update and i%2 == 0:
                self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=ts_value)
            if self.delete and i%2 != 0:
                self.tdCom.sdelete_rows(tbname=self.ctb_name, start_ts=ts_cast_delete_value)
            self.tdCom.date_time += 1
            self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=ts_value)
            if self.tdCom.update and i%2 == 0:
                self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=ts_value)
            if self.delete and i%2 != 0:
                self.tdCom.sdelete_rows(tbname=self.tb_name, start_ts=ts_cast_delete_value)
            self.tdCom.date_time += 1
            if tag_value:
                if subtable == "constant":
                    tdSql.query(f'select {tag_value} from constant_{self.tdCom.ext_ctb_stream_des_table}')
                else:
                    tdSql.query(f'select {tag_value} from {self.stb_name}')
                tag_value_list = tdSql.queryResult
            if not fill_value:
                if stb_field_name_value == self.tdCom.partitial_stb_filter_des_select_elm:
                    self.tdCom.check_query_data(f'select {self.tdCom.partitial_stb_filter_des_select_elm } from ext_{self.stb_name}{self.tdCom.des_table_suffix} order by ts', f'select _wstart AS wstart, {partitial_tb_source_str}  from {self.stb_name} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) order by wstart', sorted=True)
                elif stb_field_name_value == self.tdCom.exchange_stb_filter_des_select_elm:
                    self.tdCom.check_query_data(f'select {self.tdCom.partitial_stb_filter_des_select_elm } from ext_{self.stb_name}{self.tdCom.des_table_suffix} order by ts', f'select _wstart AS wstart, cast(max(c2) as tinyint), cast(min(c1) as smallint)  from {self.stb_name} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) order by wstart', sorted=True)
                else:
                    if partition:
                        if tag_value == self.tdCom.exchange_tag_filter_des_select_elm:
                            self.tdCom.check_query_data(f'select {self.tdCom.partitial_tag_stb_filter_des_select_elm} from ext_{self.stb_name}{self.tdCom.des_table_suffix} order by ts', f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {self.stb_name} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) order by wstart', defined_tag_count=defined_tag_count, tag_value_list=tag_value_list)
                        elif tag_value == self.tdCom.cast_tag_filter_des_select_elm:
                            tdSql.query(f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {self.stb_name} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) order by wstart')
                            limit_row = tdSql.queryRows
                            self.tdCom.check_query_data(f'select {self.tdCom.cast_tag_filter_des_select_elm} from ext_{self.stb_name}{self.tdCom.des_table_suffix} order by ts', f'select cast(t1 as TINYINT UNSIGNED),cast(t2 as varchar(256)),cast(t3 as bool) from {self.stb_name}  order by ts limit {limit_row}')
                            tdSql.query(f'select t1,t2,t3,t4,t6,t7,t8,t9,t10,t12 from ext_{self.stb_name}{self.tdCom.des_table_suffix};')
                            while list(set(tdSql.queryResult)) != [(None, None, None, None, None, None, None, None, None, None)]:
                                tdSql.query(f'select t1,t2,t3,t4,t6,t7,t8,t9,t10,t12 from ext_{self.stb_name}{self.tdCom.des_table_suffix};')
                                if latency < self.tdCom.default_interval:
                                    latency += 1
                                    time.sleep(1)
                                else:
                                    return False
                            tdSql.checkEqual(list(set(tdSql.queryResult)), [(None, None, None, None, None, None, None, None, None, None)])
                        else:
                            self.tdCom.check_query_data(f'select {self.tdCom.stb_filter_des_select_elm} from ext_{self.stb_name}{self.tdCom.des_table_suffix} order by ts', f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {self.stb_name} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) order by wstart', defined_tag_count=defined_tag_count, tag_value_list=tag_value_list)
                    else:
                        if use_exist_stb and not tag_value:
                            self.tdCom.check_query_data(f'select {self.tdCom.stb_filter_des_select_elm} from ext_{self.stb_name}{self.tdCom.des_table_suffix} order by ts', f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {self.stb_name} interval({self.tdCom.dataDict["interval"]}s) order by wstart', defined_tag_count=defined_tag_count, tag_value_list=tag_value_list, partition=partition, use_exist_stb=use_exist_stb)
                        else:
                            self.tdCom.check_query_data(f'select {self.tdCom.stb_filter_des_select_elm} from ext_{self.stb_name}{self.tdCom.des_table_suffix} order by ts', f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {self.stb_name} interval({self.tdCom.dataDict["interval"]}s) order by wstart', defined_tag_count=defined_tag_count, tag_value_list=tag_value_list, partition=partition, subtable=subtable)

        if subtable:
            for tname in [self.stb_name]:
                tdSql.query(f'select * from {self.ctb_name}')
                ptn_counter = 0
                for c1_value in tdSql.queryResult:
                    if partition == "c1":
                        tbname = self.tdCom.get_subtable_wait(f'{tname}_{self.tdCom.subtable_prefix}{abs(c1_value[1])}{self.tdCom.subtable_suffix}')
                        tdSql.query(f'select count(*) from `{tbname}`')
                    elif partition == "abs(c1)":
                        abs_c1_value = abs(c1_value[1])
                        tbname = self.tdCom.get_subtable_wait(f'{tname}_{self.tdCom.subtable_prefix}{abs_c1_value}{self.tdCom.subtable_suffix}')
                        tdSql.query(f'select count(*) from `{tbname}`')
                    elif partition == "tbname" and ptn_counter == 0:
                        tbname = self.tdCom.get_subtable_wait(f'{tname}_{self.tdCom.subtable_prefix}{self.ctb_name}{self.tdCom.subtable_suffix}')
                        tdSql.query(f'select count(*) from `{tbname}`')
                        ptn_counter += 1
                    else:
                        tdSql.query(f'select cast(cast(cast({c1_value[1]} as int unsigned) as bigint) as varchar(100))')
                        subtable_value = tdSql.queryResult[0][0]
                        if subtable == "constant":
                            return
                        else:
                            tbname = self.tdCom.get_subtable_wait(f'{tname}_{self.tdCom.subtable_prefix}{subtable_value}{self.tdCom.subtable_suffix}')
                            tdSql.query(f'select count(*) from `{tbname}`')
                    tdSql.checkEqual(tdSql.queryResult[0][0] > 0, True)

    def run(self):
        # self.at_once_interval_ext(interval=random.randint(10, 15), delete=False, fill_history_value=1, partition=None, subtable="constant", stb_field_name_value=self.tdCom.tb_filter_des_select_elm, tag_value=self.tdCom.tag_filter_des_select_elm, use_exist_stb=True)
        for delete in [True, False]:
            for fill_history_value in [0, 1]:
                self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=f'tbname,{self.tdCom.tag_filter_des_select_elm.split(",")[0]},c1', subtable="c1", stb_field_name_value=self.tdCom.tb_filter_des_select_elm, tag_value=self.tdCom.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True)
                self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=f'tbname,{self.tdCom.tag_filter_des_select_elm},c1', subtable="c1", stb_field_name_value=None, tag_value=self.tdCom.tag_filter_des_select_elm, use_exist_stb=True)
                self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=f'tbname,{self.tdCom.tag_filter_des_select_elm},c1', stb_field_name_value=None, tag_value=self.tdCom.tag_filter_des_select_elm, use_exist_stb=True)
                self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=f'tbname,{self.tdCom.tag_filter_des_select_elm},c1', stb_field_name_value=self.tdCom.tb_filter_des_select_elm, tag_value=self.tdCom.tag_filter_des_select_elm, use_exist_stb=True)
                self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=f'tbname,{self.tdCom.tag_filter_des_select_elm.split(",")[0]},c1', subtable="c1", stb_field_name_value=self.tdCom.partitial_stb_filter_des_select_elm, tag_value=self.tdCom.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True)
                self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=f'tbname,{self.tdCom.tag_filter_des_select_elm.split(",")[0]},c1', subtable="c1", stb_field_name_value=self.tdCom.exchange_stb_filter_des_select_elm, tag_value=self.tdCom.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True)
                self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=None, subtable=None, stb_field_name_value=self.tdCom.tb_filter_des_select_elm, tag_value=self.tdCom.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True)
                # self-define tag
                self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=f'{self.tdCom.tag_filter_des_select_elm}', subtable=None, stb_field_name_value=None, tag_value=self.tdCom.tag_filter_des_select_elm, use_exist_stb=True)
                self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=f'{self.tdCom.partitial_tag_filter_des_select_elm}', subtable=None, stb_field_name_value=None, tag_value=self.tdCom.partitial_tag_filter_des_select_elm, use_exist_stb=True)
                self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=f'{self.tdCom.partitial_tag_filter_des_select_elm}', subtable=None, stb_field_name_value=None, tag_value=self.tdCom.exchange_tag_filter_des_select_elm, use_exist_stb=True)
                self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition="t1 as t5,t2 as t11,t3 as t13", subtable=None, stb_field_name_value=None, tag_value="t5,t11,t13", use_exist_stb=True)
                self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=None, subtable=None, stb_field_name_value=self.tdCom.tb_filter_des_select_elm, tag_value=None, use_exist_stb=True)
                self.at_once_interval_ext(interval=random.randint(10, 15), delete=delete, fill_history_value=fill_history_value, partition=None, subtable=None, stb_field_name_value=self.tdCom.tb_filter_des_select_elm, tag_value="t1", use_exist_stb=True)
            # error cases
            self.at_once_interval_ext(interval=random.randint(10, 15), partition=f'tbname,{self.tdCom.tag_filter_des_select_elm},c1', stb_field_name_value="", tag_value=self.tdCom.tag_filter_des_select_elm, use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), partition=f'tbname,{self.tdCom.tag_filter_des_select_elm},c1', stb_field_name_value=self.tdCom.tb_filter_des_select_elm.replace("c1","c19"), tag_value=self.tdCom.tag_filter_des_select_elm, use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), partition=f'tbname', subtable="c1", stb_field_name_value=self.tdCom.tb_filter_des_select_elm, tag_value=self.tdCom.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), partition=f'tbname,{self.tdCom.tag_filter_des_select_elm},c1', subtable="ttt", stb_field_name_value=self.tdCom.tb_filter_des_select_elm, tag_value=self.tdCom.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), partition=f'tbname,{self.tdCom.tag_filter_des_select_elm},c1', subtable="c1", stb_field_name_value=self.tdCom.tb_filter_des_select_elm, tag_value=None, use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), partition=f'tbname,{self.tdCom.tag_filter_des_select_elm},c1', subtable="c1", stb_field_name_value=self.tdCom.tb_filter_des_select_elm, tag_value="t15", use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), partition=f'tbname,{self.tdCom.tag_filter_des_select_elm},c1', subtable="c1", stb_field_name_value=self.tdCom.tb_filter_des_select_elm, tag_value="c5", use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), partition=f'tbname,{self.tdCom.tag_filter_des_select_elm.split(",")[0]},c1', subtable="c1", stb_field_name_value="ts,c1,c2,c3", tag_value=self.tdCom.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), partition=f'tbname,{self.tdCom.tag_filter_des_select_elm.split(",")[0]},c1', subtable="c1", stb_field_name_value="ts,c1", tag_value=self.tdCom.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), partition=f'tbname,{self.tdCom.tag_filter_des_select_elm.split(",")[0]},c1', subtable="c1", stb_field_name_value="c1,c2,c3", tag_value=self.tdCom.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), delete=False, fill_history_value=1, partition="t1 as t5,t2 as t11", subtable=None, stb_field_name_value=self.tdCom.tb_filter_des_select_elm, tag_value="t5,t11,t13", use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), delete=False, fill_history_value=1, partition="t1 as t5,t2 as t11,t3 as t14", subtable=None, stb_field_name_value=self.tdCom.tb_filter_des_select_elm, tag_value="t5,t11,t13", use_exist_stb=True, use_except=True)
            self.at_once_interval_ext(interval=random.randint(10, 15), delete=False, fill_history_value=1, partition="t1 as t5,t2 as t11,t3 as c13", subtable=None, stb_field_name_value=self.tdCom.tb_filter_des_select_elm, tag_value="t5,t11,c13", use_exist_stb=True, use_except=True)
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())