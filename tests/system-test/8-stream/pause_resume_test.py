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
        self.date_time = 1694054350870
        self.interval = 15

    def pause_resume_test(self, interval, partition="tbname", delete=False, fill_history_value=None, pause=True, resume=True, ignore_untreated=False):
        tdLog.info(f"*** testing stream pause+resume: interval: {interval}, partition: {partition}, delete: {delete}, fill_history: {fill_history_value}, ignore_untreated: {ignore_untreated} ***")
        date_time = self.date_time
        if_exist_value_list = [None, True]
        if_exist = random.choice(if_exist_value_list)
        reverse_check = True if ignore_untreated else False
        range_count = (self.tdCom.range_count + 3) * 3
        self.delete = delete
        self.tdCom.case_name = sys._getframe().f_code.co_name
        self.tdCom.prepare_data(interval=interval, fill_history_value=fill_history_value)
        self.stb_name = self.tdCom.stb_name.replace(f"{self.tdCom.dbname}.", "")
        self.ctb_name = self.tdCom.ctb_name.replace(f"{self.tdCom.dbname}.", "")
        self.tb_name = self.tdCom.tb_name.replace(f"{self.tdCom.dbname}.", "")
        self.stb_stream_des_table = f'{self.stb_name}{self.tdCom.des_table_suffix}'
        self.ctb_stream_des_table = f'{self.ctb_name}{self.tdCom.des_table_suffix}'
        self.tb_stream_des_table = f'{self.tb_name}{self.tdCom.des_table_suffix}'

        if partition == "tbname":
            partition_elm_alias = self.tdCom.partition_tbname_alias
        elif partition == "c1":
            partition_elm_alias = self.tdCom.partition_col_alias
        elif partition == "abs(c1)":
            partition_elm_alias = self.tdCom.partition_expression_alias
        elif partition is None:
            partition_elm_alias = '"no_partition"'
        else:
            partition_elm_alias = self.tdCom.partition_tag_alias
        if partition == "tbname" or partition is None:
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
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.tdCom.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {self.stb_name} {partition_elm} interval({self.tdCom.dataDict["interval"]}s)', trigger_mode="at_once", subtable_value=stb_subtable_value, fill_history_value=fill_history_value)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.tdCom.stream_suffix}', des_table=self.tdCom.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {self.ctb_name} {partition_elm} interval({self.tdCom.dataDict["interval"]}s)', trigger_mode="at_once", subtable_value=ctb_subtable_value, fill_history_value=fill_history_value)
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.tdCom.stream_suffix}', des_table=self.tdCom.tb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tdCom.tb_source_select_str}  from {self.tb_name} {partition_elm} interval({self.tdCom.dataDict["interval"]}s)', trigger_mode="at_once", subtable_value=tb_subtable_value, fill_history_value=fill_history_value)

        time.sleep(1)

        for i in range(range_count):
            ts_value = str(date_time+self.tdCom.dataDict["interval"])+f'+{i*10}s'
            ts_cast_delete_value = self.tdCom.time_cast(ts_value)
            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=ts_value)
            if self.tdCom.update and i%2 == 0:
                self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=ts_value)
            if self.delete and i%2 != 0:
                self.tdCom.sdelete_rows(tbname=self.ctb_name, start_ts=ts_cast_delete_value)
            date_time += 1
            self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=ts_value)
            if self.tdCom.update and i%2 == 0:
                self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=ts_value)
            if self.delete and i%2 != 0:
                self.tdCom.sdelete_rows(tbname=self.tb_name, start_ts=ts_cast_delete_value)
            date_time += 1
            if partition:
                partition_elm = f'partition by {partition}'
            else:
                partition_elm = ""

            time.sleep(1)

            # if i == int(range_count/2):
            if i > 2 and i % 3 == 0:
                for stream_name in [f'{self.stb_name}{self.tdCom.stream_suffix}', f'{self.ctb_name}{self.tdCom.stream_suffix}', f'{self.tb_name}{self.tdCom.stream_suffix}']:
                    if if_exist is not None:
                        tdSql.execute(f'pause stream if exists {stream_name}_no_exist')
                    tdSql.error(f'pause stream if not exists {stream_name}')
                    tdSql.error(f'pause stream {stream_name}_no_exist')
                    self.tdCom.pause_stream(stream_name, if_exist)
                if pause and not resume and range_count-i <= 3:
                    time.sleep(self.tdCom.default_interval)
                    tdSql.query(f'select wstart, {self.tdCom.stb_output_select_str} from {self.stb_name}{self.tdCom.des_table_suffix} order by wstart')
                    res_after_pause = tdSql.queryResult
            if resume:
                if i > 2 and i % 3 != 0:
                    for stream_name in [f'{self.stb_name}{self.tdCom.stream_suffix}', f'{self.ctb_name}{self.tdCom.stream_suffix}', f'{self.tb_name}{self.tdCom.stream_suffix}']:
                        if if_exist is not None:
                            tdSql.execute(f'resume stream if exists {stream_name}_no_exist')
                        tdSql.error(f'resume stream if not exists {stream_name}')
                        self.tdCom.resume_stream(stream_name, if_exist, None, ignore_untreated)
        if pause and not resume:
            tdSql.query(f'select wstart, {self.tdCom.stb_output_select_str} from {self.stb_name}{self.tdCom.des_table_suffix} order by wstart')
            res_without_resume = tdSql.queryResult
            tdSql.checkEqual(res_after_pause, res_without_resume)
        else:
            for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    self.tdCom.check_query_data(f'select wstart, {self.tdCom.stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {tbname} {partition_elm} interval({self.tdCom.dataDict["interval"]}s) order by wstart', sorted=True, reverse_check=reverse_check)
                else:
                    self.tdCom.check_query_data(f'select wstart, {self.tdCom.tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.tdCom.tb_source_select_str}  from {tbname} {partition_elm} interval({self.tdCom.dataDict["interval"]}s) order by wstart', sorted=True, reverse_check=reverse_check)

        if self.tdCom.subtable:
            for tname in [self.stb_name, self.ctb_name]:
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
                        tbname = self.tdCom.get_subtable_wait(f'{tname}_{self.tdCom.subtable_prefix}{self.ctb_name}{self.tdCom.subtable_suffix}')
                        tdSql.query(f'select count(*) from `{tbname}`')
                        ptn_counter += 1
                    tdSql.checkEqual(tdSql.queryResult[0][0] > 0, True)

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
                    tbname = self.tdCom.get_subtable_wait(f'{self.tb_name}_{self.tdCom.subtable_prefix}{self.tb_name}{self.tdCom.subtable_suffix}')
                    tdSql.query(f'select count(*) from `{tbname}`')
                    ptn_counter += 1

                tdSql.checkEqual(tdSql.queryResult[0][0] > 0, True)


    def run(self):
        for delete in [True, False]:
            for fill_history_value in [0, 1]:
                # pause/resume
                self.pause_resume_test(interval=self.interval, partition="tbname", ignore_untreated=False, fill_history_value=fill_history_value, delete=delete)
                self.pause_resume_test(interval=self.interval, partition="tbname", ignore_untreated=True, fill_history_value=fill_history_value, delete=delete)
                # self.pause_resume_test(interval=random.randint(10, 15), partition="tbname", resume=False, fill_history_value=fill_history_value, delete=delete)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
