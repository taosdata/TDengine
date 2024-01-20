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

    def at_once_session(self, session, ignore_expired=None, ignore_update=None, partition="tbname", delete=False, fill_history_value=None, case_when=None, subtable=True):
        tdLog.info(f"*** testing stream at_once+interval: session: {session}, ignore_expired: {ignore_expired}, ignore_update: {ignore_update}, partition: {partition}, delete: {delete}, fill_history: {fill_history_value}, case_when: {case_when}, subtable: {subtable} ***")
        self.delete = delete
        self.tdCom.case_name = sys._getframe().f_code.co_name
        self.tdCom.prepare_data(session=session, fill_history_value=fill_history_value)
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
            partition_elm_alias = self.tdCom.partition_col_alias
        elif partition == "abs(c1)":
            if subtable:
                partition_elm_alias = self.tdCom.partition_expression_alias
            else:
                partition_elm_alias = "constant"
        else:
            partition_elm_alias = self.tdCom.partition_tag_alias
        if partition == "tbname" or subtable is None:
            if case_when:
                ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.tdCom.subtable_prefix}", {stream_case_when_partition}), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
                tb_subtable_value = f'concat(concat("{self.tb_name}_{self.tdCom.subtable_prefix}", {stream_case_when_partition}), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
            else:
                if subtable:
                    ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.tdCom.subtable_prefix}", {partition_elm_alias}), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
                    tb_subtable_value = f'concat(concat("{self.tb_name}_{self.tdCom.subtable_prefix}", {partition_elm_alias}), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
                else:
                    ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.tdCom.subtable_prefix}", "{partition_elm_alias}"), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
                    tb_subtable_value = f'concat(concat("{self.tb_name}_{self.tdCom.subtable_prefix}", "{partition_elm_alias}"), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
        else:
            if 'abs' in partition:
                ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.tdCom.subtable_prefix}", cast(cast(abs(cast({partition_elm_alias} as int)) as bigint) as varchar(20))), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
                tb_subtable_value = f'concat(concat("{self.tb_name}_{self.tdCom.subtable_prefix}", cast(cast(abs(cast({partition_elm_alias} as int)) as bigint) as varchar(20))), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None

            else:
                ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.tdCom.subtable_prefix}", cast(cast({partition_elm_alias} as bigint) as varchar(20))), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
                tb_subtable_value = f'concat(concat("{self.tb_name}_{self.tdCom.subtable_prefix}", cast(cast({partition_elm_alias} as bigint) as varchar(20))), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None

        time.sleep(1)

        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.tdCom.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, _wend AS wend, {self.tdCom.stb_source_select_str} from {self.ctb_name} partition by {partition} {partition_elm_alias} session(ts, {self.tdCom.dataDict["session"]}s)', trigger_mode="at_once", ignore_expired=ignore_expired, ignore_update=ignore_update, subtable_value=ctb_subtable_value, fill_history_value=fill_history_value)
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.tdCom.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS wstart, _wend AS wend, {self.tdCom.tb_source_select_str} from {self.tb_name} partition by {partition} {partition_elm_alias} session(ts, {self.tdCom.dataDict["session"]}s)', trigger_mode="at_once", ignore_expired=ignore_expired, ignore_update=ignore_update, subtable_value=tb_subtable_value, fill_history_value=fill_history_value)

        time.sleep(1)

        for i in range(self.tdCom.range_count):
            ctb_name = self.tdCom.get_long_name()
            self.tdCom.screate_ctable(stbname=self.stb_name, ctbname=ctb_name)

            if i == 0:
                window_close_ts = self.tdCom.cal_watermark_window_close_session_endts(self.tdCom.date_time, session=session)
            else:
                self.tdCom.date_time = window_close_ts + 1
                window_close_ts = self.tdCom.cal_watermark_window_close_session_endts(self.tdCom.date_time, session=session)
            if i == 0:
                record_window_close_ts = window_close_ts
            for ts_value in [self.tdCom.date_time, window_close_ts]:
                self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=ts_value, need_null=True)
                self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=ts_value, need_null=True)
                if self.tdCom.update and i%2 == 0:
                    self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=ts_value, need_null=True)
                    self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=ts_value, need_null=True)
                if self.delete and i%2 != 0:
                    dt = f'cast({self.tdCom.date_time-1} as timestamp)'
                    self.tdCom.sdelete_rows(tbname=self.ctb_name, start_ts=dt)
                    self.tdCom.sdelete_rows(tbname=self.tb_name, start_ts=dt)
                ts_value += 1

            # check result
            if partition != "tbname":
                for colname in self.tdCom.partition_by_downsampling_function_list:
                    if "first" not in colname and "last" not in colname:
                        self.tdCom.check_query_data(f'select wstart, wend-{self.tdCom.dataDict["session"]}s, {self.tdCom.tb_output_select_str} from {self.ctb_stream_des_table} order by wstart, `min(c1)`,`max(c2)`,`sum(c3)`;', f'select _wstart AS wstart, _wend AS wend, {self.tdCom.tb_source_select_str} from {self.ctb_name} partition by {partition} session(ts, {self.tdCom.dataDict["session"]}s) order by wstart, `min(c1)`,`max(c2)`,`sum(c3)`;', sorted=True)
                        self.tdCom.check_query_data(f'select wstart, wend-{self.tdCom.dataDict["session"]}s, {self.tdCom.tb_output_select_str} from {self.tb_stream_des_table} order by wstart, `min(c1)`,`max(c2)`,`sum(c3)`;', f'select _wstart AS wstart, _wend AS wend, {self.tdCom.tb_source_select_str} from {self.tb_name} partition by {partition} session(ts, {self.tdCom.dataDict["session"]}s) order by wstart, `min(c1)`,`max(c2)`,`sum(c3)`;')
            else:
                for tbname in [self.tb_name]:
                    if tbname != self.tb_name:
                        self.tdCom.check_query_data(f'select wstart, wend-{self.tdCom.dataDict["session"]}s, {self.tdCom.stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}', f'select _wstart AS wstart, _wend AS wend, {self.tdCom.stb_source_select_str}  from {tbname} partition by {partition} session(ts, {self.tdCom.dataDict["session"]}s)', sorted=True)
                    else:
                        self.tdCom.check_query_data(f'select wstart, wend-{self.tdCom.dataDict["session"]}s, {self.tdCom.tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}', f'select _wstart AS wstart, _wend AS wend, {self.tdCom.tb_source_select_str}  from {tbname} partition by {partition} session(ts, {self.tdCom.dataDict["session"]}s)', sorted=True)

        if self.tdCom.disorder:
            if ignore_expired:
                for tbname in [self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        tdSql.query(f'select wstart, {self.tdCom.stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}')
                        res2 = tdSql.queryResult
                        self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=str(self.tdCom.date_time)+f'-{self.tdCom.default_interval*(self.tdCom.range_count+session)}s')
                        tdSql.query(f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {tbname} session(ts, {self.tdCom.dataDict["session"]}s)')
                        res1 = tdSql.queryResult
                        tdSql.checkNotEqual(res1, res2)
                        tdSql.query(f'select wstart, {self.tdCom.stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}')
                        res1 = tdSql.queryResult
                        tdSql.checkEqual(res1, res2)
                    else:
                        tdSql.query(f'select wstart, {self.tdCom.tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}')
                        res2 = tdSql.queryResult
                        self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=str(self.tdCom.date_time)+f'-{self.tdCom.default_interval*(self.tdCom.range_count+session)}s')
                        tdSql.query(f'select _wstart AS wstart, {self.tdCom.tb_source_select_str}  from {tbname} session(ts, {self.tdCom.dataDict["session"]}s)')
                        res1 = tdSql.queryResult
                        tdSql.checkNotEqual(res1, res2)
                        tdSql.query(f'select wstart, {self.tdCom.tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}')
                        res1 = tdSql.queryResult
                        tdSql.checkEqual(res1, res2)
            else:
                if ignore_update:
                    for tbname in [self.ctb_name, self.tb_name]:
                        if tbname != self.tb_name:
                            tdSql.query(f'select wstart, {self.tdCom.stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}')
                            res2 = tdSql.queryResult
                            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=record_window_close_ts)
                            tdSql.query(f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {tbname} session(ts, {self.tdCom.dataDict["session"]}s)')
                            res1 = tdSql.queryResult
                            tdSql.checkNotEqual(res1, res2)
                        else:
                            tdSql.query(f'select wstart, {self.tdCom.tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}')
                            res2 = tdSql.queryResult
                            self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=record_window_close_ts)
                            tdSql.query(f'select _wstart AS wstart, {self.tdCom.tb_source_select_str}  from {tbname} session(ts, {self.tdCom.dataDict["session"]}s)')
                            res1 = tdSql.queryResult
                            tdSql.checkNotEqual(res1, res2)
                else:
                    self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=record_window_close_ts)
                    self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=record_window_close_ts)
                    if partition != "tbname":
                        for colname in self.tdCom.partition_by_downsampling_function_list:
                            if "first" not in colname and "last" not in colname:
                                self.tdCom.check_query_data(f'select wstart, {self.tdCom.tb_output_select_str} from {self.ctb_stream_des_table} order by wstart, `min(c1)`,`max(c2)`,`sum(c3)`;', f'select _wstart AS wstart, {self.tdCom.tb_source_select_str} from {self.ctb_name} partition by {partition} session(ts, {self.tdCom.dataDict["session"]}s) order by wstart, `min(c1)`,`max(c2)`,`sum(c3)`;', sorted=True)
                                self.tdCom.check_query_data(f'select wstart, {self.tdCom.tb_output_select_str} from {self.tb_stream_des_table} order by wstart, `min(c1)`,`max(c2)`,`sum(c3)`;', f'select _wstart AS wstart, {self.tdCom.tb_source_select_str} from {self.tb_name} partition by {partition} session(ts, {self.tdCom.dataDict["session"]}s) order by wstart, `min(c1)`,`max(c2)`,`sum(c3)`;')
                    else:
                        for tbname in [self.tb_name]:
                            if tbname != self.tb_name:
                                self.tdCom.check_query_data(f'select wstart, {self.tdCom.stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}', f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {tbname} partition by {partition} session(ts, {self.tdCom.dataDict["session"]}s)', sorted=True)
                            else:
                                self.tdCom.check_query_data(f'select wstart, {self.tdCom.tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}', f'select _wstart AS wstart, {self.tdCom.tb_source_select_str}  from {tbname} partition by {partition} session(ts, {self.tdCom.dataDict["session"]}s)', sorted=True)

        if fill_history_value:
            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=self.tdCom.record_history_ts)
            self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=self.tdCom.record_history_ts)
            if self.delete:
                self.tdCom.sdelete_rows(tbname=self.ctb_name, start_ts=self.tdCom.time_cast(self.tdCom.record_history_ts, "-"))
                self.tdCom.sdelete_rows(tbname=self.tb_name, start_ts=self.tdCom.time_cast(self.tdCom.record_history_ts, "-"))

        if self.tdCom.subtable:
            tdSql.query(f'select * from {self.ctb_name}')
            ptn_counter = 0
            for c1_value in tdSql.queryResult:
                if c1_value[1] is not None:
                    if partition == "c1":
                        tbname = self.tdCom.get_subtable_wait(f'{self.ctb_name}_{self.tdCom.subtable_prefix}{c1_value[1]}{self.tdCom.subtable_suffix}')
                        tdSql.query(f'select count(*) from `{tbname}`')
                    elif partition == "abs(c1)":
                        if subtable:
                            abs_c1_value = abs(c1_value[1])
                            tbname = self.tdCom.get_subtable_wait(f'{self.ctb_name}_{self.tdCom.subtable_prefix}{abs_c1_value}{self.tdCom.subtable_suffix}')
                            tdSql.query(f'select count(*) from `{tbname}`')
                        else:
                            tbname = self.tdCom.get_subtable_wait(f'{self.ctb_name}_{self.tdCom.subtable_prefix}{partition_elm_alias}{self.tdCom.subtable_suffix}')
                            tdSql.query(f'select count(*) from `{tbname}`')
                    elif partition == "tbname" and ptn_counter == 0:
                        tbname = self.tdCom.get_subtable_wait(f'{self.ctb_name}_{self.tdCom.subtable_prefix}{self.ctb_name}{self.tdCom.subtable_suffix}')
                        tdSql.query(f'select count(*) from `{tbname}`')
                        ptn_counter += 1
                    tdSql.checkEqual(tdSql.queryResult[0][0] > 0, True) if subtable is not None else tdSql.checkEqual(tdSql.queryResult[0][0] >= 0, True)

            tdSql.query(f'select * from {self.tb_name}')
            ptn_counter = 0
            for c1_value in tdSql.queryResult:
                if c1_value[1] is not None:
                    if partition == "c1":
                        tbname = self.tdCom.get_subtable_wait(f'{self.tb_name}_{self.tdCom.subtable_prefix}{c1_value[1]}{self.tdCom.subtable_suffix}')
                        tdSql.query(f'select count(*) from `{tbname}`')
                    elif partition == "abs(c1)":
                        if subtable:
                            abs_c1_value = abs(c1_value[1])
                            tbname = self.tdCom.get_subtable_wait(f'{self.tb_name}_{self.tdCom.subtable_prefix}{abs_c1_value}{self.tdCom.subtable_suffix}')
                            tdSql.query(f'select count(*) from `{tbname}`')
                        else:
                            tbname = self.tdCom.get_subtable_wait(f'{self.tb_name}_{self.tdCom.subtable_prefix}{partition_elm_alias}{self.tdCom.subtable_suffix}')
                            tdSql.query(f'select count(*) from `{tbname}`')
                    elif partition == "tbname" and ptn_counter == 0:
                        tbname = self.tdCom.get_subtable_wait(f'{self.tb_name}_{self.tdCom.subtable_prefix}{self.tb_name}{self.tdCom.subtable_suffix}')
                        tdSql.query(f'select count(*) from `{tbname}`')
                        ptn_counter += 1

                    tdSql.checkEqual(tdSql.queryResult[0][0] > 0, True) if subtable is not None else tdSql.checkEqual(tdSql.queryResult[0][0] >= 0, True)



    def run(self):
        self.at_once_session(session=random.randint(10, 15), partition=self.tdCom.stream_case_when_tbname, delete=True, case_when=f'case when {self.tdCom.stream_case_when_tbname} = tbname then {self.tdCom.partition_tbname_alias} else tbname end')
        for subtable in [None, True]:
            self.at_once_session(session=random.randint(10, 15), subtable=subtable, partition="abs(c1)")
        for ignore_expired in [None, 0, 1]:
            for fill_history_value in [None, 1]:
                self.at_once_session(session=random.randint(10, 15), ignore_expired=ignore_expired, fill_history_value=fill_history_value)
        for fill_history_value in [None, 1]:
            self.at_once_session(session=random.randint(10, 15), partition="tbname", delete=True, fill_history_value=fill_history_value)
            self.at_once_session(session=random.randint(10, 15), partition="c1", delete=True, fill_history_value=fill_history_value)
            self.at_once_session(session=random.randint(10, 15), partition="abs(c1)", delete=True, fill_history_value=fill_history_value)
            self.at_once_session(session=random.randint(10, 15), partition="abs(c1)", delete=True, subtable=None, fill_history_value=fill_history_value)
            self.at_once_session(session=random.randint(10, 15), ignore_update=1, fill_history_value=fill_history_value)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
