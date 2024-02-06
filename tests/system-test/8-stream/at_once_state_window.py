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

    def at_once_state_window(self, state_window, partition="tbname", delete=False, fill_history_value=None, case_when=None, subtable=True):
        tdLog.info(f"*** testing stream at_once+interval: state_window: {state_window}, partition: {partition}, fill_history: {fill_history_value}, case_when: {case_when}***, delete: {delete}")
        self.delete = delete
        self.tdCom.case_name = sys._getframe().f_code.co_name
        self.tdCom.prepare_data(state_window=state_window, fill_history_value=fill_history_value)
        self.stb_name = self.tdCom.stb_name.replace(f"{self.tdCom.dbname}.", "")
        self.ctb_name = self.tdCom.ctb_name.replace(f"{self.tdCom.dbname}.", "")
        self.tb_name = self.tdCom.tb_name.replace(f"{self.tdCom.dbname}.", "")
        self.ctb_stream_des_table = f'{self.ctb_name}{self.tdCom.des_table_suffix}'
        self.tb_stream_des_table = f'{self.tb_name}{self.tdCom.des_table_suffix}'

        if partition == "tbname":
            partition_elm_alias = self.tdCom.partition_tbname_alias
        elif partition == "c1" and subtable is not None:
            partition_elm_alias = self.tdCom.partition_col_alias
        elif partition == "c1" and subtable is None:
            partition_elm_alias = 'constant'
        elif partition == "abs(c1)":
            partition_elm_alias = self.tdCom.partition_expression_alias
        else:
            partition_elm_alias = self.tdCom.partition_tag_alias
        if partition == "tbname" or subtable is None:
            if partition == "tbname":
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

        state_window_col_name = self.tdCom.dataDict["state_window"]
        if case_when:
            stream_state_window = case_when
        else:
            stream_state_window = state_window_col_name
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.tdCom.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {self.ctb_name} partition by {partition} {partition_elm_alias} state_window({stream_state_window})', trigger_mode="at_once", subtable_value=ctb_subtable_value, fill_history_value=fill_history_value)
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.tdCom.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tdCom.tb_source_select_str}  from {self.tb_name} partition by {partition} {partition_elm_alias} state_window({stream_state_window})', trigger_mode="at_once", subtable_value=tb_subtable_value, fill_history_value=fill_history_value)
        range_times = self.tdCom.range_count
        state_window_max = self.tdCom.dataDict['state_window_max']

        time.sleep(2)

        for i in range(range_times):
            state_window_value = random.randint(int((i)*state_window_max/range_times), int((i+1)*state_window_max/range_times))
            for i in range(2, range_times+3):
                tdSql.execute(f'insert into {self.ctb_name} (ts, {state_window_col_name}) values ({self.tdCom.date_time}, {state_window_value})')
                if self.tdCom.update and i%2 == 0:
                    tdSql.execute(f'insert into {self.ctb_name} (ts, {state_window_col_name}) values ({self.tdCom.date_time}, {state_window_value})')
                if self.delete and i%2 != 0:
                    dt = f'cast({self.tdCom.date_time-1} as timestamp)'
                    tdSql.execute(f'delete from {self.ctb_name} where ts = {dt}')
                tdSql.execute(f'insert into {self.tb_name} (ts, {state_window_col_name}) values ({self.tdCom.date_time}, {state_window_value})')
                if self.tdCom.update and i%2 == 0:
                    tdSql.execute(f'insert into {self.tb_name} (ts, {state_window_col_name}) values ({self.tdCom.date_time}, {state_window_value})')
                if self.delete and i%2 != 0:
                    tdSql.execute(f'delete from {self.tb_name} where ts = {dt}')
                self.tdCom.date_time += 1

        # for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
        for tbname in [self.ctb_name, self.tb_name]:
            if tbname != self.tb_name:
                self.tdCom.check_query_data(f'select wstart, {self.tdCom.stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {tbname} partition by {partition} state_window({state_window_col_name})  order by wstart,{state_window}', sorted=True)
            else:
                self.tdCom.check_query_data(f'select wstart, {self.tdCom.tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.tdCom.tb_source_select_str}  from {tbname} partition by {partition} state_window({state_window_col_name}) order by wstart,{state_window}', sorted=True)

        if fill_history_value:
            self.tdCom.update_delete_history_data(self.delete)

        if self.tdCom.subtable:
            tdSql.query(f'select * from {self.ctb_name}')
            ptn_counter = 0
            for c1_value in tdSql.queryResult:
                if partition == "c1":
                    if subtable:
                        tbname = self.tdCom.get_subtable_wait(f'{self.ctb_name}_{self.tdCom.subtable_prefix}{c1_value[1]}{self.tdCom.subtable_suffix}')
                        tdSql.query(f'select count(*) from `{tbname}`')
                    else:
                        tbname = self.tdCom.get_subtable_wait(f'{self.ctb_name}_{self.tdCom.subtable_prefix}{partition_elm_alias}{self.tdCom.subtable_suffix}')
                        tdSql.query(f'select count(*) from `{tbname}`')
                        return
                elif partition == "abs(c1)":
                    abs_c1_value = abs(c1_value[1])
                    tbname = self.tdCom.get_subtable_wait(f'{self.ctb_name}_{self.tdCom.subtable_prefix}{abs_c1_value}{self.tdCom.subtable_suffix}')
                    tdSql.query(f'select count(*) from `{tbname}`')
                elif partition == "tbname" and ptn_counter == 0:
                    tbname = self.tdCom.get_subtable_wait(f'{self.ctb_name}_{self.tdCom.subtable_prefix}{self.ctb_name}{self.tdCom.subtable_suffix}')
                    tdSql.query(f'select count(*) from `{tbname}`')
                    ptn_counter += 1
                tdSql.checkEqual(tdSql.queryResult[0][0] > 0, True)

            tdSql.query(f'select * from {self.tb_name}')
            ptn_counter = 0
            for c1_value in tdSql.queryResult:
                if partition == "c1":
                    if subtable:
                        tbname = self.tdCom.get_subtable_wait(f'{self.tb_name}_{self.tdCom.subtable_prefix}{c1_value[1]}{self.tdCom.subtable_suffix}')
                        tdSql.query(f'select count(*) from `{tbname}`')
                    else:
                        tbname = self.tdCom.get_subtable_wait(f'{self.tb_name}_{self.tdCom.subtable_prefix}{partition_elm_alias}{self.tdCom.subtable_suffix}')
                        tdSql.query(f'select count(*) from `{tbname}`')
                        return
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
        self.at_once_state_window(state_window="c2", partition="tbname", case_when="case when c1 < 0 then c1 else c2 end")
        self.at_once_state_window(state_window="c1", partition="tbname", case_when="case when c1 >= 0 then c1 else c2 end")
        for fill_history_value in [None, 1]:
            self.at_once_state_window(state_window="c1", partition="tbname", fill_history_value=fill_history_value)
            self.at_once_state_window(state_window="c1", partition="c1", fill_history_value=fill_history_value)
            self.at_once_state_window(state_window="c1", partition="abs(c1)", fill_history_value=fill_history_value)
            self.at_once_state_window(state_window="c1", partition="tbname", delete=True, fill_history_value=fill_history_value)
            self.at_once_state_window(state_window="c1", partition="c1", delete=True, fill_history_value=fill_history_value)
            self.at_once_state_window(state_window="c1", partition="abs(c1)", delete=True, fill_history_value=fill_history_value)
            self.at_once_state_window(state_window="c1", partition="c1", subtable=None, fill_history_value=fill_history_value)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())