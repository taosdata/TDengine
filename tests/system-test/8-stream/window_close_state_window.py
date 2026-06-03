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

    def window_close_state_window(self, state_window, delete=True):
        tdLog.info(f"*** testing stream window_close+session: state_window: {state_window}, delete: {delete} ***")
        self.case_name = sys._getframe().f_code.co_name
        self.delete = delete
        self.tdCom.prepare_data(state_window=state_window)
        self.stb_name = self.tdCom.stb_name.replace(f"{self.tdCom.dbname}.", "")
        self.ctb_name = self.tdCom.ctb_name.replace(f"{self.tdCom.dbname}.", "")
        self.tb_name = self.tdCom.tb_name.replace(f"{self.tdCom.dbname}.", "")
        self.stb_stream_des_table = f'{self.stb_name}{self.tdCom.des_table_suffix}'
        self.ctb_stream_des_table = f'{self.ctb_name}{self.tdCom.des_table_suffix}'
        self.tb_stream_des_table = f'{self.tb_name}{self.tdCom.des_table_suffix}'
        state_window_col_name = self.tdCom.dataDict["state_window"]
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.tdCom.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {self.ctb_name} state_window({state_window_col_name})', trigger_mode="window_close")
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.tdCom.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tdCom.tb_source_select_str}  from {self.tb_name} state_window({state_window_col_name})', trigger_mode="window_close")
        state_window_max = self.tdCom.dataDict['state_window_max']
        state_window_value_inmem = 0
        sleep_step = 0
        for i in range(self.tdCom.range_count):
            state_window_value = random.randint(int((i)*state_window_max/self.tdCom.range_count), int((i+1)*state_window_max/self.tdCom.range_count))
            while state_window_value == state_window_value_inmem:
                state_window_value = random.randint(int((i)*state_window_max/self.tdCom.range_count), int((i+1)*state_window_max/self.tdCom.range_count))
                if sleep_step < self.tdCom.default_interval:
                    sleep_step += 1
                    time.sleep(1)
                else:
                    return
            for j in range(2, self.tdCom.range_count+3):
                tdSql.execute(f'insert into {self.ctb_name} (ts, {state_window_col_name}) values ({self.tdCom.date_time}, {state_window_value})')
                tdSql.execute(f'insert into {self.tb_name} (ts, {state_window_col_name}) values ({self.tdCom.date_time}, {state_window_value})')
                if self.tdCom.update and i%2 == 0:
                    tdSql.execute(f'insert into {self.ctb_name} (ts, {state_window_col_name}) values ({self.tdCom.date_time}, {state_window_value})')
                    tdSql.execute(f'insert into {self.tb_name} (ts, {state_window_col_name}) values ({self.tdCom.date_time}, {state_window_value})')
                if self.delete and i%2 != 0:
                    dt = f'cast({self.tdCom.date_time-1} as timestamp)'
                    self.tdCom.sdelete_rows(tbname=self.ctb_name, start_ts=dt)
                    self.tdCom.sdelete_rows(tbname=self.tb_name, start_ts=dt)
                self.tdCom.date_time += 1
            for tbname in [self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    self.tdCom.check_stream(f'select wstart, {self.tdCom.stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}', f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {tbname} state_window({state_window_col_name}) limit {i}', i)
                else:
                    self.tdCom.check_stream(f'select wstart, {self.tdCom.tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}', f'select _wstart AS wstart, {self.tdCom.tb_source_select_str}  from {tbname} state_window({state_window_col_name}) limit {i}', i)
            state_window_value_inmem = state_window_value



    def run(self):
        for delete in [True, False]:
            self.window_close_state_window(state_window="c1", delete=delete)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())