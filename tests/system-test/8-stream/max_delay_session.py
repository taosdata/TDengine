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

    def watermark_max_delay_session(self, session, watermark, max_delay, fill_history_value=None):
        tdLog.info(f"*** testing stream max_delay+session: session: {session}, watermark: {watermark}, max_delay: {max_delay}, fill_history_value: {fill_history_value} ***")
        self.tdCom.case_name = sys._getframe().f_code.co_name
        if watermark is not None:
            self.tdCom.case_name = "watermark" + sys._getframe().f_code.co_name
        self.tdCom.prepare_data(session=session, watermark=watermark, fill_history_value=fill_history_value)
        self.stb_name = self.tdCom.stb_name.replace(f"{self.tdCom.dbname}.", "")
        self.ctb_name = self.tdCom.ctb_name.replace(f"{self.tdCom.dbname}.", "")
        self.tb_name = self.tdCom.tb_name.replace(f"{self.tdCom.dbname}.", "")
        self.stb_stream_des_table = f'{self.stb_name}{self.tdCom.des_table_suffix}'
        self.ctb_stream_des_table = f'{self.ctb_name}{self.tdCom.des_table_suffix}'
        self.tb_stream_des_table = f'{self.tb_name}{self.tdCom.des_table_suffix}'
        self.tdCom.date_time = self.tdCom.dataDict["start_ts"]

        time.sleep(1)

        if watermark is not None:
            watermark_value = f'{self.tdCom.dataDict["watermark"]}s'
        else:
            watermark_value = None
        max_delay_value = f'{self.tdCom.trans_time_to_s(max_delay)}s'
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.tdCom.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, _wend AS wend, {self.tdCom.stb_source_select_str}  from {self.ctb_name} session(ts, {self.tdCom.dataDict["session"]}s)', trigger_mode="max_delay", watermark=watermark_value, max_delay=max_delay_value, fill_history_value=fill_history_value)
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.tdCom.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS wstart, _wend AS wend, {self.tdCom.tb_source_select_str}  from {self.tb_name} session(ts, {self.tdCom.dataDict["session"]}s)', trigger_mode="max_delay", watermark=watermark_value, max_delay=max_delay_value, fill_history_value=fill_history_value)
        init_num = 0
        time.sleep(1)

        for i in range(self.tdCom.range_count):
            if i == 0:
                window_close_ts = self.tdCom.cal_watermark_window_close_session_endts(self.tdCom.date_time, self.tdCom.dataDict['watermark'], self.tdCom.dataDict['session'])
            else:
                self.tdCom.date_time = window_close_ts + 1
                window_close_ts = self.tdCom.cal_watermark_window_close_session_endts(self.tdCom.date_time, self.tdCom.dataDict['watermark'], self.tdCom.dataDict['session'])

            if watermark_value is not None:
                for ts_value in [self.tdCom.date_time, window_close_ts-1]:
                    self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=ts_value)
                    self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=ts_value)
                    if self.tdCom.update and i%2 == 0:
                        self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=ts_value)
                        self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=ts_value)
                    for tbname in [self.ctb_stream_des_table, self.tb_stream_des_table]:
                        if tbname != self.tb_stream_des_table:
                            tdSql.query(f'select wstart, {self.tdCom.stb_output_select_str} from {tbname}')
                        else:
                            tdSql.query(f'select wstart, {self.tdCom.tb_output_select_str} from {tbname}')
                        # if not fill_history_value:
                        #     tdSql.checkEqual(tdSql.queryRows, init_num)

            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
            self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=window_close_ts)
            if self.tdCom.update and i%2 == 0:
                self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
                self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=window_close_ts)
            if i == 0:
                init_num = 2 + i
            else:
                init_num += 1
            if watermark_value is not None:
                expected_value = init_num
            else:
                expected_value = i + 1

            if not fill_history_value:
                for tbname in [self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        self.tdCom.check_stream(f'select wstart, wend-{self.tdCom.dataDict["session"]}s, {self.tdCom.stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}', f'select _wstart AS wstart, _wend AS wend, {self.tdCom.stb_source_select_str}  from {tbname} session(ts, {self.tdCom.dataDict["session"]}s)', expected_value, max_delay)
                    else:
                        self.tdCom.check_stream(f'select wstart, wend-{self.tdCom.dataDict["session"]}s, {self.tdCom.tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}', f'select _wstart AS wstart, _wend AS wend, {self.tdCom.tb_source_select_str}  from {tbname} session(ts, {self.tdCom.dataDict["session"]}s)', expected_value, max_delay)
            else:
                self.tdCom.update_delete_history_data(delete=True)
                for tbname in [self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        self.tdCom.check_query_data(f'select wstart, wend-{self.tdCom.dataDict["session"]}s, {self.tdCom.stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}', f'select _wstart AS wstart, _wend AS wend, {self.tdCom.stb_source_select_str}  from {tbname} session(ts, {self.tdCom.dataDict["session"]}s)')
                    else:
                        self.tdCom.check_query_data(f'select wstart, wend-{self.tdCom.dataDict["session"]}s, {self.tdCom.tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}', f'select _wstart AS wstart, _wend AS wend, {self.tdCom.tb_source_select_str}  from {tbname} session(ts, {self.tdCom.dataDict["session"]}s)')

    def run(self):
        for fill_history_value in [None, 1]:
            for watermark in [None, random.randint(20, 30)]:
                self.watermark_max_delay_session(session=random.randint(10, 15), watermark=watermark, max_delay=f"{random.randint(1, 3)}s", fill_history_value=fill_history_value)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())