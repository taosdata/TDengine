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

    def watermark_window_close_session_ext(self, session, watermark, fill_history_value=None, partition=None, subtable=None, stb_field_name_value=None, tag_value=None, use_exist_stb=False, delete=False):
        tdLog.info(f"*** testing stream window_close+session+exist_stb+custom_tag: session: {session}, partition: {partition}, fill_history: {fill_history_value}, subtable: {subtable}, stb_field_name_value: {stb_field_name_value}, tag_value: {tag_value} ***")
        if stb_field_name_value == self.tdCom.partitial_stb_filter_des_select_elm or stb_field_name_value == self.tdCom.exchange_stb_filter_des_select_elm:
            partitial_tb_source_str = self.tdCom.partitial_ext_tb_source_select_str
        else:
            partitial_tb_source_str = self.tdCom.ext_tb_source_select_str
        if not stb_field_name_value:
            stb_field_name_value = self.tdCom.tb_filter_des_select_elm
        self.tdCom.case_name = sys._getframe().f_code.co_name
        defined_tag_count = len(tag_value.split())
        if watermark is not None:
            self.case_name = "watermark" + sys._getframe().f_code.co_name
        self.tdCom.prepare_data(session=session, watermark=watermark, fill_history_value=fill_history_value, ext_stb=use_exist_stb)
        self.stb_name = self.tdCom.stb_name.replace(f"{self.tdCom.dbname}.", "")
        self.ctb_name = self.tdCom.ctb_name.replace(f"{self.tdCom.dbname}.", "")
        self.tb_name = self.tdCom.tb_name.replace(f"{self.tdCom.dbname}.", "")
        self.stb_stream_des_table = f'{self.stb_name}{self.tdCom.des_table_suffix}'
        self.ctb_stream_des_table = f'{self.ctb_name}{self.tdCom.des_table_suffix}'
        self.tb_stream_des_table = f'{self.tb_name}{self.tdCom.des_table_suffix}'
        self.tdCom.date_time = self.tdCom.dataDict["start_ts"]
        if subtable:
            stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", cast(cast(abs(cast({subtable} as int)) as bigint) as varchar(100))), "{self.subtable_suffix}")' if self.subtable else None
        else:
            stb_subtable_value = None
        if watermark is not None:
            watermark_value = f'{self.tdCom.dataDict["watermark"]}s'
        else:
            watermark_value = None
        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.tdCom.stream_suffix}', des_table=self.tdCom.ext_stb_stream_des_table, source_sql=f'select _wstart AS wstart, {partitial_tb_source_str} from {self.stb_name} session(ts, {self.tdCom.dataDict["session"]}s)', trigger_mode="window_close", watermark=watermark_value, subtable_value=stb_subtable_value, fill_history_value=fill_history_value, stb_field_name_value=stb_field_name_value, tag_value=tag_value, use_exist_stb=use_exist_stb)

        time.sleep(1)

        for i in range(self.tdCom.range_count):
            if i == 0:
                window_close_ts = self.tdCom.cal_watermark_window_close_session_endts(self.tdCom.date_time, self.tdCom.dataDict['watermark'], self.tdCom.dataDict['session'])
            else:
                self.tdCom.date_time = window_close_ts + 1
                window_close_ts = self.tdCom.cal_watermark_window_close_session_endts(self.tdCom.date_time, self.tdCom.dataDict['watermark'], self.tdCom.dataDict['session'])
            if watermark_value is not None:
                expected_value = i + 1
                for ts_value in [self.tdCom.date_time, window_close_ts-1]:
                    self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=ts_value)
                    if self.tdCom.update and i%2 == 0:
                        self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=ts_value)
            else:
                expected_value = i
            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
            if self.tdCom.update and i%2 == 0:
                self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts)

            if fill_history_value:
                self.tdCom.update_delete_history_data(delete=delete)
            if tag_value:
                tdSql.query(f'select {tag_value} from {self.stb_name}')
                tag_value_list = tdSql.queryResult
            self.tdCom.check_query_data(f'select {self.tdCom.stb_filter_des_select_elm} from ext_{self.stb_name}{self.tdCom.des_table_suffix} order by ts', f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {self.stb_name} session(ts, {self.tdCom.dataDict["session"]}s) order by wstart limit {expected_value};', sorted=True, defined_tag_count=defined_tag_count, tag_value_list=tag_value_list, partition=partition)

    def run(self):
        #! TD-25893
        # self.watermark_window_close_session_ext(session=random.randint(10, 12), watermark=random.randint(20, 25), subtable=None, partition=None, stb_field_name_value=self.tdCom.tb_filter_des_select_elm, tag_value=self.tdCom.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True, delete=False, fill_history_value=1)
        self.watermark_window_close_session_ext(session=random.randint(10, 12), watermark=random.randint(20, 25), subtable=None, partition=None, stb_field_name_value=self.tdCom.tb_filter_des_select_elm, tag_value=self.tdCom.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True, delete=True)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())