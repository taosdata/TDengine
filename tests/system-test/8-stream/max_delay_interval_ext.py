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

    def watermark_max_delay_interval_ext(self, interval, max_delay, watermark=None, fill_value=None, partition="tbname", delete=False, fill_history_value=None, subtable=None, stb_field_name_value=None, tag_value=None, use_exist_stb=False):
        tdLog.info(f"*** testing stream max_delay+interval+exist_stb+custom_tag: interval: {interval}, partition: {partition}, max_delay: {max_delay}, fill_history: {fill_history_value}, subtable: {subtable}, stb_field_name_value: {stb_field_name_value}, tag_value: {tag_value} ***")
        if stb_field_name_value == self.tdCom.partitial_stb_filter_des_select_elm or stb_field_name_value == self.tdCom.exchange_stb_filter_des_select_elm:
            partitial_tb_source_str = self.tdCom.partitial_ext_tb_source_select_str
        else:
            partitial_tb_source_str = self.tdCom.ext_tb_source_select_str
        if not stb_field_name_value:
            stb_field_name_value = self.tdCom.tb_filter_des_select_elm
        self.delete = delete
        self.tdCom.case_name = sys._getframe().f_code.co_name
        defined_tag_count = len(tag_value.split())
        if watermark is not None:
            self.tdCom.case_name = "watermark" + sys._getframe().f_code.co_name
        self.tdCom.prepare_data(interval=interval, watermark=watermark, ext_stb=use_exist_stb)
        self.stb_name = self.tdCom.stb_name.replace(f"{self.tdCom.dbname}.", "")
        self.ctb_name = self.tdCom.ctb_name.replace(f"{self.tdCom.dbname}.", "")
        self.tb_name = self.tdCom.tb_name.replace(f"{self.tdCom.dbname}.", "")
        self.stb_stream_des_table = f'{self.stb_name}{self.tdCom.des_table_suffix}'
        self.ctb_stream_des_table = f'{self.ctb_name}{self.tdCom.des_table_suffix}'
        self.tb_stream_des_table = f'{self.tb_name}{self.tdCom.des_table_suffix}'
        if subtable:
            stb_subtable_value = f'concat(concat("{self.stb_name}_{self.subtable_prefix}", cast(cast(abs(cast({subtable} as int)) as bigint) as varchar(100))), "{self.subtable_suffix}")' if self.subtable else None
        else:
            stb_subtable_value = None
        self.tdCom.date_time = 1658921623245
        if watermark is not None:
            watermark_value = f'{self.tdCom.dataDict["watermark"]}s'
        else:
            watermark_value = None

        max_delay_value = f'{self.tdCom.trans_time_to_s(max_delay)}s'
        if fill_value:
            if "value" in fill_value.lower():
                fill_value='VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.tdCom.stream_suffix}', des_table=self.tdCom.ext_stb_stream_des_table, subtable_value=stb_subtable_value, source_sql=f'select _wstart AS wstart, {partitial_tb_source_str}  from {self.stb_name} interval({self.tdCom.dataDict["interval"]}s)', trigger_mode="max_delay", watermark=watermark_value, max_delay=max_delay_value, fill_value=fill_value, fill_history_value=fill_history_value, stb_field_name_value=stb_field_name_value, tag_value=tag_value, use_exist_stb=use_exist_stb)

        time.sleep(1)

        init_num = 0
        for i in range(self.tdCom.range_count):
            if i == 0:
                if watermark is not None:
                    window_close_ts = self.tdCom.cal_watermark_window_close_interval_endts(self.tdCom.date_time, self.tdCom.dataDict['interval'], self.tdCom.dataDict['watermark'])
                else:
                    window_close_ts = self.tdCom.cal_watermark_window_close_interval_endts(self.tdCom.date_time, self.tdCom.dataDict['interval'])
            else:
                self.tdCom.date_time = window_close_ts + self.tdCom.offset
                window_close_ts += self.tdCom.dataDict['interval']*self.tdCom.offset
            for num in range(int(window_close_ts/self.tdCom.offset-self.tdCom.date_time/self.tdCom.offset)):
                self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=self.tdCom.date_time+num*self.tdCom.offset)
                if self.tdCom.update and i%2 == 0:
                    self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=self.tdCom.date_time+num*self.tdCom.offset)

            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts-1)
            if self.tdCom.update and i%2 == 0:
                self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts-1)

            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
            if self.tdCom.update and i%2 == 0:
                self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts)

            if i == 0:
                init_num = 2 + i
                if watermark is not None:
                    init_num += 1
            else:
                init_num += 1
            time.sleep(int(max_delay.replace("s", "")))
            if tag_value:
                tdSql.query(f'select {tag_value} from {self.stb_name}')
                tag_value_list = tdSql.queryResult
            if not fill_value:
                self.tdCom.check_query_data(f'select {self.tdCom.stb_filter_des_select_elm} from ext_{self.stb_name}{self.tdCom.des_table_suffix} order by ts;', f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {self.stb_name} interval({self.tdCom.dataDict["interval"]}s)', defined_tag_count=defined_tag_count, tag_value_list=tag_value_list, partition=partition)

    def run(self):
        for delete in [True, False]:
            for fill_history_value in [0, 1]:
                self.watermark_max_delay_interval_ext(interval=random.choice([15]), watermark=random.randint(20, 25), max_delay=f"{random.randint(5, 6)}s", delete=delete, fill_history_value=fill_history_value, partition=None, subtable=None, stb_field_name_value=self.tdCom.tb_filter_des_select_elm, tag_value=self.tdCom.tag_filter_des_select_elm.split(",")[0], use_exist_stb=True)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())