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

    def watermark_max_delay_interval(self, interval, max_delay, watermark=None, fill_value=None, delete=False):
        tdLog.info(f"*** testing stream max_delay+interval: interval: {interval}, watermark: {watermark}, fill_value: {fill_value}, delete: {delete} ***")
        self.delete = delete
        self.tdCom.case_name = sys._getframe().f_code.co_name
        if watermark is not None:
            self.case_name = "watermark" + sys._getframe().f_code.co_name
        self.tdCom.prepare_data(interval=interval, watermark=watermark)
        self.stb_name = self.tdCom.stb_name.replace(f"{self.tdCom.dbname}.", "")
        self.ctb_name = self.tdCom.ctb_name.replace(f"{self.tdCom.dbname}.", "")
        self.tb_name = self.tdCom.tb_name.replace(f"{self.tdCom.dbname}.", "")
        self.stb_stream_des_table = f'{self.stb_name}{self.tdCom.des_table_suffix}'
        self.ctb_stream_des_table = f'{self.ctb_name}{self.tdCom.des_table_suffix}'
        self.tb_stream_des_table = f'{self.tb_name}{self.tdCom.des_table_suffix}'
        self.tdCom.date_time = 1658921623245
        if watermark is not None:
            watermark_value = f'{self.tdCom.dataDict["watermark"]}s'
            fill_watermark_value = watermark_value
        else:
            watermark_value = None
            fill_watermark_value = "0s"

        max_delay_value = f'{self.tdCom.trans_time_to_s(max_delay)}s'
        if fill_value:
            if "value" in fill_value.lower():
                fill_value='VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.tdCom.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {self.stb_name} interval({self.tdCom.dataDict["interval"]}s)', trigger_mode="max_delay", watermark=watermark_value, max_delay=max_delay_value, fill_value=fill_value)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.tdCom.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {self.ctb_name} interval({self.tdCom.dataDict["interval"]}s)', trigger_mode="max_delay", watermark=watermark_value, max_delay=max_delay_value, fill_value=fill_value)
        if fill_value:
            if "value" in fill_value.lower():
                fill_value='VALUE,1,2,3,4,5,6,7,8,9,10,11'
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.tdCom.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tdCom.tb_source_select_str}  from {self.tb_name} interval({self.tdCom.dataDict["interval"]}s)', trigger_mode="max_delay", watermark=watermark_value, max_delay=max_delay_value, fill_value=fill_value)
        init_num = 0
        start_time = self.tdCom.date_time
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
                self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=self.tdCom.date_time+num*self.tdCom.offset)
                if self.tdCom.update and i%2 == 0:
                    self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=self.tdCom.date_time+num*self.tdCom.offset)
                    self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=self.tdCom.date_time+num*self.tdCom.offset)
                # if not fill_value:
                #     for tbname in [self.stb_stream_des_table, self.ctb_stream_des_table, self.tb_stream_des_table]:
                #         if tbname != self.tb_stream_des_table:
                #             tdSql.query(f'select wstart, {self.tdCom.stb_output_select_str} from {tbname}')
                #         else:
                #             tdSql.query(f'select wstart, {self.tdCom.tb_output_select_str} from {tbname}')
                #         tdSql.checkEqual(tdSql.queryRows, init_num)

            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts-1)
            self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=window_close_ts-1)
            if self.tdCom.update and i%2 == 0:
                self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts-1)
                self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=window_close_ts-1)

            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
            self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=window_close_ts)
            if self.tdCom.update and i%2 == 0:
                self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
                self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=window_close_ts)

            if i == 0:
                init_num = 2 + i
                if watermark is not None:
                    init_num += 1
            else:
                init_num += 1
            time.sleep(int(max_delay.replace("s", "")))
            if not fill_value:
                for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        self.tdCom.check_query_data(f'select wstart, {self.tdCom.stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}', f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {tbname} interval({self.tdCom.dataDict["interval"]}s)')
                    else:
                        self.tdCom.check_query_data(f'select wstart, {self.tdCom.tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}', f'select _wstart AS wstart, {self.tdCom.tb_source_select_str}  from {tbname} interval({self.tdCom.dataDict["interval"]}s)')
        if fill_value:
            history_ts = str(start_time)+f'-{self.tdCom.dataDict["interval"]*(self.tdCom.range_count+2)}s'
            start_ts = self.tdCom.time_cast(history_ts, "-")
            future_ts = str(self.tdCom.date_time)+f'+{self.tdCom.dataDict["interval"]*(self.tdCom.range_count+2)}s'
            end_ts = self.tdCom.time_cast(future_ts)
            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=history_ts)
            self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=history_ts)
            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=future_ts)
            self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=future_ts)
            future_ts_bigint = self.tdCom.str_ts_trans_bigint(future_ts)
            if watermark is not None:
                window_close_ts = self.tdCom.cal_watermark_window_close_interval_endts(future_ts_bigint, self.tdCom.dataDict['interval'], self.tdCom.dataDict['watermark'])
            else:
                window_close_ts = self.tdCom.cal_watermark_window_close_interval_endts(future_ts_bigint, self.tdCom.dataDict['interval'])
            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
            self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=window_close_ts)

            if self.tdCom.update:
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
                        self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=self.tdCom.date_time+num*self.tdCom.offset)

                    self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts-1)
                    self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=window_close_ts-1)
                    self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
                    self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=window_close_ts)
            if self.delete:
                self.tdCom.sdelete_rows(tbname=self.ctb_name, start_ts=self.tdCom.time_cast(start_time), end_ts=self.tdCom.time_cast(window_close_ts))
                self.tdCom.sdelete_rows(tbname=self.tb_name, start_ts=self.tdCom.time_cast(start_time), end_ts=self.tdCom.time_cast(window_close_ts))
            time.sleep(int(max_delay.replace("s", "")))
            for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    if "value" in fill_value.lower():
                        fill_value='VALUE,1,2,3,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
                    self.tdCom.check_query_data(f'select wstart, {self.tdCom.fill_stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}', f'select _wstart AS wstart, {self.tdCom.fill_stb_source_select_str}  from {tbname}  where ts >= {start_ts} and ts <= {end_ts}+{self.tdCom.dataDict["interval"]}s+{fill_watermark_value}  interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value})', fill_value=fill_value)
                else:
                    if "value" in fill_value.lower():
                        fill_value='VALUE,1,2,3,6,7,8,9,10,11'
                    self.tdCom.check_query_data(f'select wstart, {self.tdCom.fill_tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}', f'select _wstart AS wstart, {self.tdCom.fill_tb_source_select_str}  from {tbname}  where ts >= {start_ts} and ts <= {end_ts}+{self.tdCom.dataDict["interval"]}s+{fill_watermark_value}  interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value})', fill_value=fill_value)


    def run(self):
        for watermark in [None, random.randint(20, 25)]:
            self.watermark_max_delay_interval(interval=random.choice([15]), watermark=watermark, max_delay=f"{random.randint(5, 6)}s")
        for fill_value in ["NULL", "PREV", "NEXT", "LINEAR", "VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11"]:
            self.watermark_max_delay_interval(interval=random.randint(10, 15), watermark=None, max_delay=f"{random.randint(5, 6)}s", fill_value=fill_value)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())