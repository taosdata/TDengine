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

    def window_close_interval(self, interval, watermark=None, ignore_expired=None, partition="tbname", fill_value=None, delete=False):
        tdLog.info(f"*** testing stream window_close+interval: interval: {interval}, watermark: {watermark}, ignore_expired: {ignore_expired}, partition: {partition}, fill: {fill_value}, delete: {delete} ***")
        self.delete = delete
        self.tdCom.case_name = sys._getframe().f_code.co_name
        if watermark is not None:
            self.tdCom.case_name = "watermark" + sys._getframe().f_code.co_name
        self.tdCom.prepare_data(interval=interval, watermark=watermark)
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
        else:
            partition_elm_alias = self.tdCom.partition_tag_alias
        if partition == "tbname":
            stb_subtable_value = f'concat(concat("{self.stb_name}_{self.tdCom.subtable_prefix}", {partition_elm_alias}), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
            ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.tdCom.subtable_prefix}", {partition_elm_alias}), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
            tb_subtable_value = f'concat(concat("{self.tb_name}_{self.tdCom.subtable_prefix}", {partition_elm_alias}), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
        else:
            stb_subtable_value = f'concat(concat("{self.stb_name}_{self.tdCom.subtable_prefix}", cast({partition_elm_alias} as varchar(20))), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
            ctb_subtable_value = f'concat(concat("{self.ctb_name}_{self.tdCom.subtable_prefix}", cast({partition_elm_alias} as varchar(20))), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
            tb_subtable_value = f'concat(concat("{self.tb_name}_{self.tdCom.subtable_prefix}", cast({partition_elm_alias} as varchar(20))), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None

        if watermark is not None:
            watermark_value = f'{self.tdCom.dataDict["watermark"]}s'
        else:
            watermark_value = None
        # create stb/ctb/tb stream
        if fill_value:
            if "value" in fill_value.lower():
                fill_value='VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.tdCom.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {self.stb_name} partition by {partition} {partition_elm_alias} interval({self.tdCom.dataDict["interval"]}s)', trigger_mode="window_close", watermark=watermark_value, ignore_expired=ignore_expired, subtable_value=stb_subtable_value, fill_value=fill_value)
        self.tdCom.create_stream(stream_name=f'{self.ctb_name}{self.tdCom.stream_suffix}', des_table=self.ctb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {self.ctb_name} partition by {partition} {partition_elm_alias} interval({self.tdCom.dataDict["interval"]}s)', trigger_mode="window_close", watermark=watermark_value, ignore_expired=ignore_expired, subtable_value=ctb_subtable_value, fill_value=fill_value)
        if fill_value:
            if "value" in fill_value.lower():
                fill_value='VALUE,1,2,3,4,5,6,7,8,9,10,11'
        self.tdCom.create_stream(stream_name=f'{self.tb_name}{self.tdCom.stream_suffix}', des_table=self.tb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.tdCom.tb_source_select_str}  from {self.tb_name} partition by {partition} {partition_elm_alias} interval({self.tdCom.dataDict["interval"]}s)', trigger_mode="window_close", watermark=watermark_value, ignore_expired=ignore_expired, subtable_value=tb_subtable_value, fill_value=fill_value)

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
            if i == 0:
                record_window_close_ts = window_close_ts
            for num in range(int(window_close_ts/self.tdCom.offset-self.tdCom.date_time/self.tdCom.offset)):
                ts_value=self.tdCom.date_time+num*self.tdCom.offset
                self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=ts_value)
                self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=ts_value)
                if self.tdCom.update and i%2 == 0:
                    self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=ts_value)
                    self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=ts_value)

                if self.delete and i%2 != 0:
                    dt = f'cast({ts_value-num*self.tdCom.offset} as timestamp)'
                    self.tdCom.sdelete_rows(tbname=self.ctb_name, start_ts=dt)
                    self.tdCom.sdelete_rows(tbname=self.tb_name, start_ts=dt)
                if not fill_value:
                    for tbname in [self.stb_stream_des_table, self.ctb_stream_des_table, self.tb_stream_des_table]:
                        if tbname != self.tb_stream_des_table:
                            tdSql.query(f'select wstart, {self.tdCom.stb_output_select_str} from {tbname}')
                        else:
                            tdSql.query(f'select wstart, {self.tdCom.tb_output_select_str} from {tbname}')
                        tdSql.checkEqual(tdSql.queryRows, i)

            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts-1)
            self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=window_close_ts-1)
            if self.tdCom.update and i%2 == 0:
                self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts-1)
                self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=window_close_ts-1)
            if not fill_value:
                for tbname in [self.stb_stream_des_table, self.ctb_stream_des_table, self.tb_stream_des_table]:
                    if tbname != self.tb_stream_des_table:
                        tdSql.query(f'select wstart, {self.tdCom.stb_output_select_str} from {tbname}')
                    else:
                        tdSql.query(f'select wstart, {self.tdCom.tb_output_select_str} from {tbname}')

                    tdSql.checkEqual(tdSql.queryRows, i)

            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
            self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=window_close_ts)
            if self.tdCom.update and i%2 == 0:
                self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
                self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=window_close_ts)

            if not fill_value:
                for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        self.tdCom.check_stream(f'select wstart, {self.tdCom.stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {tbname}  partition by {partition} interval({self.tdCom.dataDict["interval"]}s) order by wstart limit {i+1}', i+1)
                    else:
                        self.tdCom.check_stream(f'select wstart, {self.tdCom.tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.tdCom.tb_source_select_str}  from {tbname}  partition by {partition} interval({self.tdCom.dataDict["interval"]}s) order by wstart limit {i+1}', i+1)
        if self.tdCom.disorder and not fill_value:
            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=record_window_close_ts)
            self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=record_window_close_ts)
            if ignore_expired:
                for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        tdSql.query(f'select wstart, {self.tdCom.stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}')
                        res1 = tdSql.queryResult
                        tdSql.query(f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {tbname} interval({self.tdCom.dataDict["interval"]}s) limit {i+1}')
                        res2 = tdSql.queryResult
                        tdSql.checkNotEqual(res1, res2)
                    else:
                        tdSql.query(f'select wstart, {self.tdCom.tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}')
                        res1 = tdSql.queryResult
                        tdSql.query(f'select _wstart AS wstart, {self.tdCom.tb_source_select_str}  from {tbname} interval({self.tdCom.dataDict["interval"]}s) limit {i+1}')
                        res2 = tdSql.queryResult
                        tdSql.checkNotEqual(res1, res2)
            else:
                for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                    if tbname != self.tb_name:
                        self.tdCom.check_stream(f'select wstart, {self.tdCom.stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}', f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {tbname} interval({self.tdCom.dataDict["interval"]}s) limit {i+1}', i+1)
                    else:
                        self.tdCom.check_stream(f'select wstart, {self.tdCom.tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix}', f'select _wstart AS wstart, {self.tdCom.tb_source_select_str}  from {tbname} interval({self.tdCom.dataDict["interval"]}s) limit {i+1}', i+1)
        if self.tdCom.subtable:
            tdSql.query(f'select * from {self.ctb_name}')
            for tname in [self.stb_name, self.ctb_name]:
                ptn_counter = 0
                for c1_value in tdSql.queryResult:
                    if partition == "c1":
                        tbname = self.tdCom.get_subtable_wait(f'{tname}_{self.tdCom.subtable_prefix}{c1_value[1]}{self.tdCom.subtable_suffix}')
                        tdSql.query(f'select count(*) from `{tbname}`', count_expected_res=self.tdCom.range_count)
                    elif partition == "abs(c1)":
                        abs_c1_value = abs(c1_value[1])
                        tbname = self.tdCom.get_subtable_wait(f'{tname}_{self.tdCom.subtable_prefix}{abs_c1_value}{self.tdCom.subtable_suffix}')
                        tdSql.query(f'select count(*) from `{tbname}`', count_expected_res=self.tdCom.range_count)
                    elif partition == "tbname" and ptn_counter == 0:
                        tbname = self.tdCom.get_subtable_wait(f'{tname}_{self.tdCom.subtable_prefix}{self.ctb_name}{self.tdCom.subtable_suffix}')
                        tdSql.query(f'select count(*) from `{tbname}`', count_expected_res=self.tdCom.range_count)
                        ptn_counter += 1

                    tdSql.checkEqual(tdSql.queryResult[0][0] , self.tdCom.range_count)
                    tdSql.checkEqual(tdSql.queryResult[0][0] > 0, True)

            tdSql.query(f'select * from {self.tb_name}')
            ptn_counter = 0
            for c1_value in tdSql.queryResult:
                if partition == "c1":
                    tbname = self.tdCom.get_subtable_wait(f'{self.tb_name}_{self.tdCom.subtable_prefix}{c1_value[1]}{self.tdCom.subtable_suffix}')
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
                    if i == 0:
                        record_window_close_ts = window_close_ts
                    for num in range(int(window_close_ts/self.tdCom.offset-self.tdCom.date_time/self.tdCom.offset)):
                        ts_value=self.tdCom.date_time+num*self.tdCom.offset
                        self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=ts_value)
                        self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=ts_value)
                    self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts-1)
                    self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=window_close_ts-1)
                    self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts)
                    self.tdCom.sinsert_rows(tbname=self.tb_name, ts_value=window_close_ts)
            if self.delete:
                self.tdCom.sdelete_rows(tbname=self.ctb_name, start_ts=self.tdCom.time_cast(start_time), end_ts=self.tdCom.time_cast(window_close_ts))
                self.tdCom.sdelete_rows(tbname=self.tb_name, start_ts=self.tdCom.time_cast(start_time), end_ts=self.tdCom.time_cast(window_close_ts))
            self.tdCom.date_time = start_time
            for tbname in [self.stb_name, self.ctb_name, self.tb_name]:
                if tbname != self.tb_name:
                    if "value" in fill_value.lower():
                        fill_value='VALUE,1,2,3,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
                    if (fill_value == "NULL" or fill_value == "NEXT" or fill_value == "LINEAR") and self.delete:
                        self.tdCom.check_query_data(f'select wstart, {self.tdCom.fill_stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} order by wstart', f'select * from (select _wstart AS wstart, {self.tdCom.fill_stb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts}  partition by {partition} interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by wstart) where `min(c1)` is not Null', fill_value=fill_value)
                    else:
                        if self.delete and (fill_value == "PREV" or "value" in fill_value.lower()):
                            additional_options = f"where ts >= {start_ts}-1s and  ts <= {start_ts}"
                        else:
                            additional_options = f"where ts >= {start_ts} and ts <= {end_ts}"
                        self.tdCom.check_query_data(f'select wstart, {self.tdCom.fill_stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.tdCom.fill_stb_source_select_str}  from {tbname} {additional_options}  partition by {partition} interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by wstart', fill_value=fill_value)
                else:
                    if "value" in fill_value.lower():
                        fill_value='VALUE,1,2,3,6,7,8,9,10,11'
                    if (fill_value == "NULL" or fill_value == "NEXT" or fill_value == "LINEAR") and self.delete:
                        self.tdCom.check_query_data(f'select wstart, {self.tdCom.fill_tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} order by wstart', f'select * from (select _wstart AS wstart, {self.tdCom.fill_tb_source_select_str}  from {tbname} where ts >= {start_ts} and ts <= {end_ts}  partition by {partition} interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by wstart) where `min(c1)` is not Null', fill_value=fill_value)
                    else:
                        if self.delete and (fill_value == "PREV" or "value" in fill_value.lower()):
                            additional_options = f"where ts >= {start_ts}-1s and  ts <= {start_ts}"
                        else:
                            additional_options = f"where ts >= {start_ts} and ts <= {end_ts}"
                        self.tdCom.check_query_data(f'select wstart, {self.tdCom.fill_tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.tdCom.fill_tb_source_select_str}  from {tbname} {additional_options}  partition by {partition} interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by wstart', fill_value=fill_value)


    def run(self):
        for watermark in [None, random.randint(15, 20)]:
            for ignore_expired in [0, 1]:
                self.window_close_interval(interval=random.randint(10, 15), watermark=watermark, ignore_expired=ignore_expired)
        for fill_value in ["NULL", "PREV", "NEXT", "LINEAR", "VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11"]:
            for watermark in [None, random.randint(15, 20)]:
                self.window_close_interval(interval=random.randint(10, 12), watermark=watermark, fill_value=fill_value)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())