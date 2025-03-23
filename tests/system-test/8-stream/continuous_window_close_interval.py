import sys
import threading
from util.log import *
from util.sql import *
from util.cases import *
from util.common import *


class TDTestCase:
    updatecfgDict = {"debugFlag": 135, "asynclog": 0, "ratioOfVnodeStreamThreads": 4}

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.tdCom = tdCom
    
    def check_stream_all_task_status(self, stream_timeout=0):
        """check stream status

        Args:
            stream_name (str): stream_name
        Returns:
            str: status
        """
        timeout = self.stream_timeout if stream_timeout is None else stream_timeout

        #check stream task rows
        sql_task_status = f"select * from information_schema.ins_stream_tasks where status != \"ready\";"
        sql_task_all = f"select * from information_schema.ins_stream_tasks;"
                
        #check stream task status
        checktimes = 0
        while checktimes <= timeout:
            tdLog.notice(f"checktimes:{checktimes}")
            try:
                tdSql.query(sql_task_status,row_tag=True)
                result_task_status_rows = tdSql.getRows()
                if result_task_status_rows == 0:
                    tdSql.query(sql_task_all,row_tag=True)
                    result_task_status_rows = tdSql.getRows()
                    if result_task_status_rows > 0:
                        break
                time.sleep(1) 
                checktimes += 1 
            except Exception as e:
                tdLog.notice(f"Try to check stream status again, check times: {checktimes}")
                checktimes += 1 
                tdSql.print_error_frame_info(f"status is not ready")
        else:
            tdLog.notice(f"it has spend {checktimes} for checking stream task status but it failed")
            if checktimes == timeout:
                tdSql.print_error_frame_info(f"status is ready,")
    
    def docontinuous(
        self,
        interval,
        watermark=None,
        partition=None,
        fill_value=None,
        fill_history_value=None,
        ignore_expired=0,
        ignore_update=0,
        use_exist_stb=None,
        tag_value=None
    ):
        tdLog.info(f"*** testing stream continuous window close: interval: {interval}, partition: {partition}, fill_history: {fill_history_value}, use_exist_stb: {use_exist_stb}, fill: {fill_value}, tag_value: {tag_value} ***")
        self.tdCom.case_name = sys._getframe().f_code.co_name
        if watermark is not None:
            self.tdCom.case_name = "watermark" + sys._getframe().f_code.co_name
        self.tdCom.prepare_data(interval=interval, watermark=watermark, ext_stb=use_exist_stb)
        tdLog.info(
            f"testing stream continue_window_close finish prepare_data"
        )

        sqlstr = "alter local 'streamCoverage' '1'"
        tdSql.query(sqlstr)
        recalculatetime = 60
        recalculatetimeStr = f"recalculate {recalculatetime}s"

        self.stb_name = self.tdCom.stb_name.replace(f"{self.tdCom.dbname}.", "")
        self.ctb_name = self.tdCom.ctb_name.replace(f"{self.tdCom.dbname}.", "")
        self.tb_name = self.tdCom.tb_name.replace(f"{self.tdCom.dbname}.", "")
        self.stb_stream_des_table = f'{self.stb_name}{self.tdCom.des_table_suffix}'
        self.delete = True

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
        else:
            stb_subtable_value = f'concat(concat("{self.stb_name}_{self.tdCom.subtable_prefix}", cast({partition_elm_alias} as varchar(20))), "{self.tdCom.subtable_suffix}")' if self.tdCom.subtable else None
            
        if watermark is not None:
            watermark_value = f'{self.tdCom.dataDict["watermark"]}s'
        else:
            watermark_value = None
        # create stb/ctb/tb stream
        if fill_value:
            if "value" in fill_value.lower():
                fill_value='VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
        tdLog.info(
            f"*** testing stream continue_window_close + interval + fill. partition: {partition}, interval: {interval}, fill: {fill_value} ***"
        )

        # no subtable
        # create stream super table and child table
        tdLog.info("create stream super table and child table")
        if use_exist_stb:
            self.stb_stream_des_table = self.tdCom.ext_stb_stream_des_table
            self.des_select_str = self.tdCom.ext_tb_source_select_str
        else:
            self.des_select_str = self.tdCom.stb_source_select_str
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.tdCom.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=f'select _wstart AS wstart, {self.des_select_str}  from {self.stb_name} partition by {partition} {partition_elm_alias} interval({self.tdCom.dataDict["interval"]}s)', trigger_mode="continuous_window_close", watermark=watermark_value, ignore_expired=ignore_expired, subtable_value=stb_subtable_value, fill_value=fill_value, use_exist_stb=use_exist_stb, tag_value=tag_value, max_delay=recalculatetimeStr)

        # wait and check stream_task status is ready
        tdSql.query("show streams")
        tdLog.info(f"tdSql.queryResult:{tdSql.queryResult},tdSql.queryRows:{tdSql.queryRows}")
        self.check_stream_all_task_status(
            stream_timeout=120
        )

        # insert data
        start_time = self.tdCom.date_time
        print(f"range count:{self.tdCom.range_count}")
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
                ts_value=self.tdCom.date_time+num*self.tdCom.offset
                self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=ts_value)
                if i%2 == 0:
                    self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=ts_value)
                if self.delete and i%2 != 0:
                    ts_cast_delete_value = self.tdCom.time_cast(ts_value)
                    self.tdCom.sdelete_rows(tbname=self.ctb_name, start_ts=ts_cast_delete_value)

                if not fill_value and partition != "c1":
                    for tbname in [self.stb_stream_des_table]:
                        if use_exist_stb and tbname == self.stb_stream_des_table:
                            tdSql.waitedQuery(f'select {self.tdCom.partitial_stb_filter_des_select_elm} from {self.stb_stream_des_table}', i, 60)
                        else:
                            tdSql.waitedQuery(f'select wstart, {self.tdCom.stb_output_select_str} from {tbname}', i, 60)

            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts-1)

            if not fill_value:
                for tbname in [self.stb_stream_des_table]:
                    if use_exist_stb and tbname == self.stb_stream_des_table:
                        tdSql.waitedQuery(f'select {self.tdCom.partitial_stb_filter_des_select_elm} from {self.stb_stream_des_table}', i, 60)
                    else:
                        tdSql.waitedQuery(f'select wstart, {self.tdCom.stb_output_select_str} from {tbname}', i, 60)

        start_ts = start_time
        future_ts = str(self.tdCom.date_time)+f'+{self.tdCom.dataDict["interval"]*(self.tdCom.range_count+2)}s'
        end_ts = self.tdCom.time_cast(future_ts)
        self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=future_ts)
        future_ts_bigint = self.tdCom.str_ts_trans_bigint(future_ts)
        if watermark is not None:
            window_close_ts = self.tdCom.cal_watermark_window_close_interval_endts(future_ts_bigint, self.tdCom.dataDict['interval'], self.tdCom.dataDict['watermark'])
        else:
            window_close_ts = self.tdCom.cal_watermark_window_close_interval_endts(future_ts_bigint, self.tdCom.dataDict['interval'])
        self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=window_close_ts)

        waitTime = recalculatetime * 2
        tdLog.info(f"sleep {waitTime} s")
        time.sleep(waitTime)

        if fill_value:
            for tbname in [self.stb_name]:
                if "value" in fill_value.lower():
                    fill_value='VALUE,1,2,3,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11'
                additional_options = f"where ts >= {start_ts} and ts <= {end_ts}"
                self.tdCom.check_query_data(f'select wstart, {self.tdCom.fill_stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.tdCom.fill_stb_source_select_str}  from {tbname} {additional_options}  partition by {partition} interval({self.tdCom.dataDict["interval"]}s) fill ({fill_value}) order by wstart', fill_value=fill_value)
        else:
            for tbname in [self.stb_name]:
                additional_options = f"where ts <= {end_ts}"
                self.tdCom.check_query_data(f'select wstart, {self.tdCom.stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} order by wstart', f'select _wstart AS wstart, {self.tdCom.stb_source_select_str}  from {tbname} {additional_options} partition by {partition} interval({self.tdCom.dataDict["interval"]}s) order by wstart', fill_value=fill_value)

    def run(self):
        for fill_value in [None, "VALUE", "NULL", "PREV", "NEXT", "LINEAR"]:
            self.docontinuous(
                interval=random.randint(10, 15),
                partition="tbname",
                fill_value=fill_value
            )
        for fill_value in ["VALUE", "NULL", "PREV", "NEXT", "LINEAR", None]:
            self.docontinuous(
                interval=random.randint(10, 12),
                partition="t1 as t5,t2 as t11,t3 as t13, t4",
                fill_value=fill_value
            )

    def stop(self):
        tdLog.info("stop========================================")
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


event = threading.Event()


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
