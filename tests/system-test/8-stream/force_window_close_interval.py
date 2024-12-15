import sys
import threading
from util.log import *
from util.sql import *
from util.cases import *
from util.common import *


class TDTestCase:
    updatecfgDict = {"debugFlag": 135, "asynclog": 0}

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.tdCom = tdCom
    
    def get_source_firt_ts(self, table_name1):
        tdSql.query(
            f'select  cast(first(ts) as bigint) from  {table_name1} order by 1'
        )
        # getData don't support negative index
        res_ts = tdSql.getData(0, 0)
        return res_ts

    def get_source_last_ts(self, table_name1):
        tdSql.query(
            f'select  cast(last(ts) as bigint) from  {table_name1} order by 1'
        )
        # getData don't support negative index
        res_ts = tdSql.getData(0, 0)
        return res_ts
    
    def get_stream_first_win_ts(self, table_name1):
        tdSql.query(
            f'select _wstart, count(*) from  {table_name1} interval({self.tdCom.dataDict["interval"]}s) order by 1'
        )
        res_ts = tdSql.getData(0, 0)
        return res_ts
    
    def insert_data(self, custom_col_index, col_value_type):
        self.tdCom.date_time = self.tdCom.genTs(precision=self.tdCom.precision)[0]
        time.sleep(1)
        min_new_ts = 0
        for i in range(self.tdCom.range_count):
            cur_time = str(self.tdCom.date_time + self.tdCom.dataDict["interval"])
            ts_value = (
                cur_time + f"+{i * 5 + 30}s"
            )
            if min_new_ts == 0:
                min_new_ts = ts_value

            ts_cast_delete_value = self.tdCom.time_cast(ts_value)
            self.tdCom.sinsert_rows(
                tbname=self.tdCom.ctb_name,
                ts_value=ts_value,
                custom_col_index=custom_col_index,
                col_value_type=col_value_type,
            )
            if i % 2 == 0 and min_new_ts != ts_value:
                self.tdCom.sinsert_rows(
                    tbname=self.tdCom.ctb_name,
                    ts_value=ts_value,
                    custom_col_index=custom_col_index,
                    col_value_type=col_value_type,
                )
            if self.delete and i % 2 != 0:
                self.tdCom.sdelete_rows(
                    tbname=self.tdCom.ctb_name, start_ts=ts_cast_delete_value
                )
            self.tdCom.date_time += 1
            self.tdCom.sinsert_rows(
                tbname=self.tdCom.tb_name,
                ts_value=ts_value,
                custom_col_index=custom_col_index,
                col_value_type=col_value_type,
            )
            if i % 2 == 0 and min_new_ts != ts_value:
                self.tdCom.sinsert_rows(
                    tbname=self.tdCom.tb_name,
                    ts_value=ts_value,
                    custom_col_index=custom_col_index,
                    col_value_type=col_value_type,
                )
            if self.delete and i % 2 != 0:
                self.tdCom.sdelete_rows(
                    tbname=self.tdCom.tb_name, start_ts=ts_cast_delete_value
                )
            self.tdCom.date_time += 1
        cur_time = str(self.tdCom.date_time + self.tdCom.dataDict["interval"])
        max_new_ts = (cur_time + f"+{self.tdCom.range_count * 10 + 30}s")
        self.tdCom.sinsert_rows(
                tbname=self.tdCom.ctb_name,
                ts_value=max_new_ts,
                custom_col_index=custom_col_index,
                col_value_type=col_value_type,
        )
        self.tdCom.sinsert_rows(
                tbname=self.tdCom.tb_name,
                ts_value=max_new_ts,
                custom_col_index=custom_col_index,
                col_value_type=col_value_type,
        )
        return (min_new_ts, max_new_ts)

    def insert_disorder_data(self, custom_col_index, col_value_type):
        min_ts = self.get_source_firt_ts(self.tb_name)
        max_ts = self.get_source_last_ts(self.tb_name)
        min_ts_str = str(min_ts) + f"-10000s"
        max_ts_str = str(max_ts) + f"+10000s"
        self.tdCom.sinsert_rows(
                tbname=self.tdCom.ctb_name,
                ts_value=min_ts_str,
                custom_col_index=custom_col_index,
                col_value_type=col_value_type,
        )
        self.tdCom.sinsert_rows(
                tbname=self.tdCom.tb_name,
                ts_value=min_ts_str,
                custom_col_index=custom_col_index,
                col_value_type=col_value_type,
        )
        self.tdCom.sinsert_rows(
                tbname=self.tdCom.ctb_name,
                ts_value=max_ts_str,
                custom_col_index=custom_col_index,
                col_value_type=col_value_type,
        )
        self.tdCom.sinsert_rows(
                tbname=self.tdCom.tb_name,
                ts_value=max_ts_str,
                custom_col_index=custom_col_index,
                col_value_type=col_value_type,
        )

    def force_window_close(
        self,
        interval,
        partition="tbname",
        delete=False,
        fill_value=None,
        filter=None
    ):
        # partition must be tbname, and not NONE.
        tdLog.info(
            f"*** testing stream force_window_close + interval + fill. partition: {partition}, interval: {interval}, fill: {fill_value}, delete: {delete} ***"
        )
        fwc_downsampling_function_list = ["min(c1)", "max(c2)", "sum(c3)", "twa(c7)", "count(c8)", "elapsed(ts)", "timediff(1, 0, 1h)", "timezone()","min(t1)", "max(t2)", "sum(t3)",
            "twa(t7)", "count(t8)"]
        fwc_stb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', fwc_downsampling_function_list)))
        fwc_tb_output_select_str = ','.join(list(map(lambda x:f'`{x}`', fwc_downsampling_function_list[0:7])))
        fwc_stb_source_select_str = ','.join(fwc_downsampling_function_list)
        fwc_tb_source_select_str = ','.join(fwc_downsampling_function_list[0:7])

        fill_history_value = 0
        ignore_expired = 1
        ignore_update = 1
        self.tdCom.subtable = False
        col_value_type = "Incremental" if partition == "c1" else "random"
        custom_col_index = 1 if partition == "c1" else None
        self.tdCom.custom_col_val = 0
        self.delete = delete
        self.tdCom.case_name = sys._getframe().f_code.co_name
        self.tdCom.prepare_data(
            interval=interval,
            custom_col_index=custom_col_index,
            col_value_type=col_value_type,
        )
        self.stb_name = self.tdCom.stb_name.replace(f"{self.tdCom.dbname}.", "")
        self.ctb_name = self.tdCom.ctb_name.replace(f"{self.tdCom.dbname}.", "")
        self.tb_name = self.tdCom.tb_name.replace(f"{self.tdCom.dbname}.", "")
        self.stb_stream_des_table = f"{self.stb_name}{self.tdCom.des_table_suffix}"

        self.ctb_stream_des_table = f"{self.ctb_name}{self.tdCom.des_table_suffix}"
        self.tb_stream_des_table = f"{self.tb_name}{self.tdCom.des_table_suffix}"
        
        if partition:
            partition_elm = f"partition by {partition}"
        else:
            partition_elm = ""
        
        query_partition_elm = partition_elm

        if fill_value:
            if "value" in fill_value.lower():
                stb_fill_value='VALUE,1,2,3,4,5,6,1,2,3,4,5'
                tb_fill_value='VALUE,1,2,3,4,5,6'
            else:
                stb_fill_value=fill_value
                tb_fill_value=fill_value
            query_stb_fill_elm = f"fill({stb_fill_value})"
            query_tb_fill_elm = f"fill({tb_fill_value})"
        else:
            query_stb_fill_elm = ""
            query_tb_fill_elm = ""
            stb_fill_value = None
            tb_fill_value=None
        
        where_elm = "where 1=1"
        if filter:
            where_elm = f" and {filter}"

        trigger_mode = "force_window_close"

        # no subtable
        # create stream super table and child table
        tdLog.info("create stream super table and child table")
        self.tdCom.create_stream(
            stream_name=f'{self.stb_name}{self.tdCom.stream_suffix}',
            des_table=self.stb_stream_des_table,
            source_sql=f'select _wstart AS wstart, {fwc_stb_source_select_str} from {self.stb_name} {where_elm} {partition_elm} interval({self.tdCom.dataDict["interval"]}s)',
            trigger_mode=trigger_mode,
            fill_value=stb_fill_value,
            fill_history_value=fill_history_value,
            ignore_expired=ignore_expired,
            ignore_update=ignore_update,
        )

        self.tdCom.create_stream(
            stream_name=f"{self.tb_name}{self.tdCom.stream_suffix}",
            des_table=self.tb_stream_des_table,
            source_sql=f'select _wstart AS wstart, {fwc_tb_source_select_str} from {self.tb_name} {where_elm} {partition_elm} interval({self.tdCom.dataDict["interval"]}s)',
            trigger_mode=trigger_mode,
            fill_value=tb_fill_value,
            fill_history_value=fill_history_value,
            ignore_expired=ignore_expired,
            ignore_update=ignore_update,
        )

        # wait and check stream_task status is ready
        tdSql.query("show streams")
        tdLog.info(f"tdSql.queryResult:{tdSql.queryResult},tdSql.queryRows:{tdSql.queryRows}")
        localQueryResult = tdSql.queryResult
        for stream_number in range(tdSql.queryRows):
            stream_name = localQueryResult[stream_number][0]
            tdCom.check_stream_task_status(
                stream_name=stream_name, vgroups=2, stream_timeout=20,check_wal_info=False
            )
        time.sleep(self.tdCom.dataDict["interval"])
        time.sleep(20)

        # insert data
        tdLog.info("insert data")
        start_new_ts, temp = self.insert_data(custom_col_index, col_value_type)
        time.sleep(self.tdCom.dataDict["interval"] * 2)
        tdLog.info("insert data")
        temp, end_new_ts = self.insert_data(custom_col_index, col_value_type)

        #history and future
        self.insert_disorder_data(custom_col_index, col_value_type)

        time.sleep(self.tdCom.dataDict["interval"] * 6 * 2)

        tdLog.info("check data")
        # check the data
        where_elm = f'{where_elm} and ts >= {start_new_ts} and ts <= {end_new_ts}'
        for tbname in [self.stb_name, self.tb_name]:
            if fill_value:
                query_first_win_ts = self.get_stream_first_win_ts(tbname)
                query_where_elm = f'where wstart >= "{query_first_win_ts}"'
                stream_where_elm = f'where wstart <= {end_new_ts}'
            else:
                query_where_elm = ""
                stream_where_elm = ""

            # check data
            tdLog.info(f"check data for table {tbname}")
            if tbname == self.stb_name:
                self.tdCom.check_query_data(
                    f'select wstart, {fwc_stb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} {stream_where_elm} order by wstart',
                    f'select * from (select _wstart AS wstart, {fwc_stb_source_select_str}  from {tbname} {where_elm} {query_partition_elm} interval({self.tdCom.dataDict["interval"]}s) {query_stb_fill_elm} order by wstart) {query_where_elm}',
                    sorted=True
                )
            else:
                self.tdCom.check_query_data(
                    f'select wstart, {fwc_tb_output_select_str} from {tbname}{self.tdCom.des_table_suffix} {stream_where_elm} order by wstart',
                    f'select * from (select _wstart AS wstart, {fwc_tb_source_select_str}  from  {tbname} {where_elm} {query_partition_elm} interval({self.tdCom.dataDict["interval"]}s) {query_tb_fill_elm} order by wstart) {query_where_elm}',
                    sorted=True
                )

    def run(self):
        for fill_value in ["VALUE", "NULL", "PREV", None]:
            self.force_window_close(
                interval=5,
                partition="tbname",
                delete=True,
                fill_value=fill_value,
            )

    def stop(self):
        tdLog.info("stop========================================")
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


event = threading.Event()


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
