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

    def partitionby_interval(self, interval=None, partition_by_elm="tbname", ignore_expired=None):
        tdLog.info(f"*** testing stream partition+interval: interval: {interval}, partition_by: {partition_by_elm}, ignore_expired: {ignore_expired} ***")
        self.tdCom.case_name = sys._getframe().f_code.co_name
        self.tdCom.prepare_data(interval=interval)
        self.stb_name = self.tdCom.stb_name.replace(f"{self.tdCom.dbname}.", "")
        self.ctb_name = self.tdCom.ctb_name.replace(f"{self.tdCom.dbname}.", "")
        self.tb_name = self.tdCom.tb_name.replace(f"{self.tdCom.dbname}.", "")
        self.stb_stream_des_table = f'{self.stb_name}{self.tdCom.des_table_suffix}'
        self.ctb_stream_des_table = f'{self.ctb_name}{self.tdCom.des_table_suffix}'
        self.tb_stream_des_table = f'{self.tb_name}{self.tdCom.des_table_suffix}'
        ctb_name_list = list()
        for i in range(1, self.tdCom.range_count):
            ctb_name = self.tdCom.get_long_name()
            ctb_name_list.append(ctb_name)
            self.tdCom.screate_ctable(stbname=self.stb_name, ctbname=ctb_name)
        if interval is not None:
            source_sql = f'select _wstart AS wstart, {self.tdCom.partition_by_stb_source_select_str}  from {self.stb_name} partition by {partition_by_elm} interval({self.tdCom.dataDict["interval"]}s)'
        else:
            source_sql = f'select {self.tdCom.stb_filter_des_select_elm} from {self.stb_name} partition by {partition_by_elm}'

        # create stb/ctb/tb stream
        self.tdCom.create_stream(stream_name=f'{self.stb_name}{self.tdCom.stream_suffix}', des_table=self.stb_stream_des_table, source_sql=source_sql, ignore_expired=ignore_expired)

        time.sleep(1)

        # insert data
        count = 1
        step_count = 1
        for i in range(1, self.tdCom.range_count):
            if i == 1:
                record_window_close_ts = self.tdCom.date_time - 15 * self.tdCom.offset
            ctb_name = self.tdCom.get_long_name()
            self.tdCom.screate_ctable(stbname=self.stb_name, ctbname=ctb_name)
            if i % 2 == 0:
                step_count += i
                for j in range(count, step_count):
                    self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=f'{self.tdCom.date_time}+{j}s')
                    for ctb_name in ctb_name_list:
                        self.tdCom.sinsert_rows(tbname=ctb_name, ts_value=f'{self.tdCom.date_time}+{j}s')
                count += i
            else:
                step_count += 1
                for i in range(2):
                    self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=f'{self.tdCom.date_time}+{count}s')
                    for ctb_name in ctb_name_list:
                        self.tdCom.sinsert_rows(tbname=ctb_name, ts_value=f'{self.tdCom.date_time}+{count}s')
                count += 1
            # check result
            for colname in self.tdCom.partition_by_downsampling_function_list:
                if "first" not in colname and "last" not in colname:
                    if interval is not None:
                        self.tdCom.check_query_data(f'select `{colname}` from {self.stb_name}{self.tdCom.des_table_suffix} order by `{colname}`;', f'select {colname}  from {self.stb_name} partition by {partition_by_elm} interval({self.tdCom.dataDict["interval"]}s) order by `{colname}`;')
                    else:
                        self.tdCom.check_query_data(f'select {self.tdCom.stb_filter_des_select_elm} from {self.stb_name}{self.tdCom.des_table_suffix} order by c1,c2,c3;', f'select {self.tdCom.stb_filter_des_select_elm}  from {self.stb_name} partition by {partition_by_elm} order by c1,c2,c3;')

        if self.tdCom.disorder:
            self.tdCom.sinsert_rows(tbname=self.ctb_name, ts_value=record_window_close_ts)
            for ctb_name in ctb_name_list:
                self.tdCom.sinsert_rows(tbname=ctb_name, ts_value=record_window_close_ts)
            if ignore_expired:
                if "first" not in colname and "last" not in colname:
                    for colname in self.tdCom.partition_by_downsampling_function_list:
                        if interval is not None:
                            tdSql.query(f'select `{colname}` from {self.stb_name}{self.tdCom.des_table_suffix} order by `{colname}`;')
                            res1 = tdSql.queryResult
                            tdSql.query(f'select {colname}  from {self.stb_name} partition by {partition_by_elm} interval({self.tdCom.dataDict["interval"]}s) order by `{colname}`;')
                            res2 = tdSql.queryResult
                            tdSql.checkNotEqual(res1, res2)
                        else:
                            self.tdCom.check_query_data(f'select {self.tdCom.stb_filter_des_select_elm} from {self.stb_name}{self.tdCom.des_table_suffix} order by c1,c2,c3;', f'select {self.tdCom.stb_filter_des_select_elm}  from {self.stb_name} partition by {partition_by_elm} order by c1,c2,c3;')

            else:
                for colname in self.tdCom.partition_by_downsampling_function_list:
                    if "first" not in colname and "last" not in colname:
                        if interval is not None:
                            self.tdCom.check_query_data(f'select `{colname}` from {self.stb_name}{self.tdCom.des_table_suffix} order by `{colname}`;', f'select {colname}  from {self.stb_name} partition by {partition_by_elm} interval({self.tdCom.dataDict["interval"]}s) order by `{colname}`;')
                        else:
                            self.tdCom.check_query_data(f'select {self.tdCom.stb_filter_des_select_elm} from {self.stb_name}{self.tdCom.des_table_suffix} order by c1,c2,c3;', f'select {self.tdCom.stb_filter_des_select_elm}  from {self.stb_name} partition by {partition_by_elm} order by c1,c2,c3;')

    def run(self):
        for interval in [None, 10]:
            for ignore_expired in [0, 1]:
                self.partitionby_interval(interval=interval, partition_by_elm="tbname", ignore_expired=ignore_expired)
        self.partitionby_interval(interval=10, partition_by_elm="t1")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())