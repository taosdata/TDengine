import taos
import sys
import time
import socket
import os
import threading

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
class TDTestCase:
    """
    test for TD-32391
    """
    clientCfgDict = {'debugFlag': 131}
    updatecfgDict = {
        "debugFlag"        : "131",
        "queryBufferSize"  : 10240,
        'clientCfg'        : clientCfgDict
    }
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor())
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def test_td_32391(self):
        tdLog.printNoPrefix("======== test TD-32391")
        taosBen_bin = tdCom.get_path(tool="taosBenchmark")
        if not taosBen_bin:
            tdLog.exit("taosBenchmark not found!")
        else:
            tdLog.info(f"taosBenchmark found: {taosBen_bin}")
        os.system(f"{taosBen_bin} -f  ./99-TDcase/com_totest_stream.json")
        tdSql.execute("create stream if not exists insert_energy_consmption fill_history 1 trigger window_close watermark 10s ignore expired 1 into test.energy_consumption_record tags(device_no varchar(64)) subtable(tbname) as select _wstart as ts_start ,_wend as ts_end,_wduration as ts_duration, (first(val) + last(val))/2*(_wduration/1000) as val, first(order_no) as order_no, first(production_no) as production_no, first(modal_no) as modal_no from test.meters partition by tbname,device_no count_window(2,1);")
        tdSql.execute("create stream if not exists insert_oee_status fill_history 1 trigger window_close into test.oee_status tags(device_no varchar(64)) subtable(device_no) as select max(case point_no when 'STP0' then 1 when 'STP1' then 3 when 'STP2' then 2 when 'STP4' then 4 else null end) as status_index, first(order_no) as order_no, first(production_no) as production_no, first(modal_no) as modal_no from test.meters partition by device_no interval(2s);")
        os.system(f"{taosBen_bin} -f  ./99-TDcase/com_totest_stream_continue.json")
        tdSql.query("select count(*) from test.meters")



    def run(self):
        self.test_td_32391()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
