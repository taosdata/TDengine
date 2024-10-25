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

    def force_window_close(self, interval, partition="tbname", funciton_name="",delete=False, fill_value=None, fill_history_value=None, case_when=None):
        tdLog.info(f"*** testing stream at_once+interval: interval: {interval}, partition: {partition}, fill_history: {fill_history_value}, fill: {fill_value}, delete: {delete}, case_when: {case_when} ***")
        #prepare data
        tdSql.execute(f"create database dbtest;")
        tdSql.execute(f"use dbtest;")
        tdSql.execute(f"create STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT);")
        # create stream 
        #  create stream itp_force_5s_pre  trigger force_window_close  into itp_5s_pre as  select _irowts,tbname,_isfilled,interp(current) from meters   partition by tbname   every(5s)   fill(prev)  ;
        funciton_name = ["interp","twa"]
        trigger_mode = "force_window_close"
        interval = ["5s","10s"]
        for funciton_name in funciton_name:
            for trigger_mode in trigger_mode:
                for interval in interval:
                    stream_name = f"{funciton_name}_{trigger_mode}_{interval}"
                    stream_sql = f"create stream {stream_name} AS SELECT * FROM meters AT ONCE INTERVAL {interval} SECOND"
                    tdSql.execute(f"create stream meters_stream AS SELECT * FROM meters;")
                    tdSql.execute(f"create stream {stream_name} AS SELECT * FROM meters AT ONCE INTERVAL {interval} SECOND")
                    if partition:
                        tdSql.execute(f"create stream {stream_name} AS SELECT * FROM meters AT ONCE INTERVAL {interval} SECOND PARTITION BY {partition}")
                    if fill_value:
                        tdSql.execute(f"create stream {stream_name} AS SELECT * FROM meters AT ONCE INTERVAL {interval} SECOND FILL({fill_value})")
                    if fill_history_value:
                        tdSql.execute(f"create stream {stream_name} AS SELECT * FROM meters AT ONCE INTERVAL {interval} SECOND FILL({fill_value}) FILL_HISTORY_VALUE({fill_history_value})")
                    if case_when:
                        tdSql.execute(f"create stream {stream_name} AS SELECT * FROM meters AT ONCE INTERVAL {interval} SECOND PARTITION BY {partition} CASE WHEN {case_when}")

        # stream_name = f"{funciton_name}_{trigger_mode}_{interval}"
        # stream_sql = f"create stream {stream_name} AS SELECT * FROM meters AT ONCE INTERVAL {interval} SECOND"
        # tdSql.execute(f"create stream meters_stream AS SELECT * FROM meters;")

    def run(self):
        self.at_once_interval(interval=random.randint(10, 15), partition="tbname", delete=True)
        self.at_once_interval(interval=random.randint(10, 15), partition="c1", delete=True)
        self.at_once_interval(interval=random.randint(10, 15), partition="abs(c1)", delete=True)
        self.at_once_interval(interval=random.randint(10, 15), partition=None, delete=True)
        self.at_once_interval(interval=random.randint(10, 15), partition=self.tdCom.stream_case_when_tbname, case_when=f'case when {self.tdCom.stream_case_when_tbname} = tbname then {self.tdCom.partition_tbname_alias} else tbname end')
        self.at_once_interval(interval=random.randint(10, 15), partition="tbname", fill_history_value=1, fill_value="NULL")
        for fill_value in ["NULL", "PREV", "NEXT", "LINEAR", "VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11"]:
        # for fill_value in ["PREV", "NEXT", "LINEAR", "VALUE,1,2,3,4,5,6,7,8,9,10,11,1,2,3,4,5,6,7,8,9,10,11"]:
            self.at_once_interval(interval=random.randint(10, 15), partition="tbname", fill_value=fill_value)
            self.at_once_interval(interval=random.randint(10, 15), partition="tbname", fill_value=fill_value, delete=True)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())