
import taos
import sys
import time
import socket
import os
import threading
import math

from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    cluster,
    StreamCheckItem,
)

class Test_snode_restart_with_checkpoint:
    # updatecfgDict = {'checkpointInterval': 5}
    # print("===================: ", updatecfgDict)

    def setup_class(cls):
        tdLog.info(f"start to excute {__file__}")
        # tdSql.init(conn.cursor(), True)


    def test_case1(self):
        """OldPy: snode

        1. -

        Catalog:
            - Streams:OldPyCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-21 lvze Migrated from community/tests/system-test/8-stream/snode_restart_with_checkpoint.py -N 4

        """
        tdLog.info("========case1 start========")

        os.system("nohup taosBenchmark -y -B 1 -t 4 -S 1000 -n 1000 -i 1000 -v 2  > /dev/null 2>&1 &")
        time.sleep(4)
        tdSql.query("use test", queryTimes=60)
        tdSql.query("create snode on dnode 4")
        tdSql.query(f"""
                    create stream test.s1 interval(2s) sliding(2s) from test.meters 
                    partition by groupid
                    stream_options(fill_history| low_latency_calc)
                    into test.st1 
                    tags(
                    groupid int as groupid)
                    as select
                        _twstart  ts,
                        sum(voltage) voltage
                    from
                        %%trows t1 ;
                    """)
        tdLog.info("========create stream using snode and insert data ok========")
        # time.sleep(60)
        self.checkStreamRunning()
        

        tdDnodes = cluster.dnodes
        tdDnodes[3].stoptaosd()
        time.sleep(2)
        os.system("unset LD_PRELOAD;kill -9 `pgrep taosBenchmark`")
        tdLog.info("========stop insert ok========")
        tdDnodes[3].starttaosd()
        tdLog.info("========snode restart ok========")

        self.checkStreamRunning()

        tdSql.query("select _wstart,sum(voltage),groupid from meters partition by groupid interval(2s) order by groupid,_wstart",queryTimes=3)
        rowCnt = tdSql.getRows()
        tdLog.info(f"result num is {rowCnt}")
        # results = []
        # for i in range(rowCnt):
        #     results.append(tdSql.getData(i,1))

        sql = "select * from st1 order by groupid,`ts`"
        for i in range(100):
            tdSql.query(sql,queryTimes=3)
            time.sleep(1)
            if tdSql.getRows() == rowCnt:
                break
        rowCnt = tdSql.getRows()
        tdLog.info(f"stream result num is {rowCnt}")
        
        # tdSql.checkRowsLoop(rowCnt, sql, loopCount=100, waitTime=3)
        # stRow = tdSql.getRows()
        # tdLog.info(f"stream result num is {stRow}")
        # if stRow < rowCnt -1:
        #     raise Exception("error:result is not right")

        # tdSql.checkRows(rowCnt)
        # for i in range(rowCnt):
        #     data1 = tdSql.getData(i,1)
        #     data2 = results[i]
        #     if data1 != data2:
        #         tdLog.info("num: %d, act data: %d, expect data: %d"%(i, data1, data2))
        #         tdLog.exit("check data error!")


        tdLog.info("case1 end")

    def run(self):
        self.case1()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
        
    def checkStreamRunning(self):
        tdLog.info(f"check stream running status:")

        timeout = 60 
        start_time = time.time()

        while True:
            if time.time() - start_time > timeout:
                tdLog.error("Timeout waiting for all streams to be running.")
                tdLog.error(f"Final stream running status: {streamRunning}")
                raise TimeoutError(f"Stream status did not reach 'Running' within {timeout}s timeout.")
            
            tdSql.query(f"select status from information_schema.ins_streams order by stream_name;")
            streamRunning=tdSql.getColData(0)

            if all(status == "Running" for status in streamRunning):
                tdLog.info("All Stream running!")
                tdLog.info(f"stream running status: {streamRunning}")
                return
            else:
                tdLog.info("Stream not running! Wait stream running ...")
                tdLog.info(f"stream running status: {streamRunning}")
                time.sleep(1)

event = threading.Event()

