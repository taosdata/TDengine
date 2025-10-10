
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
from taos.tmq import *
from taos import *

sys.path.append("./7-tmq")
from tmqCommon import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def test(self):
        tdSql.execute(f'drop topic if exists topic1');
        tdSql.execute(f'drop topic if exists topic2');
        tdSql.execute(f'drop database if exists test');
        tdSql.execute(f'create database test vgroups 1');
        tdSql.execute(f'use test');
        tdSql.execute(f'create table st (ts timestamp, id int) tags (t1 int)');

        tdSql.execute("create topic topic1 with meta as stable st where t1 = 2")
        tdSql.execute("create topic topic2 with meta as database test")

        tdSql.execute(f'create table nt (ts timestamp, id int)');
        tdSql.execute(f'create table t1 using st tags(1)');
        tdSql.execute(f'create table t2 using st tags(2)');
        tdSql.execute(f"insert into t1 values ('2025-01-01 00:00:01', 0)")
        tdSql.execute(f"insert into t1 values ('2025-01-01 00:00:02', 0)")
        tdSql.execute(f"insert into t3 using st tags(2) values ('2025-01-02 00:00:03', 0)")
        tdSql.execute(f"insert into t4 using st tags(3) values ('2025-01-02 00:00:04', 0)")
        tdSql.execute(f'alter table t1 set tag t1=2');
        tdSql.execute(f"insert into t1 values ('2025-01-01 00:00:05', 0)")
        tdSql.execute(f'alter table t2 set tag t1=23');
        tdSql.execute(f"insert into t2 values ('2025-01-01 00:00:06', 0)")
        tdLog.info("write data done, wait tmq process exit")

        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_ts7402'%(buildPath)
        # cmdStr = '/Users/mingming/code/TDengine2/debug/build/bin/tmq_ts7402'
        tdLog.info(cmdStr)
        ret = os.system(cmdStr)
        if ret != 0:
            raise Exception("run failed")

        tdLog.success(f"{__file__} successfully executed")


    def run(self):
        self.test()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())