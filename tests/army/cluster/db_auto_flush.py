import taos
import sys
import os
import subprocess
import glob
import shutil
import time

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.srvCtl import *
from frame.caseBase import *
from frame import *
from frame.autogen import *
from frame import epath
# from frame.server.dnodes import *
# from frame.server.cluster import *


class TDTestCase(TBase):
    
    def init(self, conn, logSql, replicaVar=1):
        updatecfgDict = {'dDebugFlag':131}
        super(TDTestCase, self).init(conn, logSql, replicaVar=1, checkColName="c1")
        
        self.valgrind = 0
        self.db = "test"
        self.stb = "meters"
        self.childtable_count = 10
        tdSql.init(conn.cursor(), logSql)  

    def run(self):
        tdSql.execute('CREATE DATABASE db1 vgroups 1 replica 3 FLUSH_INTERVAL 10;')

        if self.waitTransactionZero() is False:
            tdLog.exit(f"transaction not finished")
            return False

        tdSql.execute('CREATE DATABASE db2 vgroups 1 replica 1 FLUSH_INTERVAL 20;')

        if self.waitTransactionZero() is False:
            tdLog.exit(f"transaction not finished")
            return False

        tdSql.execute('alter DATABASE db2 FLUSH_INTERVAL 30;')

        if self.waitTransactionZero() is False:
            tdLog.exit(f"transaction not finished")
            return False

        tdSql.query('SHOW CREATE DATABASE db2;')
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "CREATE DATABASE `db2` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 2 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 1 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0 ENCRYPT_ALGORITHM 'none' S3_CHUNKPAGES 131072 S3_KEEPLOCAL 525600m S3_COMPACT 1 COMPACT_INTERVAL 0d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h FLUSH_INTERVAL 20")

        tdSql.query("select name,`flush_interval` from information_schema.ins_databases where name = 'db2';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 30)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
