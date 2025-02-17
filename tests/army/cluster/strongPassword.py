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
        super(TDTestCase, self).init(conn, logSql, replicaVar=1, checkColName="c1")
        
        tdSql.init(conn.cursor(), logSql)  

    def run(self):
        # strong
        tdSql.error("create user test pass '12345678' sysinfo 0;", expectErrInfo="Invalid password")

        tdSql.execute("create user test pass '12345678@Abc' sysinfo 0;")

        tdSql.error("alter user test pass '23456789'", expectErrInfo="Invalid password")

        tdSql.execute("alter user test pass '23456789@Abc';")

        # change setting
        tdSql.execute("ALTER ALL DNODES 'enableStrongPassword' '0'")

        time.sleep(3)

        # weak
        tdSql.execute("create user test1 pass '12345678' sysinfo 0;")

        tdSql.execute("alter user test1 pass '12345678';")

        # pass length    
        tdSql.error("alter user test1 pass '1234567';", expectErrInfo="Password too short or empty")
        
        tdSql.error("create user test2 pass '1234567' sysinfo 0;", expectErrInfo="Password too short or empty")

        tdSql.error("create user test2 pass '1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456' sysinfo 0;", expectErrInfo="Name or password too long")

        tdSql.execute("create user test2 pass '123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345' sysinfo 0;")

        cmd = "taos -u test2 -p123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345 -s 'show databases;'"
        if os.system(cmd) != 0:
            raise Exception("failed to execute system command. cmd: %s" % cmd)

        tdSql.error("alter user test2 pass '1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456';", expectErrInfo="Name or password too long")

        tdSql.execute("CREATE USER `_xTest1` PASS '2729c41a99b2c5222aa7dd9fc1ce3de7' SYSINFO 1 CREATEDB 0 IS_IMPORT 1 HOST '127.0.0.1';")

        tdSql.error("CREATE USER `_xTest2` PASS '2729c41a99b2c5222aa7dd9fc1ce3de7' SYSINFO 1 CREATEDB 0 IS_IMPORT 0 HOST '127.0.0.1';", expectErrInfo="Invalid password")

        tdSql.error("CREATE USER `_xTest3` PASS '2729c41' SYSINFO 1 CREATEDB 0 IS_IMPORT 1 HOST '127.0.0.1';", expectErrInfo="Invalid password")

        tdSql.error("CREATE USER `_xTest4` PASS '2729c417' SYSINFO 1 CREATEDB 0 IS_IMPORT 0 HOST '127.0.0.1';", expectErrInfo="Invalid password")

        tdSql.error("CREATE USER `_xTest5` PASS '2xF' SYSINFO 1 CREATEDB 0 IS_IMPORT 1 HOST '127.0.0.1';", expectErrInfo="Invalid password")

        tdSql.error("CREATE USER `_xTest6` PASS '2xF' SYSINFO 1 CREATEDB 0 IS_IMPORT 0 HOST '127.0.0.1';", expectErrInfo="Invalid password")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
