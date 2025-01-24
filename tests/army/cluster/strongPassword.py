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
        tdSql.error("create user test pass '12345678' sysinfo 0;", expectErrInfo="Invalid password format")

        tdSql.execute("create user test pass '12345678@Abc' sysinfo 0;")

        tdSql.error("alter user test pass '23456789'", expectErrInfo="Invalid password format")

        tdSql.execute("alter user test pass '23456789@Abc';")

        # change setting
        tdSql.execute("ALTER ALL DNODES 'enableStrongPassword' '0'")

        # weak
        tdSql.execute("create user test1 pass '12345678' sysinfo 0;")

        tdSql.execute("alter user test1 pass '12345678';")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
